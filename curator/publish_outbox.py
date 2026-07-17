from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .config import load_config
from .dates import datetime_to_iso
from .remote_api import post_remote_action, remote_api_configured
from .state import load_state, save_state
from .telegram_publisher import (
    fail_closed_telegram_delivery_outcome,
    process_telegram_delivery_outbox,
    send_telegram_message,
    telegram_bot_token,
    telegram_chat_id,
    telegram_is_configured,
)
from .telegram_sources import load_env_files


DEFAULT_REMOTE_DELIVERY_LIMIT = 5
DEFAULT_DELIVERY_LEASE_SECONDS = 900
MIN_DELIVERY_LEASE_SECONDS = 300
MAX_DELIVERY_LEASE_SECONDS = 1800


def _int_value(value: object, default: int = 0) -> int:
    if isinstance(value, (int, str, bytes, bytearray)):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
    return default


def _claimed_items(response: dict[str, Any]) -> list[dict[str, object]]:
    for key in ("items", "deliveries", "outbox"):
        value = response.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def _delivery_text(item: dict[str, object]) -> str:
    direct = str(item.get("payload_text") or item.get("text") or "")
    if direct:
        return direct
    payload = item.get("payload")
    if isinstance(payload, dict):
        return str(payload.get("text") or payload.get("payload_text") or "")
    payload_json = item.get("payload_json")
    if isinstance(payload_json, str) and payload_json:
        try:
            parsed = json.loads(payload_json)
        except ValueError:
            return ""
        if isinstance(parsed, dict):
            return str(parsed.get("text") or parsed.get("payload_text") or "")
    return ""


def _delivery_value(item: dict[str, object], name: str, default: object = None) -> object:
    if item.get(name) not in (None, ""):
        return item.get(name)
    payload = item.get("payload")
    if isinstance(payload, dict) and payload.get(name) not in (None, ""):
        return payload.get(name)
    payload_json = item.get("payload_json")
    if isinstance(payload_json, str) and payload_json:
        try:
            parsed = json.loads(payload_json)
        except ValueError:
            parsed = None
        if isinstance(parsed, dict) and parsed.get(name) not in (None, ""):
            return parsed.get(name)
    return default


def _ack_remote_delivery(payload: dict[str, object], attempts: int = 3) -> dict[str, Any]:
    """Retry an idempotent acknowledgement to avoid duplicate Telegram sends."""

    last: dict[str, Any] = {"ok": False, "error": "ack_delivery_outbox_failed"}
    for _attempt in range(max(1, attempts)):
        try:
            last = post_remote_action("ack_delivery_outbox", payload)
        except Exception as exc:  # noqa: BLE001 - retry the idempotent ack with the same external ID.
            last = {"ok": False, "error": exc.__class__.__name__}
            continue
        if last.get("ok"):
            return last
        status_code = _int_value(last.get("status_code"))
        if status_code and status_code < 500:
            break
    return last


def _delivery_lease_seconds() -> int:
    configured = _int_value(os.environ.get("DELIVERY_LEASE_SECONDS"), DEFAULT_DELIVERY_LEASE_SECONDS)
    return max(MIN_DELIVERY_LEASE_SECONDS, min(MAX_DELIVERY_LEASE_SECONDS, configured))


def process_remote_delivery_outbox(
    config: dict[str, object],
    now: datetime,
    *,
    limit: int = DEFAULT_REMOTE_DELIVERY_LIMIT,
    delivery_id: str = "",
) -> dict[str, object]:
    worker_id = (
        os.environ.get("DELIVERY_WORKER_ID", "").strip()
        or f"github:{os.environ.get('GITHUB_RUN_ID', 'local')}:{os.environ.get('GITHUB_RUN_ATTEMPT', '1')}"
    )
    lease_seconds = _delivery_lease_seconds()
    item_budget = 1 if delivery_id else max(1, int(limit))
    claimed_count = sent = failed = 0
    baseline_dead_letter: int | None = None
    reported_dead_letter = 0
    new_dead_letter_ids: set[str] = set()
    rights_blocked = 0
    outcome_unknown = 0
    requested_status = ""
    already_delivered = False
    summary_error = ""
    seen_delivery_ids: set[str] = set()

    # Claim one row at a time. A lease therefore starts immediately before its
    # external send rather than while dozens of earlier messages are processed.
    while claimed_count < item_budget:
        claim_payload: dict[str, object] = {
            "channel": "telegram",
            "limit": 1,
            "lease_seconds": lease_seconds,
            "worker_id": worker_id,
        }
        if delivery_id:
            claim_payload["delivery_id"] = delivery_id
        try:
            claim = post_remote_action("claim_delivery_outbox", claim_payload)
        except Exception as exc:  # noqa: BLE001 - expose a structured nonzero result to Actions.
            claim = {"ok": False, "error": exc.__class__.__name__}
        if not claim.get("ok"):
            failed += 1
            summary_error = str(claim.get("error") or claim.get("reason") or "claim_delivery_outbox_failed")
            break

        current_dead_letter = _int_value(claim.get("dead_letter_count"))
        if baseline_dead_letter is None:
            baseline_dead_letter = current_dead_letter
        reported_dead_letter = max(reported_dead_letter, current_dead_letter)
        rights_blocked += _int_value(claim.get("rights_blocked_count"))
        outcome_unknown += _int_value(claim.get("outcome_unknown_count"))
        requested_status = str(claim.get("requested_status") or requested_status)
        if delivery_id and requested_status == "delivered":
            already_delivered = True
            break

        claimed = _claimed_items(claim)
        if not claimed:
            if delivery_id and not already_delivered:
                failed += 1
            break
        if len(claimed) != 1:
            # The server contract is deliberately singleton. Do not start any
            # external sends when that safety invariant is violated.
            failed += 1
            summary_error = "delivery_claim_not_singleton"
            break

        item = claimed[0]
        outbox_id = str(item.get("outbox_id") or item.get("delivery_id") or item.get("id") or "")
        lease_token = str(item.get("lease_token") or item.get("lock_token") or "")
        claimed_count += 1
        if outbox_id in seen_delivery_ids:
            failed += 1
            summary_error = "duplicate_delivery_claimed_in_run"
            break
        seen_delivery_ids.add(outbox_id)
        text = _delivery_text(item)
        destination = str(_delivery_value(item, "destination", telegram_chat_id(config)) or telegram_chat_id(config))
        disable_preview = bool(_delivery_value(item, "disable_web_page_preview", True))
        if not outbox_id or not text:
            response: dict[str, object] = {
                "ok": False,
                "error": "invalid_delivery_payload",
                "description": "outbox_id and payload text are required",
                "retryable": False,
            }
        else:
            response = send_telegram_message(
                telegram_bot_token(),
                destination,
                text,
                config,
                disable_web_page_preview=disable_preview,
            )
            response = fail_closed_telegram_delivery_outcome(response)

        if response.get("ok") and response.get("message_id") not in (None, ""):
            ack = _ack_remote_delivery(
                {
                    "outbox_id": outbox_id,
                    "delivery_id": outbox_id,
                    "lease_token": lease_token,
                    "worker_id": worker_id,
                    "external_message_id": str(response["message_id"]),
                    "external_chat_id": response.get("chat_id"),
                    "delivered_at": datetime_to_iso(now),
                }
            )
            if ack.get("ok"):
                sent += 1
                continue
            response = {
                "ok": False,
                "error": "ack_delivery_outbox_failed",
                "description": str(ack.get("error") or ack.get("reason") or "remote acknowledgement failed"),
                # Telegram already accepted this message.  Retrying the send could
                # duplicate a market-sensitive alert, so require reconciliation.
                "retryable": False,
                "external_message_id": response.get("message_id"),
            }

        failed += 1
        try:
            fail = post_remote_action(
                "fail_delivery_outbox",
                {
                    "outbox_id": outbox_id,
                    "delivery_id": outbox_id,
                    "lease_token": lease_token,
                    "worker_id": worker_id,
                    "error": str(response.get("error") or "telegram_delivery_failed")[:191],
                    "description": str(response.get("description") or "")[:500],
                    "status_code": response.get("status_code"),
                    "retryable": bool(response.get("retryable")),
                    "retry_after_seconds": _int_value(response.get("retry_after_seconds")),
                    "external_message_id": response.get("external_message_id"),
                    "attempted_at": datetime_to_iso(now),
                },
            )
        except Exception as exc:  # noqa: BLE001 - lease expiry remains the final recovery path.
            fail = {"ok": False, "error": exc.__class__.__name__}
        status = str(fail.get("status") or fail.get("delivery_status") or "")
        if status == "dead_letter" or bool(fail.get("dead_letter")):
            new_dead_letter_ids.add(outbox_id)
        if not fail.get("ok"):
            # The lease will eventually expire, but this run must surface that the
            # failure could not be durably recorded.
            new_dead_letter_ids.add(outbox_id)

    initial_dead_letter = baseline_dead_letter or 0
    dead_letter = max(reported_dead_letter, initial_dead_letter + len(new_dead_letter_ids))

    result: dict[str, object] = {
        "mode": "remote",
        "telegram_outbox_claimed": claimed_count,
        "telegram_sent": sent,
        "telegram_failed": failed,
        "telegram_dead_letter": dead_letter,
        "telegram_already_delivered": 1 if already_delivered else 0,
        "rights_blocked_count": rights_blocked,
        "outcome_unknown_count": outcome_unknown,
        "requested_status": requested_status,
    }
    if summary_error:
        result["error"] = summary_error
    return result


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish pending Telegram DeliveryOutbox rows without running ingestion.")
    parser.add_argument("--root", default=".", help="Project root containing config.yaml, env files, and data/state.json")
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_REMOTE_DELIVERY_LIMIT,
        help="Maximum due deliveries to process (remote rows are leased one at a time)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    root = Path(args.root).resolve()
    load_env_files(root)
    config = load_config(root / "config.yaml")
    if not telegram_is_configured(config):
        print(
            json.dumps(
                {
                    "mode": "skipped",
                    "telegram_outbox_skipped": 1,
                    "reason": "telegram_not_configured",
                    "telegram_sent": 0,
                    "telegram_failed": 0,
                },
                ensure_ascii=False,
            )
        )
        # This is a dedicated delivery command.  Treat missing/disabled
        # transport configuration as an operational failure instead of a
        # successful no-op so scheduled runs cannot hide unsent work.
        return 1

    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    now = datetime.now(ZoneInfo(timezone_name))
    if remote_api_configured():
        summary = process_remote_delivery_outbox(config, now, limit=max(1, int(args.limit)))
    else:
        state_path = root / "data" / "state.json"
        state = load_state(state_path)
        summary = {
            "mode": "local",
            **process_telegram_delivery_outbox(
                state,
                config,
                now,
                max_items=max(1, int(args.limit)),
            ),
        }
        save_state(state_path, state)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 1 if _int_value(summary.get("telegram_failed")) or _int_value(summary.get("telegram_dead_letter")) else 0


if __name__ == "__main__":
    raise SystemExit(main())
