from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path

from .config import load_config
from .dates import now_in_timezone
from .remote_state import hydrate_runtime_state, preflight_telegram_signal_runtime
from .state import load_state, save_state
from .telegram_sources import (
    backfill_telegram_messages,
    parse_handle_list,
    pending_remote_messages,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FAILURE_KEYS = (
    "telegram_channel_failed",
    "telegram_remote_failed",
    "telegram_backfill_truncated_channels",
)
MAX_REPAIR_DAYS = 365
MAX_REPAIR_MESSAGES_PER_CHANNEL = 3000
MAX_REPAIR_CHANNELS = 500
MAX_REPAIR_MESSAGES = 300_000
MAX_TELEGRAM_MESSAGE_ID = 9_223_372_036_854_775_807
SELECTION_FINGERPRINT_PATTERN = re.compile(r"[0-9a-f]{64}")


def validate_repair_request(
    *,
    days: int,
    limit_per_channel: int,
    channel_limit: int,
    max_messages: int,
    only_handles: set[str] | None,
    start_after_handle: str,
    before_message_id: int = 0,
    expected_selection_fingerprint: str = "",
    finalize_signal_rebuild: bool = False,
) -> None:
    bounds = {
        "days": (days, 1, MAX_REPAIR_DAYS),
        "limit_per_channel": (limit_per_channel, 1, MAX_REPAIR_MESSAGES_PER_CHANNEL),
        "channel_limit": (channel_limit, 0, MAX_REPAIR_CHANNELS),
        "max_messages": (max_messages, 1, MAX_REPAIR_MESSAGES),
    }
    for name, (value, minimum, maximum) in bounds.items():
        if not minimum <= value <= maximum:
            raise ValueError(f"{name}_out_of_bounds")

    handles = set(only_handles or set())
    if len(handles) > MAX_REPAIR_CHANNELS:
        raise ValueError("only_handles_out_of_bounds")
    handle_pattern = r"[A-Za-z0-9_]{1,64}"
    if any(not re.fullmatch(handle_pattern, handle) for handle in handles):
        raise ValueError("only_handles_invalid")
    if start_after_handle and not re.fullmatch(handle_pattern, start_after_handle):
        raise ValueError("start_after_handle_invalid")
    if not 0 <= before_message_id <= MAX_TELEGRAM_MESSAGE_ID:
        raise ValueError("before_message_id_out_of_bounds")
    if before_message_id and (len(handles) != 1 or start_after_handle):
        raise ValueError("before_message_id_requires_one_handle")
    if finalize_signal_rebuild and not start_after_handle:
        raise ValueError("finalize_signal_rebuild_requires_last_handle")
    if finalize_signal_rebuild and (
        handles or channel_limit > 0 or before_message_id > 0
    ):
        raise ValueError("finalize_signal_rebuild_requires_global_scope")
    expected_fingerprint = str(expected_selection_fingerprint or "").strip()
    if expected_fingerprint and not SELECTION_FINGERPRINT_PATTERN.fullmatch(
        expected_fingerprint
    ):
        raise ValueError("expected_selection_fingerprint_invalid")
    is_resume = bool(start_after_handle) or bool(before_message_id)
    if is_resume and not expected_fingerprint:
        raise ValueError("expected_selection_fingerprint_required")


def run_repair(
    root: Path,
    *,
    days: int,
    limit_per_channel: int,
    channel_limit: int = 0,
    max_messages: int = MAX_REPAIR_MESSAGES,
    only_handles: set[str] | None = None,
    start_after_handle: str = "",
    before_message_id: int = 0,
    expected_selection_fingerprint: str = "",
    finalize_signal_rebuild: bool = False,
) -> dict[str, object]:
    validate_repair_request(
        days=days,
        limit_per_channel=limit_per_channel,
        channel_limit=channel_limit,
        max_messages=max_messages,
        only_handles=only_handles,
        start_after_handle=start_after_handle,
        before_message_id=before_message_id,
        expected_selection_fingerprint=expected_selection_fingerprint,
        finalize_signal_rebuild=finalize_signal_rebuild,
    )
    config = load_config(root / "config.yaml")
    now = now_in_timezone(str(config.get("timezone") or "Asia/Seoul"))
    state_path = root / "data" / "state.json"
    state = load_state(state_path)
    hydration = hydrate_runtime_state(state, config, now)
    runtime_preflight = preflight_telegram_signal_runtime(config, now)
    checkpoint_count = 0

    def checkpoint(progress_record: dict[str, object]) -> None:
        nonlocal checkpoint_count
        checkpoint_count += 1
        save_state(state_path, state)
        checkpoint_metrics: dict[str, object] = {
            "status": "running",
            **hydration,
            "telegram_repair_checkpoints": checkpoint_count,
            "telegram_remote_pending": progress_record.get(
                "telegram_remote_pending",
                len(pending_remote_messages(state)),
            ),
            "telegram_remote_failed": progress_record.get(
                "telegram_remote_failed",
                0,
            ),
            "telegram_repair_last_handle": progress_record.get("handle") or "",
            "telegram_repair_last_status": progress_record.get("status") or "",
            "telegram_repair_remote_checkpoint_complete": progress_record.get(
                "remote_checkpoint_complete",
                0,
            ),
            "telegram_repair_resume_before_message_id": progress_record.get(
                "resume_before_message_id",
                0,
            ),
            "telegram_backfill_selection_fingerprint": progress_record.get(
                "telegram_backfill_selection_fingerprint",
                "",
            ),
            "telegram_backfill_selection_count": progress_record.get(
                "telegram_backfill_selection_count",
                0,
            ),
            "telegram_backfill_universe_count": progress_record.get(
                "telegram_backfill_universe_count",
                0,
            ),
            "telegram_backfill_selected_count": progress_record.get(
                "telegram_backfill_selected_count",
                0,
            ),
            "telegram_backfill_started_selection_fingerprint": (
                progress_record.get(
                    "telegram_backfill_started_selection_fingerprint",
                    "",
                )
            ),
            "telegram_backfill_started_universe_count": progress_record.get(
                "telegram_backfill_started_universe_count",
                0,
            ),
            "telegram_backfill_started_selected_count": progress_record.get(
                "telegram_backfill_started_selected_count",
                0,
            ),
        }
        for key in (
            "telegram_remote_last_error",
            "telegram_remote_last_status_code",
            "telegram_remote_max_request_bytes",
        ):
            if progress_record.get(key) not in (None, "", 0):
                checkpoint_metrics[key] = progress_record[key]
        write_metrics(checkpoint_metrics, ok=False)

    try:
        repair = backfill_telegram_messages(
            state,
            config,
            now,
            days=days,
            limit_per_channel=limit_per_channel,
            channel_limit=channel_limit,
            sync_remote=True,
            progress=True,
            only_handles=only_handles,
            start_after_handle=start_after_handle,
            before_message_id=before_message_id,
            expected_selection_fingerprint=expected_selection_fingerprint,
            max_messages=max_messages,
            checkpoint_callback=checkpoint,
            selection_checkpoint_callback=checkpoint,
            force_remote_resync=True,
            rebuild_remote_signals=True,
            finalize_signal_rebuild=finalize_signal_rebuild,
            require_unique_universe_handles=True,
        )
    finally:
        # A normal Python exception preserves the last channel locally. Hard job
        # termination can lose at most the bounded in-flight channel because each
        # completed channel is durably synced and checkpointed above.
        save_state(state_path, state)
    return {
        **hydration,
        **runtime_preflight,
        **repair,
        "telegram_repair_checkpoints": checkpoint_count,
    }


def write_metrics(summary: dict[str, object], *, ok: bool) -> None:
    destination = os.environ.get("CURATOR_RUN_METRICS_PATH", "").strip()
    if not destination:
        return
    path = Path(destination)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing: dict[str, object] = {}
    if path.exists():
        try:
            loaded = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            loaded = None
        if isinstance(loaded, dict):
            existing = loaded
    path.write_text(
        json.dumps(
            {**existing, "ok": ok, **summary},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
            default=str,
        )
        + "\n",
        encoding="utf-8",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Repair Telegram history into the MySQL source of truth."
    )
    parser.add_argument("--root", default=".")
    parser.add_argument("--days", type=int, default=365)
    parser.add_argument("--limit-per-channel", type=int, default=3000)
    parser.add_argument("--channel-limit", type=int, default=0)
    parser.add_argument("--max-messages", type=int, default=MAX_REPAIR_MESSAGES)
    parser.add_argument("--only-handles", default="")
    parser.add_argument("--start-after", default="")
    parser.add_argument("--before-message-id", type=int, default=0)
    parser.add_argument("--expected-selection-fingerprint", default="")
    parser.add_argument("--finalize-signal-rebuild", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    try:
        summary = run_repair(
            Path(args.root).resolve(),
            days=args.days,
            limit_per_channel=args.limit_per_channel,
            channel_limit=args.channel_limit,
            max_messages=args.max_messages,
            only_handles=parse_handle_list(args.only_handles),
            start_after_handle=args.start_after,
            before_message_id=args.before_message_id,
            expected_selection_fingerprint=args.expected_selection_fingerprint,
            finalize_signal_rebuild=args.finalize_signal_rebuild,
        )
    except Exception as exc:
        write_metrics(
            {"status": "failed", "error_type": exc.__class__.__name__}, ok=False
        )
        raise
    failed = any(int(summary.get(key) or 0) > 0 for key in FAILURE_KEYS)
    write_metrics(
        {"status": "failed" if failed else "complete", **summary}, ok=not failed
    )
    print(
        json.dumps(summary, ensure_ascii=False, sort_keys=True, default=str), flush=True
    )
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
