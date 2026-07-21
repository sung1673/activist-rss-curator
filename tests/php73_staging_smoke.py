#!/usr/bin/env python3
"""End-to-end PHP 7.3/MySQL smoke for the Telegram staging protocol."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any


API_SECRET = b"php73-ci-only-hmac-key-00000000000000000000000000000000"
CHANNEL_ID = -1001234567890
MESSAGE_KEY = f"id:{CHANNEL_ID}:42"


class SmokeFailure(RuntimeError):
    """Raised when the live HTTP or database contract differs from expectations."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SmokeFailure(message)


class Client:
    def __init__(self, base_url: str, inspection_token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.inspection_token = inspection_token
        self.counter = 0

    def wait_until_ready(self) -> None:
        last_error = "server did not answer"
        for _ in range(60):
            try:
                with urllib.request.urlopen(
                    f"{self.base_url}/api.php?action=health", timeout=2
                ) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                    if response.status == 200 and payload.get("ok") is True:
                        return
                    last_error = f"status={response.status} payload={payload!r}"
            except (OSError, ValueError, urllib.error.URLError) as exc:
                last_error = repr(exc)
            time.sleep(0.5)
        raise SmokeFailure(f"PHP server was not ready: {last_error}")

    def post(
        self, action: str, payload: dict[str, Any], expected_status: int = 200
    ) -> dict[str, Any]:
        body = json.dumps(
            payload, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        timestamp = str(int(time.time()))
        self.counter += 1
        nonce = f"php73-ci-{self.counter:06d}-{timestamp}"
        signature = hmac.new(
            API_SECRET,
            timestamp.encode("ascii")
            + b"\n"
            + nonce.encode("ascii")
            + b"\n"
            + body,
            hashlib.sha256,
        ).hexdigest()
        request = urllib.request.Request(
            f"{self.base_url}/api.php?{urllib.parse.urlencode({'action': action})}",
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "X-Activist-Timestamp": timestamp,
                "X-Activist-Nonce": nonce,
                "X-Activist-Signature": f"sha256={signature}",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                status = response.status
                raw = response.read()
        except urllib.error.HTTPError as exc:
            status = exc.code
            raw = exc.read()
        try:
            decoded = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SmokeFailure(
                f"{action} returned non-JSON HTTP {status}: {raw[:500]!r}"
            ) from exc
        require(
            status == expected_status,
            f"{action} expected HTTP {expected_status}, got {status}: {decoded!r}",
        )
        require(
            isinstance(decoded, dict),
            f"{action} returned a non-object: {decoded!r}",
        )
        return decoded

    def state(self) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{self.base_url}/__test/state",
            method="GET",
            headers={"X-CI-Inspection-Token": self.inspection_token},
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            raw = response.read()
            require(
                response.status == 200,
                f"state inspection returned HTTP {response.status}",
            )
        decoded = json.loads(raw.decode("utf-8"))
        require(decoded.get("ok") is True, f"state inspection failed: {decoded!r}")
        return decoded

    def pin_active_lease(self, rebuild_token: str) -> None:
        request = urllib.request.Request(
            f"{self.base_url}/__test/pin-lease",
            data=b"",
            method="POST",
            headers={
                "X-CI-Inspection-Token": self.inspection_token,
                "X-CI-Rebuild-Token": rebuild_token,
            },
        )
        with urllib.request.urlopen(request, timeout=15) as response:
            decoded = json.loads(response.read().decode("utf-8"))
            require(response.status == 200, f"lease pin returned HTTP {response.status}")
        require(decoded.get("lease_pinned_seconds") == 30, repr(decoded))


def signal(article_id: str, count: int) -> dict[str, Any]:
    return {
        "article_id": article_id,
        "related_telegram_count": count,
        "related_telegram_channels_count": 1,
        "first_seen_at": "2026-07-20T00:00:00Z",
        "latest_seen_at": "2026-07-21T00:00:00Z",
        "confidence_score": 0.75,
        "signal_title": article_id,
        "source_right_ids": ["telegram:ci_channel"],
    }


def signal_ids(state: dict[str, Any], key: str) -> list[str]:
    return [str(row["article_id"]) for row in state[key]]


def run(base_url: str, inspection_token: str) -> None:
    client = Client(base_url, inspection_token)
    client.wait_until_ready()

    capabilities = client.post("telegram_snapshot_capabilities", {})
    require(
        capabilities.get("signal_rebuild_protocol") == "staging-v1",
        repr(capabilities),
    )
    require(
        isinstance(capabilities.get("live_revision"), int)
        and not isinstance(capabilities.get("live_revision"), bool),
        f"capabilities omitted integer live_revision: {capabilities!r}",
    )
    base_revision = capabilities["live_revision"]
    require(
        base_revision == 0,
        f"fresh CI database revision must be zero: {capabilities!r}",
    )
    print("capabilities: staging-v1, live_revision=0", flush=True)

    channel_write = client.post(
        "upsert_telegram_snapshot",
        {
            "channels": [
                {
                    "handle": "ci_channel",
                    "telegram_channel_id": CHANNEL_ID,
                    "title": "CI channel",
                    "last_message_id": 42,
                }
            ]
        },
    )
    live_revision = base_revision + 1
    require(channel_write.get("channels") == 1, repr(channel_write))
    require(channel_write.get("messages") == 0, repr(channel_write))
    require(channel_write.get("article_matches") == 0, repr(channel_write))
    require(channel_write.get("issue_signals") == 0, repr(channel_write))
    require(channel_write.get("live_revision") == live_revision, repr(channel_write))
    state = client.state()
    require(
        state["channels"]
        == [{"handle": "ci_channel", "telegram_channel_id": str(CHANNEL_ID)}],
        repr(state),
    )
    print("channels-only write: live_revision advanced", flush=True)

    seed_payload = {
        "messages": [
            {
                "handle": "ci_channel",
                "telegram_channel_id": CHANNEL_ID,
                "telegram_message_id": 42,
                "posted_at": "2026-07-21T00:00:00Z",
                "collected_at": "2026-07-21T00:01:00Z",
                "text": "CI governance signal",
                "normalized_text": "ci governance signal",
                "message_url": "https://t.me/ci_channel/42",
                "urls": ["https://example.test/disclosure/42"],
            }
        ],
        "article_matches": [
            {
                "article_id": "ci-message-article",
                "telegram_message_key": MESSAGE_KEY,
                "match_type": "exact_url",
                "score": 1.0,
                "channel_handle": "ci_channel",
                "telegram_message_id": 42,
                "message_url": "https://t.me/ci_channel/42",
            }
        ],
        "issue_signals": [signal("ci-old-signal", 2)],
    }
    seeded = client.post("upsert_telegram_snapshot", seed_payload)
    expected_ack = {
        "channels": 0,
        "messages": 1,
        "article_matches": 1,
        "issue_signals": 1,
        "issue_signals_deleted": 0,
    }
    for field, value in expected_ack.items():
        require(seeded.get(field) == value, f"inexact {field} ACK: {seeded!r}")
    live_revision += 1
    require(
        seeded.get("live_revision") == live_revision,
        f"revision did not advance: {seeded!r}",
    )

    state = client.state()
    require(len(state["messages"]) == 1, f"message was not committed: {state!r}")
    require(
        state["messages"][0]["message_key"] == MESSAGE_KEY,
        f"wrong canonical message key: {state!r}",
    )
    require(
        state["messages"][0]["telegram_channel_id"] == str(CHANNEL_ID),
        f"integer channel ID was not preserved: {state!r}",
    )
    require(
        state["matches"]
        == [
            {
                "article_id": "ci-message-article",
                "message_key": MESSAGE_KEY,
                "match_type": "exact_url",
            }
        ],
        repr(state),
    )
    require(signal_ids(state, "signals") == ["ci-old-signal"], repr(state))
    print("normal write: integer channel ID and exact ACK verified", flush=True)

    invalid_payload = {
        "messages": [
            {
                "handle": "ci_channel",
                "telegram_channel_id": CHANNEL_ID,
                "telegram_message_id": 43,
                "text": "must roll back",
            },
            {
                "handle": "ci_channel",
                "telegram_channel_id": CHANNEL_ID,
                "telegram_message_id": 0,
                "text": "invalid identity",
            },
        ]
    }
    invalid = client.post(
        "upsert_telegram_snapshot", invalid_payload, expected_status=400
    )
    require(
        invalid.get("error") == "invalid_telegram_message_identity", repr(invalid)
    )
    state = client.state()
    require(
        len(state["messages"]) == 1,
        f"invalid batch partially committed: {state!r}",
    )
    require(
        int(state["state"]["live_revision"]) == live_revision,
        f"invalid batch changed revision: {state!r}",
    )
    print("invalid batch: failed closed without a partial write", flush=True)

    conflict_token = "c" * 64
    conflict = client.post(
        "upsert_telegram_snapshot",
        {
            "signal_rebuild_token": conflict_token,
            "signal_rebuild_begin": True,
            "signal_rebuild_base_revision": live_revision + 1,
            "issue_signals": [signal("ci-conflict-signal", 3)],
        },
        expected_status=409,
    )
    require(
        conflict.get("error") == "signal_rebuild_revision_conflict", repr(conflict)
    )
    require(conflict.get("live_revision") == live_revision, repr(conflict))
    state = client.state()
    require(
        state["state"]["active_token"] is None and state["staging"] == [],
        repr(state),
    )
    print("rebuild begin: stale live_revision rejected", flush=True)

    rebuild_token = "a" * 64
    begun = client.post(
        "upsert_telegram_snapshot",
        {
            "signal_rebuild_token": rebuild_token,
            "signal_rebuild_begin": True,
            "signal_rebuild_base_revision": live_revision,
            "issue_signals": [signal("ci-new-signal", 7)],
        },
    )
    require(
        begun.get("issue_signals") == 0,
        f"staged signal leaked into live ACK: {begun!r}",
    )
    require(
        begun.get("issue_signals_staged") == 1,
        f"wrong staging ACK: {begun!r}",
    )
    require(begun.get("signal_rebuild_token") == rebuild_token, repr(begun))
    require(begun.get("live_revision") == live_revision, repr(begun))
    state = client.state()
    require(
        signal_ids(state, "signals") == ["ci-old-signal"],
        f"staging mutated live signals: {state!r}",
    )
    require(
        signal_ids(state, "staging") == ["ci-new-signal"],
        f"staging row missing: {state!r}",
    )
    require(state["state"]["active_token"] == rebuild_token, repr(state))
    print("staging: new signal invisible before finalize", flush=True)

    retried = client.post(
        "upsert_telegram_snapshot",
        {
            "signal_rebuild_token": rebuild_token,
            "signal_rebuild_begin": True,
            "signal_rebuild_base_revision": live_revision,
            "issue_signals": [],
        },
    )
    require(retried.get("issue_signals_staged") == 0, repr(retried))
    require(retried.get("live_revision") == live_revision, repr(retried))
    state = client.state()
    require(state["state"]["active_token"] == rebuild_token, repr(state))
    require(int(state["state"]["live_revision"]) == live_revision, repr(state))
    require(
        signal_ids(state, "staging") == ["ci-new-signal"],
        f"same-token begin retry cleared staging: {state!r}",
    )

    invalid_stage = client.post(
        "upsert_telegram_snapshot",
        {
            "signal_rebuild_token": rebuild_token,
            "channels": [{"handle": "ci_forbidden_channel"}],
            "messages": [
                {
                    "handle": "ci_channel",
                    "telegram_channel_id": CHANNEL_ID,
                    "telegram_message_id": 46,
                    "text": "must not be staged",
                }
            ],
            "article_matches": [
                {
                    "article_id": "ci-forbidden-match",
                    "telegram_message_key": f"id:{CHANNEL_ID}:46",
                    "match_type": "exact_url",
                }
            ],
            "issue_signals": [],
        },
        expected_status=400,
    )
    require(
        invalid_stage.get("error") == "signal_rebuild_stage_requires_signals_only",
        repr(invalid_stage),
    )
    state = client.state()
    require(len(state["channels"]) == 1, f"invalid stage wrote a channel: {state!r}")
    require(len(state["messages"]) == 1, f"invalid stage wrote a message: {state!r}")
    require(len(state["matches"]) == 1, f"invalid stage wrote a match: {state!r}")
    require(state["state"]["active_token"] == rebuild_token, repr(state))
    require(
        signal_ids(state, "staging") == ["ci-new-signal"],
        f"invalid stage changed staging: {state!r}",
    )
    client.pin_active_lease(rebuild_token)

    competing = client.post(
        "upsert_telegram_snapshot",
        {
            "signal_rebuild_token": "b" * 64,
            "signal_rebuild_begin": True,
            "signal_rebuild_base_revision": live_revision,
            "issue_signals": [signal("ci-competing-signal", 4)],
        },
        expected_status=409,
    )
    require(competing.get("error") == "signal_rebuild_in_progress", repr(competing))
    require(competing.get("live_revision") == live_revision, repr(competing))
    state = client.state()
    require(state["state"]["active_token"] == rebuild_token, repr(state))
    require(int(state["state"]["live_revision"]) == live_revision, repr(state))
    require(
        signal_ids(state, "staging") == ["ci-new-signal"],
        f"competing begin replaced active staging: {state!r}",
    )
    print("rebuild concurrency: retry is idempotent and competing begin is fenced", flush=True)

    blocked = client.post(
        "upsert_telegram_snapshot",
        {
            "messages": [
                {
                    "handle": "ci_channel",
                    "telegram_channel_id": CHANNEL_ID,
                    "telegram_message_id": 44,
                    "text": "must be fenced",
                }
            ]
        },
        expected_status=409,
    )
    require(blocked.get("error") == "signal_rebuild_in_progress", repr(blocked))
    require(blocked.get("live_revision") == live_revision, repr(blocked))
    state = client.state()
    require(
        len(state["messages"]) == 1,
        f"normal write bypassed rebuild fence: {state!r}",
    )
    require(int(state["state"]["live_revision"]) == live_revision, repr(state))

    stale = client.post(
        "upsert_telegram_snapshot",
        {
            "signal_rebuild_token": "b" * 64,
            "issue_signals": [signal("ci-stale-token-signal", 5)],
        },
        expected_status=409,
    )
    require(stale.get("error") == "stale_signal_rebuild_token", repr(stale))
    state = client.state()
    require(
        signal_ids(state, "staging") == ["ci-new-signal"],
        f"stale token changed staging: {state!r}",
    )
    require(int(state["state"]["live_revision"]) == live_revision, repr(state))
    print("rebuild fencing: normal writes and stale tokens rejected", flush=True)

    finalize_payload = {
        "signal_rebuild_token": rebuild_token,
        "signal_rebuild_finalize": True,
        "replace_issue_signals": True,
        "issue_signals_replace_since": "2026-01-01T00:00:00Z",
        "issue_signals": [],
    }
    finalized = client.post("upsert_telegram_snapshot", finalize_payload)
    require(
        finalized.get("signal_rebuild_finalized") == rebuild_token, repr(finalized)
    )
    require(
        finalized.get("issue_signals") == 1,
        f"staged signal was not promoted: {finalized!r}",
    )
    require(
        finalized.get("issue_signals_deleted") == 1,
        f"old signal was not deleted: {finalized!r}",
    )
    final_revision = live_revision + 1
    require(finalized.get("live_revision") == final_revision, repr(finalized))
    state = client.state()
    require(
        signal_ids(state, "signals") == ["ci-new-signal"],
        f"final live set is not atomic: {state!r}",
    )
    require(state["staging"] == [], f"finalized staging rows remain: {state!r}")
    require(state["state"]["active_token"] is None, repr(state))
    require(state["state"]["finalized_token"] == rebuild_token, repr(state))
    require(int(state["state"]["live_revision"]) == final_revision, repr(state))

    idempotent_finalize = client.post(
        "upsert_telegram_snapshot", finalize_payload
    )
    require(idempotent_finalize.get("signal_rebuild_idempotent") is True, repr(idempotent_finalize))
    require(idempotent_finalize.get("signal_rebuild_finalized") == rebuild_token, repr(idempotent_finalize))
    require(idempotent_finalize.get("live_revision") == final_revision, repr(idempotent_finalize))
    require(idempotent_finalize.get("issue_signals") == 0, repr(idempotent_finalize))
    require(idempotent_finalize.get("issue_signals_deleted") == 0, repr(idempotent_finalize))
    state = client.state()
    require(signal_ids(state, "signals") == ["ci-new-signal"], repr(state))
    require(state["staging"] == [], repr(state))
    require(state["state"]["active_token"] is None, repr(state))
    require(int(state["state"]["live_revision"]) == final_revision, repr(state))
    live_revision = final_revision
    print("finalize retry: idempotent ACK left live state unchanged", flush=True)

    final_capabilities = client.post("telegram_snapshot_capabilities", {})
    require(
        final_capabilities.get("live_revision") == live_revision,
        repr(final_capabilities),
    )
    print(
        "finalize: atomic promotion/deletion and live_revision advance verified",
        flush=True,
    )

    lease_token = "d" * 64
    leased = client.post(
        "upsert_telegram_snapshot",
        {
            "signal_rebuild_token": lease_token,
            "signal_rebuild_begin": True,
            "signal_rebuild_base_revision": live_revision,
            "issue_signals": [signal("ci-expired-staging-signal", 9)],
        },
    )
    require(leased.get("issue_signals_staged") == 1, repr(leased))
    require(leased.get("live_revision") == live_revision, repr(leased))
    state = client.state()
    require(state["state"]["active_token"] == lease_token, repr(state))
    require(
        signal_ids(state, "staging") == ["ci-expired-staging-signal"], repr(state)
    )

    time.sleep(2.2)
    recovered = client.post(
        "upsert_telegram_snapshot",
        {
            "messages": [
                {
                    "handle": "ci_channel",
                    "telegram_channel_id": CHANNEL_ID,
                    "telegram_message_id": 45,
                    "text": "live write after expired rebuild lease",
                }
            ]
        },
    )
    live_revision += 1
    require(recovered.get("messages") == 1, repr(recovered))
    require(recovered.get("live_revision") == live_revision, repr(recovered))
    state = client.state()
    require(state["state"]["active_token"] is None, repr(state))
    require(int(state["state"]["live_revision"]) == live_revision, repr(state))
    require(state["staging"] == [], f"expired staging was not cleaned: {state!r}")
    require(
        signal_ids(state, "signals") == ["ci-new-signal"],
        f"expired staging leaked into live signals: {state!r}",
    )
    require(
        sorted(row["message_key"] for row in state["messages"])
        == [MESSAGE_KEY, f"id:{CHANNEL_ID}:45"],
        f"recovery live write did not commit: {state!r}",
    )
    capabilities_after_recovery = client.post("telegram_snapshot_capabilities", {})
    require(capabilities_after_recovery.get("live_revision") == live_revision, repr(capabilities_after_recovery))
    print("expired rebuild lease: stale staging cleared and normal live write resumed", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8787")
    args = parser.parse_args()
    inspection_token = os.environ.get("PHP73_CI_INSPECTION_TOKEN", "")
    require(bool(inspection_token), "PHP73_CI_INSPECTION_TOKEN is required")
    run(args.base_url, inspection_token)
    print("PHP 7.3/MySQL Telegram staging smoke passed.", flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"FAILED: {exc}", file=sys.stderr, flush=True)
        raise
