from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def event_source_right_ids(event: dict[str, object]) -> list[str]:
    raw_ids = event.get("source_right_ids")
    if not isinstance(raw_ids, list):
        return []
    return sorted(
        {
            str(source_right_id).strip()
            for source_right_id in raw_ids
            if str(source_right_id).strip()
        }
    )


def _int_value(value: object) -> int:
    if isinstance(value, (int, str, bytes, bytearray)):
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0
    return 0


def event_revision(event: dict[str, object]) -> str:
    stable_fields = (
        event.get("event_id"),
        event.get("title"),
        event.get("occurred_at"),
        event.get("deadline_at"),
        event.get("event_type"),
        event.get("publication_status"),
        event.get("verification_status"),
        event.get("evidence_revision"),
        ",".join(event_source_right_ids(event)),
    )
    serialized = "\x1f".join(str(value or "").strip() for value in stable_fields)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:32]


def eligible_events(events: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        event
        for event in events
        if str(event.get("publication_status") or "") == "published"
        and str(event.get("review_status") or "") in {"approved", "not_required"}
        and (
            str(event.get("importance") or "medium") not in {"high", "critical"}
            or str(event.get("review_status") or "") == "approved"
        )
        and str(event.get("verification_status") or "")
        in {"official", "confirmed", "corroborated", "withdrawn"}
        and _int_value(event.get("publishable_evidence_count")) > 0
        and event.get("event_id")
    ]


def enqueue_published_governance_events(
    root: Path | None = None,
    *,
    now: datetime | None = None,
) -> dict[str, int]:
    """Keep the historical publisher as a permanent web-only policy no-op."""

    return {
        "governance_events_scanned": 0,
        "governance_events_publishable": 0,
        "governance_deliveries_enqueued": 0,
        "governance_deliveries_rejected": 0,
        "governance_delivery_enqueue_failed": 0,
        "outbound_delivery_disabled": 1,
    }


def main() -> None:
    summary = enqueue_published_governance_events()
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    if int(summary.get("governance_delivery_enqueue_failed") or 0):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
