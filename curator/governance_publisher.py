from __future__ import annotations

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from urllib.parse import quote

from .config import load_config
from .remote_api import post_remote_action, remote_api_configured
from .remote_state import fetch_runtime_resource
from .telegram_sources import load_env_files


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _public_site_base(config: dict[str, object]) -> str:
    explicit = os.environ.get("BSIDE_PUBLIC_SITE_URL", "").strip()
    if explicit:
        return explicit.rstrip("/")
    feed_url = str(config.get("public_feed_url") or "").strip()
    if feed_url.endswith("/feed.xml"):
        return feed_url[: -len("/feed.xml")]
    return feed_url.rsplit("/", 1)[0] if "/" in feed_url else ""


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


def event_delivery(event: dict[str, object], config: dict[str, object], destination: str) -> dict[str, object]:
    event_id = str(event.get("event_id") or "")
    revision = event_revision(event)
    site_base = _public_site_base(config)
    event_url = f"{site_base}/governance/#/events/{quote(event_id, safe='')}" if site_base else ""
    title = escape(str(event.get("title") or "제목 없음"))
    title_line = f'<a href="{escape(event_url, quote=True)}">{title}</a>' if event_url else title
    text = "\n".join(
        (
            "<b>공식 거버넌스 사건 / Official governance event</b>",
            title_line,
            escape(
                f"{event.get('company_id') or '—'} · {event.get('event_type') or 'other'} · "
                f"{event.get('verification_status') or 'confirmed'}"
            ),
        )
    )
    delivery_id = "gov:" + hashlib.sha256(f"{event_id}:{revision}".encode("utf-8")).hexdigest()[:40]
    return {
        "delivery_id": delivery_id,
        "event_id": event_id,
        "channel": "telegram",
        "destination": destination,
        "idempotency_key": f"gov:{revision}",
        "payload": {
            "text": text,
            "disable_web_page_preview": False,
            "event_id": event_id,
            "event_revision": revision,
            "rights_lineage_complete": True,
            "source_right_ids": event_source_right_ids(event),
        },
    }


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
    project_root = root or PROJECT_ROOT
    config = load_config(project_root / "config.yaml")
    destination = os.environ.get("TELEGRAM_CHAT_ID", "").strip()
    if not remote_api_configured() or not destination:
        return {
            "governance_events_scanned": 0,
            "governance_events_publishable": 0,
            "governance_deliveries_enqueued": 0,
            "governance_deliveries_rejected": 0,
            "governance_delivery_enqueue_failed": 1,
        }
    current = now or datetime.now(timezone.utc)
    rows = fetch_runtime_resource(
        "governance_events",
        since=current - timedelta(days=14),
        max_records=5000,
    )
    events = eligible_events(rows)
    deliveries = [event_delivery(event, config, destination) for event in events]
    accepted = rejected = failed = 0
    for index in range(0, len(deliveries), 500):
        batch = deliveries[index : index + 500]
        response = post_remote_action("enqueue_delivery_outbox", {"deliveries": batch})
        if not response.get("ok"):
            failed += 1
            continue
        accepted += int(response.get("accepted") or 0)
        rejected += int(response.get("rejected") or 0)
    if rejected or accepted < len(deliveries):
        failed += 1
    return {
        "governance_events_scanned": len(rows),
        "governance_events_publishable": len(events),
        "governance_deliveries_enqueued": accepted,
        "governance_deliveries_rejected": rejected,
        "governance_delivery_enqueue_failed": failed,
    }


def main() -> None:
    load_env_files(PROJECT_ROOT)
    summary = enqueue_published_governance_events()
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    if int(summary.get("governance_delivery_enqueue_failed") or 0):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
