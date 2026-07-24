from __future__ import annotations

from datetime import datetime, timezone

from curator import governance_publisher


def test_governance_events_are_never_enqueued_for_outbound_delivery(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("ACTIVIST_API_URL", "https://api.example.test/api.php")
    monkeypatch.setenv("ACTIVIST_API_SECRET", "secret")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@channel")

    summary = governance_publisher.enqueue_published_governance_events(
        tmp_path / "missing-project",
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )

    assert summary == {
        "governance_events_scanned": 0,
        "governance_events_publishable": 0,
        "governance_deliveries_enqueued": 0,
        "governance_deliveries_rejected": 0,
        "governance_delivery_enqueue_failed": 0,
        "outbound_delivery_disabled": 1,
    }
    source = (governance_publisher.PROJECT_ROOT / "curator" / "governance_publisher.py").read_text(
        encoding="utf-8"
    )
    for forbidden in (
        "fetch_runtime_resource",
        "enqueue_delivery_outbox",
        "post_remote_action",
        "remote_api_configured",
        "event_delivery",
        "send_telegram_message",
    ):
        assert forbidden not in source


def test_event_revision_is_stable_but_changes_for_correction() -> None:
    event = {
        "event_id": "event:one",
        "title": "원 공시",
        "occurred_at": "2026-07-15",
        "publication_status": "published",
        "verification_status": "official",
    }
    assert governance_publisher.event_revision(event) == governance_publisher.event_revision(dict(event))
    assert governance_publisher.event_revision(event) != governance_publisher.event_revision(
        {**event, "title": "[정정] 원 공시", "occurred_at": "2026-07-16"}
    )
    assert governance_publisher.event_revision(event) != governance_publisher.event_revision(
        {**event, "evidence_revision": "document-content-v2"}
    )


def test_governance_delivery_fails_closed_without_publishable_evidence() -> None:
    event = {
        "event_id": "event:orphan",
        "title": "근거 없는 사건",
        "verification_status": "official",
        "review_status": "approved",
        "publication_status": "published",
        "publishable_evidence_count": 0,
    }
    assert governance_publisher.eligible_events([event]) == []
    assert governance_publisher.eligible_events(
        [{**event, "publishable_evidence_count": "not-a-number"}]
    ) == []


def test_market_sensitive_event_requires_explicit_editor_approval() -> None:
    event = {
        "event_id": "event:critical",
        "title": "시장 민감 공시",
        "importance": "critical",
        "verification_status": "official",
        "review_status": "not_required",
        "publication_status": "published",
        "publishable_evidence_count": 1,
    }
    assert governance_publisher.eligible_events([event]) == []
    assert governance_publisher.eligible_events([{**event, "review_status": "approved"}]) == [
        {**event, "review_status": "approved"}
    ]


def test_approved_withdrawal_is_deliverable_as_a_new_evidence_revision() -> None:
    event = {
        "event_id": "event:withdrawn",
        "title": "공개매수 철회",
        "importance": "high",
        "verification_status": "withdrawn",
        "review_status": "approved",
        "publication_status": "published",
        "publishable_evidence_count": 1,
        "evidence_revision": "withdrawal-document-v1",
    }
    assert governance_publisher.eligible_events([event]) == [event]
