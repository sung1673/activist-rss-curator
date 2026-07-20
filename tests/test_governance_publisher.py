from __future__ import annotations

from datetime import datetime, timezone

from curator import governance_publisher


def test_only_confirmed_or_approved_public_events_are_enqueued(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    (tmp_path / "config.yaml").write_text(
        'public_feed_url: "https://news.bside.ai/feed.xml"\n', encoding="utf-8"
    )
    events = [
        {
            "event_id": "event:official",
            "company_id": "00126380",
            "event_type": "treasury_shares",
            "title": "자기주식취득결정",
            "occurred_at": "2026-07-16 09:00:00",
            "verification_status": "official",
            "review_status": "not_required",
            "publication_status": "published",
            "publishable_evidence_count": 1,
            "evidence_revision": "evidence-v1",
            "source_right_ids": ["official:dart"],
        },
        {
            "event_id": "event:pending",
            "title": "공개매수",
            "verification_status": "official",
            "review_status": "pending",
            "publication_status": "draft",
        },
    ]
    posted: list[dict[str, object]] = []
    monkeypatch.setenv("ACTIVIST_API_URL", "https://api.example.test/api.php")
    monkeypatch.setenv("ACTIVIST_API_SECRET", "secret")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@channel")
    monkeypatch.setattr(governance_publisher, "fetch_runtime_resource", lambda *_args, **_kwargs: events)

    def fake_post(action: str, payload: dict[str, object]) -> dict[str, object]:
        assert action == "enqueue_delivery_outbox"
        batch = payload["deliveries"]
        assert isinstance(batch, list)
        posted.extend(batch)
        return {"ok": True, "accepted": len(batch), "rejected": 0}

    monkeypatch.setattr(governance_publisher, "post_remote_action", fake_post)

    summary = governance_publisher.enqueue_published_governance_events(
        tmp_path, now=datetime(2026, 7, 16, tzinfo=timezone.utc)
    )

    assert summary["governance_deliveries_enqueued"] == 1
    assert posted[0]["event_id"] == "event:official"
    payload = posted[0]["payload"]
    assert isinstance(payload, dict)
    assert "자기주식취득결정" in str(payload["text"])
    assert "#/events/event%3Aofficial" in str(payload["text"])
    assert payload["rights_lineage_complete"] is True
    assert payload["source_right_ids"] == ["official:dart"]


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
