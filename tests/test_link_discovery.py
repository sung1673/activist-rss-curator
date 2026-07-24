from __future__ import annotations

import pytest

from curator.fetch import GoogleNewsDecodeResult
from curator.link_discovery import (
    LINK_DISCOVERY_LINEAGE_VERSION,
    enqueue_link_discoveries,
    link_discovery_record,
    partition_link_discoveries,
    resolved_link_articles,
)
from curator.resolve_links import resolve_remote_links


def test_unresolved_google_news_becomes_discovery_not_rejection(config, now) -> None:  # type: ignore[no-untyped-def]
    unresolved = {
        "title": "공시 기사",
        "canonical_url": "https://news.google.com/rss/articles/CBMiABC?oc=5",
        "source_kind": "google_discovery",
        "original_resolution_status": "unresolved",
    }
    direct = {"title": "직접 기사", "canonical_url": "https://example.com/a", "source_kind": "direct"}
    ready, discoveries = partition_link_discoveries([unresolved, direct], now)
    assert ready == [direct]
    assert discoveries[0]["status"] == "discovered"
    assert discoveries[0]["discovered_url"].startswith("https://news.google.com/")


def test_discovery_enqueue_uses_remote_idempotent_queue(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    discoveries = [{"discovery_id": "link:1", "discovered_url": "https://news.google.com/rss/articles/ABC"}]
    monkeypatch.setattr("curator.link_discovery.remote_api_configured", lambda: True)
    monkeypatch.setattr(
        "curator.link_discovery.post_remote_action",
        lambda action, payload: {"ok": action == "enqueue_link_discoveries", "accepted": len(payload["discoveries"])},
    )
    state: dict[str, object] = {}
    summary = enqueue_link_discoveries(discoveries, state, config)
    assert summary == {"link_discoveries": 1, "link_discoveries_enqueued": 1, "link_discoveries_failed": 0}
    assert state["link_discovery_queue"] == discoveries


def test_discovery_record_preserves_media_and_rights_lineage(now) -> None:  # type: ignore[no-untyped-def]
    record = link_discovery_record(
        {
            "title": "원문 제목",
            "summary": "원문 요약",
            "canonical_url": "https://news.google.com/rss/articles/ABC",
            "source": "언론사",
            "feed_name": "google-news-governance",
            "feed_category": "core",
            "source_kind": "authorized_telegram",
            "source_right_id": "telegram:licensed",
            "feed_published_at": "2026-07-24T09:00:00+09:00",
        },
        now,
    )

    assert record["lineage_version"] == LINK_DISCOVERY_LINEAGE_VERSION
    assert record["source_kind"] == "authorized_telegram"
    assert record["source_right_id"] == "telegram:licensed"
    assert record["title"] == "원문 제목"
    assert record["summary"] == "원문 요약"


def test_resolved_discoveries_reenter_media_path_without_losing_lineage(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("curator.link_discovery.remote_api_configured", lambda: True)
    monkeypatch.setattr(
        "curator.link_discovery.fetch_runtime_resource",
        lambda resource, **_kwargs: [
            {
                "discovery_id": "link:media",
                "discovered_url": "https://news.google.com/rss/articles/MEDIA",
                "resolved_url": "https://example.com/original",
                "source": "언론사",
                "title": "주주총회 의안 공개",
                "summary": "원문 요약",
                "feed_name": "google-news-governance",
                "feed_category": "core",
                "source_kind": "google_discovery",
                "source_right_id": None,
                "lineage_version": LINK_DISCOVERY_LINEAGE_VERSION,
                "published_at": "2026-07-24 00:00:00",
                "discovered_at": "2026-07-24 00:05:00",
                "resolved_at": "2026-07-24 00:06:00",
                "status": "resolved",
            },
            {
                "discovery_id": "link:licensed",
                "discovered_url": "https://news.google.com/rss/articles/LICENSED",
                "resolved_url": "https://example.com/licensed",
                "title": "허가 계보 보존",
                "source_kind": "authorized_telegram",
                "source_right_id": "telegram:licensed",
                "lineage_version": LINK_DISCOVERY_LINEAGE_VERSION,
                "discovered_at": "2026-07-24T09:05:00+09:00",
                "status": "resolved",
            },
            {
                "discovery_id": "link:legacy",
                "discovered_url": "https://news.google.com/rss/articles/LEGACY",
                "resolved_url": "https://example.com/legacy",
                "title": "계보 없는 과거 행",
                "lineage_version": 0,
                "status": "resolved",
            },
        ],
    )

    articles = resolved_link_articles(config, now)

    assert [article["canonical_url"] for article in articles] == [
        "https://example.com/original",
        "https://example.com/licensed",
    ]
    assert articles[0]["title"] == "주주총회 의안 공개"
    assert articles[0]["source_right_id"] is None
    assert articles[0]["original_resolution_status"] == "resolved_queue"
    assert articles[0]["article_published_at"] == "2026-07-24T00:00:00+00:00"
    assert articles[0]["seen_at"] == "2026-07-24T00:05:00+00:00"
    assert articles[1]["source_right_id"] == "telegram:licensed"


def test_old_runtime_without_link_export_is_a_safe_rolling_deploy(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("curator.link_discovery.remote_api_configured", lambda: True)
    monkeypatch.setattr(
        "curator.link_discovery.fetch_runtime_resource",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError(
                "runtime state export failed for link_discoveries: "
                "invalid_runtime_resource"
            )
        ),
    )
    assert resolved_link_articles(config, now) == []


def test_runtime_link_export_does_not_hide_database_outages(
    config, now, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr("curator.link_discovery.remote_api_configured", lambda: True)
    monkeypatch.setattr(
        "curator.link_discovery.fetch_runtime_resource",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("runtime state export failed: database_unavailable")
        ),
    )
    with pytest.raises(RuntimeError, match="database_unavailable"):
        resolved_link_articles(config, now)


def test_resolver_claims_and_acks_resolution(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []
    claims = 0

    def fake_post(action: str, payload: dict[str, object], **_kwargs: object) -> dict[str, object]:
        nonlocal claims
        calls.append((action, payload))
        if action == "claim_link_discoveries":
            claims += 1
            if claims > 1:
                return {"ok": True, "lease_token": None, "discoveries": []}
            return {
                "ok": True,
                "lease_token": "lease_123",
                "discoveries": [
                    {
                        "discovery_id": "link:1",
                        "discovered_url": "https://news.google.com/rss/articles/ABC",
                        "attempt_count": 1,
                    }
                ],
            }
        return {"ok": True, "status": "resolved"}

    monkeypatch.setattr("curator.resolve_links.remote_api_configured", lambda: True)
    monkeypatch.setattr("curator.resolve_links.post_remote_action", fake_post)
    monkeypatch.setattr(
        "curator.resolve_links.decode_google_news_url_online_result",
        lambda _url, _client: GoogleNewsDecodeResult(decoded_url="https://example.com/resolved"),
    )
    summary = resolve_remote_links(limit=10)
    assert summary == {"links_claimed": 1, "links_resolved": 1, "links_retry": 0, "links_expired": 0, "links_failed": 0}
    resolutions = [payload for action, payload in calls if action == "resolve_link_discovery"]
    assert resolutions[-1]["resolved_url"] == "https://example.com/resolved"


def test_resolver_drains_multiple_api_batches(monkeypatch) -> None:
    claim_limits: list[int] = []
    next_id = 0

    def fake_post(
        action: str, payload: dict[str, object], **_kwargs: object
    ) -> dict[str, object]:
        nonlocal next_id
        if action == "claim_link_discoveries":
            limit = int(payload["limit"])
            claim_limits.append(limit)
            items = []
            for _ in range(limit):
                next_id += 1
                items.append(
                    {
                        "discovery_id": f"link:{next_id}",
                        "discovered_url": (
                            f"https://news.google.com/rss/articles/{next_id}"
                        ),
                        "attempt_count": 1,
                    }
                )
            return {
                "ok": True,
                "lease_token": f"lease_{len(claim_limits)}",
                "discoveries": items,
            }
        return {"ok": True, "status": "resolved"}

    monkeypatch.setattr("curator.resolve_links.remote_api_configured", lambda: True)
    monkeypatch.setattr("curator.resolve_links.post_remote_action", fake_post)
    monkeypatch.setattr(
        "curator.resolve_links.decode_google_news_url_online_result",
        lambda url, _client: GoogleNewsDecodeResult(
            decoded_url=url.replace(
                "https://news.google.com/rss/articles/", "https://example.com/"
            )
        ),
    )

    summary = resolve_remote_links(limit=205)

    assert claim_limits == ([25] * 8) + [5]
    assert summary == {
        "links_claimed": 205,
        "links_resolved": 205,
        "links_retry": 0,
        "links_expired": 0,
        "links_failed": 0,
    }


def test_discovery_enqueue_requires_exact_remote_ack(
    config, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    discoveries = [
        {
            "discovery_id": "link:1",
            "discovered_url": "https://news.google.com/rss/articles/ONE",
        },
        {
            "discovery_id": "link:2",
            "discovered_url": "https://news.google.com/rss/articles/TWO",
        },
    ]
    monkeypatch.setattr("curator.link_discovery.remote_api_configured", lambda: True)
    monkeypatch.setattr(
        "curator.link_discovery.post_remote_action",
        lambda *_args, **_kwargs: {"ok": True, "accepted": 1, "rejected": 1},
    )

    assert enqueue_link_discoveries(discoveries, {}, config) == {
        "link_discoveries": 2,
        "link_discoveries_enqueued": 1,
        "link_discoveries_failed": 1,
    }
