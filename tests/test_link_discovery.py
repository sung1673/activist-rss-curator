from __future__ import annotations

from curator.fetch import GoogleNewsDecodeResult
from curator.link_discovery import enqueue_link_discoveries, partition_link_discoveries
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


def test_resolver_claims_and_acks_resolution(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_post(action: str, payload: dict[str, object], **_kwargs: object) -> dict[str, object]:
        calls.append((action, payload))
        if action == "claim_link_discoveries":
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
    assert calls[-1][0] == "resolve_link_discovery"
    assert calls[-1][1]["resolved_url"] == "https://example.com/resolved"
