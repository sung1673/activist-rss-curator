from __future__ import annotations

from datetime import timedelta

from curator.state import compact_state


def test_compact_state_respects_two_month_retention(config, now) -> None:  # type: ignore[no-untyped-def]
    config["state"]["retention_days"] = 60  # type: ignore[index]
    old_seen = (now - timedelta(days=61)).isoformat()
    recent_seen = (now - timedelta(days=10)).isoformat()
    state = {
        "articles": [
            {
                "title": "오래된 기사",
                "canonical_url_hash": "old-url",
                "title_hash": "old-title",
                "seen_at": old_seen,
            },
            {
                "title": "최근 기사",
                "canonical_url_hash": "new-url",
                "title_hash": "new-title",
                "seen_at": recent_seen,
            },
        ],
        "rejected_articles": [{"title": "오래된 제외", "seen_at": old_seen}],
        "published_clusters": [
            {"guid": "old", "published_at": old_seen},
            {"guid": "new", "published_at": recent_seen},
        ],
        "telegram_sent_cluster_guids": ["old", "new"],
        "telegram_send_records": [{"guid": "old", "sent_at": old_seen}],
        "daily_digest_records": [{"digest_id": "old", "sent_at": old_seen}],
        "daily_digest_sent_dates": [(now - timedelta(days=61)).strftime("%Y-%m-%d")],
        "telegram_digest_records": [{"sent_at": old_seen}],
    }

    compact_state(state, config, now)

    assert [article["title"] for article in state["articles"]] == ["최근 기사"]
    assert state["seen_url_hashes"] == ["new-url"]
    assert state["seen_title_hashes"] == ["new-title"]
    assert state["rejected_articles"] == []
    assert [cluster["guid"] for cluster in state["published_clusters"]] == ["new"]
    assert state["telegram_sent_cluster_guids"] == ["new"]
    assert state["telegram_send_records"] == []
    assert state["daily_digest_records"] == []
    assert state["daily_digest_sent_dates"] == []
    assert state["telegram_digest_records"] == []
