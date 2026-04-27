from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from curator.cluster import cluster_articles
from curator.summaries import publish_daily_digest_if_due

from conftest import make_article


def published_cluster(config, now):  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    cluster_articles(
        [
            make_article(
                "신한금융 밸류업 2.0 발표",
                "https://example.com/a",
                summary="신한금융 주주환원 확대",
            ),
            make_article(
                "신한금융 밸류업 주주환원 강화",
                "https://example.com/b",
                summary="배당과 자사주 소각 계획",
            ),
        ],
        state,
        config,
        now,
    )
    cluster_articles([], state, config, now + timedelta(minutes=46))
    return state["published_clusters"][0]


def test_daily_digest_sends_once_in_morning_window(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setattr(summaries, "generate_daily_digest_review", lambda *_args, **_kwargs: "오늘의 리뷰\n- 주주환원 이슈 정리")

    sent_messages = []

    def fake_send(_bot_token, _chat_id, text, _config):  # type: ignore[no-untyped-def]
        sent_messages.append(text)
        return {"ok": True, "message_id": 77, "chat_id": -100}

    monkeypatch.setattr(summaries, "send_telegram_message", fake_send)
    cluster = published_cluster(config, now)
    digest_now = datetime(2026, 4, 26, 7, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    cluster["published_at"] = (digest_now - timedelta(minutes=30)).isoformat()
    state = {
        "published_clusters": [cluster],
        "pending_clusters": [],
        "daily_digest_sent_dates": [],
        "daily_digest_records": [],
    }

    first = publish_daily_digest_if_due(state, config, digest_now)
    second = publish_daily_digest_if_due(state, config, digest_now + timedelta(minutes=15))

    assert first == {"daily_digest_sent": 1, "daily_digest_failed": 0}
    assert second == {"daily_digest_sent": 0, "daily_digest_failed": 0}
    assert state["daily_digest_sent_dates"] == ["2026-04-26"]
    assert state["daily_digest_records"][0]["message_ids"] == [77]
    assert "오늘의 리뷰" in sent_messages[0]


def test_daily_digest_skips_outside_send_hour(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    cluster = published_cluster(config, now)
    digest_now = datetime(2026, 4, 26, 6, 55, tzinfo=ZoneInfo("Asia/Seoul"))
    cluster["published_at"] = (digest_now - timedelta(minutes=30)).isoformat()
    state = {
        "published_clusters": [cluster],
        "pending_clusters": [],
        "daily_digest_sent_dates": [],
        "daily_digest_records": [],
    }

    assert publish_daily_digest_if_due(state, config, digest_now) == {
        "daily_digest_sent": 0,
        "daily_digest_failed": 0,
    }
