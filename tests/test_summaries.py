from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from curator.cluster import cluster_articles
from curator.summaries import build_daily_digest_messages, publish_daily_digest_if_due

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


def test_daily_digest_lists_domestic_and_global_article_links(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setattr(
        summaries,
        "generate_daily_digest_review",
        lambda *_args, **_kwargs: "- 국내 주주환원 흐름이 이어졌음\n- 해외 행동주의 이슈도 보였음",
    )
    config["digest"]["link_title_max_chars"] = 22  # type: ignore[index]
    domestic_article = make_article(
        "주주제안과 자사주 소각 의무화 논의가 매우 길게 이어지는 기사 제목",
        "https://example.com/domestic",
        source="국내뉴스",
        published_at="2026-04-27T09:00:00+09:00",
        summary="주주제안 자사주 소각",
    )
    domestic_article["feed_category"] = "supplemental"
    global_article = make_article(
        "Shareholder activism proxy fight expands across global boards",
        "https://example.com/global",
        source="Global News",
        published_at="2026-04-26T21:00:00+09:00",
        summary="shareholder activism proxy fight",
    )
    global_article["feed_category"] = "global"
    clusters = [
        {
            "representative_title": "국내 주주환원",
            "published_at": "2026-04-27T09:10:00+09:00",
            "theme_group": "valueup_return",
            "articles": [domestic_article],
        },
        {
            "representative_title": "해외 행동주의",
            "published_at": "2026-04-26T21:10:00+09:00",
            "theme_group": "activism_trend",
            "articles": [global_article],
        },
    ]

    message = build_daily_digest_messages(clusters, config, now, now - timedelta(hours=24))[0]

    assert "<b>국내</b>" in message
    assert "<b>해외</b>" in message
    assert 'href="https://example.com/domestic"' in message
    assert 'href="https://example.com/global"' in message
    assert "04.27 /" in message
    assert "04.26 /" in message
    assert "매우 길게 이어지는 기사 제목" not in message
    assert "..." in message


def test_daily_digest_groups_similar_article_titles(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import summaries

    monkeypatch.setattr(summaries, "generate_daily_digest_review", lambda *_args, **_kwargs: "- 소액주주 이슈가 이어졌음")
    first = make_article(
        "고려아연 소액주주, 사외이사 검찰 고발",
        "https://example.com/a",
        source="데일리안",
        published_at="2026-04-27T09:00:00+09:00",
        relevance_level="high",
    )
    first["company_candidates"] = ["고려아연"]
    second = make_article(
        "고려아연 소액주주, 검찰 고발·금융위 진정 동시 제기",
        "https://example.com/b",
        source="뉴스워치",
        published_at="2026-04-27T09:10:00+09:00",
        relevance_level="high",
    )
    second["company_candidates"] = ["고려아연"]
    clusters = [
        {"representative_title": "고려아연 소액주주", "published_at": first["published_at"], "articles": [first]},
        {"representative_title": "고려아연 소액주주", "published_at": second["published_at"], "articles": [second]},
    ]

    message = build_daily_digest_messages(clusters, config, now, now - timedelta(hours=24))[0]

    assert "04.27 /" in message
    assert "(2건)" in message
    assert "링크:" in message
    assert 'href="https://example.com/a"' in message
    assert 'href="https://example.com/b"' in message
