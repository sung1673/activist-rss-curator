from __future__ import annotations

from copy import deepcopy
from datetime import timedelta

from curator.cluster import cluster_articles
from curator.telegram_publisher import (
    build_telegram_message,
    initialize_telegram_state,
    publish_unsent_telegram_clusters,
    unsent_telegram_clusters,
)

from conftest import make_article


def published_cluster(config, now):  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    articles = [
        make_article(
            "고려아연 소액주주, 사외이사 검찰 고발",
            "https://example.com/a",
            summary="고려아연 소액주주연대",
            relevance_level="high",
        ),
        make_article(
            "고려아연 소액주주, 금융위 진정",
            "https://example.com/b",
            summary="고려아연 소액주주연대",
            relevance_level="high",
        ),
    ]
    cluster_articles(articles, state, config, now)
    cluster_articles([], state, config, now + timedelta(minutes=21))
    return state["published_clusters"][0]


def single_article_cluster(config, now):  # type: ignore[no-untyped-def]
    state = {"pending_clusters": [], "published_clusters": []}
    article = make_article(
        "금융당국, 상장회사 임원보수 공시 강화",
        "https://example.com/single",
        summary="성과보수와 주식보상 공시가 투자자 보호 쟁점으로 부각됐다",
        relevance_level="high",
    )
    cluster_articles([article], state, config, now)
    cluster_articles([], state, config, now + timedelta(minutes=21))
    return state["published_clusters"][0]


def test_telegram_message_uses_html_links_without_visible_raw_urls(config, now) -> None:  # type: ignore[no-untyped-def]
    cluster = published_cluster(config, now)
    message = build_telegram_message(cluster, config)

    assert "<a href=" in message
    assert "대표 기사 보기" not in message
    assert "분류:" not in message
    assert "기준시각:" not in message
    assert "[ 지배구조·주주권 ]" not in message
    assert "<b>고려아연</b>" in message
    assert "1. " in message
    assert ">https://example.com/a<" not in message
    assert "\nhttps://example.com/a" not in message
    assert len(message) <= config["telegram"]["max_message_chars"]


def test_single_article_message_omits_duplicate_heading_and_adds_preview(config, now) -> None:  # type: ignore[no-untyped-def]
    cluster = single_article_cluster(config, now)
    message = build_telegram_message(cluster, config)

    assert not message.startswith("<b>")
    assert "<a href=" in message
    assert "금융당국, 상장회사 임원보수 공시 강화</a>" in message
    assert "본문: 성과보수와 주식보상 공시가 투자자 보호 쟁점으로..." in message


def test_telegram_initialization_does_not_backfill_old_clusters(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@test_channel")
    old_cluster = published_cluster(config, now)
    new_cluster = deepcopy(old_cluster)
    new_cluster["guid"] = "cluster:new:20260425:1"

    state = {
        "published_clusters": [old_cluster],
        "telegram_sent_cluster_guids": [],
    }
    initialize_telegram_state(state, config, now)
    assert old_cluster["guid"] in state["telegram_sent_cluster_guids"]

    state["published_clusters"].append(new_cluster)
    assert unsent_telegram_clusters(state, config) == [new_cluster]


def test_publish_unsent_telegram_clusters_marks_success(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_publisher

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@test_channel")
    cluster = published_cluster(config, now)
    state = {
        "published_clusters": [cluster],
        "telegram_sent_cluster_guids": [],
        "telegram_send_records": [],
        "telegram_initialized_at": "2026-04-25T08:00:00+09:00",
    }

    def fake_send(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        return {"ok": True, "message_id": 123, "chat_id": -100}

    monkeypatch.setattr(telegram_publisher, "send_telegram_message", fake_send)
    summary = publish_unsent_telegram_clusters(state, config, now)

    assert summary == {"telegram_sent": 1, "telegram_failed": 0}
    assert cluster["guid"] in state["telegram_sent_cluster_guids"]
    assert state["telegram_send_records"][0]["message_id"] == 123


def test_single_article_publish_enables_web_page_preview(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    from curator import telegram_publisher

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "test-token")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "@test_channel")
    cluster = single_article_cluster(config, now)
    state = {
        "published_clusters": [cluster],
        "telegram_sent_cluster_guids": [],
        "telegram_send_records": [],
        "telegram_initialized_at": "2026-04-25T08:00:00+09:00",
    }
    kwargs_seen = []

    def fake_send(*_args, **kwargs):  # type: ignore[no-untyped-def]
        kwargs_seen.append(kwargs)
        return {"ok": True, "message_id": 123, "chat_id": -100}

    monkeypatch.setattr(telegram_publisher, "send_telegram_message", fake_send)
    summary = publish_unsent_telegram_clusters(state, config, now)

    assert summary == {"telegram_sent": 1, "telegram_failed": 0}
    assert kwargs_seen[0]["disable_web_page_preview"] is False
