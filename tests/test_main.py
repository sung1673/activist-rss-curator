from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from curator.main import article_is_before_previous_day, prune_excluded_pending_articles

from conftest import make_article


def test_article_before_previous_day_filter(config) -> None:  # type: ignore[no-untyped-def]
    now = datetime(2026, 4, 28, 8, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    old_article = make_article(
        "오래된 주주제안 기사",
        "https://example.com/old",
        published_at="2026-04-26T23:59:00+09:00",
    )
    previous_day_article = make_article(
        "전일 주주제안 기사",
        "https://example.com/previous-day",
        published_at="2026-04-27T00:00:00+09:00",
    )

    assert article_is_before_previous_day(old_article, config, now)
    assert not article_is_before_previous_day(previous_day_article, config, now)


def test_previous_day_filter_can_be_disabled(config) -> None:  # type: ignore[no-untyped-def]
    config["date_filter"]["exclude_before_previous_day"] = False  # type: ignore[index]
    now = datetime(2026, 4, 28, 8, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    old_article = make_article(
        "오래된 주주제안 기사",
        "https://example.com/old",
        published_at="2026-04-26T23:59:00+09:00",
    )

    assert not article_is_before_previous_day(old_article, config, now)


def test_prune_excluded_pending_articles_removes_old_articles_from_state(config) -> None:  # type: ignore[no-untyped-def]
    now = datetime(2026, 4, 28, 8, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    old_article = make_article(
        "오래된 주주제안 기사",
        "https://example.com/old",
        published_at="2026-04-26T23:59:00+09:00",
    )
    fresh_article = make_article(
        "전일 주주제안 기사",
        "https://example.com/fresh",
        published_at="2026-04-27T09:00:00+09:00",
    )
    state = {
        "pending_clusters": [{"articles": [old_article, fresh_article], "article_count": 2}],
        "published_clusters": [{"articles": [old_article], "article_count": 1}],
    }

    prune_excluded_pending_articles(state, config, now)

    assert state["pending_clusters"][0]["articles"] == [fresh_article]
    assert state["pending_clusters"][0]["article_count"] == 1
    assert state["published_clusters"] == []
