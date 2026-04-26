from __future__ import annotations

from copy import deepcopy
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from curator.config import DEFAULT_CONFIG
from curator.normalize import canonical_url_hash, normalize_title_parts


@pytest.fixture
def config() -> dict[str, object]:
    return deepcopy(DEFAULT_CONFIG)


@pytest.fixture
def now() -> datetime:
    return datetime(2026, 4, 25, 9, 30, tzinfo=ZoneInfo("Asia/Seoul"))


def make_article(
    title: str,
    url: str,
    *,
    summary: str = "",
    source: str = "테스트뉴스",
    relevance_level: str = "medium",
    published_at: str = "2026-04-25T09:00:00+09:00",
) -> dict[str, object]:
    title_parts = normalize_title_parts(title)
    return {
        "title": title,
        "clean_title": title_parts["clean_title"],
        "normalized_title": title_parts["normalized_title"],
        "prefixes": title_parts["prefixes"],
        "source": source,
        "link": url,
        "canonical_url": url,
        "canonical_url_hash": canonical_url_hash(url),
        "title_hash": title_parts["title_hash"],
        "summary": summary,
        "article_published_at": published_at,
        "feed_published_at": published_at,
        "published_at": published_at,
        "date_status": "article",
        "relevance_level": relevance_level,
    }
