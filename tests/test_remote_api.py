from __future__ import annotations

import hashlib
import hmac
from datetime import datetime
from zoneinfo import ZoneInfo

from conftest import make_article

from curator.remote_api import (
    report_payload,
    signed_headers,
    snapshot_payload,
    sync_state_to_remote_api,
)
from curator.state import article_record


def test_signed_headers_match_php_api_contract() -> None:
    body = b'{"ok":true}'
    headers = signed_headers(body, "secret", timestamp=1_777_777_777, nonce="nonce-1234567890")
    expected = hmac.new(
        b"secret",
        b"1777777777\nnonce-1234567890\n" + body,
        hashlib.sha256,
    ).hexdigest()

    assert headers["X-Activist-Timestamp"] == "1777777777"
    assert headers["X-Activist-Nonce"] == "nonce-1234567890"
    assert headers["X-Activist-Signature"] == f"sha256={expected}"


def test_snapshot_payload_builds_articles_and_stories(config) -> None:  # type: ignore[no-untyped-def]
    now = datetime(2026, 5, 3, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    article = make_article("소액주주 주주제안", "https://example.com/a")
    record = article_record(article, "accepted", now)
    cluster = {
        "guid": "cluster:test:20260503:1",
        "cluster_key": "test-cluster",
        "status": "published",
        "representative_title": "소액주주 주주제안",
        "representative_url": "https://example.com/a",
        "relevance_level": "high",
        "article_count": 1,
        "articles": [article],
        "published_at": "2026-05-03T09:00:00+09:00",
    }
    state = {
        "articles": [record],
        "published_clusters": [cluster],
        "pending_clusters": [],
        "last_run_at": "2026-05-03T09:00:00+09:00",
    }

    payload = snapshot_payload(state, config, now, {"fetched": 1, "accepted": 1})

    assert payload["run"]["fetched"] == 1  # type: ignore[index]
    assert payload["articles"]
    assert payload["stories"]
    story = payload["stories"][0]  # type: ignore[index]
    assert story["article_ids"]
    assert story["status"] == "published"


def test_sync_state_skips_when_api_secret_missing(config, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("ACTIVIST_API_URL", raising=False)
    monkeypatch.delenv("ACTIVIST_API_SECRET", raising=False)
    now = datetime(2026, 5, 3, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))

    assert sync_state_to_remote_api({}, config, now) == {}


def test_report_payload_is_compact() -> None:
    start_at = datetime(2026, 5, 2, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    end_at = datetime(2026, 5, 3, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    payload = report_payload(
        {
            "date_id": "2026-05-03",
            "start_at": start_at,
            "end_at": end_at,
            "report_url": "https://news.bside.ai/feed/2026-05-03.html",
            "stats": {"stories": 2, "articles": 5},
            "review": "핵심 요약",
        }
    )

    assert payload["date_id"] == "2026-05-03"
    assert payload["story_count"] == 2
    assert payload["article_count"] == 5
    assert payload["public_url"].endswith("/2026-05-03.html")
