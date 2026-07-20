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
    assert payload["raw_records"]
    assert payload["stories"]
    article_payload = payload["articles"][0]  # type: ignore[index]
    assert "archive_version" not in article_payload
    assert "relevance_keywords" not in article_payload
    raw_payload = payload["raw_records"][0]  # type: ignore[index]
    assert raw_payload["raw_kind"] == "decision_trace"
    assert raw_payload["compression"] == "gzip"
    assert raw_payload["record_id"] == article_payload["record_id"]
    story = payload["stories"][0]  # type: ignore[index]
    assert story["article_ids"]
    assert story["status"] == "published"


def test_snapshot_payload_excludes_revoked_telegram_article_and_refreshes_story(config) -> None:  # type: ignore[no-untyped-def]
    now = datetime(2026, 5, 3, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    config["source_rights"] = {
        "enforce": True,
        "records": [
            {
                "source_right_id": "telegram:revoked",
                "source_category": "authorized_telegram",
                "source_identity": "revoked",
                "scope": "collection,ai,redistribution",
                "evidence_ref": "evidence://test/revoked",
                "valid_from": "2021-01-01",
                "revoked_at": "2026-05-03",
                "allow_ai": True,
                "allow_redistribution": True,
                "status": "active",
            }
        ],
    }
    telegram_article = make_article("철회된 Telegram 주장", "https://example.com/telegram")
    telegram_article.update(
        {
            "source_kind": "telegram_reference",
            "source_right_id": "telegram:revoked",
            "telegram_source_handle": "revoked",
        }
    )
    direct_article = make_article("독립 확인 기사", "https://example.com/direct")
    direct_article["source_kind"] = "direct"
    records = [
        article_record(telegram_article, "accepted", now),
        article_record(direct_article, "accepted", now),
    ]
    cluster = {
        "guid": "cluster:mixed:20260503:1",
        "cluster_key": "mixed-cluster",
        "status": "published",
        "representative_title": telegram_article["title"],
        "representative_url": telegram_article["canonical_url"],
        "article_count": 2,
        "articles": [telegram_article, direct_article],
        "published_at": now.isoformat(),
        "source_kind": "telegram_reference",
        "source_right_id": "telegram:revoked",
    }

    payload = snapshot_payload(
        {"articles": records, "published_clusters": [cluster], "pending_clusters": []},
        config,
        now,
    )

    assert [article["canonical_url"] for article in payload["articles"]] == ["https://example.com/direct"]
    story = payload["stories"][0]
    assert story["representative_title"] == "독립 확인 기사"
    assert story["representative_url"] == "https://example.com/direct"
    assert story["source_kind"] == "direct"
    assert story["source_right_ids"] == []
    assert len(story["article_ids"]) == 1


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
