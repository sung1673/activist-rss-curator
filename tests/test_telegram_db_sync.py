from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from curator.telegram_db_sync import channel_rows, match_rows, message_rows, mysql_datetime, table_name


def test_mysql_datetime_formats_timezone_aware_value() -> None:
    value = datetime(2026, 5, 5, 1, 2, 3, tzinfo=ZoneInfo("UTC"))

    assert mysql_datetime(value, "Asia/Seoul") == "2026-05-05 10:02:03"


def test_table_name_uses_configured_prefix(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DB_TABLE_PREFIX", "activist_")

    assert table_name("telegram_messages") == "activist_telegram_messages"


def test_telegram_db_rows_are_compact_and_keyed(now) -> None:  # type: ignore[no-untyped-def]
    state = {
        "telegram_source_channels": [
            {"handle": "marketnews", "title": "경제 뉴스", "enabled": True, "telegram_channel_id": "100"}
        ],
        "telegram_source_messages": [
            {
                "handle": "marketnews",
                "telegram_channel_id": "100",
                "telegram_message_id": 7,
                "posted_at": now.isoformat(),
                "text": "행동주의 뉴스 https://example.com/a?utm_source=tg",
                "normalized_text": "행동주의 뉴스 https://example.com/a",
                "message_url": "https://t.me/marketnews/7",
                "urls": ["https://example.com/a"],
            }
        ],
        "telegram_article_matches": [
            {
                "article_id": "article-1",
                "telegram_message_key": "id:100:7",
                "match_type": "exact_url",
                "score": 1,
            }
        ],
    }

    channels = channel_rows(state, "Asia/Seoul")
    messages = message_rows(state, "Asia/Seoul")
    matches = match_rows(state)

    assert channels[0]["handle"] == "marketnews"
    assert messages[0]["message_key"] == "id:100:7"
    assert messages[0]["risk_flags_json"] == "[]"
    assert matches[0]["article_id"] == "article-1"
