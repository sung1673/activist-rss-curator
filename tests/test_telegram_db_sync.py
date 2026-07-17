from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

from curator.telegram_db_sync import (
    channel_rows,
    delete_existing_match_rows,
    match_rows,
    message_rows,
    mysql_datetime,
    outbox_rows,
    reconcile_db_channel_identities,
    reconcile_db_message_identities,
    table_name,
)


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


def test_delete_existing_match_rows_replaces_selected_messages(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DB_TABLE_PREFIX", "activist_")
    calls: list[tuple[str, object]] = []

    class Cursor:
        def execute(self, sql: str, params: object = None) -> int:
            calls.append((sql, params))
            return 2

        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

    class Conn:
        def cursor(self) -> Cursor:
            return Cursor()

    deleted = delete_existing_match_rows(Conn(), {"id:1:1", "id:1:2"}, replace_all=False)

    assert deleted == 2
    assert "WHERE message_key IN" in calls[0][0]
    assert calls[0][1] == ["id:1:1", "id:1:2"]


def test_outbox_rows_persist_delivery_state(now) -> None:  # type: ignore[no-untyped-def]
    state = {
        "telegram_delivery_outbox": [
            {
                "outbox_id": "telegram:cluster-1",
                "cluster_guid": "cluster-1",
                "destination": "@channel",
                "payload_text": "hello",
                "status": "retry",
                "attempt_count": 2,
                "next_attempt_at": now.isoformat(),
                "last_error": "telegram_http_error",
                "created_at": now.isoformat(),
                "source_kind": "telegram_reference",
                "source_right_id": "telegram:licensed",
                "source_kinds": ["telegram_reference"],
                "source_right_ids": ["telegram:licensed"],
                "article_sources": [
                    {
                        "canonical_url_hash": "a" * 64,
                        "source_kind": "telegram_reference",
                        "source_right_id": "telegram:licensed",
                    }
                ],
            }
        ]
    }

    rows = outbox_rows(state, "Asia/Seoul")

    assert rows[0]["delivery_id"] == "telegram:cluster-1"
    assert rows[0]["status"] == "retry"
    assert rows[0]["attempt_count"] == 2
    assert rows[0]["external_message_id"] is None
    payload = json.loads(str(rows[0]["payload_json"]))
    assert payload["rights_lineage_complete"] is True
    assert payload["source_right_ids"] == ["telegram:licensed"]
    assert payload["article_sources"][0]["source_right_id"] == "telegram:licensed"


def test_outbox_rows_marks_legacy_missing_lineage_incomplete(now) -> None:  # type: ignore[no-untyped-def]
    rows = outbox_rows(
        {
            "telegram_delivery_outbox": [
                {
                    "outbox_id": "telegram:legacy",
                    "cluster_guid": "legacy",
                    "destination": "@channel",
                    "payload_text": "legacy",
                    "status": "pending",
                }
            ]
        },
        "Asia/Seoul",
    )

    payload = json.loads(str(rows[0]["payload_json"]))
    assert payload["rights_lineage_complete"] is False
    assert payload["source_right_ids"] == []


def test_db_channel_identity_reconciliation_removes_stale_handle(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DB_TABLE_PREFIX", "activist_")
    calls: list[tuple[str, object]] = []

    class Cursor:
        def execute(self, sql: str, params: object = None) -> int:
            calls.append((sql, params))
            return 1

        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

    class Conn:
        def cursor(self) -> Cursor:
            return Cursor()

    removed = reconcile_db_channel_identities(
        Conn(),
        [{"handle": "new_handle", "telegram_channel_id": "100"}],
    )

    assert removed == 1
    assert "telegram_channel_id=%s AND handle<>%s" in calls[0][0]
    assert calls[0][1] == ("100", "new_handle")


def test_db_message_identity_reconciliation_rewrites_handle_keys(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setenv("DB_TABLE_PREFIX", "activist_")
    calls: list[tuple[str, object]] = []

    class Cursor:
        def execute(self, sql: str, params: object = None) -> int:
            calls.append((sql, params))
            return 1

        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

    class Conn:
        def cursor(self) -> Cursor:
            return Cursor()

    migrated = reconcile_db_message_identities(
        Conn(),
        [{"handle": "marketnews", "telegram_channel_id": "100"}],
    )

    assert migrated == 1
    assert "DELETE stale" in calls[0][0]
    assert "SET message_key=CONCAT('id:'" in calls[1][0]
    assert calls[1][1] == ("100", "marketnews", "100", "marketnews", "100")
