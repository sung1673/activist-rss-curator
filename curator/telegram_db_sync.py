from __future__ import annotations

import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from .dates import parse_datetime
from .state import load_state
from .telegram_sources import (
    canonicalize_telegram_channels,
    ensure_telegram_state,
    load_env_files,
    message_key,
    risk_flags_for_text,
)


def table_name(name: str) -> str:
    prefix = os.environ.get("DB_TABLE_PREFIX", "activist").strip().strip("_") or "activist"
    prefix = re.sub(r"[^0-9A-Za-z_]", "", prefix) or "activist"
    safe_name = re.sub(r"[^0-9A-Za-z_]", "", name)
    return f"{prefix}_{safe_name}"


def mysql_datetime(value: object, timezone_name: str = "Asia/Seoul") -> str | None:
    parsed = parse_datetime(value, timezone_name)
    if not parsed:
        return None
    return parsed.strftime("%Y-%m-%d %H:%M:%S")


def mysql_bool(value: object) -> int:
    return 1 if bool(value) else 0


def compact_text(value: object, max_chars: int) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip()


def db_configured() -> bool:
    return bool(os.environ.get("DB_HOST") and os.environ.get("DB_USER") and os.environ.get("DB_NAME"))


def connect_db() -> Any:
    try:
        import pymysql
    except ImportError as exc:  # pragma: no cover - dependency is declared in requirements.
        raise RuntimeError("PyMySQL is not installed") from exc
    if not db_configured():
        raise RuntimeError("DB_HOST, DB_USER, DB_NAME, DB_PASSWORD are required in .env")
    return pymysql.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", "3306")),
        user=os.environ["DB_USER"],
        password=os.environ.get("DB_PASSWORD", ""),
        database=os.environ["DB_NAME"],
        charset=os.environ.get("DB_CHARSET", "utf8mb4"),
        connect_timeout=int(os.environ.get("DB_CONNECT_TIMEOUT", "10")),
        autocommit=False,
    )


def create_schema(conn: Any) -> None:
    channels = table_name("telegram_channels")
    messages = table_name("telegram_messages")
    matches = table_name("telegram_article_matches")
    signals = table_name("telegram_issue_signals")
    outbox = table_name("delivery_outbox")
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {channels} (
              handle VARCHAR(191) NOT NULL,
              telegram_channel_id VARCHAR(64) DEFAULT NULL,
              title VARCHAR(255) DEFAULT NULL,
              description TEXT DEFAULT NULL,
              joined TINYINT(1) NOT NULL DEFAULT 0,
              enabled TINYINT(1) NOT NULL DEFAULT 1,
              source VARCHAR(40) DEFAULT NULL,
              source_type VARCHAR(60) DEFAULT NULL,
              is_public_channel TINYINT(1) NOT NULL DEFAULT 1,
              quality_score INT NOT NULL DEFAULT 0,
              last_message_id BIGINT DEFAULT 0,
              last_collected_at DATETIME DEFAULT NULL,
              last_recommendation_checked_at DATETIME DEFAULT NULL,
              last_error VARCHAR(191) DEFAULT NULL,
              payload_json MEDIUMTEXT DEFAULT NULL,
              updated_at DATETIME NOT NULL,
              PRIMARY KEY (handle),
              KEY idx_channel_id (telegram_channel_id),
              KEY idx_enabled_quality (enabled, quality_score)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {messages} (
              message_key VARCHAR(180) NOT NULL,
              channel_handle VARCHAR(191) NOT NULL,
              telegram_channel_id VARCHAR(64) DEFAULT NULL,
              telegram_message_id BIGINT NOT NULL,
              posted_at DATETIME DEFAULT NULL,
              edited_at DATETIME DEFAULT NULL,
              deleted_at DATETIME DEFAULT NULL,
              collected_at DATETIME DEFAULT NULL,
              text MEDIUMTEXT DEFAULT NULL,
              normalized_text MEDIUMTEXT DEFAULT NULL,
              views INT NOT NULL DEFAULT 0,
              forwards INT NOT NULL DEFAULT 0,
              replies_count INT NOT NULL DEFAULT 0,
              message_url TEXT DEFAULT NULL,
              urls_json MEDIUMTEXT DEFAULT NULL,
              risk_flags_json TEXT DEFAULT NULL,
              raw_json MEDIUMTEXT DEFAULT NULL,
              updated_at DATETIME NOT NULL,
              PRIMARY KEY (message_key),
              UNIQUE KEY uq_channel_message (channel_handle, telegram_message_id),
              KEY idx_posted_at (posted_at),
              KEY idx_channel_posted (channel_handle, posted_at),
              KEY idx_deleted_at (deleted_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {matches} (
              article_id VARCHAR(96) NOT NULL,
              message_key VARCHAR(180) NOT NULL,
              match_type VARCHAR(40) NOT NULL,
              score DECIMAL(6,4) NOT NULL DEFAULT 0,
              reason VARCHAR(500) DEFAULT NULL,
              channel_handle VARCHAR(191) DEFAULT NULL,
              telegram_message_id BIGINT DEFAULT NULL,
              message_url TEXT DEFAULT NULL,
              updated_at DATETIME NOT NULL,
              PRIMARY KEY (article_id, message_key, match_type),
              KEY idx_message_key (message_key),
              KEY idx_article_score (article_id, score),
              KEY idx_match_type (match_type, score)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {signals} (
              article_id VARCHAR(96) NOT NULL,
              related_telegram_count INT NOT NULL DEFAULT 0,
              related_telegram_channels_count INT NOT NULL DEFAULT 0,
              first_seen_at DATETIME DEFAULT NULL,
              latest_seen_at DATETIME DEFAULT NULL,
              confidence_score DECIMAL(6,4) NOT NULL DEFAULT 0,
              payload_json MEDIUMTEXT DEFAULT NULL,
              updated_at DATETIME NOT NULL,
              PRIMARY KEY (article_id),
              KEY idx_signal_strength (related_telegram_channels_count, related_telegram_count),
              KEY idx_latest_seen (latest_seen_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {outbox} (
              delivery_id VARCHAR(96) NOT NULL,
              event_id VARCHAR(96) DEFAULT NULL,
              delivery_channel VARCHAR(40) NOT NULL DEFAULT 'telegram',
              destination VARCHAR(191) NOT NULL,
              idempotency_key VARCHAR(191) NOT NULL,
              payload_json MEDIUMTEXT NOT NULL,
              status VARCHAR(24) NOT NULL DEFAULT 'pending',
              attempt_count INT NOT NULL DEFAULT 0,
              next_attempt_at DATETIME DEFAULT NULL,
              lease_token VARCHAR(64) DEFAULT NULL,
              locked_by VARCHAR(96) DEFAULT NULL,
              locked_at DATETIME DEFAULT NULL,
              lease_expires_at DATETIME DEFAULT NULL,
              external_message_id VARCHAR(191) DEFAULT NULL,
              last_error TEXT DEFAULT NULL,
              delivered_at DATETIME DEFAULT NULL,
              dead_lettered_at DATETIME DEFAULT NULL,
              created_at DATETIME NOT NULL,
              updated_at DATETIME NOT NULL,
              PRIMARY KEY (delivery_id),
              UNIQUE KEY uq_delivery_idempotency (delivery_channel, destination, idempotency_key),
              KEY idx_delivery_ready (status, next_attempt_at),
              KEY idx_delivery_lease (status, lease_expires_at),
              KEY idx_delivery_event (event_id),
              KEY idx_delivery_dead_letter (dead_lettered_at)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """
        )
    conn.commit()


def channel_rows(state: dict[str, object], timezone_name: str) -> list[dict[str, object]]:
    canonicalize_telegram_channels(state)
    rows: list[dict[str, object]] = []
    for channel in state.get("telegram_source_channels", []):
        if not isinstance(channel, dict):
            continue
        handle = str(channel.get("handle") or "").strip().removeprefix("@")
        if not handle:
            continue
        rows.append(
            {
                "handle": handle,
                "telegram_channel_id": str(channel.get("telegram_channel_id") or "") or None,
                "title": compact_text(channel.get("title"), 255),
                "description": str(channel.get("description") or ""),
                "joined": mysql_bool(channel.get("joined")),
                "enabled": mysql_bool(channel.get("enabled", True)),
                "source": compact_text(channel.get("source"), 40),
                "source_type": compact_text(channel.get("source_type") or "public_channel", 60),
                "is_public_channel": mysql_bool(channel.get("is_public_channel", True)),
                "quality_score": int(channel.get("quality_score") or 0),
                "last_message_id": int(channel.get("last_message_id") or 0),
                "last_collected_at": mysql_datetime(channel.get("last_collected_at"), timezone_name),
                "last_recommendation_checked_at": mysql_datetime(channel.get("last_recommendation_checked_at"), timezone_name),
                "last_error": compact_text(channel.get("last_error"), 191) or None,
                "payload_json": json.dumps(channel, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return rows


def message_rows(state: dict[str, object], timezone_name: str, *, limit: int = 0) -> list[dict[str, object]]:
    messages = [message for message in state.get("telegram_source_messages", []) if isinstance(message, dict)]
    if limit:
        messages = messages[-limit:]
    rows: list[dict[str, object]] = []
    for message in messages:
        key = message_key(message)
        handle = str(message.get("handle") or "").strip().removeprefix("@")
        message_id = int(message.get("telegram_message_id") or 0)
        if not key or not handle or not message_id:
            continue
        text = str(message.get("text") or "")
        raw_json = message.get("raw_json") if isinstance(message.get("raw_json"), dict) else None
        rows.append(
            {
                "message_key": key,
                "channel_handle": handle,
                "telegram_channel_id": str(message.get("telegram_channel_id") or "") or None,
                "telegram_message_id": message_id,
                "posted_at": mysql_datetime(message.get("posted_at"), timezone_name),
                "edited_at": mysql_datetime(message.get("edited_at"), timezone_name),
                "deleted_at": mysql_datetime(message.get("deleted_at"), timezone_name),
                "collected_at": mysql_datetime(message.get("collected_at"), timezone_name),
                "text": text,
                "normalized_text": str(message.get("normalized_text") or ""),
                "views": int(message.get("views") or 0),
                "forwards": int(message.get("forwards") or 0),
                "replies_count": int(message.get("replies_count") or 0),
                "message_url": str(message.get("message_url") or ""),
                "urls_json": json.dumps(message.get("urls") or [], ensure_ascii=False, separators=(",", ":")),
                "risk_flags_json": json.dumps(risk_flags_for_text(text), ensure_ascii=False, separators=(",", ":")),
                "raw_json": json.dumps(raw_json, ensure_ascii=False, sort_keys=True, separators=(",", ":")) if raw_json else None,
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return rows


def match_rows(state: dict[str, object], message_keys: set[str] | None = None) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for match in state.get("telegram_article_matches", []):
        if not isinstance(match, dict):
            continue
        key = str(match.get("telegram_message_key") or "")
        if message_keys is not None and key not in message_keys:
            continue
        article_id = str(match.get("article_id") or "")
        match_type = str(match.get("match_type") or "")
        if not article_id or not key or not match_type:
            continue
        rows.append(
            {
                "article_id": article_id,
                "message_key": key,
                "match_type": match_type,
                "score": float(match.get("score") or 0),
                "reason": compact_text(match.get("reason"), 500) or None,
                "channel_handle": compact_text(match.get("channel_handle"), 191) or None,
                "telegram_message_id": int(match.get("telegram_message_id") or 0) or None,
                "message_url": str(match.get("message_url") or ""),
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return rows


def signal_rows(state: dict[str, object], timezone_name: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for signal in state.get("telegram_issue_signals", []):
        if not isinstance(signal, dict):
            continue
        article_id = str(signal.get("article_id") or "")
        if not article_id:
            continue
        rows.append(
            {
                "article_id": article_id,
                "related_telegram_count": int(signal.get("related_telegram_count") or 0),
                "related_telegram_channels_count": int(signal.get("related_telegram_channels_count") or 0),
                "first_seen_at": mysql_datetime(signal.get("first_seen_at"), timezone_name),
                "latest_seen_at": mysql_datetime(signal.get("latest_seen_at"), timezone_name),
                "confidence_score": float(signal.get("confidence_score") or 0),
                "payload_json": json.dumps(signal, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )
    return rows


def outbox_rows(state: dict[str, object], timezone_name: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for entry in state.get("telegram_delivery_outbox", []):
        if not isinstance(entry, dict):
            continue
        if str(entry.get("status") or "") == "remote_queued":
            # The API-owned row already exists.  Re-upserting it from ephemeral
            # local state could reset a delivered/processing row to pending.
            continue
        outbox_id = compact_text(entry.get("outbox_id"), 96)
        destination = compact_text(entry.get("destination"), 191)
        payload_text = str(entry.get("payload_text") or "")
        if not outbox_id or not destination or not payload_text:
            continue
        rows.append(
            {
                "delivery_id": outbox_id,
                "event_id": compact_text(entry.get("event_id"), 96) or None,
                "delivery_channel": compact_text(entry.get("channel") or "telegram", 40),
                "destination": destination,
                "idempotency_key": compact_text(entry.get("cluster_guid") or outbox_id, 191),
                "payload_json": json.dumps(
                    {
                        "text": payload_text,
                        "disable_web_page_preview": bool(entry.get("disable_web_page_preview", True)),
                        "cluster_guid": entry.get("cluster_guid"),
                        "source_kind": entry.get("source_kind"),
                        "source_right_id": entry.get("source_right_id"),
                        "source_kinds": entry.get("source_kinds") or [],
                        "rights_lineage_complete": isinstance(entry.get("source_right_ids"), list)
                        and isinstance(entry.get("article_sources"), list),
                        "source_right_ids": entry.get("source_right_ids") or [],
                        "article_sources": entry.get("article_sources") or [],
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "status": compact_text(entry.get("status") or "pending", 24),
                "attempt_count": int(entry.get("attempt_count") or 0),
                "next_attempt_at": mysql_datetime(entry.get("next_attempt_at"), timezone_name),
                "lease_token": compact_text(entry.get("lease_token"), 64) or None,
                "locked_by": compact_text(entry.get("locked_by"), 96) or None,
                "locked_at": mysql_datetime(entry.get("locked_at"), timezone_name),
                "lease_expires_at": mysql_datetime(entry.get("lease_expires_at"), timezone_name),
                "external_message_id": compact_text(entry.get("external_message_id"), 191) or None,
                "last_error": compact_text(entry.get("last_error"), 500) or None,
                "delivered_at": mysql_datetime(entry.get("delivered_at"), timezone_name),
                "dead_lettered_at": mysql_datetime(entry.get("dead_lettered_at"), timezone_name),
                "created_at": mysql_datetime(entry.get("created_at"), timezone_name) or now_text,
                "updated_at": now_text,
            }
        )
    return rows


def reconcile_db_channel_identities(conn: Any, rows: list[dict[str, object]]) -> int:
    """Remove stale handle aliases before upserting the canonical channel row."""

    removed = 0
    table = table_name("telegram_channels")
    with conn.cursor() as cur:
        for row in rows:
            channel_id = str(row.get("telegram_channel_id") or "")
            handle = str(row.get("handle") or "")
            if not channel_id or not handle:
                continue
            removed += int(
                cur.execute(
                    f"DELETE FROM {table} WHERE telegram_channel_id=%s AND handle<>%s",
                    (channel_id, handle),
                )
            )
    return removed


def reconcile_db_message_identities(conn: Any, rows: list[dict[str, object]]) -> int:
    """Rewrite handle-keyed message rows to the authoritative channel-ID key."""

    migrated = 0
    table = table_name("telegram_messages")
    with conn.cursor() as cur:
        for row in rows:
            channel_id = str(row.get("telegram_channel_id") or "")
            handle = str(row.get("handle") or "")
            if not channel_id or not handle:
                continue
            # If both an old alias and a canonical row already exist, retain the
            # canonical row before rewriting the remaining aliases.
            cur.execute(
                f"""
                DELETE stale FROM {table} AS stale
                INNER JOIN {table} AS canonical
                  ON canonical.message_key=CONCAT('id:', %s, ':', stale.telegram_message_id)
                WHERE stale.message_key<>canonical.message_key
                  AND (stale.channel_handle=%s OR stale.telegram_channel_id=%s)
                """,
                (channel_id, handle, channel_id),
            )
            migrated += int(
                cur.execute(
                    f"""
                    UPDATE {table}
                    SET message_key=CONCAT('id:', %s, ':', telegram_message_id),
                        channel_handle=%s,
                        telegram_channel_id=%s
                    WHERE channel_handle=%s OR telegram_channel_id=%s
                    """,
                    (channel_id, handle, channel_id, handle, channel_id),
                )
            )
    return migrated


def reconcile_db_match_identities(conn: Any, rows: list[dict[str, object]]) -> int:
    """Rewrite article-match foreign keys after a handle-only channel gains an ID."""

    migrated = 0
    table = table_name("telegram_article_matches")
    with conn.cursor() as cur:
        for row in rows:
            channel_id = str(row.get("telegram_channel_id") or "")
            handle = str(row.get("handle") or "")
            if not channel_id or not handle:
                continue
            cur.execute(
                f"""
                DELETE stale FROM {table} AS stale
                INNER JOIN {table} AS canonical
                  ON canonical.article_id=stale.article_id
                 AND canonical.match_type=stale.match_type
                 AND canonical.message_key=CONCAT('id:', %s, ':', stale.telegram_message_id)
                WHERE stale.message_key<>canonical.message_key
                  AND stale.channel_handle=%s
                """,
                (channel_id, handle),
            )
            migrated += int(
                cur.execute(
                    f"""
                    UPDATE {table}
                    SET message_key=CONCAT('id:', %s, ':', telegram_message_id),
                        channel_handle=%s
                    WHERE channel_handle=%s AND telegram_message_id IS NOT NULL
                    """,
                    (channel_id, handle, handle),
                )
            )
    return migrated


def executemany_upsert(conn: Any, table: str, rows: list[dict[str, object]], update_columns: list[str]) -> int:
    if not rows:
        return 0
    columns = list(rows[0].keys())
    placeholders = ", ".join(["%s"] * len(columns))
    assignments = ", ".join(f"{column}=VALUES({column})" for column in update_columns)
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {assignments}"
    values = [tuple(row.get(column) for column in columns) for row in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, values)
    return len(rows)


def delete_existing_match_rows(conn: Any, message_keys: set[str], *, replace_all: bool) -> int:
    table = table_name("telegram_article_matches")
    signals = table_name("telegram_issue_signals")
    with conn.cursor() as cur:
        if replace_all:
            match_count = cur.execute(f"DELETE FROM {table}")
            cur.execute(f"DELETE FROM {signals}")
            return int(match_count)
        deleted = 0
        keys = sorted(message_keys)
        for index in range(0, len(keys), 500):
            chunk = keys[index : index + 500]
            if not chunk:
                continue
            placeholders = ", ".join(["%s"] * len(chunk))
            deleted += int(cur.execute(f"DELETE FROM {table} WHERE message_key IN ({placeholders})", chunk))
        return deleted


def sync_state_to_db(
    state: dict[str, object],
    *,
    limit: int = 0,
    timezone_name: str = "Asia/Seoul",
    migrate: bool = True,
    replace_matches: bool = False,
) -> dict[str, int]:
    ensure_telegram_state(state)
    conn = connect_db()
    try:
        if migrate:
            create_schema(conn)
        channels = channel_rows(state, timezone_name)
        messages = message_rows(state, timezone_name, limit=limit)
        message_key_set = {str(row["message_key"]) for row in messages}
        matches = match_rows(state, message_key_set if limit else None)
        signals = signal_rows(state, timezone_name)
        outbox = outbox_rows(state, timezone_name)
        deleted_matches = delete_existing_match_rows(conn, message_key_set, replace_all=not limit) if replace_matches else 0
        migrated_channels = reconcile_db_channel_identities(conn, channels)
        migrated_messages = reconcile_db_message_identities(conn, channels)
        migrated_matches = reconcile_db_match_identities(conn, channels)
        channel_count = executemany_upsert(
            conn,
            table_name("telegram_channels"),
            channels,
            [
                "telegram_channel_id",
                "title",
                "description",
                "joined",
                "enabled",
                "source",
                "source_type",
                "is_public_channel",
                "quality_score",
                "last_message_id",
                "last_collected_at",
                "last_recommendation_checked_at",
                "last_error",
                "payload_json",
                "updated_at",
            ],
        )
        message_count = executemany_upsert(
            conn,
            table_name("telegram_messages"),
            messages,
            [
                "channel_handle",
                "telegram_channel_id",
                "posted_at",
                "edited_at",
                "deleted_at",
                "collected_at",
                "text",
                "normalized_text",
                "views",
                "forwards",
                "replies_count",
                "message_url",
                "urls_json",
                "risk_flags_json",
                "raw_json",
                "updated_at",
            ],
        )
        match_count = executemany_upsert(
            conn,
            table_name("telegram_article_matches"),
            matches,
            ["score", "reason", "channel_handle", "telegram_message_id", "message_url", "updated_at"],
        )
        signal_count = executemany_upsert(
            conn,
            table_name("telegram_issue_signals"),
            signals,
            [
                "related_telegram_count",
                "related_telegram_channels_count",
                "first_seen_at",
                "latest_seen_at",
                "confidence_score",
                "payload_json",
                "updated_at",
            ],
        )
        outbox_count = executemany_upsert(
            conn,
            table_name("delivery_outbox"),
            outbox,
            [
                "destination",
                "payload_json",
                "status",
                "attempt_count",
                "next_attempt_at",
                "lease_token",
                "locked_by",
                "locked_at",
                "lease_expires_at",
                "delivered_at",
                "dead_lettered_at",
                "external_message_id",
                "last_error",
                "updated_at",
            ],
        )
        conn.commit()
        return {
            "telegram_db_channels": channel_count,
            "telegram_db_channels_migrated": migrated_channels,
            "telegram_db_messages_migrated": migrated_messages,
            "telegram_db_messages": message_count,
            "telegram_db_matches": match_count,
            "telegram_db_matches_migrated": migrated_matches,
            "telegram_db_matches_deleted": deleted_matches,
            "telegram_db_signals": signal_count,
            "telegram_db_outbox": outbox_count,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def db_counts() -> dict[str, int]:
    conn = connect_db()
    try:
        with conn.cursor() as cur:
            counts: dict[str, int] = {}
            for key in (
                "telegram_channels",
                "telegram_messages",
                "telegram_article_matches",
                "telegram_issue_signals",
                "delivery_outbox",
            ):
                table = table_name(key)
                cur.execute("SHOW TABLES LIKE %s", (table,))
                if not cur.fetchone():
                    counts[key] = 0
                    continue
                cur.execute("SELECT COUNT(*) FROM " + table)
                counts[key] = int(cur.fetchone()[0])
            return counts
    finally:
        conn.close()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync local Telegram source state into MySQL tables.")
    parser.add_argument("--root", default=".", help="Project root containing .env and data/state.json")
    parser.add_argument("--limit", type=int, default=0, help="Sync only the most recent N messages, 0 means all")
    parser.add_argument("--no-migrate", action="store_true", help="Do not create missing activist_telegram_* tables")
    parser.add_argument("--replace-matches", action="store_true", help="Delete existing Telegram article matches before inserting current matches")
    parser.add_argument("--counts", action="store_true", help="Only print current DB counts")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    root = Path(args.root).resolve()
    load_env_files(root)
    if args.counts:
        print(json.dumps(db_counts(), ensure_ascii=False, indent=2))
        return 0
    state = load_state(root / "data" / "state.json")
    summary = sync_state_to_db(
        state,
        limit=max(0, int(args.limit)),
        timezone_name="Asia/Seoul",
        migrate=not args.no_migrate,
        replace_matches=bool(args.replace_matches),
    )
    summary.update(db_counts())
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
