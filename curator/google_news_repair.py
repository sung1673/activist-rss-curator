from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from .config import load_config
from .dates import datetime_to_iso, get_timezone, parse_datetime
from .fetch import (
    USER_AGENT,
    apply_decoded_google_news_url,
    decode_google_news_url_online_result,
    enrich_article,
)
from .main import PROJECT_ROOT
from .normalize import canonical_url_hash, normalize_url


DEFAULT_LIMIT = 50


@dataclass
class RepairStats:
    scanned: int = 0
    decoded: int = 0
    enriched: int = 0
    updated: int = 0
    state_updated: int = 0
    conflicts: int = 0
    skipped: int = 0
    failed: int = 0
    rate_limited: bool = False


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def load_local_env(root: Path) -> None:
    load_env_file(root / ".env")
    load_env_file(root / ".env.api")


def require_pymysql() -> Any:
    try:
        import pymysql  # type: ignore
    except ImportError as exc:
        raise RuntimeError("PyMySQL is required for DB repair. Install requirements.txt or run `pip install PyMySQL`.") from exc
    return pymysql


def db_config_from_env() -> dict[str, Any]:
    host = os.getenv("DB_HOST") or os.getenv("MYSQL_HOST")
    database = os.getenv("DB_NAME") or os.getenv("MYSQL_DATABASE")
    user = os.getenv("DB_USER") or os.getenv("MYSQL_USER")
    password = os.getenv("DB_PASSWORD") or os.getenv("MYSQL_PASSWORD")
    if not all([host, database, user, password]):
        raise RuntimeError("DB_HOST/DB_NAME/DB_USER/DB_PASSWORD are required in .env")
    return {
        "host": host,
        "port": int(os.getenv("DB_PORT") or os.getenv("MYSQL_PORT") or "3306"),
        "user": user,
        "password": password,
        "database": database,
        "charset": os.getenv("DB_CHARSET") or "utf8mb4",
        "connect_timeout": 10,
        "read_timeout": 30,
        "write_timeout": 30,
    }


def db_connect() -> Any:
    pymysql = require_pymysql()
    return pymysql.connect(**db_config_from_env(), cursorclass=pymysql.cursors.DictCursor)


def google_news_host(url: str) -> bool:
    return (urlsplit(str(url or "")).hostname or "").casefold() == "news.google.com"


def select_candidates(conn: Any, *, limit: int, include_rejected: bool) -> list[dict[str, Any]]:
    status_clause = "" if include_rejected else "AND status <> 'rejected'"
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT record_id, canonical_url, canonical_url_hash, title_hash, title, normalized_title,
                   summary, source, feed_name, feed_category, image_url, published_at, seen_at,
                   status, reason, relevance_level, priority_score, priority_level, story_key
            FROM activist_articles
            WHERE canonical_url LIKE %s
              {status_clause}
            ORDER BY sort_at DESC, updated_at DESC
            LIMIT %s
            """,
            ("https://news.google.com/%", limit),
        )
        return list(cur.fetchall())


def existing_record_for_hash(conn: Any, url_hash: str, record_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT record_id, status, title
            FROM activist_articles
            WHERE canonical_url_hash = %s AND record_id <> %s
            LIMIT 1
            """,
            (url_hash, record_id),
        )
        return cur.fetchone()


def row_to_article(row: dict[str, Any]) -> dict[str, object]:
    article: dict[str, object] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            article[key] = datetime_to_iso(value.replace(tzinfo=get_timezone()))
        elif value is not None:
            article[key] = value
    if row.get("published_at"):
        article["article_published_at"] = datetime_to_iso(row["published_at"].replace(tzinfo=get_timezone()))
    return article


def mysql_datetime(value: object) -> datetime | None:
    parsed = parse_datetime(value, "Asia/Seoul")
    if not parsed:
        return None
    return parsed.replace(tzinfo=None)


def update_article_row(
    conn: Any,
    *,
    record_id: str,
    old_url: str,
    repaired: dict[str, object],
    mark_duplicate_of: dict[str, Any] | None,
    update_published_at: bool,
) -> None:
    canonical_url = str(repaired.get("canonical_url") or "")
    values: dict[str, Any] = {
        "canonical_url": canonical_url,
        "canonical_url_hash": str(repaired.get("canonical_url_hash") or canonical_url_hash(canonical_url)),
        "source": str(repaired.get("source") or ""),
        "image_url": str(repaired.get("image_url") or ""),
        "record_id": record_id,
    }
    assignments = [
        "canonical_url = %(canonical_url)s",
        "canonical_url_hash = %(canonical_url_hash)s",
        "updated_at = NOW()",
    ]
    if values["source"]:
        assignments.append("source = %(source)s")
    if values["image_url"]:
        assignments.append("image_url = %(image_url)s")
    if update_published_at and repaired.get("article_published_at"):
        values["published_at"] = mysql_datetime(repaired.get("article_published_at"))
        if values["published_at"]:
            assignments.append("published_at = %(published_at)s")
            assignments.append("sort_at = %(published_at)s")
    if mark_duplicate_of:
        values["reason"] = f"google_news_resolved_duplicate:{mark_duplicate_of.get('record_id')}"
        assignments.append("status = 'duplicate'")
        assignments.append("reason = %(reason)s")

    with conn.cursor() as cur:
        cur.execute(
            f"UPDATE activist_articles SET {', '.join(assignments)} WHERE record_id = %(record_id)s",
            values,
        )
        cur.execute(
            """
            UPDATE activist_stories
            SET representative_url = %s, updated_at = NOW()
            WHERE representative_url = %s
            """,
            (canonical_url, old_url),
        )


def repair_state_file(path: Path, old_url: str, repaired: dict[str, object], *, apply: bool) -> int:
    if not path.exists():
        return 0
    try:
        state = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return 0
    updated = 0
    containers = [state.get("articles", []), state.get("rejected_articles", [])]
    for cluster_key in ("pending_clusters", "published_clusters"):
        for cluster in state.get(cluster_key, []) if isinstance(state.get(cluster_key), list) else []:
            if isinstance(cluster, dict):
                containers.append(cluster.get("articles", []))
    for records in containers:
        if not isinstance(records, list):
            continue
        for record in records:
            if not isinstance(record, dict):
                continue
            if normalize_url(str(record.get("canonical_url") or "")) != normalize_url(old_url):
                continue
            record["canonical_url"] = repaired.get("canonical_url")
            record["canonical_url_hash"] = repaired.get("canonical_url_hash")
            if repaired.get("source"):
                record["source"] = repaired.get("source")
            if repaired.get("image_url"):
                record["image_url"] = repaired.get("image_url")
            updated += 1
    if updated and apply:
        path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return updated


def repair_google_news_urls(args: argparse.Namespace) -> RepairStats:
    root = Path(args.root).resolve()
    load_local_env(root)
    config = load_config(root / "config.yaml")
    stats = RepairStats()
    conn = db_connect()
    page_timeout = float(args.page_timeout)
    timeout = httpx.Timeout(page_timeout, connect=min(5.0, page_timeout))
    headers = {"User-Agent": USER_AGENT}
    state_path = root / args.state_path if args.state_path else None
    try:
        rows = select_candidates(conn, limit=args.limit, include_rejected=args.include_rejected)
        with httpx.Client(timeout=timeout, headers=headers, follow_redirects=True) as client:
            for row in rows:
                stats.scanned += 1
                old_url = str(row.get("canonical_url") or "")
                result = decode_google_news_url_online_result(old_url, client)
                if result.rate_limited:
                    stats.rate_limited = True
                    print(f"[{stats.scanned}/{len(rows)}] rate-limited by Google News; stopping this run", flush=True)
                    break
                if not result.decoded_url or google_news_host(result.decoded_url):
                    stats.failed += 1
                    print(f"[{stats.scanned}/{len(rows)}] decode failed: {row.get('record_id')} {result.error}", flush=True)
                    if args.sleep:
                        time.sleep(args.sleep)
                    continue
                stats.decoded += 1
                repaired = apply_decoded_google_news_url(row_to_article(row), result.decoded_url)
                enriched = enrich_article(repaired, client, config, decode_google_news=False)
                if enriched.get("canonical_url") and not google_news_host(str(enriched.get("canonical_url"))):
                    repaired = enriched
                    stats.enriched += 1
                target_hash = str(repaired.get("canonical_url_hash") or canonical_url_hash(str(repaired.get("canonical_url") or "")))
                conflict = existing_record_for_hash(conn, target_hash, str(row.get("record_id") or ""))
                if conflict and not args.mark_duplicates:
                    stats.conflicts += 1
                    print(
                        f"[{stats.scanned}/{len(rows)}] conflict skipped: {row.get('record_id')} -> {conflict.get('record_id')}",
                        flush=True,
                    )
                    if args.sleep:
                        time.sleep(args.sleep)
                    continue
                if args.apply:
                    update_article_row(
                        conn,
                        record_id=str(row.get("record_id") or ""),
                        old_url=old_url,
                        repaired=repaired,
                        mark_duplicate_of=conflict if args.mark_duplicates else None,
                        update_published_at=args.update_published_at,
                    )
                    if state_path:
                        stats.state_updated += repair_state_file(state_path, old_url, repaired, apply=True)
                    conn.commit()
                    stats.updated += 1
                else:
                    print(
                        f"[{stats.scanned}/{len(rows)}] dry-run: {row.get('record_id')} -> {repaired.get('canonical_url')}",
                        flush=True,
                    )
                    if state_path:
                        stats.state_updated += repair_state_file(state_path, old_url, repaired, apply=False)
                if args.sleep:
                    time.sleep(args.sleep)
    finally:
        conn.close()
    return stats


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Repair stored Google News URLs into source article URLs.")
    parser.add_argument("--root", default=str(PROJECT_ROOT))
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--sleep", type=float, default=1.0, help="Seconds to wait between Google News decode attempts.")
    parser.add_argument("--page-timeout", type=float, default=8.0)
    parser.add_argument("--apply", action="store_true", help="Write repaired URLs to MySQL. Without this, only prints candidates.")
    parser.add_argument("--include-rejected", action="store_true", help="Also repair rejected rows.")
    parser.add_argument("--mark-duplicates", action="store_true", help="When resolved URL already exists, mark current row duplicate.")
    parser.add_argument("--update-published-at", action="store_true", help="Overwrite published_at/sort_at with page article date when available.")
    parser.add_argument("--state-path", default="data/state.json", help="Also repair matching local state records. Empty string disables.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        stats = repair_google_news_urls(args)
    except Exception as exc:
        print(f"google news repair failed: {exc}", file=sys.stderr)
        return 1
    print(
        "google news repair finished: "
        f"scanned={stats.scanned}, decoded={stats.decoded}, enriched={stats.enriched}, "
        f"updated={stats.updated}, state_updated={stats.state_updated}, conflicts={stats.conflicts}, "
        f"failed={stats.failed}, rate_limited={int(stats.rate_limited)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
