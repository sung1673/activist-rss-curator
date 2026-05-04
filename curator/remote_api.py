from __future__ import annotations

import base64
import gzip
import hashlib
import hmac
import json
import os
import uuid
from datetime import datetime, timedelta
from typing import Any

import httpx

from .archive import compact_archive_record
from .dates import datetime_to_iso
from .normalize import stable_hash
from .priority import article_hash_key


MAX_SNAPSHOT_BYTES = 1_750_000
DEFAULT_MAX_ARTICLES = 900
DEFAULT_MAX_STORIES = 500

ARTICLE_PUBLIC_KEYS = (
    "record_id",
    "canonical_url_hash",
    "title_hash",
    "canonical_url",
    "title",
    "normalized_title",
    "summary",
    "source",
    "feed_name",
    "feed_category",
    "image_url",
    "published_at",
    "seen_at",
    "status",
    "reason",
    "relevance_level",
    "priority_score",
    "priority_level",
    "story_key",
    "updated_at",
)
RAW_KIND_DECISION_TRACE = "decision_trace"


def remote_api_url() -> str:
    return os.environ.get("ACTIVIST_API_URL", "").strip()


def remote_api_secret() -> str:
    return os.environ.get("ACTIVIST_API_SECRET", "").strip()


def remote_api_configured() -> bool:
    return bool(remote_api_url() and remote_api_secret())


def signed_headers(body: bytes, secret: str, *, timestamp: int | None = None, nonce: str | None = None) -> dict[str, str]:
    timestamp_text = str(timestamp if timestamp is not None else int(datetime.now().timestamp()))
    nonce_text = nonce or uuid.uuid4().hex
    signing_base = timestamp_text.encode("ascii") + b"\n" + nonce_text.encode("ascii") + b"\n" + body
    signature = hmac.new(secret.encode("utf-8"), signing_base, hashlib.sha256).hexdigest()
    return {
        "Content-Type": "application/json; charset=utf-8",
        "X-Activist-Timestamp": timestamp_text,
        "X-Activist-Nonce": nonce_text,
        "X-Activist-Signature": f"sha256={signature}",
    }


def post_remote_action(action: str, payload: dict[str, Any], *, timeout: float = 20.0) -> dict[str, Any]:
    if not remote_api_configured():
        return {"ok": False, "skipped": True, "reason": "not_configured"}
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    headers = signed_headers(body, remote_api_secret())
    url = remote_api_url()
    separator = "&" if "?" in url else "?"
    with httpx.Client(timeout=timeout) as client:
        response = client.post(f"{url}{separator}action={action}", content=body, headers=headers)
    try:
        data = response.json()
    except ValueError:
        data = {"ok": False, "error": "invalid_json_response"}
    data.setdefault("status_code", response.status_code)
    if response.status_code >= 400:
        data["ok"] = False
    return data


def record_limit_from_env(name: str, default: int) -> int:
    raw_value = os.environ.get(name, "")
    try:
        return max(1, int(raw_value))
    except ValueError:
        return default


def article_archive_record(article: dict[str, object], config: dict[str, object], now: datetime, *, status: str | None = None) -> dict[str, object]:
    record = dict(article)
    if status and not record.get("status"):
        record["status"] = status
    if not record.get("seen_at"):
        record["seen_at"] = datetime_to_iso(now)
    return compact_archive_record(record, config, now)


def compact_article_payload(record: dict[str, object]) -> dict[str, object]:
    """Return the hot-table article DTO, without raw/debug payload fields."""
    return {key: record[key] for key in ARTICLE_PUBLIC_KEYS if record.get(key) not in (None, "", [])}


def raw_retention_until(record: dict[str, object], now: datetime) -> str:
    status = str(record.get("status") or "").lower()
    reason = str(record.get("reason") or "").lower()
    priority = str(record.get("priority_level") or "").lower()
    days = 90
    if priority in {"top", "watch"} or status in {"published", "clustered"}:
        days = 365
    elif status == "duplicate":
        days = 90
    elif status == "rejected":
        if reason in {"before_previous_day", "old_article", "low_relevance", "excluded_domain"}:
            days = 14
        else:
            days = 60
    return datetime_to_iso(now + timedelta(days=days))


def raw_record_payload(record: dict[str, object], now: datetime) -> dict[str, object]:
    body = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    compressed = gzip.compress(body, compresslevel=6)
    payload_hash = hashlib.sha256(body).hexdigest()
    record_id = str(record.get("record_id") or "")
    raw_id = f"raw:{stable_hash(record_id + ':' + RAW_KIND_DECISION_TRACE + ':' + payload_hash, length=32)}"
    return {
        "raw_id": raw_id,
        "record_id": record_id,
        "raw_kind": RAW_KIND_DECISION_TRACE,
        "payload_hash": payload_hash,
        "compression": "gzip",
        "payload_base64": base64.b64encode(compressed).decode("ascii"),
        "schema_version": int(record.get("archive_version") or 1),
        "retained_until": raw_retention_until(record, now),
    }


def cluster_story_key(cluster: dict[str, object]) -> str:
    for key in ("story_key", "cluster_key", "guid"):
        value = str(cluster.get(key) or "").strip()
        if value:
            return value[:120]
    title = str(cluster.get("representative_title") or cluster.get("title") or "")
    return f"story:{stable_hash(title, length=20)}"


def cluster_story_record(
    cluster: dict[str, object],
    config: dict[str, object],
    now: datetime,
    article_id_lookup: dict[str, str],
) -> tuple[dict[str, object], list[dict[str, object]]]:
    articles = [article for article in list(cluster.get("articles") or []) if isinstance(article, dict)]
    article_ids: list[str] = []
    extra_articles: list[dict[str, object]] = []
    for article in articles:
        key = article_hash_key(article)
        article_id = article_id_lookup.get(key)
        if not article_id:
            archived = article_archive_record(article, config, now, status=str(cluster.get("status") or "clustered"))
            article_id = str(archived.get("record_id") or "")
            if article_id:
                article_id_lookup[key] = article_id
                extra_articles.append(archived)
        if article_id:
            article_ids.append(article_id)

    story_key = cluster_story_key(cluster)
    if story_key.startswith("cluster:"):
        story_key = f"story:{stable_hash(story_key, length=24)}"
    if not story_key.startswith("story:"):
        story_key = f"story:{stable_hash(story_key, length=24)}"

    return (
        {
            "story_key": story_key,
            "guid": cluster.get("guid") or None,
            "representative_title": cluster.get("representative_title") or cluster.get("title") or "",
            "representative_url": cluster.get("representative_url") or "",
            "relevance_level": cluster.get("relevance_level") or "",
            "theme_group": cluster.get("theme_group") or "",
            "status": cluster.get("status") or "",
            "article_count": int(cluster.get("article_count") or len(articles) or 0),
            "priority_score": int(cluster.get("priority_score") or 0),
            "published_at": cluster.get("published_at") or None,
            "last_article_seen_at": cluster.get("last_article_seen_at") or None,
            "article_ids": article_ids,
        },
        extra_articles,
    )


def snapshot_payload(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
    run_summary: dict[str, int] | None = None,
) -> dict[str, object]:
    article_limit = record_limit_from_env("ACTIVIST_API_MAX_ARTICLES", DEFAULT_MAX_ARTICLES)
    story_limit = record_limit_from_env("ACTIVIST_API_MAX_STORIES", DEFAULT_MAX_STORIES)
    source_articles = [article for article in list(state.get("articles") or []) if isinstance(article, dict)][-article_limit:]
    article_id_lookup: dict[str, str] = {}
    articles_by_id: dict[str, dict[str, object]] = {}
    raw_records_by_id: dict[str, dict[str, object]] = {}

    for article in source_articles:
        archived = article_archive_record(article, config, now)
        record_id = str(archived.get("record_id") or "")
        if not record_id:
            continue
        articles_by_id[record_id] = compact_article_payload(archived)
        raw_records_by_id[record_id] = raw_record_payload(archived, now)
        article_id_lookup[article_hash_key(article)] = record_id

    clusters = [
        cluster
        for cluster in list(state.get("published_clusters") or []) + list(state.get("pending_clusters") or [])
        if isinstance(cluster, dict)
    ][-story_limit:]
    stories: list[dict[str, object]] = []
    for cluster in clusters:
        story, extra_articles = cluster_story_record(cluster, config, now, article_id_lookup)
        stories.append(story)
        for article in extra_articles:
            record_id = str(article.get("record_id") or "")
            if record_id:
                articles_by_id[record_id] = compact_article_payload(article)
                raw_records_by_id[record_id] = raw_record_payload(article, now)

    run_summary = run_summary or {}
    finished_at = str(state.get("last_run_at") or datetime_to_iso(now))
    run_id = f"run:{stable_hash(finished_at + json.dumps(run_summary, sort_keys=True), length=24)}"
    return {
        "run": {
            "run_id": run_id,
            "started_at": finished_at,
            "finished_at": finished_at,
            "mode": os.environ.get("GITHUB_EVENT_NAME", "local"),
            **{key: int(value) for key, value in run_summary.items() if isinstance(value, int)},
        },
        "articles": sorted(articles_by_id.values(), key=lambda item: str(item.get("seen_at") or "")),
        "raw_records": sorted(raw_records_by_id.values(), key=lambda item: str(item.get("record_id") or "")),
        "stories": stories,
    }


def shrink_snapshot_payload(payload: dict[str, object], max_bytes: int = MAX_SNAPSHOT_BYTES) -> dict[str, object]:
    current = payload
    while len(json.dumps(current, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")) > max_bytes:
        articles = list(current.get("articles") or [])
        raw_records = list(current.get("raw_records") or [])
        stories = list(current.get("stories") or [])
        if len(articles) > 100 or len(raw_records) > 100:
            trim_count = max(len(articles), len(raw_records)) // 4
            if articles:
                current["articles"] = articles[min(trim_count, len(articles)) :]
            if raw_records:
                current["raw_records"] = raw_records[min(trim_count, len(raw_records)) :]
            continue
        if len(stories) > 50:
            current["stories"] = stories[len(stories) // 4 :]
            continue
        break
    return current


def sync_state_to_remote_api(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
    run_summary: dict[str, int] | None = None,
) -> dict[str, int]:
    if not remote_api_configured():
        return {}
    try:
        payload = shrink_snapshot_payload(snapshot_payload(state, config, now, run_summary))
        response = post_remote_action("upsert_snapshot", payload)
    except Exception:
        return {"remote_api_synced": 0, "remote_api_failed": 1}
    if response.get("ok"):
        return {
            "remote_api_synced": 1,
            "remote_api_failed": 0,
            "remote_api_articles": int(response.get("articles") or 0),
            "remote_api_raw_records": int(response.get("raw_records") or 0),
            "remote_api_stories": int(response.get("stories") or 0),
        }
    return {"remote_api_synced": 0, "remote_api_failed": 1}


def report_payload(report: dict[str, object]) -> dict[str, object]:
    stats = report.get("stats") if isinstance(report.get("stats"), dict) else {}
    stories = report.get("stories") if isinstance(report.get("stories"), list) else []
    return {
        "date_id": str(report.get("date_id") or ""),
        "title": "주주·자본시장 데일리",
        "start_at": datetime_to_iso(report["start_at"]) if isinstance(report.get("start_at"), datetime) else str(report.get("start_at") or ""),
        "end_at": datetime_to_iso(report["end_at"]) if isinstance(report.get("end_at"), datetime) else str(report.get("end_at") or ""),
        "public_url": str(report.get("report_url") or ""),
        "story_count": int(stats.get("stories") or len(stories)),
        "article_count": int(stats.get("articles") or 0),
        "stats": stats,
        "review": str(report.get("review") or ""),
    }


def sync_report_to_remote_api(report: dict[str, object]) -> dict[str, int]:
    if not remote_api_configured():
        return {}
    try:
        response = post_remote_action("upsert_report", {"report": report_payload(report)})
    except Exception:
        return {"remote_report_synced": 0, "remote_report_failed": 1}
    if response.get("ok"):
        return {"remote_report_synced": 1, "remote_report_failed": 0}
    return {"remote_report_synced": 0, "remote_report_failed": 1}
