from __future__ import annotations

import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from .dates import datetime_to_iso, parse_datetime
from .dedupe import article_title_hash


def default_state() -> dict[str, object]:
    return {
        "seen_url_hashes": [],
        "seen_title_hashes": [],
        "articles": [],
        "pending_clusters": [],
        "published_clusters": [],
        "rejected_articles": [],
        "telegram_sent_cluster_guids": [],
        "telegram_send_records": [],
        "telegram_initialized_at": None,
        "daily_digest_sent_dates": [],
        "daily_digest_records": [],
        "telegram_digest_records": [],
        "last_run_at": None,
    }


def load_state(path: str | Path) -> dict[str, object]:
    state_path = Path(path)
    if not state_path.exists():
        return default_state()
    try:
        with state_path.open("r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except (json.JSONDecodeError, OSError):
        backup_path = state_path.with_suffix(state_path.suffix + f".bak.{datetime.now().strftime('%Y%m%d%H%M%S')}")
        try:
            shutil.copy2(state_path, backup_path)
        except OSError:
            pass
        return default_state()

    if not isinstance(loaded, dict):
        return default_state()
    state = default_state()
    state.update(loaded)
    for key in (
        "seen_url_hashes",
        "seen_title_hashes",
        "articles",
        "pending_clusters",
        "published_clusters",
        "rejected_articles",
        "telegram_sent_cluster_guids",
        "telegram_send_records",
        "daily_digest_sent_dates",
        "daily_digest_records",
        "telegram_digest_records",
    ):
        if not isinstance(state.get(key), list):
            state[key] = []
    return state


def save_state(path: str | Path, state: dict[str, object]) -> None:
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_path.with_suffix(state_path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    tmp_path.replace(state_path)


def clean_duplicate_matches(article: dict[str, object]) -> list[dict[str, object]]:
    matches: list[dict[str, object]] = []
    for match in list(article.get("duplicate_matches") or [])[:3]:
        if not isinstance(match, dict):
            continue
        matches.append(
            {
                "title": match.get("title") or "",
                "canonical_url": match.get("canonical_url") or "",
                "source": match.get("source") or None,
                "feed_name": match.get("feed_name") or None,
                "feed_category": match.get("feed_category") or None,
                "relevance_keywords": match.get("relevance_keywords") or [],
                "published_at": match.get("published_at") or None,
                "seen_at": match.get("seen_at") or None,
                "status": match.get("status") or None,
                "similarity": match.get("similarity") or None,
            }
        )
    return matches


def article_record(article: dict[str, object], status: str, now: datetime, reason: str | None = None) -> dict[str, object]:
    record = {
        "title": article.get("clean_title") or article.get("title") or "",
        "normalized_title": article.get("normalized_title") or "",
        "canonical_url": article.get("canonical_url") or article.get("link") or "",
        "canonical_url_hash": article.get("canonical_url_hash") or "",
        "title_hash": article.get("title_hash") or article_title_hash(article),
        "published_at": article.get("published_at") or article.get("feed_published_at") or None,
        "seen_at": datetime_to_iso(now),
        "status": status,
        "reason": reason,
        "relevance_level": article.get("relevance_level") or None,
        "source": article.get("source") or None,
        "image_url": article.get("image_url") or None,
        "feed_name": article.get("feed_name") or None,
        "feed_category": article.get("feed_category") or None,
        "relevance_keywords": article.get("relevance_keywords") or [],
    }
    duplicate_matches = clean_duplicate_matches(article)
    if duplicate_matches:
        record["duplicate_matches"] = duplicate_matches
    return record


def remember_article(state: dict[str, object], article: dict[str, object], status: str, now: datetime, reason: str | None = None) -> None:
    record = article_record(article, status, now, reason)
    state.setdefault("articles", [])
    state.setdefault("seen_url_hashes", [])
    state.setdefault("seen_title_hashes", [])
    state["articles"].append(record)  # type: ignore[index, union-attr]

    url_hash = str(record.get("canonical_url_hash") or "")
    title_hash = str(record.get("title_hash") or "")
    if url_hash and url_hash not in state["seen_url_hashes"]:  # type: ignore[operator]
        state["seen_url_hashes"].append(url_hash)  # type: ignore[index, union-attr]
    if title_hash and title_hash not in state["seen_title_hashes"]:  # type: ignore[operator]
        state["seen_title_hashes"].append(title_hash)  # type: ignore[index, union-attr]


def remember_rejected(state: dict[str, object], article: dict[str, object], now: datetime, reason: str) -> None:
    state.setdefault("rejected_articles", [])
    state["rejected_articles"].append(article_record(article, "rejected", now, reason))  # type: ignore[index, union-attr]
    remember_article(state, article, "rejected", now, reason)


def compact_state(state: dict[str, object], config: dict[str, object], now: datetime) -> None:
    seen_days = int(config.get("dedupe", {}).get("seen_history_days", 90))  # type: ignore[union-attr]
    state_config = config.get("state", {})
    retention_days = int(state_config.get("retention_days", seen_days)) if isinstance(state_config, dict) else seen_days
    cutoff = now - timedelta(days=retention_days)
    timezone_name = str(config.get("timezone") or "Asia/Seoul")

    articles = [
        article
        for article in state.get("articles", [])
        if (parse_datetime(str(article.get("seen_at") or ""), timezone_name) or now) >= cutoff
    ]
    max_articles = int(state_config.get("max_articles", 5000)) if isinstance(state_config, dict) else 5000
    max_rejected = int(state_config.get("max_rejected_articles", 1000)) if isinstance(state_config, dict) else 1000
    max_published = int(state_config.get("max_published_clusters", 500)) if isinstance(state_config, dict) else 500
    max_telegram_records = int(state_config.get("max_telegram_records", 1000)) if isinstance(state_config, dict) else 1000
    max_digest_records = int(state_config.get("max_digest_records", 400)) if isinstance(state_config, dict) else 400

    retained_articles = articles[-max_articles:]
    state["articles"] = retained_articles
    state["seen_url_hashes"] = sorted({str(article.get("canonical_url_hash")) for article in retained_articles if article.get("canonical_url_hash")})
    state["seen_title_hashes"] = sorted({str(article.get("title_hash")) for article in retained_articles if article.get("title_hash")})
    state["rejected_articles"] = [
        article
        for article in state.get("rejected_articles", [])
        if (parse_datetime(str(article.get("seen_at") or ""), timezone_name) or now) >= cutoff
    ][-max_rejected:]
    state["published_clusters"] = [
        cluster
        for cluster in state.get("published_clusters", [])
        if (
            parse_datetime(str(cluster.get("published_at") or ""), timezone_name)
            or parse_datetime(str(cluster.get("last_article_seen_at") or ""), timezone_name)
            or now
        )
        >= cutoff
    ][-max_published:]
    state["telegram_send_records"] = [
        record
        for record in state.get("telegram_send_records", [])
        if (parse_datetime(str(record.get("sent_at") or ""), timezone_name) or now) >= cutoff
    ][-max_telegram_records:]
    state["daily_digest_records"] = [
        record
        for record in state.get("daily_digest_records", [])
        if (parse_datetime(str(record.get("sent_at") or ""), timezone_name) or now) >= cutoff
    ][-max_digest_records:]
    state["telegram_digest_records"] = [
        record
        for record in state.get("telegram_digest_records", [])
        if (parse_datetime(str(record.get("sent_at") or ""), timezone_name) or now) >= cutoff
    ][-max_digest_records:]
    digest_ids = {
        str(record.get("digest_id"))
        for record in state.get("daily_digest_records", [])
        if isinstance(record, dict) and record.get("digest_id")
    }
    recent_digest_ids = set(digest_ids)
    for value in state.get("daily_digest_sent_dates", []):
        digest_id = str(value)
        parsed = parse_datetime(digest_id, timezone_name) or parse_datetime(f"{digest_id}T00:00:00", timezone_name)
        if parsed and parsed >= cutoff:
            recent_digest_ids.add(digest_id)
    state["daily_digest_sent_dates"] = sorted(recent_digest_ids)
    published_guids = {
        str(cluster.get("guid"))
        for cluster in state.get("published_clusters", [])
        if cluster.get("guid")
    }
    recent_sent_guids = [
        str(guid)
        for guid in state.get("telegram_sent_cluster_guids", [])
        if str(guid) in published_guids
    ]
    state["telegram_sent_cluster_guids"] = sorted(set(recent_sent_guids))
