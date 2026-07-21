from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml


DEFAULT_CONFIG: dict[str, Any] = {
    "feed_url": "",
    "feeds": [],
    # Legacy configurations remain permissive. Production can opt into the
    # fail-closed scope allowlist used by config.yaml.
    "media_feed_policy": {
        "enforce": False,
        "default_scope": "unclassified",
        "allowed_scopes": [],
        "category_scopes": {},
        "feed_scopes": {},
    },
    "public_feed_url": "",
    "timezone": "Asia/Seoul",
    "fetch": {
        "max_entries_per_feed": 5,
        "enrich_pages": True,
        "max_enrich_articles": 120,
        "feed_timeout_seconds": 20,
        "feed_fetch_workers": 8,
        "page_timeout_seconds": 6,
        "enrich_workers": 6,
        # Google News is a discovery source. URL resolution runs in a separate
        # worker so it cannot stall official/media ingestion.
        "google_news_decode_limit": 0,
        "google_news_decoded_enrich_limit": 80,
        "google_news_decode_sleep_seconds": 0.0,
        "google_news_decode_stop_on_rate_limit": True,
        "google_news_title_fallback_enabled": True,
        "google_news_title_match_threshold": 86,
        "google_news_title_match_window_days": 7,
        "google_news_title_match_min_overlap": 2,
        "google_news_block_unresolved": False,
        "state_google_news_decode_limit": 0,
    },
    "source_registry": {
        "enabled": True,
        "sources": [
            {"name": "etnews", "domains": ["etnews.com"], "aliases": ["전자신문", "Electronic Times"]},
            {"name": "etoday", "domains": ["etoday.co.kr"], "aliases": ["이투데이"]},
            {"name": "newspim", "domains": ["newspim.com"], "aliases": ["뉴스핌"]},
            {"name": "betanews", "domains": ["betanews.net"], "aliases": ["베타뉴스"]},
            {"name": "todayeconomic", "domains": ["todayeconomic.com"], "aliases": ["투데이경제"]},
            {"name": "mk", "domains": ["mk.co.kr"], "aliases": ["매일경제", "매경"]},
            {"name": "hankyung", "domains": ["hankyung.com"], "aliases": ["한국경제"]},
            {"name": "edaily", "domains": ["edaily.co.kr"], "aliases": ["이데일리"]},
            {"name": "dt", "domains": ["dt.co.kr"], "aliases": ["디지털타임스"]},
            {"name": "newstomato", "domains": ["newstomato.com"], "aliases": ["뉴스토마토"]},
            {"name": "ajunews", "domains": ["ajunews.com"], "aliases": ["아주경제"]},
            {"name": "yna", "domains": ["yna.co.kr"], "aliases": ["연합뉴스"]},
            {"name": "sedaily", "domains": ["sedaily.com"], "aliases": ["서울경제"]},
            {"name": "mt", "domains": ["mt.co.kr"], "aliases": ["머니투데이"]},
            {"name": "fnnews", "domains": ["fnnews.com"], "aliases": ["파이낸셜뉴스"]},
            {"name": "heraldcorp", "domains": ["heraldcorp.com"], "aliases": ["헤럴드경제"]},
            {"name": "viva100", "domains": ["viva100.com"], "aliases": ["브릿지경제"]},
            {"name": "dailian", "domains": ["dailian.co.kr"], "aliases": ["데일리안"]},
            {"name": "thebell", "domains": ["thebell.co.kr"], "aliases": ["더벨"]},
        ],
    },
    "source_rights": {
        "enforce": True,
        "records": [
            {
                "source_right_id": "telegram:activistkorea",
                "source_category": "authorized_telegram",
                "source_identity": "activistkorea",
                "scope": "collection,ai,event-context,redistribution",
                "evidence_ref": "",
                "valid_from": "2021-01-01",
                "expires_at": None,
                "revoked_at": None,
                "allow_ai": True,
                "allow_redistribution": True,
                "status": "pending",
            }
        ],
    },
    "official_ingest": {
        "dart_enabled": True,
        "kind_enabled": True,
        "lookback_days": 2,
        "page_count": 100,
        "max_pages": 100,
        "backfill_start": "2021-01-01",
    },
    "display": {
        "exclude_link_domains": ["msn.com"],
    },
    "date_filter": {
        "max_article_age_days": 7,
        "allow_unknown_date": True,
        "exclude_before_previous_day": True,
    },
    "cluster": {
        "buffer_minutes_default": 45,
        "buffer_minutes_high": 20,
        "max_pending_hours": 3,
        "cluster_window_hours": 48,
        "theme_group_window_hours": 168,
        "max_links_per_item": 7,
        "max_description_chars": 3500,
    },
    "dedupe": {
        "title_duplicate_threshold": 92,
        "title_cluster_threshold": 80,
        "summary_cluster_threshold": 85,
        "seen_history_days": 60,
        "duplicate_mention_days": 30,
    },
    "state": {
        "retention_days": 60,
        "max_articles": 5000,
        "max_rejected_articles": 1000,
        "max_published_clusters": 500,
        "max_telegram_records": 1000,
        "max_digest_records": 400,
    },
    "priority": {
        "enabled": True,
        "overrides_path": "data/priority_overrides.yaml",
        "thresholds": {
            "top": 80,
            "watch": 55,
            "normal": 25,
        },
    },
    "story_rules": {
        "path": "data/story_rules.yaml",
    },
    "story_review": {
        "enabled": True,
        "min_score": 72,
        "max_candidates": 12,
        "benchmark_max_missing": 12,
    },
    "archive": {
        "enabled": True,
        "path": "data/archive",
        "retention_days": 365,
    },
    "publish": {
        "max_items_in_feed": 50,
        "publish_levels": ["high", "medium"],
    },
    "telegram": {
        "enabled": True,
        "chat_id": "@o2fjwoei",
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
        "single_article_web_page_preview": True,
        "single_article_link_title_chars": 54,
        "multi_article_link_title_chars": 84,
        "show_article_groups": False,
        "batch_digest_enabled": True,
        "batch_digest_min_clusters": 2,
        "hourly_digest_window_hours": 0.5,
        "overnight_half_windows": [
            {"send_hour": 3, "start_hour": 1, "start_minute": 0},
            {"send_hour": 6, "start_hour": 3, "start_minute": 30},
        ],
        "skip_hours": [1, 2, 4, 5, 7],
        "max_duplicate_mentions": 3,
        "max_articles_per_message": 7,
        "max_message_chars": 3900,
        "send_old_on_first_run": False,
        "timeout_seconds": 20,
    },
    "telegram_sources": {
        "enabled": True,
        "channels": [
            {
                "handle": "activistkorea",
                "title": "한국기업거버넌스포럼",
                "source": "manual",
                "quality_score": 90,
                "enabled": True,
                "joined": False,
                "source_type": "public_channel",
                "is_public_channel": True,
            }
        ],
        "backfill_limit": 100,
        "incremental_limit": 200,
        "history_backfill_days": 180,
        "history_backfill_limit_per_channel": 3000,
        "history_backfill_channel_limit": 0,
        "backfill_channel_timeout_seconds": 60,
        "backfill_channel_workers": 1,
        "weak_match_min_overlap": 3,
        "weak_match_min_strong_overlap": 2,
        "weak_match_window_hours": 96,
        "weak_match_limit_per_message": 3,
        "signal_window_hours": 72,
        "signal_min_messages": 3,
        "signal_min_channels": 2,
        "signal_limit": 40,
        "signal_max_messages_per_signal": 5,
        "candidate_source_enabled": True,
        "candidate_source_handles": ["activistkorea"],
        "candidate_window_hours": 168,
        "candidate_limit_per_run": 50,
        "max_remote_messages": 500,
        "remote_batch_size": 300,
        "remote_channel_batch_size": 5,
        "discover_enabled": False,
        "recommendation_limit": 20,
        "auto_join_enabled": False,
        "auto_join_daily_limit": 0,
        "auto_join_delay_min_seconds": 3,
        "auto_join_delay_max_seconds": 11,
    },
    "ai": {
        "enabled": True,
        "provider": "github_models",
        "endpoint": "https://models.github.ai/inference/chat/completions",
        "timeout_seconds": 25,
        "daily_digest_enabled": False,
        "daily_digest_model": "openai/gpt-4.1",
        "daily_digest_max_tokens": 220,
        "daily_report_enabled": True,
        "daily_report_model": "openai/gpt-4.1",
        "daily_report_max_tokens": 900,
        "hourly_digest_enabled": True,
        "hourly_digest_model": "openai/gpt-4.1",
        "hourly_digest_max_tokens": 180,
        "story_judge_enabled": True,
        "story_judge_model": "openai/gpt-4.1",
        "story_judge_max_tokens": 90,
        "story_judge_max_calls_per_run": 8,
        "story_judge_confidence_threshold": 0.75,
        "story_judge_auto_accept_title_score": 88,
    },
    "digest": {
        "enabled": False,
        "send_hour": 6,
        "send_minute": 5,
        "send_window_minutes": 120,
        "window_hours": 24,
        "max_clusters": 0,
        "max_articles_per_cluster": 0,
        "max_links_total": 0,
        "max_links_per_section": 0,
        "max_links_per_group": 0,
        "max_duplicate_links": 0,
        "link_title_max_chars": 54,
        "summary_bullets": 3,
        "summary_bullet_max_chars": 48,
        "max_message_chars": 3900,
    },
    "report": {
        "image_enrich_limit": 120,
        "image_timeout_seconds": 4,
    },
}


def config_bool(value: object, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off", ""}:
            return False
    return bool(value)


def media_feed_scope(feed: dict[str, Any], config: dict[str, Any]) -> str:
    explicit_scope = str(feed.get("scope") or "").strip()
    if explicit_scope:
        return explicit_scope

    policy = config.get("media_feed_policy", {})
    if not isinstance(policy, dict):
        return ""

    name = str(feed.get("name") or "").strip()
    feed_scopes = policy.get("feed_scopes", {})
    if isinstance(feed_scopes, dict):
        named_scope = str(feed_scopes.get(name) or "").strip()
        if named_scope:
            return named_scope

    category = str(feed.get("category") or "").strip()
    category_scopes = policy.get("category_scopes", {})
    if isinstance(category_scopes, dict):
        category_scope = str(category_scopes.get(category) or "").strip()
        if category_scope:
            return category_scope

    if config_bool(policy.get("enforce"), default=False):
        return str(policy.get("default_scope") or "").strip()
    return ""


def media_feed_is_enabled(feed: dict[str, Any], config: dict[str, Any]) -> bool:
    if not config_bool(feed.get("enabled"), default=True):
        return False

    policy = config.get("media_feed_policy", {})
    if not isinstance(policy, dict) or not config_bool(policy.get("enforce"), default=False):
        return True

    allowed_values = policy.get("allowed_scopes", [])
    if not isinstance(allowed_values, list):
        return False
    allowed_scopes = {str(value).strip() for value in allowed_values if str(value).strip()}
    if not allowed_scopes:
        return False
    return media_feed_scope(feed, config) in allowed_scopes


def configured_feeds(config: dict[str, Any]) -> list[dict[str, str]]:
    feeds = config.get("feeds")
    normalized: list[dict[str, str]] = []
    if isinstance(feeds, list) and feeds:
        for index, feed in enumerate(feeds, start=1):
            if isinstance(feed, str):
                feed_record: dict[str, Any] = {"name": f"feed-{index}", "url": feed, "category": ""}
            elif isinstance(feed, dict) and feed.get("url"):
                feed_record = dict(feed)
                feed_record["name"] = str(feed.get("name") or f"feed-{index}")
                feed_record["url"] = str(feed["url"])
                feed_record["category"] = str(feed.get("category") or "")
            else:
                continue
            if not media_feed_is_enabled(feed_record, config):
                continue
            normalized_feed = {
                "name": str(feed_record["name"]),
                "url": str(feed_record["url"]),
                "category": str(feed_record.get("category") or ""),
            }
            scope = media_feed_scope(feed_record, config)
            if scope:
                normalized_feed["scope"] = scope
            normalized.append(normalized_feed)
    elif config.get("feed_url"):
        feed_record = {
            "name": str(config.get("feed_name") or "google-alert"),
            "url": str(config["feed_url"]),
            "category": str(config.get("feed_category") or ""),
            "scope": str(config.get("feed_scope") or ""),
        }
        if media_feed_is_enabled(feed_record, config):
            normalized_feed = {
                "name": str(feed_record["name"]),
                "url": str(feed_record["url"]),
                "category": str(feed_record["category"]),
            }
            scope = media_feed_scope(feed_record, config)
            if scope:
                normalized_feed["scope"] = scope
            normalized.append(normalized_feed)
    return normalized


def excluded_link_domains(config: dict[str, Any]) -> set[str]:
    display_config = config.get("display", {})
    domains = display_config.get("exclude_link_domains", ["msn.com"]) if isinstance(display_config, dict) else []
    return {str(domain).lower().removeprefix("www.") for domain in domains}


def url_domain_is_excluded(url: object, config: dict[str, Any]) -> bool:
    hostname = (urlsplit(str(url or "")).hostname or "").lower().removeprefix("www.")
    if not hostname:
        return False
    return any(hostname == domain or hostname.endswith(f".{domain}") for domain in excluded_link_domains(config))


def article_domain_is_excluded(article: dict[str, object], config: dict[str, Any]) -> bool:
    return url_domain_is_excluded(article.get("canonical_url") or article.get("link"), config)


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(path: str | Path = "config.yaml") -> dict[str, Any]:
    config_path = Path(path)
    if not config_path.exists():
        return apply_env_overrides(deepcopy(DEFAULT_CONFIG))

    with config_path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}

    if not isinstance(loaded, dict):
        raise ValueError(f"Config file must contain a mapping: {config_path}")
    return apply_env_overrides(deep_merge(DEFAULT_CONFIG, loaded))


def apply_env_overrides(config: dict[str, Any]) -> dict[str, Any]:
    import os

    feeds_value = os.environ.get("CURATOR_FEEDS")
    if feeds_value:
        existing_feeds = config.get("feeds") if isinstance(config.get("feeds"), list) else []
        feeds: list[dict[str, Any]] = []
        try:
            structured_feeds = json.loads(feeds_value)
        except json.JSONDecodeError:
            structured_feeds = None
        if isinstance(structured_feeds, list):
            for index, feed in enumerate(structured_feeds, start=1):
                if not isinstance(feed, dict) or not str(feed.get("url") or "").strip():
                    continue
                feeds.append(
                    {
                        "name": str(feed.get("name") or f"env-feed-{index}"),
                        "category": str(feed.get("category") or "env"),
                        "url": str(feed["url"]).strip(),
                        "scope": str(feed.get("scope") or ""),
                        "enabled": feed.get("enabled", True),
                    }
                )
        else:
            for index, raw_url in enumerate(feeds_value.replace("\n", ",").split(","), start=1):
                url = raw_url.strip()
                if url:
                    feeds.append({"name": f"env-feed-{index}", "category": "env", "url": url})
        if feeds:
            config["feeds"] = feeds + list(existing_feeds)
            config["feed_url"] = feeds[0]["url"]
    elif os.environ.get("CURATOR_FEED_URL"):
        config["feed_url"] = os.environ["CURATOR_FEED_URL"]
        config["feeds"] = [{"name": "env-feed", "category": "env", "url": os.environ["CURATOR_FEED_URL"]}]
    return config
