from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml


DEFAULT_CONFIG: dict[str, Any] = {
    "feed_url": "",
    "feeds": [],
    "public_feed_url": "",
    "timezone": "Asia/Seoul",
    "fetch": {
        "max_entries_per_feed": 5,
        "enrich_pages": True,
        "max_enrich_articles": 90,
        "page_timeout_seconds": 8,
        "google_news_decode_limit": 160,
    },
    "display": {
        "exclude_link_domains": ["msn.com"],
    },
    "date_filter": {
        "max_article_age_days": 7,
        "allow_unknown_date": True,
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
        "seen_history_days": 90,
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
        "max_articles_per_message": 7,
        "max_message_chars": 3900,
        "send_old_on_first_run": False,
        "timeout_seconds": 20,
    },
    "ai": {
        "enabled": True,
        "provider": "github_models",
        "endpoint": "https://models.github.ai/inference/chat/completions",
        "timeout_seconds": 25,
        "daily_digest_enabled": True,
        "daily_digest_model": "openai/gpt-4.1",
        "daily_digest_max_tokens": 900,
    },
    "digest": {
        "enabled": True,
        "send_hour": 7,
        "send_window_minutes": 59,
        "window_hours": 24,
        "max_clusters": 30,
        "max_articles_per_cluster": 5,
        "max_message_chars": 3900,
    },
}


def configured_feeds(config: dict[str, Any]) -> list[dict[str, str]]:
    feeds = config.get("feeds")
    normalized: list[dict[str, str]] = []
    if isinstance(feeds, list) and feeds:
        for index, feed in enumerate(feeds, start=1):
            if isinstance(feed, str):
                normalized.append({"name": f"feed-{index}", "url": feed, "category": ""})
            elif isinstance(feed, dict) and feed.get("url"):
                normalized.append(
                    {
                        "name": str(feed.get("name") or f"feed-{index}"),
                        "url": str(feed["url"]),
                        "category": str(feed.get("category") or ""),
                    }
                )
    elif config.get("feed_url"):
        normalized.append(
            {
                "name": str(config.get("feed_name") or "google-alert"),
                "url": str(config["feed_url"]),
                "category": str(config.get("feed_category") or ""),
            }
        )
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
        feeds = []
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
