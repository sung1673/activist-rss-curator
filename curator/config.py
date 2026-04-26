from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any

import yaml


DEFAULT_CONFIG: dict[str, Any] = {
    "feed_url": "",
    "feeds": [],
    "public_feed_url": "",
    "timezone": "Asia/Seoul",
    "fetch": {
        "max_entries_per_feed": 0,
        "enrich_pages": True,
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
        feeds = []
        for index, raw_url in enumerate(feeds_value.replace("\n", ",").split(","), start=1):
            url = raw_url.strip()
            if url:
                feeds.append({"name": f"env-feed-{index}", "category": "env", "url": url})
        if feeds:
            config["feeds"] = feeds
            config["feed_url"] = feeds[0]["url"]
    elif os.environ.get("CURATOR_FEED_URL"):
        config["feed_url"] = os.environ["CURATOR_FEED_URL"]
        config["feeds"] = [{"name": "env-feed", "category": "env", "url": os.environ["CURATOR_FEED_URL"]}]
    return config
