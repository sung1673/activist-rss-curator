from __future__ import annotations

import argparse
import json
from copy import deepcopy
from datetime import timedelta
from pathlib import Path

from .cluster import cluster_articles
from .config import configured_feeds, load_config
from .dates import choose_publication_datetime, datetime_to_iso, is_too_old, now_in_timezone
from .dedupe import dedupe_articles
from .fetch import fetch_google_alerts_articles
from .main import PROJECT_ROOT, prepare_article
from .rss_writer import item_description, item_link, item_title, write_feed
from .state import default_state, load_state


def force_pending_ready(state: dict[str, object], now, config: dict[str, object]) -> None:
    minutes = int(config.get("cluster", {}).get("buffer_minutes_default", 45))  # type: ignore[union-attr]
    ready_at = datetime_to_iso(now - timedelta(minutes=minutes + 1))
    for cluster in state.get("pending_clusters", []):
        cluster["last_article_seen_at"] = ready_at


def build_preview(
    root: Path | None = None,
    *,
    ignore_state: bool = False,
    force_publish: bool = True,
    output_path: Path | None = None,
) -> dict[str, object]:
    project_root = root or PROJECT_ROOT
    config = load_config(project_root / "config.yaml")
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    now = now_in_timezone(timezone_name)
    base_state = default_state() if ignore_state else load_state(project_root / "data" / "state.json")
    state = deepcopy(base_state)

    fetched_articles = fetch_google_alerts_articles(config)
    publish_levels = set(config.get("publish", {}).get("publish_levels", ["high", "medium"]))  # type: ignore[union-attr]
    max_age_days = int(config.get("date_filter", {}).get("max_article_age_days", 7))  # type: ignore[union-attr]

    candidates: list[dict[str, object]] = []
    rejected: list[dict[str, object]] = []
    for raw_article in fetched_articles:
        article = prepare_article(raw_article, config)
        published_at, _ = choose_publication_datetime(
            article.get("article_published_at"),
            article.get("feed_published_at"),
            article.get("feed_updated_at"),
            timezone_name,
        )
        reject_reason = None
        if is_too_old(published_at, now, max_age_days):
            reject_reason = "old_article"
        elif article.get("relevance_level") not in publish_levels:
            reject_reason = "low_relevance"

        if reject_reason:
            rejected_article = dict(article)
            rejected_article["reject_reason"] = reject_reason
            rejected.append(rejected_article)
        else:
            candidates.append(article)

    unique_articles, duplicates = dedupe_articles(candidates, state, config)
    cluster_articles(unique_articles, state, config, now)
    if force_publish:
        force_pending_ready(state, now, config)
        cluster_articles([], state, config, now)

    preview_feed_path = output_path or Path("/tmp/curated-preview-feed.xml")
    write_feed(preview_feed_path, list(state.get("published_clusters", [])), config, now)
    published = list(state.get("published_clusters", []))
    samples = []
    for cluster in published[-5:]:
        samples.append(
            {
                "title": item_title(cluster),
                "relevance_level": cluster.get("relevance_level"),
                "article_count": cluster.get("article_count"),
                "guid": cluster.get("guid"),
                "link": item_link(cluster, config),
                "description": item_description(cluster, config),
            }
        )

    return {
        "feeds": configured_feeds(config),
        "output_path": str(preview_feed_path),
        "fetched": len(fetched_articles),
        "candidates": len(candidates),
        "unique": len(unique_articles),
        "duplicates": len(duplicates),
        "rejected": len(rejected),
        "pending": len(state.get("pending_clusters", [])),
        "published_total": len(published),
        "samples": samples,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Preview curated RSS output without mutating state.")
    parser.add_argument("--ignore-state", action="store_true", help="Ignore data/state.json while previewing.")
    parser.add_argument("--no-force-publish", action="store_true", help="Keep buffer window behavior in preview.")
    parser.add_argument("--output", default="/tmp/curated-preview-feed.xml", help="Preview feed output path.")
    args = parser.parse_args()
    summary = build_preview(
        ignore_state=args.ignore_state,
        force_publish=not args.no_force_publish,
        output_path=Path(args.output),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
