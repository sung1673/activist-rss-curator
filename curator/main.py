from __future__ import annotations

from pathlib import Path

from .cluster import cluster_articles
from .config import load_config
from .dates import choose_publication_datetime, datetime_to_iso, is_too_old, now_in_timezone
from .dedupe import dedupe_articles
from .fetch import fetch_google_alerts_articles
from .relevance import relevance_details
from .rss_writer import write_article_redirect_pages, write_cluster_pages, write_feed, write_index
from .state import compact_state, load_state, remember_article, remember_rejected, save_state


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def prepare_article(article: dict[str, object], config: dict[str, object]) -> dict[str, object]:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    prepared = dict(article)
    published_at, date_status = choose_publication_datetime(
        prepared.get("article_published_at"),
        prepared.get("feed_published_at"),
        prepared.get("feed_updated_at"),
        timezone_name,
    )
    prepared["published_at"] = datetime_to_iso(published_at)
    prepared["date_status"] = date_status

    relevance = relevance_details(
        str(prepared.get("clean_title") or prepared.get("title") or ""),
        str(prepared.get("summary") or ""),
    )
    level = str(relevance["level"])
    if date_status == "unknown" and level == "high":
        level = "medium"
    prepared["relevance_level"] = level
    prepared["relevance_keywords"] = relevance["matched_keywords"]
    prepared["low_patterns"] = relevance["low_patterns"]
    return prepared


def run(root: Path | None = None) -> dict[str, int]:
    project_root = root or PROJECT_ROOT
    config = load_config(project_root / "config.yaml")
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    now = now_in_timezone(timezone_name)
    state_path = project_root / "data" / "state.json"
    state = load_state(state_path)

    fetched_articles = fetch_google_alerts_articles(config)
    publish_levels = set(config.get("publish", {}).get("publish_levels", ["high", "medium"]))  # type: ignore[union-attr]
    max_age_days = int(config.get("date_filter", {}).get("max_article_age_days", 7))  # type: ignore[union-attr]
    allow_unknown = bool(config.get("date_filter", {}).get("allow_unknown_date", True))  # type: ignore[union-attr]

    candidates: list[dict[str, object]] = []
    rejected_count = 0
    for raw_article in fetched_articles:
        article = prepare_article(raw_article, config)
        published_at, _ = choose_publication_datetime(
            article.get("article_published_at"),
            article.get("feed_published_at"),
            article.get("feed_updated_at"),
            timezone_name,
        )
        if is_too_old(published_at, now, max_age_days):
            remember_rejected(state, article, now, "old_article")
            rejected_count += 1
            continue
        if article.get("date_status") == "unknown" and not allow_unknown:
            remember_rejected(state, article, now, "date_unknown")
            rejected_count += 1
            continue
        if article.get("relevance_level") not in publish_levels:
            remember_rejected(state, article, now, "low_relevance")
            rejected_count += 1
            continue
        candidates.append(article)

    unique_articles, duplicates = dedupe_articles(candidates, state, config)
    for duplicate in duplicates:
        remember_article(state, duplicate, "duplicate", now, str(duplicate.get("duplicate_reason") or "duplicate"))
    for article in unique_articles:
        remember_article(state, article, "accepted", now)

    published_now = cluster_articles(unique_articles, state, config, now)
    state["last_run_at"] = datetime_to_iso(now)
    compact_state(state, config, now)
    published_clusters = list(state.get("published_clusters", []))
    write_feed(project_root / "public" / "feed.xml", published_clusters, config, now)
    write_cluster_pages(project_root / "public" / "items", published_clusters, config, now)
    write_article_redirect_pages(project_root / "public" / "u", published_clusters)
    write_index(project_root / "public" / "index.html", state, config, now)
    save_state(state_path, state)

    return {
        "fetched": len(fetched_articles),
        "accepted": len(unique_articles),
        "duplicates": len(duplicates),
        "rejected": rejected_count,
        "published_now": len(published_now),
        "pending": len(state.get("pending_clusters", [])),
        "published_total": len(state.get("published_clusters", [])),
    }


def main() -> None:
    summary = run()
    print(
        "RSS curator finished: "
        + ", ".join(f"{key}={value}" for key, value in summary.items())
    )


if __name__ == "__main__":
    main()
