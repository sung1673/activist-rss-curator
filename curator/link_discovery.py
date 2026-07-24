from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable

from .dates import datetime_to_iso
from .fetch import article_has_unresolved_google_news, is_google_news_url
from .governance import stable_id
from .normalize import canonical_url_hash, normalize_title_parts, normalize_url
from .remote_api import post_remote_action, remote_api_configured
from .remote_state import fetch_runtime_resource


LINK_DISCOVERY_LINEAGE_VERSION = 1
RESOLVED_LINK_LOOKBACK_DAYS = 7
RESOLVED_LINK_MAX_RECORDS = 1000


def runtime_utc_timestamp(value: object) -> str | None:
    """Restore timezone information stripped by MySQL DATETIME transport."""

    if not value:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return datetime_to_iso(parsed)


def link_discovery_record(article: dict[str, object], now: datetime) -> dict[str, object]:
    url = str(article.get("canonical_url") or article.get("link") or "").strip()
    return {
        "discovery_id": stable_id("link", url, length=40),
        "discovered_url": url,
        "source": str(article.get("source") or article.get("feed_name") or "")[:191],
        "title": str(article.get("title") or article.get("clean_title") or "")[:700],
        "summary": str(article.get("summary") or "")[:4000],
        "feed_name": str(article.get("feed_name") or "")[:191],
        "feed_category": str(article.get("feed_category") or "")[:64],
        "source_kind": str(article.get("source_kind") or "")[:40],
        "source_right_id": str(article.get("source_right_id") or "")[:64] or None,
        "published_at": (
            article.get("article_published_at")
            or article.get("feed_published_at")
            or article.get("feed_updated_at")
        ),
        "lineage_version": LINK_DISCOVERY_LINEAGE_VERSION,
        "discovered_at": str(article.get("seen_at") or datetime_to_iso(now)),
        "status": "discovered",
    }


def resolved_link_article(row: dict[str, object]) -> dict[str, object] | None:
    """Materialize a resolved discovery without dropping its source lineage."""

    if str(row.get("status") or "") != "resolved":
        return None
    try:
        lineage_version = int(str(row.get("lineage_version") or "0"))
    except ValueError:
        return None
    if lineage_version != LINK_DISCOVERY_LINEAGE_VERSION:
        # Rows created before lineage persistence are intentionally ignored.
        # A current collector run will re-enqueue the URL and upgrade the row.
        return None
    resolved_url = normalize_url(str(row.get("resolved_url") or ""))
    if (
        not resolved_url.startswith(("http://", "https://"))
        or is_google_news_url(resolved_url)
    ):
        return None
    title = str(row.get("title") or "").strip()
    if not title:
        return None
    title_parts = normalize_title_parts(title)
    published_at = next(
        (
            timestamp
            for timestamp in (
                runtime_utc_timestamp(row.get("published_at")),
                runtime_utc_timestamp(row.get("discovered_at")),
                runtime_utc_timestamp(row.get("resolved_at")),
            )
            if timestamp
        ),
        None,
    )
    resolved_at = runtime_utc_timestamp(row.get("resolved_at"))
    updated_at = runtime_utc_timestamp(row.get("updated_at"))
    discovered_at = runtime_utc_timestamp(row.get("discovered_at"))
    source_right_id = str(row.get("source_right_id") or "").strip()
    return {
        "title": title,
        "clean_title": title_parts["clean_title"],
        "normalized_title": title_parts["normalized_title"],
        "prefixes": title_parts["prefixes"],
        "source": str(row.get("source") or ""),
        "link": resolved_url,
        "canonical_url": resolved_url,
        "canonical_url_hash": canonical_url_hash(resolved_url),
        "title_hash": title_parts["title_hash"],
        "summary": str(row.get("summary") or ""),
        "image_url": None,
        "image_candidates": [],
        "feed_published_at": published_at,
        "feed_updated_at": resolved_at or updated_at,
        "article_published_at": published_at,
        "seen_at": discovered_at or resolved_at,
        "feed_name": str(row.get("feed_name") or "resolved-link-discovery"),
        "feed_category": str(row.get("feed_category") or "media"),
        "feed_scope": "korean_governance",
        "source_kind": str(row.get("source_kind") or "google_discovery"),
        "source_right_id": source_right_id or None,
        "google_news_url": str(row.get("discovered_url") or ""),
        "original_resolution_status": "resolved_queue",
        "original_resolution_score": 100,
        "link_discovery_id": str(row.get("discovery_id") or ""),
    }


def resolved_link_articles(
    config: dict[str, object],
    now: datetime,
) -> list[dict[str, object]]:
    """Read recently resolved URLs back into the media curation path."""

    if not remote_api_configured():
        return []
    try:
        rows = fetch_runtime_resource(
            "link_discoveries",
            since=now - timedelta(days=RESOLVED_LINK_LOOKBACK_DAYS),
            max_records=RESOLVED_LINK_MAX_RECORDS,
        )
    except RuntimeError as exc:
        # The PHP expansion can safely be deployed before or after this client.
        # Only an explicitly old capability is tolerated; all other failures
        # remain fatal so a database or authentication outage is never hidden.
        if "invalid_runtime_resource" in str(exc):
            return []
        raise
    articles: list[dict[str, object]] = []
    for row in rows:
        article = resolved_link_article(row)
        if article is not None:
            articles.append(article)
    return articles

def partition_link_discoveries(
    articles: Iterable[dict[str, object]],
    now: datetime,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    ready: list[dict[str, object]] = []
    discoveries: list[dict[str, object]] = []
    for article in articles:
        if article_has_unresolved_google_news(article):
            discoveries.append(link_discovery_record(article, now))
        else:
            ready.append(article)
    return ready, discoveries


def enqueue_link_discoveries(
    discoveries: list[dict[str, object]],
    state: dict[str, object],
    config: dict[str, object],
) -> dict[str, int]:
    if not discoveries:
        return {"link_discoveries": 0, "link_discoveries_enqueued": 0, "link_discoveries_failed": 0}
    # Local state is a bounded compatibility/debug view, never the production
    # source of truth.
    local = state.get("link_discovery_queue")
    if not isinstance(local, list):
        local = []
        state["link_discovery_queue"] = local
    by_id = {
        str(item.get("discovery_id") or ""): item
        for item in local
        if isinstance(item, dict) and item.get("discovery_id")
    }
    for discovery in discoveries:
        by_id[str(discovery["discovery_id"])] = discovery
    state["link_discovery_queue"] = list(by_id.values())[-500:]
    if not remote_api_configured():
        return {
            "link_discoveries": len(discoveries),
            "link_discoveries_enqueued": 0,
            "link_discoveries_failed": 0,
        }
    try:
        response = post_remote_action("enqueue_link_discoveries", {"discoveries": discoveries})
    except Exception:
        response = {"ok": False}
    accepted = int(response.get("accepted") or 0) if response.get("ok") else 0
    rejected = int(response.get("rejected") or 0) if response.get("ok") else 0
    fully_acknowledged = (
        bool(response.get("ok"))
        and accepted == len(discoveries)
        and rejected == 0
    )
    return {
        "link_discoveries": len(discoveries),
        "link_discoveries_enqueued": accepted,
        "link_discoveries_failed": (
            0 if fully_acknowledged else max(1, len(discoveries) - accepted)
        ),
    }
