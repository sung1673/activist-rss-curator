from __future__ import annotations

from datetime import datetime
from typing import Iterable

from .dates import datetime_to_iso
from .fetch import article_has_unresolved_google_news
from .governance import stable_id
from .remote_api import post_remote_action, remote_api_configured


def link_discovery_record(article: dict[str, object], now: datetime) -> dict[str, object]:
    url = str(article.get("canonical_url") or article.get("link") or "").strip()
    return {
        "discovery_id": stable_id("link", url, length=40),
        "discovered_url": url,
        "source": str(article.get("source") or article.get("feed_name") or "")[:191],
        "title": str(article.get("title") or article.get("clean_title") or "")[:700],
        "discovered_at": str(article.get("seen_at") or datetime_to_iso(now)),
        "status": "discovered",
    }

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
    return {
        "link_discoveries": len(discoveries),
        "link_discoveries_enqueued": int(response.get("accepted") or 0) if response.get("ok") else 0,
        "link_discoveries_failed": 0 if response.get("ok") else 1,
    }
