from __future__ import annotations

import os
from copy import deepcopy
from datetime import datetime
from html import escape
from typing import Any

from .cluster import refresh_cluster_source_lineage
from .dates import datetime_to_iso
from .rss_writer import (
    article_link,
    article_source_label,
    compact_text,
    display_article_title,
    publishable_articles,
)
from .source_rights import source_is_authorized


def telegram_config(config: dict[str, object]) -> dict[str, Any]:
    value = config.get("telegram", {})
    return value if isinstance(value, dict) else {}


def telegram_bot_token() -> str:
    return os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()


def telegram_chat_id(config: dict[str, object]) -> str:
    return os.environ.get("TELEGRAM_CHAT_ID", "").strip() or str(telegram_config(config).get("chat_id") or "").strip()


def telegram_is_configured(config: dict[str, object]) -> bool:
    # Read-only Telethon collection is configured elsewhere. Bot delivery is
    # permanently disabled for this product regardless of secrets or YAML.
    return False


def _telegram_error_response(
    *,
    error: str,
    status_code: int | None = None,
    description: object = "",
    parameters: object = None,
) -> dict[str, object]:
    retry_after = 0
    if isinstance(parameters, dict):
        try:
            retry_after = max(0, int(parameters.get("retry_after") or 0))
        except (TypeError, ValueError):
            retry_after = 0
    description_text = str(description or "")[:500]
    retryable = bool(
        status_code == 429
        or (status_code is not None and status_code >= 500)
        or error == "telegram_request_failed"
    )
    result: dict[str, object] = {
        "ok": False,
        "error": error,
        "description": description_text,
        "retryable": retryable,
    }
    if status_code is not None:
        result["status_code"] = status_code
    if retry_after:
        result["retry_after_seconds"] = retry_after
    return result


def _post_telegram_method(
    bot_token: str,
    method: str,
    payload: dict[str, object],
    *,
    timeout: float,
    client: object | None,
) -> dict[str, object]:
    """Reject every Bot API method before a client can be constructed or used."""

    return _telegram_error_response(error="telegram_outbound_disabled")


def validate_telegram_chat(
    bot_token: str,
    chat_id: str,
    config: dict[str, object],
    client: object | None = None,
) -> dict[str, object]:
    """Reject the historical getChat preflight under the web-only policy."""

    return {
        **_telegram_error_response(error="telegram_outbound_disabled"),
        "delivery_stage": "policy",
    }


def html_link(label: str, url: str) -> str:
    safe_label = escape(label)
    if not url:
        return safe_label
    return f'<a href="{escape(url, quote=True)}">{safe_label}</a>'


def cluster_guid_value(cluster: dict[str, object]) -> str:
    return str(cluster.get("guid") or "").strip()


def authorized_cluster_for_delivery(
    cluster: dict[str, object],
    config: dict[str, object],
    at: datetime | None = None,
) -> dict[str, object] | None:
    raw_articles = cluster.get("articles")
    articles = [article for article in raw_articles if isinstance(article, dict)] if isinstance(raw_articles, list) else []
    allowed = [
        article
        for article in articles
        if source_is_authorized(article, config, at, purpose="public")
    ]
    if not allowed:
        return None
    public_cluster = dict(cluster)
    public_cluster["articles"] = allowed
    public_cluster["article_count"] = len(allowed)
    if len(allowed) != len(articles):
        representative = allowed[0]
        public_cluster["representative_title"] = representative.get("clean_title") or representative.get("title") or ""
        public_cluster["representative_title_normalized"] = representative.get("normalized_title") or ""
        public_cluster["representative_url"] = representative.get("canonical_url") or representative.get("link") or ""
    refresh_cluster_source_lineage(public_cluster)
    return public_cluster


def article_group_label(article: dict[str, object]) -> str:
    raw_companies = article.get("company_candidates")
    companies = (
        [str(company).strip() for company in raw_companies if str(company).strip()]
        if isinstance(raw_companies, list)
        else []
    )
    if companies:
        return companies[0]
    return ""


def grouped_articles(articles: list[dict[str, object]]) -> list[tuple[str, list[dict[str, object]]]]:
    groups: list[tuple[str, list[dict[str, object]]]] = []
    positions: dict[str, int] = {}
    for article in articles:
        label = article_group_label(article) or "기타"
        if label not in positions:
            positions[label] = len(groups)
            groups.append((label, []))
        groups[positions[label]][1].append(article)
    return groups


def should_show_article_groups(groups: list[tuple[str, list[dict[str, object]]]]) -> bool:
    named_groups = [(label, items) for label, items in groups if label != "기타"]
    return len(named_groups) >= 2 or any(len(items) >= 2 for _label, items in named_groups)


def article_link_label(article: dict[str, object], config: dict[str, object], *, single: bool) -> str:
    settings = telegram_config(config)
    max_chars = int(
        settings.get(
            "single_article_link_title_chars" if single else "multi_article_link_title_chars",
            54 if single else 84,
        )
    )
    source = article_source_label(article)
    title = display_article_title(article, source)
    return compact_text(title, max_chars=max_chars)


def cluster_should_show_web_preview(cluster: dict[str, object], config: dict[str, object]) -> bool:
    settings = telegram_config(config)
    if not settings.get("single_article_web_page_preview", True):
        return False
    return len(publishable_articles(cluster, config)) == 1


def initialize_telegram_state(state: dict[str, object], config: dict[str, object], now: datetime) -> None:
    if not telegram_is_configured(config) or state.get("telegram_initialized_at"):
        return
    raw_sent = state.get("telegram_sent_cluster_guids")
    sent = {str(value) for value in raw_sent} if isinstance(raw_sent, list) else set()
    if not telegram_config(config).get("send_old_on_first_run", False):
        raw_clusters = state.get("published_clusters")
        if isinstance(raw_clusters, list):
            sent.update(
                cluster_guid_value(cluster)
                for cluster in raw_clusters
                if isinstance(cluster, dict) and cluster_guid_value(cluster)
            )
    state["telegram_sent_cluster_guids"] = sorted(sent)
    state["telegram_initialized_at"] = datetime_to_iso(now)


def unsent_telegram_clusters(
    state: dict[str, object],
    config: dict[str, object],
    *,
    require_sender: bool = True,
    now: datetime | None = None,
) -> list[dict[str, object]]:
    settings = telegram_config(config)
    if require_sender and not telegram_is_configured(config):
        return []
    if not require_sender and (not settings.get("enabled", True) or not telegram_chat_id(config)):
        return []
    raw_sent = state.get("telegram_sent_cluster_guids")
    sent = {str(guid) for guid in raw_sent} if isinstance(raw_sent, list) else set()
    clusters: list[dict[str, object]] = []
    raw_clusters = state.get("published_clusters")
    for cluster in raw_clusters if isinstance(raw_clusters, list) else []:
        if not isinstance(cluster, dict):
            continue
        public_cluster = authorized_cluster_for_delivery(cluster, config, now)
        if public_cluster is None:
            continue
        guid = cluster_guid_value(public_cluster)
        if not guid or guid in sent:
            continue
        if not publishable_articles(public_cluster, config):
            continue
        clusters.append(public_cluster)
    return clusters


def build_telegram_message(cluster: dict[str, object], config: dict[str, object]) -> str:
    settings = telegram_config(config)
    max_articles = int(settings.get("max_articles_per_message", 7))
    max_chars = int(settings.get("max_message_chars", 3900))
    articles = publishable_articles(cluster, config)
    count = len(articles)

    lines: list[str] = []

    shown_count = 0
    article_groups = grouped_articles(articles[:max_articles])
    show_groups = bool(settings.get("show_article_groups", False)) and should_show_article_groups(
        article_groups
    )
    stop = False
    for group_label, group_items in article_groups:
        if stop:
            break
        if show_groups and group_label:
            group_line = f"<b>{escape(group_label)}</b>"
            candidate = "\n".join(lines + [group_line])
            if shown_count > 0 and len(candidate) > max_chars:
                break
            lines.append(group_line)
        for article in group_items:
            label = article_link_label(article, config, single=count == 1)
            link_text = html_link(label, article_link(article))
            row = link_text if count == 1 else f"{shown_count + 1}. {link_text}"
            row_lines = [row]
            candidate = "\n".join(lines + row_lines)
            if shown_count > 0 and len(candidate) > max_chars:
                stop = True
                break
            lines.extend(row_lines)
            shown_count += 1
        if show_groups and group_label and shown_count < min(count, max_articles):
            lines.append("")

    while lines and lines[-1] == "":
        lines.pop()

    remaining = count - shown_count
    if remaining > 0:
        lines.append(f"외 {remaining}건")

    message = "\n".join(lines).strip()
    if len(message) <= max_chars:
        return message
    marker = "\n... 내용 일부 생략"
    return message[: max(0, max_chars - len(marker))].rstrip() + marker


def send_telegram_message(
    bot_token: str,
    chat_id: str,
    text: str,
    config: dict[str, object],
    client: object | None = None,
    disable_web_page_preview: bool | None = None,
) -> dict[str, object]:
    """Reject Telegram sends before validating, rendering, or contacting Telegram."""

    return {
        **_telegram_error_response(error="telegram_outbound_disabled"),
        "delivery_stage": "policy",
    }


def mark_telegram_sent(
    state: dict[str, object],
    cluster: dict[str, object],
    now: datetime,
    response: dict[str, object],
) -> bool:
    """Preserve historical state without recording new delivery acknowledgements."""

    return False


def ensure_telegram_delivery_outbox(state: dict[str, object]) -> list[dict[str, object]]:
    """Return a detached historical snapshot without creating or replacing a queue."""

    outbox = state.get("telegram_delivery_outbox")
    return deepcopy(outbox) if isinstance(outbox, list) else []


def enqueue_telegram_delivery(
    state: dict[str, object],
    cluster: dict[str, object],
    config: dict[str, object],
    now: datetime,
) -> dict[str, object] | None:
    # Historical local outbox rows remain readable for audit purposes, but the
    # web-only product policy forbids creating any new outbound delivery.
    return None


def enqueue_unsent_telegram_clusters(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
) -> int:
    return 0


def enqueue_unsent_telegram_clusters_to_remote(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
) -> dict[str, int]:
    """Refuse Telegram enqueue under the immutable web-only policy."""

    return {
        "telegram_outbox_enqueued": 0,
        "telegram_outbox_rejected": 0,
        "telegram_outbox_enqueue_failed": 0,
        "telegram_outbox_enqueue_skipped": 1,
        "telegram_outbox_rights_blocked": 0,
    }


def process_telegram_delivery_outbox(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
    *,
    client: object | None = None,
    max_items: int = 0,
) -> dict[str, int]:
    """Preserve historical local outbox rows without processing or mutating them."""

    return {
        "telegram_outbox_processed": 0,
        "telegram_sent": 0,
        "telegram_failed": 0,
        "telegram_retried": 0,
        "telegram_dead_letter": 0,
        "telegram_outbox_skipped": 1,
    }


def publish_unsent_telegram_clusters(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
) -> dict[str, int]:
    return {"telegram_sent": 0, "telegram_failed": 0}
