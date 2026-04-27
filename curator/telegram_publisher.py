from __future__ import annotations

import os
from datetime import datetime
from html import escape
from typing import Any

import httpx

from .dates import datetime_to_iso
from .rss_writer import (
    article_link,
    article_source_label,
    compact_text,
    display_article_title,
    item_title,
    publishable_articles,
)


def telegram_config(config: dict[str, object]) -> dict[str, Any]:
    value = config.get("telegram", {})
    return value if isinstance(value, dict) else {}


def telegram_bot_token() -> str:
    return os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()


def telegram_chat_id(config: dict[str, object]) -> str:
    return os.environ.get("TELEGRAM_CHAT_ID", "").strip() or str(telegram_config(config).get("chat_id") or "").strip()


def telegram_is_configured(config: dict[str, object]) -> bool:
    settings = telegram_config(config)
    return bool(settings.get("enabled", True) and telegram_bot_token() and telegram_chat_id(config))


def html_link(label: str, url: str) -> str:
    safe_label = escape(label)
    if not url:
        return safe_label
    return f'<a href="{escape(url, quote=True)}">{safe_label}</a>'


def cluster_guid_value(cluster: dict[str, object]) -> str:
    return str(cluster.get("guid") or "").strip()


def article_group_label(article: dict[str, object]) -> str:
    companies = [str(company).strip() for company in (article.get("company_candidates") or []) if str(company).strip()]
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


def article_summary_preview(article: dict[str, object], config: dict[str, object]) -> str:
    max_chars = int(telegram_config(config).get("single_article_summary_chars", 30))
    summary = compact_text(article.get("summary") or "", max_chars=max_chars)
    title = str(article.get("clean_title") or article.get("title") or "").strip()
    if title and summary.casefold().startswith(title.casefold()):
        summary = summary[len(title) :].lstrip(" -:|")
        summary = compact_text(summary, max_chars=max_chars)
    return summary


def cluster_should_show_web_preview(cluster: dict[str, object], config: dict[str, object]) -> bool:
    settings = telegram_config(config)
    if not settings.get("single_article_web_page_preview", True):
        return False
    return len(publishable_articles(cluster, config)) == 1


def initialize_telegram_state(state: dict[str, object], config: dict[str, object], now: datetime) -> None:
    if not telegram_is_configured(config) or state.get("telegram_initialized_at"):
        return
    sent = set(state.get("telegram_sent_cluster_guids", []))
    if not telegram_config(config).get("send_old_on_first_run", False):
        sent.update(
            cluster_guid_value(cluster)
            for cluster in state.get("published_clusters", [])
            if cluster_guid_value(cluster)
        )
    state["telegram_sent_cluster_guids"] = sorted(sent)
    state["telegram_initialized_at"] = datetime_to_iso(now)


def unsent_telegram_clusters(state: dict[str, object], config: dict[str, object]) -> list[dict[str, object]]:
    if not telegram_is_configured(config):
        return []
    sent = {str(guid) for guid in state.get("telegram_sent_cluster_guids", [])}
    clusters = []
    for cluster in state.get("published_clusters", []):
        guid = cluster_guid_value(cluster)
        if not guid or guid in sent:
            continue
        if not publishable_articles(cluster, config):
            continue
        clusters.append(cluster)
    return clusters


def build_telegram_message(cluster: dict[str, object], config: dict[str, object]) -> str:
    settings = telegram_config(config)
    max_articles = int(settings.get("max_articles_per_message", 7))
    max_chars = int(settings.get("max_message_chars", 3900))
    articles = publishable_articles(cluster, config)
    count = len(articles)

    lines = [] if count == 1 else [f"<b>{escape(item_title(cluster, count))}</b>", ""]

    shown_count = 0
    article_groups = grouped_articles(articles[:max_articles])
    show_groups = should_show_article_groups(article_groups)
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
            source = article_source_label(article)
            title = display_article_title(article, source)
            label = compact_text(f"{source} - {title}", max_chars=110)
            row = f"{shown_count + 1}. {html_link(label, article_link(article))}"
            preview = article_summary_preview(article, config) if count == 1 else ""
            row_lines = [row]
            if preview:
                row_lines.append(f"본문: {escape(preview)}")
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
    client: httpx.Client | None = None,
    disable_web_page_preview: bool | None = None,
) -> dict[str, object]:
    settings = telegram_config(config)
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": str(settings.get("parse_mode") or "HTML"),
        "disable_web_page_preview": (
            bool(settings.get("disable_web_page_preview", True))
            if disable_web_page_preview is None
            else disable_web_page_preview
        ),
    }
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    timeout = float(settings.get("timeout_seconds", 20))
    try:
        if client is None:
            with httpx.Client(timeout=timeout) as local_client:
                response = local_client.post(url, json=payload)
        else:
            response = client.post(url, json=payload)
        data = response.json()
    except (httpx.HTTPError, ValueError):
        return {"ok": False, "error": "telegram_request_failed"}

    if response.status_code >= 400:
        return {
            "ok": False,
            "error": "telegram_http_error",
            "status_code": response.status_code,
            "description": data.get("description") if isinstance(data, dict) else "",
        }
    if not isinstance(data, dict) or not data.get("ok"):
        return {
            "ok": False,
            "error": "telegram_api_error",
            "description": data.get("description") if isinstance(data, dict) else "",
        }
    result = data.get("result") if isinstance(data.get("result"), dict) else {}
    return {
        "ok": True,
        "message_id": result.get("message_id"),
        "chat_id": (result.get("chat") or {}).get("id") if isinstance(result.get("chat"), dict) else None,
    }


def mark_telegram_sent(
    state: dict[str, object],
    cluster: dict[str, object],
    now: datetime,
    response: dict[str, object],
) -> None:
    guid = cluster_guid_value(cluster)
    if not guid:
        return
    state.setdefault("telegram_sent_cluster_guids", [])
    if guid not in state["telegram_sent_cluster_guids"]:  # type: ignore[operator]
        state["telegram_sent_cluster_guids"].append(guid)  # type: ignore[index, union-attr]
    state.setdefault("telegram_send_records", [])
    state["telegram_send_records"].append(  # type: ignore[index, union-attr]
        {
            "guid": guid,
            "sent_at": datetime_to_iso(now),
            "message_id": response.get("message_id"),
            "chat_id": response.get("chat_id"),
        }
    )


def publish_unsent_telegram_clusters(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
) -> dict[str, int]:
    if not telegram_is_configured(config):
        return {"telegram_sent": 0, "telegram_failed": 0}

    bot_token = telegram_bot_token()
    chat_id = telegram_chat_id(config)
    sent = 0
    failed = 0
    for cluster in unsent_telegram_clusters(state, config):
        message = build_telegram_message(cluster, config)
        disable_preview = not cluster_should_show_web_preview(cluster, config)
        response = send_telegram_message(
            bot_token,
            chat_id,
            message,
            config,
            disable_web_page_preview=disable_preview,
        )
        if response.get("ok"):
            mark_telegram_sent(state, cluster, now, response)
            sent += 1
        else:
            failed += 1
    return {"telegram_sent": sent, "telegram_failed": failed}
