from __future__ import annotations

import os
from datetime import datetime, timedelta
from html import escape
from typing import Any

import httpx

from .cluster import refresh_cluster_source_lineage
from .dates import datetime_to_iso, parse_datetime
from .remote_api import post_remote_action, remote_api_configured
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


def telegram_admin_chat_id() -> str:
    """Return the explicit private control-plane destination."""

    return os.environ.get("TELEGRAM_ADMIN_CHAT_ID", "").strip()


def telegram_admin_destination_error(config: dict[str, object]) -> str:
    """Reject missing or public admin destinations without a fallback."""

    admin_chat_id = telegram_admin_chat_id()
    if not admin_chat_id:
        return "telegram_admin_chat_id_missing"
    if not admin_chat_id.isdigit() or int(admin_chat_id) <= 0:
        return "telegram_admin_chat_id_must_be_private_user"
    public_chat_id = telegram_chat_id(config)
    if public_chat_id and admin_chat_id == public_chat_id:
        return "telegram_admin_chat_matches_public_destination"
    if not telegram_config(config).get("enabled", True) or not telegram_bot_token():
        return "telegram_not_configured"
    return ""


def telegram_is_configured(config: dict[str, object]) -> bool:
    settings = telegram_config(config)
    return bool(settings.get("enabled", True) and telegram_bot_token() and telegram_chat_id(config))


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


def fail_closed_telegram_delivery_outcome(response: dict[str, object]) -> dict[str, object]:
    """Quarantine sendMessage responses whose external outcome is ambiguous."""

    error = str(response.get("error") or "")
    stage = str(response.get("delivery_stage") or "")
    try:
        status_code = int(response.get("status_code") or 0)
    except (TypeError, ValueError):
        status_code = 0
    ambiguous = error in {
        "telegram_request_failed",
        "telegram_missing_external_message_id",
    } or (stage == "send_message" and status_code >= 500)
    if not ambiguous:
        return response
    return {
        **response,
        "ok": False,
        "error": "telegram_delivery_outcome_unknown",
        "description": error,
        "retryable": False,
    }


def _post_telegram_method(
    bot_token: str,
    method: str,
    payload: dict[str, object],
    *,
    timeout: float,
    client: httpx.Client | None,
) -> dict[str, object]:
    url = f"https://api.telegram.org/bot{bot_token}/{method}"
    try:
        if client is None:
            with httpx.Client(timeout=timeout) as local_client:
                response = local_client.post(url, json=payload)
        else:
            response = client.post(url, json=payload)
        data = response.json()
    except (httpx.HTTPError, ValueError):
        return _telegram_error_response(error="telegram_request_failed")
    if not isinstance(data, dict):
        data = {}
    if response.status_code >= 400:
        return _telegram_error_response(
            error="telegram_http_error",
            status_code=response.status_code,
            description=data.get("description"),
            parameters=data.get("parameters"),
        )
    if not data.get("ok"):
        try:
            error_status = int(data.get("error_code") or response.status_code)
        except (TypeError, ValueError):
            error_status = response.status_code
        return _telegram_error_response(
            error="telegram_api_error",
            status_code=error_status,
            description=data.get("description"),
            parameters=data.get("parameters"),
        )
    return {"ok": True, "result": data.get("result")}


def validate_telegram_chat(
    bot_token: str,
    chat_id: str,
    config: dict[str, object],
    client: httpx.Client | None = None,
) -> dict[str, object]:
    """Validate the destination with getChat before attempting a delivery."""

    settings = telegram_config(config)
    if not settings.get("preflight_get_chat", True):
        return {"ok": True, "chat_id": chat_id, "preflight_skipped": True}
    response = _post_telegram_method(
        bot_token,
        "getChat",
        {"chat_id": chat_id},
        timeout=float(settings.get("timeout_seconds", 20)),
        client=client,
    )
    if not response.get("ok"):
        response["error"] = "telegram_chat_validation_failed"
        return response
    result = response.get("result") if isinstance(response.get("result"), dict) else {}
    validated_chat_id = result.get("id")
    if validated_chat_id in (None, ""):
        return _telegram_error_response(error="telegram_chat_validation_failed", description="getChat response omitted chat id")
    return {
        "ok": True,
        "chat_id": validated_chat_id,
        "username": result.get("username"),
        "title": result.get("title"),
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
    articles = [article for article in list(cluster.get("articles") or []) if isinstance(article, dict)]
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


def cluster_source_lineage_payload(cluster: dict[str, object]) -> dict[str, object]:
    articles = [article for article in list(cluster.get("articles") or []) if isinstance(article, dict)]
    return {
        "source_kind": cluster.get("source_kind") or None,
        "source_right_id": cluster.get("source_right_id") or None,
        "source_kinds": list(cluster.get("source_kinds") or []),
        "source_right_ids": list(cluster.get("source_right_ids") or []),
        "article_sources": [
            {
                "canonical_url_hash": article.get("canonical_url_hash") or None,
                "source_kind": article.get("source_kind") or None,
                "source_right_id": article.get("source_right_id") or None,
            }
            for article in articles
        ],
    }


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
    sent = set(state.get("telegram_sent_cluster_guids", []))
    if not telegram_config(config).get("send_old_on_first_run", False):
        sent.update(
            cluster_guid_value(cluster)
            for cluster in state.get("published_clusters", [])
            if cluster_guid_value(cluster)
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
    sent = {str(guid) for guid in state.get("telegram_sent_cluster_guids", [])}
    clusters = []
    for cluster in state.get("published_clusters", []):
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
    client: httpx.Client | None = None,
    disable_web_page_preview: bool | None = None,
) -> dict[str, object]:
    settings = telegram_config(config)
    preflight = validate_telegram_chat(bot_token, chat_id, config, client=client)
    if not preflight.get("ok"):
        return {**preflight, "delivery_stage": "preflight"}
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
    timeout = float(settings.get("timeout_seconds", 20))
    response = _post_telegram_method(
        bot_token,
        "sendMessage",
        payload,
        timeout=timeout,
        client=client,
    )
    if not response.get("ok"):
        return {**response, "delivery_stage": "send_message"}
    result = response.get("result") if isinstance(response.get("result"), dict) else {}
    message_id = result.get("message_id")
    if message_id in (None, ""):
        return {
            **_telegram_error_response(
                error="telegram_missing_external_message_id",
                description="sendMessage response omitted message_id",
            ),
            "delivery_stage": "send_message",
        }
    return {
        "ok": True,
        "message_id": message_id,
        "chat_id": (result.get("chat") or {}).get("id") if isinstance(result.get("chat"), dict) else None,
        "validated_chat_id": preflight.get("chat_id"),
    }


def mark_telegram_sent(
    state: dict[str, object],
    cluster: dict[str, object],
    now: datetime,
    response: dict[str, object],
) -> bool:
    guid = cluster_guid_value(cluster)
    if not guid or not response.get("ok") or response.get("message_id") in (None, ""):
        return False
    state.setdefault("telegram_sent_cluster_guids", [])
    if guid not in state["telegram_sent_cluster_guids"]:  # type: ignore[operator]
        state["telegram_sent_cluster_guids"].append(guid)  # type: ignore[index, union-attr]
    state.setdefault("telegram_send_records", [])
    state["telegram_send_records"].append(  # type: ignore[index, union-attr]
        {
            "guid": guid,
            "sent_at": datetime_to_iso(now),
            "message_id": response.get("message_id"),
            "external_message_id": response.get("message_id"),
            "chat_id": response.get("chat_id"),
            "delivery_status": "delivered",
        }
    )
    return True


def ensure_telegram_delivery_outbox(state: dict[str, object]) -> list[dict[str, object]]:
    outbox = state.get("telegram_delivery_outbox")
    if not isinstance(outbox, list):
        outbox = []
        state["telegram_delivery_outbox"] = outbox
    return outbox


def enqueue_telegram_delivery(
    state: dict[str, object],
    cluster: dict[str, object],
    config: dict[str, object],
    now: datetime,
) -> dict[str, object] | None:
    public_cluster = authorized_cluster_for_delivery(cluster, config, now)
    if public_cluster is None:
        return None
    guid = cluster_guid_value(public_cluster)
    if not guid:
        return None
    lineage = cluster_source_lineage_payload(public_cluster)
    outbox = ensure_telegram_delivery_outbox(state)
    for entry in outbox:
        if isinstance(entry, dict) and str(entry.get("cluster_guid") or "") == guid:
            entry.update(lineage)
            entry["payload_text"] = build_telegram_message(public_cluster, config)
            entry["disable_web_page_preview"] = not cluster_should_show_web_preview(public_cluster, config)
            return entry
    entry: dict[str, object] = {
        "outbox_id": f"telegram:{guid}",
        "channel": "telegram",
        "cluster_guid": guid,
        "destination": telegram_chat_id(config),
        "payload_text": build_telegram_message(public_cluster, config),
        "disable_web_page_preview": not cluster_should_show_web_preview(public_cluster, config),
        **lineage,
        "status": "pending",
        "attempt_count": 0,
        "created_at": datetime_to_iso(now),
        "next_attempt_at": datetime_to_iso(now),
        "external_message_id": None,
        "last_error": None,
    }
    outbox.append(entry)
    return entry


def enqueue_unsent_telegram_clusters(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
) -> int:
    before = len(ensure_telegram_delivery_outbox(state))
    for cluster in unsent_telegram_clusters(state, config, now=now):
        enqueue_telegram_delivery(state, cluster, config, now)
    return max(0, len(ensure_telegram_delivery_outbox(state)) - before)


def enqueue_unsent_telegram_clusters_to_remote(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
) -> dict[str, int]:
    """Durably enqueue unsent clusters in the MySQL-backed remote outbox."""

    settings = telegram_config(config)
    if not settings.get("enabled", True) or not telegram_chat_id(config) or not remote_api_configured():
        return {
            "telegram_outbox_enqueued": 0,
            "telegram_outbox_rejected": 0,
            "telegram_outbox_enqueue_failed": 0,
            "telegram_outbox_enqueue_skipped": 1,
            "telegram_outbox_rights_blocked": 0,
        }
    for cluster in unsent_telegram_clusters(state, config, require_sender=False, now=now):
        enqueue_telegram_delivery(state, cluster, config, now)
    clusters_by_guid: dict[str, dict[str, object]] = {}
    for cluster in state.get("published_clusters", []):
        if not isinstance(cluster, dict):
            continue
        public_cluster = authorized_cluster_for_delivery(cluster, config, now)
        if public_cluster is not None and cluster_guid_value(public_cluster):
            clusters_by_guid[cluster_guid_value(public_cluster)] = public_cluster
    entries: list[dict[str, object]] = []
    rights_blocked = 0
    for entry in ensure_telegram_delivery_outbox(state):
        if (
            not isinstance(entry, dict)
            or str(entry.get("status") or "") not in {"pending", "retry", "remote_queued"}
            or entry.get("external_message_id")
        ):
            continue
        public_cluster = clusters_by_guid.get(str(entry.get("cluster_guid") or ""))
        if public_cluster is None:
            entry["status"] = "blocked_source_right"
            entry["last_error"] = "source_right_inactive_or_scope_denied"
            rights_blocked += 1
            continue
        entry.update(cluster_source_lineage_payload(public_cluster))
        entry["payload_text"] = build_telegram_message(public_cluster, config)
        entry["disable_web_page_preview"] = not cluster_should_show_web_preview(public_cluster, config)
        entries.append(entry)
    if not entries:
        return {
            "telegram_outbox_enqueued": 0,
            "telegram_outbox_rejected": 0,
            "telegram_outbox_enqueue_failed": 0,
            "telegram_outbox_enqueue_skipped": 0,
            "telegram_outbox_rights_blocked": rights_blocked,
        }
    deliveries = [
        {
            "channel": "telegram",
            "destination": str(entry.get("destination") or telegram_chat_id(config)),
            "idempotency_key": str(entry.get("cluster_guid") or entry.get("outbox_id") or "")[:191],
            "payload": {
                "text": str(entry.get("payload_text") or ""),
                "disable_web_page_preview": bool(entry.get("disable_web_page_preview", True)),
                "cluster_guid": entry.get("cluster_guid"),
                "source_kind": entry.get("source_kind"),
                "source_right_id": entry.get("source_right_id"),
                "source_kinds": entry.get("source_kinds") or [],
                "rights_lineage_complete": True,
                "source_right_ids": entry.get("source_right_ids") or [],
                "article_sources": entry.get("article_sources") or [],
            },
        }
        for entry in entries
    ]
    try:
        response = post_remote_action("enqueue_delivery_outbox", {"deliveries": deliveries})
    except Exception:  # noqa: BLE001 - caller surfaces a nonzero operational summary.
        response = {"ok": False}
    accepted = int(response.get("accepted") or 0) if response.get("ok") else 0
    rejected = int(response.get("rejected") or 0) if response.get("ok") else len(deliveries)
    failed = int(not response.get("ok") or accepted < len(deliveries) or rejected > 0)
    if not failed:
        queued_at = datetime_to_iso(now)
        for entry in entries:
            entry["status"] = "remote_queued"
            entry["remote_enqueued_at"] = queued_at
    return {
        "telegram_outbox_enqueued": accepted,
        "telegram_outbox_rejected": rejected,
        "telegram_outbox_enqueue_failed": failed,
        "telegram_outbox_enqueue_skipped": 0,
        "telegram_outbox_rights_blocked": rights_blocked,
    }


def _outbox_entry_due(entry: dict[str, object], now: datetime, timezone_name: str) -> bool:
    if str(entry.get("status") or "") not in {"pending", "retry"}:
        return False
    next_attempt_at = parse_datetime(entry.get("next_attempt_at"), timezone_name)
    return next_attempt_at is None or next_attempt_at <= now


def _delivery_retry_delay(response: dict[str, object], attempt_count: int, config: dict[str, object]) -> int:
    settings = telegram_config(config)
    explicit = int(response.get("retry_after_seconds") or 0)
    if explicit:
        return explicit
    base = max(1, int(settings.get("retry_base_seconds", 60)))
    maximum = max(base, int(settings.get("retry_max_seconds", 3600)))
    return min(maximum, base * (2 ** max(0, attempt_count - 1)))


def process_telegram_delivery_outbox(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
    *,
    client: httpx.Client | None = None,
    max_items: int = 0,
) -> dict[str, int]:
    """Process due deliveries and persist retry/dead-letter state.

    A row is marked delivered only after Telegram returns both ``ok`` and a
    concrete external ``message_id``.
    """

    if not telegram_is_configured(config):
        return {
            "telegram_outbox_processed": 0,
            "telegram_sent": 0,
            "telegram_failed": 0,
            "telegram_retried": 0,
            "telegram_dead_letter": 0,
            "telegram_outbox_skipped": 1,
        }
    settings = telegram_config(config)
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    max_attempts = max(1, int(settings.get("max_delivery_attempts", 5)))
    processed = sent = failed = retried = rights_blocked = 0
    clusters: dict[str, dict[str, object]] = {}
    for cluster in state.get("published_clusters", []):
        if not isinstance(cluster, dict):
            continue
        public_cluster = authorized_cluster_for_delivery(cluster, config, now)
        if public_cluster is not None and cluster_guid_value(public_cluster):
            clusters[cluster_guid_value(public_cluster)] = public_cluster
    for entry in ensure_telegram_delivery_outbox(state):
        if not isinstance(entry, dict) or not _outbox_entry_due(entry, now, timezone_name):
            continue
        if max_items and processed >= max_items:
            break
        processed += 1
        cluster = clusters.get(str(entry.get("cluster_guid") or ""))
        if cluster is None:
            entry["status"] = "blocked_source_right"
            entry["last_error"] = "source_right_inactive_or_scope_denied"
            entry["blocked_at"] = datetime_to_iso(now)
            rights_blocked += 1
            continue
        entry.update(cluster_source_lineage_payload(cluster))
        entry["payload_text"] = build_telegram_message(cluster, config)
        entry["disable_web_page_preview"] = not cluster_should_show_web_preview(cluster, config)
        entry["status"] = "sending"
        entry["locked_at"] = datetime_to_iso(now)
        response = send_telegram_message(
            telegram_bot_token(),
            str(entry.get("destination") or telegram_chat_id(config)),
            str(entry.get("payload_text") or ""),
            config,
            client=client,
            disable_web_page_preview=bool(entry.get("disable_web_page_preview", True)),
        )
        response = fail_closed_telegram_delivery_outcome(response)
        if response.get("ok") and response.get("message_id") not in (None, ""):
            entry["status"] = "delivered"
            entry["delivered_at"] = datetime_to_iso(now)
            entry["external_message_id"] = response.get("message_id")
            entry["external_chat_id"] = response.get("chat_id")
            entry["last_error"] = None
            mark_telegram_sent(state, cluster, now, response)
            sent += 1
            continue

        failed += 1
        attempt_count = int(entry.get("attempt_count") or 0) + 1
        entry["attempt_count"] = attempt_count
        entry["last_attempt_at"] = datetime_to_iso(now)
        entry["last_error"] = str(response.get("error") or response.get("description") or "telegram_delivery_failed")[:500]
        entry["last_status_code"] = response.get("status_code")
        retryable = bool(response.get("retryable"))
        if retryable and attempt_count < max_attempts:
            delay = _delivery_retry_delay(response, attempt_count, config)
            entry["status"] = "retry"
            entry["next_attempt_at"] = datetime_to_iso(now + timedelta(seconds=delay))
            retried += 1
        else:
            entry["status"] = "dead_letter"
            entry["dead_lettered_at"] = datetime_to_iso(now)
    dead_letter_total = sum(
        1
        for entry in ensure_telegram_delivery_outbox(state)
        if isinstance(entry, dict) and str(entry.get("status") or "") == "dead_letter"
    )
    return {
        "telegram_outbox_processed": processed,
        "telegram_sent": sent,
        "telegram_failed": failed,
        "telegram_retried": retried,
        "telegram_dead_letter": dead_letter_total,
        "telegram_outbox_rights_blocked": rights_blocked,
        "telegram_outbox_skipped": 0,
    }


def publish_unsent_telegram_clusters(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
) -> dict[str, int]:
    if not telegram_is_configured(config):
        return {"telegram_sent": 0, "telegram_failed": 0}
    enqueue_unsent_telegram_clusters(state, config, now)
    summary = process_telegram_delivery_outbox(state, config, now)
    return {
        "telegram_sent": int(summary.get("telegram_sent") or 0),
        "telegram_failed": int(summary.get("telegram_failed") or 0),
    }
