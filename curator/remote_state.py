from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from .normalize import normalize_title_parts
from .remote_api import post_remote_action, remote_api_configured
from .source_rights import find_source_right, source_is_authorized
from .telegram_sources import channel_key, telegram_remote_payload_budget


def mysql_runtime_enabled() -> bool:
    return os.environ.get("CURATOR_DATA_SOURCE", "").strip().casefold() in {
        "mysql",
        "remote",
        "api",
    }


def _required() -> bool:
    return (
        os.environ.get("CURATOR_REQUIRE_REMOTE_API", "").strip().casefold()
        in {"1", "true", "yes", "on"}
        or mysql_runtime_enabled()
    )


def _json_value(value: object, default: Any) -> Any:
    if not isinstance(value, str) or not value:
        return default
    try:
        parsed = json.loads(value)
    except ValueError:
        return default
    return parsed


def _bool_value(value: object) -> bool:
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "yes", "on"}
    return bool(value)


def _int_value(value: object, default: int = 0) -> int:
    if isinstance(value, (int, str, bytes, bytearray)):
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
    return default


def telegram_snapshot_capabilities(
    config: dict[str, object],
) -> dict[str, int]:
    response = post_remote_action("telegram_snapshot_capabilities", {})
    if not response.get("ok") or (
        str(response.get("signal_rebuild_protocol") or "") != "staging-v1"
    ):
        raise RuntimeError("telegram signal staging protocol unavailable")
    remote_max_payload_bytes = _int_value(response.get("max_payload_bytes"), -1)
    if remote_max_payload_bytes < telegram_remote_payload_budget(config):
        raise RuntimeError("telegram signal remote body limit is too small")
    live_revision = _int_value(response.get("live_revision"), -1)
    if live_revision < 0:
        raise RuntimeError("telegram signal live revision unavailable")
    return {
        "telegram_signal_remote_max_payload_bytes": remote_max_payload_bytes,
        "telegram_signal_live_revision": live_revision,
    }


def _merge_runtime_source_rights(
    config: dict[str, object],
    source_right_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    rights_records: list[dict[str, object]] = []
    for row in source_right_rows:
        if str(row.get("status") or "") not in {
            "active",
            "pending",
            "expired",
            "revoked",
        }:
            continue
        rights_records.append(
            {
                "source_right_id": row.get("source_right_id"),
                "source_category": "authorized_telegram"
                if "telegram" in str(row.get("source_type") or "")
                else row.get("source_type"),
                "source_identity": row.get("source_key"),
                "scope": row.get("permission_scope"),
                "evidence_ref": row.get("evidence_uri")
                or (
                    f"sha256:{row.get('evidence_hash')}"
                    if row.get("evidence_hash")
                    else ""
                ),
                "valid_from": row.get("valid_from"),
                "expires_at": row.get("valid_until"),
                "revoked_at": row.get("revoked_at"),
                "allow_ai": _bool_value(row.get("ai_allowed")),
                "allow_redistribution": _bool_value(row.get("redistribution_allowed")),
                "status": row.get("status") or "pending",
            }
        )
    source_right_settings = config.get("source_rights")
    if not isinstance(source_right_settings, dict):
        source_right_settings = {}
        config["source_rights"] = source_right_settings
    # MySQL is the operational source of truth.  Replace, rather than merge,
    # the local placeholder list even when the remote table is empty so a
    # removed or revoked right cannot survive in process memory.
    source_right_settings["enforce"] = True
    source_right_settings["records"] = rights_records
    return rights_records


def _derived_record_is_authorized(
    record: dict[str, object],
    config: dict[str, object],
    at: datetime,
    *,
    purpose: str,
) -> bool:
    right_ids = record.get("source_right_ids")
    normalized_right_ids = (
        [str(right_id) for right_id in right_ids if right_id]
        if isinstance(right_ids, list)
        else []
    )
    if normalized_right_ids:
        return all(
            source_is_authorized(
                {
                    "source_kind": record.get("source_kind") or "telegram_signal",
                    "source_right_id": right_id,
                },
                config,
                at,
                purpose=purpose,
            )
            for right_id in normalized_right_ids
        )
    return source_is_authorized(record, config, at, purpose=purpose)


def fetch_runtime_resource(
    resource: str,
    *,
    since: datetime,
    max_records: int,
    require_complete: bool = False,
) -> list[dict[str, object]]:
    if not remote_api_configured():
        if _required():
            raise RuntimeError(
                "MySQL runtime requested but ACTIVIST_API_URL/SECRET are missing"
            )
        return []
    records: list[dict[str, object]] = []
    cursor = ""
    while len(records) < max_records:
        requested_limit = min(100, max_records - len(records))
        response = post_remote_action(
            "export_runtime_state",
            {
                "resource": resource,
                "limit": requested_limit,
                "after": cursor,
                "since": since.isoformat(),
                "order": "updated_desc",
            },
            timeout=30.0,
        )
        if not response.get("ok"):
            raise RuntimeError(
                f"runtime state export failed for {resource}: {response.get('error') or 'unknown_error'}"
            )
        page = response.get("state")
        if not isinstance(page, dict):
            raise RuntimeError(f"runtime state export omitted page for {resource}")
        rows = page.get("records")
        if not isinstance(rows, list):
            raise RuntimeError(f"runtime state export omitted records for {resource}")
        if require_complete and (
            not isinstance(page.get("has_more"), bool)
            or any(not isinstance(row, dict) for row in rows)
            or len(rows) > requested_limit
        ):
            raise RuntimeError(f"runtime state export malformed page for {resource}")
        records.extend(row for row in rows if isinstance(row, dict))
        has_more = bool(page.get("has_more"))
        if not has_more:
            break
        next_cursor = str(page.get("next_cursor") or "")
        if not next_cursor or next_cursor == cursor:
            raise RuntimeError(f"runtime state cursor did not advance for {resource}")
        if require_complete and len(records) >= max_records:
            raise RuntimeError(
                f"runtime state export exceeded complete limit for {resource}"
            )
        cursor = next_cursor
    return records[:max_records]


def hydrate_telegram_signal_window(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
) -> dict[str, int]:
    """Atomically replace the local signal inputs with a complete MySQL window."""

    if not mysql_runtime_enabled():
        raise RuntimeError("telegram signal rebuild requires MySQL runtime")
    settings = config.get("telegram_sources")
    if not isinstance(settings, dict):
        settings = {}
    window_hours = max(1, int(settings.get("signal_window_hours", 72)))
    max_messages = max(
        1, int(settings.get("signal_rebuild_max_messages", 300_000))
    )
    max_matches = max(
        1, int(settings.get("signal_rebuild_max_matches", max_messages * 4))
    )
    max_rights = max(1, int(settings.get("signal_rebuild_max_source_rights", 3_000)))
    since = now - timedelta(hours=window_hours)
    all_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
    # Capture the revision immediately before reading the authoritative input
    # window. Rebuild begin will reject if any message, match, or live signal
    # write commits while this snapshot is being fetched and derived.
    capability_summary = telegram_snapshot_capabilities(config)

    # Fetch every resource before mutating state. A cap, malformed page, or API
    # failure therefore leaves the last valid signal inputs untouched.
    source_right_rows = fetch_runtime_resource(
        "source_rights",
        since=all_time,
        max_records=max_rights,
        require_complete=True,
    )
    message_rows = fetch_runtime_resource(
        "telegram_signal_messages",
        since=since,
        max_records=max_messages,
        require_complete=True,
    )
    match_rows = fetch_runtime_resource(
        "telegram_signal_matches",
        since=since,
        max_records=max_matches,
        require_complete=True,
    )

    _merge_runtime_source_rights(config, source_right_rows)
    messages: list[dict[str, object]] = []
    for row in message_rows:
        message = dict(row)
        message["handle"] = message.get("channel_handle") or ""
        message["source_kind"] = message.get("source_kind") or "authorized_telegram"
        right = find_source_right(message, config)
        if right is not None:
            message["source_right_id"] = right.source_right_id
        message["urls"] = _json_value(message.pop("urls_json", None), [])
        message["risk_flags"] = _json_value(
            message.pop("risk_flags_json", None), []
        )
        if _derived_record_is_authorized(message, config, now, purpose="ai"):
            messages.append(message)

    authorized_message_keys = {
        str(message.get("message_key") or "") for message in messages
    }
    matches: list[dict[str, object]] = []
    for row in match_rows:
        remote_message_key = str(row.get("message_key") or "")
        if not remote_message_key or remote_message_key not in authorized_message_keys:
            continue
        match = dict(row)
        match["telegram_message_key"] = remote_message_key
        matches.append(match)

    state["telegram_source_messages"] = messages
    state["telegram_article_matches"] = matches
    return {
        **capability_summary,
        "telegram_signal_window_rebuilt": 1,
        "telegram_signal_window_hours": window_hours,
        "telegram_signal_window_messages": len(messages),
        "telegram_signal_window_matches": len(matches),
    }


def preflight_telegram_signal_runtime(
    config: dict[str, object],
    now: datetime,
) -> dict[str, int]:
    """Verify repair-only PHP resources before any Telegram history is read."""

    if not mysql_runtime_enabled():
        raise RuntimeError("telegram signal preflight requires MySQL runtime")
    settings = config.get("telegram_sources")
    if not isinstance(settings, dict):
        settings = {}
    window_hours = max(1, int(settings.get("signal_window_hours", 72)))
    since = now - timedelta(hours=window_hours)
    capability_summary = telegram_snapshot_capabilities(config)
    sampled = 0
    for resource in ("telegram_signal_messages", "telegram_signal_matches"):
        sampled += len(
            fetch_runtime_resource(resource, since=since, max_records=1)
        )
    return {
        "telegram_signal_runtime_preflight": 1,
        "telegram_signal_staging_preflight": 1,
        **capability_summary,
        "telegram_signal_runtime_samples": sampled,
    }


def _article(row: dict[str, object]) -> dict[str, object]:
    article = dict(row)
    url = str(article.get("canonical_url") or "")
    article.setdefault("link", url)
    article.setdefault("article_published_at", article.get("published_at"))
    article.setdefault("feed_published_at", article.get("published_at"))
    title = str(article.get("title") or "")
    parts = normalize_title_parts(title)
    article.setdefault("clean_title", parts["clean_title"])
    article.setdefault("normalized_title", parts["normalized_title"])
    article.setdefault("title_hash", parts["title_hash"])
    return article


def _cluster(
    row: dict[str, object], articles: list[dict[str, object]]
) -> dict[str, object]:
    payload = _json_value(row.get("payload_json"), {})
    cluster = dict(payload) if isinstance(payload, dict) else {}
    cluster.update(
        {
            key: value
            for key, value in row.items()
            if key != "payload_json" and value is not None
        }
    )
    cluster["articles"] = articles
    cluster["article_count"] = int(cluster.get("article_count") or len(articles))
    cluster.setdefault("cluster_key", cluster.get("story_key"))
    cluster.setdefault("guid", cluster.get("story_key"))
    cluster.setdefault(
        "last_article_at",
        cluster.get("last_article_seen_at") or cluster.get("published_at"),
    )
    cluster.setdefault("last_article_seen_at", cluster.get("last_article_at"))
    cluster.setdefault(
        "first_seen_at", cluster.get("published_at") or cluster.get("last_article_at")
    )
    if articles:
        representative = articles[0]
        cluster["source_kind"] = representative.get("source_kind") or None
        cluster["source_right_id"] = representative.get("source_right_id") or None
        cluster["source_kinds"] = sorted(
            {
                str(article.get("source_kind") or "").strip()
                for article in articles
                if article.get("source_kind")
            }
        )
        cluster["source_right_ids"] = sorted(
            {
                str(article.get("source_right_id") or "").strip()
                for article in articles
                if article.get("source_right_id")
            }
        )
    if not articles and cluster.get("representative_url"):
        synthetic = _article(
            {
                "record_id": f"runtime:{cluster.get('story_key')}",
                "title": cluster.get("representative_title") or "",
                "canonical_url": cluster.get("representative_url") or "",
                "published_at": cluster.get("published_at")
                or cluster.get("last_article_seen_at"),
                "source": "",
                "status": cluster.get("status") or "published",
                "relevance_level": cluster.get("relevance_level") or "medium",
                "priority_score": cluster.get("priority_score") or 0,
            }
        )
        cluster["articles"] = [synthetic]
        cluster["article_count"] = 1
    return cluster


def hydrate_runtime_state(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
) -> dict[str, int]:
    if not mysql_runtime_enabled():
        return {"runtime_hydrated": 0}
    retention = config.get("state", {})
    retention_days = (
        int(retention.get("retention_days", 60)) if isinstance(retention, dict) else 60
    )
    max_articles = (
        int(retention.get("max_articles", 5000))
        if isinstance(retention, dict)
        else 5000
    )
    max_telegram = (
        int(retention.get("max_telegram_records", 5000))
        if isinstance(retention, dict)
        else 5000
    )
    since = now - timedelta(days=max(2, retention_days))

    article_rows = fetch_runtime_resource(
        "articles", since=since, max_records=max_articles
    )
    story_rows = fetch_runtime_resource("stories", since=since, max_records=1000)
    all_time = datetime(1970, 1, 1, tzinfo=timezone.utc)
    channel_rows = fetch_runtime_resource(
        "telegram_channels", since=all_time, max_records=1000
    )
    message_rows = fetch_runtime_resource(
        "telegram_messages", since=since, max_records=max_telegram
    )
    match_rows = fetch_runtime_resource(
        "telegram_article_matches", since=since, max_records=max_telegram * 2
    )
    signal_rows = fetch_runtime_resource(
        "telegram_issue_signals", since=since, max_records=3000
    )
    outbox_rows = fetch_runtime_resource(
        "delivery_outbox", since=since, max_records=3000
    )
    source_right_rows = fetch_runtime_resource(
        "source_rights", since=all_time, max_records=3000
    )
    collection_runs = fetch_runtime_resource(
        "collection_runs", since=since, max_records=3000
    )
    company_rows = fetch_runtime_resource("companies", since=since, max_records=5000)
    governance_rows = fetch_runtime_resource(
        "governance_events", since=since, max_records=3000
    )
    document_rows = fetch_runtime_resource("documents", since=since, max_records=3000)

    # Rights must be materialized before any derived rows are hydrated.  This
    # makes an expiration or revocation effective in the very same run.
    rights_records = _merge_runtime_source_rights(config, source_right_rows)

    raw_articles = [_article(row) for row in article_rows]
    articles = [
        article
        for article in raw_articles
        if _derived_record_is_authorized(article, config, now, purpose="ai")
    ]
    public_articles_by_story: dict[str, list[dict[str, object]]] = {}
    story_keys_with_articles: set[str] = set()
    for article in raw_articles:
        story_key = str(article.get("story_key") or "")
        if story_key:
            story_keys_with_articles.add(story_key)
    for article in articles:
        story_key = str(article.get("story_key") or "")
        if story_key:
            if _derived_record_is_authorized(article, config, now, purpose="public"):
                public_articles_by_story.setdefault(story_key, []).append(article)
    clusters: list[dict[str, object]] = []
    for row in story_rows:
        story_key = str(row.get("story_key") or "")
        story_articles = public_articles_by_story.get(story_key, [])
        if story_key in story_keys_with_articles and not story_articles:
            continue
        cluster = _cluster(row, story_articles)
        if not _derived_record_is_authorized(cluster, config, now, purpose="public"):
            continue
        clusters.append(cluster)
    state["articles"] = articles
    state["published_clusters"] = [
        cluster
        for cluster in clusters
        if str(cluster.get("status") or "") == "published"
    ]
    state["pending_clusters"] = [
        cluster
        for cluster in clusters
        if str(cluster.get("status") or "") != "published"
    ]
    hydrated_channels: list[dict[str, object]] = []
    for row in channel_rows:
        channel = dict(row)
        payload = _json_value(channel.pop("payload_json", None), {})
        if isinstance(payload, dict):
            for key in (
                "identity_review_required",
                "identity_review_reason",
                "observed_telegram_channel_id",
                "identity_review_detected_at",
            ):
                if key in payload:
                    channel[key] = payload[key]
        hydrated_channels.append(channel)
    state["telegram_source_channels"] = hydrated_channels
    # Every channel cursor returned by MySQL was committed in the same
    # transaction as its acknowledged Telegram message batch. Seed the local
    # acknowledgement map so a fresh GitHub Actions runner does not re-upload
    # the 5,000 hydrated messages on every invocation.
    state["telegram_remote_sync_cursors"] = {
        channel_key(row): _int_value(row.get("last_message_id"))
        for row in hydrated_channels
        if _int_value(row.get("last_message_id")) > 0
    }
    messages: list[dict[str, object]] = []
    for row in message_rows:
        message = dict(row)
        message["handle"] = message.get("channel_handle") or ""
        message["source_kind"] = message.get("source_kind") or "authorized_telegram"
        right = find_source_right(message, config)
        if right is not None:
            message["source_right_id"] = right.source_right_id
        message["urls"] = _json_value(message.pop("urls_json", None), [])
        message["risk_flags"] = _json_value(message.pop("risk_flags_json", None), [])
        if _derived_record_is_authorized(message, config, now, purpose="ai"):
            messages.append(message)
    state["telegram_source_messages"] = messages
    matches: list[dict[str, object]] = []
    for row in match_rows:
        match = dict(row)
        match["telegram_message_key"] = match.get("message_key") or ""
        matches.append(match)
    state["telegram_article_matches"] = matches
    signals: list[dict[str, object]] = []
    for row in signal_rows:
        payload = _json_value(row.get("payload_json"), {})
        signal = dict(payload) if isinstance(payload, dict) else {}
        signal.update(
            {
                key: value
                for key, value in row.items()
                if key != "payload_json" and value is not None
            }
        )
        if _derived_record_is_authorized(signal, config, now, purpose="ai"):
            signals.append(signal)
    state["telegram_issue_signals"] = signals
    delivery_outbox: list[dict[str, object]] = []
    delivered_cluster_guids: list[str] = []
    for row in outbox_rows:
        payload = _json_value(row.get("payload_json"), {})
        delivery = dict(row)
        if isinstance(payload, dict):
            delivery["payload"] = payload
            delivery["payload_text"] = (
                payload.get("text") or payload.get("payload_text") or ""
            )
            cluster_guid = str(payload.get("cluster_guid") or "")
            if cluster_guid and str(row.get("status") or "") == "delivered":
                delivered_cluster_guids.append(cluster_guid)
        delivery["outbox_id"] = row.get("delivery_id")
        delivery["channel"] = row.get("delivery_channel")
        delivery["attempt_count"] = _int_value(row.get("attempt_count"))
        delivery_outbox.append(delivery)
    state["telegram_delivery_outbox"] = delivery_outbox
    state["telegram_sent_cluster_guids"] = sorted(set(delivered_cluster_guids))
    state["governance_events"] = [dict(row) for row in governance_rows]
    state["governance_documents"] = [dict(row) for row in document_rows]
    state["companies"] = [dict(row) for row in company_rows]
    state["collection_runs"] = [dict(row) for row in collection_runs]
    state["runtime_hydrated_at"] = now.isoformat()
    state["runtime_data_source"] = "mysql"
    return {
        "runtime_hydrated": 1,
        "runtime_articles": len(articles),
        "runtime_stories": len(clusters),
        "runtime_telegram_messages": len(messages),
        "runtime_telegram_matches": len(matches),
        "runtime_source_rights": len(rights_records),
        "runtime_governance_events": len(governance_rows),
    }
