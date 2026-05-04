from __future__ import annotations

import asyncio
import argparse
import json
import os
import random
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from .config import load_config
from .dates import datetime_to_iso
from .normalize import canonical_url_hash, normalize_title, normalize_url, stable_hash
from .remote_api import post_remote_action, remote_api_configured
from .state import load_state, save_state


URL_PATTERN = re.compile(r"https?://[^\s<>()\"']+", re.IGNORECASE)
TRAILING_URL_CHARS = ".,;:!?)]}>\u3002"
POSITIVE_CHANNEL_KEYWORDS = {
    "경제",
    "증권",
    "주식",
    "종목",
    "공시",
    "실적",
    "환율",
    "채권",
    "반도체",
    "바이오",
    "ai",
    "뉴스",
}
NEGATIVE_CHANNEL_KEYWORDS = {
    "수익보장",
    "리딩방",
    "무료추천",
    "선물",
    "해외선물",
    "카지노",
    "도박",
    "레퍼럴",
    "vip방",
    "급등주 보장",
}
MARKET_SENSITIVE_KEYWORDS = {"상장폐지", "거래정지", "불성실공시", "감사의견", "공개매수", "유상증자"}
RUMOR_KEYWORDS = {"찌라시", "루머", "카더라", "확인안됨", "미확인"}
PROMOTIONAL_KEYWORDS = {"매수", "급등", "추천", "수익", "목표가", "리딩"}
GENERIC_MATCH_TOKENS = {
    "관련",
    "기사",
    "뉴스",
    "보도",
    "시장",
    "자본시장",
    "기업",
    "주주",
    "증권",
    "금융",
    "경제",
    "공시",
}


class TelegramFloodWait(Exception):
    def __init__(self, seconds: int) -> None:
        super().__init__(f"Telegram FloodWait: {seconds}s")
        self.seconds = seconds


class TelegramMessageClient(Protocol):
    async def get_channel_info(self, channel: dict[str, object]) -> dict[str, object]:
        ...

    async def iter_messages(self, channel: dict[str, object], *, min_id: int, limit: int) -> list[dict[str, object]]:
        ...

    async def recommend_channels(self, seed_channel: dict[str, object], *, limit: int) -> list[dict[str, object]]:
        ...

    async def join_channel(self, candidate: dict[str, object]) -> dict[str, object]:
        ...

    async def close(self) -> None:
        ...


def telegram_sources_config(config: dict[str, object]) -> dict[str, Any]:
    settings = config.get("telegram_sources", {})
    return settings if isinstance(settings, dict) else {}


def telegram_sources_enabled(config: dict[str, object]) -> bool:
    settings = telegram_sources_config(config)
    return bool(settings.get("enabled", False))


def normalize_channel_handle(value: object) -> str:
    handle = str(value or "").strip()
    handle = re.sub(r"^https?://t\.me/s?/", "", handle, flags=re.IGNORECASE)
    handle = handle.removeprefix("@").strip("/")
    return handle


def channel_key(channel: dict[str, object]) -> str:
    channel_id = str(channel.get("telegram_channel_id") or channel.get("channel_id") or "").strip()
    if channel_id:
        return f"id:{channel_id}"
    return f"handle:{normalize_channel_handle(channel.get('handle') or channel.get('username'))}"


def configured_channels(config: dict[str, object]) -> list[dict[str, object]]:
    channels = telegram_sources_config(config).get("channels", [])
    if not isinstance(channels, list):
        return []
    normalized: list[dict[str, object]] = []
    for raw in channels:
        if isinstance(raw, str):
            raw_channel: dict[str, object] = {"handle": raw}
        elif isinstance(raw, dict):
            raw_channel = dict(raw)
        else:
            continue
        handle = normalize_channel_handle(raw_channel.get("handle") or raw_channel.get("username"))
        if not handle and not raw_channel.get("telegram_channel_id"):
            continue
        raw_channel["handle"] = handle
        raw_channel.setdefault("source", "manual")
        raw_channel.setdefault("enabled", True)
        raw_channel.setdefault("joined", False)
        raw_channel.setdefault("quality_score", score_channel_candidate(raw_channel))
        normalized.append(raw_channel)
    return normalized


def ensure_telegram_state(state: dict[str, object]) -> None:
    for key in (
        "telegram_source_channels",
        "telegram_source_messages",
        "telegram_article_matches",
        "telegram_channel_candidates",
        "telegram_issue_signals",
        "telegram_source_runs",
    ):
        if not isinstance(state.get(key), list):
            state[key] = []


def upsert_telegram_channel(state: dict[str, object], channel: dict[str, object]) -> dict[str, object]:
    ensure_telegram_state(state)
    key = channel_key(channel)
    channels = state["telegram_source_channels"]  # type: ignore[index]
    for existing in channels:
        if isinstance(existing, dict) and channel_key(existing) == key:
            existing.update({name: value for name, value in channel.items() if value not in (None, "")})
            return existing
    record = {
        "handle": normalize_channel_handle(channel.get("handle") or channel.get("username")),
        "telegram_channel_id": channel.get("telegram_channel_id") or channel.get("channel_id") or None,
        "title": channel.get("title") or "",
        "description": channel.get("description") or "",
        "joined": bool(channel.get("joined", False)),
        "enabled": bool(channel.get("enabled", True)),
        "source": channel.get("source") or "manual",
        "quality_score": int(channel.get("quality_score") or score_channel_candidate(channel)),
        "last_message_id": int(channel.get("last_message_id") or 0),
        "last_collected_at": channel.get("last_collected_at") or None,
        "last_recommendation_checked_at": channel.get("last_recommendation_checked_at") or None,
        "last_error": channel.get("last_error") or None,
    }
    channels.append(record)
    return record


def register_configured_channels(state: dict[str, object], config: dict[str, object]) -> int:
    before = len(state.get("telegram_source_channels", []) if isinstance(state.get("telegram_source_channels"), list) else [])
    for channel in configured_channels(config):
        upsert_telegram_channel(state, channel)
    after = len(state.get("telegram_source_channels", []) if isinstance(state.get("telegram_source_channels"), list) else [])
    return max(0, after - before)


def enabled_channels(state: dict[str, object]) -> list[dict[str, object]]:
    ensure_telegram_state(state)
    return [
        channel
        for channel in state.get("telegram_source_channels", [])
        if isinstance(channel, dict) and bool(channel.get("enabled", True))
    ]


def normalize_message_text(text: object) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def extract_urls(text: str) -> list[str]:
    urls: list[str] = []
    for match in URL_PATTERN.findall(str(text or "")):
        url = match.rstrip(TRAILING_URL_CHARS)
        if url and url not in urls:
            urls.append(url)
    return urls


def canonicalize_telegram_url(url: str) -> str:
    return normalize_url(url)


def telegram_message_url(channel: dict[str, object], message_id: int) -> str:
    handle = normalize_channel_handle(channel.get("handle") or channel.get("username"))
    return f"https://t.me/{handle}/{message_id}" if handle and message_id else ""


def message_key(message: dict[str, object]) -> str:
    return f"{channel_key(message)}:{int(message.get('telegram_message_id') or 0)}"


def normalize_telegram_message(channel: dict[str, object], raw_message: dict[str, object], now: datetime) -> dict[str, object]:
    message_id = int(raw_message.get("telegram_message_id") or raw_message.get("id") or 0)
    text = str(raw_message.get("text") or raw_message.get("message") or "")
    posted_at = raw_message.get("posted_at") or raw_message.get("date") or datetime_to_iso(now)
    edited_at = raw_message.get("edited_at") or raw_message.get("edit_date") or None
    urls = [canonicalize_telegram_url(url) for url in extract_urls(text)]
    return {
        "handle": normalize_channel_handle(channel.get("handle") or channel.get("username")),
        "telegram_channel_id": channel.get("telegram_channel_id") or channel.get("channel_id") or None,
        "channel_title": channel.get("title") or "",
        "telegram_message_id": message_id,
        "posted_at": posted_at if isinstance(posted_at, str) else datetime_to_iso(posted_at),
        "edited_at": edited_at if isinstance(edited_at, str) or edited_at is None else datetime_to_iso(edited_at),
        "deleted_at": raw_message.get("deleted_at") or None,
        "text": text,
        "normalized_text": normalize_message_text(text).casefold(),
        "views": int(raw_message.get("views") or 0),
        "forwards": int(raw_message.get("forwards") or 0),
        "replies_count": int(raw_message.get("replies_count") or raw_message.get("replies") or 0),
        "message_url": raw_message.get("message_url") or telegram_message_url(channel, message_id),
        "urls": [url for url in urls if url],
        "raw_json": raw_message.get("raw_json") if isinstance(raw_message.get("raw_json"), dict) else None,
        "collected_at": datetime_to_iso(now),
    }


def upsert_telegram_message(state: dict[str, object], message: dict[str, object]) -> str:
    ensure_telegram_state(state)
    key = message_key(message)
    messages = state["telegram_source_messages"]  # type: ignore[index]
    for existing in messages:
        if not isinstance(existing, dict) or message_key(existing) != key:
            continue
        changed = False
        for field in ("text", "normalized_text", "edited_at", "deleted_at", "views", "forwards", "replies_count", "urls", "raw_json"):
            if message.get(field) != existing.get(field):
                existing[field] = message.get(field)
                changed = True
        existing["collected_at"] = message.get("collected_at")
        return "updated" if changed else "unchanged"
    messages.append(message)
    return "inserted"


def mark_deleted_message(state: dict[str, object], channel: dict[str, object], telegram_message_id: int, deleted_at: datetime) -> bool:
    ensure_telegram_state(state)
    target = f"{channel_key(channel)}:{telegram_message_id}"
    for existing in state.get("telegram_source_messages", []):
        if isinstance(existing, dict) and message_key(existing) == target:
            existing["deleted_at"] = datetime_to_iso(deleted_at)
            return True
    return False


def reconcile_recent_deletions(
    state: dict[str, object],
    channel: dict[str, object],
    observed_message_ids: set[int],
    deleted_at: datetime,
    *,
    recent_limit: int = 100,
) -> int:
    """Mark recent missing messages as deleted when a caller can provide a fresh window.

    Public-channel polling cannot perfectly observe Telegram delete events. This helper keeps
    the correction path explicit for backfills or admin-triggered window checks.
    """
    ensure_telegram_state(state)
    channel_prefix = channel_key(channel)
    candidates = [
        message
        for message in state.get("telegram_source_messages", [])
        if isinstance(message, dict) and message_key(message).startswith(f"{channel_prefix}:") and not message.get("deleted_at")
    ]
    candidates.sort(key=lambda item: int(item.get("telegram_message_id") or 0), reverse=True)
    marked = 0
    for message in candidates[:recent_limit]:
        message_id = int(message.get("telegram_message_id") or 0)
        if message_id and message_id not in observed_message_ids:
            message["deleted_at"] = datetime_to_iso(deleted_at)
            marked += 1
    return marked


def article_id(article: dict[str, object]) -> str:
    return str(article.get("record_id") or article.get("canonical_url_hash") or article.get("title_hash") or "").strip()


def article_url_index(state: dict[str, object]) -> dict[str, dict[str, object]]:
    index: dict[str, dict[str, object]] = {}
    for article in state.get("articles", []):
        if not isinstance(article, dict):
            continue
        for key in ("canonical_url", "link", "original_url", "resolved_url"):
            url = canonicalize_telegram_url(str(article.get(key) or ""))
            if url:
                index[url] = article
                index[canonical_url_hash(url)] = article
    return index


def article_tokens(article: dict[str, object]) -> set[str]:
    text = " ".join(
        str(article.get(key) or "")
        for key in ("title", "normalized_title", "summary")
    )
    tokens = {token.casefold() for token in re.findall(r"[0-9A-Za-z가-힣]{2,}", text)}
    tokens.update(title_signature(article.get("title")))
    return {token for token in tokens if token not in GENERIC_MATCH_TOKENS}


def title_signature(value: object) -> set[str]:
    normalized = normalize_title(str(value or ""))
    tokens = {token.casefold() for token in re.findall(r"[0-9A-Za-z가-힣]{2,}", normalized)}
    return {token for token in tokens if token not in GENERIC_MATCH_TOKENS}


def message_tokens(message: dict[str, object]) -> set[str]:
    tokens = {token.casefold() for token in re.findall(r"[0-9A-Za-z가-힣]{2,}", str(message.get("normalized_text") or ""))}
    return {token for token in tokens if token not in GENERIC_MATCH_TOKENS}


def upsert_article_match(state: dict[str, object], match: dict[str, object]) -> str:
    ensure_telegram_state(state)
    matches = state["telegram_article_matches"]  # type: ignore[index]
    identity = (
        str(match.get("article_id") or ""),
        str(match.get("telegram_message_key") or ""),
        str(match.get("match_type") or ""),
    )
    for existing in matches:
        if not isinstance(existing, dict):
            continue
        existing_identity = (
            str(existing.get("article_id") or ""),
            str(existing.get("telegram_message_key") or ""),
            str(existing.get("match_type") or ""),
        )
        if existing_identity == identity:
            existing.update(match)
            return "updated"
    matches.append(match)
    return "inserted"


def match_message_to_articles(state: dict[str, object], message: dict[str, object], config: dict[str, object]) -> list[dict[str, object]]:
    url_index = article_url_index(state)
    results: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()

    for url in message.get("urls") or []:
        canonical = canonicalize_telegram_url(str(url))
        candidates = [
            ("exact_url", url_index.get(canonical), 1.0, "URL 직접 일치"),
            ("canonical_url", url_index.get(canonical_url_hash(canonical)), 0.96, "canonical URL hash 일치"),
        ]
        for match_type, article, score, reason in candidates:
            if not isinstance(article, dict):
                continue
            key = (article_id(article), "url")
            if key in seen:
                continue
            seen.add(key)
            results.append(
                {
                    "article_id": article_id(article),
                    "telegram_message_key": message_key(message),
                    "telegram_message_id": message.get("telegram_message_id"),
                    "message_url": message.get("message_url") or "",
                    "channel_handle": message.get("handle") or "",
                    "channel_title": message.get("channel_title") or "",
                    "match_type": match_type,
                    "score": score,
                    "reason": reason,
                }
            )

    if results:
        return results

    settings = telegram_sources_config(config)
    min_overlap = int(settings.get("weak_match_min_overlap", 2))
    tokens = message_tokens(message)
    if not tokens:
        return []
    for article in state.get("articles", []):
        if not isinstance(article, dict):
            continue
        overlap = sorted(tokens & article_tokens(article))
        if len(overlap) < min_overlap:
            continue
        results.append(
            {
                "article_id": article_id(article),
                "telegram_message_key": message_key(message),
                "telegram_message_id": message.get("telegram_message_id"),
                "message_url": message.get("message_url") or "",
                "channel_handle": message.get("handle") or "",
                "channel_title": message.get("channel_title") or "",
                "match_type": "keyword",
                "score": min(0.72, 0.35 + len(overlap) * 0.08),
                "reason": "키워드 추정 일치: " + ", ".join(overlap[:5]),
            }
        )
    return results[: int(settings.get("weak_match_limit_per_message", 5))]


def risk_flags_for_text(text: str) -> list[str]:
    lowered = str(text or "").casefold()
    flags: list[str] = []
    if any(keyword in lowered for keyword in RUMOR_KEYWORDS):
        flags.append("rumor")
    if any(keyword in lowered for keyword in PROMOTIONAL_KEYWORDS):
        flags.append("promotional")
    if any(keyword in lowered for keyword in MARKET_SENSITIVE_KEYWORDS):
        flags.append("market_sensitive")
    if "?" in lowered and any(keyword in lowered for keyword in ("확인", "사실", "진위")):
        flags.append("unverified")
    return flags


def telegram_issue_signals(state: dict[str, object], *, limit: int = 20) -> list[dict[str, object]]:
    ensure_telegram_state(state)
    messages_by_key = {
        message_key(message): message
        for message in state.get("telegram_source_messages", [])
        if isinstance(message, dict) and not message.get("deleted_at")
    }
    grouped: dict[str, list[dict[str, object]]] = {}
    for match in state.get("telegram_article_matches", []):
        if not isinstance(match, dict):
            continue
        article = str(match.get("article_id") or "")
        if not article:
            continue
        grouped.setdefault(article, []).append(match)

    signals: list[dict[str, object]] = []
    for article, matches in grouped.items():
        related_messages = [messages_by_key.get(str(match.get("telegram_message_key") or "")) for match in matches]
        related_messages = [message for message in related_messages if isinstance(message, dict)]
        if not related_messages:
            continue
        channels = {str(message.get("handle") or message.get("telegram_channel_id") or "") for message in related_messages}
        dates = sorted(str(message.get("posted_at") or "") for message in related_messages if message.get("posted_at"))
        keyword_counter: Counter[str] = Counter()
        flags: set[str] = set()
        for message in related_messages:
            keyword_counter.update(list(message_tokens(message))[:8])
            flags.update(risk_flags_for_text(str(message.get("text") or "")))
        confidence = min(1.0, 0.18 + len(related_messages) * 0.08 + len(channels) * 0.16)
        signals.append(
            {
                "article_id": article,
                "related_telegram_count": len(related_messages),
                "related_telegram_channels_count": len(channels),
                "first_seen_at": dates[0] if dates else "",
                "latest_seen_at": dates[-1] if dates else "",
                "top_related_messages": sorted(
                    related_messages,
                    key=lambda item: int(item.get("views") or 0) + int(item.get("forwards") or 0) * 2,
                    reverse=True,
                )[:5],
                "top_channels": sorted(channels)[:8],
                "top_keywords": [keyword for keyword, _count in keyword_counter.most_common(8)],
                "confidence_score": round(confidence, 3),
                "risk_flags": sorted(flags),
            }
        )
    return sorted(
        signals,
        key=lambda item: (int(item.get("related_telegram_channels_count") or 0), int(item.get("related_telegram_count") or 0)),
        reverse=True,
    )[:limit]


def score_channel_candidate(candidate: dict[str, object]) -> int:
    text = " ".join(
        str(candidate.get(key) or "")
        for key in ("handle", "username", "title", "description")
    ).casefold()
    score = 50
    for keyword in POSITIVE_CHANNEL_KEYWORDS:
        if keyword.casefold() in text:
            score += 6
    for keyword in NEGATIVE_CHANNEL_KEYWORDS:
        if keyword.casefold() in text:
            score -= 18
    return max(0, min(100, score))


def upsert_channel_candidate(state: dict[str, object], candidate: dict[str, object]) -> dict[str, object]:
    ensure_telegram_state(state)
    key = channel_key(candidate)
    candidates = state["telegram_channel_candidates"]  # type: ignore[index]
    for existing in candidates:
        if isinstance(existing, dict) and channel_key(existing) == key:
            existing.update({name: value for name, value in candidate.items() if value not in (None, "")})
            existing["quality_score"] = score_channel_candidate(existing)
            return existing
    record = {
        "handle": normalize_channel_handle(candidate.get("handle") or candidate.get("username")),
        "telegram_channel_id": candidate.get("telegram_channel_id") or candidate.get("channel_id") or None,
        "title": candidate.get("title") or "",
        "description": candidate.get("description") or "",
        "source": candidate.get("source") or "recommendation",
        "status": candidate.get("status") or "pending",
        "quality_score": score_channel_candidate(candidate),
        "last_checked_at": candidate.get("last_checked_at") or None,
        "failure_reason": candidate.get("failure_reason") or None,
    }
    candidates.append(record)
    return record


def flood_wait_seconds(error: BaseException) -> int | None:
    seconds = getattr(error, "seconds", None)
    if seconds is None and error.__class__.__name__.lower().startswith("floodwait"):
        seconds = getattr(error, "value", None)
    try:
        return int(seconds) if seconds is not None else None
    except (TypeError, ValueError):
        return None


class TelethonClientAdapter:
    def __init__(self, config: dict[str, object]) -> None:
        try:
            from telethon import TelegramClient  # type: ignore
            from telethon.sessions import StringSession  # type: ignore
        except ImportError as exc:  # pragma: no cover - exercised through not_configured path
            raise RuntimeError("Telethon is not installed") from exc
        settings = telegram_sources_config(config)
        api_id = int(os.environ.get("TELEGRAM_API_ID") or settings.get("api_id") or 0)
        api_hash = os.environ.get("TELEGRAM_API_HASH", "").strip() or str(settings.get("api_hash") or "")
        session = os.environ.get("TELEGRAM_SESSION_STRING", "").strip() or os.environ.get("TELEGRAM_SESSION", "").strip() or str(settings.get("session") or "activist-reader")
        if not api_id or not api_hash:
            raise RuntimeError("TELEGRAM_API_ID/TELEGRAM_API_HASH is required")
        session_arg = StringSession(session) if len(session) > 80 and "/" not in session and "\\" not in session else session
        self.client = TelegramClient(session_arg, api_id, api_hash)

    async def __aenter__(self) -> "TelethonClientAdapter":
        await self.client.start()
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self.client.disconnect()

    async def get_channel_info(self, channel: dict[str, object]) -> dict[str, object]:
        entity = await self.client.get_entity(normalize_channel_handle(channel.get("handle") or channel.get("username")))
        return {
            "handle": getattr(entity, "username", None) or channel.get("handle"),
            "telegram_channel_id": getattr(entity, "id", None),
            "title": getattr(entity, "title", None) or channel.get("title") or "",
            "description": getattr(entity, "about", None) or channel.get("description") or "",
            "joined": True,
        }

    async def iter_messages(self, channel: dict[str, object], *, min_id: int, limit: int) -> list[dict[str, object]]:
        entity = await self.client.get_entity(normalize_channel_handle(channel.get("handle") or channel.get("username")))
        messages: list[dict[str, object]] = []
        async for message in self.client.iter_messages(entity, min_id=min_id, limit=limit, reverse=True):
            messages.append(
                {
                    "id": int(message.id or 0),
                    "text": message.message or "",
                    "date": message.date,
                    "edit_date": message.edit_date,
                    "views": getattr(message, "views", 0) or 0,
                    "forwards": getattr(message, "forwards", 0) or 0,
                    "replies_count": getattr(getattr(message, "replies", None), "replies", 0) or 0,
                }
            )
        return messages

    async def recommend_channels(self, seed_channel: dict[str, object], *, limit: int) -> list[dict[str, object]]:
        # Telethon does not expose a stable public-channel recommendation primitive for all accounts.
        # Keep this hook for an existing backend/admin implementation; default collector records no candidates.
        return []

    async def join_channel(self, candidate: dict[str, object]) -> dict[str, object]:
        from telethon.tl.functions.channels import JoinChannelRequest  # type: ignore

        result = await self.client(JoinChannelRequest(normalize_channel_handle(candidate.get("handle"))))
        return {"ok": True, "result": str(result)[:120]}


async def _collect_with_client(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
    client: TelegramMessageClient,
) -> dict[str, int]:
    settings = telegram_sources_config(config)
    backfill_limit = int(settings.get("backfill_limit", 100))
    incremental_limit = int(settings.get("incremental_limit", 200))
    inserted = updated = unchanged = failed = matches_inserted = 0

    for channel in enabled_channels(state):
        try:
            info = await client.get_channel_info(channel)
            channel.update(info)
            min_id = int(channel.get("last_message_id") or 0)
            limit = incremental_limit if min_id else backfill_limit
            raw_messages = await client.iter_messages(channel, min_id=min_id, limit=limit)
        except Exception as exc:  # noqa: BLE001 - channel failures should not stop the whole run.
            wait = flood_wait_seconds(exc)
            channel["last_error"] = f"flood_wait_{wait}s" if wait else exc.__class__.__name__
            failed += 1
            continue
        max_message_id = int(channel.get("last_message_id") or 0)
        for raw_message in raw_messages:
            message = normalize_telegram_message(channel, raw_message, now)
            if not message.get("telegram_message_id"):
                continue
            status = upsert_telegram_message(state, message)
            inserted += int(status == "inserted")
            updated += int(status == "updated")
            unchanged += int(status == "unchanged")
            max_message_id = max(max_message_id, int(message.get("telegram_message_id") or 0))
            for match in match_message_to_articles(state, message, config):
                if upsert_article_match(state, match) == "inserted":
                    matches_inserted += 1
        channel["last_message_id"] = max_message_id
        channel["last_collected_at"] = datetime_to_iso(now)
        channel["last_error"] = None

    state["telegram_issue_signals"] = telegram_issue_signals(state)
    return {
        "telegram_channels": len(enabled_channels(state)),
        "telegram_messages_inserted": inserted,
        "telegram_messages_updated": updated,
        "telegram_messages_unchanged": unchanged,
        "telegram_matches_inserted": matches_inserted,
        "telegram_channel_failed": failed,
    }


async def _discover_with_client(state: dict[str, object], config: dict[str, object], now: datetime, client: TelegramMessageClient) -> dict[str, int]:
    settings = telegram_sources_config(config)
    if not settings.get("discover_enabled", False):
        return {"telegram_candidates_found": 0, "telegram_candidates_joined": 0}
    found = joined = 0
    recommendation_limit = int(settings.get("recommendation_limit", 20))
    for channel in enabled_channels(state):
        try:
            candidates = await client.recommend_channels(channel, limit=recommendation_limit)
        except Exception as exc:  # noqa: BLE001
            channel["last_recommendation_error"] = exc.__class__.__name__
            continue
        channel["last_recommendation_checked_at"] = datetime_to_iso(now)
        for candidate in candidates:
            candidate["last_checked_at"] = datetime_to_iso(now)
            upsert_channel_candidate(state, candidate)
            found += 1
    if settings.get("auto_join_enabled", False):
        joined = await auto_join_candidates(state, config, now, client)
    return {"telegram_candidates_found": found, "telegram_candidates_joined": joined}


async def auto_join_candidates(state: dict[str, object], config: dict[str, object], now: datetime, client: TelegramMessageClient) -> int:
    settings = telegram_sources_config(config)
    if not settings.get("auto_join_enabled", False):
        return 0
    daily_limit = int(settings.get("auto_join_daily_limit", 0))
    if daily_limit <= 0:
        return 0
    min_delay = float(settings.get("auto_join_delay_min_seconds", 3))
    max_delay = float(settings.get("auto_join_delay_max_seconds", 11))
    joined = 0
    for candidate in state.get("telegram_channel_candidates", []):
        if joined >= daily_limit:
            break
        if not isinstance(candidate, dict) or candidate.get("status") != "accepted":
            continue
        try:
            await asyncio.sleep(random.uniform(min_delay, max_delay))
            await client.join_channel(candidate)
            candidate["status"] = "joined"
            upsert_telegram_channel(state, {**candidate, "enabled": True, "joined": True, "source": "recommendation"})
            joined += 1
        except Exception as exc:  # noqa: BLE001
            wait = flood_wait_seconds(exc)
            candidate["status"] = "failed"
            candidate["failure_reason"] = f"flood_wait_{wait}s" if wait else exc.__class__.__name__
    return joined


def telegram_snapshot_payload(state: dict[str, object], config: dict[str, object]) -> dict[str, object]:
    ensure_telegram_state(state)
    settings = telegram_sources_config(config)
    max_messages = int(settings.get("max_remote_messages", 500))
    return {
        "channels": list(state.get("telegram_source_channels", [])),
        "messages": list(state.get("telegram_source_messages", []))[-max_messages:],
        "article_matches": list(state.get("telegram_article_matches", []))[-max_messages:],
        "issue_signals": list(state.get("telegram_issue_signals", [])),
        "channel_candidates": list(state.get("telegram_channel_candidates", [])),
    }


def sync_telegram_to_remote_api(state: dict[str, object], config: dict[str, object]) -> dict[str, int]:
    if not remote_api_configured():
        return {}
    try:
        response = post_remote_action("upsert_telegram_snapshot", telegram_snapshot_payload(state, config))
    except Exception:
        return {"telegram_remote_synced": 0, "telegram_remote_failed": 1}
    if response.get("ok"):
        return {
            "telegram_remote_synced": 1,
            "telegram_remote_failed": 0,
            "telegram_remote_messages": int(response.get("messages") or 0),
            "telegram_remote_matches": int(response.get("article_matches") or 0),
        }
    return {"telegram_remote_synced": 0, "telegram_remote_failed": 1}


def collect_telegram_sources(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
    client: TelegramMessageClient | None = None,
) -> dict[str, int]:
    ensure_telegram_state(state)
    registered = register_configured_channels(state, config)
    if not telegram_sources_enabled(config):
        return {"telegram_source_channels_registered": registered, "telegram_source_skipped": 1}

    owns_client = client is None
    if client is None:
        try:
            adapter = TelethonClientAdapter(config)
        except RuntimeError:
            return {"telegram_source_channels_registered": registered, "telegram_source_not_configured": 1}

        async def run_with_adapter() -> dict[str, int]:
            async with adapter as opened:
                summary = await _collect_with_client(state, config, now, opened)
                summary.update(await _discover_with_client(state, config, now, opened))
                return summary

        summary = asyncio.run(run_with_adapter())
    else:
        async def run_with_client() -> dict[str, int]:
            summary = await _collect_with_client(state, config, now, client)
            summary.update(await _discover_with_client(state, config, now, client))
            return summary

        summary = asyncio.run(run_with_client())

    summary["telegram_source_channels_registered"] = registered
    if summary.get("telegram_messages_inserted") or summary.get("telegram_messages_updated") or summary.get("telegram_matches_inserted"):
        summary.update(sync_telegram_to_remote_api(state, config))
    state.setdefault("telegram_source_runs", [])
    state["telegram_source_runs"].append({"ran_at": datetime_to_iso(now), **summary})  # type: ignore[index, union-attr]
    return summary


def project_root_from_cwd() -> Path:
    return Path.cwd()


def state_path_for_root(root: Path) -> Path:
    return root / "data" / "state.json"


def cli_channel_table(channels: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        {
            "handle": channel.get("handle") or "",
            "telegram_channel_id": channel.get("telegram_channel_id") or "",
            "title": channel.get("title") or "",
            "enabled": bool(channel.get("enabled", True)),
            "joined": bool(channel.get("joined", False)),
            "source": channel.get("source") or "",
            "quality_score": int(channel.get("quality_score") or 0),
            "last_message_id": int(channel.get("last_message_id") or 0),
            "last_collected_at": channel.get("last_collected_at") or "",
            "last_error": channel.get("last_error") or "",
        }
        for channel in channels
        if isinstance(channel, dict)
    ]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage Telegram public-channel sources for the RSS curator.")
    parser.add_argument("--root", default=".", help="Project root containing config.yaml and data/state.json")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="List configured Telegram source channels")
    add_parser = subparsers.add_parser("add", help="Add or update a manual public channel source")
    add_parser.add_argument("handle")
    add_parser.add_argument("--title", default="")
    add_parser.add_argument("--disabled", action="store_true")

    disable_parser = subparsers.add_parser("disable", help="Disable a source channel")
    disable_parser.add_argument("handle")

    enable_parser = subparsers.add_parser("enable", help="Enable a source channel")
    enable_parser.add_argument("handle")

    subparsers.add_parser("candidates", help="List discovered candidate channels")
    subparsers.add_parser("collect", help="Run one Telegram source collection pass")
    return parser


def cli_main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    root = Path(args.root).resolve()
    config = load_config(root / "config.yaml")
    state_path = state_path_for_root(root)
    state = load_state(state_path)
    ensure_telegram_state(state)

    if args.command == "list":
        register_configured_channels(state, config)
        print(json.dumps(cli_channel_table(list(state.get("telegram_source_channels", []))), ensure_ascii=False, indent=2))
        return 0
    if args.command == "add":
        record = upsert_telegram_channel(
            state,
            {
                "handle": args.handle,
                "title": args.title,
                "enabled": not args.disabled,
                "joined": False,
                "source": "manual",
            },
        )
        save_state(state_path, state)
        print(json.dumps(cli_channel_table([record])[0], ensure_ascii=False, indent=2))
        return 0
    if args.command in {"enable", "disable"}:
        target = normalize_channel_handle(args.handle)
        changed = False
        for channel in state.get("telegram_source_channels", []):
            if isinstance(channel, dict) and normalize_channel_handle(channel.get("handle") or channel.get("username")) == target:
                channel["enabled"] = args.command == "enable"
                changed = True
        save_state(state_path, state)
        print(json.dumps({"ok": changed, "handle": target, "enabled": args.command == "enable"}, ensure_ascii=False))
        return 0 if changed else 1
    if args.command == "candidates":
        print(json.dumps(list(state.get("telegram_channel_candidates", [])), ensure_ascii=False, indent=2))
        return 0
    if args.command == "collect":
        from .dates import now_in_timezone

        now = now_in_timezone(str(config.get("timezone") or "Asia/Seoul"))
        summary = collect_telegram_sources(state, config, now)
        save_state(state_path, state)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(cli_main())
