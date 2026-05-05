from __future__ import annotations

import asyncio
import argparse
import json
import os
import random
import re
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from .config import load_config
from .dates import datetime_to_iso, parse_datetime
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
    "속보",
    "시장",
    "자본시장",
    "기업",
    "주주",
    "증권",
    "금융",
    "경제",
    "공시",
    "https",
    "http",
    "www",
    "com",
    "co",
    "kr",
    "html",
    "article",
    "articleview",
    "idxno",
    "news",
    "utm",
    "rss",
}

try:  # Windows PowerShell often defaults to cp949 even after WSL launches python.exe.
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass
PUBLIC_CHANNEL_SOURCE_TYPES = {"public_channel", "telegram_public_channel", "channel"}
NON_COLLECTABLE_SOURCE_TYPES = {
    "private_chat",
    "user",
    "bot",
    "saved_messages",
    "basic_group",
    "group",
    "public_group",
    "megagroup",
    "supergroup",
}

class TelegramFloodWait(Exception):
    def __init__(self, seconds: int) -> None:
        super().__init__(f"Telegram FloodWait: {seconds}s")
        self.seconds = seconds


class TelegramUnsafeSource(Exception):
    """Raised when a Telegram entity is not a public broadcast channel."""


class TelegramMessageClient(Protocol):
    async def get_channel_info(self, channel: dict[str, object]) -> dict[str, object]:
        ...

    async def iter_messages(
        self,
        channel: dict[str, object],
        *,
        min_id: int,
        limit: int,
        since: datetime | None = None,
    ) -> list[dict[str, object]]:
        ...

    async def recommend_channels(self, seed_channel: dict[str, object], *, limit: int) -> list[dict[str, object]]:
        ...

    async def join_channel(self, candidate: dict[str, object]) -> dict[str, object]:
        ...

    async def list_joined_public_channels(self, *, limit: int) -> list[dict[str, object]]:
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
        raw_channel.setdefault("source_type", "public_channel")
        raw_channel.setdefault("is_public_channel", True)
        raw_channel.setdefault("quality_score", score_channel_candidate(raw_channel))
        normalized.append(raw_channel)
    return normalized


def is_collectable_public_channel(channel: dict[str, object]) -> bool:
    handle = normalize_channel_handle(channel.get("handle") or channel.get("username"))
    if not handle:
        return False
    source_type = str(channel.get("source_type") or "public_channel").strip().casefold()
    if source_type in NON_COLLECTABLE_SOURCE_TYPES:
        return False
    if source_type and source_type not in PUBLIC_CHANNEL_SOURCE_TYPES:
        return False
    if channel.get("is_public_channel") is False:
        return False
    if channel.get("is_private_chat") or channel.get("is_saved_messages") or channel.get("is_group"):
        return False
    return True


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
        "source_type": channel.get("source_type") or "public_channel",
        "is_public_channel": bool(channel.get("is_public_channel", True)),
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
        if isinstance(channel, dict) and bool(channel.get("enabled", True)) and is_collectable_public_channel(channel)
    ]


def load_env_files(root: Path, names: tuple[str, ...] = (".env", ".env.local", ".env.telegram")) -> list[Path]:
    loaded: list[Path] = []
    for name in names:
        path = root / name
        if not path.exists():
            continue
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
        loaded.append(path)
    return loaded


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
        "source_type": "public_channel",
        "is_public_channel": True,
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
    text = URL_PATTERN.sub(" ", text)
    tokens = {token.casefold() for token in re.findall(r"[0-9A-Za-z가-힣]{2,}", text)}
    tokens.update(title_signature(article.get("title")))
    return {token for token in tokens if token not in GENERIC_MATCH_TOKENS}


def title_signature(value: object) -> set[str]:
    normalized = normalize_title(str(value or ""))
    tokens = {token.casefold() for token in re.findall(r"[0-9A-Za-z가-힣]{2,}", normalized)}
    return {token for token in tokens if token not in GENERIC_MATCH_TOKENS}


def message_tokens(message: dict[str, object]) -> set[str]:
    text = URL_PATTERN.sub(" ", str(message.get("normalized_text") or message.get("text") or ""))
    tokens = {token.casefold() for token in re.findall(r"[0-9A-Za-z가-힣]{2,}", text)}
    return {token for token in tokens if token not in GENERIC_MATCH_TOKENS}


def weak_match_within_window(
    message: dict[str, object],
    article: dict[str, object],
    *,
    timezone_name: str,
    window_hours: int,
) -> bool:
    message_dt = parse_datetime(message.get("posted_at"), timezone_name)
    article_dt = parse_datetime(article.get("published_at") or article.get("seen_at"), timezone_name)
    if not message_dt or not article_dt:
        return True
    return abs((message_dt - article_dt).total_seconds()) <= window_hours * 3600


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
    min_strong_overlap = int(settings.get("weak_match_min_strong_overlap", 1))
    window_hours = int(settings.get("weak_match_window_hours", 168))
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    tokens = message_tokens(message)
    if not tokens:
        return []
    for article in state.get("articles", []):
        if not isinstance(article, dict):
            continue
        if not weak_match_within_window(message, article, timezone_name=timezone_name, window_hours=window_hours):
            continue
        overlap = sorted(tokens & article_tokens(article))
        if len(overlap) < min_overlap:
            continue
        strong_overlap = [token for token in overlap if len(token) >= 3 or re.search(r"\d", token)]
        if len(strong_overlap) < min_strong_overlap:
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
        "source_type": candidate.get("source_type") or "public_channel",
        "is_public_channel": bool(candidate.get("is_public_channel", True)),
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

    def _public_broadcast_record(self, entity: object, fallback: dict[str, object]) -> dict[str, object]:
        handle = normalize_channel_handle(getattr(entity, "username", "") or fallback.get("handle") or fallback.get("username"))
        is_broadcast = bool(getattr(entity, "broadcast", False))
        is_group = bool(getattr(entity, "megagroup", False) or getattr(entity, "gigagroup", False))
        if not handle or not is_broadcast or is_group:
            raise TelegramUnsafeSource("not_public_broadcast_channel")
        record = {
            "handle": handle,
            "telegram_channel_id": getattr(entity, "id", None) or fallback.get("telegram_channel_id") or fallback.get("channel_id"),
            "title": getattr(entity, "title", None) or fallback.get("title") or "",
            "description": getattr(entity, "about", None) or fallback.get("description") or "",
            "joined": True,
            "source_type": "public_channel",
            "is_public_channel": True,
        }
        record["quality_score"] = int(fallback.get("quality_score") or score_channel_candidate(record))
        return record

    async def _get_public_broadcast_entity(self, channel: dict[str, object]) -> tuple[object, dict[str, object]]:
        handle = normalize_channel_handle(channel.get("handle") or channel.get("username"))
        if not handle:
            raise TelegramUnsafeSource("public_channel_handle_required")
        entity = await self.client.get_entity(handle)
        return entity, self._public_broadcast_record(entity, channel)

    async def get_channel_info(self, channel: dict[str, object]) -> dict[str, object]:
        _entity, record = await self._get_public_broadcast_entity(channel)
        return record

    async def iter_messages(
        self,
        channel: dict[str, object],
        *,
        min_id: int,
        limit: int,
        since: datetime | None = None,
    ) -> list[dict[str, object]]:
        entity, _record = await self._get_public_broadcast_entity(channel)
        messages: list[dict[str, object]] = []
        iter_kwargs: dict[str, object] = {"limit": limit}
        if since is not None:
            iter_kwargs["reverse"] = False
        elif min_id:
            iter_kwargs["min_id"] = min_id
            iter_kwargs["reverse"] = True
        else:
            iter_kwargs["reverse"] = False
        async for message in self.client.iter_messages(entity, **iter_kwargs):
            message_date = parse_datetime(getattr(message, "date", None), "Asia/Seoul")
            if since is not None and message_date and message_date < since:
                break
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
        if since is not None or not min_id:
            messages.reverse()
        return messages

    async def recommend_channels(self, seed_channel: dict[str, object], *, limit: int) -> list[dict[str, object]]:
        try:
            from telethon.tl.functions.channels import GetChannelRecommendationsRequest  # type: ignore
        except ImportError:  # pragma: no cover
            return []

        entity, _record = await self._get_public_broadcast_entity(seed_channel)
        result = await self.client(GetChannelRecommendationsRequest(entity))
        raw_chats = list(getattr(result, "chats", []) or [])
        candidates: list[dict[str, object]] = []
        for chat in raw_chats:
            try:
                record = self._public_broadcast_record(chat, {"source": "recommendation"})
            except TelegramUnsafeSource:
                continue
            record["source"] = "recommendation"
            record["status"] = "pending"
            record["quality_score"] = score_channel_candidate(record)
            candidates.append(record)
            if len(candidates) >= limit:
                break
        return candidates

    async def join_channel(self, candidate: dict[str, object]) -> dict[str, object]:
        from telethon.tl.functions.channels import JoinChannelRequest  # type: ignore

        entity, _record = await self._get_public_broadcast_entity(candidate)
        result = await self.client(JoinChannelRequest(entity))
        return {"ok": True, "result": str(result)[:120]}

    async def list_joined_public_channels(self, *, limit: int) -> list[dict[str, object]]:
        channels: list[dict[str, object]] = []
        async for dialog in self.client.iter_dialogs(limit=limit):
            entity = getattr(dialog, "entity", None)
            try:
                record = self._public_broadcast_record(entity, {"source": "discovered"})
            except TelegramUnsafeSource:
                continue
            title = str(getattr(dialog, "title", "") or getattr(entity, "title", "") or "")
            record["title"] = title or record.get("title") or ""
            record["source"] = "discovered"
            record["quality_score"] = score_channel_candidate(record)
            channels.append(record)
        return channels


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


async def _import_joined_with_client(
    state: dict[str, object],
    client: TelegramMessageClient,
    *,
    limit: int,
    enable: bool,
    min_quality: int,
    source: str,
) -> dict[str, int]:
    ensure_telegram_state(state)
    imported = updated = skipped = enabled_count = 0
    existing_keys = {
        channel_key(channel)
        for channel in state.get("telegram_source_channels", [])
        if isinstance(channel, dict)
    }
    joined_channels = await client.list_joined_public_channels(limit=limit)
    for channel in joined_channels:
        if not is_collectable_public_channel(channel):
            skipped += 1
            continue
        score = int(channel.get("quality_score") or score_channel_candidate(channel))
        if score < min_quality:
            skipped += 1
            continue
        payload = dict(channel)
        payload["quality_score"] = score
        payload["joined"] = True
        key = channel_key(payload)
        is_existing = key in existing_keys
        if is_existing:
            payload.pop("source", None)
            if enable:
                payload["enabled"] = True
                enabled_count += 1
            else:
                payload.pop("enabled", None)
            updated += 1
        else:
            payload["source"] = source
            payload["enabled"] = enable
            imported += 1
            enabled_count += int(enable)
            existing_keys.add(key)
        upsert_telegram_channel(state, payload)
    return {
        "telegram_joined_seen": len(joined_channels),
        "telegram_joined_imported": imported,
        "telegram_joined_updated": updated,
        "telegram_joined_skipped_low_quality": skipped,
        "telegram_joined_enabled": enabled_count,
    }


def import_joined_public_channels(
    state: dict[str, object],
    config: dict[str, object],
    *,
    limit: int = 500,
    enable: bool = False,
    min_quality: int = 0,
    source: str = "discovered",
    client: TelegramMessageClient | None = None,
) -> dict[str, int]:
    if client is not None:
        return asyncio.run(
            _import_joined_with_client(
                state,
                client,
                limit=limit,
                enable=enable,
                min_quality=min_quality,
                source=source,
            )
        )

    adapter = TelethonClientAdapter(config)

    async def run_with_adapter() -> dict[str, int]:
        async with adapter as opened:
            return await _import_joined_with_client(
                state,
                opened,
                limit=limit,
                enable=enable,
                min_quality=min_quality,
                source=source,
            )

    return asyncio.run(run_with_adapter())


def make_telegram_session_string(config: dict[str, object]) -> str:
    try:
        from telethon import TelegramClient  # type: ignore
        from telethon.sessions import StringSession  # type: ignore
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("Telethon is not installed") from exc
    settings = telegram_sources_config(config)
    api_id = int(os.environ.get("TELEGRAM_API_ID") or settings.get("api_id") or 0)
    api_hash = os.environ.get("TELEGRAM_API_HASH", "").strip() or str(settings.get("api_hash") or "")
    if not api_id or not api_hash:
        raise RuntimeError("TELEGRAM_API_ID/TELEGRAM_API_HASH is required")

    async def create_session() -> str:
        client = TelegramClient(StringSession(), api_id, api_hash)
        await client.start()
        session = client.session.save()
        await client.disconnect()
        return str(session)

    return asyncio.run(create_session())


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


def sync_telegram_batch_to_remote_api(
    state: dict[str, object],
    config: dict[str, object],
    *,
    messages: list[dict[str, object]],
    matches: list[dict[str, object]],
) -> dict[str, int]:
    if not remote_api_configured() or not messages:
        return {}
    settings = telegram_sources_config(config)
    batch_size = max(1, int(settings.get("remote_batch_size", 300)))
    synced = failed = remote_messages = remote_matches = 0
    channels = list(state.get("telegram_source_channels", []))
    signals = list(state.get("telegram_issue_signals", []))
    candidates = list(state.get("telegram_channel_candidates", []))
    message_keys = {message_key(message) for message in messages}
    relevant_matches = [
        match for match in matches if isinstance(match, dict) and str(match.get("telegram_message_key") or "") in message_keys
    ]
    for index in range(0, len(messages), batch_size):
        chunk = messages[index : index + batch_size]
        chunk_keys = {message_key(message) for message in chunk}
        chunk_matches = [
            match
            for match in relevant_matches
            if isinstance(match, dict) and str(match.get("telegram_message_key") or "") in chunk_keys
        ]
        try:
            response = post_remote_action(
                "upsert_telegram_snapshot",
                {
                    "channels": channels,
                    "messages": chunk,
                    "article_matches": chunk_matches,
                    "issue_signals": signals,
                    "channel_candidates": candidates,
                },
            )
        except Exception:
            failed += 1
            continue
        if response.get("ok"):
            synced += 1
            remote_messages += int(response.get("messages") or len(chunk))
            remote_matches += int(response.get("article_matches") or len(chunk_matches))
        else:
            failed += 1
    return {
        "telegram_remote_synced": synced,
        "telegram_remote_failed": failed,
        "telegram_remote_messages": remote_messages,
        "telegram_remote_matches": remote_matches,
    }


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


async def _backfill_messages_with_client(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
    client: TelegramMessageClient,
    *,
    days: int,
    limit_per_channel: int,
    channel_limit: int,
    progress: bool = False,
) -> dict[str, object]:
    settings = telegram_sources_config(config)
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    channel_timeout = max(5.0, float(settings.get("backfill_channel_timeout_seconds", 60)))
    since = now - timedelta(days=max(1, days))
    channels = enabled_channels(state)
    if channel_limit > 0:
        channels = channels[:channel_limit]

    inserted = updated = unchanged = failed = seen = outside_window = matches_inserted = 0
    touched_messages: list[dict[str, object]] = []
    touched_matches: list[dict[str, object]] = []
    per_channel: list[dict[str, object]] = []

    for index, channel in enumerate(channels, start=1):
        channel_seen = channel_inserted = channel_updated = channel_failed = 0
        started_at = datetime.now()
        if progress:
            print(f"[{index}/{len(channels)}] @{channel.get('handle') or ''} backfill start", flush=True)
        try:
            async def fetch_channel() -> tuple[dict[str, object], list[dict[str, object]]]:
                info = await client.get_channel_info(channel)
                channel.update(info)
                raw_messages = await client.iter_messages(channel, min_id=0, limit=limit_per_channel, since=since)
                return info, raw_messages

            _info, raw_messages = await asyncio.wait_for(fetch_channel(), timeout=channel_timeout)
        except Exception as exc:  # noqa: BLE001
            wait = flood_wait_seconds(exc)
            channel["last_error"] = f"flood_wait_{wait}s" if wait else ("timeout" if isinstance(exc, TimeoutError) else exc.__class__.__name__)
            failed += 1
            channel_failed = 1
            per_channel.append(
                {
                    "handle": channel.get("handle") or "",
                    "title": channel.get("title") or "",
                    "status": "failed",
                    "error": channel.get("last_error") or "",
                    "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 2),
                    "index": index,
                    "total": len(channels),
                }
            )
            if progress:
                print(
                    f"[{index}/{len(channels)}] @{channel.get('handle') or ''} failed={channel.get('last_error')} "
                    f"elapsed={round((datetime.now() - started_at).total_seconds(), 1)}s",
                    flush=True,
                )
            continue

        max_message_id = int(channel.get("last_message_id") or 0)
        for raw_message in raw_messages:
            message = normalize_telegram_message(channel, raw_message, now)
            posted_at = parse_datetime(message.get("posted_at"), timezone_name)
            if posted_at and posted_at < since:
                outside_window += 1
                continue
            if not message.get("telegram_message_id"):
                continue
            seen += 1
            channel_seen += 1
            status = upsert_telegram_message(state, message)
            inserted += int(status == "inserted")
            updated += int(status == "updated")
            unchanged += int(status == "unchanged")
            channel_inserted += int(status == "inserted")
            channel_updated += int(status == "updated")
            touched_messages.append(message)
            max_message_id = max(max_message_id, int(message.get("telegram_message_id") or 0))
            for match in match_message_to_articles(state, message, config):
                if upsert_article_match(state, match) == "inserted":
                    matches_inserted += 1
                    touched_matches.append(match)
        channel["last_message_id"] = max(max_message_id, int(channel.get("last_message_id") or 0))
        channel["last_collected_at"] = datetime_to_iso(now)
        channel["last_error"] = None
        per_channel.append(
            {
                "handle": channel.get("handle") or "",
                "title": channel.get("title") or "",
                "status": "ok" if not channel_failed else "failed",
                "messages_seen": channel_seen,
                "inserted": channel_inserted,
                "updated": channel_updated,
                "elapsed_seconds": round((datetime.now() - started_at).total_seconds(), 2),
                "index": index,
                "total": len(channels),
            }
        )
        if progress:
            print(
                f"[{index}/{len(channels)}] @{channel.get('handle') or ''} "
                f"seen={channel_seen} inserted={channel_inserted} updated={channel_updated} "
                f"elapsed={round((datetime.now() - started_at).total_seconds(), 1)}s",
                flush=True,
            )

    state["telegram_issue_signals"] = telegram_issue_signals(state)
    summary: dict[str, object] = {
        "telegram_backfill_channels": len(channels),
        "telegram_backfill_days": max(1, days),
        "telegram_backfill_since": datetime_to_iso(since),
        "telegram_backfill_messages_seen": seen,
        "telegram_messages_inserted": inserted,
        "telegram_messages_updated": updated,
        "telegram_messages_unchanged": unchanged,
        "telegram_matches_inserted": matches_inserted,
        "telegram_channel_failed": failed,
        "telegram_messages_outside_window": outside_window,
        "telegram_backfill_per_channel": per_channel,
        "_touched_messages": touched_messages,
        "_touched_matches": touched_matches,
    }
    if settings.get("estimate_storage_bytes", True):
        sample = touched_messages[-500:] if len(touched_messages) > 500 else touched_messages
        if sample:
            sample_bytes = len(json.dumps(sample, ensure_ascii=False, sort_keys=True).encode("utf-8"))
            avg_message_bytes = max(1, round(sample_bytes / len(sample)))
            daily_messages = seen / max(1, days)
            summary["telegram_estimated_avg_message_bytes"] = avg_message_bytes
            summary["telegram_estimated_daily_messages"] = round(daily_messages, 1)
            summary["telegram_estimated_monthly_messages"] = round(daily_messages * 30)
            summary["telegram_estimated_yearly_messages"] = round(daily_messages * 365)
            summary["telegram_estimated_monthly_mb"] = round(daily_messages * 30 * avg_message_bytes / 1024 / 1024, 2)
            summary["telegram_estimated_yearly_mb"] = round(daily_messages * 365 * avg_message_bytes / 1024 / 1024, 2)
    return summary


def backfill_telegram_messages(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
    *,
    days: int = 14,
    limit_per_channel: int = 1000,
    channel_limit: int = 0,
    client: TelegramMessageClient | None = None,
    sync_remote: bool = True,
    progress: bool = False,
) -> dict[str, object]:
    ensure_telegram_state(state)
    register_configured_channels(state, config)
    owns_client = client is None
    if client is None:
        adapter = TelethonClientAdapter(config)

        async def run_with_adapter() -> dict[str, object]:
            async with adapter as opened:
                return await _backfill_messages_with_client(
                    state,
                    config,
                    now,
                    opened,
                    days=days,
                    limit_per_channel=limit_per_channel,
                    channel_limit=channel_limit,
                    progress=progress,
                )

        summary = asyncio.run(run_with_adapter())
    else:
        async def run_with_client() -> dict[str, object]:
            return await _backfill_messages_with_client(
                state,
                config,
                now,
                client,
                days=days,
                limit_per_channel=limit_per_channel,
                channel_limit=channel_limit,
                progress=progress,
            )

        summary = asyncio.run(run_with_client())

    touched_messages = [message for message in summary.pop("_touched_messages", []) if isinstance(message, dict)]
    touched_matches = [match for match in summary.pop("_touched_matches", []) if isinstance(match, dict)]
    if sync_remote and not owns_client and touched_messages:
        summary.update(sync_telegram_batch_to_remote_api(state, config, messages=touched_messages, matches=touched_matches))
    elif sync_remote and touched_messages:
        summary.update(sync_telegram_batch_to_remote_api(state, config, messages=touched_messages, matches=touched_matches))
    state.setdefault("telegram_source_runs", [])
    state["telegram_source_runs"].append({"ran_at": datetime_to_iso(now), "mode": "backfill", **{k: v for k, v in summary.items() if isinstance(v, (int, float, str))}})  # type: ignore[index, union-attr]
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

    discover_parser = subparsers.add_parser("discover", help="Discover similar public-channel candidates from enabled seed channels")
    discover_parser.add_argument("--limit", type=int, default=20, help="Maximum recommendations per seed channel")
    discover_parser.add_argument("--dry-run", action="store_true", help="Discover and print a summary without writing state.json")

    backfill_parser = subparsers.add_parser("backfill-messages", help="Backfill public-channel messages for a historical window")
    backfill_parser.add_argument("--days", type=int, default=14, help="How many days back to collect")
    backfill_parser.add_argument("--limit-per-channel", type=int, default=1000, help="Maximum messages to scan per channel")
    backfill_parser.add_argument("--channel-limit", type=int, default=0, help="Limit number of enabled channels, 0 means all")
    backfill_parser.add_argument("--dry-run", action="store_true", help="Run against a state copy without writing state.json or remote DB")
    backfill_parser.add_argument("--no-remote", action="store_true", help="Do not sync collected messages to the remote DB API")

    import_parser = subparsers.add_parser("import-joined", help="Import public channels already joined by the Telegram reader account")
    import_parser.add_argument("--limit", type=int, default=500, help="Maximum dialogs to scan")
    import_parser.add_argument("--min-quality", type=int, default=0, help="Skip channels below this quality score")
    import_parser.add_argument("--enable", action="store_true", help="Enable imported channels for collection immediately")
    import_parser.add_argument("--dry-run", action="store_true", help="Scan and print a summary without writing state.json")

    session_parser = subparsers.add_parser("make-session", help="Interactively create a TELEGRAM_SESSION_STRING for GitHub Actions")
    session_parser.add_argument("--out", default="", help="Optional local env file to write TELEGRAM_SESSION_STRING into, e.g. .env.telegram")
    return parser


def cli_main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    root = Path(args.root).resolve()
    load_env_files(root)
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
    if args.command == "discover":
        from .dates import now_in_timezone

        target_state = json.loads(json.dumps(state, ensure_ascii=False)) if args.dry_run else state
        target_config = dict(config)
        telegram_settings = dict(telegram_sources_config(config))
        telegram_settings["discover_enabled"] = True
        telegram_settings["recommendation_limit"] = max(1, int(args.limit))
        target_config["telegram_sources"] = telegram_settings
        now = now_in_timezone(str(config.get("timezone") or "Asia/Seoul"))
        adapter = TelethonClientAdapter(target_config)

        async def run_discover() -> dict[str, int]:
            async with adapter as opened:
                return await _discover_with_client(target_state, target_config, now, opened)

        summary = asyncio.run(run_discover())
        if not args.dry_run:
            save_state(state_path, target_state)
        summary["dry_run"] = int(bool(args.dry_run))
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    if args.command == "backfill-messages":
        from .dates import now_in_timezone

        target_state = json.loads(json.dumps(state, ensure_ascii=False)) if args.dry_run else state
        now = now_in_timezone(str(config.get("timezone") or "Asia/Seoul"))
        summary = backfill_telegram_messages(
            target_state,
            config,
            now,
            days=max(1, int(args.days)),
            limit_per_channel=max(1, int(args.limit_per_channel)),
            channel_limit=max(0, int(args.channel_limit)),
            sync_remote=not args.dry_run and not args.no_remote,
            progress=True,
        )
        if not args.dry_run:
            save_state(state_path, target_state)
        summary["dry_run"] = int(bool(args.dry_run))
        summary["remote_disabled"] = int(bool(args.no_remote))
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    if args.command == "import-joined":
        target_state = json.loads(json.dumps(state, ensure_ascii=False)) if args.dry_run else state
        summary = import_joined_public_channels(
            target_state,
            config,
            limit=max(1, int(args.limit)),
            enable=bool(args.enable),
            min_quality=max(0, int(args.min_quality)),
        )
        if not args.dry_run:
            save_state(state_path, target_state)
        summary["dry_run"] = int(bool(args.dry_run))
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    if args.command == "make-session":
        session = make_telegram_session_string(config)
        if args.out:
            out_path = (root / str(args.out)).resolve()
            existing = out_path.read_text(encoding="utf-8") if out_path.exists() else ""
            lines = [line for line in existing.splitlines() if not line.startswith("TELEGRAM_SESSION_STRING=")]
            lines.append(f"TELEGRAM_SESSION_STRING={session}")
            out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            print(json.dumps({"ok": True, "written": str(out_path)}, ensure_ascii=False, indent=2))
        else:
            print(session)
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(cli_main())
