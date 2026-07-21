from __future__ import annotations

import asyncio
import argparse
import hashlib
import json
import os
import random
import re
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Protocol
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from .config import load_config
from .dates import datetime_to_iso, parse_datetime
from .normalize import (
    canonical_url_hash,
    hostname_from_url,
    normalize_title,
    normalize_title_parts,
    normalize_url,
    stable_hash,
)
from .remote_api import post_remote_action, remote_api_configured
from .source_rights import apply_channel_source_rights, source_is_authorized
from .state import load_state, save_state


URL_PATTERN = re.compile(r"https?://[^\s<>()\"']+", re.IGNORECASE)
TRAILING_URL_CHARS = ".,;:!?)]}>\u3002"
REMOTE_TELEGRAM_SIGNAL_BATCH_SIZE = 500
REMOTE_TELEGRAM_MAX_CHANNEL_BATCH_SIZE = 20
REMOTE_TELEGRAM_DEFAULT_CHANNEL_BATCH_SIZE = 5
REMOTE_TELEGRAM_SAFE_PAYLOAD_BYTES = 1_750_000
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
STRONG_CHANNEL_KEYWORDS = {
    "공시",
    "실적",
    "증권사",
    "리서치",
    "리포트",
    "기업분석",
    "시장",
    "매크로",
    "금리",
    "채권",
    "환율",
    "해외주식",
    "글로벌",
    "자본시장",
    "지배구조",
    "주주",
    "밸류업",
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
MARKET_SENSITIVE_KEYWORDS = {
    "상장폐지",
    "거래정지",
    "불성실공시",
    "감사의견",
    "공개매수",
    "유상증자",
}
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
SIGNAL_STOP_TOKENS = GENERIC_MATCH_TOKENS | {
    "amp",
    "api",
    "channel",
    "daily",
    "flashnews",
    "id",
    "interface",
    "investment",
    "pdf",
    "qoq",
    "rd",
    "report",
    "review",
    "krx",
    "stock",
    "url",
    "naver",
    "signal",
    "yoy",
    "telegram",
    "clt",
    "억원",
    "경우",
    "견조한",
    "금액",
    "기업명",
    "기업분석",
    "기업정보",
    "대비",
    "거래",
    "공시링크",
    "공유",
    "구독",
    "기준",
    "내용",
    "내일",
    "대한",
    "링크",
    "리포트",
    "목표가",
    "매출",
    "매출액",
    "매일",
    "미디어",
    "바른",
    "보기",
    "브리핑",
    "분석",
    "비중",
    "시가총액",
    "순이익",
    "상위",
    "서울경제",
    "시그널",
    "예상",
    "예상치",
    "영업익",
    "영업이익",
    "오늘",
    "오전",
    "오후",
    "올해",
    "영업",
    "이번",
    "있는",
    "전년",
    "전망",
    "자료",
    "정보",
    "주요",
    "종목",
    "주식",
    "채널",
    "최근",
    "공정공시",
    "기업명",
    "회사정보",
    "보고서명",
    "잠정",
    "잠정실적",
    "추이",
    "연결재무제표기준영업",
    "컨버전스",
    "투자의",
    "투자",
    "프리미엄",
    "합니다",
    "했습니다",
    "한다",
    "했다",
    "현재",
    "확인",
}
SIGNAL_STOP_SUBSTRINGS = {
    "rassiro",
    "sedaily",
    "stockinfo",
    "telegram",
    "한국투자증권",
    "한투증권",
}
SIGNAL_STOP_DOMAIN_SUFFIXES = {
    ".com",
    ".co.kr",
    ".kr",
    ".net",
    ".org",
    ".io",
    ".ai",
}
AMBIGUOUS_ENGLISH_BOARD_TOKENS = {"board", "boards", "director", "directors"}
BOARD_GOVERNANCE_CONTEXT_TOKENS = {
    "activist",
    "activism",
    "boardroom",
    "campaign",
    "contest",
    "director",
    "directors",
    "governance",
    "nomination",
    "nominee",
    "nominees",
    "proxy",
    "settlement",
    "shareholder",
    "shareholders",
    "stewardship",
}
BOARD_PRODUCT_CONTEXT_TOKENS = {
    "clt",
    "circuit",
    "contract",
    "controller",
    "interface",
    "mainboard",
    "motherboard",
    "pcb",
    "supply",
    "공급계약체결",
    "계약내용",
    "계약상대",
}
SIGNAL_EVENT_LABELS = {
    "activist": "행동주의",
    "activism": "행동주의",
    "board": "이사회",
    "buyback": "자사주",
    "campaign": "행동주의 캠페인",
    "contest": "표대결",
    "delisting": "상장폐지",
    "director": "이사회",
    "dividend": "배당",
    "governance": "지배구조",
    "proxy": "위임장",
    "settlement": "합의",
    "shareholder": "주주",
    "stake": "지분",
    "stewardship": "스튜어드십",
    "tender": "공개매수",
    "감리": "감리",
    "감사의견": "감사의견",
    "감자": "감자",
    "거래정지": "거래정지",
    "검찰": "검찰",
    "경영권": "경영권",
    "고발": "고발",
    "공개매수": "공개매수",
    "공개서한": "공개서한",
    "교체": "이사회 교체",
    "금감원": "감독당국",
    "노조": "노조",
    "리스크": "리스크",
    "물적분할": "물적분할",
    "배당": "배당",
    "밸류업": "밸류업",
    "불성실공시": "불성실공시",
    "분쟁": "분쟁",
    "분할": "분할",
    "상장폐지": "상장폐지",
    "선임": "선임",
    "소각": "자사주 소각",
    "소송": "소송",
    "소액주주": "소액주주",
    "스튜어드십": "스튜어드십",
    "실적": "실적",
    "위임장": "위임장",
    "유상증자": "유상증자",
    "의결권": "의결권",
    "이사회": "이사회",
    "자사주": "자사주",
    "정정": "정정",
    "제재": "제재",
    "주주권": "주주권",
    "주주제안": "주주제안",
    "주주총회": "주주총회",
    "주주환원": "주주환원",
    "지배구조": "지배구조",
    "합병": "합병",
    "해임": "해임",
}
WEAK_MATCH_EVENT_TOKENS = {
    "activist",
    "activism",
    "board",
    "buyback",
    "campaign",
    "contest",
    "delisting",
    "director",
    "dividend",
    "governance",
    "letter",
    "nominee",
    "nomination",
    "proxy",
    "settlement",
    "shareholder",
    "stake",
    "stewardship",
    "tender",
    "감리",
    "감사",
    "감사의견",
    "감자",
    "거래소",
    "거래정지",
    "검찰",
    "경영권",
    "고발",
    "공개매수",
    "공개서한",
    "교체",
    "금감원",
    "노조",
    "리스크",
    "물적분할",
    "배당",
    "밸류업",
    "불성실공시",
    "분쟁",
    "분할",
    "상장폐지",
    "선임",
    "소각",
    "소송",
    "소액주주",
    "스튜어드십",
    "실적",
    "위임장",
    "유상증자",
    "의결권",
    "이사회",
    "자사주",
    "정정",
    "제재",
    "주주권",
    "주주제안",
    "주주총회",
    "주주환원",
    "지배구조",
    "합병",
    "해임",
}
WEAK_MATCH_EVENT_SUBSTRINGS = {
    "감사",
    "거래정지",
    "경영권",
    "공개매수",
    "물적분할",
    "밸류업",
    "불성실공시",
    "상장폐지",
    "소액주주",
    "스튜어드십",
    "위임장",
    "유상증자",
    "자사주",
    "주주제안",
    "주주환원",
    "지배구조",
}


@dataclass(frozen=True)
class ArticleTokenRecord:
    article: dict[str, object]
    tokens: set[str]
    article_dt: datetime | None


@dataclass(frozen=True)
class TelegramArticleMatchContext:
    url_index: dict[str, dict[str, object]]
    article_tokens: list[ArticleTokenRecord]
    token_index: dict[str, list[int]]


@dataclass
class TelegramBackfillFetchResult:
    index: int
    total: int
    channel: dict[str, object]
    channel_info: dict[str, object]
    raw_messages: list[dict[str, object]]
    fetch_truncated: bool
    next_max_id: int
    error: BaseException | None
    started_at: datetime
    monotonic_started_at: float
    fetch_elapsed_seconds: float


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
    async def get_channel_info(
        self, channel: dict[str, object]
    ) -> dict[str, object]: ...

    async def iter_messages(
        self,
        channel: dict[str, object],
        *,
        min_id: int,
        limit: int,
        since: datetime | None = None,
        max_id: int = 0,
    ) -> list[dict[str, object]]: ...

    async def recommend_channels(
        self, seed_channel: dict[str, object], *, limit: int
    ) -> list[dict[str, object]]: ...

    async def join_channel(self, candidate: dict[str, object]) -> dict[str, object]: ...

    async def list_joined_public_channels(
        self, *, limit: int
    ) -> list[dict[str, object]]: ...

    async def close(self) -> None: ...


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
    channel_id = str(
        channel.get("telegram_channel_id") or channel.get("channel_id") or ""
    ).strip()
    if channel_id:
        return f"id:{channel_id}"
    return f"handle:{normalize_channel_handle(channel.get('handle') or channel.get('username'))}"


def channel_identity_matches(left: dict[str, object], right: dict[str, object]) -> bool:
    """Return whether two channel records refer to the same Telegram entity.

    Telegram usernames are case-insensitive and can change.  A numeric channel ID is
    therefore authoritative when available, while a matching username bridges the
    common migration from a handle-only record to an ID-backed record.
    """

    left_id = str(
        left.get("telegram_channel_id") or left.get("channel_id") or ""
    ).strip()
    right_id = str(
        right.get("telegram_channel_id") or right.get("channel_id") or ""
    ).strip()
    if left_id and right_id:
        return left_id == right_id
    left_handle = normalize_channel_handle(
        left.get("handle") or left.get("username")
    ).casefold()
    right_handle = normalize_channel_handle(
        right.get("handle") or right.get("username")
    ).casefold()
    return bool(left_handle and right_handle and left_handle == right_handle)


def channel_identity_review_required(channel: dict[str, object]) -> bool:
    return (
        bool(channel.get("identity_review_required"))
        or str(channel.get("last_error") or "") == "channel_identity_review_required"
    )


def mark_channel_identity_review_required(
    channel: dict[str, object],
    resolved_channel: dict[str, object],
    now: datetime,
) -> None:
    """Fail closed when a public handle resolves to a different Telegram entity."""

    resolved_id = str(
        resolved_channel.get("telegram_channel_id")
        or resolved_channel.get("channel_id")
        or ""
    ).strip()
    channel["enabled"] = False
    channel["identity_review_required"] = True
    channel["identity_review_reason"] = "authoritative_channel_id_changed"
    channel["observed_telegram_channel_id"] = resolved_id
    channel["identity_review_detected_at"] = datetime_to_iso(now)
    channel["last_error"] = "channel_identity_review_required"


def _merge_channel_record(
    target: dict[str, object], incoming: dict[str, object]
) -> None:
    target["last_message_id"] = max(
        int(target.get("last_message_id") or 0),
        int(incoming.get("last_message_id") or 0),
    )
    for name, value in incoming.items():
        if name == "last_message_id":
            continue
        if value not in (None, ""):
            target[name] = value
    target["handle"] = normalize_channel_handle(
        target.get("handle") or target.get("username")
    )
    target["telegram_channel_id"] = (
        target.get("telegram_channel_id") or target.get("channel_id") or None
    )


def _migrate_channel_references(
    state: dict[str, object],
    channel: dict[str, object],
    aliases: list[dict[str, object]],
) -> None:
    canonical_id = str(channel.get("telegram_channel_id") or "").strip()
    canonical_handle = normalize_channel_handle(
        channel.get("handle") or channel.get("username")
    )
    alias_ids = {
        str(alias.get("telegram_channel_id") or alias.get("channel_id") or "").strip()
        for alias in aliases
        if str(
            alias.get("telegram_channel_id") or alias.get("channel_id") or ""
        ).strip()
    }
    alias_handles = {
        normalize_channel_handle(
            alias.get("handle") or alias.get("username")
        ).casefold()
        for alias in aliases
        if normalize_channel_handle(alias.get("handle") or alias.get("username"))
    }

    key_changes: dict[str, str] = {}
    migrated_messages: list[dict[str, object]] = []
    messages_by_key: dict[str, dict[str, object]] = {}
    for message in state.get("telegram_source_messages", []):
        if not isinstance(message, dict):
            continue
        old_key = message_key(message)
        message_id = str(
            message.get("telegram_channel_id") or message.get("channel_id") or ""
        ).strip()
        message_handle = normalize_channel_handle(
            message.get("handle") or message.get("username")
        )
        identity_match = (
            message_id in alias_ids
            if message_id
            else bool(message_handle and message_handle.casefold() in alias_handles)
        )
        if identity_match:
            if canonical_id:
                message["telegram_channel_id"] = canonical_id
            if canonical_handle:
                message["handle"] = canonical_handle
            new_key = message_key(message)
            key_changes[old_key] = new_key
        else:
            new_key = old_key
        existing = messages_by_key.get(new_key)
        if existing is None:
            messages_by_key[new_key] = message
            migrated_messages.append(message)
            continue
        for name, value in message.items():
            if value not in (None, ""):
                existing[name] = value
    state["telegram_source_messages"] = migrated_messages

    migrated_matches: list[dict[str, object]] = []
    matches_by_key: dict[tuple[str, str, str], dict[str, object]] = {}
    for match in state.get("telegram_article_matches", []):
        if not isinstance(match, dict):
            continue
        old_key = str(match.get("telegram_message_key") or "")
        if old_key in key_changes:
            match["telegram_message_key"] = key_changes[old_key]
            if canonical_handle:
                match["channel_handle"] = canonical_handle
        identity = (
            str(match.get("article_id") or ""),
            str(match.get("telegram_message_key") or ""),
            str(match.get("match_type") or ""),
        )
        if not all(identity):
            migrated_matches.append(match)
            continue
        existing_match = matches_by_key.get(identity)
        if existing_match is None:
            matches_by_key[identity] = match
            migrated_matches.append(match)
        elif float(match.get("score") or 0) > float(existing_match.get("score") or 0):
            existing_match.update(match)
    state["telegram_article_matches"] = migrated_matches

    cursors = state.get("telegram_remote_sync_cursors")
    if isinstance(cursors, dict):
        cursor_value = 0
        for alias in aliases:
            cursor_value = max(
                cursor_value, int(cursors.pop(channel_key(alias), 0) or 0)
            )
        if cursor_value:
            cursors[channel_key(channel)] = max(
                int(cursors.get(channel_key(channel), 0) or 0), cursor_value
            )


def canonicalize_telegram_channels(state: dict[str, object]) -> int:
    """Collapse pre-existing duplicate channel rows and migrate their references."""

    raw_channels = [
        channel
        for channel in state.get("telegram_source_channels", [])
        if isinstance(channel, dict)
    ]
    if not raw_channels:
        state["telegram_source_channels"] = []
        return 0
    canonical: list[dict[str, object]] = []
    aliases_by_record: dict[int, list[dict[str, object]]] = {}
    removed = 0
    for raw in raw_channels:
        matches = [
            record for record in canonical if channel_identity_matches(record, raw)
        ]
        if not matches:
            record = dict(raw)
            record["handle"] = normalize_channel_handle(
                record.get("handle") or record.get("username")
            )
            canonical.append(record)
            aliases_by_record[id(record)] = [raw]
            continue
        target = matches[0]
        _merge_channel_record(target, raw)
        aliases_by_record[id(target)].append(raw)
        removed += 1
        for duplicate in matches[1:]:
            _merge_channel_record(target, duplicate)
            aliases_by_record[id(target)].extend(
                aliases_by_record.pop(id(duplicate), [duplicate])
            )
            canonical.remove(duplicate)
            removed += 1
    state["telegram_source_channels"] = canonical
    for record in canonical:
        _migrate_channel_references(
            state, record, aliases_by_record.get(id(record), [record])
        )
    return removed


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
        handle = normalize_channel_handle(
            raw_channel.get("handle") or raw_channel.get("username")
        )
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
    if bool(channel.get("source_right_blocked")):
        return False
    if channel_identity_review_required(channel):
        return False
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
    if (
        channel.get("is_private_chat")
        or channel.get("is_saved_messages")
        or channel.get("is_group")
    ):
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
    if not isinstance(state.get("telegram_remote_sync_cursors"), dict):
        state["telegram_remote_sync_cursors"] = {}


def upsert_telegram_channel(
    state: dict[str, object], channel: dict[str, object]
) -> dict[str, object]:
    ensure_telegram_state(state)
    channels = state["telegram_source_channels"]  # type: ignore[index]
    matches = [
        existing
        for existing in channels
        if isinstance(existing, dict) and channel_identity_matches(existing, channel)
    ]
    authoritative_ids = {
        str(
            existing.get("telegram_channel_id") or existing.get("channel_id") or ""
        ).strip()
        for existing in matches
        if str(
            existing.get("telegram_channel_id") or existing.get("channel_id") or ""
        ).strip()
    }
    if len(authoritative_ids) > 1:
        incoming_id = str(
            channel.get("telegram_channel_id") or channel.get("channel_id") or ""
        ).strip()
        if incoming_id:
            matches = [
                existing
                for existing in matches
                if str(
                    existing.get("telegram_channel_id")
                    or existing.get("channel_id")
                    or ""
                ).strip()
                == incoming_id
            ]
        else:
            matches = [
                max(
                    matches,
                    key=lambda existing: int(existing.get("last_message_id") or 0),
                )
            ]
    if matches:
        existing = matches[0]
        aliases = [dict(existing), dict(channel)]
        _merge_channel_record(existing, channel)
        for duplicate in matches[1:]:
            aliases.append(dict(duplicate))
            _merge_channel_record(existing, duplicate)
            channels.remove(duplicate)
        _migrate_channel_references(state, existing, aliases)
        return existing
    record = {
        "handle": normalize_channel_handle(
            channel.get("handle") or channel.get("username")
        ),
        "telegram_channel_id": channel.get("telegram_channel_id")
        or channel.get("channel_id")
        or None,
        "title": channel.get("title") or "",
        "description": channel.get("description") or "",
        "joined": bool(channel.get("joined", False)),
        "enabled": bool(channel.get("enabled", True)),
        "source": channel.get("source") or "manual",
        "source_type": channel.get("source_type") or "public_channel",
        "is_public_channel": bool(channel.get("is_public_channel", True)),
        "quality_score": int(
            channel.get("quality_score") or score_channel_candidate(channel)
        ),
        "last_message_id": int(channel.get("last_message_id") or 0),
        "last_collected_at": channel.get("last_collected_at") or None,
        "last_recommendation_checked_at": channel.get("last_recommendation_checked_at")
        or None,
        "last_error": channel.get("last_error") or None,
    }
    channels.append(record)
    _migrate_channel_references(state, record, [record])
    return record


def register_configured_channels(
    state: dict[str, object], config: dict[str, object]
) -> int:
    canonicalize_telegram_channels(state)
    before = len(
        state.get("telegram_source_channels", [])
        if isinstance(state.get("telegram_source_channels"), list)
        else []
    )
    for channel in configured_channels(config):
        configured_handle = normalize_channel_handle(
            channel.get("handle") or channel.get("username")
        ).casefold()
        review_records = [
            existing
            for existing in state.get("telegram_source_channels", [])
            if (
                isinstance(existing, dict)
                and normalize_channel_handle(
                    existing.get("handle") or existing.get("username")
                ).casefold()
                == configured_handle
                and channel_identity_review_required(existing)
            )
        ]
        approved_id = str(
            channel.get("telegram_channel_id") or channel.get("channel_id") or ""
        ).strip()
        explicit_reassignment_approval = bool(
            channel.get("identity_review_approved")
            and approved_id
            and any(
                str(existing.get("observed_telegram_channel_id") or "").strip()
                == approved_id
                for existing in review_records
            )
        )
        if review_records and not explicit_reassignment_approval:
            # A handle-only config entry must not silently reactivate an entity
            # whose authoritative ID changed. A reviewed replacement is added
            # as a distinct ID-backed row only when both the observed ID and an
            # explicit approval flag are committed in config.
            continue
        upsert_telegram_channel(state, channel)
    after = len(
        state.get("telegram_source_channels", [])
        if isinstance(state.get("telegram_source_channels"), list)
        else []
    )
    return max(0, after - before)


def enabled_channels(state: dict[str, object]) -> list[dict[str, object]]:
    ensure_telegram_state(state)
    return [
        channel
        for channel in state.get("telegram_source_channels", [])
        if isinstance(channel, dict)
        and bool(channel.get("enabled", True))
        and is_collectable_public_channel(channel)
    ]


TELEGRAM_BACKFILL_SELECTION_ORDER_VERSION = "handle-id-v1"
TELEGRAM_BACKFILL_SELECTION_FINGERPRINT_PATTERN = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True)
class TelegramBackfillChannelSelection:
    channels: list[dict[str, object]]
    fingerprint: str
    universe_count: int


def _backfill_channel_identity_record(
    channel: dict[str, object],
) -> dict[str, str]:
    handle = normalize_channel_handle(
        channel.get("handle") or channel.get("username")
    ).casefold()
    telegram_channel_id = str(
        channel.get("telegram_channel_id") or channel.get("channel_id") or ""
    ).strip()
    return {
        "identity": (
            f"id:{telegram_channel_id}"
            if telegram_channel_id
            else f"handle:{handle}"
        ),
        "handle": handle,
        "telegram_channel_id": telegram_channel_id,
    }


def _backfill_selection_fingerprint(
    channels: list[dict[str, object]],
) -> str:
    records = [
        json.dumps(
            _backfill_channel_identity_record(channel),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        for channel in channels
    ]
    payload = (
        TELEGRAM_BACKFILL_SELECTION_ORDER_VERSION
        + "\x1e"
        + "\x1e".join(records)
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _backfill_selection_metrics(
    selection: TelegramBackfillChannelSelection,
) -> dict[str, object]:
    return {
        "telegram_backfill_selection_fingerprint": selection.fingerprint,
        "telegram_backfill_selection_count": selection.universe_count,
        "telegram_backfill_universe_count": selection.universe_count,
        "telegram_backfill_selected_count": len(selection.channels),
    }


def _backfill_channel_universe(
    state: dict[str, object],
) -> list[dict[str, object]]:
    return sorted(
        enabled_channels(state),
        key=lambda channel: (
            normalize_channel_handle(
                channel.get("handle") or channel.get("username")
            ).casefold(),
            str(
                channel.get("telegram_channel_id")
                or channel.get("channel_id")
                or ""
            ).strip(),
        ),
    )


def _current_backfill_selection_metrics(
    state: dict[str, object],
    *,
    selected_count: int,
) -> dict[str, object]:
    universe = _backfill_channel_universe(state)
    metrics = _backfill_selection_metrics(
        TelegramBackfillChannelSelection(
            channels=universe,
            fingerprint=_backfill_selection_fingerprint(universe),
            universe_count=len(universe),
        )
    )
    metrics["telegram_backfill_selected_count"] = selected_count
    return metrics


def _duplicate_backfill_universe_handles(
    state: dict[str, object],
) -> list[str]:
    counts: dict[str, int] = {}
    for channel in _backfill_channel_universe(state):
        handle = normalize_channel_handle(
            channel.get("handle") or channel.get("username")
        ).casefold()
        if handle:
            counts[handle] = counts.get(handle, 0) + 1
    return sorted(handle for handle, count in counts.items() if count > 1)


def _select_backfill_channels(
    state: dict[str, object],
    *,
    only_handles: set[str] | None,
    skip_handles: set[str] | None,
    start_after_handle: str,
    channel_limit: int,
    before_message_id: int = 0,
    expected_selection_fingerprint: str = "",
) -> TelegramBackfillChannelSelection:
    """Return a deterministic repair snapshot independent of DB row order."""

    universe = _backfill_channel_universe(state)
    fingerprint = _backfill_selection_fingerprint(universe)
    expected_fingerprint = str(expected_selection_fingerprint or "").strip()
    is_resume = bool(normalize_channel_handle(start_after_handle)) or bool(
        before_message_id
    )
    if expected_fingerprint and not TELEGRAM_BACKFILL_SELECTION_FINGERPRINT_PATTERN.fullmatch(
        expected_fingerprint
    ):
        raise ValueError("expected_selection_fingerprint_invalid")
    if is_resume and not expected_fingerprint:
        raise ValueError("expected_selection_fingerprint_required")
    if expected_fingerprint and expected_fingerprint != fingerprint:
        raise ValueError("selection_fingerprint_mismatch")

    included = {
        normalize_channel_handle(handle).casefold()
        for handle in (only_handles or set())
        if normalize_channel_handle(handle)
    }
    if before_message_id and (
        len(included) != 1 or normalize_channel_handle(start_after_handle)
    ):
        raise ValueError("before_message_id_requires_one_handle")
    if included:
        resolved_entity_ids: set[str] = set()
        for requested_handle in sorted(included):
            matches = [
                channel
                for channel in universe
                if normalize_channel_handle(
                    channel.get("handle") or channel.get("username")
                ).casefold()
                == requested_handle
            ]
            if not matches:
                raise ValueError(f"only_handles_not_found:{requested_handle}")
            if len(matches) != 1:
                raise ValueError(f"only_handles_not_unique:{requested_handle}")
            entity_id = _backfill_channel_identity_record(matches[0])["identity"]
            if entity_id in resolved_entity_ids:
                raise ValueError("only_handles_not_one_to_one")
            resolved_entity_ids.add(entity_id)

    channels = universe
    if included:
        channels = [
            channel
            for channel in channels
            if normalize_channel_handle(
                channel.get("handle") or channel.get("username")
            ).casefold()
            in included
        ]
    skipped = {
        normalize_channel_handle(handle).casefold()
        for handle in (skip_handles or set())
        if normalize_channel_handle(handle)
    }
    if skipped:
        channels = [
            channel
            for channel in channels
            if normalize_channel_handle(
                channel.get("handle") or channel.get("username")
            ).casefold()
            not in skipped
        ]
    start_after = normalize_channel_handle(start_after_handle).casefold()
    if start_after:
        start_indices = [
            index
            for index, channel in enumerate(channels)
            if normalize_channel_handle(
                channel.get("handle") or channel.get("username")
            ).casefold()
            == start_after
        ]
        if not start_indices:
            raise ValueError("start_after_handle_not_found")
        if len(start_indices) > 1:
            raise ValueError("start_after_handle_not_unique")
        channels = channels[start_indices[0] + 1 :]
    if channel_limit > 0:
        channels = channels[:channel_limit]
    return TelegramBackfillChannelSelection(
        channels=channels,
        fingerprint=fingerprint,
        universe_count=len(universe),
    )


def load_env_files(
    root: Path,
    names: tuple[str, ...] = (".env", ".env.local", ".env.api", ".env.telegram"),
) -> list[Path]:
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


def compact_backfill_per_channel(
    rows: object, *, limit: int = 80
) -> list[dict[str, object]]:
    if not isinstance(rows, list):
        return []
    compacted: list[dict[str, object]] = []
    for row in rows[-limit:]:
        if not isinstance(row, dict):
            continue
        compacted.append(
            {
                "handle": row.get("handle") or "",
                "title": row.get("title") or "",
                "status": row.get("status") or "",
                "messages_seen": int(row.get("messages_seen") or 0),
                "inserted": int(row.get("inserted") or 0),
                "updated": int(row.get("updated") or 0),
                "elapsed_seconds": float(row.get("elapsed_seconds") or 0),
                "fetch_elapsed_seconds": float(row.get("fetch_elapsed_seconds") or 0),
                "index": int(row.get("index") or 0),
                "total": int(row.get("total") or 0),
                **({"error": row.get("error")} if row.get("error") else {}),
            }
        )
    return compacted


def telegram_run_record(
    now: datetime, mode: str, summary: dict[str, object]
) -> dict[str, object]:
    record: dict[str, object] = {
        "ran_at": datetime_to_iso(now),
        "mode": mode,
        **{
            key: value
            for key, value in summary.items()
            if isinstance(value, (int, float, str))
        },
    }
    per_channel = compact_backfill_per_channel(
        summary.get("telegram_backfill_per_channel")
    )
    if per_channel:
        record["telegram_backfill_per_channel"] = per_channel
    return record


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


def build_telegram_message_index(
    state: dict[str, object],
) -> dict[str, dict[str, object]]:
    """Build an in-memory key index for one collection pass.

    The index is deliberately kept outside ``state`` so it can never leak into a
    persisted snapshot.  Large catch-up runs previously scanned the entire
    message list for every insert, turning a 75k-message recovery into O(n^2)
    work.
    """

    ensure_telegram_state(state)
    return {
        message_key(message): message
        for message in state.get("telegram_source_messages", [])
        if isinstance(message, dict)
    }


def normalize_telegram_message(
    channel: dict[str, object], raw_message: dict[str, object], now: datetime
) -> dict[str, object]:
    message_id = int(
        raw_message.get("telegram_message_id") or raw_message.get("id") or 0
    )
    text = str(raw_message.get("text") or raw_message.get("message") or "")
    posted_at = (
        raw_message.get("posted_at") or raw_message.get("date") or datetime_to_iso(now)
    )
    edited_at = raw_message.get("edited_at") or raw_message.get("edit_date") or None
    urls = [canonicalize_telegram_url(url) for url in extract_urls(text)]
    return {
        "handle": normalize_channel_handle(
            channel.get("handle") or channel.get("username")
        ),
        "telegram_channel_id": channel.get("telegram_channel_id")
        or channel.get("channel_id")
        or None,
        "channel_title": channel.get("title") or "",
        "source_kind": "authorized_telegram",
        "source_right_id": channel.get("source_right_id") or None,
        "source_type": "public_channel",
        "is_public_channel": True,
        "telegram_message_id": message_id,
        "posted_at": posted_at
        if isinstance(posted_at, str)
        else datetime_to_iso(posted_at),
        "edited_at": edited_at
        if isinstance(edited_at, str) or edited_at is None
        else datetime_to_iso(edited_at),
        "deleted_at": raw_message.get("deleted_at") or None,
        "text": text,
        "normalized_text": normalize_message_text(text).casefold(),
        "views": int(raw_message.get("views") or 0),
        "forwards": int(raw_message.get("forwards") or 0),
        "replies_count": int(
            raw_message.get("replies_count") or raw_message.get("replies") or 0
        ),
        "message_url": raw_message.get("message_url")
        or telegram_message_url(channel, message_id),
        "urls": [url for url in urls if url],
        "raw_json": raw_message.get("raw_json")
        if isinstance(raw_message.get("raw_json"), dict)
        else None,
        "collected_at": datetime_to_iso(now),
    }


def upsert_telegram_message(
    state: dict[str, object],
    message: dict[str, object],
    *,
    message_index: dict[str, dict[str, object]] | None = None,
) -> str:
    ensure_telegram_state(state)
    key = message_key(message)
    messages = state["telegram_source_messages"]  # type: ignore[index]
    existing = message_index.get(key) if message_index is not None else None
    if existing is None and message_index is None:
        # Keep the public helper backwards compatible for one-off callers.
        existing = next(
            (
                candidate
                for candidate in messages
                if isinstance(candidate, dict) and message_key(candidate) == key
            ),
            None,
        )
    if existing is not None:
        changed = False
        for field in (
            "text",
            "normalized_text",
            "edited_at",
            "deleted_at",
            "views",
            "forwards",
            "replies_count",
            "urls",
            "raw_json",
            "source_kind",
            "source_right_id",
        ):
            if message.get(field) != existing.get(field):
                existing[field] = message.get(field)
                changed = True
        existing["collected_at"] = message.get("collected_at")
        return "updated" if changed else "unchanged"
    messages.append(message)
    if message_index is not None:
        message_index[key] = message
    return "inserted"


def mark_deleted_message(
    state: dict[str, object],
    channel: dict[str, object],
    telegram_message_id: int,
    deleted_at: datetime,
) -> bool:
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
        if isinstance(message, dict)
        and message_key(message).startswith(f"{channel_prefix}:")
        and not message.get("deleted_at")
    ]
    candidates.sort(
        key=lambda item: int(item.get("telegram_message_id") or 0), reverse=True
    )
    marked = 0
    for message in candidates[:recent_limit]:
        message_id = int(message.get("telegram_message_id") or 0)
        if message_id and message_id not in observed_message_ids:
            message["deleted_at"] = datetime_to_iso(deleted_at)
            marked += 1
    return marked


def article_id(article: dict[str, object]) -> str:
    return str(
        article.get("record_id")
        or article.get("canonical_url_hash")
        or article.get("title_hash")
        or ""
    ).strip()


def article_match_status(article: dict[str, object]) -> str:
    return str(article.get("status") or "").strip().casefold()


def article_all_urls(article: dict[str, object]) -> list[str]:
    urls: list[str] = []
    for key in ("canonical_url", "link", "original_url", "resolved_url"):
        value = str(article.get(key) or "").strip()
        if value:
            urls.append(value)
    for duplicate in article.get("duplicate_matches") or []:
        if not isinstance(duplicate, dict):
            continue
        for key in ("canonical_url", "link", "original_url", "resolved_url"):
            value = str(duplicate.get(key) or "").strip()
            if value:
                urls.append(value)
    return urls


def index_article_url(
    index: dict[str, dict[str, object]], url: str, article: dict[str, object]
) -> None:
    canonical = canonicalize_telegram_url(url)
    if not canonical:
        return
    for key in (canonical, canonical_url_hash(canonical)):
        existing = index.get(key)
        if existing is None or article_match_status(existing) in {
            "duplicate",
            "rejected",
        }:
            index[key] = article


def article_url_index(state: dict[str, object]) -> dict[str, dict[str, object]]:
    index: dict[str, dict[str, object]] = {}
    for article in state.get("articles", []):
        if not isinstance(article, dict):
            continue
        for url in article_all_urls(article):
            index_article_url(index, url, article)
    return index


def build_article_match_context(
    state: dict[str, object], config: dict[str, object]
) -> TelegramArticleMatchContext:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    records: list[ArticleTokenRecord] = []
    for article in state.get("articles", []):
        if not isinstance(article, dict):
            continue
        if article_match_status(article) in {"duplicate", "rejected"}:
            continue
        tokens = article_tokens(article)
        if not tokens:
            continue
        records.append(
            ArticleTokenRecord(
                article=article,
                tokens=tokens,
                article_dt=parse_datetime(
                    article.get("published_at") or article.get("seen_at"), timezone_name
                ),
            )
        )
    token_index: dict[str, list[int]] = {}
    for index, record in enumerate(records):
        for token in record.tokens:
            token_index.setdefault(token, []).append(index)
    return TelegramArticleMatchContext(
        url_index=article_url_index(state),
        article_tokens=records,
        token_index=token_index,
    )


def article_tokens(article: dict[str, object]) -> set[str]:
    text = " ".join(
        str(article.get(key) or "") for key in ("title", "normalized_title", "summary")
    )
    text = URL_PATTERN.sub(" ", text)
    tokens = {token.casefold() for token in re.findall(r"[0-9A-Za-z가-힣]{2,}", text)}
    tokens.update(title_signature(article.get("title")))
    return {token for token in tokens if token not in GENERIC_MATCH_TOKENS}


def title_signature(value: object) -> set[str]:
    normalized = normalize_title(str(value or ""))
    tokens = {
        token.casefold() for token in re.findall(r"[0-9A-Za-z가-힣]{2,}", normalized)
    }
    return {token for token in tokens if token not in GENERIC_MATCH_TOKENS}


def message_tokens(message: dict[str, object]) -> set[str]:
    text = URL_PATTERN.sub(
        " ", str(message.get("normalized_text") or message.get("text") or "")
    )
    tokens = {token.casefold() for token in re.findall(r"[0-9A-Za-z가-힣]{2,}", text)}
    return {token for token in tokens if token not in GENERIC_MATCH_TOKENS}


def ordered_message_tokens(message: dict[str, object]) -> list[str]:
    text = URL_PATTERN.sub(
        " ", str(message.get("normalized_text") or message.get("text") or "")
    )
    seen: set[str] = set()
    tokens: list[str] = []
    for token in re.findall(r"[0-9A-Za-z가-힣]{2,}", text):
        lowered = token.casefold()
        if lowered in seen or is_signal_noise_token(lowered):
            continue
        if re.fullmatch(r"\d{4}", lowered):
            continue
        if re.fullmatch(r"\d{4}년", lowered):
            continue
        if re.fullmatch(r"\d{1,2}[월일시분]", lowered):
            continue
        if re.fullmatch(r"\d+q\d+", lowered):
            continue
        if re.fullmatch(r"\d+(?:\.\d+)?%?", lowered):
            continue
        if re.fullmatch(r"[0-9,]+원", lowered):
            continue
        if lowered.isdigit() and len(lowered) < 4:
            continue
        seen.add(lowered)
        tokens.append(lowered)
    return tokens


def is_signal_noise_token(token: str) -> bool:
    lowered = str(token or "").casefold().strip()
    if not lowered:
        return True
    if lowered in SIGNAL_STOP_TOKENS:
        return True
    if any(part in lowered for part in SIGNAL_STOP_SUBSTRINGS):
        return True
    if any(lowered.endswith(suffix) for suffix in SIGNAL_STOP_DOMAIN_SUFFIXES):
        return True
    if re.fullmatch(
        r"(?:m|www|news|article|view|readnews|contents?|files?|feed)s?", lowered
    ):
        return True
    if re.fullmatch(r"\d+(?:\.\d+)?q", lowered):
        return True
    if re.fullmatch(r"\d+(?:\.\d+)?(?:조|억|만원|천원|원|달러|usd|krw)", lowered):
        return True
    if re.fullmatch(r"[a-z]\d{5,6}", lowered):
        return True
    if re.fullmatch(r"[a-z]{1,2}", lowered) and lowered not in {"ai"}:
        return True
    return False


def is_event_match_token(token: str) -> bool:
    lowered = token.casefold()
    return lowered in WEAK_MATCH_EVENT_TOKENS or any(
        keyword in lowered for keyword in WEAK_MATCH_EVENT_SUBSTRINGS
    )


def english_board_token_is_governance(tokens: list[str], text: str) -> bool:
    token_set = {token.casefold() for token in tokens}
    lowered_text = text.casefold()
    if token_set & BOARD_PRODUCT_CONTEXT_TOKENS:
        if not token_set & BOARD_GOVERNANCE_CONTEXT_TOKENS:
            return False
    return bool(
        token_set & BOARD_GOVERNANCE_CONTEXT_TOKENS
        or re.search(
            r"\bboard\s+(?:member|members|seat|seats|nominee|nominees|refresh|representation)\b",
            lowered_text,
        )
        or re.search(r"\bboard\s+of\s+directors?\b", lowered_text)
    )


def signal_event_tokens_for_message(
    message: dict[str, object], tokens: list[str]
) -> list[str]:
    text = str(message.get("normalized_text") or message.get("text") or "")
    events: list[str] = []
    for token in tokens:
        lowered = token.casefold()
        if not is_event_match_token(lowered):
            continue
        if (
            lowered in AMBIGUOUS_ENGLISH_BOARD_TOKENS
            and not english_board_token_is_governance(tokens, text)
        ):
            continue
        events.append(lowered)
    return events


def signal_event_label(token: str) -> str:
    lowered = token.casefold()
    if lowered in SIGNAL_EVENT_LABELS:
        return SIGNAL_EVENT_LABELS[lowered]
    for keyword, label in SIGNAL_EVENT_LABELS.items():
        if keyword in lowered:
            return label
    return token


def signal_entity_tokens(tokens: list[str], *, limit: int = 6) -> list[str]:
    entities: list[str] = []
    for token in tokens:
        if is_event_match_token(token):
            continue
        if is_signal_noise_token(token):
            continue
        if len(token) < 3 and not re.search(r"\d", token):
            continue
        if re.fullmatch(r"\d{1,3}", token):
            continue
        if re.search(r"(?:월|일|분기|실적발표|컨센서스)$", token) and len(token) <= 6:
            continue
        entities.append(token)
        if len(entities) >= limit:
            break
    return entities


def telegram_signal_message_score(message: dict[str, object]) -> int:
    views = max(0, int(message.get("views") or 0))
    forwards = max(0, int(message.get("forwards") or 0))
    replies = max(0, int(message.get("replies_count") or 0))
    return views + forwards * 3 + replies * 4


def telegram_signal_excerpt(message: dict[str, object], *, max_chars: int = 170) -> str:
    text = re.sub(
        r"\s+", " ", str(message.get("text") or message.get("normalized_text") or "")
    ).strip()
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 1)].rstrip() + "…"


def telegram_candidate_title(message: dict[str, object], url: str) -> str:
    text = str(message.get("text") or "")
    text = URL_PATTERN.sub(" ", text)
    lines = [re.sub(r"\s+", " ", line).strip(" -|·\t") for line in text.splitlines()]
    for line in lines:
        if len(line) >= 12 and not line.startswith(("@", "#")):
            return line[:220]
    host = hostname_from_url(url).removeprefix("www.")
    return f"Telegram 공개채널 공유 기사 - {host or 'unknown'}"


def telegram_candidate_articles(
    state: dict[str, object], config: dict[str, object], now: datetime
) -> list[dict[str, object]]:
    settings = telegram_sources_config(config)
    if not bool(settings.get("candidate_source_enabled", True)):
        return []
    ensure_telegram_state(state)
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    window_hours = max(1, int(settings.get("candidate_window_hours", 168)))
    limit = max(0, int(settings.get("candidate_limit_per_run", 50)))
    if limit == 0:
        return []
    allowed_handles = {
        normalize_channel_handle(handle)
        for handle in settings.get("candidate_source_handles", [])
        if normalize_channel_handle(handle)
    }
    cutoff = now - timedelta(hours=window_hours)
    existing_hashes = {
        str(article.get("canonical_url_hash") or "")
        for article in state.get("articles", [])
        if isinstance(article, dict) and article.get("canonical_url_hash")
    }
    emitted_hashes: set[str] = set()
    candidates: list[dict[str, object]] = []
    messages = [
        message
        for message in state.get("telegram_source_messages", [])
        if isinstance(message, dict) and not message.get("deleted_at")
    ]
    messages.sort(
        key=lambda item: str(item.get("posted_at") or item.get("collected_at") or ""),
        reverse=True,
    )
    for message in messages:
        if not source_is_authorized(message, config, now, purpose="ai"):
            continue
        handle = normalize_channel_handle(
            message.get("handle") or message.get("username")
        )
        if allowed_handles and handle not in allowed_handles:
            continue
        posted_at = parse_datetime(message.get("posted_at"), timezone_name)
        if posted_at and posted_at < cutoff:
            continue
        for url in message.get("urls") or []:
            canonical_url = canonicalize_telegram_url(str(url or ""))
            if not canonical_url or is_boilerplate_signal_url(canonical_url):
                continue
            url_hash = canonical_url_hash(canonical_url)
            if url_hash in existing_hashes or url_hash in emitted_hashes:
                continue
            title = telegram_candidate_title(message, canonical_url)
            title_parts = normalize_title_parts(title)
            posted_iso = (
                datetime_to_iso(posted_at)
                if posted_at
                else str(message.get("posted_at") or "")
            )
            candidate = {
                "title": title,
                "clean_title": title_parts["clean_title"],
                "normalized_title": title_parts["normalized_title"],
                "prefixes": title_parts["prefixes"],
                "source": hostname_from_url(canonical_url).removeprefix("www.")
                or str(message.get("channel_title") or "Telegram"),
                "link": canonical_url,
                "canonical_url": canonical_url,
                "canonical_url_hash": url_hash,
                "title_hash": title_parts["title_hash"],
                "summary": telegram_signal_excerpt(message, max_chars=260),
                "image_url": None,
                "feed_published_at": posted_iso or None,
                "feed_updated_at": message.get("collected_at") or posted_iso or None,
                "article_published_at": posted_iso or None,
                "feed_name": f"Telegram:{handle}" if handle else "Telegram",
                "feed_category": "telegram_reference",
                "source_kind": "telegram_reference",
                "source_right_id": message.get("source_right_id") or None,
                "original_resolution_status": "direct",
                "telegram_candidate": True,
                "telegram_source_handle": handle,
                "telegram_source_title": message.get("channel_title") or "",
                "telegram_source_message_url": message.get("message_url") or "",
            }
            candidates.append(candidate)
            emitted_hashes.add(url_hash)
            if len(candidates) >= limit:
                return candidates
    return candidates


def telegram_signal_message_payload(message: dict[str, object]) -> dict[str, object]:
    return {
        "source_kind": "telegram_signal",
        "source_right_id": message.get("source_right_id") or None,
        "message_url": message.get("message_url") or "",
        "channel_title": message.get("channel_title") or "",
        "channel_handle": message.get("handle") or "",
        "posted_at": message.get("posted_at") or "",
        "excerpt": telegram_signal_excerpt(message),
        "views": int(message.get("views") or 0),
        "forwards": int(message.get("forwards") or 0),
        "risk_flags": risk_flags_for_text(str(message.get("text") or "")),
    }


def _channel_quality_metrics_for_records(
    channel: dict[str, object],
    messages: list[dict[str, object]],
    matches: list[dict[str, object]],
) -> dict[str, object]:
    if not normalize_channel_handle(channel.get("handle") or channel.get("username")):
        return {
            "messages": 0,
            "matches": 0,
            "direct_matches": 0,
            "weak_matches": 0,
            "risk_messages": 0,
            "match_rate": 0.0,
            "direct_match_rate": 0.0,
            "risk_rate": 0.0,
            "signal_quality_score": int(
                channel.get("quality_score") or score_channel_candidate(channel)
            ),
        }

    message_count = len(messages)
    match_count = len(matches)
    direct_match_count = sum(
        1
        for match in matches
        if str(match.get("match_type") or "") in {"exact_url", "canonical_url"}
    )
    weak_match_count = match_count - direct_match_count
    risk_message_count = sum(
        1
        for message in messages
        if risk_flags_for_text(
            str(message.get("text") or message.get("normalized_text") or "")
        )
    )
    match_rate = match_count / message_count if message_count else 0.0
    direct_match_rate = direct_match_count / message_count if message_count else 0.0
    risk_rate = risk_message_count / message_count if message_count else 0.0
    base_score = int(channel.get("quality_score") or score_channel_candidate(channel))
    activity_bonus = min(10, message_count // 250)
    direct_bonus = min(18, direct_match_count * 3)
    weak_bonus = min(8, weak_match_count)
    match_rate_bonus = min(14, int(match_rate * 42))
    risk_penalty = min(24, int(risk_rate * 48))
    signal_quality_score = max(
        0,
        min(
            100,
            base_score
            + activity_bonus
            + direct_bonus
            + weak_bonus
            + match_rate_bonus
            - risk_penalty,
        ),
    )
    return {
        "messages": message_count,
        "matches": match_count,
        "direct_matches": direct_match_count,
        "weak_matches": weak_match_count,
        "risk_messages": risk_message_count,
        "match_rate": round(match_rate, 4),
        "direct_match_rate": round(direct_match_rate, 4),
        "risk_rate": round(risk_rate, 4),
        "signal_quality_score": signal_quality_score,
    }


def channel_quality_metrics(
    state: dict[str, object], channel: dict[str, object]
) -> dict[str, object]:
    handle = normalize_channel_handle(channel.get("handle") or channel.get("username"))
    messages = [
        message
        for message in state.get("telegram_source_messages", [])
        if isinstance(message, dict)
        and not message.get("deleted_at")
        and normalize_channel_handle(
            message.get("handle") or message.get("channel_handle")
        )
        == handle
    ]
    message_keys = {message_key(message) for message in messages}
    matches = [
        match
        for match in state.get("telegram_article_matches", [])
        if isinstance(match, dict)
        and (
            normalize_channel_handle(match.get("channel_handle")) == handle
            or str(match.get("telegram_message_key") or "") in message_keys
        )
    ]
    return _channel_quality_metrics_for_records(channel, messages, matches)


def refresh_channel_runtime_quality(state: dict[str, object]) -> None:
    ensure_telegram_state(state)
    messages_by_handle: dict[str, list[dict[str, object]]] = defaultdict(list)
    message_handle_by_key: dict[str, str] = {}
    for message in state.get("telegram_source_messages", []):
        if not isinstance(message, dict) or message.get("deleted_at"):
            continue
        handle = normalize_channel_handle(
            message.get("handle") or message.get("channel_handle")
        )
        if not handle:
            continue
        messages_by_handle[handle].append(message)
        message_handle_by_key[message_key(message)] = handle

    matches_by_handle: dict[str, list[dict[str, object]]] = defaultdict(list)
    for match in state.get("telegram_article_matches", []):
        if not isinstance(match, dict):
            continue
        handles = {
            normalize_channel_handle(match.get("channel_handle")),
            message_handle_by_key.get(str(match.get("telegram_message_key") or ""), ""),
        }
        for handle in handles - {""}:
            matches_by_handle[handle].append(match)

    for channel in state.get("telegram_source_channels", []):
        if not isinstance(channel, dict):
            continue
        handle = normalize_channel_handle(
            channel.get("handle") or channel.get("username")
        )
        metrics = _channel_quality_metrics_for_records(
            channel,
            messages_by_handle.get(handle, []),
            matches_by_handle.get(handle, []),
        )
        channel["signal_quality_score"] = metrics["signal_quality_score"]
        channel["quality_metrics"] = metrics


def prune_telegram_state(
    state: dict[str, object], config: dict[str, object], now: datetime
) -> dict[str, int]:
    ensure_telegram_state(state)
    settings = telegram_sources_config(config)
    retention_days = int(settings.get("message_retention_days", 365))
    max_messages = int(settings.get("local_state_message_limit", 80000))
    messages = [
        message
        for message in state.get("telegram_source_messages", [])
        if isinstance(message, dict)
    ]
    if retention_days > 0:
        cutoff = now - timedelta(days=retention_days)
        messages = [
            message
            for message in messages
            if (
                parse_datetime(
                    message.get("posted_at"),
                    str(config.get("timezone") or "Asia/Seoul"),
                )
                or now
            )
            >= cutoff
        ]
    if max_messages > 0 and len(messages) > max_messages:
        messages = sorted(
            messages,
            key=lambda message: str(message.get("posted_at") or "")
            + ":"
            + str(message.get("telegram_message_id") or ""),
            reverse=True,
        )[:max_messages]
        messages.sort(
            key=lambda message: str(message.get("posted_at") or "")
            + ":"
            + str(message.get("telegram_message_id") or "")
        )
    kept_keys = {message_key(message) for message in messages}
    old_message_count = (
        len(state.get("telegram_source_messages", []))
        if isinstance(state.get("telegram_source_messages"), list)
        else 0
    )
    old_match_count = (
        len(state.get("telegram_article_matches", []))
        if isinstance(state.get("telegram_article_matches"), list)
        else 0
    )
    state["telegram_source_messages"] = messages
    state["telegram_article_matches"] = [
        match
        for match in state.get("telegram_article_matches", [])
        if isinstance(match, dict)
        and str(match.get("telegram_message_key") or "") in kept_keys
    ]
    return {
        "telegram_messages_pruned": max(0, old_message_count - len(messages)),
        "telegram_matches_pruned": max(
            0, old_match_count - len(state.get("telegram_article_matches", []))
        ),
    }


def is_boilerplate_signal_url(url: str) -> bool:
    try:
        parsed = urlsplit(url)
    except ValueError:
        return True
    host = parsed.netloc.casefold()
    path = parsed.path.strip("/")
    if not host:
        return True
    if host in {"signal.sedaily.com", "www.sedaily.com"} and not path:
        return True
    if host in {"t.me", "telegram.me"}:
        return True
    if not path and not parsed.query:
        return True
    return False


def is_strong_match_token(token: str) -> bool:
    return len(token) >= 3 or bool(re.search(r"\d", token))


def weak_match_components(overlap: list[str]) -> tuple[list[str], list[str], list[str]]:
    event_tokens = [token for token in overlap if is_event_match_token(token)]
    entity_tokens = [
        token
        for token in overlap
        if token not in event_tokens and is_strong_match_token(token)
    ]
    strong_tokens = [token for token in overlap if is_strong_match_token(token)]
    return event_tokens, entity_tokens, strong_tokens


def weak_match_is_plausible(
    overlap: list[str], *, min_overlap: int, min_strong_overlap: int
) -> bool:
    if len(overlap) < min_overlap:
        return False
    event_tokens, entity_tokens, strong_tokens = weak_match_components(overlap)
    if len(strong_tokens) < min_strong_overlap:
        return False
    if not event_tokens or not entity_tokens:
        return False
    return True


def weak_match_score(overlap: list[str]) -> float:
    event_tokens, entity_tokens, strong_tokens = weak_match_components(overlap)
    score = (
        0.34
        + len(event_tokens) * 0.07
        + len(entity_tokens) * 0.06
        + max(0, len(strong_tokens) - 2) * 0.03
    )
    return round(min(0.68, score), 4)


def weak_match_within_window(
    message: dict[str, object],
    article: dict[str, object],
    *,
    timezone_name: str,
    window_hours: int,
) -> bool:
    message_dt = parse_datetime(message.get("posted_at"), timezone_name)
    article_dt = parse_datetime(
        article.get("published_at") or article.get("seen_at"), timezone_name
    )
    if not message_dt or not article_dt:
        return True
    return abs((message_dt - article_dt).total_seconds()) <= window_hours * 3600


def weak_match_datetimes_within_window(
    message_dt: datetime | None, article_dt: datetime | None, *, window_hours: int
) -> bool:
    if not message_dt or not article_dt:
        return True
    return abs((message_dt - article_dt).total_seconds()) <= window_hours * 3600


def article_match_identity(match: dict[str, object]) -> tuple[str, str, str]:
    return (
        str(match.get("article_id") or ""),
        str(match.get("telegram_message_key") or ""),
        str(match.get("match_type") or ""),
    )


def build_telegram_match_index(
    state: dict[str, object],
) -> dict[tuple[str, str, str], dict[str, object]]:
    ensure_telegram_state(state)
    return {
        article_match_identity(match): match
        for match in state.get("telegram_article_matches", [])
        if isinstance(match, dict)
    }


def upsert_article_match(
    state: dict[str, object],
    match: dict[str, object],
    *,
    match_index: dict[tuple[str, str, str], dict[str, object]] | None = None,
) -> str:
    ensure_telegram_state(state)
    matches = state["telegram_article_matches"]  # type: ignore[index]
    identity = article_match_identity(match)
    existing = match_index.get(identity) if match_index is not None else None
    if existing is None and match_index is None:
        existing = next(
            (
                candidate
                for candidate in matches
                if isinstance(candidate, dict)
                and article_match_identity(candidate) == identity
            ),
            None,
        )
    if existing is not None:
        existing.update(match)
        return "updated"
    matches.append(match)
    if match_index is not None:
        match_index[identity] = match
    return "inserted"


def match_message_to_articles(
    state: dict[str, object],
    message: dict[str, object],
    config: dict[str, object],
    context: TelegramArticleMatchContext | None = None,
) -> list[dict[str, object]]:
    context = context or build_article_match_context(state, config)
    url_index = context.url_index
    results: list[dict[str, object]] = []
    seen: set[tuple[str, str]] = set()

    for url in message.get("urls") or []:
        canonical = canonicalize_telegram_url(str(url))
        candidates = [
            ("exact_url", url_index.get(canonical), 1.0, "URL 직접 일치"),
            (
                "canonical_url",
                url_index.get(canonical_url_hash(canonical)),
                0.96,
                "canonical URL hash 일치",
            ),
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
    min_overlap = int(settings.get("weak_match_min_overlap", 3))
    min_strong_overlap = int(settings.get("weak_match_min_strong_overlap", 2))
    window_hours = int(settings.get("weak_match_window_hours", 96))
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    tokens = message_tokens(message)
    if not tokens:
        return []
    message_dt = parse_datetime(message.get("posted_at"), timezone_name)
    candidate_indexes: set[int] = set()
    for token in tokens:
        candidate_indexes.update(context.token_index.get(token, []))
    for record_index in candidate_indexes:
        record = context.article_tokens[record_index]
        article = record.article
        if not weak_match_datetimes_within_window(
            message_dt, record.article_dt, window_hours=window_hours
        ):
            continue
        overlap = sorted(tokens & record.tokens)
        if not weak_match_is_plausible(
            overlap, min_overlap=min_overlap, min_strong_overlap=min_strong_overlap
        ):
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
                "score": weak_match_score(overlap),
                "reason": "키워드 추정 일치: " + ", ".join(overlap[:5]),
            }
        )
    results.sort(
        key=lambda item: (float(item.get("score") or 0), str(item.get("reason") or "")),
        reverse=True,
    )
    return results[: int(settings.get("weak_match_limit_per_message", 3))]


def risk_flags_for_text(text: str) -> list[str]:
    lowered = str(text or "").casefold()
    flags: list[str] = []
    if any(keyword in lowered for keyword in RUMOR_KEYWORDS):
        flags.append("rumor")
    if any(keyword in lowered for keyword in PROMOTIONAL_KEYWORDS):
        flags.append("promotional")
    if any(keyword in lowered for keyword in MARKET_SENSITIVE_KEYWORDS):
        flags.append("market_sensitive")
    if "?" in lowered and any(
        keyword in lowered for keyword in ("확인", "사실", "진위")
    ):
        flags.append("unverified")
    return flags


def telegram_article_match_signals(
    state: dict[str, object],
    config: dict[str, object] | None = None,
    *,
    now: datetime | None = None,
) -> list[dict[str, object]]:
    ensure_telegram_state(state)
    rights_config = config or {}
    messages_by_key = {
        message_key(message): message
        for message in state.get("telegram_source_messages", [])
        if isinstance(message, dict)
        and not message.get("deleted_at")
        and source_is_authorized(message, rights_config, now, purpose="ai")
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
        related_messages = [
            messages_by_key.get(str(match.get("telegram_message_key") or ""))
            for match in matches
        ]
        related_messages = [
            message for message in related_messages if isinstance(message, dict)
        ]
        if not related_messages:
            continue
        channels = {
            str(message.get("handle") or message.get("telegram_channel_id") or "")
            for message in related_messages
        }
        source_right_ids = sorted(
            {
                str(message.get("source_right_id") or "")
                for message in related_messages
                if message.get("source_right_id")
            }
        )
        dates = sorted(
            str(message.get("posted_at") or "")
            for message in related_messages
            if message.get("posted_at")
        )
        keyword_counter: Counter[str] = Counter()
        channel_counter: Counter[str] = Counter()
        flags: set[str] = set()
        for message in related_messages:
            keyword_counter.update(ordered_message_tokens(message)[:8])
            channel_counter.update(
                [
                    str(
                        message.get("channel_title")
                        or message.get("handle")
                        or message.get("telegram_channel_id")
                        or ""
                    )
                ]
            )
            flags.update(risk_flags_for_text(str(message.get("text") or "")))
        direct_count = len(
            [
                match
                for match in matches
                if str(match.get("match_type") or "") in {"exact_url", "canonical_url"}
            ]
        )
        weak_count = max(0, len(matches) - direct_count)
        score_values = [float(match.get("score") or 0) for match in matches]
        avg_score = sum(score_values) / len(score_values) if score_values else 0
        confidence = min(
            1.0,
            0.16
            + len(related_messages) * 0.055
            + len(channels) * 0.14
            + direct_count * 0.16
            + max(0.0, avg_score - 0.5) * 0.28,
        )
        top_messages = []
        for match in matches:
            message = messages_by_key.get(str(match.get("telegram_message_key") or ""))
            if not isinstance(message, dict):
                continue
            payload = telegram_signal_message_payload(message)
            payload["match_type"] = match.get("match_type") or ""
            payload["score"] = float(match.get("score") or 0)
            payload["reason"] = match.get("reason") or ""
            top_messages.append(payload)
        signals.append(
            {
                "article_id": article,
                "signal_type": "article_match",
                "source_kind": "telegram_signal",
                "source_right_ids": source_right_ids,
                "signal_key": article,
                "signal_title": f"기사 매칭 {article[:10]}",
                "related_telegram_count": len(related_messages),
                "related_telegram_channels_count": len(channels),
                "direct_url_count": direct_count,
                "keyword_match_count": weak_count,
                "first_seen_at": dates[0] if dates else "",
                "latest_seen_at": dates[-1] if dates else "",
                "top_related_messages": sorted(
                    top_messages,
                    key=lambda item: (
                        float(item.get("score") or 0),
                        int(item.get("views") or 0)
                        + int(item.get("forwards") or 0) * 3,
                    ),
                    reverse=True,
                )[:5],
                "top_channels": sorted(channels)[:8],
                "top_channel_counts": [
                    {"channel": channel, "count": count}
                    for channel, count in channel_counter.most_common(8)
                    if channel
                ],
                "top_keywords": [
                    keyword for keyword, _count in keyword_counter.most_common(8)
                ],
                "confidence_score": round(confidence, 3),
                "risk_flags": sorted(flags),
                "signal_summary": f"URL 직접 {direct_count}건, 키워드 추정 {weak_count}건",
            }
        )
    return signals


def latest_signal_reference_time(
    messages: list[dict[str, object]], timezone_name: str
) -> datetime | None:
    dates = [
        parse_datetime(message.get("posted_at"), timezone_name) for message in messages
    ]
    dates = [date for date in dates if date is not None]
    return max(dates) if dates else None


def telegram_url_burst_signals(
    messages: list[dict[str, object]],
    *,
    min_messages: int,
    min_channels: int,
    max_messages_per_signal: int,
) -> list[dict[str, object]]:
    grouped: dict[str, list[dict[str, object]]] = defaultdict(list)
    for message in messages:
        for url in message.get("urls") or []:
            canonical = canonicalize_telegram_url(str(url))
            if canonical and not is_boilerplate_signal_url(canonical):
                grouped[canonical].append(message)

    signals: list[dict[str, object]] = []
    for canonical, related_messages in grouped.items():
        channels = {
            str(message.get("handle") or message.get("telegram_channel_id") or "")
            for message in related_messages
        }
        source_right_ids = sorted(
            {
                str(message.get("source_right_id") or "")
                for message in related_messages
                if message.get("source_right_id")
            }
        )
        if len(related_messages) < min_messages or len(channels) < min_channels:
            continue
        dates = sorted(
            str(message.get("posted_at") or "")
            for message in related_messages
            if message.get("posted_at")
        )
        flags: set[str] = set()
        keyword_counter: Counter[str] = Counter()
        channel_counter: Counter[str] = Counter()
        for message in related_messages:
            keyword_counter.update(ordered_message_tokens(message)[:8])
            channel_counter.update(
                [
                    str(
                        message.get("channel_title")
                        or message.get("handle")
                        or message.get("telegram_channel_id")
                        or ""
                    )
                ]
            )
            flags.update(risk_flags_for_text(str(message.get("text") or "")))
        top_messages = sorted(
            related_messages, key=telegram_signal_message_score, reverse=True
        )[:max_messages_per_signal]
        title = (
            telegram_signal_excerpt(top_messages[0], max_chars=80)
            if top_messages
            else canonical
        )
        confidence = min(
            1.0, 0.24 + len(related_messages) * 0.06 + len(channels) * 0.18
        )
        signals.append(
            {
                "article_id": "telegram-url:" + stable_hash(canonical, 24),
                "signal_type": "url_burst",
                "source_kind": "telegram_signal",
                "source_right_ids": source_right_ids,
                "signal_key": canonical,
                "signal_title": title,
                "related_telegram_count": len(related_messages),
                "related_telegram_channels_count": len(channels),
                "direct_url_count": len(related_messages),
                "keyword_match_count": 0,
                "first_seen_at": dates[0] if dates else "",
                "latest_seen_at": dates[-1] if dates else "",
                "top_related_messages": [
                    telegram_signal_message_payload(message) for message in top_messages
                ],
                "top_channels": sorted(channels)[:8],
                "top_channel_counts": [
                    {"channel": channel, "count": count}
                    for channel, count in channel_counter.most_common(8)
                    if channel
                ],
                "top_keywords": [
                    keyword for keyword, _count in keyword_counter.most_common(8)
                ],
                "confidence_score": round(confidence, 3),
                "risk_flags": sorted(flags),
                "signal_summary": f"동일 URL {len(related_messages)}건 공유",
            }
        )
    return signals


def telegram_topic_burst_signals(
    messages: list[dict[str, object]],
    *,
    min_messages: int,
    min_channels: int,
    max_messages_per_signal: int,
) -> list[dict[str, object]]:
    grouped: dict[tuple[str, str], list[dict[str, object]]] = defaultdict(list)
    for message in messages:
        tokens = ordered_message_tokens(message)
        event_tokens = signal_event_tokens_for_message(message, tokens)
        if not event_tokens:
            continue
        entities = signal_entity_tokens(tokens)
        if not entities:
            continue
        for event in event_tokens[:4]:
            for entity in entities[:4]:
                grouped[(entity, signal_event_label(event))].append(message)

    signals: list[dict[str, object]] = []
    seen_message_sets: set[tuple[str, ...]] = set()
    for (entity, event_label), related_messages in grouped.items():
        unique_by_key = {message_key(message): message for message in related_messages}
        related_messages = list(unique_by_key.values())
        channels = {
            str(message.get("handle") or message.get("telegram_channel_id") or "")
            for message in related_messages
        }
        source_right_ids = sorted(
            {
                str(message.get("source_right_id") or "")
                for message in related_messages
                if message.get("source_right_id")
            }
        )
        enough_single_channel_volume = len(related_messages) >= max(min_messages * 2, 5)
        if len(related_messages) < min_messages:
            continue
        if len(channels) < min_channels and not enough_single_channel_volume:
            continue
        message_set_key = tuple(
            sorted(message_key(message) for message in related_messages)[:24]
        )
        if message_set_key in seen_message_sets:
            continue
        seen_message_sets.add(message_set_key)
        dates = sorted(
            str(message.get("posted_at") or "")
            for message in related_messages
            if message.get("posted_at")
        )
        flags: set[str] = set()
        keyword_counter: Counter[str] = Counter()
        channel_counter: Counter[str] = Counter()
        for message in related_messages:
            keyword_counter.update(ordered_message_tokens(message)[:10])
            channel_counter.update(
                [
                    str(
                        message.get("channel_title")
                        or message.get("handle")
                        or message.get("telegram_channel_id")
                        or ""
                    )
                ]
            )
            flags.update(risk_flags_for_text(str(message.get("text") or "")))
        top_messages = sorted(
            related_messages, key=telegram_signal_message_score, reverse=True
        )[:max_messages_per_signal]
        confidence = min(
            1.0, 0.14 + len(related_messages) * 0.045 + len(channels) * 0.15
        )
        title = f"{entity} · {event_label}"
        signals.append(
            {
                "article_id": "telegram-topic:"
                + stable_hash(f"{entity}|{event_label}", 24),
                "signal_type": "topic_burst",
                "source_kind": "telegram_signal",
                "source_right_ids": source_right_ids,
                "signal_key": f"{entity}|{event_label}",
                "signal_title": title,
                "related_telegram_count": len(related_messages),
                "related_telegram_channels_count": len(channels),
                "direct_url_count": 0,
                "keyword_match_count": len(related_messages),
                "first_seen_at": dates[0] if dates else "",
                "latest_seen_at": dates[-1] if dates else "",
                "top_related_messages": [
                    telegram_signal_message_payload(message) for message in top_messages
                ],
                "top_channels": sorted(channels)[:8],
                "top_channel_counts": [
                    {"channel": channel, "count": count}
                    for channel, count in channel_counter.most_common(8)
                    if channel
                ],
                "top_keywords": [
                    keyword for keyword, _count in keyword_counter.most_common(8)
                ],
                "confidence_score": round(confidence, 3),
                "risk_flags": sorted(flags),
                "signal_summary": f"{event_label} 관련 언급 {len(related_messages)}건",
            }
        )
    return signals


def telegram_issue_signals(
    state: dict[str, object],
    config: dict[str, object] | None = None,
    *,
    limit: int = 20,
    now: datetime | None = None,
) -> list[dict[str, object]]:
    ensure_telegram_state(state)
    settings = telegram_sources_config(config or {})
    timezone_name = str((config or {}).get("timezone") or "Asia/Seoul")
    configured_limit = int(settings.get("signal_limit", limit))
    signal_limit = configured_limit if limit == 20 else min(configured_limit, limit)
    window_hours = int(settings.get("signal_window_hours", 72))
    min_messages = int(settings.get("signal_min_messages", 3))
    min_channels = int(settings.get("signal_min_channels", 2))
    max_messages_per_signal = int(settings.get("signal_max_messages_per_signal", 5))
    messages = [
        message
        for message in state.get("telegram_source_messages", [])
        if isinstance(message, dict)
        and not message.get("deleted_at")
        and source_is_authorized(message, config or {}, now, purpose="ai")
    ]
    reference_time = now or latest_signal_reference_time(messages, timezone_name)
    recent_messages = messages
    if reference_time and window_hours > 0:
        window_start = reference_time - timedelta(hours=window_hours)
        recent_messages = [
            message
            for message in messages
            if (
                parse_datetime(message.get("posted_at"), timezone_name)
                or reference_time
            )
            >= window_start
        ]
    signals = telegram_article_match_signals(state, config, now=reference_time or now)
    signals.extend(
        telegram_url_burst_signals(
            recent_messages,
            min_messages=max(2, min_messages),
            min_channels=max(1, min_channels),
            max_messages_per_signal=max_messages_per_signal,
        )
    )
    signals.extend(
        telegram_topic_burst_signals(
            recent_messages,
            min_messages=min_messages,
            min_channels=min_channels,
            max_messages_per_signal=max_messages_per_signal,
        )
    )
    return sorted(
        signals,
        key=lambda item: (
            float(item.get("confidence_score") or 0),
            int(item.get("related_telegram_channels_count") or 0),
            int(item.get("related_telegram_count") or 0),
        ),
        reverse=True,
    )[:signal_limit]


def score_channel_candidate(candidate: dict[str, object]) -> int:
    text = " ".join(
        str(candidate.get(key) or "")
        for key in ("handle", "username", "title", "description")
    ).casefold()
    score = 50
    for keyword in POSITIVE_CHANNEL_KEYWORDS:
        if keyword.casefold() in text:
            score += 6
    for keyword in STRONG_CHANNEL_KEYWORDS:
        if keyword.casefold() in text:
            score += 5
    for keyword in NEGATIVE_CHANNEL_KEYWORDS:
        if keyword.casefold() in text:
            score -= 22
    if any(
        keyword.casefold() in text
        for keyword in ("증권사", "리서치", "공시", "기업분석")
    ):
        score += 8
    if any(keyword.casefold() in text for keyword in ("급등", "추천", "수익")):
        score -= 8
    return max(0, min(100, score))


def upsert_channel_candidate(
    state: dict[str, object], candidate: dict[str, object]
) -> dict[str, object]:
    ensure_telegram_state(state)
    key = channel_key(candidate)
    candidates = state["telegram_channel_candidates"]  # type: ignore[index]
    for existing in candidates:
        if isinstance(existing, dict) and channel_key(existing) == key:
            existing.update(
                {
                    name: value
                    for name, value in candidate.items()
                    if value not in (None, "")
                }
            )
            existing["quality_score"] = score_channel_candidate(existing)
            return existing
    record = {
        "handle": normalize_channel_handle(
            candidate.get("handle") or candidate.get("username")
        ),
        "telegram_channel_id": candidate.get("telegram_channel_id")
        or candidate.get("channel_id")
        or None,
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


def error_label(error: BaseException) -> str:
    wait = flood_wait_seconds(error)
    if wait:
        return f"flood_wait_{wait}s"
    if isinstance(error, (TimeoutError, asyncio.TimeoutError)):
        return "timeout"
    message = str(error).strip()
    if "old message" in message.casefold() or "security error" in message.casefold():
        return "telegram_security_old_message"
    return error.__class__.__name__


def parse_handle_list(value: object) -> set[str]:
    if not value:
        return set()
    if isinstance(value, (list, tuple, set)):
        raw_items = [str(item) for item in value]
    else:
        raw_items = re.split(r"[,\s]+", str(value))
    return {
        normalize_channel_handle(item)
        for item in raw_items
        if normalize_channel_handle(item)
    }


class TelethonClientAdapter:
    def __init__(self, config: dict[str, object]) -> None:
        try:
            from telethon import TelegramClient  # type: ignore
            from telethon.sessions import StringSession  # type: ignore
        except (
            ImportError
        ) as exc:  # pragma: no cover - exercised through not_configured path
            raise RuntimeError("Telethon is not installed") from exc
        settings = telegram_sources_config(config)
        api_id = int(os.environ.get("TELEGRAM_API_ID") or settings.get("api_id") or 0)
        api_hash = os.environ.get("TELEGRAM_API_HASH", "").strip() or str(
            settings.get("api_hash") or ""
        )
        session = (
            os.environ.get("TELEGRAM_SESSION_STRING", "").strip()
            or os.environ.get("TELEGRAM_SESSION", "").strip()
            or str(settings.get("session") or "activist-reader")
        )
        if not api_id or not api_hash:
            raise RuntimeError("TELEGRAM_API_ID/TELEGRAM_API_HASH is required")
        session_arg = (
            StringSession(session)
            if len(session) > 80 and "/" not in session and "\\" not in session
            else session
        )
        self.client = TelegramClient(session_arg, api_id, api_hash)

    async def __aenter__(self) -> "TelethonClientAdapter":
        await self.client.start()
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.close()

    async def close(self) -> None:
        await self.client.disconnect()

    def _public_broadcast_record(
        self, entity: object, fallback: dict[str, object]
    ) -> dict[str, object]:
        handle = normalize_channel_handle(
            getattr(entity, "username", "")
            or fallback.get("handle")
            or fallback.get("username")
        )
        is_broadcast = bool(getattr(entity, "broadcast", False))
        is_group = bool(
            getattr(entity, "megagroup", False) or getattr(entity, "gigagroup", False)
        )
        if not handle or not is_broadcast or is_group:
            raise TelegramUnsafeSource("not_public_broadcast_channel")
        record = {
            "handle": handle,
            "telegram_channel_id": getattr(entity, "id", None)
            or fallback.get("telegram_channel_id")
            or fallback.get("channel_id"),
            "title": getattr(entity, "title", None) or fallback.get("title") or "",
            "description": getattr(entity, "about", None)
            or fallback.get("description")
            or "",
            "joined": True,
            "source_type": "public_channel",
            "is_public_channel": True,
        }
        record["quality_score"] = int(
            fallback.get("quality_score") or score_channel_candidate(record)
        )
        return record

    async def _get_public_broadcast_entity(
        self, channel: dict[str, object]
    ) -> tuple[object, dict[str, object]]:
        handle = normalize_channel_handle(
            channel.get("handle") or channel.get("username")
        )
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
        max_id: int = 0,
    ) -> list[dict[str, object]]:
        entity, record = await self._get_public_broadcast_entity(channel)
        expected_id = str(
            channel.get("telegram_channel_id") or channel.get("channel_id") or ""
        ).strip()
        resolved_id = str(
            record.get("telegram_channel_id") or record.get("channel_id") or ""
        ).strip()
        if expected_id and resolved_id and expected_id != resolved_id:
            raise TelegramUnsafeSource("channel_identity_review_required")
        messages: list[dict[str, object]] = []
        iter_kwargs: dict[str, object] = {"limit": limit}
        if max_id:
            iter_kwargs["max_id"] = max_id
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
                    "replies_count": getattr(
                        getattr(message, "replies", None), "replies", 0
                    )
                    or 0,
                }
            )
        if since is not None or not min_id:
            messages.reverse()
        return messages

    async def recommend_channels(
        self, seed_channel: dict[str, object], *, limit: int
    ) -> list[dict[str, object]]:
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
                record = self._public_broadcast_record(
                    chat, {"source": "recommendation"}
                )
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

    async def list_joined_public_channels(
        self, *, limit: int
    ) -> list[dict[str, object]]:
        channels: list[dict[str, object]] = []
        async for dialog in self.client.iter_dialogs(limit=limit):
            entity = getattr(dialog, "entity", None)
            try:
                record = self._public_broadcast_record(entity, {"source": "discovered"})
            except TelegramUnsafeSource:
                continue
            title = str(
                getattr(dialog, "title", "") or getattr(entity, "title", "") or ""
            )
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
    incremental_limit = max(1, int(settings.get("incremental_limit", 200)))
    incremental_max_pages = max(0, int(settings.get("incremental_max_pages", 0)))
    channel_timeout = max(
        1.0,
        float(
            settings.get(
                "incremental_channel_timeout_seconds",
                settings.get("backfill_channel_timeout_seconds", 60),
            )
        ),
    )
    inserted = updated = unchanged = failed = matches_inserted = 0
    pages_fetched = 0
    backlog_channels = 0
    match_context = build_article_match_context(state, config)
    message_index = build_telegram_message_index(state)
    match_index = build_telegram_match_index(state)

    for channel in enabled_channels(state):
        try:
            info = await asyncio.wait_for(
                client.get_channel_info(channel), timeout=channel_timeout
            )
            stored_id = str(
                channel.get("telegram_channel_id") or channel.get("channel_id") or ""
            ).strip()
            resolved_id = str(
                info.get("telegram_channel_id") or info.get("channel_id") or ""
            ).strip()
            if stored_id and resolved_id and stored_id != resolved_id:
                mark_channel_identity_review_required(channel, info, now)
                failed += 1
                continue
            else:
                channel = upsert_telegram_channel(state, {**channel, **info})
            if resolved_id and resolved_id != stored_id:
                # Canonical identity migration rewrites existing message and
                # match keys, so refresh the two ephemeral indexes once.
                message_index = build_telegram_message_index(state)
                match_index = build_telegram_match_index(state)
        except Exception as exc:  # noqa: BLE001 - channel failures should not stop the whole run.
            wait = flood_wait_seconds(exc)
            channel["last_error"] = (
                f"flood_wait_{wait}s" if wait else exc.__class__.__name__
            )
            failed += 1
            continue

        initial_min_id = int(channel.get("last_message_id") or 0)
        page_min_id = initial_min_id
        page_number = 0
        page_failed = False
        while True:
            limit = incremental_limit if initial_min_id else max(1, backfill_limit)
            try:
                raw_messages = await asyncio.wait_for(
                    client.iter_messages(channel, min_id=page_min_id, limit=limit),
                    timeout=channel_timeout,
                )
            except Exception as exc:  # noqa: BLE001 - retain the page cursor so the next run resumes safely.
                wait = flood_wait_seconds(exc)
                channel["last_error"] = (
                    f"flood_wait_{wait}s" if wait else exc.__class__.__name__
                )
                failed += 1
                page_failed = True
                break
            page_number += 1
            pages_fetched += 1
            max_message_id = page_min_id
            for raw_message in raw_messages:
                message = normalize_telegram_message(channel, raw_message, now)
                if not message.get("telegram_message_id"):
                    continue
                status = upsert_telegram_message(
                    state, message, message_index=message_index
                )
                inserted += int(status == "inserted")
                updated += int(status == "updated")
                unchanged += int(status == "unchanged")
                max_message_id = max(
                    max_message_id, int(message.get("telegram_message_id") or 0)
                )
                for match in match_message_to_articles(
                    state, message, config, match_context
                ):
                    if (
                        upsert_article_match(state, match, match_index=match_index)
                        == "inserted"
                    ):
                        matches_inserted += 1

            channel["last_message_id"] = max(
                int(channel.get("last_message_id") or 0), max_message_id
            )
            # The first collection remains an intentionally bounded backfill.  Once a
            # cursor exists, however, keep paging until Telegram is exhausted so a
            # burst larger than 500/1,000 messages cannot create a permanent gap.
            if not initial_min_id or len(raw_messages) < limit:
                break
            if max_message_id <= page_min_id:
                channel["last_error"] = "incremental_cursor_stalled"
                failed += 1
                page_failed = True
                break
            page_min_id = max_message_id
            if incremental_max_pages and page_number >= incremental_max_pages:
                # This is an intentional, durable checkpoint rather than a
                # collection failure. The next scheduled run resumes from the
                # acknowledged channel cursor.
                backlog_channels += 1
                break
        channel["last_collected_at"] = datetime_to_iso(now)
        if not page_failed:
            channel["last_error"] = None

    state["telegram_issue_signals"] = telegram_issue_signals(state, config, now=now)
    refresh_channel_runtime_quality(state)
    return {
        "telegram_channels": len(enabled_channels(state)),
        "telegram_messages_inserted": inserted,
        "telegram_messages_updated": updated,
        "telegram_messages_unchanged": unchanged,
        "telegram_matches_inserted": matches_inserted,
        "telegram_channel_failed": failed,
        "telegram_incremental_pages": pages_fetched,
        "telegram_incremental_backlog_channels": backlog_channels,
    }


async def _discover_with_client(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
    client: TelegramMessageClient,
) -> dict[str, int]:
    settings = telegram_sources_config(config)
    if not settings.get("discover_enabled", False):
        return {"telegram_candidates_found": 0, "telegram_candidates_joined": 0}
    found = joined = 0
    recommendation_limit = int(settings.get("recommendation_limit", 20))
    for channel in enabled_channels(state):
        try:
            candidates = await client.recommend_channels(
                channel, limit=recommendation_limit
            )
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


async def auto_join_candidates(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
    client: TelegramMessageClient,
) -> int:
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
            upsert_telegram_channel(
                state,
                {
                    **candidate,
                    "enabled": True,
                    "joined": True,
                    "source": "recommendation",
                },
            )
            joined += 1
        except Exception as exc:  # noqa: BLE001
            wait = flood_wait_seconds(exc)
            candidate["status"] = "failed"
            candidate["failure_reason"] = (
                f"flood_wait_{wait}s" if wait else exc.__class__.__name__
            )
    return joined


async def expand_similar_channels(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
    client: TelegramMessageClient,
    *,
    target_multiplier: float = 3.0,
    target_count: int = 0,
    recommendation_limit: int = 30,
    seed_limit: int = 0,
    seed_min_quality: int = 60,
    min_quality: int = 70,
    max_join: int = 0,
    delay_min_seconds: float = 3.0,
    delay_max_seconds: float = 9.0,
    dry_run: bool = False,
) -> dict[str, object]:
    """Expand public Telegram sources through high-quality similar-channel recommendations."""
    ensure_telegram_state(state)
    current_channels = enabled_channels(state)
    current_enabled = len(current_channels)
    computed_target = max(
        current_enabled, int(round(current_enabled * max(1.0, target_multiplier)))
    )
    if target_count > 0:
        computed_target = max(current_enabled, target_count)
    needed = max(0, computed_target - current_enabled)
    if max_join > 0:
        needed = min(needed, max_join)

    existing_keys = {
        channel_key(channel)
        for channel in state.get("telegram_source_channels", [])
        if isinstance(channel, dict)
    }
    existing_keys.update(
        channel_key(candidate)
        for candidate in state.get("telegram_channel_candidates", [])
        if isinstance(candidate, dict) and candidate.get("status") == "joined"
    )
    seeds = sorted(
        [
            channel
            for channel in current_channels
            if int(channel.get("quality_score") or score_channel_candidate(channel))
            >= seed_min_quality
        ],
        key=lambda channel: int(
            channel.get("quality_score") or score_channel_candidate(channel)
        ),
        reverse=True,
    )
    if seed_limit > 0:
        seeds = seeds[:seed_limit]

    found = 0
    eligible: dict[str, dict[str, object]] = {}
    seed_failures: list[dict[str, object]] = []
    for seed in seeds:
        if len(eligible) >= needed and needed > 0:
            break
        try:
            recommendations = await client.recommend_channels(
                seed, limit=max(1, recommendation_limit)
            )
        except Exception as exc:  # noqa: BLE001
            seed["last_recommendation_error"] = error_label(exc)
            seed_failures.append(
                {"handle": seed.get("handle") or "", "error": error_label(exc)}
            )
            continue
        seed["last_recommendation_checked_at"] = datetime_to_iso(now)
        for candidate in recommendations:
            if not isinstance(candidate, dict):
                continue
            candidate["last_checked_at"] = datetime_to_iso(now)
            candidate["source"] = candidate.get("source") or "recommendation"
            candidate_record = upsert_channel_candidate(state, candidate)
            found += 1
            if not is_collectable_public_channel(candidate_record):
                candidate_record["status"] = (
                    candidate_record.get("status") or "rejected"
                )
                candidate_record["failure_reason"] = "not_public_channel"
                continue
            key = channel_key(candidate_record)
            if key in existing_keys or key in eligible:
                continue
            score = int(
                candidate_record.get("quality_score")
                or score_channel_candidate(candidate_record)
            )
            if score < min_quality:
                candidate_record["status"] = candidate_record.get("status") or "pending"
                candidate_record["failure_reason"] = f"low_quality_{score}"
                continue
            candidate_record["status"] = "accepted" if dry_run else "pending_join"
            eligible[key] = candidate_record
            if len(eligible) >= needed and needed > 0:
                break

    joined = failed = 0
    joined_handles: list[str] = []
    failed_channels: list[dict[str, object]] = []
    flood_wait_until_retry = 0
    join_targets = sorted(
        eligible.values(),
        key=lambda candidate: int(
            candidate.get("quality_score") or score_channel_candidate(candidate)
        ),
        reverse=True,
    )
    if needed > 0:
        join_targets = join_targets[:needed]
    if not dry_run:
        for candidate in join_targets:
            try:
                if delay_max_seconds > 0 or delay_min_seconds > 0:
                    await asyncio.sleep(
                        random.uniform(
                            max(0, delay_min_seconds),
                            max(delay_min_seconds, delay_max_seconds),
                        )
                    )
                await client.join_channel(candidate)
                candidate["status"] = "joined"
                candidate["failure_reason"] = None
                channel = upsert_telegram_channel(
                    state,
                    {
                        **candidate,
                        "enabled": True,
                        "joined": True,
                        "source": "recommendation",
                        "quality_score": int(
                            candidate.get("quality_score")
                            or score_channel_candidate(candidate)
                        ),
                    },
                )
                joined += 1
                joined_handles.append(str(channel.get("handle") or ""))
                existing_keys.add(channel_key(channel))
            except Exception as exc:  # noqa: BLE001
                failed += 1
                label = error_label(exc)
                candidate["status"] = "failed"
                candidate["failure_reason"] = label
                failed_channels.append(
                    {"handle": candidate.get("handle") or "", "error": label}
                )
                wait = flood_wait_seconds(exc)
                if wait:
                    flood_wait_until_retry = wait
                    break

    final_enabled = (
        len(enabled_channels(state))
        if not dry_run
        else current_enabled + min(needed, len(join_targets))
    )
    return {
        "telegram_expand_current_enabled": current_enabled,
        "telegram_expand_target_count": computed_target,
        "telegram_expand_needed": max(0, computed_target - current_enabled),
        "telegram_expand_seed_count": len(seeds),
        "telegram_expand_candidates_found": found,
        "telegram_expand_eligible_candidates": len(eligible),
        "telegram_expand_join_targets": len(join_targets),
        "telegram_expand_joined": joined,
        "telegram_expand_failed": failed,
        "telegram_expand_final_enabled": final_enabled,
        "telegram_expand_joined_handles": joined_handles[:80],
        "telegram_expand_failed_channels": failed_channels[:20],
        "telegram_expand_seed_failures": seed_failures[:20],
        "telegram_expand_retry_after_seconds": flood_wait_until_retry,
        "dry_run": int(bool(dry_run)),
    }


async def _import_joined_with_client(
    state: dict[str, object],
    client: TelegramMessageClient,
    *,
    limit: int,
    enable: bool,
    min_quality: int,
    source: str,
    max_import: int = 0,
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
        if max_import > 0 and imported >= max_import:
            break
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
    max_import: int = 0,
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
                max_import=max_import,
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
                max_import=max_import,
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
    api_hash = os.environ.get("TELEGRAM_API_HASH", "").strip() or str(
        settings.get("api_hash") or ""
    )
    if not api_id or not api_hash:
        raise RuntimeError("TELEGRAM_API_ID/TELEGRAM_API_HASH is required")

    async def create_session() -> str:
        client = TelegramClient(StringSession(), api_id, api_hash)
        await client.start()
        session = client.session.save()
        await client.disconnect()
        return str(session)

    return asyncio.run(create_session())


def telegram_snapshot_payload(
    state: dict[str, object], config: dict[str, object]
) -> dict[str, object]:
    ensure_telegram_state(state)
    settings = telegram_sources_config(config)
    max_messages = max(0, int(settings.get("max_remote_messages", 0)))
    messages = list(state.get("telegram_source_messages", []))
    matches = list(state.get("telegram_article_matches", []))
    if max_messages:
        messages = messages[-max_messages:]
        message_keys = {
            message_key(message) for message in messages if isinstance(message, dict)
        }
        matches = [
            match
            for match in matches
            if isinstance(match, dict)
            and str(match.get("telegram_message_key") or "") in message_keys
        ]
    return {
        "channels": list(state.get("telegram_source_channels", [])),
        "messages": messages,
        "article_matches": matches,
        "issue_signals": list(state.get("telegram_issue_signals", [])),
        "channel_candidates": list(state.get("telegram_channel_candidates", [])),
    }


def pending_remote_messages(state: dict[str, object]) -> list[dict[str, object]]:
    """Return every locally stored message newer than its acknowledged remote cursor."""

    ensure_telegram_state(state)
    cursors = state["telegram_remote_sync_cursors"]  # type: ignore[index]
    pending = [
        message
        for message in state.get("telegram_source_messages", [])
        if isinstance(message, dict)
        and int(message.get("telegram_message_id") or 0)
        > int(cursors.get(channel_key(message), 0) or 0)  # type: ignore[union-attr]
    ]
    return sorted(
        pending,
        key=lambda message: (
            channel_key(message),
            int(message.get("telegram_message_id") or 0),
        ),
    )


def remote_response_error(response: dict[str, Any]) -> str:
    for key in ("error", "reason", "message"):
        value = str(response.get(key) or "").strip()
        if value:
            return value[:180]
    status_code = response.get("status_code")
    if status_code:
        return f"remote_http_{status_code}"
    return "remote_api_rejected"


def remote_ack_count(response: dict[str, Any], key: str) -> int | None:
    value = response.get(key)
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def remote_payload_bytes(payload: dict[str, object]) -> int:
    """Return the exact request-body size used by post_remote_action()."""

    return len(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    )


def telegram_remote_payload_budget(
    config: dict[str, object] | None = None,
) -> int:
    settings = telegram_sources_config(config or {})
    try:
        configured = int(
            settings.get(
                "remote_max_payload_bytes", REMOTE_TELEGRAM_SAFE_PAYLOAD_BYTES
            )
        )
    except (TypeError, ValueError):
        configured = REMOTE_TELEGRAM_SAFE_PAYLOAD_BYTES
    # The PHP default is 2 MiB. Keep signing/envelope and configuration drift
    # headroom even if a caller attempts to raise the local limit.
    return max(65_536, min(REMOTE_TELEGRAM_SAFE_PAYLOAD_BYTES, configured))


def telegram_remote_channel_batch_size(
    config: dict[str, object] | None = None,
) -> int:
    """Bound identities per DB transaction independently of payload bytes."""

    settings = telegram_sources_config(config or {})
    try:
        configured = int(
            settings.get(
                "remote_channel_batch_size",
                REMOTE_TELEGRAM_DEFAULT_CHANNEL_BATCH_SIZE,
            )
        )
    except (TypeError, ValueError):
        configured = REMOTE_TELEGRAM_DEFAULT_CHANNEL_BATCH_SIZE
    return max(1, min(REMOTE_TELEGRAM_MAX_CHANNEL_BATCH_SIZE, configured))


def sync_telegram_metadata_to_remote_api(
    state: dict[str, object],
    config: dict[str, object] | None = None,
    *,
    include_issue_signals: bool = True,
    replace_issue_signals: bool = False,
    replace_issue_signals_since: datetime | None = None,
    signal_rebuild_base_revision: int | None = None,
) -> dict[str, object]:
    """Persist channel state and, when requested, freshly derived signals."""

    if not remote_api_configured():
        return {
            "telegram_remote_metadata_synced": 0,
            "telegram_remote_metadata_failed": 0,
            "telegram_remote_metadata_skipped": 1,
            "telegram_remote_failed": 0,
        }
    cursors = state.get("telegram_remote_sync_cursors")
    if not isinstance(cursors, dict):
        cursors = {}
    channels: list[dict[str, object]] = []
    for channel in state.get("telegram_source_channels", []):
        if not isinstance(channel, dict):
            continue
        snapshot = dict(channel)
        # Metadata-only writes must never advance the durable DB cursor beyond
        # the last message batch that the remote API acknowledged. Otherwise a
        # partial message failure can be hidden by the following metadata call
        # and a fresh runner will permanently skip the missing IDs.
        snapshot["last_message_id"] = int(cursors.get(channel_key(channel), 0) or 0)
        channels.append(snapshot)
    if replace_issue_signals and not include_issue_signals:
        raise ValueError("signal_rebuild_requires_issue_signals")
    signals = (
        [
            signal
            for signal in state.get("telegram_issue_signals", [])
            if isinstance(signal, dict)
        ]
        if include_issue_signals
        else []
    )
    if len(channels) > 1000:
        return {
            "telegram_remote_metadata_synced": 0,
            "telegram_remote_metadata_failed": 1,
            "telegram_remote_failed": 1,
            "telegram_remote_last_error": "remote_metadata_too_many_channels",
        }
    if replace_issue_signals and replace_issue_signals_since is None:
        raise ValueError("replace_issue_signals_since_required")
    if replace_issue_signals and (
        signal_rebuild_base_revision is None or signal_rebuild_base_revision < 0
    ):
        raise ValueError("signal_rebuild_base_revision_required")
    if replace_issue_signals and any(
        not str(signal.get("article_id") or "") for signal in signals
    ):
        return {
            "telegram_remote_metadata_synced": 0,
            "telegram_remote_metadata_failed": 1,
            "telegram_remote_failed": 1,
            "telegram_remote_last_error": "signal_rebuild_invalid_signal_id",
        }
    replace_since_iso = (
        datetime_to_iso(replace_issue_signals_since)
        if replace_issue_signals_since is not None
        else ""
    )
    signal_rebuild_token = ""
    if replace_issue_signals:
        rebuild_body = json.dumps(
            {
                "base_revision": signal_rebuild_base_revision,
                "replace_since": replace_since_iso,
                "signals": signals,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        signal_rebuild_token = hashlib.sha256(rebuild_body).hexdigest()
    payload_budget = telegram_remote_payload_budget(config)
    channel_batch_size = telegram_remote_channel_batch_size(config)
    if not replace_issue_signals and len(channels) > channel_batch_size:
        # Channel rows contain runtime diagnostics and can be substantially
        # larger than their count suggests. Persist them in bounded transactions
        # before the independent signal payload so PHP/MySQL processing time,
        # rather than only request bytes, cannot turn a routine metadata refresh
        # into a client-side timeout.
        batched_channels = 0
        batched_signals = 0
        batched_deleted = 0
        batched_max_request_bytes = 0
        for offset in range(0, len(channels), channel_batch_size):
            channel_batch = channels[offset : offset + channel_batch_size]
            batch_cursors = {
                channel_key(channel): int(
                    cursors.get(channel_key(channel), 0) or 0
                )
                for channel in channel_batch
            }
            batch_result = sync_telegram_metadata_to_remote_api(
                {
                    "telegram_source_channels": channel_batch,
                    "telegram_remote_sync_cursors": batch_cursors,
                    "telegram_issue_signals": [],
                },
                config,
                include_issue_signals=False,
            )
            batched_max_request_bytes = max(
                batched_max_request_bytes,
                int(batch_result.get("telegram_remote_max_request_bytes") or 0),
            )
            if int(batch_result.get("telegram_remote_failed") or 0):
                failed_result = dict(batch_result)
                failed_result["telegram_remote_channels"] = batched_channels + int(
                    batch_result.get("telegram_remote_channels") or 0
                )
                failed_result["telegram_remote_signals"] = batched_signals
                failed_result["telegram_remote_signals_deleted"] = batched_deleted
                failed_result["telegram_remote_max_request_bytes"] = (
                    batched_max_request_bytes
                )
                return failed_result
            batched_channels += int(
                batch_result.get("telegram_remote_channels") or 0
            )

        if include_issue_signals and signals:
            signal_result = sync_telegram_metadata_to_remote_api(
                {
                    "telegram_source_channels": [],
                    "telegram_remote_sync_cursors": {},
                    "telegram_issue_signals": signals,
                },
                config,
                include_issue_signals=True,
            )
            batched_max_request_bytes = max(
                batched_max_request_bytes,
                int(signal_result.get("telegram_remote_max_request_bytes") or 0),
            )
            if int(signal_result.get("telegram_remote_failed") or 0):
                failed_result = dict(signal_result)
                failed_result["telegram_remote_channels"] = batched_channels
                failed_result["telegram_remote_signals"] = int(
                    signal_result.get("telegram_remote_signals") or 0
                )
                failed_result["telegram_remote_signals_deleted"] = int(
                    signal_result.get("telegram_remote_signals_deleted") or 0
                )
                failed_result["telegram_remote_max_request_bytes"] = (
                    batched_max_request_bytes
                )
                return failed_result
            batched_signals += int(
                signal_result.get("telegram_remote_signals") or 0
            )
            batched_deleted += int(
                signal_result.get("telegram_remote_signals_deleted") or 0
            )
        return {
            "telegram_remote_metadata_synced": 1,
            "telegram_remote_metadata_failed": 0,
            "telegram_remote_failed": 0,
            "telegram_remote_channels": batched_channels,
            "telegram_remote_signals": batched_signals,
            "telegram_remote_signals_deleted": batched_deleted,
            "telegram_remote_max_request_bytes": batched_max_request_bytes,
        }
    base_payload: dict[str, object] = {
        # An authoritative staging request is deliberately signal-only. Channel
        # canonicalization can rewrite message/match identities, so the API
        # fences it as a separate live transaction after signal finalization.
        "channels": [] if replace_issue_signals else channels,
        "messages": [],
        "article_matches": [],
        "issue_signals": [],
        # The current PHP endpoint does not persist discovery candidates. Do
        # not spend the signed request budget on an ignored collection.
        "channel_candidates": [],
    }
    if replace_issue_signals and channels:
        channel_payload = dict(base_payload)
        channel_payload["channels"] = channels
        if remote_payload_bytes(channel_payload) > payload_budget:
            return {
                "telegram_remote_metadata_synced": 0,
                "telegram_remote_metadata_failed": 1,
                "telegram_remote_failed": 1,
                "telegram_remote_last_error": "remote_metadata_payload_too_large",
            }
    remote_channels = 0
    remote_signals = 0
    max_request_bytes = 0
    signal_index = 0
    first_request = True
    while first_request or signal_index < len(signals):
        upper = min(
            len(signals), signal_index + REMOTE_TELEGRAM_SIGNAL_BATCH_SIZE
        )
        if not signals:
            upper = signal_index
        low = signal_index + 1
        high = upper
        best_end = signal_index if not signals else -1
        best_payload: dict[str, object] | None = None
        while low <= high:
            middle = (low + high) // 2
            candidate_payload = dict(base_payload)
            candidate_payload["channels"] = (
                channels if first_request and not replace_issue_signals else []
            )
            candidate_payload["issue_signals"] = signals[signal_index:middle]
            if replace_issue_signals:
                candidate_payload["signal_rebuild_token"] = signal_rebuild_token
                if first_request:
                    candidate_payload["signal_rebuild_begin"] = True
                    candidate_payload["signal_rebuild_base_revision"] = (
                        signal_rebuild_base_revision
                    )
            candidate_size = remote_payload_bytes(candidate_payload)
            if candidate_size <= payload_budget:
                best_end = middle
                best_payload = candidate_payload
                low = middle + 1
            else:
                high = middle - 1
        if not signals:
            best_payload = dict(base_payload)
            best_payload["channels"] = (
                channels if first_request and not replace_issue_signals else []
            )
            if replace_issue_signals:
                best_payload["signal_rebuild_token"] = signal_rebuild_token
                best_payload["signal_rebuild_begin"] = True
                best_payload["signal_rebuild_base_revision"] = (
                    signal_rebuild_base_revision
                )
        elif best_payload is None and first_request and not replace_issue_signals:
            channel_only_payload = dict(base_payload)
            channel_only_payload["channels"] = channels
            if replace_issue_signals:
                channel_only_payload["signal_rebuild_token"] = signal_rebuild_token
                channel_only_payload["signal_rebuild_begin"] = True
                channel_only_payload["signal_rebuild_base_revision"] = (
                    signal_rebuild_base_revision
                )
            if remote_payload_bytes(channel_only_payload) <= payload_budget:
                best_payload = channel_only_payload
                best_end = signal_index
        if best_payload is None or remote_payload_bytes(best_payload) > payload_budget:
            return {
                "telegram_remote_metadata_synced": 0,
                "telegram_remote_metadata_failed": 1,
                "telegram_remote_failed": 1,
                "telegram_remote_last_error": "remote_metadata_payload_too_large",
            }
        payload = best_payload
        request_bytes = remote_payload_bytes(payload)
        max_request_bytes = max(max_request_bytes, request_bytes)
        try:
            response = post_remote_action("upsert_telegram_snapshot", payload)
        except Exception as exc:  # noqa: BLE001
            return {
                "telegram_remote_metadata_synced": 0,
                "telegram_remote_metadata_failed": 1,
                "telegram_remote_failed": 1,
                "telegram_remote_last_error": error_label(exc),
                "telegram_remote_max_request_bytes": max_request_bytes,
            }
        if not response.get("ok"):
            return {
                "telegram_remote_metadata_synced": 0,
                "telegram_remote_metadata_failed": 1,
                "telegram_remote_failed": 1,
                "telegram_remote_last_error": remote_response_error(response),
                "telegram_remote_last_status_code": int(
                    response.get("status_code") or 0
                ),
                "telegram_remote_max_request_bytes": max_request_bytes,
            }
        acknowledged_channels = remote_ack_count(response, "channels")
        if acknowledged_channels != len(payload["channels"]):
            return {
                "telegram_remote_metadata_synced": 0,
                "telegram_remote_metadata_failed": 1,
                "telegram_remote_failed": 1,
                "telegram_remote_last_error": "remote_channel_ack_mismatch",
                "telegram_remote_max_request_bytes": max_request_bytes,
            }
        remote_channels += len(payload["channels"])
        signal_ack_key = (
            "issue_signals_staged"
            if replace_issue_signals
            else "issue_signals"
        )
        if signal_ack_key not in response:
            return {
                "telegram_remote_metadata_synced": 0,
                "telegram_remote_metadata_failed": 1,
                "telegram_remote_failed": 1,
                "telegram_remote_last_error": (
                    "signal_rebuild_staging_ack_missing"
                    if replace_issue_signals
                    else "remote_signal_ack_missing"
                ),
                "telegram_remote_max_request_bytes": max_request_bytes,
            }
        acknowledged_signals = remote_ack_count(response, signal_ack_key)
        if acknowledged_signals != len(payload["issue_signals"]):
            return {
                "telegram_remote_metadata_synced": 0,
                "telegram_remote_metadata_failed": 1,
                "telegram_remote_failed": 1,
                "telegram_remote_last_error": (
                    "signal_rebuild_partial_signal_ack"
                    if replace_issue_signals
                    else "remote_partial_signal_ack"
                ),
                "telegram_remote_max_request_bytes": max_request_bytes,
            }
        if replace_issue_signals and str(
            response.get("signal_rebuild_token") or ""
        ) != signal_rebuild_token:
            return {
                "telegram_remote_metadata_synced": 0,
                "telegram_remote_metadata_failed": 1,
                "telegram_remote_failed": 1,
                "telegram_remote_last_error": "signal_rebuild_token_ack_mismatch",
                "telegram_remote_max_request_bytes": max_request_bytes,
            }
        remote_signals += acknowledged_signals
        first_request = False
        if not signals:
            break
        signal_index = best_end

    signals_deleted = 0
    if replace_issue_signals:
        assert replace_issue_signals_since is not None
        finalize_payload = {
            "channels": [],
            "messages": [],
            "article_matches": [],
            "issue_signals": [],
            "channel_candidates": [],
            "replace_issue_signals": True,
            "signal_rebuild_token": signal_rebuild_token,
            "signal_rebuild_finalize": True,
            "issue_signals_replace_since": replace_since_iso,
        }
        finalize_bytes = remote_payload_bytes(finalize_payload)
        max_request_bytes = max(max_request_bytes, finalize_bytes)
        if finalize_bytes > payload_budget:
            return {
                "telegram_remote_metadata_synced": 0,
                "telegram_remote_metadata_failed": 1,
                "telegram_remote_failed": 1,
                "telegram_remote_last_error": "signal_rebuild_finalize_payload_too_large",
                "telegram_remote_max_request_bytes": max_request_bytes,
            }
        try:
            response = post_remote_action(
                "upsert_telegram_snapshot", finalize_payload
            )
        except Exception as exc:  # noqa: BLE001
            return {
                "telegram_remote_metadata_synced": 0,
                "telegram_remote_metadata_failed": 1,
                "telegram_remote_failed": 1,
                "telegram_remote_last_error": error_label(exc),
                "telegram_remote_max_request_bytes": max_request_bytes,
            }
        if not response.get("ok"):
            return {
                "telegram_remote_metadata_synced": 0,
                "telegram_remote_metadata_failed": 1,
                "telegram_remote_failed": 1,
                "telegram_remote_last_error": remote_response_error(response),
                "telegram_remote_last_status_code": int(
                    response.get("status_code") or 0
                ),
                "telegram_remote_max_request_bytes": max_request_bytes,
            }
        if str(response.get("signal_rebuild_finalized") or "") != signal_rebuild_token:
            return {
                "telegram_remote_metadata_synced": 0,
                "telegram_remote_metadata_failed": 1,
                "telegram_remote_failed": 1,
                "telegram_remote_last_error": "signal_rebuild_finalize_ack_mismatch",
                "telegram_remote_max_request_bytes": max_request_bytes,
            }
        finalized_signals_ack = remote_ack_count(response, "issue_signals")
        expected_finalized_signals = (
            0 if response.get("signal_rebuild_idempotent") is True else remote_signals
        )
        if finalized_signals_ack != expected_finalized_signals:
            return {
                "telegram_remote_metadata_synced": 0,
                "telegram_remote_metadata_failed": 1,
                "telegram_remote_failed": 1,
                "telegram_remote_last_error": (
                    "signal_rebuild_finalize_signal_ack_mismatch"
                ),
                "telegram_remote_max_request_bytes": max_request_bytes,
            }
        signals_deleted_ack = remote_ack_count(response, "issue_signals_deleted")
        if signals_deleted_ack is None:
            return {
                "telegram_remote_metadata_synced": 0,
                "telegram_remote_metadata_failed": 1,
                "telegram_remote_failed": 1,
                "telegram_remote_last_error": "signal_rebuild_deleted_ack_invalid",
                "telegram_remote_max_request_bytes": max_request_bytes,
            }
        signals_deleted = signals_deleted_ack
        if channels:
            channel_summary = sync_telegram_metadata_to_remote_api(
                {
                    "telegram_source_channels": channels,
                    "telegram_remote_sync_cursors": cursors,
                    "telegram_issue_signals": [],
                },
                config,
                include_issue_signals=False,
            )
            max_request_bytes = max(
                max_request_bytes,
                int(channel_summary.get("telegram_remote_max_request_bytes") or 0),
            )
            if int(channel_summary.get("telegram_remote_failed") or 0):
                return {
                    **channel_summary,
                    "telegram_remote_signals": remote_signals,
                    "telegram_remote_signals_deleted": signals_deleted,
                    "telegram_remote_max_request_bytes": max_request_bytes,
                }
            remote_channels += int(
                channel_summary.get("telegram_remote_channels") or 0
            )
    return {
        "telegram_remote_metadata_synced": 1,
        "telegram_remote_metadata_failed": 0,
        "telegram_remote_failed": 0,
        "telegram_remote_channels": remote_channels,
        "telegram_remote_signals": remote_signals,
        "telegram_remote_signals_deleted": signals_deleted,
        "telegram_remote_max_request_bytes": max_request_bytes,
    }


def sync_telegram_to_remote_api(
    state: dict[str, object], config: dict[str, object]
) -> dict[str, object]:
    if not remote_api_configured():
        return {
            "telegram_remote_synced": 0,
            "telegram_remote_failed": 0,
            "telegram_remote_skipped": 1,
        }
    messages = pending_remote_messages(state)
    if not messages:
        return {
            "telegram_remote_synced": 0,
            "telegram_remote_failed": 0,
            "telegram_remote_pending": 0,
        }
    message_keys = {message_key(message) for message in messages}
    matches = [
        match
        for match in state.get("telegram_article_matches", [])
        if isinstance(match, dict)
        and str(match.get("telegram_message_key") or "") in message_keys
    ]
    result = sync_telegram_batch_to_remote_api(
        state,
        config,
        messages=messages,
        matches=matches,
        advance_cursors=True,
    )
    result["telegram_remote_pending"] = len(pending_remote_messages(state))
    return result


def sync_telegram_batch_to_remote_api(
    state: dict[str, object],
    config: dict[str, object],
    *,
    messages: list[dict[str, object]],
    matches: list[dict[str, object]],
    advance_cursors: bool = False,
) -> dict[str, object]:
    if not messages:
        return {}
    if not remote_api_configured():
        return {
            "telegram_remote_synced": 0,
            "telegram_remote_failed": 0,
            "telegram_remote_skipped": 1,
        }
    settings = telegram_sources_config(config)
    batch_size = max(1, min(2500, int(settings.get("remote_batch_size", 300))))
    channel_batch_size = telegram_remote_channel_batch_size(config)
    payload_budget = telegram_remote_payload_budget(config)
    synced = failed = remote_channels = remote_messages = remote_matches = 0
    last_error = ""
    last_status_code = 0
    max_request_bytes = 0
    channels = list(state.get("telegram_source_channels", []))
    message_keys = {message_key(message) for message in messages}
    relevant_matches_by_message: dict[str, list[dict[str, object]]] = defaultdict(list)
    for match in matches:
        if not isinstance(match, dict):
            continue
        telegram_message_key = str(match.get("telegram_message_key") or "")
        if telegram_message_key in message_keys:
            relevant_matches_by_message[telegram_message_key].append(match)
    ordered_messages = sorted(
        messages,
        key=lambda message: (
            channel_key(message),
            int(message.get("telegram_message_id") or 0),
        ),
    )
    cursors = state.get("telegram_remote_sync_cursors")
    if not isinstance(cursors, dict):
        cursors = {}
        state["telegram_remote_sync_cursors"] = cursors

    def payload_for(chunk: list[dict[str, object]]) -> dict[str, object]:
        chunk_cursor_by_channel: dict[str, int] = {}
        for message in chunk:
            identity = channel_key(message)
            chunk_cursor_by_channel[identity] = max(
                chunk_cursor_by_channel.get(identity, 0),
                int(message.get("telegram_message_id") or 0),
            )
        chunk_channels: list[dict[str, object]] = []
        for channel in channels:
            if not isinstance(channel, dict):
                continue
            identity = channel_key(channel)
            if identity not in chunk_cursor_by_channel:
                continue
            channel_snapshot = dict(channel)
            channel_snapshot["last_message_id"] = chunk_cursor_by_channel[identity]
            chunk_channels.append(channel_snapshot)
        chunk_matches = [
            match
            for message in chunk
            for match in relevant_matches_by_message.get(message_key(message), [])
        ]
        return {
            "channels": chunk_channels,
            "messages": chunk,
            "article_matches": chunk_matches,
            # Message durability and signal replacement have different
            # atomicity requirements. Repeating every hydrated signal in
            # each message chunk can exceed the PHP record cap before the
            # first durable cursor advances. Signals are sent once, in
            # bounded batches, by sync_telegram_metadata_to_remote_api().
            "issue_signals": [],
            "channel_candidates": [],
        }

    index = 0
    while index < len(ordered_messages):
        upper = min(len(ordered_messages), index + batch_size)
        low = index + 1
        high = upper
        best_end = -1
        best_payload: dict[str, object] | None = None
        while low <= high:
            middle = (low + high) // 2
            candidate_payload = payload_for(ordered_messages[index:middle])
            candidate_size = remote_payload_bytes(candidate_payload)
            candidate_matches = candidate_payload["article_matches"]
            candidate_channels = candidate_payload["channels"]
            within_record_caps = (
                isinstance(candidate_matches, list)
                and len(candidate_matches) <= 10_000
                and isinstance(candidate_channels, list)
                and len(candidate_channels) <= 1000
                and len(candidate_channels) <= channel_batch_size
            )
            if within_record_caps and candidate_size <= payload_budget:
                best_end = middle
                best_payload = candidate_payload
                low = middle + 1
            else:
                high = middle - 1
        if best_payload is None:
            failed += 1
            last_error = "remote_payload_too_large_single_message"
            max_request_bytes = max(
                max_request_bytes,
                remote_payload_bytes(payload_for(ordered_messages[index : index + 1])),
            )
            break
        chunk = ordered_messages[index:best_end]
        chunk_channel_identities = {channel_key(message) for message in chunk}
        payload_channels = best_payload["channels"]
        payload_channel_identities = (
            [
                channel_key(channel)
                for channel in payload_channels
                if isinstance(channel, dict)
            ]
            if isinstance(payload_channels, list)
            else []
        )
        if (
            not isinstance(payload_channels, list)
            or set(payload_channel_identities) != chunk_channel_identities
            or len(payload_channel_identities) != len(set(payload_channel_identities))
        ):
            failed += 1
            last_error = "remote_channel_snapshot_missing_or_ambiguous"
            break
        request_bytes = remote_payload_bytes(best_payload)
        max_request_bytes = max(max_request_bytes, request_bytes)
        try:
            response = post_remote_action(
                "upsert_telegram_snapshot",
                best_payload,
            )
        except Exception as exc:  # noqa: BLE001
            failed += 1
            last_error = error_label(exc)
            # A later acknowledgement must never advance beyond an unpersisted
            # earlier chunk; stop here and resume from the stored cursor next run.
            break
        if response.get("ok"):
            acknowledged = remote_ack_count(response, "messages")
            if acknowledged != len(chunk):
                failed += 1
                last_error = "remote_partial_message_ack"
                break
            acknowledged_matches = remote_ack_count(response, "article_matches")
            expected_matches = len(best_payload["article_matches"])  # type: ignore[arg-type]
            if acknowledged_matches != expected_matches:
                failed += 1
                last_error = "remote_partial_match_ack"
                break
            acknowledged_channels = remote_ack_count(response, "channels")
            expected_channels = len(best_payload["channels"])  # type: ignore[arg-type]
            if acknowledged_channels != expected_channels:
                failed += 1
                last_error = "remote_partial_channel_ack"
                break
            synced += 1
            remote_messages += len(chunk)
            remote_matches += expected_matches
            remote_channels += expected_channels
            if advance_cursors:
                for message in chunk:
                    identity = channel_key(message)
                    cursors[identity] = max(
                        int(cursors.get(identity, 0) or 0),
                        int(message.get("telegram_message_id") or 0),
                    )
            index = best_end
        else:
            failed += 1
            last_error = remote_response_error(response)
            last_status_code = int(response.get("status_code") or 0)
            break
    result: dict[str, object] = {
        "telegram_remote_synced": synced,
        "telegram_remote_failed": failed,
        "telegram_remote_messages": remote_messages,
        "telegram_remote_matches": remote_matches,
        "telegram_remote_channels": remote_channels,
        "telegram_remote_max_request_bytes": max_request_bytes,
    }
    if last_error:
        result["telegram_remote_last_error"] = last_error
    if last_status_code:
        result["telegram_remote_last_status_code"] = last_status_code
    return result


def collect_telegram_sources(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
    client: TelegramMessageClient | None = None,
) -> dict[str, int]:
    ensure_telegram_state(state)
    registered = register_configured_channels(state, config)
    rights_blocked = apply_channel_source_rights(state, config, now)
    if not telegram_sources_enabled(config):
        return {
            "telegram_source_channels_registered": registered,
            "telegram_source_rights_blocked": rights_blocked,
            "telegram_source_skipped": 1,
        }

    if client is None:
        try:
            adapter = TelethonClientAdapter(config)
        except RuntimeError:
            return {
                "telegram_source_channels_registered": registered,
                "telegram_source_not_configured": 1,
            }

        async def run_with_adapter() -> dict[str, int]:
            async with adapter as opened:
                summary = await _collect_with_client(state, config, now, opened)
                summary.update(await _discover_with_client(state, config, now, opened))
                return summary

        try:
            summary = asyncio.run(run_with_adapter())
        except Exception as exc:  # noqa: BLE001 - Telegram auth/session errors must not break feed builds.
            summary = {
                "telegram_source_channels_registered": registered,
                "telegram_source_connect_failed": 1,
                "telegram_source_error": exc.__class__.__name__,
            }
            state.setdefault("telegram_source_runs", [])
            state["telegram_source_runs"].append(
                telegram_run_record(now, "collect", summary)
            )  # type: ignore[index, union-attr]
            return summary
    else:

        async def run_with_client() -> dict[str, int]:
            summary = await _collect_with_client(state, config, now, client)
            summary.update(await _discover_with_client(state, config, now, client))
            return summary

        summary = asyncio.run(run_with_client())

    summary["telegram_source_channels_registered"] = registered
    summary["telegram_source_rights_blocked"] = rights_blocked
    if pending_remote_messages(state):
        summary.update(sync_telegram_to_remote_api(state, config))
    message_remote_failed = int(summary.get("telegram_remote_failed") or 0)
    message_remote_pending = int(summary.get("telegram_remote_pending") or 0)
    summary["telegram_remote_message_failed"] = message_remote_failed
    summary["telegram_remote_message_max_request_bytes"] = int(
        summary.get("telegram_remote_max_request_bytes") or 0
    )
    if summary.get("telegram_remote_last_error"):
        summary["telegram_remote_message_last_error"] = summary[
            "telegram_remote_last_error"
        ]
    if remote_api_configured() and (
        message_remote_failed or message_remote_pending
    ):
        # A timed-out request may still be finishing server-side. Do not pile a
        # metadata transaction onto the same DB lock or overwrite the evidence
        # from the first unacknowledged message chunk.
        metadata_summary: dict[str, object] = {
            "telegram_remote_metadata_synced": 0,
            "telegram_remote_metadata_failed": 0,
            "telegram_remote_metadata_skipped_due_pending": 1,
            "telegram_remote_failed": 0,
        }
    else:
        metadata_summary = sync_telegram_metadata_to_remote_api(state, config)
    summary["telegram_remote_failed"] = int(
        summary.get("telegram_remote_failed") or 0
    ) + int(metadata_summary.get("telegram_remote_failed") or 0)
    for key, value in metadata_summary.items():
        if key == "telegram_remote_max_request_bytes":
            summary[key] = max(
                int(summary.get(key) or 0),
                int(value or 0),
            )
        elif key != "telegram_remote_failed":
            summary[key] = value  # type: ignore[assignment]
    remote_pending = int(summary.get("telegram_remote_pending") or 0)
    remote_failed = int(summary.get("telegram_remote_failed") or 0)
    if remote_api_configured() and (remote_pending or remote_failed):
        # Never discard an item that has not been durably acknowledged by the
        # MySQL API. A failed run can safely re-fetch from the last DB cursor.
        summary.update(
            {
                "telegram_messages_pruned": 0,
                "telegram_matches_pruned": 0,
                "telegram_prune_deferred": 1,
            }
        )
    else:
        summary.update(prune_telegram_state(state, config, now))
        summary["telegram_prune_deferred"] = 0
    state.setdefault("telegram_source_runs", [])
    state["telegram_source_runs"].append(telegram_run_record(now, "collect", summary))  # type: ignore[index, union-attr]
    return summary


async def _backfill_messages_with_client_legacy_batch(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
    client: TelegramMessageClient,
    *,
    days: int,
    limit_per_channel: int,
    channel_limit: int,
    progress: bool = False,
    only_handles: set[str] | None = None,
    skip_handles: set[str] | None = None,
    start_after_handle: str = "",
    expected_selection_fingerprint: str = "",
    max_messages: int = 0,
    checkpoint_callback: Callable[[dict[str, object]], None] | None = None,
    force_remote_resync: bool = False,
) -> dict[str, object]:
    settings = telegram_sources_config(config)
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    channel_timeout = max(
        5.0, float(settings.get("backfill_channel_timeout_seconds", 60))
    )
    since = now - timedelta(days=max(1, days))
    selection = _select_backfill_channels(
        state,
        only_handles=only_handles,
        skip_handles=skip_handles,
        start_after_handle=start_after_handle,
        channel_limit=channel_limit,
        expected_selection_fingerprint=expected_selection_fingerprint,
    )
    channels = selection.channels
    worker_count = max(1, int(settings.get("backfill_channel_workers", 1)))

    inserted = updated = unchanged = failed = seen = outside_window = (
        matches_inserted
    ) = 0
    completed_channels = 0
    truncated_channels = 0
    global_limit_reached = False
    stop_backfill = False
    resume_handle = ""
    resume_after_handle = ""
    last_completed_handle = ""
    touched_messages: list[dict[str, object]] = []
    per_channel: list[dict[str, object]] = []
    match_context = build_article_match_context(state, config)
    message_index = build_telegram_message_index(state)
    match_index = build_telegram_match_index(state)

    def emit_checkpoint(progress_record: dict[str, object]) -> None:
        if checkpoint_callback:
            checkpoint_callback(
                {
                    **progress_record,
                    **_current_backfill_selection_metrics(
                        state,
                        selected_count=len(channels),
                    ),
                }
            )

    async def fetch_channel(
        index: int, channel: dict[str, object]
    ) -> TelegramBackfillFetchResult:
        started_at = datetime.now()
        monotonic_started_at = time.monotonic()
        channel_info: dict[str, object] = {}
        if progress:
            print(
                f"[{index}/{len(channels)}] @{channel.get('handle') or ''} backfill start",
                flush=True,
            )
        try:

            async def load_messages() -> tuple[list[dict[str, object]], bool, int]:
                nonlocal channel_info
                channel_info = await client.get_channel_info(channel)
                stored_id = str(
                    channel.get("telegram_channel_id")
                    or channel.get("channel_id")
                    or ""
                ).strip()
                resolved_id = str(
                    channel_info.get("telegram_channel_id")
                    or channel_info.get("channel_id")
                    or ""
                ).strip()
                if stored_id and resolved_id and stored_id != resolved_id:
                    raise TelegramUnsafeSource("channel_identity_review_required")
                resolved_channel = {**channel, **channel_info}
                messages: list[dict[str, object]] = []
                page_max_id = 0
                fetch_cap = max_messages + 1 if max_messages > 0 else 0
                while True:
                    remaining = (
                        fetch_cap - len(messages) if fetch_cap else limit_per_channel
                    )
                    if fetch_cap and remaining <= 0:
                        return messages, True, page_max_id
                    page_limit = (
                        min(limit_per_channel, remaining)
                        if fetch_cap
                        else limit_per_channel
                    )
                    page = await client.iter_messages(
                        resolved_channel,
                        min_id=0,
                        max_id=page_max_id,
                        limit=page_limit,
                        since=since,
                    )
                    if not page:
                        return messages, False, 0
                    messages.extend(page)
                    message_ids = [
                        int(
                            message.get("id") or message.get("telegram_message_id") or 0
                        )
                        for message in page
                    ]
                    positive_ids = [
                        message_id for message_id in message_ids if message_id > 0
                    ]
                    if not positive_ids:
                        raise RuntimeError("telegram_history_cursor_missing")
                    next_max_id = min(positive_ids)
                    if page_max_id and next_max_id >= page_max_id:
                        raise RuntimeError("telegram_history_cursor_stalled")
                    if len(page) < page_limit:
                        return messages, False, 0
                    page_max_id = next_max_id
                    if fetch_cap and len(messages) >= fetch_cap:
                        return messages, True, page_max_id

            raw_messages, fetch_truncated, next_max_id = await asyncio.wait_for(
                load_messages(),
                timeout=channel_timeout,
            )
            raw_messages.sort(
                key=lambda message: int(
                    message.get("id") or message.get("telegram_message_id") or 0
                )
            )
            return TelegramBackfillFetchResult(
                index=index,
                total=len(channels),
                channel=channel,
                channel_info=channel_info,
                raw_messages=raw_messages,
                fetch_truncated=fetch_truncated,
                next_max_id=next_max_id,
                error=None,
                started_at=started_at,
                monotonic_started_at=monotonic_started_at,
                fetch_elapsed_seconds=round(time.monotonic() - monotonic_started_at, 2),
            )
        except Exception as exc:  # noqa: BLE001
            return TelegramBackfillFetchResult(
                index=index,
                total=len(channels),
                channel=channel,
                channel_info=channel_info,
                raw_messages=[],
                fetch_truncated=False,
                next_max_id=0,
                error=exc,
                started_at=started_at,
                monotonic_started_at=monotonic_started_at,
                fetch_elapsed_seconds=round(time.monotonic() - monotonic_started_at, 2),
            )

    indexed_channels = list(enumerate(channels, start=1))
    for batch_start in range(0, len(indexed_channels), worker_count):
        if stop_backfill:
            break
        batch = indexed_channels[batch_start : batch_start + worker_count]
        fetch_results = await asyncio.gather(
            *(fetch_channel(index, channel) for index, channel in batch)
        )
        identity_changed = False
        for fetch_result in fetch_results:
            if not fetch_result.channel_info:
                continue
            previous_key = channel_key(fetch_result.channel)
            stored_id = str(
                fetch_result.channel.get("telegram_channel_id")
                or fetch_result.channel.get("channel_id")
                or ""
            ).strip()
            resolved_id = str(
                fetch_result.channel_info.get("telegram_channel_id")
                or fetch_result.channel_info.get("channel_id")
                or ""
            ).strip()
            previous_message_count = len(state.get("telegram_source_messages", []))
            previous_match_count = len(state.get("telegram_article_matches", []))
            if stored_id and resolved_id and stored_id != resolved_id:
                mark_channel_identity_review_required(
                    fetch_result.channel,
                    fetch_result.channel_info,
                    now,
                )
                fetch_result.raw_messages = []
                fetch_result.error = TelegramUnsafeSource(
                    "channel_identity_review_required"
                )
            else:
                fetch_result.channel = upsert_telegram_channel(
                    state,
                    {**fetch_result.channel, **fetch_result.channel_info},
                )
            identity_changed = identity_changed or (
                previous_key != channel_key(fetch_result.channel)
                or previous_message_count
                != len(state.get("telegram_source_messages", []))
                or previous_match_count
                != len(state.get("telegram_article_matches", []))
            )
        if identity_changed:
            # Canonical migration may rewrite message and match keys. Refresh the
            # ephemeral indexes once per worker batch so subsequent upserts stay
            # O(1) without retaining stale handle-only identities.
            message_index = build_telegram_message_index(state)
            match_index = build_telegram_match_index(state)
        for fetch_result in sorted(fetch_results, key=lambda result: result.index):
            if stop_backfill:
                break
            index = fetch_result.index
            channel = fetch_result.channel
            channel_seen = channel_inserted = channel_updated = channel_failed = 0
            if fetch_result.error is not None:
                if not channel_identity_review_required(channel):
                    channel["last_error"] = error_label(fetch_result.error)
                failed += 1
                channel_failed = 1
                progress_record = {
                    "handle": channel.get("handle") or "",
                    "title": channel.get("title") or "",
                    "status": "failed",
                    "error": channel.get("last_error") or "",
                    "elapsed_seconds": round(
                        time.monotonic() - fetch_result.monotonic_started_at, 2
                    ),
                    "fetch_elapsed_seconds": fetch_result.fetch_elapsed_seconds,
                    "index": index,
                    "total": len(channels),
                }
                per_channel.append(progress_record)
                emit_checkpoint(progress_record)
                if progress:
                    print(
                        f"[{index}/{len(channels)}] @{channel.get('handle') or ''} failed={channel.get('last_error')} "
                        f"elapsed={round(time.monotonic() - fetch_result.monotonic_started_at, 1)}s",
                        flush=True,
                    )
                continue

            raw_messages = fetch_result.raw_messages
            if fetch_result.fetch_truncated or (
                max_messages > 0 and seen + len(raw_messages) > max_messages
            ):
                global_limit_reached = True
                stop_backfill = True
                truncated_channels = len(channels) - (index - 1)
                resume_handle = normalize_channel_handle(
                    channel.get("handle") or channel.get("username")
                )
                resume_after_handle = last_completed_handle
                progress_record = {
                    "handle": resume_handle,
                    "title": channel.get("title") or "",
                    "status": "truncated",
                    "error": "telegram_history_global_limit_reached",
                    "messages_available": len(raw_messages),
                    "messages_capacity_remaining": max(0, max_messages - seen),
                    "next_max_id": fetch_result.next_max_id,
                    "elapsed_seconds": round(
                        time.monotonic() - fetch_result.monotonic_started_at,
                        2,
                    ),
                    "fetch_elapsed_seconds": fetch_result.fetch_elapsed_seconds,
                    "index": index,
                    "total": len(channels),
                }
                per_channel.append(progress_record)
                emit_checkpoint(progress_record)
                if progress:
                    print(
                        f"[{index}/{len(channels)}] @{resume_handle} truncated: "
                        f"remaining_capacity={max(0, max_messages - seen)}",
                        flush=True,
                    )
                break
            if force_remote_resync:
                cursors = state.get("telegram_remote_sync_cursors")
                if not isinstance(cursors, dict):
                    cursors = {}
                    state["telegram_remote_sync_cursors"] = cursors
                # Historical repair must replay every fetched ID even when an
                # older buggy run advanced the DB channel cursor after pruning
                # unsynced rows. The remote upsert is idempotent and keeps the DB
                # cursor monotonic with GREATEST().
                cursors[channel_key(channel)] = 0
            max_message_id = int(channel.get("last_message_id") or 0)
            for raw_message in raw_messages:
                if (
                    time.monotonic() - fetch_result.monotonic_started_at
                    > channel_timeout
                ):
                    channel_failed = 1
                    channel["last_error"] = "processing_timeout"
                    failed += 1
                    break
                message = normalize_telegram_message(channel, raw_message, now)
                posted_at = parse_datetime(message.get("posted_at"), timezone_name)
                if posted_at and posted_at < since:
                    outside_window += 1
                    continue
                if not message.get("telegram_message_id"):
                    continue
                seen += 1
                channel_seen += 1
                status = upsert_telegram_message(
                    state, message, message_index=message_index
                )
                inserted += int(status == "inserted")
                updated += int(status == "updated")
                unchanged += int(status == "unchanged")
                channel_inserted += int(status == "inserted")
                channel_updated += int(status == "updated")
                touched_messages.append(message)
                max_message_id = max(
                    max_message_id, int(message.get("telegram_message_id") or 0)
                )
                for match in match_message_to_articles(
                    state, message, config, match_context
                ):
                    if (
                        upsert_article_match(state, match, match_index=match_index)
                        == "inserted"
                    ):
                        matches_inserted += 1
            channel["last_message_id"] = max(
                max_message_id, int(channel.get("last_message_id") or 0)
            )
            channel["last_collected_at"] = datetime_to_iso(now)
            if not channel_failed:
                channel["last_error"] = None
            progress_record = {
                "handle": channel.get("handle") or "",
                "title": channel.get("title") or "",
                "status": "ok" if not channel_failed else "failed",
                "messages_seen": channel_seen,
                "inserted": channel_inserted,
                "updated": channel_updated,
                "elapsed_seconds": round(
                    time.monotonic() - fetch_result.monotonic_started_at, 2
                ),
                "fetch_elapsed_seconds": fetch_result.fetch_elapsed_seconds,
                "index": index,
                "total": len(channels),
            }
            per_channel.append(progress_record)
            if progress:
                print(
                    f"[{index}/{len(channels)}] @{channel.get('handle') or ''} "
                    f"seen={channel_seen} inserted={channel_inserted} updated={channel_updated} "
                    f"fetch={fetch_result.fetch_elapsed_seconds}s elapsed={round(time.monotonic() - fetch_result.monotonic_started_at, 1)}s",
                    flush=True,
                )
            emit_checkpoint(progress_record)
            if not channel_failed:
                completed_channels += 1
                last_completed_handle = normalize_channel_handle(
                    channel.get("handle") or channel.get("username")
                )
            if max_messages > 0 and seen >= max_messages and index < len(channels):
                global_limit_reached = True
                stop_backfill = True
                truncated_channels = len(channels) - index
                next_channel = channels[index]
                resume_handle = normalize_channel_handle(
                    next_channel.get("handle") or next_channel.get("username")
                )
                resume_after_handle = last_completed_handle
                if progress:
                    print(
                        f"max_messages={max_messages} reached; resume with "
                        f"start_after={resume_after_handle}",
                        flush=True,
                    )
                break

    summary: dict[str, object] = {
        **_current_backfill_selection_metrics(
            state,
            selected_count=len(channels),
        ),
        "telegram_backfill_channels": len(channels),
        "telegram_backfill_days": max(1, days),
        "telegram_backfill_since": datetime_to_iso(since),
        "telegram_backfill_channel_workers": worker_count,
        "telegram_backfill_channels_attempted": len(per_channel),
        "telegram_backfill_channels_completed": completed_channels,
        "telegram_backfill_truncated_channels": truncated_channels,
        "telegram_backfill_global_limit_reached": int(global_limit_reached),
        "telegram_backfill_resume_handle": resume_handle,
        "telegram_backfill_resume_after_handle": resume_after_handle,
        "telegram_force_remote_resync": int(force_remote_resync),
        "telegram_backfill_messages_seen": seen,
        "telegram_messages_inserted": inserted,
        "telegram_messages_updated": updated,
        "telegram_messages_unchanged": unchanged,
        "telegram_matches_inserted": matches_inserted,
        "telegram_channel_failed": failed,
        "telegram_messages_outside_window": outside_window,
        "telegram_backfill_per_channel": per_channel,
    }
    if settings.get("estimate_storage_bytes", True):
        sample = (
            touched_messages[-500:] if len(touched_messages) > 500 else touched_messages
        )
        if sample:
            sample_bytes = len(
                json.dumps(sample, ensure_ascii=False, sort_keys=True).encode("utf-8")
            )
            avg_message_bytes = max(1, round(sample_bytes / len(sample)))
            daily_messages = seen / max(1, days)
            summary["telegram_estimated_avg_message_bytes"] = avg_message_bytes
            summary["telegram_estimated_daily_messages"] = round(daily_messages, 1)
            summary["telegram_estimated_monthly_messages"] = round(daily_messages * 30)
            summary["telegram_estimated_yearly_messages"] = round(daily_messages * 365)
            summary["telegram_estimated_monthly_mb"] = round(
                daily_messages * 30 * avg_message_bytes / 1024 / 1024, 2
            )
            summary["telegram_estimated_yearly_mb"] = round(
                daily_messages * 365 * avg_message_bytes / 1024 / 1024, 2
            )
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
    only_handles: set[str] | None = None,
    skip_handles: set[str] | None = None,
    start_after_handle: str = "",
    before_message_id: int = 0,
    expected_selection_fingerprint: str = "",
    max_messages: int = 0,
    checkpoint_callback: Callable[[dict[str, object]], None] | None = None,
    force_remote_resync: bool = False,
) -> dict[str, object]:
    """Backfill one durable history page at a time.

    A page is normalized and handed to the checkpoint callback before the next
    page is requested. The production callback durably upserts that exact page
    and records ``resume_before_message_id``. This keeps a slow or very large
    channel resumable without retaining an unacknowledged channel-sized batch.
    """

    settings = telegram_sources_config(config)
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    page_timeout = max(5.0, float(settings.get("backfill_channel_timeout_seconds", 60)))
    processing_timeout = max(
        0.01,
        float(settings.get("backfill_processing_timeout_seconds", page_timeout)),
    )
    since = now - timedelta(days=max(1, days))
    selection = _select_backfill_channels(
        state,
        only_handles=only_handles,
        skip_handles=skip_handles,
        start_after_handle=start_after_handle,
        channel_limit=channel_limit,
        before_message_id=before_message_id,
        expected_selection_fingerprint=expected_selection_fingerprint,
    )
    channels = selection.channels

    requested_workers = max(1, int(settings.get("backfill_channel_workers", 1)))
    page_size = max(1, int(limit_per_channel))
    inserted = updated = unchanged = failed = seen = outside_window = 0
    matches_inserted = completed_channels = pages_checkpointed = 0
    global_limit_reached = False
    truncated_channels = 0
    resume_handle = ""
    resume_after_handle = ""
    resume_before_message_id = 0
    first_failed_handle = ""
    first_failed_before_message_id = 0
    last_completed_handle = ""
    per_channel: list[dict[str, object]] = []
    touched_sample: list[dict[str, object]] = []
    match_context = build_article_match_context(state, config)
    message_index = build_telegram_message_index(state)
    match_index = build_telegram_match_index(state)

    def emit_checkpoint(
        progress_record: dict[str, object],
        page_messages: list[dict[str, object]] | None = None,
        page_matches: list[dict[str, object]] | None = None,
        resume_on_failure: int | None = None,
        checkpoint_channel: dict[str, object] | None = None,
    ) -> None:
        nonlocal pages_checkpointed
        pages_checkpointed += int(bool(page_messages))
        if checkpoint_callback:
            checkpoint_callback(
                {
                    **progress_record,
                    **_current_backfill_selection_metrics(
                        state,
                        selected_count=len(channels),
                    ),
                    "_checkpoint_messages": list(page_messages or []),
                    "_checkpoint_matches": list(page_matches or []),
                    "_checkpoint_resume_on_failure": resume_on_failure,
                    "_checkpoint_channel_key": (
                        channel_key(checkpoint_channel)
                        if isinstance(checkpoint_channel, dict)
                        else ""
                    ),
                }
            )

    for channel_index, selected_channel in enumerate(channels, start=1):
        handle = normalize_channel_handle(
            selected_channel.get("handle") or selected_channel.get("username")
        )
        started_handle = handle
        if max_messages > 0 and seen >= max_messages:
            global_limit_reached = True
            truncated_channels = len(channels) - (channel_index - 1)
            resume_handle = handle
            resume_after_handle = last_completed_handle
            break

        started_at = time.monotonic()
        fetch_elapsed_seconds = 0.0
        if progress:
            print(
                f"[{channel_index}/{len(channels)}] @{handle} backfill start",
                flush=True,
            )

        info_fetch_started = time.monotonic()
        try:
            channel_info = await asyncio.wait_for(
                client.get_channel_info(selected_channel), timeout=page_timeout
            )
        except Exception as exc:  # noqa: BLE001
            fetch_elapsed_seconds += time.monotonic() - info_fetch_started
            selected_channel["last_error"] = error_label(exc)
            failed += 1
            if not first_failed_handle:
                first_failed_handle = handle
                first_failed_before_message_id = int(before_message_id or 0)
            failure_record = {
                "handle": handle,
                "title": selected_channel.get("title") or "",
                "status": "failed",
                "error": selected_channel["last_error"],
                "resume_before_message_id": int(before_message_id or 0),
                "fetch_elapsed_seconds": round(fetch_elapsed_seconds, 2),
                "elapsed_seconds": round(time.monotonic() - started_at, 2),
                "index": channel_index,
                "total": len(channels),
            }
            per_channel.append(failure_record)
            emit_checkpoint(failure_record, checkpoint_channel=selected_channel)
            continue
        fetch_elapsed_seconds += time.monotonic() - info_fetch_started

        stored_id = str(
            selected_channel.get("telegram_channel_id")
            or selected_channel.get("channel_id")
            or ""
        ).strip()
        resolved_id = str(
            channel_info.get("telegram_channel_id")
            or channel_info.get("channel_id")
            or ""
        ).strip()
        if stored_id and resolved_id and stored_id != resolved_id:
            mark_channel_identity_review_required(selected_channel, channel_info, now)
            failed += 1
            if not first_failed_handle:
                first_failed_handle = handle
            failure_record = {
                "handle": handle,
                "title": selected_channel.get("title") or "",
                "status": "failed",
                "error": "channel_identity_review_required",
                "resume_before_message_id": 0,
                "fetch_elapsed_seconds": round(fetch_elapsed_seconds, 2),
                "elapsed_seconds": round(time.monotonic() - started_at, 2),
                "index": channel_index,
                "total": len(channels),
            }
            per_channel.append(failure_record)
            emit_checkpoint(failure_record, checkpoint_channel=selected_channel)
            continue

        previous_key = channel_key(selected_channel)
        previous_message_count = len(state.get("telegram_source_messages", []))
        previous_match_count = len(state.get("telegram_article_matches", []))
        channel = upsert_telegram_channel(
            state, {**selected_channel, **channel_info, "last_error": None}
        )
        resolved_handle = normalize_channel_handle(
            channel.get("handle") or channel.get("username")
        )
        # A case-only canonicalization keeps the same deterministic sort marker.
        # A real username rename can move the entity to another point in the
        # ordered universe, so retaining the original marker makes a subsequent
        # resume fail closed (the new fingerprint contains the rename while the
        # old marker is no longer present) instead of skipping channels.
        if resolved_handle and resolved_handle.casefold() == started_handle.casefold():
            handle = resolved_handle
        else:
            handle = started_handle
        if (
            previous_key != channel_key(channel)
            or previous_message_count != len(state.get("telegram_source_messages", []))
            or previous_match_count != len(state.get("telegram_article_matches", []))
        ):
            message_index = build_telegram_message_index(state)
            match_index = build_telegram_match_index(state)

        page_max_id = int(before_message_id or 0) if channel_index == 1 else 0
        channel_seen = channel_inserted = channel_updated = channel_pages = 0
        channel_complete = False
        channel_failed = False
        max_message_id = int(channel.get("last_message_id") or 0)

        while not channel_complete:
            remaining = max_messages - seen if max_messages > 0 else page_size
            if max_messages > 0 and remaining <= 0:
                global_limit_reached = True
                truncated_channels = len(channels) - (channel_index - 1)
                resume_handle = handle
                resume_after_handle = last_completed_handle
                resume_before_message_id = page_max_id
                break
            request_limit = min(page_size, remaining) if max_messages > 0 else page_size

            page_fetch_started = time.monotonic()
            try:
                raw_messages = await asyncio.wait_for(
                    client.iter_messages(
                        channel,
                        min_id=0,
                        max_id=page_max_id,
                        limit=request_limit,
                        since=since,
                    ),
                    timeout=page_timeout,
                )
            except Exception as exc:  # noqa: BLE001
                fetch_elapsed_seconds += time.monotonic() - page_fetch_started
                channel["last_error"] = error_label(exc)
                failed += 1
                channel_failed = True
                if not first_failed_handle:
                    first_failed_handle = handle
                    first_failed_before_message_id = page_max_id
                failure_record = {
                    "handle": handle,
                    "title": channel.get("title") or "",
                    "status": "failed",
                    "error": channel["last_error"],
                    "resume_before_message_id": page_max_id,
                    "pages_checkpointed": channel_pages,
                    "messages_seen": channel_seen,
                    "fetch_elapsed_seconds": round(fetch_elapsed_seconds, 2),
                    "elapsed_seconds": round(time.monotonic() - started_at, 2),
                    "index": channel_index,
                    "total": len(channels),
                }
                per_channel.append(failure_record)
                emit_checkpoint(failure_record, checkpoint_channel=channel)
                break
            fetch_elapsed_seconds += time.monotonic() - page_fetch_started

            raw_messages = sorted(
                raw_messages,
                key=lambda message: int(
                    message.get("id") or message.get("telegram_message_id") or 0
                ),
            )
            if not raw_messages:
                channel_complete = True
                channel["last_collected_at"] = datetime_to_iso(now)
                channel["last_error"] = None
                channel["last_backfill_before_message_id"] = None
                completed_channels += 1
                last_completed_handle = handle
                complete_record = {
                    "handle": handle,
                    "title": channel.get("title") or "",
                    "status": "ok",
                    "messages_seen": channel_seen,
                    "inserted": channel_inserted,
                    "updated": channel_updated,
                    "pages_checkpointed": channel_pages,
                    "resume_before_message_id": 0,
                    "fetch_elapsed_seconds": round(fetch_elapsed_seconds, 2),
                    "elapsed_seconds": round(time.monotonic() - started_at, 2),
                    "index": channel_index,
                    "total": len(channels),
                }
                per_channel.append(complete_record)
                emit_checkpoint(complete_record, checkpoint_channel=channel)
                break

            positive_ids = [
                int(message.get("id") or message.get("telegram_message_id") or 0)
                for message in raw_messages
                if int(message.get("id") or message.get("telegram_message_id") or 0) > 0
            ]
            if not positive_ids:
                channel["last_error"] = "telegram_history_cursor_missing"
                failed += 1
                channel_failed = True
                if not first_failed_handle:
                    first_failed_handle = handle
                    first_failed_before_message_id = page_max_id
                failure_record = {
                    "handle": handle,
                    "status": "failed",
                    "error": channel["last_error"],
                    "resume_before_message_id": page_max_id,
                    "fetch_elapsed_seconds": round(fetch_elapsed_seconds, 2),
                    "index": channel_index,
                    "total": len(channels),
                }
                per_channel.append(failure_record)
                emit_checkpoint(failure_record, checkpoint_channel=channel)
                break
            next_max_id = min(positive_ids)
            if page_max_id and next_max_id >= page_max_id:
                channel["last_error"] = "telegram_history_cursor_stalled"
                failed += 1
                channel_failed = True
                if not first_failed_handle:
                    first_failed_handle = handle
                    first_failed_before_message_id = page_max_id
                failure_record = {
                    "handle": handle,
                    "status": "failed",
                    "error": channel["last_error"],
                    "resume_before_message_id": page_max_id,
                    "fetch_elapsed_seconds": round(fetch_elapsed_seconds, 2),
                    "index": channel_index,
                    "total": len(channels),
                }
                per_channel.append(failure_record)
                emit_checkpoint(failure_record, checkpoint_channel=channel)
                break

            page_messages: list[dict[str, object]] = []
            page_matches: list[dict[str, object]] = []
            processing_timed_out = False
            processing_started = time.monotonic()
            for raw_message in raw_messages:
                if time.monotonic() - processing_started > processing_timeout:
                    processing_timed_out = True
                    break
                message = normalize_telegram_message(channel, raw_message, now)
                posted_at = parse_datetime(message.get("posted_at"), timezone_name)
                if posted_at and posted_at < since:
                    outside_window += 1
                    continue
                if not message.get("telegram_message_id"):
                    continue
                seen += 1
                channel_seen += 1
                status = upsert_telegram_message(
                    state, message, message_index=message_index
                )
                inserted += int(status == "inserted")
                updated += int(status == "updated")
                unchanged += int(status == "unchanged")
                channel_inserted += int(status == "inserted")
                channel_updated += int(status == "updated")
                page_messages.append(message)
                touched_sample.append(message)
                if len(touched_sample) > 500:
                    del touched_sample[:-500]
                max_message_id = max(
                    max_message_id, int(message.get("telegram_message_id") or 0)
                )
                for match in match_message_to_articles(
                    state, message, config, match_context
                ):
                    page_matches.append(match)
                    if (
                        upsert_article_match(state, match, match_index=match_index)
                        == "inserted"
                    ):
                        matches_inserted += 1

            channel["last_message_id"] = max(
                max_message_id, int(channel.get("last_message_id") or 0)
            )
            channel["last_backfill_before_message_id"] = next_max_id
            channel["last_backfill_checkpoint_at"] = datetime_to_iso(now)
            channel_pages += 1
            page_is_last = (
                len(raw_messages) < request_limit and not processing_timed_out
            )
            cap_reached = max_messages > 0 and seen >= max_messages and not page_is_last
            if page_is_last:
                # Persist completion metadata atomically with the last non-empty
                # message page. Repair mode intentionally has no later global
                # metadata write, so setting these fields after the ACK would
                # leave MySQL with a permanent in-progress cursor.
                channel_complete = True
                channel["last_collected_at"] = datetime_to_iso(now)
                channel["last_error"] = None
                channel["last_backfill_before_message_id"] = None
                completed_channels += 1
                last_completed_handle = handle
            page_status = (
                "ok" if page_is_last else "truncated" if cap_reached else "page"
            )
            page_record = {
                "handle": handle,
                "title": channel.get("title") or "",
                "status": page_status,
                "messages_seen": channel_seen,
                "page_messages": len(page_messages),
                "inserted": channel_inserted,
                "updated": channel_updated,
                "pages_checkpointed": channel_pages,
                "resume_before_message_id": 0 if page_is_last else next_max_id,
                "fetch_elapsed_seconds": round(fetch_elapsed_seconds, 2),
                "elapsed_seconds": round(time.monotonic() - started_at, 2),
                "index": channel_index,
                "total": len(channels),
            }
            emit_checkpoint(
                page_record,
                page_messages,
                page_matches,
                resume_on_failure=page_max_id,
                checkpoint_channel=channel,
            )

            if processing_timed_out:
                channel["last_error"] = "processing_timeout"
                failed += 1
                channel_failed = True
                if not first_failed_handle:
                    first_failed_handle = handle
                    first_failed_before_message_id = page_max_id
                failure_record = {
                    **page_record,
                    "status": "failed",
                    "error": channel["last_error"],
                    # Re-fetch this page so the unprocessed suffix cannot be skipped.
                    "resume_before_message_id": page_max_id,
                }
                per_channel.append(failure_record)
                emit_checkpoint(failure_record, checkpoint_channel=channel)
                break
            if page_is_last:
                per_channel.append(page_record)
                break
            if cap_reached:
                global_limit_reached = True
                truncated_channels = len(channels) - (channel_index - 1)
                resume_handle = handle
                resume_after_handle = last_completed_handle
                resume_before_message_id = next_max_id
                per_channel.append(page_record)
                break
            page_max_id = next_max_id

        if progress:
            print(
                f"[{channel_index}/{len(channels)}] @{handle} "
                f"seen={channel_seen} inserted={channel_inserted} updated={channel_updated} "
                f"pages={channel_pages} elapsed={round(time.monotonic() - started_at, 1)}s",
                flush=True,
            )
        if global_limit_reached:
            break
        if channel_failed:
            continue

    summary: dict[str, object] = {
        **_current_backfill_selection_metrics(
            state,
            selected_count=len(channels),
        ),
        "telegram_backfill_channels": len(channels),
        "telegram_backfill_days": max(1, days),
        "telegram_backfill_since": datetime_to_iso(since),
        "telegram_backfill_channel_workers": requested_workers,
        "telegram_backfill_effective_workers": 1,
        "telegram_backfill_pages_checkpointed": pages_checkpointed,
        "telegram_backfill_channels_attempted": len(per_channel),
        "telegram_backfill_channels_completed": completed_channels,
        "telegram_backfill_truncated_channels": truncated_channels,
        "telegram_backfill_global_limit_reached": int(global_limit_reached),
        "telegram_backfill_resume_handle": resume_handle,
        "telegram_backfill_resume_after_handle": resume_after_handle,
        "telegram_backfill_resume_before_message_id": resume_before_message_id,
        "telegram_backfill_first_failed_handle": first_failed_handle,
        "telegram_backfill_first_failed_before_message_id": first_failed_before_message_id,
        "telegram_force_remote_resync": int(force_remote_resync),
        "telegram_backfill_messages_seen": seen,
        "telegram_messages_inserted": inserted,
        "telegram_messages_updated": updated,
        "telegram_messages_unchanged": unchanged,
        "telegram_matches_inserted": matches_inserted,
        "telegram_channel_failed": failed,
        "telegram_messages_outside_window": outside_window,
        "telegram_backfill_per_channel": per_channel,
    }
    if settings.get("estimate_storage_bytes", True) and touched_sample:
        sample_bytes = len(
            json.dumps(touched_sample, ensure_ascii=False, sort_keys=True).encode(
                "utf-8"
            )
        )
        avg_message_bytes = max(1, round(sample_bytes / len(touched_sample)))
        daily_messages = seen / max(1, days)
        summary["telegram_estimated_avg_message_bytes"] = avg_message_bytes
        summary["telegram_estimated_daily_messages"] = round(daily_messages, 1)
        summary["telegram_estimated_monthly_messages"] = round(daily_messages * 30)
        summary["telegram_estimated_yearly_messages"] = round(daily_messages * 365)
        summary["telegram_estimated_monthly_mb"] = round(
            daily_messages * 30 * avg_message_bytes / 1024 / 1024, 2
        )
        summary["telegram_estimated_yearly_mb"] = round(
            daily_messages * 365 * avg_message_bytes / 1024 / 1024, 2
        )
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
    only_handles: set[str] | None = None,
    skip_handles: set[str] | None = None,
    start_after_handle: str = "",
    before_message_id: int = 0,
    expected_selection_fingerprint: str = "",
    max_messages: int = 0,
    checkpoint_callback: Callable[[dict[str, object]], None] | None = None,
    selection_checkpoint_callback: Callable[[dict[str, object]], None] | None = None,
    force_remote_resync: bool = False,
    rebuild_remote_signals: bool = False,
    finalize_signal_rebuild: bool = False,
    require_unique_universe_handles: bool = False,
) -> dict[str, object]:
    ensure_telegram_state(state)
    if rebuild_remote_signals and not sync_remote:
        raise RuntimeError("telegram signal rebuild requires remote sync")
    if finalize_signal_rebuild and not rebuild_remote_signals:
        raise ValueError("finalize_signal_rebuild_requires_rebuild")
    if finalize_signal_rebuild and (
        only_handles
        or skip_handles
        or channel_limit > 0
        or before_message_id > 0
        or not normalize_channel_handle(start_after_handle)
    ):
        raise ValueError("finalize_signal_rebuild_requires_global_scope")
    registered = register_configured_channels(state, config)
    rights_blocked = apply_channel_source_rights(state, config, now)
    # Resolve and guard the complete channel universe before a Telethon adapter
    # is opened. This makes unknown/ambiguous explicit handles and stale resume
    # fingerprints fail before any Telegram API call.
    preflight_selection = _select_backfill_channels(
        state,
        only_handles=only_handles,
        skip_handles=skip_handles,
        start_after_handle=start_after_handle,
        channel_limit=channel_limit,
        before_message_id=before_message_id,
        expected_selection_fingerprint=expected_selection_fingerprint,
    )
    duplicate_universe_handles = _duplicate_backfill_universe_handles(state)
    if duplicate_universe_handles and (
        require_unique_universe_handles or finalize_signal_rebuild
    ):
        raise ValueError(
            "telegram_backfill_universe_handle_not_unique:"
            + duplicate_universe_handles[0]
        )
    if finalize_signal_rebuild and preflight_selection.universe_count == 0:
        raise ValueError("finalize_signal_rebuild_empty_universe")
    if finalize_signal_rebuild and preflight_selection.channels:
        raise ValueError("finalize_signal_rebuild_requires_last_handle")
    partial_backfill_scope = bool(
        only_handles
        or skip_handles
        or normalize_channel_handle(start_after_handle)
        or channel_limit > 0
        or before_message_id > 0
    )
    repair_signal_mode = bool(
        rebuild_remote_signals
        or force_remote_resync
        or finalize_signal_rebuild
        or partial_backfill_scope
    )
    started_selection_metrics: dict[str, object] = {
        "telegram_backfill_started_selection_fingerprint": (
            preflight_selection.fingerprint
        ),
        "telegram_backfill_started_universe_count": (
            preflight_selection.universe_count
        ),
        "telegram_backfill_started_selected_count": len(
            preflight_selection.channels
        ),
    }

    def current_selection_metrics() -> dict[str, object]:
        return {
            **_current_backfill_selection_metrics(
                state,
                selected_count=len(preflight_selection.channels),
            ),
            **started_selection_metrics,
        }

    if selection_checkpoint_callback:
        selection_checkpoint_callback(
            {
                **current_selection_metrics(),
                "handle": "",
                "status": "selection_validated",
                "remote_checkpoint_complete": 0,
                "resume_before_message_id": int(before_message_id or 0),
                "telegram_remote_failed": 0,
                "telegram_remote_pending": len(pending_remote_messages(state)),
            }
        )
    remote_totals: dict[str, object] = {
        "telegram_remote_synced": 0,
        "telegram_remote_failed": 0,
        "telegram_remote_channels": 0,
        "telegram_remote_messages": 0,
        "telegram_remote_matches": 0,
        "telegram_remote_signals": 0,
        "telegram_remote_signals_deleted": 0,
        "telegram_remote_metadata_synced": 0,
        "telegram_remote_metadata_failed": 0,
        "telegram_remote_pending": 0,
    }

    def merge_remote_summary(result: dict[str, object]) -> None:
        for key in (
            "telegram_remote_synced",
            "telegram_remote_failed",
            "telegram_remote_channels",
            "telegram_remote_messages",
            "telegram_remote_matches",
            "telegram_remote_signals",
            "telegram_remote_signals_deleted",
            "telegram_remote_metadata_synced",
            "telegram_remote_metadata_failed",
        ):
            remote_totals[key] = int(remote_totals.get(key) or 0) + int(
                result.get(key) or 0
            )
        if "telegram_remote_pending" in result:
            remote_totals["telegram_remote_pending"] = int(
                result.get("telegram_remote_pending") or 0
            )
        if result.get("telegram_remote_skipped"):
            remote_totals["telegram_remote_skipped"] = 1
        if result.get("telegram_remote_last_error"):
            remote_totals["telegram_remote_last_error"] = result[
                "telegram_remote_last_error"
            ]
        if result.get("telegram_remote_last_status_code"):
            remote_totals["telegram_remote_last_status_code"] = int(
                result["telegram_remote_last_status_code"]  # type: ignore[arg-type]
            )
        if result.get("telegram_remote_max_request_bytes"):
            remote_totals["telegram_remote_max_request_bytes"] = max(
                int(remote_totals.get("telegram_remote_max_request_bytes") or 0),
                int(result["telegram_remote_max_request_bytes"]),  # type: ignore[arg-type]
            )

    def durable_checkpoint(progress_record: dict[str, object]) -> None:
        public_record = dict(progress_record)
        page_messages = public_record.pop("_checkpoint_messages", [])
        page_matches = public_record.pop("_checkpoint_matches", [])
        resume_on_failure = public_record.pop("_checkpoint_resume_on_failure", None)
        checkpoint_channel_identity = str(
            public_record.pop("_checkpoint_channel_key", "") or ""
        )
        # Channel discovery can safely promote a handle-only row to an
        # authoritative Telegram ID. Checkpoint the resulting universe rather
        # than restoring the stale preflight fingerprint supplied by the inner
        # page loop. The preflight value remains available under the started_*
        # keys for auditability.
        public_record.update(current_selection_metrics())
        current_remote_failed = 0
        if sync_remote:
            if isinstance(page_messages, list) and page_messages:
                checkpoint_messages = [
                    message for message in page_messages if isinstance(message, dict)
                ]
                if not force_remote_resync:
                    cursors = state.get("telegram_remote_sync_cursors")
                    if not isinstance(cursors, dict):
                        cursors = {}
                    checkpoint_messages = [
                        message
                        for message in checkpoint_messages
                        if int(message.get("telegram_message_id") or 0)
                        > int(cursors.get(channel_key(message), 0) or 0)
                    ]
            else:
                checkpoint_messages = []
            if checkpoint_messages:
                remote_result = sync_telegram_batch_to_remote_api(
                    state,
                    config,
                    messages=checkpoint_messages,
                    matches=[match for match in page_matches if isinstance(match, dict)]
                    if isinstance(page_matches, list)
                    else [],
                    advance_cursors=True,
                )
            else:
                metadata_state = state
                if checkpoint_channel_identity:
                    checkpoint_channels = [
                        channel
                        for channel in state.get("telegram_source_channels", [])
                        if isinstance(channel, dict)
                        and channel_key(channel) == checkpoint_channel_identity
                    ]
                    if len(checkpoint_channels) != 1:
                        remote_result = {
                            "telegram_remote_metadata_synced": 0,
                            "telegram_remote_metadata_failed": 1,
                            "telegram_remote_failed": 1,
                            "telegram_remote_last_error": (
                                "remote_checkpoint_channel_missing_or_ambiguous"
                            ),
                        }
                    else:
                        remote_cursors = state.get("telegram_remote_sync_cursors", {})
                        if not isinstance(remote_cursors, dict):
                            remote_cursors = {}
                        metadata_state = {
                            "telegram_source_channels": checkpoint_channels,
                            "telegram_remote_sync_cursors": {
                                checkpoint_channel_identity: int(
                                    remote_cursors.get(
                                        checkpoint_channel_identity, 0
                                    )
                                    or 0
                                )
                            },
                            "telegram_issue_signals": [],
                        }
                        remote_result = sync_telegram_metadata_to_remote_api(
                            metadata_state,
                            config,
                            include_issue_signals=False,
                        )
                elif repair_signal_mode:
                    remote_result = {
                        "telegram_remote_metadata_synced": 0,
                        "telegram_remote_metadata_failed": 0,
                        "telegram_remote_metadata_skipped": 1,
                        "telegram_remote_failed": 0,
                    }
                else:
                    remote_result = sync_telegram_metadata_to_remote_api(
                        metadata_state,
                        config,
                        include_issue_signals=False,
                    )
            current_remote_failed = int(
                remote_result.get("telegram_remote_failed") or 0
            )
            merge_remote_summary(remote_result)
            remote_totals["telegram_remote_pending"] = (
                max(
                    0,
                    len(checkpoint_messages)
                    - int(remote_result.get("telegram_remote_messages") or 0),
                )
                if current_remote_failed
                else 0
            )
        checkpoint_complete = not sync_remote or (
            remote_api_configured() and current_remote_failed == 0
        )
        if not checkpoint_complete and resume_on_failure is not None:
            public_record["resume_before_message_id"] = int(resume_on_failure or 0)
        public_record["telegram_remote_failed"] = current_remote_failed
        public_record["telegram_remote_pending"] = int(
            remote_totals.get("telegram_remote_pending") or 0
        )
        if remote_totals.get("telegram_remote_last_error"):
            public_record["telegram_remote_last_error"] = remote_totals[
                "telegram_remote_last_error"
            ]
        if remote_totals.get("telegram_remote_last_status_code"):
            public_record["telegram_remote_last_status_code"] = remote_totals[
                "telegram_remote_last_status_code"
            ]
        if remote_totals.get("telegram_remote_max_request_bytes"):
            public_record["telegram_remote_max_request_bytes"] = remote_totals[
                "telegram_remote_max_request_bytes"
            ]
        if checkpoint_callback:
            checkpoint_callback(
                {
                    **public_record,
                    "remote_checkpoint_complete": int(checkpoint_complete),
                }
            )
        if not checkpoint_complete:
            # Stop before another channel is fetched. Successful chunks already
            # advanced a durable cursor and the caller checkpointed the remaining
            # local rows. Forced historical repair can safely replay this bounded
            # channel idempotently on the next run.
            raise RuntimeError("telegram_remote_checkpoint_failed")

    if finalize_signal_rebuild:
        # The explicit finalizer proves that start_after is the current last
        # unique handle and therefore selects zero channels. Run the normal
        # summary path without opening Telethon; finalization reads the complete
        # 72-hour window from MySQL below and needs no Telegram credentials.
        finalize_client: Any = client if client is not None else object()

        async def run_finalize_only() -> dict[str, object]:
            return await _backfill_messages_with_client(
                state,
                config,
                now,
                finalize_client,
                days=days,
                limit_per_channel=limit_per_channel,
                channel_limit=channel_limit,
                progress=progress,
                only_handles=only_handles,
                skip_handles=skip_handles,
                start_after_handle=start_after_handle,
                before_message_id=before_message_id,
                expected_selection_fingerprint=expected_selection_fingerprint,
                max_messages=max_messages,
                checkpoint_callback=durable_checkpoint,
                force_remote_resync=force_remote_resync,
            )

        summary = asyncio.run(run_finalize_only())
    elif client is None:
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
                    only_handles=only_handles,
                    skip_handles=skip_handles,
                    start_after_handle=start_after_handle,
                    before_message_id=before_message_id,
                    expected_selection_fingerprint=expected_selection_fingerprint,
                    max_messages=max_messages,
                    checkpoint_callback=durable_checkpoint,
                    force_remote_resync=force_remote_resync,
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
                only_handles=only_handles,
                skip_handles=skip_handles,
                start_after_handle=start_after_handle,
                before_message_id=before_message_id,
                expected_selection_fingerprint=expected_selection_fingerprint,
                max_messages=max_messages,
                checkpoint_callback=durable_checkpoint,
                force_remote_resync=force_remote_resync,
            )

        summary = asyncio.run(run_with_client())

    # Covers a zero-channel ordinary backfill or an unusual leftover hydrated
    # row. The explicit signal finalizer is intentionally signal-only: it must
    # not mutate channel metadata and must fail before staging when any message
    # still lacks a durable remote cursor.
    if finalize_signal_rebuild and pending_remote_messages(state):
        raise RuntimeError("finalize_signal_rebuild_pending_messages")
    if not finalize_signal_rebuild and (
        int(summary.get("telegram_backfill_channels") or 0) == 0
        or (sync_remote and pending_remote_messages(state))
    ):
        durable_checkpoint({"handle": "", "status": "final"})
    summary["telegram_source_channels_registered"] = registered
    summary["telegram_source_rights_blocked"] = rights_blocked
    state["telegram_source_messages"] = sorted(
        [
            message
            for message in state.get("telegram_source_messages", [])
            if isinstance(message, dict)
        ],
        key=lambda message: (
            channel_key(message),
            int(message.get("telegram_message_id") or 0),
        ),
    )
    summary.update(current_selection_metrics())
    full_unfiltered_scope = (
        not only_handles
        and not skip_handles
        and not normalize_channel_handle(start_after_handle)
        and channel_limit == 0
        and before_message_id == 0
    )
    explicit_finalize_scope = (
        finalize_signal_rebuild
        and not preflight_selection.channels
        and preflight_selection.universe_count > 0
    )
    remote_pending = len(pending_remote_messages(state))
    remote_totals["telegram_remote_pending"] = max(
        int(remote_totals.get("telegram_remote_pending") or 0),
        remote_pending,
    )
    durable_complete = (
        int(summary.get("telegram_channel_failed") or 0) == 0
        and int(summary.get("telegram_backfill_truncated_channels") or 0) == 0
        and int(summary.get("telegram_backfill_global_limit_reached") or 0) == 0
        and int(summary.get("telegram_backfill_channels_completed") or 0)
        == int(summary.get("telegram_backfill_channels") or 0)
        and int(remote_totals.get("telegram_remote_failed") or 0) == 0
        and int(remote_totals.get("telegram_remote_pending") or 0) == 0
        and remote_pending == 0
    )
    automatic_full_rebuild = (
        full_unfiltered_scope
        and preflight_selection.universe_count > 0
        and durable_complete
    )
    signal_rebuild_authorized = bool(
        rebuild_remote_signals
        and remote_api_configured()
        and durable_complete
        and (automatic_full_rebuild or explicit_finalize_scope)
    )
    if finalize_signal_rebuild and not signal_rebuild_authorized:
        raise RuntimeError("finalize_signal_rebuild_not_authorized")
    summary.update(
        {
            "telegram_signal_rebuild_requested": int(rebuild_remote_signals),
            "telegram_signal_rebuild_authorized": int(signal_rebuild_authorized),
            "telegram_signal_rebuild_skipped_partial": int(
                rebuild_remote_signals and not signal_rebuild_authorized
            ),
            "telegram_signal_rebuild_finalize_mode": int(
                finalize_signal_rebuild
            ),
            "telegram_signal_rebuild_full_scope": int(full_unfiltered_scope),
            "telegram_signal_rebuild_durable_complete": int(durable_complete),
        }
    )

    signal_window_summary: dict[str, object] = {}
    if signal_rebuild_authorized:
        # Lazy import avoids the remote_state -> telegram_sources module cycle.
        from .remote_state import hydrate_telegram_signal_window

        signal_window_summary = hydrate_telegram_signal_window(
            state, config, now
        )
    # Repair segments must not derive or upsert signals from an incomplete local
    # slice. A complete unfiltered run, or the explicit zero-channel finalizer,
    # first hydrates the full acknowledged 72-hour MySQL window. Non-repair
    # callers retain the ordinary live signal refresh behavior.
    if signal_rebuild_authorized or not repair_signal_mode:
        state["telegram_issue_signals"] = telegram_issue_signals(
            state, config, now=now
        )
    refresh_channel_runtime_quality(state)
    metadata_summary: dict[str, object] = {}
    should_sync_final_metadata = signal_rebuild_authorized or not repair_signal_mode
    if sync_remote and should_sync_final_metadata:
        replace_since = None
        if signal_rebuild_authorized:
            window_hours = int(
                signal_window_summary.get("telegram_signal_window_hours") or 72
            )
            replace_since = now - timedelta(hours=max(1, window_hours))
        metadata_state = state
        if signal_rebuild_authorized:
            # Every channel page was already ACKed by its durable message or
            # channel-only checkpoint. Keep authoritative replacement strictly
            # signal-only so a post-commit channel write cannot turn a completed
            # signal generation into an ambiguous failure.
            metadata_state = {
                "telegram_source_channels": [],
                "telegram_remote_sync_cursors": {},
                "telegram_issue_signals": state.get("telegram_issue_signals", []),
            }
        metadata_summary = sync_telegram_metadata_to_remote_api(
            metadata_state,
            config,
            include_issue_signals=(
                signal_rebuild_authorized or not repair_signal_mode
            ),
            replace_issue_signals=signal_rebuild_authorized,
            replace_issue_signals_since=replace_since,
            signal_rebuild_base_revision=(
                int(signal_window_summary["telegram_signal_live_revision"])
                if signal_rebuild_authorized
                else None
            ),
        )
        merge_remote_summary(metadata_summary)
    elif sync_remote:
        metadata_summary = {
            "telegram_remote_metadata_skipped": 1,
        }
    summary.update(remote_totals)
    summary.update(signal_window_summary)
    summary.update(
        {
            key: value
            for key, value in metadata_summary.items()
            if key not in remote_totals
        }
    )
    summary.update(prune_telegram_state(state, config, now))
    summary["telegram_prune_deferred"] = 0
    summary.update(current_selection_metrics())
    state.setdefault("telegram_source_runs", [])
    state["telegram_source_runs"].append(telegram_run_record(now, "backfill", summary))  # type: ignore[index, union-attr]
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


def telegram_state_stats(
    state: dict[str, object], config: dict[str, object]
) -> dict[str, object]:
    ensure_telegram_state(state)
    register_configured_channels(state, config)
    channels = enabled_channels(state)
    messages = [
        message
        for message in state.get("telegram_source_messages", [])
        if isinstance(message, dict)
    ]
    matches = [
        match
        for match in state.get("telegram_article_matches", [])
        if isinstance(match, dict)
    ]
    message_counts: Counter[str] = Counter(
        normalize_channel_handle(message.get("handle") or message.get("username"))
        for message in messages
        if normalize_channel_handle(message.get("handle") or message.get("username"))
    )
    match_types: Counter[str] = Counter(
        str(match.get("match_type") or "unknown") for match in matches
    )
    channel_rows: list[dict[str, object]] = []
    first_uncollected = ""
    last_processed = ""
    uncollected_handles: list[str] = []
    for index, channel in enumerate(channels, start=1):
        handle = normalize_channel_handle(
            channel.get("handle") or channel.get("username")
        )
        count = int(message_counts.get(handle, 0))
        collected_at = str(channel.get("last_collected_at") or "")
        processed = bool(collected_at or count > 0)
        if processed:
            last_processed = handle
        else:
            uncollected_handles.append(handle)
            if not first_uncollected:
                first_uncollected = handle
        channel_rows.append(
            {
                "index": index,
                "handle": handle,
                "title": channel.get("title") or "",
                "messages": count,
                "last_message_id": int(channel.get("last_message_id") or 0),
                "last_collected_at": channel.get("last_collected_at") or "",
                "last_error": channel.get("last_error") or "",
            }
        )

    retry_handles = ",".join(uncollected_handles[:10])
    return {
        "telegram_channels_enabled": len(channels),
        "telegram_messages": len(messages),
        "telegram_article_matches": len(matches),
        "telegram_match_types": dict(match_types),
        "telegram_source_runs": len(
            [
                run
                for run in state.get("telegram_source_runs", [])
                if isinstance(run, dict)
            ]
        ),
        "first_uncollected_handle": first_uncollected,
        "uncollected_handles": uncollected_handles,
        "last_processed_handle": last_processed,
        "resume_after_handle": last_processed,
        "next_backfill_command": (
            ".\\.venv\\Scripts\\python.exe -m curator.telegram_sources backfill-messages "
            f"--days 180 --limit-per-channel 1000 --only-handles {retry_handles} "
            "--timeout-per-channel 90 --workers 3"
            if retry_handles
            else ""
        ),
        "channels": channel_rows,
    }


def rematch_telegram_articles(
    state: dict[str, object],
    config: dict[str, object],
    *,
    limit: int = 0,
    progress: bool = False,
) -> dict[str, object]:
    ensure_telegram_state(state)
    messages = [
        message
        for message in state.get("telegram_source_messages", [])
        if isinstance(message, dict)
    ]
    if limit:
        messages = messages[-limit:]
    selected_keys = {message_key(message) for message in messages}
    old_matches = [
        match
        for match in state.get("telegram_article_matches", [])
        if isinstance(match, dict)
    ]
    old_selected_count = sum(
        1
        for match in old_matches
        if str(match.get("telegram_message_key") or "") in selected_keys
    )
    kept_matches = [
        match
        for match in old_matches
        if limit and str(match.get("telegram_message_key") or "") not in selected_keys
    ]
    kept_count = len(kept_matches)
    state["telegram_article_matches"] = kept_matches

    context = build_article_match_context(state, config)
    inserted = 0
    match_types: Counter[str] = Counter()
    started_at = time.monotonic()
    for index, message in enumerate(messages, start=1):
        for match in match_message_to_articles(state, message, config, context):
            if upsert_article_match(state, match) == "inserted":
                inserted += 1
                match_types.update([str(match.get("match_type") or "unknown")])
        if progress and (index % 1000 == 0 or index == len(messages)):
            elapsed = max(0.001, time.monotonic() - started_at)
            rate = index / elapsed
            remaining = max(0, len(messages) - index)
            eta = round(remaining / rate, 1) if rate else 0
            print(
                f"rematch {index}/{len(messages)} messages, new_matches={inserted}, eta={eta}s",
                flush=True,
            )

    prune_summary = prune_telegram_state(
        state,
        config,
        datetime.now(ZoneInfo(str(config.get("timezone") or "Asia/Seoul"))),
    )
    state["telegram_issue_signals"] = telegram_issue_signals(state, config)
    refresh_channel_runtime_quality(state)
    return {
        "telegram_rematch_messages": len(messages),
        "telegram_rematch_old_matches": old_selected_count,
        "telegram_rematch_new_matches": inserted,
        "telegram_rematch_kept_matches": kept_count,
        "telegram_rematch_match_types": dict(match_types),
        "telegram_rematch_issue_signals": len(state.get("telegram_issue_signals", [])),
        **prune_summary,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage Telegram public-channel sources for the RSS curator."
    )
    parser.add_argument(
        "--root",
        default=".",
        help="Project root containing config.yaml and data/state.json",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="List configured Telegram source channels")
    subparsers.add_parser(
        "stats", help="Show local Telegram collection statistics and next backfill hint"
    )
    add_parser = subparsers.add_parser(
        "add", help="Add or update a manual public channel source"
    )
    add_parser.add_argument("handle")
    add_parser.add_argument("--title", default="")
    add_parser.add_argument("--disabled", action="store_true")

    disable_parser = subparsers.add_parser("disable", help="Disable a source channel")
    disable_parser.add_argument("handle")

    enable_parser = subparsers.add_parser("enable", help="Enable a source channel")
    enable_parser.add_argument("handle")

    subparsers.add_parser("candidates", help="List discovered candidate channels")
    subparsers.add_parser("collect", help="Run one Telegram source collection pass")
    sync_parser = subparsers.add_parser(
        "sync-remote",
        help="Sync locally stored Telegram messages to the remote API in batches",
    )
    sync_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit most recent messages to sync, 0 means all local messages",
    )

    rematch_parser = subparsers.add_parser(
        "rematch",
        help="Rebuild Telegram article matches with the current matching policy",
    )
    rematch_parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Rematch only the most recent N messages, 0 means all",
    )
    rematch_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the rematch summary without writing state.json",
    )
    rematch_parser.add_argument(
        "--progress", action="store_true", help="Print periodic rematch progress"
    )

    discover_parser = subparsers.add_parser(
        "discover",
        help="Discover similar public-channel candidates from enabled seed channels",
    )
    discover_parser.add_argument(
        "--limit", type=int, default=20, help="Maximum recommendations per seed channel"
    )
    discover_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover and print a summary without writing state.json",
    )

    expand_parser = subparsers.add_parser(
        "expand-similar",
        help="Join and enable high-quality similar public channels until a target count is reached",
    )
    expand_parser.add_argument(
        "--target-multiplier",
        type=float,
        default=3.0,
        help="Target enabled-channel multiplier, default triples the current count",
    )
    expand_parser.add_argument(
        "--target-count",
        type=int,
        default=0,
        help="Absolute target enabled-channel count, 0 uses multiplier",
    )
    expand_parser.add_argument(
        "--recommendation-limit",
        type=int,
        default=30,
        help="Maximum recommendations per seed channel",
    )
    expand_parser.add_argument(
        "--seed-limit",
        type=int,
        default=0,
        help="Limit high-quality seed channels, 0 means all",
    )
    expand_parser.add_argument(
        "--seed-min-quality",
        type=int,
        default=60,
        help="Use only seed channels at or above this quality score",
    )
    expand_parser.add_argument(
        "--min-quality",
        type=int,
        default=70,
        help="Join only candidate channels at or above this quality score",
    )
    expand_parser.add_argument(
        "--max-join",
        type=int,
        default=0,
        help="Maximum channels to join in this run, 0 means up to target",
    )
    expand_parser.add_argument(
        "--delay-min-seconds",
        type=float,
        default=3.0,
        help="Minimum random delay between joins",
    )
    expand_parser.add_argument(
        "--delay-max-seconds",
        type=float,
        default=9.0,
        help="Maximum random delay between joins",
    )
    expand_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Discover join targets without joining or writing state.json",
    )

    backfill_parser = subparsers.add_parser(
        "backfill-messages",
        help="Backfill public-channel messages for a historical window",
    )
    backfill_parser.add_argument(
        "--days", type=int, default=14, help="How many days back to collect"
    )
    backfill_parser.add_argument(
        "--limit-per-channel",
        type=int,
        default=1000,
        help="Telegram history page size per channel; pages continue to the requested date boundary",
    )
    backfill_parser.add_argument(
        "--channel-limit",
        type=int,
        default=0,
        help="Limit number of enabled channels, 0 means all",
    )
    backfill_parser.add_argument(
        "--only-handles",
        default="",
        help="Comma/space separated channel handles to include, empty means all",
    )
    backfill_parser.add_argument(
        "--skip-handles",
        default="",
        help="Comma/space separated channel handles to skip",
    )
    backfill_parser.add_argument(
        "--start-after",
        default="",
        help="Resume after this channel handle in the enabled-channel order",
    )
    backfill_parser.add_argument(
        "--expected-selection-fingerprint",
        default="",
        help="Optional channel-universe assertion; required with --start-after",
    )
    backfill_parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after this many messages across all channels, 0 means unlimited",
    )
    backfill_parser.add_argument(
        "--timeout-per-channel",
        type=float,
        default=0,
        help="Override per-channel backfill timeout seconds",
    )
    backfill_parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Fetch this many channels concurrently during backfill, 0 uses config",
    )
    backfill_parser.add_argument(
        "--no-checkpoint",
        action="store_true",
        help="Do not save state after each channel",
    )
    backfill_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run against a state copy without writing state.json or remote DB",
    )
    backfill_parser.add_argument(
        "--no-remote",
        action="store_true",
        help="Do not sync collected messages to the remote DB API",
    )

    import_parser = subparsers.add_parser(
        "import-joined",
        help="Import public channels already joined by the Telegram reader account",
    )
    import_parser.add_argument(
        "--limit", type=int, default=500, help="Maximum dialogs to scan"
    )
    import_parser.add_argument(
        "--min-quality",
        type=int,
        default=0,
        help="Skip channels below this quality score",
    )
    import_parser.add_argument(
        "--max-import",
        type=int,
        default=0,
        help="Import at most this many new channels, 0 means unlimited",
    )
    import_parser.add_argument(
        "--enable",
        action="store_true",
        help="Enable imported channels for collection immediately",
    )
    import_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan and print a summary without writing state.json",
    )

    session_parser = subparsers.add_parser(
        "make-session",
        help="Interactively create a TELEGRAM_SESSION_STRING for GitHub Actions",
    )
    session_parser.add_argument(
        "--out",
        default="",
        help="Optional local env file to write TELEGRAM_SESSION_STRING into, e.g. .env.telegram",
    )
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
        print(
            json.dumps(
                cli_channel_table(list(state.get("telegram_source_channels", []))),
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if args.command == "stats":
        print(
            json.dumps(
                telegram_state_stats(state, config), ensure_ascii=False, indent=2
            )
        )
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
            if (
                isinstance(channel, dict)
                and normalize_channel_handle(
                    channel.get("handle") or channel.get("username")
                )
                == target
            ):
                channel["enabled"] = args.command == "enable"
                changed = True
        save_state(state_path, state)
        print(
            json.dumps(
                {"ok": changed, "handle": target, "enabled": args.command == "enable"},
                ensure_ascii=False,
            )
        )
        return 0 if changed else 1
    if args.command == "candidates":
        print(
            json.dumps(
                list(state.get("telegram_channel_candidates", [])),
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0
    if args.command == "collect":
        from .dates import now_in_timezone

        now = now_in_timezone(str(config.get("timezone") or "Asia/Seoul"))
        summary = collect_telegram_sources(state, config, now)
        save_state(state_path, state)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    if args.command == "sync-remote":
        limit = max(0, int(args.limit))
        messages = [
            message
            for message in state.get("telegram_source_messages", [])
            if isinstance(message, dict)
        ]
        if limit:
            messages = messages[-limit:]
        message_keys = {message_key(message) for message in messages}
        matches = [
            match
            for match in state.get("telegram_article_matches", [])
            if isinstance(match, dict)
            and str(match.get("telegram_message_key") or "") in message_keys
        ]
        summary = sync_telegram_batch_to_remote_api(
            state, config, messages=messages, matches=matches
        )
        if not summary.get("telegram_remote_failed"):
            metadata_summary = sync_telegram_metadata_to_remote_api(state, config)
            summary["telegram_remote_failed"] = int(
                summary.get("telegram_remote_failed") or 0
            ) + int(metadata_summary.get("telegram_remote_failed") or 0)
            for key, value in metadata_summary.items():
                if key != "telegram_remote_failed":
                    summary[key] = value
        summary["telegram_local_messages_selected"] = len(messages)
        summary["telegram_local_matches_selected"] = len(matches)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0 if not summary.get("telegram_remote_failed") else 1
    if args.command == "rematch":
        target_state = (
            json.loads(json.dumps(state, ensure_ascii=False)) if args.dry_run else state
        )
        summary = rematch_telegram_articles(
            target_state,
            config,
            limit=max(0, int(args.limit)),
            progress=bool(args.progress),
        )
        if not args.dry_run:
            save_state(state_path, target_state)
        summary["dry_run"] = int(bool(args.dry_run))
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    if args.command == "discover":
        from .dates import now_in_timezone

        target_state = (
            json.loads(json.dumps(state, ensure_ascii=False)) if args.dry_run else state
        )
        target_config = dict(config)
        telegram_settings = dict(telegram_sources_config(config))
        telegram_settings["discover_enabled"] = True
        telegram_settings["recommendation_limit"] = max(1, int(args.limit))
        target_config["telegram_sources"] = telegram_settings
        now = now_in_timezone(str(config.get("timezone") or "Asia/Seoul"))
        adapter = TelethonClientAdapter(target_config)

        async def run_discover() -> dict[str, int]:
            async with adapter as opened:
                return await _discover_with_client(
                    target_state, target_config, now, opened
                )

        summary = asyncio.run(run_discover())
        if not args.dry_run:
            save_state(state_path, target_state)
        summary["dry_run"] = int(bool(args.dry_run))
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    if args.command == "expand-similar":
        from .dates import now_in_timezone

        register_configured_channels(state, config)
        target_state = (
            json.loads(json.dumps(state, ensure_ascii=False)) if args.dry_run else state
        )
        now = now_in_timezone(str(config.get("timezone") or "Asia/Seoul"))
        adapter = TelethonClientAdapter(config)

        async def run_expand() -> dict[str, object]:
            async with adapter as opened:
                return await expand_similar_channels(
                    target_state,
                    config,
                    now,
                    opened,
                    target_multiplier=max(1.0, float(args.target_multiplier)),
                    target_count=max(0, int(args.target_count)),
                    recommendation_limit=max(1, int(args.recommendation_limit)),
                    seed_limit=max(0, int(args.seed_limit)),
                    seed_min_quality=max(0, int(args.seed_min_quality)),
                    min_quality=max(0, int(args.min_quality)),
                    max_join=max(0, int(args.max_join)),
                    delay_min_seconds=max(0.0, float(args.delay_min_seconds)),
                    delay_max_seconds=max(0.0, float(args.delay_max_seconds)),
                    dry_run=bool(args.dry_run),
                )

        summary = asyncio.run(run_expand())
        if not args.dry_run:
            save_state(state_path, target_state)
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0 if not summary.get("telegram_expand_failed") else 1
    if args.command == "backfill-messages":
        from .dates import now_in_timezone

        target_state = (
            json.loads(json.dumps(state, ensure_ascii=False)) if args.dry_run else state
        )
        target_config = dict(config)
        if args.timeout_per_channel and float(args.timeout_per_channel) > 0:
            telegram_settings = dict(telegram_sources_config(config))
            telegram_settings["backfill_channel_timeout_seconds"] = float(
                args.timeout_per_channel
            )
            target_config["telegram_sources"] = telegram_settings
        if args.workers and int(args.workers) > 0:
            telegram_settings = dict(telegram_sources_config(target_config))
            telegram_settings["backfill_channel_workers"] = max(1, int(args.workers))
            target_config["telegram_sources"] = telegram_settings
        checkpoint_callback = None
        if not args.dry_run and not args.no_checkpoint:

            def checkpoint_callback(_progress_record: dict[str, object]) -> None:
                save_state(state_path, target_state)

        now = now_in_timezone(str(config.get("timezone") or "Asia/Seoul"))
        summary = backfill_telegram_messages(
            target_state,
            target_config,
            now,
            days=max(1, int(args.days)),
            limit_per_channel=max(1, int(args.limit_per_channel)),
            channel_limit=max(0, int(args.channel_limit)),
            sync_remote=not args.dry_run and not args.no_remote,
            progress=True,
            only_handles=parse_handle_list(args.only_handles),
            skip_handles=parse_handle_list(args.skip_handles),
            start_after_handle=args.start_after,
            expected_selection_fingerprint=args.expected_selection_fingerprint,
            max_messages=max(0, int(args.max_messages)),
            checkpoint_callback=checkpoint_callback,
        )
        if not args.dry_run:
            save_state(state_path, target_state)
        summary["dry_run"] = int(bool(args.dry_run))
        summary["remote_disabled"] = int(bool(args.no_remote))
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0
    if args.command == "import-joined":
        target_state = (
            json.loads(json.dumps(state, ensure_ascii=False)) if args.dry_run else state
        )
        summary = import_joined_public_channels(
            target_state,
            config,
            limit=max(1, int(args.limit)),
            enable=bool(args.enable),
            min_quality=max(0, int(args.min_quality)),
            max_import=max(0, int(args.max_import)),
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
            lines = [
                line
                for line in existing.splitlines()
                if not line.startswith("TELEGRAM_SESSION_STRING=")
            ]
            lines.append(f"TELEGRAM_SESSION_STRING={session}")
            out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
            print(
                json.dumps(
                    {"ok": True, "written": str(out_path)}, ensure_ascii=False, indent=2
                )
            )
        else:
            print(session)
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(cli_main())
