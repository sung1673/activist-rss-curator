from __future__ import annotations

import json
import hashlib
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from html import escape
from pathlib import Path

from .cluster import extract_company_candidates
from .dates import datetime_to_iso, parse_datetime
from .source_rights import source_is_authorized
from .telegram_sources import (
    channel_quality_metrics,
    ensure_telegram_state,
    is_collectable_public_channel,
    ordered_message_tokens,
    risk_flags_for_text,
    telegram_issue_signals,
)


TELEGRAM_DASHBOARD_RELATIVE_PATH = Path("public") / "feed" / "telegram-admin.html"
TELEGRAM_ADMIN_STORAGE_KEY = "telegramAdminAccessToken"
TOKEN_STOPWORDS = {
    "그리고",
    "관련",
    "기사",
    "뉴스",
    "시장",
    "오늘",
    "이번",
    "지난",
    "있는",
    "없는",
    "으로",
    "에서",
    "한다",
    "했다",
    "합니다",
    "보도",
    "공유",
}
NON_LISTED_COMPANY_NAMES = {
    "NPS",
    "국민연금",
    "Elliott Management",
    "Starboard Value",
    "Third Point",
    "Trian Partners",
    "D.E. Shaw",
    "ValueAct",
    "Sachem Head",
    "Saba Capital",
    "Browning West",
}


def _dt(value: object, timezone_name: str) -> datetime | None:
    return parse_datetime(value, timezone_name)


def _date_key(value: object, timezone_name: str) -> str:
    parsed = _dt(value, timezone_name)
    return parsed.strftime("%Y-%m-%d") if parsed else "날짜 미상"


def _compact(value: object, max_chars: int = 90) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 1)].rstrip() + "…"


def _tokens(text: str) -> list[str]:
    return [token for token in ordered_message_tokens({"text": text}) if token not in TOKEN_STOPWORDS and len(token) >= 2]


def _listed_company_candidates(text: str) -> list[str]:
    candidates = []
    for company in extract_company_candidates(text):
        cleaned = re.sub(r"\s+", " ", str(company or "")).strip(" -·,")
        if not cleaned or cleaned in NON_LISTED_COMPANY_NAMES:
            continue
        if cleaned.casefold() in {name.casefold() for name in NON_LISTED_COMPANY_NAMES}:
            continue
        candidates.append(cleaned)

    filtered: list[str] = []
    for company in sorted(dict.fromkeys(candidates), key=len, reverse=True):
        if any(company != existing and company in existing for existing in filtered):
            continue
        filtered.append(company)
    return filtered[:4]


def _message_type(message: dict[str, object]) -> str:
    text = str(message.get("normalized_text") or message.get("text") or "").casefold()
    if any(keyword in text for keyword in ("공시", "불성실공시", "거래정지", "상장폐지", "정정신고서")):
        return "공시·규제"
    if any(keyword in text for keyword in ("실적", "매출", "영업이익", "컨센서스", "가이던스")):
        return "실적"
    if any(keyword in text for keyword in ("주주", "행동주의", "경영권", "위임장", "공개매수", "이사회")):
        return "주주·지배구조"
    if any(keyword in text for keyword in ("밸류업", "벨류업", "배당", "자사주", "주주환원")):
        return "밸류업·환원"
    if any(keyword in text for keyword in ("환율", "채권", "금리", "fed", "미국", "중국", "일본")):
        return "매크로·해외"
    return "기타"


def _message_datetime(message: dict[str, object], timezone_name: str, fallback: datetime) -> datetime:
    return _dt(message.get("posted_at"), timezone_name) or fallback


def _signal_datetime(signal: dict[str, object], key: str, timezone_name: str, fallback: datetime) -> datetime:
    return _dt(signal.get(key), timezone_name) or fallback


def _signal_risk_flags(signal: dict[str, object]) -> list[str]:
    flags = [str(flag) for flag in signal.get("risk_flags", []) if str(flag)]
    if int(signal.get("related_telegram_channels_count") or 0) <= 1 and int(signal.get("related_telegram_count") or 0) >= 5:
        flags.append("single_channel_spike")
    return sorted(set(flags))


def _signal_lifecycle(signal: dict[str, object], timezone_name: str, now: datetime) -> str:
    first_seen = _signal_datetime(signal, "first_seen_at", timezone_name, now)
    latest_seen = _signal_datetime(signal, "latest_seen_at", timezone_name, first_seen)
    first_age_hours = max(0.0, (now - first_seen).total_seconds() / 3600)
    latest_age_hours = max(0.0, (now - latest_seen).total_seconds() / 3600)
    count = int(signal.get("related_telegram_count") or 0)
    channels = int(signal.get("related_telegram_channels_count") or 0)
    if first_age_hours <= 24 and latest_age_hours <= 8:
        return "new"
    if latest_age_hours <= 12 and (channels >= 2 or count >= 5):
        return "rising"
    if latest_age_hours <= 36:
        return "active"
    if latest_age_hours <= 96:
        return "fading"
    return "stale"


def _signal_score(signal: dict[str, object], timezone_name: str, now: datetime) -> int:
    count = int(signal.get("related_telegram_count") or 0)
    channels = int(signal.get("related_telegram_channels_count") or 0)
    confidence = float(signal.get("confidence_score") or 0)
    latest_seen = _signal_datetime(signal, "latest_seen_at", timezone_name, now)
    latest_age_hours = max(0.0, (now - latest_seen).total_seconds() / 3600)
    freshness = 14 if latest_age_hours <= 6 else 10 if latest_age_hours <= 24 else 5 if latest_age_hours <= 72 else 0
    score = min(26, count * 4) + min(30, channels * 10) + min(22, round(confidence * 22)) + freshness
    flags = _signal_risk_flags(signal)
    if "promotional" in flags:
        score -= 14
    if "rumor" in flags or "unverified" in flags:
        score -= 8
    if "single_channel_spike" in flags:
        score -= 10
    return max(0, min(100, int(score)))


def _signal_bucket(signal: dict[str, object], timezone_name: str, now: datetime) -> str:
    lifecycle = _signal_lifecycle(signal, timezone_name, now)
    signal_type = str(signal.get("signal_type") or "")
    flags = set(_signal_risk_flags(signal))
    if flags & {"rumor", "promotional", "unverified", "single_channel_spike"}:
        return "risk_watch"
    if signal_type in {"topic_burst", "url_burst"}:
        return "watchlist_candidate"
    if lifecycle in {"new", "rising"}:
        return "new_rising"
    return "confirmed_reaction"


def _enrich_signal(signal: dict[str, object], timezone_name: str, now: datetime) -> dict[str, object]:
    enriched = dict(signal)
    enriched["signal_score"] = _signal_score(signal, timezone_name, now)
    enriched["lifecycle"] = _signal_lifecycle(signal, timezone_name, now)
    enriched["analysis_bucket"] = _signal_bucket(signal, timezone_name, now)
    enriched["risk_flags"] = _signal_risk_flags(signal)
    return enriched


def _message_volume_snapshot(messages: list[dict[str, object]], timezone_name: str, now: datetime) -> dict[str, object]:
    since_24h = now - timedelta(hours=24)
    prev_24h = now - timedelta(hours=48)
    recent = [message for message in messages if _message_datetime(message, timezone_name, now) >= since_24h]
    previous = [
        message
        for message in messages
        if prev_24h <= _message_datetime(message, timezone_name, now) < since_24h
    ]
    ratio = (len(recent) / len(previous)) if previous else (float(len(recent)) if recent else 0.0)
    label = "rising" if ratio >= 1.4 and len(recent) >= 5 else "cooling" if previous and ratio <= 0.7 else "steady"
    return {
        "recent_24h": len(recent),
        "previous_24h": len(previous),
        "velocity_ratio": round(ratio, 2),
        "velocity_label": label,
    }


def _top_risk_flags(messages: list[dict[str, object]], signals: list[dict[str, object]]) -> list[tuple[str, int]]:
    counter: Counter[str] = Counter()
    for message in messages:
        counter.update(risk_flags_for_text(str(message.get("text") or message.get("normalized_text") or "")))
    for signal in signals:
        counter.update(_signal_risk_flags(signal))
    return counter.most_common(12)


def _company_signal_score(row: dict[str, object]) -> int:
    mentions = int(row.get("mentions_14d") or 0)
    recent = int(row.get("mentions_24h") or 0)
    channels = int(row.get("channels_count") or 0)
    events = len(row.get("event_types") or [])
    velocity_ratio = float(row.get("velocity_ratio") or 0)
    risk_flags = set(str(flag) for flag in row.get("risk_flags", []))
    score = min(30, mentions * 3) + min(24, channels * 8) + min(16, recent * 5) + min(12, events * 4)
    if velocity_ratio >= 2 and recent >= 2:
        score += 12
    elif velocity_ratio >= 1.3 and recent >= 2:
        score += 6
    if "promotional" in risk_flags:
        score -= 12
    if "rumor" in risk_flags or "unverified" in risk_flags:
        score -= 8
    if channels <= 1 and mentions >= 5:
        score -= 10
    return max(0, min(100, int(score)))


def _company_lifecycle(row: dict[str, object]) -> str:
    recent = int(row.get("mentions_24h") or 0)
    previous = int(row.get("mentions_prev_24h") or 0)
    velocity = float(row.get("velocity_ratio") or 0)
    channels = int(row.get("channels_count") or 0)
    if recent >= 2 and previous == 0:
        return "new"
    if recent >= 2 and velocity >= 1.4 and channels >= 2:
        return "rising"
    if recent > 0:
        return "active"
    return "fading"


def _company_signal_bucket(row: dict[str, object]) -> str:
    risk_flags = set(str(flag) for flag in row.get("risk_flags", []))
    if risk_flags & {"rumor", "promotional", "unverified"}:
        return "risk_watch"
    lifecycle = str(row.get("lifecycle") or "")
    if lifecycle in {"new", "rising"}:
        return "new_rising"
    return "tracked_company"


def company_signal_rows(messages: list[dict[str, object]], timezone_name: str, now: datetime) -> list[dict[str, object]]:
    since_24h = now - timedelta(hours=24)
    prev_24h = now - timedelta(hours=48)
    since_14d = now - timedelta(days=14)
    grouped: dict[str, dict[str, object]] = {}
    for message in messages:
        posted_at = _message_datetime(message, timezone_name, now)
        if posted_at < since_14d:
            continue
        text = str(message.get("normalized_text") or message.get("text") or "")
        companies = _listed_company_candidates(text)
        if not companies:
            continue
        handle = str(message.get("handle") or message.get("channel_title") or "")
        message_url = str(message.get("message_url") or "")
        event_type = _message_type(message)
        flags = risk_flags_for_text(text)
        for company in companies:
            row = grouped.setdefault(
                company,
                {
                    "company": company,
                    "mentions_14d": 0,
                    "mentions_24h": 0,
                    "mentions_prev_24h": 0,
                    "channels": Counter(),
                    "event_types": Counter(),
                    "risk_flags_counter": Counter(),
                    "top_messages": [],
                    "latest_at": "",
                },
            )
            row["mentions_14d"] = int(row["mentions_14d"]) + 1
            if posted_at >= since_24h:
                row["mentions_24h"] = int(row["mentions_24h"]) + 1
            elif posted_at >= prev_24h:
                row["mentions_prev_24h"] = int(row["mentions_prev_24h"]) + 1
            row["channels"][handle or "unknown"] += 1  # type: ignore[index]
            row["event_types"][event_type] += 1  # type: ignore[index]
            row["risk_flags_counter"].update(flags)  # type: ignore[index]
            latest_at = datetime_to_iso(posted_at)
            if latest_at > str(row.get("latest_at") or ""):
                row["latest_at"] = latest_at
            row["top_messages"].append(  # type: ignore[union-attr]
                {
                    "channel_title": message.get("channel_title") or handle,
                    "channel_handle": handle,
                    "posted_at": latest_at,
                    "message_url": message_url,
                    "excerpt": _compact(text, 120),
                    "event_type": event_type,
                    "risk_flags": flags,
                }
            )

    rows: list[dict[str, object]] = []
    for row in grouped.values():
        channels: Counter[str] = row.pop("channels")  # type: ignore[assignment]
        event_types: Counter[str] = row.pop("event_types")  # type: ignore[assignment]
        risk_counter: Counter[str] = row.pop("risk_flags_counter")  # type: ignore[assignment]
        previous = int(row.get("mentions_prev_24h") or 0)
        recent = int(row.get("mentions_24h") or 0)
        velocity_ratio = round((recent / previous) if previous else (float(recent) if recent else 0.0), 2)
        messages_for_company = sorted(
            list(row.get("top_messages") or []),
            key=lambda message: str(message.get("posted_at") or ""),
            reverse=True,
        )[:5]
        public_row = {
            **row,
            "channels_count": len([channel for channel in channels if channel]),
            "top_channels": [{"label": label, "count": count} for label, count in channels.most_common(6)],
            "event_types": [{"label": label, "count": count} for label, count in event_types.most_common(5)],
            "risk_flags": [label for label, _count in risk_counter.most_common(6)],
            "velocity_ratio": velocity_ratio,
            "top_messages": messages_for_company,
        }
        public_row["signal_score"] = _company_signal_score(public_row)
        public_row["lifecycle"] = _company_lifecycle(public_row)
        public_row["analysis_bucket"] = _company_signal_bucket(public_row)
        rows.append(public_row)

    rows.sort(
        key=lambda row: (
            int(row.get("signal_score") or 0),
            int(row.get("mentions_24h") or 0),
            int(row.get("channels_count") or 0),
            int(row.get("mentions_14d") or 0),
        ),
        reverse=True,
    )
    return rows


def telegram_dashboard_model(state: dict[str, object], config: dict[str, object], now: datetime) -> dict[str, object]:
    ensure_telegram_state(state)
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    all_channels = [channel for channel in state.get("telegram_source_channels", []) if isinstance(channel, dict)]
    channels = [
        channel
        for channel in all_channels
        if source_is_authorized(
            {"source_kind": "telegram", "handle": channel.get("handle"), "source_right_id": channel.get("source_right_id")},
            config,
            now,
            purpose="public",
        )
    ]
    collectable_channels = [channel for channel in channels if is_collectable_public_channel(channel)]
    enabled_channels = [channel for channel in collectable_channels if bool(channel.get("enabled", True))]
    messages = [
        message
        for message in state.get("telegram_source_messages", [])
        if isinstance(message, dict)
        and not message.get("deleted_at")
        and source_is_authorized(
            {
                "source_kind": message.get("source_kind") or "telegram",
                "handle": message.get("handle") or message.get("channel_handle"),
                "source_right_id": message.get("source_right_id"),
            },
            config,
            now,
            purpose="public",
        )
    ]
    matches = [match for match in state.get("telegram_article_matches", []) if isinstance(match, dict)]
    candidates = [candidate for candidate in state.get("telegram_channel_candidates", []) if isinstance(candidate, dict)]
    since_24h = now - timedelta(hours=24)
    since_14d = now - timedelta(days=14)
    recent_24h = [message for message in messages if (_dt(message.get("posted_at"), timezone_name) or now) >= since_24h]
    recent_14d = [message for message in messages if (_dt(message.get("posted_at"), timezone_name) or now) >= since_14d]
    volume = _message_volume_snapshot(messages, timezone_name, now)

    messages_by_channel: dict[str, list[dict[str, object]]] = defaultdict(list)
    type_counter: Counter[str] = Counter()
    day_counter: Counter[str] = Counter()
    keyword_counter: Counter[str] = Counter()
    for message in messages:
        handle = str(message.get("handle") or message.get("channel_title") or "unknown")
        messages_by_channel[handle].append(message)
        type_counter[_message_type(message)] += 1
        day_counter[_date_key(message.get("posted_at"), timezone_name)] += 1
    for message in recent_14d:
        keyword_counter.update(_tokens(str(message.get("normalized_text") or message.get("text") or ""))[:18])
    company_signals = company_signal_rows(messages, timezone_name, now)
    company_buckets: dict[str, list[dict[str, object]]] = {"new_rising": [], "risk_watch": [], "tracked_company": []}
    for row in company_signals:
        company_buckets.setdefault(str(row.get("analysis_bucket") or "tracked_company"), []).append(row)

    channel_rows: list[dict[str, object]] = []
    for channel in enabled_channels:
        handle = str(channel.get("handle") or "")
        channel_messages = messages_by_channel.get(handle, [])
        latest_at = max((str(message.get("posted_at") or "") for message in channel_messages), default="")
        metrics = channel_quality_metrics(state, channel)
        channel_rows.append(
            {
                "handle": handle,
                "title": channel.get("title") or handle,
                "quality_score": int(channel.get("quality_score") or 0),
                "signal_quality_score": int(metrics.get("signal_quality_score") or 0),
                "messages": len(channel_messages),
                "matches": int(metrics.get("matches") or 0),
                "direct_matches": int(metrics.get("direct_matches") or 0),
                "weak_matches": int(metrics.get("weak_matches") or 0),
                "risk_messages": int(metrics.get("risk_messages") or 0),
                "match_rate": float(metrics.get("match_rate") or 0),
                "risk_rate": float(metrics.get("risk_rate") or 0),
                "latest_at": latest_at,
                "last_error": channel.get("last_error") or "",
            }
        )
    channel_rows.sort(
        key=lambda row: (
            int(row.get("signal_quality_score") or 0),
            int(row.get("matches") or 0),
            int(row.get("messages") or 0),
            str(row.get("latest_at") or ""),
        ),
        reverse=True,
    )

    sample = recent_14d[-500:] if len(recent_14d) > 500 else recent_14d
    avg_bytes = 0
    if sample:
        avg_bytes = max(1, round(len(json.dumps(sample, ensure_ascii=False, sort_keys=True).encode("utf-8")) / len(sample)))
    daily_messages = len(recent_14d) / 14 if recent_14d else 0

    signals = [
        _enrich_signal(signal, timezone_name, now)
        for signal in telegram_issue_signals(state, config, limit=30, now=now)
    ]
    signals.sort(key=lambda signal: (int(signal.get("signal_score") or 0), float(signal.get("confidence_score") or 0)), reverse=True)
    analysis_buckets: dict[str, list[dict[str, object]]] = {"new_rising": [], "watchlist_candidate": [], "risk_watch": [], "confirmed_reaction": []}
    for signal in signals:
        bucket = str(signal.get("analysis_bucket") or "confirmed_reaction")
        analysis_buckets.setdefault(bucket, []).append(signal)
    match_type_counter: Counter[str] = Counter(str(match.get("match_type") or "unknown") for match in matches)
    quality_bands: Counter[str] = Counter()
    for row in channel_rows:
        score = int(row.get("signal_quality_score") or 0)
        if score >= 80:
            quality_bands["80+"] += 1
        elif score >= 60:
            quality_bands["60-79"] += 1
        elif score >= 40:
            quality_bands["40-59"] += 1
        else:
            quality_bands["0-39"] += 1
    return {
        "generated_at": datetime_to_iso(now),
        "channels_total": len(all_channels),
        "channels_authorized": len(channels),
        "channels_rights_blocked": len(all_channels) - len(channels),
        "channels_collectable": len(collectable_channels),
        "channels_enabled": len(enabled_channels),
        "channels_failed": len([channel for channel in enabled_channels if channel.get("last_error")]),
        "messages_total": len(messages),
        "messages_24h": len(recent_24h),
        "messages_14d": len(recent_14d),
        "matches_total": len(matches),
        "candidates_total": len(candidates),
        "candidate_pending": len([candidate for candidate in candidates if candidate.get("status") == "pending"]),
        "top_channels": channel_rows[:24],
        "type_counts": type_counter.most_common(),
        "day_counts": sorted(day_counter.items())[-21:],
        "top_keywords": keyword_counter.most_common(30),
        "signals": signals[:12],
        "signal_overview": {
            "top_score": int(signals[0].get("signal_score") or 0) if signals else 0,
            "new_rising": len(analysis_buckets.get("new_rising", [])),
            "watchlist_candidates": len(analysis_buckets.get("watchlist_candidate", [])),
            "risk_watch": len(analysis_buckets.get("risk_watch", [])),
            "confirmed_reactions": len(analysis_buckets.get("confirmed_reaction", [])),
            **volume,
        },
        "new_rising_signals": analysis_buckets.get("new_rising", [])[:8],
        "watchlist_candidates": analysis_buckets.get("watchlist_candidate", [])[:8],
        "risk_watch_signals": analysis_buckets.get("risk_watch", [])[:8],
        "risk_flag_counts": _top_risk_flags(recent_14d, signals),
        "company_signal_overview": {
            "companies_total": len(company_signals),
            "top_score": int(company_signals[0].get("signal_score") or 0) if company_signals else 0,
            "new_rising": len(company_buckets.get("new_rising", [])),
            "risk_watch": len(company_buckets.get("risk_watch", [])),
            "tracked": len(company_buckets.get("tracked_company", [])),
        },
        "top_company_signals": company_signals[:16],
        "new_rising_companies": company_buckets.get("new_rising", [])[:8],
        "company_risk_watch": company_buckets.get("risk_watch", [])[:8],
        "match_type_counts": match_type_counter.most_common(),
        "quality_bands": quality_bands.most_common(),
        "growth": {
            "avg_message_bytes": avg_bytes,
            "daily_messages": round(daily_messages, 1),
            "monthly_messages": round(daily_messages * 30),
            "yearly_messages": round(daily_messages * 365),
            "monthly_mb": round(daily_messages * 30 * avg_bytes / 1024 / 1024, 2) if avg_bytes else 0,
            "yearly_mb": round(daily_messages * 365 * avg_bytes / 1024 / 1024, 2) if avg_bytes else 0,
        },
    }


def _stat_card(label: str, value: object, note: str = "") -> str:
    note_html = f"<span>{escape(str(note))}</span>" if note else ""
    return f'<article class="stat"><strong>{escape(str(value))}</strong><p>{escape(label)}</p>{note_html}</article>'


def telegram_admin_access_token() -> str:
    return os.environ.get("TELEGRAM_ADMIN_ACCESS_TOKEN", "").strip()


def telegram_dashboard_api_url() -> str:
    """Return only the browser-safe public API endpoint."""

    return os.environ.get("ACTIVIST_PUBLIC_API_URL", "").strip()


def telegram_admin_access_token_hash(token: str | None = None) -> str:
    value = (token if token is not None else telegram_admin_access_token()).strip()
    if not value:
        return ""
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _locked_fallback_model(model: dict[str, object]) -> dict[str, object]:
    # Static Pages artifacts never embed signal/message arrays. Authorized
    # administrators retrieve paginated data from the API after unlocking.
    compact = {
        "generated_at": model.get("generated_at"),
        "channels_total": 0,
        "channels_collectable": 0,
        "channels_enabled": 0,
        "channels_failed": 0,
        "messages_total": 0,
        "messages_24h": 0,
        "messages_14d": 0,
        "matches_total": 0,
        "candidates_total": 0,
        "candidate_pending": 0,
        "top_channels": [],
        "type_counts": [],
        "day_counts": [],
        "top_keywords": [],
        "signals": [],
        "signal_overview": {
            "top_score": 0,
            "new_rising": 0,
            "watchlist_candidates": 0,
            "risk_watch": 0,
            "confirmed_reactions": 0,
            "velocity_ratio": 0,
            "velocity_label": "locked",
        },
        "new_rising_signals": [],
        "watchlist_candidates": [],
        "risk_watch_signals": [],
        "risk_flag_counts": [],
        "company_signal_overview": {
            "companies_total": 0,
            "top_score": 0,
            "new_rising": 0,
            "risk_watch": 0,
            "tracked": 0,
        },
        "top_company_signals": [],
        "new_rising_companies": [],
        "company_risk_watch": [],
        "match_type_counts": [],
        "quality_bands": [],
        "growth": {
            "avg_message_bytes": 0,
            "daily_messages": 0,
            "monthly_messages": 0,
            "yearly_messages": 0,
            "monthly_mb": 0,
            "yearly_mb": 0,
        },
    }
    return compact


def _signal_card(signal: dict[str, object]) -> str:
    flags = [str(flag) for flag in signal.get("risk_flags", [])[:4]]
    flag_html = "".join(f"<span>{escape(flag)}</span>" for flag in flags)
    keywords = ", ".join(str(keyword) for keyword in signal.get("top_keywords", [])[:5])
    return (
        '<article class="signal-card">'
        f'<div class="signal-card__top"><b>{escape(str(signal.get("signal_score") or 0))}</b>'
        f'<span>{escape(str(signal.get("lifecycle") or ""))}</span></div>'
        f'<h3>{escape(_compact(signal.get("signal_title"), 78))}</h3>'
        f'<p>{escape(_compact(signal.get("signal_summary") or keywords or signal.get("signal_type"), 112))}</p>'
        f'<div class="signal-card__meta"><span>{escape(str(signal.get("related_telegram_count") or 0))}건</span>'
        f'<span>{escape(str(signal.get("related_telegram_channels_count") or 0))}채널</span>'
        f'<span>{escape(str(signal.get("signal_type") or ""))}</span></div>'
        f'<div class="risk-chips">{flag_html}</div>'
        "</article>"
    )


def _company_card(row: dict[str, object]) -> str:
    flags = [str(flag) for flag in row.get("risk_flags", [])[:4]]
    flag_html = "".join(f"<span>{escape(flag)}</span>" for flag in flags)
    events = ", ".join(str(item.get("label") or "") for item in row.get("event_types", [])[:4] if isinstance(item, dict))
    return (
        '<article class="signal-card company-card">'
        f'<div class="signal-card__top"><b>{escape(str(row.get("signal_score") or 0))}</b>'
        f'<span>{escape(str(row.get("lifecycle") or ""))}</span></div>'
        f'<h3>{escape(_compact(row.get("company"), 48))}</h3>'
        f'<p>{escape(_compact(events or "상장사 언급 추적", 112))}</p>'
        f'<div class="signal-card__meta"><span>24h {escape(str(row.get("mentions_24h") or 0))}</span>'
        f'<span>14d {escape(str(row.get("mentions_14d") or 0))}</span>'
        f'<span>{escape(str(row.get("channels_count") or 0))}채널</span>'
        f'<span>{escape(str(row.get("velocity_ratio") or 0))}x</span></div>'
        f'<div class="risk-chips">{flag_html}</div>'
        "</article>"
    )


def write_telegram_dashboard(project_root: Path, state: dict[str, object], config: dict[str, object], now: datetime) -> Path:
    model = telegram_dashboard_model(state, config, now)
    access_hash = telegram_admin_access_token_hash()
    model = _locked_fallback_model(model)
    api_url = telegram_dashboard_api_url()
    fallback_model_json = json.dumps(model, ensure_ascii=False, separators=(",", ":"))
    access_hash_json = json.dumps(access_hash, ensure_ascii=False)
    api_url_json = json.dumps(api_url, ensure_ascii=False)
    output_path = project_root / TELEGRAM_DASHBOARD_RELATIVE_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    stats = "\n".join(
        [
            _stat_card("수집 가능 공개 채널", model["channels_collectable"], f"enabled {model['channels_enabled']}"),
            _stat_card("최근 24시간 메시지", model["messages_24h"]),
            _stat_card("최근 14일 메시지", model["messages_14d"]),
            _stat_card("기사 매칭", model["matches_total"]),
            _stat_card("시그널 채널", len([row for row in model["top_channels"] if row.get("matches")]), "매칭 1건 이상"),
            _stat_card("추천 후보", model["candidates_total"], f"pending {model['candidate_pending']}"),
            _stat_card("월간 예상", f"{model['growth']['monthly_messages']}건", f"{model['growth']['monthly_mb']} MB"),
        ]
    )
    overview = model["signal_overview"]
    signal_stats = "\n".join(
        [
            _stat_card("상위 시그널 점수", overview["top_score"], "100점 기준"),
            _stat_card("New/Rising", overview["new_rising"], "최근 부상"),
            _stat_card("Watch 후보", overview["watchlist_candidates"], "기사 전 단계"),
            _stat_card("Risk watch", overview["risk_watch"], "확인 필요"),
            _stat_card("24h 속도", f"{overview['velocity_ratio']}x", str(overview["velocity_label"])),
        ]
    )
    new_rising_cards = "\n".join(_signal_card(signal) for signal in model["new_rising_signals"])
    watchlist_cards = "\n".join(_signal_card(signal) for signal in model["watchlist_candidates"])
    risk_cards = "\n".join(_signal_card(signal) for signal in model["risk_watch_signals"])
    risk_flag_rows = "\n".join(
        f"<span>{escape(str(label))} <b>{count}</b></span>"
        for label, count in model["risk_flag_counts"]
    )
    company_overview = model["company_signal_overview"]
    company_stats = "\n".join(
        [
            _stat_card("상장사 추적", company_overview["companies_total"], "최근 14일"),
            _stat_card("상위 회사 점수", company_overview["top_score"], "100점 기준"),
            _stat_card("상승 상장사", company_overview["new_rising"], "24h 증가"),
            _stat_card("리스크 확인", company_overview["risk_watch"], "루머·홍보성 등"),
            _stat_card("추적 중", company_overview["tracked"], "반복 언급"),
        ]
    )
    new_company_cards = "\n".join(_company_card(row) for row in model["new_rising_companies"])
    risk_company_cards = "\n".join(_company_card(row) for row in model["company_risk_watch"])
    company_rows = "\n".join(
        "<tr>"
        f"<td><b>{escape(str(row.get('company') or ''))}</b><br><small>{escape(str(row.get('lifecycle') or ''))}</small></td>"
        f"<td>{escape(str(row.get('signal_score') or 0))}</td>"
        f"<td>{escape(str(row.get('mentions_24h') or 0))} / {escape(str(row.get('mentions_14d') or 0))}</td>"
        f"<td>{escape(str(row.get('channels_count') or 0))}</td>"
        f"<td>{escape(str(row.get('velocity_ratio') or 0))}x</td>"
        f"<td>{escape(', '.join(str(item.get('label') or '') for item in row.get('event_types', [])[:3] if isinstance(item, dict)))}</td>"
        f"<td>{escape(', '.join(str(flag) for flag in row.get('risk_flags', [])[:4]))}</td>"
        f"<td>{escape(str(row.get('latest_at') or ''))}</td>"
        "</tr>"
        for row in model["top_company_signals"][:16]
    )
    channel_rows = "\n".join(
        "<tr>"
        f"<td>@{escape(str(row.get('handle') or ''))}</td>"
        f"<td>{escape(_compact(row.get('title'), 42))}</td>"
        f"<td>{escape(str(row.get('signal_quality_score') or row.get('quality_score') or 0))}<br><small>기본 {escape(str(row.get('quality_score') or 0))}</small></td>"
        f"<td>{escape(str(row.get('messages') or 0))}</td>"
        f"<td>{escape(str(row.get('matches') or 0))}<br><small>URL {escape(str(row.get('direct_matches') or 0))} · 추정 {escape(str(row.get('weak_matches') or 0))}</small></td>"
        f"<td>{escape(str(round(float(row.get('match_rate') or 0) * 100, 1)))}%</td>"
        f"<td>{escape(str(row.get('risk_messages') or 0))}</td>"
        f"<td>{escape(str(row.get('latest_at') or ''))}</td>"
        f"<td>{escape(str(row.get('last_error') or ''))}</td>"
        "</tr>"
        for row in model["top_channels"]
    )
    type_rows = "\n".join(
        f"<li><b>{escape(str(label))}</b><span>{count}건</span></li>"
        for label, count in model["type_counts"]
    )
    keyword_rows = "\n".join(
        f"<span>{escape(str(keyword))} <b>{count}</b></span>"
        for keyword, count in model["top_keywords"][:24]
    )
    signal_rows = "\n".join(
        "<tr>"
        f"<td><b>{escape(str(signal.get('signal_title') or signal.get('article_id') or ''))}</b><br><small>{escape(str(signal.get('signal_summary') or signal.get('signal_type') or ''))}</small></td>"
        f"<td>{escape(str(signal.get('related_telegram_count') or 0))}</td>"
        f"<td>{escape(str(signal.get('related_telegram_channels_count') or 0))}</td>"
        f"<td>{escape(', '.join(str(keyword) for keyword in signal.get('top_keywords', [])[:5]))}</td>"
        f"<td>{escape(', '.join(str(flag) for flag in signal.get('risk_flags', [])[:5]))}</td>"
        "</tr>"
        for signal in model["signals"]
    )
    match_type_rows = "\n".join(
        f"<li><b>{escape(str(label))}</b><span>{count}건</span></li>"
        for label, count in model["match_type_counts"]
    )
    quality_rows = "\n".join(
        f"<span>{escape(str(label))} <b>{count}</b></span>"
        for label, count in model["quality_bands"]
    )
    day_rows = "\n".join(
        f"<div><span>{escape(str(day))}</span><b style=\"width:{min(100, count * 100 / max(1, max((c for _d, c in model['day_counts']), default=1))):.1f}%\"></b><em>{count}</em></div>"
        for day, count in model["day_counts"]
    )
    html = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Telegram 수집 운영 대시보드 | BSIDE Daily News</title>
  <style>
    :root {{ --ink:#171321; --muted:#6d6478; --accent:#6f35e8; --line:#ded7ec; --soft:#f6f1ff; --paper:#fff; }}
    * {{ box-sizing:border-box; }}
    [hidden] {{ display:none !important; }}
    body {{ margin:0; font-family:Arial, "Noto Sans KR", sans-serif; color:var(--ink); background:#fbf9ff; }}
    main {{ max-width:1120px; margin:0 auto; padding:28px 22px 56px; }}
    header {{ display:flex; justify-content:space-between; gap:20px; align-items:flex-start; border-bottom:2px solid var(--ink); padding-bottom:18px; }}
    h1 {{ margin:20px 0 8px; font-family:Georgia, "Times New Roman", serif; font-size:42px; letter-spacing:0; }}
    h2 {{ margin:30px 0 12px; font-size:20px; }}
    p {{ color:var(--muted); line-height:1.6; }}
    a {{ color:var(--accent); }}
    .brand {{ color:var(--accent); font-size:34px; font-weight:800; letter-spacing:-1px; text-decoration:none; }}
    .brand span {{ font-size:12px; letter-spacing:2px; margin-left:8px; }}
    .stats {{ display:grid; grid-template-columns:repeat(7,1fr); gap:10px; margin:22px 0; }}
    .stat {{ border:1px solid var(--line); background:var(--paper); padding:14px; min-height:96px; }}
    .stat strong {{ display:block; font-size:25px; color:var(--accent); }}
    .stat p {{ margin:8px 0 0; color:var(--ink); font-weight:700; }}
    .stat span {{ color:var(--muted); font-size:12px; }}
    .grid {{ display:grid; grid-template-columns:1.1fr .9fr; gap:20px; }}
    .signal-stats {{ grid-template-columns:repeat(5,1fr); margin-top:12px; }}
    .signal-board {{ display:grid; grid-template-columns:repeat(3,1fr); gap:16px; margin:16px 0 28px; }}
    .signal-lane {{ border-top:2px solid var(--ink); padding-top:10px; }}
    .signal-lane h2 {{ margin:0 0 10px; font-size:18px; }}
    .signal-list {{ display:grid; gap:10px; }}
    .signal-card {{ border:1px solid var(--line); background:var(--paper); padding:12px; min-height:152px; }}
    .signal-card__top {{ display:flex; justify-content:space-between; align-items:center; gap:8px; color:var(--accent); font-weight:900; font-size:12px; }}
    .signal-card__top b {{ display:inline-flex; align-items:center; justify-content:center; min-width:34px; height:26px; border-radius:999px; background:var(--soft); border:1px solid rgba(111,53,232,.22); }}
    .signal-card h3 {{ margin:10px 0 7px; font-size:15px; line-height:1.35; }}
    .signal-card p {{ margin:0; font-size:12.5px; line-height:1.45; display:-webkit-box; -webkit-line-clamp:3; -webkit-box-orient:vertical; overflow:hidden; }}
    .signal-card__meta {{ display:flex; flex-wrap:wrap; gap:6px; margin-top:10px; color:var(--muted); font-size:11px; }}
    .risk-chips {{ display:flex; flex-wrap:wrap; gap:5px; margin-top:8px; }}
    .risk-chips span {{ border:1px solid rgba(111,53,232,.18); border-radius:999px; padding:3px 7px; color:var(--accent); background:var(--soft); font-size:10.5px; font-weight:800; }}
    .company-board {{ grid-template-columns:1fr 1fr; }}
    .company-table {{ overflow-x:auto; border:1px solid var(--line); background:var(--paper); }}
    .company-table table {{ min-width:860px; border:0; }}
    table {{ width:100%; border-collapse:collapse; background:var(--paper); border:1px solid var(--line); }}
    th, td {{ border-bottom:1px solid var(--line); padding:9px 10px; text-align:left; font-size:13px; vertical-align:top; }}
    th {{ color:var(--accent); background:var(--soft); }}
    .chips {{ display:flex; flex-wrap:wrap; gap:8px; }}
    .chips span {{ border:1px solid var(--line); border-radius:999px; padding:7px 10px; background:var(--paper); font-size:13px; }}
    .types {{ list-style:none; padding:0; margin:0; border:1px solid var(--line); background:var(--paper); }}
    .types li {{ display:flex; justify-content:space-between; padding:10px 12px; border-bottom:1px solid var(--line); }}
    .bars {{ border:1px solid var(--line); background:var(--paper); padding:12px; }}
    .bars div {{ display:grid; grid-template-columns:88px minmax(20px,1fr) 44px; gap:8px; align-items:center; margin:6px 0; font-size:12px; }}
    .bars b {{ display:block; height:8px; border-radius:99px; background:var(--accent); }}
    .note {{ border-left:4px solid var(--accent); background:var(--soft); padding:12px 14px; }}
    .status {{ display:inline-flex; align-items:center; gap:6px; border:1px solid var(--line); border-radius:999px; padding:6px 10px; font-size:12px; color:var(--muted); background:var(--paper); }}
    .status b {{ color:var(--accent); }}
    .access-gate {{ margin:24px 0; border:1px solid var(--line); background:var(--paper); padding:22px; max-width:620px; }}
    .access-gate h1 {{ margin-top:0; font-size:30px; }}
    .access-form {{ display:flex; gap:8px; flex-wrap:wrap; margin-top:14px; }}
    .access-form input {{ flex:1 1 280px; min-height:42px; border:1px solid var(--line); border-radius:8px; padding:0 12px; font-size:14px; }}
    .access-form button {{ min-height:42px; border:1px solid var(--accent); border-radius:999px; padding:0 18px; color:var(--accent); background:var(--paper); font-weight:800; cursor:pointer; }}
    .access-error {{ color:#a22828; font-size:13px; }}
    .lock-button {{ float:right; min-height:36px; border:1px solid var(--line); border-radius:999px; padding:0 14px; color:var(--muted); background:var(--paper); font-weight:700; cursor:pointer; }}
    @media (max-width:900px) {{ .stats, .signal-stats {{ grid-template-columns:repeat(2,1fr); }} .grid, .signal-board {{ grid-template-columns:1fr; }} h1 {{ font-size:32px; }} }}
  </style>
</head>
<body>
<main>
  <header>
    <a class="brand" href="https://bside.ai">bside<span>DAILY NEWS</span></a>
    <p>{escape(str(model["generated_at"]))}</p>
  </header>
  <section class="access-gate" id="access-gate" hidden>
    <h1>Telegram admin 승인 필요</h1>
    <p>고정된 관리자 페이지에서 발급된 token을 직접 입력하세요. token 링크는 Telegram이나 URL로 전달하지 않습니다.</p>
    <form class="access-form" id="access-form">
      <input id="access-token-input" type="password" autocomplete="one-time-code" placeholder="관리자 token">
      <button id="access-submit" type="submit">승인</button>
    </form>
    <p class="access-error" id="access-error" hidden>token이 올바르지 않습니다.</p>
    <p class="access-error" id="access-config-error" hidden>관리자 인증이 설정되지 않았습니다. 운영자가 TELEGRAM_ADMIN_ACCESS_TOKEN을 등록해야 합니다.</p>
  </section>
  <div id="dashboard-content" hidden>
  <button class="lock-button" id="lock-dashboard" type="button">잠금</button>
  <h1>Telegram 시장 시그널 대시보드</h1>
  <p>공개 broadcast 채널의 언급 확산, 기사 매칭, 다채널 반응, 위험 플래그를 함께 보며 시장 관심 흐름을 확인합니다. 개인 대화, 저장한 메시지, 그룹 대화는 수집 대상에서 제외됩니다.</p>
  <p class="status" id="data-status"><b>정적 fallback</b> DB API를 확인하는 중입니다.</p>
  <section class="stats" id="stats">{stats}</section>
  <section>
    <h2>상장사 시그널 분석</h2>
    <p>상장사명 기준으로 Telegram 언급을 다시 묶어 24시간 증가, 다채널 확산, 주요 이벤트, 위험 플래그를 확인합니다. 회사 중심으로 먼저 보고, 아래 이슈 신호는 보조 맥락으로 사용합니다.</p>
    <section class="stats signal-stats" id="company-stats">{company_stats}</section>
    <div class="signal-board company-board">
      <section class="signal-lane">
        <h2>상승 상장사</h2>
        <div class="signal-list" id="new-company-rows">{new_company_cards or '<p>최근 상승 상장사 신호가 아직 없습니다.</p>'}</div>
      </section>
      <section class="signal-lane">
        <h2>리스크 확인</h2>
        <div class="signal-list" id="risk-company-rows">{risk_company_cards or '<p>확인 필요 상장사 신호가 아직 없습니다.</p>'}</div>
      </section>
    </div>
    <div class="company-table">
      <table>
        <thead><tr><th>상장사</th><th>Score</th><th>24h/14d</th><th>Channels</th><th>Velocity</th><th>Events</th><th>Risk</th><th>Latest</th></tr></thead>
        <tbody id="company-rows">{company_rows or '<tr><td colspan="8">상장사 기준으로 묶을 Telegram 신호가 아직 없습니다.</td></tr>'}</tbody>
      </table>
    </div>
  </section>
  <section>
    <h2>시장 시그널 분석</h2>
    <p>점수는 언급량, 채널 폭, 최신성, confidence를 더하고 루머·홍보성·단일 채널 도배 위험을 감점한 보조 지표입니다. 투자 추천이 아니라 확인 우선순위를 정하기 위한 신호입니다.</p>
    <section class="stats signal-stats" id="signal-stats">{signal_stats}</section>
    <div class="chips" id="risk-flag-rows">{risk_flag_rows or '<span>위험 플래그 없음</span>'}</div>
    <div class="signal-board">
      <section class="signal-lane">
        <h2>New/Rising</h2>
        <div class="signal-list" id="new-rising-rows">{new_rising_cards or '<p>최근 부상 신호가 아직 없습니다.</p>'}</div>
      </section>
      <section class="signal-lane">
        <h2>Watch 후보</h2>
        <div class="signal-list" id="watchlist-rows">{watchlist_cards or '<p>기사 전 단계 후보가 아직 없습니다.</p>'}</div>
      </section>
      <section class="signal-lane">
        <h2>Risk watch</h2>
        <div class="signal-list" id="risk-watch-rows">{risk_cards or '<p>확인 필요 신호가 아직 없습니다.</p>'}</div>
      </section>
    </div>
  </section>
  <section class="grid">
    <div>
      <h2>채널별 수집 상태</h2>
      <table>
        <thead><tr><th>Handle</th><th>Title</th><th>Quality</th><th>Messages</th><th>Matches</th><th>Match rate</th><th>Risk</th><th>Latest</th><th>Error</th></tr></thead>
        <tbody id="channel-rows">{channel_rows or '<tr><td colspan="9">수집 대상 채널이 아직 없습니다.</td></tr>'}</tbody>
      </table>
    </div>
    <div>
      <h2>메시지 유형</h2>
      <ul class="types" id="type-rows">{type_rows or '<li><b>데이터 없음</b><span>0건</span></li>'}</ul>
      <h2>최근 14일 키워드</h2>
      <div class="chips" id="keyword-rows">{keyword_rows or '<span>키워드 없음</span>'}</div>
      <h2>매칭 품질</h2>
      <ul class="types" id="match-type-rows">{match_type_rows or '<li><b>매칭 없음</b><span>0건</span></li>'}</ul>
      <h2>채널 품질 분포</h2>
      <div class="chips" id="quality-rows">{quality_rows or '<span>아직 평가 전</span>'}</div>
    </div>
  </section>
  <section class="grid">
    <div>
      <h2>일별 수집량</h2>
      <div class="bars" id="day-rows">{day_rows or '<p>아직 표시할 수집량이 없습니다.</p>'}</div>
    </div>
    <div>
      <h2>분석 제안</h2>
      <div class="note">
        <p>URL 직접 공유는 기사 반응도, 키워드 반복 언급은 시장 관심도, 여러 채널 동시 언급은 이슈 확산 신호로 해석할 수 있습니다.</p>
        <p>추천 후보는 바로 가입하지 않고 pending 상태로 유지한 뒤, 운영자가 품질 점수와 제목을 보고 승인하는 방식이 안전합니다.</p>
      </div>
    </div>
  </section>
  <section>
      <h2>Telegram 이슈 신호</h2>
    <table>
      <thead><tr><th>Article</th><th>Messages</th><th>Channels</th><th>Keywords</th><th>Risk flags</th></tr></thead>
      <tbody id="signal-rows">{signal_rows or '<tr><td colspan="5">아직 기사와 연결된 Telegram 신호가 없습니다.</td></tr>'}</tbody>
    </table>
  </section>
  </div>
</main>
<script>
const fallbackModel = {fallback_model_json};
const telegramDashboardApiUrl = {api_url_json};
const telegramAdminAccessHash = {access_hash_json};
const telegramAdminStorageKey = "{TELEGRAM_ADMIN_STORAGE_KEY}";
const esc = (value) => String(value ?? "").replace(/[&<>"']/g, (ch) => ({{"&":"&amp;","<":"&lt;",">":"&gt;","\\"":"&quot;","'":"&#39;"}}[ch]));
const compact = (value, max = 90) => {{
  const text = String(value ?? "").replace(/\\s+/g, " ").trim();
  return text.length <= max ? text : `${{text.slice(0, Math.max(0, max - 1)).trim()}}…`;
}};
const statCard = (label, value, note = "") => `<article class="stat"><strong>${{esc(value)}}</strong><p>${{esc(label)}}</p>${{note ? `<span>${{esc(note)}}</span>` : ""}}</article>`;
const listEntries = (items) => Array.isArray(items) ? items : Object.entries(items || {{}}).map(([label, count]) => ({{label, count}}));
const dashboardNoiseTokens = new Set([
  "article", "articleview", "channel", "com", "contents", "daily", "feed", "flashnews", "html", "http", "https",
  "investment", "m", "news", "pdf", "rd", "report", "review", "rss", "spot", "stock", "url", "view", "www",
  "관련", "공시링크", "공정공시", "기사", "기업명", "기업분석", "기업정보", "뉴스", "리포트", "링크",
  "매출", "매출액", "목표가", "보고서명", "브리핑", "시가총액", "순이익", "연결재무제표기준영업",
  "영업", "영업익", "영업이익", "예상치", "잠정", "잠정실적", "종목", "주식", "주요", "채널",
  "최근", "추이", "투자의", "회사정보",
]);
const dashboardNoiseParts = ["rassiro", "sedaily", "stockinfo", "telegram", "한국투자증권", "한투증권"];
function keywordLabel(row) {{
  return String(row?.label ?? row?.[0] ?? "").trim();
}}
function keywordCount(row) {{
  return row?.count ?? row?.[1] ?? 0;
}}
function isUsefulDashboardKeyword(value) {{
  const text = String(value ?? "").trim().toLowerCase();
  if (!text || dashboardNoiseTokens.has(text)) return false;
  if (dashboardNoiseParts.some((part) => text.includes(part))) return false;
  if (/^(?:https?:\\/\\/|www\\.)/.test(text)) return false;
  if (/\\.(?:com|co\\.kr|kr|net|org|io|ai)(?:\\/|$)/.test(text)) return false;
  if (/^[0-9,.%+-]+$/.test(text)) return false;
  if (/^\\d+(?:\\.\\d+)?q$/.test(text)) return false;
  if (/^\\d+(?:\\.\\d+)?(?:조|억|만원|천원|원|달러|usd|krw)$/.test(text)) return false;
  if (/^[a-z]\\d{{5,6}}$/.test(text)) return false;
  if (/^[a-z]{{1,2}}$/.test(text) && text !== "ai") return false;
  return true;
}}
function signalTitleTokens(signal) {{
  return String(signal?.signal_key || signal?.signal_title || "")
    .split(/[·|,]/)
    .map((token) => token.trim())
    .filter(Boolean);
}}
function cleanKeywordRows(items, limit = 24) {{
  return listEntries(items).filter((row) => isUsefulDashboardKeyword(keywordLabel(row))).slice(0, limit);
}}
function cleanIssueRows(signals) {{
  return (signals || [])
    .map((signal) => ({{
      ...signal,
      top_keywords: (signal.top_keywords || []).filter(isUsefulDashboardKeyword).slice(0, 8),
    }}))
    .filter((signal) => {{
      if (signal.signal_type === "article_match" || signal.signal_type === "url_burst") return true;
      const titleTokens = signalTitleTokens(signal);
      const usefulTitleTokens = titleTokens.filter(isUsefulDashboardKeyword);
      return usefulTitleTokens.length >= 2 || (usefulTitleTokens.length >= 1 && (signal.top_keywords || []).length >= 2);
    }});
}}
function signalKeywordText(signal) {{
  return (signal.top_keywords || []).filter(isUsefulDashboardKeyword).slice(0, 5).join(", ");
}}
function signalDate(value) {{
  const date = value ? new Date(value) : null;
  return date && !Number.isNaN(date.getTime()) ? date : new Date();
}}
function signalRiskFlags(signal) {{
  const flags = new Set(signal.risk_flags || []);
  if (Number(signal.related_telegram_channels_count || 0) <= 1 && Number(signal.related_telegram_count || 0) >= 5) flags.add("single_channel_spike");
  return [...flags].sort();
}}
function signalLifecycle(signal) {{
  const now = new Date();
  const first = signalDate(signal.first_seen_at);
  const latest = signalDate(signal.latest_seen_at || signal.first_seen_at);
  const firstAge = Math.max(0, (now - first) / 36e5);
  const latestAge = Math.max(0, (now - latest) / 36e5);
  const count = Number(signal.related_telegram_count || 0);
  const channels = Number(signal.related_telegram_channels_count || 0);
  if (firstAge <= 24 && latestAge <= 8) return "new";
  if (latestAge <= 12 && (channels >= 2 || count >= 5)) return "rising";
  if (latestAge <= 36) return "active";
  if (latestAge <= 96) return "fading";
  return "stale";
}}
function signalScore(signal) {{
  const now = new Date();
  const latest = signalDate(signal.latest_seen_at || signal.first_seen_at);
  const latestAge = Math.max(0, (now - latest) / 36e5);
  const freshness = latestAge <= 6 ? 14 : latestAge <= 24 ? 10 : latestAge <= 72 ? 5 : 0;
  let score = Math.min(26, Number(signal.related_telegram_count || 0) * 4)
    + Math.min(30, Number(signal.related_telegram_channels_count || 0) * 10)
    + Math.min(22, Math.round(Number(signal.confidence_score || 0) * 22))
    + freshness;
  const flags = new Set(signalRiskFlags(signal));
  if (flags.has("promotional")) score -= 14;
  if (flags.has("rumor") || flags.has("unverified")) score -= 8;
  if (flags.has("single_channel_spike")) score -= 10;
  return Math.max(0, Math.min(100, Math.round(score)));
}}
function signalBucket(signal) {{
  const flags = new Set(signal.risk_flags || []);
  if (["rumor", "promotional", "unverified", "single_channel_spike"].some((flag) => flags.has(flag))) return "risk_watch";
  if (["topic_burst", "url_burst"].includes(signal.signal_type || "")) return "watchlist_candidate";
  return ["new", "rising"].includes(signal.lifecycle) ? "new_rising" : "confirmed_reaction";
}}
function enrichDashboardSignals(signals) {{
  return cleanIssueRows(signals).map((signal) => {{
    const risk_flags = signalRiskFlags(signal);
    const lifecycle = signal.lifecycle || signalLifecycle(signal);
    const enriched = {{...signal, risk_flags, lifecycle, signal_score: signal.signal_score || signalScore({{...signal, risk_flags, lifecycle}})}};
    return {{...enriched, analysis_bucket: signal.analysis_bucket || signalBucket(enriched)}};
  }}).sort((a, b) => Number(b.signal_score || 0) - Number(a.signal_score || 0));
}}
function signalGroups(signals) {{
  return {{
    new_rising: signals.filter((signal) => signal.analysis_bucket === "new_rising"),
    watchlist_candidate: signals.filter((signal) => signal.analysis_bucket === "watchlist_candidate"),
    risk_watch: signals.filter((signal) => signal.analysis_bucket === "risk_watch"),
    confirmed_reaction: signals.filter((signal) => signal.analysis_bucket === "confirmed_reaction"),
  }};
}}
function signalOverview(model, signals) {{
  const groups = signalGroups(signals);
  const fallback = model.signal_overview || {{}};
  return {{
    top_score: signals[0]?.signal_score || fallback.top_score || 0,
    new_rising: groups.new_rising.length,
    watchlist_candidates: groups.watchlist_candidate.length,
    risk_watch: groups.risk_watch.length,
    confirmed_reactions: groups.confirmed_reaction.length,
    velocity_ratio: fallback.velocity_ratio ?? 0,
    velocity_label: fallback.velocity_label ?? "steady",
  }};
}}
function signalCard(signal) {{
  const flags = (signal.risk_flags || []).slice(0, 4).map((flag) => `<span>${{esc(flag)}}</span>`).join("");
  const summary = signal.signal_summary || signalKeywordText(signal) || signal.signal_type || "";
  return `<article class="signal-card"><div class="signal-card__top"><b>${{esc(signal.signal_score || 0)}}</b><span>${{esc(signal.lifecycle || "")}}</span></div><h3>${{esc(compact(signal.signal_title || signal.article_id || "", 78))}}</h3><p>${{esc(compact(summary, 112))}}</p><div class="signal-card__meta"><span>${{esc(signal.related_telegram_count || 0)}}건</span><span>${{esc(signal.related_telegram_channels_count || 0)}}채널</span><span>${{esc(signal.signal_type || "")}}</span></div><div class="risk-chips">${{flags}}</div></article>`;
}}
function companyEventText(row, limit = 4) {{
  return listEntries(row.event_types || []).slice(0, limit).map((item) => keywordLabel(item)).filter(Boolean).join(", ");
}}
function companyRiskText(row, limit = 4) {{
  return (row.risk_flags || []).slice(0, limit).join(", ");
}}
function companyCard(row) {{
  const flags = (row.risk_flags || []).slice(0, 4).map((flag) => `<span>${{esc(flag)}}</span>`).join("");
  return `<article class="signal-card company-card"><div class="signal-card__top"><b>${{esc(row.signal_score || 0)}}</b><span>${{esc(row.lifecycle || "")}}</span></div><h3>${{esc(compact(row.company || "", 48))}}</h3><p>${{esc(compact(companyEventText(row) || "상장사 언급 추적", 112))}}</p><div class="signal-card__meta"><span>24h ${{esc(row.mentions_24h || 0)}}</span><span>14d ${{esc(row.mentions_14d || 0)}}</span><span>${{esc(row.channels_count || 0)}}채널</span><span>${{esc(row.velocity_ratio || 0)}}x</span></div><div class="risk-chips">${{flags}}</div></article>`;
}}
function companyOverview(model, rows) {{
  const fallback = model.company_signal_overview || {{}};
  return {{
    companies_total: fallback.companies_total ?? rows.length,
    top_score: rows[0]?.signal_score || fallback.top_score || 0,
    new_rising: fallback.new_rising ?? rows.filter((row) => row.analysis_bucket === "new_rising").length,
    risk_watch: fallback.risk_watch ?? rows.filter((row) => row.analysis_bucket === "risk_watch").length,
    tracked: fallback.tracked ?? rows.filter((row) => row.analysis_bucket === "tracked_company").length,
  }};
}}
function riskFlagRows(model, signals) {{
  const counts = new Map();
  listEntries(model.risk_flag_counts || []).forEach((row) => {{
    counts.set(keywordLabel(row), Number(keywordCount(row) || 0));
  }});
  signals.forEach((signal) => (signal.risk_flags || []).forEach((flag) => counts.set(flag, (counts.get(flag) || 0) + 1)));
  return [...counts.entries()].filter(([label]) => label).sort((a, b) => b[1] - a[1]).slice(0, 12);
}}
function modelFromApi(data) {{
  const counts = data.counts || {{}};
  const sourceModel = {{
    signal_overview: data.signal_overview || fallbackModel.signal_overview || {{}},
    risk_flag_counts: data.risk_flag_counts || fallbackModel.risk_flag_counts || [],
  }};
  const signals = enrichDashboardSignals(data.signals || fallbackModel.signals || []);
  const groups = signalGroups(signals);
  return {{
    generated_at: data.generated_at || fallbackModel.generated_at,
    channels_collectable: counts.channels_collectable ?? fallbackModel.channels_collectable,
    channels_enabled: counts.channels_enabled ?? fallbackModel.channels_enabled,
    channels_failed: counts.channels_failed ?? fallbackModel.channels_failed,
    messages_24h: counts.messages_24h ?? fallbackModel.messages_24h,
    messages_14d: counts.messages_14d ?? fallbackModel.messages_14d,
    matches_total: counts.matches_total ?? fallbackModel.matches_total,
    signals_total: counts.signals_total ?? 0,
    candidates_total: fallbackModel.candidates_total ?? 0,
    candidate_pending: fallbackModel.candidate_pending ?? 0,
    top_channels: data.top_channels || fallbackModel.top_channels || [],
    type_counts: data.type_counts || fallbackModel.type_counts || [],
    day_counts: data.day_counts || fallbackModel.day_counts || [],
    top_keywords: data.top_keywords || fallbackModel.top_keywords || [],
    signals: signals.slice(0, 12),
    company_signal_overview: data.company_signal_overview || fallbackModel.company_signal_overview || {{}},
    top_company_signals: data.top_company_signals || fallbackModel.top_company_signals || [],
    new_rising_companies: data.new_rising_companies || fallbackModel.new_rising_companies || [],
    company_risk_watch: data.company_risk_watch || fallbackModel.company_risk_watch || [],
    signal_overview: signalOverview(sourceModel, signals),
    new_rising_signals: groups.new_rising.slice(0, 8),
    watchlist_candidates: groups.watchlist_candidate.slice(0, 8),
    risk_watch_signals: groups.risk_watch.slice(0, 8),
    risk_flag_counts: riskFlagRows(sourceModel, signals),
    match_type_counts: data.match_type_counts || fallbackModel.match_type_counts || [],
    quality_bands: data.quality_bands || fallbackModel.quality_bands || [],
    growth: data.growth || fallbackModel.growth || {{}},
  }};
}}
function renderDashboard(model, sourceLabel) {{
  const growth = model.growth || {{}};
  const signals = enrichDashboardSignals(model.signals || []);
  const groups = signalGroups(signals);
  const overview = model.signal_overview || signalOverview(model, signals);
  const companyRows = model.top_company_signals || [];
  const companyStats = companyOverview(model, companyRows);
  document.getElementById("stats").innerHTML = [
    statCard("수집 가능 공개 채널", model.channels_collectable, `enabled ${{model.channels_enabled ?? 0}}`),
    statCard("최근 24시간 메시지", model.messages_24h ?? 0),
    statCard("최근 14일 메시지", model.messages_14d ?? 0),
    statCard("기사 매칭", model.matches_total ?? 0),
    statCard("시그널 채널", (model.top_channels || []).filter((row) => Number(row.matches || 0) > 0).length, "매칭 1건 이상"),
    statCard("이슈 신호", model.signals_total ?? (model.signals || []).length),
    statCard("월간 예상", `${{growth.monthly_messages ?? 0}}건`, `${{growth.monthly_mb ?? 0}} MB`),
  ].join("");
  document.getElementById("company-stats").innerHTML = [
    statCard("상장사 추적", companyStats.companies_total ?? 0, "최근 14일"),
    statCard("상위 회사 점수", companyStats.top_score ?? 0, "100점 기준"),
    statCard("상승 상장사", companyStats.new_rising ?? 0, "24h 증가"),
    statCard("리스크 확인", companyStats.risk_watch ?? 0, "루머·홍보성 등"),
    statCard("추적 중", companyStats.tracked ?? 0, "반복 언급"),
  ].join("");
  document.getElementById("new-company-rows").innerHTML = (model.new_rising_companies || companyRows.filter((row) => row.analysis_bucket === "new_rising")).slice(0, 8).map(companyCard).join("") || '<p>최근 상승 상장사 신호가 아직 없습니다.</p>';
  document.getElementById("risk-company-rows").innerHTML = (model.company_risk_watch || companyRows.filter((row) => row.analysis_bucket === "risk_watch")).slice(0, 8).map(companyCard).join("") || '<p>확인 필요 상장사 신호가 아직 없습니다.</p>';
  document.getElementById("company-rows").innerHTML = companyRows.slice(0, 16).map((row) => `<tr><td><b>${{esc(row.company || "")}}</b><br><small>${{esc(row.lifecycle || "")}}</small></td><td>${{esc(row.signal_score || 0)}}</td><td>${{esc(row.mentions_24h || 0)}} / ${{esc(row.mentions_14d || 0)}}</td><td>${{esc(row.channels_count || 0)}}</td><td>${{esc(row.velocity_ratio || 0)}}x</td><td>${{esc(companyEventText(row, 3))}}</td><td>${{esc(companyRiskText(row, 4))}}</td><td>${{esc(row.latest_at || "")}}</td></tr>`).join("") || '<tr><td colspan="8">상장사 기준으로 묶을 Telegram 신호가 아직 없습니다.</td></tr>';
  document.getElementById("signal-stats").innerHTML = [
    statCard("상위 시그널 점수", overview.top_score ?? 0, "100점 기준"),
    statCard("New/Rising", overview.new_rising ?? groups.new_rising.length, "최근 부상"),
    statCard("Watch 후보", overview.watchlist_candidates ?? groups.watchlist_candidate.length, "기사 전 단계"),
    statCard("Risk watch", overview.risk_watch ?? groups.risk_watch.length, "확인 필요"),
    statCard("24h 속도", `${{overview.velocity_ratio ?? 0}}x`, overview.velocity_label || "steady"),
  ].join("");
  document.getElementById("risk-flag-rows").innerHTML = riskFlagRows(model, signals).map(([label, count]) => `<span>${{esc(label)}} <b>${{esc(count)}}</b></span>`).join("") || '<span>위험 플래그 없음</span>';
  document.getElementById("new-rising-rows").innerHTML = (model.new_rising_signals || groups.new_rising).slice(0, 8).map(signalCard).join("") || '<p>최근 부상 신호가 아직 없습니다.</p>';
  document.getElementById("watchlist-rows").innerHTML = (model.watchlist_candidates || groups.watchlist_candidate).slice(0, 8).map(signalCard).join("") || '<p>기사 전 단계 후보가 아직 없습니다.</p>';
  document.getElementById("risk-watch-rows").innerHTML = (model.risk_watch_signals || groups.risk_watch).slice(0, 8).map(signalCard).join("") || '<p>확인 필요 신호가 아직 없습니다.</p>';
  document.getElementById("channel-rows").innerHTML = (model.top_channels || []).map((row) => `<tr><td>@${{esc(row.handle || "")}}</td><td>${{esc(compact(row.title, 42))}}</td><td>${{esc(row.signal_quality_score || row.quality_score || 0)}}<br><small>기본 ${{esc(row.quality_score || 0)}}</small></td><td>${{esc(row.messages || 0)}}</td><td>${{esc(row.matches || 0)}}<br><small>URL ${{esc(row.direct_matches || 0)}} · 추정 ${{esc(row.weak_matches || 0)}}</small></td><td>${{esc(((Number(row.match_rate || 0)) * 100).toFixed(1))}}%</td><td>${{esc(row.risk_messages || 0)}}</td><td>${{esc(row.latest_at || row.last_collected_at || "")}}</td><td>${{esc(row.last_error || "")}}</td></tr>`).join("") || '<tr><td colspan="9">수집 대상 채널이 아직 없습니다.</td></tr>';
  document.getElementById("type-rows").innerHTML = listEntries(model.type_counts).map((row) => `<li><b>${{esc(row.label ?? row[0] ?? "")}}</b><span>${{esc(row.count ?? row[1] ?? 0)}}건</span></li>`).join("") || '<li><b>데이터 없음</b><span>0건</span></li>';
  document.getElementById("keyword-rows").innerHTML = cleanKeywordRows(model.top_keywords).map((row) => `<span>${{esc(keywordLabel(row))}} <b>${{esc(keywordCount(row))}}</b></span>`).join("") || '<span>키워드 없음</span>';
  document.getElementById("match-type-rows").innerHTML = listEntries(model.match_type_counts).map((row) => `<li><b>${{esc(row.label ?? row[0] ?? "")}}</b><span>${{esc(row.count ?? row[1] ?? 0)}}건</span></li>`).join("") || '<li><b>매칭 없음</b><span>0건</span></li>';
  document.getElementById("quality-rows").innerHTML = listEntries(model.quality_bands).map((row) => `<span>${{esc(row.label ?? row[0] ?? "")}} <b>${{esc(row.count ?? row[1] ?? 0)}}</b></span>`).join("") || '<span>아직 평가 전</span>';
  const maxDay = Math.max(1, ...(model.day_counts || []).map((row) => Number(row[1] || row.count || 0)));
  document.getElementById("day-rows").innerHTML = (model.day_counts || []).map((row) => {{
    const day = row[0] ?? row.day ?? "";
    const count = Number(row[1] ?? row.count ?? 0);
    return `<div><span>${{esc(day)}}</span><b style="width:${{Math.min(100, count * 100 / maxDay).toFixed(1)}}%"></b><em>${{count}}</em></div>`;
  }}).join("") || '<p>아직 표시할 수집량이 없습니다.</p>';
  document.getElementById("signal-rows").innerHTML = signals.map((signal) => `<tr><td><b>${{esc(signal.signal_title || signal.article_id || "")}}</b><br><small>${{esc(signal.signal_summary || signal.signal_type || "")}}</small></td><td>${{esc(signal.related_telegram_count || 0)}}</td><td>${{esc(signal.related_telegram_channels_count || 0)}}</td><td>${{esc(signalKeywordText(signal))}}</td><td>${{esc((signal.risk_flags || []).slice(0, 5).join(", "))}}</td></tr>`).join("") || '<tr><td colspan="5">아직 기사와 연결된 Telegram 신호가 없습니다.</td></tr>';
  document.getElementById("data-status").innerHTML = `<b>${{esc(sourceLabel)}}</b> 생성시각 ${{esc(model.generated_at || "")}}`;
}}
async function sha256Hex(text) {{
  const bytes = new TextEncoder().encode(text);
  const digest = await crypto.subtle.digest("SHA-256", bytes);
  return Array.from(new Uint8Array(digest)).map((byte) => byte.toString(16).padStart(2, "0")).join("");
}}
function dashboardApiUrl() {{
  if (!telegramDashboardApiUrl) return "";
  const url = new URL(telegramDashboardApiUrl, window.location.href);
  url.searchParams.set("action", "telegram_dashboard");
  return url.toString();
}}
async function loadDashboard(token) {{
  renderDashboard(fallbackModel, "정적 fallback");
  if (!telegramDashboardApiUrl) return;
  try {{
    const headers = token ? {{"X-Telegram-Admin-Token": token}} : {{}};
    const response = await fetch(dashboardApiUrl(), {{cache: "no-store", headers}});
    const data = response.ok ? await response.json() : await Promise.reject(new Error(`HTTP ${{response.status}}`));
    if (!data.ok) throw new Error(data.error || "api_error");
    renderDashboard(modelFromApi(data), "DB 기준");
  }} catch (error) {{
    document.getElementById("data-status").innerHTML = `<b>정적 fallback</b> DB API 확인 실패: ${{esc(error.message)}}`;
  }}
}}
async function unlockDashboard(rawToken) {{
  const token = String(rawToken || "").trim();
  if (!telegramAdminAccessHash) {{
    document.getElementById("access-config-error").hidden = false;
    document.getElementById("access-gate").hidden = false;
    document.getElementById("dashboard-content").hidden = true;
    return;
  }}
  if (!token || await sha256Hex(token) !== telegramAdminAccessHash) {{
    document.getElementById("access-error").hidden = false;
    document.getElementById("access-gate").hidden = false;
    document.getElementById("dashboard-content").hidden = true;
    return;
  }}
  window.sessionStorage.setItem(telegramAdminStorageKey, token);
  document.getElementById("access-error").hidden = true;
  document.getElementById("access-gate").hidden = true;
  document.getElementById("dashboard-content").hidden = false;
  await loadDashboard(token);
}}
document.getElementById("access-form").addEventListener("submit", (event) => {{
  event.preventDefault();
  unlockDashboard(document.getElementById("access-token-input").value);
}});
document.getElementById("lock-dashboard").addEventListener("click", () => {{
  window.sessionStorage.removeItem(telegramAdminStorageKey);
  window.location.reload();
}});
(async () => {{
  const token = window.sessionStorage.getItem(telegramAdminStorageKey) || "";
  if (!telegramAdminAccessHash) {{
    document.getElementById("access-config-error").hidden = false;
    document.getElementById("access-token-input").disabled = true;
    document.getElementById("access-submit").disabled = true;
    document.getElementById("access-gate").hidden = false;
    document.getElementById("dashboard-content").hidden = true;
    return;
  }}
  if (token) {{
    await unlockDashboard(token);
  }} else {{
    document.getElementById("access-gate").hidden = false;
    document.getElementById("dashboard-content").hidden = true;
  }}
}})();
</script>
</body>
</html>
"""
    output_path.write_text(html, encoding="utf-8")
    return output_path


def main() -> None:
    from .config import load_config
    from .dates import now_in_timezone
    from .state import load_state

    command = sys.argv[1] if len(sys.argv) > 1 else "write"
    project_root = Path.cwd()
    config = load_config(project_root / "config.yaml")
    state = load_state(project_root / "data" / "state.json")
    now = now_in_timezone(str(config.get("timezone") or "Asia/Seoul"))
    if command != "write":
        raise SystemExit(f"unknown command: {command}")
    path = write_telegram_dashboard(project_root, state, config, now)
    print(path)


if __name__ == "__main__":
    main()
