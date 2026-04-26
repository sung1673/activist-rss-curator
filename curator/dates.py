from __future__ import annotations

import json
from datetime import datetime, timedelta
from email.utils import format_datetime
from typing import Any
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
from dateutil import parser


TZINFOS = {"KST": 9 * 60 * 60}


def get_timezone(timezone_name: str = "Asia/Seoul") -> ZoneInfo:
    return ZoneInfo(timezone_name or "Asia/Seoul")


def now_in_timezone(timezone_name: str = "Asia/Seoul") -> datetime:
    return datetime.now(get_timezone(timezone_name))


def parse_datetime(value: Any, timezone_name: str = "Asia/Seoul") -> datetime | None:
    if not value:
        return None

    tz = get_timezone(timezone_name)
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = parser.parse(str(value), tzinfos=TZINFOS)
        except (TypeError, ValueError, OverflowError):
            return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    return parsed.astimezone(tz)


def datetime_to_iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def parse_iso_datetime(value: str | None, timezone_name: str = "Asia/Seoul") -> datetime | None:
    return parse_datetime(value, timezone_name)


def find_jsonld_date(value: Any) -> str | None:
    if isinstance(value, dict):
        for key in ("datePublished", "dateCreated", "uploadDate"):
            if value.get(key):
                return str(value[key])
        for child in value.values():
            found = find_jsonld_date(child)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = find_jsonld_date(child)
            if found:
                return found
    return None


def extract_published_datetime_from_html(html_text: str, timezone_name: str = "Asia/Seoul") -> datetime | None:
    soup = BeautifulSoup(html_text or "", "html.parser")
    meta_selectors = [
        {"property": "article:published_time"},
        {"name": "pubdate"},
        {"name": "date"},
    ]
    for attrs in meta_selectors:
        tag = soup.find("meta", attrs=attrs)
        content = tag.get("content") if tag else None
        parsed = parse_datetime(content, timezone_name)
        if parsed:
            return parsed

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        if not script.string:
            continue
        try:
            payload = json.loads(script.string)
        except json.JSONDecodeError:
            continue
        parsed = parse_datetime(find_jsonld_date(payload), timezone_name)
        if parsed:
            return parsed
    return None


def choose_publication_datetime(
    article_published_at: datetime | str | None,
    feed_published_at: datetime | str | None,
    feed_updated_at: datetime | str | None = None,
    timezone_name: str = "Asia/Seoul",
) -> tuple[datetime | None, str]:
    article_dt = parse_datetime(article_published_at, timezone_name)
    if article_dt:
        return article_dt, "article"
    feed_dt = parse_datetime(feed_published_at, timezone_name) or parse_datetime(feed_updated_at, timezone_name)
    if feed_dt:
        return feed_dt, "feed"
    return None, "unknown"


def is_too_old(value: datetime | None, now: datetime, max_age_days: int) -> bool:
    if value is None:
        return False
    return value < now - timedelta(days=max_age_days)


def hours_between(left: datetime, right: datetime) -> float:
    return abs((left - right).total_seconds()) / 3600


def format_rfc822(value: datetime) -> str:
    return format_datetime(value)


def format_kst(value: datetime | str | None, timezone_name: str = "Asia/Seoul") -> str:
    parsed = parse_datetime(value, timezone_name)
    if not parsed:
        return "날짜 미상"
    return parsed.strftime("%Y-%m-%d %H:%M KST")
