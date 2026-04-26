from __future__ import annotations

from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import feedparser
import httpx
from bs4 import BeautifulSoup

from .config import configured_feeds
from .dates import datetime_to_iso, extract_published_datetime_from_html, parse_datetime
from .normalize import canonical_url_hash, hostname_from_url, normalize_title_parts, normalize_url


USER_AGENT = "activist-rss-curator/1.0 (+https://github.com/)"


def fetch_feed_xml(feed_url: str, timeout: float = 20.0) -> str:
    response = httpx.get(feed_url, timeout=timeout, follow_redirects=True, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    return response.text


def source_from_entry(entry: object, title_parts: dict[str, object], link: str) -> str:
    source = getattr(entry, "source", None)
    if isinstance(source, dict) and source.get("title"):
        return str(source.get("title"))
    suffix = title_parts.get("source_suffix")
    if suffix:
        return str(suffix)
    return hostname_from_url(link).removeprefix("www.")


def summary_text(summary_html: str) -> str:
    return BeautifulSoup(summary_html or "", "html.parser").get_text(" ", strip=True)


def article_from_entry(
    entry: object,
    config: dict[str, object],
    feed_meta: dict[str, str] | None = None,
) -> dict[str, object]:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    feed_meta = feed_meta or {}
    raw_title = str(getattr(entry, "title", "") or "")
    link = str(getattr(entry, "link", "") or "")
    normalized_link = normalize_url(link)
    title_parts = normalize_title_parts(raw_title)
    feed_published_at = parse_datetime(getattr(entry, "published", None), timezone_name)
    feed_updated_at = parse_datetime(getattr(entry, "updated", None), timezone_name)
    summary = summary_text(str(getattr(entry, "summary", "") or ""))
    canonical = normalized_link
    article = {
        "title": raw_title,
        "clean_title": title_parts["clean_title"],
        "normalized_title": title_parts["normalized_title"],
        "prefixes": title_parts["prefixes"],
        "source": source_from_entry(entry, title_parts, normalized_link),
        "link": link,
        "canonical_url": canonical,
        "canonical_url_hash": canonical_url_hash(canonical),
        "title_hash": title_parts["title_hash"],
        "summary": summary,
        "feed_published_at": datetime_to_iso(feed_published_at),
        "feed_updated_at": datetime_to_iso(feed_updated_at),
        "article_published_at": None,
        "feed_name": feed_meta.get("name", ""),
        "feed_category": feed_meta.get("category", ""),
    }
    return article


def parse_feed(
    xml_text: str,
    config: dict[str, object],
    feed_meta: dict[str, str] | None = None,
) -> list[dict[str, object]]:
    parsed = feedparser.parse(xml_text)
    return [article_from_entry(entry, config, feed_meta) for entry in parsed.entries]


def canonical_href(html_text: str, base_url: str) -> str | None:
    soup = BeautifulSoup(html_text or "", "html.parser")
    link = soup.find("link", rel=lambda value: value and "canonical" in value)
    href = link.get("href") if link else None
    if href:
        return urljoin(base_url, str(href))
    og_url = soup.find("meta", attrs={"property": "og:url"})
    content = og_url.get("content") if og_url else None
    if content:
        return urljoin(base_url, str(content))
    return None


def enrich_article(article: dict[str, object], client: httpx.Client, config: dict[str, object]) -> dict[str, object]:
    enriched = dict(article)
    url = str(article.get("canonical_url") or article.get("link") or "")
    if not url:
        return enriched

    try:
        response = client.get(url, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError:
        return enriched

    final_url = str(response.url)
    html_text = response.text
    canonical = canonical_href(html_text, final_url) or final_url
    normalized_canonical = normalize_url(canonical)
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    article_published = extract_published_datetime_from_html(html_text, timezone_name)
    enriched["canonical_url"] = normalized_canonical
    enriched["canonical_url_hash"] = canonical_url_hash(normalized_canonical)
    enriched["article_published_at"] = datetime_to_iso(article_published)
    return enriched


def fetch_google_alerts_articles(config: dict[str, object]) -> list[dict[str, object]]:
    articles: list[dict[str, object]] = []
    fetch_config = config.get("fetch", {})
    max_entries = int(fetch_config.get("max_entries_per_feed", 0) or 0)  # type: ignore[union-attr]
    for feed_meta in configured_feeds(config):
        try:
            xml_text = fetch_feed_xml(feed_meta["url"])
        except httpx.HTTPError:
            continue
        feed_articles = parse_feed(xml_text, config, feed_meta)
        if max_entries > 0:
            feed_articles = feed_articles[:max_entries]
        articles.extend(feed_articles)

    if not bool(fetch_config.get("enrich_pages", True)):  # type: ignore[union-attr]
        return articles

    timeout = httpx.Timeout(10.0, connect=5.0)
    limits = httpx.Limits(max_connections=5, max_keepalive_connections=2)
    headers = {"User-Agent": USER_AGENT}
    enriched_articles: list[dict[str, object]] = []
    with httpx.Client(timeout=timeout, limits=limits, headers=headers) as client:
        for article in articles:
            enriched_articles.append(enrich_article(article, client, config))
    return enriched_articles


def parse_feed_file(path: str | Path, config: dict[str, object]) -> list[dict[str, object]]:
    return parse_feed(Path(path).read_text(encoding="utf-8"), config)
