from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable
from urllib.parse import quote, urljoin, urlsplit

import feedparser
import httpx
from bs4 import BeautifulSoup

from .config import configured_feeds
from .dates import datetime_to_iso, extract_published_datetime_from_html, parse_datetime
from .normalize import canonical_url_hash, hostname_from_url, normalize_title_parts, normalize_url


USER_AGENT = "activist-rss-curator/1.0 (+https://github.com/)"
GOOGLE_NEWS_DECODE_ENDPOINT = "https://news.google.com/_/DotsSplashUi/data/batchexecute"


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


def google_news_article_id(url: str) -> str | None:
    parsed = urlsplit(str(url or ""))
    if parsed.hostname != "news.google.com":
        return None
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 2 and parts[-2] in {"articles", "read"}:
        return parts[-1]
    return None


def google_news_decoding_params(html_text: str) -> tuple[str, str] | None:
    soup = BeautifulSoup(html_text or "", "html.parser")
    element = soup.find(attrs={"data-n-a-sg": True, "data-n-a-ts": True})
    if not element:
        return None
    signature = str(element.get("data-n-a-sg") or "")
    timestamp = str(element.get("data-n-a-ts") or "")
    if not signature or not timestamp:
        return None
    return signature, timestamp


def parse_google_news_batch_response(text: str) -> str | None:
    try:
        payload_text = text.split("\n\n", 1)[1]
        payload = json.loads(payload_text)
        decoded_payload = json.loads(payload[0][2])
    except (IndexError, TypeError, ValueError, json.JSONDecodeError):
        return None
    if isinstance(decoded_payload, list) and len(decoded_payload) >= 2:
        decoded_url = decoded_payload[1]
        if isinstance(decoded_url, str) and decoded_url.startswith(("http://", "https://")):
            return decoded_url
    return None


def decode_google_news_url_online(url: str, client: httpx.Client) -> str | None:
    article_id = google_news_article_id(url)
    if not article_id:
        return None

    params: tuple[str, str] | None = None
    for prefix in ("https://news.google.com/articles/", "https://news.google.com/rss/articles/"):
        try:
            response = client.get(prefix + article_id, follow_redirects=True)
            response.raise_for_status()
        except httpx.HTTPError:
            continue
        params = google_news_decoding_params(response.text)
        if params:
            break
    if not params:
        return None

    signature, timestamp = params
    request_payload = [
        "Fbv4je",
        (
            '["garturlreq",[["X","X",["X","X"],null,null,1,1,"US:en",null,1,'
            'null,null,null,null,null,0,1],"X","X",1,[1,1,1],1,1,null,0,0,'
            f'null,0],"{article_id}",{timestamp},"{signature}"]'
        ),
    ]
    headers = {
        "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
        "User-Agent": USER_AGENT,
    }
    try:
        response = client.post(
            GOOGLE_NEWS_DECODE_ENDPOINT,
            content=f"f.req={quote(json.dumps([[request_payload]], separators=(',', ':')))}",
            headers=headers,
        )
        response.raise_for_status()
    except httpx.HTTPError:
        return None
    return parse_google_news_batch_response(response.text)


def enrich_article(
    article: dict[str, object],
    client: httpx.Client,
    config: dict[str, object],
    *,
    decode_google_news: bool = True,
) -> dict[str, object]:
    enriched = dict(article)
    url = str(article.get("canonical_url") or article.get("link") or "")
    if not url:
        return enriched

    decoded_google_news_url = decode_google_news_url_online(url, client) if decode_google_news else None
    if decoded_google_news_url:
        normalized_decoded = normalize_url(decoded_google_news_url)
        enriched["canonical_url"] = normalized_decoded
        enriched["canonical_url_hash"] = canonical_url_hash(normalized_decoded)
        url = normalized_decoded

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

    page_timeout = float(fetch_config.get("page_timeout_seconds", 8.0) or 8.0)  # type: ignore[union-attr]
    max_enrich_articles = int(fetch_config.get("max_enrich_articles", 0) or 0)  # type: ignore[union-attr]
    google_news_decode_limit = int(fetch_config.get("google_news_decode_limit", 25) or 0)  # type: ignore[union-attr]
    timeout = httpx.Timeout(page_timeout, connect=min(5.0, page_timeout))
    limits = httpx.Limits(max_connections=5, max_keepalive_connections=2)
    headers = {"User-Agent": USER_AGENT}
    enriched_articles: list[dict[str, object]] = []
    enrich_attempts = 0
    google_news_decode_attempts = 0
    with httpx.Client(timeout=timeout, limits=limits, headers=headers) as client:
        for article in articles:
            if max_enrich_articles > 0 and enrich_attempts >= max_enrich_articles:
                enriched_articles.append(article)
                continue
            enrich_attempts += 1
            url = str(article.get("canonical_url") or article.get("link") or "")
            is_google_news = bool(google_news_article_id(url))
            should_decode_google_news = is_google_news and (
                google_news_decode_limit < 0 or google_news_decode_attempts < google_news_decode_limit
            )
            if should_decode_google_news:
                google_news_decode_attempts += 1
            enriched_articles.append(
                enrich_article(
                    article,
                    client,
                    config,
                    decode_google_news=should_decode_google_news,
                )
            )
    return enriched_articles


def parse_feed_file(path: str | Path, config: dict[str, object]) -> list[dict[str, object]]:
    return parse_feed(Path(path).read_text(encoding="utf-8"), config)
