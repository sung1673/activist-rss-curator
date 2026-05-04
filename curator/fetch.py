from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
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


def fetch_config_int(fetch_config: object, key: str, default: int) -> int:
    if not isinstance(fetch_config, dict):
        return default
    value = fetch_config.get(key, default)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def fetch_config_float(fetch_config: object, key: str, default: float) -> float:
    if not isinstance(fetch_config, dict):
        return default
    value = fetch_config.get(key, default)
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


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


def image_url_from_entry(entry: object, base_url: str) -> str | None:
    for attr in ("media_thumbnail", "media_content", "links"):
        value = getattr(entry, attr, None)
        if isinstance(value, list):
            for item in value:
                if not isinstance(item, dict):
                    continue
                url = item.get("url") or item.get("href")
                media_type = str(item.get("type") or "")
                rel = str(item.get("rel") or "")
                medium = str(item.get("medium") or "")
                if not url:
                    continue
                if attr == "media_thumbnail":
                    return urljoin(base_url, str(url))
                if "image" in media_type or "thumbnail" in rel or medium == "image":
                    return urljoin(base_url, str(url))
    return None


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
        "image_url": image_url_from_entry(entry, normalized_link),
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


def fetch_feed_articles(
    feed_meta: dict[str, str],
    config: dict[str, object],
    *,
    max_entries: int,
    timeout: float,
) -> list[dict[str, object]]:
    xml_text = fetch_feed_xml(feed_meta["url"], timeout=timeout)
    feed_articles = parse_feed(xml_text, config, feed_meta)
    if max_entries > 0:
        return feed_articles[:max_entries]
    return feed_articles


def fetch_all_feed_articles(config: dict[str, object], fetch_config: object) -> list[dict[str, object]]:
    feeds = configured_feeds(config)
    if not feeds:
        return []

    max_entries = fetch_config_int(fetch_config, "max_entries_per_feed", 0)
    feed_timeout = fetch_config_float(fetch_config, "feed_timeout_seconds", 20.0)
    workers = max(1, fetch_config_int(fetch_config, "feed_fetch_workers", 1))

    if workers <= 1 or len(feeds) <= 1:
        articles: list[dict[str, object]] = []
        for feed_meta in feeds:
            try:
                articles.extend(fetch_feed_articles(feed_meta, config, max_entries=max_entries, timeout=feed_timeout))
            except httpx.HTTPError:
                continue
        return articles

    results: list[list[dict[str, object]]] = [[] for _ in feeds]
    with ThreadPoolExecutor(max_workers=min(workers, len(feeds))) as executor:
        future_map = {
            executor.submit(fetch_feed_articles, feed_meta, config, max_entries=max_entries, timeout=feed_timeout): index
            for index, feed_meta in enumerate(feeds)
        }
        for future in as_completed(future_map):
            index = future_map[future]
            try:
                results[index] = future.result()
            except httpx.HTTPError:
                results[index] = []
    return [article for batch in results for article in batch]


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


def image_href(html_text: str, base_url: str) -> str | None:
    soup = BeautifulSoup(html_text or "", "html.parser")
    meta_candidates = (
        {"property": "og:image"},
        {"property": "og:image:url"},
        {"property": "og:image:secure_url"},
        {"name": "twitter:image"},
        {"name": "twitter:image:src"},
        {"name": "twitter:image:url"},
        {"name": "thumbnail"},
        {"itemprop": "image"},
    )
    for attrs in meta_candidates:
        tag = soup.find("meta", attrs=attrs)
        content = tag.get("content") if tag else None
        if content and usable_image_url(str(content)):
            return urljoin(base_url, str(content))
    image_link = soup.find("link", rel=lambda value: value and "image_src" in value)
    href = image_link.get("href") if image_link else None
    if href and usable_image_url(str(href)):
        return urljoin(base_url, str(href))
    json_image = image_href_from_json_ld(soup, base_url)
    if json_image:
        return json_image
    for tag in soup.find_all("img"):
        for attr in ("src", "data-src", "data-original", "data-lazy-src", "data-url"):
            src = tag.get(attr)
            if src and usable_image_url(str(src)) and image_tag_is_large_enough(tag):
                return urljoin(base_url, str(src))
    return None


def usable_image_url(value: str) -> bool:
    lowered = value.strip().casefold()
    if not lowered or lowered.startswith(("data:", "blob:", "javascript:")):
        return False
    parsed = urlsplit(value.strip())
    if parsed.scheme in {"http", "https"} and not parsed.path.strip("/"):
        return False
    generic_tokens = (
        "logo",
        "icon",
        "sprite",
        "blank",
        "spacer",
        "profile_default",
        "default_image",
        "noimage",
        "facebook_",
        "facebook-",
        "go_share",
        "/image/isw",
        "ic_mai",
        "search_pn",
        "_next/static/media",
        "thumb_400x226",
    )
    if any(token in lowered for token in generic_tokens):
        return False
    if lowered.endswith((".svg", ".gif")):
        return False
    return True


def image_tag_is_large_enough(tag: object) -> bool:
    def numeric_attr(name: str) -> int:
        try:
            return int(str(tag.get(name) or "0").replace("px", "").strip())  # type: ignore[attr-defined]
        except ValueError:
            return 0

    width = numeric_attr("width")
    height = numeric_attr("height")
    return not ((width and width < 120) or (height and height < 80))


def image_href_from_json_ld(soup: BeautifulSoup, base_url: str) -> str | None:
    def image_from_value(value: object) -> str | None:
        if isinstance(value, str) and usable_image_url(value):
            return urljoin(base_url, value)
        if isinstance(value, list):
            for item in value:
                result = image_from_value(item)
                if result:
                    return result
        if isinstance(value, dict):
            for key in ("url", "contentUrl", "@id"):
                result = image_from_value(value.get(key))
                if result:
                    return result
        return None

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            payload = json.loads(script.string or script.get_text() or "")
        except json.JSONDecodeError:
            continue
        candidates = payload if isinstance(payload, list) else [payload]
        for candidate in candidates:
            if isinstance(candidate, dict):
                result = image_from_value(candidate.get("image") or candidate.get("thumbnailUrl"))
                if result:
                    return result
    return None


def clean_page_source_name(value: object, base_url: str) -> str | None:
    text = " ".join(str(value or "").split()).strip()
    if not text:
        return None
    hostname = (urlsplit(base_url).hostname or "").lower().removeprefix("www.")
    if hostname == "v.daum.net" and "|" in text:
        parts = [part.strip() for part in text.split("|") if part.strip()]
        if parts and parts[0].casefold() == "daum" and len(parts) >= 2:
            text = parts[-1]
    generic_names = {
        "",
        "daum",
        "daum 뉴스",
        "daum news",
        "뉴스",
        "news",
        hostname,
    }
    if text.casefold() in generic_names:
        return None
    return text


def source_from_html(html_text: str, base_url: str) -> str | None:
    soup = BeautifulSoup(html_text or "", "html.parser")
    meta_candidates = (
        {"property": "og:site_name"},
        {"property": "article:publisher"},
        {"name": "publisher"},
        {"name": "dc.publisher"},
        {"name": "author"},
    )
    for attrs in meta_candidates:
        tag = soup.find("meta", attrs=attrs)
        content = tag.get("content") if tag else None
        source = clean_page_source_name(content, base_url)
        if source:
            return source
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


def apply_decoded_google_news_url(article: dict[str, object], decoded_url: str | None) -> dict[str, object]:
    if not decoded_url:
        return article
    normalized_decoded = normalize_url(decoded_url)
    enriched = dict(article)
    enriched["canonical_url"] = normalized_decoded
    enriched["canonical_url_hash"] = canonical_url_hash(normalized_decoded)
    return enriched


def decode_google_news_links_in_state(state: dict[str, object], config: dict[str, object]) -> int:
    fetch_config = config.get("fetch", {})
    limit = int(fetch_config.get("state_google_news_decode_limit", 60) or 0)  # type: ignore[union-attr]
    if limit == 0:
        return 0

    page_timeout = float(fetch_config.get("page_timeout_seconds", 8.0) or 8.0)  # type: ignore[union-attr]
    timeout = httpx.Timeout(page_timeout, connect=min(5.0, page_timeout))
    headers = {"User-Agent": USER_AGENT}
    decoded_count = 0
    attempted = 0
    clusters = list(state.get("pending_clusters", [])) + list(state.get("published_clusters", []))[-50:]
    with httpx.Client(timeout=timeout, headers=headers) as client:
        for cluster in clusters:
            for article in list(cluster.get("articles", [])):
                url = str(article.get("canonical_url") or article.get("link") or "")
                if not google_news_article_id(url):
                    continue
                if limit > 0 and attempted >= limit:
                    return decoded_count
                attempted += 1
                decoded = decode_google_news_url_online(url, client)
                if not decoded:
                    continue
                updated = apply_decoded_google_news_url(article, decoded)
                article.update(updated)
                decoded_count += 1
    return decoded_count


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
    enriched = apply_decoded_google_news_url(enriched, decoded_google_news_url)
    if decoded_google_news_url:
        url = str(enriched.get("canonical_url") or url)

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
    source = source_from_html(html_text, final_url)
    image = image_href(html_text, final_url)
    enriched["canonical_url"] = normalized_canonical
    enriched["canonical_url_hash"] = canonical_url_hash(normalized_canonical)
    enriched["article_published_at"] = datetime_to_iso(article_published)
    if source:
        enriched["source"] = source
    if image:
        enriched["image_url"] = normalize_url(image)
    return enriched


def enrichment_jobs(
    articles: list[dict[str, object]],
    *,
    max_enrich_articles: int,
    google_news_decode_limit: int,
) -> list[tuple[int, dict[str, object], bool, bool]]:
    jobs: list[tuple[int, dict[str, object], bool, bool]] = []
    google_news_decode_attempts = 0
    for index, article in enumerate(articles):
        url = str(article.get("canonical_url") or article.get("link") or "")
        is_google_news = bool(google_news_article_id(url))
        should_decode_google_news = is_google_news and (
            google_news_decode_limit < 0 or google_news_decode_attempts < google_news_decode_limit
        )
        if should_decode_google_news:
            google_news_decode_attempts += 1
        should_enrich = not (max_enrich_articles > 0 and index + 1 > max_enrich_articles)
        jobs.append((index, article, should_decode_google_news, should_enrich))
    return jobs


def enrich_article_job(
    job: tuple[int, dict[str, object], bool, bool],
    config: dict[str, object],
    *,
    timeout: httpx.Timeout,
    limits: httpx.Limits,
    headers: dict[str, str],
) -> tuple[int, dict[str, object]]:
    index, article, should_decode_google_news, should_enrich = job
    if not should_decode_google_news and not should_enrich:
        return index, article

    decoded_article = article
    with httpx.Client(timeout=timeout, limits=limits, headers=headers) as client:
        if should_decode_google_news:
            url = str(article.get("canonical_url") or article.get("link") or "")
            decoded_article = apply_decoded_google_news_url(
                article,
                decode_google_news_url_online(url, client),
            )
        if should_enrich:
            decoded_article = enrich_article(
                decoded_article,
                client,
                config,
                decode_google_news=False,
            )
    return index, decoded_article


def fetch_google_alerts_articles(config: dict[str, object]) -> list[dict[str, object]]:
    fetch_config = config.get("fetch", {})
    articles = fetch_all_feed_articles(config, fetch_config)

    if not bool(fetch_config.get("enrich_pages", True)):  # type: ignore[union-attr]
        return articles

    page_timeout = fetch_config_float(fetch_config, "page_timeout_seconds", 8.0)
    max_enrich_articles = fetch_config_int(fetch_config, "max_enrich_articles", 0)
    google_news_decode_limit = fetch_config_int(fetch_config, "google_news_decode_limit", 25)
    enrich_workers = max(1, fetch_config_int(fetch_config, "enrich_workers", 1))
    timeout = httpx.Timeout(page_timeout, connect=min(5.0, page_timeout))
    limits = httpx.Limits(max_connections=max(5, enrich_workers), max_keepalive_connections=max(2, enrich_workers // 2))
    headers = {"User-Agent": USER_AGENT}
    jobs = enrichment_jobs(
        articles,
        max_enrich_articles=max_enrich_articles,
        google_news_decode_limit=google_news_decode_limit,
    )

    if enrich_workers > 1 and len(jobs) > 1:
        results: list[dict[str, object] | None] = [None for _ in jobs]
        with ThreadPoolExecutor(max_workers=min(enrich_workers, len(jobs))) as executor:
            future_map = {
                executor.submit(enrich_article_job, job, config, timeout=timeout, limits=limits, headers=headers): job[0]
                for job in jobs
            }
            for future in as_completed(future_map):
                index = future_map[future]
                try:
                    result_index, result_article = future.result()
                    results[result_index] = result_article
                except httpx.HTTPError:
                    results[index] = articles[index]
        return [article for article in results if article is not None]

    enriched_articles: list[dict[str, object]] = []
    with httpx.Client(timeout=timeout, limits=limits, headers=headers) as client:
        for _index, article, should_decode_google_news, should_enrich in jobs:
            decoded_article = article
            if should_decode_google_news:
                url = str(article.get("canonical_url") or article.get("link") or "")
                decoded_article = apply_decoded_google_news_url(
                    article,
                    decode_google_news_url_online(url, client),
                )
            if not should_enrich:
                enriched_articles.append(decoded_article)
                continue
            enriched_articles.append(
                enrich_article(
                    decoded_article,
                    client,
                    config,
                    decode_google_news=False,
                )
            )
    return enriched_articles


def parse_feed_file(path: str | Path, config: dict[str, object]) -> list[dict[str, object]]:
    return parse_feed(Path(path).read_text(encoding="utf-8"), config)
