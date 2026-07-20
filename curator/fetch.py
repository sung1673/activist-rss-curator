from __future__ import annotations

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlsplit

import feedparser
import httpx
from bs4 import BeautifulSoup

from .config import configured_feeds
from .dates import datetime_to_iso, extract_published_datetime_from_html, parse_datetime
from .normalize import canonical_url_hash, hostname_from_url, normalize_title_parts, normalize_url
from .relevance import relevance_details


USER_AGENT = "activist-rss-curator/1.0 (+https://github.com/)"
GOOGLE_NEWS_DECODE_ENDPOINT = "https://news.google.com/_/DotsSplashUi/data/batchexecute"
GOOGLE_NEWS_HOST = "news.google.com"
TITLE_MATCH_TOKEN_PATTERN = re.compile(r"[0-9A-Za-z가-힣]{2,}")
TITLE_MATCH_STOPWORDS = {
    "news",
    "google",
    "뉴스",
    "기사",
    "관련",
    "보도",
    "단독",
    "종합",
    "기획",
    "속보",
    "오늘",
    "내일",
    "이번",
}


@dataclass(frozen=True)
class GoogleNewsDecodeResult:
    decoded_url: str | None = None
    rate_limited: bool = False
    error: str = ""


@dataclass(frozen=True)
class OriginalUrlMatch:
    article: dict[str, object]
    score: int
    title_score: int
    overlap: int
    reason: str


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


def block_unresolved_google_news(config: dict[str, object]) -> bool:
    fetch_config = config.get("fetch", {})
    if not isinstance(fetch_config, dict):
        return True
    return bool(fetch_config.get("google_news_block_unresolved", True))


def is_google_news_url(url: object) -> bool:
    parsed = urlsplit(str(url or ""))
    return (parsed.hostname or "").casefold() == GOOGLE_NEWS_HOST


def source_kind_for_url(url: object, feed_meta: dict[str, str] | None = None) -> str:
    if is_google_news_url(url):
        return "google_discovery"
    category = str((feed_meta or {}).get("category") or "").casefold()
    if category == "telegram_reference":
        return "telegram_reference"
    if category in {"official", "disclosure", "dart", "krx"}:
        return "official"
    return "direct"


def clean_source_key(value: object) -> str:
    return re.sub(r"[^0-9a-z가-힣]+", "", str(value or "").casefold())


def article_url(article: dict[str, object]) -> str:
    return str(article.get("canonical_url") or article.get("link") or "")


def article_source_domain(article: dict[str, object]) -> str:
    return (urlsplit(article_url(article)).hostname or "").casefold().removeprefix("www.")


def source_registry_entries(config: dict[str, object]) -> list[dict[str, object]]:
    registry = config.get("source_registry", {})
    if not isinstance(registry, dict) or not registry.get("enabled", True):
        return []
    entries = registry.get("sources", [])
    return [entry for entry in entries if isinstance(entry, dict)]


def source_registry_domains_for_label(label: object, config: dict[str, object]) -> set[str]:
    label_key = clean_source_key(label)
    if not label_key:
        return set()
    domains: set[str] = set()
    for entry in source_registry_entries(config):
        names = [entry.get("name")]
        aliases = entry.get("aliases")
        if isinstance(aliases, list):
            names.extend(aliases)
        if label_key not in {clean_source_key(name) for name in names if name}:
            continue
        raw_domains = entry.get("domains")
        if isinstance(raw_domains, list):
            domains.update(str(domain).casefold().removeprefix("www.") for domain in raw_domains if domain)
    return domains


def source_matches(article: dict[str, object], candidate: dict[str, object], config: dict[str, object]) -> bool:
    article_source = clean_source_key(article.get("source"))
    candidate_source = clean_source_key(candidate.get("source"))
    if article_source and candidate_source and article_source == candidate_source:
        return True
    candidate_domain = article_source_domain(candidate)
    if not candidate_domain:
        return False
    allowed_domains = source_registry_domains_for_label(article.get("source"), config)
    return any(candidate_domain == domain or candidate_domain.endswith(f".{domain}") for domain in allowed_domains)


def title_match_tokens(value: object) -> set[str]:
    return {
        token.casefold()
        for token in TITLE_MATCH_TOKEN_PATTERN.findall(str(value or ""))
        if token.casefold() not in TITLE_MATCH_STOPWORDS
    }


def title_similarity_score(left: object, right: object) -> int:
    left_text = " ".join(str(left or "").split()).casefold()
    right_text = " ".join(str(right or "").split()).casefold()
    if not left_text or not right_text:
        return 0
    return int(round(SequenceMatcher(None, left_text, right_text).ratio() * 100))


def article_resolution_datetime(article: dict[str, object], timezone_name: str):
    for key in ("article_published_at", "feed_published_at", "published_at", "feed_updated_at", "seen_at"):
        parsed = parse_datetime(article.get(key), timezone_name)
        if parsed:
            return parsed
    return None


def dates_are_near(
    article: dict[str, object],
    candidate: dict[str, object],
    config: dict[str, object],
) -> bool:
    fetch_config = config.get("fetch", {})
    window_days = fetch_config_int(fetch_config, "google_news_title_match_window_days", 7)
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    left_dt = article_resolution_datetime(article, timezone_name)
    right_dt = article_resolution_datetime(candidate, timezone_name)
    if not left_dt or not right_dt:
        return True
    return abs((left_dt - right_dt).total_seconds()) <= window_days * 86400


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
    candidates = image_urls_from_entry(entry, base_url)
    return candidates[0] if candidates else None


def image_urls_from_entry(entry: object, base_url: str) -> list[str]:
    candidates: list[str] = []
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
                image_url = urljoin(base_url, str(url))
                if attr == "media_thumbnail":
                    if usable_image_url(image_url) and image_url not in candidates:
                        candidates.append(image_url)
                    continue
                if "image" in media_type or "thumbnail" in rel or medium == "image":
                    if usable_image_url(image_url) and image_url not in candidates:
                        candidates.append(image_url)
    return candidates


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
    image_candidates = image_urls_from_entry(entry, normalized_link)
    source_kind = source_kind_for_url(normalized_link, feed_meta)
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
        "image_url": image_candidates[0] if image_candidates else None,
        "image_candidates": image_candidates,
        "feed_published_at": datetime_to_iso(feed_published_at),
        "feed_updated_at": datetime_to_iso(feed_updated_at),
        "article_published_at": None,
        "feed_name": feed_meta.get("name", ""),
        "feed_category": feed_meta.get("category", ""),
        "feed_scope": feed_meta.get("scope", ""),
        "source_kind": source_kind,
        "original_resolution_status": "unresolved" if source_kind == "google_discovery" else "direct",
    }
    if source_kind == "google_discovery":
        article["google_news_url"] = normalized_link
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
    candidates = image_hrefs(html_text, base_url)
    return candidates[0] if candidates else None


def append_image_candidate(candidates: list[str], value: str, base_url: str) -> None:
    image_url = urljoin(base_url, value)
    if usable_image_url(image_url) and image_url not in candidates:
        candidates.append(image_url)


def image_hrefs(html_text: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html_text or "", "html.parser")
    candidates: list[str] = []
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
            append_image_candidate(candidates, str(content), base_url)
    image_link = soup.find("link", rel=lambda value: value and "image_src" in value)
    href = image_link.get("href") if image_link else None
    if href and usable_image_url(str(href)):
        append_image_candidate(candidates, str(href), base_url)
    for json_image in image_hrefs_from_json_ld(soup, base_url):
        if json_image not in candidates:
            candidates.append(json_image)
    for tag in soup.find_all("img"):
        for attr in ("src", "data-src", "data-original", "data-lazy-src", "data-url"):
            src = tag.get(attr)
            if src and usable_image_url(str(src)) and image_tag_is_large_enough(tag):
                append_image_candidate(candidates, str(src), base_url)
                break
    return candidates


def usable_image_url(value: str) -> bool:
    lowered = value.strip().casefold()
    if not lowered or lowered.startswith(("data:", "blob:", "javascript:")):
        return False
    parsed = urlsplit(value.strip())
    if parsed.scheme in {"http", "https"} and not parsed.path.strip("/"):
        return False
    generic_tokens = (
        "j6_cofbog",
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
        "favicon",
        "favicons",
        "/image/isw",
        "ic_mai",
        "search_pn",
        "_next/static/media",
        "thumb_400x226",
        "defaultimg",
        "/images/content/",
        "/images/aichat/",
        "/news/포토",
        "/news/이슈",
        "image.edaily.co.kr/images/photo/files/",
        "image.edaily.co.kr/images/vision/files/",
        "banner",
        "grandbanner",
        "배너",
        "최상단",
        "공모전",
        "전략포럼",
        "gaic_",
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


def image_hrefs_from_json_ld(soup: BeautifulSoup, base_url: str) -> list[str]:
    results: list[str] = []

    def collect_image_from_value(value: object) -> None:
        if isinstance(value, str) and usable_image_url(value):
            image_url = urljoin(base_url, value)
            if image_url not in results:
                results.append(image_url)
            return
        if isinstance(value, list):
            for item in value:
                collect_image_from_value(item)
        if isinstance(value, dict):
            for key in ("url", "contentUrl", "@id"):
                collect_image_from_value(value.get(key))

    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            payload = json.loads(script.string or script.get_text() or "")
        except json.JSONDecodeError:
            continue
        candidates = payload if isinstance(payload, list) else [payload]
        for candidate in candidates:
            if isinstance(candidate, dict):
                collect_image_from_value(candidate.get("image") or candidate.get("thumbnailUrl"))
    return results


def image_href_from_json_ld(soup: BeautifulSoup, base_url: str) -> str | None:
    candidates = image_hrefs_from_json_ld(soup, base_url)
    return candidates[0] if candidates else None


def image_quality_score(image_url: object) -> int:
    url = str(image_url or "").strip()
    if not usable_image_url(url):
        return 100
    lowered = url.casefold()
    score = 0
    if "googleusercontent.com" in lowered:
        score += 12
    if "logo" in lowered or "favicon" in lowered or "icon" in lowered:
        score += 45
    if "banner" in lowered or "promo" in lowered or "event" in lowered:
        score += 35
    if is_google_news_url(url):
        score += 80
    return score


def merge_image_candidates(article: dict[str, object], new_candidates: Iterable[str]) -> list[str]:
    ordered: list[str] = []
    current_image = str(article.get("image_url") or "").strip()
    if current_image:
        ordered.append(current_image)
    existing_candidates = article.get("image_candidates")
    if isinstance(existing_candidates, list):
        ordered.extend(str(value or "").strip() for value in existing_candidates)
    ordered.extend(str(value or "").strip() for value in new_candidates)

    unique_urls: list[str] = []
    for image_url in ordered:
        if (
            image_url.startswith(("http://", "https://"))
            and usable_image_url(image_url)
            and image_url not in unique_urls
        ):
            unique_urls.append(image_url)
    return sorted(unique_urls, key=lambda url: (image_quality_score(url), unique_urls.index(url)))


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
    marker = '[\\"garturlres\\",\\"'
    if marker in text:
        tail = text.split(marker, 1)[1]
        encoded_url = tail.split('\\",', 1)[0]
        try:
            decoded_url = json.loads(f'"{encoded_url}"')
        except json.JSONDecodeError:
            decoded_url = encoded_url.replace("\\/", "/")
        if isinstance(decoded_url, str) and decoded_url.startswith(("http://", "https://")):
            return decoded_url
    return None


def decode_google_news_url_online_result(url: str, client: httpx.Client) -> GoogleNewsDecodeResult:
    article_id = google_news_article_id(url)
    if not article_id:
        return GoogleNewsDecodeResult(error="not_google_news")

    params: tuple[str, str] | None = None
    for prefix in ("https://news.google.com/articles/", "https://news.google.com/rss/articles/"):
        try:
            response = client.get(prefix + article_id, follow_redirects=True)
            if response.status_code == 429:
                return GoogleNewsDecodeResult(rate_limited=True, error="google_news_get_rate_limited")
            response.raise_for_status()
        except httpx.HTTPError as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            continue
        params = google_news_decoding_params(response.text)
        if params:
            break
    if not params:
        return GoogleNewsDecodeResult(error=locals().get("last_error", "missing_decoding_params"))

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
        "Referer": "https://news.google.com/",
    }
    try:
        response = client.post(
            f"{GOOGLE_NEWS_DECODE_ENDPOINT}?rpcids=Fbv4je",
            data={"f.req": json.dumps([[request_payload]], separators=(",", ":"))},
            headers=headers,
        )
        if response.status_code == 429:
            return GoogleNewsDecodeResult(rate_limited=True, error="google_news_post_rate_limited")
        response.raise_for_status()
    except httpx.HTTPError as exc:
        return GoogleNewsDecodeResult(error=f"{type(exc).__name__}: {exc}")
    decoded_url = parse_google_news_batch_response(response.text)
    if decoded_url:
        return GoogleNewsDecodeResult(decoded_url=decoded_url)
    return GoogleNewsDecodeResult(error="missing_decoded_url")


def decode_google_news_url_online(url: str, client: httpx.Client) -> str | None:
    return decode_google_news_url_online_result(url, client).decoded_url


def apply_decoded_google_news_url(article: dict[str, object], decoded_url: str | None) -> dict[str, object]:
    if not decoded_url:
        return article
    normalized_decoded = normalize_url(decoded_url)
    enriched = dict(article)
    original_url = str(article.get("canonical_url") or article.get("link") or "")
    if google_news_article_id(original_url):
        enriched["google_news_url"] = original_url
    enriched["canonical_url"] = normalized_decoded
    enriched["canonical_url_hash"] = canonical_url_hash(normalized_decoded)
    enriched["google_news_decoded"] = True
    enriched["original_resolution_status"] = "decoded"
    enriched["original_resolution_score"] = 100
    return enriched


def should_decode_google_news_article(article: dict[str, object], config: dict[str, object]) -> bool:
    fetch_config = config.get("fetch", {})
    if not isinstance(fetch_config, dict) or not bool(fetch_config.get("google_news_decode_publish_levels_only", False)):
        return True
    publish_levels = set(config.get("publish", {}).get("publish_levels", ["high", "medium"]))  # type: ignore[union-attr]
    relevance = relevance_details(
        str(article.get("clean_title") or article.get("title") or ""),
        str(article.get("summary") or ""),
    )
    return str(relevance.get("level") or "") in publish_levels


def article_has_unresolved_google_news(article: dict[str, object]) -> bool:
    url = article_url(article)
    if not is_google_news_url(url):
        return False
    status = str(article.get("original_resolution_status") or "").strip()
    if status in {"decoded", "title_matched"}:
        return False
    return not bool(article.get("google_news_decoded"))


def title_fallback_candidate_score(
    article: dict[str, object],
    candidate: dict[str, object],
    config: dict[str, object],
) -> OriginalUrlMatch | None:
    candidate_url = article_url(candidate)
    if not candidate_url or is_google_news_url(candidate_url):
        return None
    if not source_matches(article, candidate, config):
        return None
    if not dates_are_near(article, candidate, config):
        return None

    article_title = article.get("clean_title") or article.get("title") or ""
    candidate_title = candidate.get("clean_title") or candidate.get("title") or ""
    title_score = title_similarity_score(article_title, candidate_title)
    overlap = len(title_match_tokens(article_title) & title_match_tokens(candidate_title))
    fetch_config = config.get("fetch", {})
    threshold = fetch_config_int(fetch_config, "google_news_title_match_threshold", 86)
    min_overlap = fetch_config_int(fetch_config, "google_news_title_match_min_overlap", 2)
    if title_score < threshold:
        return None
    if overlap < min_overlap and title_score < 94:
        return None
    score = title_score + min(10, overlap * 2)
    return OriginalUrlMatch(
        article=candidate,
        score=score,
        title_score=title_score,
        overlap=overlap,
        reason=f"title:{title_score};overlap:{overlap};source_match",
    )


def best_title_fallback_match(
    article: dict[str, object],
    candidates: Iterable[dict[str, object]],
    config: dict[str, object],
) -> OriginalUrlMatch | None:
    matches = [
        match
        for candidate in candidates
        for match in [title_fallback_candidate_score(article, candidate, config)]
        if match is not None
    ]
    if not matches:
        return None
    return max(matches, key=lambda match: (match.score, match.title_score, match.overlap))


def apply_title_matched_original_url(article: dict[str, object], match: OriginalUrlMatch) -> dict[str, object]:
    matched_article = match.article
    matched_url = normalize_url(article_url(matched_article))
    updated = dict(article)
    google_url = article_url(article)
    if is_google_news_url(google_url):
        updated["google_news_url"] = google_url
    updated["canonical_url"] = matched_url
    updated["canonical_url_hash"] = canonical_url_hash(matched_url)
    updated["original_resolution_status"] = "title_matched"
    updated["original_resolution_score"] = match.score
    updated["original_resolution_reason"] = match.reason
    updated["source_kind"] = "google_discovery"
    for key in ("source", "summary", "article_published_at", "feed_published_at", "feed_updated_at"):
        if matched_article.get(key) and not updated.get(key):
            updated[key] = matched_article.get(key)
    if matched_article.get("source"):
        updated["source"] = matched_article.get("source")
    raw_candidates = matched_article.get("image_candidates")
    matched_candidates = raw_candidates if isinstance(raw_candidates, list) else []
    merged_images = merge_image_candidates(updated, [str(matched_article.get("image_url") or ""), *[str(value or "") for value in matched_candidates]])
    if merged_images:
        updated["image_candidates"] = merged_images
        updated["image_url"] = merged_images[0]
        updated["image_quality_score"] = image_quality_score(merged_images[0])
    return updated


def mark_unresolved_google_news(article: dict[str, object]) -> dict[str, object]:
    if not is_google_news_url(article_url(article)):
        return article
    updated = dict(article)
    updated.setdefault("google_news_url", article_url(article))
    updated["source_kind"] = "google_discovery"
    updated["original_resolution_status"] = "unresolved"
    return updated


def resolve_google_news_originals_from_candidates(
    articles: list[dict[str, object]],
    config: dict[str, object],
    *,
    extra_candidates: Iterable[dict[str, object]] | None = None,
) -> list[dict[str, object]]:
    fetch_config = config.get("fetch", {})
    enabled = True
    if isinstance(fetch_config, dict):
        enabled = bool(fetch_config.get("google_news_title_fallback_enabled", True))
    if not enabled:
        return [mark_unresolved_google_news(article) for article in articles]

    candidate_pool = [
        candidate
        for candidate in [*articles, *(list(extra_candidates or []))]
        if isinstance(candidate, dict) and article_url(candidate) and not is_google_news_url(article_url(candidate))
    ]
    resolved: list[dict[str, object]] = []
    for article in articles:
        if not article_has_unresolved_google_news(article):
            resolved.append(article)
            continue
        match = best_title_fallback_match(article, candidate_pool, config)
        if match:
            resolved.append(apply_title_matched_original_url(article, match))
        else:
            resolved.append(mark_unresolved_google_news(article))
    return resolved


def google_news_quality_summary(articles: Iterable[dict[str, object]]) -> dict[str, int]:
    summary = {
        "google_news_items": 0,
        "google_news_decoded": 0,
        "google_news_title_matched": 0,
        "google_news_unresolved": 0,
        "articles_with_image": 0,
    }
    for article in articles:
        if article.get("image_url"):
            summary["articles_with_image"] += 1
        is_google_observation = bool(article.get("google_news_url")) or str(article.get("source_kind") or "") == "google_discovery" or is_google_news_url(article_url(article))
        if not is_google_observation:
            continue
        summary["google_news_items"] += 1
        status = str(article.get("original_resolution_status") or "")
        if status == "decoded" or article.get("google_news_decoded"):
            summary["google_news_decoded"] += 1
        elif status == "title_matched":
            summary["google_news_title_matched"] += 1
        elif article_has_unresolved_google_news(article):
            summary["google_news_unresolved"] += 1
    return summary


def decode_google_news_articles(
    articles: list[dict[str, object]],
    config: dict[str, object],
    *,
    timeout: httpx.Timeout,
    limits: httpx.Limits,
    headers: dict[str, str],
) -> list[dict[str, object]]:
    fetch_config = config.get("fetch", {})
    decode_limit = fetch_config_int(fetch_config, "google_news_decode_limit", 25)
    if decode_limit == 0:
        return articles
    sleep_seconds = max(0.0, fetch_config_float(fetch_config, "google_news_decode_sleep_seconds", 0.0))
    stop_on_rate_limit = True
    if isinstance(fetch_config, dict):
        stop_on_rate_limit = bool(fetch_config.get("google_news_decode_stop_on_rate_limit", True))

    decoded_articles = [dict(article) for article in articles]
    decode_attempts = 0
    with httpx.Client(timeout=timeout, limits=limits, headers=headers) as client:
        for index, article in enumerate(decoded_articles):
            url = str(article.get("canonical_url") or article.get("link") or "")
            if not google_news_article_id(url):
                continue
            if not should_decode_google_news_article(article, config):
                continue
            if decode_limit > 0 and decode_attempts >= decode_limit:
                break
            decode_attempts += 1
            result = decode_google_news_url_online_result(url, client)
            if result.decoded_url:
                decoded_articles[index] = apply_decoded_google_news_url(article, result.decoded_url)
            elif result.rate_limited and stop_on_rate_limit:
                break
            if sleep_seconds and (decode_limit < 0 or decode_attempts < decode_limit):
                time.sleep(sleep_seconds)
    return decoded_articles


def decode_google_news_links_in_state(state: dict[str, object], config: dict[str, object]) -> int:
    fetch_config = config.get("fetch", {})
    limit = int(fetch_config.get("state_google_news_decode_limit", 60) or 0)  # type: ignore[union-attr]
    if limit == 0:
        return 0

    page_timeout = float(fetch_config.get("page_timeout_seconds", 8.0) or 8.0)  # type: ignore[union-attr]
    sleep_seconds = max(0.0, fetch_config_float(fetch_config, "google_news_decode_sleep_seconds", 0.0))
    stop_on_rate_limit = bool(fetch_config.get("google_news_decode_stop_on_rate_limit", True))  # type: ignore[union-attr]
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
                result = decode_google_news_url_online_result(url, client)
                if result.rate_limited and stop_on_rate_limit:
                    return decoded_count
                if not result.decoded_url:
                    if sleep_seconds and (limit < 0 or attempted < limit):
                        time.sleep(sleep_seconds)
                    continue
                updated = apply_decoded_google_news_url(article, result.decoded_url)
                article.update(updated)
                decoded_count += 1
                if sleep_seconds and (limit < 0 or attempted < limit):
                    time.sleep(sleep_seconds)
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
    image_candidates = image_hrefs(html_text, final_url)
    enriched["canonical_url"] = normalized_canonical
    enriched["canonical_url_hash"] = canonical_url_hash(normalized_canonical)
    enriched["article_published_at"] = datetime_to_iso(article_published)
    if source:
        enriched["source"] = source
    merged_candidates = merge_image_candidates(enriched, (normalize_url(image) for image in image_candidates))
    if merged_candidates:
        enriched["image_candidates"] = merged_candidates
        enriched["image_url"] = merged_candidates[0]
        enriched["image_quality_score"] = image_quality_score(merged_candidates[0])
    return enriched


def enrichment_jobs(
    articles: list[dict[str, object]],
    config: dict[str, object],
    *,
    max_enrich_articles: int,
    google_news_decode_limit: int,
    google_news_decoded_enrich_limit: int = 0,
) -> list[tuple[int, dict[str, object], bool, bool, bool]]:
    jobs: list[tuple[int, dict[str, object], bool, bool, bool]] = []
    google_news_decode_attempts = 0
    google_news_decoded_enrich_attempts = 0
    for index, article in enumerate(articles):
        url = str(article.get("canonical_url") or article.get("link") or "")
        is_google_news = bool(google_news_article_id(url))
        is_decoded_google_news = bool(article.get("google_news_decoded"))
        can_decode_google_news = is_google_news and should_decode_google_news_article(article, config)
        should_decode_google_news = can_decode_google_news and (
            google_news_decode_limit < 0 or google_news_decode_attempts < google_news_decode_limit
        )
        if should_decode_google_news:
            google_news_decode_attempts += 1
        should_enrich = not (max_enrich_articles > 0 and index + 1 > max_enrich_articles)
        if is_google_news and not is_decoded_google_news and not should_decode_google_news:
            should_enrich = False
        should_enrich_decoded_google_news = False
        if (is_decoded_google_news or (is_google_news and should_decode_google_news)) and not should_enrich:
            should_enrich_decoded_google_news = (
                google_news_decoded_enrich_limit < 0
                or google_news_decoded_enrich_attempts < google_news_decoded_enrich_limit
            )
            if should_enrich_decoded_google_news:
                google_news_decoded_enrich_attempts += 1
        jobs.append((index, article, should_decode_google_news, should_enrich, should_enrich_decoded_google_news))
    return jobs


def enrich_article_job(
    job: tuple[int, dict[str, object], bool, bool, bool],
    config: dict[str, object],
    *,
    timeout: httpx.Timeout,
    limits: httpx.Limits,
    headers: dict[str, str],
) -> tuple[int, dict[str, object]]:
    index, article, should_decode_google_news, should_enrich, should_enrich_decoded_google_news = job
    if not should_decode_google_news and not should_enrich and not should_enrich_decoded_google_news:
        return index, article

    decoded_article = article
    with httpx.Client(timeout=timeout, limits=limits, headers=headers) as client:
        decoded_url = None
        if should_decode_google_news:
            url = str(article.get("canonical_url") or article.get("link") or "")
            decoded_url = decode_google_news_url_online(url, client)
            decoded_article = apply_decoded_google_news_url(
                article,
                decoded_url,
            )
        should_enrich_now = should_enrich or (
            should_enrich_decoded_google_news
            and (bool(decoded_url) or bool(decoded_article.get("google_news_decoded")))
            and not bool(google_news_article_id(str(decoded_article.get("canonical_url") or "")))
        )
        if should_enrich_now:
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

    page_timeout = fetch_config_float(fetch_config, "page_timeout_seconds", 8.0)
    max_enrich_articles = fetch_config_int(fetch_config, "max_enrich_articles", 0)
    enrich_workers = max(1, fetch_config_int(fetch_config, "enrich_workers", 1))
    timeout = httpx.Timeout(page_timeout, connect=min(5.0, page_timeout))
    limits = httpx.Limits(max_connections=max(5, enrich_workers), max_keepalive_connections=max(2, enrich_workers // 2))
    headers = {"User-Agent": USER_AGENT}
    articles = decode_google_news_articles(articles, config, timeout=timeout, limits=limits, headers=headers)

    if not bool(fetch_config.get("enrich_pages", True)):  # type: ignore[union-attr]
        return resolve_google_news_originals_from_candidates(articles, config)

    jobs = enrichment_jobs(
        articles,
        config,
        max_enrich_articles=max_enrich_articles,
        google_news_decode_limit=0,
        google_news_decoded_enrich_limit=fetch_config_int(fetch_config, "google_news_decoded_enrich_limit", 0),
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
        return resolve_google_news_originals_from_candidates([article for article in results if article is not None], config)

    enriched_articles: list[dict[str, object]] = []
    with httpx.Client(timeout=timeout, limits=limits, headers=headers) as client:
        for _index, article, should_decode_google_news, should_enrich, should_enrich_decoded_google_news in jobs:
            decoded_article = article
            decoded_url = None
            if should_decode_google_news:
                url = str(article.get("canonical_url") or article.get("link") or "")
                decoded_url = decode_google_news_url_online(url, client)
                decoded_article = apply_decoded_google_news_url(
                    article,
                    decoded_url,
                )
            should_enrich_now = should_enrich or (
                should_enrich_decoded_google_news
                and (bool(decoded_url) or bool(decoded_article.get("google_news_decoded")))
                and not bool(google_news_article_id(str(decoded_article.get("canonical_url") or "")))
            )
            if not should_enrich_now:
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
    return resolve_google_news_originals_from_candidates(enriched_articles, config)


def parse_feed_file(path: str | Path, config: dict[str, object]) -> list[dict[str, object]]:
    return parse_feed(Path(path).read_text(encoding="utf-8"), config)
