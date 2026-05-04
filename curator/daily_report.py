from __future__ import annotations

import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from html import escape
from pathlib import Path
from urllib.parse import parse_qsl, quote, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import httpx

from .ai import ai_config, call_github_models
from .config import load_config
from .dates import format_kst, now_in_timezone
from .fetch import USER_AGENT, image_href
from .normalize import canonical_url_hash
from .rss_writer import article_link, article_source_label, compact_text, display_article_title
from .remote_api import sync_report_to_remote_api
from .state import load_state
from .telegram_sources import risk_flags_for_text
from .summaries import (
    digest_article_identity_keys,
    digest_article_entries,
    digest_category_label_for_group,
    digest_config,
    digest_context,
    digest_group_title,
    digest_representative_entry,
    digest_clusters_in_window,
    duplicate_records_in_window,
    group_digest_entries,
)
from .telegram_publisher import (
    html_link,
    send_telegram_message,
    telegram_bot_token,
    telegram_chat_id,
    telegram_is_configured,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FEED_DIR = Path("public") / "feed"
NON_DATE_REPORT_PAGES = {"latest.html", "index.html", "workbench.html"}
REPORT_CATEGORY_ORDER = [
    "주주행동·경영권",
    "밸류업·주주환원",
    "자본시장 제도·공시",
    "해외·영문",
    "기타",
]
BSIDE_URL = "https://bside.ai"
BSIDE_LOGO_SVG = """<svg fill="currentColor" viewBox="0 0 57 20" class="bside-logo__image" aria-hidden="true"><path fill="currentColor" d="M7.11306 19.3232C5.87526 19.3232 4.76917 19.032 3.78817 18.4496C2.80716 17.8673 2.03026 17.0798 1.46404 16.0738C0.891241 15.0745 0.601547 13.9429 0.588379 12.6855V1.80579C0.588379 1.44181 0.700305 1.14401 0.930744 0.925618C1.16118 0.700612 1.45087 0.594727 1.79982 0.594727C2.14877 0.594727 2.45821 0.707229 2.67549 0.925618C2.89934 1.15063 3.00467 1.44181 3.00467 1.80579V8.25158C3.55773 7.58316 4.22929 7.05372 5.01279 6.66328C5.79626 6.27284 6.65217 6.08093 7.58709 6.08093C8.73929 6.08093 9.77956 6.37211 10.6947 6.95446C11.6099 7.53684 12.3341 8.32437 12.8674 9.31705C13.4007 10.3097 13.6641 11.4347 13.6641 12.6921C13.6641 13.9495 13.3744 15.0812 12.795 16.0805C12.2156 17.0798 11.4387 17.8739 10.4577 18.4563C9.47667 19.0386 8.35741 19.3298 7.11306 19.3298V19.3232ZM7.11306 17.179C7.92288 17.179 8.64053 16.9871 9.27259 16.5967C9.90464 16.2062 10.405 15.6701 10.7803 14.9885C11.149 14.3069 11.3399 13.5392 11.3399 12.6988C11.3399 11.8583 11.1556 11.0774 10.7803 10.409C10.4116 9.74058 9.90464 9.21116 9.27259 8.82069C8.64053 8.43025 7.91629 8.23831 7.11306 8.23831C6.30982 8.23831 5.60532 8.43025 4.9667 8.82069C4.32806 9.21116 3.82109 9.74058 3.45897 10.409C3.09685 11.0774 2.91251 11.8384 2.91251 12.6988C2.91251 13.5591 3.09685 14.3069 3.45897 14.9885C3.82109 15.6701 4.32806 16.2128 4.9667 16.5967C5.60532 16.9871 6.32297 17.179 7.11306 17.179Z"></path><path fill="currentColor" d="M19.6288 19.3236C18.5227 19.3236 17.4956 19.1515 16.5541 18.814C15.6126 18.4765 14.8818 18.0397 14.355 17.5103C14.118 17.2588 14.0193 16.9676 14.0588 16.6367C14.0983 16.3125 14.2497 16.0411 14.5196 15.836C14.8357 15.5845 15.1517 15.472 15.4546 15.5117C15.764 15.5514 16.0273 15.6904 16.2512 15.9286C16.5211 16.233 16.9491 16.511 17.5482 16.7757C18.1408 17.0404 18.8058 17.1728 19.53 17.1728C20.4452 17.1728 21.1496 17.0206 21.6303 16.7161C22.1109 16.4117 22.3611 16.0213 22.3808 15.5448C22.4006 15.0683 22.1702 14.6514 21.7027 14.3006C21.2352 13.9499 20.3859 13.6653 19.1481 13.4403C17.5482 13.1227 16.3895 12.6462 15.6718 12.0108C14.9542 11.3756 14.5921 10.5946 14.5921 9.67476C14.5921 8.86076 14.8291 8.19238 15.3031 7.65632C15.7772 7.12029 16.3895 6.72323 17.1335 6.46511C17.8774 6.20041 18.6543 6.06805 19.4576 6.06805C20.5044 6.06805 21.4262 6.2335 22.236 6.571C23.0458 6.90853 23.6845 7.36514 24.1585 7.95414C24.3823 8.20561 24.4811 8.47694 24.4679 8.75488C24.4548 9.03285 24.3165 9.27108 24.0663 9.45638C23.8161 9.63505 23.5133 9.688 23.1643 9.62182C22.8154 9.55564 22.5257 9.41667 22.2887 9.19167C21.8936 8.80785 21.4657 8.54973 21.0048 8.40414C20.5439 8.25855 20.0172 8.19238 19.4115 8.19238C18.7136 8.19238 18.1276 8.3115 17.6404 8.54973C17.1598 8.78797 16.9162 9.13873 16.9162 9.60197C16.9162 9.88653 16.9886 10.1446 17.14 10.3763C17.2915 10.6079 17.5878 10.813 18.0289 10.9983C18.47 11.1836 19.1218 11.3623 19.9777 11.5344C21.1628 11.7726 22.1043 12.0771 22.7891 12.441C23.4803 12.805 23.9741 13.2352 24.2704 13.7183C24.5733 14.2014 24.7247 14.7639 24.7247 15.3992C24.7247 16.1338 24.5338 16.7889 24.1453 17.3779C23.7569 17.9669 23.1907 18.4368 22.4335 18.7875C21.683 19.1383 20.7414 19.3103 19.6222 19.3103L19.6288 19.3236Z"></path><path fill="currentColor" d="M26.8648 4.04907C26.4369 4.04907 26.0682 3.89686 25.7587 3.58583C25.4493 3.27478 25.2979 2.90419 25.2979 2.47403C25.2979 2.04388 25.4493 1.67327 25.7587 1.36223C26.0682 1.0512 26.4369 0.898987 26.8648 0.898987C27.2928 0.898987 27.6615 1.0512 27.9709 1.36223C28.2804 1.67327 28.4318 2.04388 28.4318 2.47403C28.4318 2.90419 28.2804 3.27478 27.9709 3.58583C27.6615 3.89686 27.2928 4.04907 26.8648 4.04907ZM26.8648 19.2304C26.5159 19.2304 26.2262 19.1179 25.9958 18.8862C25.7653 18.6546 25.6534 18.3635 25.6534 18.0127V7.39107C25.6534 7.0271 25.7653 6.7293 25.9958 6.51089C26.2262 6.28589 26.5159 6.18001 26.8648 6.18001C27.2138 6.18001 27.5232 6.29251 27.7405 6.51089C27.9644 6.73592 28.0697 7.0271 28.0697 7.39107V18.0127C28.0697 18.3635 27.9578 18.6546 27.7405 18.8862C27.5166 19.1179 27.2269 19.2304 26.8648 19.2304Z"></path><path fill="currentColor" d="M36.1018 19.3233C34.8641 19.3233 33.758 19.0322 32.7638 18.4498C31.7762 17.8674 30.9927 17.0799 30.4133 16.074C29.8341 15.0747 29.5444 13.9431 29.5444 12.6857C29.5444 11.4283 29.8077 10.3032 30.3409 9.31055C30.8741 8.31787 31.5918 7.53038 32.5136 6.94799C33.4288 6.36564 34.4691 6.07443 35.6212 6.07443C36.5562 6.07443 37.4186 6.26635 38.2088 6.65682C38.9988 7.04726 39.6638 7.57667 40.2036 8.24508V1.79932C40.2036 1.43534 40.3156 1.13754 40.5462 0.919148C40.7765 0.694142 41.0662 0.588257 41.415 0.588257C41.7641 0.588257 42.0736 0.70076 42.2909 0.919148C42.5147 1.14415 42.62 1.43534 42.62 1.79932V12.6791C42.62 13.9364 42.3303 15.0681 41.7509 16.0674C41.1715 17.0667 40.3947 17.8608 39.4136 18.4432C38.4327 19.0255 37.3265 19.3167 36.0888 19.3167L36.1018 19.3233ZM36.1018 17.1792C36.9118 17.1792 37.6294 16.9873 38.2615 16.5968C38.8936 16.2063 39.3938 15.6703 39.7559 14.9887C40.118 14.307 40.3024 13.5393 40.3024 12.6989C40.3024 11.8584 40.118 11.0775 39.7559 10.4091C39.3938 9.74073 38.8936 9.21129 38.2615 8.82085C37.6294 8.4304 36.905 8.23846 36.1018 8.23846C35.2986 8.23846 34.5941 8.4304 33.9556 8.82085C33.3168 9.21129 32.81 9.74073 32.4347 10.4091C32.0594 11.0775 31.875 11.8386 31.875 12.6989C31.875 13.5592 32.0594 14.307 32.4347 14.9887C32.8033 15.6703 33.3103 16.213 33.9556 16.5968C34.5941 16.9873 35.3118 17.1792 36.1018 17.1792Z"></path><path fill="currentColor" d="M50.6328 19.3231C49.3157 19.3231 48.1504 19.0386 47.1301 18.476C46.1095 17.9135 45.3063 17.1326 44.7334 16.14C44.154 15.1473 43.8643 14.0024 43.8643 12.7186C43.8643 11.4347 44.1343 10.2633 44.6807 9.27064C45.2272 8.27796 45.9843 7.49708 46.9457 6.93455C47.9134 6.37205 49.0195 6.08746 50.2704 6.08746C51.5216 6.08746 52.5684 6.36543 53.4504 6.90808C54.3393 7.45737 55.011 8.21179 55.4784 9.17799C55.9457 10.1442 56.1763 11.2494 56.1763 12.5068C56.1763 12.8112 56.071 13.0627 55.8669 13.2612C55.6628 13.4597 55.3995 13.559 55.0834 13.559H45.5169V11.6531H55.011L54.0366 12.3215C54.0234 11.5273 53.8654 10.8126 53.5625 10.1839C53.2596 9.55523 52.8316 9.05888 52.2787 8.6949C51.7254 8.3309 51.054 8.14561 50.264 8.14561C49.3619 8.14561 48.5916 8.34414 47.9463 8.74123C47.3078 9.13829 46.8204 9.68096 46.4848 10.3758C46.1554 11.0707 45.9843 11.8516 45.9843 12.7252C45.9843 13.5987 46.1819 14.3796 46.5769 15.0613C46.9719 15.7429 47.5184 16.2856 48.2163 16.6826C48.9143 17.0797 49.711 17.2782 50.6128 17.2782C51.1001 17.2782 51.6072 17.1856 52.1207 17.0069C52.6343 16.8282 53.049 16.6098 53.3648 16.3716C53.6019 16.1995 53.8587 16.1069 54.1354 16.0936C54.4119 16.087 54.6554 16.1664 54.8595 16.3451C55.1295 16.5833 55.2678 16.8481 55.2875 17.1326C55.3007 17.4172 55.1754 17.6687 54.9057 17.8738C54.3657 18.304 53.7007 18.6547 52.8975 18.9261C52.1007 19.1974 51.3372 19.3298 50.6063 19.3298L50.6328 19.3231Z"></path></svg>"""

def report_hours() -> int:
    raw_value = os.environ.get("DAILY_REPORT_HOURS", "24")
    try:
        return max(1, int(raw_value))
    except ValueError:
        return 24


def public_base_url(config: dict[str, object]) -> str:
    feed_url = str(config.get("public_feed_url") or "").strip()
    if feed_url.endswith("/feed.xml"):
        return feed_url[: -len("/feed.xml")]
    return feed_url.rstrip("/")


def report_public_url(config: dict[str, object], date_id: str) -> str:
    base_url = public_base_url(config)
    if not base_url:
        return f"feed/{date_id}.html"
    return f"{base_url}/feed/{date_id}.html"


def report_read_api_url() -> str:
    return os.environ.get("ACTIVIST_PUBLIC_API_URL", "").strip()


def article_domain(url: str) -> str:
    hostname = (urlsplit(url).hostname or "").lower().removeprefix("www.")
    return hostname or "source"


def source_logo_url(domain: str) -> str:
    normalized = domain.lower().removeprefix("www.").strip()
    if not normalized or normalized == "source":
        return ""
    return f"https://www.google.com/s2/favicons?domain={quote(normalized, safe='')}&sz=128"


def mobile_article_url(url: str) -> str:
    """Return a conservative mobile-friendly article URL when a safe mapping is known."""
    raw_url = str(url or "").strip()
    if not raw_url.startswith(("http://", "https://")):
        return raw_url
    parsed = urlsplit(raw_url)
    hostname = (parsed.hostname or "").lower()
    bare_host = hostname.removeprefix("www.")
    path = parsed.path or ""

    if bare_host in {"n.news.naver.com", "m.news.nate.com", "v.daum.net"} or bare_host.startswith(("m.", "mobile.")):
        return raw_url

    if bare_host == "news.naver.com":
        if path.startswith("/article/"):
            return urlunsplit((parsed.scheme, "n.news.naver.com", path, parsed.query, ""))
        if path == "/main/read.naver":
            params = dict(parse_qsl(parsed.query, keep_blank_values=True))
            oid = params.get("oid")
            aid = params.get("aid")
            if oid and aid:
                return urlunsplit((parsed.scheme, "n.news.naver.com", f"/article/{oid}/{aid}", "", ""))

    if bare_host in {"news.v.daum.net", "v.daum.net"} and path.startswith("/v/"):
        return urlunsplit((parsed.scheme, "v.daum.net", path, parsed.query, ""))

    if bare_host == "news.nate.com" and path.startswith("/view/"):
        return urlunsplit((parsed.scheme, "m.news.nate.com", path, parsed.query, ""))

    return raw_url


def mobile_link_attrs(url: str) -> str:
    mobile_url = mobile_article_url(url)
    if not mobile_url or mobile_url == url:
        return ""
    return f' data-mobile-url="{escape(mobile_url, quote=True)}"'


def slugify(value: object, fallback: str = "section") -> str:
    text = re.sub(r"[^0-9A-Za-z가-힣]+", "-", str(value or "")).strip("-")
    return text or fallback


def entry_datetime(entry: dict[str, object]) -> datetime | None:
    value = entry.get("datetime")
    return value if isinstance(value, datetime) else None


def best_story_summary(group: list[dict[str, object]]) -> str:
    for entry in group:
        article = entry.get("article")
        if not isinstance(article, dict):
            continue
        summary = compact_text(str(article.get("summary") or ""), max_chars=220)
        if summary and summary.casefold() not in {"제목 없음", "no summary"}:
            return summary
    return ""


PORTAL_LINK_DOMAINS = {
    "news.google.com",
    "www.google.com",
    "news.url.google.com",
    "v.daum.net",
    "news.v.daum.net",
    "daum.net",
    "msn.com",
}
PORTAL_SOURCE_LABELS = {"NEWS", "GOOGLE", "MSN", "DAUM", "다음뉴스", "v.daum.net"}


def story_link_quality(link: dict[str, str]) -> int:
    domain = article_domain(str(link.get("url") or ""))
    source = str(link.get("source") or "").strip()
    score = 0
    if domain and domain not in PORTAL_LINK_DOMAINS:
        score += 10
    if source and source not in PORTAL_SOURCE_LABELS and not source.endswith(".net"):
        score += 5
    if str(link.get("image_url") or "").startswith(("http://", "https://")):
        score += 2
    if str(link.get("url") or "").startswith("https://"):
        score += 1
    return score


def story_priority_score(group: list[dict[str, object]]) -> int:
    scores: list[int] = []
    for entry in group:
        article = entry.get("article")
        if isinstance(article, dict):
            try:
                scores.append(int(article.get("priority_score") or 0))
            except (TypeError, ValueError):
                pass
    return max(scores, default=0)


def story_priority_level(group: list[dict[str, object]]) -> str:
    levels = [
        str(entry.get("article", {}).get("priority_level") or "")
        for entry in group
        if isinstance(entry.get("article"), dict)
    ]
    for level in ("top", "watch", "normal", "archive", "suppress"):
        if level in levels:
            return level
    return ""


def story_summary_for_display(story: dict[str, object]) -> str:
    summary = compact_text(str(story.get("summary") or ""), max_chars=220)
    generic_patterns = (
        "관련 보도를 묶어",
        "원문 링크와 함께 정리",
        "관련 기사를 묶어",
        "관련 뉴스를 묶어",
    )
    if any(pattern in summary for pattern in generic_patterns):
        return fallback_story_summary(story)
    return summary or fallback_story_summary(story)


def fallback_story_summary(story: dict[str, object]) -> str:
    title = compact_text(str(story.get("title") or "이 이슈"), max_chars=82)
    category = str(story.get("category") or "")
    source_line = compact_text(str(story.get("source_line") or story.get("primary_source") or ""), max_chars=42)
    link_count = int(story.get("link_count") or 0)
    category_tail = {
        "주주행동·경영권": "주주권과 경영권 이슈의 후속 흐름을 보여줍니다.",
        "밸류업·주주환원": "주주환원 정책의 실행 가능성과 시장 반응을 확인할 수 있습니다.",
        "자본시장 제도·공시": "공시·감독 제도 변화가 자본시장에 미치는 영향을 짚어볼 사안입니다.",
        "해외·영문": "해외 투자자와 외신이 바라보는 지배구조·행동주의 흐름을 보여줍니다.",
    }.get(category, "자본시장 관점에서 후속 흐름을 확인할 만한 사안입니다.")
    if source_line and link_count > 1:
        return f"{source_line} 등 {link_count}개 매체가 '{title}' 흐름을 전했습니다. {category_tail}"
    if source_line:
        return f"{source_line} 보도로 확인된 '{title}' 이슈입니다. {category_tail}"
    return f"'{title}' 이슈입니다. {category_tail}"


def story_links(group: list[dict[str, object]]) -> list[dict[str, str]]:
    links: list[dict[str, str]] = []
    seen_keys: dict[str, int] = {}
    for entry in group:
        article = entry.get("article")
        if not isinstance(article, dict):
            continue
        url = str(entry.get("url") or article_link(article) or "")
        if not url:
            continue
        source = article_source_label(article)
        title = display_article_title(article, source) or str(entry.get("title") or article.get("clean_title") or article.get("title") or source)
        published_at = entry_datetime(entry)
        link = {
            "source": source,
            "title": title,
            "url": url,
            "mobile_url": mobile_article_url(url),
            "domain": article_domain(url),
            "image_url": str(article.get("image_url") or ""),
            "published_at": published_at.isoformat() if published_at else "",
        }
        identity_keys = digest_article_identity_keys(article) or {f"url:{url}"}
        existing_indices = [seen_keys[key] for key in identity_keys if key in seen_keys]
        if existing_indices:
            existing_index = min(existing_indices)
            if story_link_quality(link) > story_link_quality(links[existing_index]):
                links[existing_index] = link
            for key in identity_keys:
                seen_keys[key] = existing_index
            continue
        links.append(link)
        current_index = len(links) - 1
        for key in identity_keys:
            seen_keys[key] = current_index
    return links


def story_db_key(group: list[dict[str, object]]) -> str:
    for entry in group:
        article = entry.get("article")
        if isinstance(article, dict):
            story_key = str(article.get("story_key") or "").strip()
            if story_key:
                return story_key
        cluster = entry.get("cluster")
        if isinstance(cluster, dict):
            story_key = str(cluster.get("story_key") or "").strip()
            if story_key:
                return story_key
    return ""


def story_db_query(title: str, links: list[dict[str, str]]) -> str:
    stopwords = {
        "관련",
        "기사",
        "보도",
        "뉴스",
        "종합",
        "단독",
        "속보",
        "시장",
        "자본시장",
        "주주",
        "기업",
        "지난해",
        "올해",
        "오늘",
        "이슈",
        "확인",
        "한국어",
        "google",
        "news",
    }
    tokens: list[str] = []
    for source_text in [title, " ".join(link.get("title") or "" for link in links[:2])]:
        for token in re.findall(r"[0-9A-Za-z가-힣]{2,}", source_text):
            normalized = token.casefold()
            if normalized in stopwords or token in stopwords:
                continue
            if token not in tokens:
                tokens.append(token)
            if len(tokens) >= 4:
                return " ".join(tokens)
    return compact_text(title, max_chars=32)


def story_image_urls(group: list[dict[str, object]]) -> list[str]:
    image_urls: list[str] = []
    for entry in group:
        article = entry.get("article")
        if not isinstance(article, dict):
            continue
        image_url = str(article.get("image_url") or "").strip()
        if image_url.startswith(("http://", "https://")) and image_url not in image_urls:
            image_urls.append(image_url)
    return ordered_image_urls(image_urls)


def story_link_image_urls(links: list[dict[str, str]]) -> list[str]:
    image_urls: list[str] = []
    for link in links:
        image_url = str(link.get("image_url") or "").strip()
        if image_url.startswith(("http://", "https://")) and image_url not in image_urls:
            image_urls.append(image_url)
    return ordered_image_urls(image_urls)


def story_image_url(group: list[dict[str, object]]) -> str:
    urls = story_image_urls(group)
    return urls[0] if urls else ""


def image_quality_rank(image_url: str) -> int:
    lower_url = image_url.casefold()
    if any(pattern in lower_url for pattern in ("trans_30x13", "blank.", "spacer", "noimage", "no_img")):
        return 50
    if "lh3.googleusercontent.com/j6_cofbog" in lower_url:
        return 45
    if "googleusercontent.com" in lower_url and "s0-w300" in lower_url:
        return 35
    if "/logo" in lower_url or "logo." in lower_url:
        return 40
    return 0


def article_preview_image_url(image_urls: list[str]) -> str:
    return next((image_url for image_url in ordered_image_urls(image_urls) if image_quality_rank(image_url) < 35), "")


def ordered_image_urls(image_urls: list[str]) -> list[str]:
    unique_urls: list[str] = []
    for image_url in image_urls:
        image_url = str(image_url or "").strip()
        if image_url.startswith(("http://", "https://")) and image_url not in unique_urls:
            unique_urls.append(image_url)
    return sorted(unique_urls, key=lambda url: (image_quality_rank(url), unique_urls.index(url)))


def image_enrich_settings(config: dict[str, object]) -> tuple[int, float]:
    report_config = config.get("report", {})
    if not isinstance(report_config, dict):
        report_config = {}
    limit = int(report_config.get("image_enrich_limit", 120) or 120)
    timeout = float(report_config.get("image_timeout_seconds", 4) or 4)
    return max(0, limit), max(1.0, timeout)


def story_image_candidates(story: dict[str, object]) -> list[str]:
    candidates: list[str] = []
    for value in [story.get("primary_url")]:
        text = str(value or "").strip()
        if text.startswith(("http://", "https://")) and text not in candidates:
            candidates.append(text)
    links = story.get("links") if isinstance(story.get("links"), list) else []
    for link in links[:10]:
        if not isinstance(link, dict):
            continue
        url = str(link.get("url") or "").strip()
        if url.startswith(("http://", "https://")) and url not in candidates:
            candidates.append(url)
    return candidates


def discover_story_image(url: str, client: httpx.Client) -> str:
    try:
        response = client.get(url, follow_redirects=True)
        response.raise_for_status()
    except httpx.HTTPError:
        return ""
    image_url = image_href(response.text, str(response.url))
    return image_url or ""


def append_story_image_candidate(story: dict[str, object], image_url: str) -> None:
    image_url = str(image_url or "").strip()
    if not image_url.startswith(("http://", "https://")):
        return
    image_candidates = story.get("image_candidates")
    if not isinstance(image_candidates, list):
        image_candidates = []
        story["image_candidates"] = image_candidates
    if image_url not in [str(value) for value in image_candidates]:
        image_candidates.append(image_url)
        story["image_candidates"] = ordered_image_urls([str(value) for value in image_candidates])
    current_image = str(story.get("image_url") or "").strip()
    if image_quality_rank(image_url) >= 35:
        return
    if not current_image.startswith(("http://", "https://")) or image_quality_rank(image_url) < image_quality_rank(current_image):
        story["image_url"] = image_url


def enrich_story_images(stories: list[dict[str, object]], config: dict[str, object]) -> None:
    limit, timeout = image_enrich_settings(config)
    if limit <= 0:
        return
    checked = 0
    with httpx.Client(timeout=timeout, headers={"User-Agent": USER_AGENT}) as client:
        for story in stories:
            current_candidates = story.get("image_candidates")
            candidate_count = len(current_candidates) if isinstance(current_candidates, list) else 0
            if str(story.get("image_url") or "").startswith(("http://", "https://")) and candidate_count >= 3:
                continue
            for candidate_url in story_image_candidates(story):
                if checked >= limit:
                    return
                checked += 1
                image_url = discover_story_image(candidate_url, client)
                if image_url:
                    append_story_image_candidate(story, image_url)
                    current_candidates = story.get("image_candidates")
                    if isinstance(current_candidates, list) and len(current_candidates) >= 3:
                        break


def story_source_line(links: list[dict[str, str]]) -> str:
    counter = Counter(link["source"] for link in links if link.get("source"))
    return " · ".join(source for source, _count in counter.most_common(4))


def story_logo_context(story: dict[str, object]) -> tuple[str, str]:
    links = story.get("links") if isinstance(story.get("links"), list) else []
    normalized_links = [link for link in links if isinstance(link, dict)]
    first_link = next(
        (
            link
            for link in normalized_links
            if article_domain(str(link.get("url") or "")) not in {"news.google.com", "www.google.com"}
        ),
        normalized_links[0] if normalized_links else {},
    )
    source = str(
        (first_link.get("source") if isinstance(first_link, dict) else "")
        or story.get("primary_source")
        or story.get("source_line")
        or "NO IMAGE"
    )
    url = str(story.get("primary_url") or (first_link.get("url") if isinstance(first_link, dict) else ""))
    domain = str(first_link.get("domain") if isinstance(first_link, dict) else "") or article_domain(url)
    label = compact_text(source, max_chars=18) or "NO IMAGE"
    return label, source_logo_url(domain)


def source_logo_html(story: dict[str, object], href: str) -> str:
    label, logo_url = story_logo_context(story)
    safe_label = escape(label)
    safe_attr_label = escape(label, quote=True)
    safe_logo = escape(logo_url, quote=True)
    safe_href = escape(href, quote=True)
    mobile_attrs = mobile_link_attrs(href)
    logo_img = (
        f'<img class="story__source-logo" src="{safe_logo}" alt="" loading="lazy" decoding="async" referrerpolicy="no-referrer">'
        if logo_url
        else ""
    )
    return (
        f'<a class="story__image story__image--logo" href="{safe_href}"{mobile_attrs} aria-label="{safe_attr_label} 기사 보기" '
        f'data-logo-label="{safe_attr_label}" data-logo-src="{safe_logo}"{story_image_data_attrs(story, include_logo_context=False)}>'
        f'{logo_img}<span>{safe_label}</span></a>'
    )


def story_image_data_attrs(story: dict[str, object], *, include_logo_context: bool = True) -> str:
    label, logo_url = story_logo_context(story)
    raw_candidates = story.get("image_candidates")
    candidates: list[str] = []
    if isinstance(raw_candidates, list):
        for value in raw_candidates:
            image_url = str(value or "").strip()
            if image_url.startswith(("http://", "https://")) and image_url not in candidates:
                candidates.append(image_url)
    primary_image = str(story.get("image_url") or "").strip()
    if primary_image.startswith(("http://", "https://")) and primary_image not in candidates:
        candidates.insert(0, primary_image)
    candidates = ordered_image_urls(candidates)
    candidates_json = json.dumps(candidates[:5], ensure_ascii=False)
    attrs = f' data-image-candidates="{escape(candidates_json, quote=True)}"'
    if include_logo_context:
        attrs = (
            f' data-logo-label="{escape(label, quote=True)}"'
            f' data-logo-src="{escape(logo_url, quote=True)}"'
            f"{attrs}"
        )
    return attrs


def bside_logo_html(extra_class: str = "") -> str:
    class_name = f"bside-logo {extra_class}".strip()
    return (
        f'<a class="{class_name}" href="{BSIDE_URL}" aria-label="BSIDE Korea 홈페이지">'
        f"{BSIDE_LOGO_SVG}"
        '<span class="bside-logo__label">DAILY NEWS</span>'
        '</a>'
    )


def daily_report_write_only() -> bool:
    value = os.environ.get("CURATOR_DAILY_REPORT_WRITE_ONLY", "")
    return value.casefold() in {"1", "true", "yes", "on"}


def build_report_stories(
    clusters: list[dict[str, object]],
    duplicate_records: list[dict[str, object]],
    config: dict[str, object],
) -> list[dict[str, object]]:
    entries = digest_article_entries(clusters, config, duplicate_records)
    stories: list[dict[str, object]] = []

    for section_key, section_label in (("domestic", ""), ("global", "해외·영문")):
        for group in group_digest_entries(entries[section_key], config):
            representative = digest_representative_entry(group, config)
            links = story_links(group)
            if not links:
                continue
            latest_dt = max((dt for dt in (entry_datetime(entry) for entry in group) if dt), default=None)
            category = section_label or digest_category_label_for_group(group)
            title = str(representative.get("title") or digest_group_title(group, config) or "제목 없음")
            image_candidates = ordered_image_urls([*story_image_urls(group), *story_link_image_urls(links)])
            priority_score = story_priority_score(group)
            db_key = story_db_key(group)
            stories.append(
                {
                    "title": title,
                    "category": category,
                    "summary": best_story_summary(group),
                    "links": links,
                    "link_count": len(links),
                    "image_url": article_preview_image_url(image_candidates),
                    "image_candidates": image_candidates,
                    "primary_url": str(representative.get("url") or links[0]["url"]),
                    "primary_source": links[0]["source"],
                    "source_line": story_source_line(links),
                    "datetime": latest_dt,
                    "section": section_key,
                    "priority_score": priority_score,
                    "priority_level": story_priority_level(group),
                    "story_key": db_key,
                    "db_query": story_db_query(title, links),
                    "score": priority_score + len(links) * 5 + (6 if category == "주주행동·경영권" else 0),
                }
            )

    stories.sort(
        key=lambda story: (
            int(story.get("score") or 0),
            story.get("datetime") if isinstance(story.get("datetime"), datetime) else datetime.min,
        ),
        reverse=True,
    )
    for index, story in enumerate(stories, start=1):
        story["id"] = f"story-{index}"
    return stories


def attach_telegram_mentions(stories: list[dict[str, object]], state: dict[str, object]) -> None:
    messages_by_key = {
        f"id:{message.get('telegram_channel_id')}:{int(message.get('telegram_message_id') or 0)}": message
        for message in state.get("telegram_source_messages", [])
        if isinstance(message, dict) and message.get("telegram_channel_id") and message.get("telegram_message_id") and not message.get("deleted_at")
    }
    messages_by_key.update(
        {
            f"handle:{str(message.get('handle') or '').removeprefix('@')}:{int(message.get('telegram_message_id') or 0)}": message
            for message in state.get("telegram_source_messages", [])
            if isinstance(message, dict) and message.get("handle") and message.get("telegram_message_id") and not message.get("deleted_at")
        }
    )
    matches_by_article: dict[str, list[dict[str, object]]] = defaultdict(list)
    for match in state.get("telegram_article_matches", []):
        if isinstance(match, dict) and match.get("article_id") and match.get("telegram_message_key"):
            matches_by_article[str(match["article_id"])].append(match)

    for story in stories:
        seen_messages: set[str] = set()
        mentions: list[dict[str, object]] = []
        links = story.get("links") if isinstance(story.get("links"), list) else []
        article_ids = {
            canonical_url_hash(str(link.get("url") or ""))
            for link in links
            if isinstance(link, dict) and link.get("url")
        }
        for article_id in article_ids:
            for match in matches_by_article.get(article_id, []):
                message_key = str(match.get("telegram_message_key") or "")
                if not message_key or message_key in seen_messages:
                    continue
                message = messages_by_key.get(message_key)
                if not isinstance(message, dict):
                    continue
                seen_messages.add(message_key)
                text = str(message.get("text") or "")
                mentions.append(
                    {
                        "message_url": message.get("message_url") or match.get("message_url") or "",
                        "channel_title": message.get("channel_title") or match.get("channel_title") or "",
                        "channel_handle": message.get("handle") or match.get("channel_handle") or "",
                        "posted_at": message.get("posted_at") or "",
                        "text": compact_text(text, max_chars=160),
                        "excerpt": compact_text(text, max_chars=120),
                        "match_type": match.get("match_type") or "",
                        "score": match.get("score") or 0,
                        "risk_flags": risk_flags_for_text(text),
                    }
                )
        mentions.sort(
            key=lambda item: (
                float(item.get("score") or 0),
                str(item.get("posted_at") or ""),
            ),
            reverse=True,
        )
        if mentions:
            story["telegram_mentions"] = mentions[:5]


def story_context(stories: list[dict[str, object]], config: dict[str, object], max_stories: int = 18) -> str:
    blocks: list[str] = []
    for index, story in enumerate(stories[:max_stories], start=1):
        summary = str(story.get("summary") or "")
        links = story.get("links") if isinstance(story.get("links"), list) else []
        sources = ", ".join(str(link.get("source") or "") for link in links[:5] if isinstance(link, dict))
        blocks.append(
            "\n".join(
                line
                for line in (
                    f"{index}. [{story.get('category')}] {story.get('title')}",
                    f"매체: {sources}" if sources else "",
                    f"요약: {summary}" if summary else "",
                )
                if line
            )
        )
    return "\n\n".join(blocks) or digest_context([], config)


def brief_bullet(text: str, *, max_chars: int = 82) -> str:
    bullet = clean_brief_source_noise(text)
    bullet = re.sub(r"\s+", " ", bullet).strip(" -·|.。")
    replacements = (
        ("보도했습니다", "보도됨"),
        ("보도합니다", "보도됨"),
        ("이어지고 있습니다", "이어짐"),
        ("했습니다", "했음"),
        ("합니다", "함"),
        ("됐습니다", "됐음"),
        ("되었습니다", "됨"),
        ("됩니다", "됨"),
        ("있습니다", "있음"),
        ("부각됩니다", "부각됨"),
        ("필요합니다", "필요 있음"),
    )
    for before, after in replacements:
        bullet = re.sub(f"{before}$", after, bullet)
    if bullet and not re.search(r"(음|함|됨|있음|이어짐|부각|확인|필요)$", bullet):
        bullet = f"{bullet} 보도됨"
    return compact_text(bullet, max_chars=max_chars).strip(" .")


def fallback_story_brief(story: dict[str, object]) -> dict[str, list[str]]:
    title = compact_text(str(story.get("title") or ""), max_chars=86)
    category = str(story.get("category") or "")
    link_count = int(story.get("link_count") or 0)
    summary = clean_brief_source_noise(story_summary_for_display(story))
    category_tail = {
        "주주행동·경영권": "주주권 행사와 이사회 책임 쟁점으로 이어짐",
        "밸류업·주주환원": "주주환원 실행 가능성과 공시 구체성 확인 필요 있음",
        "자본시장 제도·공시": "감독·공시 제도 변화와 투자자 보호 쟁점 있음",
        "해외·영문": "해외 투자자 시각과 글로벌 행동주의 흐름 확인됨",
    }.get(category, "자본시장 후속 흐름을 확인할 사안 있음")
    bullets: list[str] = []
    if link_count <= 1 and summary and len(summary) >= 30:
        bullets.append(brief_bullet(summary, max_chars=82))
    else:
        bullets.append(brief_bullet(f"{title} 이슈 보도됨", max_chars=82))
    bullets.append(brief_bullet(category_tail, max_chars=82))
    return {"bullets": [bullet for bullet in bullets if bullet][:2]}


def clean_brief_source_noise(text: str) -> str:
    cleaned = re.sub(r"https?://\S+", " ", str(text or ""))
    cleaned = re.sub(r"\b[\w.-]+\.(?:com|net|co\.kr|kr|org|io)\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\bGoogle News\b|\bv\.daum\.net\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" -·|")
    return cleaned


def story_brief_context(stories: list[dict[str, object]], config: dict[str, object], max_stories: int) -> str:
    blocks: list[str] = []
    for story in stories[:max_stories]:
        links = story.get("links") if isinstance(story.get("links"), list) else []
        sources = ", ".join(str(link.get("source") or "") for link in links[:4] if isinstance(link, dict))
        blocks.append(
            "\n".join(
                line
                for line in (
                    f"id: {story.get('id')}",
                    f"category: {story.get('category')}",
                    f"title: {story.get('title')}",
                    f"sources: {sources}" if sources else "",
                    f"summary: {story_summary_for_display(story)}",
                )
                if line
            )
        )
    return "\n\n".join(blocks)


def parse_story_brief_response(content: str | None) -> dict[str, dict[str, list[str]]]:
    if not content:
        return {}
    cleaned = re.sub(r"^```(?:json)?|```$", "", content.strip(), flags=re.IGNORECASE | re.MULTILINE).strip()
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError:
        return {}
    if isinstance(data, dict):
        items = data.get("stories")
    else:
        items = data
    if not isinstance(items, list):
        return {}
    parsed: dict[str, dict[str, list[str]]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        story_id = str(item.get("id") or "").strip()
        if not story_id:
            continue
        raw_bullets = item.get("bullets")
        bullets: list[str] = []
        if isinstance(raw_bullets, list):
            bullets = [
                brief_bullet(str(raw_bullet or ""), max_chars=88)
                for raw_bullet in raw_bullets
                if str(raw_bullet or "").strip()
            ]
        if not bullets:
            bullets = [
                brief_bullet(str(item.get(key) or ""), max_chars=88)
                for key in ("point", "why")
                if str(item.get(key) or "").strip()
            ]
        bullets = [bullet for bullet in bullets if bullet]
        if bullets:
            parsed[story_id] = {"bullets": bullets[:3]}
    return parsed


def attach_story_briefs(stories: list[dict[str, object]], config: dict[str, object]) -> None:
    for story in stories:
        story["brief"] = fallback_story_brief(story)

    settings = ai_config(config)
    if not settings.get("daily_report_enabled", True) or not settings.get("story_brief_enabled", True):
        return
    max_stories = int(settings.get("story_brief_max_stories", 8))
    if max_stories <= 0:
        return

    model = str(settings.get("story_brief_model") or settings.get("daily_report_model") or "openai/gpt-4.1")
    max_tokens = int(settings.get("story_brief_max_tokens", 1400))
    system_prompt = (
        "당신은 한국 자본시장 데일리 페이지의 편집자입니다. "
        "기사 제목과 수집 요약만 바탕으로 투자자가 빠르게 읽을 수 있는 짧은 bullet 요약을 씁니다. "
        "기사에 없는 사실을 만들지 말고, 매수·매도 판단은 금지합니다."
    )
    user_prompt = (
        "아래 기사 묶음별로 JSON만 출력하세요.\n"
        "형식: {\"stories\":[{\"id\":\"story-1\",\"bullets\":[\"...\",\"...\"]}]}\n"
        "- bullets: 기사 핵심과 투자자/주주권/공시/제도 관점 의미를 1~2개로 작성\n"
        "- 각 bullet은 22~58자, '보도됨/이어짐/있음/확인됨/부각됨' 같은 짧은 정보성 문체\n"
        "- '근거', '요점', '맥락' 같은 라벨은 쓰지 않음\n"
        "- 저작권 보호를 위해 원문 문장을 길게 그대로 복사하지 않음\n"
        "- 제공된 정보 밖의 수치·사실을 추가하지 않음\n\n"
        f"{story_brief_context(stories, config, max_stories)}"
    )
    content = call_github_models(
        system_prompt,
        user_prompt,
        model=model,
        max_tokens=max_tokens,
        config=config,
    )
    ai_briefs = parse_story_brief_response(content)
    if not ai_briefs:
        return
    for story in stories[:max_stories]:
        story_id = str(story.get("id") or "")
        brief = ai_briefs.get(story_id)
        if not brief:
            continue
        fallback = fallback_story_brief(story)
        bullets = brief.get("bullets") or fallback["bullets"]
        story["brief"] = {"bullets": [bullet for bullet in bullets if bullet][:3]}


def fallback_report_review(stories: list[dict[str, object]]) -> str:
    def titles_for(category: str, limit: int = 3) -> list[str]:
        return [str(story.get("title") or "") for story in stories if story.get("category") == category][:limit]

    shareholder = titles_for("주주행동·경영권")
    valueup = titles_for("밸류업·주주환원")
    capital = titles_for("자본시장 제도·공시")
    global_titles = titles_for("해외·영문")
    top_titles = [str(story.get("title") or "") for story in stories[:4]]

    paragraphs = []
    lead_titles = shareholder or top_titles
    if lead_titles:
        paragraphs.append(
            "주주행동·경영권 이슈가 이사회 책임과 공시 투명성 쟁점으로 이어짐"
        )
    if valueup:
        paragraphs.append(
            "밸류업·주주환원은 자사주·배당 실행 가능성과 공시 구체성이 부각됨"
        )
    if capital:
        paragraphs.append(
            "자본시장 제도·공시는 감독당국 요구와 시장 규율 강화 흐름 확인 필요 있음"
        )
    if global_titles:
        paragraphs.append(
            "해외·영문 보도는 행동주의 캠페인과 한국 시장 평가가 맞물리는 지점 있음"
        )
    if not paragraphs:
        paragraphs.append("신규 발행 이슈는 제한적이나 주주권·공시 후속 흐름 확인 필요 있음")
    return "\n".join(f"- {paragraph}" for paragraph in paragraphs[:4])


def clean_report_paragraphs(text: str, *, max_paragraphs: int = 4) -> list[str]:
    normalized = re.sub(r"\r\n?", "\n", str(text or "")).strip()
    raw_parts = re.split(r"\n\s*\n", normalized)
    if len(raw_parts) == 1:
        raw_parts = [line for line in normalized.splitlines() if line.strip()]
    paragraphs: list[str] = []
    for raw_part in raw_parts:
        paragraph = re.sub(r"^\s*(?:[-*•]|\d+[.)])\s*", "", raw_part).strip()
        paragraph = re.sub(r"\s+", " ", paragraph)
        if not paragraph or len(paragraph) < 20:
            continue
        if any(pattern in paragraph for pattern in ("링크", "몇 건", "정리했")) and len(paragraph) < 80:
            continue
        paragraphs.append(paragraph)
        if len(paragraphs) >= max_paragraphs:
            break
    return paragraphs


def clean_report_bullets(text: str, *, max_bullets: int = 4) -> list[str]:
    normalized = re.sub(r"\r\n?", "\n", str(text or "")).strip()
    candidates = [part for part in re.split(r"\n+|(?<=다\.)\s+", normalized) if part.strip()]
    bullets: list[str] = []
    for candidate in candidates:
        bullet = re.sub(r"^\s*(?:[-*•·]|\d+[.)]|[①-⑩])\s*", "", candidate).strip()
        bullet = re.sub(r"\s+", " ", bullet)
        if not bullet or len(bullet) < 12:
            continue
        if any(pattern in bullet for pattern in ("링크", "몇 건", "정리했", "HTML", "텔레그램")) and len(bullet) < 90:
            continue
        bullets.append(compact_report_bullet(bullet))
        if len(bullets) >= max_bullets:
            break
    if len(bullets) >= 2:
        return bullets
    return [compact_report_bullet(paragraph) for paragraph in clean_report_paragraphs(text, max_paragraphs=max_bullets)]


def compact_report_bullet(text: str, max_chars: int = 118) -> str:
    bullet = re.sub(r"\s+", " ", str(text or "")).strip(" -•·.。")
    return compact_text(bullet, max_chars=max_chars)


def generate_report_review(
    clusters: list[dict[str, object]],
    stories: list[dict[str, object]],
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
) -> str:
    settings = ai_config(config)
    if not settings.get("daily_report_enabled", True):
        return fallback_report_review(stories)
    model = str(settings.get("daily_report_model") or settings.get("daily_digest_model") or "openai/gpt-4.1")
    max_tokens = int(settings.get("daily_report_max_tokens", 900))
    system_prompt = (
        "당신은 금융위원회, 금감원, 거래소, 기관투자자, 행동주의 펀드를 오래 취재한 전문 자본시장 기자입니다. "
        "수집된 기사 묶음을 바탕으로 하루치 브리핑의 핵심 bullet만 간결한 한국어 기사체로 작성합니다. "
        "투자 조언이나 매매 권유는 하지 말고, 기사에 없는 사실을 단정하지 마세요."
    )
    user_prompt = (
        "아래 기사 묶음을 바탕으로 Telegram과 HTML 데일리 상단에 들어갈 상세 요약을 작성하세요.\n"
        "- bullet point 3~4개로 작성\n"
        "- 각 bullet은 30~68자 안팎의 한 문장으로 작성\n"
        "- 예: '주주권 행사와 이사회 책임 이슈가 맞물리며 투자자 보호 논의 부각됨'\n"
        "- 전체 흐름, 주요 사건, 제도/정책적 의미, 해외/영문 흐름을 균형 있게 반영\n"
        "- 전문 자본시장 기자의 톤으로, 정책·공시·주주권 의미를 해석하되 과장하지 않음\n"
        "- '그랬음/보도됨/이어짐/있음/필요 있음'처럼 짧은 정보성 어미 사용\n"
        "- '기사 N건을 정리했다' 같은 운영 설명은 쓰지 않음\n"
        "- 특정 종목 매수/매도 판단은 쓰지 않음\n\n"
        f"기간: {format_kst(start_at, str(config.get('timezone') or 'Asia/Seoul'))} - {format_kst(end_at, str(config.get('timezone') or 'Asia/Seoul'))}\n\n"
        f"{story_context(stories, config)}"
    )
    content = call_github_models(
        system_prompt,
        user_prompt,
        model=model,
        max_tokens=max_tokens,
        config=config,
    )
    if content and len(clean_report_bullets(content)) >= 2:
        return "\n".join(f"- {bullet}" for bullet in clean_report_bullets(content))
    return fallback_report_review(stories)


def report_stats(stories: list[dict[str, object]], clusters: list[dict[str, object]], duplicate_records: list[dict[str, object]]) -> dict[str, int]:
    article_count = sum(int(story.get("link_count") or 0) for story in stories)
    source_count = len(
        {
            str(link.get("source") or "")
            for story in stories
            for link in (story.get("links") if isinstance(story.get("links"), list) else [])
            if isinstance(link, dict) and link.get("source")
        }
    )
    return {
        "stories": len(stories),
        "articles": article_count,
        "sources": source_count,
        "clusters": len(clusters),
        "duplicates": len(duplicate_records),
    }


def category_buckets(stories: list[dict[str, object]]) -> dict[str, list[dict[str, object]]]:
    buckets: dict[str, list[dict[str, object]]] = defaultdict(list)
    for story in stories:
        buckets[str(story.get("category") or "기타")].append(story)
    return buckets


def date_label(value: object, config: dict[str, object]) -> str:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    if not isinstance(value, datetime):
        return ""
    return value.astimezone(ZoneInfo(timezone_name)).strftime("%m.%d %H:%M")


def link_date_label(link: dict[str, str], config: dict[str, object]) -> str:
    raw_value = str(link.get("published_at") or "")
    if not raw_value:
        return ""
    try:
        parsed = datetime.fromisoformat(raw_value)
    except ValueError:
        return ""
    return date_label(parsed, config)


def render_link_list(links: list[dict[str, str]], config: dict[str, object], *, compact: bool = False) -> str:
    items = []
    for index, link in enumerate(links, start=1):
        source = escape(link.get("source") or link.get("domain") or f"기사 {index}")
        title = escape(compact_text(link.get("title") or "", max_chars=86))
        raw_url = link.get("url") or ""
        url = escape(raw_url, quote=True)
        mobile_attrs = mobile_link_attrs(raw_url)
        if compact:
            items.append(f'<a href="{url}"{mobile_attrs}>{source}</a>')
        else:
            published = escape(link_date_label(link, config))
            items.append(
                "<tr>"
                f'<td class="link-table__time">{published}</td>'
                f'<td class="link-table__source">{source}</td>'
                f'<td class="link-table__title"><a href="{url}"{mobile_attrs}>{title}</a></td>'
                "</tr>"
            )
    return " ".join(items) if compact else "\n".join(items)


def render_source_links(links: list[dict[str, str]], *, max_sources: int = 7) -> str:
    items = []
    seen_sources: set[str] = set()
    for index, link in enumerate(links, start=1):
        if len(items) >= max_sources:
            break
        source = compact_text(link.get("source") or link.get("domain") or f"기사 {index}", max_chars=28)
        key = source.casefold()
        url = str(link.get("url") or "")
        if not source or not url or key in seen_sources:
            continue
        seen_sources.add(key)
        items.append(f'<a href="{escape(url, quote=True)}"{mobile_link_attrs(url)}>{escape(source)}</a>')
    unique_source_count = len(
        {
            compact_text(str(link.get("source") or link.get("domain") or ""), max_chars=28).casefold()
            for link in links
            if str(link.get("source") or link.get("domain") or "").strip()
        }
    )
    remaining = max(0, unique_source_count - len(seen_sources))
    if remaining:
        items.append(f"<em>외 {remaining}건</em>")
    return " ".join(items)


def json_script_payload(value: object) -> str:
    return escape(json.dumps(value, ensure_ascii=False).replace("</", "<\\/"))


def story_brief_bullets(story: dict[str, object], *, max_chars: int = 88, max_items: int = 3) -> list[str]:
    brief = story.get("brief") if isinstance(story.get("brief"), dict) else {}
    raw_bullets = brief.get("bullets") if isinstance(brief, dict) else None
    bullets = [str(item) for item in raw_bullets if str(item or "").strip()] if isinstance(raw_bullets, list) else []
    if not bullets and isinstance(brief, dict):
        bullets = [str(brief.get(key) or "") for key in ("point", "why") if str(brief.get(key) or "").strip()]
    return [brief_bullet(bullet, max_chars=max_chars) for bullet in bullets[:max_items] if str(bullet or "").strip()]


def render_story(
    story: dict[str, object],
    config: dict[str, object],
    *,
    featured: bool = False,
    show_details: bool = True,
    section_id: str = "",
    section_index: int = 0,
    section_total: int = 0,
    editorial: bool = False,
) -> str:
    links = story.get("links") if isinstance(story.get("links"), list) else []
    story_id = escape(str(story.get("id") or slugify(story.get("title"), "story")), quote=True)
    safe_title = escape(str(story.get("title") or "제목 없음"))
    raw_primary_url = str(story.get("primary_url") or "#")
    primary_url = escape(raw_primary_url, quote=True)
    primary_mobile_attrs = mobile_link_attrs(raw_primary_url)
    category = escape(str(story.get("category") or "기타"))
    story_key = str(story.get("story_key") or "").strip()
    db_query = str(story.get("db_query") or story.get("title") or "").strip()
    sources = escape(str(story.get("source_line") or story.get("primary_source") or ""))
    summary = escape(story_summary_for_display(story))
    summary_html = ""
    summary_after_body_html = ""
    if editorial:
        bullet_items = "\n".join(f"<li>{escape(bullet)}</li>" for bullet in story_brief_bullets(story))
        summary_after_body_html = f'<ul class="story__summary">{bullet_items}</ul>' if bullet_items else ""
    else:
        summary_html = f"<p>{summary}</p>" if summary else ""
    timestamp = escape(date_label(story.get("datetime"), config))
    image_url = escape(str(story.get("image_url") or ""), quote=True)
    image_html = (
        f'<a class="story__image" href="{primary_url}"{primary_mobile_attrs} aria-label="기사 이미지 보기"{story_image_data_attrs(story)}><img src="{image_url}" alt="" loading="lazy" decoding="async" referrerpolicy="no-referrer"></a>'
        if image_url
        else source_logo_html(story, raw_primary_url)
    )
    normalized_links = [link for link in links if isinstance(link, dict)]
    has_grouped_links = len(normalized_links) > 1
    source_links = render_source_links(normalized_links) if has_grouped_links else ""
    source_meta = source_links or sources
    source_meta_html = f'<span class="story__sources">{source_meta}</span>' if source_meta else ""
    current_links_data_html = (
        f'<script type="application/json" data-story-current-links>{json_script_payload(normalized_links)}</script>'
        if has_grouped_links
        else ""
    )
    telegram_mentions = story.get("telegram_mentions") if isinstance(story.get("telegram_mentions"), list) else []
    telegram_mentions_data_html = (
        f'<script type="application/json" data-story-telegram-mentions>{json_script_payload(telegram_mentions)}</script>'
        if telegram_mentions
        else ""
    )
    related_html = (
        f"""
            <details class="story-context" data-story-context>
              <summary>관련 기사 보기</summary>
              {current_links_data_html}
              {telegram_mentions_data_html}
              <div class="story-context__body" data-story-context-body>펼치면 아카이브에서 관련 기사와 매체 확산을 불러옵니다.</div>
            </details>
        """
        if show_details and (story_key or db_query or has_grouped_links or telegram_mentions)
        else ""
    )
    featured_class = " story--featured" if featured else ""
    section_attrs = (
        f' data-story-db-key="{escape(story_key, quote=True)}"'
        f' data-story-db-query="{escape(db_query, quote=True)}"'
        f' data-story-url="{escape(raw_primary_url, quote=True)}"'
    )
    if section_id:
        section_attrs += (
            f' data-section-key="{escape(section_id, quote=True)}"'
            f' data-section-index="{section_index}"'
            f' data-section-total="{section_total}"'
        )
    return f"""
          <article class="story{featured_class}" id="{story_id}" data-story{section_attrs}>
            {image_html}
            <div class="story__body">
              <div class="story__meta"><span>{category}</span><span>{timestamp}</span>{source_meta_html}</div>
              <h3><a href="{primary_url}"{primary_mobile_attrs}>{safe_title}</a></h3>
              {summary_html}
            </div>
            {summary_after_body_html}
            {related_html}
          </article>
    """


def render_report_html(
    stories: list[dict[str, object]],
    review: str,
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
    date_id: str,
    report_url: str,
    duplicate_records: list[dict[str, object]],
    clusters: list[dict[str, object]],
    archive_links_html: str = "",
    layout_variant: str = "standard",
    in_variant_dir: bool = False,
) -> str:
    _ = (layout_variant, in_variant_dir)
    stats = report_stats(stories, clusters, duplicate_records)
    buckets = category_buckets(stories)
    review_bullets = clean_report_bullets(review) or clean_report_bullets(fallback_report_review(stories))
    featured_stories = stories[:5]
    featured_ids = {str(story.get("id") or "") for story in featured_stories}
    section_buckets = {
        category: (
            [story for story in category_stories if str(story.get("id") or "") not in featured_ids]
            if len(stories) > len(featured_stories)
            else list(category_stories)
        )
        for category, category_stories in buckets.items()
    }
    review_items: list[str] = []
    for index, bullet in enumerate(review_bullets):
        target_story = featured_stories[index] if index < len(featured_stories) else None
        target_id = str(target_story.get("id") or "") if isinstance(target_story, dict) else ""
        if target_id:
            review_items.append(
                f'<li><a class="brief__link" href="#{escape(target_id, quote=True)}">{escape(bullet)}</a></li>'
            )
        else:
            review_items.append(f"<li>{escape(bullet)}</li>")
    review_html = "\n".join(review_items)
    review_block_html = f'<ul class="brief__bullets">{review_html}</ul>' if review_html else ""
    featured_html = "\n".join(
        render_story(story, config, featured=True, show_details=False, editorial=True)
        for story in featured_stories
    )
    featured_block_html = (
        f"""
    <section class="priority" aria-label="오늘의 중요 기사">
      <div class="priority__head">
        <h2>오늘의 중요 기사</h2>
        <p>복수 보도, 주주권·공시 영향, 제도적 파급을 기준으로 먼저 읽을 기사를 배치했습니다.</p>
      </div>
      <div class="featured featured--priority">
        {featured_html}
      </div>
    </section>
        """
    )
    category_sections = []
    for category in REPORT_CATEGORY_ORDER:
        category_stories = section_buckets.get(category, [])
        if not category_stories:
            continue
        section_id = slugify(category, "section")
        category_sections.append(
            f"""
        <section class="section" id="{escape(section_id, quote=True)}" data-section data-section-label="{escape(category, quote=True)}" data-section-count="{len(category_stories)}">
          <div class="section__rule"></div>
          <div class="section__head">
            <h2>{escape(category)}</h2>
            <span>{len(category_stories)}개 이슈</span>
          </div>
          <div class="story-list">
            {''.join(render_story(story, config, section_id=section_id, section_index=index, section_total=len(category_stories), editorial=True) for index, story in enumerate(category_stories, start=1))}
          </div>
        </section>
            """
        )
    toc = "\n".join(
        f'<a class="chip" data-toc-section="{escape(slugify(category, "section"), quote=True)}" href="#{escape(slugify(category, "section"), quote=True)}" style="--progress:0"><span class="chip__label">{escape(category)}</span><span class="chip__progress" data-progress-text>0/{len(section_buckets.get(category, []))}</span></a>'
        for category in REPORT_CATEGORY_ORDER
        if section_buckets.get(category)
    )
    side_category_links = "\n".join(
        f'<a data-nav-section data-section-target="{escape(slugify(category, "section"), quote=True)}" href="#{escape(slugify(category, "section"), quote=True)}"><span class="nav-label">{escape(category)}</span><span class="nav-progress" data-progress-text>0/{len(section_buckets.get(category, []))}</span></a>'
        for category in REPORT_CATEGORY_ORDER
        if section_buckets.get(category)
    )
    ordered_section_stories = [
        story
        for category in REPORT_CATEGORY_ORDER
        for story in section_buckets.get(category, [])
    ]
    side_story_links = "\n".join(
        f'<a data-nav-story data-nav-story-index="{index}" href="#{escape(str(story.get("id") or ""), quote=True)}">{escape(compact_text(str(story.get("title") or ""), max_chars=46))}</a>'
        for index, story in enumerate(ordered_section_stories)
    )
    mobile_story_links = "\n".join(
        f'<a data-mobile-nav-story data-nav-story-index="{index}" href="#{escape(str(story.get("id") or ""), quote=True)}">{escape(compact_text(str(story.get("title") or ""), max_chars=36))}</a>'
        for index, story in enumerate(ordered_section_stories)
    )
    start_label = escape(format_kst(start_at, str(config.get("timezone") or "Asia/Seoul")))
    end_label = escape(format_kst(end_at, str(config.get("timezone") or "Asia/Seoul")))
    archive_links_html = archive_links_html or '<span class="archive-panel__empty">아직 발행된 데일리가 없습니다.</span>'
    report_date_label = escape(date_id)
    title = f"비사이드 자본시장 데일리 - {date_id}"
    description = compact_text(" ".join(review_bullets), max_chars=180)
    canonical_url = escape(report_url, quote=True)
    header_logo = bside_logo_html("bside-logo--top")
    nav_logo = bside_logo_html("bside-logo--nav")
    footer_logo = bside_logo_html("bside-logo--footer")
    read_api_url_json = json.dumps(report_read_api_url(), ensure_ascii=False)
    date_id_json = json.dumps(date_id, ensure_ascii=False)
    brief_title_html = '<span class="brief-title__eyebrow">오늘의</span><span>핵심 브리핑</span>'
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
  <meta name="description" content="{escape(description, quote=True)}">
  <meta property="og:title" content="{escape(title, quote=True)}">
  <meta property="og:description" content="{escape(description, quote=True)}">
  <meta property="og:type" content="article">
  <meta property="og:url" content="{canonical_url}">
  <style>
    :root {{
      --ink: #17131f;
      --muted: #6f6878;
      --line: #ded7e8;
      --paper: #fbfafc;
      --surface: #ffffff;
      --accent: #6b35d8;
      --accent-deep: #42207e;
      --accent-soft: #f0eafb;
      --green: #00785f;
    }}
    * {{ box-sizing: border-box; }}
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      color: var(--ink);
      background: var(--paper);
      font-family: -apple-system, BlinkMacSystemFont, "Apple SD Gothic Neo", "Malgun Gothic", "Segoe UI", sans-serif;
      line-height: 1.58;
    }}
    a {{ color: inherit; text-decoration-thickness: 1px; text-underline-offset: 3px; }}
    .page {{ max-width: 1000px; margin: 0 auto; padding: 24px 24px 72px; }}
    .masthead {{ border-bottom: 2px solid var(--ink); padding-bottom: 22px; }}
    .brand-row {{ display: flex; justify-content: space-between; gap: 16px; align-items: baseline; border-bottom: 1px solid var(--line); padding-bottom: 10px; margin-bottom: 24px; }}
    .bside-logo {{ display: inline-flex; align-items: center; gap: 9px; color: var(--accent); text-decoration: none; }}
    .bside-logo__image {{ width: 86px; height: auto; display: block; color: var(--accent); flex: 0 0 auto; }}
    .bside-logo__label {{ font-size: 11px; font-weight: 900; letter-spacing: .12em; color: var(--accent); }}
    .bside-logo:hover .bside-logo__label {{ color: var(--accent-deep); }}
    .bside-logo--top .bside-logo__image {{ width: 92px; }}
    .bside-logo--footer {{ margin-bottom: 10px; }}
    .edition {{ color: var(--muted); font-size: 13px; }}
    h1 {{ font-family: Georgia, "Times New Roman", serif; font-size: clamp(36px, 5.8vw, 64px); line-height: 1; letter-spacing: 0; margin: 0 0 14px; max-width: 880px; }}
    .dek {{ max-width: 700px; color: #322b3d; font-size: 15.5px; line-height: 1.6; margin: 0; text-wrap: pretty; word-break: keep-all; overflow-wrap: break-word; }}
    .meta-strip {{ display: flex; flex-wrap: wrap; gap: 10px 18px; margin-top: 20px; color: var(--muted); font-size: 13px; }}
    .meta-strip strong {{ color: var(--accent-deep); }}
    .archive-trigger {{ appearance: none; border: 0; background: transparent; color: inherit; cursor: pointer; font: inherit; text-decoration: underline; text-decoration-thickness: 1px; text-underline-offset: 3px; }}
    .archive-panel[hidden] {{ display: none !important; }}
    .archive-panel {{ position: fixed; top: 78px; right: 24px; z-index: 20; width: min(280px, calc(100vw - 32px)); }}
    .archive-panel__card {{ border: 1px solid var(--line); background: rgba(255,255,255,.98); box-shadow: 0 18px 48px rgba(44, 27, 84, .16); padding: 10px; max-height: calc(100vh - 108px); overflow: auto; }}
    .archive-panel__head {{ display: flex; justify-content: space-between; gap: 12px; padding: 4px 4px 8px; border-bottom: 1px solid var(--line); color: var(--muted); font-size: 11px; font-weight: 900; letter-spacing: .04em; }}
    .archive-panel__close {{ appearance: none; border: 0; background: transparent; color: var(--muted); cursor: pointer; font-size: 14px; line-height: 1; }}
    .archive-panel__links {{ display: grid; gap: 4px; padding-top: 8px; }}
    .archive-panel__link {{ display: flex; justify-content: space-between; gap: 12px; border-radius: 8px; padding: 8px 9px; color: var(--ink); text-decoration: none; font-size: 13px; }}
    .archive-panel__link:hover, .archive-panel__link.is-current {{ background: var(--accent-soft); color: var(--accent-deep); }}
    .archive-panel__link span {{ color: var(--muted); font-size: 11px; }}
    .archive-panel__empty {{ padding: 8px 4px; color: var(--muted); font-size: 13px; }}
    .brief {{ display: grid; grid-template-columns: 168px 1fr; gap: 22px; align-items: start; border-bottom: 1px solid var(--ink); padding: 18px 0; }}
    .brief h2 {{ display: grid; gap: 3px; align-content: start; border-left: 3px solid var(--accent); padding-left: 10px; font-family: Georgia, "Times New Roman", serif; font-size: 20px; line-height: 1.12; margin: 0; word-break: keep-all; }}
    .brief-title__eyebrow {{ color: var(--accent); font-family: -apple-system, BlinkMacSystemFont, "Apple SD Gothic Neo", "Malgun Gothic", "Segoe UI", sans-serif; font-size: 11px; font-weight: 900; letter-spacing: .08em; }}
    .section h2 {{ font-family: Georgia, "Times New Roman", serif; font-size: 26px; line-height: 1.1; margin: 0; }}
    .brief__bullets {{ margin: 0; padding: 2px 0 0; list-style: none; display: grid; gap: 6px; }}
    .brief__bullets li {{ position: relative; padding-left: 13px; font-size: 12.5px; line-height: 1.42; color: #2e2738; word-break: keep-all; overflow-wrap: break-word; }}
    .brief__bullets li::before {{ content: ""; position: absolute; left: 0; top: .72em; width: 4px; height: 4px; border-radius: 50%; background: var(--accent); }}
    .brief__link {{ color: inherit; text-decoration: none; border-bottom: 1px solid rgba(112, 55, 224, .22); }}
    .brief__link:hover {{ color: var(--accent-deep); border-bottom-color: var(--accent); }}
    .brief__link::after {{ content: " 이동"; color: var(--accent); font-size: 10px; font-weight: 900; letter-spacing: .02em; }}
    .db-pulse {{ border-bottom: 1px solid var(--ink); padding: 18px 0 20px; }}
    .db-pulse[hidden] {{ display: none !important; }}
    .db-pulse__head {{ display: flex; align-items: end; justify-content: space-between; gap: 18px; margin-bottom: 12px; }}
    .db-pulse__head h2 {{ margin: 0; font-family: Georgia, "Times New Roman", serif; font-size: 22px; line-height: 1.12; }}
    .db-pulse__head p {{ margin: 4px 0 0; color: var(--muted); font-size: 12.5px; line-height: 1.42; }}
    .db-pulse__badge {{ flex: 0 0 auto; border: 1px solid rgba(112, 55, 224, .24); border-radius: 999px; padding: 5px 9px; color: var(--accent-deep); background: var(--accent-soft); font-size: 11px; font-weight: 900; }}
    .db-pulse__list {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px 12px; }}
    .db-pulse__item {{ display: grid; gap: 4px; min-width: 0; border-top: 1px solid var(--line); padding: 10px 0 2px; color: inherit; text-decoration: none; }}
    .db-pulse__item:hover h3 {{ color: var(--accent-deep); text-decoration: underline; text-underline-offset: 3px; }}
    .db-pulse__item h3 {{ margin: 0; font-size: 14px; line-height: 1.36; font-weight: 850; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; word-break: keep-all; overflow-wrap: break-word; }}
    .db-pulse__meta {{ display: flex; flex-wrap: wrap; gap: 6px 9px; color: var(--muted); font-size: 10.8px; line-height: 1.35; }}
    .db-pulse__meta strong {{ color: var(--accent-deep); font-weight: 900; }}
    .db-search {{ display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 8px; margin-top: 14px; padding-top: 12px; border-top: 1px solid var(--line); }}
    .db-search input {{ min-width: 0; width: 100%; border: 1px solid var(--line); border-radius: 8px; background: var(--surface); color: var(--ink); padding: 9px 10px; font: inherit; font-size: 13px; }}
    .db-search button {{ border: 1px solid var(--accent); border-radius: 8px; background: var(--accent); color: #fff; padding: 0 13px; font: inherit; font-size: 12px; font-weight: 900; cursor: pointer; }}
    .db-search__results[hidden] {{ display: none !important; }}
    .db-search__results {{ display: grid; gap: 6px; margin-top: 10px; }}
    .db-search__result {{ display: grid; gap: 3px; border-top: 1px solid var(--line); padding: 8px 0 2px; text-decoration: none; color: inherit; }}
    .db-search__result:hover h3 {{ color: var(--accent-deep); text-decoration: underline; text-underline-offset: 3px; }}
    .db-search__result h3 {{ margin: 0; font-size: 13px; line-height: 1.36; font-weight: 820; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; word-break: keep-all; overflow-wrap: break-word; }}
    .db-search__meta {{ display: flex; flex-wrap: wrap; gap: 6px 9px; color: var(--muted); font-size: 10.6px; line-height: 1.35; }}
    .db-search__summary {{ margin: 0; color: #4b4357; font-size: 11.6px; line-height: 1.42; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }}
    .db-search__why {{ display: flex; flex-wrap: wrap; gap: 5px; margin-top: 2px; }}
    .db-search__why span {{ border: 1px solid rgba(112, 55, 224, .16); border-radius: 999px; padding: 2px 6px; background: rgba(246, 240, 255, .5); color: var(--accent-deep); font-size: 10px; font-weight: 850; }}
    .db-search__message {{ color: var(--muted); font-size: 12px; padding: 8px 0 0; }}
    .priority {{ border-bottom: 1px solid var(--ink); padding: 22px 0 8px; }}
    .priority__head {{ display: flex; align-items: end; justify-content: space-between; gap: 20px; border-bottom: 1px solid var(--line); padding-bottom: 12px; }}
    .priority__head h2 {{ font-family: Georgia, "Times New Roman", serif; font-size: 28px; line-height: 1.1; margin: 0; }}
    .priority__head p {{ max-width: 520px; margin: 0; color: var(--muted); font-size: 13px; line-height: 1.45; word-break: keep-all; }}
    .toc {{ position: sticky; top: 0; z-index: 5; display: flex; align-items: center; gap: 13px; padding: 10px 0; border-bottom: 1px solid var(--line); background: color-mix(in srgb, var(--paper) 94%, transparent); backdrop-filter: blur(8px); }}
    .toc__brand {{ display: flex; align-items: center; flex: 0 0 auto; padding-right: 2px; }}
    .bside-logo--nav {{ gap: 7px; }}
    .bside-logo--nav .bside-logo__image {{ width: 66px; }}
    .bside-logo--nav .bside-logo__label {{ font-size: 9px; letter-spacing: .14em; white-space: nowrap; }}
    .toc__chips {{ display: flex; flex: 1 1 auto; flex-wrap: wrap; gap: 8px; min-width: 0; }}
    .chip {{ --progress: 0; position: relative; overflow: hidden; display: inline-flex; align-items: center; gap: 7px; border: 1px solid var(--line); border-radius: 999px; padding: 6px 10px; background: var(--surface); text-decoration: none; font-size: 12px; transition: border-color .18s ease, background .18s ease, color .18s ease; }}
    .chip::after {{ content: ""; position: absolute; left: 0; right: auto; bottom: 0; height: 3px; width: calc(var(--progress, 0) * 100%); background: var(--accent); transition: width .18s ease; }}
    .chip__progress {{ color: var(--accent); font-weight: 800; font-variant-numeric: tabular-nums; }}
    .chip.is-active {{ border-color: var(--accent); background: var(--accent-soft); color: var(--accent-deep); }}
    .mobile-story-nav {{ display: none; }}
    .featured {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 0 28px; border-bottom: 1px solid var(--ink); padding: 18px 0 8px; align-items: start; }}
    .priority .featured {{ border-bottom: 0; padding-bottom: 8px; }}
    .featured .story--featured, .featured .story--featured:first-child, .featured .story--featured:nth-child(n+2) {{ display: grid; grid-template-columns: 104px minmax(0, 1fr); gap: 10px 14px; border-top: 1px solid var(--line); border-right: 0; padding: 14px 0 16px; }}
    .featured .story--featured:first-child, .featured .story--featured:nth-child(2) {{ border-top: 0; padding-top: 0; }}
    .featured .story--featured .story__image {{ aspect-ratio: 4 / 3; }}
    .featured .story--featured h3 {{ font-size: 17.5px; }}
    .featured .story--featured p {{ display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }}
    .featured .story--featured .story__summary {{ grid-column: 1 / -1; margin: 0; }}
    .featured .story--featured details {{ grid-column: 1 / -1; }}
    .section {{ position: relative; padding: 34px 0 6px; scroll-margin-top: 108px; }}
    .section__rule {{ height: 3px; background: linear-gradient(90deg, var(--accent), var(--ink)); margin-bottom: 14px; }}
    .section__head {{ position: sticky; top: 49px; z-index: 4; display: flex; align-items: center; justify-content: space-between; gap: 16px; margin: 0 -2px 0; padding: 10px 2px 9px; border-bottom: 1px solid var(--line); background: color-mix(in srgb, var(--paper) 96%, transparent); backdrop-filter: blur(8px); }}
    .section.is-active-section .section__head {{ border-bottom-color: rgba(112, 55, 224, .42); box-shadow: 0 8px 18px rgba(44, 27, 84, .06); }}
    .section__head span {{ color: var(--muted); font-size: 13px; }}
    .story-list {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 0 28px; margin-top: 10px; align-items: start; }}
    .story-list .story:first-child {{ grid-template-columns: 104px minmax(0, 1fr); }}
    .story {{ position: relative; display: grid; width: 100%; grid-template-columns: 104px minmax(0, 1fr); gap: 10px 14px; min-width: 0; border-top: 1px solid var(--line); padding: 14px 0 16px; scroll-margin-top: 112px; vertical-align: top; }}
    .story--featured {{ align-self: start; min-width: 0; overflow: hidden; }}
    .story__body {{ min-width: 0; max-width: 780px; }}
    .story--featured .story__body {{ max-width: none; }}
    .story__image {{ display: block; aspect-ratio: 4 / 3; background: var(--accent-soft); overflow: hidden; border: 1px solid var(--line); }}
    .story__image img {{ width: 100%; height: 100%; object-fit: cover; display: block; }}
    .story__image--empty {{ display: grid; place-items: center; color: var(--accent); font-size: 12px; font-weight: 800; letter-spacing: .08em; }}
    .story__image--logo {{ display: grid; place-items: center; justify-items: center; gap: 8px; padding: 14px; text-align: center; text-decoration: none; color: var(--accent-deep); background: linear-gradient(135deg, #f4efff, #ffffff); }}
    .story__image--logo span {{ font-size: 12px; font-weight: 900; letter-spacing: .02em; line-height: 1.2; overflow-wrap: anywhere; }}
    .story__source-logo {{ width: 42px !important; height: 42px !important; object-fit: contain !important; border-radius: 10px; background: #fff; padding: 6px; box-shadow: 0 4px 14px rgba(44, 27, 84, .10); }}
    .story--featured .story__image {{ aspect-ratio: 16 / 9; }}
    .story__meta {{ display: flex; flex-wrap: wrap; gap: 8px; color: var(--muted); font-size: 11px; line-height: 1.42; margin-bottom: 6px; }}
    .story__meta span:not(:last-child)::after {{ content: "·"; margin-left: 8px; color: var(--line); }}
    .story__sources a {{ margin-right: 8px; white-space: nowrap; color: var(--accent-deep); }}
    .story__sources em {{ font-style: normal; color: var(--muted); white-space: nowrap; }}
    .story h3 {{ font-family: -apple-system, BlinkMacSystemFont, "Apple SD Gothic Neo", "Malgun Gothic", "Segoe UI", sans-serif; font-size: 17.5px; line-height: 1.34; margin: 0 0 5px; letter-spacing: 0; font-weight: 800; word-break: keep-all; overflow-wrap: break-word; text-wrap: pretty; }}
    .story h3 a {{ text-decoration-thickness: 1px; text-underline-offset: 4px; }}
    .story.is-read {{ background: linear-gradient(90deg, rgba(112, 55, 224, .055), transparent 64%); border-top-color: rgba(112, 55, 224, .24); }}
    .story.is-read::after {{ content: "읽음"; position: absolute; top: 16px; left: 8px; z-index: 2; border: 1px solid rgba(112, 55, 224, .30); border-radius: 999px; padding: 2px 7px; color: var(--accent-deep); background: rgba(255,255,255,.92); box-shadow: 0 4px 12px rgba(44, 27, 84, .12); font-size: 10px; font-weight: 900; line-height: 1.2; pointer-events: none; }}
    .story.is-read .story__image {{ filter: saturate(.86) grayscale(.12); opacity: .90; }}
    .story.is-read h3 a {{ color: #5f566e; }}
    .story--featured h3 {{ font-size: 18.5px; line-height: 1.32; }}
    .story p {{ max-width: 700px; margin: 0 0 8px; color: #3f3948; font-size: 14px; line-height: 1.58; word-break: keep-all; overflow-wrap: break-word; text-wrap: pretty; }}
    .story--featured p {{ font-size: 13.5px; line-height: 1.55; }}
    .story__summary {{ grid-column: 1 / -1; display: grid; gap: 4px; overflow: visible; margin: 0; padding: 8px 10px 8px 13px; border-left: 3px solid rgba(112, 55, 224, .52); background: rgba(246, 240, 255, .50); list-style: none; color: #342d3d; font-size: 12.6px; line-height: 1.45; word-break: keep-all; overflow-wrap: break-word; }}
    .story__summary li {{ position: relative; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; padding-left: 11px; }}
    .story__summary li::before {{ content: ""; position: absolute; left: 0; top: .68em; width: 4px; height: 4px; border-radius: 50%; background: var(--accent); }}
    details {{ grid-column: 1 / -1; margin-top: 8px; max-width: 100%; min-width: 0; }}
    details[open] {{ padding-bottom: 3px; }}
    summary {{ cursor: pointer; color: var(--green); font-size: 13px; font-weight: 800; }}
    summary::after {{ content: " · 좌우 스크롤"; color: var(--muted); font-size: 11px; font-weight: 700; }}
    .story-context {{ margin-top: 4px; border-top: 1px solid rgba(112, 55, 224, .14); padding-top: 6px; }}
    .story-context[hidden], .story-context__body[hidden] {{ display: none !important; }}
    .story-context summary {{ color: var(--accent-deep); }}
    .story-context summary::after {{ content: " · 통합 표"; color: var(--muted); font-size: 11px; font-weight: 700; }}
    .story-context__body {{ display: grid; gap: 8px; margin-top: 8px; padding: 9px 10px; border-left: 3px solid rgba(112, 55, 224, .34); background: rgba(246, 240, 255, .38); color: #342d3d; font-size: 12px; line-height: 1.45; }}
    .story-context__message {{ color: var(--muted); }}
    .story-context__stats {{ display: flex; flex-wrap: wrap; gap: 6px; }}
    .story-context__stat {{ border: 1px solid rgba(112, 55, 224, .18); border-radius: 999px; padding: 3px 7px; background: #fff; color: var(--accent-deep); font-size: 10.8px; font-weight: 850; }}
    .story-context__spread {{ display: flex; flex-wrap: wrap; gap: 5px 8px; color: var(--muted); font-size: 11px; }}
    .story-context__spread strong {{ color: var(--ink); }}
    .story-context__timeline {{ display: grid; gap: 5px; margin: 0; padding: 0; list-style: none; }}
    .story-context__timeline a {{ display: grid; grid-template-columns: 68px minmax(0, 1fr); gap: 8px; color: inherit; text-decoration: none; }}
    .story-context__timeline a:hover .story-context__timeline-title {{ color: var(--accent-deep); text-decoration: underline; text-underline-offset: 3px; }}
    .story-context__timeline-time {{ color: var(--muted); font-size: 10.8px; white-space: nowrap; }}
    .story-context__timeline-title {{ min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .story-context__kind {{ display: inline-flex; align-items: center; justify-content: center; min-width: 52px; border: 1px solid rgba(112, 55, 224, .22); border-radius: 999px; padding: 2px 6px; background: #fff; color: var(--accent-deep); font-size: 10.5px; font-weight: 850; white-space: nowrap; }}
    .story-context__kind--archive {{ color: var(--green); border-color: rgba(0, 120, 95, .25); }}
    .story-context__row--current td {{ background: rgba(255,255,255,.55); }}
    .link-table {{ width: 100%; max-width: 100%; min-width: 0; margin-top: 10px; border: 1px solid var(--line); background: var(--surface); overflow-x: auto; overflow-y: hidden; -webkit-overflow-scrolling: touch; }}
    .link-table table {{ width: 100%; min-width: 660px; table-layout: fixed; border-collapse: collapse; font-size: 12px; }}
    th, td {{ padding: 8px 9px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 700; background: #faf8fd; }}
    th:first-child, td:first-child {{ width: 92px; color: var(--muted); white-space: nowrap; }}
    th:nth-child(2), td:nth-child(2) {{ width: 120px; color: var(--accent-deep); }}
    .story-context__table table {{ min-width: 720px; }}
    .story-context__table th:first-child, .story-context__table td:first-child {{ width: 76px; color: inherit; }}
    .story-context__table th:nth-child(2), .story-context__table td:nth-child(2) {{ width: 94px; color: var(--muted); white-space: nowrap; }}
    .story-context__table th:nth-child(3), .story-context__table td:nth-child(3) {{ width: 120px; color: var(--accent-deep); }}
    .story-context__telegram {{ display: grid; gap: 7px; padding-top: 2px; }}
    .story-context__telegram-head {{ display: flex; align-items: center; justify-content: space-between; gap: 8px; color: var(--ink); font-size: 11.5px; font-weight: 900; }}
    .story-context__telegram-list {{ display: grid; gap: 6px; margin: 0; padding: 0; list-style: none; }}
    .story-context__telegram-list a {{ display: grid; gap: 3px; padding: 7px 8px; border: 1px solid rgba(112, 55, 224, .12); border-radius: 8px; background: rgba(255,255,255,.72); color: inherit; text-decoration: none; }}
    .story-context__telegram-list a:hover strong {{ color: var(--accent-deep); text-decoration: underline; text-underline-offset: 3px; }}
    .story-context__telegram-meta {{ display: flex; flex-wrap: wrap; gap: 4px 7px; color: var(--muted); font-size: 10.8px; }}
    .story-context__telegram-meta span {{ white-space: nowrap; }}
    .story-context__telegram-list p {{ margin: 0; color: var(--ink); font-size: 11.5px; line-height: 1.38; }}
    td a {{ overflow-wrap: anywhere; }}
    .floating-nav {{ position: fixed; top: 84px; right: 12px; z-index: 8; width: 210px; max-height: calc(100vh - 108px); overflow: auto; border: 1px solid var(--line); background: rgba(255,255,255,.94); box-shadow: 0 14px 40px rgba(44, 27, 84, .10); padding: 10px; }}
    .floating-nav__meta {{ display: grid; gap: 8px; margin-bottom: 12px; padding-bottom: 10px; border-bottom: 1px solid var(--line); }}
    .floating-nav__meta-item {{ display: grid; gap: 2px; }}
    .floating-nav__meta span {{ color: var(--muted); font-size: 10px; font-weight: 800; letter-spacing: .05em; }}
    .floating-nav__meta strong {{ color: var(--ink); font-size: 12px; line-height: 1.25; }}
    .floating-nav__meta em {{ color: var(--muted); font-size: 11px; font-style: normal; line-height: 1.25; }}
    .floating-nav__archive {{ display: flex; align-items: center; justify-content: center; border: 1px solid var(--accent); border-radius: 999px; background: var(--accent-soft); color: var(--accent-deep) !important; font-weight: 800; padding: 7px 10px !important; text-decoration: none; }}
    .floating-nav h2 {{ font-size: 11px; margin: 0 0 7px; color: var(--accent-deep); letter-spacing: .04em; }}
    .floating-nav a {{ display: flex; align-items: baseline; justify-content: space-between; gap: 10px; text-decoration: none; border-left: 2px solid transparent; padding: 6px 8px; color: var(--muted); font-size: 12px; transition: border-color .18s ease, background .18s ease, color .18s ease; }}
    .floating-nav .nav-label {{ min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .floating-nav .nav-progress {{ flex: 0 0 auto; color: var(--accent); font-weight: 800; font-variant-numeric: tabular-nums; }}
    .floating-nav a.is-active {{ border-left-color: var(--accent); color: var(--ink); background: var(--accent-soft); }}
    .floating-nav__stories {{ margin-top: 12px; padding-top: 10px; border-top: 1px solid var(--line); }}
    .floating-nav__stories a {{ display: flex; align-items: center; justify-content: space-between; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .floating-nav__stories a.is-read {{ color: #9a93a5; background: #f8f5fc; }}
    .floating-nav__stories a.is-read::after {{ content: "✓"; flex: 0 0 auto; margin-left: 8px; color: var(--accent); font-weight: 900; }}
    .floating-nav__stories a:not(.is-near-active) {{ display: none; }}
    .top-button {{ position: fixed; right: 22px; bottom: 24px; z-index: 9; width: 42px; height: 42px; border-radius: 50%; display: grid; place-items: center; color: #fff; background: var(--accent); text-decoration: none; box-shadow: 0 12px 28px rgba(76, 38, 156, .26); }}
    .footer {{ margin-top: 48px; border-top: 2px solid var(--ink); padding-top: 20px; color: var(--muted); font-size: 13px; }}
    .footer__brand {{ color: var(--accent); font-weight: 900; letter-spacing: .06em; }}
    .footer__grid {{ display: grid; grid-template-columns: 1.4fr 1fr; gap: 22px; }}
    @media (min-width: 1161px) and (max-width: 1320px) {{
      .page {{ max-width: calc(100vw - 260px); margin-left: 24px; margin-right: 236px; }}
    }}
    @media (max-width: 1160px) {{
      .floating-nav {{ display: none; }}
    }}
    @media (max-width: 860px) {{
      body {{ line-height: 1.52; -webkit-text-size-adjust: 100%; }}
      .page {{ padding: 16px 14px 46px; }}
      .masthead {{ padding-bottom: 18px; }}
      h1 {{ font-size: 42px; line-height: 1.02; }}
      .dek, .brief__bullets li, .story h3, .story p {{ word-break: keep-all; overflow-wrap: break-word; }}
      .dek {{ font-size: 16px; line-height: 1.55; }}
      .meta-strip {{ gap: 8px 13px; font-size: 12px; }}
      .brief {{ gap: 12px; padding: 18px 0; }}
      .brief h2 {{ font-size: 21px; }}
      .brief-title__eyebrow {{ font-size: 10.5px; }}
      .priority {{ padding-top: 20px; }}
      .priority__head {{ display: block; }}
      .priority__head h2 {{ font-size: 25px; }}
      .priority__head p {{ margin-top: 7px; font-size: 12.5px; }}
      .section h2 {{ font-size: 26px; }}
      .brief__bullets {{ gap: 9px; }}
      .brief__bullets li {{ font-size: 14.5px; line-height: 1.55; }}
      .toc {{ flex-wrap: nowrap; gap: 8px; margin-left: -14px; margin-right: -14px; overflow: hidden; padding: 9px 14px; }}
      .toc__brand {{ padding-right: 0; }}
      .bside-logo--nav {{ gap: 5px; }}
      .bside-logo--nav .bside-logo__image {{ width: 56px; }}
      .bside-logo--nav .bside-logo__label {{ font-size: 8px; letter-spacing: .1em; }}
      .toc__chips {{ flex-wrap: nowrap; gap: 8px; overflow-x: auto; scrollbar-width: none; }}
      .toc__chips::-webkit-scrollbar {{ display: none; }}
      .chip {{ padding: 7px 10px; font-size: 12px; }}
      .chip {{ flex: 0 0 auto; }}
      .archive-panel {{ top: 58px; left: 14px; right: 14px; width: auto; }}
      .archive-panel__card {{ max-height: 54vh; }}
      .mobile-story-nav {{ display: none; }}
      .mobile-story-nav__status {{ display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 9px 14px 5px; color: var(--muted); font-size: 11px; font-weight: 800; }}
      .mobile-story-nav__status strong {{ min-width: 0; overflow: hidden; color: var(--accent-deep); text-overflow: ellipsis; white-space: nowrap; }}
      .mobile-story-nav__status span {{ flex: 0 0 auto; color: var(--accent); font-variant-numeric: tabular-nums; }}
      .mobile-story-nav__links {{ display: grid; gap: 4px; padding: 0 14px 10px; }}
      .mobile-story-nav__links a {{ display: none; min-width: 0; align-items: center; gap: 8px; border: 1px solid var(--line); border-radius: 8px; padding: 6px 9px; background: var(--surface); color: #5f566e; text-decoration: none; overflow: hidden; font-size: 11.5px; line-height: 1.25; }}
      .mobile-story-nav__links a::before {{ content: attr(data-context-label); flex: 0 0 28px; color: var(--muted); font-size: 9.5px; font-weight: 900; letter-spacing: .02em; }}
      .mobile-story-nav__links a.is-mobile-context {{ display: flex; }}
      .mobile-story-nav__links a.is-active {{ border-color: var(--accent); background: var(--accent-soft); color: var(--accent-deep); font-weight: 800; }}
      .mobile-story-nav__links a.is-active::before {{ color: var(--accent); }}
      .mobile-story-nav__links a.is-read:not(.is-active) {{ color: #9a93a5; background: #f8f5fc; }}
      .brief, .featured {{ grid-template-columns: 1fr; }}
      .db-pulse {{ padding: 16px 0 18px; }}
      .db-pulse__head {{ align-items: flex-start; gap: 10px; }}
      .db-pulse__head h2 {{ font-size: 21px; }}
      .db-pulse__head p {{ font-size: 12px; }}
      .db-pulse__list {{ grid-template-columns: 1fr; gap: 4px; }}
      .db-pulse__item h3 {{ font-size: 13.5px; -webkit-line-clamp: 2; }}
      .db-search {{ grid-template-columns: 1fr auto; gap: 7px; }}
      .db-search input {{ font-size: 12.5px; padding: 8px 9px; }}
      .db-search button {{ padding: 0 11px; }}
      .db-search__result h3 {{ font-size: 12.8px; }}
      .brand-row {{ align-items: flex-start; flex-direction: column; }}
      .featured {{ gap: 0; padding: 22px 0; }}
      .featured .story--featured:first-child {{ grid-row: auto; border-right: 0; padding-right: 0; }}
      .featured .story--featured:nth-child(n+2) {{ grid-template-columns: 82px minmax(0, 1fr); gap: 11px; padding: 15px 0; }}
      .section {{ padding-top: 28px; scroll-margin-top: 124px; }}
      .section__head {{ top: 50px; margin-left: -1px; margin-right: -1px; padding: 9px 1px 8px; }}
      .section__head h2 {{ max-width: 72%; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
      .story-list {{ display: grid; grid-template-columns: 1fr; gap: 0; margin-top: 10px; }}
      .story-list .story:first-child {{ grid-column: auto; grid-template-columns: 82px minmax(0, 1fr); }}
      .story, .story--featured {{ display: grid; grid-template-columns: 82px minmax(0, 1fr); gap: 11px; align-items: start; padding: 15px 0; }}
      .story.is-read::after {{ top: 20px; left: 6px; padding: 2px 6px; font-size: 9.5px; }}
      .story--featured {{ border-top: 1px solid var(--line); }}
      .story--featured .story__image {{ aspect-ratio: 4 / 3; }}
      .story--featured h3, .story h3 {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; font-size: 16.5px; line-height: 1.32; font-weight: 800; margin-bottom: 6px; }}
      .story h3 a {{ display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; text-decoration: none; }}
      .story h3 a:focus-visible {{ outline: 2px solid var(--accent); outline-offset: 2px; }}
      .story p {{ display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; margin-bottom: 5px; color: #4a4353; font-size: 13.5px; line-height: 1.45; }}
      .story__summary {{ display: grid; gap: 3px; overflow: visible; padding: 7px 9px; font-size: 12.2px; line-height: 1.42; }}
      .story__summary li {{ display: -webkit-box; -webkit-line-clamp: 1; -webkit-box-orient: vertical; overflow: hidden; padding-left: 10px; }}
      .story__summary li::before {{ content: ""; position: absolute; left: 0; top: .66em; width: 4px; height: 4px; border-radius: 50%; background: var(--accent); }}
      .story-context__body {{ padding: 8px 9px; font-size: 11.5px; line-height: 1.42; }}
      .story-context__timeline a {{ grid-template-columns: 58px minmax(0, 1fr); gap: 7px; }}
      .story__meta {{ flex-wrap: nowrap; gap: 6px; margin-bottom: 5px; overflow: hidden; color: #7a7285; font-size: 10.5px; line-height: 1.3; white-space: nowrap; }}
      .story__meta span {{ min-width: 0; overflow: hidden; text-overflow: ellipsis; }}
      .story__meta span:not(:last-child)::after {{ margin-left: 6px; }}
      .story__sources {{ display: inline-block; max-width: 42%; overflow: hidden; text-overflow: ellipsis; vertical-align: bottom; }}
      .story__sources a {{ display: none; margin-right: 0; }}
      .story__sources a:first-child {{ display: inline; }}
      .story__sources em {{ display: none; }}
      .story__image--logo {{ gap: 5px; padding: 8px; }}
      .story__image--logo span {{ font-size: 9px; }}
      .story__source-logo {{ width: 32px !important; height: 32px !important; border-radius: 8px; padding: 5px; }}
      summary {{ font-size: 12px; }}
      summary::after {{ content: " · 밀어서 보기"; font-size: 10.5px; }}
      .link-table {{ border: 1px solid var(--line); background: var(--surface); overflow-x: auto; overflow-y: hidden; }}
      .link-table table {{ width: 100%; min-width: 620px; table-layout: fixed; border-collapse: collapse; font-size: 11.5px; }}
      .link-table th, .link-table td {{ display: table-cell; padding: 7px 8px; border-bottom: 1px solid var(--line); vertical-align: top; }}
      .link-table thead {{ display: table-header-group; }}
      .link-table tbody {{ display: table-row-group; }}
      .link-table tr {{ display: table-row; }}
      .link-table__time {{ color: var(--muted); white-space: nowrap; }}
      .link-table__source {{ color: var(--accent-deep); overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
      .link-table__title {{ line-height: 1.35; }}
      .footer__grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body id="top">
  <aside class="floating-nav" aria-label="데일리 네비게이션">
    <div class="floating-nav__meta" aria-label="발행 및 수집 정보">
      <div class="floating-nav__meta-item">
        <span>발행일자</span>
        <strong>{report_date_label}</strong>
      </div>
      <div class="floating-nav__meta-item">
        <span>수집기간</span>
        <strong>{start_label}</strong>
        <em>{end_label}</em>
      </div>
      <button class="archive-trigger floating-nav__archive" type="button" data-archive-toggle aria-expanded="false" aria-controls="archive-panel">다른 일자 보기</button>
    </div>
    <h2>검색 유형</h2>
    {side_category_links}
    <div class="floating-nav__stories">
      <h2>기사 목록</h2>
      {side_story_links}
    </div>
  </aside>
  <a class="top-button" href="#top" aria-label="맨 위로">↑</a>
  <div class="page">
    <header class="masthead">
      <div class="brand-row">
        {header_logo}
        <div class="edition">{start_label} - {end_label}</div>
      </div>
      <h1>주주·자본시장 데일리</h1>
      <p class="dek">건강한 자본시장을 위한 주주행동, 지배구조, 밸류업, 자본시장 제도 뉴스를 하루 단위로 묶어 읽기 쉽게 정리했습니다.</p>
      <div class="meta-strip" aria-label="report stats">
        <span><strong>{stats['stories']}</strong>개 이슈</span>
        <span><strong>{stats['articles']}</strong>건 기사</span>
        <span><strong>{stats['sources']}</strong>개 매체</span>
        <a href="workbench.html">AI 워크벤치 보기</a>
        <button class="archive-trigger" type="button" data-archive-toggle aria-expanded="false" aria-controls="archive-panel">다른 일자 보기</button>
      </div>
    </header>

    <section class="brief">
      <h2>{brief_title_html}</h2>
      <div>{review_block_html}</div>
    </section>

    <section class="db-pulse" data-db-pulse hidden aria-label="이슈 레이더">
      <div class="db-pulse__head">
        <div>
          <h2>이슈 레이더</h2>
          <p>아카이브에 누적된 최근 기사 중 후속 확인이 필요한 흐름을 보여줍니다.</p>
        </div>
        <span class="db-pulse__badge" data-db-pulse-status>최근 흐름</span>
      </div>
      <div class="db-pulse__list" data-db-pulse-list></div>
      <form class="db-search" data-db-search>
        <input type="search" name="q" autocomplete="off" placeholder="아카이브 검색: 고려아연, 유상증자, 소액주주">
        <button type="submit">검색</button>
      </form>
      <div class="db-search__results" data-db-search-results hidden></div>
    </section>

    <nav class="toc" aria-label="report sections">
      <div class="toc__brand">{nav_logo}</div>
      <div class="toc__chips">{toc}</div>
    </nav>
    <div class="mobile-story-nav" aria-label="현재 섹션 기사 네비게이션">
      <div class="mobile-story-nav__status">
        <strong data-mobile-section-label>섹션</strong>
        <span data-mobile-progress>0/0</span>
      </div>
      <div class="mobile-story-nav__links">
        {mobile_story_links}
      </div>
    </div>

    {featured_block_html}

    {''.join(category_sections)}

    <footer class="footer">
      <div class="footer__grid">
        <div>
          {footer_logo}
          <p>건강한 자본시장을 위한 주주행동과 투자자 커뮤니케이션을 지향합니다. 이 페이지는 공개 뉴스와 RSS를 자동으로 큐레이션한 데일리이며 투자 조언이나 매매 권유가 아닙니다.</p>
        </div>
        <div>
          <p>문의: support@bside.ai</p>
          <p>원문 기사와 이미지는 각 언론사와 발행자에게 권리가 있습니다.</p>
        </div>
      </div>
    </footer>
  </div>
  <div class="archive-panel" id="archive-panel" data-archive-panel hidden>
    <div class="archive-panel__card" role="dialog" aria-label="다른 일자 선택">
      <div class="archive-panel__head">
        <span>다른 일자 선택</span>
        <button class="archive-panel__close" type="button" data-archive-close aria-label="닫기">×</button>
      </div>
      <div class="archive-panel__links">
        {archive_links_html}
      </div>
    </div>
  </div>
  <script>
    function attachSourceLogoGuard(container) {{
      container.querySelectorAll('.story__source-logo').forEach((logo) => {{
        logo.addEventListener('error', () => logo.remove(), {{ once: true }});
        if (logo.complete && logo.naturalWidth === 0) logo.remove();
      }});
    }}

    function replaceWithSourceLogo(container) {{
      const label = container.dataset.logoLabel || 'NO IMAGE';
      const logoSrc = container.dataset.logoSrc || '';
      container.classList.add('story__image--logo', 'story__image--broken');
      container.classList.remove('story__image--empty');
      container.innerHTML = '';
      if (logoSrc) {{
        const logo = new Image();
        logo.className = 'story__source-logo';
        logo.src = logoSrc;
        logo.alt = '';
        logo.loading = 'lazy';
        logo.decoding = 'async';
        logo.referrerPolicy = 'no-referrer';
        logo.addEventListener('error', () => logo.remove(), {{ once: true }});
        container.appendChild(logo);
      }}
      const text = document.createElement('span');
      text.textContent = label;
      container.appendChild(text);
    }}

    function imageCandidates(container) {{
      try {{
        const candidates = JSON.parse(container.dataset.imageCandidates || '[]');
        if (!Array.isArray(candidates)) return [];
        return candidates.filter((url, index) => typeof url === 'string' && url.startsWith('http') && candidates.indexOf(url) === index);
      }} catch (error) {{
        return [];
      }}
    }}

    function promoteCandidateImage(container) {{
      const candidates = imageCandidates(container);
      if (!candidates.length) return null;
      container.dataset.imageIndex = '0';
      container.classList.remove('story__image--logo', 'story__image--empty', 'story__image--broken');
      container.innerHTML = '';
      const image = new Image();
      image.src = candidates[0];
      image.alt = '';
      image.loading = 'lazy';
      image.decoding = 'async';
      image.referrerPolicy = 'no-referrer';
      container.appendChild(image);
      return image;
    }}

    function tryNextImageCandidate(container, image) {{
      const candidates = imageCandidates(container);
      let currentIndex = Number(container.dataset.imageIndex || '0');
      const currentSrc = image.currentSrc || image.src || '';
      if (candidates[currentIndex] && currentSrc && currentSrc !== candidates[currentIndex]) {{
        currentIndex = Math.max(candidates.indexOf(currentSrc), currentIndex);
      }}
      for (let nextIndex = currentIndex + 1; nextIndex < candidates.length; nextIndex += 1) {{
        if (!candidates[nextIndex] || candidates[nextIndex] === currentSrc) continue;
        container.dataset.imageIndex = String(nextIndex);
        image.src = candidates[nextIndex];
        return true;
      }}
      return false;
    }}

    document.querySelectorAll('.story__image').forEach((container) => {{
      attachSourceLogoGuard(container);
      let image = container.querySelector('img:not(.story__source-logo)');
      if (!image) image = promoteCandidateImage(container);
      if (!image) return;
      const markBroken = () => {{
        if (!tryNextImageCandidate(container, image)) replaceWithSourceLogo(container);
      }};
      const fallbackTimer = window.setTimeout(() => {{
        if (!image.complete || image.naturalWidth === 0) markBroken();
      }}, 8000);
      image.addEventListener('load', () => window.clearTimeout(fallbackTimer), {{ once: true }});
      image.addEventListener('error', markBroken);
      if (image.complete && image.naturalWidth === 0) markBroken();
    }});

    const sections = Array.from(document.querySelectorAll('[data-section]'));
    const sectionStories = Array.from(document.querySelectorAll('[data-story][data-section-key]'));
    const categoryLinks = Array.from(document.querySelectorAll('[data-toc-section], [data-nav-section]'));
    const desktopStoryLinks = Array.from(document.querySelectorAll('[data-nav-story]'));
    const mobileStoryLinks = Array.from(document.querySelectorAll('[data-mobile-nav-story]'));
    const storyLinks = [...desktopStoryLinks, ...mobileStoryLinks];
    const mobileArticleLinkQuery = window.matchMedia('(max-width: 860px)');
    const mobileSectionLabel = document.querySelector('[data-mobile-section-label]');
    const mobileProgress = document.querySelector('[data-mobile-progress]');
    const archivePanel = document.querySelector('[data-archive-panel]');
    const archiveToggles = Array.from(document.querySelectorAll('[data-archive-toggle]'));
    const archiveClose = document.querySelector('[data-archive-close]');
    const archiveLinksContainer = document.querySelector('.archive-panel__links');
    const dbPulse = document.querySelector('[data-db-pulse]');
    const dbPulseList = document.querySelector('[data-db-pulse-list]');
    const dbPulseStatus = document.querySelector('[data-db-pulse-status]');
    const dbSearchForm = document.querySelector('[data-db-search]');
    const dbSearchResults = document.querySelector('[data-db-search-results]');
    const storyContextDetails = Array.from(document.querySelectorAll('[data-story-context]'));
    const remoteReportsApiUrl = {read_api_url_json};
    const currentReportDateId = {date_id_json};
    const readStorageKey = `bside-daily-read:${{location.pathname}}`;
    let readStoryIds = new Set();

    try {{
      readStoryIds = new Set(JSON.parse(localStorage.getItem(readStorageKey) || '[]'));
    }} catch (error) {{
      readStoryIds = new Set();
    }}

    function sectionIdForLink(link) {{
      return link.dataset.tocSection || link.dataset.sectionTarget || (link.getAttribute('href') || '').replace('#', '');
    }}

    function applyResponsiveArticleLinks() {{
      const useMobileUrls = mobileArticleLinkQuery.matches;
      document.querySelectorAll('a[data-mobile-url]').forEach((link) => {{
        if (!link.dataset.desktopUrl) link.dataset.desktopUrl = link.getAttribute('href') || '';
        link.setAttribute('href', useMobileUrls ? link.dataset.mobileUrl : link.dataset.desktopUrl);
      }});
    }}

    function progressLinks(sectionId) {{
      return categoryLinks.filter((link) => sectionIdForLink(link) === sectionId);
    }}

    function setSectionProgress(sectionId, index, total) {{
      const ratio = total ? Math.max(0, Math.min(1, index / total)) : 0;
      progressLinks(sectionId).forEach((link) => {{
        link.style.setProperty('--progress', String(ratio));
        const progress = link.querySelector('[data-progress-text]');
        if (progress) progress.textContent = `${{index}}/${{total}}`;
      }});
    }}

    function updateStoryWindowForLinks(links, activeStoryId) {{
      if (!links.length) return;
      const activeIndex = visualStoryIndexByHref.get(activeStoryId) ?? 0;
      links.forEach((link, index) => {{
        const linkIndex = visualStoryIndexByHref.get(link.getAttribute('href') || '') ?? index;
        const isNear = Math.abs(linkIndex - activeIndex) <= 4;
        link.classList.toggle('is-near-active', isNear);
      }});
    }}

    function updateMobileStoryContext(activeStoryId) {{
      if (!mobileStoryLinks.length) return;
      const activeIndex = visualStoryIndexByHref.get(activeStoryId) ?? 0;
      const contextLabels = new Map([
        [activeIndex - 1, '이전'],
        [activeIndex, '현재'],
        [activeIndex + 1, '다음'],
      ]);
      mobileStoryLinks.forEach((link) => {{
        const linkIndex = visualStoryIndexByHref.get(link.getAttribute('href') || '') ?? Number(link.dataset.navStoryIndex || 0);
        const label = contextLabels.get(linkIndex) || '';
        link.classList.toggle('is-mobile-context', Boolean(label));
        if (label) link.dataset.contextLabel = label;
        else delete link.dataset.contextLabel;
      }});
    }}

    function updateStoryWindow(activeStoryId) {{
      updateStoryWindowForLinks(desktopStoryLinks, activeStoryId);
      updateMobileStoryContext(activeStoryId);
    }}

    function applyReadState(storyId) {{
      if (!storyId) return;
      const story = document.getElementById(storyId);
      if (story) story.classList.add('is-read');
      storyLinks.forEach((link) => {{
        link.classList.toggle('is-read', link.getAttribute('href') === `#${{storyId}}` || link.classList.contains('is-read'));
      }});
    }}

    function saveReadState() {{
      try {{
        localStorage.setItem(readStorageKey, JSON.stringify(Array.from(readStoryIds).slice(-500)));
      }} catch (error) {{}}
    }}

    function markStoryRead(storyId) {{
      if (!storyId) return;
      readStoryIds.add(storyId);
      applyReadState(storyId);
      saveReadState();
    }}

    function setArchiveOpen(open) {{
      if (!archivePanel) return;
      archivePanel.hidden = !open;
      archiveToggles.forEach((toggle) => toggle.setAttribute('aria-expanded', open ? 'true' : 'false'));
    }}

    function apiUrlWithAction(baseUrl, action) {{
      if (!baseUrl) return '';
      const separator = baseUrl.includes('?') ? '&' : '?';
      return `${{baseUrl}}${{separator}}action=${{encodeURIComponent(action)}}`;
    }}

    function renderRemoteArchiveLinks(reports) {{
      if (!archiveLinksContainer || !Array.isArray(reports) || !reports.length) return;
      archiveLinksContainer.innerHTML = '';
      reports.forEach((report) => {{
        const dateId = String(report.date_id || '').slice(0, 10);
        if (!/^\\d{{4}}-\\d{{2}}-\\d{{2}}$/.test(dateId)) return;
        const link = document.createElement('a');
        link.className = `archive-panel__link${{dateId === currentReportDateId ? ' is-current' : ''}}`;
        link.href = report.public_url || `${{dateId}}.html`;
        link.textContent = dateId;
        const label = document.createElement('span');
        label.textContent = dateId === currentReportDateId ? '현재' : `${{Number(report.article_count || 0)}}건`;
        link.appendChild(label);
        archiveLinksContainer.appendChild(link);
      }});
      if (!archiveLinksContainer.children.length) {{
        const empty = document.createElement('span');
        empty.className = 'archive-panel__empty';
        empty.textContent = '아직 발행된 데일리가 없습니다.';
        archiveLinksContainer.appendChild(empty);
      }}
    }}

    function compactDbText(value, maxChars) {{
      const text = String(value || '').replace(/\\s+/g, ' ').trim();
      if (text.length <= maxChars) return text;
      return `${{text.slice(0, Math.max(0, maxChars - 1)).trim()}}…`;
    }}

    function storyStatusLabel(story) {{
      const status = String(story.status || '').toLowerCase();
      if (status === 'published') return '발행';
      if (status === 'pending') return '대기';
      if (status === 'clustered') return '묶음';
      return status || '수집';
    }}

    function storyDateLabel(story) {{
      const raw = String(story.published_at || story.last_article_seen_at || '').trim();
      const match = raw.match(/^(\\d{{4}})-(\\d{{2}})-(\\d{{2}})\\s+(\\d{{2}}):(\\d{{2}})/);
      if (!match) return '';
      return `${{match[2]}}.${{match[3]}} ${{match[4]}}:${{match[5]}}`;
    }}

    function articleDateLabel(article) {{
      const raw = String(article.published_at || article.seen_at || article.sort_at || '').trim();
      const match = raw.match(/^(\\d{{4}})-(\\d{{2}})-(\\d{{2}})\\s+(\\d{{2}}):(\\d{{2}})/);
      if (!match) return '';
      return `${{match[2]}}.${{match[3]}} ${{match[4]}}:${{match[5]}}`;
    }}

    function articleStatusLabel(article) {{
      const status = String(article.status || '').toLowerCase();
      if (status === 'published') return '발행';
      if (status === 'accepted') return '수집';
      if (status === 'pending') return '대기';
      if (status === 'duplicate') return '중복';
      return status || '수집';
    }}

    function isGenericDbPulseTitle(value) {{
      const title = String(value || '').replace(/\\s+/g, ' ').trim();
      if (!title) return true;
      const genericTitles = new Set([
        '밸류업·주주환원·지배구조',
        '주주행동·경영권',
        '자본시장 제도·공시',
        '해외·영문',
      ]);
      if (genericTitles.has(title)) return true;
      return title.length <= 28 && /^[0-9A-Za-z가-힣]+(?:[·/|][0-9A-Za-z가-힣]+)+$/.test(title);
    }}

    function searchTokens(query) {{
      return String(query || '')
        .match(/[0-9A-Za-z가-힣]{{2,}}/g)?.map((token) => token.toLowerCase())
        .filter((token, index, list) => list.indexOf(token) === index)
        .slice(0, 5) || [];
    }}

    function articleMatchReasons(article, query) {{
      const tokens = searchTokens(query);
      const title = String(article.title || '').toLowerCase();
      const summary = String(article.summary || '').toLowerCase();
      const source = String(article.source || article.feed_name || '').toLowerCase();
      const feed = String(article.feed_category || article.relevance_level || article.priority_level || '').toLowerCase();
      const reasons = [];
      if (tokens.some((token) => title.includes(token))) reasons.push('제목 일치');
      if (tokens.some((token) => summary.includes(token))) reasons.push('요약 일치');
      if (tokens.some((token) => source.includes(token))) reasons.push('매체 일치');
      if (tokens.some((token) => feed.includes(token))) reasons.push('분류 일치');
      return reasons.length ? reasons.slice(0, 3) : ['관련도순'];
    }}

    function articleSearchSnippet(article, query) {{
      const text = String(article.summary || article.title || '').replace(/\\s+/g, ' ').trim();
      if (!text) return '';
      const tokens = searchTokens(query);
      const lower = text.toLowerCase();
      const hit = tokens.find((token) => lower.includes(token));
      if (!hit) return compactDbText(text, 118);
      const index = Math.max(0, lower.indexOf(hit) - 34);
      const snippet = text.slice(index, index + 124);
      return `${{index > 0 ? '…' : ''}}${{compactDbText(snippet, 118)}}${{index + 124 < text.length ? '…' : ''}}`;
    }}

    function renderDbPulse(stories) {{
      if (!dbPulse || !dbPulseList || !Array.isArray(stories)) return;
      const items = stories
        .filter((story) => story && story.representative_title && story.representative_url)
        .filter((story) => !isGenericDbPulseTitle(story.representative_title))
        .slice(0, 6);
      if (!items.length) return;
      dbPulseList.innerHTML = '';
      items.forEach((story) => {{
        const link = document.createElement('a');
        link.className = 'db-pulse__item';
        link.href = story.representative_url;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        const title = document.createElement('h3');
        title.textContent = compactDbText(story.representative_title, 86);
        const meta = document.createElement('div');
        meta.className = 'db-pulse__meta';
        const status = document.createElement('strong');
        status.textContent = storyStatusLabel(story);
        meta.appendChild(status);
        const count = document.createElement('span');
        count.textContent = `${{Number(story.article_count || 1)}}건`;
        meta.appendChild(count);
        const priority = Number(story.priority_score || 0);
        if (priority) {{
          const score = document.createElement('span');
          score.textContent = `점수 ${{priority}}`;
          meta.appendChild(score);
        }}
        const date = storyDateLabel(story);
        if (date) {{
          const dateEl = document.createElement('span');
          dateEl.textContent = date;
          meta.appendChild(dateEl);
        }}
        link.appendChild(title);
        link.appendChild(meta);
        dbPulseList.appendChild(link);
      }});
      if (dbPulseStatus) dbPulseStatus.textContent = `${{items.length}}개 이슈`;
      dbPulse.hidden = false;
    }}

    async function loadDbPulse() {{
      if (!remoteReportsApiUrl || !dbPulse || !dbPulseList) return;
      try {{
        const response = await fetch(`${{apiUrlWithAction(remoteReportsApiUrl, 'latest_snapshot')}}&limit=8`, {{
          headers: {{ 'Accept': 'application/json' }},
          credentials: 'omit',
        }});
        if (!response.ok) return;
        const data = await response.json();
        if (data && data.ok) renderDbPulse(data.stories || []);
      }} catch (error) {{}}
    }}

    function showDbSearchMessage(message) {{
      if (!dbSearchResults) return;
      dbSearchResults.innerHTML = '';
      const item = document.createElement('div');
      item.className = 'db-search__message';
      item.textContent = message;
      dbSearchResults.appendChild(item);
      dbSearchResults.hidden = false;
    }}

    function renderDbSearchResults(articles, query) {{
      if (!dbSearchResults) return;
      const items = Array.isArray(articles) ? articles.filter((article) => article && article.title && article.canonical_url).slice(0, 8) : [];
      dbSearchResults.innerHTML = '';
      if (!items.length) {{
        showDbSearchMessage(`'${{query}}' 검색 결과가 없습니다.`);
        return;
      }}
      const message = document.createElement('div');
      message.className = 'db-search__message';
      message.textContent = `'${{query}}' 검색 결과 ${{items.length}}건`;
      dbSearchResults.appendChild(message);
      items.forEach((article) => {{
        const link = document.createElement('a');
        link.className = 'db-search__result';
        link.href = article.canonical_url;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        const title = document.createElement('h3');
        title.textContent = compactDbText(article.title, 96);
        const meta = document.createElement('div');
        meta.className = 'db-search__meta';
        [articleDateLabel(article), article.source || article.feed_name || '', articleStatusLabel(article), article.relevance_level || '', article.priority_level || ''].filter(Boolean).forEach((value) => {{
          const span = document.createElement('span');
          span.textContent = String(value);
          meta.appendChild(span);
        }});
        const summary = document.createElement('p');
        summary.className = 'db-search__summary';
        summary.textContent = articleSearchSnippet(article, query);
        const reasons = document.createElement('div');
        reasons.className = 'db-search__why';
        articleMatchReasons(article, query).forEach((reason) => {{
          const chip = document.createElement('span');
          chip.textContent = reason;
          reasons.appendChild(chip);
        }});
        link.appendChild(title);
        link.appendChild(meta);
        if (summary.textContent) link.appendChild(summary);
        link.appendChild(reasons);
        dbSearchResults.appendChild(link);
      }});
      dbSearchResults.hidden = false;
    }}

    async function searchDbArticles(query) {{
      if (!remoteReportsApiUrl || !dbSearchResults) return;
      const cleaned = String(query || '').replace(/\\s+/g, ' ').trim();
      if (cleaned.length < 2) {{
        showDbSearchMessage('검색어를 2자 이상 입력해주세요.');
        return;
      }}
      showDbSearchMessage('아카이브를 검색하는 중입니다.');
      try {{
        const response = await fetch(`${{apiUrlWithAction(remoteReportsApiUrl, 'articles')}}&q=${{encodeURIComponent(cleaned)}}&limit=8&days=90`, {{
          headers: {{ 'Accept': 'application/json' }},
          credentials: 'omit',
        }});
        if (!response.ok) {{
          showDbSearchMessage('아카이브 검색을 불러오지 못했습니다.');
          return;
        }}
        const data = await response.json();
        if (data && data.ok) renderDbSearchResults(data.articles || [], cleaned);
        else showDbSearchMessage('아카이브 검색을 불러오지 못했습니다.');
      }} catch (error) {{
        showDbSearchMessage('아카이브 검색을 불러오지 못했습니다.');
      }}
    }}

    function articleUrlKey(value) {{
      const raw = String(value || '').trim();
      if (!raw) return '';
      try {{
        const url = new URL(raw, location.href);
        url.hash = '';
        return `${{url.origin}}${{url.pathname.replace(/\\/$/, '')}}${{url.search}}`.toLowerCase();
      }} catch (error) {{
        return raw.replace(/#.*$/, '').replace(/\\/$/, '').toLowerCase();
      }}
    }}

    function normalizedContextTitle(value) {{
      return String(value || '')
        .toLowerCase()
        .replace(/\\s+-\\s+[^-·|]+$/, '')
        .replace(/[\\[\\]()"“”'‘’·….,:;!?~\\-_/|]/g, ' ')
        .replace(/\\s+/g, ' ')
        .trim();
    }}

    function contextArticleKey(article) {{
      const titleKey = normalizedContextTitle(article.title);
      if (titleKey.length >= 12) return `title:${{titleKey}}`;
      return `url:${{articleUrlKey(article.canonical_url)}}`;
    }}

    function contextArticleQuality(article) {{
      const url = String(article.canonical_url || '').toLowerCase();
      let score = 0;
      if (!url.includes('news.google.com')) score += 3;
      if (!url.includes('google.com/rss')) score += 1;
      if (article.summary) score += 1;
      if (article.image_url) score += 1;
      return score;
    }}

    async function fetchDbArticles(params) {{
      if (!remoteReportsApiUrl) return [];
      const query = new URLSearchParams(params);
      try {{
        const response = await fetch(`${{apiUrlWithAction(remoteReportsApiUrl, 'articles')}}&${{query.toString()}}`, {{
          headers: {{ 'Accept': 'application/json' }},
          credentials: 'omit',
        }});
        if (!response.ok) return [];
        const data = await response.json();
        return data && data.ok && Array.isArray(data.articles) ? data.articles : [];
      }} catch (error) {{
        return [];
      }}
    }}

    async function fetchTelegramMentions(story) {{
      if (!remoteReportsApiUrl || !story) return [];
      const params = new URLSearchParams();
      const url = String(story.dataset.storyUrl || '').trim();
      const query = String(story.dataset.storyDbQuery || story.querySelector('h3')?.textContent || '').trim();
      if (url) params.set('url', url);
      if (query) params.set('q', query);
      params.set('limit', '5');
      try {{
        const response = await fetch(`${{apiUrlWithAction(remoteReportsApiUrl, 'telegram_reactions')}}&${{params.toString()}}`, {{
          headers: {{ 'Accept': 'application/json' }},
          credentials: 'omit',
        }});
        if (!response.ok) return [];
        const data = await response.json();
        const messages = data.messages || data.telegram_messages || data.reactions || [];
        return data && data.ok && Array.isArray(messages) ? messages : [];
      }} catch (error) {{
        return [];
      }}
    }}

    function mergeContextArticles(batches) {{
      const seen = new Map();
      batches.flat().forEach((article) => {{
        if (!article || !article.canonical_url || !article.title) return;
        const key = contextArticleKey(article);
        if (!key) return;
        const previous = seen.get(key);
        if (!previous || contextArticleQuality(article) > contextArticleQuality(previous)) seen.set(key, article);
      }});
      const merged = Array.from(seen.values());
      return merged.sort((left, right) => String(right.sort_at || right.published_at || '').localeCompare(String(left.sort_at || left.published_at || '')));
    }}

    function sourceSpread(articles) {{
      const counts = new Map();
      articles.forEach((article) => {{
        const source = String(article.source || article.feed_name || '매체 미상').trim();
        counts.set(source, (counts.get(source) || 0) + 1);
      }});
      return Array.from(counts.entries()).sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0]));
    }}

    function contextDateRange(articles) {{
      const labels = articles.map(articleDateLabel).filter(Boolean);
      if (!labels.length) return '';
      const sorted = labels.slice().sort();
      return sorted[0] === sorted[sorted.length - 1] ? sorted[0] : `${{sorted[0]}}-${{sorted[sorted.length - 1]}}`;
    }}

    function contextFilterTokens(query, title = '') {{
      const generic = new Set([
        '관련', '기사', '보도', '뉴스', '시장', '자본시장', '주주', '기업', '증시', '한국어',
        '밸류업', '주주환원', '자사주', '소각', '지배구조', '경영권', '분쟁', '소액주주',
        '공시', '제도', '거래소', '코스닥', '상장', '중복상장', '유상증자', '물적분할',
        '종료보고서', '제출', '불성실공시법인', '지정', 'google', 'news'
      ]);
      const rawTokens = `${{title || ''}} ${{query || ''}}`.match(/[0-9A-Za-z가-힣]{{2,}}/g) || [];
      const tokens = [];
      rawTokens.forEach((token) => {{
        const normalized = token.toLowerCase();
        if (generic.has(normalized) || tokens.includes(token)) return;
        tokens.push(token);
      }});
      return tokens.slice(0, 6);
    }}

    function isWeakContextToken(token) {{
      return new Set([
        '밸류업', '주주환원', '자사주', '소각', '지배구조', '경영권', '분쟁', '소액주주',
        '공시', '제도', '거래소', '코스닥', '상장', '중복상장', '유상증자', '물적분할',
        '종료보고서', '불성실공시법인', '감독', '제재'
      ]).has(String(token || '').toLowerCase());
    }}

    function articleMatchesContext(article, tokens) {{
      if (!tokens.length) return false;
      const text = `${{article.title || ''}} ${{article.summary || ''}} ${{article.source || article.feed_name || ''}}`.toLowerCase();
      const hits = tokens.filter((token) => text.includes(token.toLowerCase()));
      const strongHits = hits.filter((token) => token.length >= 3 && !isWeakContextToken(token));
      return strongHits.length >= 1 || hits.length >= Math.min(3, Math.max(2, tokens.length));
    }}

    function storyContextHasCurrentLinks(details) {{
      return Boolean(details.querySelector('[data-story-current-links]'));
    }}

    function staticTelegramMentions(details) {{
      const script = details.querySelector('[data-story-telegram-mentions]');
      if (!script) return [];
      try {{
        const mentions = JSON.parse(script.textContent || '[]');
        return Array.isArray(mentions) ? mentions : [];
      }} catch (error) {{
        return [];
      }}
    }}

    function mergeTelegramMentions(batches) {{
      const seen = new Map();
      batches.flat().forEach((message) => {{
        if (!message || !(message.message_url || message.url)) return;
        const key = message.message_url || message.url;
        if (!seen.has(key)) seen.set(key, message);
      }});
      return Array.from(seen.values()).slice(0, 5);
    }}

    function currentContextArticles(details) {{
      const script = details.querySelector('[data-story-current-links]');
      if (!script) return [];
      try {{
        const links = JSON.parse(script.textContent || '[]');
        if (!Array.isArray(links)) return [];
        return links
          .filter((link) => link && link.url && link.title)
          .map((link) => ({{
            canonical_url: link.url,
            title: link.title,
            source: link.source || link.domain || '',
            feed_name: link.source || link.domain || '',
            published_at: link.published_at || '',
            sort_at: link.published_at || '',
            context_kind: 'current',
          }}));
      }} catch (error) {{
        return [];
      }}
    }}

    function contextKindLabel(article) {{
      return article.context_kind === 'current' ? '현재 묶음' : '아카이브';
    }}

    function telegramMatchLabel(value) {{
      const type = String(value || '');
      if (type === 'exact_url' || type === 'canonical_url') return 'URL 직접';
      if (type === 'ticker') return '종목 추정';
      if (type === 'keyword') return '키워드 추정';
      return '관련 언급';
    }}

    function renderTelegramMentions(body, mentions) {{
      const items = Array.isArray(mentions) ? mentions.filter((message) => message && (message.message_url || message.url) && (message.text || message.excerpt)).slice(0, 5) : [];
      if (!items.length) return;
      const section = document.createElement('div');
      section.className = 'story-context__telegram';
      const head = document.createElement('div');
      head.className = 'story-context__telegram-head';
      const title = document.createElement('strong');
      title.textContent = 'Telegram 언급';
      const count = document.createElement('span');
      const channelCount = new Set(items.map((item) => item.channel_title || item.channel_handle || item.handle).filter(Boolean)).size;
      count.textContent = `${{items.length}}건 · 채널 ${{channelCount}}곳`;
      head.appendChild(title);
      head.appendChild(count);
      section.appendChild(head);
      const list = document.createElement('ul');
      list.className = 'story-context__telegram-list';
      items.forEach((item) => {{
        const row = document.createElement('li');
        const link = document.createElement('a');
        link.href = item.message_url || item.url;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        const meta = document.createElement('div');
        meta.className = 'story-context__telegram-meta';
        [item.channel_title || item.channel_handle || item.handle || '공개 채널', item.posted_at || '', telegramMatchLabel(item.match_type), ...(Array.isArray(item.risk_flags) ? item.risk_flags : [])].filter(Boolean).forEach((value) => {{
          const span = document.createElement('span');
          span.textContent = String(value);
          meta.appendChild(span);
        }});
        const excerpt = document.createElement('p');
        excerpt.textContent = compactDbText(item.excerpt || item.text || '', 120);
        link.appendChild(meta);
        link.appendChild(excerpt);
        row.appendChild(link);
        list.appendChild(row);
      }});
      section.appendChild(list);
      body.appendChild(section);
    }}

    function renderStoryContext(details, storyArticles, queryArticles, telegramMentions = []) {{
      const body = details.querySelector('[data-story-context-body]');
      const story = details.closest('[data-story]');
      if (!body || !story) return;
      const currentKey = articleUrlKey(story.dataset.storyUrl || story.querySelector('h3 a')?.href || '');
      const storyTitle = story.querySelector('h3')?.textContent || '';
      const filterTokens = contextFilterTokens(story.dataset.storyDbQuery || '', storyTitle);
      const isNotCurrent = (article) => articleUrlKey(article.canonical_url) !== currentKey;
      const currentItems = mergeContextArticles([currentContextArticles(details)]).map((article) => ({{ ...article, context_kind: 'current' }}));
      const currentKeys = new Set(currentItems.map((article) => contextArticleKey(article)).filter(Boolean));
      const isNotCurrentGroup = (article) => !currentKeys.has(contextArticleKey(article));
      const storyItems = mergeContextArticles([storyArticles])
        .filter(isNotCurrent)
        .filter(isNotCurrentGroup)
        .filter((article) => articleMatchesContext(article, filterTokens))
        .map((article) => ({{ ...article, context_kind: 'archive' }}));
      const queryItems = mergeContextArticles([queryArticles])
        .filter(isNotCurrent)
        .filter(isNotCurrentGroup)
        .filter((article) => articleMatchesContext(article, filterTokens));
      const archiveItems = mergeContextArticles([storyItems, queryItems])
        .map((article) => ({{ ...article, context_kind: 'archive' }}))
        .slice(0, Math.max(0, 10 - currentItems.length));
      const items = [...currentItems, ...archiveItems];
      body.innerHTML = '';
      if (!items.length && !telegramMentions.length) {{
        if (storyContextHasCurrentLinks(details)) {{
          body.hidden = true;
        }} else {{
          details.open = false;
          details.hidden = true;
          details.dataset.empty = '1';
        }}
        return;
      }}
      body.hidden = false;

      if (!items.length) {{
        renderTelegramMentions(body, telegramMentions);
        return;
      }}

      const spread = sourceSpread(items);
      const stats = document.createElement('div');
      stats.className = 'story-context__stats';
      [
        currentItems.length ? `현재 묶음 ${{currentItems.length}}건` : '',
        archiveItems.length ? `아카이브 ${{archiveItems.length}}건` : '',
        `매체 ${{spread.length}}곳`,
        contextDateRange(items),
      ].filter(Boolean).forEach((label) => {{
        const chip = document.createElement('span');
        chip.className = 'story-context__stat';
        chip.textContent = label;
        stats.appendChild(chip);
      }});
      body.appendChild(stats);

      const spreadLine = document.createElement('div');
      spreadLine.className = 'story-context__spread';
      const spreadTitle = document.createElement('strong');
      spreadTitle.textContent = '확산';
      spreadLine.appendChild(spreadTitle);
      spread.slice(0, 5).forEach(([source, count]) => {{
        const item = document.createElement('span');
        item.textContent = `${{source}} ${{count}}`;
        spreadLine.appendChild(item);
      }});
      body.appendChild(spreadLine);

      const tableWrap = document.createElement('div');
      tableWrap.className = 'link-table story-context__table';
      const table = document.createElement('table');
      table.innerHTML = '<thead><tr><th>구분</th><th>일시</th><th>매체</th><th>기사</th></tr></thead><tbody></tbody>';
      const tbody = table.querySelector('tbody');
      items.forEach((article) => {{
        const row = document.createElement('tr');
        row.className = article.context_kind === 'current' ? 'story-context__row--current' : 'story-context__row--archive';
        const kindCell = document.createElement('td');
        const kind = document.createElement('span');
        kind.className = `story-context__kind${{article.context_kind === 'archive' ? ' story-context__kind--archive' : ''}}`;
        kind.textContent = contextKindLabel(article);
        kindCell.appendChild(kind);
        const timeCell = document.createElement('td');
        timeCell.textContent = articleDateLabel(article) || '일시 미상';
        const sourceCell = document.createElement('td');
        sourceCell.textContent = article.source || article.feed_name || '매체 미상';
        const titleCell = document.createElement('td');
        const link = document.createElement('a');
        link.href = article.canonical_url;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        link.textContent = compactDbText(article.title, 96);
        titleCell.appendChild(link);
        row.appendChild(kindCell);
        row.appendChild(timeCell);
        row.appendChild(sourceCell);
        row.appendChild(titleCell);
        tbody.appendChild(row);
      }});
      tableWrap.appendChild(table);
      body.appendChild(tableWrap);
      renderTelegramMentions(body, telegramMentions);
    }}

    async function loadStoryContext(details) {{
      if (details.dataset.loaded === '1') return;
      const body = details.querySelector('[data-story-context-body]');
      const story = details.closest('[data-story]');
      if (!body || !story) return;
      body.innerHTML = '<div class="story-context__message">아카이브에서 관련 흐름을 불러오는 중입니다.</div>';
      if (!remoteReportsApiUrl) {{
        renderStoryContext(details, [], []);
        details.dataset.loaded = '1';
        return;
      }}
      const storyKey = String(story.dataset.storyDbKey || '').trim();
      const query = String(story.dataset.storyDbQuery || '').trim();
      let storyArticles = [];
      let queryArticles = [];
      let telegramMentions = staticTelegramMentions(details);
      if (storyKey) {{
        storyArticles = await fetchDbArticles({{ story_key: storyKey, limit: '16', days: '180' }});
      }}
      if (query) {{
        const currentItems = mergeContextArticles([storyArticles]);
        if (currentItems.length < 4) {{
          queryArticles = await fetchDbArticles({{ q: query, limit: '12', days: '180' }});
        }}
      }}
      telegramMentions = mergeTelegramMentions([telegramMentions, await fetchTelegramMentions(story)]);
      renderStoryContext(details, storyArticles, queryArticles, telegramMentions);
      details.dataset.loaded = '1';
    }}

    function hideUnavailableStoryContexts() {{
      if (remoteReportsApiUrl) return;
      storyContextDetails.forEach((details) => {{
        if (!storyContextHasCurrentLinks(details)) details.hidden = true;
      }});
    }}

    async function loadRemoteArchiveLinks() {{
      if (!remoteReportsApiUrl || !archiveLinksContainer) return;
      try {{
        const response = await fetch(`${{apiUrlWithAction(remoteReportsApiUrl, 'reports')}}&limit=30`, {{
          headers: {{ 'Accept': 'application/json' }},
          credentials: 'omit',
        }});
        if (!response.ok) return;
        const data = await response.json();
        if (data && data.ok) renderRemoteArchiveLinks(data.reports || []);
      }} catch (error) {{}}
    }}

    function pageTop(element) {{
      const rect = element.getBoundingClientRect();
      return rect.top + window.scrollY;
    }}

    function visualStoryEntries(stories) {{
      return stories.map((story) => {{
        const rect = story.getBoundingClientRect();
        return {{
          story,
          top: rect.top + window.scrollY,
          left: rect.left + window.scrollX,
          bottom: rect.bottom + window.scrollY,
        }};
      }}).sort((a, b) => {{
        const topDelta = a.top - b.top;
        if (Math.abs(topDelta) > 2) return topDelta;
        return a.left - b.left;
      }});
    }}

    function updateNavigation() {{
      if (!sections.length) return;
      const marker = window.scrollY + Math.min(220, window.innerHeight * 0.34);
      let activeSection = sections[0];
      sections.forEach((section) => {{
        if (pageTop(section) <= marker) activeSection = section;
      }});
      const activeSectionId = activeSection.id;
      const activeStories = sectionStories.filter((story) => story.dataset.sectionKey === activeSectionId);
      const visualEntries = visualStoryEntries(activeStories);
      visualStoryIndexByHref = new Map();
      visualEntries.forEach((entry, index) => {{
        if (entry.story.id) visualStoryIndexByHref.set(`#${{entry.story.id}}`, index);
      }});
      let activeStory = activeStories[0] || null;
      let activeVisualIndex = 0;
      visualEntries.forEach((entry, index) => {{
        if (entry.top <= marker) {{
          activeStory = entry.story;
          activeVisualIndex = index;
        }}
      }});
      const total = Number(activeSection.dataset.sectionCount || activeStory?.dataset.sectionTotal || activeStories.length || 0);
      const index = activeStory ? Math.min(total, activeVisualIndex + 1) : 0;
      const activeSectionLabel = activeSection.dataset.sectionLabel || '';

      sections.forEach((section) => {{
        section.classList.toggle('is-active-section', section.id === activeSectionId);
      }});
      categoryLinks.forEach((link) => {{
        const isActive = sectionIdForLink(link) === activeSectionId;
        link.classList.toggle('is-active', isActive);
        if (!isActive) setSectionProgress(sectionIdForLink(link), 0, Number(link.querySelector('[data-progress-text]')?.textContent?.split('/')[1] || 0));
      }});
      if (activeSectionId !== lastActiveSectionId) {{
        const activeChip = categoryLinks.find((link) => link.dataset.tocSection === activeSectionId);
        if (activeChip) activeChip.scrollIntoView({{ block: 'nearest', inline: 'center', behavior: 'smooth' }});
        lastActiveSectionId = activeSectionId;
      }}
      setSectionProgress(activeSectionId, index, total);
      if (mobileSectionLabel) mobileSectionLabel.textContent = activeSectionLabel;
      if (mobileProgress) mobileProgress.textContent = `${{index}}/${{total}}`;

      const activeStoryId = activeStory ? `#${{activeStory.id}}` : '';
      storyLinks.forEach((link) => {{
        link.classList.toggle('is-active', link.getAttribute('href') === activeStoryId);
      }});
      updateStoryWindow(activeStoryId);
    }}

    let navTicking = false;
    let lastActiveSectionId = '';
    let visualStoryIndexByHref = new Map();
    function requestNavigationUpdate() {{
      if (navTicking) return;
      navTicking = true;
      window.requestAnimationFrame(() => {{
        updateNavigation();
        navTicking = false;
      }});
    }}
    window.addEventListener('scroll', requestNavigationUpdate, {{ passive: true }});
    window.addEventListener('resize', () => {{
      applyResponsiveArticleLinks();
      requestNavigationUpdate();
    }});
    if (mobileArticleLinkQuery.addEventListener) {{
      mobileArticleLinkQuery.addEventListener('change', applyResponsiveArticleLinks);
    }}
    applyResponsiveArticleLinks();
    readStoryIds.forEach((storyId) => applyReadState(storyId));
    document.addEventListener('click', (event) => {{
      const link = event.target.closest('a');
      if (!link) return;
      const story = link.closest('[data-story]');
      if (!story) return;
      const href = link.getAttribute('href') || '';
      if (href.startsWith('#')) return;
      markStoryRead(story.id);
    }});
    archiveToggles.forEach((toggle) => {{
      toggle.addEventListener('click', (event) => {{
        event.preventDefault();
        event.stopPropagation();
        setArchiveOpen(archivePanel ? archivePanel.hidden : false);
      }});
    }});
    if (archiveClose) {{
      archiveClose.addEventListener('click', () => setArchiveOpen(false));
    }}
    document.addEventListener('click', (event) => {{
      if (!archivePanel || archivePanel.hidden) return;
      if (archivePanel.contains(event.target) || archiveToggles.some((toggle) => toggle.contains(event.target))) return;
      setArchiveOpen(false);
    }});
    document.addEventListener('keydown', (event) => {{
      if (event.key === 'Escape') setArchiveOpen(false);
    }});
    categoryLinks.forEach((link) => {{
      link.addEventListener('click', (event) => {{
        const sectionId = sectionIdForLink(link);
        const target = document.getElementById(sectionId);
        if (!target) return;
        event.preventDefault();
        target.scrollIntoView({{ behavior: 'smooth', block: 'start' }});
        if (history.pushState) history.pushState(null, '', `#${{sectionId}}`);
      }});
    }});
    if (dbSearchForm) {{
      dbSearchForm.addEventListener('submit', (event) => {{
        event.preventDefault();
        const input = dbSearchForm.querySelector('input[name="q"]');
        searchDbArticles(input ? input.value : '');
      }});
    }}
    storyContextDetails.forEach((details) => {{
      details.addEventListener('toggle', () => {{
        if (details.open) loadStoryContext(details);
      }});
    }});
    hideUnavailableStoryContexts();
    updateNavigation();
    loadRemoteArchiveLinks();
    loadDbPulse();
  </script>
</body>
</html>
"""


def workbench_story_payload(story: dict[str, object], config: dict[str, object]) -> dict[str, object]:
    links = [link for link in (story.get("links") if isinstance(story.get("links"), list) else []) if isinstance(link, dict)]
    return {
        "id": str(story.get("id") or ""),
        "title": str(story.get("title") or "제목 없음"),
        "category": str(story.get("category") or "기타"),
        "datetime": date_label(story.get("datetime"), config),
        "source_line": str(story.get("source_line") or story.get("primary_source") or ""),
        "summary": story_summary_for_display(story),
        "bullets": story_brief_bullets(story, max_chars=96, max_items=3),
        "primary_url": str(story.get("primary_url") or ""),
        "image_url": str(story.get("image_url") or ""),
        "story_key": str(story.get("story_key") or ""),
        "db_query": str(story.get("db_query") or story.get("title") or ""),
        "links": links[:12],
    }


def render_workbench_html(
    stories: list[dict[str, object]],
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
    date_id: str,
    report_url: str,
) -> str:
    story_payloads = [workbench_story_payload(story, config) for story in stories[:40]]
    start_label = escape(format_kst(start_at, str(config.get("timezone") or "Asia/Seoul")))
    end_label = escape(format_kst(end_at, str(config.get("timezone") or "Asia/Seoul")))
    report_link = escape(report_url, quote=True)
    data_json = json_script_payload(story_payloads)
    read_api_url_json = json.dumps(report_read_api_url(), ensure_ascii=False)
    logo = bside_logo_html("bside-logo--top")
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>AI 워크벤치 - {escape(date_id)}</title>
  <style>
    :root {{ --ink:#17131f; --muted:#6f6878; --line:#ded7e8; --paper:#fbfafc; --surface:#fff; --accent:#6b35d8; --accent-deep:#42207e; --accent-soft:#f0eafb; --green:#00785f; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; background:var(--paper); color:var(--ink); font-family:-apple-system,BlinkMacSystemFont,"Apple SD Gothic Neo","Malgun Gothic","Segoe UI",sans-serif; line-height:1.55; }}
    a {{ color:inherit; text-decoration-thickness:1px; text-underline-offset:3px; }}
    .shell {{ max-width:1220px; margin:0 auto; padding:24px 24px 64px; }}
    .top {{ display:flex; justify-content:space-between; align-items:center; gap:20px; border-bottom:2px solid var(--ink); padding-bottom:18px; }}
    .bside-logo {{ display:inline-flex; align-items:center; gap:9px; color:var(--accent); text-decoration:none; }}
    .bside-logo__image {{ width:92px; height:auto; display:block; color:var(--accent); }}
    .bside-logo__label {{ color:var(--accent); font-size:11px; font-weight:900; letter-spacing:.12em; }}
    .top__meta {{ color:var(--muted); font-size:12px; text-align:right; }}
    .hero {{ display:grid; grid-template-columns:minmax(0,1fr) auto; gap:18px; align-items:end; padding:22px 0 18px; border-bottom:1px solid var(--line); }}
    h1 {{ margin:0; font-family:Georgia,"Times New Roman",serif; font-size:42px; line-height:1; letter-spacing:0; }}
    .hero p {{ margin:8px 0 0; color:#352e40; font-size:14px; }}
    .hero a {{ display:inline-flex; border:1px solid var(--accent); border-radius:999px; padding:8px 12px; color:var(--accent-deep); background:var(--accent-soft); font-size:12px; font-weight:850; text-decoration:none; }}
    .workbench {{ display:grid; grid-template-columns:340px minmax(0,1fr); gap:22px; align-items:start; padding-top:20px; }}
    .story-list {{ position:sticky; top:16px; max-height:calc(100vh - 32px); overflow:auto; border:1px solid var(--line); background:rgba(255,255,255,.84); }}
    .story-button {{ width:100%; appearance:none; border:0; border-bottom:1px solid var(--line); background:transparent; text-align:left; padding:13px 14px; cursor:pointer; color:inherit; }}
    .story-button:hover, .story-button.is-active {{ background:var(--accent-soft); }}
    .story-button strong {{ display:block; font-size:14px; line-height:1.35; word-break:keep-all; }}
    .story-button span {{ display:flex; gap:7px; flex-wrap:wrap; margin-top:6px; color:var(--muted); font-size:11px; }}
    .panel {{ min-height:calc(100vh - 160px); border-top:3px solid var(--accent); background:var(--surface); padding:22px; box-shadow:0 18px 46px rgba(44,27,84,.08); }}
    .panel__meta {{ display:flex; flex-wrap:wrap; gap:8px; color:var(--muted); font-size:12px; margin-bottom:12px; }}
    .panel h2 {{ margin:0; max-width:760px; font-size:28px; line-height:1.22; letter-spacing:0; word-break:keep-all; }}
    .panel__layout {{ display:grid; grid-template-columns:220px minmax(0,1fr); gap:20px; margin-top:18px; align-items:start; }}
    .panel__image {{ aspect-ratio:4/3; border:1px solid var(--line); background:var(--accent-soft); object-fit:cover; width:100%; }}
    .panel__image[hidden] {{ display:none; }}
    .panel__summary {{ margin:0; padding:12px 14px; border-left:3px solid rgba(112,55,224,.5); background:rgba(246,240,255,.58); list-style:none; display:grid; gap:7px; font-size:14px; line-height:1.48; }}
    .panel__summary li {{ position:relative; padding-left:12px; }}
    .panel__summary li::before {{ content:""; position:absolute; left:0; top:.72em; width:4px; height:4px; border-radius:50%; background:var(--accent); }}
    .panel__actions {{ display:flex; flex-wrap:wrap; gap:8px; margin-top:14px; }}
    .panel__actions a, .panel__actions button {{ appearance:none; border:1px solid var(--line); background:#fff; color:var(--ink); border-radius:999px; padding:8px 11px; font:inherit; font-size:12px; font-weight:850; text-decoration:none; cursor:pointer; }}
    .panel__actions a:first-child {{ border-color:var(--accent); color:var(--accent-deep); background:var(--accent-soft); }}
    .related {{ margin-top:24px; }}
    .related__head {{ display:flex; align-items:center; justify-content:space-between; gap:12px; margin-bottom:8px; color:var(--muted); font-size:12px; }}
    .related__chips {{ display:flex; flex-wrap:wrap; gap:6px; margin-bottom:9px; }}
    .related__chips span {{ border:1px solid rgba(112,55,224,.18); border-radius:999px; padding:3px 8px; color:var(--accent-deep); background:#fff; font-size:11px; font-weight:850; }}
    .related__table {{ border:1px solid var(--line); overflow:auto; }}
    table {{ width:100%; min-width:720px; border-collapse:collapse; font-size:12.5px; }}
    th, td {{ padding:8px 9px; border-bottom:1px solid var(--line); text-align:left; vertical-align:top; }}
    th {{ background:#faf8fd; color:var(--muted); font-weight:800; }}
    th:first-child, td:first-child {{ width:82px; }}
    th:nth-child(2), td:nth-child(2) {{ width:96px; color:var(--muted); white-space:nowrap; }}
    th:nth-child(3), td:nth-child(3) {{ width:124px; color:var(--accent-deep); }}
    .kind {{ display:inline-flex; border:1px solid rgba(112,55,224,.22); border-radius:999px; padding:2px 7px; color:var(--accent-deep); background:#fff; font-size:10.5px; font-weight:850; white-space:nowrap; }}
    .kind--archive {{ color:var(--green); border-color:rgba(0,120,95,.25); }}
    .empty {{ color:var(--muted); font-size:13px; padding:12px; border:1px solid var(--line); background:#fff; }}
    @media (max-width:900px) {{ .workbench {{ grid-template-columns:1fr; }} .story-list {{ position:static; max-height:none; }} .panel__layout {{ grid-template-columns:1fr; }} }}
  </style>
</head>
<body>
  <div class="shell">
    <header class="top">
      {logo}
      <div class="top__meta">{start_label}<br>{end_label}</div>
    </header>
    <section class="hero">
      <div>
        <h1>AI 요약 워크벤치</h1>
        <p>기사 목록을 벗어나지 않고 요약, 현재 묶음, DB 아카이브 관련 기사를 한 화면에서 확인하는 데스크톱 실험 페이지입니다.</p>
      </div>
      <a href="{report_link}">데일리로 돌아가기</a>
    </section>
    <main class="workbench">
      <nav class="story-list" data-workbench-list aria-label="기사 선택"></nav>
      <section class="panel" data-workbench-panel aria-live="polite"></section>
    </main>
  </div>
  <script type="application/json" id="workbench-data">{data_json}</script>
  <script>
    const stories = JSON.parse(document.getElementById('workbench-data')?.textContent || '[]');
    const apiUrl = {read_api_url_json};
    const list = document.querySelector('[data-workbench-list]');
    const panel = document.querySelector('[data-workbench-panel]');
    let activeIndex = 0;

    function apiUrlWithAction(url, action) {{
      if (!url) return '';
      return `${{url}}${{url.includes('?') ? '&' : '?'}}action=${{encodeURIComponent(action)}}`;
    }}
    function compactText(value, max = 90) {{
      const text = String(value || '').replace(/\\s+/g, ' ').trim();
      return text.length <= max ? text : `${{text.slice(0, max - 1).trim()}}…`;
    }}
    function urlKey(value) {{
      try {{
        const url = new URL(String(value || ''), location.href);
        url.hash = '';
        return `${{url.origin}}${{url.pathname.replace(/\\/$/, '')}}${{url.search}}`.toLowerCase();
      }} catch (error) {{
        return String(value || '').replace(/#.*$/, '').replace(/\\/$/, '').toLowerCase();
      }}
    }}
    function normalizedTitle(value) {{
      return String(value || '')
        .toLowerCase()
        .replace(/\\s+-\\s+[^-·|]+$/, '')
        .replace(/[\\[\\]()"“”'‘’·….,:;!?~\\-_/|]/g, ' ')
        .replace(/\\s+/g, ' ')
        .trim();
    }}
    function rowKey(article) {{
      const title = normalizedTitle(article.title);
      return title.length >= 12 ? `title:${{title}}` : `url:${{urlKey(article.canonical_url)}}`;
    }}
    function rowQuality(article) {{
      const url = String(article.canonical_url || '').toLowerCase();
      let score = 0;
      if (!url.includes('news.google.com')) score += 3;
      if (!url.includes('google.com/rss')) score += 1;
      if (article.summary) score += 1;
      if (article.image_url) score += 1;
      return score;
    }}
    function dateLabel(article) {{
      const raw = String(article.published_at || article.sort_at || '').trim();
      const match = raw.match(/^(\\d{{4}})-(\\d{{2}})-(\\d{{2}})\\s+(\\d{{2}}):(\\d{{2}})/);
      return match ? `${{match[2]}}.${{match[3]}} ${{match[4]}}:${{match[5]}}` : (article.datetime || '');
    }}
    function contextTokens(story) {{
      const generic = new Set(['관련','기사','보도','뉴스','시장','자본시장','주주','기업','증시','한국어','밸류업','주주환원','자사주','소각','지배구조','경영권','분쟁','소액주주','공시','제도','거래소','코스닥','상장','중복상장','유상증자','물적분할','종료보고서','제출','불성실공시법인','지정','google','news']);
      const tokens = [];
      `${{story.title || ''}} ${{story.db_query || ''}}`.match(/[0-9A-Za-z가-힣]{{2,}}/g)?.forEach((token) => {{
        const normalized = token.toLowerCase();
        if (!generic.has(normalized) && !tokens.includes(token)) tokens.push(token);
      }});
      return tokens.slice(0, 6);
    }}
    function matchesStory(article, tokens) {{
      if (!tokens.length) return false;
      const weak = new Set(['밸류업','주주환원','자사주','소각','지배구조','경영권','분쟁','소액주주','공시','제도','거래소','코스닥','상장','중복상장','유상증자','물적분할','종료보고서','불성실공시법인','감독','제재']);
      const text = `${{article.title || ''}} ${{article.summary || ''}} ${{article.source || article.feed_name || ''}}`.toLowerCase();
      const hits = tokens.filter((token) => text.includes(token.toLowerCase()));
      return hits.some((token) => token.length >= 3 && !weak.has(token.toLowerCase())) || hits.length >= Math.min(3, Math.max(2, tokens.length));
    }}
    function currentRows(story) {{
      return (Array.isArray(story.links) ? story.links : []).filter((link) => link && link.url && link.title).map((link) => ({{
        canonical_url: link.url,
        title: link.title,
        source: link.source || link.domain || '',
        published_at: link.published_at || '',
        context_kind: 'current',
      }}));
    }}
    function mergeRows(rows) {{
      const seen = new Map();
      rows.flat().forEach((article) => {{
        if (!article || !article.canonical_url || !article.title) return;
        const key = rowKey(article);
        if (!key) return;
        const previous = seen.get(key);
        if (!previous || rowQuality(article) > rowQuality(previous)) seen.set(key, article);
      }});
      const merged = Array.from(seen.values());
      return merged.sort((left, right) => String(right.sort_at || right.published_at || '').localeCompare(String(left.sort_at || left.published_at || '')));
    }}
    async function fetchArchiveRows(story) {{
      if (!apiUrl) return [];
      const batches = [];
      const tokens = contextTokens(story);
      for (const params of [
        story.story_key ? {{ story_key: story.story_key, limit: '16', days: '180' }} : null,
        story.db_query ? {{ q: story.db_query, limit: '12', days: '180' }} : null,
      ].filter(Boolean)) {{
        try {{
          const query = new URLSearchParams(params);
          const response = await fetch(`${{apiUrlWithAction(apiUrl, 'articles')}}&${{query.toString()}}`, {{ headers: {{ Accept: 'application/json' }}, credentials: 'omit' }});
          if (!response.ok) continue;
          const data = await response.json();
          if (data && data.ok && Array.isArray(data.articles)) batches.push(data.articles);
        }} catch (error) {{}}
      }}
      const currentKeys = new Set(currentRows(story).map((article) => rowKey(article)));
      return mergeRows(batches)
        .filter((article) => !currentKeys.has(rowKey(article)))
        .filter((article) => matchesStory(article, tokens))
        .slice(0, 8)
        .map((article) => ({{ ...article, context_kind: 'archive' }}));
    }}
    function renderList() {{
      if (!list) return;
      list.innerHTML = '';
      stories.forEach((story, index) => {{
        const button = document.createElement('button');
        button.className = `story-button${{index === activeIndex ? ' is-active' : ''}}`;
        button.type = 'button';
        button.innerHTML = `<strong>${{compactText(story.title, 76)}}</strong><span><em>${{story.category || '기타'}}</em><em>${{story.datetime || ''}}</em><em>${{story.links?.length || 1}}건</em></span>`;
        button.addEventListener('click', () => openStory(index));
        list.appendChild(button);
      }});
    }}
    function relatedTable(rows) {{
      if (!rows.length) return '<div class="empty">표시할 관련 기사가 없습니다.</div>';
      const chips = [`관련 기사 ${{rows.length}}건`, `매체 ${{new Set(rows.map((row) => row.source || row.feed_name || '')).size}}곳`];
      return `
        <div class="related__chips">${{chips.map((chip) => `<span>${{chip}}</span>`).join('')}}</div>
        <div class="related__table"><table>
          <thead><tr><th>구분</th><th>일시</th><th>매체</th><th>기사</th></tr></thead>
          <tbody>${{rows.map((row) => `<tr><td><span class="kind${{row.context_kind === 'archive' ? ' kind--archive' : ''}}">${{row.context_kind === 'archive' ? '아카이브' : '현재 묶음'}}</span></td><td>${{dateLabel(row) || '일시 미상'}}</td><td>${{row.source || row.feed_name || '매체 미상'}}</td><td><a href="${{row.canonical_url}}" target="_blank" rel="noopener noreferrer">${{compactText(row.title, 110)}}</a></td></tr>`).join('')}}</tbody>
        </table></div>`;
    }}
    async function openStory(index) {{
      activeIndex = Math.max(0, Math.min(index, stories.length - 1));
      renderList();
      const story = stories[activeIndex] || {{}};
      const bullets = Array.isArray(story.bullets) && story.bullets.length ? story.bullets : [story.summary || '요약 정보가 부족합니다.'];
      panel.innerHTML = `
        <div class="panel__meta"><span>${{story.category || '기타'}}</span><span>${{story.datetime || ''}}</span><span>${{story.source_line || ''}}</span></div>
        <h2>${{story.title || '제목 없음'}}</h2>
        <div class="panel__layout">
          <img class="panel__image" src="${{story.image_url || ''}}" alt="" referrerpolicy="no-referrer" ${{!story.image_url ? 'hidden' : ''}}>
          <div>
            <ul class="panel__summary">${{bullets.map((bullet) => `<li>${{bullet}}</li>`).join('')}}</ul>
            <div class="panel__actions">
              <a href="${{story.primary_url || '#'}}" target="_blank" rel="noopener noreferrer">원문 새 탭</a>
              <button type="button" data-prev>이전 기사</button>
              <button type="button" data-next>다음 기사</button>
            </div>
          </div>
        </div>
        <section class="related">
          <div class="related__head"><strong>관련 기사</strong><span data-related-status>현재 묶음과 DB 아카이브를 확인하는 중입니다.</span></div>
          <div data-related-body>${{relatedTable(currentRows(story))}}</div>
        </section>`;
      panel.querySelector('[data-prev]')?.addEventListener('click', () => openStory(activeIndex - 1));
      panel.querySelector('[data-next]')?.addEventListener('click', () => openStory(activeIndex + 1));
      const archiveRows = await fetchArchiveRows(story);
      const rows = [...currentRows(story), ...archiveRows];
      panel.querySelector('[data-related-body]').innerHTML = relatedTable(rows);
      panel.querySelector('[data-related-status]').textContent = archiveRows.length ? '아카이브 관련 기사까지 반영했습니다.' : '현재 묶음을 중심으로 표시합니다.';
    }}
    renderList();
    openStory(0);
  </script>
</body>
</html>
"""


def build_daily_report(root: Path | None = None, now: datetime | None = None) -> dict[str, object]:
    project_root = root or PROJECT_ROOT
    config = load_config(project_root / "config.yaml")
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    end_at = now or now_in_timezone(timezone_name)
    start_at = end_at - timedelta(hours=report_hours())
    state = load_state(project_root / "data" / "state.json")
    clusters = digest_clusters_in_window(state, config, start_at, end_at)
    duplicate_records = duplicate_records_in_window(state, config, start_at, end_at)
    stories = build_report_stories(clusters, duplicate_records, config)
    attach_telegram_mentions(stories, state)
    enrich_story_images(stories, config)
    attach_story_briefs(stories, config)
    review = generate_report_review(clusters, stories, config, start_at, end_at)
    date_id = end_at.astimezone(ZoneInfo(timezone_name)).strftime("%Y-%m-%d")
    report_url = report_public_url(config, date_id)
    archive_links_html = render_report_archive_links(project_root / FEED_DIR, date_id)
    html = render_report_html(
        stories,
        review,
        config,
        start_at,
        end_at,
        date_id,
        report_url,
        duplicate_records,
        clusters,
        archive_links_html,
        "standard",
        False,
    )
    workbench_html = render_workbench_html(stories, config, start_at, end_at, date_id, report_url)
    return {
        "config": config,
        "date_id": date_id,
        "start_at": start_at,
        "end_at": end_at,
        "stories": stories,
        "review": review,
        "html": html,
        "workbench_html": workbench_html,
        "report_url": report_url,
        "stats": report_stats(stories, clusters, duplicate_records),
        "clusters": clusters,
        "duplicate_records": duplicate_records,
    }


def normalize_generated_html(html: str) -> str:
    return "\n".join(line.rstrip() for line in str(html).splitlines()) + "\n"


def write_report_files(report: dict[str, object], root: Path | None = None) -> list[Path]:
    project_root = root or PROJECT_ROOT
    date_id = str(report["date_id"])
    feed_dir = project_root / FEED_DIR
    feed_dir.mkdir(parents=True, exist_ok=True)
    html = normalize_generated_html(str(report["html"]))
    dated_path = feed_dir / f"{date_id}.html"
    latest_path = feed_dir / "latest.html"
    index_path = feed_dir / "index.html"
    workbench_path = feed_dir / "workbench.html"
    dated_path.write_text(html, encoding="utf-8", newline="\n")
    latest_path.write_text(html, encoding="utf-8", newline="\n")
    workbench_path.write_text(normalize_generated_html(str(report.get("workbench_html") or "")), encoding="utf-8", newline="\n")
    variant_dir = feed_dir / "variants"
    if variant_dir.exists():
        for stale_path in variant_dir.glob("*.html"):
            stale_path.unlink()
    index_path.write_text(render_report_index(feed_dir), encoding="utf-8", newline="\n")
    refreshed_paths = refresh_existing_report_archive_links(feed_dir, date_id)
    return [dated_path, latest_path, workbench_path, index_path, *refreshed_paths]


def render_report_archive_links(feed_dir: Path, current_date_id: str, *, link_prefix: str = "", max_items: int = 20) -> str:
    date_ids = {current_date_id}
    if feed_dir.exists():
        date_ids.update(
            path.stem
            for path in feed_dir.glob("*.html")
            if path.name not in NON_DATE_REPORT_PAGES and path.stem
        )
    sorted_date_ids = sorted(date_ids, reverse=True)[:max_items]
    if not sorted_date_ids:
        return ""
    items = []
    for date_id in sorted_date_ids:
        is_current = date_id == current_date_id
        label = "현재" if is_current else ""
        current_class = " is-current" if is_current else ""
        items.append(
            f'<a class="archive-panel__link{current_class}" href="{escape(link_prefix + date_id, quote=True)}.html">'
            f"{escape(date_id)}"
            f"<span>{escape(label)}</span>"
            "</a>"
        )
    return "\n".join(items)


ARCHIVE_LINKS_PATTERN = re.compile(
    r'(<div class="archive-panel__links">\n)(.*?)(\n\s*</div>)',
    re.DOTALL,
)


def refresh_report_archive_links_in_html(html: str, links_html: str) -> str:
    replacement = r"\1" + links_html + r"\3"
    return ARCHIVE_LINKS_PATTERN.sub(replacement, html, count=1)


def refresh_existing_report_archive_links(feed_dir: Path, current_date_id: str) -> list[Path]:
    if not feed_dir.exists():
        return []
    refreshed: list[Path] = []
    dated_paths = [
        path
        for path in feed_dir.glob("*.html")
        if path.name not in NON_DATE_REPORT_PAGES and path.stem
    ]
    dated_paths.append(feed_dir / "latest.html")
    for path in dated_paths:
        if not path.exists():
            continue
        page_date_id = current_date_id if path.name == "latest.html" else path.stem
        links_html = render_report_archive_links(feed_dir, page_date_id)
        html = path.read_text(encoding="utf-8")
        updated = refresh_report_archive_links_in_html(html, links_html)
        if updated != html:
            path.write_text(normalize_generated_html(updated), encoding="utf-8", newline="\n")
            refreshed.append(path)

    return refreshed


def render_report_index(feed_dir: Path) -> str:
    feed_files = sorted(
        [
            path
            for path in feed_dir.glob("*.html")
            if path.name not in NON_DATE_REPORT_PAGES
        ],
        reverse=True,
    )
    links = "\n".join(
        f'<li><a href="{escape(path.name, quote=True)}">{escape(path.stem)}</a></li>'
        for path in feed_files
    )
    if not links:
        links = "<li>아직 발행된 데일리가 없습니다.</li>"
    logo = bside_logo_html("brand")
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>비사이드 자본시장 데일리 아카이브</title>
  <style>
    :root {{ --ink:#17131f; --muted:#6f6878; --line:#ded7e8; --paper:#fbfafc; --accent:#6b35d8; }}
    body {{ margin:0; color:var(--ink); background:var(--paper); font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }}
    main {{ max-width:780px; margin:0 auto; padding:36px 20px 72px; }}
    .brand {{ display:inline-flex; align-items:center; gap:8px; color:var(--accent); font-weight:900; letter-spacing:.08em; font-size:13px; text-decoration:none; border-bottom:1px solid var(--line); padding-bottom:12px; }}
    .bside-logo__image {{ width:92px; height:auto; display:block; color:var(--accent); flex:0 0 auto; }}
    .bside-logo__label {{ font-size:11px; }}
    h1 {{ font-family:Georgia,"Times New Roman",serif; font-size:clamp(40px,7vw,68px); line-height:1; margin:26px 0 10px; }}
    p {{ color:var(--muted); }}
    ul {{ list-style:none; padding:0; margin:32px 0 0; border-top:2px solid var(--ink); }}
    li {{ border-bottom:1px solid var(--line); }}
    a {{ display:block; padding:16px 0; color:inherit; text-decoration:none; font-size:20px; }}
    a:hover {{ color:var(--accent); }}
  </style>
</head>
<body>
  <main>
    {logo}
    <h1>데일리 아카이브</h1>
    <p>매일 발행된 주주·자본시장 데일리를 날짜별로 확인할 수 있습니다.</p>
    <ul>{links}</ul>
  </main>
</body>
</html>
"""


def report_link_label(report: dict[str, object]) -> str:
    date_id = str(report.get("date_id") or "")
    try:
        parsed = datetime.strptime(date_id, "%Y-%m-%d")
    except ValueError:
        return "주주·자본시장 데일리"
    return f"{parsed.year % 100:02d}년 {parsed.month}월 {parsed.day}일 주주·자본시장 데일리"


def telegram_story_title(story: dict[str, object]) -> str:
    return compact_text(str(story.get("title") or "제목 없음"), max_chars=62)


def build_report_telegram_message(report: dict[str, object]) -> str:
    stories = report.get("stories") if isinstance(report.get("stories"), list) else []
    report_url = str(report.get("report_url") or "")
    link_label = report_link_label(report)
    stats = report.get("stats") if isinstance(report.get("stats"), dict) else {}
    story_count = int(stats.get("stories") or len(stories))
    article_count = int(stats.get("articles") or sum(int(story.get("link_count") or 0) for story in stories if isinstance(story, dict)))
    source_count = int(
        stats.get("sources")
        or len(
            {
                str(link.get("source") or "")
                for story in stories
                if isinstance(story, dict)
                for link in (story.get("links") if isinstance(story.get("links"), list) else [])
                if isinstance(link, dict) and link.get("source")
            }
        )
    )
    lines = [f"<b>{escape(link_label)}</b>"]
    lines.append(f"수집 기사 {article_count}건 · 이슈 {story_count}개 · 매체 {source_count}개")
    if stories:
        lines.append("")
        lines.append("<b>메인 기사</b>")
        for story in [story for story in stories if isinstance(story, dict)][:3]:
            lines.append(f"• {escape(telegram_story_title(story))}")
    lines.append("")
    lines.append(html_link(link_label, report_url))
    return "\n".join(lines).strip()


def send_daily_report(root: Path | None = None) -> dict[str, int]:
    project_root = root or PROJECT_ROOT
    report = build_daily_report(project_root)
    write_report_files(report, project_root)
    remote_summary = sync_report_to_remote_api(report)
    config = report["config"] if isinstance(report.get("config"), dict) else load_config(project_root / "config.yaml")
    if daily_report_write_only():
        return {"daily_report_written": 1, "daily_report_sent": 0, "daily_report_failed": 0, **remote_summary}
    if not telegram_is_configured(config):
        return {"daily_report_written": 1, "daily_report_sent": 0, "daily_report_failed": 0, **remote_summary}
    response = send_telegram_message(
        telegram_bot_token(),
        telegram_chat_id(config),
        build_report_telegram_message(report),
        config,
        disable_web_page_preview=False,
    )
    return {
        "daily_report_written": 1,
        "daily_report_sent": 1 if response.get("ok") else 0,
        "daily_report_failed": 0 if response.get("ok") else 1,
        **remote_summary,
    }


def main() -> None:
    summary = send_daily_report()
    print(
        "Daily report finished: "
        + ", ".join(f"{key}={value}" for key, value in summary.items())
    )


if __name__ == "__main__":
    main()
