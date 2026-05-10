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
from .cluster import KNOWN_COMPANIES, extract_company_candidates
from .config import load_config
from .dates import format_kst, now_in_timezone, parse_datetime
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
NON_DATE_REPORT_PAGES = {"latest.html", "index.html", "telegram.html", "workbench.html", "search.html", "telegram-admin.html"}
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


CONTEXT_EXCERPT_STOPWORDS = {
    "관련",
    "기사",
    "보도",
    "뉴스",
    "시장",
    "자본시장",
    "주주",
    "기업",
    "증시",
    "한국어",
    "밸류업",
    "주주환원",
    "자사주",
    "소각",
    "지배구조",
    "경영권",
    "분쟁",
    "소액주주",
    "공시",
    "제도",
    "google",
    "news",
}


def context_excerpt_tokens(value: str, max_tokens: int = 10) -> list[str]:
    tokens: list[str] = []
    for token in re.findall(r"[0-9A-Za-z가-힣]{2,}", value or ""):
        normalized = token.casefold()
        if normalized in CONTEXT_EXCERPT_STOPWORDS or normalized in tokens:
            continue
        tokens.append(normalized)
        if len(tokens) >= max_tokens:
            break
    return tokens


def contextual_text_excerpt(text: str, query: str, max_chars: int = 140) -> str:
    compacted = re.sub(r"\s+", " ", text or "").strip()
    if not compacted:
        return ""
    lowered = compacted.casefold()
    hit_index: int | None = None
    for token in context_excerpt_tokens(query):
        index = lowered.find(token)
        if index >= 0:
            hit_index = index
            break
    if hit_index is None:
        return compact_text(compacted, max_chars=max_chars)
    start = max(0, hit_index - 42)
    snippet = compacted[start : start + max_chars]
    prefix = "... " if start > 0 else ""
    suffix = " ..." if start + max_chars < len(compacted) else ""
    return compact_text(f"관련 문맥: {prefix}{snippet}{suffix}", max_chars=max_chars + 14)


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
        "제약",
        "바이오",
        "레이더",
        "정기",
        "마무리",
        "매출",
        "클럽",
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
        context_query = " ".join(
            [
                str(story.get("title") or ""),
                " ".join(str(link.get("title") or "") for link in links if isinstance(link, dict)),
            ]
        )
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
                score = float(match.get("score") or 0)
                match_type = str(match.get("match_type") or "")
                if match_type in {"keyword", "ticker"} and score < 0.53:
                    continue
                seen_messages.add(message_key)
                text = str(message.get("text") or "")
                contextual = contextual_text_excerpt(text, context_query, max_chars=140)
                mentions.append(
                    {
                        "message_url": message.get("message_url") or match.get("message_url") or "",
                        "channel_title": message.get("channel_title") or match.get("channel_title") or "",
                        "channel_handle": message.get("handle") or match.get("channel_handle") or "",
                        "posted_at": message.get("posted_at") or "",
                        "text": contextual,
                        "excerpt": contextual,
                        "match_type": match_type,
                        "score": score,
                        "reason": match.get("reason") or "",
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
    return (
        json.dumps(value, ensure_ascii=False)
        .replace("</", "<\\/")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


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
        if normalized_links
        else ""
    )
    telegram_mentions = story.get("telegram_mentions") if isinstance(story.get("telegram_mentions"), list) else []
    telegram_mentions_data_html = (
        f'<script type="application/json" data-story-telegram-mentions>{json_script_payload(telegram_mentions)}</script>'
        if telegram_mentions
        else ""
    )
    has_reliable_telegram_context = any(
        isinstance(mention, dict) and str(mention.get("match_type") or "") in {"exact_url", "canonical_url"}
        for mention in telegram_mentions
    )
    has_static_related_context = has_grouped_links or has_reliable_telegram_context
    context_visibility_attrs = "" if has_static_related_context else ' hidden data-context-pending="1"'
    related_html = (
        f"""
            <details class="story-context" data-story-context{context_visibility_attrs}>
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
    .search-entry {{ display: flex; align-items: center; justify-content: space-between; gap: 12px; margin-top: 14px; padding-top: 12px; border-top: 1px solid var(--line); }}
    .search-entry p {{ margin: 0; color: var(--muted); font-size: 12.5px; line-height: 1.42; }}
    .search-entry__button {{ flex: 0 0 auto; display: inline-flex; align-items: center; justify-content: center; border: 1px solid var(--accent); border-radius: 999px; background: var(--accent-soft); color: var(--accent-deep); padding: 7px 11px; text-decoration: none; font-size: 12px; font-weight: 900; }}
    .search-entry__button:hover {{ background: #fff; }}
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
    .story-context summary::after {{ content: " · 통합 목록"; color: var(--muted); font-size: 11px; font-weight: 700; }}
    .story-context__body {{ display: grid; gap: 8px; margin-top: 8px; padding: 9px 10px; border-left: 3px solid rgba(112, 55, 224, .34); background: rgba(246, 240, 255, .38); color: #342d3d; font-size: 12px; line-height: 1.45; }}
    .story-context__message {{ color: var(--muted); }}
    .story-context__stats {{ display: flex; flex-wrap: wrap; gap: 6px; }}
    .story-context__stat {{ border: 1px solid rgba(112, 55, 224, .18); border-radius: 999px; padding: 3px 7px; background: #fff; color: var(--accent-deep); font-size: 10.8px; font-weight: 850; }}
    .story-context__spread {{ display: flex; flex-wrap: wrap; gap: 5px 8px; color: var(--muted); font-size: 11px; }}
    .story-context__spread strong {{ color: var(--ink); }}
    .story-context__articles {{ display: grid; gap: 7px; min-width: 0; }}
    .story-context__article {{ display: grid; gap: 4px; padding: 8px 9px; border: 1px solid rgba(112, 55, 224, .12); border-radius: 8px; background: rgba(255,255,255,.72); color: inherit; text-decoration: none; min-width: 0; }}
    .story-context__article:hover .story-context__article-title {{ color: var(--accent-deep); text-decoration: underline; text-underline-offset: 3px; }}
    .story-context__article-meta {{ display: flex; flex-wrap: wrap; gap: 4px 7px; align-items: center; color: var(--muted); font-size: 10.8px; }}
    .story-context__article-title {{ color: var(--ink); font-size: 12px; font-weight: 850; line-height: 1.38; word-break: keep-all; overflow-wrap: anywhere; }}
    .story-context__article-snippet {{ margin: 0; color: #4d4659; font-size: 11.4px; line-height: 1.38; display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }}
    .story-context__kind {{ display: inline-flex; align-items: center; justify-content: center; min-width: 52px; border: 1px solid rgba(112, 55, 224, .22); border-radius: 999px; padding: 2px 6px; background: #fff; color: var(--accent-deep); font-size: 10.5px; font-weight: 850; white-space: nowrap; }}
    .story-context__kind--archive {{ color: var(--green); border-color: rgba(0, 120, 95, .25); }}
    .story-context__article--current {{ background: rgba(255,255,255,.86); }}
    .link-table {{ width: 100%; max-width: 100%; min-width: 0; margin-top: 10px; border: 1px solid var(--line); background: var(--surface); overflow-x: auto; overflow-y: hidden; -webkit-overflow-scrolling: touch; }}
    .link-table table {{ width: 100%; min-width: 660px; table-layout: fixed; border-collapse: collapse; font-size: 12px; }}
    th, td {{ padding: 8px 9px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 700; background: #faf8fd; }}
    th:first-child, td:first-child {{ width: 92px; color: var(--muted); white-space: nowrap; }}
    th:nth-child(2), td:nth-child(2) {{ width: 120px; color: var(--accent-deep); }}
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
      .search-entry {{ align-items: flex-start; flex-direction: column; gap: 8px; }}
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
      .story-context__articles {{ gap: 6px; }}
      .story-context__article {{ padding: 7px 8px; }}
      .story-context__article-title {{ font-size: 11.8px; line-height: 1.34; }}
      .story-context__article-snippet {{ font-size: 11px; -webkit-line-clamp: 2; }}
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
        <a href="telegram.html">Telegram 데일리 보기</a>
        <a href="search.html">시장 이슈 검색</a>
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
      <div class="search-entry">
        <p>기사·이슈·Telegram 신호를 함께 보려면 별도 검색 화면에서 확인하세요.</p>
        <a class="search-entry__button" href="search.html">시장 이슈 검색</a>
      </div>
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

    function compactDateLabelFromValue(value) {{
      const raw = String(value || '').trim();
      if (!raw) return '';
      const direct = raw.match(/^(\\d{{4}})-(\\d{{2}})-(\\d{{2}})[ T](\\d{{2}}):(\\d{{2}})/);
      if (direct) return `${{direct[2]}}.${{direct[3]}} ${{direct[4]}}:${{direct[5]}}`;
      const parsed = new Date(raw);
      if (Number.isNaN(parsed.getTime())) return '';
      const parts = new Intl.DateTimeFormat('en-CA', {{
        timeZone: 'Asia/Seoul',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
      }}).formatToParts(parsed).reduce((acc, part) => {{
        acc[part.type] = part.value;
        return acc;
      }}, {{}});
      return parts.month && parts.day && parts.hour && parts.minute ? `${{parts.month}}.${{parts.day}} ${{parts.hour}}:${{parts.minute}}` : '';
    }}

    function firstDateLabel(record, keys) {{
      for (const key of keys) {{
        const label = compactDateLabelFromValue(record?.[key]);
        if (label) return label;
      }}
      return '';
    }}

    function storyDateLabel(story) {{
      return firstDateLabel(story, ['published_at', 'last_article_seen_at', 'last_article_at', 'sort_at', 'created_at', 'datetime']);
    }}

    function articleDateLabel(article) {{
      return firstDateLabel(article, ['published_at', 'article_published_at', 'feed_published_at', 'seen_at', 'sort_at', 'created_at', 'updated_at', 'datetime']);
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
        '제약', '바이오', '레이더', '정기', '마무리', '매출', '클럽',
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
        '종료보고서', '불성실공시법인', '감독', '제재', '이사회', '의장', '이사',
        '사외이사', '감사', '감사위원', '선임', '검토', 'board', 'director', 'directors',
        'chair', 'chairman', 'nominee', 'nominees', '정기', '마무리', '매출', '성장', '개선', '통해', '실적'
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
      return currentContextArticles(details).length > 1;
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

    function usefulTelegramMentions(mentions, tokens = []) {{
      return Array.isArray(mentions)
        ? mentions
          .filter((message) => message && (message.message_url || message.url) && (message.text || message.excerpt || message.context_excerpt))
          .filter((message) => telegramMentionIsUseful(message, tokens))
          .slice(0, 5)
        : [];
    }}

    function storyContextHasStaticContent(details) {{
      const story = details.closest('[data-story]');
      const storyTitle = story?.querySelector('h3')?.textContent || '';
      const tokens = contextFilterTokens(story?.dataset.storyDbQuery || '', storyTitle);
      return storyContextHasCurrentLinks(details) || usefulTelegramMentions(staticTelegramMentions(details), tokens).length > 0;
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

    function telegramTokenHitCount(text, tokens) {{
      const lowered = String(text || '').toLowerCase();
      return (tokens || []).filter((token) => lowered.includes(String(token || '').toLowerCase())).length;
    }}

    function telegramMentionIsUseful(item, tokens) {{
      const matchType = String(item.match_type || '');
      if (matchType === 'exact_url' || matchType === 'canonical_url') return true;
      const score = Number(item.score || 0);
      const flags = Array.isArray(item.risk_flags) ? item.risk_flags : [];
      if (flags.includes('promotional')) return false;
      const text = `${{item.context_excerpt || ''}} ${{item.excerpt || ''}} ${{item.text || ''}}`;
      const hits = telegramTokenHitCount(text, tokens);
      return score >= 0.58 && hits >= Math.min(2, Math.max(1, tokens.length));
    }}

    function telegramContextSnippet(item, tokens) {{
      const raw = String(item.context_excerpt || item.excerpt || item.text || '').replace(/\\s+/g, ' ').trim();
      if (!raw) return '';
      const lowered = raw.toLowerCase();
      const hit = (tokens || []).find((token) => lowered.includes(String(token || '').toLowerCase()));
      if (!hit || raw.startsWith('관련 문맥:')) return compactDbText(raw, 138);
      const index = Math.max(0, lowered.indexOf(String(hit).toLowerCase()) - 42);
      const snippet = raw.slice(index, index + 138);
      return compactDbText(`관련 문맥: ${{index > 0 ? '... ' : ''}}${{snippet}}${{index + 138 < raw.length ? ' ...' : ''}}`, 154);
    }}

    function renderTelegramMentions(body, mentions, tokens = []) {{
      const items = usefulTelegramMentions(mentions, tokens);
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
        [item.channel_title || item.channel_handle || item.handle || '공개 채널', item.posted_at || '', telegramMatchLabel(item.match_type), item.reason || '', ...(Array.isArray(item.risk_flags) ? item.risk_flags : [])].filter(Boolean).forEach((value) => {{
          const span = document.createElement('span');
          span.textContent = compactDbText(String(value), 52);
          meta.appendChild(span);
        }});
        const excerpt = document.createElement('p');
        excerpt.textContent = telegramContextSnippet(item, tokens);
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
      const filteredTelegramMentions = usefulTelegramMentions(telegramMentions, filterTokens);
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
      const hasGroupedCurrent = currentItems.length > 1;
      const hasRelatedContext = hasGroupedCurrent || archiveItems.length > 0 || filteredTelegramMentions.length > 0;
      const items = (hasGroupedCurrent || archiveItems.length > 0) ? [...currentItems, ...archiveItems] : [];
      body.innerHTML = '';
      if (!hasRelatedContext) {{
        if (storyContextHasStaticContent(details)) {{
          body.hidden = true;
        }} else {{
          details.open = false;
          details.hidden = true;
          details.dataset.empty = '1';
        }}
        return;
      }}
      details.hidden = false;
      details.dataset.contextPending = '0';
      delete details.dataset.empty;
      body.hidden = false;

      if (!items.length) {{
        renderTelegramMentions(body, filteredTelegramMentions, filterTokens);
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

      const list = document.createElement('div');
      list.className = 'story-context__articles';
      items.forEach((article) => {{
        const row = document.createElement('a');
        row.className = `story-context__article${{article.context_kind === 'current' ? ' story-context__article--current' : ''}}`;
        row.href = article.canonical_url;
        row.target = '_blank';
        row.rel = 'noopener noreferrer';
        const meta = document.createElement('div');
        meta.className = 'story-context__article-meta';
        const kind = document.createElement('span');
        kind.className = `story-context__kind${{article.context_kind === 'archive' ? ' story-context__kind--archive' : ''}}`;
        kind.textContent = contextKindLabel(article);
        meta.appendChild(kind);
        [articleDateLabel(article) || '일시 미상', article.source || article.feed_name || '매체 미상', ...articleMatchReasons(article, story.dataset.storyDbQuery || storyTitle)].filter(Boolean).forEach((value) => {{
          const span = document.createElement('span');
          span.textContent = compactDbText(value, 42);
          meta.appendChild(span);
        }});
        const title = document.createElement('strong');
        title.className = 'story-context__article-title';
        title.textContent = compactDbText(article.title, 108);
        const snippetText = articleSearchSnippet(article, story.dataset.storyDbQuery || storyTitle);
        row.appendChild(meta);
        row.appendChild(title);
        if (snippetText) {{
          const snippet = document.createElement('p');
          snippet.className = 'story-context__article-snippet';
          snippet.textContent = snippetText;
          row.appendChild(snippet);
        }}
        list.appendChild(row);
      }});
      body.appendChild(list);
      renderTelegramMentions(body, filteredTelegramMentions, filterTokens);
    }}

    function contextFallbackQueries(query, title = '') {{
      const tokens = contextFilterTokens(query, title);
      const strongTokens = tokens.filter((token) => token.length >= 3 && !isWeakContextToken(token));
      const eventTokens = tokens.filter((token) => token.length >= 2 && isWeakContextToken(token));
      const queries = [];
      if (strongTokens[0]) queries.push(strongTokens[0]);
      if (strongTokens[0] && eventTokens[0]) queries.push(`${{strongTokens[0]}} ${{eventTokens[0]}}`);
      if (strongTokens.length >= 2) queries.push(`${{strongTokens[0]}} ${{strongTokens[1]}}`);
      return Array.from(new Set(queries.filter((item) => item && item !== query))).slice(0, 3);
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
      const storyTitle = story.querySelector('h3')?.textContent || '';
      const filterTokens = contextFilterTokens(query, storyTitle);
      let storyArticles = [];
      let queryArticles = [];
      let telegramMentions = staticTelegramMentions(details);
      if (storyKey) {{
        storyArticles = await fetchDbArticles({{ story_key: storyKey, limit: '16', days: '180' }});
      }}
      if (query) {{
        const currentItems = mergeContextArticles([storyArticles]).filter((article) => articleMatchesContext(article, filterTokens));
        if (currentItems.length < 4) {{
          queryArticles = await fetchDbArticles({{ q: query, limit: '12', days: '180' }});
        }}
        let queryItems = mergeContextArticles([queryArticles]).filter((article) => articleMatchesContext(article, filterTokens));
        if (currentItems.length + queryItems.length < 3) {{
          for (const fallbackQuery of contextFallbackQueries(query, storyTitle)) {{
            const fallbackArticles = await fetchDbArticles({{ q: fallbackQuery, limit: '12', days: '180' }});
            queryArticles = mergeContextArticles([queryArticles, fallbackArticles]);
            queryItems = mergeContextArticles([queryArticles]).filter((article) => articleMatchesContext(article, filterTokens));
            if (currentItems.length + queryItems.length >= 3) break;
          }}
        }}
      }}
      telegramMentions = mergeTelegramMentions([telegramMentions, await fetchTelegramMentions(story)]);
      renderStoryContext(details, storyArticles, queryArticles, telegramMentions);
      details.dataset.loaded = '1';
    }}

    function hideUnavailableStoryContexts() {{
      if (remoteReportsApiUrl) return;
      storyContextDetails.forEach((details) => {{
        if (!storyContextHasStaticContent(details)) details.hidden = true;
      }});
    }}

    function preloadPendingStoryContexts() {{
      if (!remoteReportsApiUrl) return;
      const pending = storyContextDetails.filter((details) => details.hidden && details.dataset.contextPending === '1');
      if (!pending.length) return;
      if (!('IntersectionObserver' in window)) {{
        pending.slice(0, 12).forEach((details) => loadStoryContext(details));
        return;
      }}
      const detailsByTarget = new Map();
      const observer = new IntersectionObserver((entries) => {{
        entries.forEach((entry) => {{
          if (!entry.isIntersecting) return;
          const details = detailsByTarget.get(entry.target);
          if (details) loadStoryContext(details);
          observer.unobserve(entry.target);
          detailsByTarget.delete(entry.target);
        }});
      }}, {{ rootMargin: '900px 0px' }});
      pending.forEach((details) => {{
        const target = details.closest('[data-story]') || details;
        detailsByTarget.set(target, details);
        observer.observe(target);
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
    storyContextDetails.forEach((details) => {{
      details.addEventListener('toggle', () => {{
        if (details.open) loadStoryContext(details);
      }});
    }});
    hideUnavailableStoryContexts();
    preloadPendingStoryContexts();
    updateNavigation();
    loadRemoteArchiveLinks();
    loadDbPulse();
  </script>
</body>
</html>
"""


def telegram_daily_dt(value: object, config: dict[str, object]) -> datetime | None:
    return parse_datetime(value, str(config.get("timezone") or "Asia/Seoul"))


def telegram_daily_messages(
    state: dict[str, object],
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    channel_lookup = telegram_daily_channel_lookup(state)
    for message in state.get("telegram_source_messages", []):
        if not isinstance(message, dict) or message.get("deleted_at"):
            continue
        posted_at = telegram_daily_dt(message.get("posted_at"), config)
        if posted_at and start_at <= posted_at <= end_at:
            row = dict(message)
            channel = telegram_daily_channel_record_for_message(row, channel_lookup)
            channel_type = telegram_daily_classify_channel(channel, row)
            row["channel_content_type"] = channel_type
            row["channel_content_type_label"] = telegram_daily_channel_type_label(channel_type)
            if channel and not row.get("channel_title"):
                row["channel_title"] = channel.get("title") or ""
            rows.append(row)
    rows.sort(key=lambda item: telegram_daily_dt(item.get("posted_at"), config) or start_at, reverse=True)
    return rows


def telegram_daily_story_rows(stories: list[dict[str, object]]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for story in stories:
        mentions = [
            mention
            for mention in (story.get("telegram_mentions") if isinstance(story.get("telegram_mentions"), list) else [])
            if isinstance(mention, dict)
        ]
        if not mentions:
            continue
        channels = {
            str(mention.get("channel_handle") or mention.get("channel_title") or "")
            for mention in mentions
            if mention.get("channel_handle") or mention.get("channel_title")
        }
        rows.append(
            {
                "story": story,
                "mentions": mentions,
                "mention_count": len(mentions),
                "channel_count": len(channels),
            }
        )
    return sorted(rows, key=lambda row: (int(row["mention_count"]), int(row["channel_count"])), reverse=True)


def telegram_daily_signal_rows(
    state: dict[str, object],
    *,
    limit: int = 10,
    config: dict[str, object] | None = None,
    start_at: datetime | None = None,
    end_at: datetime | None = None,
) -> list[dict[str, object]]:
    signals = [
        signal
        for signal in state.get("telegram_issue_signals", [])
        if isinstance(signal, dict) and int(signal.get("related_telegram_count") or 0) > 0
    ]
    if config and start_at and end_at:
        filtered_signals: list[dict[str, object]] = []
        for signal in signals:
            first_seen_at = telegram_daily_dt(signal.get("first_seen_at"), config)
            latest_seen_at = telegram_daily_dt(signal.get("latest_seen_at"), config)
            if latest_seen_at and latest_seen_at < start_at:
                continue
            if first_seen_at and first_seen_at > end_at:
                continue
            filtered_signals.append(signal)
        signals = filtered_signals
    signals.sort(
        key=lambda signal: (
            float(signal.get("confidence_score") or 0),
            int(signal.get("related_telegram_channels_count") or 0),
            int(signal.get("related_telegram_count") or 0),
        ),
        reverse=True,
    )
    return signals[:limit]


def telegram_daily_excerpt(value: object, *, max_chars: int = 150) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    return compact_text(text, max_chars=max_chars)


def telegram_daily_list(value: object, *, limit: int = 5) -> list[str]:
    if not isinstance(value, list):
        return []
    rows = [str(item).strip() for item in value if str(item).strip()]
    return rows[:limit]


def telegram_daily_signal_title(signal: dict[str, object]) -> str:
    raw_title = str(signal.get("signal_title") or "").strip()
    if raw_title and not raw_title.startswith("기사 매칭 "):
        return compact_text(raw_title, max_chars=82)
    keywords = telegram_daily_list(signal.get("top_keywords"), limit=4)
    if keywords:
        return compact_text(" · ".join(keywords), max_chars=82)
    return "Telegram 언급 신호"


def telegram_daily_signal_type_label(signal_type: object) -> str:
    labels = {
        "article_match": "기사 반응",
        "url_burst": "URL 확산",
        "topic_burst": "주제 급증",
        "entity_rising": "종목 부상",
        "risk_watch": "검증 필요",
    }
    return labels.get(str(signal_type or ""), "시장 언급")


def telegram_daily_signal_risk_flags(signal: dict[str, object]) -> list[str]:
    flags = [str(flag) for flag in signal.get("risk_flags", []) if str(flag)]
    count = int(signal.get("related_telegram_count") or 0)
    channels = int(signal.get("related_telegram_channels_count") or 0)
    if count >= 5 and channels <= 1 and "single_source" not in flags:
        flags.append("single_source")
    return flags


def telegram_daily_signal_confirmation(signal: dict[str, object]) -> dict[str, str]:
    signal_type = str(signal.get("signal_type") or "")
    direct_count = int(signal.get("direct_url_count") or 0)
    keyword_count = int(signal.get("keyword_match_count") or 0)
    if direct_count > 0:
        return {"key": "direct", "label": f"URL 직접 {direct_count}건", "tone": "confirmed"}
    if signal_type == "url_burst":
        return {"key": "url_burst", "label": "동일 URL 확산", "tone": "confirmed"}
    if keyword_count > 0:
        return {"key": "keyword", "label": f"키워드 추정 {keyword_count}건", "tone": "estimated"}
    return {"key": "tg_only", "label": "Telegram-only", "tone": "watch"}


def telegram_daily_signal_channel_types(signal: dict[str, object]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for message in signal.get("top_related_messages", []):
        if not isinstance(message, dict):
            continue
        channel_type = telegram_daily_classify_channel(
            {},
            {
                "channel_title": message.get("channel_title") or message.get("channel_handle"),
                "text": message.get("excerpt") or "",
            },
        )
        counter[channel_type] += 1
    return counter


def telegram_daily_signal_institutional_relevance(
    signal: dict[str, object],
    channel_types: Counter[str],
) -> int:
    text = " ".join(
        [
            telegram_daily_signal_title(signal),
            str(signal.get("signal_summary") or ""),
            " ".join(telegram_daily_list(signal.get("top_keywords"), limit=10)),
        ]
    )
    institutional_terms = (
        "공시",
        "실적",
        "리포트",
        "목표가",
        "투자의견",
        "주주",
        "밸류업",
        "자사주",
        "배당",
        "거버넌스",
        "경영권",
        "이사회",
        "주총",
        "공개매수",
        "유상증자",
        "상장폐지",
    )
    score = 0
    if channel_types.get("research"):
        score += 32
    if channel_types.get("disclosure"):
        score += 30
    if channel_types.get("news"):
        score += 10
    score += min(28, sum(1 for term in institutional_terms if term in text) * 7)
    return min(100, score)


def telegram_daily_signal_scores(signal: dict[str, object]) -> dict[str, object]:
    count = int(signal.get("related_telegram_count") or 0)
    channels = int(signal.get("related_telegram_channels_count") or 0)
    confidence = max(0.0, min(1.0, float(signal.get("confidence_score") or 0)))
    signal_type = str(signal.get("signal_type") or "")
    direct_count = int(signal.get("direct_url_count") or 0)
    keyword_count = int(signal.get("keyword_match_count") or 0)
    risk_flags = telegram_daily_signal_risk_flags(signal)
    channel_types = telegram_daily_signal_channel_types(signal)
    confirmation = telegram_daily_signal_confirmation(signal)

    velocity_score = min(100, count * 5 + channels * 3)
    breadth_score = min(100, channels * 7 + len(channel_types) * 12)
    quality_score = min(100, int(confidence * 44) + min(28, direct_count * 14) + min(28, channels * 4))
    novelty_score = min(100, 40 + (20 if signal_type in {"topic_burst", "url_burst"} else 0) + min(40, count * 3))
    confirmation_score = 90 if direct_count else 78 if signal_type == "url_burst" else 58 if keyword_count else 32
    institutional_score = telegram_daily_signal_institutional_relevance(signal, channel_types)
    risk_score = min(100, len(risk_flags) * 22 + (16 if channels <= 1 and count >= 5 else 0) + (8 if not direct_count else 0))
    attention_score = min(
        100,
        round(
            velocity_score * 0.22
            + breadth_score * 0.18
            + quality_score * 0.16
            + novelty_score * 0.14
            + confirmation_score * 0.15
            + institutional_score * 0.15
        ),
    )

    badges: list[dict[str, str]] = []
    if velocity_score >= 60 or signal_type in {"topic_burst", "url_burst"}:
        badges.append({"key": "rising", "label": "RISING"})
    if confirmation["key"] in {"direct", "url_burst"}:
        badges.append({"key": "confirmed", "label": "CONFIRMED"})
    elif confirmation["key"] == "keyword":
        badges.append({"key": "estimated", "label": "KEYWORD-MATCH"})
    if institutional_score >= 45:
        badges.append({"key": "institutional", "label": "INSTITUTIONAL"})
    if risk_score >= 40:
        badges.append({"key": "risk", "label": "RISK WATCH"})
    if confirmation["key"] == "tg_only":
        badges.append({"key": "tg_only", "label": "TG-ONLY"})

    return {
        "market_attention_score": attention_score,
        "velocity_score": velocity_score,
        "source_breadth_score": breadth_score,
        "quality_weighted_score": quality_score,
        "novelty_score": novelty_score,
        "news_confirmation_score": confirmation_score,
        "institutional_relevance_score": institutional_score,
        "rumor_risk_score": risk_score,
        "risk_flags": risk_flags,
        "channel_types": [
            {
                "key": key,
                "label": telegram_daily_channel_type_label(key),
                "count": value,
            }
            for key, value in channel_types.most_common()
        ],
        "confirmation": confirmation,
        "badges": badges,
    }


def telegram_daily_enriched_signals(signal_rows: list[dict[str, object]]) -> list[dict[str, object]]:
    enriched: list[dict[str, object]] = []
    for signal in signal_rows:
        row = dict(signal)
        row.update(telegram_daily_signal_scores(row))
        row["display_title"] = telegram_daily_signal_title(row)
        row["signal_type_label"] = telegram_daily_signal_type_label(row.get("signal_type"))
        enriched.append(row)
    enriched.sort(
        key=lambda row: (
            int(row.get("market_attention_score") or 0),
            int(row.get("related_telegram_channels_count") or 0),
            int(row.get("related_telegram_count") or 0),
        ),
        reverse=True,
    )
    return enriched


def telegram_daily_signal_summary_counts(signals: list[dict[str, object]]) -> dict[str, int]:
    return {
        "rising": sum(1 for signal in signals if any(badge.get("key") == "rising" for badge in signal.get("badges", []) if isinstance(badge, dict))),
        "confirmed": sum(1 for signal in signals if str((signal.get("confirmation") or {}).get("key") if isinstance(signal.get("confirmation"), dict) else "") in {"direct", "url_burst"}),
        "institutional": sum(1 for signal in signals if int(signal.get("institutional_relevance_score") or 0) >= 45),
        "risk": sum(1 for signal in signals if int(signal.get("rumor_risk_score") or 0) >= 40),
        "tg_only": sum(1 for signal in signals if str((signal.get("confirmation") or {}).get("key") if isinstance(signal.get("confirmation"), dict) else "") == "tg_only"),
    }


TELEGRAM_DAILY_CHANNEL_TYPES = (
    {
        "key": "news",
        "label": "뉴스·기사",
        "description": "기사 링크와 속보성 뉴스 공유가 중심인 채널",
    },
    {
        "key": "disclosure",
        "label": "공시·속보",
        "description": "공시, 실적, 거래소·감독 이벤트 알림이 많은 채널",
    },
    {
        "key": "research",
        "label": "리서치·종목분석",
        "description": "증권사 리포트, 기업분석, 종목 코멘트가 많은 채널",
    },
    {
        "key": "macro",
        "label": "매크로·해외",
        "description": "해외주식, 금리, 환율, 채권, 글로벌 뉴스 중심 채널",
    },
    {
        "key": "community",
        "label": "시장 코멘트",
        "description": "투자자 반응, 커뮤니티성 코멘트, 빠른 시장 언급 채널",
    },
)
TELEGRAM_DAILY_ANALYSIS_PERIODS = (
    {"key": "1d", "label": "1일", "days": 1},
    {"key": "3d", "label": "3일", "days": 3},
    {"key": "7d", "label": "일주일", "days": 7},
    {"key": "30d", "label": "한달", "days": 30},
)
TELEGRAM_DAILY_SESSION_BUCKETS = (
    {"key": "pre_market", "label": "장전", "detail": "06:00-09:00"},
    {"key": "morning", "label": "오전", "detail": "09:00-12:00"},
    {"key": "regular_close", "label": "장중 후반", "detail": "12:00-15:30"},
    {"key": "after_market", "label": "이후", "detail": "15:30-익일 06:00"},
)
TELEGRAM_DAILY_KEYWORD_STOPWORDS = CONTEXT_EXCERPT_STOPWORDS | {
    "ai",
    "a",
    "b",
    "c",
    "com",
    "co",
    "kr",
    "net",
    "org",
    "www",
    "http",
    "https",
    "t",
    "me",
    "amp",
    "api",
    "pdf",
    "url",
    "view",
    "rss",
    "qoq",
    "yoy",
    "clt",
    "id",
    "뉴스",
    "기사",
    "관련",
    "문맥",
    "링크",
    "보고서",
    "보고서명",
    "공시링크",
    "회사정보",
    "기업명",
    "기업정보",
    "리포트",
    "자료",
    "현재",
    "오늘",
    "내일",
    "오전",
    "오후",
    "최근",
    "이번",
    "대한",
    "대비",
    "기준",
    "내용",
    "주요",
    "확인",
    "합니다",
    "했습니다",
    "있습니다",
    "한다",
    "했다",
    "시장",
    "주식",
    "종목",
    "투자",
    "증권",
    "채널",
    "한국투자증권",
    "한투증권",
    "서울경제",
    "프리미엄",
    "컨버전스",
    "미디어",
    "시그널",
    "signal",
    "본문보기",
    "본문",
    "키워드",
    "함께",
    "바른",
    "길을",
    "투자의",
    "네이버뉴스",
    "다음뉴스",
    "것으로",
    "있다",
    "있는",
    "이는",
    "이런",
    "모든",
    "전체",
    "가능성",
    "보도했습니다",
    "보도했습니다.",
    "보도됨",
    "panews는",
    "특히",
    "통해",
    "없음",
    "최종",
    "핵심",
    "수요가",
    "있으며",
    "있고",
    "따르면",
    "따라",
    "아니다",
    "보고자",
    "이후",
    "한국",
    "요약",
    "하여",
    "내용을",
    "클릭",
    "읽으십시오",
    "학습",
    "향후",
    "지원",
    "않으면",
    "판단은",
    "여긴",
    "모른다",
    "핵심적",
    "투자책임",
    "내용이",
    "전용이다",
    "sight",
    "직접해라",
    "기다릴뿐",
    "떄까지",
    "오지",
    "원문보기",
    "테마보기",
    "그것은",
    "우라가",
    "부정확할",
    "인사이트가",
    "이러한",
    "하고",
    "있다고",
    "인용하여",
    "따른",
    "따라서",
    "했다고",
    "된다",
    "됐다",
    "이라고",
    "이라며",
    "라는",
    "통신",
    "교도통신",
    "제목",
    "시간외",
    "컨센",
    "발표",
    "기존",
    "000으로",
    "위한",
    "있다는",
    "가장",
    "일부",
    "높은",
    "메세지",
    "세계",
    "최대",
    "단독",
    "속보",
    "억원",
    "내용은",
    "브리핑",
    "위해",
    "위해서",
    "등으로",
    "관련해",
    "관련한",
    "빠르게",
    "국내외",
    "받을",
    "제공합니다",
    "인용한",
    "별도의",
    "절차",
    "없이",
    "양승수",
    "전기전자",
    "메리츠증권",
}
TELEGRAM_DAILY_KEYWORD_ALIASES = {
    "벨류업": "밸류업",
    "valueup": "밸류업",
    "buyback": "자사주",
    "governance": "거버넌스",
    "activism": "행동주의",
    "activist": "행동주의",
}
TELEGRAM_DAILY_COMPANY_STOPWORDS = {
    "Google",
    "Google News",
    "Investing",
    "Investing.com",
    "한국투자증권",
    "한투증권",
    "서울경제",
    "LS증권",
    "대신증권",
    "키움증권",
    "하나증권",
    "현대차증권",
    "SK증권",
    "다올투자증권",
    "유진투자증권",
    "미래에셋증권",
    "NH투자증권",
    "KB증권",
    "삼성증권",
    "신한투자증권",
    "메리츠증권",
    "연합인포맥스",
    "매일경제TV",
    "네이버",
    "NAVER",
    "전기전자",
    "방위산업",
    "세계은행",
    "투자은행",
    "워킹그룹",
    "국제에너지",
    "국제정세",
    "미국증시",
    "중국증시",
    "50만전자",
    "30만전자",
    "주요주주특정증권",
    "단순투자",
    "장기투자",
    "국제금융",
    "공동보유",
    "공급부족",
    "조각투자",
    "금융투자",
    "중앙은행",
    "석유화학",
    "글로벌에너지",
    "한국은행",
    "교도통신",
    "재생에너지",
    "금융위원회",
    "금융감독원",
    "증권선물위원회",
    "공정거래위원회",
    "공정위",
    "거래소",
    "한국거래소",
    "코스피",
    "코스닥",
    "타법인주식및출자증권",
    "불성실공시법인",
    "공시대상기업집단",
    "소액공모공시서류",
    "주요사항보고서",
}
TELEGRAM_DAILY_EXTRA_COMPANY_NAMES = {
    "삼성전자",
    "SK하이닉스",
    "LG에너지솔루션",
    "LG유플러스",
    "카카오뱅크",
    "카카오페이",
    "기아",
    "현대글로비스",
    "현대건설",
    "현대제철",
    "포스코퓨처엠",
    "POSCO홀딩스",
    "삼성바이오로직스",
    "삼성생명",
    "삼성화재",
    "한화생명",
    "한화에어로스페이스",
    "두산에너빌리티",
    "에이비엘바이오",
    "효성중공업",
    "하이브",
    "큐로홀딩스",
}
TELEGRAM_DAILY_KNOWN_COMPANY_NAMES = {company.casefold() for company in KNOWN_COMPANIES + list(TELEGRAM_DAILY_EXTRA_COMPANY_NAMES)}
TELEGRAM_DAILY_COMPANY_NOISE_PATTERN = re.compile(
    r"(?:특정증권|단순투자|장기투자|국제금융|전기전자|방위산업|세계은행|투자은행|워킹그룹|국제에너지|공동보유|공급부족|조각투자|금융투자|네이버뉴스|다음뉴스|공시대상|불성실공시|주요사항보고서)"
)


def html_json(value: object) -> str:
    return (
        json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        .replace("&", "\\u0026")
        .replace("<", "\\u003c")
        .replace(">", "\\u003e")
    )


def telegram_daily_channel_type_label(type_key: object) -> str:
    key = str(type_key or "")
    for channel_type in TELEGRAM_DAILY_CHANNEL_TYPES:
        if channel_type["key"] == key:
            return str(channel_type["label"])
    return "기타"


def telegram_daily_channel_type_options() -> list[dict[str, str]]:
    return [
        {"key": "all", "label": "전체", "description": "모든 수집 채널"}
    ] + [dict(channel_type) for channel_type in TELEGRAM_DAILY_CHANNEL_TYPES]


def telegram_daily_channel_lookup(state: dict[str, object]) -> dict[str, dict[str, object]]:
    lookup: dict[str, dict[str, object]] = {}
    for channel in state.get("telegram_source_channels", []):
        if not isinstance(channel, dict):
            continue
        handle = str(channel.get("handle") or "").removeprefix("@").casefold()
        channel_id = str(channel.get("telegram_channel_id") or "").strip()
        if handle:
            lookup[f"handle:{handle}"] = channel
        if channel_id:
            lookup[f"id:{channel_id}"] = channel
    return lookup


def telegram_daily_channel_record_for_message(
    message: dict[str, object],
    lookup: dict[str, dict[str, object]],
) -> dict[str, object]:
    handle = str(message.get("handle") or message.get("channel_handle") or "").removeprefix("@").casefold()
    channel_id = str(message.get("telegram_channel_id") or "").strip()
    if channel_id and f"id:{channel_id}" in lookup:
        return lookup[f"id:{channel_id}"]
    if handle and f"handle:{handle}" in lookup:
        return lookup[f"handle:{handle}"]
    return {}


def telegram_daily_classify_channel(channel: dict[str, object], message: dict[str, object] | None = None) -> str:
    explicit = str(
        channel.get("content_type")
        or channel.get("channel_type")
        or (message or {}).get("channel_content_type")
        or (message or {}).get("content_type")
        or ""
    ).strip().casefold()
    alias_map = {
        "article": "news",
        "articles": "news",
        "news": "news",
        "news_only": "news",
        "disclosure": "disclosure",
        "alert": "disclosure",
        "research": "research",
        "analysis": "research",
        "macro": "macro",
        "global": "macro",
        "community": "community",
        "commentary": "community",
    }
    if explicit in alias_map:
        return alias_map[explicit]

    text = " ".join(
        str(value or "")
        for value in (
            channel.get("handle"),
            channel.get("title"),
            channel.get("description"),
            (message or {}).get("channel_title"),
            (message or {}).get("text"),
        )
    ).casefold()
    scores = {
        "news": sum(1 for token in ("뉴스", "속보", "신문", "경제tv", "issue", "news", "realtime") if token in text),
        "disclosure": sum(1 for token in ("공시", "dart", "kind", "거래소", "실적분석", "불성실공시", "알리미", "disclosure") if token in text),
        "research": sum(1 for token in ("리서치", "리포트", "기업분석", "종목분석", "증권사", "애널", "report", "research") if token in text),
        "macro": sum(1 for token in ("매크로", "해외", "글로벌", "미국", "채권", "환율", "금리", "크레딧", "global", "macro") if token in text),
        "community": sum(1 for token in ("주식", "종목", "전략", "메신저", "커뮤니티", "오를주식", "머니", "stock") if token in text),
    }
    if scores["disclosure"] >= 1 and scores["disclosure"] >= scores["news"]:
        return "disclosure"
    best_key, best_score = max(scores.items(), key=lambda item: item[1])
    return best_key if best_score > 0 else "community"


def telegram_daily_message_text(message: dict[str, object]) -> str:
    return str(message.get("normalized_text") or message.get("text") or "")


def telegram_daily_keyword_token(token: str) -> str:
    cleaned = (token or "").strip(" \t\r\n#@$/\\[](){},.:;·'\"“”‘’<>")
    for suffix in ("에서는", "에게는", "으로는", "이라는", "이라고", "이라며", "부터", "까지", "보다", "처럼", "에서", "으로", "에게", "와의", "과의", "들은", "들이", "에도", "에는"):
        if len(cleaned) > len(suffix) + 2 and cleaned.endswith(suffix):
            cleaned = cleaned[: -len(suffix)]
            break
    return TELEGRAM_DAILY_KEYWORD_ALIASES.get(cleaned.casefold(), cleaned)


def telegram_daily_keyword_tokens(text: str) -> list[str]:
    without_urls = re.sub(r"https?://\S+|www\.\S+|t\.me/\S+", " ", text or "", flags=re.I)
    tokens: list[str] = []
    for raw_token in re.findall(r"[가-힣A-Za-z][가-힣A-Za-z0-9&.+_-]{1,}", without_urls):
        token = telegram_daily_keyword_token(raw_token)
        lowered = token.casefold()
        if len(token) < 2 or lowered in TELEGRAM_DAILY_KEYWORD_STOPWORDS:
            continue
        if any(part in lowered for part in ("stockinfo", "telegram", "sedaily", "rassiro")):
            continue
        if any(lowered.endswith(suffix) for suffix in (".com", ".co.kr", ".kr", ".net", ".org", ".io", ".ai")):
            continue
        if re.fullmatch(r"\d+(?:년|월|일|분기|조|억|원|%)?", token):
            continue
        if re.fullmatch(r"[A-Za-z]?\d{1,4}[A-Za-z]?", token):
            continue
        if re.fullmatch(r"[A-Za-z]{2}", token) and lowered != "ai":
            continue
        if len(token) > 18:
            token = compact_text(token, max_chars=18)
        tokens.append(token)
    return tokens


def telegram_daily_cached_keyword_tokens(message: dict[str, object]) -> list[str]:
    cached = message.get("_keyword_tokens")
    if isinstance(cached, list):
        return [str(token) for token in cached]
    tokens = telegram_daily_keyword_tokens(telegram_daily_message_text(message))
    message["_keyword_tokens"] = tokens
    message["_keyword_token_keys"] = {token.casefold() for token in tokens}
    return tokens


def telegram_daily_cached_keyword_keys(message: dict[str, object]) -> set[str]:
    cached = message.get("_keyword_token_keys")
    if isinstance(cached, set):
        return {str(token) for token in cached}
    tokens = telegram_daily_cached_keyword_tokens(message)
    keys = {token.casefold() for token in tokens}
    message["_keyword_token_keys"] = keys
    return keys


def telegram_daily_keyword_cloud(
    messages: list[dict[str, object]],
    signal_rows: list[dict[str, object]],
    *,
    limit: int = 34,
) -> list[dict[str, object]]:
    counter: Counter[str] = Counter()
    channel_map: dict[str, Counter[str]] = defaultdict(Counter)
    display_names: dict[str, str] = {}
    for message in messages:
        channel = str(message.get("channel_title") or message.get("handle") or "")
        unique_tokens = set(telegram_daily_cached_keyword_tokens(message))
        for token in unique_tokens:
            key = token.casefold()
            counter[key] += 1
            display_names.setdefault(key, token)
            if channel:
                channel_map[key][channel] += 1

    for signal in signal_rows:
        for keyword in telegram_daily_list(signal.get("top_keywords"), limit=6):
            token = telegram_daily_keyword_token(keyword)
            key = token.casefold()
            if not token or key in TELEGRAM_DAILY_KEYWORD_STOPWORDS:
                continue
            counter[key] += 2
            display_names.setdefault(key, token)

    if not counter:
        return []
    max_count = max(counter.values()) or 1
    rows: list[dict[str, object]] = []
    for key, count in counter.most_common(limit):
        level = 3 if max_count <= 1 else 1 + int((count - 1) / max(1, max_count - 1) * 5)
        rows.append(
            {
                "keyword": display_names.get(key, key),
                "count": count,
                "channels_count": len(channel_map.get(key, Counter())),
                "top_channels": [
                    {"label": label, "count": channel_count}
                    for label, channel_count in channel_map.get(key, Counter()).most_common(3)
                ],
                "level": max(1, min(6, level)),
            }
        )
    return rows


def telegram_daily_message_has_keyword(message: dict[str, object], keyword: str) -> bool:
    token = telegram_daily_keyword_token(keyword).casefold()
    if not token:
        return False
    if token in telegram_daily_cached_keyword_keys(message):
        return True
    return token in telegram_daily_message_text(message).casefold()


def telegram_daily_keyword_trend(
    messages: list[dict[str, object]],
    keyword: str,
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
    period_key: str,
) -> list[dict[str, object]]:
    if period_key == "1d":
        buckets = telegram_daily_time_buckets(start_at, end_at, config)
        counts = [0 for _ in buckets]
        for message in messages:
            posted_at = telegram_daily_dt(message.get("posted_at"), config)
            if posted_at and telegram_daily_message_has_keyword(message, keyword):
                counts[telegram_daily_bucket_index(posted_at, buckets, config)] += 1
        return [
            {"label": str(bucket.get("label") or ""), "detail": str(bucket.get("detail") or ""), "count": counts[index]}
            for index, bucket in enumerate(buckets)
        ]

    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    timezone = ZoneInfo(timezone_name)
    start_date = start_at.astimezone(timezone).date()
    end_date = end_at.astimezone(timezone).date()
    day_count = max(1, (end_date - start_date).days + 1)
    labels = [(start_date + timedelta(days=index)).strftime("%m.%d") for index in range(day_count)]
    counts = [0 for _ in labels]
    for message in messages:
        posted_at = telegram_daily_dt(message.get("posted_at"), config)
        if not posted_at or not telegram_daily_message_has_keyword(message, keyword):
            continue
        index = (posted_at.astimezone(timezone).date() - start_date).days
        if 0 <= index < len(counts):
            counts[index] += 1
    return [{"label": label, "count": counts[index]} for index, label in enumerate(labels)]


def telegram_daily_keyword_details(
    keyword_rows: list[dict[str, object]],
    messages: list[dict[str, object]],
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
    period_key: str,
    channel_filter: str,
    *,
    limit: int = 30,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for keyword_row in keyword_rows[:limit]:
        keyword = str(keyword_row.get("keyword") or "").strip()
        if not keyword:
            continue
        matched_messages = [
            message
            for message in messages
            if telegram_daily_message_has_keyword(message, keyword)
        ]
        channel_counter: Counter[str] = Counter(
            str(message.get("channel_title") or message.get("handle") or "Telegram")
            for message in matched_messages
        )
        type_counter: Counter[str] = Counter(
            str(message.get("channel_content_type") or "community")
            for message in matched_messages
        )
        recent_messages = []
        for message in matched_messages[:5]:
            posted_at = telegram_daily_dt(message.get("posted_at"), config)
            recent_messages.append(
                {
                    "channel": str(message.get("channel_title") or message.get("handle") or "Telegram"),
                    "channel_type": telegram_daily_channel_type_label(message.get("channel_content_type")),
                    "posted_at": date_label(posted_at, config) if posted_at else "일시 미상",
                    "url": str(message.get("message_url") or ""),
                    "excerpt": contextual_text_excerpt(telegram_daily_message_text(message), keyword, max_chars=116),
                }
            )
        rows.append(
            {
                "keyword": keyword,
                "channel_filter": channel_filter,
                "count": int(keyword_row.get("count") or len(matched_messages)),
                "channels_count": len(channel_counter),
                "top_channels": [
                    {"label": label, "count": count}
                    for label, count in channel_counter.most_common(5)
                ],
                "channel_types": [
                    {
                        "key": key,
                        "label": telegram_daily_channel_type_label(key),
                        "count": count,
                    }
                    for key, count in type_counter.most_common()
                ],
                "trend": telegram_daily_keyword_trend(messages, keyword, config, start_at, end_at, period_key),
                "messages": recent_messages,
            }
        )
    return rows


def telegram_daily_stock_candidates(text: str) -> list[str]:
    search_text = re.sub(r"네이버뉴스|다음뉴스|Google News|Investing\.com", " ", text or "", flags=re.I)
    candidates = list(extract_company_candidates(search_text))
    folded_text = search_text.casefold()
    for company in TELEGRAM_DAILY_EXTRA_COMPANY_NAMES:
        if company in search_text or company.casefold() in folded_text:
            candidates.append(company)
    for match in re.finditer(r"(?:기업명|종목명|회사명)\s*[:：]\s*([가-힣A-Za-z0-9&.\-]{2,24})", text or ""):
        candidates.append(match.group(1))
    for match in re.finditer(
        r"\b(?:NASDAQ|NYSE|AMEX|KOSPI|KOSDAQ|ticker|티커)\s*[:：]?\s*([A-Z][A-Z0-9.\-]{1,7})\b",
        text or "",
        flags=re.I,
    ):
        candidates.append(match.group(1).upper())

    filtered: list[str] = []
    stopwords = {value.casefold() for value in TELEGRAM_DAILY_COMPANY_STOPWORDS}
    for raw_company in candidates:
        company = re.sub(r"\s+", " ", str(raw_company or "")).strip(" -·,.:;()[]")
        if not company:
            continue
        if company.casefold() in stopwords:
            continue
        if company.casefold() not in TELEGRAM_DAILY_KNOWN_COMPANY_NAMES and TELEGRAM_DAILY_COMPANY_NOISE_PATTERN.search(company):
            continue
        if company.casefold() not in TELEGRAM_DAILY_KNOWN_COMPANY_NAMES and company.endswith(("금융", "투자", "증권", "전자")) and len(company) <= 6:
            continue
        if re.fullmatch(r"\d{1,6}", company):
            continue
        if len(company) < 2 or len(company) > 28:
            continue
        if any(company != existing and company in existing for existing in filtered):
            continue
        filtered.append(company)
        if len(filtered) >= 4:
            break
    return filtered


def telegram_daily_message_stock_candidates(message: dict[str, object]) -> list[str]:
    cached = message.get("_stock_candidates")
    if isinstance(cached, list):
        return [str(company) for company in cached]
    companies = telegram_daily_stock_candidates(telegram_daily_message_text(message))
    message["_stock_candidates"] = companies
    return companies


def telegram_daily_time_buckets(
    start_at: datetime,
    end_at: datetime,
    config: dict[str, object],
    *,
    bucket_count: int = 4,
) -> list[dict[str, object]]:
    return [dict(bucket) for bucket in TELEGRAM_DAILY_SESSION_BUCKETS]


def telegram_daily_bucket_index(posted_at: datetime, buckets: list[dict[str, object]], config: dict[str, object]) -> int:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    local_dt = posted_at.astimezone(ZoneInfo(timezone_name))
    minutes = local_dt.hour * 60 + local_dt.minute
    if 6 * 60 <= minutes < 9 * 60:
        key = "pre_market"
    elif 9 * 60 <= minutes < 12 * 60:
        key = "morning"
    elif 12 * 60 <= minutes < 15 * 60 + 30:
        key = "regular_close"
    else:
        key = "after_market"
    for index, bucket in enumerate(buckets):
        if bucket.get("key") == key:
            return index
    return max(0, len(buckets) - 1)


def telegram_daily_stock_heatmap(
    messages: list[dict[str, object]],
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
    *,
    limit: int = 14,
) -> tuple[list[str], list[dict[str, object]]]:
    buckets = telegram_daily_time_buckets(start_at, end_at, config)
    grouped: dict[str, dict[str, object]] = {}
    for message in messages:
        posted_at = telegram_daily_dt(message.get("posted_at"), config)
        if not posted_at:
            continue
        companies = telegram_daily_message_stock_candidates(message)
        if not companies:
            continue
        channel = str(message.get("channel_title") or message.get("handle") or "")
        bucket_index = telegram_daily_bucket_index(posted_at, buckets, config)
        for company in dict.fromkeys(companies):
            row = grouped.setdefault(
                company,
                {
                    "company": company,
                    "count": 0,
                    "channels": Counter(),
                    "buckets": [0 for _ in buckets],
                    "latest_at": "",
                },
            )
            row["count"] = int(row["count"]) + 1
            row["buckets"][bucket_index] += 1  # type: ignore[index]
            if channel:
                row["channels"][channel] += 1  # type: ignore[index]
            latest = posted_at.isoformat()
            if latest > str(row.get("latest_at") or ""):
                row["latest_at"] = latest

    rows: list[dict[str, object]] = []
    max_cell = 1
    for row in grouped.values():
        channel_counter: Counter[str] = row.pop("channels")  # type: ignore[assignment]
        bucket_values = [int(value) for value in row.get("buckets", [])]
        max_cell = max(max_cell, *(bucket_values or [1]))
        rows.append(
            {
                **row,
                "channels_count": len([channel for channel in channel_counter if channel]),
                "top_channels": [{"label": label, "count": count} for label, count in channel_counter.most_common(3)],
            }
        )
    rows.sort(
        key=lambda row: (
            int(row.get("count") or 0),
            int(row.get("channels_count") or 0),
            str(row.get("latest_at") or ""),
        ),
        reverse=True,
    )
    for row in rows:
        row["max_cell"] = max_cell
    return [f'{bucket.get("label")}\n{bucket.get("detail")}' for bucket in buckets], rows[:limit]


def telegram_daily_heat_level(value: int, max_cell: int) -> int:
    if value <= 0:
        return 0
    if max_cell <= 1:
        return 3
    return max(1, min(5, 1 + int((value - 1) / max(1, max_cell - 1) * 4)))


def render_telegram_daily_html(
    stories: list[dict[str, object]],
    state: dict[str, object],
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
    date_id: str,
    report_url: str,
) -> str:
    messages = telegram_daily_messages(state, config, start_at, end_at)
    story_rows = telegram_daily_story_rows(stories)
    signal_rows = telegram_daily_enriched_signals(telegram_daily_signal_rows(state, limit=18))
    signal_summary_counts = telegram_daily_signal_summary_counts(signal_rows)
    analysis_periods: list[dict[str, object]] = []
    for period_index, period in enumerate(TELEGRAM_DAILY_ANALYSIS_PERIODS):
        period_key = str(period["key"])
        period_start_at = start_at if period_key == "1d" else end_at - timedelta(days=int(period["days"]))
        period_messages = telegram_daily_messages(state, config, period_start_at, end_at)
        period_signal_rows = telegram_daily_enriched_signals(
            telegram_daily_signal_rows(
                state,
                limit=24,
                config=config,
                start_at=period_start_at,
                end_at=end_at,
            )
        )
        period_channels = {
            str(message.get("channel_title") or message.get("handle") or "")
            for message in period_messages
            if message.get("channel_title") or message.get("handle")
        }
        period_views: list[dict[str, object]] = []
        for type_index, channel_type in enumerate(telegram_daily_channel_type_options()):
            channel_type_key = str(channel_type["key"])
            filtered_messages = (
                period_messages
                if channel_type_key == "all"
                else [
                    message
                    for message in period_messages
                    if str(message.get("channel_content_type") or "") == channel_type_key
                ]
            )
            view_signal_rows = period_signal_rows if channel_type_key == "all" else []
            keyword_cloud = telegram_daily_keyword_cloud(filtered_messages, view_signal_rows, limit=30)
            heatmap_labels, heatmap_rows = telegram_daily_stock_heatmap(
                filtered_messages,
                config,
                period_start_at,
                end_at,
                limit=14,
            )
            filtered_channels = {
                str(message.get("channel_title") or message.get("handle") or "")
                for message in filtered_messages
                if message.get("channel_title") or message.get("handle")
            }
            period_views.append(
                {
                    "key": channel_type_key,
                    "label": str(channel_type["label"]),
                    "description": str(channel_type["description"]),
                    "active": type_index == 0,
                    "messages": filtered_messages,
                    "messages_count": len(filtered_messages),
                    "channels_count": len(filtered_channels),
                    "keyword_cloud": keyword_cloud,
                    "keyword_details": telegram_daily_keyword_details(
                        keyword_cloud,
                        filtered_messages,
                        config,
                        period_start_at,
                        end_at,
                        period_key,
                        channel_type_key,
                    ),
                    "heatmap_labels": heatmap_labels,
                    "heatmap_rows": heatmap_rows,
                }
            )
        analysis_periods.append(
            {
                "key": period_key,
                "label": str(period["label"]),
                "active": period_index == 0,
                "start_at": period_start_at,
                "end_at": end_at,
                "messages": period_messages,
                "messages_count": len(period_messages),
                "channels_count": len(period_channels),
                "views": period_views,
            }
        )
    channel_counter: Counter[str] = Counter(
        str(message.get("channel_title") or message.get("handle") or "")
        for message in messages
        if message.get("channel_title") or message.get("handle")
    )
    matched_story_ids = {str(row["story"].get("id") or "") for row in story_rows if isinstance(row.get("story"), dict)}
    start_label = escape(format_kst(start_at, str(config.get("timezone") or "Asia/Seoul")))
    end_label = escape(format_kst(end_at, str(config.get("timezone") or "Asia/Seoul")))
    report_link = escape(report_url, quote=True)
    logo = bside_logo_html("bside-logo--top")
    top_signals_html = "\n".join(
        f"""
        <article class="signal-card">
          <div class="signal-card__score">{int(signal.get("market_attention_score") or 0)}</div>
          <div class="signal-card__meta">
            <span>{escape(str(signal.get("signal_type_label") or "시장 언급"))}</span>
            <strong>{int(signal.get("related_telegram_count") or 0)}건 · {int(signal.get("related_telegram_channels_count") or 0)}채널</strong>
          </div>
          <h3>{escape(str(signal.get("display_title") or telegram_daily_signal_title(signal)))}</h3>
          <p>{escape(telegram_daily_excerpt(signal.get("signal_summary") or "", max_chars=112))}</p>
          <div class="signal-badges">
            {''.join(f'<span class="signal-badge signal-badge--{escape(str(badge.get("key") or ""), quote=True)}">{escape(str(badge.get("label") or ""))}</span>' for badge in signal.get("badges", []) if isinstance(badge, dict))}
          </div>
          <div class="tag-row">{''.join(f'<span>{escape(keyword)}</span>' for keyword in telegram_daily_list(signal.get("top_keywords"), limit=5))}</div>
        </article>
        """
        for signal in signal_rows[:4]
    ) or '<p class="empty">아직 표시할 시장 언급 신호가 충분하지 않습니다.</p>'
    signal_table_rows_html = "\n".join(
        f"""
        <tr>
          <td><strong class="score-pill">{int(signal.get("market_attention_score") or 0)}</strong></td>
          <td>
            <b>{escape(str(signal.get("display_title") or telegram_daily_signal_title(signal)))}</b>
            <div class="signal-table__badges">
              {''.join(f'<span class="signal-badge signal-badge--{escape(str(badge.get("key") or ""), quote=True)}">{escape(str(badge.get("label") or ""))}</span>' for badge in signal.get("badges", []) if isinstance(badge, dict))}
            </div>
            <small>{escape(telegram_daily_excerpt(signal.get("signal_summary") or " · ".join(telegram_daily_list(signal.get("top_keywords"), limit=5)), max_chars=118))}</small>
          </td>
          <td>{escape(str(signal.get("signal_type_label") or "시장 언급"))}</td>
          <td>{int(signal.get("related_telegram_count") or 0)}건<br><small>{int(signal.get("related_telegram_channels_count") or 0)}채널</small></td>
          <td><span class="confirm-pill confirm-pill--{escape(str((signal.get("confirmation") or {}).get("tone") if isinstance(signal.get("confirmation"), dict) else "watch"), quote=True)}">{escape(str((signal.get("confirmation") or {}).get("label") if isinstance(signal.get("confirmation"), dict) else "확인 필요"))}</span></td>
          <td>{''.join(f'<span>{escape(str(item.get("label") or ""))} {int(item.get("count") or 0)}</span>' for item in signal.get("channel_types", []) if isinstance(item, dict)) or '<span>유형 미상</span>'}</td>
          <td>{int(signal.get("institutional_relevance_score") or 0)}<br><small>Risk {int(signal.get("rumor_risk_score") or 0)}</small></td>
        </tr>
        """
        for signal in signal_rows[:12]
    ) or '<tr><td colspan="7">아직 표시할 시장 언급 신호가 충분하지 않습니다.</td></tr>'
    signal_table_html = f"""
      <div class="signal-table-wrap" aria-label="시장 언급 신호 상세">
        <table class="signal-table">
          <thead>
            <tr>
              <th>Score</th>
              <th>Signal</th>
              <th>유형</th>
              <th>언급</th>
              <th>확인상태</th>
              <th>채널 유형</th>
              <th>기관/Risk</th>
            </tr>
          </thead>
          <tbody>{signal_table_rows_html}</tbody>
        </table>
      </div>
    """

    analysis_tabs_html = "\n".join(
        f'<button type="button" class="analysis-tab{" is-active" if period["active"] else ""}" '
        f'data-analysis-period="{escape(str(period["key"]), quote=True)}">{escape(str(period["label"]))}</button>'
        for period in analysis_periods
    )
    analysis_panels_html_parts: list[str] = []
    keyword_detail_data: dict[str, list[dict[str, object]]] = {}
    for period in analysis_periods:
        period_key = str(period["key"])
        period_start = period["start_at"] if isinstance(period.get("start_at"), datetime) else start_at
        period_end = period["end_at"] if isinstance(period.get("end_at"), datetime) else end_at
        period_range = (
            f'{format_kst(period_start, str(config.get("timezone") or "Asia/Seoul"))} - '
            f'{format_kst(period_end, str(config.get("timezone") or "Asia/Seoul"))}'
        )
        period_views = [view for view in period.get("views", []) if isinstance(view, dict)]
        channel_tabs_html = "\n".join(
            f'<button type="button" class="channel-type-tab{" is-active" if bool(view.get("active")) else ""}" '
            f'data-analysis-channel="{escape(period_key + ":" + str(view.get("key") or ""), quote=True)}">'
            f'{escape(str(view.get("label") or ""))}<small>{int(view.get("messages_count") or 0)}건</small></button>'
            for view in period_views
        )
        view_html_parts: list[str] = []
        for view in period_views:
            view_key = f'{period_key}:{str(view.get("key") or "")}'
            keyword_rows = [row for row in view.get("keyword_cloud", []) if isinstance(row, dict)]
            keyword_detail_data[view_key] = [
                detail for detail in view.get("keyword_details", []) if isinstance(detail, dict)
            ]
            keyword_cloud_html = "\n".join(
                f'<button type="button" class="keyword-chip keyword-chip--l{int(row["level"])}" '
                f'data-keyword-detail-key="{escape(view_key, quote=True)}" '
                f'data-keyword="{escape(str(row["keyword"]), quote=True)}" '
                f'title="{escape(str(int(row["count"])) + "건 · " + str(int(row["channels_count"])) + "채널", quote=True)}">'
                f'{escape(str(row["keyword"]))}<small>{int(row["count"])}건 · {int(row["channels_count"])}채널</small></button>'
                for row in keyword_rows
            ) or '<p class="empty">키워드를 계산할 Telegram 메시지가 아직 충분하지 않습니다.</p>'
            heatmap_labels = [str(label) for label in view.get("heatmap_labels", [])]
            heatmap_header_html = "".join(
                f'<span class="heatmap__bucket">{escape(label).replace(chr(10), "<br>")}</span>' for label in heatmap_labels
            )
            heatmap_rows_html = "\n".join(
                f"""
                <div class="heatmap__row">
                  <div class="heatmap__name">
                    <div class="heatmap__name-line">
                      <strong>{escape(str(row.get("company") or ""))}</strong>
                      <span>{int(row.get("count") or 0)}건 · {int(row.get("channels_count") or 0)}채널</span>
                    </div>
                    <em>{escape(", ".join(str(channel.get("label") or "") for channel in row.get("top_channels", []) if isinstance(channel, dict))[:46])}</em>
                  </div>
                  {''.join(
                      f'<span class="heatmap__cell heatmap__cell--l{telegram_daily_heat_level(int(value), int(row.get("max_cell") or 1))}" '
                      f'aria-label="{escape(str(row.get("company") or ""), quote=True)} {escape(heatmap_labels[index].replace(chr(10), " "), quote=True) if index < len(heatmap_labels) else ""} {int(value)}건">{int(value) if int(value) else ""}</span>'
                      for index, value in enumerate(row.get("buckets", []))
                  )}
                </div>
                """
                for row in view.get("heatmap_rows", [])
                if isinstance(row, dict)
            ) or '<p class="empty">종목명으로 묶을 Telegram 언급이 아직 없습니다.</p>'
            view_html_parts.append(
                f"""
                <div class="analysis-view{' is-active' if bool(view.get("active")) else ''}" data-analysis-view="{escape(view_key, quote=True)}">
                  <div class="keyword-layout">
                    <article class="analysis-panel analysis-panel--cloud">
                      <h3>키워드 클라우드</h3>
                      <p>{escape(str(view.get("description") or ""))} 기준으로 반복되는 표현을 추렸습니다. 키워드를 누르면 빈도 추이와 원문 발췌가 오른쪽에 표시됩니다.</p>
                      <div class="keyword-cloud">{keyword_cloud_html}</div>
                    </article>
                    <aside class="keyword-detail" data-keyword-detail-panel="{escape(view_key, quote=True)}">
                      <div class="keyword-detail__empty">키워드를 선택하면 언급 추이와 대표 원문이 표시됩니다.</div>
                    </aside>
                  </div>
                  <article class="analysis-panel analysis-panel--heatmap">
                    <h3>종목 언급 히트맵</h3>
                    <p>장전·오전·장중 후반·이후 구간별 회사·티커 후보 언급량입니다. 가로 스크롤 없이 전체 흐름을 볼 수 있도록 압축했습니다.</p>
                    <div class="heatmap" role="table" aria-label="종목 언급 히트맵">
                      <div class="heatmap__row heatmap__row--head" role="row">
                        <span>종목</span>
                        {heatmap_header_html}
                      </div>
                      {heatmap_rows_html}
                    </div>
                  </article>
                </div>
                """
            )
        analysis_panels_html_parts.append(
            f"""
            <div class="analysis-period{' is-active' if period['active'] else ''}" data-analysis-panel="{escape(period_key, quote=True)}">
              <div class="analysis-controls">
                <div class="analysis-period__meta">
                  <strong>{escape(str(period["label"]))} 분석 기간</strong>
                  <span>{escape(period_range)}</span>
                  <span>{int(period.get("messages_count") or 0)}건 · {int(period.get("channels_count") or 0)}채널</span>
                </div>
                <div class="channel-type-tabs" role="tablist" aria-label="Telegram 채널 유형">
                  {channel_tabs_html}
                </div>
              </div>
              {''.join(view_html_parts)}
            </div>
            """
        )
    analysis_panels_html = "\n".join(analysis_panels_html_parts)
    keyword_detail_json = html_json(keyword_detail_data)

    story_cards_html = "\n".join(
        f"""
        <article class="story-card">
          <div class="story-card__head">
            <div>
              <div class="story-card__meta">
                <span>{escape(str(row["story"].get("category") or "기타"))}</span>
                <span>{escape(date_label(row["story"].get("datetime"), config))}</span>
                <span>Telegram {int(row["mention_count"])}건 · {int(row["channel_count"])}채널</span>
              </div>
              <h3><a href="{report_link}#{escape(str(row["story"].get("id") or ""), quote=True)}">{escape(str(row["story"].get("title") or "제목 없음"))}</a></h3>
            </div>
            <a class="story-card__source" href="{escape(str(row["story"].get("primary_url") or "#"), quote=True)}" target="_blank" rel="noopener noreferrer">기사</a>
          </div>
          <ul class="story-card__bullets">
            {''.join(f'<li>{escape(bullet)}</li>' for bullet in story_brief_bullets(row["story"], max_chars=86, max_items=2))}
          </ul>
          <div class="mention-list">
            {''.join(
                f'<a class="mention" href="{escape(str(mention.get("message_url") or "#"), quote=True)}" target="_blank" rel="noopener noreferrer">'
                f'<span>{escape(str(mention.get("channel_title") or mention.get("channel_handle") or "Telegram"))} · {escape(compact_text(str(mention.get("match_type") or "언급"), max_chars=18))}</span>'
                f'<strong>{escape(telegram_daily_excerpt(mention.get("excerpt") or mention.get("text") or "", max_chars=118))}</strong>'
                f'</a>'
                for mention in list(row["mentions"])[:4]
            )}
          </div>
        </article>
        """
        for row in story_rows[:18]
    ) or '<p class="empty">기사와 직접 연결된 Telegram 언급이 아직 없습니다.</p>'

    channel_rows_html = "\n".join(
        f"""
        <tr>
          <td>{rank}</td>
          <td>{escape(channel)}</td>
          <td>{count}</td>
        </tr>
        """
        for rank, (channel, count) in enumerate(channel_counter.most_common(18), start=1)
    ) or '<tr><td colspan="3">수집된 채널 언급이 없습니다.</td></tr>'

    recent_messages_html = "\n".join(
        f"""
        <a class="recent-message" href="{escape(str(message.get("message_url") or "#"), quote=True)}" target="_blank" rel="noopener noreferrer">
          <span>{escape(date_label(telegram_daily_dt(message.get("posted_at"), config), config))} · {escape(str(message.get("channel_title") or message.get("handle") or "Telegram"))}</span>
          <strong>{escape(telegram_daily_excerpt(message.get("text"), max_chars=136))}</strong>
        </a>
        """
        for message in messages[:24]
    ) or '<p class="empty">최근 Telegram 메시지가 없습니다.</p>'

    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Telegram 데일리 - {escape(date_id)}</title>
  <style>
    :root {{ --ink:#17131f; --muted:#6f6878; --line:#ded7e8; --paper:#fbfafc; --surface:#fff; --accent:#6b35d8; --accent-deep:#42207e; --accent-soft:#f0eafb; --green:#00785f; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; background:var(--paper); color:var(--ink); font-family:-apple-system,BlinkMacSystemFont,"Apple SD Gothic Neo","Malgun Gothic","Segoe UI",sans-serif; line-height:1.55; }}
    a {{ color:inherit; text-decoration-thickness:1px; text-underline-offset:3px; }}
    .page {{ max-width:1080px; margin:0 auto; padding:24px 22px 68px; }}
    .brand-row {{ display:flex; justify-content:space-between; gap:16px; align-items:baseline; border-bottom:2px solid var(--ink); padding-bottom:14px; margin-bottom:26px; }}
    .bside-logo {{ display:inline-flex; align-items:center; gap:9px; color:var(--accent); text-decoration:none; }}
    .bside-logo__image {{ width:92px; height:auto; display:block; color:var(--accent); flex:0 0 auto; }}
    .bside-logo__label {{ color:var(--accent); font-size:11px; font-weight:900; letter-spacing:.12em; }}
    .edition {{ color:var(--muted); font-size:12px; text-align:right; }}
    .hero {{ display:grid; gap:13px; border-bottom:1px solid var(--line); padding-bottom:22px; }}
    h1 {{ margin:0; font-family:Georgia,"Times New Roman",serif; font-size:clamp(40px,6vw,64px); line-height:1; letter-spacing:0; }}
    .dek {{ max-width:760px; margin:0; color:#342d3d; font-size:15px; word-break:keep-all; }}
    .actions, .tag-row {{ display:flex; flex-wrap:wrap; gap:7px; }}
    .actions a, .tag-row span {{ display:inline-flex; border:1px solid var(--line); border-radius:999px; padding:6px 10px; background:var(--surface); color:var(--accent-deep); text-decoration:none; font-size:12px; font-weight:850; }}
    .metric-grid {{ display:grid; grid-template-columns:repeat(5,minmax(0,1fr)); gap:10px; margin-top:8px; }}
    .metric {{ border:1px solid rgba(112,55,224,.15); background:var(--accent-soft); padding:12px; }}
    .metric span {{ display:block; color:var(--muted); font-size:11px; font-weight:850; }}
    .metric strong {{ display:block; color:var(--accent-deep); font-size:24px; line-height:1.15; }}
    .metric-footnote {{ color:var(--muted); font-size:12px; margin-top:3px; }}
    .section {{ border-top:2px solid var(--ink); margin-top:28px; padding-top:18px; }}
    .section h2 {{ margin:0 0 14px; font-family:Georgia,"Times New Roman",serif; font-size:28px; line-height:1.1; }}
    .section-note {{ margin:-8px 0 14px; color:var(--muted); font-size:13px; }}
    .analysis-tabs {{ position:sticky; top:0; z-index:34; display:flex; flex-wrap:wrap; gap:7px; margin:-2px 0 0; padding:8px 0 9px; background:linear-gradient(180deg,var(--bg) 0%,rgba(250,248,252,.96) 100%); border-bottom:1px solid var(--line); backdrop-filter:blur(10px); }}
    .analysis-tab {{ border:1px solid var(--line); border-radius:999px; background:var(--surface); color:var(--muted); padding:7px 11px; font:inherit; font-size:12px; font-weight:900; cursor:pointer; }}
    .analysis-tab.is-active {{ border-color:var(--accent); background:var(--accent-soft); color:var(--accent-deep); box-shadow:inset 0 0 0 1px rgba(112,55,224,.24); }}
    .analysis-period {{ display:none; }}
    .analysis-period.is-active {{ display:grid; gap:12px; }}
    .analysis-controls {{ position:sticky; top:49px; z-index:33; display:grid; gap:8px; margin:0 0 3px; padding:8px 0 9px; background:linear-gradient(180deg,rgba(250,248,252,.98) 0%,rgba(250,248,252,.94) 100%); border-bottom:1px solid var(--line); backdrop-filter:blur(10px); }}
    .analysis-period__meta {{ display:flex; flex-wrap:wrap; gap:8px 12px; align-items:center; color:var(--muted); font-size:12px; }}
    .analysis-period__meta strong {{ color:var(--accent-deep); }}
    .channel-type-tabs {{ display:flex; flex-wrap:wrap; gap:7px; margin:0 0 3px; }}
    .channel-type-tab {{ border:1px solid var(--line); border-radius:999px; background:var(--surface); color:#40364d; padding:7px 10px; font:inherit; font-size:12px; font-weight:900; cursor:pointer; }}
    .channel-type-tab small {{ margin-left:5px; color:var(--muted); font-size:10px; }}
    .channel-type-tab.is-active {{ border-color:var(--accent); background:var(--accent-soft); color:var(--accent-deep); }}
    .analysis-view {{ display:none; gap:14px; }}
    .analysis-view.is-active {{ display:grid; }}
    .keyword-layout {{ display:grid; grid-template-columns:minmax(220px,.72fr) minmax(360px,1.18fr); gap:16px; align-items:stretch; }}
    .analysis-panel {{ border:1px solid var(--line); background:var(--surface); padding:14px; box-shadow:0 14px 32px rgba(70,43,102,.05); }}
    .analysis-panel--cloud {{ min-height:220px; }}
    .analysis-panel--heatmap {{ padding-bottom:12px; }}
    .analysis-panel h3 {{ margin:0 0 7px; font-size:16px; color:var(--accent-deep); }}
    .analysis-panel p {{ margin:0 0 12px; color:var(--muted); font-size:12px; line-height:1.45; }}
    .keyword-cloud {{ display:flex; flex-wrap:wrap; gap:8px; align-items:center; }}
    .keyword-chip {{ display:inline-flex; align-items:center; gap:5px; border:1px solid rgba(112,55,224,.16); border-radius:999px; padding:5px 9px; background:var(--accent-soft); color:var(--accent-deep); font:inherit; font-weight:900; line-height:1; cursor:pointer; }}
    .keyword-chip.is-active {{ background:var(--accent); border-color:var(--accent); color:#fff; box-shadow:0 8px 20px rgba(107,53,216,.18); }}
    .keyword-chip.is-active small {{ color:rgba(255,255,255,.78); }}
    .keyword-chip small {{ color:var(--muted); font-size:10px; font-weight:850; white-space:nowrap; }}
    .keyword-chip--l1 {{ font-size:11px; opacity:.76; }}
    .keyword-chip--l2 {{ font-size:12px; opacity:.86; }}
    .keyword-chip--l3 {{ font-size:13px; }}
    .keyword-chip--l4 {{ font-size:15px; }}
    .keyword-chip--l5 {{ font-size:17px; background:#ece3ff; }}
    .keyword-chip--l6 {{ font-size:20px; background:#e5d8ff; border-color:rgba(112,55,224,.32); }}
    .keyword-detail {{ border:1px solid rgba(112,55,224,.22); background:linear-gradient(180deg,#fff,#faf7ff); padding:14px; min-height:220px; box-shadow:0 14px 32px rgba(70,43,102,.05); }}
    .keyword-detail__empty {{ color:var(--muted); font-size:13px; line-height:1.55; }}
    .keyword-detail h3 {{ margin:0 0 5px; font-size:20px; line-height:1.2; color:var(--ink); }}
    .keyword-detail__meta {{ display:flex; flex-wrap:wrap; gap:6px; margin-bottom:10px; }}
    .keyword-detail__meta span, .keyword-detail__types span {{ display:inline-flex; border:1px solid var(--line); border-radius:999px; padding:3px 7px; background:#fff; color:var(--accent-deep); font-size:11px; font-weight:900; }}
    .keyword-detail__types {{ display:flex; flex-wrap:wrap; gap:5px; margin:8px 0 10px; }}
    .trend-card {{ border:1px solid var(--line); background:#fff; padding:9px; margin:8px 0 10px; }}
    .trend-card__head {{ display:flex; justify-content:space-between; gap:10px; color:var(--muted); font-size:11px; font-weight:900; margin-bottom:5px; }}
    .trend-line {{ width:100%; height:104px; display:block; overflow:visible; }}
    .trend-line text {{ fill:#81768e; font-size:10px; }}
    .trend-line path {{ fill:none; stroke:var(--accent); stroke-width:2.5; stroke-linecap:round; stroke-linejoin:round; }}
    .trend-line circle {{ fill:#fff; stroke:var(--accent); stroke-width:2; }}
    .keyword-detail__channels {{ display:grid; gap:5px; margin:8px 0 10px; color:#4b4357; font-size:12px; }}
    .keyword-detail__channels span {{ display:flex; justify-content:space-between; gap:10px; border-bottom:1px solid rgba(222,215,232,.55); padding-bottom:4px; }}
    .keyword-detail__messages {{ display:grid; gap:7px; }}
    .keyword-detail__messages a {{ display:grid; gap:3px; border:1px solid rgba(112,55,224,.12); background:#fff; padding:8px; text-decoration:none; }}
    .keyword-detail__messages a:hover strong {{ color:var(--accent-deep); text-decoration:underline; text-underline-offset:3px; }}
    .keyword-detail__messages span {{ color:var(--muted); font-size:10.5px; }}
    .keyword-detail__messages strong {{ color:#2f2839; font-size:12px; line-height:1.42; }}
    .heatmap {{ display:grid; gap:7px; overflow:hidden; padding-bottom:2px; }}
    .heatmap__row {{ display:grid; grid-template-columns:minmax(164px,1.38fr) repeat(4,minmax(46px,.5fr)); gap:5px; align-items:stretch; }}
    .heatmap__row--head {{ color:var(--muted); font-size:11px; font-weight:900; }}
    .heatmap__name {{ display:grid; gap:2px; border:1px solid var(--line); background:#fff; padding:6px 7px; min-height:36px; overflow:hidden; }}
    .heatmap__name-line {{ display:flex; align-items:baseline; gap:6px; min-width:0; }}
    .heatmap__name strong {{ flex:1 1 auto; min-width:0; font-size:12.5px; line-height:1.2; word-break:keep-all; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
    .heatmap__name span {{ flex:0 0 auto; color:var(--muted); font-size:10.5px; white-space:nowrap; }}
    .heatmap__name em {{ color:#8a8195; font-size:10px; line-height:1.2; font-style:normal; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
    .heatmap__bucket, .heatmap__cell {{ display:flex; align-items:center; justify-content:center; min-height:38px; border:1px solid var(--line); background:#fff; border-radius:7px; text-align:center; }}
    .heatmap__bucket {{ padding:4px; }}
    .heatmap__cell {{ color:var(--accent-deep); font-size:12px; font-weight:950; }}
    .heatmap__cell--l0 {{ color:transparent; background:#fff; }}
    .heatmap__cell--l1 {{ background:#f7f2ff; }}
    .heatmap__cell--l2 {{ background:#eee3ff; }}
    .heatmap__cell--l3 {{ background:#ddccff; }}
    .heatmap__cell--l4 {{ background:#c6a8ff; }}
    .heatmap__cell--l5 {{ background:#9b6cf0; color:#fff; }}
    .signal-grid {{ display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:12px; }}
    .signal-card, .story-card {{ position:relative; border:1px solid var(--line); background:var(--surface); padding:14px; box-shadow:0 14px 32px rgba(70,43,102,.05); }}
    .signal-card__score {{ position:absolute; right:12px; top:12px; min-width:38px; height:32px; display:grid; place-items:center; border-radius:10px; background:var(--ink); color:#fff; font-weight:950; font-size:16px; }}
    .signal-card__meta, .story-card__meta {{ display:flex; flex-wrap:wrap; gap:6px 9px; color:var(--muted); font-size:11px; }}
    .signal-card__meta strong {{ color:var(--accent-deep); }}
    .signal-card h3, .story-card h3 {{ margin:7px 0 7px; font-size:18px; line-height:1.32; word-break:keep-all; }}
    .signal-card h3 {{ padding-right:40px; }}
    .signal-card p {{ margin:0 0 10px; color:#4d4659; font-size:13px; line-height:1.46; }}
    .signal-badges, .signal-table__badges {{ display:flex; flex-wrap:wrap; gap:5px; margin:7px 0; }}
    .signal-badge {{ display:inline-flex; border:1px solid rgba(112,55,224,.18); border-radius:999px; background:#fff; color:var(--accent-deep); padding:3px 6px; font-size:9.5px; font-weight:950; letter-spacing:.02em; }}
    .signal-badge--confirmed {{ border-color:rgba(0,120,95,.25); color:var(--green); background:#f0fbf7; }}
    .signal-badge--risk {{ border-color:rgba(176,83,0,.26); color:#9a4a00; background:#fff6eb; }}
    .signal-badge--tg_only, .signal-badge--estimated {{ color:#665b72; background:#f8f6fb; }}
    .signal-table-wrap {{ margin-top:14px; overflow-x:auto; border:1px solid var(--line); background:#fff; }}
    .signal-table {{ border:0; min-width:860px; }}
    .signal-table th, .signal-table td {{ vertical-align:top; }}
    .signal-table b {{ display:block; margin-bottom:3px; line-height:1.35; }}
    .signal-table small {{ color:var(--muted); line-height:1.35; }}
    .signal-table td:nth-child(6) span {{ display:inline-flex; margin:0 4px 4px 0; border:1px solid var(--line); border-radius:999px; padding:2px 6px; color:var(--accent-deep); background:#fff; white-space:nowrap; }}
    .score-pill {{ display:inline-grid; place-items:center; min-width:34px; height:30px; border-radius:9px; background:var(--ink); color:#fff; font-size:14px; }}
    .confirm-pill {{ display:inline-flex; border-radius:999px; border:1px solid var(--line); padding:4px 7px; font-size:11px; font-weight:950; white-space:nowrap; }}
    .confirm-pill--confirmed {{ color:var(--green); border-color:rgba(0,120,95,.24); background:#effbf7; }}
    .confirm-pill--estimated {{ color:#7b5700; border-color:rgba(189,137,0,.24); background:#fff9e8; }}
    .confirm-pill--watch {{ color:#665b72; background:#f8f6fb; }}
    .story-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:14px; }}
    .story-card__head {{ display:flex; justify-content:space-between; gap:12px; align-items:flex-start; }}
    .story-card__source {{ flex:0 0 auto; border:1px solid var(--accent); border-radius:999px; padding:5px 9px; color:var(--accent-deep); background:var(--accent-soft); text-decoration:none; font-size:11px; font-weight:900; }}
    .story-card__bullets {{ margin:10px 0 0; padding:10px 12px 10px 24px; border-left:3px solid rgba(112,55,224,.45); background:rgba(246,240,255,.6); color:#342d3d; font-size:13px; line-height:1.45; }}
    .mention-list {{ display:grid; gap:7px; margin-top:11px; }}
    .mention, .recent-message {{ display:grid; gap:3px; border:1px solid rgba(112,55,224,.12); background:rgba(255,255,255,.86); padding:9px 10px; color:inherit; text-decoration:none; }}
    .mention:hover strong, .recent-message:hover strong {{ color:var(--accent-deep); text-decoration:underline; text-underline-offset:3px; }}
    .mention span, .recent-message span {{ color:var(--muted); font-size:11px; }}
    .mention strong, .recent-message strong {{ font-size:12.5px; line-height:1.42; color:#322b3e; }}
    .split {{ display:grid; grid-template-columns:330px minmax(0,1fr); gap:22px; align-items:start; }}
    table {{ width:100%; border-collapse:collapse; background:#fff; border:1px solid var(--line); }}
    th, td {{ border-bottom:1px solid var(--line); padding:8px 9px; text-align:left; font-size:12px; }}
    th {{ color:var(--accent-deep); background:var(--accent-soft); font-weight:900; }}
    .recent-list {{ display:grid; gap:7px; }}
    .empty {{ color:var(--muted); font-size:13px; padding:12px; border:1px solid var(--line); background:#fff; }}
    @media (max-width:900px) {{
      .page {{ padding:18px 14px 50px; }}
      .brand-row {{ align-items:flex-start; flex-direction:column; }}
      .edition {{ text-align:left; }}
      .metric-grid {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
      .keyword-layout, .signal-grid, .story-grid, .split {{ grid-template-columns:1fr; }}
      .metric:last-child {{ grid-column:1 / -1; }}
      .keyword-chip--l5 {{ font-size:15px; }}
      .keyword-chip--l6 {{ font-size:17px; }}
      .analysis-tabs {{ top:0; gap:6px; padding:7px 0 8px; overflow-x:auto; flex-wrap:nowrap; }}
      .analysis-tab {{ padding:7px 10px; }}
      .analysis-controls {{ top:45px; gap:6px; padding:7px 0 8px; }}
      .channel-type-tabs {{ overflow-x:auto; flex-wrap:nowrap; padding-bottom:4px; }}
      .channel-type-tab {{ flex:0 0 auto; }}
      .analysis-period__meta {{ display:grid; gap:4px; }}
      .keyword-detail {{ min-height:0; }}
      .heatmap__row {{ grid-template-columns:minmax(124px,1.35fr) repeat(4,minmax(36px,.52fr)); gap:4px; }}
      .heatmap__name {{ padding:5px 6px; }}
      .heatmap__name-line {{ gap:4px; }}
      .heatmap__name strong {{ font-size:11px; }}
      .heatmap__name span {{ font-size:9.5px; }}
      .heatmap__name em {{ display:none; }}
      .heatmap__bucket, .heatmap__cell {{ min-height:34px; font-size:9px; border-radius:6px; }}
      .story-card h3 {{ font-size:16px; }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <header class="hero">
      <div class="brand-row">
        {logo}
        <div class="edition">{start_label}<br>{end_label}</div>
      </div>
      <h1>Telegram 데일리</h1>
      <p class="dek">공개 금융·증권 Telegram 채널에서 포착된 시장 언급, 기사 공유, 키워드 확산을 주주·자본시장 데일리의 보조 신호로 정리했습니다. 투자 추천이 아니라 공개 출처 기반의 시장 반응 현황입니다.</p>
      <div class="actions">
        <a href="{report_link}">주주·자본시장 데일리로 돌아가기</a>
        <a href="search.html">시장 이슈 검색</a>
        <a href="telegram-admin.html">Telegram 수집 현황</a>
      </div>
      <div class="metric-grid">
        <div class="metric"><span>급증 후보</span><strong>{signal_summary_counts["rising"]}</strong></div>
        <div class="metric"><span>URL·기사 확인</span><strong>{signal_summary_counts["confirmed"]}</strong></div>
        <div class="metric"><span>기관·공시 관여</span><strong>{signal_summary_counts["institutional"]}</strong></div>
        <div class="metric"><span>검증 필요</span><strong>{signal_summary_counts["risk"]}</strong></div>
        <div class="metric"><span>TG-only</span><strong>{signal_summary_counts["tg_only"]}</strong></div>
      </div>
      <div class="metric-footnote">{len({str(message.get("handle") or message.get("channel_title") or "") for message in messages if message.get("handle") or message.get("channel_title")})}개 채널 · {len(messages)}개 메시지 · 기사 연결 이슈 {len(matched_story_ids)}건을 바탕으로 계산했습니다.</div>
    </header>
    <section class="section">
      <h2>Telegram 분석</h2>
      <div class="analysis-tabs" role="tablist" aria-label="Telegram 분석 기간">
        {analysis_tabs_html}
      </div>
      {analysis_panels_html}
    </section>
    <section class="section">
      <h2>시장 언급 신호</h2>
      <p class="section-note">언급량, 채널 폭, 확인상태, 기관성, 검증 필요 요소를 합산한 Market Attention Score 기준입니다.</p>
      <div class="signal-grid">{top_signals_html}</div>
      {signal_table_html}
    </section>
    <section class="section">
        <h2>주요 이슈와 Telegram 반응</h2>
      <div class="story-grid">{story_cards_html}</div>
    </section>
    <section class="section split">
      <div>
        <h2>채널별 언급</h2>
        <table>
          <thead><tr><th>#</th><th>채널</th><th>건수</th></tr></thead>
          <tbody>{channel_rows_html}</tbody>
        </table>
      </div>
      <div>
        <h2>최근 Telegram 원문 발췌</h2>
        <div class="recent-list">{recent_messages_html}</div>
      </div>
    </section>
  </div>
  <script>
    const keywordDetailData = {keyword_detail_json};
    function escapeHtml(value) {{
      return String(value || '').replace(/[&<>"']/g, (char) => ({{ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }}[char]));
    }}
    function compactText(value, maxLength) {{
      const text = String(value || '').replace(/\\s+/g, ' ').trim();
      return text.length > maxLength ? text.slice(0, Math.max(0, maxLength - 1)).trim() + '…' : text;
    }}
    function detailPanelFor(key) {{
      return Array.from(document.querySelectorAll('[data-keyword-detail-panel]')).find((panel) => panel.getAttribute('data-keyword-detail-panel') === key);
    }}
    function renderTrendSvg(trend) {{
      const rows = Array.isArray(trend) ? trend : [];
      if (!rows.length) return '<div class="trend-card__empty">추이 데이터가 부족합니다.</div>';
      const width = 360;
      const height = 112;
      const padX = 20;
      const padY = 18;
      const maxValue = Math.max(1, ...rows.map((row) => Number(row.count || 0)));
      const xFor = (index) => rows.length === 1 ? width / 2 : padX + (index * (width - padX * 2)) / (rows.length - 1);
      const yFor = (value) => height - padY - (Number(value || 0) / maxValue) * (height - padY * 2);
      const points = rows.map((row, index) => `${{xFor(index).toFixed(1)}},${{yFor(row.count).toFixed(1)}}`).join(' ');
      const circles = rows.map((row, index) => `<circle cx="${{xFor(index).toFixed(1)}}" cy="${{yFor(row.count).toFixed(1)}}" r="3"><title>${{escapeHtml(row.label)}} ${{Number(row.count || 0)}}건</title></circle>`).join('');
      const labelIndexes = Array.from(new Set([0, Math.floor((rows.length - 1) / 2), rows.length - 1])).filter((index) => index >= 0);
      const labels = labelIndexes.map((index) => `<text x="${{xFor(index).toFixed(1)}}" y="${{height - 2}}" text-anchor="${{index === 0 ? 'start' : index === rows.length - 1 ? 'end' : 'middle'}}">${{escapeHtml(rows[index].label || '')}}</text>`).join('');
      return `<svg class="trend-line" viewBox="0 0 ${{width}} ${{height}}" role="img" aria-label="키워드 언급 빈도 라인그래프"><path d="M ${{points}}" /><g>${{circles}}</g>${{labels}}</svg>`;
    }}
    function renderKeywordDetail(key, keyword) {{
      const panel = detailPanelFor(key);
      if (!panel) return;
      const details = Array.isArray(keywordDetailData[key]) ? keywordDetailData[key] : [];
      const detail = details.find((row) => String(row.keyword || '') === String(keyword || '')) || details[0];
      if (!detail) {{
        panel.innerHTML = '<div class="keyword-detail__empty">키워드를 선택하면 언급 추이와 대표 원문이 표시됩니다.</div>';
        return;
      }}
      const channels = Array.isArray(detail.top_channels) ? detail.top_channels : [];
      const types = Array.isArray(detail.channel_types) ? detail.channel_types : [];
      const messages = Array.isArray(detail.messages) ? detail.messages : [];
      const channelHtml = channels.map((row) => `<span><b>${{escapeHtml(compactText(row.label, 34))}}</b><em>${{Number(row.count || 0)}}건</em></span>`).join('') || '<span><b>채널 데이터 없음</b><em>0건</em></span>';
      const typeHtml = types.map((row) => `<span>${{escapeHtml(row.label)}} ${{Number(row.count || 0)}}건</span>`).join('');
      const messageHtml = messages.map((message) => {{
        const href = String(message.url || '#');
        return `<a href="${{escapeHtml(href)}}" target="_blank" rel="noopener noreferrer"><span>${{escapeHtml(message.posted_at)}} · ${{escapeHtml(message.channel_type)}} · ${{escapeHtml(compactText(message.channel, 28))}}</span><strong>${{escapeHtml(compactText(message.excerpt, 128))}}</strong></a>`;
      }}).join('') || '<div class="keyword-detail__empty">대표 원문 발췌가 아직 없습니다.</div>';
      panel.innerHTML = `
        <h3>${{escapeHtml(detail.keyword)}}</h3>
        <div class="keyword-detail__meta">
          <span>${{Number(detail.count || 0)}}건 언급</span>
          <span>${{Number(detail.channels_count || 0)}}채널</span>
        </div>
        <div class="trend-card">
          <div class="trend-card__head"><span>언급 빈도</span><strong>${{escapeHtml(detail.keyword)}}</strong></div>
          ${{renderTrendSvg(detail.trend)}}
        </div>
        <div class="keyword-detail__types">${{typeHtml}}</div>
        <div class="keyword-detail__channels">${{channelHtml}}</div>
        <div class="keyword-detail__messages">${{messageHtml}}</div>
      `;
    }}
    function selectKeywordButton(button) {{
      const key = button.getAttribute('data-keyword-detail-key') || '';
      const keyword = button.getAttribute('data-keyword') || '';
      document.querySelectorAll(`[data-keyword-detail-key="${{key.replace(/"/g, '\\"')}}"]`).forEach((item) => {{
        item.classList.toggle('is-active', item === button);
      }});
      renderKeywordDetail(key, keyword);
    }}
    function initAnalysisView(view) {{
      if (!view) return;
      const first = view.querySelector('[data-keyword]');
      if (first) {{
        selectKeywordButton(first);
      }} else {{
        const key = view.getAttribute('data-analysis-view') || '';
        const panel = detailPanelFor(key);
        if (panel) panel.innerHTML = '<div class="keyword-detail__empty">표시할 키워드가 아직 충분하지 않습니다.</div>';
      }}
    }}
    function activateChannelView(key) {{
      const [period] = key.split(':');
      const panel = Array.from(document.querySelectorAll('[data-analysis-panel]')).find((item) => item.getAttribute('data-analysis-panel') === period);
      if (!panel) return;
      panel.querySelectorAll('[data-analysis-channel]').forEach((button) => {{
        button.classList.toggle('is-active', button.getAttribute('data-analysis-channel') === key);
      }});
      panel.querySelectorAll('[data-analysis-view]').forEach((view) => {{
        const active = view.getAttribute('data-analysis-view') === key;
        view.classList.toggle('is-active', active);
        if (active) initAnalysisView(view);
      }});
    }}
    document.querySelectorAll('[data-analysis-period]').forEach((button) => {{
      button.addEventListener('click', () => {{
        const period = button.getAttribute('data-analysis-period');
        document.querySelectorAll('[data-analysis-period]').forEach((item) => {{
          item.classList.toggle('is-active', item === button);
        }});
        document.querySelectorAll('[data-analysis-panel]').forEach((panel) => {{
          panel.classList.toggle('is-active', panel.getAttribute('data-analysis-panel') === period);
        }});
        const activePanel = Array.from(document.querySelectorAll('[data-analysis-panel]')).find((panel) => panel.getAttribute('data-analysis-panel') === period);
        const activeChannel = activePanel ? activePanel.querySelector('[data-analysis-channel].is-active') || activePanel.querySelector('[data-analysis-channel]') : null;
        if (activeChannel) activateChannelView(activeChannel.getAttribute('data-analysis-channel') || '');
      }});
    }});
    document.querySelectorAll('[data-analysis-channel]').forEach((button) => {{
      button.addEventListener('click', () => activateChannelView(button.getAttribute('data-analysis-channel') || ''));
    }});
    document.querySelectorAll('[data-keyword]').forEach((button) => {{
      button.addEventListener('click', () => selectKeywordButton(button));
    }});
    document.querySelectorAll('[data-analysis-view].is-active').forEach(initAnalysisView);
  </script>
</body>
</html>
"""


def render_search_html(
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
    date_id: str,
    report_url: str,
) -> str:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    start_label = escape(format_kst(start_at, timezone_name))
    end_label = escape(format_kst(end_at, timezone_name))
    report_link = escape(report_url, quote=True)
    read_api_url_json = json.dumps(report_read_api_url(), ensure_ascii=False)
    logo = bside_logo_html("bside-logo--top")
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>시장 이슈 검색 | BSIDE Daily News</title>
  <style>
    :root {{ --paper:#fbfafc; --surface:#fff; --ink:#17121f; --muted:#746b80; --line:#ded5eb; --accent:#7037e0; --accent-deep:#4e20b5; --accent-soft:#f3edff; --green:#00785f; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; color:var(--ink); background:var(--paper); font-family:-apple-system,BlinkMacSystemFont,"Apple SD Gothic Neo","Malgun Gothic","Segoe UI",sans-serif; line-height:1.55; }}
    a {{ color:inherit; text-underline-offset:3px; }}
    .page {{ max-width:1120px; margin:0 auto; padding:24px 24px 72px; }}
    .brand-row {{ display:flex; justify-content:space-between; gap:16px; align-items:baseline; border-bottom:2px solid var(--ink); padding-bottom:14px; margin-bottom:26px; }}
    .bside-logo {{ display:inline-flex; align-items:center; gap:9px; color:var(--accent); text-decoration:none; }}
    .bside-logo__image {{ width:92px; height:auto; display:block; color:var(--accent); flex:0 0 auto; }}
    .bside-logo__label {{ color:var(--accent); font-size:11px; font-weight:900; letter-spacing:.12em; }}
    .edition {{ color:var(--muted); font-size:12px; text-align:right; }}
    .hero {{ display:grid; gap:14px; border-bottom:1px solid var(--line); padding-bottom:22px; }}
    h1 {{ margin:0; font-family:Georgia,"Times New Roman",serif; font-size:clamp(38px,6vw,62px); line-height:1; letter-spacing:0; }}
    .dek {{ max-width:760px; margin:0; color:#342d3d; font-size:15px; word-break:keep-all; }}
    .actions {{ display:flex; flex-wrap:wrap; gap:8px; }}
    .actions a {{ display:inline-flex; border:1px solid var(--line); border-radius:999px; padding:7px 11px; background:var(--surface); color:var(--accent-deep); text-decoration:none; font-size:12px; font-weight:850; }}
    .search-box {{ display:grid; grid-template-columns:minmax(0,1fr) auto; gap:8px; margin-top:8px; max-width:760px; }}
    .search-box input {{ min-width:0; width:100%; border:1px solid var(--line); border-radius:10px; background:var(--surface); color:var(--ink); padding:12px 13px; font:inherit; font-size:15px; }}
    .search-box button {{ border:1px solid var(--accent); border-radius:10px; background:var(--accent); color:#fff; padding:0 18px; font:inherit; font-size:13px; font-weight:900; cursor:pointer; }}
    .suggestions {{ display:flex; flex-wrap:wrap; gap:7px; margin-top:4px; }}
    .suggestions button, .tabs button {{ border:1px solid var(--line); border-radius:999px; background:var(--surface); color:var(--muted); padding:6px 10px; font:inherit; font-size:12px; font-weight:800; cursor:pointer; }}
    .suggestions button:hover, .tabs button:hover, .tabs button.is-active {{ border-color:var(--accent); background:var(--accent-soft); color:var(--accent-deep); }}
    .tabs {{ display:flex; flex-wrap:wrap; gap:8px; margin:18px 0 14px; }}
    .interpretation {{ display:flex; flex-wrap:wrap; gap:7px; margin-top:2px; }}
    .interpretation span {{ border:1px solid rgba(112,55,224,.18); border-radius:999px; background:#fff; color:var(--accent-deep); padding:4px 8px; font-size:11.5px; font-weight:850; }}
    .search-controls {{ display:flex; flex-wrap:wrap; gap:8px; align-items:center; margin-top:8px; }}
    .search-controls select {{ border:1px solid var(--line); border-radius:999px; background:#fff; color:#362d42; padding:7px 10px; font:inherit; font-size:12px; font-weight:800; }}
    .search-controls label {{ display:inline-flex; align-items:center; gap:5px; border:1px solid var(--line); border-radius:999px; background:#fff; color:#4a4255; padding:7px 9px; font-size:12px; font-weight:800; }}
    .dashboard {{ display:grid; grid-template-columns:260px minmax(0,1fr); gap:22px; align-items:start; padding-top:2px; }}
    .insight {{ position:sticky; top:16px; display:grid; gap:10px; border:1px solid var(--line); background:var(--surface); padding:14px; }}
    .insight h2, .results h2 {{ margin:0; font-family:Georgia,"Times New Roman",serif; font-size:21px; line-height:1.1; }}
    .metric-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:8px; }}
    .metric {{ border:1px solid rgba(112,55,224,.15); background:var(--accent-soft); padding:9px; }}
    .metric span {{ display:block; color:var(--muted); font-size:10.5px; font-weight:850; }}
    .metric strong {{ display:block; color:var(--accent-deep); font-size:20px; line-height:1.15; }}
    .panel {{ border-top:1px solid var(--line); padding-top:10px; }}
    .panel h3 {{ margin:0 0 6px; color:var(--accent-deep); font-size:12px; letter-spacing:.03em; }}
    .chip-list {{ display:flex; flex-wrap:wrap; gap:5px; }}
    .chip-list span {{ border:1px solid rgba(112,55,224,.16); border-radius:999px; background:#fff; color:#4c435a; padding:3px 7px; font-size:11px; }}
    .results {{ display:grid; gap:12px; min-width:0; }}
    .status {{ color:var(--muted); font-size:13px; padding:10px 0; }}
    .briefing {{ display:grid; gap:10px; border-top:3px solid var(--accent); background:#fff; padding:14px; box-shadow:0 16px 34px rgba(70,43,102,.06); }}
    .briefing[hidden] {{ display:none; }}
    .briefing h2 {{ margin:0; font-family:Georgia,"Times New Roman",serif; font-size:22px; line-height:1.15; }}
    .briefing__grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:8px; }}
    .briefing__metric {{ border:1px solid rgba(112,55,224,.14); background:var(--accent-soft); padding:8px; }}
    .briefing__metric span {{ display:block; color:var(--muted); font-size:10.5px; font-weight:850; }}
    .briefing__metric strong {{ display:block; color:var(--accent-deep); font-size:18px; line-height:1.2; }}
    .briefing ul {{ margin:0; padding-left:18px; color:#342d3d; font-size:13px; line-height:1.55; }}
    .briefing__notice {{ color:var(--muted); font-size:11.5px; }}
    .result-card {{ display:grid; gap:6px; border-top:1px solid var(--line); padding:13px 0 14px; color:inherit; text-decoration:none; }}
    .result-card:hover h3 {{ color:var(--accent-deep); text-decoration:underline; text-underline-offset:4px; }}
    .result-card h3 {{ margin:0; font-size:18px; line-height:1.34; font-weight:850; word-break:keep-all; overflow-wrap:break-word; }}
    .result-card p {{ margin:0; color:#4d4659; font-size:13px; line-height:1.48; display:-webkit-box; -webkit-line-clamp:2; -webkit-box-orient:vertical; overflow:hidden; }}
    .meta {{ display:flex; flex-wrap:wrap; gap:6px 9px; color:var(--muted); font-size:11px; }}
    .reasons {{ display:flex; flex-wrap:wrap; gap:5px; }}
    .reasons span {{ border:1px solid rgba(112,55,224,.16); border-radius:999px; padding:2px 7px; background:var(--accent-soft); color:var(--accent-deep); font-size:10.5px; font-weight:850; }}
    .telegram-preview {{ display:grid; gap:4px; border-left:3px solid rgba(112,55,224,.35); padding-left:9px; color:#4b4357; font-size:12px; line-height:1.45; }}
    .telegram-preview span {{ display:block; }}
    .why-matters {{ display:grid; gap:4px; border-left:3px solid rgba(0,120,95,.42); padding-left:9px; color:#3d3548; font-size:12.5px; line-height:1.5; }}
    .why-matters span::before {{ content:"• "; color:var(--accent); font-weight:900; }}
    .risk-flags {{ display:flex; flex-wrap:wrap; gap:5px; }}
    .risk-flags span {{ border:1px solid #efd2a7; border-radius:999px; background:#fff7ea; color:#8a4b00; padding:2px 7px; font-size:10.5px; font-weight:850; }}
    .timeline-list {{ display:grid; gap:8px; }}
    .timeline-item {{ display:grid; grid-template-columns:72px minmax(0,1fr); gap:8px; border-top:1px solid var(--line); padding-top:8px; font-size:12.5px; }}
    .timeline-item time {{ color:var(--accent-deep); font-weight:900; }}
    .section-label {{ margin:16px 0 0; border-bottom:2px solid var(--ink); padding-bottom:7px; font-family:Georgia,"Times New Roman",serif; font-size:24px; }}
    @media (max-width:860px) {{
      .page {{ padding:18px 14px 48px; }}
      .brand-row {{ align-items:flex-start; flex-direction:column; }}
      .edition {{ text-align:left; }}
      .search-box {{ grid-template-columns:1fr; }}
      .search-box button {{ padding:10px 14px; }}
      .briefing__grid {{ grid-template-columns:repeat(2,minmax(0,1fr)); }}
      .dashboard {{ grid-template-columns:1fr; }}
      .insight {{ position:static; }}
      .result-card h3 {{ font-size:16px; }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <header class="hero">
      <div class="brand-row">
        {logo}
        <div class="edition">{start_label}<br>{end_label}</div>
      </div>
      <h1>시장 이슈 검색</h1>
      <p class="dek">뉴스 기사, 이슈 묶음, Telegram 공개 채널 신호를 한 화면에서 함께 확인합니다. 검색 결과는 투자 추천이 아니라 시장 언급과 공개 출처를 정리한 보조 정보입니다.</p>
      <div class="actions">
        <a href="{report_link}">최신 데일리로 돌아가기</a>
        <a href="telegram.html">Telegram 데일리</a>
        <a href="telegram-admin.html">Telegram 현황</a>
      </div>
      <form class="search-box" data-search-form>
        <input type="search" name="q" autocomplete="off" placeholder="예: 고려아연, 상장폐지, 주주제안, 공개매수, 밸류업">
        <button type="submit">검색</button>
      </form>
      <div class="suggestions" aria-label="추천 검색어">
        <button type="button" data-suggest="상장폐지">상장폐지</button>
        <button type="button" data-suggest="주주제안">주주제안</button>
        <button type="button" data-suggest="공개매수">공개매수</button>
        <button type="button" data-suggest="밸류업">밸류업</button>
        <button type="button" data-suggest="스튜어드십">스튜어드십</button>
      </div>
      <div class="interpretation" data-query-interpretation hidden></div>
      <div class="search-controls" aria-label="검색 필터">
        <select data-event-filter>
          <option value="all">전체 이벤트</option>
          <option value="management_dispute">경영권·주주행동</option>
          <option value="delisting">상장폐지·거래정지</option>
          <option value="valueup">밸류업·자본정책</option>
          <option value="tender_offer">공개매수·M&A</option>
          <option value="shareholder_action">주주제안·의결권</option>
          <option value="disclosure_violation">불성실공시·제재</option>
          <option value="capital_policy">증자·CB·자본정책</option>
          <option value="disclosure">공시·제도</option>
          <option value="global">해외·영문</option>
        </select>
        <select data-sort-mode>
          <option value="smart">스마트 정렬</option>
          <option value="latest">최신순</option>
          <option value="spread">확산도순</option>
          <option value="telegram">Telegram 급증순</option>
          <option value="low_noise">노이즈 낮은 순</option>
        </select>
        <label><input type="checkbox" data-hide-promotional checked> 홍보성 제외</label>
        <label><input type="checkbox" data-hide-telegram-only> Telegram-only 숨김</label>
      </div>
    </header>

    <div class="tabs" role="tablist" aria-label="검색 범위">
      <button type="button" class="is-active" data-tab="all">전체</button>
      <button type="button" data-tab="issues">이슈</button>
      <button type="button" data-tab="articles">기사</button>
      <button type="button" data-tab="official">공시·제도</button>
      <button type="button" data-tab="telegram">Telegram</button>
      <button type="button" data-tab="history">과거사례</button>
      <button type="button" data-tab="timeline">타임라인</button>
    </div>

    <main class="dashboard">
      <aside class="insight" aria-label="검색 분석">
        <h2>검색 분석</h2>
        <div class="metric-grid">
          <div class="metric"><span>기사</span><strong data-count-articles>0</strong></div>
          <div class="metric"><span>이슈</span><strong data-count-stories>0</strong></div>
          <div class="metric"><span>Telegram</span><strong data-count-telegram>0</strong></div>
          <div class="metric"><span>매체</span><strong data-count-sources>0</strong></div>
        </div>
        <div class="panel">
          <h3>주요 매체</h3>
          <div class="chip-list" data-top-sources><span>검색 후 표시됩니다</span></div>
        </div>
        <div class="panel">
          <h3>분류·키워드</h3>
          <div class="chip-list" data-top-keywords><span>검색 후 표시됩니다</span></div>
        </div>
        <div class="panel">
          <h3>읽는 방법</h3>
          <div class="status">제목·요약 일치, 출처 다양성, Telegram 반복 언급, 최신성을 함께 보세요.</div>
        </div>
      </aside>
      <section class="results" aria-live="polite">
        <h2>검색 결과</h2>
        <div class="status" data-status>검색어를 입력하면 DB 아카이브와 Telegram 신호를 조회합니다.</div>
        <section class="briefing" data-briefing hidden></section>
        <div data-results></div>
      </section>
    </main>
  </div>

  <script>
    const readApiUrl = {read_api_url_json};
    const form = document.querySelector('[data-search-form]');
    const input = form?.querySelector('input[name="q"]');
    const results = document.querySelector('[data-results]');
    const statusEl = document.querySelector('[data-status]');
    const briefingEl = document.querySelector('[data-briefing]');
    const interpretationEl = document.querySelector('[data-query-interpretation]');
    const eventFilter = document.querySelector('[data-event-filter]');
    const sortMode = document.querySelector('[data-sort-mode]');
    const hidePromotional = document.querySelector('[data-hide-promotional]');
    const hideTelegramOnly = document.querySelector('[data-hide-telegram-only]');
    const tabButtons = Array.from(document.querySelectorAll('[data-tab]'));
    const state = {{ query: '', tab: 'all', articles: [], stories: [], signals: [], serverSearch: null }};
    const EVENT_RULES = [
      {{ id: 'management_dispute', label: '경영권·주주행동', keywords: ['경영권', '공개매수', '주주제안', '주주총회', '주총', '의결권', '이사회', '가처분', '소송', '행동주의', '스튜어드십', '주주행동'] }},
      {{ id: 'delisting', label: '상장폐지·거래정지', keywords: ['상장폐지', '상폐', '거래정지', '관리종목', '실질심사', '감사의견', '자본잠식', '정리매매', '불성실공시'] }},
      {{ id: 'valueup', label: '밸류업·자본정책', keywords: ['밸류업', '벨류업', '기업가치', '자사주', '소각', '배당', '주주환원', 'roe', 'pbr', '유상증자', '감자'] }},
      {{ id: 'tender_offer', label: '공개매수·M&A', keywords: ['공개매수', 'tender offer', '매수가', '응모', '최대주주 변경', '인수', '합병'] }},
      {{ id: 'shareholder_action', label: '주주제안·의결권', keywords: ['주주제안', '의결권대리행사', '위임장', '주주서한', '공개서한', '행동주의 펀드'] }},
      {{ id: 'disclosure_violation', label: '불성실공시·제재', keywords: ['불성실공시', '정정공시', '지연공시', '제재', '벌점', '공시위반'] }},
      {{ id: 'capital_policy', label: '증자·CB·자본정책', keywords: ['유상증자', '전환사채', 'cb', 'bw', 'eb', '리픽싱', '감자', '배당', '자사주', '소각'] }},
      {{ id: 'disclosure', label: '공시·제도', keywords: ['공시', '주요사항보고서', 'dart', 'kind', '거래소', '금융위', '금감원', '정정공시', '제도', '감독'] }},
      {{ id: 'global', label: '해외·영문', keywords: ['activist', 'proxy', 'board', 'shareholder', 'governance', 'stewardship', 'tender offer', 'sec', 'bloomberg', 'cnbc'] }},
    ];

    function escapeHtml(value) {{
      return String(value || '').replace(/[&<>"']/g, (char) => ({{'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}}[char]));
    }}
    function apiUrlWithAction(baseUrl, action) {{
      if (!baseUrl) return '';
      return `${{baseUrl}}${{baseUrl.includes('?') ? '&' : '?'}}action=${{encodeURIComponent(action)}}`;
    }}
    function compactText(value, maxChars = 128) {{
      const text = String(value || '').replace(/\\s+/g, ' ').trim();
      if (text.length <= maxChars) return text;
      return `${{text.slice(0, Math.max(0, maxChars - 1)).trim()}}…`;
    }}
    function tokens(value) {{
      return String(value || '').match(/[0-9A-Za-z가-힣]{{2,}}/g)?.map((token) => token.toLowerCase()).filter((token, index, list) => list.indexOf(token) === index).slice(0, 8) || [];
    }}
    function rowText(row) {{
      return [
        row.title, row.representative_title, row.signal_title, row.summary, row.signal_summary,
        row.source, row.feed_name, row.feed_category, row.topic_category,
        Array.isArray(row.top_keywords) ? row.top_keywords.join(' ') : '',
        Array.isArray(row.top_channels) ? row.top_channels.join(' ') : '',
        telegramMessages(row).map((message) => [
          message.excerpt, message.text, message.channel_title, message.channel_handle,
        ].join(' ')).join(' '),
      ].join(' ');
    }}
    function includesQuery(row, query) {{
      const haystack = rowText(row).toLowerCase();
      const queryTokens = tokens(query);
      return !queryTokens.length || queryTokens.some((token) => haystack.includes(token));
    }}
    function compactDateLabelFromValue(value) {{
      const raw = String(value || '').trim();
      if (!raw) return '';
      const direct = raw.match(/^(\\d{{4}})-(\\d{{2}})-(\\d{{2}})[ T](\\d{{2}}):(\\d{{2}})/);
      if (direct) return `${{direct[2]}}.${{direct[3]}} ${{direct[4]}}:${{direct[5]}}`;
      const parsed = new Date(raw);
      if (Number.isNaN(parsed.getTime())) return '';
      const parts = new Intl.DateTimeFormat('en-CA', {{
        timeZone: 'Asia/Seoul',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false,
      }}).formatToParts(parsed).reduce((acc, part) => {{
        acc[part.type] = part.value;
        return acc;
      }}, {{}});
      return parts.month && parts.day && parts.hour && parts.minute ? `${{parts.month}}.${{parts.day}} ${{parts.hour}}:${{parts.minute}}` : '';
    }}
    function dateLabel(row) {{
      for (const key of ['published_at', 'article_published_at', 'feed_published_at', 'sort_at', 'last_article_seen_at', 'latest_seen_at', 'first_seen_at', 'posted_at', 'seen_at', 'created_at', 'updated_at', 'datetime', 'time']) {{
        const label = compactDateLabelFromValue(row?.[key]);
        if (label) return label;
      }}
      return '';
    }}
    function matchReasons(row, query) {{
      const queryTokens = tokens(query);
      const title = String(row.title || row.representative_title || row.signal_title || '').toLowerCase();
      const summary = String(row.summary || row.signal_summary || '').toLowerCase();
      const source = String(row.source || row.feed_name || '').toLowerCase();
      const reasons = [];
      if (queryTokens.some((token) => title.includes(token))) reasons.push('제목 일치');
      if (queryTokens.some((token) => summary.includes(token))) reasons.push('요약 일치');
      if (queryTokens.some((token) => source.includes(token))) reasons.push('매체 일치');
      if (Number(row.related_telegram_count || 0)) reasons.push(`Telegram ${{Number(row.related_telegram_count || 0)}}건`);
      if (Number(row.related_telegram_channels_count || 0)) reasons.push(`채널 ${{Number(row.related_telegram_channels_count || 0)}}곳`);
      if (Number(row.publisher_count || row.related_publishers_count || 0)) reasons.push(`매체 ${{Number(row.publisher_count || row.related_publishers_count || 0)}}곳`);
      if (Number(row.article_count || 0) > 1) reasons.push(`기사 ${{Number(row.article_count || 0)}}건`);
      return reasons.length ? reasons.slice(0, 4) : ['관련도순'];
    }}
    function snippet(row, query) {{
      const primaryMessage = primaryTelegramMessage(row);
      const text = String(
        row.search_snippet || row.summary || row.signal_summary || primaryMessage?.excerpt || primaryMessage?.text || row.title || row.representative_title || row.signal_title || ''
      ).replace(/\\s+/g, ' ').trim();
      if (!text) return '';
      const lower = text.toLowerCase();
      const hit = tokens(query).find((token) => lower.includes(token));
      if (!hit) return compactText(text, 150);
      const index = Math.max(0, lower.indexOf(hit) - 42);
      return `${{index > 0 ? '…' : ''}}${{compactText(text.slice(index, index + 158), 150)}}${{index + 158 < text.length ? '…' : ''}}`;
    }}
    function telegramMessages(row) {{
      return Array.isArray(row.top_related_messages) ? row.top_related_messages.filter(Boolean) : [];
    }}
    function primaryTelegramMessage(row) {{
      return telegramMessages(row).find((message) => message && (message.message_url || message.url)) || null;
    }}
    function classifyEvent(row) {{
      if (row.event_type && typeof row.event_type === 'object' && row.event_type.id) return row.event_type;
      const haystack = rowText(row).toLowerCase();
      const matched = EVENT_RULES
        .map((rule) => ({{ ...rule, hits: rule.keywords.filter((keyword) => haystack.includes(keyword.toLowerCase())) }}))
        .filter((rule) => rule.hits.length)
        .sort((a, b) => b.hits.length - a.hits.length);
      return matched[0] || {{ id: 'general', label: '일반 이슈', keywords: [], hits: [] }};
    }}
    function riskFlags(row) {{
      const flags = Array.isArray(row.risk_flags) ? row.risk_flags : [];
      const text = rowText(row).toLowerCase();
      const inferred = [];
      if (/(수익보장|리딩방|무료추천|급등주|vip|레퍼럴)/i.test(text)) inferred.push('promotional');
      if (/(카더라|찌라시|확인[ ]?불가|미확인|루머)/i.test(text)) inferred.push('rumor');
      if (/(공개매수|경영권|상장폐지|거래정지|관리종목|감사의견|유상증자|불성실공시)/i.test(text)) inferred.push('market_sensitive');
      return [...new Set([...flags, ...inferred].map((flag) => String(flag || '').trim()).filter(Boolean))];
    }}
    function isTelegramOnly(row, kind) {{
      if (kind !== 'Telegram') return false;
      const signalType = String(row.signal_type || '').toLowerCase();
      return signalType === 'topic_burst' || !String(row.article_id || '').trim() || String(row.article_id || '').startsWith('telegram-topic:');
    }}
    function recencyScore(row) {{
      const raw = String(row.published_at || row.sort_at || row.last_article_seen_at || row.latest_seen_at || row.first_seen_at || '').trim();
      const parsed = raw ? Date.parse(raw.replace(' ', 'T')) : NaN;
      if (!Number.isFinite(parsed)) return 0.25;
      const ageHours = Math.max(0, (Date.now() - parsed) / 36e5);
      return Math.max(0, Math.min(1, 1 - ageHours / (24 * 14)));
    }}
    function spreadScore(row, kind) {{
      const articles = Number(row.article_count || row.related_article_count || 0);
      const publishers = Number(row.publisher_count || row.related_publishers_count || 0);
      const telegram = Number(row.related_telegram_count || 0);
      const channels = Number(row.related_telegram_channels_count || 0);
      const engagement = telegramMessages(row).reduce((sum, message) => sum + Number(message.views || 0) / 5000 + Number(message.forwards || 0) / 50, 0);
      return Math.min(1, Math.log1p(articles + publishers * 2 + telegram + channels * 2 + engagement) / 4);
    }}
    function materialityScore(row) {{
      const event = classifyEvent(row).id;
      if (event === 'management_dispute' || event === 'delisting') return 1;
      if (event === 'valueup' || event === 'disclosure') return 0.72;
      if (event === 'global') return 0.55;
      return 0.35;
    }}
    function riskPenalty(row) {{
      const flags = riskFlags(row);
      let penalty = 0;
      if (flags.includes('promotional')) penalty += 0.35;
      if (flags.includes('rumor')) penalty += 0.22;
      if (flags.includes('unverified')) penalty += 0.12;
      return penalty;
    }}
    function smartScore(row, kind, query) {{
      const queryTokens = tokens(query);
      const text = rowText(row).toLowerCase();
      const queryRelevance = queryTokens.length ? queryTokens.filter((token) => text.includes(token)).length / queryTokens.length : 0.5;
      const officialHint = /(공시|dart|kind|거래소|금융위|금감원|법원|주요사항보고서)/i.test(text) ? 1 : 0;
      const base = kind === '이슈' ? 0.08 : kind === '기사' ? 0.02 : -0.02;
      const score =
        base +
        0.28 * queryRelevance +
        0.14 * officialHint +
        0.16 * spreadScore(row, kind) +
        0.16 * recencyScore(row) +
        0.16 * materialityScore(row) +
        0.10 * (kind === 'Telegram' ? Number(row.confidence_score || 0.5) : 0.65) -
        riskPenalty(row);
      return Math.max(0, score);
    }}
    function rowScore(row, kind, query) {{
      const mode = sortMode?.value || 'smart';
      if (mode === 'smart' && Number.isFinite(Number(row.search_score))) return Number(row.search_score);
      if (mode === 'latest') return recencyScore(row);
      if (mode === 'spread') return spreadScore(row, kind);
      if (mode === 'telegram') return kind === 'Telegram' ? spreadScore(row, kind) + Number(row.confidence_score || 0) * 0.2 : Number(row.related_telegram_count || 0) / 20;
      if (mode === 'low_noise') return smartScore(row, kind, query) - riskPenalty(row) * 1.5;
      return smartScore(row, kind, query);
    }}
    function passesSearchFilters(row, kind) {{
      const selectedEvent = eventFilter?.value || 'all';
      if (selectedEvent !== 'all' && classifyEvent(row).id !== selectedEvent) return false;
      const flags = riskFlags(row);
      if (hidePromotional?.checked && flags.includes('promotional')) return false;
      if (hideTelegramOnly?.checked && isTelegramOnly(row, kind)) return false;
      return true;
    }}
    function rowTimestamp(row) {{
      const raw = String(row.published_at || row.sort_at || row.last_article_seen_at || row.latest_seen_at || row.first_seen_at || row.posted_at || '').trim();
      const parsed = raw ? Date.parse(raw.replace(' ', 'T')) : NaN;
      return Number.isFinite(parsed) ? parsed : 0;
    }}
    function isOfficialRow(row) {{
      const event = classifyEvent(row).id;
      return ['disclosure', 'disclosure_violation', 'delisting', 'capital_policy'].includes(event)
        || /(공시|dart|kind|거래소|금융위|금감원|법원|주요사항보고서|정정공시|불성실공시)/i.test(rowText(row));
    }}
    function isHistoricalRow(row) {{
      const ts = rowTimestamp(row);
      return ts > 0 && (Date.now() - ts) > 14 * 24 * 60 * 60 * 1000;
    }}
    function whyMatters(row, kind) {{
      if (Array.isArray(row.why_matters) && row.why_matters.length) return row.why_matters.slice(0, 3);
      const event = classifyEvent(row);
      const flags = riskFlags(row);
      const lines = [];
      if (event.id === 'management_dispute') lines.push('주주권·의결권·이사회 책임 쟁점과 연결되는 이슈입니다.');
      else if (event.id === 'delisting') lines.push('거래 가능성과 투자자 보호 절차에 직접 연결되는 시장 민감 이벤트입니다.');
      else if (event.id === 'valueup') lines.push('자사주·배당·기업가치 제고 등 실제 자본정책 여부를 함께 봐야 합니다.');
      else if (event.id === 'disclosure') lines.push('공시·제도 변화와 후속 기사 확산 여부를 확인할 필요가 있습니다.');
      else if (event.id === 'global') lines.push('해외 시장의 행동주의·거버넌스 흐름을 국내 관점에서 비교해 볼 수 있습니다.');
      if (Number(row.publisher_count || row.article_count || 0) > 1) lines.push('복수 매체가 다루고 있어 단발 보도보다 확산도가 높습니다.');
      if (Number(row.related_telegram_channels_count || 0) > 1) lines.push(`Telegram ${{Number(row.related_telegram_channels_count)}}개 채널에서 반복 언급됐습니다.`);
      if (flags.includes('promotional') || flags.includes('rumor') || flags.includes('unverified')) lines.push('미확인·홍보성 가능성이 있어 원문 확인이 필요합니다.');
      return lines.slice(0, 3);
    }}
    function renderInterpretation(query, rows) {{
      if (!interpretationEl) return;
      const serverInterpretation = state.serverSearch?.query_interpretation;
      if (serverInterpretation && query) {{
        const chips = [
          ...(Array.isArray(serverInterpretation.keywords) ? serverInterpretation.keywords.slice(0, 4).map((token) => ['검색어', token]) : []),
          ...(Array.isArray(serverInterpretation.event_types) ? serverInterpretation.event_types.slice(0, 4).map((event) => ['이벤트', `${{event.label || event.id}} ${{event.count || 0}}`]) : []),
        ];
        interpretationEl.hidden = !chips.length;
        interpretationEl.innerHTML = chips.map(([type, label]) => `<span>${{escapeHtml(type)}}: ${{escapeHtml(label)}}</span>`).join('');
        return;
      }}
      const queryTokens = tokens(query);
      const events = countValues(rows.map((row) => classifyEvent(row)), (row) => row.label).slice(0, 4);
      const chips = [
        ...queryTokens.slice(0, 4).map((token) => ['검색어', token]),
        ...events.map(([label, count]) => ['이벤트', `${{label}} ${{count}}`]),
      ];
      interpretationEl.hidden = !chips.length;
      interpretationEl.innerHTML = chips.map(([type, label]) => `<span>${{escapeHtml(type)}}: ${{escapeHtml(label)}}</span>`).join('');
    }}
    function renderBriefing(query, articles, stories, signals) {{
      if (!briefingEl) return;
      const allRows = [...stories, ...articles, ...signals];
      if (!query || !allRows.length) {{
        briefingEl.hidden = true;
        briefingEl.innerHTML = '';
        return;
      }}
      const serverBriefing = state.serverSearch?.briefing;
      if (serverBriefing) {{
        const counts = serverBriefing.source_counts || {{}};
        const event = state.serverSearch?.query_interpretation?.event_types?.[0]?.label || '검색 이슈';
        const bullets = Array.isArray(serverBriefing.bullets) ? serverBriefing.bullets.slice(0, 5) : [];
        briefingEl.hidden = false;
        briefingEl.innerHTML = `
          <h2>이슈 브리핑</h2>
          <div class="briefing__grid">
            <div class="briefing__metric"><span>이벤트</span><strong>${{escapeHtml(event)}}</strong></div>
            <div class="briefing__metric"><span>기사</span><strong>${{Number(counts.articles || articles.length)}}</strong></div>
            <div class="briefing__metric"><span>이슈</span><strong>${{Number(counts.stories || stories.length)}}</strong></div>
            <div class="briefing__metric"><span>Telegram</span><strong>${{Number(counts.telegram_signals || signals.length)}}</strong></div>
          </div>
          ${{serverBriefing.headline ? `<p>${{escapeHtml(serverBriefing.headline)}}</p>` : ''}}
          <ul>${{bullets.map((line) => `<li>${{escapeHtml(line)}}</li>`).join('')}}</ul>
          <div class="briefing__notice">${{escapeHtml(serverBriefing.disclaimer || '공개 정보 기반 이슈 정리이며 투자 제안·권유·종목 추천이 아닙니다.')}}</div>
        `;
        return;
      }}
      const event = countValues(allRows.map((row) => classifyEvent(row)), (row) => row.label)[0]?.[0] || '일반 이슈';
      const sources = new Set(articles.map(sourceName).filter(Boolean));
      const telegramChannels = new Set(signals.flatMap((row) => Array.isArray(row.top_channels) ? row.top_channels : telegramMessages(row).map((message) => message.channel_handle || message.channel_title)).filter(Boolean));
      const riskCounts = countValues(allRows.flatMap((row) => riskFlags(row)).map((value) => ({{ value }})), (row) => row.value);
      const bullets = [
        `${{event}} 관점에서 기사·이슈·Telegram 언급을 함께 정리했습니다.`,
        sources.size ? `기사 출처 ${{sources.size}}곳이 검색어와 연결됩니다.` : '아직 기사 출처 확산은 제한적입니다.',
        telegramChannels.size ? `Telegram 공개 채널 ${{telegramChannels.size}}곳에서 관련 언급이 확인됩니다.` : 'Telegram 관련 언급은 제한적입니다.',
        riskCounts.length ? `주의 플래그: ${{riskCounts.map(([label, count]) => `${{label}} ${{count}}`).join(' · ')}}` : '주요 루머·홍보성 플래그는 제한적입니다.',
      ];
      briefingEl.hidden = false;
      briefingEl.innerHTML = `
        <h2>이슈 브리핑</h2>
        <div class="briefing__grid">
          <div class="briefing__metric"><span>이벤트</span><strong>${{escapeHtml(event)}}</strong></div>
          <div class="briefing__metric"><span>기사</span><strong>${{articles.length}}</strong></div>
          <div class="briefing__metric"><span>이슈</span><strong>${{stories.length}}</strong></div>
          <div class="briefing__metric"><span>Telegram</span><strong>${{signals.length}}</strong></div>
        </div>
        <ul>${{bullets.map((line) => `<li>${{escapeHtml(line)}}</li>`).join('')}}</ul>
        <div class="briefing__notice">공개 정보 기반 이슈 정리이며 투자 제안·권유·종목 추천이 아닙니다.</div>
      `;
    }}
    function safeResultUrl(value) {{
      const raw = String(value || '').trim();
      if (!raw) return '';
      try {{
        const parsed = new URL(raw, location.href);
        if (parsed.protocol === 'http:' || parsed.protocol === 'https:') return parsed.href;
      }} catch (error) {{}}
      return '';
    }}
    function resultHref(row, kind) {{
      if (kind === 'Telegram') {{
        const primaryMessage = primaryTelegramMessage(row);
        return safeResultUrl(primaryMessage?.message_url || primaryMessage?.url || row.message_url || row.url);
      }}
      return safeResultUrl(row.canonical_url || row.representative_url || row.url || row.message_url);
    }}
    function sourceName(row) {{
      const primaryMessage = primaryTelegramMessage(row);
      if (primaryMessage) return String(primaryMessage.channel_title || primaryMessage.channel_handle || 'Telegram');
      if (Array.isArray(row.top_channels) && row.top_channels.length) return `채널 ${{row.top_channels.length}}곳`;
      return String(row.source || row.feed_name || row.primary_source || row.channel_title || row.channel_handle || '출처 미상');
    }}
    function countValues(rows, getter) {{
      const counts = new Map();
      rows.forEach((row) => {{
        const value = String(getter(row) || '').trim();
        if (!value) return;
        counts.set(value, (counts.get(value) || 0) + 1);
      }});
      return Array.from(counts.entries()).sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])).slice(0, 7);
    }}
    function setChips(selector, values) {{
      const node = document.querySelector(selector);
      if (!node) return;
      node.innerHTML = values.length ? values.map(([label, count]) => `<span>${{escapeHtml(label)}} ${{count}}</span>`).join('') : '<span>표시할 항목 없음</span>';
    }}
    function updateMetrics(articles, stories, signals) {{
      const sources = new Set([
        ...articles.map(sourceName),
        ...stories.map(sourceName),
        ...signals.flatMap((row) => Array.isArray(row.top_channels) ? row.top_channels : [sourceName(row)]),
      ].filter(Boolean));
      document.querySelector('[data-count-articles]').textContent = String(articles.length);
      document.querySelector('[data-count-stories]').textContent = String(stories.length);
      document.querySelector('[data-count-telegram]').textContent = String(signals.length);
      document.querySelector('[data-count-sources]').textContent = String(sources.size);
      setChips('[data-top-sources]', countValues([
        ...articles,
        ...stories,
        ...signals.flatMap((row) => (Array.isArray(row.top_channels) ? row.top_channels : []).map((channel) => ({{ source: channel }}))),
      ], sourceName));
      const keywords = [
        ...articles.map((row) => row.feed_category || row.relevance_level || row.priority_level || ''),
        ...stories.map((row) => row.topic_category || row.feed_category || ''),
        ...signals.flatMap((row) => Array.isArray(row.top_keywords) ? row.top_keywords : []),
      ];
      setChips('[data-top-keywords]', countValues(keywords.map((value) => ({{ value }})), (row) => row.value));
    }}
    function resultCard(row, kind, query) {{
      const title = row.title || row.representative_title || row.signal_title || '제목 없음';
      const href = resultHref(row, kind);
      const meta = [
        kind,
        dateLabel(row),
        sourceName(row),
        row.feed_category || row.topic_category || '',
      ].filter(Boolean);
      const reasons = matchReasons(row, query);
      const telegramPreview = kind === 'Telegram'
        ? telegramMessages(row).slice(0, 3).map((message) => {{
          const channel = message.channel_title || message.channel_handle || 'Telegram';
          const text = message.excerpt || message.text || '';
          return text ? `<span>${{escapeHtml(compactText(channel, 24))}} · ${{escapeHtml(compactText(text, 150))}}</span>` : '';
        }}).join('')
        : '';
      const why = whyMatters(row, kind);
      const flags = riskFlags(row);
      return `<a class="result-card" href="${{escapeHtml(href || '#')}}" target="_blank" rel="noopener noreferrer">
        <div class="meta">${{meta.map((item) => `<span>${{escapeHtml(compactText(item, 42))}}</span>`).join('')}}</div>
        <h3>${{escapeHtml(compactText(title, 118))}}</h3>
        ${{snippet(row, query) ? `<p>${{escapeHtml(snippet(row, query))}}</p>` : ''}}
        ${{why.length ? `<div class="why-matters">${{why.map((line) => `<span>${{escapeHtml(line)}}</span>`).join('')}}</div>` : ''}}
        ${{telegramPreview ? `<div class="telegram-preview">${{telegramPreview}}</div>` : ''}}
        ${{flags.length ? `<div class="risk-flags">${{flags.map((flag) => `<span>${{escapeHtml(flag)}}</span>`).join('')}}</div>` : ''}}
        <div class="reasons">${{reasons.map((reason) => `<span>${{escapeHtml(reason)}}</span>`).join('')}}</div>
      </a>`;
    }}
    function timelineRows(articles, stories, signals, query) {{
      const serverTimeline = Array.isArray(state.serverSearch?.timeline) ? state.serverSearch.timeline : [];
      if (serverTimeline.length) {{
        return serverTimeline.slice(0, 40).map((item) => {{
          const title = item.title || item.excerpt || item.text || '제목 없음';
          const href = safeResultUrl(item.url || item.message_url);
          return `<div class="timeline-item"><time>${{escapeHtml(dateLabel(item) || '일시 미상')}}</time><a href="${{escapeHtml(href || '#')}}" target="_blank" rel="noopener noreferrer">${{escapeHtml(item.kind || '항목')}} · ${{escapeHtml(compactText(title, 118))}}</a></div>`;
        }}).join('');
      }}
      const rows = [
        ...stories.map((row) => ({{ row, kind: '이슈' }})),
        ...articles.map((row) => ({{ row, kind: '기사' }})),
        ...signals.flatMap((row) => telegramMessages(row).slice(0, 4).map((message) => ({{ row: {{ ...message, signal_title: row.signal_title, top_keywords: row.top_keywords, risk_flags: row.risk_flags }}, kind: 'Telegram' }}))),
      ];
      return rows
        .map((item) => {{
          const raw = item.row.published_at || item.row.sort_at || item.row.latest_seen_at || item.row.posted_at || item.row.first_seen_at || '';
          const ts = Date.parse(String(raw).replace(' ', 'T'));
          return {{ ...item, ts: Number.isFinite(ts) ? ts : 0 }};
        }})
        .sort((a, b) => b.ts - a.ts)
        .slice(0, 30)
        .map((item) => {{
          const title = item.row.title || item.row.representative_title || item.row.signal_title || item.row.excerpt || item.row.text || '제목 없음';
          const href = item.kind === 'Telegram' ? safeResultUrl(item.row.message_url || item.row.url) : resultHref(item.row, item.kind);
          return `<div class="timeline-item"><time>${{escapeHtml(dateLabel(item.row) || '일시 미상')}}</time><a href="${{escapeHtml(href || '#')}}" target="_blank" rel="noopener noreferrer">${{escapeHtml(item.kind)}} · ${{escapeHtml(compactText(title, 118))}}</a></div>`;
        }}).join('');
    }}
    function render() {{
      const query = state.query;
      const articles = state.articles.filter((row) => includesQuery(row, query)).filter((row) => passesSearchFilters(row, '기사')).sort((a, b) => rowScore(b, '기사', query) - rowScore(a, '기사', query));
      const stories = state.stories.filter((row) => includesQuery(row, query)).filter((row) => passesSearchFilters(row, '이슈')).sort((a, b) => rowScore(b, '이슈', query) - rowScore(a, '이슈', query));
      const signals = state.signals.filter((row) => includesQuery(row, query)).filter((row) => passesSearchFilters(row, 'Telegram')).sort((a, b) => rowScore(b, 'Telegram', query) - rowScore(a, 'Telegram', query));
      updateMetrics(articles, stories, signals);
      renderInterpretation(query, [...stories, ...articles, ...signals]);
      renderBriefing(query, articles, stories, signals);
      if (state.tab === 'timeline') {{
        const timeline = timelineRows(articles, stories, signals, query);
        results.innerHTML = timeline ? `<h3 class="section-label">타임라인</h3><div class="timeline-list">${{timeline}}</div>` : '<div class="status">타임라인으로 표시할 결과가 없습니다.</div>';
        statusEl.textContent = query ? `'${{query}}' 기준 시간순 이벤트를 표시합니다.` : '검색어를 입력하면 타임라인을 조회합니다.';
        return;
      }}
      const groups = [];
      if (state.tab === 'all' || state.tab === 'issues') groups.push(['이슈', stories, '이슈']);
      if (state.tab === 'all' || state.tab === 'articles') groups.push(['기사', articles, '기사']);
      if (state.tab === 'official') groups.push(['공시·제도', [
        ...stories.map((row) => [row, '이슈']),
        ...articles.map((row) => [row, '기사']),
        ...signals.map((row) => [row, 'Telegram']),
      ].filter(([row]) => isOfficialRow(row)), 'mixed']);
      if (state.tab === 'all' || state.tab === 'telegram') groups.push(['Telegram 신호', signals, 'Telegram']);
      if (state.tab === 'history') groups.push(['과거사례', [
        ...stories.map((row) => [row, '이슈']),
        ...articles.map((row) => [row, '기사']),
        ...signals.map((row) => [row, 'Telegram']),
      ].filter(([row]) => isHistoricalRow(row)), 'mixed']);
      const html = groups.map(([label, rows, kind]) => rows.length
        ? `<h3 class="section-label">${{label}}</h3>${{rows.slice(0, state.tab === 'all' ? 12 : 40).map((entry) => Array.isArray(entry) ? resultCard(entry[0], entry[1], query) : resultCard(entry, kind, query)).join('')}}`
        : '').join('');
      results.innerHTML = html || '<div class="status">검색 결과가 없습니다. 검색어를 조금 넓혀보세요.</div>';
      statusEl.textContent = query ? `'${{query}}' 기준 기사 ${{articles.length}}건, 이슈 ${{stories.length}}건, Telegram 신호 ${{signals.length}}건` : '검색어를 입력하면 DB 아카이브와 Telegram 신호를 조회합니다.';
    }}
    async function fetchJson(url) {{
      const response = await fetch(url, {{ headers: {{ Accept: 'application/json' }}, credentials: 'omit', cache: 'no-store' }});
      if (!response.ok) throw new Error(`HTTP ${{response.status}}`);
      return response.json();
    }}
    function searchSortParam() {{
      const mode = sortMode?.value || 'smart';
      return mode === 'telegram' ? 'telegram_momentum' : mode;
    }}
    function applySearchPayload(payload) {{
      state.serverSearch = payload || null;
      state.articles = Array.isArray(payload?.articles) ? payload.articles : [];
      state.stories = Array.isArray(payload?.stories) ? payload.stories : [];
      state.signals = Array.isArray(payload?.telegram) ? payload.telegram : (Array.isArray(payload?.signals) ? payload.signals : []);
      render();
    }}
    async function runFallbackSearch(cleaned) {{
      const [articleResult, storyResult, telegramResult] = await Promise.allSettled([
        fetchJson(`${{apiUrlWithAction(readApiUrl, 'articles')}}&q=${{encodeURIComponent(cleaned)}}&limit=40&days=365`),
        fetchJson(`${{apiUrlWithAction(readApiUrl, 'latest_snapshot')}}&limit=60`),
        fetchJson(apiUrlWithAction(readApiUrl, 'telegram_dashboard')),
      ]);
      state.serverSearch = null;
      state.articles = articleResult.status === 'fulfilled' && articleResult.value?.ok ? (articleResult.value.articles || []) : [];
      state.stories = storyResult.status === 'fulfilled' && storyResult.value?.ok ? (storyResult.value.stories || []) : [];
      state.signals = telegramResult.status === 'fulfilled' && telegramResult.value?.ok ? (telegramResult.value.signals || []) : [];
      render();
    }}
    async function runSearch(query) {{
      const cleaned = String(query || '').replace(/\\s+/g, ' ').trim();
      state.query = cleaned;
      if (input) input.value = cleaned;
      if (history.replaceState) history.replaceState(null, '', cleaned ? `?q=${{encodeURIComponent(cleaned)}}` : location.pathname);
      if (cleaned.length < 2) {{
        statusEl.textContent = '검색어를 2자 이상 입력해주세요.';
        results.innerHTML = '';
        state.serverSearch = null;
        updateMetrics([], [], []);
        return;
      }}
      if (!readApiUrl) {{
        statusEl.textContent = '공개 DB API가 설정되면 검색 결과가 표시됩니다.';
        results.innerHTML = '';
        state.serverSearch = null;
        updateMetrics([], [], []);
        return;
      }}
      statusEl.textContent = '검색 중입니다.';
      try {{
        const payload = await fetchJson(`${{apiUrlWithAction(readApiUrl, 'search')}}&q=${{encodeURIComponent(cleaned)}}&limit=40&days=365&sort=${{encodeURIComponent(searchSortParam())}}`);
        if (payload?.ok) {{
          applySearchPayload(payload);
          return;
        }}
      }} catch (error) {{
        console.info('통합 검색 API를 사용할 수 없어 기존 조회 방식으로 전환합니다.', error);
      }}
      await runFallbackSearch(cleaned);
    }}
    form?.addEventListener('submit', (event) => {{
      event.preventDefault();
      runSearch(input ? input.value : '');
    }});
    document.querySelectorAll('[data-suggest]').forEach((button) => {{
      button.addEventListener('click', () => runSearch(button.dataset.suggest || ''));
    }});
    tabButtons.forEach((button) => {{
      button.addEventListener('click', () => {{
        state.tab = button.dataset.tab || 'all';
        tabButtons.forEach((item) => item.classList.toggle('is-active', item === button));
        render();
      }});
    }});
    [eventFilter, sortMode, hidePromotional, hideTelegramOnly].filter(Boolean).forEach((control) => {{
      control.addEventListener('change', render);
    }});
    const initialQuery = new URLSearchParams(location.search).get('q') || '';
    if (initialQuery) runSearch(initialQuery);
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
    telegram_html = render_telegram_daily_html(stories, state, config, start_at, end_at, date_id, report_url)
    search_html = render_search_html(config, start_at, end_at, date_id, report_url)
    return {
        "config": config,
        "date_id": date_id,
        "start_at": start_at,
        "end_at": end_at,
        "stories": stories,
        "review": review,
        "html": html,
        "telegram_html": telegram_html,
        "search_html": search_html,
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
    telegram_path = feed_dir / "telegram.html"
    workbench_path = feed_dir / "workbench.html"
    search_path = feed_dir / "search.html"
    dated_path.write_text(html, encoding="utf-8", newline="\n")
    latest_path.write_text(html, encoding="utf-8", newline="\n")
    telegram_path.write_text(normalize_generated_html(str(report.get("telegram_html") or "")), encoding="utf-8", newline="\n")
    if workbench_path.exists():
        workbench_path.unlink()
    search_path.write_text(normalize_generated_html(str(report.get("search_html") or "")), encoding="utf-8", newline="\n")
    variant_dir = feed_dir / "variants"
    if variant_dir.exists():
        for stale_path in variant_dir.glob("*.html"):
            stale_path.unlink()
    index_path.write_text(render_report_index(feed_dir), encoding="utf-8", newline="\n")
    refreshed_paths = refresh_existing_report_archive_links(feed_dir, date_id)
    return [dated_path, latest_path, telegram_path, search_path, index_path, *refreshed_paths]


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
    updated = ARCHIVE_LINKS_PATTERN.sub(replacement, html, count=1)
    return (
        updated.replace('<a href="workbench.html">AI 워크벤치 보기</a>', '<a href="telegram.html">Telegram 데일리 보기</a>')
        .replace('<a href="workbench.html">AI 워크벤치</a>', '<a href="telegram.html">Telegram 데일리</a>')
    )


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
    .actions {{ display:flex; flex-wrap:wrap; gap:8px; margin-top:18px; }}
    .actions a {{ display:inline-flex; border:1px solid var(--line); border-radius:999px; padding:8px 12px; color:var(--accent); background:#fff; font-size:13px; font-weight:850; }}
    ul {{ list-style:none; padding:0; margin:32px 0 0; border-top:2px solid var(--ink); }}
    li {{ border-bottom:1px solid var(--line); }}
    ul a {{ display:block; padding:16px 0; color:inherit; text-decoration:none; font-size:20px; }}
    a {{ color:inherit; text-decoration:none; }}
    a:hover {{ color:var(--accent); }}
  </style>
</head>
<body>
  <main>
    {logo}
    <h1>데일리 아카이브</h1>
    <p>매일 발행된 주주·자본시장 데일리를 날짜별로 확인할 수 있습니다.</p>
    <div class="actions">
      <a href="latest.html">최신 데일리</a>
      <a href="search.html">시장 이슈 검색</a>
      <a href="telegram.html">Telegram 데일리</a>
    </div>
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
    stories = [story for story in stories if isinstance(story, dict)]
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
        for story in stories[:3]:
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
