from __future__ import annotations

import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from html import escape
from pathlib import Path
from urllib.parse import quote, urlsplit
from zoneinfo import ZoneInfo

import httpx

from .ai import ai_config, call_github_models
from .config import load_config
from .dates import format_kst, now_in_timezone
from .fetch import USER_AGENT, image_href
from .rss_writer import article_link, article_source_label, compact_text
from .state import load_state
from .summaries import (
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
REPORT_CATEGORY_ORDER = [
    "주주행동·경영권",
    "밸류업·주주환원",
    "자본시장 제도·공시",
    "해외·영문",
    "기타",
]
BSIDE_URL = "https://bside.ai"
BSIDE_LOGO_SVG = """<svg fill="currentColor" viewBox="0 0 57 20" class="bside-logo__image" aria-hidden="true"><path fill="currentColor" d="M7.11306 19.3232C5.87526 19.3232 4.76917 19.032 3.78817 18.4496C2.80716 17.8673 2.03026 17.0798 1.46404 16.0738C0.891241 15.0745 0.601547 13.9429 0.588379 12.6855V1.80579C0.588379 1.44181 0.700305 1.14401 0.930744 0.925618C1.16118 0.700612 1.45087 0.594727 1.79982 0.594727C2.14877 0.594727 2.45821 0.707229 2.67549 0.925618C2.89934 1.15063 3.00467 1.44181 3.00467 1.80579V8.25158C3.55773 7.58316 4.22929 7.05372 5.01279 6.66328C5.79626 6.27284 6.65217 6.08093 7.58709 6.08093C8.73929 6.08093 9.77956 6.37211 10.6947 6.95446C11.6099 7.53684 12.3341 8.32437 12.8674 9.31705C13.4007 10.3097 13.6641 11.4347 13.6641 12.6921C13.6641 13.9495 13.3744 15.0812 12.795 16.0805C12.2156 17.0798 11.4387 17.8739 10.4577 18.4563C9.47667 19.0386 8.35741 19.3298 7.11306 19.3298V19.3232ZM7.11306 17.179C7.92288 17.179 8.64053 16.9871 9.27259 16.5967C9.90464 16.2062 10.405 15.6701 10.7803 14.9885C11.149 14.3069 11.3399 13.5392 11.3399 12.6988C11.3399 11.8583 11.1556 11.0774 10.7803 10.409C10.4116 9.74058 9.90464 9.21116 9.27259 8.82069C8.64053 8.43025 7.91629 8.23831 7.11306 8.23831C6.30982 8.23831 5.60532 8.43025 4.9667 8.82069C4.32806 9.21116 3.82109 9.74058 3.45897 10.409C3.09685 11.0774 2.91251 11.8384 2.91251 12.6988C2.91251 13.5591 3.09685 14.3069 3.45897 14.9885C3.82109 15.6701 4.32806 16.2128 4.9667 16.5967C5.60532 16.9871 6.32297 17.179 7.11306 17.179Z"></path><path fill="currentColor" d="M19.6288 19.3236C18.5227 19.3236 17.4956 19.1515 16.5541 18.814C15.6126 18.4765 14.8818 18.0397 14.355 17.5103C14.118 17.2588 14.0193 16.9676 14.0588 16.6367C14.0983 16.3125 14.2497 16.0411 14.5196 15.836C14.8357 15.5845 15.1517 15.472 15.4546 15.5117C15.764 15.5514 16.0273 15.6904 16.2512 15.9286C16.5211 16.233 16.9491 16.511 17.5482 16.7757C18.1408 17.0404 18.8058 17.1728 19.53 17.1728C20.4452 17.1728 21.1496 17.0206 21.6303 16.7161C22.1109 16.4117 22.3611 16.0213 22.3808 15.5448C22.4006 15.0683 22.1702 14.6514 21.7027 14.3006C21.2352 13.9499 20.3859 13.6653 19.1481 13.4403C17.5482 13.1227 16.3895 12.6462 15.6718 12.0108C14.9542 11.3756 14.5921 10.5946 14.5921 9.67476C14.5921 8.86076 14.8291 8.19238 15.3031 7.65632C15.7772 7.12029 16.3895 6.72323 17.1335 6.46511C17.8774 6.20041 18.6543 6.06805 19.4576 6.06805C20.5044 6.06805 21.4262 6.2335 22.236 6.571C23.0458 6.90853 23.6845 7.36514 24.1585 7.95414C24.3823 8.20561 24.4811 8.47694 24.4679 8.75488C24.4548 9.03285 24.3165 9.27108 24.0663 9.45638C23.8161 9.63505 23.5133 9.688 23.1643 9.62182C22.8154 9.55564 22.5257 9.41667 22.2887 9.19167C21.8936 8.80785 21.4657 8.54973 21.0048 8.40414C20.5439 8.25855 20.0172 8.19238 19.4115 8.19238C18.7136 8.19238 18.1276 8.3115 17.6404 8.54973C17.1598 8.78797 16.9162 9.13873 16.9162 9.60197C16.9162 9.88653 16.9886 10.1446 17.14 10.3763C17.2915 10.6079 17.5878 10.813 18.0289 10.9983C18.47 11.1836 19.1218 11.3623 19.9777 11.5344C21.1628 11.7726 22.1043 12.0771 22.7891 12.441C23.4803 12.805 23.9741 13.2352 24.2704 13.7183C24.5733 14.2014 24.7247 14.7639 24.7247 15.3992C24.7247 16.1338 24.5338 16.7889 24.1453 17.3779C23.7569 17.9669 23.1907 18.4368 22.4335 18.7875C21.683 19.1383 20.7414 19.3103 19.6222 19.3103L19.6288 19.3236Z"></path><path fill="currentColor" d="M26.8648 4.04907C26.4369 4.04907 26.0682 3.89686 25.7587 3.58583C25.4493 3.27478 25.2979 2.90419 25.2979 2.47403C25.2979 2.04388 25.4493 1.67327 25.7587 1.36223C26.0682 1.0512 26.4369 0.898987 26.8648 0.898987C27.2928 0.898987 27.6615 1.0512 27.9709 1.36223C28.2804 1.67327 28.4318 2.04388 28.4318 2.47403C28.4318 2.90419 28.2804 3.27478 27.9709 3.58583C27.6615 3.89686 27.2928 4.04907 26.8648 4.04907ZM26.8648 19.2304C26.5159 19.2304 26.2262 19.1179 25.9958 18.8862C25.7653 18.6546 25.6534 18.3635 25.6534 18.0127V7.39107C25.6534 7.0271 25.7653 6.7293 25.9958 6.51089C26.2262 6.28589 26.5159 6.18001 26.8648 6.18001C27.2138 6.18001 27.5232 6.29251 27.7405 6.51089C27.9644 6.73592 28.0697 7.0271 28.0697 7.39107V18.0127C28.0697 18.3635 27.9578 18.6546 27.7405 18.8862C27.5166 19.1179 27.2269 19.2304 26.8648 19.2304Z"></path><path fill="currentColor" d="M36.1018 19.3233C34.8641 19.3233 33.758 19.0322 32.7638 18.4498C31.7762 17.8674 30.9927 17.0799 30.4133 16.074C29.8341 15.0747 29.5444 13.9431 29.5444 12.6857C29.5444 11.4283 29.8077 10.3032 30.3409 9.31055C30.8741 8.31787 31.5918 7.53038 32.5136 6.94799C33.4288 6.36564 34.4691 6.07443 35.6212 6.07443C36.5562 6.07443 37.4186 6.26635 38.2088 6.65682C38.9988 7.04726 39.6638 7.57667 40.2036 8.24508V1.79932C40.2036 1.43534 40.3156 1.13754 40.5462 0.919148C40.7765 0.694142 41.0662 0.588257 41.415 0.588257C41.7641 0.588257 42.0736 0.70076 42.2909 0.919148C42.5147 1.14415 42.62 1.43534 42.62 1.79932V12.6791C42.62 13.9364 42.3303 15.0681 41.7509 16.0674C41.1715 17.0667 40.3947 17.8608 39.4136 18.4432C38.4327 19.0255 37.3265 19.3167 36.0888 19.3167L36.1018 19.3233ZM36.1018 17.1792C36.9118 17.1792 37.6294 16.9873 38.2615 16.5968C38.8936 16.2063 39.3938 15.6703 39.7559 14.9887C40.118 14.307 40.3024 13.5393 40.3024 12.6989C40.3024 11.8584 40.118 11.0775 39.7559 10.4091C39.3938 9.74073 38.8936 9.21129 38.2615 8.82085C37.6294 8.4304 36.905 8.23846 36.1018 8.23846C35.2986 8.23846 34.5941 8.4304 33.9556 8.82085C33.3168 9.21129 32.81 9.74073 32.4347 10.4091C32.0594 11.0775 31.875 11.8386 31.875 12.6989C31.875 13.5592 32.0594 14.307 32.4347 14.9887C32.8033 15.6703 33.3103 16.213 33.9556 16.5968C34.5941 16.9873 35.3118 17.1792 36.1018 17.1792Z"></path><path fill="currentColor" d="M50.6328 19.3231C49.3157 19.3231 48.1504 19.0386 47.1301 18.476C46.1095 17.9135 45.3063 17.1326 44.7334 16.14C44.154 15.1473 43.8643 14.0024 43.8643 12.7186C43.8643 11.4347 44.1343 10.2633 44.6807 9.27064C45.2272 8.27796 45.9843 7.49708 46.9457 6.93455C47.9134 6.37205 49.0195 6.08746 50.2704 6.08746C51.5216 6.08746 52.5684 6.36543 53.4504 6.90808C54.3393 7.45737 55.011 8.21179 55.4784 9.17799C55.9457 10.1442 56.1763 11.2494 56.1763 12.5068C56.1763 12.8112 56.071 13.0627 55.8669 13.2612C55.6628 13.4597 55.3995 13.559 55.0834 13.559H45.5169V11.6531H55.011L54.0366 12.3215C54.0234 11.5273 53.8654 10.8126 53.5625 10.1839C53.2596 9.55523 52.8316 9.05888 52.2787 8.6949C51.7254 8.3309 51.054 8.14561 50.264 8.14561C49.3619 8.14561 48.5916 8.34414 47.9463 8.74123C47.3078 9.13829 46.8204 9.68096 46.4848 10.3758C46.1554 11.0707 45.9843 11.8516 45.9843 12.7252C45.9843 13.5987 46.1819 14.3796 46.5769 15.0613C46.9719 15.7429 47.5184 16.2856 48.2163 16.6826C48.9143 17.0797 49.711 17.2782 50.6128 17.2782C51.1001 17.2782 51.6072 17.1856 52.1207 17.0069C52.6343 16.8282 53.049 16.6098 53.3648 16.3716C53.6019 16.1995 53.8587 16.1069 54.1354 16.0936C54.4119 16.087 54.6554 16.1664 54.8595 16.3451C55.1295 16.5833 55.2678 16.8481 55.2875 17.1326C55.3007 17.4172 55.1754 17.6687 54.9057 17.8738C54.3657 18.304 53.7007 18.6547 52.8975 18.9261C52.1007 19.1974 51.3372 19.3298 50.6063 19.3298L50.6328 19.3231Z"></path></svg>"""
LAYOUT_VARIANTS = [
    {
        "slug": "memo",
        "name": "Investment Memo",
        "note": "투자 메모처럼 핵심 판단 근거를 차곡차곡 읽는 노트형",
    },
    {
        "slug": "board",
        "name": "Governance Board",
        "note": "카테고리별 이슈를 보드처럼 훑는 워크룸형",
    },
    {
        "slug": "pulse",
        "name": "Market Pulse",
        "note": "30분 안에 핵심 이슈를 훑는 시장 모니터형",
    },
    {
        "slug": "deck",
        "name": "Investor Deck",
        "note": "섹션별 카드 덱으로 빠르게 넘겨보는 리딩 세션형",
    },
]


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


def article_domain(url: str) -> str:
    hostname = (urlsplit(url).hostname or "").lower().removeprefix("www.")
    return hostname or "source"


def source_logo_url(domain: str) -> str:
    normalized = domain.lower().removeprefix("www.").strip()
    if not normalized or normalized == "source":
        return ""
    return f"https://www.google.com/s2/favicons?domain={quote(normalized, safe='')}&sz=128"


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
    seen_urls: set[str] = set()
    for entry in group:
        article = entry.get("article")
        if not isinstance(article, dict):
            continue
        url = str(entry.get("url") or article_link(article) or "")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        source = article_source_label(article)
        title = str(entry.get("title") or article.get("clean_title") or article.get("title") or source)
        published_at = entry_datetime(entry)
        links.append(
            {
                "source": source,
                "title": title,
                "url": url,
                "domain": article_domain(url),
                "published_at": published_at.isoformat() if published_at else "",
            }
        )
    return links


def story_image_url(group: list[dict[str, object]]) -> str:
    for entry in group:
        article = entry.get("article")
        if not isinstance(article, dict):
            continue
        image_url = str(article.get("image_url") or "").strip()
        if image_url.startswith(("http://", "https://")):
            return image_url
    return ""


def image_enrich_settings(config: dict[str, object]) -> tuple[int, float]:
    report_config = config.get("report", {})
    if not isinstance(report_config, dict):
        report_config = {}
    limit = int(report_config.get("image_enrich_limit", 120) or 120)
    timeout = float(report_config.get("image_timeout_seconds", 4) or 4)
    return max(0, limit), max(1.0, timeout)


def story_image_candidates(story: dict[str, object]) -> list[str]:
    candidates: list[str] = []
    for value in [story.get("image_url"), story.get("primary_url")]:
        text = str(value or "").strip()
        if text.startswith(("http://", "https://")) and text not in candidates:
            candidates.append(text)
    links = story.get("links") if isinstance(story.get("links"), list) else []
    for link in links[:4]:
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


def enrich_story_images(stories: list[dict[str, object]], config: dict[str, object]) -> None:
    limit, timeout = image_enrich_settings(config)
    if limit <= 0:
        return
    checked = 0
    with httpx.Client(timeout=timeout, headers={"User-Agent": USER_AGENT}) as client:
        for story in stories:
            if str(story.get("image_url") or "").startswith(("http://", "https://")):
                continue
            for candidate_url in story_image_candidates(story):
                if checked >= limit:
                    return
                checked += 1
                image_url = discover_story_image(candidate_url, client)
                if image_url:
                    story["image_url"] = image_url
                    break


def story_source_line(links: list[dict[str, str]]) -> str:
    counter = Counter(link["source"] for link in links if link.get("source"))
    return " · ".join(source for source, _count in counter.most_common(4))


def story_logo_context(story: dict[str, object]) -> tuple[str, str]:
    links = story.get("links") if isinstance(story.get("links"), list) else []
    first_link = next((link for link in links if isinstance(link, dict)), {})
    source = str(
        story.get("primary_source")
        or (first_link.get("source") if isinstance(first_link, dict) else "")
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
    logo_img = (
        f'<img class="story__source-logo" src="{safe_logo}" alt="" loading="lazy" decoding="async" referrerpolicy="no-referrer">'
        if logo_url
        else ""
    )
    return (
        f'<a class="story__image story__image--logo" href="{href}" aria-label="{safe_attr_label} 기사 보기" '
        f'data-logo-label="{safe_attr_label}" data-logo-src="{safe_logo}">'
        f'{logo_img}<span>{safe_label}</span></a>'
    )


def story_image_data_attrs(story: dict[str, object]) -> str:
    label, logo_url = story_logo_context(story)
    return (
        f' data-logo-label="{escape(label, quote=True)}"'
        f' data-logo-src="{escape(logo_url, quote=True)}"'
    )


def bside_logo_html(extra_class: str = "") -> str:
    class_name = f"bside-logo {extra_class}".strip()
    return (
        f'<a class="{class_name}" href="{BSIDE_URL}" aria-label="BSIDE Korea 홈페이지">'
        f"{BSIDE_LOGO_SVG}"
        '<span class="bside-logo__label">DAILY NEWS</span>'
        '</a>'
    )


def variant_href(slug: str, *, in_variant_dir: bool) -> str:
    if slug == "standard":
        return "../latest.html" if in_variant_dir else "latest.html"
    return f"{slug}.html" if in_variant_dir else f"variants/{slug}.html"


def render_layout_variant_links(current_slug: str, *, in_variant_dir: bool) -> str:
    items = [
        {
            "slug": "standard",
            "name": "운영 기본",
            "note": "현재 운영 중인 기본 레이아웃",
        },
        *LAYOUT_VARIANTS,
    ]
    return "\n".join(
        (
            f'<a class="variant-switcher__link{" is-active" if item["slug"] == current_slug else ""}" '
            f'href="{escape(variant_href(str(item["slug"]), in_variant_dir=in_variant_dir), quote=True)}">'
            f'<strong>{escape(str(item["name"]))}</strong>'
            f'<span>{escape(str(item["note"]))}</span>'
            "</a>"
        )
        for item in items
    )


def layout_variant_css() -> str:
    return """
    .variant-switcher { display: grid; gap: 8px; margin-top: 18px; border-top: 1px solid var(--line); padding-top: 12px; }
    .variant-switcher__title { color: var(--muted); font-size: 11px; font-weight: 900; letter-spacing: .06em; text-transform: uppercase; }
    .variant-switcher__links { display: grid; grid-template-columns: repeat(auto-fit, minmax(132px, 1fr)); gap: 6px; }
    .variant-switcher__link { min-width: 0; border: 1px solid var(--line); border-radius: 8px; background: var(--surface); padding: 7px 8px; text-decoration: none; }
    .variant-switcher__link strong { display: block; overflow: hidden; color: var(--accent-deep); font-size: 11px; text-overflow: ellipsis; white-space: nowrap; }
    .variant-switcher__link span { display: block; overflow: hidden; color: var(--muted); font-size: 10px; line-height: 1.3; text-overflow: ellipsis; white-space: nowrap; }
    .variant-switcher__link.is-active { border-color: var(--accent); background: var(--accent-soft); }

    body.layout-memo { --paper: #fffdf7; --surface: #ffffff; --line: #eadfca; --accent: #8b5c00; --accent-deep: #5b3c00; --accent-soft: #fff4d7; --green: #557600; }
    body.layout-memo .page { max-width: 980px; }
    body.layout-memo .masthead, body.layout-memo .brief, body.layout-memo .section { background: linear-gradient(#fffdf7 31px, #f1e8d8 32px); background-size: 100% 32px; }
    body.layout-memo h1 { font-family: Georgia, "Times New Roman", serif; font-size: clamp(40px, 5vw, 62px); }
    body.layout-memo .featured { display: block; border-bottom: 1px dashed var(--line); }
    body.layout-memo .featured .story--featured:first-child, body.layout-memo .featured .story--featured:nth-child(n+2), body.layout-memo .story-list .story:first-child, body.layout-memo .story { grid-template-columns: 96px minmax(0, 1fr); border: 1px solid var(--line); border-radius: 10px; background: rgba(255,255,255,.86); box-shadow: 0 10px 24px rgba(97, 69, 18, .05); margin-bottom: 10px; padding: 12px; }
    body.layout-memo .story-list { display: block; }
    body.layout-memo .story h3 { font-size: 17px; }
    body.layout-memo .story p::before { content: "Memo  "; color: var(--accent); font-weight: 900; }

    body.layout-board { --paper: #f9fbf7; --surface: #ffffff; --line: #d9e4d2; --accent: #3b7d3a; --accent-deep: #21501f; --accent-soft: #edf8ea; --green: #2d735e; }
    body.layout-board .page { max-width: 1180px; }
    body.layout-board .featured { display: none; }
    body.layout-board .section { border: 1px solid var(--line); border-radius: 18px; background: #fff; margin: 20px 0; padding: 16px; scroll-margin-top: 96px; }
    body.layout-board .section__rule { display: none; }
    body.layout-board .section__head { border-bottom: 1px solid var(--line); padding-bottom: 10px; }
    body.layout-board .story-list { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 10px; }
    body.layout-board .story-list .story:first-child, body.layout-board .story { display: block; border: 1px solid var(--line); border-radius: 12px; background: var(--accent-soft); padding: 11px; }
    body.layout-board .story__image { display: none; }
    body.layout-board .story h3 { font-size: 16px; }

    body.layout-pulse { --paper: #f4f7fb; --surface: #ffffff; --line: #d6e0ee; --accent: #155eef; --accent-deep: #123a83; --accent-soft: #eaf2ff; --green: #087f5b; }
    body.layout-pulse .page { max-width: 1180px; padding-top: 16px; }
    body.layout-pulse .masthead { border: 0; border-radius: 20px; background: #071b3a; color: #fff; padding: 18px; box-shadow: 0 20px 48px rgba(7, 27, 58, .16); }
    body.layout-pulse .brand-row { border-color: rgba(255,255,255,.18); margin-bottom: 18px; }
    body.layout-pulse .bside-logo, body.layout-pulse .bside-logo__image, body.layout-pulse .bside-logo__label { color: #fff; }
    body.layout-pulse h1 { max-width: none; font-family: -apple-system, BlinkMacSystemFont, "Apple SD Gothic Neo", "Malgun Gothic", "Segoe UI", sans-serif; font-size: clamp(34px, 5vw, 58px); font-weight: 900; }
    body.layout-pulse .dek { max-width: 760px; color: #dbe7ff; }
    body.layout-pulse .edition, body.layout-pulse .meta-strip { color: #b9c9e7; }
    body.layout-pulse .meta-strip { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 8px; }
    body.layout-pulse .meta-strip span { border: 1px solid rgba(255,255,255,.16); border-radius: 12px; background: rgba(255,255,255,.08); padding: 8px 10px; }
    body.layout-pulse .meta-strip strong { color: #fff; }
    body.layout-pulse .brief { grid-template-columns: 176px minmax(0, 1fr); border: 1px solid var(--line); border-radius: 16px; background: #fff; margin-top: 14px; padding: 14px; }
    body.layout-pulse .brief h2 { font-size: 0; }
    body.layout-pulse .brief h2::after { content: "30분 체크포인트"; color: var(--accent-deep); font-family: -apple-system, BlinkMacSystemFont, "Apple SD Gothic Neo", "Malgun Gothic", "Segoe UI", sans-serif; font-size: 15px; font-weight: 900; }
    body.layout-pulse .brief__bullets { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px 10px; }
    body.layout-pulse .brief__bullets li { border-left: 3px solid var(--accent); border-radius: 8px; background: var(--accent-soft); padding: 8px 10px 8px 12px; }
    body.layout-pulse .brief__bullets li::before { display: none; }
    body.layout-pulse .featured { grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; border: 0; padding: 16px 0 10px; }
    body.layout-pulse .featured .story--featured:first-child,
    body.layout-pulse .featured .story--featured:nth-child(n+2) { display: block; border: 1px solid var(--line); border-radius: 16px; background: #fff; box-shadow: 0 10px 28px rgba(18, 58, 131, .07); padding: 13px; }
    body.layout-pulse .featured .story__image { display: none; }
    body.layout-pulse .featured h3 { font-size: 16px; }
    body.layout-pulse .featured p { display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }
    body.layout-pulse .section { display: grid; grid-template-columns: 176px minmax(0, 1fr); gap: 18px; border-top: 1px solid var(--line); padding: 18px 0; scroll-margin-top: 98px; }
    body.layout-pulse .section__rule { display: none; }
    body.layout-pulse .section__head { position: sticky; top: 72px; display: block; align-self: start; }
    body.layout-pulse .section__head h2 { font-family: -apple-system, BlinkMacSystemFont, "Apple SD Gothic Neo", "Malgun Gothic", "Segoe UI", sans-serif; font-size: 17px; font-weight: 900; }
    body.layout-pulse .section__head span { display: block; margin-top: 6px; }
    body.layout-pulse .story-list { display: block; counter-reset: pulse-rank; margin-top: 0; }
    body.layout-pulse .story-list .story:first-child,
    body.layout-pulse .story { position: relative; display: block; border: 0; border-bottom: 1px solid var(--line); background: #fff; margin: 0; padding: 11px 12px 11px 44px; }
    body.layout-pulse .story::before { counter-increment: pulse-rank; content: counter(pulse-rank); position: absolute; left: 10px; top: 14px; display: grid; place-items: center; width: 24px; height: 24px; border-radius: 7px; color: var(--accent-deep); background: var(--accent-soft); font-size: 11px; font-weight: 900; font-variant-numeric: tabular-nums; }
    body.layout-pulse .story__image { display: none; }
    body.layout-pulse .story h3 { font-size: 15px; margin-bottom: 3px; }
    body.layout-pulse .story p { display: none; }
    body.layout-pulse .story__sources { display: block; overflow: hidden; color: var(--muted); font-size: 11px; text-overflow: ellipsis; white-space: nowrap; }
    body.layout-pulse details { display: none; }

    body.layout-deck { --paper: #fbfafc; --surface: #ffffff; --line: #ded7e8; --accent: #6b35d8; --accent-deep: #42207e; --accent-soft: #f0eafb; --green: #00785f; }
    body.layout-deck .page { max-width: none; padding: 0 0 72px; }
    body.layout-deck .masthead { min-height: 52vh; display: grid; align-content: end; border: 0; padding: 28px clamp(22px, 7vw, 96px); background: linear-gradient(135deg, #ffffff 0%, #f5f0ff 46%, #eaf2ff 100%); }
    body.layout-deck .brand-row { max-width: 1180px; width: 100%; border-color: rgba(107,53,216,.2); }
    body.layout-deck h1 { max-width: 1040px; font-size: clamp(48px, 8vw, 94px); line-height: .92; }
    body.layout-deck .dek { max-width: 780px; font-size: 17px; }
    body.layout-deck .brief { position: relative; z-index: 2; max-width: 1080px; margin: -38px auto 0; border: 1px solid var(--line); border-radius: 24px; background: rgba(255,255,255,.96); box-shadow: 0 22px 54px rgba(44, 27, 84, .12); padding: 18px; }
    body.layout-deck .brief h2 { font-size: 0; }
    body.layout-deck .brief h2::after { content: "오늘의 리딩 맵"; font-family: Georgia, "Times New Roman", serif; font-size: 22px; }
    body.layout-deck .toc { max-width: 1080px; margin: 16px auto 0; border: 1px solid var(--line); border-radius: 999px; background: rgba(255,255,255,.9); padding: 8px 10px; }
    body.layout-deck .featured { display: none; }
    body.layout-deck .section { max-width: 1180px; min-height: 70vh; display: grid; grid-template-columns: 260px minmax(0, 1fr); gap: 24px; margin: 0 auto; border-top: 0; padding: 42px 24px; scroll-margin-top: 96px; }
    body.layout-deck .section__rule { display: none; }
    body.layout-deck .section__head { position: sticky; top: 92px; display: block; align-self: start; }
    body.layout-deck .section__head h2 { font-size: 30px; }
    body.layout-deck .section__head span { display: block; margin-top: 8px; }
    body.layout-deck .story-list { display: flex; gap: 16px; overflow-x: auto; margin-top: 0; padding: 8px 4px 22px; scroll-snap-type: x proximity; }
    body.layout-deck .story-list .story:first-child,
    body.layout-deck .story { flex: 0 0 310px; display: flex; flex-direction: column; border: 1px solid var(--line); border-radius: 24px; background: #fff; box-shadow: 0 14px 36px rgba(44, 27, 84, .08); overflow: hidden; padding: 0; scroll-snap-align: start; }
    body.layout-deck .story__image { aspect-ratio: 16 / 10; border: 0; border-bottom: 1px solid var(--line); }
    body.layout-deck .story__body { padding: 14px; }
    body.layout-deck .story h3 { font-size: 17px; }
    body.layout-deck .story p { display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden; }
    body.layout-deck .story__sources { display: none; }
    body.layout-deck details { display: none; }

    @media (max-width: 860px) {
      .variant-switcher__links { display: flex; overflow-x: auto; padding-bottom: 2px; scrollbar-width: none; }
      .variant-switcher__links::-webkit-scrollbar { display: none; }
      .variant-switcher__link { flex: 0 0 136px; }
      body.layout-board .story-list { display: block; }
      body.layout-pulse .meta-strip, body.layout-pulse .brief__bullets { grid-template-columns: 1fr; }
      body.layout-pulse .brief, body.layout-pulse .section { display: block; }
      body.layout-pulse .featured { display: block; }
      body.layout-pulse .featured .story--featured:first-child,
      body.layout-pulse .featured .story--featured:nth-child(n+2) { margin-bottom: 10px; }
      body.layout-pulse .section__head { position: static; margin-bottom: 8px; }
      body.layout-deck .masthead { min-height: auto; padding: 24px; }
      body.layout-deck h1 { font-size: clamp(40px, 12vw, 58px); }
      body.layout-deck .brief, body.layout-deck .toc { margin-left: 18px; margin-right: 18px; }
      body.layout-deck .section { display: block; min-height: auto; padding: 28px 18px; }
      body.layout-deck .section__head { position: static; margin-bottom: 12px; }
      body.layout-deck .story-list { display: grid; grid-template-columns: 1fr; overflow: visible; }
      body.layout-deck .story-list .story:first-child,
      body.layout-deck .story { flex: auto; }
    }
    """


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
            stories.append(
                {
                    "title": title,
                    "category": category,
                    "summary": best_story_summary(group),
                    "links": links,
                    "link_count": len(links),
                    "image_url": story_image_url(group),
                    "primary_url": str(representative.get("url") or links[0]["url"]),
                    "primary_source": links[0]["source"],
                    "source_line": story_source_line(links),
                    "datetime": latest_dt,
                    "section": section_key,
                    "score": len(links) * 10 + (3 if category == "주주행동·경영권" else 0),
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


def fallback_story_brief(story: dict[str, object]) -> dict[str, str]:
    title = compact_text(str(story.get("title") or ""), max_chars=86)
    category = str(story.get("category") or "")
    link_count = int(story.get("link_count") or 0)
    summary = clean_brief_source_noise(story_summary_for_display(story))
    source_line = compact_text(str(story.get("source_line") or story.get("primary_source") or ""), max_chars=72)
    category_tail = {
        "주주행동·경영권": "주주권과 경영권 이슈의 후속 흐름을 보여줍니다.",
        "밸류업·주주환원": "주주환원 정책의 실행 가능성과 시장 반응을 확인할 수 있습니다.",
        "자본시장 제도·공시": "공시·감독 제도 변화가 자본시장에 미치는 영향을 짚어볼 사안입니다.",
        "해외·영문": "해외 투자자와 외신이 바라보는 지배구조·행동주의 흐름을 보여줍니다.",
    }.get(category, "자본시장 관점에서 후속 흐름을 확인할 만한 사안입니다.")
    if link_count <= 1 and summary and len(summary) >= 30:
        point = compact_text(summary, max_chars=128)
    else:
        point = compact_text(f"{title}. {category_tail}", max_chars=128)

    if category == "주주행동·경영권":
        why = "주주권 행사, 이사회 책임, 경영권 대응의 기준을 함께 볼 사안입니다."
    elif category == "밸류업·주주환원":
        why = "주주환원 정책이 실제 실행과 공시 신뢰로 이어지는지 확인할 필요가 있습니다."
    elif category == "자본시장 제도·공시":
        why = "감독·공시·거래 제도 변화가 일반주주 보호와 시장 규율에 미칠 영향을 봐야 합니다."
    elif category == "해외·영문":
        why = "해외 투자자와 외신이 한국 시장 또는 글로벌 행동주의를 어떻게 해석하는지 보여줍니다."
    else:
        why = "자본시장 투자자 관점에서 후속 보도와 공시 연결 여부를 확인할 만합니다."

    if link_count > 1 and source_line:
        evidence = f"{source_line} 등 {link_count}건 보도"
    elif source_line:
        evidence = f"{source_line} 보도"
    else:
        evidence = "수집 기사 기준"

    return {
        "point": point,
        "why": compact_text(why, max_chars=112),
        "evidence": compact_text(evidence, max_chars=96),
    }


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


def parse_story_brief_response(content: str | None) -> dict[str, dict[str, str]]:
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
    parsed: dict[str, dict[str, str]] = {}
    for item in items:
        if not isinstance(item, dict):
            continue
        story_id = str(item.get("id") or "").strip()
        if not story_id:
            continue
        point = compact_text(str(item.get("point") or ""), max_chars=128)
        why = compact_text(str(item.get("why") or ""), max_chars=112)
        evidence = compact_text(str(item.get("evidence") or ""), max_chars=96)
        if point or why or evidence:
            parsed[story_id] = {
                "point": point,
                "why": why,
                "evidence": evidence,
            }
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
        "기사 제목과 수집 요약만 바탕으로 투자자가 빠르게 읽을 수 있는 요점, 맥락, 근거를 씁니다. "
        "기사에 없는 사실을 만들지 말고, 매수·매도 판단은 금지합니다."
    )
    user_prompt = (
        "아래 기사 묶음별로 JSON만 출력하세요.\n"
        "형식: {\"stories\":[{\"id\":\"story-1\",\"point\":\"...\",\"why\":\"...\",\"evidence\":\"...\"}]}\n"
        "- point: 기사 핵심을 55~95자, 완성된 한국어 문장으로 작성\n"
        "- why: 투자자/주주권/공시/제도 관점의 의미를 45~85자로 작성\n"
        "- evidence: 직접 인용 대신 '어느 매체들이 다뤘는지' 또는 '복수 매체 보도' 수준으로 작성\n"
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
        story["brief"] = {
            "point": brief.get("point") or fallback["point"],
            "why": brief.get("why") or fallback["why"],
            "evidence": brief.get("evidence") or fallback["evidence"],
        }


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
            "주주행동·경영권 이슈는 이사회 책임과 공시 투명성을 둘러싼 투자자 보호 쟁점으로 이어지고 있습니다."
        )
    if valueup:
        paragraphs.append(
            "밸류업과 주주환원 보도는 자사주·배당 정책의 실행 가능성과 공시 구체성이 핵심 변수로 부각됩니다."
        )
    if capital:
        paragraphs.append(
            "자본시장 제도·공시 분야에서는 감독당국의 정정 요구와 시장 규율 강화 흐름을 함께 볼 필요가 있습니다."
        )
    if global_titles:
        paragraphs.append(
            "해외·영문 보도는 행동주의 캠페인과 한국 시장 평가가 맞물리는 지점을 중심으로 추적할 만합니다."
        )
    if not paragraphs:
        paragraphs.append("신규 발행 이슈는 제한적이지만 기존 주주권·공시 이슈의 후속 보도 흐름은 계속 확인할 필요가 있습니다.")
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
        "수집된 기사 묶음을 바탕으로 하루치 브리핑의 핵심 bullet만 한국어 기사체로 작성합니다. "
        "투자 조언이나 매매 권유는 하지 말고, 기사에 없는 사실을 단정하지 마세요."
    )
    user_prompt = (
        "아래 기사 묶음을 바탕으로 Telegram과 HTML 데일리 상단에 들어갈 상세 요약을 작성하세요.\n"
        "- bullet point 3~4개로 작성\n"
        "- 각 bullet은 45~85자 안팎의 한 문장으로 작성\n"
        "- 예: '주주권 행사와 이사회 책임 이슈가 맞물리며 투자자 보호 논의가 다시 부각됩니다.'\n"
        "- 전체 흐름, 주요 사건, 제도/정책적 의미, 해외/영문 흐름을 균형 있게 반영\n"
        "- 전문 자본시장 기자의 톤으로, 정책·공시·주주권 의미를 해석하되 과장하지 않음\n"
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
        url = escape(link.get("url") or "", quote=True)
        if compact:
            items.append(f'<a href="{url}">{source}</a>')
        else:
            published = escape(link_date_label(link, config))
            items.append(
                "<tr>"
                f'<td class="link-table__time">{published}</td>'
                f'<td class="link-table__source">{source}</td>'
                f'<td class="link-table__title"><a href="{url}">{title}</a></td>'
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
        items.append(f'<a href="{escape(url, quote=True)}">{escape(source)}</a>')
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
    primary_url = escape(str(story.get("primary_url") or "#"), quote=True)
    category = escape(str(story.get("category") or "기타"))
    sources = escape(str(story.get("source_line") or story.get("primary_source") or ""))
    summary = escape(story_summary_for_display(story))
    brief = story.get("brief") if isinstance(story.get("brief"), dict) else {}
    if editorial and brief:
        point = escape(str(brief.get("point") or ""))
        why = escape(str(brief.get("why") or ""))
        evidence = escape(str(brief.get("evidence") or ""))
        insight_items = "\n".join(
            item
            for item in (
                f'<p><strong>요점</strong>{point}</p>' if point else "",
                f'<p><strong>맥락</strong>{why}</p>' if why else "",
                f'<p><strong>근거</strong>{evidence}</p>' if evidence else "",
            )
            if item
        )
        summary_html = f'<div class="story__insight">{insight_items}</div>' if insight_items else ""
    else:
        summary_html = f"<p>{summary}</p>" if summary else ""
    timestamp = escape(date_label(story.get("datetime"), config))
    image_url = escape(str(story.get("image_url") or ""), quote=True)
    image_html = (
        f'<a class="story__image" href="{primary_url}" aria-label="기사 이미지 보기"{story_image_data_attrs(story)}><img src="{image_url}" alt="" loading="lazy" decoding="async" referrerpolicy="no-referrer"></a>'
        if image_url
        else source_logo_html(story, primary_url)
    )
    normalized_links = [link for link in links if isinstance(link, dict)]
    has_grouped_links = len(normalized_links) > 1
    detail_links = render_link_list(normalized_links, config, compact=False) if has_grouped_links else ""
    source_links = render_source_links(normalized_links) if has_grouped_links else ""
    source_meta = source_links or sources
    source_meta_html = f'<span class="story__sources">{source_meta}</span>' if source_meta else ""
    details_html = (
        f"""
            <details>
              <summary>기사 링크 {len(normalized_links)}건 보기</summary>
              <div class="link-table">
                <table>
                  <thead><tr><th>일시</th><th>매체</th><th>기사</th></tr></thead>
                  <tbody>{detail_links}</tbody>
                </table>
              </div>
            </details>
        """
        if show_details and has_grouped_links
        else ""
    )
    featured_class = " story--featured" if featured else ""
    section_attrs = ""
    if section_id:
        section_attrs = (
            f' data-section-key="{escape(section_id, quote=True)}"'
            f' data-section-index="{section_index}"'
            f' data-section-total="{section_total}"'
        )
    return f"""
          <article class="story{featured_class}" id="{story_id}" data-story{section_attrs}>
            {image_html}
            <div class="story__body">
              <div class="story__meta"><span>{category}</span><span>{timestamp}</span>{source_meta_html}</div>
              <h3><a href="{primary_url}">{safe_title}</a></h3>
              {summary_html}
            </div>
            {details_html}
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
    variant_slug = layout_variant if layout_variant in {"standard", *(str(item["slug"]) for item in LAYOUT_VARIANTS)} else "standard"
    is_standard_layout = variant_slug == "standard"
    stats = report_stats(stories, clusters, duplicate_records)
    buckets = category_buckets(stories)
    review_bullets = clean_report_bullets(review) or clean_report_bullets(fallback_report_review(stories))
    review_html = "\n".join(f"<li>{escape(bullet)}</li>" for bullet in review_bullets)
    review_block_html = f'<ul class="brief__bullets">{review_html}</ul>' if review_html else ""
    featured_stories = stories[: 5 if is_standard_layout else 3]
    featured_html = "\n".join(
        render_story(story, config, featured=True, show_details=False, editorial=is_standard_layout)
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
        if is_standard_layout
        else f"""
    <section class="featured" aria-label="top stories">
      {featured_html}
    </section>
        """
    )
    category_sections = []
    for category in REPORT_CATEGORY_ORDER:
        category_stories = buckets.get(category, [])
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
            {''.join(render_story(story, config, section_id=section_id, section_index=index, section_total=len(category_stories), editorial=is_standard_layout) for index, story in enumerate(category_stories, start=1))}
          </div>
        </section>
            """
        )
    toc = "\n".join(
        f'<a class="chip" data-toc-section="{escape(slugify(category, "section"), quote=True)}" href="#{escape(slugify(category, "section"), quote=True)}" style="--progress:0"><span class="chip__label">{escape(category)}</span><span class="chip__progress" data-progress-text>0/{len(buckets.get(category, []))}</span></a>'
        for category in REPORT_CATEGORY_ORDER
        if buckets.get(category)
    )
    side_category_links = "\n".join(
        f'<a data-nav-section data-section-target="{escape(slugify(category, "section"), quote=True)}" href="#{escape(slugify(category, "section"), quote=True)}"><span class="nav-label">{escape(category)}</span><span class="nav-progress" data-progress-text>0/{len(buckets.get(category, []))}</span></a>'
        for category in REPORT_CATEGORY_ORDER
        if buckets.get(category)
    )
    ordered_section_stories = [
        story
        for category in REPORT_CATEGORY_ORDER
        for story in buckets.get(category, [])
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
    variant_class = f"layout-{variant_slug}"
    variant_links_html = render_layout_variant_links(variant_slug, in_variant_dir=in_variant_dir)
    variant_css = layout_variant_css()
    brief_title = "오늘의 핵심 브리핑" if is_standard_layout else "Editor’s Brief"
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
    .brief {{ display: grid; grid-template-columns: 150px 1fr; gap: 22px; border-bottom: 1px solid var(--ink); padding: 18px 0; }}
    .brief h2 {{ font-family: Georgia, "Times New Roman", serif; font-size: 20px; line-height: 1.1; margin: 0; }}
    .section h2 {{ font-family: Georgia, "Times New Roman", serif; font-size: 26px; line-height: 1.1; margin: 0; }}
    .brief__bullets {{ margin: 0; padding: 0; list-style: none; display: grid; gap: 5px; }}
    .brief__bullets li {{ position: relative; padding-left: 13px; font-size: 12.5px; line-height: 1.42; color: #2e2738; word-break: keep-all; overflow-wrap: break-word; }}
    .brief__bullets li::before {{ content: ""; position: absolute; left: 0; top: .72em; width: 4px; height: 4px; border-radius: 50%; background: var(--accent); }}
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
    .featured {{ display: grid; grid-template-columns: minmax(0, 1.35fr) minmax(260px, .95fr); gap: 0 24px; border-bottom: 1px solid var(--ink); padding: 24px 0; align-items: stretch; }}
    .priority .featured {{ border-bottom: 0; padding-bottom: 12px; }}
    .featured .story--featured:first-child {{ grid-row: span 2; border-right: 1px solid var(--line); padding-right: 24px; }}
    .featured .story--featured:nth-child(n+2) {{ display: grid; grid-template-columns: 112px minmax(0, 1fr); gap: 14px; border-top: 1px solid var(--line); padding: 14px 0 0; }}
    .featured .story--featured:nth-child(2) {{ border-top: 0; padding-top: 0; }}
    .featured .story--featured:nth-child(n+2) .story__image {{ aspect-ratio: 4 / 3; }}
    .featured .story--featured:nth-child(n+2) h3 {{ font-size: 17px; }}
    .featured .story--featured:nth-child(n+2) p {{ display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }}
    .section {{ padding: 34px 0 6px; scroll-margin-top: 92px; }}
    .section__rule {{ height: 3px; background: linear-gradient(90deg, var(--accent), var(--ink)); margin-bottom: 14px; }}
    .section__head {{ display: flex; align-items: baseline; justify-content: space-between; gap: 16px; }}
    .section__head span {{ color: var(--muted); font-size: 13px; }}
    .story-list {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 0 26px; margin-top: 16px; }}
    .story-list .story:first-child {{ grid-column: 1 / -1; grid-template-columns: 150px minmax(0, 1fr); }}
    .story {{ display: grid; grid-template-columns: 112px minmax(0, 1fr); gap: 16px; min-width: 0; border-top: 1px solid var(--line); padding: 15px 0; scroll-margin-top: 92px; }}
    .story--featured {{ grid-template-columns: 1fr; min-width: 0; overflow: hidden; border-top: 0; padding-top: 0; }}
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
    .story h3 {{ font-family: -apple-system, BlinkMacSystemFont, "Apple SD Gothic Neo", "Malgun Gothic", "Segoe UI", sans-serif; font-size: 18.5px; line-height: 1.34; margin: 0 0 6px; letter-spacing: 0; font-weight: 800; word-break: keep-all; overflow-wrap: break-word; text-wrap: pretty; }}
    .story h3 a {{ text-decoration-thickness: 1px; text-underline-offset: 4px; }}
    .story.is-read {{ opacity: .72; }}
    .story.is-read .story__image {{ filter: grayscale(.2); opacity: .82; }}
    .story.is-read h3 a {{ color: var(--muted); }}
    .story.is-read h3 a::after {{ content: "읽음"; display: inline-block; margin-left: 7px; border: 1px solid var(--line); border-radius: 999px; padding: 1px 5px; color: var(--muted); font-size: 10px; font-weight: 800; line-height: 1.2; vertical-align: .15em; }}
    .story--featured h3 {{ font-size: 18.5px; line-height: 1.32; }}
    .story p {{ max-width: 700px; margin: 0 0 8px; color: #3f3948; font-size: 14px; line-height: 1.58; word-break: keep-all; overflow-wrap: break-word; text-wrap: pretty; }}
    .story--featured p {{ font-size: 13.5px; line-height: 1.55; }}
    .story__insight {{ display: grid; gap: 5px; margin-top: 7px; }}
    .story__insight p {{ display: grid; grid-template-columns: 38px minmax(0, 1fr); gap: 8px; max-width: 720px; margin: 0; color: #342d3d; font-size: 13.5px; line-height: 1.48; }}
    .story__insight strong {{ color: var(--accent-deep); font-size: 11px; font-weight: 900; letter-spacing: .03em; }}
    details {{ grid-column: 1 / -1; margin-top: 10px; max-width: 100%; }}
    summary {{ cursor: pointer; color: var(--green); font-size: 13px; font-weight: 800; }}
    .link-table {{ margin-top: 10px; border: 1px solid var(--line); background: var(--surface); overflow: auto; }}
    .link-table table {{ width: 100%; min-width: 660px; table-layout: fixed; border-collapse: collapse; font-size: 12px; }}
    th, td {{ padding: 8px 9px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 700; background: #faf8fd; }}
    th:first-child, td:first-child {{ width: 92px; color: var(--muted); white-space: nowrap; }}
    th:nth-child(2), td:nth-child(2) {{ width: 120px; color: var(--accent-deep); }}
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
    .floating-nav__stories a {{ display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .floating-nav__stories a.is-read {{ color: #9a93a5; }}
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
      .brief {{ gap: 14px; padding: 18px 0; }}
      .brief h2 {{ font-size: 22px; }}
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
      .brand-row {{ align-items: flex-start; flex-direction: column; }}
      .featured {{ gap: 0; padding: 22px 0; }}
      .featured .story--featured:first-child {{ grid-row: auto; border-right: 0; padding-right: 0; }}
      .featured .story--featured:nth-child(n+2) {{ grid-template-columns: 82px minmax(0, 1fr); gap: 11px; padding: 15px 0; }}
      .section {{ padding-top: 28px; scroll-margin-top: 124px; }}
      .story-list {{ display: block; margin-top: 10px; }}
      .story-list .story:first-child {{ grid-column: auto; grid-template-columns: 82px minmax(0, 1fr); }}
      .story, .story--featured {{ grid-template-columns: 82px minmax(0, 1fr); gap: 11px; align-items: start; padding: 15px 0; }}
      .story--featured {{ border-top: 1px solid var(--line); }}
      .story--featured .story__image {{ aspect-ratio: 4 / 3; }}
      .story--featured h3, .story h3 {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; font-size: 16.5px; line-height: 1.32; font-weight: 800; margin-bottom: 6px; }}
      .story h3 a {{ display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; text-decoration: none; }}
      .story h3 a:focus-visible {{ outline: 2px solid var(--accent); outline-offset: 2px; }}
      .story p {{ display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; margin-bottom: 5px; color: #4a4353; font-size: 13.5px; line-height: 1.45; }}
      .story__insight {{ gap: 4px; }}
      .story__insight p {{ display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; padding-left: 0; font-size: 12.8px; line-height: 1.42; }}
      .story__insight strong {{ display: inline; margin-right: 6px; font-size: 10.5px; }}
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
      .link-table {{ border: 0; background: transparent; }}
      .link-table table {{ min-width: 0; }}
      table, thead, tbody, tr, th, td {{ display: block; }}
      thead {{ display: none; }}
      tr {{ display: grid; grid-template-columns: 72px minmax(0, 1fr); column-gap: 8px; row-gap: 2px; padding: 8px 0; border-bottom: 1px solid var(--line); }}
      td {{ border: 0; padding: 0; width: auto !important; }}
      .link-table__time {{ grid-column: 1; color: var(--muted); font-size: 11px; }}
      .link-table__source {{ grid-column: 2; color: var(--accent-deep); font-size: 11px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
      .link-table__title {{ grid-column: 1 / -1; font-size: 13px; line-height: 1.35; }}
      .link-table__title a {{ display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }}
      .footer__grid {{ grid-template-columns: 1fr; }}
    }}
    {variant_css}
  </style>
</head>
<body id="top" class="{escape(variant_class, quote=True)}">
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
        <button class="archive-trigger" type="button" data-archive-toggle aria-expanded="false" aria-controls="archive-panel">다른 일자 보기</button>
      </div>
      <div class="variant-switcher" aria-label="레이아웃 실험 버전">
        <div class="variant-switcher__title">Layout Lab</div>
        <div class="variant-switcher__links">{variant_links_html}</div>
      </div>
    </header>

    <section class="brief">
      <h2>{brief_title}</h2>
      <div>{review_block_html}</div>
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

    document.querySelectorAll('.story__image').forEach((container) => {{
      attachSourceLogoGuard(container);
      const image = container.querySelector('img:not(.story__source-logo)');
      if (!image) return;
      const markBroken = () => replaceWithSourceLogo(container);
      const fallbackTimer = window.setTimeout(() => {{
        if (!image.complete || image.naturalWidth === 0) markBroken();
      }}, 8000);
      image.addEventListener('load', () => window.clearTimeout(fallbackTimer), {{ once: true }});
      image.addEventListener('error', markBroken, {{ once: true }});
      if (image.complete && image.naturalWidth === 0) markBroken();
    }});

    const sections = Array.from(document.querySelectorAll('[data-section]'));
    const sectionStories = Array.from(document.querySelectorAll('[data-story][data-section-key]'));
    const categoryLinks = Array.from(document.querySelectorAll('[data-toc-section], [data-nav-section]'));
    const desktopStoryLinks = Array.from(document.querySelectorAll('[data-nav-story]'));
    const mobileStoryLinks = Array.from(document.querySelectorAll('[data-mobile-nav-story]'));
    const storyLinks = [...desktopStoryLinks, ...mobileStoryLinks];
    const mobileSectionLabel = document.querySelector('[data-mobile-section-label]');
    const mobileProgress = document.querySelector('[data-mobile-progress]');
    const archivePanel = document.querySelector('[data-archive-panel]');
    const archiveToggles = Array.from(document.querySelectorAll('[data-archive-toggle]'));
    const archiveClose = document.querySelector('[data-archive-close]');
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
      let activeIndex = links.findIndex((link) => link.getAttribute('href') === activeStoryId);
      if (activeIndex < 0) activeIndex = 0;
      links.forEach((link, index) => {{
        const isNear = Math.abs(index - activeIndex) <= 4;
        link.classList.toggle('is-near-active', isNear);
      }});
    }}

    function updateMobileStoryContext(activeStoryId) {{
      if (!mobileStoryLinks.length) return;
      let activeIndex = mobileStoryLinks.findIndex((link) => link.getAttribute('href') === activeStoryId);
      if (activeIndex < 0) activeIndex = 0;
      const contextLabels = new Map([
        [activeIndex - 1, '이전'],
        [activeIndex, '현재'],
        [activeIndex + 1, '다음'],
      ]);
      mobileStoryLinks.forEach((link, index) => {{
        const label = contextLabels.get(index) || '';
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

    function updateNavigation() {{
      if (!sections.length) return;
      const marker = window.scrollY + Math.min(220, window.innerHeight * 0.34);
      let activeSection = sections[0];
      sections.forEach((section) => {{
        if (section.offsetTop <= marker) activeSection = section;
      }});
      const activeSectionId = activeSection.id;
      const activeStories = sectionStories.filter((story) => story.dataset.sectionKey === activeSectionId);
      let activeStory = activeStories[0] || null;
      activeStories.forEach((story) => {{
        if (story.offsetTop <= marker) activeStory = story;
      }});
      const total = Number(activeSection.dataset.sectionCount || activeStory?.dataset.sectionTotal || activeStories.length || 0);
      const index = activeStory ? Number(activeStory.dataset.sectionIndex || 0) : 0;
      const activeSectionLabel = activeSection.dataset.sectionLabel || '';

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
    function requestNavigationUpdate() {{
      if (navTicking) return;
      navTicking = true;
      window.requestAnimationFrame(() => {{
        updateNavigation();
        navTicking = false;
      }});
    }}
    window.addEventListener('scroll', requestNavigationUpdate, {{ passive: true }});
    window.addEventListener('resize', requestNavigationUpdate);
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
    updateNavigation();
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
    return {
        "config": config,
        "date_id": date_id,
        "start_at": start_at,
        "end_at": end_at,
        "stories": stories,
        "review": review,
        "html": html,
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
    dated_path.write_text(html, encoding="utf-8", newline="\n")
    latest_path.write_text(html, encoding="utf-8", newline="\n")
    variant_dir = feed_dir / "variants"
    variant_dir.mkdir(parents=True, exist_ok=True)
    variant_paths: list[Path] = []
    expected_variant_files = {f'{variant["slug"]}.html' for variant in LAYOUT_VARIANTS}
    for stale_path in variant_dir.glob("*.html"):
        if stale_path.name not in expected_variant_files:
            stale_path.unlink()
    for variant in LAYOUT_VARIANTS:
        variant_slug = str(variant["slug"])
        variant_html = render_report_html(
            list(report.get("stories") or []),
            str(report.get("review") or ""),
            dict(report.get("config") or {}),
            report["start_at"],  # type: ignore[arg-type]
            report["end_at"],  # type: ignore[arg-type]
            date_id,
            str(report.get("report_url") or ""),
            list(report.get("duplicate_records") or []),
            list(report.get("clusters") or []),
            render_report_archive_links(feed_dir, date_id, link_prefix="../"),
            variant_slug,
            True,
        )
        variant_path = variant_dir / f"{variant_slug}.html"
        variant_path.write_text(normalize_generated_html(variant_html), encoding="utf-8", newline="\n")
        variant_paths.append(variant_path)
    index_path.write_text(render_report_index(feed_dir), encoding="utf-8", newline="\n")
    return [dated_path, latest_path, index_path, *variant_paths]


def render_report_archive_links(feed_dir: Path, current_date_id: str, *, link_prefix: str = "", max_items: int = 20) -> str:
    date_ids = {current_date_id}
    if feed_dir.exists():
        date_ids.update(
            path.stem
            for path in feed_dir.glob("*.html")
            if path.name not in {"latest.html", "index.html"} and path.stem
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


def render_report_index(feed_dir: Path) -> str:
    feed_files = sorted(
        [
            path
            for path in feed_dir.glob("*.html")
            if path.name not in {"latest.html", "index.html"}
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
    config = report["config"] if isinstance(report.get("config"), dict) else load_config(project_root / "config.yaml")
    if daily_report_write_only():
        return {"daily_report_written": 1, "daily_report_sent": 0, "daily_report_failed": 0}
    if not telegram_is_configured(config):
        return {"daily_report_written": 1, "daily_report_sent": 0, "daily_report_failed": 0}
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
    }


def main() -> None:
    summary = send_daily_report()
    print(
        "Daily report finished: "
        + ", ".join(f"{key}={value}" for key, value in summary.items())
    )


if __name__ == "__main__":
    main()
