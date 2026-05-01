from __future__ import annotations

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
BSIDE_LOGO_URL = "https://bside.ai/images/icons/bside-logo-gray.svg"


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
        f'<img class="bside-logo__image" src="{BSIDE_LOGO_URL}" alt="BSIDE" loading="lazy" decoding="async">'
        '<span class="bside-logo__label">DAILY NEWS</span>'
        '</a>'
    )


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
) -> str:
    links = story.get("links") if isinstance(story.get("links"), list) else []
    story_id = escape(str(story.get("id") or slugify(story.get("title"), "story")), quote=True)
    safe_title = escape(str(story.get("title") or "제목 없음"))
    primary_url = escape(str(story.get("primary_url") or "#"), quote=True)
    category = escape(str(story.get("category") or "기타"))
    sources = escape(str(story.get("source_line") or story.get("primary_source") or ""))
    summary = escape(story_summary_for_display(story))
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
) -> str:
    stats = report_stats(stories, clusters, duplicate_records)
    buckets = category_buckets(stories)
    review_bullets = clean_report_bullets(review) or clean_report_bullets(fallback_report_review(stories))
    review_html = "\n".join(f"<li>{escape(bullet)}</li>" for bullet in review_bullets)
    review_block_html = f'<ul class="brief__bullets">{review_html}</ul>' if review_html else ""
    featured_stories = stories[:3]
    featured_html = "\n".join(render_story(story, config, featured=True, show_details=False) for story in featured_stories)
    category_sections = []
    for category in REPORT_CATEGORY_ORDER:
        category_stories = buckets.get(category, [])
        if not category_stories:
            continue
        section_id = slugify(category, "section")
        category_sections.append(
            f"""
        <section class="section" id="{escape(section_id, quote=True)}" data-section data-section-count="{len(category_stories)}">
          <div class="section__rule"></div>
          <div class="section__head">
            <h2>{escape(category)}</h2>
            <span>{len(category_stories)}개 이슈</span>
          </div>
          <div class="story-list">
            {''.join(render_story(story, config, section_id=section_id, section_index=index, section_total=len(category_stories)) for index, story in enumerate(category_stories, start=1))}
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
    start_label = escape(format_kst(start_at, str(config.get("timezone") or "Asia/Seoul")))
    end_label = escape(format_kst(end_at, str(config.get("timezone") or "Asia/Seoul")))
    archive_url = "index.html"
    title = f"비사이드 자본시장 데일리 - {date_id}"
    description = compact_text(" ".join(review_bullets), max_chars=180)
    canonical_url = escape(report_url, quote=True)
    header_logo = bside_logo_html("bside-logo--top")
    footer_logo = bside_logo_html("bside-logo--footer")
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
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.58;
    }}
    a {{ color: inherit; text-decoration-thickness: 1px; text-underline-offset: 3px; }}
    .page {{ max-width: 1160px; margin: 0 auto; padding: 24px 24px 72px; }}
    .masthead {{ border-bottom: 2px solid var(--ink); padding-bottom: 22px; }}
    .brand-row {{ display: flex; justify-content: space-between; gap: 16px; align-items: baseline; border-bottom: 1px solid var(--line); padding-bottom: 10px; margin-bottom: 24px; }}
    .bside-logo {{ display: inline-flex; align-items: center; gap: 9px; color: var(--accent); text-decoration: none; }}
    .bside-logo__image {{ width: 86px; height: auto; display: block; }}
    .bside-logo__label {{ font-size: 11px; font-weight: 900; letter-spacing: .12em; color: var(--accent); }}
    .bside-logo:hover .bside-logo__label {{ color: var(--accent-deep); }}
    .bside-logo--top .bside-logo__image {{ width: 92px; }}
    .bside-logo--footer {{ margin-bottom: 10px; }}
    .edition {{ color: var(--muted); font-size: 13px; }}
    h1 {{ font-family: Georgia, "Times New Roman", serif; font-size: clamp(40px, 7vw, 78px); line-height: .96; letter-spacing: 0; margin: 0 0 16px; max-width: 940px; }}
    .dek {{ max-width: 1080px; color: #322b3d; font-size: 18px; margin: 0; text-wrap: pretty; }}
    .meta-strip {{ display: flex; flex-wrap: wrap; gap: 10px 18px; margin-top: 20px; color: var(--muted); font-size: 13px; }}
    .meta-strip strong {{ color: var(--accent-deep); }}
    .brief {{ display: grid; grid-template-columns: 220px 1fr; gap: 30px; border-bottom: 1px solid var(--ink); padding: 34px 0; }}
    .brief h2, .section h2 {{ font-family: Georgia, "Times New Roman", serif; font-size: 28px; line-height: 1.1; margin: 0; }}
    .brief__bullets {{ margin: 0; padding: 0; list-style: none; display: grid; gap: 10px; }}
    .brief__bullets li {{ position: relative; padding-left: 18px; font-size: 16px; color: #2e2738; }}
    .brief__bullets li::before {{ content: ""; position: absolute; left: 0; top: .72em; width: 6px; height: 6px; border-radius: 50%; background: var(--accent); }}
    .toc {{ position: sticky; top: 0; z-index: 5; display: flex; flex-wrap: wrap; gap: 10px; padding: 14px 0; border-bottom: 1px solid var(--line); background: color-mix(in srgb, var(--paper) 92%, transparent); backdrop-filter: blur(8px); }}
    .chip {{ --progress: 0; position: relative; overflow: hidden; display: inline-flex; align-items: center; gap: 8px; border: 1px solid var(--line); border-radius: 999px; padding: 7px 12px; background: var(--surface); text-decoration: none; font-size: 13px; transition: border-color .18s ease, background .18s ease, color .18s ease; }}
    .chip::after {{ content: ""; position: absolute; left: 0; right: auto; bottom: 0; height: 3px; width: calc(var(--progress, 0) * 100%); background: var(--accent); transition: width .18s ease; }}
    .chip__progress {{ color: var(--accent); font-weight: 800; font-variant-numeric: tabular-nums; }}
    .chip.is-active {{ border-color: var(--accent); background: var(--accent-soft); color: var(--accent-deep); }}
    .featured {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 24px; border-bottom: 1px solid var(--ink); padding: 30px 0; align-items: start; }}
    .section {{ padding: 34px 0 6px; }}
    .section__rule {{ height: 3px; background: linear-gradient(90deg, var(--accent), var(--ink)); margin-bottom: 14px; }}
    .section__head {{ display: flex; align-items: baseline; justify-content: space-between; gap: 16px; }}
    .section__head span {{ color: var(--muted); font-size: 13px; }}
    .story-list {{ margin-top: 18px; }}
    .story {{ display: grid; grid-template-columns: 128px minmax(0, 1fr); gap: 18px; min-width: 0; border-top: 1px solid var(--line); padding: 18px 0; scroll-margin-top: 92px; }}
    .story--featured {{ grid-template-columns: 1fr; min-width: 0; overflow: hidden; border-top: 0; padding-top: 0; }}
    .story__body {{ min-width: 0; }}
    .story__image {{ display: block; aspect-ratio: 4 / 3; background: var(--accent-soft); overflow: hidden; border: 1px solid var(--line); }}
    .story__image img {{ width: 100%; height: 100%; object-fit: cover; display: block; }}
    .story__image--empty {{ display: grid; place-items: center; color: var(--accent); font-size: 12px; font-weight: 800; letter-spacing: .08em; }}
    .story__image--logo {{ display: grid; place-items: center; justify-items: center; gap: 8px; padding: 14px; text-align: center; text-decoration: none; color: var(--accent-deep); background: linear-gradient(135deg, #f4efff, #ffffff); }}
    .story__image--logo span {{ font-size: 12px; font-weight: 900; letter-spacing: .02em; line-height: 1.2; overflow-wrap: anywhere; }}
    .story__source-logo {{ width: 42px !important; height: 42px !important; object-fit: contain !important; border-radius: 10px; background: #fff; padding: 6px; box-shadow: 0 4px 14px rgba(44, 27, 84, .10); }}
    .story--featured .story__image {{ aspect-ratio: 16 / 9; }}
    .story__meta {{ display: flex; flex-wrap: wrap; gap: 8px; color: var(--muted); font-size: 12px; margin-bottom: 8px; }}
    .story__meta span:not(:last-child)::after {{ content: "·"; margin-left: 8px; color: var(--line); }}
    .story__sources a {{ margin-right: 8px; white-space: nowrap; color: var(--accent-deep); }}
    .story__sources em {{ font-style: normal; color: var(--muted); white-space: nowrap; }}
    .story h3 {{ font-family: Georgia, "Times New Roman", serif; font-size: 24px; line-height: 1.16; margin: 0 0 8px; letter-spacing: 0; }}
    .story--featured h3 {{ font-size: 24px; }}
    .story p {{ margin: 0 0 10px; color: #34312d; }}
    details {{ grid-column: 1 / -1; margin-top: 10px; max-width: 100%; }}
    summary {{ cursor: pointer; color: var(--green); font-size: 13px; font-weight: 800; }}
    .link-table {{ margin-top: 10px; border: 1px solid var(--line); background: var(--surface); overflow: auto; }}
    .link-table table {{ width: 100%; min-width: 660px; table-layout: fixed; border-collapse: collapse; font-size: 12px; }}
    th, td {{ padding: 8px 9px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 700; background: #faf8fd; }}
    th:first-child, td:first-child {{ width: 92px; color: var(--muted); white-space: nowrap; }}
    th:nth-child(2), td:nth-child(2) {{ width: 120px; color: var(--accent-deep); }}
    td a {{ overflow-wrap: anywhere; }}
    .floating-nav {{ position: fixed; top: 92px; right: 20px; z-index: 8; width: 246px; max-height: calc(100vh - 118px); overflow: auto; border: 1px solid var(--line); background: rgba(255,255,255,.94); box-shadow: 0 14px 40px rgba(44, 27, 84, .10); padding: 12px; }}
    .floating-nav h2 {{ font-size: 12px; margin: 0 0 8px; color: var(--accent-deep); letter-spacing: .04em; }}
    .floating-nav a {{ display: flex; align-items: baseline; justify-content: space-between; gap: 10px; text-decoration: none; border-left: 2px solid transparent; padding: 6px 8px; color: var(--muted); font-size: 12px; transition: border-color .18s ease, background .18s ease, color .18s ease; }}
    .floating-nav .nav-label {{ min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .floating-nav .nav-progress {{ flex: 0 0 auto; color: var(--accent); font-weight: 800; font-variant-numeric: tabular-nums; }}
    .floating-nav a.is-active {{ border-left-color: var(--accent); color: var(--ink); background: var(--accent-soft); }}
    .floating-nav__stories {{ margin-top: 12px; padding-top: 10px; border-top: 1px solid var(--line); }}
    .floating-nav__stories a {{ display: block; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
    .floating-nav__stories a:not(.is-near-active) {{ display: none; }}
    .top-button {{ position: fixed; right: 22px; bottom: 24px; z-index: 9; width: 42px; height: 42px; border-radius: 50%; display: grid; place-items: center; color: #fff; background: var(--accent); text-decoration: none; box-shadow: 0 12px 28px rgba(76, 38, 156, .26); }}
    .footer {{ margin-top: 48px; border-top: 2px solid var(--ink); padding-top: 20px; color: var(--muted); font-size: 13px; }}
    .footer__brand {{ color: var(--accent); font-weight: 900; letter-spacing: .06em; }}
    .footer__grid {{ display: grid; grid-template-columns: 1.4fr 1fr; gap: 22px; }}
    @media (max-width: 1480px) {{
      .floating-nav {{ display: none; }}
    }}
    @media (max-width: 860px) {{
      .page {{ padding: 18px 16px 48px; }}
      .brief, .featured {{ grid-template-columns: 1fr; }}
      .brand-row {{ align-items: flex-start; flex-direction: column; }}
      .story {{ grid-template-columns: 96px minmax(0, 1fr); gap: 12px; }}
      .story--featured {{ grid-template-columns: 1fr; }}
      .story--featured h3, .story h3 {{ font-size: 19px; line-height: 1.22; }}
      .story p {{ display: -webkit-box; -webkit-line-clamp: 3; -webkit-box-orient: vertical; overflow: hidden; margin-bottom: 8px; }}
      .story__meta {{ font-size: 11px; }}
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
  </style>
</head>
<body id="top">
  <aside class="floating-nav" aria-label="데일리 네비게이션">
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
        <a href="{archive_url}">다른 일자 보기</a>
      </div>
    </header>

    <section class="brief">
      <h2>Editor’s Brief</h2>
      <div>{review_block_html}</div>
    </section>

    <nav class="toc" aria-label="report sections">
      {toc}
    </nav>

    <section class="featured" aria-label="top stories">
      {featured_html}
    </section>

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
    const storyLinks = Array.from(document.querySelectorAll('[data-nav-story]'));

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

    function updateStoryWindow(activeStoryId) {{
      if (!storyLinks.length) return;
      let activeIndex = storyLinks.findIndex((link) => link.getAttribute('href') === activeStoryId);
      if (activeIndex < 0) activeIndex = 0;
      storyLinks.forEach((link, index) => {{
        const isNear = Math.abs(index - activeIndex) <= 4;
        link.classList.toggle('is-near-active', isNear);
      }});
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

      categoryLinks.forEach((link) => {{
        const isActive = sectionIdForLink(link) === activeSectionId;
        link.classList.toggle('is-active', isActive);
        if (!isActive) setSectionProgress(sectionIdForLink(link), 0, Number(link.querySelector('[data-progress-text]')?.textContent?.split('/')[1] || 0));
      }});
      setSectionProgress(activeSectionId, index, total);

      const activeStoryId = activeStory ? `#${{activeStory.id}}` : '';
      storyLinks.forEach((link) => {{
        link.classList.toggle('is-active', link.getAttribute('href') === activeStoryId);
      }});
      updateStoryWindow(activeStoryId);
    }}

    let navTicking = false;
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
    review = generate_report_review(clusters, stories, config, start_at, end_at)
    date_id = end_at.astimezone(ZoneInfo(timezone_name)).strftime("%Y-%m-%d")
    report_url = report_public_url(config, date_id)
    html = render_report_html(stories, review, config, start_at, end_at, date_id, report_url, duplicate_records, clusters)
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
    }


def write_report_files(report: dict[str, object], root: Path | None = None) -> list[Path]:
    project_root = root or PROJECT_ROOT
    date_id = str(report["date_id"])
    feed_dir = project_root / FEED_DIR
    feed_dir.mkdir(parents=True, exist_ok=True)
    html = str(report["html"])
    dated_path = feed_dir / f"{date_id}.html"
    latest_path = feed_dir / "latest.html"
    index_path = feed_dir / "index.html"
    dated_path.write_text(html, encoding="utf-8")
    latest_path.write_text(html, encoding="utf-8")
    index_path.write_text(render_report_index(feed_dir), encoding="utf-8")
    return [dated_path, latest_path, index_path]


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
    .bside-logo__image {{ width:92px; height:auto; display:block; }}
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
    review = str(report.get("review") or "")
    stories = report.get("stories") if isinstance(report.get("stories"), list) else []
    bullets = clean_report_bullets(review, max_bullets=3)
    if not bullets:
        bullets = clean_report_bullets(fallback_report_review(stories), max_bullets=3)
    report_url = str(report.get("report_url") or "")
    link_label = report_link_label(report)
    lines = [f"<b>{escape(link_label)}</b>"]
    for bullet in bullets[:3]:
        lines.append(f"• {escape(bullet)}")
    if stories:
        lines.append("")
        lines.append("<b>주요 기사</b>")
        buckets = category_buckets([story for story in stories if isinstance(story, dict)])
        for category in REPORT_CATEGORY_ORDER:
            category_stories = buckets.get(category, [])[:4]
            if not category_stories:
                continue
            lines.append(f"<b>{escape(category)}</b>")
            for story in category_stories:
                lines.append(f"• {escape(telegram_story_title(story))}")
            lines.append("")
    lines.append(f"자세한 기사는 {html_link(link_label, report_url)}에서 확인하세요.")
    return "\n".join(lines).strip()


def send_daily_report(root: Path | None = None) -> dict[str, int]:
    project_root = root or PROJECT_ROOT
    report = build_daily_report(project_root)
    write_report_files(report, project_root)
    config = report["config"] if isinstance(report.get("config"), dict) else load_config(project_root / "config.yaml")
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
