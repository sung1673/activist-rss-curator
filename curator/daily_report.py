from __future__ import annotations

import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from html import escape
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from .ai import ai_config, call_github_models
from .config import load_config
from .dates import format_kst, now_in_timezone
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
REPORTS_DIR = Path("public") / "reports"
REPORT_CATEGORY_ORDER = [
    "주주행동·경영권",
    "밸류업·주주환원",
    "자본시장 제도·공시",
    "해외·영문",
    "기타",
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
        return f"reports/{date_id}.html"
    return f"{base_url}/reports/{date_id}.html"


def article_domain(url: str) -> str:
    hostname = (urlsplit(url).hostname or "").lower().removeprefix("www.")
    return hostname or "source"


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


def story_source_line(links: list[dict[str, str]]) -> str:
    counter = Counter(link["source"] for link in links if link.get("source"))
    return " · ".join(source for source, _count in counter.most_common(4))


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
            "지난 24시간 자본시장 뉴스의 축은 경영 의사결정과 일반주주 보호를 둘러싼 감시 강화였습니다. "
            + " / ".join(compact_text(title, max_chars=42) for title in lead_titles[:3])
            + " 등은 이사회 책임과 주주권 행사 기준이 다시 쟁점화되고 있음을 보여줍니다."
        )
    if valueup:
        paragraphs.append(
            "밸류업과 주주환원 보도는 자사주 소각, 배당 확대, 저평가 해소 계획으로 모였습니다. "
            "시장은 단순 환원 규모보다 공시의 구체성, 지속 가능성, 자본배분 원칙을 함께 확인하려는 분위기입니다."
        )
    if capital:
        paragraphs.append(
            "제도·공시 영역에서는 금융당국의 정정요구, 상장폐지·개선기간, 공개매수 관련 논의가 이어졌습니다. "
            "이는 발행시장과 유통시장에서 투자자에게 제공되는 정보의 품질을 더 엄격히 보겠다는 흐름으로 해석됩니다."
        )
    if global_titles:
        paragraphs.append(
            "영문·외신에서는 Korea discount, shareholder returns, activist pressure가 반복적으로 포착됐습니다. "
            "국내 투자자 관점에서는 해외 행동주의 사례와 한국 시장을 바라보는 외국인 투자자의 평가를 함께 추적할 필요가 있습니다."
        )
    if not paragraphs:
        paragraphs.append("지난 24시간에는 발행 가능한 기사 묶음이 많지 않았습니다. 새로 잡히는 주주행동과 자본시장 제도 흐름을 계속 확인합니다.")
    return "\n\n".join(paragraphs[:4])


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
        "수집된 기사 묶음을 바탕으로 하루치 브리핑의 본문 해설만 한국어 기사체로 작성합니다. "
        "투자 조언이나 매매 권유는 하지 말고, 기사에 없는 사실을 단정하지 마세요."
    )
    user_prompt = (
        "아래 기사 묶음을 바탕으로 Telegram과 HTML 리포트 상단에 들어갈 상세 요약을 작성하세요.\n"
        "- 정확히 4개 문단으로 작성\n"
        "- bullet point, 번호, 제목 없이 문단만 작성\n"
        "- 각 문단은 2문장 안팎, 180자 안팎\n"
        "- 전체 흐름, 주요 사건, 제도/정책적 의미, 해외/영문 흐름을 균형 있게 반영\n"
        "- 전문 자본시장 기자의 톤으로, 정책·공시·주주권 의미를 해석하되 과장하지 않음\n"
        "- '주목됩니다', '필요가 있습니다' 같은 일반 논평보다 기사체 문장 사용\n"
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
    if content and len(clean_report_paragraphs(content)) >= 2:
        return "\n\n".join(clean_report_paragraphs(content))
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
                f"<td>{published}</td>"
                f"<td>{source}</td>"
                f'<td><a href="{url}">{title}</a></td>'
                "</tr>"
            )
    return " ".join(items) if compact else "\n".join(items)


def render_story(story: dict[str, object], config: dict[str, object], *, featured: bool = False) -> str:
    links = story.get("links") if isinstance(story.get("links"), list) else []
    story_id = escape(str(story.get("id") or slugify(story.get("title"), "story")), quote=True)
    safe_title = escape(str(story.get("title") or "제목 없음"))
    primary_url = escape(str(story.get("primary_url") or "#"), quote=True)
    category = escape(str(story.get("category") or "기타"))
    sources = escape(str(story.get("source_line") or story.get("primary_source") or ""))
    summary = escape(str(story.get("summary") or "관련 보도를 묶어 원문 링크와 함께 정리했습니다."))
    timestamp = escape(date_label(story.get("datetime"), config))
    image_url = escape(str(story.get("image_url") or ""), quote=True)
    image_html = (
        f'<a class="story__image" href="{primary_url}" aria-label="기사 이미지 보기"><img src="{image_url}" alt="" loading="lazy"></a>'
        if image_url
        else '<div class="story__image story__image--empty" aria-hidden="true"><span>BSIDE</span></div>'
    )
    normalized_links = [link for link in links if isinstance(link, dict)]
    more_links = render_link_list(normalized_links, config, compact=True)
    detail_links = render_link_list(normalized_links, config, compact=False)
    featured_class = " story--featured" if featured else ""
    return f"""
          <article class="story{featured_class}" id="{story_id}" data-story>
            {image_html}
            <div class="story__body">
              <div class="story__meta"><span>{category}</span><span>{timestamp}</span><span>{sources}</span></div>
              <h3><a href="{primary_url}">{safe_title}</a></h3>
              <p>{summary}</p>
              <div class="story__more"><strong>More:</strong> {more_links}</div>
            </div>
            <details>
              <summary>기사 링크 {len(links)}건 보기</summary>
              <div class="link-table">
                <table>
                  <thead><tr><th>일시</th><th>매체</th><th>기사</th></tr></thead>
                  <tbody>{detail_links}</tbody>
                </table>
              </div>
            </details>
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
    review_paragraphs = clean_report_paragraphs(review) or clean_report_paragraphs(fallback_report_review(stories))
    review_html = "\n".join(f"<p>{escape(paragraph)}</p>" for paragraph in review_paragraphs)
    featured_stories = stories[:3]
    featured_html = "\n".join(render_story(story, config, featured=True) for story in featured_stories)
    category_sections = []
    for category in REPORT_CATEGORY_ORDER:
        category_stories = buckets.get(category, [])
        if not category_stories:
            continue
        section_id = slugify(category, "section")
        category_sections.append(
            f"""
        <section class="section" id="{escape(section_id, quote=True)}" data-section>
          <div class="section__rule"></div>
          <div class="section__head">
            <h2>{escape(category)}</h2>
            <span>{len(category_stories)}개 이슈</span>
          </div>
          <div class="story-list">
            {''.join(render_story(story, config) for story in category_stories)}
          </div>
        </section>
            """
        )
    toc = "\n".join(
        f'<a class="chip" href="#{escape(slugify(category, "section"), quote=True)}">{escape(category)} <span>{len(buckets.get(category, []))}</span></a>'
        for category in REPORT_CATEGORY_ORDER
        if buckets.get(category)
    )
    side_category_links = "\n".join(
        f'<a data-nav-section href="#{escape(slugify(category, "section"), quote=True)}">{escape(category)} <span>{len(buckets.get(category, []))}</span></a>'
        for category in REPORT_CATEGORY_ORDER
        if buckets.get(category)
    )
    side_story_links = "\n".join(
        f'<a data-nav-story href="#{escape(str(story.get("id") or ""), quote=True)}">{escape(compact_text(str(story.get("title") or ""), max_chars=44))}</a>'
        for story in stories
    )
    start_label = escape(format_kst(start_at, str(config.get("timezone") or "Asia/Seoul")))
    end_label = escape(format_kst(end_at, str(config.get("timezone") or "Asia/Seoul")))
    archive_url = "index.html"
    title = f"비사이드 자본시장 데일리 - {date_id}"
    description = compact_text(" ".join(review_paragraphs), max_chars=180)
    canonical_url = escape(report_url, quote=True)
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
    .brand {{ font-size: 14px; font-weight: 800; letter-spacing: .08em; color: var(--accent); }}
    .edition {{ color: var(--muted); font-size: 13px; }}
    h1 {{ font-family: Georgia, "Times New Roman", serif; font-size: clamp(40px, 7vw, 78px); line-height: .96; letter-spacing: 0; margin: 0 0 16px; max-width: 940px; }}
    .dek {{ max-width: 760px; color: #322b3d; font-size: 18px; margin: 0; }}
    .meta-strip {{ display: flex; flex-wrap: wrap; gap: 10px 18px; margin-top: 20px; color: var(--muted); font-size: 13px; }}
    .meta-strip strong {{ color: var(--accent-deep); }}
    .brief {{ display: grid; grid-template-columns: 220px 1fr; gap: 30px; border-bottom: 1px solid var(--ink); padding: 34px 0; }}
    .brief h2, .section h2 {{ font-family: Georgia, "Times New Roman", serif; font-size: 28px; line-height: 1.1; margin: 0; }}
    .brief p {{ margin: 0 0 15px; font-size: 17px; color: #2e2738; }}
    .toc {{ position: sticky; top: 0; z-index: 5; display: flex; flex-wrap: wrap; gap: 10px; padding: 14px 0; border-bottom: 1px solid var(--line); background: color-mix(in srgb, var(--paper) 92%, transparent); backdrop-filter: blur(8px); }}
    .chip {{ border: 1px solid var(--line); border-radius: 999px; padding: 7px 12px; background: var(--surface); text-decoration: none; font-size: 13px; }}
    .chip span {{ color: var(--accent); font-weight: 800; margin-left: 4px; }}
    .featured {{ display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 24px; border-bottom: 1px solid var(--ink); padding: 30px 0; align-items: start; }}
    .section {{ padding: 34px 0 6px; }}
    .section__rule {{ height: 3px; background: linear-gradient(90deg, var(--accent), var(--ink)); margin-bottom: 14px; }}
    .section__head {{ display: flex; align-items: baseline; justify-content: space-between; gap: 16px; }}
    .section__head span {{ color: var(--muted); font-size: 13px; }}
    .story-list {{ margin-top: 18px; }}
    .story {{ display: grid; grid-template-columns: 128px minmax(0, 1fr); gap: 18px; border-top: 1px solid var(--line); padding: 18px 0; scroll-margin-top: 92px; }}
    .story--featured {{ grid-template-columns: 1fr; border-top: 0; padding-top: 0; }}
    .story__image {{ display: block; aspect-ratio: 4 / 3; background: var(--accent-soft); overflow: hidden; border: 1px solid var(--line); }}
    .story__image img {{ width: 100%; height: 100%; object-fit: cover; display: block; }}
    .story__image--empty {{ display: grid; place-items: center; color: var(--accent); font-size: 12px; font-weight: 800; letter-spacing: .08em; }}
    .story--featured .story__image {{ aspect-ratio: 16 / 9; }}
    .story__meta {{ display: flex; flex-wrap: wrap; gap: 8px; color: var(--muted); font-size: 12px; margin-bottom: 8px; }}
    .story__meta span:not(:last-child)::after {{ content: "·"; margin-left: 8px; color: var(--line); }}
    .story h3 {{ font-family: Georgia, "Times New Roman", serif; font-size: 24px; line-height: 1.16; margin: 0 0 8px; letter-spacing: 0; }}
    .story--featured:first-child h3 {{ font-size: 32px; }}
    .story p {{ margin: 0 0 10px; color: #34312d; }}
    .story__more {{ font-size: 13px; color: var(--muted); }}
    .story__more strong {{ color: var(--green); }}
    .story__more a {{ margin-right: 8px; white-space: nowrap; }}
    details {{ margin-top: 10px; }}
    summary {{ cursor: pointer; color: var(--green); font-size: 13px; font-weight: 800; }}
    .link-table {{ margin-top: 10px; border: 1px solid var(--line); background: var(--surface); overflow: auto; }}
    table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
    th, td {{ padding: 8px 9px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; }}
    th {{ color: var(--muted); font-weight: 700; background: #faf8fd; }}
    td:first-child {{ width: 82px; color: var(--muted); white-space: nowrap; }}
    td:nth-child(2) {{ width: 92px; color: var(--accent-deep); }}
    .floating-nav {{ position: fixed; top: 92px; right: 20px; z-index: 8; width: 230px; max-height: calc(100vh - 118px); overflow: auto; border: 1px solid var(--line); background: rgba(255,255,255,.94); box-shadow: 0 14px 40px rgba(44, 27, 84, .10); padding: 12px; }}
    .floating-nav h2 {{ font-size: 12px; margin: 0 0 8px; color: var(--accent-deep); letter-spacing: .04em; }}
    .floating-nav a {{ display: block; text-decoration: none; border-left: 2px solid transparent; padding: 6px 8px; color: var(--muted); font-size: 12px; }}
    .floating-nav a span {{ color: var(--accent); font-weight: 800; float: right; }}
    .floating-nav a.is-active {{ border-left-color: var(--accent); color: var(--ink); background: var(--accent-soft); }}
    .floating-nav__stories {{ margin-top: 12px; padding-top: 10px; border-top: 1px solid var(--line); }}
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
      .story--featured:first-child h3, .story h3 {{ font-size: 22px; }}
      .story__meta {{ font-size: 11px; }}
      table, thead, tbody, tr, th, td {{ display: block; }}
      thead {{ display: none; }}
      tr {{ padding: 8px 0; border-bottom: 1px solid var(--line); }}
      td {{ border: 0; padding: 2px 8px; width: auto !important; }}
      .footer__grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body id="top">
  <aside class="floating-nav" aria-label="리포트 네비게이션">
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
        <div class="brand">BSIDE KOREA DAILY NEWS</div>
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
      <div>{review_html}</div>
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
          <p class="footer__brand">BSIDE KOREA</p>
          <p>건강한 자본시장을 위한 주주행동과 투자자 커뮤니케이션을 지향합니다. 이 페이지는 공개 뉴스와 RSS를 자동으로 큐레이션한 리포트이며 투자 조언이나 매매 권유가 아닙니다.</p>
        </div>
        <div>
          <p>문의: support@bside.ai</p>
          <p>원문 기사와 이미지는 각 언론사와 발행자에게 권리가 있습니다.</p>
        </div>
      </div>
    </footer>
  </div>
  <script>
    const observer = new IntersectionObserver((entries) => {{
      entries.forEach((entry) => {{
        if (!entry.isIntersecting) return;
        const id = entry.target.id;
        document.querySelectorAll('[data-nav-story], [data-nav-section]').forEach((link) => {{
          link.classList.toggle('is-active', link.getAttribute('href') === '#' + id);
        }});
      }});
    }}, {{ rootMargin: '-20% 0px -70% 0px', threshold: 0.01 }});
    document.querySelectorAll('[data-story], [data-section]').forEach((element) => observer.observe(element));
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
    reports_dir = project_root / REPORTS_DIR
    reports_dir.mkdir(parents=True, exist_ok=True)
    html = str(report["html"])
    dated_path = reports_dir / f"{date_id}.html"
    latest_path = reports_dir / "latest.html"
    index_path = reports_dir / "index.html"
    dated_path.write_text(html, encoding="utf-8")
    latest_path.write_text(html, encoding="utf-8")
    index_path.write_text(render_report_index(reports_dir), encoding="utf-8")
    return [dated_path, latest_path, index_path]


def render_report_index(reports_dir: Path) -> str:
    report_files = sorted(
        [
            path
            for path in reports_dir.glob("*.html")
            if path.name not in {"latest.html", "index.html"}
        ],
        reverse=True,
    )
    links = "\n".join(
        f'<li><a href="{escape(path.name, quote=True)}">{escape(path.stem)}</a></li>'
        for path in report_files
    )
    if not links:
        links = "<li>아직 발행된 리포트가 없습니다.</li>"
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
    .brand {{ color:var(--accent); font-weight:900; letter-spacing:.08em; font-size:13px; border-bottom:1px solid var(--line); padding-bottom:12px; }}
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
    <div class="brand">BSIDE KOREA DAILY NEWS</div>
    <h1>리포트 아카이브</h1>
    <p>매일 발행된 주주·자본시장 데일리 리포트를 날짜별로 확인할 수 있습니다.</p>
    <ul>{links}</ul>
  </main>
</body>
</html>
"""


def build_report_telegram_message(report: dict[str, object]) -> str:
    date_id = str(report.get("date_id") or "")
    review = str(report.get("review") or "")
    paragraphs = clean_report_paragraphs(review, max_paragraphs=4)
    if not paragraphs:
        paragraphs = clean_report_paragraphs(fallback_report_review(report.get("stories") if isinstance(report.get("stories"), list) else []))
    report_url = str(report.get("report_url") or "")
    stats = report.get("stats") if isinstance(report.get("stats"), dict) else {}
    stats_line = f"이슈 {stats.get('stories', 0)}개 · 기사 {stats.get('articles', 0)}건"
    lines = [f"<b>비사이드 자본시장 데일리 ({escape(date_id)})</b>", escape(stats_line), ""]
    for paragraph in paragraphs[:4]:
        lines.append(escape(paragraph))
        lines.append("")
    lines.append(html_link("전체 리포트 보기", report_url))
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
