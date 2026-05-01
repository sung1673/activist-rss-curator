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
        links.append(
            {
                "source": source,
                "title": title,
                "url": url,
                "domain": article_domain(url),
            }
        )
    return links


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
            "지난 24시간의 핵심 흐름은 주주권 행사와 경영 의사결정에 대한 시장의 감시가 이어졌다는 점입니다. "
            + " / ".join(compact_text(title, max_chars=42) for title in lead_titles[:3])
            + " 같은 보도가 주요 축을 형성했습니다."
        )
    if valueup:
        paragraphs.append(
            "밸류업과 주주환원 쪽에서는 자사주 소각, 배당, 저평가 해소 논의가 계속 확인됐습니다. "
            "개별 기업의 환원 계획뿐 아니라 제도 개선과 투자자 신뢰 회복이 함께 다뤄졌습니다."
        )
    if capital:
        paragraphs.append(
            "자본시장 제도와 공시 이슈는 금융당국의 정정요구, 상장폐지·개선기간, 공개매수와 같은 투자자 보호 장치로 모였습니다. "
            "단일 사건보다 공시 품질과 일반주주 보호 기준을 둘러싼 압박이 더 넓게 읽힙니다."
        )
    if global_titles:
        paragraphs.append(
            "영문·외신 흐름에서는 Korea discount, shareholder returns, activist pressure 같은 키워드가 잡혔습니다. "
            "국내 시장을 해외 투자자가 어떻게 해석하는지와 글로벌 행동주의 사례를 함께 추적할 필요가 있습니다."
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
        "당신은 한국 자본시장, 주주행동, 기업지배구조를 보는 시니어 에디터입니다. "
        "수집된 기사 묶음을 바탕으로 하루치 브리핑의 본문 해설만 한국어로 작성합니다. "
        "투자 조언이나 매매 권유는 하지 말고, 기사에 없는 사실을 단정하지 마세요."
    )
    user_prompt = (
        "아래 기사 묶음을 바탕으로 Telegram과 HTML 리포트 상단에 들어갈 상세 요약을 작성하세요.\n"
        "- 정확히 4개 문단으로 작성\n"
        "- bullet point, 번호, 제목 없이 문단만 작성\n"
        "- 각 문단은 2문장 안팎, 180자 안팎\n"
        "- 전체 흐름, 주요 사건, 제도/정책적 의미, 해외/영문 흐름을 균형 있게 반영\n"
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


def render_link_list(links: list[dict[str, str]], *, compact: bool = False) -> str:
    items = []
    for index, link in enumerate(links, start=1):
        source = escape(link.get("source") or link.get("domain") or f"기사 {index}")
        title = escape(compact_text(link.get("title") or "", max_chars=86))
        url = escape(link.get("url") or "", quote=True)
        if compact:
            items.append(f'<a href="{url}">{source}</a>')
        else:
            items.append(f'<li><a href="{url}"><span>{source}</span>{title}</a></li>')
    return " ".join(items) if compact else "\n".join(items)


def render_story(story: dict[str, object], config: dict[str, object], *, featured: bool = False) -> str:
    links = story.get("links") if isinstance(story.get("links"), list) else []
    safe_title = escape(str(story.get("title") or "제목 없음"))
    primary_url = escape(str(story.get("primary_url") or "#"), quote=True)
    category = escape(str(story.get("category") or "기타"))
    sources = escape(str(story.get("source_line") or story.get("primary_source") or ""))
    summary = escape(str(story.get("summary") or "관련 보도를 묶어 원문 링크와 함께 정리했습니다."))
    timestamp = escape(date_label(story.get("datetime"), config))
    more_links = render_link_list([link for link in links if isinstance(link, dict)], compact=True)
    detail_links = render_link_list([link for link in links if isinstance(link, dict)], compact=False)
    featured_class = " story--featured" if featured else ""
    return f"""
          <article class="story{featured_class}">
            <div class="story__meta"><span>{category}</span><span>{timestamp}</span><span>{sources}</span></div>
            <h3><a href="{primary_url}">{safe_title}</a></h3>
            <p>{summary}</p>
            <div class="story__more"><strong>More:</strong> {more_links}</div>
            <details>
              <summary>기사 링크 {len(links)}건 보기</summary>
              <ol>
                {detail_links}
              </ol>
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
        section_id = re.sub(r"[^a-zA-Z0-9가-힣]+", "-", category).strip("-")
        category_sections.append(
            f"""
        <section class="section" id="{escape(section_id, quote=True)}">
          <div class="section__rule"></div>
          <h2>{escape(category)}</h2>
          <div class="story-list">
            {''.join(render_story(story, config) for story in category_stories)}
          </div>
        </section>
            """
        )
    toc = "\n".join(
        f'<a href="#{escape(re.sub(r"[^a-zA-Z0-9가-힣]+", "-", category).strip("-"), quote=True)}">{escape(category)} <span>{len(buckets.get(category, []))}</span></a>'
        for category in REPORT_CATEGORY_ORDER
        if buckets.get(category)
    )
    start_label = escape(format_kst(start_at, str(config.get("timezone") or "Asia/Seoul")))
    end_label = escape(format_kst(end_at, str(config.get("timezone") or "Asia/Seoul")))
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
      --ink: #111111;
      --muted: #66615a;
      --line: #d9d4cc;
      --paper: #fbfaf7;
      --accent: #0c6b4d;
      --soft: #eef4ef;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--ink);
      background: var(--paper);
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      line-height: 1.58;
    }}
    a {{ color: inherit; text-decoration-thickness: 1px; text-underline-offset: 3px; }}
    .page {{ max-width: 1180px; margin: 0 auto; padding: 28px 22px 64px; }}
    .masthead {{ border-bottom: 2px solid var(--ink); padding-bottom: 24px; }}
    .brand-row {{ display: flex; justify-content: space-between; gap: 16px; align-items: baseline; border-bottom: 1px solid var(--line); padding-bottom: 10px; margin-bottom: 24px; }}
    .brand {{ font-size: 14px; font-weight: 800; letter-spacing: .08em; color: var(--accent); }}
    .edition {{ color: var(--muted); font-size: 13px; }}
    h1 {{ font-family: Georgia, "Times New Roman", serif; font-size: clamp(40px, 7vw, 78px); line-height: .96; letter-spacing: 0; margin: 0 0 16px; max-width: 940px; }}
    .dek {{ max-width: 760px; color: #302d2a; font-size: 18px; margin: 0; }}
    .stats {{ display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 1px; background: var(--line); margin-top: 24px; border: 1px solid var(--line); }}
    .stat {{ background: var(--paper); padding: 14px; }}
    .stat strong {{ display: block; font-family: Georgia, "Times New Roman", serif; font-size: 28px; line-height: 1; }}
    .stat span {{ color: var(--muted); font-size: 12px; }}
    .brief {{ display: grid; grid-template-columns: 220px 1fr; gap: 28px; border-bottom: 1px solid var(--ink); padding: 32px 0; }}
    .brief h2, .section h2 {{ font-family: Georgia, "Times New Roman", serif; font-size: 28px; line-height: 1.1; margin: 0; }}
    .brief p {{ margin: 0 0 15px; font-size: 17px; }}
    .toc {{ display: flex; flex-wrap: wrap; gap: 10px; padding: 18px 0; border-bottom: 1px solid var(--line); }}
    .toc a {{ border: 1px solid var(--line); border-radius: 999px; padding: 7px 12px; background: #fff; text-decoration: none; font-size: 13px; }}
    .toc span {{ color: var(--accent); font-weight: 700; margin-left: 4px; }}
    .featured {{ display: grid; grid-template-columns: 1.4fr 1fr 1fr; gap: 22px; border-bottom: 1px solid var(--ink); padding: 28px 0; }}
    .section {{ padding: 34px 0 6px; }}
    .section__rule {{ height: 3px; background: var(--ink); margin-bottom: 14px; }}
    .story-list {{ margin-top: 18px; }}
    .story {{ border-top: 1px solid var(--line); padding: 18px 0; }}
    .story--featured {{ border-top: 0; padding-top: 0; }}
    .story__meta {{ display: flex; flex-wrap: wrap; gap: 8px; color: var(--muted); font-size: 12px; margin-bottom: 8px; }}
    .story__meta span:not(:last-child)::after {{ content: "·"; margin-left: 8px; color: var(--line); }}
    .story h3 {{ font-family: Georgia, "Times New Roman", serif; font-size: 25px; line-height: 1.12; margin: 0 0 8px; letter-spacing: 0; }}
    .story--featured:first-child h3 {{ font-size: 34px; }}
    .story p {{ margin: 0 0 10px; color: #34312d; }}
    .story__more {{ font-size: 13px; color: var(--muted); }}
    .story__more strong {{ color: var(--accent); }}
    .story__more a {{ margin-right: 8px; white-space: nowrap; }}
    details {{ margin-top: 10px; }}
    summary {{ cursor: pointer; color: var(--accent); font-size: 13px; font-weight: 700; }}
    ol {{ margin: 10px 0 0 20px; padding: 0; }}
    li {{ margin: 6px 0; }}
    li span {{ display: inline-block; min-width: 90px; color: var(--muted); font-size: 12px; margin-right: 8px; }}
    .footer {{ margin-top: 42px; border-top: 2px solid var(--ink); padding-top: 18px; color: var(--muted); font-size: 13px; }}
    @media (max-width: 820px) {{
      .page {{ padding: 18px 16px 48px; }}
      .brief, .featured {{ grid-template-columns: 1fr; }}
      .stats {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .brand-row {{ align-items: flex-start; flex-direction: column; }}
      .story--featured:first-child h3, .story h3 {{ font-size: 24px; }}
    }}
  </style>
</head>
<body>
  <div class="page">
    <header class="masthead">
      <div class="brand-row">
        <div class="brand">BSIDE KOREA DAILY NEWS</div>
        <div class="edition">{start_label} - {end_label}</div>
      </div>
      <h1>주주·자본시장 데일리</h1>
      <p class="dek">건강한 자본시장을 위한 주주행동, 지배구조, 밸류업, 자본시장 제도 뉴스를 하루 단위로 묶어 읽기 쉽게 정리했습니다.</p>
      <div class="stats" aria-label="report stats">
        <div class="stat"><strong>{stats['stories']}</strong><span>이슈 묶음</span></div>
        <div class="stat"><strong>{stats['articles']}</strong><span>기사 링크</span></div>
        <div class="stat"><strong>{stats['sources']}</strong><span>매체</span></div>
        <div class="stat"><strong>{stats['clusters']}</strong><span>수집 클러스터</span></div>
        <div class="stat"><strong>{stats['duplicates']}</strong><span>중복 후보</span></div>
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
      <p>Generated by activist-rss-curator. Source links belong to each publisher. This page is an automated curation report and does not provide investment advice.</p>
    </footer>
  </div>
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
    dated_path.write_text(html, encoding="utf-8")
    latest_path.write_text(html, encoding="utf-8")
    return [dated_path, latest_path]


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
