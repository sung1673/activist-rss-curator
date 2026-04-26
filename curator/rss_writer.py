from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from xml.sax.saxutils import escape

from .cluster import cluster_guid
from .dates import format_kst, format_rfc822, parse_datetime
from .normalize import stable_hash


def cdata(text: str) -> str:
    return "<![CDATA[" + text.replace("]]>", "]]]]><![CDATA[>") + "]]>"


def article_source(article: dict[str, object]) -> str:
    source = str(article.get("source") or "").strip()
    if source:
        return source
    url = str(article.get("canonical_url") or article.get("link") or "")
    return (urlsplit(url).hostname or "unknown").removeprefix("www.")


def attr_escape(value: str) -> str:
    return escape(value, {'"': "&quot;"})


def public_base_url(config: dict[str, object]) -> str:
    configured = str(config.get("public_base_url") or "").strip().rstrip("/")
    if configured:
        return configured

    feed_url = str(config.get("public_feed_url") or "").strip()
    if not feed_url:
        return ""

    parts = urlsplit(feed_url)
    path = parts.path
    if path.endswith("/feed.xml"):
        path = path[: -len("/feed.xml")]
    elif path.endswith("feed.xml"):
        path = path.rsplit("/", 1)[0]
    else:
        path = path.rstrip("/")
    return urlunsplit((parts.scheme, parts.netloc, path.rstrip("/"), "", "")).rstrip("/")


def cluster_page_slug(cluster: dict[str, object]) -> str:
    guid = str(cluster.get("guid") or cluster_guid(cluster)).strip() or "cluster"
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", guid).strip("-")
    return f"{slug or 'cluster'}.html"


def cluster_detail_url(cluster: dict[str, object], config: dict[str, object]) -> str:
    base_url = public_base_url(config)
    if not base_url:
        return ""
    return f"{base_url}/items/{cluster_page_slug(cluster)}"


def item_link(cluster: dict[str, object], config: dict[str, object]) -> str:
    return cluster_detail_url(cluster, config) or representative_link(cluster)


def item_title(cluster: dict[str, object]) -> str:
    count = int(cluster.get("article_count") or len(cluster.get("articles", [])) or 1)
    title = str(cluster.get("representative_title") or "제목 없음").strip()
    if cluster.get("is_followup"):
        return f"[추가 {count}건] {title}"
    if count >= 2:
        return f"[묶음 {count}건] {title}"

    articles = list(cluster.get("articles", []))
    prefixes = articles[0].get("prefixes") if articles else []
    if prefixes:
        return f"[{prefixes[0]}] {title}"
    return title


def representative_link(cluster: dict[str, object]) -> str:
    return str(cluster.get("representative_url") or "")


def compact_text(value: object, *, max_chars: int = 92) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    text = text.strip(" -|")
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 3)].rstrip() + "..."


def display_article_title(article: dict[str, object], source: str) -> str:
    title = compact_text(article.get("clean_title") or article.get("title") or "제목 없음")
    source_variants = {source, source.upper(), source.lower(), source.casefold()}
    for variant in source_variants:
        if variant and title.casefold().startswith(f"{variant.casefold()} - "):
            title = title[len(variant) + 3 :].strip()
    return compact_text(title)


def article_link(article: dict[str, object]) -> str:
    return str(article.get("canonical_url") or article.get("link") or "")


def article_page_slug(article: dict[str, object]) -> str:
    seed = str(article.get("canonical_url_hash") or stable_hash(article_link(article) or "article", length=16))
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", seed).strip("-")
    return f"{slug or 'article'}.html"


def article_short_url(article: dict[str, object], config: dict[str, object]) -> str:
    base_url = public_base_url(config)
    if not base_url or not article_link(article):
        return ""
    return f"{base_url}/u/{article_page_slug(article)}"


def article_message_link(article: dict[str, object], config: dict[str, object]) -> str:
    return article_short_url(article, config) or article_link(article)


def html_anchor(label: str, url: str) -> str:
    safe_label = escape(label)
    if not url:
        return safe_label
    return f'<a href="{attr_escape(url)}">{safe_label}</a>'


def trim_description(description: str, max_chars: int) -> str:
    if len(description) <= max_chars:
        return description
    marker = "\n... (내용 생략)"
    return description[: max(0, max_chars - len(marker))].rstrip() + marker


def item_description(cluster: dict[str, object], config: dict[str, object]) -> str:
    cluster_config = config.get("cluster", {})
    max_links = int(cluster_config.get("max_links_per_item", 7))  # type: ignore[union-attr]
    max_chars = int(cluster_config.get("max_description_chars", 3500))  # type: ignore[union-attr]
    timezone_name = str(config.get("timezone") or "Asia/Seoul")

    articles = list(cluster.get("articles", []))
    article_count = int(cluster.get("article_count") or len(articles))
    published_at = parse_datetime(str(cluster.get("published_at") or ""), timezone_name)
    lines = [
        f"<b>📌 {escape(compact_text(cluster.get('representative_title') or '제목 없음', max_chars=120))}</b>",
        "",
        f"관련 기사 {article_count}건 · 분류: {escape(str(cluster.get('relevance_level') or 'medium'))}",
        f"기준시각: {escape(format_kst(published_at, timezone_name))}",
        "",
    ]

    shown = articles[:max_links]
    for index, article in enumerate(shown, start=1):
        source = compact_text(article_source(article), max_chars=28)
        title = display_article_title(article, source)
        label = f"{source} | {title}"
        lines.extend([f"{index}. {html_anchor(label, article_message_link(article, config))}", ""])

    remaining = article_count - len(shown)
    if remaining > 0:
        lines.extend([f"외 {remaining}건", ""])

    detail_url = cluster_detail_url(cluster, config)
    if detail_url:
        lines.extend([f"묶음 상세: {html_anchor('열기', detail_url)}"])
    return trim_description("\n".join(lines).strip(), max_chars)


def sort_published_clusters(clusters: list[dict[str, object]], timezone_name: str) -> list[dict[str, object]]:
    return sorted(
        clusters,
        key=lambda cluster: parse_datetime(str(cluster.get("published_at") or ""), timezone_name) or datetime.min,
        reverse=True,
    )


def build_rss(clusters: list[dict[str, object]], config: dict[str, object], now: datetime) -> str:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    max_items = int(config.get("publish", {}).get("max_items_in_feed", 50))  # type: ignore[union-attr]
    feed_clusters = sort_published_clusters(clusters, timezone_name)[:max_items]
    channel_title = "정제 RSS - 행동주의 뉴스"
    channel_link = str(config.get("public_feed_url") or "")
    lines = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<rss version="2.0" xmlns:content="http://purl.org/rss/1.0/modules/content/">',
        "<channel>",
        f"<title>{escape(channel_title)}</title>",
        f"<link>{escape(channel_link)}</link>",
        "<description>Google Alerts 기반 행동주의 뉴스 정제 RSS</description>",
        f"<lastBuildDate>{escape(format_rfc822(now))}</lastBuildDate>",
        "<language>ko</language>",
    ]

    for cluster in feed_clusters:
        pub_dt = parse_datetime(str(cluster.get("published_at") or ""), timezone_name) or now
        guid = str(cluster.get("guid") or cluster_guid(cluster, timezone_name))
        description = item_description(cluster, config)
        link = item_link(cluster, config)
        lines.extend(
            [
                "<item>",
                f"<title>{escape(item_title(cluster))}</title>",
                f"<link>{escape(link)}</link>",
                f"<guid isPermaLink=\"false\">{escape(guid)}</guid>",
                f"<pubDate>{escape(format_rfc822(pub_dt))}</pubDate>",
                f"<description>{cdata(description)}</description>",
                f"<content:encoded>{cdata(description)}</content:encoded>",
                "</item>",
            ]
        )

    lines.extend(["</channel>", "</rss>", ""])
    return "\n".join(lines)


def write_feed(path: str | Path, clusters: list[dict[str, object]], config: dict[str, object], now: datetime) -> str:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rss = build_rss(clusters, config, now)
    output_path.write_text(rss, encoding="utf-8")
    return rss


def cluster_detail_html(cluster: dict[str, object], config: dict[str, object], now: datetime) -> str:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    published_at = parse_datetime(str(cluster.get("published_at") or ""), timezone_name)
    articles = list(cluster.get("articles", []))
    article_items = "\n".join(
        f"""      <li>
        <a href="{attr_escape(article_link(article))}">{escape(display_article_title(article, article_source(article)))}</a>
        <span>{escape(article_source(article))}</span>
      </li>"""
        for article in articles
    )
    title = item_title(cluster)
    representative = representative_link(cluster)
    representative_link_html = (
        f'<p><a href="{attr_escape(representative)}">대표 기사 열기</a></p>' if representative else ""
    )
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{escape(title)}</title>
  <style>
    body {{ font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 2rem; line-height: 1.6; color: #111827; }}
    main {{ max-width: 820px; }}
    a {{ color: #0b57d0; font-weight: 650; }}
    ul {{ padding-left: 1.25rem; }}
    li {{ margin: 0.75rem 0; }}
    span {{ display: block; color: #6b7280; font-size: 0.92rem; }}
  </style>
</head>
<body>
  <main>
    <h1>{escape(title)}</h1>
    <p>분류: {escape(str(cluster.get("relevance_level") or "medium"))} / 발행: {escape(format_kst(published_at, timezone_name))}</p>
    {representative_link_html}
    <h2>관련 기사 {len(articles)}건</h2>
    <ul>
{article_items}
    </ul>
    <p><a href="../feed.xml">feed.xml</a></p>
    <p>마지막 생성: {escape(format_kst(now, timezone_name))}</p>
  </main>
</body>
</html>
"""


def write_cluster_pages(path: str | Path, clusters: list[dict[str, object]], config: dict[str, object], now: datetime) -> None:
    output_dir = Path(path)
    output_dir.mkdir(parents=True, exist_ok=True)
    active_files = set()
    for cluster in clusters:
        filename = cluster_page_slug(cluster)
        active_files.add(filename)
        (output_dir / filename).write_text(cluster_detail_html(cluster, config, now), encoding="utf-8")

    for existing in output_dir.glob("*.html"):
        if existing.name not in active_files:
            existing.unlink()


def article_redirect_html(article: dict[str, object]) -> str:
    target = article_link(article)
    title = display_article_title(article, article_source(article))
    script_target = json.dumps(target, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="robots" content="noindex">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="0; url={attr_escape(target)}">
  <link rel="canonical" href="{attr_escape(target)}">
  <title>{escape(title)}</title>
  <script>window.location.replace({script_target});</script>
</head>
<body>
  <p><a href="{attr_escape(target)}">기사 열기</a></p>
</body>
</html>
"""


def write_article_redirect_pages(path: str | Path, clusters: list[dict[str, object]]) -> None:
    output_dir = Path(path)
    output_dir.mkdir(parents=True, exist_ok=True)
    active_files = set()
    for cluster in clusters:
        for article in list(cluster.get("articles", [])):
            if not article_link(article):
                continue
            filename = article_page_slug(article)
            active_files.add(filename)
            (output_dir / filename).write_text(article_redirect_html(article), encoding="utf-8")

    for existing in output_dir.glob("*.html"):
        if existing.name not in active_files:
            existing.unlink()


def write_index(path: str | Path, state: dict[str, object], config: dict[str, object], now: datetime) -> str:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    published = sort_published_clusters(list(state.get("published_clusters", [])), timezone_name)
    articles = list(state.get("articles", []))
    unique_article_count = len({str(article.get("canonical_url")) for article in articles if article.get("canonical_url")})
    duplicate_count = sum(1 for article in articles if article.get("status") == "duplicate")
    pending_count = len(state.get("pending_clusters", []))
    recent_items = "\n".join(
        f'<li>{escape(format_kst(cluster.get("published_at"), timezone_name))} - '
        f'<a href="{attr_escape(cluster_detail_url(cluster, config) or representative_link(cluster))}">{escape(item_title(cluster))}</a></li>'
        for cluster in published[:20]
    )
    html = f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>정제 RSS - 행동주의 뉴스</title>
  <style>
    body {{ font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 2rem; line-height: 1.6; }}
    main {{ max-width: 860px; }}
    a {{ color: #0b57d0; }}
    code {{ background: #f3f4f6; padding: 0.1rem 0.3rem; border-radius: 4px; }}
  </style>
</head>
<body>
  <main>
    <h1>정제 RSS - 행동주의 뉴스</h1>
    <p><a href="./feed.xml">feed.xml</a></p>
    <p>마지막 생성 시각: {escape(format_kst(now, timezone_name))}</p>
    <p>최근 cluster: {len(published)}개 / pending: {pending_count}개 / rejected: {len(state.get("rejected_articles", []))}건</p>
    <p>unique articles: {unique_article_count}건 / total seen records: {len(articles)}건 / duplicates: {duplicate_count}건</p>
    <h2>최근 cluster 목록</h2>
    <ul>
      {recent_items}
    </ul>
  </main>
</body>
</html>
"""
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return html
