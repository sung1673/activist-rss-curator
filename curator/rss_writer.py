from __future__ import annotations

from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit
from xml.sax.saxutils import escape

from .cluster import cluster_guid
from .dates import format_kst, format_rfc822, parse_datetime


def cdata(text: str) -> str:
    return "<![CDATA[" + text.replace("]]>", "]]]]><![CDATA[>") + "]]>"


def article_source(article: dict[str, object]) -> str:
    source = str(article.get("source") or "").strip()
    if source:
        return source
    url = str(article.get("canonical_url") or article.get("link") or "")
    return (urlsplit(url).hostname or "unknown").removeprefix("www.")


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
        f"📌 {cluster.get('representative_title') or '제목 없음'}",
        "",
        f"관련 기사 {article_count}건",
        f"분류: {cluster.get('relevance_level') or 'medium'}",
        f"기준시각: {format_kst(published_at, timezone_name)}",
        "",
    ]

    shown = articles[:max_links]
    for index, article in enumerate(shown, start=1):
        title = str(article.get("clean_title") or article.get("title") or "제목 없음")
        link = str(article.get("canonical_url") or article.get("link") or "")
        lines.extend([f"{index}. {article_source(article)} - {title}", link, ""])

    remaining = article_count - len(shown)
    if remaining > 0:
        lines.extend([f"외 {remaining}건", ""])

    lines.extend(["대표 링크:", representative_link(cluster)])
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
        '<rss version="2.0">',
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
        lines.extend(
            [
                "<item>",
                f"<title>{escape(item_title(cluster))}</title>",
                f"<link>{escape(representative_link(cluster))}</link>",
                f"<guid isPermaLink=\"false\">{escape(guid)}</guid>",
                f"<pubDate>{escape(format_rfc822(pub_dt))}</pubDate>",
                f"<description>{cdata(item_description(cluster, config))}</description>",
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


def write_index(path: str | Path, state: dict[str, object], config: dict[str, object], now: datetime) -> str:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    published = sort_published_clusters(list(state.get("published_clusters", [])), timezone_name)
    articles = list(state.get("articles", []))
    unique_article_count = len({str(article.get("canonical_url")) for article in articles if article.get("canonical_url")})
    duplicate_count = sum(1 for article in articles if article.get("status") == "duplicate")
    pending_count = len(state.get("pending_clusters", []))
    recent_items = "\n".join(
        f"<li>{escape(format_kst(cluster.get('published_at'), timezone_name))} - {escape(item_title(cluster))}</li>"
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
