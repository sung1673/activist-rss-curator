from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit
from xml.sax.saxutils import escape

from .cluster import (
    COMPANY_STRICT_THEME_GROUPS,
    cluster_guid,
    extract_company_candidates,
    primary_theme_group,
)
from .config import article_domain_is_excluded
from .dates import format_kst, format_rfc822, parse_datetime
from .relevance import relevance_details


SOURCE_LABELS = {
    "alphabiz.co.kr": "알파경제",
    "biz.newdaily.co.kr": "뉴데일리경제",
    "bloter.net": "블로터",
    "ceoscoredaily.com": "CEO스코어데일리",
    "dailian.co.kr": "데일리안",
    "digitaltoday.co.kr": "디지털투데이",
    "ebn.co.kr": "EBN",
    "econovill.com": "이코노믹리뷰",
    "enewstoday.co.kr": "이뉴스투데이",
    "futurechosun.com": "더나은미래",
    "mk.co.kr": "매일경제",
    "msn.com": "MSN",
    "mt.co.kr": "머니투데이",
    "news.jkn.co.kr": "재경일보",
    "newstong.co.kr": "뉴스통",
    "newswatch.kr": "뉴스워치",
    "topdaily.kr": "톱데일리",
    "yna.co.kr": "연합뉴스",
}


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


def item_title(cluster: dict[str, object], article_count: int | None = None) -> str:
    count_source = article_count if article_count is not None else cluster.get("article_count")
    count = int(count_source or len(cluster.get("articles", [])) or 1)
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


def article_domain(article: dict[str, object]) -> str:
    return (urlsplit(article_link(article)).hostname or "").lower().removeprefix("www.")


def is_excluded_display_link(article: dict[str, object], config: dict[str, object]) -> bool:
    return article_domain_is_excluded(article, config)


def article_current_relevance_level(article: dict[str, object]) -> str:
    details = relevance_details(
        str(article.get("clean_title") or article.get("title") or ""),
        str(article.get("summary") or ""),
    )
    return str(details["level"])


def article_companies_now(article: dict[str, object]) -> set[str]:
    text = f"{article.get('clean_title') or article.get('title') or ''} {article.get('summary') or ''}"
    return set(str(company) for company in list(article.get("company_candidates") or []) + extract_company_candidates(text))


def article_matches_cluster_company(article: dict[str, object], cluster: dict[str, object]) -> bool:
    theme_group = primary_theme_group(cluster)
    if theme_group not in COMPANY_STRICT_THEME_GROUPS:
        return True
    cluster_companies = {str(company) for company in cluster.get("companies", []) if str(company)}
    if not cluster_companies:
        return True
    return bool(article_companies_now(article) & cluster_companies)


def publishable_articles(cluster: dict[str, object], config: dict[str, object]) -> list[dict[str, object]]:
    publish_config = config.get("publish", {})
    publish_levels = (
        set(publish_config.get("publish_levels", ["high", "medium"]))
        if isinstance(publish_config, dict)
        else {"high", "medium"}
    )
    articles = []
    for article in list(cluster.get("articles", [])):
        if is_excluded_display_link(article, config):
            continue
        if article_current_relevance_level(article) not in publish_levels:
            continue
        if not article_matches_cluster_company(article, cluster):
            continue
        articles.append(article)
    return articles


def has_publishable_link(cluster: dict[str, object], config: dict[str, object]) -> bool:
    articles = publishable_articles(cluster, config)
    return any(article_link(article) and not is_excluded_display_link(article, config) for article in articles)


def item_link(cluster: dict[str, object], config: dict[str, object]) -> str:
    for article in publishable_articles(cluster, config):
        link = article_link(article)
        if link:
            return link
    return representative_link(cluster)


def compact_text(value: object, *, max_chars: int = 92) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    text = text.strip(" -|")
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 3)].rstrip() + "..."


def source_label_from_domain(domain: str) -> str:
    domain = domain.lower().removeprefix("www.")
    if domain in SOURCE_LABELS:
        return SOURCE_LABELS[domain]
    parts = domain.split(".")
    return parts[0].upper() if parts else "출처"


def article_source_label(article: dict[str, object]) -> str:
    domain = article_domain(article)
    source = article_source(article)
    if domain:
        return compact_text(SOURCE_LABELS.get(domain) or source_label_from_domain(domain), max_chars=28)
    return compact_text(source, max_chars=28)


def display_article_title(article: dict[str, object], source: str) -> str:
    title = compact_text(article.get("clean_title") or article.get("title") or "제목 없음")
    source_variants = {source, source.upper(), source.lower(), source.casefold()}
    for variant in source_variants:
        if variant and title.casefold().startswith(f"{variant.casefold()} - "):
            title = title[len(variant) + 3 :].strip()
    return compact_text(title)


def article_link(article: dict[str, object]) -> str:
    return str(article.get("canonical_url") or article.get("link") or "")


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

    articles = publishable_articles(cluster, config)
    article_count = len(articles)
    lines = [
        f"<b>📌 {escape(compact_text(cluster.get('representative_title') or '제목 없음', max_chars=120))}</b>",
        "",
        f"관련 기사 {article_count}건",
        "",
    ]

    shown = articles[:max_links]
    for index, article in enumerate(shown, start=1):
        source = article_source_label(article)
        title = display_article_title(article, source)
        label = f"{source} | {title}"
        lines.append(f"{index}. {html_anchor(label, article_link(article))}")

    remaining = len(articles) - len(shown)
    if remaining > 0:
        lines.append(f"외 {remaining}건")
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
    feed_clusters = [
        cluster
        for cluster in sort_published_clusters(clusters, timezone_name)
        if has_publishable_link(cluster, config)
    ][:max_items]
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
        article_count = len(publishable_articles(cluster, config))
        lines.extend(
            [
                "<item>",
                f"<title>{escape(item_title(cluster, article_count))}</title>",
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


def write_index(path: str | Path, state: dict[str, object], config: dict[str, object], now: datetime) -> str:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    published = sort_published_clusters(list(state.get("published_clusters", [])), timezone_name)
    articles = list(state.get("articles", []))
    unique_article_count = len({str(article.get("canonical_url")) for article in articles if article.get("canonical_url")})
    duplicate_count = sum(1 for article in articles if article.get("status") == "duplicate")
    pending_count = len(state.get("pending_clusters", []))
    recent_items = "\n".join(
        f'<li>{escape(format_kst(cluster.get("published_at"), timezone_name))} - '
        f'<a href="{attr_escape(item_link(cluster, config))}">'
        f'{escape(item_title(cluster, len(publishable_articles(cluster, config))))}</a></li>'
        for cluster in published[:20]
        if has_publishable_link(cluster, config)
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
