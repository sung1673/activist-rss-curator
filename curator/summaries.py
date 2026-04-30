from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from html import escape
from typing import Any
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from rapidfuzz import fuzz

from .ai import ai_config, call_github_models
from .dates import datetime_to_iso, format_kst, parse_datetime
from .rss_writer import (
    article_link,
    article_source_label,
    compact_text,
    display_article_title,
    item_title,
    publishable_articles,
)
from .telegram_publisher import (
    build_telegram_message,
    cluster_should_show_web_preview,
    mark_telegram_sent,
    send_telegram_message,
    telegram_bot_token,
    telegram_chat_id,
    html_link,
    telegram_is_configured,
    telegram_config,
    unsent_telegram_clusters,
)
from .story_judge import judge_same_story, judgement_allows_same_story, story_judge_auto_accept_title_score


DIGEST_GROUP_STOPWORDS = {
    "관련",
    "기사",
    "뉴스",
    "논란",
    "확대",
    "강화",
    "제기",
    "동시",
    "추궁",
    "영향",
    "시장",
    "기업",
    "주주",
    "단독",
    "속보",
    "종합",
    "the",
    "and",
    "for",
    "with",
    "from",
    "that",
    "this",
}
DIGEST_GROUP_BROAD_TOKENS = {
    "경영권",
    "분쟁",
    "경영권분쟁",
    "주주",
    "소액주주",
    "소액주주연합",
    "소액주주연대",
    "주주연대",
    "행동주의",
    "이사회",
    "사외이사",
    "지배구조",
    "거버넌스",
    "밸류업",
    "벨류업",
    "주주환원",
    "자사주",
    "소각",
    "의결권",
    "상장사",
    "기업가치",
    "자본시장",
    "공정위",
    "금감원",
    "금융위",
    "관련",
    "추진",
    "부각",
    "지속",
    "확인",
    "활용",
    "수단",
    "우려",
    "확대",
    "강화",
    "shareholder",
    "shareholders",
    "activist",
    "activism",
    "governance",
    "board",
}

DIGEST_GROUP_EVENT_TOKENS = {
    "저pbr",
    "밸류업",
    "벨류업",
    "주주환원",
    "주주가치",
    "자사주",
    "소각",
    "배당",
    "공개매수",
    "의무공개매수",
    "중복상장",
    "상장폐지",
    "상장적격성",
    "실질심사",
    "거래정지",
    "개선기간",
    "주주제안",
    "임시주총",
    "이사회",
    "감사",
    "사외이사",
    "위임장",
    "공개서한",
    "경영권",
    "분쟁",
    "지배구조",
    "스튜어드십",
    "거버넌스",
    "esg",
    "ipo",
    "코너스톤",
}

OPERATIONAL_SUMMARY_PATTERNS = (
    "링크",
    "url",
    "urls",
    "href",
    "추려",
    "읽기 좋게",
    "발행",
    "전송",
    "메시지",
    "건만",
)

FALLBACK_TOPIC_RULES = (
    (
        ("임원보수", "주식보상", "성과보수", "보수 공시", "보수체계"),
        "임원보수·주식보상 공시 강화 부각",
    ),
    (
        ("etf", "의결권", "운용사", "스튜어드십"),
        "ETF·운용사 의결권 영향력 부상",
    ),
    (
        ("코너스톤", "cornerstone", "ipo", "공모주", "상장 제도"),
        "코너스톤 투자자 등 IPO 제도 논의 지속",
    ),
    (
        ("해외부동산펀드", "핵심위험", "투자자 보호", "위험공시"),
        "펀드 위험공시 등 투자자 보호 이슈 확인",
    ),
    (
        ("소액주주", "주주제안", "고발", "검찰", "소송", "주주권"),
        "소액주주 권리 행사와 법적 대응 지속",
    ),
    (
        ("행동주의", "activist", "proxy", "이사회", "위임장", "board"),
        "행동주의와 이사회 견제 흐름 지속",
    ),
    (
        ("밸류업", "주주환원", "자사주", "배당"),
        "밸류업·주주환원 논의 지속",
    ),
    (
        ("지배구조", "거버넌스", "스튜어드십", "책임경영"),
        "지배구조와 스튜어드십 논의 지속",
    ),
    (
        ("경영권", "분쟁", "공개매수", "m&a", "인수"),
        "경영권 분쟁과 자본시장 이벤트 지속",
    ),
)

CIRCLED_NUMBERS = ("①", "②", "③", "④", "⑤", "⑥", "⑦", "⑧", "⑨", "⑩")

DIGEST_SOURCE_LABEL_OVERRIDES = {
    "SISAJOURNAL": "SISA JOURNAL",
    "SEOULFN": "SEOUL FN",
    "NEWSFC": "NEWS FC",
}

DIGEST_CATEGORY_RULES = (
    (
        "shareholder",
        "주주행동·거버넌스",
        (
            "shareholder_proposal",
            "activism_trend",
            "control_dispute",
            "board_audit",
            "valueup_return",
            "governance_stewardship",
            "voting_disclosure",
        ),
        (
            "주주제안",
            "소액주주",
            "주주행동",
            "행동주의",
            "경영권 분쟁",
            "공개서한",
            "위임장",
            "이사회 교체",
            "감사 선임",
            "임시주총",
            "표 대결",
            "얼라인",
            "KCGI",
            "트러스톤",
            "엘리엇",
            "밸류업",
            "벨류업",
            "주주환원",
            "배당",
            "자사주",
            "소각",
            "지배구조",
            "거버넌스",
            "스튜어드십",
            "사외이사",
            "의결권",
            "전자투표",
            "주총",
            "주주총회",
            "임원보수",
            "주식보상",
            "성과보수",
            "activist",
            "proxy fight",
            "governance",
            "stewardship",
            "board",
            "shareholder return",
            "buyback",
        ),
    ),
    (
        "capital_market",
        "자본시장·공시·상장",
        ("capital_market_policy", "listing_risk", "capital_raise_disclosure"),
        (
            "자본시장법",
            "상법",
            "의무공개매수",
            "코너스톤",
            "IPO",
            "공모가",
            "공시",
            "ESG 공시",
            "금감원",
            "금융위",
            "공정위",
            "대기업집단",
            "총수",
            "ISA",
            "STO",
            "증권사 IB",
            "상장폐지",
            "상장적격성",
            "실질심사",
            "거래정지",
            "개선기간",
            "유상증자",
            "CB",
            "EB",
            "전환사채",
            "리픽싱",
            "불성실공시",
            "정정신고서",
            "투자자 보호",
            "capital market reform",
        ),
    ),
)

DIGEST_DEFAULT_CATEGORY_LABEL = "자본시장·공시·상장"


def digest_config(config: dict[str, object]) -> dict[str, Any]:
    value = config.get("digest", {})
    return value if isinstance(value, dict) else {}


def digest_count_limit(settings: dict[str, Any], key: str, default: int) -> int | None:
    try:
        value = int(settings.get(key, default))
    except (TypeError, ValueError):
        value = default
    return value if value > 0 else None


def digest_cluster_datetime(cluster: dict[str, object], timezone_name: str) -> datetime | None:
    for key in ("published_at", "last_article_seen_at", "last_article_at", "created_at"):
        value = cluster.get(key)
        if value:
            parsed = parse_datetime(str(value), timezone_name)
            if parsed:
                return parsed
    return None


def digest_clusters_in_window(
    state: dict[str, object],
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
) -> list[dict[str, object]]:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    clusters = list(state.get("published_clusters", [])) + list(state.get("pending_clusters", []))
    selected: list[tuple[datetime, dict[str, object]]] = []
    for cluster in clusters:
        if not publishable_articles(cluster, config):
            continue
        cluster_dt = digest_cluster_datetime(cluster, timezone_name)
        if cluster_dt and start_at <= cluster_dt <= end_at:
            selected.append((cluster_dt, cluster))
    selected.sort(key=lambda item: item[0])
    max_clusters = digest_count_limit(digest_config(config), "max_clusters", 30)
    if max_clusters is None:
        return [cluster for _dt, cluster in selected]
    return [cluster for _dt, cluster in selected[-max_clusters:]]


def digest_context(clusters: list[dict[str, object]], config: dict[str, object]) -> str:
    settings = digest_config(config)
    max_articles = digest_count_limit(settings, "max_articles_per_cluster", 5)
    blocks: list[str] = []
    for index, cluster in enumerate(clusters, start=1):
        articles = publishable_articles(cluster, config)
        block = [
            f"{index}. {item_title(cluster, len(articles))}",
            "기사:",
        ]
        articles_for_context = articles if max_articles is None else articles[:max_articles]
        for article in articles_for_context:
            source = article_source_label(article)
            title = display_article_title(article, source)
            summary = compact_text(article.get("summary") or "", max_chars=140)
            block.append(f"- {source}: {title}")
            if summary:
                block.append(f"  {summary}")
        blocks.append("\n".join(block))
    return "\n\n".join(blocks)


def digest_article_datetime(
    article: dict[str, object],
    cluster: dict[str, object],
    timezone_name: str,
) -> datetime | None:
    for key in ("article_published_at", "feed_published_at", "published_at", "feed_updated_at"):
        value = article.get(key)
        if value:
            parsed = parse_datetime(str(value), timezone_name)
            if parsed:
                return parsed
    return digest_cluster_datetime(cluster, timezone_name)


def digest_article_is_english(article: dict[str, object]) -> bool:
    title = str(article.get("clean_title") or article.get("title") or "")
    summary = str(article.get("summary") or "")
    title_hangul_count = len(re.findall(r"[가-힣]", title))
    title_latin_count = len(re.findall(r"[A-Za-z]", title))
    if title_hangul_count:
        return False
    if title_latin_count >= 12:
        return True

    text = f"{title} {summary}".strip()
    hangul_count = len(re.findall(r"[가-힣]", text))
    latin_count = len(re.findall(r"[A-Za-z]", text))
    if hangul_count:
        return False
    if latin_count >= 12:
        return True
    feed_name = str(article.get("feed_name") or "").casefold()
    if "google-news-en-" in feed_name or feed_name.endswith("-en"):
        return True
    return False


def digest_article_label(
    article: dict[str, object],
    cluster: dict[str, object],
    config: dict[str, object],
) -> str:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    article_dt = digest_article_datetime(article, cluster, timezone_name)
    if article_dt:
        date_label = article_dt.astimezone(ZoneInfo(timezone_name)).strftime("%m.%d")
    else:
        date_label = "--.--"
    source = article_source_label(article)
    title = display_article_title(article, source)
    title_max_chars = int(digest_config(config).get("link_title_max_chars", 44))
    return f"{date_label} / {compact_text(title, max_chars=title_max_chars)}"


def digest_article_title(article: dict[str, object]) -> str:
    source = article_source_label(article)
    return display_article_title(article, source)


def digest_tokens_from_text(text: str) -> set[str]:
    return {
        token.casefold()
        for token in re.findall(r"[가-힣A-Za-z0-9]{2,}", text)
        if token.casefold() not in DIGEST_GROUP_STOPWORDS
    }


def digest_group_tokens(article: dict[str, object]) -> set[str]:
    text = f"{article.get('clean_title') or article.get('title') or ''} {article.get('summary') or ''}"
    tokens = digest_tokens_from_text(text)
    for company in article.get("company_candidates") or []:
        value = str(company).strip().casefold()
        if value:
            tokens.add(value)
    return tokens


def digest_title_tokens(article: dict[str, object]) -> set[str]:
    return digest_tokens_from_text(str(article.get("clean_title") or article.get("title") or ""))


def digest_company_tokens(entry: dict[str, object]) -> set[str]:
    values: list[str] = []
    article = entry.get("article")
    cluster = entry.get("cluster")
    if isinstance(article, dict):
        values.extend(str(company) for company in article.get("company_candidates") or [])
    if isinstance(cluster, dict):
        values.extend(str(company) for company in cluster.get("companies") or [])
    return {token for value in values for token in digest_tokens_from_text(value)}


def digest_strong_tokens(entry: dict[str, object], key: str) -> set[str]:
    weak_tokens = DIGEST_GROUP_BROAD_TOKENS | digest_company_tokens(entry)
    return {
        str(token)
        for token in set(entry.get(key) or [])
        if str(token).casefold() not in weak_tokens
    }


def digest_primary_title_token(entry: dict[str, object]) -> str:
    title = str(entry.get("title") or "")
    tokens = [token.casefold() for token in re.findall(r"[가-힣A-Za-z0-9]{2,}", title)]
    for token in tokens:
        if token not in DIGEST_GROUP_STOPWORDS and token not in DIGEST_GROUP_BROAD_TOKENS:
            return token
    return ""


def digest_event_tokens(entry: dict[str, object]) -> set[str]:
    tokens = {str(token).casefold() for token in set(entry.get("tokens") or []) | set(entry.get("title_tokens") or [])}
    return tokens & DIGEST_GROUP_EVENT_TOKENS


def digest_entries_share_primary_event(left: dict[str, object], right: dict[str, object]) -> bool:
    left_subject = digest_primary_title_token(left)
    if not left_subject or left_subject != digest_primary_title_token(right):
        return False
    event_overlap = digest_event_tokens(left) & digest_event_tokens(right)
    return len(event_overlap) >= 2


def digest_entry_for_article(
    article: dict[str, object],
    cluster: dict[str, object],
    config: dict[str, object],
    seen_urls: set[str],
) -> dict[str, object] | None:
    url = article_link(article)
    if not url or url in seen_urls:
        return None
    seen_urls.add(url)
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    article_dt = digest_article_datetime(article, cluster, timezone_name)
    return {
        "article": article,
        "cluster": cluster,
        "datetime": article_dt,
        "label": digest_article_label(article, cluster, config),
        "title": digest_article_title(article),
        "title_tokens": digest_title_tokens(article),
        "tokens": digest_group_tokens(article),
        "url": url,
    }


def duplicate_record_candidates(record: dict[str, object]) -> list[dict[str, object]]:
    candidates = [record]
    candidates.extend(match for match in list(record.get("duplicate_matches") or []) if isinstance(match, dict))

    articles: list[dict[str, object]] = []
    seen_urls: set[str] = set()
    for candidate in candidates:
        url = str(candidate.get("canonical_url") or candidate.get("link") or "")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        articles.append(candidate)
    return articles


def duplicate_candidate_score(article: dict[str, object], config: dict[str, object]) -> tuple[int, datetime, str]:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    url = str(article.get("canonical_url") or article.get("link") or "")
    hostname = (urlsplit(url).hostname or "").lower().removeprefix("www.")
    raw_source = str(article.get("source") or "").strip()
    source = article_source_label(article)
    title = str(article.get("clean_title") or article.get("title") or "")
    parsed_date = parse_datetime(
        str(article.get("article_published_at") or article.get("feed_published_at") or article.get("published_at") or ""),
        timezone_name,
    )

    score = 0
    if hostname and not any(hostname == domain or hostname.endswith(f".{domain}") for domain in ("news.google.com", "google.com", "msn.com")):
        score += 6
    if raw_source and source not in {"NEWS", "V", "M", "MSN", "GOOGLE"}:
        score += 3
    if len(title) >= 12:
        score += 2
    if article.get("summary"):
        score += 1
    if parsed_date:
        score += 1
    return score, parsed_date or datetime.min.replace(tzinfo=ZoneInfo("UTC")), url


def duplicate_record_representative(record: dict[str, object], config: dict[str, object]) -> dict[str, object] | None:
    candidates = duplicate_record_candidates(record)
    if not candidates:
        return None
    return max(candidates, key=lambda article: duplicate_candidate_score(article, config))


def add_duplicate_entries(
    entries: dict[str, list[dict[str, object]]],
    duplicate_records: list[dict[str, object]],
    config: dict[str, object],
    seen_urls: set[str],
) -> None:
    for record in duplicate_records:
        article = duplicate_record_representative(record, config)
        if not article:
            continue
        entry = digest_entry_for_article(article, {}, config, seen_urls)
        if not entry:
            continue
        section = "global" if digest_article_is_english(article) else "domestic"
        entries[section].append(entry)


def digest_article_entries(
    clusters: list[dict[str, object]],
    config: dict[str, object],
    duplicate_records: list[dict[str, object]] | None = None,
) -> dict[str, list[dict[str, object]]]:
    settings = digest_config(config)
    max_articles_per_cluster = digest_count_limit(settings, "max_articles_per_cluster", 2)
    entries: dict[str, list[dict[str, object]]] = {"domestic": [], "global": []}
    seen_urls: set[str] = set()

    for cluster in clusters:
        added_for_cluster = 0
        for article in publishable_articles(cluster, config):
            if max_articles_per_cluster is not None and added_for_cluster >= max_articles_per_cluster:
                break
            entry = digest_entry_for_article(article, cluster, config, seen_urls)
            if not entry:
                continue
            section = "global" if digest_article_is_english(article) else "domestic"
            entries[section].append(entry)
            added_for_cluster += 1

    add_duplicate_entries(entries, duplicate_records or [], config, seen_urls)

    for section_entries in entries.values():
        section_entries.sort(key=lambda entry: entry["datetime"] or datetime.min.replace(tzinfo=ZoneInfo("UTC")), reverse=True)
    return entries


def limited_digest_article_entries(
    clusters: list[dict[str, object]],
    config: dict[str, object],
    duplicate_records: list[dict[str, object]] | None = None,
) -> dict[str, list[dict[str, object]]]:
    entries = digest_article_entries(clusters, config, duplicate_records)
    settings = digest_config(config)
    max_per_section = digest_count_limit(settings, "max_links_per_section", 12)
    max_total = digest_count_limit(settings, "max_links_total", 24)

    limited = {
        "domestic": entries["domestic"] if max_per_section is None else entries["domestic"][:max_per_section],
        "global": entries["global"] if max_per_section is None else entries["global"][:max_per_section],
    }
    while max_total is not None and len(limited["domestic"]) + len(limited["global"]) > max_total:
        if len(limited["domestic"]) >= len(limited["global"]) and limited["domestic"]:
            limited["domestic"].pop()
        elif limited["global"]:
            limited["global"].pop()
        else:
            break
    return limited


def digest_entries_are_same_story(
    left: dict[str, object],
    right: dict[str, object],
    config: dict[str, object] | None = None,
) -> bool:
    left_title = str(left.get("title") or "")
    right_title = str(right.get("title") or "")
    title_score = fuzz.token_set_ratio(left_title, right_title)
    local_reason = ""
    if title_score >= 82:
        local_reason = "title_similarity"

    title_overlap = digest_strong_tokens(left, "title_tokens") & digest_strong_tokens(right, "title_tokens")
    all_overlap = digest_strong_tokens(left, "tokens") & digest_strong_tokens(right, "tokens")
    if not local_reason and len(title_overlap) >= 2 and title_score >= 58:
        local_reason = "title_token_overlap"
    if not local_reason and len(title_overlap) >= 1 and len(all_overlap) >= 3 and title_score >= 58:
        local_reason = "title_and_summary_token_overlap"
    if not local_reason and digest_entries_share_primary_event(left, right):
        return True
    if not local_reason:
        return False
    if config is None or title_score >= story_judge_auto_accept_title_score(config):
        return True
    left_article = left.get("article") if isinstance(left.get("article"), dict) else {}
    right_article = right.get("article") if isinstance(right.get("article"), dict) else {}
    judgement = judge_same_story(
        left_article,  # type: ignore[arg-type]
        right_article,  # type: ignore[arg-type]
        config,
        title_score=title_score,
        local_reason=local_reason,
        context="digest_group",
    )
    return judgement_allows_same_story(judgement, config, fallback=True)


def group_digest_entries(entries: list[dict[str, object]], config: dict[str, object] | None = None) -> list[list[dict[str, object]]]:
    groups: list[list[dict[str, object]]] = []
    for entry in entries:
        matched_group: list[dict[str, object]] | None = None
        for group in groups:
            if any(digest_entries_are_same_story(entry, existing, config) for existing in group):
                matched_group = group
                break
        if matched_group is None:
            groups.append([entry])
        else:
            matched_group.append(entry)
    for group in groups:
        group.sort(key=lambda entry: entry["datetime"] or datetime.min.replace(tzinfo=ZoneInfo("UTC")), reverse=True)
    return groups


def digest_group_date_label(group: list[dict[str, object]], config: dict[str, object]) -> str:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    dates = [entry.get("datetime") for entry in group if entry.get("datetime")]
    if not dates:
        return "--.--"
    return max(dates).astimezone(ZoneInfo(timezone_name)).strftime("%m.%d")  # type: ignore[union-attr]


def digest_group_title(group: list[dict[str, object]], config: dict[str, object]) -> str:
    title_max_chars = int(digest_config(config).get("link_title_max_chars", 54))
    title = str(group[0].get("title") or "제목 없음")
    return compact_text(title, max_chars=title_max_chars)


def numbered_digest_source(index: int, source: str) -> str:
    number = CIRCLED_NUMBERS[index - 1] if index <= len(CIRCLED_NUMBERS) else f"{index}."
    label = DIGEST_SOURCE_LABEL_OVERRIDES.get(source, source)
    return f"{number} {label}"


def wrapped_digest_source_link_lines(links: list[str], max_line_chars: int = 900) -> list[str]:
    lines: list[str] = []
    current: list[str] = []
    for link in links:
        candidate_links = current + [link]
        candidate = " · ".join(candidate_links)
        if current and len(candidate) > max_line_chars:
            lines.append("  " + " · ".join(current))
            current = [link]
        else:
            current = candidate_links
    if current:
        lines.append("  " + " · ".join(current))
    return lines


def render_digest_entry_group(group: list[dict[str, object]], config: dict[str, object]) -> list[str]:
    if len(group) == 1:
        entry = group[0]
        return [f"• {html_link(str(entry['label']), str(entry['url']))}"]

    max_links = digest_count_limit(digest_config(config), "max_links_per_group", 5)
    title = digest_group_title(group, config)
    lines = [f"• {digest_group_date_label(group, config)} / {escape(title, quote=False)} ({len(group)}건)"]
    links = []
    shown_group = group if max_links is None else group[:max_links]
    for index, entry in enumerate(shown_group, start=1):
        article = entry["article"]
        source = article_source_label(article)  # type: ignore[arg-type]
        links.append(html_link(numbered_digest_source(index, source), str(entry["url"])))
    if links:
        remaining = len(group) - len(links)
        link_lines = wrapped_digest_source_link_lines(links)
        if remaining > 0:
            link_lines[-1] += f" · 외 {remaining}건"
        lines.extend(link_lines)
    return lines


def digest_representative_entry(group: list[dict[str, object]], config: dict[str, object]) -> dict[str, object]:
    return max(
        group,
        key=lambda entry: duplicate_candidate_score(entry["article"], config)
        if isinstance(entry.get("article"), dict)
        else (0, datetime.min.replace(tzinfo=ZoneInfo("UTC")), str(entry.get("url") or "")),
    )


def render_representative_digest_entry_group(group: list[dict[str, object]], config: dict[str, object]) -> list[str]:
    entry = digest_representative_entry(group, config)
    return [f"• {html_link(str(entry['label']), str(entry['url']))}"]


def is_operational_summary_line(line: str) -> bool:
    lowered = line.casefold()
    return any(pattern in lowered for pattern in OPERATIONAL_SUMMARY_PATTERNS)


def digest_summary_candidates(text: str) -> list[str]:
    candidates: list[str] = []
    for raw_line in re.split(r"[\n\r]+", text):
        line = re.sub(r"^\s*(?:[-*•]|\d+[.)])\s*", "", raw_line).strip()
        if line:
            candidates.append(line)
    if not candidates:
        candidates = [part.strip() for part in re.split(r"(?<=[.!?。])\s+|(?<=다\.)\s+", text) if part.strip()]
    return [line for line in candidates if not is_operational_summary_line(line)]


def summary_bullet_lines(text: str, config: dict[str, object]) -> list[str]:
    settings = digest_config(config)
    max_bullets = int(settings.get("summary_bullets", 3))
    max_chars = int(settings.get("summary_bullet_max_chars", 72))

    bullets: list[str] = []
    for line in digest_summary_candidates(text):
        line = concise_summary_line(line)
        if not line:
            continue
        bullets.append(f"- {escape(compact_text(line, max_chars=max_chars), quote=False)}")
        if len(bullets) >= max_bullets:
            break
    return bullets or ["- 주요 기사 흐름 요약"]


def concise_summary_line(line: str) -> str:
    text = re.sub(r"\s+", " ", str(line or "")).strip(" -•·.;。")
    replacements = (
        (r"임박했음$", "임박"),
        (r"임박한 것으로 보였음$", "임박"),
        (r"이슈로 떠올랐음$", "이슈 부상"),
        (r"흐름이 이어졌음$", "흐름 지속"),
        (r"논의가 이어졌음$", "논의 지속"),
        (r"이슈가 이어졌음$", "이슈 지속"),
        (r"대응이 이어졌음$", "대응 지속"),
        (r"이어졌음$", "지속"),
        (r"부각됐음$", "부각"),
        (r"부각되었음$", "부각"),
        (r"확인됐음$", "확인"),
        (r"확인되었음$", "확인"),
        (r"보였음$", "흐름"),
        (r"나타났음$", "확인"),
        (r"했음$", ""),
    )
    for pattern, replacement in replacements:
        text = re.sub(pattern, replacement, text)
    text = re.sub(r"(가|이) 부각$", " 부각", text)
    text = re.sub(r"([가-힣A-Za-z0-9·]+)이 이슈 부상$", r"\1 이슈 부상", text)
    return re.sub(r"\s+", " ", text).strip(" -•·.;。")


def digest_entry_content_text(entry: dict[str, object]) -> str:
    article = entry.get("article")
    cluster = entry.get("cluster")
    parts = [str(entry.get("title") or "")]
    if isinstance(article, dict):
        parts.extend(
            [
                str(article.get("title") or ""),
                str(article.get("clean_title") or ""),
                str(article.get("summary") or ""),
                " ".join(str(value) for value in article.get("keywords") or []),
            ]
        )
    if isinstance(cluster, dict):
        parts.extend(
            [
                str(cluster.get("representative_title") or ""),
                str(cluster.get("theme_group") or ""),
                " ".join(str(value) for value in cluster.get("keywords") or []),
            ]
        )
    return " ".join(part for part in parts if part).casefold()


def fallback_topic_bullets(entries: list[dict[str, object]], *, global_section: bool = False) -> list[str]:
    scored: list[tuple[int, int, str]] = []
    texts = [digest_entry_content_text(entry) for entry in entries]
    for index, (patterns, phrase) in enumerate(FALLBACK_TOPIC_RULES):
        score = 0
        for text in texts:
            if any(pattern.casefold() in text for pattern in patterns):
                score += 1
        if score:
            scored.append((score, -index, phrase))

    scored.sort(reverse=True)
    bullets: list[str] = []
    for _score, _index, phrase in scored:
        line = f"영문 기사에서는 {phrase}" if global_section and not phrase.startswith("영문") else phrase
        if line not in bullets:
            bullets.append(line)
    return bullets


def fallback_title_bullets(
    entries: list[dict[str, object]],
    config: dict[str, object],
    *,
    global_section: bool = False,
) -> list[str]:
    bullets: list[str] = []
    for group in group_digest_entries(entries, config):
        title = digest_group_title(group, config)
        title = compact_text(re.sub(r"\s+", " ", title).strip(" -|"), max_chars=36)
        if not title or is_operational_summary_line(title):
            continue
        line = f"{title} 이슈 지속"
        if global_section:
            line = f"영문 기사에서는 {line}"
        if line not in bullets:
            bullets.append(line)
    return bullets


def fallback_daily_digest(
    clusters: list[dict[str, object]],
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
) -> str:
    entries = limited_digest_article_entries(clusters, config)
    domestic_lines = fallback_topic_bullets(entries["domestic"])
    global_lines = fallback_topic_bullets(entries["global"], global_section=True)
    if not domestic_lines:
        domestic_lines = fallback_title_bullets(entries["domestic"], config)
    if not global_lines:
        global_lines = fallback_title_bullets(entries["global"], config, global_section=True)

    lines: list[str] = []
    for line in domestic_lines[:2]:
        if not is_operational_summary_line(line) and line not in lines:
            lines.append(line)
    for line in global_lines[:1]:
        if not is_operational_summary_line(line) and line not in lines:
            lines.append(line)

    if not lines:
        all_entries = entries["domestic"] + entries["global"]
        lines = fallback_title_bullets(all_entries, config)[:3]
    if not lines:
        lines = ["주주행동·거버넌스 관련 기사 흐름 지속"]
    return "\n".join(f"- {line}" for line in lines[:3])


def has_meaningful_summary(text: str) -> bool:
    return bool(digest_summary_candidates(text))


def render_digest_link_sections(
    clusters: list[dict[str, object]],
    config: dict[str, object],
    duplicate_records: list[dict[str, object]] | None = None,
) -> list[str]:
    entries = limited_digest_article_entries(clusters, config, duplicate_records)
    labels = {"domestic": "국문", "global": "영문"}
    lines: list[str] = []
    for section_key in ("domestic", "global"):
        section_entries = entries[section_key]
        if not section_entries:
            continue
        if lines:
            lines.append("")
        lines.append(f"<b>{labels[section_key]}</b>")
        for group in group_digest_entries(section_entries, config):
            lines.extend(render_digest_entry_group(group, config))
    return lines


def digest_group_content_text(group: list[dict[str, object]]) -> str:
    return " ".join(digest_entry_content_text(entry) for entry in group).casefold()


def digest_group_theme_groups(group: list[dict[str, object]]) -> set[str]:
    theme_groups: set[str] = set()
    for entry in group:
        cluster = entry.get("cluster")
        if isinstance(cluster, dict):
            theme_group = str(cluster.get("theme_group") or "").strip().casefold()
            if theme_group:
                theme_groups.add(theme_group)
    return theme_groups


def digest_category_label_for_group(group: list[dict[str, object]]) -> str:
    text = digest_group_content_text(group)
    theme_groups = digest_group_theme_groups(group)
    best_score = 0
    best_label = DIGEST_DEFAULT_CATEGORY_LABEL
    for _key, label, rule_theme_groups, keywords in DIGEST_CATEGORY_RULES:
        score = 0
        normalized_rule_theme_groups = {theme.casefold() for theme in rule_theme_groups}
        score += 3 * len(theme_groups & normalized_rule_theme_groups)
        for keyword in keywords:
            if keyword.casefold() in text:
                score += 1
        if score > best_score:
            best_score = score
            best_label = label
    return best_label


def render_daily_digest_section_blocks(
    clusters: list[dict[str, object]],
    config: dict[str, object],
    duplicate_records: list[dict[str, object]] | None = None,
) -> list[tuple[str, list[list[str]]]]:
    entries = limited_digest_article_entries(clusters, config, duplicate_records)
    section_blocks: list[tuple[str, list[list[str]]]] = []

    domestic_groups = group_digest_entries(entries["domestic"], config)
    if domestic_groups:
        buckets: dict[str, list[list[str]]] = {
            label: []
            for _key, label, _theme_groups, _keywords in DIGEST_CATEGORY_RULES
        }
        buckets.setdefault(DIGEST_DEFAULT_CATEGORY_LABEL, [])
        for group in domestic_groups:
            label = digest_category_label_for_group(group)
            buckets.setdefault(label, []).append(render_representative_digest_entry_group(group, config))
        for _key, label, _theme_groups, _keywords in DIGEST_CATEGORY_RULES:
            if buckets.get(label):
                section_blocks.append((label, buckets[label]))
        if DIGEST_DEFAULT_CATEGORY_LABEL not in [label for _key, label, _theme_groups, _keywords in DIGEST_CATEGORY_RULES] and buckets[DIGEST_DEFAULT_CATEGORY_LABEL]:
            section_blocks.append((DIGEST_DEFAULT_CATEGORY_LABEL, buckets[DIGEST_DEFAULT_CATEGORY_LABEL]))

    global_groups = group_digest_entries(entries["global"], config)
    if global_groups:
        section_blocks.append(("영문", [render_representative_digest_entry_group(group, config) for group in global_groups]))

    return section_blocks


def telegram_text_length(lines: list[str]) -> int:
    return len("\n".join(lines).strip())


def append_digest_lines(current: list[str], lines: list[str]) -> list[str]:
    if not current:
        return list(lines)
    if lines and lines[0].startswith("<b>") and current[-1] != "":
        return [*current, "", *lines]
    return [*current, *lines]


def digest_block_fallback_chunks(lines: list[str], max_chars: int) -> list[list[str]]:
    if len(lines) <= 2:
        return [chunk.splitlines() for chunk in split_plain_telegram_text("\n".join(lines).strip(), max_chars)]

    section_header = lines[0] if lines[0].startswith("<b>") else ""
    title_line = lines[1] if section_header and len(lines) > 1 and lines[1].startswith("• ") else ""
    prefix = [line for line in (section_header, title_line) if line]
    if not prefix:
        return [chunk.splitlines() for chunk in split_plain_telegram_text("\n".join(lines).strip(), max_chars)]

    chunks: list[list[str]] = []
    current = list(prefix)
    for line in lines[len(prefix) :]:
        candidate = [*current, line]
        if telegram_text_length(candidate) <= max_chars:
            current = candidate
            continue
        if current != prefix:
            chunks.append(current)
            current = [*prefix, line]
            continue
        chunks.extend(chunk.splitlines() for chunk in split_plain_telegram_text(line, max_chars))
        current = list(prefix)
    if current != prefix:
        chunks.append(current)
    return chunks or [prefix]


def split_digest_section_blocks(
    header_lines: list[str],
    section_blocks: list[tuple[str, list[list[str]]]],
    max_chars: int,
) -> list[str]:
    messages: list[str] = []
    current = [line for line in header_lines if line is not None]
    current_section = ""

    def flush() -> None:
        nonlocal current, current_section
        text = "\n".join(current).strip()
        if text:
            messages.append(text)
        current = []
        current_section = ""

    for section_label, group_blocks in section_blocks:
        for group_index, group_lines in enumerate(group_blocks):
            continued = current_section == section_label or (not current and group_index > 0)
            section_header = f"<b>{section_label}{' (계속)' if continued else ''}</b>"
            block = list(group_lines) if current_section == section_label else [section_header, *group_lines]
            candidate = append_digest_lines(current, block)
            if telegram_text_length(candidate) <= max_chars:
                current = candidate
                current_section = section_label
                continue

            if current:
                flush()

            block = [f"<b>{section_label}{' (계속)' if group_index > 0 else ''}</b>", *group_lines]
            if telegram_text_length(block) <= max_chars:
                current = block
                current_section = section_label
                continue

            chunks = digest_block_fallback_chunks(block, max_chars)
            if chunks:
                messages.extend("\n".join(chunk).strip() for chunk in chunks[:-1] if "\n".join(chunk).strip())
                current = chunks[-1]
                current_section = section_label

    flush()
    return messages


def duplicate_record_datetime(record: dict[str, object], config: dict[str, object]) -> datetime | None:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    for key in ("seen_at", "published_at"):
        parsed = parse_datetime(str(record.get(key) or ""), timezone_name)
        if parsed:
            return parsed
    return None


def duplicate_records_in_window(
    state: dict[str, object],
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
) -> list[dict[str, object]]:
    seen_urls: set[str] = set()
    selected: list[tuple[datetime, dict[str, object]]] = []
    for record in list(state.get("articles", [])):
        if not isinstance(record, dict) or record.get("status") != "duplicate":
            continue
        url = str(record.get("canonical_url") or "")
        if not url or url in seen_urls:
            continue
        record_dt = duplicate_record_datetime(record, config)
        if not record_dt or not start_at <= record_dt <= end_at:
            continue
        seen_urls.add(url)
        selected.append((record_dt, record))
    selected.sort(key=lambda item: item[0], reverse=True)
    max_links = digest_count_limit(digest_config(config), "max_duplicate_links", 12)
    if max_links is None:
        return [record for _dt, record in selected]
    return [record for _dt, record in selected[:max_links]]


def generate_daily_digest_review(
    clusters: list[dict[str, object]],
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
) -> str:
    settings = ai_config(config)
    if not settings.get("daily_digest_enabled", True):
        return fallback_daily_digest(clusters, config, start_at, end_at)
    model = str(settings.get("daily_digest_model") or "openai/gpt-4.1")
    max_tokens = int(settings.get("daily_digest_max_tokens", 900))
    system_prompt = (
        "당신은 한국 자본시장과 주주행동을 보는 시니어 에디터입니다. "
        "전날부터 오늘 오전까지의 기사 묶음을 바탕으로 텔레그램 채널용 데일리 리뷰 요약만 한국어로 작성합니다. "
        "투자 조언이나 매매 권유는 하지 말고, 기사에 없는 사실을 단정하지 마세요."
    )
    user_prompt = (
        "아래 수집 묶음을 바탕으로 데일리 digest의 맨 위 요약만 작성하세요.\n"
        "- bullet point 2~3개만 작성\n"
        "- 각 bullet은 45자 안팎으로 아주 짧게 작성\n"
        "- 문장 끝은 '임박', '부각', '지속', '확인' 같은 명사형으로 끝냄\n"
        "- '~했음', '~보였음', '~이어졌음' 같은 종결어미는 쓰지 않음\n"
        "- 링크, 기준시각, high/medium 같은 내부 분류는 쓰지 않음\n"
        "- 긴 해설, 번호 목록, 제목은 쓰지 않음\n\n"
        f"기간: {format_kst(start_at, str(config.get('timezone') or 'Asia/Seoul'))} - {format_kst(end_at, str(config.get('timezone') or 'Asia/Seoul'))}\n\n"
        f"{digest_context(clusters, config)}"
    )
    content = call_github_models(
        system_prompt,
        user_prompt,
        model=model,
        max_tokens=max_tokens,
        config=config,
    )
    if content and has_meaningful_summary(content):
        return content
    return fallback_daily_digest(clusters, config, start_at, end_at)


def generate_hourly_digest_review(
    clusters: list[dict[str, object]],
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
) -> str:
    settings = ai_config(config)
    if not settings.get("hourly_digest_enabled", True):
        return fallback_daily_digest(clusters, config, start_at, end_at)
    model = str(settings.get("hourly_digest_model") or settings.get("daily_digest_model") or "openai/gpt-4.1")
    max_tokens = int(settings.get("hourly_digest_max_tokens", 180))
    system_prompt = (
        "당신은 한국 자본시장과 주주행동을 보는 시니어 에디터입니다. "
        "최근 1시간 안팎에 새로 묶인 기사들을 바탕으로 텔레그램 업데이트용 요약만 한국어로 작성합니다. "
        "투자 조언이나 매매 권유는 하지 말고, 기사에 없는 사실을 단정하지 마세요."
    )
    user_prompt = (
        "아래 신규 기사 묶음을 바탕으로 시간당 업데이트의 맨 위 요약만 작성하세요.\n"
        "- bullet point 2~3개만 작성\n"
        "- 각 bullet은 45자 안팎으로 아주 짧게 작성\n"
        "- 문장 끝은 '임박', '부각', '지속', '확인' 같은 명사형으로 끝냄\n"
        "- '~했음', '~보였음', '~이어졌음' 같은 종결어미는 쓰지 않음\n"
        "- 링크, 기준시각, high/medium 같은 내부 분류는 쓰지 않음\n"
        "- 운영 설명이나 '몇 건 정리' 같은 말은 쓰지 않음\n\n"
        f"기간: {format_kst(start_at, str(config.get('timezone') or 'Asia/Seoul'))} - {format_kst(end_at, str(config.get('timezone') or 'Asia/Seoul'))}\n\n"
        f"{digest_context(clusters, config)}"
    )
    content = call_github_models(
        system_prompt,
        user_prompt,
        model=model,
        max_tokens=max_tokens,
        config=config,
    )
    if content and has_meaningful_summary(content):
        return content
    return fallback_daily_digest(clusters, config, start_at, end_at)


def split_plain_telegram_text(text: str, max_chars: int) -> list[str]:
    if len(text) <= max_chars:
        return [text]
    chunks: list[str] = []
    current = ""
    for paragraph in text.split("\n\n"):
        candidate = paragraph if not current else f"{current}\n\n{paragraph}"
        if len(candidate) <= max_chars:
            current = candidate
            continue
        if current:
            chunks.append(current)
            current = ""
        if len(paragraph) <= max_chars:
            current = paragraph
            continue
        for line in paragraph.splitlines():
            candidate_line = line if not current else f"{current}\n{line}"
            if len(candidate_line) <= max_chars:
                current = candidate_line
            else:
                if current:
                    chunks.append(current)
                current = line[:max_chars]
    if current:
        chunks.append(current)
    return chunks


def build_daily_digest_messages(
    clusters: list[dict[str, object]],
    config: dict[str, object],
    now: datetime,
    start_at: datetime,
    duplicate_records: list[dict[str, object]] | None = None,
) -> list[str]:
    max_chars = int(digest_config(config).get("max_message_chars", 3900))
    review = generate_daily_digest_review(clusters, config, start_at, now)
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    start_label = start_at.astimezone(ZoneInfo(timezone_name)).strftime("%m.%d")
    end_label = now.astimezone(ZoneInfo(timezone_name)).strftime("%m.%d")
    header_lines = [
        f"<b>데일리 주주·자본시장 브리핑 ({start_label}-{end_label})</b>",
        "",
        "<b>요약</b>",
        *summary_bullet_lines(review, config),
    ]
    section_blocks = render_daily_digest_section_blocks(clusters, config, duplicate_records or [])
    return split_digest_section_blocks(header_lines, section_blocks, max_chars)


def build_hourly_update_messages(
    clusters: list[dict[str, object]],
    config: dict[str, object],
    now: datetime,
    start_at: datetime,
    duplicates: list[dict[str, object]] | None = None,
) -> list[str]:
    max_chars = int(digest_config(config).get("max_message_chars", 3900))
    review = generate_hourly_digest_review(clusters, config, start_at, now)
    lines = [
        "<b>요약</b>",
        *summary_bullet_lines(review, config),
        "",
        *render_digest_link_sections(clusters, config),
    ]
    message = "\n".join(line for line in lines if line is not None).strip()
    return split_plain_telegram_text(message, max_chars)


def telegram_hour_is_skipped(config: dict[str, object], now: datetime) -> bool:
    skip_hours = {int(hour) for hour in telegram_config(config).get("skip_hours", [])}
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    return now.astimezone(ZoneInfo(timezone_name)).hour in skip_hours


def hourly_update_start_at(config: dict[str, object], now: datetime) -> datetime:
    hours = float(telegram_config(config).get("hourly_digest_window_hours", 1))
    return now - timedelta(hours=hours)


def should_batch_telegram_update(
    clusters: list[dict[str, object]],
    duplicates: list[dict[str, object]],
    config: dict[str, object],
) -> bool:
    settings = telegram_config(config)
    if not settings.get("batch_digest_enabled", True):
        return False
    min_clusters = int(settings.get("batch_digest_min_clusters", 2))
    return len(clusters) >= min_clusters


def mark_clusters_sent_with_response(
    state: dict[str, object],
    clusters: list[dict[str, object]],
    now: datetime,
    response: dict[str, object],
) -> None:
    for cluster in clusters:
        mark_telegram_sent(state, cluster, now, response)


def remember_telegram_digest(
    state: dict[str, object],
    now: datetime,
    start_at: datetime,
    clusters: list[dict[str, object]],
    duplicates: list[dict[str, object]],
    message_ids: list[object],
) -> None:
    state.setdefault("telegram_digest_records", [])
    state["telegram_digest_records"].append(  # type: ignore[index, union-attr]
        {
            "sent_at": datetime_to_iso(now),
            "window_start": datetime_to_iso(start_at),
            "window_end": datetime_to_iso(now),
            "cluster_guids": [str(cluster.get("guid") or "") for cluster in clusters],
            "duplicate_count": len([duplicate for duplicate in duplicates if duplicate.get("duplicate_matches")]),
            "message_ids": message_ids,
        }
    )


def publish_hourly_telegram_update(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
    duplicates: list[dict[str, object]] | None = None,
) -> dict[str, int]:
    if not telegram_is_configured(config) or telegram_hour_is_skipped(config, now):
        return {"telegram_sent": 0, "telegram_failed": 0}

    clusters = unsent_telegram_clusters(state, config)
    duplicate_articles = list(duplicates or [])
    if not clusters:
        return {"telegram_sent": 0, "telegram_failed": 0}

    bot_token = telegram_bot_token()
    chat_id = telegram_chat_id(config)
    if should_batch_telegram_update(clusters, duplicate_articles, config):
        start_at = hourly_update_start_at(config, now)
        message_ids: list[object] = []
        failed = 0
        first_response: dict[str, object] | None = None
        for message in build_hourly_update_messages(clusters, config, now, start_at, duplicate_articles):
            response = send_telegram_message(
                bot_token,
                chat_id,
                message,
                config,
                disable_web_page_preview=True,
            )
            if response.get("ok"):
                first_response = first_response or response
                message_ids.append(response.get("message_id"))
            else:
                failed += 1
        if failed:
            return {"telegram_sent": len(message_ids), "telegram_failed": failed}
        mark_clusters_sent_with_response(state, clusters, now, first_response or {})
        remember_telegram_digest(state, now, start_at, clusters, duplicate_articles, message_ids)
        return {"telegram_sent": len(clusters), "telegram_failed": 0}

    sent = 0
    failed = 0
    for cluster in clusters:
        response = send_telegram_message(
            bot_token,
            chat_id,
            build_telegram_message(cluster, config),
            config,
            disable_web_page_preview=not cluster_should_show_web_preview(cluster, config),
        )
        if response.get("ok"):
            mark_telegram_sent(state, cluster, now, response)
            sent += 1
        else:
            failed += 1
    return {"telegram_sent": sent, "telegram_failed": failed}


def publish_daily_digest_if_due(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
) -> dict[str, int]:
    settings = digest_config(config)
    if not settings.get("enabled", True) or not telegram_is_configured(config):
        return {"daily_digest_sent": 0, "daily_digest_failed": 0}

    send_hour = int(settings.get("send_hour", 7))
    send_minute = int(settings.get("send_minute", 0))
    send_window_minutes = int(settings.get("send_window_minutes", 59))
    send_start = now.replace(hour=send_hour, minute=send_minute, second=0, microsecond=0)
    send_end = send_start + timedelta(minutes=send_window_minutes)
    if not daily_digest_is_forced() and not send_start <= now < send_end:
        return {"daily_digest_sent": 0, "daily_digest_failed": 0}

    digest_id = now.strftime("%Y-%m-%d")
    if digest_id in {str(value) for value in state.get("daily_digest_sent_dates", [])}:
        return {"daily_digest_sent": 0, "daily_digest_failed": 0}

    start_at = now - timedelta(hours=int(settings.get("window_hours", 24)))
    clusters = digest_clusters_in_window(state, config, start_at, now)
    duplicate_records = duplicate_records_in_window(state, config, start_at, now)
    if not clusters and not duplicate_records:
        return {"daily_digest_sent": 0, "daily_digest_failed": 0}

    bot_token = telegram_bot_token()
    chat_id = telegram_chat_id(config)
    message_ids: list[object] = []
    failed = 0
    for message in build_daily_digest_messages(clusters, config, now, start_at, duplicate_records):
        response = send_telegram_message(bot_token, chat_id, message, config)
        if response.get("ok"):
            message_ids.append(response.get("message_id"))
        else:
            failed += 1

    if failed:
        return {"daily_digest_sent": len(message_ids), "daily_digest_failed": failed}

    state.setdefault("daily_digest_sent_dates", [])
    if digest_id not in state["daily_digest_sent_dates"]:  # type: ignore[operator]
        state["daily_digest_sent_dates"].append(digest_id)  # type: ignore[index, union-attr]
    state.setdefault("daily_digest_records", [])
    state["daily_digest_records"].append(  # type: ignore[index, union-attr]
        {
            "digest_id": digest_id,
            "sent_at": datetime_to_iso(now),
            "window_start": datetime_to_iso(start_at),
            "window_end": datetime_to_iso(now),
            "cluster_count": len(clusters),
            "message_ids": message_ids,
        }
    )
    return {"daily_digest_sent": len(message_ids), "daily_digest_failed": 0}


def daily_digest_is_forced() -> bool:
    forced = os.environ.get("CURATOR_FORCE_DAILY_DIGEST", "").casefold()
    if forced in {"1", "true", "yes", "on"}:
        return True
    return (
        os.environ.get("GITHUB_EVENT_NAME") == "schedule"
        and os.environ.get("CURATOR_EVENT_SCHEDULE") == "30 21 * * *"
    )
