from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from html import escape
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from rapidfuzz import fuzz

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


def ai_config(config: dict[str, object]) -> dict[str, Any]:
    value = config.get("ai", {})
    return value if isinstance(value, dict) else {}


def digest_config(config: dict[str, object]) -> dict[str, Any]:
    value = config.get("digest", {})
    return value if isinstance(value, dict) else {}


def github_models_token() -> str:
    return (
        os.environ.get("GITHUB_MODELS_TOKEN")
        or os.environ.get("GITHUB_TOKEN")
        or os.environ.get("GH_TOKEN")
        or ""
    ).strip()


def call_github_models(
    system_prompt: str,
    user_prompt: str,
    *,
    model: str,
    max_tokens: int,
    config: dict[str, object],
    client: httpx.Client | None = None,
) -> str | None:
    settings = ai_config(config)
    if not settings.get("enabled", True):
        return None
    token = github_models_token()
    if not token:
        return None

    endpoint = str(settings.get("endpoint") or "https://models.github.ai/inference/chat/completions")
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0.2,
        "max_tokens": max_tokens,
    }
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    timeout = float(settings.get("timeout_seconds", 25))

    try:
        if client is None:
            with httpx.Client(timeout=timeout) as local_client:
                response = local_client.post(endpoint, headers=headers, json=payload)
        else:
            response = client.post(endpoint, headers=headers, json=payload)
        if response.status_code >= 400:
            return None
        data = response.json()
    except (httpx.HTTPError, ValueError, TypeError):
        return None

    try:
        content = data["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError):
        return None
    if not isinstance(content, str):
        return None
    return re.sub(r"\n{3,}", "\n\n", content).strip()


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
    max_clusters = int(digest_config(config).get("max_clusters", 30))
    return [cluster for _dt, cluster in selected[-max_clusters:]]


def digest_context(clusters: list[dict[str, object]], config: dict[str, object]) -> str:
    settings = digest_config(config)
    max_articles = int(settings.get("max_articles_per_cluster", 5))
    blocks: list[str] = []
    for index, cluster in enumerate(clusters, start=1):
        articles = publishable_articles(cluster, config)
        block = [
            f"{index}. {item_title(cluster, len(articles))}",
            "기사:",
        ]
        for article in articles[:max_articles]:
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


def digest_group_tokens(article: dict[str, object]) -> set[str]:
    text = f"{article.get('clean_title') or article.get('title') or ''} {article.get('summary') or ''}"
    tokens = {
        token.casefold()
        for token in re.findall(r"[가-힣A-Za-z0-9]{2,}", text)
        if token.casefold() not in DIGEST_GROUP_STOPWORDS
    }
    for company in article.get("company_candidates") or []:
        value = str(company).strip().casefold()
        if value:
            tokens.add(value)
    return tokens


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


def add_duplicate_entries(
    entries: dict[str, list[dict[str, object]]],
    duplicate_records: list[dict[str, object]],
    config: dict[str, object],
    seen_urls: set[str],
) -> None:
    for record in duplicate_records:
        for article in duplicate_record_candidates(record):
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
    max_articles_per_cluster = int(settings.get("max_articles_per_cluster", 2))
    entries: dict[str, list[dict[str, object]]] = {"domestic": [], "global": []}
    seen_urls: set[str] = set()

    for cluster in clusters:
        added_for_cluster = 0
        for article in publishable_articles(cluster, config):
            if added_for_cluster >= max_articles_per_cluster:
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
    max_per_section = int(settings.get("max_links_per_section", 12))
    max_total = int(settings.get("max_links_total", 24))

    limited = {
        "domestic": entries["domestic"][:max_per_section],
        "global": entries["global"][:max_per_section],
    }
    while len(limited["domestic"]) + len(limited["global"]) > max_total:
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
) -> bool:
    left_title = str(left.get("title") or "")
    right_title = str(right.get("title") or "")
    title_score = fuzz.token_set_ratio(left_title, right_title)
    if title_score >= 82:
        return True

    left_tokens = set(left.get("tokens") or [])
    right_tokens = set(right.get("tokens") or [])
    overlap = left_tokens & right_tokens
    distinctive_overlap = {
        token
        for token in overlap
        if len(str(token)) >= 2 and str(token).casefold() not in DIGEST_GROUP_STOPWORDS
    }
    if len(distinctive_overlap) >= 3 and title_score >= 58:
        return True
    if len(distinctive_overlap) >= 4:
        return True
    return False


def group_digest_entries(entries: list[dict[str, object]]) -> list[list[dict[str, object]]]:
    groups: list[list[dict[str, object]]] = []
    for entry in entries:
        matched_group: list[dict[str, object]] | None = None
        for group in groups:
            if any(digest_entries_are_same_story(entry, existing) for existing in group):
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


def render_digest_entry_group(group: list[dict[str, object]], config: dict[str, object]) -> list[str]:
    if len(group) == 1:
        entry = group[0]
        return [f"• {html_link(str(entry['label']), str(entry['url']))}"]

    max_links = int(digest_config(config).get("max_links_per_group", 5))
    title = digest_group_title(group, config)
    lines = [f"• {digest_group_date_label(group, config)} / {escape(title, quote=False)} ({len(group)}건)"]
    links = []
    for index, entry in enumerate(group[:max_links], start=1):
        article = entry["article"]
        source = article_source_label(article)  # type: ignore[arg-type]
        links.append(html_link(numbered_digest_source(index, source), str(entry["url"])))
    if links:
        line = "  " + " · ".join(links)
        remaining = len(group) - len(links)
        if remaining > 0:
            line += f" · 외 {remaining}건"
        lines.append(line)
    return lines


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
    for group in group_digest_entries(entries):
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
        for group in group_digest_entries(section_entries):
            lines.extend(render_digest_entry_group(group, config))
    return lines


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
    max_links = int(digest_config(config).get("max_duplicate_links", 12))
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
    lines = [
        f"<b>데일리 주주·자본시장 브리핑 ({start_label}-{end_label})</b>",
        "",
        "<b>요약</b>",
        *summary_bullet_lines(review, config),
        "",
        *render_digest_link_sections(clusters, config, duplicate_records or []),
    ]
    message = "\n".join(line for line in lines if line is not None).strip()
    return split_plain_telegram_text(message, max_chars)


def build_hourly_update_messages(
    clusters: list[dict[str, object]],
    config: dict[str, object],
    now: datetime,
    start_at: datetime,
    duplicates: list[dict[str, object]] | None = None,
) -> list[str]:
    max_chars = int(digest_config(config).get("max_message_chars", 3900))
    review = generate_hourly_digest_review(clusters, config, start_at, now)
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    title_label = now.astimezone(ZoneInfo(timezone_name)).strftime("%m.%d %H:%M")
    lines = [
        f"<b>주주·자본시장 브리핑 ({title_label})</b>",
        "",
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
