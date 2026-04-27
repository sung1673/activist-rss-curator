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
    send_telegram_message,
    telegram_bot_token,
    telegram_chat_id,
    html_link,
    telegram_is_configured,
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


def digest_article_is_global(article: dict[str, object]) -> bool:
    feed_category = str(article.get("feed_category") or "").casefold()
    if feed_category == "global":
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


def digest_article_entries(
    clusters: list[dict[str, object]],
    config: dict[str, object],
) -> dict[str, list[dict[str, object]]]:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    settings = digest_config(config)
    max_articles_per_cluster = int(settings.get("max_articles_per_cluster", 2))
    entries: dict[str, list[dict[str, object]]] = {"domestic": [], "global": []}
    seen_urls: set[str] = set()

    for cluster in clusters:
        added_for_cluster = 0
        for article in publishable_articles(cluster, config):
            if added_for_cluster >= max_articles_per_cluster:
                break
            url = article_link(article)
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            article_dt = digest_article_datetime(article, cluster, timezone_name)
            section = "global" if digest_article_is_global(article) else "domestic"
            entries[section].append(
                {
                    "article": article,
                    "cluster": cluster,
                    "datetime": article_dt,
                    "label": digest_article_label(article, cluster, config),
                    "title": digest_article_title(article),
                    "tokens": digest_group_tokens(article),
                    "url": url,
                }
            )
            added_for_cluster += 1

    for section_entries in entries.values():
        section_entries.sort(key=lambda entry: entry["datetime"] or datetime.min.replace(tzinfo=ZoneInfo("UTC")), reverse=True)
    return entries


def limited_digest_article_entries(
    clusters: list[dict[str, object]],
    config: dict[str, object],
) -> dict[str, list[dict[str, object]]]:
    entries = digest_article_entries(clusters, config)
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


def render_digest_entry_group(group: list[dict[str, object]], config: dict[str, object]) -> list[str]:
    if len(group) == 1:
        entry = group[0]
        return [f"• {html_link(str(entry['label']), str(entry['url']))}"]

    max_links = int(digest_config(config).get("max_links_per_group", 5))
    title = digest_group_title(group, config)
    lines = [f"• {digest_group_date_label(group, config)} / {escape(title, quote=False)} ({len(group)}건)"]
    links = []
    for entry in group[:max_links]:
        article = entry["article"]
        source = article_source_label(article)  # type: ignore[arg-type]
        links.append(html_link(source, str(entry["url"])))
    if links:
        line = "  링크: " + " · ".join(links)
        remaining = len(group) - len(links)
        if remaining > 0:
            line += f" · 외 {remaining}건"
        lines.append(line)
    return lines


def summary_bullet_lines(text: str, config: dict[str, object]) -> list[str]:
    settings = digest_config(config)
    max_bullets = int(settings.get("summary_bullets", 3))
    max_chars = int(settings.get("summary_bullet_max_chars", 72))

    candidates: list[str] = []
    for raw_line in re.split(r"[\n\r]+", text):
        line = re.sub(r"^\s*(?:[-*•]|\d+[.)])\s*", "", raw_line).strip()
        if line:
            candidates.append(line)
    if not candidates:
        candidates = [part.strip() for part in re.split(r"(?<=[.!?。])\s+|(?<=다\.)\s+", text) if part.strip()]

    bullets: list[str] = []
    for line in candidates:
        if not line:
            continue
        bullets.append(f"- {escape(compact_text(line, max_chars=max_chars), quote=False)}")
        if len(bullets) >= max_bullets:
            break
    return bullets or ["- 주요 기사 흐름을 짧게 정리했음"]


def fallback_daily_digest(
    clusters: list[dict[str, object]],
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
) -> str:
    entries = limited_digest_article_entries(clusters, config)
    domestic_count = len(entries["domestic"])
    global_count = len(entries["global"])
    lines = []
    if domestic_count:
        lines.append("- 국내는 주주환원·지배구조 이슈가 이어졌음")
    if global_count:
        lines.append(f"- 해외는 행동주의·주주권 흐름을 같이 볼 만했음")
    lines.append(f"- 링크 {domestic_count + global_count}건만 추려서 읽기 좋게 정리했음")
    return "\n".join(lines[:3])


def render_digest_link_sections(
    clusters: list[dict[str, object]],
    config: dict[str, object],
) -> list[str]:
    entries = limited_digest_article_entries(clusters, config)
    labels = {"domestic": "국내", "global": "해외"}
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
        "- 문장 끝은 '~했음', '~보였음', '~이어졌음'처럼 간결한 메모체로 작성\n"
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
    return content or fallback_daily_digest(clusters, config, start_at, end_at)


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
) -> list[str]:
    max_chars = int(digest_config(config).get("max_message_chars", 3900))
    review = generate_daily_digest_review(clusters, config, start_at, now)
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    start_label = start_at.astimezone(ZoneInfo(timezone_name)).strftime("%m.%d")
    end_label = now.astimezone(ZoneInfo(timezone_name)).strftime("%m.%d")
    lines = [
        f"<b>데일리 거버넌스 리뷰 ({start_label}-{end_label})</b>",
        "",
        "<b>요약</b>",
        *summary_bullet_lines(review, config),
        "",
        *render_digest_link_sections(clusters, config),
    ]
    message = "\n".join(line for line in lines if line is not None).strip()
    return split_plain_telegram_text(message, max_chars)


def publish_daily_digest_if_due(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
) -> dict[str, int]:
    settings = digest_config(config)
    if not settings.get("enabled", True) or not telegram_is_configured(config):
        return {"daily_digest_sent": 0, "daily_digest_failed": 0}

    send_hour = int(settings.get("send_hour", 7))
    send_window_minutes = int(settings.get("send_window_minutes", 59))
    if now.hour != send_hour or now.minute > send_window_minutes:
        return {"daily_digest_sent": 0, "daily_digest_failed": 0}

    digest_id = now.strftime("%Y-%m-%d")
    if digest_id in {str(value) for value in state.get("daily_digest_sent_dates", [])}:
        return {"daily_digest_sent": 0, "daily_digest_failed": 0}

    start_at = now - timedelta(hours=int(settings.get("window_hours", 24)))
    clusters = digest_clusters_in_window(state, config, start_at, now)
    if not clusters:
        return {"daily_digest_sent": 0, "daily_digest_failed": 0}

    bot_token = telegram_bot_token()
    chat_id = telegram_chat_id(config)
    message_ids: list[object] = []
    failed = 0
    for message in build_daily_digest_messages(clusters, config, now, start_at):
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
