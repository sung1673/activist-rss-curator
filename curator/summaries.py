from __future__ import annotations

import os
import re
from datetime import datetime, timedelta
from html import escape
from typing import Any

import httpx

from .dates import datetime_to_iso, format_kst, parse_datetime
from .rss_writer import (
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
    telegram_is_configured,
    telegram_section_label,
)


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


def clean_summary_lines(text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in text.splitlines():
        line = re.sub(r"^\s*(?:[-*•]|\d+[.)])\s*", "", raw_line).strip()
        if line:
            lines.append(compact_text(line, max_chars=120))
    return lines[:2]


def fallback_cluster_summary(cluster: dict[str, object], config: dict[str, object]) -> list[str]:
    articles = publishable_articles(cluster, config)
    section = telegram_section_label(cluster)
    title = compact_text(cluster.get("representative_title") or item_title(cluster, len(articles)), max_chars=95)
    sources = ", ".join(article_source_label(article) for article in articles[:3])
    return [
        compact_text(f"{section} 흐름에서 {title} 이슈가 포착됐습니다.", max_chars=120),
        compact_text(f"주요 기사 {len(articles)}건을 묶었고, 출처는 {sources or '복수 매체'}입니다.", max_chars=120),
    ]


def cluster_prompt_context(cluster: dict[str, object], config: dict[str, object]) -> str:
    articles = publishable_articles(cluster, config)[:5]
    lines = [
        f"섹션: {telegram_section_label(cluster)}",
        f"대표 제목: {cluster.get('representative_title') or item_title(cluster, len(articles))}",
        f"기사 수: {len(publishable_articles(cluster, config))}",
        "기사:",
    ]
    for index, article in enumerate(articles, start=1):
        source = article_source_label(article)
        title = display_article_title(article, source)
        summary = compact_text(article.get("summary") or "", max_chars=180)
        lines.append(f"{index}. {source} - {title}")
        if summary:
            lines.append(f"   요약/본문 일부: {summary}")
    return "\n".join(lines)


def summarize_cluster(cluster: dict[str, object], config: dict[str, object]) -> list[str]:
    settings = ai_config(config)
    if not settings.get("cluster_summary_enabled", True):
        return []
    model = str(settings.get("cluster_summary_model") or "openai/gpt-4o-mini")
    max_tokens = int(settings.get("cluster_summary_max_tokens", 180))
    system_prompt = (
        "당신은 한국어 자본시장 뉴스 에디터입니다. "
        "행동주의, 주주권, 거버넌스 관점에서 텔레그램 메시지에 넣을 2줄 요약을 작성합니다. "
        "분류, 기준시각, 링크, 투자권유 문구는 쓰지 않습니다."
    )
    user_prompt = (
        "아래 기사 묶음을 한국어로 정확히 2줄 요약하세요. "
        "각 줄은 80자 안팎으로 간결하게 쓰고, 과장 없이 핵심 쟁점과 의미를 분리하세요.\n\n"
        f"{cluster_prompt_context(cluster, config)}"
    )
    content = call_github_models(
        system_prompt,
        user_prompt,
        model=model,
        max_tokens=max_tokens,
        config=config,
    )
    lines = clean_summary_lines(content or "")
    return lines or fallback_cluster_summary(cluster, config)


def ensure_cluster_summaries(clusters: list[dict[str, object]], config: dict[str, object]) -> None:
    settings = ai_config(config)
    if not settings.get("cluster_summary_enabled", True):
        return
    limit = int(settings.get("cluster_summary_max_clusters_per_run", 8))
    summarized = 0
    for cluster in clusters:
        if summarized >= limit:
            return
        if cluster.get("summary_lines"):
            continue
        cluster["summary_lines"] = summarize_cluster(cluster, config)
        cluster["summary_model"] = str(settings.get("cluster_summary_model") or "openai/gpt-4o-mini")
        summarized += 1


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
            f"{index}. [{telegram_section_label(cluster)}] {item_title(cluster, len(articles))}",
            f"요약 후보: {' / '.join(str(line) for line in list(cluster.get('summary_lines') or [])[:2])}",
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


def fallback_daily_digest(
    clusters: list[dict[str, object]],
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
) -> str:
    by_section: dict[str, list[dict[str, object]]] = {}
    for cluster in clusters:
        by_section.setdefault(telegram_section_label(cluster), []).append(cluster)

    lines = [
        "데일리 거버넌스 리뷰",
        f"기간: {format_kst(start_at, str(config.get('timezone') or 'Asia/Seoul'))} - {format_kst(end_at, str(config.get('timezone') or 'Asia/Seoul'))}",
        f"수집 묶음: {len(clusters)}개",
        "",
    ]
    for section, section_clusters in by_section.items():
        lines.append(f"[{section}]")
        for cluster in section_clusters[:6]:
            articles = publishable_articles(cluster, config)
            lines.append(f"- {compact_text(item_title(cluster, len(articles)), max_chars=100)}")
            summary_lines = [str(line) for line in (cluster.get("summary_lines") or []) if str(line).strip()]
            if summary_lines:
                lines.append(f"  {compact_text(' '.join(summary_lines[:2]), max_chars=140)}")
        lines.append("")
    return "\n".join(lines).strip()


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
        "전날부터 오늘 오전까지의 기사 묶음을 바탕으로 텔레그램 채널용 데일리 리뷰를 한국어로 작성합니다. "
        "투자 조언이나 매매 권유는 하지 말고, 기사에 없는 사실을 단정하지 마세요."
    )
    user_prompt = (
        "아래 수집 묶음을 바탕으로 데일리 digest를 작성하세요.\n"
        "- 5~8개 항목으로 정리\n"
        "- 카테고리/묶음별 의미와 시장 맥락을 설명\n"
        "- 링크, 기준시각, high/medium 같은 내부 분류는 쓰지 않음\n"
        "- 해외 기사는 한국 투자자 관점의 함의를 한국어로 풀어씀\n\n"
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
    escaped = escape(review, quote=False)
    return split_plain_telegram_text(escaped, max_chars)


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
