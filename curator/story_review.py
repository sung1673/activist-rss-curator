from __future__ import annotations

import hashlib
import json
import os
import re
from collections import defaultdict
from datetime import datetime
from html import escape, unescape
from pathlib import Path
from zoneinfo import ZoneInfo

from rapidfuzz import fuzz

from .cluster import extract_company_candidates
from .dates import format_kst, parse_datetime
from .normalize import canonical_url_hash
from .story_signature import event_tokens_for_text, story_signature_decision
from .summaries import digest_tokens_from_text
from .telegram_sources import canonicalize_telegram_url, extract_urls, is_boilerplate_signal_url, normalize_channel_handle


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FEED_DIR = Path("public") / "feed"
REVIEW_PAGE_NAME = "story-review.html"
REVIEW_META_NAME = "story-review-meta.json"

GENERAL_TOKENS = {
    "관련",
    "기사",
    "뉴스",
    "보도",
    "시장",
    "자본시장",
    "기업",
    "주주",
    "단독",
    "종합",
    "속보",
    "확인",
    "논란",
    "전망",
    "가능성",
    "google",
    "news",
}


def story_review_config(config: dict[str, object]) -> dict[str, object]:
    value = config.get("story_review", {})
    return value if isinstance(value, dict) else {}


def story_review_enabled(config: dict[str, object]) -> bool:
    return bool(story_review_config(config).get("enabled", True))


def public_base_url(config: dict[str, object]) -> str:
    feed_url = str(config.get("public_feed_url") or "").strip()
    if feed_url.endswith("/feed.xml"):
        return feed_url[: -len("/feed.xml")]
    if feed_url.endswith("feed.xml"):
        return feed_url[: -len("feed.xml")].rstrip("/")
    return feed_url.rstrip("/")


def story_review_public_url(config: dict[str, object]) -> str:
    base_url = public_base_url(config)
    if not base_url:
        return f"feed/{REVIEW_PAGE_NAME}"
    return f"{base_url}/feed/{REVIEW_PAGE_NAME}"


def story_review_access_token() -> str:
    return os.environ.get("STORY_REVIEW_ACCESS_TOKEN", "").strip()


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest() if token else ""


def clean_text(value: object) -> str:
    text = str(value or "")
    for _ in range(3):
        decoded = unescape(text)
        if decoded == text:
            break
        text = decoded
    text = re.sub(r"<\s*/?\s*[a-zA-Z][^>]*>", "", text)
    return re.sub(r"\s+", " ", text).strip()


def text_has_encoding_damage(value: object) -> bool:
    text = str(value or "")
    if not text:
        return False
    damaged = sum(1 for char in text if char == "\ufffd" or 0xD800 <= ord(char) <= 0xDFFF)
    if damaged:
        return True
    korean = sum(1 for char in text if "\uac00" <= char <= "\ud7a3")
    cjk = sum(1 for char in text if "\u4e00" <= char <= "\u9fff")
    if cjk >= 4 and korean == 0 and re.search(r"[\udc00-\udfff]", text):
        return True
    return False


def compact(value: object, max_chars: int = 96) -> str:
    text = clean_text(value)
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 1)].rstrip() + "…"


def story_text(story: dict[str, object]) -> str:
    links = story.get("links") if isinstance(story.get("links"), list) else []
    link_titles = " ".join(clean_text(link.get("title")) for link in links if isinstance(link, dict))
    return clean_text(
        " ".join(
            [
                str(story.get("title") or ""),
                str(story.get("summary") or ""),
                str(story.get("db_query") or ""),
                link_titles,
            ]
        )
    )


def story_company_tokens(story: dict[str, object]) -> set[str]:
    values = extract_company_candidates(story_text(story))
    return {clean_text(value).casefold() for value in values if clean_text(value)}


def story_event_tokens(story: dict[str, object], config: dict[str, object]) -> set[str]:
    text = story_text(story)
    tokens = {str(token).casefold() for token in event_tokens_for_text(text, config)}
    tokens.update(
        str(token).casefold()
        for token in digest_tokens_from_text(text)
        if str(token).casefold() not in GENERAL_TOKENS
    )
    return tokens


def story_title_tokens(story: dict[str, object]) -> set[str]:
    return {
        token.casefold()
        for token in re.findall(r"[0-9A-Za-z가-힣]{2,}", clean_text(story.get("title")))
        if token.casefold() not in GENERAL_TOKENS
    }


def story_date_label(value: object, timezone_name: str) -> str:
    if isinstance(value, datetime):
        return value.astimezone(ZoneInfo(timezone_name)).strftime("%m.%d %H:%M")
    return ""


def story_source_summary(story: dict[str, object], limit: int = 5) -> str:
    links = story.get("links") if isinstance(story.get("links"), list) else []
    sources = []
    for link in links:
        if not isinstance(link, dict):
            continue
        source = clean_text(link.get("source"))
        if source and source not in sources:
            sources.append(source)
        if len(sources) >= limit:
            break
    return " · ".join(sources)


def story_primary_link(story: dict[str, object]) -> str:
    primary = str(story.get("primary_url") or "").strip()
    if primary:
        return primary
    links = story.get("links") if isinstance(story.get("links"), list) else []
    for link in links:
        if isinstance(link, dict) and link.get("url"):
            return str(link["url"])
    return ""


def story_url_hashes(stories: list[dict[str, object]]) -> set[str]:
    hashes: set[str] = set()
    for story in stories:
        if not isinstance(story, dict):
            continue
        urls = [story_primary_link(story)]
        links = story.get("links") if isinstance(story.get("links"), list) else []
        urls.extend(str(link.get("url") or "") for link in links if isinstance(link, dict))
        for url in urls:
            canonical_url = canonicalize_telegram_url(str(url or ""))
            if canonical_url:
                hashes.add(canonical_url_hash(canonical_url))
    return hashes


def article_status_index(state: dict[str, object]) -> dict[str, dict[str, object]]:
    indexed: dict[str, dict[str, object]] = {}
    for article in state.get("articles", []):
        if not isinstance(article, dict):
            continue
        url = str(article.get("canonical_url") or article.get("link") or "")
        canonical_url = canonicalize_telegram_url(url)
        url_hash = str(article.get("canonical_url_hash") or "")
        if not url_hash and canonical_url:
            url_hash = canonical_url_hash(canonical_url)
        if not url_hash:
            continue
        indexed[url_hash] = {
            "status": clean_text(article.get("status") or "unknown"),
            "reason": clean_text(article.get("reason") or article.get("reject_reason") or article.get("duplicate_reason") or ""),
            "title": compact(article.get("clean_title") or article.get("title"), max_chars=120),
            "url": canonical_url or url,
        }
    return indexed


def benchmark_handles(config: dict[str, object]) -> set[str]:
    settings = story_review_config(config)
    handles = settings.get("benchmark_handles")
    if not isinstance(handles, list) or not handles:
        telegram_settings = config.get("telegram_sources", {})
        if isinstance(telegram_settings, dict):
            handles = telegram_settings.get("candidate_source_handles")
    return {
        normalize_channel_handle(handle)
        for handle in (handles if isinstance(handles, list) else [])
        if normalize_channel_handle(handle)
    }


def message_reference_datetime(message: dict[str, object], timezone_name: str) -> datetime | None:
    for key in ("posted_at", "collected_at", "edited_at"):
        parsed = parse_datetime(message.get(key), timezone_name)
        if parsed:
            return parsed
    return None


def message_public_url(message: dict[str, object]) -> str:
    url = str(message.get("message_url") or "").strip()
    if url:
        return url
    handle = normalize_channel_handle(message.get("handle") or message.get("username") or message.get("channel_handle"))
    message_id = str(message.get("telegram_message_id") or message.get("id") or "").strip()
    if handle and message_id:
        return f"https://t.me/{handle}/{message_id}"
    return ""


def benchmark_title_from_message(message: dict[str, object], url: str) -> str:
    text = str(message.get("text") or message.get("normalized_text") or "")
    text = re.sub(r"https?://\S+", " ", text)
    for line in text.splitlines():
        cleaned = compact(line, max_chars=120)
        if len(cleaned) >= 8:
            return cleaned
    host = re.sub(r"^www\.", "", re.sub(r"^https?://", "", url).split("/", 1)[0])
    return host or "Telegram shared URL"


def build_benchmark_coverage(
    stories: list[dict[str, object]],
    state: dict[str, object],
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
) -> dict[str, object]:
    handles = benchmark_handles(config)
    if not handles:
        return {"enabled": False, "handles": [], "url_count": 0, "matched_count": 0, "missing_count": 0, "coverage_rate": 0.0, "missing": []}
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    story_hashes = story_url_hashes(stories)
    status_index = article_status_index(state)
    max_missing = int(story_review_config(config).get("benchmark_max_missing", 12))
    observed: dict[str, dict[str, object]] = {}
    channels_by_hash: dict[str, set[str]] = defaultdict(set)
    messages_by_hash: dict[str, list[dict[str, object]]] = defaultdict(list)

    for message in state.get("telegram_source_messages", []):
        if not isinstance(message, dict) or message.get("deleted_at"):
            continue
        handle = normalize_channel_handle(message.get("handle") or message.get("username") or message.get("channel_handle"))
        if handle not in handles:
            continue
        message_dt = message_reference_datetime(message, timezone_name)
        if message_dt and not (start_at <= message_dt <= end_at):
            continue
        raw_url_value = message.get("urls")
        if isinstance(raw_url_value, list):
            raw_urls = [str(url) for url in raw_url_value if str(url).strip()]
        elif isinstance(raw_url_value, str) and raw_url_value.strip():
            raw_urls = [raw_url_value.strip()]
        else:
            raw_urls = []
        if not raw_urls:
            raw_urls = extract_urls(str(message.get("text") or message.get("normalized_text") or ""))
        for raw_url in raw_urls:
            canonical_url = canonicalize_telegram_url(str(raw_url or ""))
            if not canonical_url or is_boilerplate_signal_url(canonical_url):
                continue
            url_hash = canonical_url_hash(canonical_url)
            channel_name = clean_text(message.get("channel_title") or message.get("title") or handle)
            channels_by_hash[url_hash].add(channel_name or handle)
            messages_by_hash[url_hash].append(message)
            row = observed.setdefault(
                url_hash,
                {
                    "url_hash": url_hash,
                    "url": canonical_url,
                    "title": benchmark_title_from_message(message, canonical_url),
                    "first_seen_at": message_dt.isoformat() if message_dt else "",
                    "latest_seen_at": message_dt.isoformat() if message_dt else "",
                    "source_message_url": message_public_url(message),
                    "source_handle": handle,
                    "source_channel": channel_name or handle,
                    "status": "not_in_state",
                    "reason": "",
                },
            )
            if message_dt:
                current_latest = parse_datetime(row.get("latest_seen_at"), timezone_name)
                if not current_latest or message_dt > current_latest:
                    row["latest_seen_at"] = message_dt.isoformat()
                    row["source_message_url"] = message_public_url(message)
                    row["source_handle"] = handle
                    row["source_channel"] = channel_name or handle
                    row["title"] = benchmark_title_from_message(message, canonical_url)

    rows = list(observed.values())
    for row in rows:
        url_hash = str(row.get("url_hash") or "")
        status = status_index.get(url_hash, {})
        if status:
            row["status"] = status.get("status") or "unknown"
            row["reason"] = status.get("reason") or ""
            if status.get("title"):
                row["article_title"] = status.get("title")
        row["channel_count"] = len(channels_by_hash[url_hash])
        row["message_count"] = len(messages_by_hash[url_hash])
        row["channels"] = sorted(channels_by_hash[url_hash])[:6]
        row["matched"] = url_hash in story_hashes
        row["latest_label"] = story_date_label(parse_datetime(row.get("latest_seen_at"), timezone_name), timezone_name)

    missing = [row for row in rows if not row.get("matched")]
    missing.sort(key=lambda item: str(item.get("latest_seen_at") or ""), reverse=True)
    matched_count = len(rows) - len(missing)
    coverage_rate = round((matched_count / len(rows)) * 100, 1) if rows else 0.0
    return {
        "enabled": True,
        "handles": sorted(handles),
        "url_count": len(rows),
        "matched_count": matched_count,
        "missing_count": len(missing),
        "coverage_rate": coverage_rate,
        "missing": missing[:max_missing],
    }


def story_for_review(story: dict[str, object], timezone_name: str) -> dict[str, object]:
    links = story.get("links") if isinstance(story.get("links"), list) else []
    return {
        "id": str(story.get("id") or ""),
        "title": clean_text(story.get("title")),
        "summary": compact(story.get("summary"), max_chars=160),
        "category": clean_text(story.get("category")),
        "datetime": story_date_label(story.get("datetime"), timezone_name),
        "source_line": story_source_summary(story),
        "link_count": int(story.get("link_count") or len(links)),
        "url": story_primary_link(story),
        "links": [
            {
                "source": clean_text(link.get("source")),
                "title": clean_text(link.get("title")),
                "url": str(link.get("url") or ""),
            }
            for link in links[:5]
            if isinstance(link, dict)
        ],
    }


def candidate_reason(
    company_overlap: set[str],
    event_overlap: set[str],
    title_score: float,
    text_score: float,
    same_story_key: bool,
) -> str:
    reasons: list[str] = []
    if same_story_key:
        reasons.append("같은 story key")
    if company_overlap:
        reasons.append("회사명 겹침")
    if event_overlap:
        reasons.append("사건 토큰 겹침")
    if title_score >= 68:
        reasons.append("제목 유사도 높음")
    elif text_score >= 58:
        reasons.append("본문/요약 유사도 높음")
    return " · ".join(reasons) or "유사 후보"


def score_story_pair(
    left: dict[str, object],
    right: dict[str, object],
    config: dict[str, object],
) -> dict[str, object] | None:
    left_url = story_primary_link(left)
    right_url = story_primary_link(right)
    if left_url and right_url and left_url == right_url:
        return None

    left_title = clean_text(left.get("title"))
    right_title = clean_text(right.get("title"))
    if not left_title or not right_title:
        return None
    if text_has_encoding_damage(left.get("title")) or text_has_encoding_damage(right.get("title")):
        return None

    title_score = float(fuzz.token_set_ratio(left_title, right_title))
    text_score = float(fuzz.token_set_ratio(story_text(left)[:700], story_text(right)[:700]))
    company_overlap = story_company_tokens(left) & story_company_tokens(right)
    event_overlap = story_event_tokens(left, config) & story_event_tokens(right, config)
    title_overlap = story_title_tokens(left) & story_title_tokens(right)
    same_story_key = bool(left.get("story_key") and left.get("story_key") == right.get("story_key"))

    signature = story_signature_decision(
        left_title,
        right_title,
        left_companies=story_company_tokens(left),
        right_companies=story_company_tokens(right),
        left_event_tokens=story_event_tokens(left, config),
        right_event_tokens=story_event_tokens(right, config),
        title_score=title_score,
        config=config,
    )

    if not any(
        (
            same_story_key,
            signature.same_story,
            company_overlap and event_overlap,
            company_overlap and title_score >= 58,
            len(event_overlap) >= 2 and title_score >= 62,
            len(title_overlap) >= 2 and text_score >= 60,
            title_score >= 78,
        )
    ):
        return None

    score = 0.0
    if same_story_key:
        score += 22
    if signature.same_story:
        score += 18
    if signature.reason == "duplicate_listing_policy_signature":
        score += 20
    if company_overlap:
        score += 20
    score += min(22, title_score * 0.22)
    score += min(12, text_score * 0.12)
    score += min(18, len(event_overlap) * 6)
    score += min(8, len(title_overlap) * 2)
    if left.get("category") and left.get("category") == right.get("category"):
        score += 5

    return {
        "score": round(score, 1),
        "title_score": round(title_score, 1),
        "text_score": round(text_score, 1),
        "company_overlap": sorted(company_overlap),
        "event_overlap": sorted(event_overlap),
        "title_overlap": sorted(title_overlap),
        "reason": candidate_reason(company_overlap, event_overlap, title_score, text_score, same_story_key),
        "signature_reason": signature.reason,
    }


def candidate_id(left: dict[str, object], right: dict[str, object]) -> str:
    raw = "|".join(sorted([story_primary_link(left), story_primary_link(right), clean_text(left.get("title")), clean_text(right.get("title"))]))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


def rule_yaml_suggestion(candidate: dict[str, object]) -> str:
    companies = candidate.get("company_overlap") if isinstance(candidate.get("company_overlap"), list) else []
    events = candidate.get("event_overlap") if isinstance(candidate.get("event_overlap"), list) else []
    title_tokens = candidate.get("title_overlap") if isinstance(candidate.get("title_overlap"), list) else []
    rule_id = "review_" + str(candidate.get("id") or "candidate")
    lines = [
        f"- id: {rule_id}",
        "  tokens:",
    ]
    for token in events[:3] or title_tokens[:2] or ["후보토큰"]:
        lines.append(f"    - {token}")
    if companies:
        lines.append("  require_any_groups:")
        lines.append("    - [" + ", ".join(str(company) for company in companies[:3]) + "]")
    if events or title_tokens:
        terms = list(dict.fromkeys([*(str(token) for token in events[:4]), *(str(token) for token in title_tokens[:4])]))
        lines.append("    - [" + ", ".join(terms) + "]")
    return "\n".join(lines)


def build_story_review(
    stories: list[dict[str, object]],
    config: dict[str, object],
    start_at: datetime,
    end_at: datetime,
    date_id: str,
    state: dict[str, object] | None = None,
) -> dict[str, object]:
    settings = story_review_config(config)
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    min_score = float(settings.get("min_score", 72))
    max_candidates = int(settings.get("max_candidates", 12))
    candidates: list[dict[str, object]] = []

    for left_index, left in enumerate(stories):
        if not isinstance(left, dict):
            continue
        for right in stories[left_index + 1 :]:
            if not isinstance(right, dict):
                continue
            pair = score_story_pair(left, right, config)
            if not pair or float(pair["score"]) < min_score:
                continue
            candidate = {
                "id": candidate_id(left, right),
                **pair,
                "left": story_for_review(left, timezone_name),
                "right": story_for_review(right, timezone_name),
            }
            candidate["rule_yaml"] = rule_yaml_suggestion(candidate)
            candidates.append(candidate)

    candidates.sort(key=lambda item: (float(item.get("score") or 0), float(item.get("title_score") or 0)), reverse=True)
    candidates = candidates[:max_candidates]
    candidate_hash = hashlib.sha256(
        json.dumps(
            [
                {
                    "id": candidate.get("id"),
                    "score": candidate.get("score"),
                    "left": (candidate.get("left") or {}).get("title") if isinstance(candidate.get("left"), dict) else "",
                    "right": (candidate.get("right") or {}).get("title") if isinstance(candidate.get("right"), dict) else "",
                }
                for candidate in candidates
            ],
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()[:16]
    return {
        "date_id": date_id,
        "generated_at": datetime.now(ZoneInfo(timezone_name)).isoformat(),
        "start_at": start_at.isoformat(),
        "end_at": end_at.isoformat(),
        "start_label": format_kst(start_at, timezone_name),
        "end_label": format_kst(end_at, timezone_name),
        "candidate_count": len(candidates),
        "candidate_hash": candidate_hash,
        "page_url": story_review_public_url(config),
        "candidates": candidates,
        "benchmark_coverage": build_benchmark_coverage(stories, state or {}, config, start_at, end_at),
        "access_token_hash": token_hash(story_review_access_token()),
    }


def render_candidate_card(candidate: dict[str, object]) -> str:
    left = candidate.get("left") if isinstance(candidate.get("left"), dict) else {}
    right = candidate.get("right") if isinstance(candidate.get("right"), dict) else {}

    def story_block(story: dict[str, object]) -> str:
        links = story.get("links") if isinstance(story.get("links"), list) else []
        link_items = "\n".join(
            f'<li><a href="{escape(str(link.get("url") or ""), quote=True)}" target="_blank" rel="noopener">{escape(compact(link.get("title"), 84))}</a><span>{escape(clean_text(link.get("source")))}</span></li>'
            for link in links[:3]
            if isinstance(link, dict)
        )
        if not link_items:
            link_items = "<li><span>링크 없음</span></li>"
        return f"""
          <section class="story-mini">
            <div class="story-mini__meta">{escape(clean_text(story.get("category")))} · {escape(clean_text(story.get("datetime")))} · {escape(str(story.get("link_count") or 0))}건</div>
            <h3><a href="{escape(str(story.get("url") or ""), quote=True)}" target="_blank" rel="noopener">{escape(clean_text(story.get("title")))}</a></h3>
            <p>{escape(clean_text(story.get("summary")))}</p>
            <div class="story-mini__sources">{escape(clean_text(story.get("source_line")))}</div>
            <ul>{link_items}</ul>
          </section>
        """

    return f"""
      <article class="candidate" data-candidate-id="{escape(str(candidate.get("id") or ""), quote=True)}">
        <div class="candidate__head">
          <div>
            <span class="candidate__score">점수 {escape(str(candidate.get("score") or ""))}</span>
            <strong>{escape(clean_text(candidate.get("reason")))}</strong>
          </div>
          <label><input type="checkbox" data-review-done> 확인 완료</label>
        </div>
        <div class="candidate__signals">
          <span>제목 {escape(str(candidate.get("title_score") or 0))}</span>
          <span>내용 {escape(str(candidate.get("text_score") or 0))}</span>
          <span>회사 {escape(", ".join(str(v) for v in candidate.get("company_overlap", []) if v) or "-")}</span>
          <span>사건 {escape(", ".join(str(v) for v in candidate.get("event_overlap", []) if v) or "-")}</span>
        </div>
        <div class="candidate__stories">
          {story_block(left)}
          {story_block(right)}
        </div>
        <details class="rule">
          <summary>story_rules.yaml 초안 보기</summary>
          <pre>{escape(str(candidate.get("rule_yaml") or ""))}</pre>
          <button type="button" data-copy-rule>규칙 초안 복사</button>
        </details>
      </article>
    """


def render_benchmark_coverage(coverage: dict[str, object]) -> str:
    if not bool(coverage.get("enabled")):
        return ""
    missing = coverage.get("missing") if isinstance(coverage.get("missing"), list) else []
    handles = ", ".join(str(handle) for handle in coverage.get("handles", []) if handle) if isinstance(coverage.get("handles"), list) else ""
    missing_items = "\n".join(
        f"""
          <article class="benchmark-item">
            <div class="benchmark-item__meta">
              <span>{escape(clean_text(item.get("latest_label") or ""))}</span>
              <span>{escape(clean_text(item.get("source_channel") or item.get("source_handle") or ""))}</span>
              <span>{escape(clean_text(item.get("status") or "not_in_state"))}</span>
              <span>{escape(str(item.get("message_count") or 0))} mentions · {escape(str(item.get("channel_count") or 0))} channels</span>
            </div>
            <h3><a href="{escape(str(item.get("url") or ""), quote=True)}" target="_blank" rel="noopener">{escape(clean_text(item.get("article_title") or item.get("title") or item.get("url")))}</a></h3>
            <p>{escape(clean_text(item.get("reason") or "Daily story에 반영되지 않은 benchmark 공유 URL입니다."))}</p>
            <div class="benchmark-item__links">
              <a href="{escape(str(item.get("source_message_url") or ""), quote=True)}" target="_blank" rel="noopener">Telegram 원문</a>
              <span>{escape(", ".join(str(channel) for channel in item.get("channels", []) if channel) if isinstance(item.get("channels"), list) else "")}</span>
            </div>
          </article>
        """
        for item in missing
        if isinstance(item, dict)
    )
    if not missing_items:
        missing_items = '<div class="empty benchmark-empty">benchmark 채널 공유 URL이 모두 데일리에 반영됐습니다.</div>'
    return f"""
      <section class="benchmark">
        <div class="benchmark__head">
          <div>
            <h2>Benchmark 누락 점검</h2>
            <p>기준 채널: {escape(handles or "-")}</p>
          </div>
          <div class="benchmark__stats">
            <span>URL {escape(str(coverage.get("url_count") or 0))}건</span>
            <span>반영 {escape(str(coverage.get("matched_count") or 0))}건</span>
            <span>누락 {escape(str(coverage.get("missing_count") or 0))}건</span>
            <span>커버리지 {escape(str(coverage.get("coverage_rate") or 0.0))}%</span>
          </div>
        </div>
        <div class="benchmark__list">
          {missing_items}
        </div>
      </section>
    """


def render_story_review_html(review: dict[str, object], *, logo_html: str = "") -> str:
    access_hash = str(review.get("access_token_hash") or "")
    candidates = review.get("candidates") if isinstance(review.get("candidates"), list) else []
    cards = "\n".join(render_candidate_card(candidate) for candidate in candidates if isinstance(candidate, dict))
    if not cards:
        cards = '<div class="empty">오늘은 우선 검토할 분리 후보가 없습니다.</div>'
    benchmark_coverage = review.get("benchmark_coverage") if isinstance(review.get("benchmark_coverage"), dict) else {}
    benchmark_html = render_benchmark_coverage(benchmark_coverage)
    data_json = json.dumps(
        {
            "accessTokenHash": access_hash,
            "candidateCount": int(review.get("candidate_count") or 0),
            "candidateHash": review.get("candidate_hash") or "",
        },
        ensure_ascii=False,
    )
    logo = logo_html or '<a class="brand" href="https://bside.ai">bside <span>DAILY NEWS</span></a>'
    return f"""<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>묶음 후보 관리자 | BSIDE Daily News</title>
  <style>
    :root {{ --ink:#17131f; --muted:#6f6878; --line:#ded7e8; --paper:#fbfafc; --accent:#6b35d8; --soft:#f3eefc; --ok:#007f68; }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; color:var(--ink); background:var(--paper); font-family:-apple-system,BlinkMacSystemFont,"Apple SD Gothic Neo","Segoe UI",sans-serif; }}
    main {{ max-width:1040px; margin:0 auto; padding:28px 18px 72px; }}
    a {{ color:inherit; text-decoration-thickness:1px; text-underline-offset:3px; }}
    .bside-logo, .brand {{ display:inline-flex; align-items:center; gap:8px; color:var(--accent); font-weight:900; letter-spacing:.08em; font-size:13px; text-decoration:none; }}
    .bside-logo__image {{ width:92px; height:auto; display:block; color:var(--accent); }}
    .bside-logo__label, .brand span {{ font-size:11px; }}
    header {{ border-bottom:2px solid var(--ink); padding-bottom:18px; }}
    h1 {{ margin:22px 0 8px; font-family:Georgia,"Times New Roman",serif; font-size:clamp(34px,5vw,56px); line-height:1; letter-spacing:0; }}
    .lead {{ color:var(--muted); line-height:1.7; max-width:760px; }}
    .meta {{ display:flex; flex-wrap:wrap; gap:8px; margin-top:16px; }}
    .meta span, .candidate__signals span {{ border:1px solid var(--line); background:#fff; border-radius:999px; padding:7px 10px; font-size:12px; font-weight:800; color:#3b245f; }}
    .gate {{ margin-top:28px; padding:22px; border:1px solid var(--line); background:#fff; }}
    .gate input {{ width:min(420px,100%); padding:11px 12px; border:1px solid var(--line); border-radius:10px; font:inherit; }}
    button {{ border:1px solid var(--accent); background:#fff; color:var(--accent); border-radius:999px; padding:9px 13px; font-weight:850; cursor:pointer; }}
    .content {{ display:none; margin-top:26px; }}
    .content.is-open {{ display:block; }}
    .toolbar {{ display:flex; justify-content:space-between; align-items:center; gap:12px; border-bottom:1px solid var(--line); padding:0 0 14px; margin-bottom:18px; }}
    .benchmark {{ border:1px solid var(--line); background:#fff; margin:0 0 24px; padding:18px; }}
    .benchmark__head {{ display:flex; justify-content:space-between; gap:16px; align-items:flex-start; border-bottom:1px solid var(--line); padding-bottom:14px; }}
    .benchmark h2 {{ margin:0 0 6px; font-size:22px; }}
    .benchmark p {{ margin:0; color:var(--muted); line-height:1.55; }}
    .benchmark__stats {{ display:flex; flex-wrap:wrap; gap:7px; justify-content:flex-end; }}
    .benchmark__stats span, .benchmark-item__meta span {{ border:1px solid var(--line); border-radius:999px; padding:6px 9px; color:#3b245f; font-size:12px; font-weight:850; background:var(--soft); }}
    .benchmark__list {{ display:grid; gap:10px; margin-top:14px; }}
    .benchmark-item {{ border-left:3px solid var(--accent); background:#fdfcff; padding:12px; }}
    .benchmark-item__meta {{ display:flex; flex-wrap:wrap; gap:6px; margin-bottom:8px; }}
    .benchmark-item h3 {{ margin:0 0 7px; font-size:17px; line-height:1.35; }}
    .benchmark-item p {{ margin:0 0 8px; color:#4a4058; }}
    .benchmark-item__links {{ display:flex; justify-content:space-between; gap:10px; color:var(--muted); font-size:12px; }}
    .candidate {{ border-top:3px solid var(--accent); background:#fff; margin:18px 0 26px; padding:18px; box-shadow:0 18px 45px rgba(80, 45, 130, .06); }}
    .candidate.is-done {{ opacity:.58; }}
    .candidate__head {{ display:flex; justify-content:space-between; gap:12px; align-items:flex-start; }}
    .candidate__head strong {{ display:block; margin-top:6px; }}
    .candidate__score {{ display:inline-flex; color:#fff; background:var(--accent); padding:4px 8px; border-radius:999px; font-size:12px; font-weight:900; }}
    .candidate__signals {{ display:flex; flex-wrap:wrap; gap:7px; margin:14px 0; }}
    .candidate__stories {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:14px; }}
    .story-mini {{ border:1px solid var(--line); background:#fdfcff; padding:14px; min-width:0; }}
    .story-mini__meta, .story-mini__sources {{ color:var(--muted); font-size:12px; font-weight:750; }}
    .story-mini h3 {{ margin:8px 0; font-size:20px; line-height:1.25; word-break:keep-all; }}
    .story-mini p {{ color:#3a3145; line-height:1.55; margin:0 0 10px; }}
    .story-mini ul {{ list-style:none; padding:0; margin:10px 0 0; border-top:1px solid var(--line); }}
    .story-mini li {{ display:grid; grid-template-columns:minmax(0,1fr) 86px; gap:8px; padding:8px 0; border-bottom:1px solid #eee8f5; font-size:13px; }}
    .story-mini li span {{ color:var(--muted); text-align:right; }}
    .rule {{ margin-top:14px; background:var(--soft); border-left:3px solid var(--accent); padding:12px; }}
    .rule pre {{ white-space:pre-wrap; overflow:auto; font-size:12px; }}
    .empty {{ padding:28px; background:#fff; border:1px solid var(--line); color:var(--muted); }}
    .notice {{ color:var(--muted); font-size:13px; line-height:1.6; }}
    @media (max-width:760px) {{
      main {{ padding:20px 14px 60px; }}
      .candidate__stories {{ grid-template-columns:1fr; }}
      .toolbar, .candidate__head, .benchmark__head, .benchmark-item__links {{ flex-direction:column; align-items:stretch; }}
      .story-mini h3 {{ font-size:18px; }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      {logo}
      <h1>묶음 후보 관리자</h1>
      <p class="lead">데일리 기사 중 “같은 이슈로 묶였어야 했는데 분리됐을 가능성이 있는 후보”를 자동으로 추립니다. 이 화면에서 후보를 확인하고, 필요한 경우 <code>data/story_rules.yaml</code> 또는 묶음 로직 보강 작업으로 이어갑니다.</p>
      <div class="meta">
        <span>{escape(str(review.get("date_id") or ""))}</span>
        <span>{escape(str(review.get("start_label") or ""))} - {escape(str(review.get("end_label") or ""))}</span>
        <span>후보 {escape(str(review.get("candidate_count") or 0))}건</span>
      </div>
    </header>

    <section class="gate" id="gate">
      <h2>접근 token 확인</h2>
      <p class="notice">관리자 token을 직접 입력하세요. 이 정적 검수 화면은 인증 API 전환 전까지 공개 Pages 배포 대상에서 제외됩니다.</p>
      <input id="tokenInput" type="password" placeholder="관리자 token">
      <button type="button" id="tokenButton">열기</button>
      <p class="notice" id="tokenMessage"></p>
    </section>

    <section class="content" id="content">
      {benchmark_html}
      <div class="toolbar">
        <div>
          <strong>검토 후보</strong>
          <div class="notice">확인 완료 상태는 현재 브라우저에 저장됩니다.</div>
        </div>
        <a href="latest.html">최신 데일리로 돌아가기</a>
      </div>
      {cards}
    </section>
  </main>
  <script>
    const REVIEW = {data_json};
    const gate = document.getElementById('gate');
    const content = document.getElementById('content');
    const tokenInput = document.getElementById('tokenInput');
    const tokenMessage = document.getElementById('tokenMessage');

    async function sha256(value) {{
      const encoded = new TextEncoder().encode(value || '');
      const digest = await crypto.subtle.digest('SHA-256', encoded);
      return Array.from(new Uint8Array(digest)).map((byte) => byte.toString(16).padStart(2, '0')).join('');
    }}

    async function unlock(rawToken) {{
      if (!REVIEW.accessTokenHash) {{
        gate.style.display = 'none';
        content.classList.add('is-open');
        return true;
      }}
      const hashed = await sha256(rawToken || '');
      if (hashed !== REVIEW.accessTokenHash) {{
        tokenMessage.textContent = 'token이 일치하지 않습니다.';
        return false;
      }}
      sessionStorage.setItem('storyReviewToken', rawToken);
      gate.style.display = 'none';
      content.classList.add('is-open');
      return true;
    }}

    const storedToken = sessionStorage.getItem('storyReviewToken');
    if (storedToken || !REVIEW.accessTokenHash) {{
      unlock(storedToken || '');
    }}
    document.getElementById('tokenButton').addEventListener('click', () => unlock(tokenInput.value));
    tokenInput.addEventListener('keydown', (event) => {{
      if (event.key === 'Enter') unlock(tokenInput.value);
    }});

    for (const card of document.querySelectorAll('.candidate')) {{
      const id = card.dataset.candidateId || '';
      const doneKey = `storyReviewDone:${{id}}`;
      const checkbox = card.querySelector('[data-review-done]');
      if (localStorage.getItem(doneKey) === '1') {{
        card.classList.add('is-done');
        checkbox.checked = true;
      }}
      checkbox.addEventListener('change', () => {{
        if (checkbox.checked) {{
          localStorage.setItem(doneKey, '1');
          card.classList.add('is-done');
        }} else {{
          localStorage.removeItem(doneKey);
          card.classList.remove('is-done');
        }}
      }});
      const copyButton = card.querySelector('[data-copy-rule]');
      if (copyButton) {{
        copyButton.addEventListener('click', async () => {{
          const pre = card.querySelector('pre');
          await navigator.clipboard.writeText(pre ? pre.textContent : '');
          copyButton.textContent = '복사됨';
          setTimeout(() => copyButton.textContent = '규칙 초안 복사', 1200);
        }});
      }}
    }}
  </script>
</body>
</html>
"""


def write_story_review_files(
    review: dict[str, object],
    root: Path | None = None,
    *,
    logo_html: str = "",
) -> list[Path]:
    project_root = root or PROJECT_ROOT
    feed_dir = project_root / FEED_DIR
    feed_dir.mkdir(parents=True, exist_ok=True)
    page_path = feed_dir / REVIEW_PAGE_NAME
    meta_path = feed_dir / REVIEW_META_NAME
    html = render_story_review_html(review, logo_html=logo_html)
    page_path.write_text("\n".join(line.rstrip() for line in html.splitlines()) + "\n", encoding="utf-8", newline="\n")
    meta = {
        key: review.get(key)
        for key in ("date_id", "generated_at", "start_at", "end_at", "start_label", "end_label", "candidate_count", "candidate_hash", "page_url")
    }
    candidates = review.get("candidates") if isinstance(review.get("candidates"), list) else []
    meta["candidates"] = [
        {
            "id": candidate.get("id"),
            "score": candidate.get("score"),
            "left": candidate.get("left"),
            "right": candidate.get("right"),
        }
        for candidate in candidates[:3]
        if isinstance(candidate, dict)
    ]
    benchmark = review.get("benchmark_coverage") if isinstance(review.get("benchmark_coverage"), dict) else {}
    if benchmark:
        missing = benchmark.get("missing") if isinstance(benchmark.get("missing"), list) else []
        meta["benchmark_coverage"] = {
            "enabled": bool(benchmark.get("enabled")),
            "handles": benchmark.get("handles") if isinstance(benchmark.get("handles"), list) else [],
            "url_count": int(benchmark.get("url_count") or 0),
            "matched_count": int(benchmark.get("matched_count") or 0),
            "missing_count": int(benchmark.get("missing_count") or 0),
            "coverage_rate": float(benchmark.get("coverage_rate") or 0.0),
            "missing": [
                {
                    "title": item.get("article_title") or item.get("title") or "",
                    "url": item.get("url") or "",
                    "source_message_url": item.get("source_message_url") or "",
                    "source_channel": item.get("source_channel") or item.get("source_handle") or "",
                    "status": item.get("status") or "not_in_state",
                    "reason": item.get("reason") or "",
                }
                for item in missing[:5]
                if isinstance(item, dict)
            ],
        }
    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
    return [page_path, meta_path]
