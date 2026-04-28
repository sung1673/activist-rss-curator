from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from rapidfuzz import fuzz

from .dates import parse_datetime
from .normalize import stable_hash


def article_title_hash(article: dict[str, object]) -> str:
    return stable_hash(str(article.get("normalized_title") or ""))


def is_same_cluster_candidate(
    left: dict[str, object],
    right: dict[str, object],
    title_threshold: int = 80,
    summary_threshold: int = 85,
) -> bool:
    title_score = fuzz.token_set_ratio(
        str(left.get("normalized_title") or ""),
        str(right.get("normalized_title") or ""),
    )
    summary_score = fuzz.token_set_ratio(
        str(left.get("summary") or ""),
        str(right.get("summary") or ""),
    )
    return title_score >= title_threshold and summary_score >= summary_threshold


def duplicate_reason(
    article: dict[str, object],
    unique_articles: list[dict[str, object]],
    state: dict[str, object],
    config: dict[str, object],
) -> str | None:
    url_hash = str(article.get("canonical_url_hash") or "")
    title = str(article.get("normalized_title") or "")
    title_hash = str(article.get("title_hash") or article_title_hash(article))

    seen_url_hashes = set(state.get("seen_url_hashes", []))
    seen_title_hashes = set(state.get("seen_title_hashes", []))
    if url_hash and url_hash in seen_url_hashes:
        return "seen_url"
    if title_hash and title_hash in seen_title_hashes:
        return "seen_title"

    threshold = int(config.get("dedupe", {}).get("title_duplicate_threshold", 92))  # type: ignore[union-attr]
    candidates = unique_articles + list(state.get("articles", []))
    for candidate in candidates:
        candidate_title = str(candidate.get("normalized_title") or "")
        if title and title == candidate_title:
            return "same_title"
        if title and candidate_title and fuzz.token_set_ratio(title, candidate_title) >= threshold:
            return "similar_title"
    return None


def duplicate_match_days(config: dict[str, object]) -> int:
    return int(config.get("dedupe", {}).get("duplicate_mention_days", 30))  # type: ignore[union-attr]


def duplicate_reference_datetime(
    record: dict[str, object],
    timezone_name: str,
) -> datetime | None:
    for key in ("seen_at", "published_at", "article_published_at", "feed_published_at"):
        value = record.get(key)
        if value:
            parsed = parse_datetime(str(value), timezone_name)
            if parsed:
                return parsed
    return None


def duplicate_match_score(article: dict[str, object], record: dict[str, object], threshold: int) -> int:
    url_hash = str(article.get("canonical_url_hash") or "")
    title_hash = str(article.get("title_hash") or article_title_hash(article))
    record_url_hash = str(record.get("canonical_url_hash") or "")
    record_title_hash = str(record.get("title_hash") or "")
    if url_hash and url_hash == record_url_hash:
        return 100
    if title_hash and title_hash == record_title_hash:
        return 100
    title = str(article.get("normalized_title") or "")
    record_title = str(record.get("normalized_title") or "")
    if not title or not record_title:
        return 0
    score = int(fuzz.token_set_ratio(title, record_title))
    return score if score >= threshold else 0


def duplicate_match_record(record: dict[str, object], score: int) -> dict[str, object]:
    return {
        "title": record.get("title") or "",
        "normalized_title": record.get("normalized_title") or "",
        "canonical_url": record.get("canonical_url") or "",
        "published_at": record.get("published_at") or None,
        "seen_at": record.get("seen_at") or None,
        "status": record.get("status") or None,
        "similarity": score,
    }


def duplicate_matches(
    article: dict[str, object],
    state: dict[str, object],
    config: dict[str, object],
    now: datetime | None = None,
) -> list[dict[str, object]]:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    reference_now = now or datetime.now(ZoneInfo(timezone_name))
    cutoff = reference_now - timedelta(days=duplicate_match_days(config))
    threshold = int(config.get("dedupe", {}).get("title_duplicate_threshold", 92))  # type: ignore[union-attr]

    matches: list[tuple[datetime, dict[str, object]]] = []
    seen_keys: set[tuple[str, str]] = set()
    for record in reversed(list(state.get("articles", []))):
        if not isinstance(record, dict) or record.get("status") == "duplicate":
            continue
        record_dt = duplicate_reference_datetime(record, timezone_name)
        if record_dt and record_dt < cutoff:
            continue
        score = duplicate_match_score(article, record, threshold)
        if not score:
            continue
        key = (str(record.get("canonical_url") or ""), str(record.get("normalized_title") or ""))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        matches.append((record_dt or reference_now, duplicate_match_record(record, score)))
        if len(matches) >= 3:
            break
    matches.sort(key=lambda item: item[0], reverse=True)
    return [match for _dt, match in matches]


def dedupe_articles(
    articles: list[dict[str, object]],
    state: dict[str, object],
    config: dict[str, object],
    now: datetime | None = None,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    unique: list[dict[str, object]] = []
    duplicates: list[dict[str, object]] = []
    current_url_hashes: set[str] = set()
    current_title_hashes: set[str] = set()

    for article in articles:
        url_hash = str(article.get("canonical_url_hash") or "")
        title_hash = str(article.get("title_hash") or article_title_hash(article))
        reason = duplicate_reason(article, unique, state, config)
        if not reason and url_hash and url_hash in current_url_hashes:
            reason = "same_url_in_run"
        if not reason and title_hash and title_hash in current_title_hashes:
            reason = "same_title_in_run"

        if reason:
            duplicate = dict(article)
            duplicate["duplicate_reason"] = reason
            duplicate["duplicate_matches"] = duplicate_matches(article, state, config, now)
            duplicates.append(duplicate)
            continue

        unique.append(article)
        if url_hash:
            current_url_hashes.add(url_hash)
        if title_hash:
            current_title_hashes.add(title_hash)

    return unique, duplicates
