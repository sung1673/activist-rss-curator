from __future__ import annotations

from rapidfuzz import fuzz

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


def dedupe_articles(
    articles: list[dict[str, object]],
    state: dict[str, object],
    config: dict[str, object],
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
            duplicates.append(duplicate)
            continue

        unique.append(article)
        if url_hash:
            current_url_hashes.add(url_hash)
        if title_hash:
            current_title_hashes.add(title_hash)

    return unique, duplicates
