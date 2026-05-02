from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import yaml

from .dates import datetime_to_iso, parse_datetime
from .normalize import stable_hash


PRIORITY_VERSION = 1
DEFAULT_THRESHOLDS = {
    "top": 80,
    "watch": 55,
    "normal": 25,
}
HIGH_IMPACT_TERMS = (
    "행동주의",
    "행동주의 주주",
    "주주제안",
    "주주행동",
    "소액주주연대",
    "경영권 분쟁",
    "공개서한",
    "위임장",
    "표대결",
    "proxy fight",
    "proxy contest",
    "dissident nominee",
    "universal proxy",
    "poison pill",
    "activist investor",
)
POLICY_TERMS = (
    "자본시장법",
    "상법 개정",
    "상장폐지",
    "상장적격성",
    "거래정지",
    "개선기간",
    "의무공개매수",
    "중복상장",
    "물적분할",
    "일반주주",
    "스튜어드십 코드",
    "listing rules",
    "dual class",
    "say on pay",
)
AUTHORITY_TERMS = (
    "금융위",
    "금감원",
    "거래소",
    "공정위",
    "국회",
    "법원",
    "검찰",
    "sec",
    "fca",
    "tokyo stock exchange",
)
MAJOR_SOURCE_TERMS = (
    "연합뉴스",
    "한국경제",
    "매일경제",
    "조선비즈",
    "서울경제",
    "reuters",
    "bloomberg",
    "financial times",
    "wall street journal",
    "cnbc",
    "nikkei",
    "barron's",
)
SUPPRESS_REASONS = {"low_relevance", "excluded_domain"}


def priority_config(config: dict[str, object]) -> dict[str, Any]:
    value = config.get("priority", {})
    return value if isinstance(value, dict) else {}


def priority_enabled(config: dict[str, object]) -> bool:
    return bool(priority_config(config).get("enabled", True))


def priority_thresholds(config: dict[str, object]) -> dict[str, int]:
    raw_thresholds = priority_config(config).get("thresholds", {})
    thresholds = dict(DEFAULT_THRESHOLDS)
    if isinstance(raw_thresholds, dict):
        for key in thresholds:
            try:
                thresholds[key] = int(raw_thresholds.get(key, thresholds[key]))
            except (TypeError, ValueError):
                pass
    return thresholds


def priority_level(score: int, config: dict[str, object], *, suppress: bool = False) -> str:
    if suppress:
        return "suppress"
    thresholds = priority_thresholds(config)
    if score >= thresholds["top"]:
        return "top"
    if score >= thresholds["watch"]:
        return "watch"
    if score >= thresholds["normal"]:
        return "normal"
    return "archive"


def priority_overrides_path(project_root: Path, config: dict[str, object]) -> Path:
    raw_path = str(priority_config(config).get("overrides_path") or "data/priority_overrides.yaml")
    path = Path(raw_path)
    return path if path.is_absolute() else project_root / path


def load_priority_overrides(path: str | Path) -> dict[str, Any]:
    override_path = Path(path)
    if not override_path.exists():
        return {}
    try:
        loaded = yaml.safe_load(override_path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def article_text(article: dict[str, object]) -> str:
    values = [
        article.get("title"),
        article.get("clean_title"),
        article.get("normalized_title"),
        article.get("summary"),
        article.get("source"),
        article.get("feed_name"),
        article.get("feed_category"),
    ]
    values.extend(article.get("relevance_keywords") or [])
    return " ".join(str(value or "") for value in values).casefold()


def article_date(article: dict[str, object], timezone_name: str) -> datetime | None:
    for key in ("published_at", "article_published_at", "feed_published_at", "seen_at"):
        value = article.get(key)
        if value:
            parsed = parse_datetime(str(value), timezone_name)
            if parsed:
                return parsed
    return None


def article_hash_key(article: dict[str, object]) -> str:
    for key in ("canonical_url_hash", "title_hash"):
        value = str(article.get(key) or "").strip()
        if value:
            return value
    value = "|".join(
        str(article.get(key) or "")
        for key in ("canonical_url", "link", "title", "normalized_title", "published_at")
    )
    return stable_hash(value or "article")


def story_key_for_article(article: dict[str, object], cluster: dict[str, object] | None = None) -> str:
    if cluster:
        for key in ("cluster_key", "guid"):
            value = str(cluster.get(key) or "").strip()
            if value:
                return value
    normalized_title = str(article.get("normalized_title") or article.get("title") or "").strip()
    return f"story:{stable_hash(normalized_title or article_hash_key(article), length=16)}"


def cluster_lookup_key(article: dict[str, object]) -> str:
    return article_hash_key(article)


def build_cluster_lookup(state: dict[str, object]) -> dict[str, dict[str, object]]:
    lookup: dict[str, dict[str, object]] = {}
    for cluster in list(state.get("published_clusters", [])) + list(state.get("pending_clusters", [])):
        if not isinstance(cluster, dict):
            continue
        for article in list(cluster.get("articles") or []):
            if isinstance(article, dict):
                lookup[cluster_lookup_key(article)] = cluster
    return lookup


def override_candidates(article: dict[str, object], story_key: str, overrides: dict[str, Any]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    url_hash = str(article.get("canonical_url_hash") or "")
    record_id = str(article.get("record_id") or "")
    for section, key in (("record_ids", record_id), ("url_hashes", url_hash), ("story_keys", story_key)):
        mapping = overrides.get(section)
        if isinstance(mapping, dict) and key and isinstance(mapping.get(key), dict):
            candidates.append(mapping[key])
    keyword_rules = overrides.get("title_keywords")
    if isinstance(keyword_rules, dict):
        haystack = article_text(article)
        for keyword, rule in keyword_rules.items():
            if str(keyword).casefold() in haystack and isinstance(rule, dict):
                candidates.append(rule)
    return candidates


def apply_override(score: int, level: str, reasons: list[str], rule: dict[str, Any]) -> tuple[int, str, list[str], bool]:
    suppress = bool(rule.get("suppress", False))
    try:
        if "score" in rule:
            score = int(rule["score"])
        else:
            score += int(rule.get("score_delta", 0) or 0)
    except (TypeError, ValueError):
        pass
    manual_level = str(rule.get("level") or "").strip()
    if manual_level in {"top", "watch", "normal", "archive", "suppress"}:
        level = manual_level
    raw_reasons = rule.get("reasons")
    if isinstance(raw_reasons, list):
        reasons.extend(str(reason) for reason in raw_reasons if str(reason or "").strip())
    reasons.append("manual_override")
    return score, level, reasons, suppress or level == "suppress"


def priority_metadata(
    article: dict[str, object],
    config: dict[str, object],
    now: datetime,
    *,
    cluster: dict[str, object] | None = None,
    overrides: dict[str, Any] | None = None,
) -> dict[str, object]:
    timezone_name = str(config.get("timezone") or "Asia/Seoul")
    text = article_text(article)
    score = 0
    reasons: list[str] = []

    relevance = str(article.get("relevance_level") or "")
    if relevance == "high":
        score += 45
        reasons.append("relevance_high")
    elif relevance == "medium":
        score += 25
        reasons.append("relevance_medium")
    elif relevance == "low":
        score -= 25
        reasons.append("relevance_low")

    status = str(article.get("status") or "")
    reason = str(article.get("reason") or "")
    if status == "accepted":
        score += 8
        reasons.append("accepted")
    elif status == "duplicate":
        score += 5
        reasons.append("duplicate_seen")
    elif status == "rejected":
        score -= 20
        reasons.append(f"rejected:{reason or 'unknown'}")

    if any(term.casefold() in text for term in HIGH_IMPACT_TERMS):
        score += 18
        reasons.append("high_impact_term")
    if any(term.casefold() in text for term in POLICY_TERMS):
        score += 13
        reasons.append("policy_or_rule_change")
    if any(term.casefold() in text for term in AUTHORITY_TERMS):
        score += 8
        reasons.append("authority_or_regulator")
    if any(term.casefold() in text for term in MAJOR_SOURCE_TERMS):
        score += 5
        reasons.append("major_source")

    duplicate_matches = list(article.get("duplicate_matches") or [])
    if duplicate_matches:
        score += min(12, len(duplicate_matches) * 4)
        reasons.append("recent_duplicate_mentions")

    if cluster:
        article_count = int(cluster.get("article_count") or len(cluster.get("articles") or []) or 0)
        if article_count > 1:
            score += min(24, (article_count - 1) * 5)
            reasons.append("multi_source_cluster")
        if cluster.get("status") == "published":
            score += 4
            reasons.append("published_cluster")
        if str(cluster.get("theme_group") or "") in {"shareholder_proposal", "activism_trend", "control_dispute"}:
            score += 8
            reasons.append("priority_theme_group")

    published_at = article_date(article, timezone_name)
    if published_at:
        age_hours = max(0.0, (now - published_at.astimezone(now.tzinfo)).total_seconds() / 3600)
        if age_hours <= 24:
            score += 6
            reasons.append("fresh_24h")
        elif age_hours > 72:
            score -= 8
            reasons.append("older_than_72h")

    hostname = (urlsplit(str(article.get("canonical_url") or article.get("link") or "")).hostname or "").casefold()
    if hostname and hostname not in {"news.google.com", "www.google.com"}:
        score += 2
        reasons.append("direct_source_url")

    story_key = story_key_for_article(article, cluster)
    suppress = reason in SUPPRESS_REASONS
    level = priority_level(score, config, suppress=suppress)

    for rule in override_candidates(article, story_key, overrides or {}):
        score, level, reasons, override_suppressed = apply_override(score, level, reasons, rule)
        suppress = suppress or override_suppressed
        level = "suppress" if suppress else level

    return {
        "priority_version": PRIORITY_VERSION,
        "priority_score": max(-100, min(100, int(score))),
        "priority_level": "suppress" if suppress else priority_level(score, config) if level not in {"top", "watch", "normal", "archive"} else level,
        "priority_reasons": sorted(set(reason for reason in reasons if reason)),
        "story_key": story_key,
        "priority_updated_at": datetime_to_iso(now),
    }


def apply_priority_metadata(article: dict[str, object], metadata: dict[str, object]) -> None:
    for key, value in metadata.items():
        article[key] = value


def annotate_state_priorities(
    state: dict[str, object],
    config: dict[str, object],
    now: datetime,
    overrides: dict[str, Any] | None = None,
) -> int:
    if not priority_enabled(config):
        return 0
    cluster_lookup = build_cluster_lookup(state)
    updated = 0
    collections = [
        state.get("articles", []),
        state.get("rejected_articles", []),
    ]
    for collection in collections:
        for article in list(collection or []):
            if not isinstance(article, dict):
                continue
            cluster = cluster_lookup.get(cluster_lookup_key(article))
            metadata = priority_metadata(article, config, now, cluster=cluster, overrides=overrides)
            apply_priority_metadata(article, metadata)
            updated += 1

    for cluster in list(state.get("published_clusters", [])) + list(state.get("pending_clusters", [])):
        if not isinstance(cluster, dict):
            continue
        cluster_scores: list[int] = []
        cluster_levels: list[str] = []
        for article in list(cluster.get("articles") or []):
            if not isinstance(article, dict):
                continue
            metadata = priority_metadata(article, config, now, cluster=cluster, overrides=overrides)
            apply_priority_metadata(article, metadata)
            cluster_scores.append(int(metadata["priority_score"]))
            cluster_levels.append(str(metadata["priority_level"]))
            updated += 1
        if cluster_scores:
            cluster["priority_score"] = max(cluster_scores)
            cluster["priority_level"] = next(
                (level for level in ("top", "watch", "normal", "archive", "suppress") if level in cluster_levels),
                priority_level(max(cluster_scores), config),
            )
            cluster["priority_updated_at"] = datetime_to_iso(now)
    return updated
