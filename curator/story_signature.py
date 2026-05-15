from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import re
import yaml
from rapidfuzz import fuzz


DEFAULT_STORY_RULES_PATH = "data/story_rules.yaml"

DEFAULT_EVENT_PHRASE_TOKENS: dict[str, tuple[str, ...]] = {
    "배후의혹": ("배후설", "배후 의혹", "배후조종", "배후 조종", "배후세력"),
    "단체실체논란": ("실체 논란", "대표성 논란", "단체명 혼용"),
    "소액주주단체논란": (
        "소액주주 단체 실체",
        "소액주주 단체 배후",
        "소액주주 단체 대표성",
        "기자와 접촉 금지",
        "기자랑 말 섞지 말라",
    ),
    "집단소송": ("집단소송", "집단 소송"),
    "손해배상": ("손해배상", "손해 배상", "손배"),
    "파업소송": ("파업 집단소송", "파업시 집단소송", "파업 시 집단소송", "파업 금지 집단소송"),
    "유증정정": ("유증 신고서", "유상증자 관련 정정신고서", "정정신고서 제출", "정정 반영", "정정요구", "정정 요구", "2차 정정", "두 차례 반려"),
    "유증재추진": ("재추진", "다시 제출", "일정 공시"),
    "금감원정정요구": ("금감원 제동", "금감원 2차 정정", "금감원 정정", "금감원 반려"),
    "자사주환원공시": ("자사주 소각 결정", "자기주식 소각 결정", "자사주 매입 결정", "자기주식 취득 결정"),
    "상폐심사": ("상장폐지 실질심사", "상장적격성 실질심사", "거래정지", "개선기간 부여"),
}

DEFAULT_STORY_RULES: tuple[dict[str, object], ...] = (
    {
        "id": "minority_shareholder_group_controversy",
        "tokens": ["소액주주단체논란", "단체실체논란"],
        "require_all": ["소액주주", "단체"],
        "require_any_groups": [["실체", "배후", "대표성", "혼용", "지침", "조종", "의혹"]],
    },
    {
        "id": "rights_issue_refiling_after_regulator_review",
        "tokens": ["유증정정", "금감원정정요구"],
        "require_any_groups": [["유상증자", "유증"], ["정정신고서", "정정", "반려", "제동", "금감원", "재추진", "신고서", "다시제출"]],
    },
    {
        "id": "strike_litigation",
        "tokens": ["파업소송"],
        "require_any_groups": [["파업", "파업시", "파업 시"], ["집단소송", "집단 소송", "소송", "손해배상", "손해 배상", "손배"]],
    },
    {
        "id": "buyback_or_treasury_share_return_disclosure",
        "tokens": ["자사주환원공시"],
        "require_any_groups": [["자사주", "자기주식"], ["소각", "매입", "취득"], ["결정", "공시", "발표"]],
    },
    {
        "id": "delisting_or_listing_eligibility_review",
        "tokens": ["상폐심사"],
        "require_any_groups": [["상장폐지", "상폐", "상장적격성"], ["실질심사", "거래정지", "개선기간"]],
    },
)

SPECIFIC_EVENT_TOKENS = {
    "소액주주단체논란",
    "배후의혹",
    "단체실체논란",
    "집단소송",
    "손해배상",
    "파업소송",
    "유증정정",
    "유증재추진",
    "금감원정정요구",
    "자사주환원공시",
    "상폐심사",
}

CONTEXTUAL_EVENT_TOKENS = {
    "소액주주단체논란",
    "배후의혹",
    "단체실체논란",
    "파업소송",
    "유증정정",
    "유증재추진",
    "금감원정정요구",
    "자사주환원공시",
    "상폐심사",
}


@dataclass(frozen=True)
class StorySignatureDecision:
    same_story: bool
    score: float
    reason: str
    title_score: float
    company_overlap: frozenset[str]
    event_overlap: frozenset[str]
    contextual_overlap: frozenset[str]


def compact_for_match(text: object) -> str:
    return re.sub(r"\s+", "", str(text or "").casefold())


def normalize_token(value: object) -> str:
    return re.sub(r"\s+", "", str(value or "").casefold()).strip()


def phrase_matches(text: str, compact_text: str, phrase: object) -> bool:
    raw = str(phrase or "").casefold().strip()
    if not raw:
        return False
    compact = compact_for_match(raw)
    return raw in text or compact in compact_text


def unique_values(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def story_rules_path(config: dict[str, object] | None = None) -> str:
    settings = config.get("story_rules") if isinstance(config, dict) else None
    if isinstance(settings, dict) and settings.get("path"):
        return str(settings["path"])
    return DEFAULT_STORY_RULES_PATH


@lru_cache(maxsize=8)
def _load_external_story_rules(path_value: str, mtime_ns: int) -> tuple[tuple[dict[str, object], ...], dict[str, tuple[str, ...]]]:
    path = Path(path_value)
    if not path.exists():
        return (), {}
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle) or {}
    if not isinstance(loaded, dict):
        return (), {}

    raw_rules = loaded.get("rules") if isinstance(loaded.get("rules"), list) else []
    rules = tuple(rule for rule in raw_rules if isinstance(rule, dict))

    raw_phrases = loaded.get("phrase_tokens") if isinstance(loaded.get("phrase_tokens"), dict) else {}
    phrase_tokens: dict[str, tuple[str, ...]] = {}
    for token, phrases in raw_phrases.items():
        if isinstance(phrases, list):
            phrase_tokens[str(token)] = tuple(str(phrase) for phrase in phrases if str(phrase).strip())
    return rules, phrase_tokens


def external_story_rules(config: dict[str, object] | None = None) -> tuple[tuple[dict[str, object], ...], dict[str, tuple[str, ...]]]:
    path_value = story_rules_path(config)
    path = Path(path_value)
    try:
        mtime_ns = path.stat().st_mtime_ns
    except OSError:
        mtime_ns = 0
    return _load_external_story_rules(path_value, mtime_ns)


def story_rule_token_universe(config: dict[str, object] | None = None) -> set[str]:
    rules, phrase_tokens = external_story_rules(config)
    tokens = set(DEFAULT_EVENT_PHRASE_TOKENS) | set(phrase_tokens) | SPECIFIC_EVENT_TOKENS | CONTEXTUAL_EVENT_TOKENS
    for rule in (*DEFAULT_STORY_RULES, *rules):
        raw_tokens = rule.get("tokens") if isinstance(rule, dict) else None
        if isinstance(raw_tokens, list):
            tokens.update(str(token) for token in raw_tokens if str(token).strip())
    return tokens


def specific_event_token_universe(config: dict[str, object] | None = None) -> set[str]:
    return SPECIFIC_EVENT_TOKENS | story_rule_token_universe(config)


def contextual_event_token_universe(config: dict[str, object] | None = None) -> set[str]:
    return CONTEXTUAL_EVENT_TOKENS | story_rule_token_universe(config)


def rule_matches(text: str, compact_text: str, rule: dict[str, object]) -> bool:
    required_all = rule.get("require_all")
    if isinstance(required_all, list) and not all(phrase_matches(text, compact_text, term) for term in required_all):
        return False

    require_any_groups = rule.get("require_any_groups")
    if isinstance(require_any_groups, list):
        for group in require_any_groups:
            if isinstance(group, list) and not any(phrase_matches(text, compact_text, term) for term in group):
                return False
    return True


def event_tokens_for_text(text: object, config: dict[str, object] | None = None) -> list[str]:
    raw_text = str(text or "")
    folded = raw_text.casefold()
    compact = compact_for_match(raw_text)
    tokens: list[str] = []

    external_rules, external_phrase_tokens = external_story_rules(config)
    phrase_tokens = {**DEFAULT_EVENT_PHRASE_TOKENS, **external_phrase_tokens}
    for token, phrases in phrase_tokens.items():
        if any(phrase_matches(folded, compact, phrase) for phrase in phrases):
            tokens.append(token)

    for rule in (*DEFAULT_STORY_RULES, *external_rules):
        if not rule_matches(folded, compact, rule):
            continue
        raw_tokens = rule.get("tokens")
        if isinstance(raw_tokens, list):
            tokens.extend(str(token) for token in raw_tokens)
    return unique_values(tokens)


def normalized_token_set(values: object) -> set[str]:
    if isinstance(values, (list, tuple, set)):
        return {normalize_token(value) for value in values if normalize_token(value)}
    return {normalize_token(values)} if normalize_token(values) else set()


def story_signature_decision(
    left_title: object,
    right_title: object,
    *,
    left_companies: object = (),
    right_companies: object = (),
    left_event_tokens: object = (),
    right_event_tokens: object = (),
    title_score: float | None = None,
    config: dict[str, object] | None = None,
) -> StorySignatureDecision:
    computed_title_score = float(
        title_score
        if title_score is not None
        else fuzz.token_set_ratio(str(left_title or ""), str(right_title or ""))
    )
    company_overlap = normalized_token_set(left_companies) & normalized_token_set(right_companies)
    event_overlap = normalized_token_set(left_event_tokens) & normalized_token_set(right_event_tokens)
    specific_overlap = event_overlap & {normalize_token(token) for token in specific_event_token_universe(config)}
    contextual_overlap = event_overlap & {normalize_token(token) for token in contextual_event_token_universe(config)}

    score = computed_title_score * 0.35
    if company_overlap:
        score += 25
    score += min(30, len(specific_overlap) * 10 + len(contextual_overlap) * 6)
    if len(event_overlap) >= 3:
        score += 8

    reason = ""
    if company_overlap and len(contextual_overlap) >= 2 and computed_title_score >= 35:
        reason = "same_company_contextual_event"
    elif company_overlap and len(specific_overlap) >= 2 and computed_title_score >= 42:
        reason = "same_company_specific_event"
    elif company_overlap and len(specific_overlap) >= 1 and len(event_overlap) >= 3 and computed_title_score >= 40:
        reason = "same_company_event_signature"
    elif len(contextual_overlap) >= 3 and computed_title_score >= 52:
        reason = "contextual_event_signature"

    return StorySignatureDecision(
        same_story=bool(reason),
        score=round(score, 2),
        reason=reason,
        title_score=computed_title_score,
        company_overlap=frozenset(company_overlap),
        event_overlap=frozenset(event_overlap),
        contextual_overlap=frozenset(contextual_overlap),
    )
