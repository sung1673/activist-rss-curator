from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from .ai import ai_config, call_github_models, github_models_token


RELATIONSHIPS = {"same_story", "related_but_different", "different"}
_JUDGE_CACHE: dict[str, "StoryJudgement | None"] = {}
_JUDGE_CALLS = 0


@dataclass(frozen=True)
class StoryJudgement:
    relationship: str
    confidence: float
    reason: str = ""


def story_judge_settings(config: dict[str, object]) -> dict[str, Any]:
    return ai_config(config)


def story_judge_enabled(config: dict[str, object]) -> bool:
    settings = story_judge_settings(config)
    return bool(settings.get("enabled", True)) and bool(settings.get("story_judge_enabled", False))


def story_judge_auto_accept_title_score(config: dict[str, object]) -> float:
    settings = story_judge_settings(config)
    return float(settings.get("story_judge_auto_accept_title_score", 88))


def story_judge_confidence_threshold(config: dict[str, object]) -> float:
    settings = story_judge_settings(config)
    return float(settings.get("story_judge_confidence_threshold", 0.75))


def story_judge_call_budget_available(config: dict[str, object]) -> bool:
    settings = story_judge_settings(config)
    max_calls = int(settings.get("story_judge_max_calls_per_run", 8))
    return max_calls <= 0 or _JUDGE_CALLS < max_calls


def should_consult_story_judge(title_score: float, config: dict[str, object]) -> bool:
    return (
        story_judge_enabled(config)
        and title_score < story_judge_auto_accept_title_score(config)
        and story_judge_call_budget_available(config)
        and bool(github_models_token())
    )


def compact_value(value: object, max_chars: int = 420) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."


def article_title(article: dict[str, object]) -> str:
    return compact_value(article.get("clean_title") or article.get("title") or article.get("normalized_title") or "", 180)


def article_domain(article: dict[str, object]) -> str:
    url = str(article.get("canonical_url") or article.get("link") or "")
    return (urlsplit(url).hostname or "").lower().removeprefix("www.")


def article_brief(article: dict[str, object]) -> dict[str, object]:
    return {
        "source": article.get("source") or article.get("feed_name") or article_domain(article),
        "title": article_title(article),
        "summary": compact_value(article.get("summary") or "", 500),
        "companies": list(article.get("company_candidates") or article.get("companies") or [])[:5],
        "keywords": list(article.get("topic_keywords") or article.get("keywords") or article.get("relevance_keywords") or [])[:8],
        "domain": article_domain(article),
    }


def cache_key(left: dict[str, object], right: dict[str, object], context: str) -> str:
    left_seed = "|".join(
        [
            str(left.get("canonical_url_hash") or left.get("canonical_url") or left.get("link") or ""),
            str(left.get("title_hash") or left.get("normalized_title") or article_title(left)),
        ]
    )
    right_seed = "|".join(
        [
            str(right.get("canonical_url_hash") or right.get("canonical_url") or right.get("link") or ""),
            str(right.get("title_hash") or right.get("normalized_title") or article_title(right)),
        ]
    )
    ordered = sorted([left_seed, right_seed])
    return hashlib.sha256(f"{context}|{ordered[0]}|{ordered[1]}".encode("utf-8")).hexdigest()[:24]


def parse_story_judgement(content: str | None) -> StoryJudgement | None:
    if not content:
        return None
    match = re.search(r"\{.*\}", content, flags=re.DOTALL)
    if not match:
        return None
    try:
        data = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    relationship = str(data.get("relationship") or "").strip()
    if relationship not in RELATIONSHIPS:
        return None
    try:
        confidence = float(data.get("confidence", 0))
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = min(1.0, max(0.0, confidence))
    reason = compact_value(data.get("reason") or "", 80)
    return StoryJudgement(relationship=relationship, confidence=confidence, reason=reason)


def judge_same_story(
    left: dict[str, object],
    right: dict[str, object],
    config: dict[str, object],
    *,
    title_score: float,
    local_reason: str,
    context: str,
) -> StoryJudgement | None:
    global _JUDGE_CALLS
    if not should_consult_story_judge(title_score, config):
        return None

    key = cache_key(left, right, context)
    if key in _JUDGE_CACHE:
        return _JUDGE_CACHE[key]

    settings = story_judge_settings(config)
    model = str(settings.get("story_judge_model") or settings.get("hourly_digest_model") or "openai/gpt-4.1")
    max_tokens = int(settings.get("story_judge_max_tokens", 90))
    system_prompt = (
        "You are a conservative Korean news deduplication judge. "
        "Decide whether two articles describe the same specific news event. "
        "Same company, same broad dispute, or same market theme is not enough. "
        "Return JSON only."
    )
    user_payload = {
        "task": (
            "두 기사가 하나의 텔레그램 묶음으로 합쳐질 수 있는 같은 사건인지 판정하세요. "
            "같은 회사/경영권 분쟁/소액주주 같은 넓은 배경만 같으면 related_but_different 또는 different로 답하세요."
        ),
        "output_schema": {
            "relationship": "same_story | related_but_different | different",
            "confidence": "0.0-1.0",
            "reason": "한국어 40자 이내",
        },
        "local_signal": {
            "context": context,
            "local_reason": local_reason,
            "title_similarity": round(title_score, 1),
        },
        "article_a": article_brief(left),
        "article_b": article_brief(right),
    }
    content = call_github_models(
        system_prompt,
        json.dumps(user_payload, ensure_ascii=False, indent=2),
        model=model,
        max_tokens=max_tokens,
        config=config,
    )
    _JUDGE_CALLS += 1
    judgement = parse_story_judgement(content)
    _JUDGE_CACHE[key] = judgement
    return judgement


def judgement_allows_same_story(judgement: StoryJudgement | None, config: dict[str, object], *, fallback: bool = True) -> bool:
    if judgement is None:
        return fallback
    return judgement.relationship == "same_story" and judgement.confidence >= story_judge_confidence_threshold(config)
