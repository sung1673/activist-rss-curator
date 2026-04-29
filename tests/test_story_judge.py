from __future__ import annotations

from curator.story_judge import (
    StoryJudgement,
    judgement_allows_same_story,
    parse_story_judgement,
    should_consult_story_judge,
)


def test_parse_story_judgement_json_only() -> None:
    judgement = parse_story_judgement(
        '{"relationship":"related_but_different","confidence":0.86,"reason":"같은 회사지만 다른 사건"}'
    )

    assert judgement == StoryJudgement("related_but_different", 0.86, "같은 회사지만 다른 사건")


def test_parse_story_judgement_extracts_json_from_text() -> None:
    judgement = parse_story_judgement(
        '```json\n{"relationship":"same_story","confidence":0.91,"reason":"동일 고발 건"}\n```'
    )

    assert judgement == StoryJudgement("same_story", 0.91, "동일 고발 건")


def test_judgement_requires_same_story_and_confidence(config) -> None:  # type: ignore[no-untyped-def]
    assert judgement_allows_same_story(StoryJudgement("same_story", 0.8), config)
    assert not judgement_allows_same_story(StoryJudgement("same_story", 0.5), config)
    assert not judgement_allows_same_story(StoryJudgement("related_but_different", 0.95), config)
    assert judgement_allows_same_story(None, config, fallback=True)


def test_story_judge_consultation_needs_token_and_ambiguous_title(config, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("GITHUB_MODELS_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.delenv("GH_TOKEN", raising=False)

    assert not should_consult_story_judge(72, config)

    monkeypatch.setenv("GITHUB_TOKEN", "test-token")
    assert should_consult_story_judge(72, config)
    assert not should_consult_story_judge(95, config)
