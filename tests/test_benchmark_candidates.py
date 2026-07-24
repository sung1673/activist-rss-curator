from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from curator.benchmark_candidates import (
    CandidateDataError,
    build_core_event_candidates,
    build_same_event_candidates,
)


def article(index: int, *, story: str, company: str, title: str) -> dict[str, object]:
    return {
        "record_id": f"article-{index}",
        "title": title,
        "summary": "원문 요약",
        "published_at": (datetime(2026, 7, 1, tzinfo=timezone.utc) + timedelta(hours=index)).isoformat(),
        "canonical_url": f"https://example.test/{index}",
        "source": "test-source",
        "story_key": story,
        "company_candidates": [company],
    }


def test_same_event_candidates_are_blind_deterministic_and_stratified() -> None:
    rows: list[dict[str, object]] = []
    index = 0
    for company in range(4):
        for story in range(3):
            for duplicate in range(3):
                rows.append(
                    article(
                        index,
                        story=f"story-{company}-{story}",
                        company=f"company-{company}",
                        title=f"회사{company} 주주총회 안건 {story} 보도 {duplicate}",
                    )
                )
                index += 1
    for extra in range(30):
        rows.append(
            article(
                index,
                story=f"unique-{extra}",
                company=f"easy-company-{extra}",
                title=f"서로 다른 독립 사건 {extra}",
            )
        )
        index += 1

    first, summary = build_same_event_candidates(
        rows, predicted_same=8, hard_negative=6, easy_negative=5, seed=17
    )
    second, _ = build_same_event_candidates(
        rows, predicted_same=8, hard_negative=6, easy_negative=5, seed=17
    )

    assert first == second
    assert len(first) == 19
    assert summary["selected"] == {
        "predicted_same": 8,
        "hard_negative": 6,
        "easy_negative": 5,
    }
    assert all(record["label"] is None and record["label_source"] is None for record in first)
    assert all("story_key" not in record["left"] for record in first)


def test_same_event_candidates_fail_closed_when_sample_is_too_small() -> None:
    rows = [article(1, story="only", company="one", title="단일 기사")]
    with pytest.raises(CandidateDataError, match="insufficient"):
        build_same_event_candidates(rows, predicted_same=1, hard_negative=1, easy_negative=1)


def test_core_event_candidates_round_robin_across_strata() -> None:
    rows = [
        {
            "event_id": f"event-{index}",
            "company_id": f"{index:08d}",
            "event_type": "general_meeting" if index % 2 else "tender_offer",
            "title": f"공식 사건 {index}",
            "occurred_at": f"{2024 + index % 3}-01-01T00:00:00+00:00",
            "importance": "critical" if index % 2 else "medium",
            "document_ids": [f"document-{index}"],
        }
        for index in range(12)
    ]
    selected, summary = build_core_event_candidates(rows, count=9)
    assert len(selected) == 9
    assert len({record["event_id"] for record in selected}) == 9
    assert summary["stratum_count"] >= 4
    assert all(record["label_source"] is None for record in selected)


def test_core_event_candidates_fail_closed_without_real_events() -> None:
    with pytest.raises(CandidateDataError, match="insufficient core events"):
        build_core_event_candidates([], count=1)
