from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from curator.benchmark_candidates import CandidateDataError, build_relevance_candidates
from curator.quality_benchmark import _validate_record


def article(index: int, *, company: str) -> dict[str, object]:
    return {
        "record_id": f"article-{index}",
        "title": f"Same-company quarterly product sales {index}",
        "summary": "Original source summary",
        "published_at": (datetime(2026, 7, 1, tzinfo=timezone.utc) + timedelta(hours=index)).isoformat(),
        "canonical_url": f"https://example.test/articles/{index}",
        "source": "test-source",
        "company_candidates": [company],
    }


def event(index: int) -> dict[str, object]:
    return {
        "event_id": f"event-{index}",
        "company_id": f"{index % 3:08d}",
        "event_type": "general_meeting" if index % 2 else "tender_offer",
        "title": f"Official governance event {index}",
        "occurred_at": f"{2024 + index % 3}-01-01T00:00:00+00:00",
        "importance": "critical" if index % 2 else "medium",
        "document_ids": [f"document-{index}"],
    }


def document(index: int) -> dict[str, object]:
    return {
        "document_id": f"document-{index}",
        "company_id": f"{index % 3:08d}",
        "title": f"Official disclosure {index}",
        "received_at": f"2026-07-{index + 1:02d}T01:00:00+00:00",
        "original_url": f"https://example.test/documents/{index}",
        "source_type": "official_disclosure",
    }


def test_relevance_candidates_are_linked_stratified_and_final_contract_compatible() -> None:
    events = [event(index) for index in range(12)]
    documents = [document(index) for index in range(12)]
    negatives = [article(100 + index, company=f"{index % 3:08d}") for index in range(8)]

    selected, summary = build_relevance_candidates(
        events,
        documents,
        hard_negative_rows=negatives,
        official_events=9,
        hard_negatives=5,
        seed=17,
    )

    official = [record for record in selected if record["stratum"] == "official_event"]
    hard = [record for record in selected if record["stratum"] == "non_governance_hard_negative"]
    assert len(selected) == 14
    assert len({record["event_id"] for record in official}) == 9
    assert len(hard) == 5
    assert summary["stratum_count"] >= 4
    assert summary["selected"] == {"official_event": 9, "non_governance_hard_negative": 5}
    assert all(record["task"] == "relevance" for record in selected)
    assert all(record["linked_document_ids"] for record in official)
    assert all(not record["linked_document_ids"] for record in hard)
    assert all(record["label_source"] is None and record["annotator_id"] is None for record in selected)

    for record in selected:
        record["label"] = "relevant" if record["stratum"] == "official_event" else "not_relevant"
        record["label_source"] = "human"
        record["annotator_id"] = "reviewer-a"
        record["labeled_at"] = "2026-07-22T00:00:00+00:00"
    assert all(
        _validate_record(
            record,
            expected_task="relevance",
            location=f"candidate[{index}]",
            allow_fixture_labels=False,
        )
        for index, record in enumerate(selected)
    )


def test_relevance_candidates_fail_closed_without_linked_official_evidence() -> None:
    value = event(1)
    value["document_ids"] = ["missing-document"]
    with pytest.raises(CandidateDataError, match="linked evidence"):
        build_relevance_candidates([value], [], official_events=1, hard_negatives=1)


def test_relevance_candidates_fail_closed_below_hard_negative_floor() -> None:
    with pytest.raises(CandidateDataError, match="hard-negative"):
        build_relevance_candidates(
            [event(1)],
            [document(1)],
            hard_negative_rows=[],
            official_events=1,
            hard_negatives=1,
        )


def test_duplicate_event_rows_do_not_fill_the_official_event_floor() -> None:
    duplicate = event(1)
    with pytest.raises(CandidateDataError, match="linked evidence"):
        build_relevance_candidates(
            [event(1), duplicate],
            [document(1)],
            hard_negative_rows=[article(500, company="00000001")],
            official_events=2,
            hard_negatives=1,
        )
