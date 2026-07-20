from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from curator import editorial_ingest


def complete_bundle() -> dict[str, object]:
    return {
        "schema_version": 1,
        "actors": [
            {
                "actor_id": "actor:alpha",
                "actor_type": "activist_shareholder",
                "display_name": "  행동주주 원문  ",
                "display_name_en": "Activist Alpha",
                "country_code": "KR",
                "aliases": ["알파 연대"],
            }
        ],
        "event_actors": [
            {
                "event_id": "event:one",
                "actor_id": "actor:alpha",
                "actor_role": "proponent",
            }
        ],
        "campaigns": [
            {
                "campaign_id": "campaign:one",
                "company_id": "00126380",
                "lead_actor_id": "actor:alpha",
                "title": "이사회 구성 개선 요구",
                "original_language": "ko",
                "demand_text": "사외이사 후보를 제안합니다.",
                "stage": "public_letter",
                "started_at": "2026-07-16T09:00:00+09:00",
                "evidence_document_ids": ["dart:20260716000001"],
            }
        ],
        "claim_evidence": [
            {
                "claim_id": "claim:one",
                "event_id": "event:one",
                "campaign_id": "campaign:one",
                "actor_id": "actor:alpha",
                "document_id": "dart:20260716000001",
                "claim_type": "actor_claim",
                "claim_text": "원문의 표현을 번역하거나 정규화하지 않습니다.",
                "original_language": "ko",
            }
        ],
        "proposal_votes": [
            {
                "proposal_vote_id": "vote:one",
                "event_id": "event:one",
                "campaign_id": "campaign:one",
                "company_id": "00126380",
                "proposer_actor_id": "actor:alpha",
                "agenda_title": "제1호 의안",
                "original_language": "ko",
                "meeting_at": "2027-03-20T09:00:00+09:00",
                "result": "pending",
                "evidence_document_id": "dart:20260716000001",
            }
        ],
        "commitment_outcomes": [
            {
                "commitment_id": "commitment:one",
                "event_id": "event:one",
                "campaign_id": "campaign:one",
                "company_id": "00126380",
                "commitment_text": "2027년 정기주총까지 이사회 정책을 개정합니다.",
                "original_language": "ko",
                "target_at": "2027-03-20T09:00:00+09:00",
                "status": "announced",
                "target_metrics": {"independent_director_ratio": 0.5},
                "evidence_document_id": "dart:20260716000001",
            }
        ],
        "timeline_entries": [
            {
                "timeline_entry_id": "timeline:one",
                "event_id": "event:one",
                "campaign_id": "campaign:one",
                "document_id": "dart:20260716000001",
                "occurred_at": "2026-07-16T09:00:00+09:00",
                "entry_type": "public_letter",
                "title": "공개서한 발표",
                "description": "원문 설명입니다.",
                "original_language": "ko",
            }
        ],
    }


def test_validate_bundle_preserves_source_text_and_applies_fail_closed_defaults() -> None:
    raw = complete_bundle()
    normalized = editorial_ingest.validate_bundle(raw)

    assert normalized["actors"][0]["display_name"] == "  행동주주 원문  "
    assert normalized["claim_evidence"][0]["claim_text"] == "원문의 표현을 번역하거나 정규화하지 않습니다."
    assert normalized["actors"][0]["record_status"] == "inactive"
    assert normalized["actors"][0]["review_status"] == "pending"
    assert normalized["event_actors"][0]["review_status"] == "pending"
    assert normalized["campaigns"][0]["review_status"] == "pending"
    assert normalized["campaigns"][0]["publication_status"] == "draft"
    assert normalized["claim_evidence"][0]["editorial_status"] == "pending"
    assert normalized["proposal_votes"][0]["review_status"] == "pending"
    assert normalized["proposal_votes"][0]["publication_status"] == "draft"
    assert normalized["commitment_outcomes"][0]["review_status"] == "pending"
    assert normalized["commitment_outcomes"][0]["publication_status"] == "draft"
    assert normalized["timeline_entries"][0]["review_status"] == "pending"
    assert normalized["timeline_entries"][0]["publication_status"] == "draft"
    assert "record_status" not in raw["actors"][0]  # type: ignore[index]


def test_bundle_hash_is_deterministic_for_json_object_key_order() -> None:
    first = editorial_ingest.validate_bundle(complete_bundle())
    reordered_raw = complete_bundle()
    reordered = {key: reordered_raw[key] for key in reversed(list(reordered_raw))}
    second = editorial_ingest.validate_bundle(reordered)

    assert editorial_ingest.bundle_sha256(first) == editorial_ingest.bundle_sha256(second)
    assert len(editorial_ingest.bundle_sha256(first)) == 64


@pytest.mark.parametrize(
    ("entity", "field", "value", "message"),
    [
        ("campaigns", "publication_status", "published", "must be 'draft'"),
        ("campaigns", "review_status", "approved", "must be 'pending'"),
        ("claim_evidence", "editorial_status", "approved", "must be 'pending'"),
        ("actors", "record_status", "active", "must be 'inactive'"),
        ("actors", "review_status", "approved", "must be 'pending'"),
        ("event_actors", "review_status", "approved", "must be 'pending'"),
        ("proposal_votes", "review_status", "approved", "must be 'pending'"),
        ("commitment_outcomes", "review_status", "approved", "must be 'pending'"),
        ("timeline_entries", "review_status", "approved", "must be 'pending'"),
        ("campaigns", "started_at", "2026-07-16T09:00:00", "timezone offset"),
    ],
)
def test_validation_rejects_publication_review_bypass_and_naive_time(
    entity: str,
    field: str,
    value: str,
    message: str,
) -> None:
    raw = complete_bundle()
    raw[entity][0][field] = value  # type: ignore[index]

    with pytest.raises(editorial_ingest.EditorialValidationError, match=message):
        editorial_ingest.validate_bundle(raw)


def test_validation_requires_evidence_references() -> None:
    campaign_missing_evidence = complete_bundle()
    campaign_missing_evidence["campaigns"][0]["evidence_document_ids"] = []  # type: ignore[index]
    with pytest.raises(editorial_ingest.EditorialValidationError, match="evidence document ID"):
        editorial_ingest.validate_bundle(campaign_missing_evidence)

    vote_missing_evidence = complete_bundle()
    del vote_missing_evidence["proposal_votes"][0]["evidence_document_id"]  # type: ignore[index]
    with pytest.raises(editorial_ingest.EditorialValidationError, match="evidence_document_id"):
        editorial_ingest.validate_bundle(vote_missing_evidence)


@pytest.mark.parametrize(
    ("entity", "field", "length", "limit"),
    [
        ("actors", "display_name", 256, 255),
        ("campaigns", "title", 701, 700),
        ("claim_evidence", "evidence_locator", 501, 500),
        ("proposal_votes", "agenda_title", 701, 700),
        ("timeline_entries", "title", 701, 700),
    ],
)
def test_validation_rejects_text_that_would_be_silently_truncated(
    entity: str,
    field: str,
    length: int,
    limit: int,
) -> None:
    raw = complete_bundle()
    raw[entity][0][field] = "가" * length  # type: ignore[index]

    with pytest.raises(editorial_ingest.EditorialValidationError, match=f"at most {limit}"):
        editorial_ingest.validate_bundle(raw)


@pytest.mark.parametrize(
    ("entity", "field"),
    [
        ("actors", "actor_type"),
        ("campaigns", "outcome"),
        ("proposal_votes", "result"),
        ("commitment_outcomes", "status"),
    ],
)
def test_validation_rejects_values_outside_public_api_enums(entity: str, field: str) -> None:
    raw = complete_bundle()
    raw[entity][0][field] = "unsupported_value"  # type: ignore[index]

    with pytest.raises(editorial_ingest.EditorialValidationError, match="not supported"):
        editorial_ingest.validate_bundle(raw)


def test_validation_rejects_more_than_twenty_actor_aliases() -> None:
    raw = complete_bundle()
    raw["actors"][0]["aliases"] = [f"alias-{index}" for index in range(21)]  # type: ignore[index]

    with pytest.raises(editorial_ingest.EditorialValidationError, match="at most 20"):
        editorial_ingest.validate_bundle(raw)


def test_dry_run_is_deterministic_and_never_calls_remote(monkeypatch: pytest.MonkeyPatch) -> None:
    bundle = editorial_ingest.validate_bundle(complete_bundle())

    def forbidden(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise AssertionError("dry-run must not mutate remote state")

    monkeypatch.setattr(editorial_ingest, "post_remote_action", forbidden)
    first = editorial_ingest.ingest_bundle(bundle, dry_run=True, chunk_size=2)
    second = editorial_ingest.ingest_bundle(bundle, dry_run=True, chunk_size=2)

    assert first == second
    assert first["mode"] == "dry-run"
    assert first["total_count"] == 7
    assert first["chunk_count"] == 7
    assert first["accepted"] == {name: 0 for name in editorial_ingest.ENTITY_NAMES}


def test_live_ingest_uses_bounded_deterministic_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    actors = copy.deepcopy(complete_bundle()["actors"])
    assert isinstance(actors, list)
    actors.extend(
        [
            {
                "actor_id": "actor:beta",
                "actor_type": "institution",
                "display_name": "기관 베타",
            },
            {
                "actor_id": "actor:gamma",
                "actor_type": "advisor",
                "display_name": "자문사 감마",
            },
        ]
    )
    bundle = editorial_ingest.validate_bundle({"actors": actors})
    calls: list[dict[str, object]] = []

    def fake_post(action: str, payload: dict[str, object], *, timeout: float) -> dict[str, object]:
        assert action == "upsert_editorial_snapshot"
        assert timeout == 45.0
        calls.append(payload)
        entity = next(name for name in editorial_ingest.ENTITY_NAMES if payload[name])
        return {"ok": True, "accepted": {entity: len(payload[entity])}, "rejected": 0}  # type: ignore[arg-type]

    monkeypatch.setattr(editorial_ingest, "remote_api_configured", lambda: True)
    monkeypatch.setattr(editorial_ingest, "post_remote_action", fake_post)
    report = editorial_ingest.ingest_bundle(bundle, dry_run=False, chunk_size=2)

    assert len(calls) == 2
    assert [len(call["actors"]) for call in calls] == [2, 1]  # type: ignore[arg-type]
    assert calls[0]["chunk_index"] == 1
    assert calls[1]["chunk_index"] == calls[1]["chunk_count"] == 2
    assert calls[0]["bundle_sha256"] == calls[1]["bundle_sha256"] == report["bundle_sha256"]
    assert calls[0]["chunk_id"].endswith(":0001")  # type: ignore[union-attr]
    assert all(call[name] == [] for call in calls for name in editorial_ingest.ENTITY_NAMES if name != "actors")
    assert report["accepted"] == {"actors": 3, **{name: 0 for name in editorial_ingest.ENTITY_NAMES[1:]}}


def test_live_ingest_fails_on_any_server_rejection(monkeypatch: pytest.MonkeyPatch) -> None:
    bundle = editorial_ingest.validate_bundle({"actors": complete_bundle()["actors"]})
    monkeypatch.setattr(editorial_ingest, "remote_api_configured", lambda: True)
    monkeypatch.setattr(
        editorial_ingest,
        "post_remote_action",
        lambda *_args, **_kwargs: {"ok": True, "accepted": {"actors": 0}, "rejected": 1},
    )

    with pytest.raises(RuntimeError, match="rejected"):
        editorial_ingest.ingest_bundle(bundle, dry_run=False)


def test_published_json_schema_is_valid_json_and_lists_all_entities() -> None:
    schema_path = Path(__file__).resolve().parents[1] / "docs" / "schemas" / "editorial-ingest-bundle.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    assert schema["$schema"] == "https://json-schema.org/draft/2020-12/schema"
    assert set(editorial_ingest.ENTITY_NAMES) <= schema["properties"].keys()
