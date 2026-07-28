from __future__ import annotations

import copy
import base64
import gzip
from datetime import datetime, timezone
from typing import Mapping

import pytest

from curator.global_alpha_expedited_editorial import (
    CANDIDATE_KIND,
    DECISION_KIND,
    EVENT_COUNT,
    PAIR_COUNT,
    TOP5_COUNT,
    ExpeditedEditorialError,
    _brief_id,
    apply_publication,
    canonical_sha256,
    decode_human_decisions_secret,
    export_candidates,
)


REVISION = "a" * 40
NOW = "2026-07-28T12:00:00Z"
ARTIFACT_DIGEST = "sha256:" + "b" * 64


def _document(index: int, country: str) -> dict[str, object]:
    if country == "KR":
        connector_id = "connector:kr:dart"
        source_right_id = "official:dart"
        source_key = "dart"
        base_url = "https://opendart.fss.or.kr"
        url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={index:014d}"
        language = "ko"
    else:
        connector_id = "connector:us:sec-edgar"
        source_right_id = "official:sec-edgar"
        source_key = "sec-edgar"
        base_url = "https://www.sec.gov"
        url = f"https://www.sec.gov/Archives/edgar/data/{index}/filing.htm"
        language = "en"
    return {
        "document_id": f"document:{index:03d}",
        "issuer_id": f"issuer:{country.casefold()}:{index % 10:02d}",
        "country_code": country,
        "source_right_id": source_right_id,
        "source_class": "official_disclosure",
        "source_key": source_key,
        "document_type": "filing",
        "original_language": language,
        "title": f"Original filing title {index}",
        "original_url": url,
        "content_hash": f"{index + 1:064x}",
        "filed_at": NOW,
        "published_at": NOW,
        "retrieved_at": NOW,
        "updated_at": NOW,
        "relation_type": "evidence",
        "position_no": 0,
        "connector_id": connector_id,
        "connector_base_url": base_url,
        "coverage_mode": "market-wide",
        "connector_status": "active",
    }


def _event(index: int, *, country: str | None = None) -> dict[str, object]:
    selected_country = country or ("KR" if index % 2 == 0 else "US")
    document = _document(index, selected_country)
    event = {
        "event_id": f"event:{index:03d}",
        "issuer_id": document["issuer_id"],
        "issuer_name": f"Issuer {index % 10}",
        "country": selected_country,
        "event_family": (
            "large_ownership" if index % 2 == 0 else "meeting_and_vote"
        ),
        "title": f"Original event title {index}",
        "original_language": document["original_language"],
        "summary": f"Source-grounded summary {index}",
        "occurred_at": NOW,
        "deadline_at": None,
        "importance": "high",
        "verification_status": "official",
        "change_type": "new",
        "current_status": "filed",
        "first_observed_at": NOW,
        "review_status": "pending",
        "publication_status": "draft",
        "identity_action": f"action {index}",
        "identity_target": f"target {index}",
        "identity_actor_id": None,
        "identity_effective_at": NOW,
        "identity_deadline_at": None,
        "identity_status": "needs_review",
        "comparison_key": None,
        "updated_at": NOW,
        "latest_revision_reason": None,
        "latest_revision_value": None,
        "merged_into_event_id": None,
        "official_documents": [document],
        "official_evidence_count": 1,
        "actors": [],
    }
    event["event_evidence_sha256"] = canonical_sha256(
        {
            "event_id": event["event_id"],
            "event_updated_at": event["updated_at"],
            "official_documents": event["official_documents"],
        }
    )
    return event


class _ExportClient:
    def __init__(self, events: list[dict[str, object]]) -> None:
        self.events = events

    def health(self) -> dict[str, object]:
        return {
            "ok": True,
            "service": "bside-global-market-terminal",
            "code_revision": REVISION,
            "schema_version": 12,
            "time": NOW,
            "api_version": "v2",
        }

    def candidates(self) -> dict[str, object]:
        return {
            "ok": True,
            "data": {"items": self.events},
            "meta": {
                "returned": len(self.events),
                "limit": 50,
                "code_revision": REVISION,
                "snapshot_sha256": "c" * 64,
            },
            "api_version": "v2",
        }


def _candidate_bundle() -> tuple[dict[str, object], dict[str, object]]:
    candidate, template, _ = export_candidates(
        _ExportClient([_event(index) for index in range(30)]),  # type: ignore[arg-type]
        expected_revision=REVISION,
    )
    return candidate, template


def _human_decisions(
    candidate: Mapping[str, object],
    template: Mapping[str, object],
) -> dict[str, object]:
    decisions = copy.deepcopy(template)
    decisions.update(
        {
            "schema_version": 1,
            "kind": DECISION_KIND,
            "environment": "production",
            "is_synthetic": False,
            "code_revision": REVISION,
            "candidate_artifact": {
                "run_id": 123,
                "artifact_id": 456,
                "artifact_name": (
                    "global-alpha-expedited-editorial-candidates-" + REVISION
                ),
                "artifact_digest": ARTIFACT_DIGEST,
            },
            "candidate_sha256": candidate["candidate_sha256"],
            "ground_truth_source": "human",
            "ai_generated_ground_truth": False,
            "human_attestation": True,
            "reviewer_reference": "oversight-operator-20260728",
            "reviewed_at": NOW,
        }
    )
    events = decisions["event_reviews"]
    assert isinstance(events, list)
    for index, review in enumerate(events):
        assert isinstance(review, dict)
        review.update(
            {
                "decision": "approved" if index < TOP5_COUNT else "rejected",
                "reviewer_type": "human",
                "reviewer_reference": "oversight-operator-20260728",
                "reviewed_at": NOW,
            }
        )
        payload = review["review_payload"]
        assert isinstance(payload, dict)
        payload["reason"] = f"Human reviewed official evidence {index}"
        if index < TOP5_COUNT:
            payload.update(
                {
                    "decision": "approve",
                    "event_family": (
                        "large_ownership"
                        if index % 2 == 0
                        else "meeting_and_vote"
                    ),
                    "identity_action": f"action {index}",
                    "identity_target": f"target {index}",
                    "identity_effective_at": NOW,
                    "identity_deadline_at": None,
                    "importance": "high",
                    "summary": f"Source-grounded summary {index}",
                    "current_status": "filed",
                    "actor": {
                        "actor_id": f"actor:{index:03d}",
                        "display_name": f"Actor {index}",
                        "actor_type": "institution",
                        "actor_role": "filer",
                        "country_code": "KR" if index % 2 == 0 else "US",
                    },
                    "merge_into_event_id": None,
                }
            )
        else:
            for field in (
                "event_family",
                "identity_action",
                "identity_target",
                "identity_effective_at",
                "identity_deadline_at",
                "importance",
                "summary",
                "current_status",
                "actor",
                "merge_into_event_id",
            ):
                payload[field] = None
            payload["decision"] = "reject"
    pairs = decisions["same_event_pair_reviews"]
    assert isinstance(pairs, list)
    for index, review in enumerate(pairs):
        assert isinstance(review, dict)
        review.update(
            {
                "decision": index % 2 == 0,
                "reviewer_type": "human",
                "reviewer_reference": "oversight-operator-20260728",
                "reviewed_at": NOW,
            }
        )
    top5 = decisions["top5_reviews"]
    assert isinstance(top5, list)
    for index, review in enumerate(top5):
        assert isinstance(review, dict)
        review.update(
            {
                "decision": "approved",
                "selection_reason": f"Material official event {index}",
                "reviewer_type": "human",
                "reviewer_reference": "oversight-operator-20260728",
                "reviewed_at": NOW,
            }
        )
    return decisions


def test_export_produces_exact_blank_20_40_5_and_readable_pack() -> None:
    candidate, template, markdown = export_candidates(
        _ExportClient([_event(index) for index in range(30)]),  # type: ignore[arg-type]
        expected_revision=REVISION,
    )
    assert candidate["kind"] == CANDIDATE_KIND
    assert candidate["candidate_sha256"] == canonical_sha256(candidate["basis"])
    assert candidate["raw_counts"] == {
        "event_candidate_count": EVENT_COUNT,
        "same_event_pair_candidate_count": PAIR_COUNT,
        "top5_candidate_count": TOP5_COUNT,
    }
    assert template["human_attestation"] is False
    assert template["ground_truth_source"] is None
    assert all(
        item["decision"] is None  # type: ignore[index]
        for item in template["event_reviews"]  # type: ignore[union-attr]
    )
    assert "사건 20건" in markdown
    assert "동일 사건 후보 40쌍" in markdown
    assert str(candidate["candidate_sha256"]) in markdown


def test_export_rejects_any_non_kr_us_event() -> None:
    events = [_event(index) for index in range(29)]
    events.append(_event(29, country="JP"))
    with pytest.raises(ExpeditedEditorialError, match="KR/US event"):
        export_candidates(  # type: ignore[arg-type]
            _ExportClient(events),
            expected_revision=REVISION,
        )


def test_export_rejects_connector_host_mismatch_and_document_body() -> None:
    events = [_event(index) for index in range(30)]
    events[0]["official_documents"][0]["original_url"] = (  # type: ignore[index]
        "https://example.com/not-official"
    )
    with pytest.raises(ExpeditedEditorialError, match="connector identity"):
        export_candidates(  # type: ignore[arg-type]
            _ExportClient(events),
            expected_revision=REVISION,
        )
    events = [_event(index) for index in range(30)]
    events[0]["official_documents"][0]["body_text"] = "must never export"  # type: ignore[index]
    with pytest.raises(ExpeditedEditorialError, match="forbidden document content"):
        export_candidates(  # type: ignore[arg-type]
            _ExportClient(events),
            expected_revision=REVISION,
        )


def test_export_rejects_cross_issuer_documents_and_unexpected_fields() -> None:
    events = [_event(index) for index in range(30)]
    events[0]["official_documents"][0]["issuer_id"] = "issuer:kr:other"  # type: ignore[index]
    with pytest.raises(ExpeditedEditorialError, match="issuer/country binding"):
        export_candidates(  # type: ignore[arg-type]
            _ExportClient(events),
            expected_revision=REVISION,
        )

    events = [_event(index) for index in range(30)]
    events[0]["official_documents"][0]["credential_hint"] = "must not export"  # type: ignore[index]
    with pytest.raises(ExpeditedEditorialError, match="exact safe document fields"):
        export_candidates(  # type: ignore[arg-type]
            _ExportClient(events),
            expected_revision=REVISION,
        )


def test_export_sanitizes_actor_metadata_and_markdown_link_injection() -> None:
    events = [_event(index) for index in range(30)]
    events[0]["actors"] = [
        {
            "actor_id": "actor:001",
            "display_name": "Human ](https://evil.example)",
            "actor_type": "institution",
            "country_code": "KR",
            "actor_role": "filer",
            "actor_review_status": "pending",
            "relation_review_status": "pending",
            "record_status": "internal",
            "updated_at": NOW,
        }
    ]
    document = events[0]["official_documents"][0]  # type: ignore[index]
    document["title"] = "Official ](https://evil.example) filing"  # type: ignore[index]
    events[0]["event_evidence_sha256"] = canonical_sha256(
        {
            "event_id": events[0]["event_id"],
            "event_updated_at": events[0]["updated_at"],
            "official_documents": events[0]["official_documents"],
        }
    )
    candidate, _, markdown = export_candidates(  # type: ignore[arg-type]
        _ExportClient(events),
        expected_revision=REVISION,
    )
    first = candidate["basis"]["events"][0]  # type: ignore[index]
    assert set(first["actors"][0]) == {  # type: ignore[index]
        "actor_id",
        "display_name",
        "actor_type",
        "actor_role",
        "country_code",
    }
    assert "](https://evil.example)" not in markdown
    assert "\\]\\(https://evil.example\\)" in markdown


class _ApplyClient:
    def __init__(self, candidate: Mapping[str, object]) -> None:
        basis = candidate["basis"]
        assert isinstance(basis, dict)
        self.events = {
            str(event["event_id"]): copy.deepcopy(event)
            for event in basis["events"]
        }
        self.brief_payload: dict[str, object] | None = None

    def health(self) -> dict[str, object]:
        raise AssertionError("unused")

    def event(self, event_id: str) -> dict[str, object]:
        return {
            "ok": True,
            "data": {"event": copy.deepcopy(self.events[event_id])},
            "meta": {
                "code_revision": REVISION,
                "snapshot_sha256": "d" * 64,
            },
            "api_version": "v2",
        }

    def review(
        self, event_id: str, payload: Mapping[str, object]
    ) -> dict[str, object]:
        event = self.events[event_id]
        event["latest_revision_reason"] = payload["reason"]
        event["updated_at"] = "2026-07-28T12:01:00Z"
        if payload["decision"] == "reject":
            event.update(
                {
                    "review_status": "rejected",
                    "publication_status": "draft",
                    "identity_status": "rejected",
                }
            )
            decision = "rejected"
            published = False
        else:
            actor = copy.deepcopy(payload["actor"])
            assert isinstance(actor, dict)
            event.update(
                {
                    "review_status": "approved",
                    "publication_status": "published",
                    "identity_status": "complete",
                    "event_family": payload["event_family"],
                    "importance": payload["importance"],
                    "summary": payload["summary"],
                    "current_status": payload["current_status"],
                    "identity_action": str(payload["identity_action"]).casefold(),
                    "identity_target": str(payload["identity_target"]).casefold(),
                    "identity_actor_id": actor["actor_id"],
                    "identity_effective_at": payload["identity_effective_at"],
                    "identity_deadline_at": payload["identity_deadline_at"],
                    "actors": [
                        {
                            **actor,
                            "actor_review_status": "approved",
                            "record_status": "active",
                            "relation_review_status": "approved",
                            "updated_at": event["updated_at"],
                        }
                    ],
                }
            )
            decision = "approved"
            published = True
        event["event_evidence_sha256"] = canonical_sha256(
            {
                "event_id": event_id,
                "event_updated_at": event["updated_at"],
                "official_documents": event["official_documents"],
            }
        )
        return {
            "ok": True,
            "data": {
                "event_id": event_id,
                "decision": decision,
                "published": published,
            },
            "api_version": "v2",
        }

    def publish_brief(
        self, payload: Mapping[str, object]
    ) -> dict[str, object]:
        idempotent = self.brief_payload is not None
        if self.brief_payload is not None and self.brief_payload != payload:
            raise AssertionError("semantic brief changed")
        self.brief_payload = dict(payload)
        return {
            "ok": True,
            "data": {
                "brief_id": _brief_id(str(payload["cutoff_at"])),
                "edition": "global",
                "published": True,
                "idempotent": idempotent,
            },
            "api_version": "v2",
        }


def test_protected_apply_publishes_then_replays_idempotently() -> None:
    candidate, template = _candidate_bundle()
    decisions = _human_decisions(candidate, template)
    client = _ApplyClient(candidate)
    common = {
        "client": client,
        "candidate": candidate,
        "decisions": decisions,
        "revision": REVISION,
        "candidate_run_id": 123,
        "candidate_artifact_id": 456,
        "candidate_artifact_name": (
            "global-alpha-expedited-editorial-candidates-" + REVISION
        ),
        "candidate_artifact_digest": ARTIFACT_DIGEST,
        "now": datetime.fromisoformat(NOW.replace("Z", "+00:00")).astimezone(
            timezone.utc
        ),
    }
    human_review, first = apply_publication(**common)  # type: ignore[arg-type]
    _, replay = apply_publication(**common)  # type: ignore[arg-type]
    assert len(human_review["event_reviews"]) == EVENT_COUNT
    assert len(human_review["same_event_pair_reviews"]) == PAIR_COUNT
    assert len(human_review["top5_reviews"]) == TOP5_COUNT
    assert "artifact_id" not in human_review
    assert first["mutations_applied"] == EVENT_COUNT
    assert first["idempotent_replay"] is False
    assert replay["mutations_applied"] == 0
    assert replay["idempotent_replay"] is True
    assert first["semantic_receipt_sha256"] == replay["semantic_receipt_sha256"]


def test_stale_event_fails_before_first_mutation() -> None:
    candidate, template = _candidate_bundle()
    decisions = _human_decisions(candidate, template)
    client = _ApplyClient(candidate)
    stale_event_id = next(reversed(client.events))
    client.events[stale_event_id]["updated_at"] = "2026-07-28T12:05:00Z"
    client.events[stale_event_id]["event_evidence_sha256"] = canonical_sha256(
        {
            "event_id": stale_event_id,
            "event_updated_at": "2026-07-28T12:05:00Z",
            "official_documents": client.events[stale_event_id][
                "official_documents"
            ],
        }
    )
    with pytest.raises(ExpeditedEditorialError, match="preflight: stale"):
        apply_publication(  # type: ignore[arg-type]
            client,
            candidate=candidate,
            decisions=decisions,
            revision=REVISION,
            candidate_run_id=123,
            candidate_artifact_id=456,
            candidate_artifact_name=(
                "global-alpha-expedited-editorial-candidates-" + REVISION
            ),
            candidate_artifact_digest=ARTIFACT_DIGEST,
            now=datetime.fromisoformat(NOW.replace("Z", "+00:00")),
        )
    assert all(
        event["review_status"] == "pending" for event in client.events.values()
    )


def test_invalid_human_actor_contract_fails_before_first_mutation() -> None:
    candidate, template = _candidate_bundle()
    decisions = _human_decisions(candidate, template)
    reviews = decisions["event_reviews"]
    assert isinstance(reviews, list)
    payload = reviews[0]["review_payload"]
    assert isinstance(payload, dict)
    actor = payload["actor"]
    assert isinstance(actor, dict)
    actor["actor_role"] = "Invalid Role"
    client = _ApplyClient(candidate)
    with pytest.raises(ExpeditedEditorialError, match="invalid actor_role"):
        apply_publication(  # type: ignore[arg-type]
            client,
            candidate=candidate,
            decisions=decisions,
            revision=REVISION,
            candidate_run_id=123,
            candidate_artifact_id=456,
            candidate_artifact_name=(
                "global-alpha-expedited-editorial-candidates-" + REVISION
            ),
            candidate_artifact_digest=ARTIFACT_DIGEST,
            now=datetime.fromisoformat(NOW.replace("Z", "+00:00")),
        )
    assert all(
        event["review_status"] == "pending" for event in client.events.values()
    )


def test_bounded_human_decision_decoder_rejects_invalid_and_bomb() -> None:
    written: list[bytes] = []

    class Output:
        parent: "Output"

        def __init__(self) -> None:
            self.parent = self

        def mkdir(self, **_kwargs: object) -> None:
            return None

        def write_bytes(self, value: bytes) -> None:
            written.append(value)

        def chmod(self, _mode: int) -> None:
            return None

    output = Output()
    value = b'{"kind":"human"}'
    decode_human_decisions_secret(
        base64.b64encode(gzip.compress(value)).decode("ascii"),
        output,  # type: ignore[arg-type]
    )
    assert written == [value]
    with pytest.raises(ExpeditedEditorialError, match="invalid base64"):
        decode_human_decisions_secret("%%%invalid%%%", output)  # type: ignore[arg-type]
    bomb = base64.b64encode(gzip.compress(b"x" * 5_000_001)).decode("ascii")
    with pytest.raises(ExpeditedEditorialError, match="decompressed size"):
        decode_human_decisions_secret(bomb, output)  # type: ignore[arg-type]
