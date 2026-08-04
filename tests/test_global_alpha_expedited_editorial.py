from __future__ import annotations

import copy
import base64
import gzip
import hashlib
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

import httpx
import pytest

import curator.global_alpha_expedited_editorial as editorial
from curator.global_alpha_expedited_editorial import (
    APPROVED_CANONICAL_BASIS_KIND,
    CANDIDATE_KIND,
    DECISION_KIND,
    EVENT_COUNT,
    LEGACY_APPROVAL_ACTION_OVERRIDES,
    LEGACY_APPROVAL_REVIEWER,
    PAIR_COUNT,
    TOP5_COUNT,
    ExpeditedEditorialError,
    EditorialClient,
    _brief_id,
    _global_comparison_key,
    _validate_approved_canonical_basis,
    apply_publication,
    canonical_sha256,
    decode_human_decisions_secret,
    export_candidates,
    prepare_carry_forward_publication,
    publish_carry_forward_intent,
    repair_legacy_display_targets,
    validate_display_target_repair_receipts,
)


REVISION = "a" * 40
NOW = "2026-07-28T12:00:00Z"
CARRY_NOW = "2026-07-28T12:10:00Z"
ARTIFACT_DIGEST = "sha256:" + "b" * 64
TEST_CANDIDATE_ARTIFACT = {
    "run_id": 123,
    "artifact_id": 456,
    "artifact_name": (
        "global-alpha-expedited-editorial-candidates-" + REVISION
    ),
    "artifact_digest": ARTIFACT_DIGEST,
}
TEST_PUBLICATION_ARTIFACT = {
    "run_id": 789,
    "artifact_id": 987,
    "artifact_name": (
        "global-alpha-expedited-editorial-publication-"
        + REVISION
        + "-789-1"
    ),
    "artifact_digest": "sha256:" + "d" * 64,
}


def _editorial_response() -> httpx.Response:
    return httpx.Response(
        200,
        json={
            "ok": True,
            "api_version": "v2",
            "service": "bside-global-market-terminal",
            "code_revision": REVISION,
            "schema_version": 12,
        },
    )


def test_editorial_client_retries_only_idempotent_get_transport_errors() -> None:
    calls: list[tuple[str, str]] = []
    clients: list[httpx.Client] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.headers["Connection"]))
        if len(calls) < 3:
            raise httpx.RemoteProtocolError(
                "transient server disconnect",
                request=request,
            )
        return _editorial_response()

    def client_factory(**kwargs: object) -> httpx.Client:
        client = httpx.Client(**kwargs)  # type: ignore[arg-type]
        clients.append(client)
        return client

    client = EditorialClient(
        "https://alignpe.gabia.io/activist/api.php/api/v2",
        "x" * 32,
        transport=httpx.MockTransport(handler),
        client_factory=client_factory,
    )
    try:
        response = client.health()
    finally:
        client.close()

    assert response["ok"] is True
    assert calls == [("GET", "close")] * 3
    assert len(clients) == 3


def test_editorial_client_bounds_get_retries_and_never_retries_post() -> None:
    get_calls = 0
    secret_token = "token-" + "z" * 32

    def failing_get(request: httpx.Request) -> httpx.Response:
        nonlocal get_calls
        get_calls += 1
        raise httpx.RemoteProtocolError("transient", request=request)

    get_client = EditorialClient(
        "https://alignpe.gabia.io/activist/api.php/api/v2",
        secret_token,
        transport=httpx.MockTransport(failing_get),
    )
    try:
        with pytest.raises(ExpeditedEditorialError, match="request failed") as error:
            get_client.health()
    finally:
        get_client.close()
    assert get_calls == 3
    assert secret_token not in str(error.value)
    assert secret_token not in "".join(
        traceback.format_exception(error.type, error.value, error.tb)
    )

    write_calls = 0

    def failing_write(request: httpx.Request) -> httpx.Response:
        nonlocal write_calls
        write_calls += 1
        raise httpx.RemoteProtocolError("do not retry writes", request=request)

    for method in ("POST", "PATCH"):
        write_client = EditorialClient(
            "https://alignpe.gabia.io/activist/api.php/api/v2",
            "x" * 32,
            transport=httpx.MockTransport(failing_write),
        )
        try:
            with pytest.raises(ExpeditedEditorialError, match="request failed"):
                write_client._json(method, "/admin/write", payload={})
        finally:
            write_client.close()
    assert write_calls == 2


def test_editorial_client_does_not_retry_http_contract_failures() -> None:
    calls = 0

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            503,
            json={"ok": False, "api_version": "v2", "error": "unavailable"},
        )

    client = EditorialClient(
        "https://alignpe.gabia.io/activist/api.php/api/v2",
        "x" * 32,
        transport=httpx.MockTransport(handler),
    )
    try:
        with pytest.raises(ExpeditedEditorialError, match="HTTP 503"):
            client.health()
    finally:
        client.close()
    assert calls == 1


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
        "issuer_name": f"issuer {index % 10}",
        "country": selected_country,
        "event_family": (
            "large_ownership" if index % 2 == 0 else "meeting_and_vote"
        ),
        "title": (
            f"original event title              ({index})"
            if index in {5, 7, 11, 14, 20, 23}
            else f"original event title {index}"
        ),
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
        "actors": [
            {
                "actor_id": f"actor:{index:03d}",
                "display_name": f"Actor {index}",
                "actor_type": "institution",
                "actor_role": "filer",
                "country_code": None,
            }
        ],
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


def _legacy_candidate_bundle() -> tuple[dict[str, object], dict[str, object]]:
    candidate, template, _ = export_candidates(
        _ExportClient(
            [_event(index, country="KR") for index in range(30)]
        ),  # type: ignore[arg-type]
        expected_revision=REVISION,
    )
    return candidate, template


def _legacy_human_decisions(
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
            "reviewer_reference": LEGACY_APPROVAL_REVIEWER,
            "reviewed_at": NOW,
        }
    )
    basis = candidate["basis"]
    assert isinstance(basis, dict)
    candidate_events = basis["events"]
    assert isinstance(candidate_events, list)
    event_reviews = decisions["event_reviews"]
    assert isinstance(event_reviews, list)
    for position, (review, event) in enumerate(
        zip(event_reviews, candidate_events, strict=True),
        start=1,
    ):
        assert isinstance(review, dict)
        assert isinstance(event, dict)
        actors = event["actors"]
        assert isinstance(actors, list) and len(actors) == 1
        actor = copy.deepcopy(actors[0])
        assert isinstance(actor, dict)
        actor["country_code"] = "KR"
        effective_at = event["identity_effective_at"] or event["occurred_at"]
        family = (
            "listing_status"
            if position == 11
            else event["event_family"]
        )
        action = LEGACY_APPROVAL_ACTION_OVERRIDES.get(
            position,
            str(event["identity_action"]),
        )
        review.update(
            {
                "decision": "approved",
                "reviewer_type": "human",
                "reviewer_reference": LEGACY_APPROVAL_REVIEWER,
                "reviewed_at": NOW,
            }
        )
        payload = review["review_payload"]
        assert isinstance(payload, dict)
        payload.update(
            {
                "decision": "approve",
                "event_family": family,
                "identity_action": action,
                "identity_target": (
                    str(event["issuer_name"])
                    + " — "
                    + str(event["title"])
                ),
                "identity_effective_at": effective_at,
                "identity_deadline_at": None,
                "importance": event["importance"],
                "summary": (
                    str(event["issuer_name"])
                    + "는 DART에 「"
                    + str(event["title"])
                    + "」을 공시했다."
                ),
                "current_status": (
                    "corrected_official_disclosure"
                    if event["verification_status"] == "corrected"
                    else "official_disclosure_confirmed"
                ),
                "actor": actor,
                "merge_into_event_id": None,
                "reason": f"Legacy human approval E{position:02d}",
            }
        )
    pair_reviews = decisions["same_event_pair_reviews"]
    assert isinstance(pair_reviews, list)
    for review in pair_reviews:
        assert isinstance(review, dict)
        review.update(
            {
                "decision": False,
                "reviewer_type": "human",
                "reviewer_reference": LEGACY_APPROVAL_REVIEWER,
                "reviewed_at": NOW,
            }
        )
    top_reviews = decisions["top5_reviews"]
    assert isinstance(top_reviews, list)
    for position, review in enumerate(top_reviews, start=1):
        assert isinstance(review, dict)
        review.update(
            {
                "decision": "approved",
                "selection_reason": f"Human-approved Top {position}",
                "reviewer_type": "human",
                "reviewer_reference": LEGACY_APPROVAL_REVIEWER,
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


def test_export_diversifies_production_shaped_family_skew_deterministically() -> None:
    events = [_event(index, country="KR") for index in range(50)]
    for index, event in enumerate(events):
        if index < 38:
            event["event_family"] = "capital_issuance"
        elif index < 45:
            event["event_family"] = "tender_offer_and_mna"
        else:
            event["event_family"] = "listing_status"

    first, _, _ = export_candidates(  # type: ignore[arg-type]
        _ExportClient(events),
        expected_revision=REVISION,
    )
    second, _, _ = export_candidates(  # type: ignore[arg-type]
        _ExportClient(events),
        expected_revision=REVISION,
    )

    selected = first["basis"]["events"]  # type: ignore[index]
    pairs = first["basis"]["same_event_pair_candidates"]  # type: ignore[index]
    selected_families = {item["event_family"] for item in selected}
    pair_strata = [item["stratum"] for item in pairs]

    assert len(selected) == EVENT_COUNT
    assert [item["event_id"] for item in selected[:TOP5_COUNT]] == [
        f"event:{index:03d}" for index in range(TOP5_COUNT)
    ]
    assert selected_families == {
        "capital_issuance",
        "tender_offer_and_mna",
        "listing_status",
    }
    assert pair_strata.count("hard_same_issuer_or_family") == PAIR_COUNT // 2
    assert pair_strata.count("easy_cross_issuer_and_family") == PAIR_COUNT // 2
    assert first["candidate_sha256"] == second["candidate_sha256"]
    assert first["basis"] == second["basis"]


def test_export_keeps_single_family_pair_shortage_fail_closed() -> None:
    events = [_event(index, country="KR") for index in range(50)]
    for event in events:
        event["event_family"] = "capital_issuance"

    with pytest.raises(
        ExpeditedEditorialError,
        match="at least 20 hard and 20 easy",
    ):
        export_candidates(  # type: ignore[arg-type]
            _ExportClient(events),
            expected_revision=REVISION,
        )


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


def test_export_preserves_unknown_actor_country_for_human_verification() -> None:
    events = [_event(index) for index in range(30)]
    events[0]["actors"] = [
        {
            "actor_id": "actor:unknown-country",
            "display_name": "Cross-Border Reporting Person",
            "actor_type": "institution",
            "country_code": None,
            "actor_role": "filer",
        }
    ]
    candidate, _, markdown = export_candidates(  # type: ignore[arg-type]
        _ExportClient(events),
        expected_revision=REVISION,
    )
    actors = candidate["basis"]["events"][0]["actors"]  # type: ignore[index]
    assert actors[0]["country_code"] is None  # type: ignore[index]
    assert "국가 미확인" in markdown
    assert "사건 국가로 추론하지 말고" in markdown


@pytest.mark.parametrize("country_code", ["", "ZZ", "kr", "KOR"])
def test_export_rejects_nonempty_or_malformed_actor_country(
    country_code: str,
) -> None:
    events = [_event(index) for index in range(30)]
    events[0]["actors"] = [
        {
            "actor_id": "actor:invalid-country",
            "display_name": "Invalid Country Reporting Person",
            "actor_type": "institution",
            "country_code": country_code,
            "actor_role": "filer",
        }
    ]
    with pytest.raises(ExpeditedEditorialError, match="invalid country_code"):
        export_candidates(  # type: ignore[arg-type]
            _ExportClient(events),
            expected_revision=REVISION,
        )


class _ApplyClient:
    def __init__(
        self,
        candidate: Mapping[str, object],
        *,
        preserve_identity_target: bool = False,
    ) -> None:
        basis = candidate["basis"]
        assert isinstance(basis, dict)
        self.events = {
            str(event["event_id"]): copy.deepcopy(event)
            for event in basis["events"]
        }
        self.preserve_identity_target = preserve_identity_target
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
            event_family = str(payload["event_family"])
            identity_action = str(payload["identity_action"]).casefold()
            identity_target = (
                str(payload["identity_target"])
                if self.preserve_identity_target
                else editorial._normalize_identity(str(payload["identity_target"]))
            )
            comparison_key = _global_comparison_key(
                issuer_id=event["issuer_id"],
                event_family=event_family,
                action=identity_action,
                target=identity_target,
                actor_id=actor["actor_id"],
                effective_at=payload["identity_effective_at"],
                deadline_at=payload["identity_deadline_at"],
                location="test apply",
            )
            verification = str(event["verification_status"])
            event.update(
                {
                    "review_status": "approved",
                    "publication_status": "published",
                    "identity_status": "complete",
                    "event_family": event_family,
                    "importance": payload["importance"],
                    "summary": payload["summary"],
                    "current_status": payload["current_status"],
                    "deadline_at": payload["identity_deadline_at"],
                    "verification_status": (
                        verification
                        if verification in {"corrected", "withdrawn"}
                        else "official"
                    ),
                    "identity_action": identity_action,
                    "identity_target": identity_target,
                    "identity_actor_id": actor["actor_id"],
                    "identity_effective_at": payload["identity_effective_at"],
                    "identity_deadline_at": payload["identity_deadline_at"],
                    "comparison_key": comparison_key,
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


class _CarryClient:
    def __init__(
        self,
        events: Mapping[str, object],
        *,
        revision: str,
    ) -> None:
        self.events = copy.deepcopy(events)
        self.revision = revision
        self.brief_payload: dict[str, object] | None = None
        self.briefs: dict[str, dict[str, object]] = {}
        self.brief_calls = 0
        self.event_calls = 0
        self.mutate_on_event_call: int | None = None
        self.mutate_document_clock = False
        self.fail_brief_call: int | None = None
        self.fail_after_persist_call: int | None = None
        self.review_calls = 0

    def health(self) -> dict[str, object]:
        return {
            "ok": True,
            "service": "bside-global-market-terminal",
            "code_revision": self.revision,
            "schema_version": 12,
            "time": NOW,
            "api_version": "v2",
        }

    def event(self, event_id: str) -> dict[str, object]:
        self.event_calls += 1
        events = self.events
        assert isinstance(events, dict)
        if self.event_calls == self.mutate_on_event_call:
            event = events[event_id]
            assert isinstance(event, dict)
            if self.mutate_document_clock:
                documents = event["official_documents"]
                assert isinstance(documents, list)
                document = documents[0]
                assert isinstance(document, dict)
                document["retrieved_at"] = "2026-07-28T12:03:00Z"
                document["updated_at"] = "2026-07-28T12:03:00Z"
                event["event_evidence_sha256"] = canonical_sha256(
                    {
                        "event_id": event["event_id"],
                        "event_updated_at": event["updated_at"],
                        "official_documents": documents,
                    }
                )
            else:
                event["summary"] = "Drift during publication fence"
        return {
            "ok": True,
            "data": {"event": copy.deepcopy(events[event_id])},
            "meta": {
                "code_revision": self.revision,
                "snapshot_sha256": "e" * 64,
            },
            "api_version": "v2",
        }

    def review(
        self,
        event_id: str,
        payload: Mapping[str, object],
    ) -> dict[str, object]:
        self.review_calls += 1
        event = self.events[event_id]
        assert isinstance(event, dict)
        reason = str(payload["reason"])
        assert reason.startswith("[expedited-candidate:")
        assert "] [human-approval:" in reason
        assert "] [display-target-repair:" in reason
        assert payload["expected_updated_at"] == event["updated_at"]
        assert payload["expected_evidence_sha256"] == event[
            "event_evidence_sha256"
        ]
        before = {
            key: copy.deepcopy(event[key])
            for key in (
                "updated_at",
                "event_evidence_sha256",
                "comparison_key",
                "official_documents",
                "actors",
            )
        }
        event["identity_target"] = payload["identity_target"]
        event["latest_revision_reason"] = reason
        for key, value in before.items():
            assert event[key] == value
        return {
            "ok": True,
            "data": {
                "event_id": event_id,
                "decision": "approved",
                "published": True,
                "display_target_repaired": True,
                "updated_at_preserved": True,
            },
            "api_version": "v2",
        }

    def publish_brief(
        self,
        payload: Mapping[str, object],
    ) -> dict[str, object]:
        self.brief_calls += 1
        if self.brief_calls == self.fail_brief_call:
            raise ExpeditedEditorialError("simulated brief call failure")
        require_existing = payload.get("require_existing") is True
        semantic = {
            key: copy.deepcopy(value)
            for key, value in payload.items()
            if key != "require_existing"
        }
        brief_id = _brief_id(str(payload["cutoff_at"]))
        stored = self.briefs.get(brief_id)
        if stored is not None and stored != semantic:
            raise ExpeditedEditorialError("brief_edition_conflict")
        if stored is None and require_existing:
            raise ExpeditedEditorialError("brief_recovery_not_found")
        idempotent = stored is not None
        if stored is None:
            self.briefs[brief_id] = copy.deepcopy(semantic)
        self.brief_payload = copy.deepcopy(semantic)
        if self.brief_calls == self.fail_after_persist_call:
            raise ExpeditedEditorialError(
                "simulated persisted brief response loss"
            )
        return {
            "ok": True,
            "data": {
                "brief_id": brief_id,
                "edition": "global",
                "published": True,
                "idempotent": idempotent,
            },
            "api_version": "v2",
        }


def _published_carry_source(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
    _ApplyClient,
]:
    candidate, template = _legacy_candidate_bundle()
    monkeypatch.setattr(
        editorial,
        "LEGACY_APPROVAL_CANDIDATE_SHA256",
        candidate["candidate_sha256"],
    )
    monkeypatch.setattr(
        editorial,
        "LEGACY_APPROVAL_CANDIDATE_ARTIFACT",
        TEST_CANDIDATE_ARTIFACT,
    )
    monkeypatch.setattr(
        editorial,
        "LEGACY_APPROVAL_PUBLICATION_ARTIFACT",
        TEST_PUBLICATION_ARTIFACT,
    )
    decisions = _legacy_human_decisions(candidate, template)
    source = _ApplyClient(candidate)
    common = {
        "client": source,
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
    human_review, receipt = apply_publication(**common)  # type: ignore[arg-type]
    _, replay = apply_publication(**common)  # type: ignore[arg-type]
    return candidate, human_review, receipt, replay, source


def _fresh_human_decisions(
    candidate: Mapping[str, object],
    template: Mapping[str, object],
) -> dict[str, object]:
    decisions = _legacy_human_decisions(candidate, template)
    events = candidate["basis"]["events"]  # type: ignore[index]
    reviews = decisions["event_reviews"]
    assert isinstance(events, list) and isinstance(reviews, list)
    for position, (event, review) in enumerate(
        zip(events, reviews, strict=True), start=1
    ):
        assert isinstance(event, dict) and isinstance(review, dict)
        payload = review["review_payload"]
        assert isinstance(payload, dict)
        review["reviewer_reference"] = editorial.FRESH_APPROVAL_REVIEWER
        if position == 15:
            review["decision"] = "rejected"
            payload["decision"] = "reject"
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
            payload["reason"] = "Fresh human rejection E15"
            continue
        override = editorial.FRESH_APPROVAL_EVENT_OVERRIDES.get(
            str(event["event_id"]), {}
        )
        payload.update(
            {
                "event_family": override.get(
                    "event_family", event["event_family"]
                ),
                "identity_action": override.get(
                    "identity_action", event["identity_action"]
                ),
                "identity_target": (
                    str(event["issuer_name"]) + " — " + str(event["title"])
                ),
                "identity_effective_at": event["occurred_at"],
                "identity_deadline_at": None,
                "summary": (
                    str(event["issuer_name"])
                    + " — DART에 「"
                    + str(event["title"])
                    + "」 공시."
                ),
                "current_status": "official_disclosure_confirmed",
                "reason": f"Fresh human approval E{position:02d}",
            }
        )
    decisions["reviewer_reference"] = editorial.FRESH_APPROVAL_REVIEWER
    for group in ("same_event_pair_reviews", "top5_reviews"):
        items = decisions[group]
        assert isinstance(items, list)
        for item in items:
            assert isinstance(item, dict)
            item["reviewer_reference"] = editorial.FRESH_APPROVAL_REVIEWER
    return decisions


def _fresh_carry_common(
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[_CarryClient, dict[str, object]]:
    raw_events = [_event(index, country="KR") for index in range(30)]
    for index, event in enumerate(raw_events):
        event["title"] = f"Fresh official filing {index}"
    raw_events[1]["title"] = "Fresh official    filing 1"
    candidate, template, _ = export_candidates(
        _ExportClient(raw_events),  # type: ignore[arg-type]
        expected_revision=REVISION,
    )
    candidate_events = candidate["basis"]["events"]  # type: ignore[index]
    assert isinstance(candidate_events, list)
    event_ids = [str(item["event_id"]) for item in candidate_events]
    decisions_profile = tuple(
        (event_id, "rejected" if position == 15 else "approved")
        for position, event_id in enumerate(event_ids, start=1)
    )
    override_positions = {
        4: {"identity_action": "rights_issue_price_finalized"},
        7: {
            "event_family": "listing_status",
            "identity_action": "listing_eligibility_improvement_plan_disclosed",
        },
        8: {
            "event_family": "listing_status",
            "identity_action": (
                "trading_suspension_for_share_consolidation_or_split"
            ),
        },
        11: {
            "event_family": "listing_status",
            "identity_action": (
                "trading_suspension_for_share_consolidation_or_split"
            ),
        },
        12: {
            "identity_action": "treasury_convertible_bond_early_acquisition"
        },
        14: {"identity_action": "rights_issue_initial_price_determined"},
        17: {
            "event_family": "listing_status",
            "identity_action": (
                "trading_suspension_for_share_consolidation_or_split"
            ),
        },
    }
    overrides = {
        event_ids[position - 1]: value
        for position, value in override_positions.items()
    }
    monkeypatch.setattr(editorial, "FRESH_APPROVAL_SOURCE_REVISION", REVISION)
    monkeypatch.setattr(
        editorial, "FRESH_APPROVAL_CANDIDATE_ARTIFACT", TEST_CANDIDATE_ARTIFACT
    )
    monkeypatch.setattr(
        editorial,
        "FRESH_APPROVAL_PUBLICATION_ARTIFACT",
        TEST_PUBLICATION_ARTIFACT,
    )
    monkeypatch.setattr(
        editorial, "FRESH_APPROVAL_CANDIDATE_SHA256", candidate["candidate_sha256"]
    )
    monkeypatch.setattr(
        editorial, "FRESH_APPROVAL_EVENT_DECISIONS", decisions_profile
    )
    monkeypatch.setattr(editorial, "FRESH_APPROVAL_EVENT_OVERRIDES", overrides)
    decisions = _fresh_human_decisions(candidate, template)
    source = _ApplyClient(candidate, preserve_identity_target=True)
    apply_common = {
        "client": source,
        "candidate": candidate,
        "decisions": decisions,
        "revision": REVISION,
        "candidate_run_id": 123,
        "candidate_artifact_id": 456,
        "candidate_artifact_name": TEST_CANDIDATE_ARTIFACT["artifact_name"],
        "candidate_artifact_digest": ARTIFACT_DIGEST,
        "now": datetime.fromisoformat(NOW.replace("Z", "+00:00")),
    }
    human_review, receipt = apply_publication(**apply_common)  # type: ignore[arg-type]
    _, replay = apply_publication(**apply_common)  # type: ignore[arg-type]
    monkeypatch.setattr(
        editorial,
        "FRESH_APPROVAL_SOURCE_DECISION_SHA256",
        receipt["decision_sha256"],
    )
    monkeypatch.setattr(
        editorial,
        "FRESH_APPROVAL_SOURCE_SEMANTIC_SHA256",
        receipt["semantic_receipt_sha256"],
    )
    monkeypatch.setattr(
        editorial,
        "FRESH_APPROVAL_HUMAN_SECTION_SHA256",
        human_review["section_sha256"],
    )
    revision = "c" * 40
    client = _CarryClient(source.events, revision=revision)
    return client, {
        "client": client,
        "candidate": candidate,
        "source_human_review": human_review,
        "source_receipt": receipt,
        "source_replay_receipt": replay,
        "revision": revision,
        "candidate_artifact": TEST_CANDIDATE_ARTIFACT,
        "publication_artifact": TEST_PUBLICATION_ARTIFACT,
        "intent_artifact": {
            "run_id": 1001,
            "artifact_id": 2001,
            "artifact_name": (
                "global-alpha-expedited-editorial-carry-intent-"
                + revision
                + "-1001-1"
            ),
            "artifact_digest": "sha256:" + "9" * 64,
        },
        "now": datetime.fromisoformat(CARRY_NOW.replace("Z", "+00:00")),
    }


def _carry_common(
    monkeypatch: pytest.MonkeyPatch,
    *,
    display_targets_repaired: bool = True,
) -> tuple[_CarryClient, dict[str, object]]:
    candidate, human_review, receipt, replay, source = (
        _published_carry_source(monkeypatch)
    )
    attestation = editorial._load_legacy_human_approval_artifact()
    candidate_events = candidate["basis"]["events"]  # type: ignore[index]
    assert isinstance(candidate_events, list)
    event_reviews = human_review["event_reviews"]
    assert isinstance(event_reviews, list)
    attestation.update(
        {
            "reviewed_at": event_reviews[0]["reviewed_at"],
            "attestation_source": "test_human_message",
            "attestation_text_sha256": hashlib.sha256(
                b"Synthetic fixture human attestation"
            ).hexdigest(),
            "source_candidate_sha256": candidate["candidate_sha256"],
            "source_decision_sha256": receipt["decision_sha256"],
            "source_candidate_artifact": TEST_CANDIDATE_ARTIFACT,
            "source_publication_artifact": TEST_PUBLICATION_ARTIFACT,
            "event_approvals": [
                {
                    "position_no": position,
                    "event_id": event["event_id"],
                    "event_family": (
                        "listing_status"
                        if position == 11
                        else event["event_family"]
                    ),
                    "identity_action": (
                        LEGACY_APPROVAL_ACTION_OVERRIDES.get(
                            position,
                            event["identity_action"],
                        )
                    ),
                    "decision": "approved",
                }
                for position, event in enumerate(
                    candidate_events,
                    start=1,
                )
            ],
            "top5_approvals": [
                {
                    "position_no": item["position_no"],
                    "event_id": item["event_id"],
                    "decision": "approved",
                }
                for item in receipt["top5"]
            ],
        }
    )
    attestation_sha = canonical_sha256(attestation)
    monkeypatch.setattr(
        editorial,
        "LEGACY_APPROVAL_ATTESTATION_SHA256",
        attestation_sha,
    )
    monkeypatch.setattr(
        editorial,
        "LEGACY_APPROVAL_TEXT_SHA256",
        attestation["attestation_text_sha256"],
    )
    monkeypatch.setattr(
        editorial,
        "LEGACY_APPROVAL_SOURCE_DECISION_SHA256",
        receipt["decision_sha256"],
    )
    monkeypatch.setattr(
        editorial,
        "_load_legacy_human_approval_artifact",
        lambda: copy.deepcopy(attestation),
    )
    correction = editorial._load_legacy_human_approval_correction()
    event_ids = [str(item["event_id"]) for item in candidate_events]
    event_ids_sha = canonical_sha256(event_ids)
    correction.update(
        {
            "base_approval_canonical_sha256": attestation_sha,
            "source_candidate_sha256": candidate["candidate_sha256"],
            "source_decision_sha256": receipt["decision_sha256"],
            "source_candidate_artifact": TEST_CANDIDATE_ARTIFACT,
            "source_publication_artifact": TEST_PUBLICATION_ARTIFACT,
            "event_ids": event_ids,
            "event_ids_sha256": event_ids_sha,
        }
    )
    correction_sha = canonical_sha256(correction)
    monkeypatch.setattr(
        editorial,
        "LEGACY_APPROVAL_CORRECTION_SHA256",
        correction_sha,
    )
    monkeypatch.setattr(
        editorial,
        "LEGACY_APPROVAL_EVENT_IDS_SHA256",
        event_ids_sha,
    )
    chain_sha = canonical_sha256(editorial._legacy_human_approval_chain_basis())
    monkeypatch.setattr(editorial, "LEGACY_APPROVAL_CHAIN_SHA256", chain_sha)
    monkeypatch.setattr(
        editorial,
        "_load_legacy_human_approval_correction",
        lambda: copy.deepcopy(correction),
    )
    revision = "c" * 40
    client = _CarryClient(source.events, revision=revision)
    approved_repair_ids = []
    candidate_by_id = {
        str(item["event_id"]): item for item in candidate_events
    }
    for event_id, event in client.events.items():
        assert isinstance(event, dict)
        candidate_event = candidate_by_id[event_id]
        exact_target = (
            str(candidate_event["issuer_name"])
            + " — "
            + str(candidate_event["title"])
        )
        if exact_target == editorial._normalize_identity(exact_target):
            continue
        approved_repair_ids.append(event_id)
        if display_targets_repaired:
            event["identity_target"] = exact_target
            event["latest_revision_reason"] = (
                str(event["latest_revision_reason"])
                + f" [human-approval:{chain_sha}]"
                + " [display-target-repair:"
                + editorial.LEGACY_APPROVAL_CORRECTION_SHA256
                + "]"
            )
    assert len(approved_repair_ids) == 6
    monkeypatch.setattr(
        editorial,
        "LEGACY_DISPLAY_TARGET_REPAIR_EVENT_IDS",
        tuple(approved_repair_ids),
    )
    common = {
        "client": client,
        "candidate": candidate,
        "source_human_review": human_review,
        "source_receipt": receipt,
        "source_replay_receipt": replay,
        "revision": revision,
        "candidate_artifact": TEST_CANDIDATE_ARTIFACT,
        "publication_artifact": TEST_PUBLICATION_ARTIFACT,
        "intent_artifact": {
            "run_id": 1001,
            "artifact_id": 2001,
            "artifact_name": (
                "global-alpha-expedited-editorial-carry-intent-"
                + revision
                + "-1001-1"
            ),
            "artifact_digest": "sha256:" + "9" * 64,
        },
        "now": datetime.fromisoformat(
            CARRY_NOW.replace("Z", "+00:00")
        ).astimezone(timezone.utc),
    }
    return client, common


def _install_rehashed_correction(
    monkeypatch: pytest.MonkeyPatch,
    correction: dict[str, object],
) -> None:
    monkeypatch.setattr(
        editorial,
        "LEGACY_APPROVAL_CORRECTION_SHA256",
        canonical_sha256(correction),
    )
    monkeypatch.setattr(
        editorial,
        "LEGACY_APPROVAL_CHAIN_SHA256",
        canonical_sha256(editorial._legacy_human_approval_chain_basis()),
    )
    monkeypatch.setattr(
        editorial,
        "_load_legacy_human_approval_correction",
        lambda: copy.deepcopy(correction),
    )


def _prepare_carry_from_common(
    client: _CarryClient,
    common: Mapping[str, object],
) -> dict[str, object]:
    return prepare_carry_forward_publication(
        client,
        candidate=common["candidate"],  # type: ignore[arg-type]
        source_human_review=common["source_human_review"],  # type: ignore[arg-type]
        source_receipt=common["source_receipt"],  # type: ignore[arg-type]
        source_replay_receipt=common[  # type: ignore[arg-type]
            "source_replay_receipt"
        ],
        revision=str(common["revision"]),
        candidate_artifact=common["candidate_artifact"],  # type: ignore[arg-type]
        publication_artifact=common[  # type: ignore[arg-type]
            "publication_artifact"
        ],
        now=common["now"],  # type: ignore[arg-type]
    )


def _repair_targets_from_common(
    client: _CarryClient,
    common: Mapping[str, object],
) -> dict[str, object]:
    return repair_legacy_display_targets(
        client,  # type: ignore[arg-type]
        candidate=common["candidate"],  # type: ignore[arg-type]
        source_human_review=common["source_human_review"],  # type: ignore[arg-type]
        source_receipt=common["source_receipt"],  # type: ignore[arg-type]
        source_replay_receipt=common[  # type: ignore[arg-type]
            "source_replay_receipt"
        ],
        revision=str(common["revision"]),
        candidate_artifact=common["candidate_artifact"],  # type: ignore[arg-type]
        publication_artifact=common[  # type: ignore[arg-type]
            "publication_artifact"
        ],
        now=common["now"],  # type: ignore[arg-type]
    )


def _reconstruct_carry_from_common(
    common: Mapping[str, object],
) -> dict[str, object]:
    return editorial._reconstruct_legacy_carry_forward_basis(
        candidate=common["candidate"],  # type: ignore[arg-type]
        source_human_review=common["source_human_review"],  # type: ignore[arg-type]
        source_receipt=common["source_receipt"],  # type: ignore[arg-type]
        source_replay_receipt=common[  # type: ignore[arg-type]
            "source_replay_receipt"
        ],
        candidate_artifact=common["candidate_artifact"],  # type: ignore[arg-type]
        publication_artifact=common[  # type: ignore[arg-type]
            "publication_artifact"
        ],
        now=common["now"],  # type: ignore[arg-type]
    )


def _publish_prepared_carry_from_common(
    client: _CarryClient,
    common: Mapping[str, object],
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    intent = _prepare_carry_from_common(client, common)
    return publish_carry_forward_intent(
        client,
        intent=intent,
        revision=str(common["revision"]),
        intent_artifact=common["intent_artifact"],  # type: ignore[arg-type]
        now=common["now"],  # type: ignore[arg-type]
    )


def test_code_only_sha_carry_forward_preserves_human_review_without_event_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(monkeypatch)
    before = copy.deepcopy(client.events)
    human, receipt, replay = _publish_prepared_carry_from_common(
        client,
        common,
    )
    assert client.events == before
    assert client.brief_calls == 2
    assert client.event_calls == EVENT_COUNT * 2
    assert human["code_revision"] == "c" * 40
    assert human["event_reviews"][0]["reviewer_reference"] == (  # type: ignore[index]
        LEGACY_APPROVAL_REVIEWER
    )
    assert human["ai_generated_ground_truth"] is False
    assert human["carry_forward"]["source_code_revision"] == REVISION  # type: ignore[index]
    assert human["carry_forward"]["event_mutations_applied"] == 0  # type: ignore[index]
    approved_basis = human["carry_forward"][  # type: ignore[index]
        "approved_canonical_basis"
    ]
    approved_digest = human["carry_forward"][  # type: ignore[index]
        "approved_canonical_basis_sha256"
    ]
    assert approved_basis["kind"] == APPROVED_CANONICAL_BASIS_KIND
    assert approved_basis["reviewer_reference"] == LEGACY_APPROVAL_REVIEWER
    assert approved_digest == canonical_sha256(approved_basis)
    assert (
        _validate_approved_canonical_basis(
            approved_basis,
            approved_digest,
        )
        == approved_basis
    )
    approved_events = approved_basis["events"]
    candidate_events = common["candidate"]["basis"]["events"]  # type: ignore[index]
    assert len(approved_events) == EVENT_COUNT
    for position, (approved, candidate) in enumerate(
        zip(approved_events, candidate_events, strict=True),
        start=1,
    ):
        expected_action = LEGACY_APPROVAL_ACTION_OVERRIDES.get(
            position,
            str(candidate["identity_action"]),
        )
        assert approved["identity_action"] == expected_action.casefold()
        assert approved["event_family"] == (
            "listing_status"
            if position == 11
            else candidate["event_family"]
        )
        assert approved["identity_target"] == (
            str(candidate["issuer_name"])
            + " — "
            + str(candidate["title"])
        )
        assert approved["summary"] == (
            str(candidate["issuer_name"])
            + "는 DART에 「"
            + str(candidate["title"])
            + "」을 공시했다."
        )
        assert approved["current_status"] == (
            "corrected_official_disclosure"
            if candidate["verification_status"] == "corrected"
            else "official_disclosure_confirmed"
        )
        assert approved["deadline_at"] is None
        assert approved["identity_deadline_at"] is None
        assert approved["actor"]["country_code"] == "KR"
    assert approved_basis["human_approval_chain_sha256"] == (
        editorial.LEGACY_APPROVAL_CHAIN_SHA256
    )
    assert human["carry_forward"]["human_approval_chain_sha256"] == (  # type: ignore[index]
        editorial.LEGACY_APPROVAL_CHAIN_SHA256
    )
    assert receipt["carry_forward"]["human_approval_chain_sha256"] == (  # type: ignore[index]
        editorial.LEGACY_APPROVAL_CHAIN_SHA256
    )
    assert receipt["event_review_outcomes"][0]["result"] == (  # type: ignore[index]
        "verified_unchanged"
    )
    assert receipt["event_review_outcomes"][0][  # type: ignore[index]
        "source_issuer_name"
    ] == receipt["event_review_outcomes"][0]["current_issuer_name"]  # type: ignore[index]
    assert receipt["event_review_outcomes"][0][  # type: ignore[index]
        "issuer_name_drift"
    ] is False
    assert receipt["mutations_applied"] == 0
    assert receipt["event_mutations_applied"] == 0
    assert receipt["idempotent_replay"] is False
    assert receipt["recovered_existing_brief"] is False
    assert receipt["brief"]["idempotent"] is False  # type: ignore[index]
    assert receipt["brief"]["build_sha"] == "c" * 40  # type: ignore[index]
    assert receipt["brief"]["brief_id"] == _brief_id(  # type: ignore[index]
        CARRY_NOW
    )
    assert replay["mutations_applied"] == 0
    assert replay["idempotent_replay"] is True
    assert replay["recovered_existing_brief"] is False
    assert receipt["semantic_receipt_sha256"] == replay[
        "semantic_receipt_sha256"
    ]
    assert receipt["same_event_pair_reviews"] == common[
        "source_human_review"
    ]["same_event_pair_reviews"]  # type: ignore[index]
    assert receipt["top5"] == common["source_receipt"]["top5"]  # type: ignore[index]


def test_fresh_carry_forward_preserves_exact_19_1_40_5_without_event_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _fresh_carry_common(monkeypatch)
    before = copy.deepcopy(client.events)
    intent = _prepare_carry_from_common(client, common)
    human, receipt, replay = publish_carry_forward_intent(
        client,
        intent=intent,
        revision=str(common["revision"]),
        intent_artifact=common["intent_artifact"],  # type: ignore[arg-type]
        now=common["now"],  # type: ignore[arg-type]
    )
    event_reviews = human["event_reviews"]
    pairs = human["same_event_pair_reviews"]
    top5 = human["top5_reviews"]
    outcomes = receipt["event_review_outcomes"]
    assert isinstance(event_reviews, list)
    assert isinstance(pairs, list)
    assert isinstance(top5, list)
    assert isinstance(outcomes, list)
    assert [item["decision"] for item in event_reviews].count("approved") == 19
    assert [item["decision"] for item in event_reviews].count("rejected") == 1
    assert event_reviews[14]["decision"] == "rejected"
    assert all(item["decision"] is False for item in pairs)
    assert len(top5) == TOP5_COUNT
    assert all(item["decision"] == "approved" for item in top5)
    assert outcomes[14]["decision"] == "rejected"
    assert outcomes[14]["final_review_status"] == "rejected"
    assert outcomes[14]["final_publication_status"] == "draft"
    assert outcomes[14]["final_identity_status"] == "rejected"
    assert outcomes[14]["result"] == "verified_unchanged"
    rejected_id = event_reviews[14]["event_id"]
    assert rejected_id not in {item["event_id"] for item in receipt["top5"]}
    assert client.events == before
    assert client.review_calls == 0
    assert client.event_calls == EVENT_COUNT * 2
    assert receipt["event_mutations_applied"] == 0
    assert replay["event_mutations_applied"] == 0
    assert replay["idempotent_replay"] is True
    assert receipt["semantic_receipt_sha256"] == replay[
        "semantic_receipt_sha256"
    ]
    assert intent["carry_forward"]["profile_id"] == (  # type: ignore[index]
        editorial.FRESH_APPROVAL_PROFILE_ID
    )


@pytest.mark.parametrize("drift", ["state", "evidence"])
def test_fresh_rejected_e15_current_basis_is_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    client, common = _fresh_carry_common(monkeypatch)
    event_id = editorial.FRESH_APPROVAL_EVENT_DECISIONS[14][0]
    event = client.events[event_id]
    assert isinstance(event, dict)
    if drift == "state":
        event["publication_status"] = "published"
    else:
        documents = event["official_documents"]
        assert isinstance(documents, list)
        document = documents[0]
        assert isinstance(document, dict)
        document["content_hash"] = "f" * 64
    with pytest.raises(
        ExpeditedEditorialError,
        match=(
            "rejected (state|official evidence) drift|"
            "event.data.event: evidence digest mismatch"
        ),
    ):
        _prepare_carry_from_common(client, common)
    assert client.brief_calls == 0
    assert client.review_calls == 0


def test_fresh_rejected_e15_accepts_shared_actor_kr_country_enrichment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _fresh_carry_common(monkeypatch)
    event_id = editorial.FRESH_APPROVAL_EVENT_DECISIONS[14][0]
    candidate_events = common["candidate"]["basis"]["events"]  # type: ignore[index]
    candidate_event = next(
        item for item in candidate_events if item["event_id"] == event_id
    )
    candidate_actor = candidate_event["actors"][0]
    assert candidate_actor["country_code"] is None

    # An approved event for the same global actor can enrich the shared actor
    # record even though this rejected event remains otherwise unchanged.
    current_event = client.events[event_id]
    assert isinstance(current_event, dict)
    current_actors = current_event["actors"]
    assert isinstance(current_actors, list) and len(current_actors) == 1
    current_actor = current_actors[0]
    assert isinstance(current_actor, dict)
    current_actor["country_code"] = "KR"

    intent = _prepare_carry_from_common(client, common)
    _, receipt, replay = publish_carry_forward_intent(
        client,
        intent=intent,
        revision=str(common["revision"]),
        intent_artifact=common["intent_artifact"],  # type: ignore[arg-type]
        now=common["now"],  # type: ignore[arg-type]
    )
    outcomes = receipt["event_review_outcomes"]
    assert isinstance(outcomes, list)
    assert outcomes[14]["decision"] == "rejected"
    assert outcomes[14]["result"] == "verified_unchanged"
    assert receipt["event_mutations_applied"] == 0
    assert replay["event_mutations_applied"] == 0
    assert replay["idempotent_replay"] is True
    assert client.review_calls == 0


@pytest.mark.parametrize(
    ("drift", "mutated"),
    [
        ("country_code", "US"),
        ("actor_id", "actor:other"),
        ("display_name", "Other filer"),
        ("actor_type", "company"),
        ("actor_role", "target"),
        ("missing_country_code", None),
        ("count", None),
    ],
)
def test_fresh_rejected_e15_rejects_non_kr_actor_enrichment_and_other_drift(
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
    mutated: object,
) -> None:
    client, common = _fresh_carry_common(monkeypatch)
    event_id = editorial.FRESH_APPROVAL_EVENT_DECISIONS[14][0]
    event = client.events[event_id]
    assert isinstance(event, dict)
    actors = event["actors"]
    assert isinstance(actors, list) and len(actors) == 1
    actor = actors[0]
    assert isinstance(actor, dict)
    if drift == "count":
        actors.append(copy.deepcopy(actor))
    elif drift == "missing_country_code":
        actor.pop("country_code")
    else:
        actor[drift] = mutated
    with pytest.raises(
        ExpeditedEditorialError,
        match="fresh carry-forward: rejected actor drift",
    ):
        _prepare_carry_from_common(client, common)
    assert client.brief_calls == 0
    assert client.review_calls == 0


def test_fresh_rejected_actor_basis_rejects_order_drift() -> None:
    first = {
        "actor_id": "actor:first",
        "display_name": "First filer",
        "actor_type": "company",
        "actor_role": "filer",
        "country_code": None,
    }
    second = {
        "actor_id": "actor:second",
        "display_name": "Second filer",
        "actor_type": "company",
        "actor_role": "filer",
        "country_code": None,
    }
    assert editorial._fresh_rejected_actor_basis_matches(
        [first, second],
        [first, second],
    )
    assert not editorial._fresh_rejected_actor_basis_matches(
        [second, first],
        [first, second],
    )


def test_fresh_profile_rejects_mixed_source_artifact_tuple(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _fresh_carry_common(monkeypatch)
    common["publication_artifact"] = editorial.LEGACY_APPROVAL_PUBLICATION_ARTIFACT
    with pytest.raises(
        ExpeditedEditorialError,
        match="source artifact profile is not approved",
    ):
        _prepare_carry_from_common(client, common)
    assert client.brief_calls == 0
    assert client.review_calls == 0


def test_pinned_legacy_source_age_extension_is_narrow_and_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, common = _carry_common(monkeypatch)
    reviewed_at = datetime.fromisoformat(NOW.replace("Z", "+00:00"))

    common["now"] = reviewed_at + timedelta(hours=80)
    reconstructed = _reconstruct_carry_from_common(common)
    assert reconstructed["candidate_artifact"] == TEST_CANDIDATE_ARTIFACT

    common["now"] = reviewed_at + timedelta(hours=169)
    with pytest.raises(ExpeditedEditorialError, match="stale or future"):
        _reconstruct_carry_from_common(common)

    common["now"] = editorial.LEGACY_CARRY_FORWARD_SOURCE_DEADLINE + timedelta(
        seconds=1
    )
    with pytest.raises(ExpeditedEditorialError, match="stale or future"):
        _reconstruct_carry_from_common(common)

    mismatched = dict(TEST_CANDIDATE_ARTIFACT)
    mismatched["artifact_id"] = 999
    assert editorial._legacy_carry_forward_source_max_age(
        now=reviewed_at + timedelta(hours=80),
        candidate_artifact=mismatched,
        publication_artifact=TEST_PUBLICATION_ARTIFACT,
    ) == timedelta(hours=72)

    mismatched_publication = dict(TEST_PUBLICATION_ARTIFACT)
    mismatched_publication["artifact_id"] = 999
    assert editorial._legacy_carry_forward_source_max_age(
        now=reviewed_at + timedelta(hours=80),
        candidate_artifact=TEST_CANDIDATE_ARTIFACT,
        publication_artifact=mismatched_publication,
    ) == timedelta(hours=72)
    assert editorial._legacy_carry_forward_source_max_age(
        now=editorial.LEGACY_CARRY_FORWARD_SOURCE_DEADLINE,
        candidate_artifact=TEST_CANDIDATE_ARTIFACT,
        publication_artifact=TEST_PUBLICATION_ARTIFACT,
    ) == timedelta(hours=168)
    with pytest.raises(ExpeditedEditorialError, match="timezone-aware"):
        editorial._legacy_carry_forward_source_max_age(
            now=reviewed_at.replace(tzinfo=None),
            candidate_artifact=TEST_CANDIDATE_ARTIFACT,
            publication_artifact=TEST_PUBLICATION_ARTIFACT,
        )


def test_pinned_source_review_and_receipt_age_limits_are_independent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, common = _carry_common(monkeypatch)
    candidate = common["candidate"]
    assert isinstance(candidate, dict)
    source_human_review = common["source_human_review"]
    assert isinstance(source_human_review, dict)
    source_receipt = common["source_receipt"]
    assert isinstance(source_receipt, dict)
    source_replay = common["source_replay_receipt"]
    assert isinstance(source_replay, dict)
    source_revision, events, pairs, top5 = (
        editorial._validate_carry_source_candidate(candidate)
    )
    reviewed_at = datetime.fromisoformat(NOW.replace("Z", "+00:00"))
    source_max_age = timedelta(hours=168)

    event_reviews, pair_reviews, top_reviews = (
        editorial._validate_carry_source_human_review(
            source_human_review,
            candidate=candidate,
            source_revision=source_revision,
            events=events,
            pairs=pairs,
            top5=top5,
            candidate_artifact=common["candidate_artifact"],
            now=reviewed_at + timedelta(hours=80),
            source_max_age=source_max_age,
        )
    )
    with pytest.raises(ExpeditedEditorialError, match="stale or future receipt"):
        editorial._validate_carry_source_receipts(
            source_receipt,
            source_replay,
            candidate=candidate,
            source_revision=source_revision,
            candidate_artifact=common["candidate_artifact"],
            event_reviews=event_reviews,
            pair_reviews=pair_reviews,
            top_reviews=top_reviews,
            now=reviewed_at + timedelta(hours=169),
            source_max_age=source_max_age,
        )
    with pytest.raises(ExpeditedEditorialError, match="stale or future"):
        editorial._validate_carry_source_human_review(
            source_human_review,
            candidate=candidate,
            source_revision=source_revision,
            events=events,
            pairs=pairs,
            top5=top5,
            candidate_artifact=common["candidate_artifact"],
            now=reviewed_at + timedelta(hours=73),
            source_max_age=timedelta(hours=72),
        )


def test_six_human_approved_display_targets_repair_once_then_replay_idempotently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(
        monkeypatch,
        display_targets_repaired=False,
    )
    stable_before = {
        event_id: {
            field: copy.deepcopy(event[field])
            for field in (
                "updated_at",
                "event_evidence_sha256",
                "comparison_key",
                "official_documents",
                "actors",
            )
        }
        for event_id, event in client.events.items()
        if event_id in editorial.LEGACY_DISPLAY_TARGET_REPAIR_EVENT_IDS
    }
    first = _repair_targets_from_common(client, common)
    replay = _repair_targets_from_common(client, common)
    assert first["expected_event_ids"] == list(
        editorial.LEGACY_DISPLAY_TARGET_REPAIR_EVENT_IDS
    )
    assert first["applied_event_ids"] == first["expected_event_ids"]
    assert first["mutations_applied"] == 6
    assert first["idempotent_replay"] is False
    assert replay["applied_event_ids"] == []
    assert replay["mutations_applied"] == 0
    assert replay["idempotent_replay"] is True
    assert client.review_calls == 6
    for event_id, before in stable_before.items():
        event = client.events[event_id]
        assert isinstance(event, dict)
        for field, value in before.items():
            assert event[field] == value

    intent = _prepare_carry_from_common(client, common)
    validated_first, validated_replay = (
        validate_display_target_repair_receipts(
            first,
            replay,
            revision=str(common["revision"]),
            approved_canonical_basis_sha256=intent["carry_forward"][  # type: ignore[index]
                "approved_canonical_basis_sha256"
            ],
        )
    )
    assert validated_first == first
    assert validated_replay == replay


def test_display_target_repair_fails_before_write_on_non_target_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(
        monkeypatch,
        display_targets_repaired=False,
    )
    event_id = editorial.LEGACY_DISPLAY_TARGET_REPAIR_EVENT_IDS[-1]
    event = client.events[event_id]
    assert isinstance(event, dict)
    event["summary"] = "unapproved drift"
    with pytest.raises(ExpeditedEditorialError, match="summary drift"):
        _repair_targets_from_common(client, common)
    assert client.review_calls == 0


def test_display_target_repair_receipts_reject_replay_state_tamper(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(
        monkeypatch,
        display_targets_repaired=False,
    )
    first = _repair_targets_from_common(client, common)
    replay = _repair_targets_from_common(client, common)
    intent = _prepare_carry_from_common(client, common)
    tampered = copy.deepcopy(replay)
    tampered["event_results"][0]["before_state_sha256"] = "f" * 64  # type: ignore[index]
    tampered["receipt_sha256"] = canonical_sha256(  # type: ignore[index]
        {
            key: value
            for key, value in tampered.items()
            if key != "receipt_sha256"
        }
    )
    with pytest.raises(
        ExpeditedEditorialError,
        match="idempotent state mismatch",
    ):
        validate_display_target_repair_receipts(
            first,
            tampered,
            revision=str(common["revision"]),
            approved_canonical_basis_sha256=intent["carry_forward"][  # type: ignore[index]
                "approved_canonical_basis_sha256"
            ],
        )


def test_carry_forward_audits_issuer_master_name_drift_without_event_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(monkeypatch)
    event_id = next(iter(client.events))
    event = client.events[event_id]
    assert isinstance(event, dict)
    source_name = str(event["issuer_name"])
    current_name = "Renamed current issuer"
    event["issuer_name"] = current_name
    before = copy.deepcopy(client.events)

    intent = _prepare_carry_from_common(client, common)
    outcomes = intent["verified_outcomes"]
    assert isinstance(outcomes, list)
    outcome = next(
        item for item in outcomes if item["event_id"] == event_id
    )
    assert outcome["source_issuer_name"] == source_name
    assert outcome["current_issuer_name"] == current_name
    assert outcome["issuer_name_drift"] is True
    assert intent["carry_forward"]["event_mutations_applied"] == 0  # type: ignore[index]
    assert client.events == before

    human, receipt, replay = publish_carry_forward_intent(
        client,
        intent=intent,
        revision=str(common["revision"]),
        intent_artifact=common["intent_artifact"],  # type: ignore[arg-type]
        now=common["now"],  # type: ignore[arg-type]
    )
    assert client.events == before
    assert human["carry_forward"]["event_mutations_applied"] == 0  # type: ignore[index]
    receipt_outcome = next(
        item
        for item in receipt["event_review_outcomes"]  # type: ignore[union-attr]
        if item["event_id"] == event_id
    )
    assert receipt_outcome["source_issuer_name"] == source_name
    assert receipt_outcome["current_issuer_name"] == current_name
    assert receipt_outcome["issuer_name_drift"] is True
    assert receipt["event_mutations_applied"] == 0
    assert replay["event_review_outcomes"] == receipt["event_review_outcomes"]

    brief = receipt["brief"]
    assert isinstance(brief, dict)
    semantic = {
        "candidate_artifact": receipt["candidate_artifact"],
        "candidate_sha256": receipt["candidate_sha256"],
        "decision_sha256": receipt["decision_sha256"],
        "code_revision": receipt["code_revision"],
        "prepared_intent_artifact": receipt["prepared_intent_artifact"],
        "prepared_intent_sha256": receipt["prepared_intent_sha256"],
        "carry_forward": receipt["carry_forward"],
        "event_review_outcomes": receipt["event_review_outcomes"],
        "event_reviews": intent["event_reviews"],
        "same_event_pair_reviews": receipt["same_event_pair_reviews"],
        "top5": [
            {
                "event_id": item["event_id"],
                "position_no": item["position_no"],
                "selection_reason": item["selection_reason"],
            }
            for item in receipt["top5"]  # type: ignore[union-attr]
        ],
        "brief": {
            "brief_id": brief["brief_id"],
            "build_sha": brief["build_sha"],
            "cutoff_at": brief["cutoff_at"],
            "payload_sha256": brief["payload_sha256"],
        },
    }
    assert receipt["semantic_receipt_sha256"] == canonical_sha256(semantic)


def test_carry_forward_rejects_inconsistent_issuer_name_drift_audit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(monkeypatch)
    intent = _prepare_carry_from_common(client, common)
    outcomes = intent["verified_outcomes"]
    assert isinstance(outcomes, list)
    outcomes[0]["issuer_name_drift"] = True
    intent["intent_sha256"] = canonical_sha256(
        {
            key: value
            for key, value in intent.items()
            if key != "intent_sha256"
        }
    )
    with pytest.raises(
        ExpeditedEditorialError,
        match="frozen review basis mismatch",
    ):
        publish_carry_forward_intent(
            client,
            intent=intent,
            revision=str(common["revision"]),
            intent_artifact=common["intent_artifact"],  # type: ignore[arg-type]
            now=common["now"],  # type: ignore[arg-type]
        )
    assert client.brief_calls == 0


def test_preuploaded_intent_is_write_free_and_uses_a_new_frozen_cutoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(monkeypatch)
    source_cutoff = common["source_receipt"]["collected_at"]  # type: ignore[index]
    intent = _prepare_carry_from_common(client, common)
    assert client.brief_calls == 0
    assert client.event_calls == EVENT_COUNT * 2
    assert intent["prepared_at"] == CARRY_NOW
    assert intent["prepared_at"] != source_cutoff
    assert intent["expected_brief_id"] == _brief_id(CARRY_NOW)
    brief = intent["brief_payload"]
    assert isinstance(brief, dict)
    assert brief["cutoff_at"] == CARRY_NOW
    assert brief["build_sha"] == common["revision"]
    assert len(brief["items"]) == TOP5_COUNT
    snapshot_basis = sorted(
        [
            {
                "event_id": item["event_id"],
                "snapshot_sha256": item["expected_snapshot_sha256"],
            }
            for item in brief["items"]
        ],
        key=lambda item: str(item["event_id"]),
    )
    assert brief["expected_event_basis_sha256"] == canonical_sha256(
        snapshot_basis
    )
    assert all(
        item["expected_snapshot_sha256"] == "e" * 64
        for item in brief["items"]
    )
    assert canonical_sha256(
        {
            key: value
            for key, value in intent.items()
            if key != "intent_sha256"
        }
    ) == intent["intent_sha256"]


@pytest.mark.parametrize("lost_response_call", [1, 2])
def test_preuploaded_intent_recovers_after_response_or_artifact_loss(
    monkeypatch: pytest.MonkeyPatch,
    lost_response_call: int,
) -> None:
    client, common = _carry_common(monkeypatch)
    intent = _prepare_carry_from_common(client, common)
    reads_after_prepare = client.event_calls
    client.fail_after_persist_call = lost_response_call
    with pytest.raises(
        ExpeditedEditorialError,
        match="simulated persisted brief response loss",
    ):
        publish_carry_forward_intent(
            client,
            intent=intent,
            revision=str(common["revision"]),
            intent_artifact=common["intent_artifact"],  # type: ignore[arg-type]
            now=common["now"],  # type: ignore[arg-type]
        )
    assert len(client.briefs) == 1

    # Simulate a failed final artifact upload followed by source-review expiry
    # and live event drift. Recovery is bound only to the pre-uploaded intent
    # and an exact existing brief; it must not re-read or rewrite live events.
    event = next(iter(client.events.values()))
    assert isinstance(event, dict)
    event["summary"] = "Later live-state drift"
    client.fail_after_persist_call = None
    recovered_at = common["now"] + timedelta(hours=73)  # type: ignore[operator]
    human, receipt, replay = publish_carry_forward_intent(
        client,
        intent=intent,
        revision=str(common["revision"]),
        intent_artifact=common["intent_artifact"],  # type: ignore[arg-type]
        now=recovered_at,
    )
    assert client.event_calls == reads_after_prepare
    assert receipt["publication_mode"] == "existing_only_recovery"
    assert receipt["recovered_existing_brief"] is True
    assert receipt["brief"]["idempotent"] is True  # type: ignore[index]
    assert replay["brief"]["idempotent"] is True  # type: ignore[index]
    assert human["carry_forward"]["prepared_intent_sha256"] == (  # type: ignore[index]
        intent["intent_sha256"]
    )


def test_stale_intent_cannot_create_a_missing_brief(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(monkeypatch)
    intent = _prepare_carry_from_common(client, common)
    missing = _CarryClient(client.events, revision=str(common["revision"]))
    recovered_at = common["now"] + timedelta(hours=73)  # type: ignore[operator]
    with pytest.raises(
        ExpeditedEditorialError,
        match="brief_recovery_not_found",
    ):
        publish_carry_forward_intent(
            missing,
            intent=intent,
            revision=str(common["revision"]),
            intent_artifact=common["intent_artifact"],  # type: ignore[arg-type]
            now=recovered_at,
        )
    assert missing.briefs == {}
    assert missing.event_calls == 0
    assert missing.brief_calls == 1


def test_current_sha_brief_does_not_reuse_the_persisted_source_brief_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(monkeypatch)
    source_receipt = common["source_receipt"]
    assert isinstance(source_receipt, dict)
    source_brief = source_receipt["brief"]
    assert isinstance(source_brief, dict)
    source_brief_id = str(source_brief["brief_id"])
    client.briefs[source_brief_id] = {
        "edition": "global",
        "cutoff_at": source_brief["cutoff_at"],
        "build_sha": REVISION,
        "empty_reason": None,
        "items": [],
    }
    intent = _prepare_carry_from_common(client, common)
    assert intent["expected_brief_id"] != source_brief_id
    _, receipt, replay = publish_carry_forward_intent(
        client,
        intent=intent,
        revision=str(common["revision"]),
        intent_artifact=common["intent_artifact"],  # type: ignore[arg-type]
        now=common["now"],  # type: ignore[arg-type]
    )
    assert len(client.briefs) == 2
    assert receipt["brief"]["idempotent"] is False  # type: ignore[index]
    assert replay["brief"]["idempotent"] is True  # type: ignore[index]


def test_carry_forward_rechecks_all_events_and_hash_fences_before_brief(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(monkeypatch)
    client.mutate_on_event_call = EVENT_COUNT + 1
    client.mutate_document_clock = True
    with pytest.raises(
        ExpeditedEditorialError,
        match="pre-publication fence",
    ):
        _publish_prepared_carry_from_common(client, common)
    assert client.event_calls == EVENT_COUNT * 2
    assert client.brief_calls == 0


def test_carry_forward_recovers_after_first_brief_was_persisted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(monkeypatch)
    intent = _prepare_carry_from_common(client, common)
    client.fail_brief_call = 2
    with pytest.raises(
        ExpeditedEditorialError,
        match="simulated brief call failure",
    ):
        publish_carry_forward_intent(
            client,
            intent=intent,
            revision=str(common["revision"]),
            intent_artifact=common["intent_artifact"],  # type: ignore[arg-type]
            now=common["now"],  # type: ignore[arg-type]
        )
    persisted_payload = copy.deepcopy(client.brief_payload)
    assert persisted_payload is not None

    client.fail_brief_call = None
    human, receipt, replay = publish_carry_forward_intent(
        client,
        intent=intent,
        revision=str(common["revision"]),
        intent_artifact=common["intent_artifact"],  # type: ignore[arg-type]
        now=common["now"],  # type: ignore[arg-type]
    )
    assert client.brief_payload == persisted_payload
    assert client.brief_calls == 4
    assert receipt["recovered_existing_brief"] is True
    assert receipt["brief"]["idempotent"] is True  # type: ignore[index]
    assert receipt["idempotent_replay"] is True
    assert replay["recovered_existing_brief"] is True
    assert replay["brief"]["idempotent"] is True  # type: ignore[index]
    assert replay["idempotent_replay"] is True
    assert human["carry_forward"]["event_mutations_applied"] == 0  # type: ignore[index]


def test_legacy_approval_profile_is_bound_to_the_recorded_candidate_and_reviewer() -> None:
    attestation = editorial._load_legacy_human_approval_artifact()
    assert canonical_sha256(attestation) == (
        editorial.LEGACY_APPROVAL_ATTESTATION_SHA256
    )
    assert "attestation_text" not in attestation
    assert attestation["attestation_text_sha256"] == (
        editorial.LEGACY_APPROVAL_TEXT_SHA256
    )
    assert attestation["approved_transform_contract"]["summary"] == (
        "candidate.issuer_name \u2014 candidate.title"
    )
    assert [
        ord(char)
        for char in attestation["approved_transform_contract"]["summary"]
        if not char.isascii()
    ] == [0x2014]
    assert attestation["source_decision_sha256"] == (
        editorial.LEGACY_APPROVAL_SOURCE_DECISION_SHA256
    )
    assert editorial.LEGACY_APPROVAL_CANDIDATE_SHA256 == (
        "c24627699633cf02084a2caeb3334c182c404861f85e8f4d27acf116fc6d8f76"
    )
    assert editorial.LEGACY_APPROVAL_REVIEWER == "bside-owner-20260731"
    assert editorial.LEGACY_APPROVAL_CANDIDATE_ARTIFACT == {
        "run_id": 30581161308,
        "artifact_id": 8774655231,
        "artifact_name": (
            "global-alpha-expedited-editorial-candidates-"
            "b44a1aebc2eb6e7b58e5960b0f8245b87e901052"
        ),
        "artifact_digest": (
            "sha256:"
            "f7eec4481564f52b89fbda166544cb1bc0b79e8ee940a8173ed5859aade40afd"
        ),
    }
    assert editorial.LEGACY_APPROVAL_PUBLICATION_ARTIFACT == {
        "run_id": 30587485449,
        "artifact_id": 8777083749,
        "artifact_name": (
            "global-alpha-expedited-editorial-publication-"
            "b44a1aebc2eb6e7b58e5960b0f8245b87e901052-30587485449-1"
        ),
        "artifact_digest": (
            "sha256:"
            "95028a16adedfc19b5dfe3c6e0b0c36696b5c2619a44f0040d51ef3b1ffcbbaa"
        ),
    }
    correction = editorial._load_legacy_human_approval_correction()
    assert hashlib.sha256(
        editorial.LEGACY_APPROVAL_CORRECTION_PATH.read_bytes()
    ).hexdigest() == editorial.LEGACY_APPROVAL_CORRECTION_RAW_SHA256
    assert canonical_sha256(correction) == (
        editorial.LEGACY_APPROVAL_CORRECTION_SHA256
    )
    assert correction["ground_truth_source"] == "human"
    assert correction["ai_generated_ground_truth"] is False
    assert correction["human_attestation"] is True
    assert correction["reviewer_type"] == "human"
    assert correction["reviewer_reference"] == LEGACY_APPROVAL_REVIEWER
    correction_attestation_text = (
        "기존 E01~E20, P01~P40, T01~T05, 당사자·국가·공식 근거 판단은 "
        "변경하지 않습니다. 대상=회사명 — 원문 제목, 요약=회사명는 DART에 "
        "「원문 제목」을 공시했다., 상태=정정 공시는 "
        "corrected_official_disclosure, 그 외는 "
        "official_disclosure_confirmed 규칙만 별도 정정 증빙으로 추가하는 것을 "
        "승인합니다. 이는 사람이 직접 판단한 것이며 AI 생성 정답이 아닙니다."
    )
    assert hashlib.sha256(
        correction_attestation_text.encode("utf-8")
    ).hexdigest() == correction["attestation_text_sha256"]
    assert correction["base_approval_canonical_sha256"] == (
        editorial.LEGACY_APPROVAL_ATTESTATION_SHA256
    )
    assert correction["source_candidate_sha256"] == (
        editorial.LEGACY_APPROVAL_CANDIDATE_SHA256
    )
    assert correction["source_decision_sha256"] == (
        editorial.LEGACY_APPROVAL_SOURCE_DECISION_SHA256
    )
    assert correction["source_publication_artifact"] == (
        editorial.LEGACY_APPROVAL_PUBLICATION_ARTIFACT
    )
    assert len(correction["event_ids"]) == EVENT_COUNT
    assert canonical_sha256(correction["event_ids"]) == (
        editorial.LEGACY_APPROVAL_EVENT_IDS_SHA256
    )
    assert correction["corrected_transform_contract"] == {
        "identity_target": "회사명 — 원문 제목",
        "summary": "회사명는 DART에 「원문 제목」을 공시했다.",
        "current_status": {
            "corrected": "corrected_official_disclosure",
            "official": "official_disclosure_confirmed",
        },
    }
    assert editorial._legacy_human_approval_chain_sha256() == (
        editorial.LEGACY_APPROVAL_CHAIN_SHA256
    )


def test_corrected_current_status_is_exact_and_unknown_values_fail_closed() -> None:
    assert editorial._legacy_approved_current_status("corrected") == (
        "corrected_official_disclosure"
    )
    assert editorial._legacy_approved_current_status("official") == (
        "official_disclosure_confirmed"
    )
    for value in ("withdrawn", "signal", "", "OFFICIAL"):
        with pytest.raises(
            ExpeditedEditorialError,
            match="unsupported verification status",
        ):
            editorial._legacy_approved_current_status(value)


def test_corrected_event_delta_is_exact_and_fail_closed() -> None:
    base: dict[str, object] = {
        "issuer_id": "issuer:test",
        "event_family": "capital_issuance",
        "identity_action": "rights_issue",
        "identity_target": "issuer",
        "identity_actor_id": "actor:test",
        "identity_effective_at": "2026-07-30T00:00:00Z",
        "identity_deadline_at": None,
        "summary": "issuer — title",
        "current_status": "official",
        "actor": {"actor_id": "actor:test"},
        "country": "KR",
        "official_documents": [{"document_id": "dart:test"}],
        "official_evidence_count": 1,
        "source_event_evidence_sha256": "a" * 64,
    }
    base["comparison_key"] = editorial._legacy_event_comparison_key(base, "base")
    corrected = copy.deepcopy(base)
    corrected.update(
        identity_target="issuer — title",
        summary="issuer는 DART에 「title」을 공시했다.",
        current_status="official_disclosure_confirmed",
    )
    corrected["comparison_key"] = editorial._legacy_event_comparison_key(
        corrected, "corrected"
    )
    editorial._validate_legacy_corrected_event_delta(base, corrected, "test")
    for field, value in {
        "actor": {"actor_id": "actor:changed"},
        "country": "US",
        "official_documents": [{"document_id": "dart:changed"}],
        "official_evidence_count": 2,
        "source_event_evidence_sha256": "b" * 64,
    }.items():
        tampered = copy.deepcopy(corrected)
        tampered[field] = value
        with pytest.raises(ExpeditedEditorialError, match="correction"):
            editorial._validate_legacy_corrected_event_delta(
                base, tampered, "test"
            )


def test_correction_loader_rejects_byte_only_file_drift(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = editorial.LEGACY_APPROVAL_CORRECTION_PATH
    altered = tmp_path / "correction.json"
    altered.write_bytes(path.read_bytes() + b"\n")
    monkeypatch.setattr(editorial, "LEGACY_APPROVAL_CORRECTION_PATH", altered)
    with pytest.raises(ExpeditedEditorialError, match="raw file digest mismatch"):
        editorial._load_legacy_human_approval_correction()


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value.__setitem__("ai_generated_ground_truth", True),
         "provenance mismatch"),
        (lambda value: value.__setitem__("reviewer_type", "ai"),
         "provenance mismatch"),
        (lambda value: value.__setitem__("unexpected", "field"),
         "provenance mismatch"),
        (
            lambda value: value.__setitem__(
                "reviewed_at", "2026-07-01T00:00:00Z"
            ),
            "correction must follow base approval",
        ),
        (lambda value: value["event_ids"].reverse(), "event binding mismatch"),
        (
            lambda value: value["corrected_transform_contract"].__setitem__(
                "summary", "tampered"
            ),
            "provenance mismatch",
        ),
    ],
)
def test_correction_rejects_ai_extra_field_reorder_and_transform_tampering(
    monkeypatch: pytest.MonkeyPatch,
    mutation: object,
    message: str,
) -> None:
    client, common = _carry_common(monkeypatch)
    correction = editorial._load_legacy_human_approval_correction()
    assert isinstance(correction, dict)
    assert callable(mutation)
    mutation(correction)
    _install_rehashed_correction(monkeypatch, correction)
    with pytest.raises(ExpeditedEditorialError, match=message):
        _prepare_carry_from_common(client, common)
    assert client.brief_calls == 0


def test_global_comparison_key_matches_the_php_v2_identity_contract() -> None:
    assert _global_comparison_key(
        issuer_id="issuer:kr:dart:001",
        event_family="capital_issuance",
        action="Rights Issue",
        target="  Test   Issuer ",
        actor_id="ACTOR:ABC",
        effective_at="2026-07-30T09:00:00+09:00",
        deadline_at=None,
        location="comparison key regression",
    ) == (
        "global:"
        "5b0fcd2e0cd512f179b5ee5b1f1b29c05b3fa8e1213e69e44c266f0322d00f00"
    )


@pytest.mark.parametrize(
    ("field", "mutated"),
    [
        ("issuer_id", "issuer:kr:mutated"),
        ("country", "US"),
        ("title", "Mutated original title"),
        ("original_language", "en"),
        ("event_family", "listing_status"),
        ("summary", "Mutated summary"),
        ("importance", "low"),
        ("current_status", "mutated"),
        ("deadline_at", "2026-07-31T00:00:00Z"),
        ("verification_status", "corrected"),
        ("change_type", "corrected"),
        ("review_status", "pending"),
        ("publication_status", "draft"),
        ("identity_action", "mutated_action"),
        ("identity_target", "Mutated target"),
        ("identity_actor_id", "actor:mutated"),
        ("identity_effective_at", "2026-07-31T00:00:00Z"),
        ("identity_deadline_at", "2026-07-31T00:00:00Z"),
        ("identity_status", "needs_review"),
        ("comparison_key", "global:" + "f" * 64),
        ("occurred_at", "2026-07-31T00:00:00Z"),
        ("first_observed_at", "2026-07-31T00:00:00Z"),
    ],
)
def test_carry_forward_rejects_every_canonical_event_field_drift_before_brief(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    mutated: object,
) -> None:
    client, common = _carry_common(monkeypatch)
    event = next(iter(client.events.values()))
    assert isinstance(event, dict)
    event[field] = mutated
    with pytest.raises(ExpeditedEditorialError):
        _publish_prepared_carry_from_common(client, common)
    assert client.brief_calls == 0


@pytest.mark.parametrize(
    ("field", "mutated"),
    [
        ("actor_id", "actor:mutated"),
        ("display_name", "Mutated actor"),
        ("actor_type", "company"),
        ("actor_role", "target"),
        ("country_code", "US"),
        ("actor_review_status", "pending"),
        ("relation_review_status", "pending"),
        ("record_status", "inactive"),
    ],
)
def test_carry_forward_rejects_every_approved_actor_field_drift_before_brief(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    mutated: object,
) -> None:
    client, common = _carry_common(monkeypatch)
    event = next(iter(client.events.values()))
    assert isinstance(event, dict)
    actors = event["actors"]
    assert isinstance(actors, list)
    actor = actors[0]
    assert isinstance(actor, dict)
    actor[field] = mutated
    with pytest.raises(ExpeditedEditorialError):
        _publish_prepared_carry_from_common(client, common)
    assert client.brief_calls == 0


def test_carry_forward_rejects_evidence_count_and_digest_drift_before_brief(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(monkeypatch)
    event = next(iter(client.events.values()))
    assert isinstance(event, dict)
    event["event_evidence_sha256"] = "f" * 64
    with pytest.raises(ExpeditedEditorialError, match="evidence digest mismatch"):
        _publish_prepared_carry_from_common(client, common)
    assert client.brief_calls == 0

    client, common = _carry_common(monkeypatch)
    event = next(iter(client.events.values()))
    assert isinstance(event, dict)
    event["official_evidence_count"] = 2
    with pytest.raises(ExpeditedEditorialError, match="evidence count mismatch"):
        _publish_prepared_carry_from_common(client, common)
    assert client.brief_calls == 0


def test_approved_canonical_basis_digest_is_mandatory_and_tamper_evident(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, common = _carry_common(monkeypatch)
    candidate = common["candidate"]
    assert isinstance(candidate, dict)
    source_human_review = common["source_human_review"]
    assert isinstance(source_human_review, dict)
    source_receipt = common["source_receipt"]
    assert isinstance(source_receipt, dict)
    source_replay = common["source_replay_receipt"]
    assert isinstance(source_replay, dict)
    source_revision, events, pairs, top5 = (
        editorial._validate_carry_source_candidate(candidate)
    )
    source_max_age = editorial._legacy_carry_forward_source_max_age(
        now=common["now"],
        candidate_artifact=common["candidate_artifact"],
        publication_artifact=common["publication_artifact"],
    )
    event_reviews, pair_reviews, top_reviews = (
        editorial._validate_carry_source_human_review(
            source_human_review,
            candidate=candidate,
            source_revision=source_revision,
            events=events,
            pairs=pairs,
            top5=top5,
            candidate_artifact=common["candidate_artifact"],
            now=common["now"],
            source_max_age=source_max_age,
        )
    )
    source_outcomes, publication_top5 = (
        editorial._validate_carry_source_receipts(
            source_receipt,
            source_replay,
            candidate=candidate,
            source_revision=source_revision,
            candidate_artifact=common["candidate_artifact"],
            event_reviews=event_reviews,
            pair_reviews=pair_reviews,
            top_reviews=top_reviews,
            now=common["now"],
            source_max_age=source_max_age,
        )
    )
    basis = editorial._legacy_approved_canonical_basis(
        candidate=candidate,
        candidate_artifact=common["candidate_artifact"],
        publication_artifact=common["publication_artifact"],
        approval_attestation=(
            editorial._load_legacy_human_approval_artifact()
        ),
        approval_correction=(
            editorial._load_legacy_human_approval_correction()
        ),
        source_decision_sha256=source_receipt["decision_sha256"],
        events=events,
        event_reviews=event_reviews,
        pair_reviews=pair_reviews,
        top_reviews=top_reviews,
        source_outcomes=source_outcomes,
        publication_top5=publication_top5,
    )
    digest = canonical_sha256(basis)
    assert _validate_approved_canonical_basis(basis, digest) == basis
    tampered = copy.deepcopy(basis)
    tampered["events"][0]["summary"] = "tampered"  # type: ignore[index]
    with pytest.raises(ExpeditedEditorialError, match="digest mismatch"):
        _validate_approved_canonical_basis(tampered, digest)
    tampered_correction = copy.deepcopy(basis)
    tampered_correction["human_approval_correction_artifact"][  # type: ignore[index]
        "reviewer_type"
    ] = "ai"
    with pytest.raises(ExpeditedEditorialError, match="profile mismatch"):
        _validate_approved_canonical_basis(
            tampered_correction,
            canonical_sha256(tampered_correction),
        )
    tampered_chain = copy.deepcopy(basis)
    tampered_chain["human_approval_chain_sha256"] = "f" * 64
    with pytest.raises(ExpeditedEditorialError, match="profile mismatch"):
        _validate_approved_canonical_basis(
            tampered_chain,
            canonical_sha256(tampered_chain),
        )
    with pytest.raises(ExpeditedEditorialError):
        _validate_approved_canonical_basis(basis, None)
    wrong_artifact = copy.deepcopy(common["candidate_artifact"])
    assert isinstance(wrong_artifact, dict)
    wrong_artifact["run_id"] = 999
    with pytest.raises(
        ExpeditedEditorialError,
        match="exact protected artifacts",
    ):
        editorial._legacy_approved_canonical_basis(
            candidate=candidate,
            candidate_artifact=wrong_artifact,
            publication_artifact=common["publication_artifact"],
            approval_attestation=(
                editorial._load_legacy_human_approval_artifact()
            ),
            approval_correction=(
                editorial._load_legacy_human_approval_correction()
            ),
            source_decision_sha256=source_receipt["decision_sha256"],
            events=events,
            event_reviews=event_reviews,
            pair_reviews=pair_reviews,
            top_reviews=top_reviews,
            source_outcomes=source_outcomes,
            publication_top5=publication_top5,
        )


def test_carry_forward_fails_closed_on_current_official_content_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(monkeypatch)
    event = next(iter(client.events.values()))
    assert isinstance(event, dict)
    documents = event["official_documents"]
    assert isinstance(documents, list)
    document = documents[0]
    assert isinstance(document, dict)
    document["content_hash"] = "f" * 64
    event["event_evidence_sha256"] = canonical_sha256(
        {
            "event_id": event["event_id"],
            "event_updated_at": event["updated_at"],
            "official_documents": documents,
        }
    )
    with pytest.raises(ExpeditedEditorialError, match="official evidence drift"):
        _publish_prepared_carry_from_common(client, common)
    assert client.brief_calls == 0


def test_carry_forward_fails_closed_on_event_timestamp_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(monkeypatch)
    event = next(iter(client.events.values()))
    assert isinstance(event, dict)
    event["updated_at"] = "2026-07-28T12:02:00Z"
    event["event_evidence_sha256"] = canonical_sha256(
        {
            "event_id": event["event_id"],
            "event_updated_at": event["updated_at"],
            "official_documents": event["official_documents"],
        }
    )
    with pytest.raises(ExpeditedEditorialError, match="editorial state drift"):
        _publish_prepared_carry_from_common(client, common)
    assert client.brief_calls == 0


def test_carry_forward_rejects_rewritten_pair_basis_even_with_rehashed_candidate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(monkeypatch)
    candidate = copy.deepcopy(common["candidate"])
    assert isinstance(candidate, dict)
    basis = candidate["basis"]
    assert isinstance(basis, dict)
    pairs = basis["same_event_pair_candidates"]
    assert isinstance(pairs, list)
    pair = pairs[0]
    assert isinstance(pair, dict)
    pair["pair_id"] = "pair:" + "f" * 40
    candidate["candidate_sha256"] = canonical_sha256(basis)
    common["candidate"] = candidate
    with pytest.raises(ExpeditedEditorialError, match="pair basis mismatch"):
        _publish_prepared_carry_from_common(client, common)
    assert client.brief_calls == 0


def test_carry_forward_cannot_use_another_carry_as_its_human_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(monkeypatch)
    human = copy.deepcopy(common["source_human_review"])
    assert isinstance(human, dict)
    human["evidence_source"] = "protected_editorial_carry_forward"
    human["carry_forward"] = {"event_mutations_applied": 0}
    common["source_human_review"] = human
    with pytest.raises(
        ExpeditedEditorialError,
        match="original protected publication required",
    ):
        _publish_prepared_carry_from_common(client, common)
    assert client.brief_calls == 0


def test_carry_forward_cannot_use_another_carry_receipt_as_its_source(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, common = _carry_common(monkeypatch)
    receipt = copy.deepcopy(common["source_receipt"])
    assert isinstance(receipt, dict)
    receipt["evidence_source"] = "protected_editorial_carry_forward"
    receipt["carry_forward"] = {"event_mutations_applied": 0}
    common["source_receipt"] = receipt
    with pytest.raises(ExpeditedEditorialError, match="provenance mismatch"):
        _publish_prepared_carry_from_common(client, common)
    assert client.brief_calls == 0


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
    first_event = client.events["event:000"]
    first_actors = first_event["actors"]
    assert isinstance(first_actors, list)
    assert first_actors[0]["country_code"] == "KR"


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


@pytest.mark.parametrize("country_code", [None, "", "ZZ", "kr", "KOR"])
def test_human_actor_country_remains_required_and_strict_before_mutation(
    country_code: object,
) -> None:
    candidate, template = _candidate_bundle()
    decisions = _human_decisions(candidate, template)
    reviews = decisions["event_reviews"]
    assert isinstance(reviews, list)
    payload = reviews[0]["review_payload"]
    assert isinstance(payload, dict)
    actor = payload["actor"]
    assert isinstance(actor, dict)
    actor["country_code"] = country_code
    client = _ApplyClient(candidate)
    with pytest.raises(ExpeditedEditorialError, match="invalid country_code"):
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


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("actor_id", "actor:arbitrary"),
        ("display_name", "Different Reporting Person"),
        ("actor_type", "company"),
        ("actor_role", "beneficial_owner"),
    ],
)
def test_human_actor_identity_must_exactly_bind_to_candidate_before_mutation(
    field: str,
    value: str,
) -> None:
    # The actor_id variant is the original exploit: before candidate binding,
    # this otherwise-valid arbitrary actor was accepted and written.
    candidate, template = _candidate_bundle()
    decisions = _human_decisions(candidate, template)
    reviews = decisions["event_reviews"]
    assert isinstance(reviews, list)
    payload = reviews[0]["review_payload"]
    assert isinstance(payload, dict)
    actor = payload["actor"]
    assert isinstance(actor, dict)
    actor[field] = value
    client = _ApplyClient(candidate)
    with pytest.raises(
        ExpeditedEditorialError,
        match="candidate actor binding mismatch",
    ):
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
    assert client.brief_payload is None
    assert all(
        event["review_status"] == "pending" for event in client.events.values()
    )


def test_human_actor_whitespace_is_not_normalized_into_a_candidate_match() -> None:
    candidate, template = _candidate_bundle()
    decisions = _human_decisions(candidate, template)
    reviews = decisions["event_reviews"]
    assert isinstance(reviews, list)
    payload = reviews[0]["review_payload"]
    assert isinstance(payload, dict)
    actor = payload["actor"]
    assert isinstance(actor, dict)
    actor["display_name"] = "Actor 0 "
    client = _ApplyClient(candidate)
    with pytest.raises(
        ExpeditedEditorialError,
        match="exact actor field values required",
    ):
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
    assert client.brief_payload is None
    assert all(
        event["review_status"] == "pending" for event in client.events.values()
    )


@pytest.mark.parametrize(
    ("candidate_actors", "message"),
    [
        ([], "candidate actor required"),
        (
            [
                {
                    "actor_id": "actor:000",
                    "display_name": "Actor 0",
                    "actor_type": "institution",
                    "actor_role": "filer",
                    "country_code": None,
                },
                {
                    "actor_id": "actor:000",
                    "display_name": "Actor 0",
                    "actor_type": "institution",
                    "actor_role": "filer",
                    "country_code": None,
                },
            ],
            "ambiguous candidate actor binding",
        ),
    ],
)
def test_missing_or_ambiguous_candidate_actor_fails_before_mutation(
    candidate_actors: list[dict[str, object]],
    message: str,
) -> None:
    candidate, template = _candidate_bundle()
    basis = candidate["basis"]
    assert isinstance(basis, dict)
    events = basis["events"]
    assert isinstance(events, list)
    event = events[0]
    assert isinstance(event, dict)
    event["actors"] = copy.deepcopy(candidate_actors)
    candidate["candidate_sha256"] = canonical_sha256(basis)
    decisions = _human_decisions(candidate, template)
    client = _ApplyClient(candidate)
    with pytest.raises(ExpeditedEditorialError, match=message):
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
    assert client.brief_payload is None
    assert all(
        event["review_status"] == "pending" for event in client.events.values()
    )


def test_nonnull_candidate_actor_country_cannot_be_overridden() -> None:
    candidate, template = _candidate_bundle()
    basis = candidate["basis"]
    assert isinstance(basis, dict)
    events = basis["events"]
    assert isinstance(events, list)
    event = events[0]
    assert isinstance(event, dict)
    actors = event["actors"]
    assert isinstance(actors, list)
    actor = actors[0]
    assert isinstance(actor, dict)
    actor["country_code"] = "US"
    candidate["candidate_sha256"] = canonical_sha256(basis)
    decisions = _human_decisions(candidate, template)
    client = _ApplyClient(candidate)
    with pytest.raises(
        ExpeditedEditorialError,
        match="candidate actor country mismatch",
    ):
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
    assert client.brief_payload is None
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
