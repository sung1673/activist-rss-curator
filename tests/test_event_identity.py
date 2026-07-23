from __future__ import annotations

from dataclasses import replace

import pytest

from curator.event_identity import (
    IDENTITY_FIELDS,
    EventIdentityMatch,
    EventIdentityStatus,
    build_event_identity,
    compare_event_identities,
)
from curator.governance import EventStatus, GovernanceEvent, GovernanceEventType, same_specific_event
from curator.official_sources import (
    cross_source_identity_conflicts,
    disclosure_payloads,
    link_correction_versions,
    parse_dart_disclosure,
    parse_kind_disclosure,
)


def complete_identity(**overrides: object):  # type: ignore[no-untyped-def]
    values: dict[str, object] = {
        "company_id": "00126380",
        "event_type": "tender_offer",
        "action": "launch tender offer",
        "target": "common shares",
        "actor_id": "actor:alpha-fund",
        "effective_at": "2026-07-16",
        "deadline_at": "2026-08-16",
    }
    values.update(overrides)
    return build_event_identity(**values)


def dart_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "corp_code": "00126380",
        "corp_name": "삼성전자",
        "stock_code": "005930",
        "corp_cls": "Y",
        "report_nm": "공개매수신고서",
        "rcept_no": "20260716000123",
        "flr_nm": "Alpha Fund",
        "rcept_dt": "20260716",
        "rm": "",
    }
    row.update(overrides)
    return row


def identity_source_fields(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "identity_action": "launch tender offer",
        "identity_target": "common shares",
        "identity_actor_id": "actor:alpha-fund",
        "identity_effective_at": "2026-07-16",
        "identity_deadline_at": "2026-08-16",
    }
    values.update(overrides)
    return values


def test_complete_identity_normalizes_unicode_whitespace_and_aware_timestamps() -> None:
    left = complete_identity(
        action="  LAUNCH   TENDER OFFER ",
        target="Ｃｏｍｍｏｎ Shares",
        effective_at="2026-07-16T09:00:00+09:00",
        deadline_at="2026-08-16T00:00:00Z",
    )
    right = complete_identity(
        effective_at="2026-07-16T00:00:00Z",
        deadline_at="2026-08-16T00:00:00+00:00",
    )

    assert left.status is EventIdentityStatus.COMPLETE
    assert left.comparison_key == right.comparison_key
    assert compare_event_identities(left, right).outcome is EventIdentityMatch.SAME


@pytest.mark.parametrize("missing_field", IDENTITY_FIELDS)
def test_every_missing_identity_dimension_fails_closed(missing_field: str) -> None:
    incomplete = complete_identity(**{missing_field: ""})

    assert incomplete.status is EventIdentityStatus.NEEDS_REVIEW
    assert incomplete.comparison_key is None
    assert f"missing_{missing_field}" in incomplete.review_reasons
    assert compare_event_identities(incomplete, incomplete).outcome is EventIdentityMatch.NEEDS_REVIEW


@pytest.mark.parametrize(
    ("field", "other"),
    [
        ("company_id", "00164779"),
        ("event_type", "merger"),
        ("action", "withdraw tender offer"),
        ("target", "preferred shares"),
        ("actor_id", "actor:beta-fund"),
        ("effective_at", "2026-07-17"),
        ("deadline_at", "2026-08-17"),
    ],
)
def test_every_known_identity_conflict_prevents_merge(field: str, other: str) -> None:
    decision = compare_event_identities(complete_identity(), complete_identity(**{field: other}))

    assert decision.outcome is EventIdentityMatch.DIFFERENT
    assert field in decision.conflicting_fields


def test_invalid_or_naive_identity_dates_require_review() -> None:
    assert complete_identity(effective_at="2026-02-30").review_reasons == ("invalid_effective_at",)
    assert complete_identity(deadline_at="2026-08-16T09:00:00").review_reasons == ("invalid_deadline_at",)


def test_governance_event_exposes_strict_identity_and_does_not_match_when_incomplete() -> None:
    event = GovernanceEvent(
        event_id="event:one",
        company_id="00126380",
        event_type=GovernanceEventType.TENDER_OFFER,
        occurred_at="2026-07-16",
        deadline_at="2026-08-16",
        status=EventStatus.CONFIRMED,
        actor_id="actor:alpha-fund",
        action="Launch tender offer",
        target="Common shares",
    )
    same = replace(event, event_id="event:two")
    incomplete = replace(event, event_id="event:three", deadline_at=None, identity_deadline_at=None)

    assert event.identity_status is EventIdentityStatus.COMPLETE
    assert event.comparison_key
    assert event.to_dict()["identity_status"] == "complete"
    assert same_specific_event(event, same)
    assert not same_specific_event(event, incomplete)


def test_incomplete_official_correction_is_isolated_and_requires_review() -> None:
    original = parse_dart_disclosure(dart_row())
    correction_title = "[정정]공개매수신고서"
    correction = parse_dart_disclosure(
        dart_row(report_nm=correction_title, rcept_no="20260717000124", rcept_dt="20260717")
    )
    assert original is not None and correction is not None

    linked = link_correction_versions([correction, original])
    assert [row[1] for row in linked] == [None, None]
    assert len({row[3] for row in linked}) == 2

    payload = disclosure_payloads([correction, original])
    assert payload["documents"][1]["title"] == correction_title
    assert payload["documents"][0]["collection_key"] != payload["documents"][1]["collection_key"]
    assert payload["events"][1]["identity_status"] == "needs_review"
    assert payload["events"][1]["comparison_key"] is None
    assert payload["events"][1]["review_required"] is True


def test_complete_correction_links_only_to_the_same_strict_identity() -> None:
    original = parse_dart_disclosure(dart_row(**identity_source_fields()))
    correction = parse_dart_disclosure(
        dart_row(
            report_nm="[정정]공개매수신고서",
            rcept_no="20260717000124",
            rcept_dt="20260717",
            **identity_source_fields(),
        )
    )
    conflicting = parse_dart_disclosure(
        dart_row(
            report_nm="[정정]공개매수신고서",
            rcept_no="20260718000125",
            rcept_dt="20260718",
            **identity_source_fields(identity_target="preferred shares"),
        )
    )
    assert original is not None and correction is not None and conflicting is not None

    original_link, correction_link = link_correction_versions([correction, original])
    assert correction_link[1] == original.document_id
    assert correction_link[3] == original_link[3]
    linked_payload = disclosure_payloads([correction, original])
    assert linked_payload["documents"][0]["collection_key"] == linked_payload["documents"][1]["collection_key"]
    assert linked_payload["documents"][1]["correction_of_document_id"] == original.document_id

    conflict_links = link_correction_versions([conflicting, original])
    assert conflict_links[1][1] is None
    assert conflict_links[1][3] != conflict_links[0][3]


def test_dart_and_kind_share_a_canonical_key_only_with_complete_matching_facts() -> None:
    dart = parse_dart_disclosure(dart_row(**identity_source_fields()))
    kind = parse_kind_disclosure(
        {
            "acptno": "20260716000999",
            "dart_corp_code": "00126380",
            "company_name": "삼성전자",
            "stock_code": "005930",
            "market": "KOSPI",
            "title": "Tender offer statement",
            "received_at": "2026-07-16T09:30:00+09:00",
            **identity_source_fields(),
        }
    )
    assert dart is not None and kind is not None
    assert dart.title == "공개매수신고서"
    assert kind.title == "Tender offer statement"
    assert dart.identity.comparison_key == kind.identity.comparison_key
    assert dart.identity.comparison_key is not None

    payload = disclosure_payloads([dart, kind])
    assert {row["event_id"] for row in payload["events"]} == {dart.identity.comparison_key}
    assert all(row["identity_status"] == "complete" for row in payload["events"])
    assert payload["events"][0]["collection_key"] == payload["events"][1]["collection_key"]
    assert payload["documents"][0]["collection_key"] != payload["documents"][1]["collection_key"]


def test_single_field_dart_kind_identity_mismatch_isolated_for_review() -> None:
    dart = parse_dart_disclosure(dart_row(**identity_source_fields()))
    kind = parse_kind_disclosure(
        {
            "acptno": "20260716000999",
            "dart_corp_code": "00126380",
            "company_name": "삼성전자",
            "stock_code": "005930",
            "market": "KOSPI",
            "title": "Tender offer statement",
            "received_at": "2026-07-16T09:30:00+09:00",
            **identity_source_fields(identity_target="preferred shares"),
        }
    )
    assert dart is not None and kind is not None
    assert dart.identity.status is EventIdentityStatus.COMPLETE
    assert kind.identity.status is EventIdentityStatus.COMPLETE
    assert dart.identity.comparison_key != kind.identity.comparison_key

    conflicts = cross_source_identity_conflicts([dart, kind])
    assert conflicts[("DART", dart.receipt_no)] == ("target",)
    assert conflicts[("KIND", kind.receipt_no)] == ("target",)

    payload = disclosure_payloads([dart, kind])
    assert len({event["event_id"] for event in payload["events"]}) == 2
    assert all(event["identity_status"] == "needs_review" for event in payload["events"])
    assert all(event["review_required"] is True for event in payload["events"])
    assert all(
        "cross_source_conflict_target" in event["identity_review_reasons"]
        for event in payload["events"]
    )
