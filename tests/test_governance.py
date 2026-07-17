from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone

import pytest

from curator.governance import (
    CampaignStage,
    Company,
    Document,
    EventStatus,
    GovernanceEvent,
    GovernanceEventType,
    Importance,
    SourceCategory,
    SourceRight,
    campaign_stage_options,
    event_fingerprint,
    publication_status,
    same_specific_event,
)


def test_company_uses_dart_corp_code_as_primary_id() -> None:
    company = Company(company_id="00126380", legal_name="삼성전자", stock_code="005930", market="KOSPI")
    assert company.to_dict()["company_id"] == "00126380"
    with pytest.raises(ValueError):
        Company(company_id="005930", legal_name="삼성전자")


def test_document_preserves_original_title_and_language() -> None:
    title = "[정정]주요사항보고서(자기주식취득결정)"
    document = Document(
        document_id="dart:20260716000123",
        stable_source_id="20260716000123",
        source_category=SourceCategory.OFFICIAL_DISCLOSURE,
        company_id="00126380",
        title=title,
        original_language="ko",
        received_at="2026-07-16T09:00:00+09:00",
        original_url="https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260716000123",
    )
    assert document.title == title
    assert document.to_dict()["original_language"] == "ko"


def test_source_right_fails_closed_for_expiry_revocation_and_scope() -> None:
    right = SourceRight(
        source_right_id="telegram:example",
        source_category=SourceCategory.AUTHORIZED_TELEGRAM,
        source_identity="example",
        scope="collection",
        evidence_ref="evidence://contract/1",
        valid_from="2026-01-01",
        expires_at="2026-07-16",
        allow_ai=False,
        allow_redistribution=False,
    )
    assert right.is_active(date(2026, 7, 15))
    assert not right.is_active(date(2026, 7, 16))
    assert not right.is_active(date(2026, 7, 15), purpose="ai")
    assert not right.is_active(date(2026, 7, 15), purpose="public")
    assert not replace(right, status="pending").is_active(date(2026, 7, 15))
    assert not replace(right, status="expired").is_active(date(2026, 7, 15))
    assert not replace(right, scope="").is_active(date(2026, 7, 15))


def test_source_right_uses_inclusive_start_at_datetime_precision() -> None:
    right = SourceRight(
        source_right_id="telegram:precise-start",
        source_category=SourceCategory.AUTHORIZED_TELEGRAM,
        source_identity="precise-start",
        scope="collection",
        evidence_ref="evidence://contract/precise-start",
        valid_from="2026-07-15T12:00:00Z",
    )

    assert not right.is_active(datetime(2026, 7, 15, 11, 59, 59, tzinfo=timezone.utc))
    assert right.is_active(datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc))


def test_source_right_expiry_is_exclusive_at_datetime_precision() -> None:
    right = SourceRight(
        source_right_id="telegram:precise-expiry",
        source_category=SourceCategory.AUTHORIZED_TELEGRAM,
        source_identity="precise-expiry",
        scope="collection",
        evidence_ref="evidence://contract/precise-expiry",
        valid_from="2026-07-15T00:00:00Z",
        expires_at="2026-07-15T12:00:00Z",
    )

    assert right.is_active(datetime(2026, 7, 15, 11, 59, 59, tzinfo=timezone.utc))
    assert not right.is_active(datetime(2026, 7, 15, 12, 0, 0, tzinfo=timezone.utc))
    assert not right.is_active(datetime(2026, 7, 15, 12, 0, 1, tzinfo=timezone.utc))


def test_source_right_normalizes_explicit_offsets_to_utc() -> None:
    kst = timezone(timedelta(hours=9))
    right = SourceRight(
        source_right_id="telegram:offset",
        source_category=SourceCategory.AUTHORIZED_TELEGRAM,
        source_identity="offset",
        scope="collection",
        evidence_ref="evidence://contract/offset",
        valid_from="2026-07-15T21:00:00+09:00",
        expires_at="2026-07-15T22:00:00+09:00",
    )

    assert not right.is_active(datetime(2026, 7, 15, 20, 59, 59, tzinfo=kst))
    assert right.is_active(datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc))
    assert not right.is_active(datetime(2026, 7, 15, 13, 0, tzinfo=timezone.utc))


def test_source_right_date_only_boundaries_are_midnight_utc_and_end_exclusive() -> None:
    right = SourceRight(
        source_right_id="telegram:date-only",
        source_category=SourceCategory.AUTHORIZED_TELEGRAM,
        source_identity="date-only",
        scope="collection",
        evidence_ref="evidence://contract/date-only",
        valid_from="2026-07-15",
        expires_at="2026-07-16",
    )

    assert not right.is_active(datetime(2026, 7, 14, 23, 59, 59, 999999, tzinfo=timezone.utc))
    assert right.is_active(date(2026, 7, 15))
    assert right.is_active(datetime(2026, 7, 15, 23, 59, 59, 999999, tzinfo=timezone.utc))
    assert not right.is_active(date(2026, 7, 16))


def test_source_right_naive_datetimes_are_explicitly_utc_and_invalid_boundaries_fail_closed() -> None:
    right = SourceRight(
        source_right_id="telegram:naive-utc",
        source_category=SourceCategory.AUTHORIZED_TELEGRAM,
        source_identity="naive-utc",
        scope="collection",
        evidence_ref="evidence://contract/naive-utc",
        valid_from="2026-07-15 12:00:00",
        expires_at="2026-07-15 13:00:00",
    )

    assert right.is_active(datetime(2026, 7, 15, 12, 0))
    assert not right.is_active(datetime(2026, 7, 15, 13, 0))
    assert not replace(right, valid_from="not-a-datetime").is_active(datetime(2026, 7, 15, 12, 0))
    assert not replace(right, expires_at="not-a-datetime").is_active(datetime(2026, 7, 15, 12, 0))
    assert not replace(right, revoked_at="not-a-datetime").is_active(datetime(2026, 7, 15, 12, 0))


def test_same_event_requires_company_action_target_actor_and_deadline() -> None:
    common = dict(
        event_id="event:one",
        company_id="00126380",
        event_type=GovernanceEventType.SHAREHOLDER_PROPOSAL,
        occurred_at="2026-03-01",
        status=EventStatus.CONFIRMED,
        actor_id="actor:fund",
        action="이사 선임 제안",
        target="정기주주총회 제3호 의안",
        deadline_at="2026-03-25",
    )
    left = GovernanceEvent(**common)
    right = GovernanceEvent(**{**common, "event_id": "event:two"})
    different_deadline = GovernanceEvent(**{**common, "event_id": "event:three", "deadline_at": "2026-03-26"})
    assert same_specific_event(left, right)
    assert not same_specific_event(left, different_deadline)
    assert event_fingerprint(
        left.company_id,
        left.event_type,
        actor_id=left.actor_id,
        action=left.action,
        target=left.target,
        deadline_at=left.deadline_at,
    ) != event_fingerprint(
        different_deadline.company_id,
        different_deadline.event_type,
        actor_id=different_deadline.actor_id,
        action=different_deadline.action,
        target=different_deadline.target,
        deadline_at=different_deadline.deadline_at,
    )


def test_telegram_only_is_signal_and_market_sensitive_requires_editor() -> None:
    assert publication_status([SourceCategory.AUTHORIZED_TELEGRAM]) == EventStatus.SIGNAL
    event = GovernanceEvent(
        event_id="event:tender",
        company_id="00126380",
        event_type=GovernanceEventType.TENDER_OFFER,
        occurred_at="2026-07-16",
        status=EventStatus.NEEDS_REVIEW,
        importance=Importance.HIGH,
    )
    assert publication_status([SourceCategory.OFFICIAL_DISCLOSURE], event=event) == EventStatus.NEEDS_REVIEW
    assert publication_status([SourceCategory.OFFICIAL_DISCLOSURE], event=event, editor_approved=True) == EventStatus.CONFIRMED


def test_campaign_stages_are_fixed_and_bilingual() -> None:
    options = campaign_stage_options()
    assert [option["value"] for option in options] == [stage.value for stage in CampaignStage]
    assert options[0] == {"value": "initial_signal", "label_ko": "초기 신호", "label_en": "Initial signal"}
