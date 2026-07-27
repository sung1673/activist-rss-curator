from __future__ import annotations

import io
import re
import zipfile
from datetime import date, datetime, timezone

import httpx
import pytest

from curator.dart_quota import (
    DartCredentialUnavailableError,
    DartGlobalQuotaExceededError,
    DartQuotaLedgerError,
)
from curator.governance import GovernanceEventType
from curator.official_ingest import source_right_payloads
from curator.opendart_credentials import load_opendart_credentials
from curator.official_sources import (
    DART_GOVERNANCE_DETAIL_CODES,
    DART_LIST_URL,
    DartConnector,
    DartInvocationQuota,
    DartQuotaExceededError,
    DartRequestBudget,
    DartRequestBudgetError,
    KindConnector,
    OfficialSourceError,
    base_disclosure_title,
    classify_governance_disclosure,
    disclosure_collection_key,
    disclosure_payloads,
    link_correction_versions,
    original_language,
    parse_corp_code_zip,
    parse_dart_disclosure,
    parse_dart_list_payload,
    parse_kind_disclosure,
    parse_kind_list_payload,
    validate_kind_endpoint,
)


def dart_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "corp_code": "00126380",
        "corp_name": "삼성전자",
        "stock_code": "005930",
        "corp_cls": "Y",
        "report_nm": "주요사항보고서(자기주식취득결정)",
        "rcept_no": "20260716000123",
        "flr_nm": "삼성전자",
        "rcept_dt": "20260716",
        "rm": "",
    }
    row.update(overrides)
    return row


def strict_identity(**overrides: object) -> dict[str, object]:
    identity: dict[str, object] = {
        "identity_target": "governance subject",
        "identity_effective_at": "2026-07-16",
        "identity_deadline_at": "2026-08-31",
    }
    identity.update(overrides)
    return identity


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://kind.example/api",
        "https://user:password@kind.example/api",
        "https://kind.example/api?token=secret",
        "https://kind.example/api#fragment",
        "https://kind.example/api;parameter",
        " https://kind.example/api",
        "https://kind.example:invalid/api",
        "//kind.example/api",
    ],
)
def test_kind_endpoint_rejects_plaintext_or_credential_bearing_urls(endpoint: str) -> None:
    with pytest.raises(ValueError, match="KIND endpoint"):
        validate_kind_endpoint(endpoint)


def test_kind_endpoint_accepts_clean_absolute_https_url() -> None:
    endpoint = "https://kind.example:8443/v1/disclosures"
    assert validate_kind_endpoint(endpoint) == endpoint


def test_classifies_governance_scope_without_translating_title() -> None:
    title = "[정정] 주요사항보고서(회사분할결정)"
    assert classify_governance_disclosure(title) == GovernanceEventType.SPLIT
    assert base_disclosure_title(title) == "주요사항보고서(회사분할결정)"
    assert original_language(title) == "ko"
    assert original_language("Tender offer statement") == "en"


def test_title_rules_avoid_insider_ownership_and_generic_board_false_positives() -> None:
    assert classify_governance_disclosure("임원ㆍ주요주주특정증권등소유상황보고서") is None
    assert classify_governance_disclosure("이사회결의") is None
    assert classify_governance_disclosure("사외이사 중도퇴임에 관한 신고") == GovernanceEventType.BOARD
    assert (
        classify_governance_disclosure("주식매수선택권부여에관한신고")
        == GovernanceEventType.EXECUTIVE_COMPENSATION
    )


@pytest.mark.parametrize(
    ("detail_code", "expected"),
    [
        ("D001", GovernanceEventType.FIVE_PERCENT_HOLDING),
        ("D003", GovernanceEventType.GENERAL_MEETING),
        ("D004", GovernanceEventType.TENDER_OFFER),
        ("E001", GovernanceEventType.TREASURY_SHARES),
        ("E002", GovernanceEventType.TREASURY_SHARES),
        ("E004", GovernanceEventType.EXECUTIVE_COMPENSATION),
        ("E005", GovernanceEventType.BOARD),
        ("E006", GovernanceEventType.GENERAL_MEETING),
    ],
)
def test_dart_detail_metadata_recovers_generic_titles(
    detail_code: str,
    expected: GovernanceEventType,
) -> None:
    disclosure = parse_dart_disclosure(
        dart_row(report_nm="공시보고서", pblntf_detail_ty=detail_code)
    )
    assert disclosure is not None
    assert disclosure.event_type == expected
    assert disclosure.title == "공시보고서"


@pytest.mark.parametrize("detail_code", ["D002", "D005"])
def test_dart_insider_ownership_detail_types_are_not_five_percent_events(detail_code: str) -> None:
    disclosure = parse_dart_disclosure(
        dart_row(
            report_nm="주식등의대량보유상황보고서",
            pblntf_detail_ty=detail_code,
        )
    )
    assert disclosure is None


def test_conflicting_official_metadata_fails_closed() -> None:
    assert (
        parse_dart_disclosure(
            dart_row(
                report_nm="공시보고서",
                pblntf_detail_ty="D001",
                document_type="board",
            )
        )
        is None
    )


def test_open_dart_detail_row_signatures_distinguish_large_holding_board_and_compensation() -> None:
    large_holding = parse_dart_disclosure(
        dart_row(
            report_nm="지분변동 보고",
            report_tp="신규보고",
            repror="보고자",
            stkqy="100000",
            stkrt="5.10",
            report_resn="신규 보유",
        )
    )
    board = parse_dart_disclosure(
        dart_row(
            report_nm="정기보고서 임원 현황",
            reprt_code="11011",
            rgist_exctv_at="등기임원",
            chrg_job="사외이사",
        )
    )
    compensation = parse_dart_disclosure(
        dart_row(
            report_nm="정기보고서 보수 현황",
            reprt_code="11011",
            nm="홍길동",
            ofcps="이사",
            mendng_totamt="600000000",
        )
    )
    insider = parse_dart_disclosure(
        dart_row(
            report_nm="지분변동 보고",
            api_endpoint="https://opendart.fss.or.kr/api/elestock.json",
            isu_exctv_rgist_at="등기임원",
            repror="보고자",
        )
    )
    assert large_holding is not None and large_holding.event_type == GovernanceEventType.FIVE_PERCENT_HOLDING
    assert board is not None and board.event_type == GovernanceEventType.BOARD
    assert compensation is not None and compensation.event_type == GovernanceEventType.EXECUTIVE_COMPENSATION
    assert insider is None


def test_kind_adapter_uses_exact_detail_metadata_and_preserves_original_title() -> None:
    title = "Outside director filing"
    disclosure = parse_kind_disclosure(
        {
            "acptno": "20260716000999",
            "dart_corp_code": "00126380",
            "company_name": "삼성전자",
            "stock_code": "005930",
            "market": "KOSPI",
            "title": title,
            "date": "20260716",
            "metadata": {"disclosure_detail_code": "E005"},
        }
    )
    assert disclosure is not None
    assert disclosure.event_type == GovernanceEventType.BOARD
    assert disclosure.title == title


def test_kind_adapter_rejects_d002_even_when_title_looks_like_large_holding() -> None:
    disclosure = parse_kind_disclosure(
        {
            "acptno": "20260716000999",
            "dart_corp_code": "00126380",
            "company_name": "삼성전자",
            "title": "주식등의대량보유상황보고서",
            "date": "20260716",
            "disclosure_detail_type": "D002",
        }
    )
    assert disclosure is None


def test_dart_parser_preserves_receipt_title_language_and_official_url() -> None:
    title = "[정정]주요사항보고서(자기주식취득결정)"
    disclosure = parse_dart_disclosure(dart_row(report_nm=title))
    assert disclosure is not None
    assert disclosure.title == title
    assert disclosure.document_id == "dart:20260716000123"
    assert "rcpNo=20260716000123" in disclosure.original_url
    payload = disclosure_payloads([disclosure], retrieved_at=datetime(2026, 7, 16, tzinfo=timezone.utc))
    assert payload["documents"][0]["original_language"] == "ko"
    assert payload["documents"][0]["title"] == title
    assert payload["documents"][0]["metadata"] == {"title_provenance": "source"}
    assert payload["events"][0]["title"] == title
    assert payload["events"][0]["metadata"] == {"title_provenance": "source"}


def test_official_payload_preserves_filer_as_reviewable_actor_and_event_relation() -> None:
    disclosure = parse_kind_disclosure(
        {
            "acptno": "20260716000998",
            "dart_corp_code": "00126380",
            "company_name": "Target Corp",
            "stock_code": "005930",
            "market": "KOSPI",
            "title": "Outside director filing",
            "date": "20260716",
            "filer_name": "Alpha Capital LLC",
            "metadata": {"disclosure_detail_code": "E005"},
            **strict_identity(identity_target="board composition"),
        }
    )
    assert disclosure is not None

    event = disclosure_payloads([disclosure])["events"][0]
    actor = event["actor"]
    relation = event["event_actor"]
    assert actor == {
        "actor_id": disclosure.identity.actor_id,
        "actor_type": "institution",
        "display_name": "Alpha Capital LLC",
        "company_id": None,
        "review_status": "pending",
        "record_status": "inactive",
    }
    assert relation == {
        "event_id": event["event_id"],
        "actor_id": disclosure.identity.actor_id,
        "actor_role": "filer",
        "review_status": "pending",
    }


def test_official_payload_does_not_invent_actor_display_name_when_filer_is_missing() -> None:
    disclosure = parse_kind_disclosure(
        {
            "acptno": "20260716000997",
            "dart_corp_code": "00126380",
            "company_name": "Target Corp",
            "stock_code": "005930",
            "market": "KOSPI",
            "title": "Outside director filing",
            "date": "20260716",
            "metadata": {"disclosure_detail_code": "E005"},
            **strict_identity(identity_target="board composition"),
        }
    )
    assert disclosure is not None
    assert disclosure.identity.actor_id

    event = disclosure_payloads([disclosure])["events"][0]
    assert "actor" not in event
    assert "event_actor" not in event


def test_official_payload_links_company_filer_to_target_company() -> None:
    disclosure = parse_kind_disclosure(
        {
            "acptno": "20260716000996",
            "dart_corp_code": "00126380",
            "company_name": "Target Corp",
            "stock_code": "005930",
            "market": "KOSPI",
            "title": "Outside director filing",
            "date": "20260716",
            "filer_name": "  Target Corp  ",
            "metadata": {"disclosure_detail_code": "E005"},
            **strict_identity(identity_target="board composition"),
        }
    )
    assert disclosure is not None

    actor = disclosure_payloads([disclosure])["events"][0]["actor"]
    assert actor["display_name"] == "Target Corp"
    assert actor["actor_type"] == "company"
    assert actor["company_id"] == "00126380"


def test_official_parsers_reject_lossy_identifiers_and_missing_original_title() -> None:
    with pytest.raises(ValueError, match="DART report_nm is required"):
        parse_dart_disclosure(dart_row(report_nm="", pblntf_detail_ty="D001"))
    with pytest.raises(ValueError, match="invalid DART"):
        parse_dart_disclosure(dart_row(rcept_no="2026-0716-000123"))
    with pytest.raises(ValueError, match="stable receipt number"):
        parse_kind_disclosure(
            {
                "acptno": "unsafe/receipt",
                "dart_corp_code": "00126380",
                "company_name": "삼성전자",
                "title": "매매거래정지",
                "date": "20260716",
            }
        )


def test_correction_links_only_when_predecessor_is_present() -> None:
    original = parse_dart_disclosure(dart_row(**strict_identity(identity_target="treasury shares")))
    correction = parse_dart_disclosure(
        dart_row(
            report_nm="[정정]주요사항보고서(자기주식취득결정)",
            rcept_no="20260717000124",
            rcept_dt="20260717",
            **strict_identity(identity_target="treasury shares"),
        )
    )
    assert original is not None and correction is not None
    linked = link_correction_versions([correction, original])
    assert linked[0][1] is None
    assert linked[1][1] == original.document_id
    assert linked[1][2] == 2
    assert linked[1][3] == linked[0][3]
    correction_alone = link_correction_versions([correction])
    assert correction_alone[0][1] is None
    assert correction_alone[0][2] == 1


def test_collection_key_includes_filer_and_keeps_the_legacy_call_valid() -> None:
    title = "주식등의대량보유상황보고서"
    legacy = disclosure_collection_key("DART", "00126380", title)
    assert disclosure_collection_key("DART", "00126380", title, "") == legacy
    assert disclosure_collection_key("DART", "00126380", title, "Alpha Fund") != legacy
    assert disclosure_collection_key("DART", "00126380", title, "Alpha Fund") != (
        disclosure_collection_key("DART", "00126380", title, "Beta Fund")
    )


def test_correction_linking_fails_closed_when_multiple_predecessors_share_a_key() -> None:
    original_a = parse_dart_disclosure(
        dart_row(
            report_nm="주식등의대량보유상황보고서",
            rcept_no="20260716000121",
            **strict_identity(identity_target="issuer shares"),
        )
    )
    original_b = parse_dart_disclosure(
        dart_row(
            report_nm="주식등의대량보유상황보고서",
            rcept_no="20260717000122",
            rcept_dt="20260717",
            **strict_identity(identity_target="issuer shares"),
        )
    )
    correction = parse_dart_disclosure(
        dart_row(
            report_nm="[정정]주식등의대량보유상황보고서",
            rcept_no="20260718000123",
            rcept_dt="20260718",
            **strict_identity(identity_target="issuer shares"),
        )
    )
    assert original_a is not None and original_b is not None and correction is not None

    linked = link_correction_versions([correction, original_b, original_a])
    assert [correction_of for _, correction_of, _, _ in linked] == [None, None, None]
    assert [version_no for _, _, version_no, _ in linked] == [1, 1, 1]
    assert len({event_id for _, _, _, event_id in linked}) == 1


def test_correction_linking_uses_filer_to_disambiguate_repeated_reports() -> None:
    original_alpha = parse_dart_disclosure(
        dart_row(
            report_nm="주식등의대량보유상황보고서",
            flr_nm="Alpha Fund",
            rcept_no="20260716000121",
            **strict_identity(identity_target="issuer shares"),
        )
    )
    original_beta = parse_dart_disclosure(
        dart_row(
            report_nm="주식등의대량보유상황보고서",
            flr_nm="Beta Fund",
            rcept_no="20260716000122",
            **strict_identity(identity_target="issuer shares"),
        )
    )
    correction_alpha = parse_dart_disclosure(
        dart_row(
            report_nm="[정정]주식등의대량보유상황보고서",
            flr_nm="Alpha Fund",
            rcept_no="20260717000123",
            rcept_dt="20260717",
            **strict_identity(identity_target="issuer shares"),
        )
    )
    assert original_alpha is not None and original_beta is not None and correction_alpha is not None

    linked = link_correction_versions([correction_alpha, original_beta, original_alpha])
    alpha_original, beta_original, alpha_correction = linked
    assert alpha_correction[1] == alpha_original[0].document_id
    assert alpha_correction[2] == 2
    assert alpha_correction[3] == alpha_original[3]
    assert beta_original[1] is None
    assert beta_original[3] != alpha_original[3]


def test_dart_rm_correction_flag_marks_a_later_version_not_the_current_receipt() -> None:
    original = parse_dart_disclosure(dart_row(rm="유정"))
    assert original is not None
    assert original.has_later_correction is True
    assert original.is_correction is False
    assert original.is_cancelled is False

    payload = disclosure_payloads([original])
    assert payload["documents"][0]["has_later_correction"] is True
    assert payload["documents"][0]["correction_of_document_id"] is None
    assert payload["documents"][0]["version_no"] == 1
    assert payload["documents"][0]["publication_status"] == "published"
    assert payload["events"][0]["is_correction"] is False


@pytest.mark.parametrize("prefix", ["[정정명령부과]", "[정정제출요구]"])
def test_dart_regulatory_correction_notices_are_not_current_filing_corrections(prefix: str) -> None:
    title = f"{prefix}주요사항보고서(자기주식취득결정)"
    disclosure = parse_dart_disclosure(dart_row(report_nm=title))
    assert disclosure is not None
    assert disclosure.is_correction is False
    assert base_disclosure_title(title) == title


def test_cancelled_disclosure_is_withdrawn_but_retained() -> None:
    cancelled = parse_dart_disclosure(dart_row(report_nm="[철회]주요사항보고서(유상증자결정)"))
    assert cancelled is not None and cancelled.is_cancelled
    payload = disclosure_payloads([cancelled])
    assert payload["documents"][0]["publication_status"] == "withdrawn"
    assert payload["events"][0]["is_cancelled"] is True


def test_withdrawal_receipt_advances_a_unique_document_and_event_chain() -> None:
    original = parse_dart_disclosure(
        dart_row(
            report_nm="주요사항보고서(유상증자결정)",
            rcept_no="20260716000121",
            **strict_identity(identity_target="new shares"),
        )
    )
    withdrawn = parse_dart_disclosure(
        dart_row(
            report_nm="[철회]주요사항보고서(유상증자결정)",
            rcept_no="20260717000122",
            rcept_dt="20260717",
            **strict_identity(identity_target="new shares"),
        )
    )
    assert original is not None and withdrawn is not None

    linked = link_correction_versions([withdrawn, original])
    assert linked[1][1] == original.document_id
    assert linked[1][2] == 2
    assert linked[1][3] == linked[0][3]
    payload = disclosure_payloads([withdrawn, original])
    assert [row["version_no"] for row in payload["documents"]] == [1, 2]
    assert payload["documents"][1]["publication_status"] == "withdrawn"


def test_english_revision_prefixes_share_a_collection_key() -> None:
    assert base_disclosure_title("Withdrawal: Tender offer statement") == "Tender offer statement"
    assert disclosure_collection_key("KIND", "00126380", "Tender offer statement") == (
        disclosure_collection_key("KIND", "00126380", "Withdrawal: Tender offer statement")
    )


def test_dart_withdrawal_remark_code_is_fail_closed_and_retained() -> None:
    withdrawn = parse_dart_disclosure(dart_row(rm="유철"))
    assert withdrawn is not None and withdrawn.is_cancelled
    payload = disclosure_payloads([withdrawn])
    assert payload["documents"][0]["publication_status"] == "withdrawn"


def test_dart_withdrawal_remark_does_not_link_repeated_independent_filing() -> None:
    prior = parse_dart_disclosure(
        dart_row(
            report_nm="주식등의대량보유상황보고서",
            flr_nm="Alpha Fund",
            rcept_no="20260715000121",
            rcept_dt="20260715",
        )
    )
    withdrawn_same_receipt = parse_dart_disclosure(
        dart_row(
            report_nm="주식등의대량보유상황보고서",
            flr_nm="Alpha Fund",
            rcept_no="20260716000122",
            rm="유철",
        )
    )
    assert prior is not None and withdrawn_same_receipt is not None

    linked = link_correction_versions([withdrawn_same_receipt, prior])
    assert linked[1][1] is None
    assert linked[1][2] == 1
    assert linked[1][3] != linked[0][3]


def test_kind_contract_covers_trading_halt_and_stable_dart_company_id() -> None:
    disclosure = parse_kind_disclosure(
        {
            "acptno": "20260716000999",
            "dart_corp_code": "00126380",
            "company_name": "삼성전자",
            "stock_code": "005930",
            "market": "KOSPI",
            "title": "매매거래정지 및 재개",
            "date": "20260716",
        }
    )
    assert disclosure is not None
    assert disclosure.event_type == GovernanceEventType.TRADING_SUSPENSION
    assert disclosure.corp_code == "00126380"
    assert disclosure.source == "KIND"


def test_kind_timestamp_assumes_kst_only_when_offset_is_omitted_and_sanitizes_url() -> None:
    disclosure = parse_kind_disclosure(
        {
            "acptno": "20260716000999",
            "dart_corp_code": "00126380",
            "company_name": "삼성전자",
            "title": "Outside director filing",
            "received_at": "2026-07-16T09:00:00",
            "url": "javascript:alert(1)",
            "metadata": {"disclosure_detail_code": "E005"},
        }
    )
    assert disclosure is not None
    assert disclosure.received_at == "2026-07-16T00:00:00+00:00"
    assert disclosure.original_url.startswith("https://kind.krx.co.kr/")


def test_kind_adapter_envelope_requires_explicit_failure_and_pagination_contract() -> None:
    rows, page, total_pages = parse_kind_list_payload(
        {
            "status": "success",
            "data": {
                "items": [{"id": 1}],
                "pagination": {"current_page": 2, "last_page": 3},
            },
        }
    )
    assert rows == [{"id": 1}]
    assert (page, total_pages) == (2, 3)

    with pytest.raises(OfficialSourceError, match="reported a failure"):
        parse_kind_list_payload({"success": False, "items": [], "page": 1, "total_pages": 1})
    with pytest.raises(OfficialSourceError, match="requires page and total_pages"):
        parse_kind_list_payload({"items": []})
    assert parse_kind_list_payload({"items": [], "unpaginated": True}) == ([], 1, 1)


def test_dart_no_data_is_empty_and_errors_are_not_silenced() -> None:
    assert parse_dart_list_payload({"status": "013", "message": "조회된 데이타가 없습니다."}) == ([], 0, 0)
    with pytest.raises(OfficialSourceError, match=r"non-success status 010"):
        parse_dart_list_payload({"status": "010", "message": "등록되지 않은 키입니다."})
    with pytest.raises(OfficialSourceError):
        parse_dart_list_payload({"status": "020", "message": "요청 제한 초과"})
    with pytest.raises(OfficialSourceError, match="omitted list"):
        parse_dart_list_payload({"status": "000", "page_no": 1, "total_page": 1})


def test_dart_connector_paginates_until_total_page() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page_no"])
        return httpx.Response(
            200,
            json={
                "status": "000",
                "page_no": page,
                "total_page": 2,
                "list": [dart_row(rcept_no=f"2026071600012{page}")],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        rows = list(
            DartConnector("x" * 40, client=client, governance_detail_codes=()).iter_disclosure_rows(
                date(2026, 7, 15), date(2026, 7, 16)
            )
        )
    assert [row["rcept_no"] for row in rows] == ["20260716000121", "20260716000122"]


def test_dart_connector_owned_pool_is_reused_and_closed_once() -> None:
    created: list[httpx.Client] = []
    requests = 0
    close_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        page = int(request.url.params["page_no"])
        return httpx.Response(
            200,
            json={
                "status": "000",
                "page_no": page,
                "total_page": 2,
                "list": [dart_row(rcept_no=f"2026071600012{page}")],
            },
        )

    class TrackingClient(httpx.Client):
        def close(self) -> None:
            nonlocal close_calls
            close_calls += 1
            super().close()

    def factory(**kwargs: object) -> httpx.Client:
        client = TrackingClient(
            transport=httpx.MockTransport(handler),
            **kwargs,  # type: ignore[arg-type]
        )
        created.append(client)
        return client

    connector = DartConnector(
        "x" * 40,
        client_factory=factory,
        governance_detail_codes=(),
    )
    with connector:
        rows = list(
            connector.iter_disclosure_rows(
                date(2026, 7, 15),
                date(2026, 7, 16),
            )
        )

    assert len(rows) == 2
    assert requests == 2
    assert len(created) == 1
    assert created[0].is_closed is True
    connector.close()
    assert close_calls == 1


def test_dart_connector_close_failure_is_fail_closed_and_can_be_retried() -> None:
    close_attempts = 0

    class FlakyCloseClient(httpx.Client):
        def close(self) -> None:
            nonlocal close_attempts
            close_attempts += 1
            if close_attempts == 1:
                raise RuntimeError("transient connector close failure")
            super().close()

    def factory(**kwargs: object) -> httpx.Client:
        return FlakyCloseClient(
            transport=httpx.MockTransport(
                lambda _request: httpx.Response(
                    200,
                    json={"status": "013", "message": "no data"},
                )
            ),
            **kwargs,  # type: ignore[arg-type]
        )

    budget = DartRequestBudget(1)
    connector = DartConnector(
        "x" * 40,
        client_factory=factory,
        governance_detail_codes=(),
        request_budget=budget,
    )

    with pytest.raises(RuntimeError, match="transient connector close failure"):
        connector.close()
    with pytest.raises(OfficialSourceError, match="closed"):
        list(
            connector.iter_disclosure_rows(
                date(2026, 7, 15),
                date(2026, 7, 16),
            )
        )

    connector.close()
    connector.close()
    assert close_attempts == 2
    assert budget.used == 0


def test_dart_connector_does_not_close_borrowed_client() -> None:
    borrowed = httpx.Client(
        transport=httpx.MockTransport(
            lambda _request: httpx.Response(
                200,
                json={"status": "013", "message": "no data"},
            )
        )
    )
    connector = DartConnector(
        "x" * 40,
        client=borrowed,
        governance_detail_codes=(),
    )

    connector.close()
    assert borrowed.is_closed is False
    assert borrowed.get(DART_LIST_URL).status_code == 200
    borrowed.close()


def test_closed_dart_connector_fails_before_consuming_quota() -> None:
    budget = DartRequestBudget(1)
    with httpx.Client(transport=httpx.MockTransport(lambda _request: httpx.Response(500))) as client:
        connector = DartConnector(
            "x" * 40,
            client=client,
            governance_detail_codes=(),
            request_budget=budget,
        )
        connector.close()
        with pytest.raises(OfficialSourceError, match="closed"):
            list(
                connector.iter_disclosure_rows(
                    date(2026, 7, 15),
                    date(2026, 7, 16),
                )
            )

    assert budget.used == 0


def test_owned_dart_pool_does_not_carry_cookie_across_credential_rotation() -> None:
    key_a, key_b = "a" * 40, "b" * 40
    credentials = load_opendart_credentials(
        {"OPENDART_API_KEYS": f"{key_a}\n{key_b}"}
    )
    observed: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        key = str(request.url.params["crtfc_key"])
        observed.append((key, request.headers.get("cookie", "")))
        if key == key_a:
            return httpx.Response(
                200,
                headers={"Set-Cookie": "credential=a; Path=/"},
                json={"status": "020"},
            )
        return httpx.Response(200, json={"status": "013", "message": "no data"})

    def factory(**kwargs: object) -> httpx.Client:
        return httpx.Client(
            transport=httpx.MockTransport(handler),
            **kwargs,  # type: ignore[arg-type]
        )

    with DartConnector(
        credentials,
        client_factory=factory,
        governance_detail_codes=(),
        quota_day_provider=lambda: date(2026, 7, 26),
    ) as connector:
        assert list(
            connector.iter_disclosure_rows(
                date(2026, 7, 26),
                date(2026, 7, 26),
            )
        ) == []

    assert observed == [(key_a, ""), (key_b, "")]


def test_dart_connector_retries_429_and_5xx_with_bounded_backoff() -> None:
    statuses = iter((429, 503, 200))
    sleeps: list[float] = []

    def handler(_request: httpx.Request) -> httpx.Response:
        status = next(statuses)
        if status != 200:
            return httpx.Response(status, headers={"Retry-After": "2"})
        return httpx.Response(
            200,
            json={"status": "000", "page_no": 1, "total_page": 1, "list": [dart_row()]},
        )

    budget = DartRequestBudget(3)
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = DartConnector(
            "x" * 40,
            client=client,
            governance_detail_codes=(),
            request_budget=budget,
            max_retries=2,
            backoff_seconds=1,
            sleeper=sleeps.append,
        )
        rows = list(connector.iter_disclosure_rows(date(2026, 7, 15), date(2026, 7, 16)))

    assert len(rows) == 1
    assert sleeps == [2.0, 2.0]
    assert connector.requests_made == 3
    assert budget.used == 3


def test_dart_connector_fails_fast_on_daily_quota_status_020() -> None:
    requests = 0
    sleeps: list[float] = []

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(200, json={"status": "020", "message": "quota exceeded"})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = DartConnector(
            "x" * 40,
            client=client,
            governance_detail_codes=(),
            max_retries=2,
            sleeper=sleeps.append,
        )
        with pytest.raises(DartQuotaExceededError, match="resume from the checkpoint later"):
            list(connector.iter_disclosure_rows(date(2026, 7, 15), date(2026, 7, 16)))

    assert requests == 1
    assert sleeps == []


def test_dart_pool_rotates_logical_requests_and_blocks_only_status_020_key() -> None:
    key_a, key_b, key_c = "a" * 40, "b" * 40, "c" * 40
    credentials = load_opendart_credentials(
        {"OPENDART_API_KEYS": f"{key_a}\r\n{key_b},{key_c}"}
    )
    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        key = str(request.url.params["crtfc_key"])
        requested.append(key)
        if key == key_a:
            return httpx.Response(200, json={"status": "020"})
        return httpx.Response(
            200,
            json={
                "status": "000",
                "page_no": 1,
                "total_page": 1,
                "list": [dart_row()],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = DartConnector(
            credentials,
            client=client,
            governance_detail_codes=(),
            quota_day_provider=lambda: date(2026, 7, 26),
        )
        for _ in range(3):
            assert len(
                list(
                    connector.iter_disclosure_rows(
                        date(2026, 7, 26),
                        date(2026, 7, 26),
                    )
                )
            ) == 1

    assert requested == [key_a, key_b, key_c, key_b]
    assert set(connector.credential_requests) == {
        credential.credential_id for credential in credentials
    }
    assert all(key not in repr(connector.credential_requests) for key in requested)


def test_dart_pool_disables_status_901_key_and_retries_same_logical_request() -> None:
    key_a, key_b = "a" * 40, "b" * 40
    credentials = load_opendart_credentials(
        {"OPENDART_API_KEYS": f"{key_a}\n{key_b}"}
    )
    requested: list[str] = []
    actions: list[tuple[str, str]] = []

    class RecordingQuota:
        limit = 40_000
        used = 0

        def consume(self, *, operation: str, credential_id: str) -> object:
            assert operation == "list"
            self.used += 1
            actions.append(("consume", credential_id))
            return credential_id

        def block_020(self, permit: object) -> None:
            raise AssertionError(f"unexpected 020 block for {permit!r}")

        def disable_901(self, permit: object) -> None:
            actions.append(("disable_901", str(permit)))

    def handler(request: httpx.Request) -> httpx.Response:
        key = str(request.url.params["crtfc_key"])
        requested.append(key)
        if key == key_a:
            return httpx.Response(
                200,
                json={"status": "901", "message": f"invalid {key_a}"},
            )
        return httpx.Response(
            200,
            json={
                "status": "000",
                "page_no": 1,
                "total_page": 1,
                "list": [dart_row()],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = DartConnector(
            credentials,
            client=client,
            governance_detail_codes=(),
            request_budget=RecordingQuota(),
            quota_day_provider=lambda: date(2026, 7, 26),
        )
        rows = list(
            connector.iter_disclosure_rows(
                date(2026, 7, 26),
                date(2026, 7, 26),
            )
        )

    assert len(rows) == 1
    assert requested == [key_a, key_b]
    first_id, second_id = (credential.credential_id for credential in credentials)
    assert actions == [
        ("consume", first_id),
        ("disable_901", first_id),
        ("consume", second_id),
    ]


def test_dart_pool_does_not_try_next_key_when_901_disable_ack_is_unverified() -> None:
    key_a, key_b = "a" * 40, "b" * 40
    credentials = load_opendart_credentials(
        {"OPENDART_API_KEYS": f"{key_a}\n{key_b}"}
    )
    requested: list[str] = []

    class UnverifiedDisableQuota:
        limit = 40_000
        used = 0

        def consume(self, *, operation: str, credential_id: str) -> object:
            assert operation == "list"
            self.used += 1
            return credential_id

        def block_020(self, permit: object) -> None:
            raise AssertionError(f"unexpected 020 block for {permit!r}")

        def disable_901(self, _permit: object) -> None:
            raise DartQuotaLedgerError(
                "DART quota API explicit replay was not acknowledged as duplicate"
            )

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url.params["crtfc_key"]))
        return httpx.Response(200, json={"status": "901"})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = DartConnector(
            credentials,
            client=client,
            governance_detail_codes=(),
            request_budget=UnverifiedDisableQuota(),
            quota_day_provider=lambda: date(2026, 7, 26),
        )
        with pytest.raises(DartQuotaLedgerError, match="explicit replay"):
            list(
                connector.iter_disclosure_rows(
                    date(2026, 7, 26),
                    date(2026, 7, 26),
                )
            )

    assert requested == [key_a]


def test_dart_pool_binds_durable_actions_to_sha256_credential_identity() -> None:
    key_a, key_b = "a" * 40, "b" * 40
    credentials = load_opendart_credentials(
        {"OPENDART_API_KEYS": f"{key_a},{key_b}"}
    )
    actions: list[tuple[str, str]] = []

    class RecordingQuota:
        limit = 40_000
        used = 0

        def consume(self, *, operation: str, credential_id: str) -> object:
            assert operation == "list"
            self.used += 1
            actions.append(("consume", credential_id))
            return credential_id

        def block_020(self, permit: object) -> None:
            actions.append(("block_020", str(permit)))

        def disable_901(self, permit: object) -> None:
            actions.append(("disable_901", str(permit)))

    def handler(request: httpx.Request) -> httpx.Response:
        key = str(request.url.params["crtfc_key"])
        if key == key_a:
            return httpx.Response(200, json={"status": "020"})
        return httpx.Response(
            200,
            json={
                "status": "000",
                "page_no": 1,
                "total_page": 1,
                "list": [dart_row()],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = DartConnector(
            credentials,
            client=client,
            governance_detail_codes=(),
            request_budget=RecordingQuota(),
            quota_day_provider=lambda: date(2026, 7, 26),
        )
        assert len(
            list(
                connector.iter_disclosure_rows(
                    date(2026, 7, 26),
                    date(2026, 7, 26),
                )
            )
        ) == 1

    first_id, second_id = (credential.credential_id for credential in credentials)
    assert actions == [
        ("consume", first_id),
        ("block_020", first_id),
        ("consume", second_id),
    ]


def test_dart_pool_skips_only_durably_unavailable_credential() -> None:
    key_a, key_b = "a" * 40, "b" * 40
    credentials = load_opendart_credentials(
        {"OPENDART_API_KEYS": f"{key_a},{key_b}"}
    )
    rejected_id = credentials[0].credential_id
    requested: list[str] = []

    class PreblockedQuota:
        limit = 40_000
        used = 0

        def consume(self, *, operation: str, credential_id: str) -> object:
            assert operation == "list"
            if credential_id == rejected_id:
                raise DartCredentialUnavailableError(
                    reason="blocked_020",
                    credential_id=credential_id,
                )
            self.used += 1
            return credential_id

        def block_020(self, permit: object) -> None:
            raise AssertionError(f"no provider response exists for {permit!r}")

        def disable_901(self, permit: object) -> None:
            raise AssertionError(f"no provider response exists for {permit!r}")

    def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url.params["crtfc_key"]))
        return httpx.Response(
            200,
            json={
                "status": "000",
                "page_no": 1,
                "total_page": 1,
                "list": [dart_row()],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = DartConnector(
            credentials,
            client=client,
            governance_detail_codes=(),
            request_budget=PreblockedQuota(),
            quota_day_provider=lambda: date(2026, 7, 26),
        )
        rows = list(
            connector.iter_disclosure_rows(
                date(2026, 7, 26),
                date(2026, 7, 26),
            )
        )

    assert len(rows) == 1
    assert requested == [key_b]


def test_dart_connector_classifies_global_ledger_exhaustion_as_quota_stop() -> None:
    requests = 0

    class ExhaustedGlobalLedger:
        limit = 40_000
        used = 40_000

        def consume(self, *, operation: str, credential_id: str) -> object:
            assert operation == "list"
            assert re.fullmatch(r"[0-9a-f]{64}", credential_id)
            raise DartGlobalQuotaExceededError("global daily limit exhausted")

        def block_020(self, permit: object) -> None:
            raise AssertionError(f"no provider response exists for {permit!r}")

        def disable_901(self, permit: object) -> None:
            raise AssertionError(f"no provider response exists for {permit!r}")

    def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        raise AssertionError("provider request must not be sent")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = DartConnector(
            "a" * 40,
            client=client,
            governance_detail_codes=(),
            request_budget=ExhaustedGlobalLedger(),
            quota_day_provider=lambda: date(2026, 7, 26),
        )
        with pytest.raises(
            DartQuotaExceededError,
            match="global quota exhausted",
        ):
            list(
                connector.iter_disclosure_rows(
                    date(2026, 7, 26),
                    date(2026, 7, 26),
                )
            )

    assert requests == 0


@pytest.mark.parametrize("status", ("020", "901"))
def test_dart_company_master_rotates_on_bounded_xml_credential_status(
    status: str,
) -> None:
    key_a, key_b = "a" * 40, "b" * 40
    credentials = load_opendart_credentials(
        {"OPENDART_API_KEYS": f"{key_a},{key_b}"}
    )
    requested: list[str] = []
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(
            "CORPCODE.xml",
            (
                "<result><list><corp_code>00126380</corp_code>"
                "<corp_name>Issuer</corp_name><stock_code>005930</stock_code>"
                "<modify_date>20260726</modify_date></list></result>"
            ),
        )

    def handler(request: httpx.Request) -> httpx.Response:
        key = str(request.url.params["crtfc_key"])
        requested.append(key)
        if key == key_a:
            return httpx.Response(
                200,
                content=f"<result><status>{status}</status><message>{key_a}</message></result>",
            )
        return httpx.Response(200, content=buffer.getvalue())

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = DartConnector(
            credentials,
            client=client,
            quota_day_provider=lambda: date(2026, 7, 26),
        )
        companies = connector.fetch_company_master()

    assert companies[0]["company_id"] == "00126380"
    assert requested == [key_a, key_b]


def test_dart_transport_error_is_sanitized_without_request_url_or_key() -> None:
    secret = "a" * 40

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(f"failed {request.url}", request=request)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = DartConnector(
            secret,
            client=client,
            governance_detail_codes=(),
            max_retries=0,
        )
        with pytest.raises(OfficialSourceError) as captured:
            list(
                connector.iter_disclosure_rows(
                    date(2026, 7, 26),
                    date(2026, 7, 26),
                )
            )

    assert str(captured.value) == "OpenDART transport error"
    assert secret not in str(captured.value)
    assert captured.value.__cause__ is None


def test_dart_connector_enforces_shared_physical_request_budget() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503)

    budget = DartRequestBudget(1)
    sleeps: list[float] = []
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = DartConnector(
            "x" * 40,
            client=client,
            governance_detail_codes=(),
            request_budget=budget,
            max_retries=2,
            sleeper=sleeps.append,
        )
        with pytest.raises(DartRequestBudgetError, match="1/1"):
            list(connector.iter_disclosure_rows(date(2026, 7, 15), date(2026, 7, 16)))

    assert connector.requests_made == 1
    assert budget.used == 1


def test_invocation_quota_caps_durable_40k_ledger_without_losing_permit_actions() -> None:
    actions: list[tuple[str, object]] = []

    class DailyLedger:
        limit = 40_000
        used = 0

        def consume(self, *, operation: str, credential_id: str) -> object:
            self.used += 1
            permit = (operation, credential_id, self.used)
            actions.append(("consume", permit))
            return permit

        def block_020(self, permit: object) -> None:
            actions.append(("block_020", permit))

        def disable_901(self, permit: object) -> None:
            actions.append(("disable_901", permit))

    ledger = DailyLedger()
    budget = DartInvocationQuota(ledger, limit=2)
    credential_id = "d" * 64
    first = budget.consume(operation="list", credential_id=credential_id)
    second = budget.consume(operation="corp_code", credential_id=credential_id)
    budget.block_020(first)
    budget.disable_901(second)

    with pytest.raises(DartRequestBudgetError, match=r"2/2"):
        budget.consume(operation="list", credential_id=credential_id)

    assert budget.limit == 2
    assert budget.used == 2
    assert ledger.used == 2
    assert actions == [
        ("consume", ("list", credential_id, 1)),
        ("consume", ("corp_code", credential_id, 2)),
        ("block_020", first),
        ("disable_901", second),
    ]


def test_owned_invocation_quota_retries_failed_delegate_close_but_stays_closed() -> None:
    class FlakyLedger:
        limit = 40_000
        used = 0

        def __init__(self) -> None:
            self.close_attempts = 0

        def consume(self, *, operation: str, credential_id: str) -> object:
            self.used += 1
            return operation, credential_id

        def block_020(self, permit: object) -> None:
            del permit

        def disable_901(self, permit: object) -> None:
            del permit

        def close(self) -> None:
            self.close_attempts += 1
            if self.close_attempts == 1:
                raise RuntimeError("delegate close failed")

    ledger = FlakyLedger()
    budget = DartInvocationQuota(
        ledger,
        limit=2,
        close_delegate=True,
    )

    with pytest.raises(RuntimeError, match="delegate close failed"):
        budget.close()
    with pytest.raises(DartRequestBudgetError, match="closed"):
        budget.consume(operation="list", credential_id="d" * 64)

    budget.close()
    budget.close()
    assert ledger.close_attempts == 2
    assert ledger.used == 0


def test_dart_http_error_never_exposes_api_key_or_response_body() -> None:
    secret = "dart-secret-key-that-must-not-leak"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.params["crtfc_key"] == secret
        return httpx.Response(401, json={"message": secret})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = DartConnector(
            secret,
            client=client,
            governance_detail_codes=(),
            max_retries=0,
        )
        with pytest.raises(OfficialSourceError) as captured:
            list(connector.iter_disclosure_rows(date(2026, 7, 15), date(2026, 7, 15)))

    rendered = str(captured.value)
    assert rendered == "OpenDART HTTP 401"
    assert secret not in rendered
    assert "crtfc_key" not in rendered


def test_dart_and_kind_success_http_status_errors_do_not_echo_hostile_body() -> None:
    secret = "provider-secret-in-hostile-body"
    with pytest.raises(OfficialSourceError) as dart_error:
        parse_dart_list_payload({"status": secret, "message": secret})
    with pytest.raises(OfficialSourceError) as kind_error:
        parse_kind_list_payload({"status": secret, "items": []})

    assert str(dart_error.value) == "OpenDART list returned non-success status invalid"
    assert secret not in str(dart_error.value)
    assert secret not in str(kind_error.value)


def test_dart_validated_error_status_never_echoes_hostile_message() -> None:
    secret = "provider-secret-in-hostile-message"

    with pytest.raises(OfficialSourceError) as captured:
        parse_dart_list_payload({"status": "010", "message": secret})

    rendered = str(captured.value)
    assert rendered == "OpenDART list returned non-success status 010"
    assert secret not in rendered


def test_kind_http_error_never_exposes_authorization_token_or_response_body() -> None:
    secret = "kind-secret-token-that-must-not-leak"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["Authorization"] == f"Bearer {secret}"
        return httpx.Response(403, json={"error": secret})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = KindConnector(
            "https://kind.example.test/disclosures",
            api_key=secret,
            client=client,
        )
        with pytest.raises(OfficialSourceError) as captured:
            list(connector.iter_disclosure_rows(date(2026, 7, 15), date(2026, 7, 15)))

    rendered = str(captured.value)
    assert rendered == "KIND HTTP 403"
    assert secret not in rendered


def test_dart_connector_annotates_detail_queries_and_deduplicates_broad_results() -> None:
    requested_details: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        detail = str(request.url.params.get("pblntf_detail_ty") or "")
        requested_details.append(detail)
        if detail:
            rows = [dart_row(report_nm="공시보고서", rcept_no="20260716000123")]
        else:
            rows = [
                dart_row(report_nm="공시보고서", rcept_no="20260716000123"),
                dart_row(report_nm="주요사항보고서(회사분할결정)", rcept_no="20260716000124"),
            ]
        return httpx.Response(
            200,
            json={"status": "000", "page_no": 1, "total_page": 1, "list": rows},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        rows = list(
            DartConnector(
                "x" * 40,
                client=client,
                governance_detail_codes=("D001",),
            ).iter_disclosure_rows(date(2026, 7, 15), date(2026, 7, 16))
        )
    assert requested_details == ["D001", ""]
    assert [row["rcept_no"] for row in rows] == ["20260716000123", "20260716000124"]
    assert rows[0]["pblntf_ty"] == "D"
    assert rows[0]["pblntf_detail_ty"] == "D001"
    parsed = [parse_dart_disclosure(row) for row in rows]
    assert parsed[0] is not None and parsed[0].event_type == GovernanceEventType.FIVE_PERCENT_HOLDING
    assert parsed[1] is not None and parsed[1].event_type == GovernanceEventType.SPLIT


def test_dart_connector_uses_all_governance_detail_filters_by_default() -> None:
    requested_details: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_details.append(str(request.url.params.get("pblntf_detail_ty") or ""))
        return httpx.Response(200, json={"status": "013", "message": "조회된 데이터가 없습니다."})

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        rows = list(
            DartConnector("x" * 40, client=client).iter_disclosure_rows(
                date(2026, 7, 15), date(2026, 7, 16)
            )
        )

    assert rows == []
    assert requested_details == [*DART_GOVERNANCE_DETAIL_CODES, ""]


def test_dart_filtered_detail_query_truncation_fails_closed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page_no"])
        return httpx.Response(
            200,
            json={
                "status": "000",
                "page_no": page,
                "total_page": 3,
                "list": [dart_row(report_nm="공시보고서", rcept_no=f"20260716{page:06d}")],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(OfficialSourceError, match="detail D001 result truncated"):
            list(
                DartConnector(
                    "x" * 40,
                    client=client,
                    governance_detail_codes=("D001",),
                ).iter_disclosure_rows(
                    date(2026, 7, 15),
                    date(2026, 7, 16),
                    max_pages=2,
                )
            )


def test_official_connectors_fail_closed_when_page_limit_truncates_results() -> None:
    def dart_handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page_no"])
        return httpx.Response(
            200,
            json={
                "status": "000",
                "page_no": page,
                "total_page": 101,
                "list": [dart_row(rcept_no=f"20260716{page:06d}")],
            },
        )

    def kind_handler(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page"])
        return httpx.Response(
            200,
            json={
                "page": page,
                "total_pages": 101,
                "items": [{"id": page}],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(dart_handler)) as client:
        with pytest.raises(OfficialSourceError, match="truncated"):
            list(
                DartConnector("x" * 40, client=client, governance_detail_codes=()).iter_disclosure_rows(
                    date(2026, 7, 15), date(2026, 7, 16), max_pages=2
                )
            )
    with httpx.Client(transport=httpx.MockTransport(kind_handler)) as client:
        with pytest.raises(OfficialSourceError, match="truncated"):
            list(
                KindConnector("https://kind.example/api", client=client).iter_disclosure_rows(
                    date(2026, 7, 15), date(2026, 7, 16), max_pages=2
                )
            )


def test_kind_connector_sends_adapter_key_only_in_authorization_header() -> None:
    observed_params: dict[str, str] = {}
    observed_authorization = ""

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal observed_authorization
        observed_params.update(dict(request.url.params))
        observed_authorization = request.headers.get("Authorization", "")
        return httpx.Response(
            200,
            json={"page": 1, "total_pages": 1, "items": [{"id": 1}]},
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        rows = list(
            KindConnector(
                "https://kind.example/api",
                api_key="adapter-secret",
                client=client,
            ).iter_disclosure_rows(date(2026, 7, 15), date(2026, 7, 16))
        )

    assert rows == [{"id": 1}]
    assert "api_key" not in observed_params
    assert observed_authorization == "Bearer adapter-secret"


def test_official_connectors_reject_page_drift_empty_success_pages_and_count_mismatch() -> None:
    def dart_page_drift(request: httpx.Request) -> httpx.Response:
        requested = int(request.url.params["page_no"])
        return httpx.Response(
            200,
            json={
                "status": "000",
                "page_no": 1 if requested == 2 else requested,
                "total_page": 2,
                "list": [dart_row(rcept_no=f"2026071600012{requested}")],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(dart_page_drift)) as client:
        with pytest.raises(OfficialSourceError, match="requested page 2 but received page 1"):
            list(
                DartConnector("x" * 40, client=client, governance_detail_codes=()).iter_disclosure_rows(
                    date(2026, 7, 15), date(2026, 7, 16)
                )
            )

    def dart_empty_page(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page_no"])
        return httpx.Response(
            200,
            json={
                "status": "000",
                "page_no": page,
                "total_page": 2,
                "list": [] if page == 2 else [dart_row()],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(dart_empty_page)) as client:
        with pytest.raises(OfficialSourceError, match="empty page 2"):
            list(
                DartConnector("x" * 40, client=client, governance_detail_codes=()).iter_disclosure_rows(
                    date(2026, 7, 15), date(2026, 7, 16)
                )
            )

    def dart_no_data_after_first_page(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page_no"])
        if page == 2:
            return httpx.Response(200, json={"status": "013", "message": "no data"})
        return httpx.Response(
            200,
            json={"status": "000", "page_no": 1, "total_page": 2, "list": [dart_row()]},
        )

    with httpx.Client(transport=httpx.MockTransport(dart_no_data_after_first_page)) as client:
        with pytest.raises(OfficialSourceError, match="no-data status.*page 2"):
            list(
                DartConnector("x" * 40, client=client, governance_detail_codes=()).iter_disclosure_rows(
                    date(2026, 7, 15), date(2026, 7, 16)
                )
            )

    def dart_count_mismatch(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "000",
                "page_no": 1,
                "total_page": 1,
                "total_count": 2,
                "list": [dart_row()],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(dart_count_mismatch)) as client:
        with pytest.raises(OfficialSourceError, match="reported 2"):
            list(
                DartConnector("x" * 40, client=client, governance_detail_codes=()).iter_disclosure_rows(
                    date(2026, 7, 15), date(2026, 7, 16)
                )
            )

    def kind_empty_page(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page"])
        return httpx.Response(
            200,
            json={"page": page, "total_pages": 2, "items": [] if page == 2 else [{"id": 1}]},
        )

    with httpx.Client(transport=httpx.MockTransport(kind_empty_page)) as client:
        with pytest.raises(OfficialSourceError, match="empty page 2"):
            list(
                KindConnector("https://kind.example/api", client=client).iter_disclosure_rows(
                    date(2026, 7, 15), date(2026, 7, 16)
                )
            )

    def kind_no_data_after_first_page(request: httpx.Request) -> httpx.Response:
        page = int(request.url.params["page"])
        if page == 2:
            return httpx.Response(200, json={"status": "no_data", "items": []})
        return httpx.Response(
            200,
            json={"page": 1, "total_pages": 2, "items": [{"id": 1}]},
        )

    with httpx.Client(transport=httpx.MockTransport(kind_no_data_after_first_page)) as client:
        with pytest.raises(OfficialSourceError, match="no-data status.*page 2"):
            list(
                KindConnector("https://kind.example/api", client=client).iter_disclosure_rows(
                    date(2026, 7, 15), date(2026, 7, 16)
                )
            )

    def kind_count_mismatch(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "page": 1,
                "total_pages": 1,
                "total_count": 2,
                "items": [{"id": 1}],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(kind_count_mismatch)) as client:
        with pytest.raises(OfficialSourceError, match="reported 2"):
            list(
                KindConnector("https://kind.example/api", client=client).iter_disclosure_rows(
                    date(2026, 7, 15), date(2026, 7, 16)
                )
            )


def test_company_master_zip_and_collector_never_manages_source_rights(
    config: dict[str, object],
) -> None:
    xml = b"""<?xml version='1.0' encoding='UTF-8'?><result><list><corp_code>00126380</corp_code><corp_name>Samsung</corp_name><stock_code>005930</stock_code><modify_date>20260716</modify_date></list></result>"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("CORPCODE.xml", xml)
    companies = parse_corp_code_zip(buffer.getvalue())
    assert companies[0]["company_id"] == "00126380"
    assert companies[0]["listing_status"] == "listed"
    assert companies[0]["master_modified_at"] == "2026-07-16T00:00:00+00:00"
    rights = source_right_payloads(config, include_kind=True)
    assert rights == []


def test_company_master_marks_missing_stock_code_unlisted_and_rejects_bad_modify_date() -> None:
    xml = b"""<?xml version='1.0' encoding='UTF-8'?><result><list><corp_code>00126380</corp_code><corp_name>Private</corp_name><stock_code></stock_code><modify_date>20260716</modify_date></list></result>"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("CORPCODE.xml", xml)
    companies = parse_corp_code_zip(buffer.getvalue())
    assert companies[0]["stock_code"] == ""
    assert companies[0]["listing_status"] == "unlisted"

    invalid = xml.replace(b"20260716", b"20260231")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("CORPCODE.xml", invalid)
    with pytest.raises(OfficialSourceError, match="invalid modify_date"):
        parse_corp_code_zip(buffer.getvalue())
