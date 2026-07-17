from __future__ import annotations

import io
import zipfile
from datetime import date, datetime, timezone

import httpx
import pytest

from curator.governance import GovernanceEventType
from curator.official_ingest import source_right_payloads
from curator.official_sources import (
    DART_GOVERNANCE_DETAIL_CODES,
    DartConnector,
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
    original = parse_dart_disclosure(dart_row())
    correction = parse_dart_disclosure(
        dart_row(
            report_nm="[정정]주요사항보고서(자기주식취득결정)",
            rcept_no="20260717000124",
            rcept_dt="20260717",
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
        dart_row(report_nm="주식등의대량보유상황보고서", rcept_no="20260716000121")
    )
    original_b = parse_dart_disclosure(
        dart_row(report_nm="주식등의대량보유상황보고서", rcept_no="20260717000122", rcept_dt="20260717")
    )
    correction = parse_dart_disclosure(
        dart_row(
            report_nm="[정정]주식등의대량보유상황보고서",
            rcept_no="20260718000123",
            rcept_dt="20260718",
        )
    )
    assert original_a is not None and original_b is not None and correction is not None

    linked = link_correction_versions([correction, original_b, original_a])
    assert [correction_of for _, correction_of, _, _ in linked] == [None, None, None]
    assert [version_no for _, _, version_no, _ in linked] == [1, 1, 1]
    assert len({event_id for _, _, _, event_id in linked}) == 3


def test_correction_linking_uses_filer_to_disambiguate_repeated_reports() -> None:
    original_alpha = parse_dart_disclosure(
        dart_row(
            report_nm="주식등의대량보유상황보고서",
            flr_nm="Alpha Fund",
            rcept_no="20260716000121",
        )
    )
    original_beta = parse_dart_disclosure(
        dart_row(
            report_nm="주식등의대량보유상황보고서",
            flr_nm="Beta Fund",
            rcept_no="20260716000122",
        )
    )
    correction_alpha = parse_dart_disclosure(
        dart_row(
            report_nm="[정정]주식등의대량보유상황보고서",
            flr_nm="Alpha Fund",
            rcept_no="20260717000123",
            rcept_dt="20260717",
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
        )
    )
    withdrawn = parse_dart_disclosure(
        dart_row(
            report_nm="[철회]주요사항보고서(유상증자결정)",
            rcept_no="20260717000122",
            rcept_dt="20260717",
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


def test_company_master_zip_and_source_right_payload_contract(config: dict[str, object]) -> None:
    xml = b"""<?xml version='1.0' encoding='UTF-8'?><result><list><corp_code>00126380</corp_code><corp_name>Samsung</corp_name><stock_code>005930</stock_code><modify_date>20260716</modify_date></list></result>"""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("CORPCODE.xml", xml)
    companies = parse_corp_code_zip(buffer.getvalue())
    assert companies[0]["company_id"] == "00126380"
    rights = source_right_payloads(config, include_kind=True)
    assert {row["source_right_id"] for row in rights} == {"official:dart", "official:kind"}
    assert all(row.get("evidence_uri") for row in rights)
