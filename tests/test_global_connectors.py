from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from curator.global_connectors import (
    AUSTRALIA_ASIC_DESCRIPTOR,
    CANADA_IR_DESCRIPTOR,
    CompaniesHouseFilingHistoryConnector,
    EdinetDocumentsConnector,
    GlobalConnectorContractError,
    GlobalConnectorError,
    GlobalConnectorPaginationError,
    GlobalConnectorRequest,
    GlobalSourceRightDenied,
    IssuerReference,
    SecCurrentFilingsConnector,
    SecDailyIndexConnector,
    SecHybridConnector,
    SecSubmissionsConnector,
    _sec_current_cutoff,
    global_document_content_hash,
)
from curator.global_market import (
    CoverageMode,
    GLOBAL_EVENT_FAMILIES,
    GLOBAL_JURISDICTIONS,
    Issuer,
    SourceCoverage,
    global_issuer_id,
)
from curator.official_source_rights import OfficialSourceRightEligibility


NOW = datetime(2026, 7, 24, 7, 30, tzinfo=timezone.utc)
RIGHTS_REVISION = "a" * 64
ROOT = Path(__file__).resolve().parents[1]


def eligibility(
    source_right_id: str,
    source_key: str,
    *,
    source_type: str = "official_disclosure",
    redistribute: bool = True,
) -> OfficialSourceRightEligibility:
    return OfficialSourceRightEligibility(
        source_right_id=source_right_id,
        use="collect",
        rights_revision=RIGHTS_REVISION,
        checked_at="2026-07-24T07:29:59+00:00",
        source_type=source_type,
        source_key=source_key,
        redistribution_allowed=redistribute,
        ai_allowed=False,
    )


def test_global_market_contract_has_six_explicit_jurisdictions() -> None:
    assert [item.country_code for item in GLOBAL_JURISDICTIONS] == [
        "KR",
        "US",
        "JP",
        "GB",
        "CA",
        "AU",
    ]
    assert len(GLOBAL_EVENT_FAMILIES) == 8
    issuer_id = global_issuer_id("US", "US:CIK", "0000320193")
    assert issuer_id == global_issuer_id("US", "US:CIK", "0000320193")
    assert issuer_id.startswith("issuer:us:")
    Issuer(
        issuer_id=issuer_id,
        country_code="US",
        legal_name="Example, Inc.",
        original_language="en",
    )


def test_official_connectors_never_follow_credentialed_redirects() -> None:
    source = (ROOT / "curator" / "global_connectors.py").read_text(encoding="utf-8")
    assert "follow_redirects=True" not in source
    assert source.count("follow_redirects=False") >= 8


def test_selected_issuer_coverage_fails_closed_without_scope() -> None:
    with pytest.raises(ValueError, match="issuer scope"):
        SourceCoverage(
            coverage_id="coverage:test",
            connector_id="connector:ca:issuer-ir",
            country_code="CA",
            coverage_mode=CoverageMode.SELECTED_ISSUERS,
            public_note="Selected issuers only",
        )


def test_sec_connector_preserves_source_title_and_is_idempotent() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["user-agent"] == "BSIDE test ops@example.com"
        assert request.url.scheme == "https"
        assert request.url.host == "data.sec.gov"
        assert request.url.path == "/submissions/CIK0000320193.json"
        assert not request.url.query
        assert "authorization" not in request.headers
        assert "x-api-key" not in request.headers
        return httpx.Response(
            200,
            json={
                "cik": "320193",
                "name": "Apple Inc.",
                "filings": {
                    "recent": {
                        "accessionNumber": [
                            "0000320193-26-000123",
                            "0000320193-26-000122",
                        ],
                        "filingDate": ["2026-07-24", "2026-07-23"],
                        "acceptanceDateTime": ["20260724153000", "20260723120000"],
                        "form": ["8-K", "10-Q"],
                        "primaryDocument": ["aapl-20260724.htm", "aapl-20260723.htm"],
                        "primaryDocDescription": [
                            "Current report",
                            "Quarterly report",
                        ],
                        "items": ["5.02", ""],
                    }
                },
            },
        )

    request = GlobalConnectorRequest(
        window_start=date(2026, 7, 24),
        window_end_exclusive=date(2026, 7, 25),
        issuers=(
            IssuerReference(
                namespace="US:CIK",
                identifier_type="CIK",
                value="320193",
                legal_name="Apple Inc.",
                market="NASDAQ",
                ticker="AAPL",
            ),
        ),
    )
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = SecSubmissionsConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        )
        first = connector.fetch(
            request,
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            now=NOW,
        )
        second = connector.fetch(
            request,
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            now=NOW,
        )

    assert first == second
    assert first.raw_count == 2
    assert len(first.records) == 1
    record = first.records[0]
    assert record.external_id == (
        "sec-accession-cik-v1:0000320193-26-000123:0000320193"
    )
    assert record.title == "Current report"
    assert record.metadata["accession_number"] == "0000320193-26-000123"
    assert record.metadata["title_provenance"] == "source"
    assert record.event_family == "board_and_compensation"
    assert record.original_language == "en"
    assert record.original_url.startswith("https://www.sec.gov/Archives/edgar/data/")
    assert record.content_hash == global_document_content_hash(
        record,
        source_type="official_disclosure",
        public_allowed=True,
        ai_allowed=False,
    )
    semantic_variants = (
        replace(record, original_url="https://www.sec.gov/Archives/other.htm"),
        replace(record, document_type="DEF 14A"),
        replace(record, event_family="meeting_and_vote"),
        replace(record, original_language="ko"),
        replace(record, change_type="corrected"),
        replace(record, body_text="Source-preserved body"),
        replace(record, metadata={**record.metadata, "semantic_revision": 2}),
        replace(
            record,
            issuer_reference=replace(
                record.issuer_reference,
                ticker="AAPL2",
            ),
        ),
    )
    assert all(
        global_document_content_hash(
            variant,
            source_type="official_disclosure",
            public_allowed=True,
            ai_allowed=False,
        )
        != record.content_hash
        for variant in semantic_variants
    )
    assert global_document_content_hash(
        record,
        source_type="official_disclosure",
        public_allowed=False,
        ai_allowed=False,
    ) != record.content_hash
    assert global_document_content_hash(
        record,
        source_type="official_disclosure",
        public_allowed=True,
        ai_allowed=True,
    ) != record.content_hash
    assert global_document_content_hash(
        record,
        source_type="official_register",
        public_allowed=True,
        ai_allowed=False,
    ) != record.content_hash
    nested_empty_object = replace(
        record,
        metadata={"nested": {}},
    )
    nested_empty_list = replace(
        record,
        metadata={"nested": []},
    )
    assert global_document_content_hash(
        nested_empty_object,
        source_type="official_disclosure",
        public_allowed=True,
        ai_allowed=False,
    ) == global_document_content_hash(
        nested_empty_list,
        source_type="official_disclosure",
        public_allowed=True,
        ai_allowed=False,
    )
    assert nested_empty_object.public_payload(
        allow_body=True
    )["metadata"] == {"nested": []}
    with pytest.raises(
        GlobalConnectorContractError,
        match="floats are not cross-runtime canonical",
    ):
        global_document_content_hash(
            replace(record, metadata={"not_canonical": 1.25}),
            source_type="official_disclosure",
            public_allowed=True,
            ai_allowed=False,
        )


@pytest.mark.parametrize(
    "user_agent",
    (
        "",
        "ops@example.com",
        "BSIDE",
        "BSIDE ops@example",
        "BSIDE ops@example.com\rInjected: value",
        "BSIDE ops@example.com\x7f",
    ),
)
@pytest.mark.parametrize(
    "connector_class",
    (
        SecDailyIndexConnector,
        SecCurrentFilingsConnector,
        SecSubmissionsConnector,
    ),
)
def test_sec_connectors_require_declared_service_and_contact_email(
    connector_class: type[
        SecDailyIndexConnector
        | SecCurrentFilingsConnector
        | SecSubmissionsConnector
    ],
    user_agent: str,
) -> None:
    with pytest.raises(
        ValueError,
        match="service and contact email",
    ):
        connector_class(user_agent=user_agent)


def test_sec_daily_index_connector_is_market_wide_and_filters_forms() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.scheme == "https"
        assert request.url.host == "www.sec.gov"
        assert request.url.path.endswith("/2026/QTR3/master.20260724.idx")
        assert not request.url.query
        assert request.headers["user-agent"] == "BSIDE test ops@example.com"
        assert "authorization" not in request.headers
        assert "x-api-key" not in request.headers
        return httpx.Response(
            200,
            text=(
                "Description\n"
                "CIK|Company Name|Form Type|Date Filed|Filename\n"
                "--------------------------------------------------------------------------------\n"
                "320193|Apple Inc.|SC 13D|2026-07-24|"
                "edgar/data/320193/0000320193-26-000999.txt\n"
                "320193|Apple Inc.|10-Q|2026-07-24|"
                "edgar/data/320193/0000320193-26-000998.txt\n"
            ),
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = SecDailyIndexConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 7, 24),
                window_end_exclusive=date(2026, 7, 25),
            ),
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            now=NOW,
        )
    assert result.coverage_mode is CoverageMode.MARKET_WIDE
    assert result.request_count == 1
    assert result.raw_count == 2
    assert [record.document_type for record in result.records] == ["SC 13D"]
    assert result.records[0].issuer_id == "issuer:us:cik:0000320193"
    assert result.records[0].external_id == (
        "sec-accession-cik-v1:0000320193-26-000999:0000320193"
    )
    assert result.records[0].metadata["accession_number"] == (
        "0000320193-26-000999"
    )
    assert result.records[0].metadata["title_provenance"] == "generated_metadata"


def test_sec_daily_accepts_current_official_header_and_compact_date() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/2026/QTR2/master.20260629.idx")
        return httpx.Response(
            200,
            text=(
                "Description: Daily Index of EDGAR Dissemination Feed\n"
                "CIK|Company Name|Form Type|Date Filed|File Name\n"
                "--------------------------------------------------------------------------------\n"
                "320193|Apple Inc.|SC 13D|20260629|"
                "edgar/data/320193/0000320193-26-000999.txt\n"
            ),
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = SecDailyIndexConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 6, 29),
                window_end_exclusive=date(2026, 6, 30),
            ),
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            now=NOW,
        )

    assert result.raw_count == 1
    assert len(result.records) == 1
    assert result.records[0].filed_at == "2026-06-29T04:00:00+00:00"
    assert result.records[0].external_id == (
        "sec-accession-cik-v1:0000320193-26-000999:0000320193"
    )


def test_sec_daily_preserves_late_added_historical_filing_date() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/2026/QTR2/master.20260630.idx")
        return httpx.Response(
            200,
            text=(
                "Description: Daily Index of EDGAR Dissemination Feed\n"
                "CIK|Company Name|Form Type|Date Filed|File Name\n"
                "--------------------------------------------------------------------------------\n"
                "1289868|Historical Issuer|DEF 14A|20230922|"
                "edgar/data/1289868/0001289868-23-000011.txt\n"
            ),
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = SecDailyIndexConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 6, 30),
                window_end_exclusive=date(2026, 7, 1),
            ),
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            now=NOW,
        )

    assert len(result.records) == 1
    assert result.records[0].external_id == (
        "sec-accession-cik-v1:0001289868-23-000011:0001289868"
    )
    assert result.records[0].filed_at == "2023-09-22T04:00:00+00:00"


def test_sec_daily_scopes_shared_accession_by_cik_and_preserves_provenance() -> None:
    accession = "0001104659-26-086735"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/2026/QTR2/master.20260629.idx")
        return httpx.Response(
            200,
            text=(
                "Description: Daily Index of EDGAR Dissemination Feed\n"
                "CIK|Company Name|Form Type|Date Filed|File Name\n"
                "--------------------------------------------------------------------------------\n"
                "1009268|D. E. SHAW & CO., L.P.|SC 13D/A|20260629|"
                f"edgar/data/1104659/{accession}.txt\n"
                "1728117|Gossamer Bio, Inc.|SC 13D/A|20260629|"
                f"edgar/data/1104659/{accession}.txt\n"
            ),
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = SecDailyIndexConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 6, 29),
                window_end_exclusive=date(2026, 6, 30),
            ),
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            now=NOW,
        )

    assert result.raw_count == 2
    assert len(result.records) == 2
    by_cik = {record.metadata["cik"]: record for record in result.records}
    assert set(by_cik) == {"0001009268", "0001728117"}
    assert by_cik["0001009268"].external_id == (
        f"sec-accession-cik-v1:{accession}:0001009268"
    )
    assert by_cik["0001728117"].external_id == (
        f"sec-accession-cik-v1:{accession}:0001728117"
    )
    assert len({record.external_id for record in result.records}) == 2
    assert len({record.record_id for record in result.records}) == 2
    assert {
        record.metadata["accession_number"] for record in result.records
    } == {accession}


def test_sec_daily_deduplicates_reprocessed_identity_across_index_days() -> None:
    accession = "0001104659-26-086735"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith(
            ("/master.20260629.idx", "/master.20260630.idx")
        )
        return httpx.Response(
            200,
            text=(
                "Description: Daily Index of EDGAR Dissemination Feed\n"
                "CIK|Company Name|Form Type|Date Filed|File Name\n"
                "--------------------------------------------------------------------------------\n"
                "1728117|Gossamer Bio, Inc.|SC 13D/A|20260629|"
                f"edgar/data/1104659/{accession}.txt\n"
            ),
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = SecDailyIndexConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 6, 29),
                window_end_exclusive=date(2026, 7, 1),
                max_pages=2,
            ),
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            now=NOW,
        )

    assert result.request_count == 2
    assert result.raw_count == 2
    assert len(result.records) == 1
    assert result.records[0].external_id == (
        f"sec-accession-cik-v1:{accession}:0001728117"
    )
    assert result.records[0].metadata["accession_number"] == accession


def test_sec_daily_rejects_future_filing_date() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text=(
                "CIK|Company Name|Form Type|Date Filed|File Name\n"
                "--------------------------------------------------------------------------------\n"
                "320193|Apple Inc.|SC 13D|20260701|"
                "edgar/data/320193/0000320193-26-000999.txt\n"
            ),
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = SecDailyIndexConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        )
        with pytest.raises(
            GlobalConnectorContractError,
            match="date exceeds requested index",
        ):
            connector.fetch(
                GlobalConnectorRequest(
                    window_start=date(2026, 6, 30),
                    window_end_exclusive=date(2026, 7, 1),
                ),
                eligibility=eligibility(
                    "official:sec-edgar",
                    "sec-edgar",
                ),
                now=NOW,
            )


def test_sec_daily_treats_weekend_access_denied_as_missing_index() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/2026/QTR2/master.20260628.idx"):
            return httpx.Response(403)
        assert request.url.path.endswith("/2026/QTR2/master.20260629.idx")
        return httpx.Response(
            200,
            text=(
                "Description\n"
                "CIK|Company Name|Form Type|Date Filed|Filename\n"
                "--------------------------------------------------------------------------------\n"
                "320193|Apple Inc.|SC 13D|2026-06-29|"
                "edgar/data/320193/0000320193-26-000999.txt\n"
            ),
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = SecDailyIndexConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 6, 28),
                window_end_exclusive=date(2026, 6, 30),
            ),
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            now=NOW,
        )

    assert result.request_count == 2
    assert result.raw_count == 1
    assert len(result.records) == 1
    assert result.records[0].external_id == (
        "sec-accession-cik-v1:0000320193-26-000999:0000320193"
    )


def test_sec_daily_keeps_weekday_access_denied_fail_closed() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/2026/QTR2/master.20260629.idx")
        return httpx.Response(403)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = SecDailyIndexConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        )
        with pytest.raises(GlobalConnectorError, match="HTTP 403"):
            connector.fetch(
                GlobalConnectorRequest(
                    window_start=date(2026, 6, 29),
                    window_end_exclusive=date(2026, 6, 30),
                ),
                eligibility=eligibility(
                    "official:sec-edgar",
                    "sec-edgar",
                ),
                now=NOW,
            )


def test_sec_daily_retries_429_with_retry_after_and_counts_attempts() -> None:
    calls = 0
    rights_checks = 0
    retry_delays: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "3"})
        return httpx.Response(
            200,
            text=(
                "CIK|Company Name|Form Type|Date Filed|File Name\n"
                "--------------------------------------------------------------------------------\n"
                "320193|Apple Inc.|SC 13D|20260629|"
                "edgar/data/320193/0000320193-26-000999.txt\n"
            ),
        )

    def recheck() -> OfficialSourceRightEligibility:
        nonlocal rights_checks
        rights_checks += 1
        return replace(
            eligibility("official:sec-edgar", "sec-edgar"),
            checked_at=datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = SecDailyIndexConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
            max_retries=3,
            sleep=lambda _delay: None,
            retry_sleep=retry_delays.append,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 6, 29),
                window_end_exclusive=date(2026, 6, 30),
            ),
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            eligibility_provider=recheck,
            now=NOW,
        )

    assert calls == rights_checks == result.request_count == 2
    assert retry_delays == [3.0]
    assert result.raw_count == 1
    assert len(result.records) == 1


def test_sec_daily_5xx_retries_are_bounded_and_structured() -> None:
    calls = 0
    retry_delays: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = SecDailyIndexConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
            max_retries=2,
            sleep=lambda _delay: None,
            retry_sleep=retry_delays.append,
        )
        with pytest.raises(
            GlobalConnectorError,
            match="failed after retries",
        ) as raised:
            connector.fetch(
                GlobalConnectorRequest(
                    window_start=date(2026, 6, 29),
                    window_end_exclusive=date(2026, 6, 30),
                ),
                eligibility=eligibility(
                    "official:sec-edgar",
                    "sec-edgar",
                ),
                now=NOW,
            )

    assert calls == 3
    assert retry_delays == [1.0, 2.0]
    assert raised.value.http_status == 503


def test_sec_daily_preserves_8k_as_private_unclassified_candidate() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text=(
                "Description\n"
                "CIK|Company Name|Form Type|Date Filed|Filename\n"
                "--------------------------------------------------------------------------------\n"
                "320193|Apple Inc.|8-K|2026-07-24|"
                "edgar/data/320193/0000320193-26-000997.txt\n"
            ),
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = SecDailyIndexConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 7, 24),
                window_end_exclusive=date(2026, 7, 25),
            ),
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            now=NOW,
        )
    assert len(result.records) == 1
    assert result.records[0].document_type == "8-K"
    assert result.records[0].event_family == "unclassified"


def _sec_current_atom(
    *entries: str,
    updated: str = "2026-07-24T03:05:00-04:00",
) -> str:
    return (
        '<?xml version="1.0" encoding="ISO-8859-1" ?>'
        '<feed xmlns="http://www.w3.org/2005/Atom">'
        "<title>Latest Filings</title>"
        f"<updated>{updated}</updated>"
        + "".join(entries)
        + "</feed>"
    )


def _sec_current_entry(
    *,
    accession: str,
    title: str,
    form: str,
    cik: str,
    accepted: str = "2026-07-24T03:00:00-04:00",
) -> str:
    compact = accession.replace("-", "")
    return (
        "<entry>"
        f"<title>{title}</title>"
        '<link rel="alternate" type="text/html" '
        f'href="https://www.sec.gov/Archives/edgar/data/{int(cik)}/'
        f'{compact}/{accession}-index.htm"/>'
        f"<updated>{accepted}</updated>"
        f'<category scheme="https://www.sec.gov/" term="{form}"/>'
        f"<id>urn:tag:sec.gov,2008:accession-number={accession}</id>"
        "</entry>"
    )


def test_sec_current_atom_is_cursor_driven_exact_and_preserves_source_fields() -> None:
    accession = "0001104659-26-086735"
    atom = _sec_current_atom(
        _sec_current_entry(
            accession=accession,
            title=(
                "SCHEDULE 13D/A - Gossamer Bio, Inc. "
                "(0001728117) (Subject)"
            ),
            form="SCHEDULE 13D/A",
            cik="0001728117",
        ),
        _sec_current_entry(
            accession=accession,
            title=(
                "SCHEDULE 13D/A - D. E. SHAW &amp; CO, L.P. "
                "(0001009268) (Filed by)"
            ),
            form="SCHEDULE 13D/A",
            cik="0001009268",
        ),
        _sec_current_entry(
            accession="0000320193-26-000111",
            title="10-Q - Apple Inc. (0000320193) (Filer)",
            form="10-Q",
            cik="0000320193",
        ),
        _sec_current_entry(
            accession="0000320193-26-000110",
            title="10-Q - Apple Inc. (0000320193) (Filer)",
            form="10-Q",
            cik="0000320193",
            accepted="2026-07-23T21:00:00-04:00",
        ),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "www.sec.gov"
        assert request.url.path == "/cgi-bin/browse-edgar"
        assert request.url.params["action"] == "getcurrent"
        assert request.url.params["output"] == "atom"
        assert request.url.params["count"] == "100"
        assert set(request.url.params) == {
            "action",
            "company",
            "count",
            "dateb",
            "output",
            "owner",
            "start",
            "type",
        }
        assert request.headers["user-agent"] == "BSIDE test ops@example.com"
        assert "authorization" not in request.headers
        assert "x-api-key" not in request.headers
        return httpx.Response(200, content=atom.encode("iso-8859-1"))

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = SecCurrentFilingsConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        )
        first = connector.fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 7, 23),
                window_end_exclusive=date(2026, 7, 24),
                max_pages=2,
            ),
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            now=NOW,
        )
        replay = connector.fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 7, 23),
                window_end_exclusive=date(2026, 7, 24),
                cursor=first.next_cursor,
                max_pages=2,
            ),
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            now=NOW,
        )

    assert first.raw_count == 4
    assert first.request_count == 1
    assert len(first.records) == 1
    assert first.next_cursor and first.next_cursor.startswith("sec-current-v1:")
    record = first.records[0]
    assert record.external_id == (
        f"sec-accession-cik-v1:{accession}:0001728117"
    )
    assert record.document_type == "SC 13D/A"
    assert record.title == (
        "SCHEDULE 13D/A - Gossamer Bio, Inc. (0001728117) (Subject)"
    )
    assert record.original_language == "en"
    assert record.original_url.startswith("https://www.sec.gov/Archives/")
    assert record.metadata["accession_number"] == accession
    assert record.metadata["discovery"] == "current-filings-atom"
    assert replay.next_cursor == first.next_cursor
    assert [item.content_hash for item in replay.records] == [
        item.content_hash for item in first.records
    ]


def test_sec_current_pagination_observes_fair_access_rate_limit() -> None:
    filtered_entry = (
        "<entry><updated>2026-07-24T03:00:00-04:00</updated>"
        '<category scheme="https://www.sec.gov/" term="10-Q"/></entry>'
    )
    first_page = _sec_current_atom(*(filtered_entry for _ in range(100)))
    final_page = _sec_current_atom(
        "<entry><updated>2026-07-23T21:00:00-04:00</updated>"
        '<category scheme="https://www.sec.gov/" term="10-Q"/></entry>'
    )
    clock_value = [0.0]
    sleeps: list[float] = []

    def sleep(delay: float) -> None:
        sleeps.append(delay)
        clock_value[0] += delay

    def handler(request: httpx.Request) -> httpx.Response:
        page = first_page if request.url.params["start"] == "0" else final_page
        return httpx.Response(200, content=page.encode("iso-8859-1"))

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = SecCurrentFilingsConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
            sleep=sleep,
            clock=lambda: clock_value[0],
            minimum_request_interval=0.12,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 7, 23),
                window_end_exclusive=date(2026, 7, 24),
                max_pages=2,
            ),
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            now=NOW,
        )

    assert result.request_count == 2
    assert result.raw_count == 101
    assert result.records == ()
    assert sleeps == [pytest.approx(0.12)]


def test_sec_current_bootstrap_stops_at_completed_day_boundary() -> None:
    current_entry = (
        "<entry><updated>2026-07-29T12:00:00-04:00</updated>"
        '<category scheme="https://www.sec.gov/" term="10-Q"/></entry>'
    )
    old_irrelevant_entry = (
        "<entry><updated>2026-07-28T21:00:00-04:00</updated>"
        '<category scheme="https://www.sec.gov/" term="10-Q"/></entry>'
    )
    overlap_accession = "0001104659-26-090001"
    older_accession = "0001104659-26-090000"
    pages = {
        "0": _sec_current_atom(
            *(current_entry for _ in range(100)),
            updated="2026-07-29T12:05:00-04:00",
        ),
        "100": _sec_current_atom(
            _sec_current_entry(
                accession=overlap_accession,
                title=(
                    "SCHEDULE 13D - Example Corp. "
                    "(0001728117) (Subject)"
                ),
                form="SCHEDULE 13D",
                cik="0001728117",
                accepted="2026-07-28T22:45:00-04:00",
            ),
            _sec_current_entry(
                accession=older_accession,
                title=(
                    "SCHEDULE 13D - Older Corp. "
                    "(0001728118) (Subject)"
                ),
                form="SCHEDULE 13D",
                cik="0001728118",
                accepted="2026-07-28T22:15:00-04:00",
            ),
            *(old_irrelevant_entry for _ in range(98)),
            updated="2026-07-29T12:05:00-04:00",
        ),
    }
    starts: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        start = request.url.params["start"]
        starts.append(start)
        if start not in pages:
            raise AssertionError("bootstrap read beyond the completed-day overlap")
        return httpx.Response(
            200,
            content=pages[start].encode("iso-8859-1"),
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = SecCurrentFilingsConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 7, 27),
                window_end_exclusive=date(2026, 7, 29),
                max_pages=10,
            ),
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            now=NOW,
        )

    assert starts == ["0", "100"]
    assert result.request_count == 2
    assert result.raw_count == 200
    assert [record.metadata["accession_number"] for record in result.records] == [
        overlap_accession
    ]
    assert result.next_cursor and result.next_cursor.startswith(
        "sec-current-v1:"
    )


def test_sec_current_bootstrap_short_page_before_cutoff_fails_closed() -> None:
    atom = _sec_current_atom(
        "<entry><updated>2026-07-29T12:00:00-04:00</updated>"
        '<category scheme="https://www.sec.gov/" term="10-Q"/></entry>'
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=atom.encode("iso-8859-1"))

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = SecCurrentFilingsConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        )
        with pytest.raises(
            GlobalConnectorPaginationError,
            match="bootstrap cutoff was not reached",
        ):
            connector.fetch(
                GlobalConnectorRequest(
                    window_start=date(2026, 7, 27),
                    window_end_exclusive=date(2026, 7, 29),
                    max_pages=10,
                ),
                eligibility=eligibility(
                    "official:sec-edgar",
                    "sec-edgar",
                ),
                now=NOW,
            )


def test_sec_current_bootstrap_page_budget_before_cutoff_fails_closed() -> None:
    current_entry = (
        "<entry><updated>2026-07-29T12:00:00-04:00</updated>"
        '<category scheme="https://www.sec.gov/" term="10-Q"/></entry>'
    )
    atom = _sec_current_atom(
        *(current_entry for _ in range(100)),
        updated="2026-07-29T12:05:00-04:00",
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=atom.encode("iso-8859-1"))

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = SecCurrentFilingsConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        )
        with pytest.raises(
            GlobalConnectorPaginationError,
            match="exceeded max_pages request budget",
        ):
            connector.fetch(
                GlobalConnectorRequest(
                    window_start=date(2026, 7, 27),
                    window_end_exclusive=date(2026, 7, 29),
                    max_pages=1,
                ),
                eligibility=eligibility(
                    "official:sec-edgar",
                    "sec-edgar",
                ),
                now=NOW,
            )


@pytest.mark.parametrize(
    ("window_end", "expected"),
    (
        (
            date(2026, 3, 9),
            datetime(2026, 3, 9, 2, 30, tzinfo=timezone.utc),
        ),
        (
            date(2026, 11, 2),
            datetime(2026, 11, 2, 3, 30, tzinfo=timezone.utc),
        ),
    ),
)
def test_sec_current_bootstrap_cutoff_respects_eastern_dst(
    window_end: date,
    expected: datetime,
) -> None:
    request = GlobalConnectorRequest(
        window_start=window_end - timedelta(days=1),
        window_end_exclusive=window_end,
    )

    assert _sec_current_cutoff(request, None) == expected


def test_sec_submissions_observes_shared_fair_access_interval_and_budget() -> None:
    clock_value = [0.0]
    sleeps: list[float] = []
    requests: list[httpx.Request] = []

    def sleep(delay: float) -> None:
        sleeps.append(delay)
        clock_value[0] += delay

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        cik = request.url.path.removeprefix("/submissions/CIK").removesuffix(
            ".json"
        )
        return httpx.Response(
            200,
            json={
                "cik": cik,
                "name": f"Issuer {cik}",
                "filings": {
                    "recent": {
                        "accessionNumber": [],
                        "filingDate": [],
                        "acceptanceDateTime": [],
                        "form": [],
                        "primaryDocument": [],
                        "primaryDocDescription": [],
                        "items": [],
                    }
                },
            },
        )

    scoped_request = GlobalConnectorRequest(
        window_start=date(2026, 7, 24),
        window_end_exclusive=date(2026, 7, 25),
        issuers=(
            IssuerReference(
                namespace="US:CIK",
                identifier_type="CIK",
                value="1",
            ),
            IssuerReference(
                namespace="US:CIK",
                identifier_type="CIK",
                value="2",
            ),
        ),
        max_pages=2,
    )
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = SecSubmissionsConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
            sleep=sleep,
            clock=lambda: clock_value[0],
        )
        result = connector.fetch(
            scoped_request,
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            now=NOW,
        )

        with pytest.raises(
            GlobalConnectorPaginationError,
            match="issuer scope exceeds max_pages",
        ):
            connector.fetch(
                replace(scoped_request, max_pages=1),
                eligibility=eligibility(
                    "official:sec-edgar",
                    "sec-edgar",
                ),
                now=NOW,
            )

    assert result.request_count == 2
    assert len(requests) == 2
    assert sleeps == [pytest.approx(0.12)]


def test_sec_submissions_uses_same_cik_scoped_identity_as_market_wide_paths() -> None:
    accession = "0001104659-26-086735"

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/submissions/CIK0001728117.json"
        return httpx.Response(
            200,
            json={
                "cik": "1728117",
                "name": "Gossamer Bio, Inc.",
                "filings": {
                    "recent": {
                        "accessionNumber": [accession],
                        "filingDate": ["2026-07-24"],
                        "acceptanceDateTime": ["20260724090000"],
                        "form": ["SC 13D/A"],
                        "primaryDocument": ["ownership.htm"],
                        "primaryDocDescription": [
                            "Amended beneficial ownership report"
                        ],
                        "items": [""],
                    }
                },
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = SecSubmissionsConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 7, 24),
                window_end_exclusive=date(2026, 7, 25),
                issuers=(
                    IssuerReference(
                        namespace="US:CIK",
                        identifier_type="CIK",
                        value="1728117",
                    ),
                ),
            ),
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            now=NOW,
        )

    assert len(result.records) == 1
    record = result.records[0]
    assert record.external_id == (
        f"sec-accession-cik-v1:{accession}:0001728117"
    )
    assert record.metadata["accession_number"] == accession
    assert record.metadata["cik"] == "0001728117"


def test_sec_hybrid_merges_current_and_daily_by_cik_scoped_identity() -> None:
    accession = "0001104659-26-086735"
    atom = _sec_current_atom(
        _sec_current_entry(
            accession=accession,
            title=(
                "SCHEDULE 13D/A - Gossamer Bio, Inc. "
                "(0001728117) (Subject)"
            ),
            form="SCHEDULE 13D/A",
            cik="0001728117",
        ),
        _sec_current_entry(
            accession=accession,
            title=(
                "SCHEDULE 13D/A - D. E. SHAW &amp; CO, L.P. "
                "(0001009268) (Filed by)"
            ),
            form="SCHEDULE 13D/A",
            cik="0001009268",
        ),
        _sec_current_entry(
            accession="0000320193-26-000110",
            title="10-Q - Apple Inc. (0000320193) (Filer)",
            form="10-Q",
            cik="0000320193",
            accepted="2026-07-23T21:00:00-04:00",
        ),
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/master.20260723.idx"):
            return httpx.Response(
                200,
                text=(
                    "Description\n"
                    "CIK|Company Name|Form Type|Date Filed|Filename\n"
                    "------------------------------------------------------------\n"
                    "1009268|D. E. SHAW & CO., L.P.|SC 13D/A|20260723|"
                    f"edgar/data/1104659/{accession}.txt\n"
                    "1728117|Gossamer Bio, Inc.|SC 13D/A|20260723|"
                    f"edgar/data/1104659/{accession}.txt\n"
                ),
            )
        assert request.url.path == "/cgi-bin/browse-edgar"
        return httpx.Response(200, content=atom.encode("iso-8859-1"))

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = SecHybridConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 7, 23),
                window_end_exclusive=date(2026, 7, 24),
                max_pages=3,
            ),
            eligibility=eligibility("official:sec-edgar", "sec-edgar"),
            now=NOW,
        )

    assert result.raw_count == 5
    assert len(result.records) == 2
    by_cik = {record.metadata["cik"]: record for record in result.records}
    assert by_cik["0001009268"].external_id == (
        f"sec-accession-cik-v1:{accession}:0001009268"
    )
    assert by_cik["0001009268"].metadata["discovery"] == "daily-master-index"
    assert by_cik["0001728117"].external_id == (
        f"sec-accession-cik-v1:{accession}:0001728117"
    )
    assert (
        by_cik["0001728117"].metadata["discovery"]
        == "current-filings-atom"
    )
    assert {
        record.metadata["accession_number"] for record in result.records
    } == {accession}


def test_sec_hybrid_fails_when_current_feed_fails() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/master.20260723.idx"):
            return httpx.Response(
                200,
                text=(
                    "Description\n"
                    "CIK|Company Name|Form Type|Date Filed|Filename\n"
                    "------------------------------------------------------------\n"
                ),
            )
        return httpx.Response(503)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = SecHybridConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        )
        with pytest.raises(GlobalConnectorError, match="current-filings HTTP 503"):
            connector.fetch(
                GlobalConnectorRequest(
                    window_start=date(2026, 7, 23),
                    window_end_exclusive=date(2026, 7, 24),
                    max_pages=3,
                ),
                eligibility=eligibility("official:sec-edgar", "sec-edgar"),
                now=NOW,
            )


def test_source_right_is_checked_before_any_network_request() -> None:
    called = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal called
        called += 1
        return httpx.Response(500)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = SecSubmissionsConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        )
        with pytest.raises(GlobalSourceRightDenied):
            connector.fetch(
                GlobalConnectorRequest(
                    window_start=date(2026, 7, 24),
                    window_end_exclusive=date(2026, 7, 25),
                    issuers=(
                        IssuerReference(
                            namespace="US:CIK",
                            identifier_type="CIK",
                            value="320193",
                        ),
                    ),
                ),
                eligibility=eligibility(
                    "official:sec-edgar",
                    "wrong-source-key",
                ),
                now=NOW,
            )
    assert called == 0


def test_collection_only_right_downgrades_full_record_to_link() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "cik": "1",
                "name": "Example Corp.",
                "filings": {
                    "recent": {
                        "accessionNumber": ["0000000001-26-000001"],
                        "filingDate": ["2026-07-24"],
                        "acceptanceDateTime": ["20260724090000"],
                        "form": ["SC 13D"],
                        "primaryDocument": ["ownership.htm"],
                        "primaryDocDescription": ["Beneficial ownership report"],
                        "items": [""],
                    }
                },
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = SecSubmissionsConnector(
            user_agent="BSIDE test ops@example.com",
            client=client,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 7, 24),
                window_end_exclusive=date(2026, 7, 25),
                issuers=(
                    IssuerReference(
                        namespace="US:CIK",
                        identifier_type="CIK",
                        value="1",
                    ),
                ),
            ),
            eligibility=eligibility(
                "official:sec-edgar",
                "sec-edgar",
                redistribute=False,
            ),
            now=NOW,
        )
    assert result.records[0].record_kind == "link"
    assert result.records[0].body_text is None
    assert isinstance(result.records[0].issuer_reference, IssuerReference)


def test_edinet_connector_interprets_naive_source_timestamp_as_jst() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "subscription-key" not in request.headers
        assert request.url.params["Subscription-Key"] == "edinet-key"
        assert request.url.params["date"] == "2026-07-24"
        return httpx.Response(
            200,
            json={
                "metadata": {"status": "200"},
                "results": [
                    {
                        "docID": "S100ABCD",
                        "edinetCode": "E01234",
                        "filerName": "テスト株式会社",
                        "docDescription": "大量保有報告書",
                        "submitDateTime": "2026-07-24T15:00:00",
                        "docTypeCode": "350",
                        "ordinanceCode": "060",
                        "formCode": "010000",
                    }
                ],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = EdinetDocumentsConnector(
            api_key="edinet-key",
            client=client,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 7, 24),
                window_end_exclusive=date(2026, 7, 25),
            ),
            eligibility=eligibility("official:edinet", "edinet"),
            now=NOW,
        )
    assert result.records[0].filed_at == "2026-07-24T06:00:00+00:00"
    assert result.records[0].title == "大量保有報告書"
    assert result.records[0].original_language == "ja"
    assert result.records[0].original_url.endswith(
        "/WZEK0040.aspx?S100ABCD"
    )


def test_edinet_extraordinary_report_is_conservatively_classified() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "metadata": {"status": "200"},
                "results": [
                    {
                        "docID": "S100EFGH",
                        "edinetCode": "E01234",
                        "filerName": "テスト株式会社",
                        "docDescription": "臨時報告書",
                        "currentReportReason": "代表取締役の異動",
                        "submitDateTime": "2026-07-24T15:00:00",
                        "docTypeCode": "180",
                        "ordinanceCode": "010",
                        "formCode": "19A000",
                    },
                    {
                        "docID": "S100IJKL",
                        "edinetCode": "E01234",
                        "filerName": "テスト株式会社",
                        "docDescription": "臨時報告書",
                        "currentReportReason": "その他の重要な事象",
                        "submitDateTime": "2026-07-24T16:00:00",
                        "docTypeCode": "180",
                        "ordinanceCode": "010",
                        "formCode": "19A000",
                    },
                ],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = EdinetDocumentsConnector(
            api_key="edinet-key",
            client=client,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 7, 24),
                window_end_exclusive=date(2026, 7, 25),
            ),
            eligibility=eligibility("official:edinet", "edinet"),
            now=NOW,
        )
    assert [record.event_family for record in result.records] == [
        "board_and_compensation",
        "unclassified",
    ]


def test_edinet_models_nonpublic_transitions_without_withdrawal() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "metadata": {"status": "200"},
                "results": [
                    {
                        "seqNumber": 91,
                        "docID": "S100NDS1",
                        "edinetCode": "E01234",
                        "filerName": "Example One",
                        "docDescription": "大量保有報告書",
                        "submitDateTime": "2026-07-24 09:00",
                        "opeDateTime": "2026-07-24 11:00",
                        "docTypeCode": "350",
                        "withdrawalStatus": "0",
                        "docInfoEditStatus": "0",
                        "disclosureStatus": "1",
                    },
                    {
                        "seqNumber": 1,
                        "docID": "S100NDS2",
                        "edinetCode": "E01235",
                        "filerName": "Example Two",
                        "docDescription": "大量保有報告書",
                        "submitDateTime": "2026-07-24 10:00",
                        "opeDateTime": None,
                        "docTypeCode": "350",
                        "withdrawalStatus": "0",
                        "docInfoEditStatus": "0",
                        "disclosureStatus": "2",
                    },
                    {
                        "seqNumber": 92,
                        "docID": "S100REST",
                        "edinetCode": "E01236",
                        "filerName": "Example Three",
                        "docDescription": "大量保有報告書",
                        "submitDateTime": "2026-07-24 08:00",
                        "opeDateTime": "2026-07-24 13:00",
                        "docTypeCode": "350",
                        "withdrawalStatus": "0",
                        "docInfoEditStatus": "0",
                        "disclosureStatus": "3",
                    },
                ],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = EdinetDocumentsConnector(
            api_key="edinet-key",
            client=client,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 7, 24),
                window_end_exclusive=date(2026, 7, 25),
            ),
            eligibility=eligibility("official:edinet", "edinet"),
            now=NOW,
        )

    records = {record.external_id: record for record in result.records}
    assert set(records) == {"S100NDS1", "S100NDS2", "S100REST"}
    assert {record.change_type for record in records.values()} == {"updated"}
    # A complete official row is one accepted entity. Its record already
    # carries the lifecycle transition, so emitting a second observation
    # would make acknowledged_count exceed the official raw row count.
    assert result.lifecycle_observations == ()
    assert result.raw_count == len(result.records) == 3
    assert records["S100NDS1"].filed_at == "2026-07-24T00:00:00+00:00"
    assert {
        key: record.metadata["disclosure_state"]
        for key, record in records.items()
    } == {
        "S100NDS1": "nonpublic_started",
        "S100NDS2": "nonpublic",
        "S100REST": "public_restored",
    }
    assert {
        key: record.metadata["lifecycle_observed_at"]
        for key, record in records.items()
    } == {
        "S100NDS1": "2026-07-24T02:00:00+00:00",
        "S100NDS2": "2026-07-24T01:00:00+00:00",
        "S100REST": "2026-07-24T04:00:00+00:00",
    }


def test_edinet_missing_lifecycle_timestamp_has_stable_file_day_fallback() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "metadata": {"status": "200"},
                "results": [
                    {
                        "seqNumber": 1,
                        "docID": "S100MISS",
                        "edinetCode": None,
                        "filerName": None,
                        "docDescription": None,
                        "submitDateTime": None,
                        "opeDateTime": None,
                        "docTypeCode": None,
                        "parentDocID": "S100BASE",
                        "withdrawalStatus": "2",
                        "docInfoEditStatus": "0",
                        "disclosureStatus": "0",
                    }
                ],
            },
        )

    request = GlobalConnectorRequest(
        window_start=date(2026, 7, 24),
        window_end_exclusive=date(2026, 7, 25),
    )
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = EdinetDocumentsConnector(
            api_key="edinet-key",
            client=client,
        )
        first = connector.fetch(
            request,
            eligibility=eligibility("official:edinet", "edinet"),
            now=NOW,
        )
        second = connector.fetch(
            request,
            eligibility=eligibility("official:edinet", "edinet"),
            now=NOW.replace(minute=31),
        )

    assert first.lifecycle_observations == second.lifecycle_observations
    assert len(first.lifecycle_observations) == 1
    assert (
        first.lifecycle_observations[0].observed_at
        == "2026-07-23T15:00:00+00:00"
    )


def test_edinet_rejects_unknown_official_lifecycle_status() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "metadata": {"status": "200"},
                "results": [
                    {
                        "docID": "S100BAD1",
                        "edinetCode": "E01234",
                        "filerName": "Example",
                        "docDescription": "大量保有報告書",
                        "submitDateTime": "2026-07-24 09:00",
                        "docTypeCode": "350",
                        "withdrawalStatus": "0",
                        "docInfoEditStatus": "0",
                        "disclosureStatus": "9",
                    }
                ],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(
            GlobalConnectorContractError,
            match="disclosureStatus is invalid",
        ):
            EdinetDocumentsConnector(
                api_key="edinet-key",
                client=client,
            ).fetch(
                GlobalConnectorRequest(
                    window_start=date(2026, 7, 24),
                    window_end_exclusive=date(2026, 7, 25),
                ),
                eligibility=eligibility("official:edinet", "edinet"),
                now=NOW,
            )


def test_companies_house_detects_pagination_drift() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(
                200,
                json={
                    "start_index": 0,
                    "items_per_page": 1,
                    "total_count": 2,
                    "kind": "filing-history",
                    "items": [
                        {
                            "transaction_id": "tx-1",
                            "date": "2026-07-24",
                            "category": "officers",
                            "type": "AP01",
                            "description": "Appointment of director",
                        }
                    ],
                },
            )
        return httpx.Response(
            200,
            json={
                "start_index": 0,
                "items_per_page": 1,
                "total_count": 2,
                "kind": "filing-history",
                "items": [],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        connector = CompaniesHouseFilingHistoryConnector(
            api_key="companies-house-key",
            client=client,
        )
        with pytest.raises(GlobalConnectorPaginationError, match="drifted"):
            connector.fetch(
                GlobalConnectorRequest(
                    window_start=date(2026, 7, 24),
                    window_end_exclusive=date(2026, 7, 25),
                    issuers=(
                        IssuerReference(
                            namespace="GB:COMPANIES_HOUSE",
                            identifier_type="COMPANY_NUMBER",
                            value="01234567",
                            legal_name="Example Limited",
                        ),
                    ),
                    page_size=1,
                ),
                eligibility=eligibility(
                    "official:companies-house",
                    "companies-house",
                    source_type="official_register",
                ),
                now=NOW,
            )


def test_companies_house_stops_after_descending_page_crosses_window() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json={
                "start_index": 0,
                "items_per_page": 2,
                "total_count": 200,
                "kind": "filing-history",
                "items": [
                    {
                        "transaction_id": "tx-current",
                        "date": "2026-07-24",
                        "category": "officers",
                        "type": "AP01",
                        "description": "Appointment of director",
                    },
                    {
                        "transaction_id": "tx-old",
                        "date": "2026-07-23",
                        "category": "officers",
                        "type": "TM01",
                        "description": "Termination of director",
                    },
                ],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = CompaniesHouseFilingHistoryConnector(
            api_key="companies-house-key",
            client=client,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 7, 24),
                window_end_exclusive=date(2026, 7, 25),
                issuers=(
                    IssuerReference(
                        namespace="GB:COMPANIES_HOUSE",
                        identifier_type="COMPANY_NUMBER",
                        value="01234567",
                        legal_name="Example Limited",
                    ),
                ),
                page_size=2,
                max_pages=10,
            ),
            eligibility=eligibility(
                "official:companies-house",
                "companies-house",
                source_type="official_register",
            ),
            now=NOW,
        )

    assert calls == 1
    assert result.request_count == 1
    assert result.raw_count == 1
    assert [record.external_id for record in result.records] == ["tx-current"]
    assert result.records[0].metadata["title_provenance"] == "source"


def test_companies_house_marks_type_fallback_as_generated_metadata() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "start_index": 0,
                "items_per_page": 1,
                "total_count": 1,
                "kind": "filing-history",
                "items": [
                    {
                        "transaction_id": "tx-without-description",
                        "date": "2026-07-24",
                        "category": "officers",
                        "type": "AP01",
                    }
                ],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = CompaniesHouseFilingHistoryConnector(
            api_key="companies-house-key",
            client=client,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 7, 24),
                window_end_exclusive=date(2026, 7, 25),
                issuers=(
                    IssuerReference(
                        namespace="GB:COMPANIES_HOUSE",
                        identifier_type="COMPANY_NUMBER",
                        value="01234567",
                        legal_name="Example Limited",
                    ),
                ),
            ),
            eligibility=eligibility(
                "official:companies-house",
                "companies-house",
                source_type="official_register",
            ),
            now=NOW,
        )

    assert result.records[0].title == "AP01"
    assert (
        result.records[0].metadata["title_provenance"]
        == "generated_metadata"
    )


def test_companies_house_refuses_early_stop_when_source_order_drifts() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "start_index": 0,
                "items_per_page": 2,
                "total_count": 2,
                "kind": "filing-history",
                "items": [
                    {
                        "transaction_id": "tx-old",
                        "date": "2026-07-23",
                        "category": "officers",
                        "type": "TM01",
                        "description": "Termination of director",
                    },
                    {
                        "transaction_id": "tx-new",
                        "date": "2026-07-24",
                        "category": "officers",
                        "type": "AP01",
                        "description": "Appointment of director",
                    },
                ],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(
            GlobalConnectorPaginationError,
            match="not descending",
        ):
            CompaniesHouseFilingHistoryConnector(
                api_key="companies-house-key",
                client=client,
            ).fetch(
                GlobalConnectorRequest(
                    window_start=date(2026, 7, 24),
                    window_end_exclusive=date(2026, 7, 25),
                    issuers=(
                        IssuerReference(
                            namespace="GB:COMPANIES_HOUSE",
                            identifier_type="COMPANY_NUMBER",
                            value="01234567",
                            legal_name="Example Limited",
                        ),
                    ),
                    page_size=2,
                ),
                eligibility=eligibility(
                    "official:companies-house",
                    "companies-house",
                    source_type="official_register",
                ),
                now=NOW,
            )


def test_companies_house_request_budget_is_global_across_issuers() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json={
                "start_index": 0,
                "items_per_page": 1,
                "total_count": 1,
                "kind": "filing-history",
                "items": [
                    {
                        "transaction_id": f"tx-{calls}",
                        "date": "2026-07-24",
                        "category": "officers",
                        "type": "AP01",
                        "description": "Appointment of director",
                    }
                ],
            },
        )

    issuers = tuple(
        IssuerReference(
            namespace="GB:COMPANIES_HOUSE",
            identifier_type="COMPANY_NUMBER",
            value=company_number,
            legal_name=f"Example {company_number}",
        )
        for company_number in ("01234567", "07654321")
    )
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(
            GlobalConnectorPaginationError,
            match="run request budget exhausted",
        ):
            CompaniesHouseFilingHistoryConnector(
                api_key="companies-house-key",
                client=client,
            ).fetch(
                GlobalConnectorRequest(
                    window_start=date(2026, 7, 24),
                    window_end_exclusive=date(2026, 7, 25),
                    issuers=issuers,
                    page_size=1,
                    max_pages=1,
                ),
                eligibility=eligibility(
                    "official:companies-house",
                    "companies-house",
                    source_type="official_register",
                ),
                now=NOW,
            )
    assert calls == 1


def test_companies_house_honors_retry_after_and_bounded_5xx_backoff() -> None:
    calls = 0
    delays: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(429, headers={"Retry-After": "3"})
        if calls == 2:
            return httpx.Response(503)
        return httpx.Response(
            200,
            json={
                "start_index": 0,
                "items_per_page": 100,
                "total_count": 0,
                "kind": "filing-history",
                "items": [],
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        result = CompaniesHouseFilingHistoryConnector(
            api_key="companies-house-key",
            client=client,
            max_retries=2,
            sleep=delays.append,
        ).fetch(
            GlobalConnectorRequest(
                window_start=date(2026, 7, 24),
                window_end_exclusive=date(2026, 7, 25),
                issuers=(
                    IssuerReference(
                        namespace="GB:COMPANIES_HOUSE",
                        identifier_type="COMPANY_NUMBER",
                        value="01234567",
                        legal_name="Example Limited",
                    ),
                ),
                max_pages=3,
            ),
            eligibility=eligibility(
                "official:companies-house",
                "companies-house",
                source_type="official_register",
            ),
            now=NOW,
        )

    assert calls == result.request_count == 3
    assert delays == [3.0, 2.0]


def test_companies_house_retry_count_is_bounded() -> None:
    calls = 0
    delays: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503)

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(GlobalConnectorError, match="after retries"):
            CompaniesHouseFilingHistoryConnector(
                api_key="companies-house-key",
                client=client,
                max_retries=2,
                sleep=delays.append,
            ).fetch(
                GlobalConnectorRequest(
                    window_start=date(2026, 7, 24),
                    window_end_exclusive=date(2026, 7, 25),
                    issuers=(
                        IssuerReference(
                            namespace="GB:COMPANIES_HOUSE",
                            identifier_type="COMPANY_NUMBER",
                            value="01234567",
                            legal_name="Example Limited",
                        ),
                    ),
                    max_pages=3,
                ),
                eligibility=eligibility(
                    "official:companies-house",
                    "companies-house",
                    source_type="official_register",
                ),
                now=NOW,
            )
    assert calls == 3
    assert delays == [1.0, 2.0]


def test_canada_descriptor_is_neutral_and_does_not_claim_sedar_as_issuer_ir() -> None:
    assert "sedar" not in CANADA_IR_DESCRIPTOR.base_url.casefold()
    assert "sedar" not in CANADA_IR_DESCRIPTOR.source_name.casefold()
    assert CANADA_IR_DESCRIPTOR.base_url.startswith("https://www.canada.ca/")
    assert CANADA_IR_DESCRIPTOR.coverage_mode is CoverageMode.LINK_ONLY
    assert CANADA_IR_DESCRIPTOR.schedule_minutes == 30


def test_australia_descriptor_matches_manual_asic_link_contract() -> None:
    assert AUSTRALIA_ASIC_DESCRIPTOR.base_url == "https://www.asic.gov.au/"
    assert AUSTRALIA_ASIC_DESCRIPTOR.coverage_mode is CoverageMode.LINK_ONLY
    assert AUSTRALIA_ASIC_DESCRIPTOR.schedule_minutes == 30
