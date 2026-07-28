from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest

from curator.global_connectors import GlobalConnectorEnvelope
from curator.global_connectors import global_document_content_hash
from curator.global_ingest import GlobalIngestChunk, GlobalIngestReceipt
from curator.official_source_rights import OfficialSourceRightEligibility
from curator.selected_market_ingest import (
    MANUAL_LINK_EVENT_FAMILIES,
    MAX_APPROVED_HOSTS_PER_COUNTRY,
    MAX_ISSUERS_PER_COUNTRY,
    SelectedMarketApiError,
    SelectedMarketConfigurationError,
    SelectedMarketRightsError,
    execute_selected_market_ingest,
    main,
    parse_selected_official_links,
)


REVISION = "a" * 40
RIGHTS_REVISION = "b" * 64
WORKFLOW = (
    Path(__file__).resolve().parents[1]
    / ".github"
    / "workflows"
    / "ingest-selected-markets.yml"
)
MIGRATION = (
    Path(__file__).resolve().parents[1]
    / "deploy"
    / "activist"
    / "migrations"
    / "011_global_terminal_v2.sql"
).read_text(encoding="utf-8")


def _link(
    *,
    country: str = "CA",
    suffix: str = "1",
    **changes: object,
) -> dict[str, object]:
    source_right_id = (
        "official:ca-issuer-ir"
        if country == "CA"
        else "official:asic-register"
    )
    payload: dict[str, object] = {
        "country_code": country,
        "issuer_identifier_type": (
            "SEDAR_ISSUER_ID" if country == "CA" else "ACN"
        ),
        "issuer_identifier": f"{country}000{suffix}",
        "issuer_name": f"Issuer {suffix}",
        "source_right_id": source_right_id,
        "official_host": (
            f"investors.issuer{suffix}.ca"
            if country == "CA"
            else "asic.gov.au"
        ),
        "original_url": (
            f"https://investors.issuer{suffix}.ca/notices/{suffix}.pdf"
            if country == "CA"
            else f"https://asic.gov.au/register/{suffix}"
        ),
        "title": f"Original title {suffix}",
        "original_language": "en",
        "filed_at": "2026-07-23T12:00:00-04:00",
        "first_observed_at": "2026-07-23T16:15:00Z",
        "event_family": (
            "meeting_and_vote" if country == "CA" else "listing_status"
        ),
    }
    payload.update(changes)
    return payload


def _approved_host(record: dict[str, object]) -> dict[str, object]:
    return {
        "hostname": record["official_host"],
        "issuer_identifier_type": record[
            "issuer_identifier_type"
        ],
        "issuer_identifier": record["issuer_identifier"],
        "evidence_sha256": "e" * 64,
    }


def _config(
    *items: dict[str, object],
    approved_hosts: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    hosts = (
        approved_hosts
        if approved_hosts is not None
        else [_approved_host(item) for item in items]
    )
    return {
        "schema_version": 1,
        "approved_hosts": hosts,
        "records": list(items),
    }


def _parse(
    *items: dict[str, object],
    country: str = "CA",
    approved_hosts: list[dict[str, object]] | None = None,
):
    return parse_selected_official_links(
        json.dumps(
            _config(
                *items,
                approved_hosts=approved_hosts,
            )
        ),
        country_code=country,
    )


class _Rights:
    def __init__(
        self,
        *,
        country: str = "CA",
        revisions: list[str] | None = None,
        public_allowed: bool = True,
        checked_at: str | None = None,
    ) -> None:
        self.country = country
        self.revisions = list(revisions or [])
        self.public_allowed = public_allowed
        self.checked_at = checked_at
        self.calls: list[tuple[str, str]] = []

    def check(
        self,
        source_right_id: str,
        *,
        use: str = "collect",
    ) -> OfficialSourceRightEligibility:
        self.calls.append((source_right_id, use))
        revision = (
            self.revisions.pop(0)
            if self.revisions
            else RIGHTS_REVISION
        )
        return OfficialSourceRightEligibility(
            source_right_id=source_right_id,
            use=use,
            rights_revision=revision,
            checked_at=(
                self.checked_at
                or datetime.now(timezone.utc)
                .replace(microsecond=0)
                .isoformat()
            ),
            source_type=(
                "official_issuer"
                if self.country == "CA"
                else "official_register"
            ),
            source_key=(
                "issuer-ir"
                if self.country == "CA"
                else "asic-register"
            ),
            redistribution_allowed=self.public_allowed,
            ai_allowed=False,
        )


class _Ingest:
    def __init__(
        self,
        *,
        acknowledged_delta: int = 0,
    ) -> None:
        self.acknowledged_delta = acknowledged_delta
        self.calls: list[
            tuple[GlobalConnectorEnvelope, str, str]
        ] = []

    def submit(
        self,
        *,
        envelope: GlobalConnectorEnvelope,
        chunk: GlobalIngestChunk,
        idempotency_key: str,
        code_revision: str,
    ) -> GlobalIngestReceipt:
        assert chunk.index == chunk.count == 1
        assert chunk.batch_raw_count == len(envelope.records)
        assert chunk.batch_acknowledged_count == len(envelope.records)
        expected_window_start = datetime.fromisoformat(
            envelope.retrieved_at.replace("Z", "+00:00")
        ).date()
        assert chunk.window_start == expected_window_start.isoformat()
        assert chunk.window_end_exclusive == (
            expected_window_start + timedelta(days=1)
        ).isoformat()
        self.calls.append(
            (envelope, idempotency_key, code_revision)
        )
        return GlobalIngestReceipt(
            ingest_id=f"selected:{len(self.calls)}",
            connector_id=envelope.connector_id,
            raw_count=envelope.raw_count,
            acknowledged_count=(
                len(envelope.records)
                + self.acknowledged_delta
            ),
            idempotent=False,
        )


def test_parser_preserves_exact_metadata_and_adds_no_body() -> None:
    source = _link(
        issuer_namespace="CA:SEDAR",
        market="TSX",
        ticker="AAA",
        external_id="official-record-1",
        document_type="issuer_notice",
    )
    records = _parse(source)
    assert len(records) == 1
    record = records[0]
    assert record.country_code == source["country_code"]
    assert record.original_url == source["original_url"]
    assert record.title == source["title"]
    assert record.original_language == source["original_language"]
    assert record.event_family == source["event_family"]
    assert record.official_host == source["official_host"]
    assert record.host_evidence_sha256 == "e" * 64
    assert record.issuer_reference.legal_name == source["issuer_name"]
    assert record.issuer_reference.value == source["issuer_identifier"]
    assert record.first_observed_at == "2026-07-23T16:15:00+00:00"


@pytest.mark.parametrize(
    "raw",
    (
        "",
        " ",
        '{"schema_version":1,"approved_hosts":[],"records":[]}',
    ),
)
def test_empty_config_is_an_explicit_empty_scope(raw: str) -> None:
    assert (
        parse_selected_official_links(raw, country_code="CA")
        == ()
    )


def test_legacy_list_shape_and_unknown_top_level_fields_are_rejected() -> None:
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="invalid_selected_market_json",
    ):
        parse_selected_official_links("[]", country_code="CA")
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="invalid_selected_market_json",
    ):
        parse_selected_official_links(
            json.dumps(
                {
                    **_config(_link()),
                    "fetch_urls": True,
                }
            ),
            country_code="CA",
        )


def test_parser_rejects_cross_country_rows() -> None:
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="selected_link_country_mismatch",
    ):
        _parse(_link(country="AU"), country="CA")


@pytest.mark.parametrize(
    ("country", "source_right_id"),
    (
        ("CA", "official:some-issuer"),
        ("AU", "official:some-register"),
    ),
)
def test_parser_requires_one_registered_aggregate_right(
    country: str,
    source_right_id: str,
) -> None:
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="selected_market_source_right_mismatch",
    ):
        _parse(
            _link(
                country=country,
                source_right_id=source_right_id,
            ),
            country=country,
        )


def test_parser_rejects_unclassified_event_family() -> None:
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="unsupported_selected_event_family",
    ):
        _parse(_link(event_family="unclassified"))


@pytest.mark.parametrize(
    ("country", "allowed"),
    tuple(
        (country, family)
        for country, families in MANUAL_LINK_EVENT_FAMILIES.items()
        for family in families
    ),
)
def test_parser_accepts_only_migration_declared_country_families(
    country: str,
    allowed: str,
) -> None:
    assert len(
        _parse(
            _link(country=country, event_family=allowed),
            country=country,
        )
    ) == 1


def test_parser_event_families_exactly_match_migration_coverage() -> None:
    rows = re.findall(
        r"\('coverage:(ca|au):[^']+',\s*"
        r"'connector:[^']+',\s*'(CA|AU)',\s*'[^']*',\s*"
        r"'([^']+)',\s*'(link-only)'",
        MIGRATION,
    )
    migration_families: dict[str, set[str]] = {"CA": set(), "AU": set()}
    for _, country, event_family, coverage_mode in rows:
        assert coverage_mode == "link-only"
        migration_families[country].add(event_family)
    assert migration_families == {
        country: set(event_families)
        for country, event_families in MANUAL_LINK_EVENT_FAMILIES.items()
    }


@pytest.mark.parametrize(
    ("country", "disallowed"),
    (
        ("CA", "listing_status"),
        ("CA", "capital_issuance"),
        ("AU", "meeting_and_vote"),
        ("AU", "tender_offer_and_mna"),
    ),
)
def test_parser_rejects_family_outside_country_source_coverage(
    country: str,
    disallowed: str,
) -> None:
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="unsupported_selected_event_family",
    ):
        _parse(
            _link(country=country, event_family=disallowed),
            country=country,
        )


def test_timestamp_order_uses_instants_not_offset_strings() -> None:
    records = _parse(
        _link(
            filed_at="2026-07-24T00:30:00+14:00",
            first_observed_at="2026-07-23T23:00:00-10:00",
        )
    )
    assert records[0].filed_at == "2026-07-23T10:30:00+00:00"
    assert records[0].first_observed_at == "2026-07-24T09:00:00+00:00"


@pytest.mark.parametrize(
    "url",
    (
        "http://investors.public-company.ca/report",
        "https://localhost/report",
        "https://127.0.0.1/report",
        "https://10.0.0.8/report",
        "https://issuer.internal/report",
        "https://issuer.example/report",
        "https://user:password@issuer.ca/report",
        "https://issuer.ca/report#fragment",
        "https://issuer.ca:8443/report",
    ),
)
def test_parser_rejects_non_public_or_non_exact_https_urls(
    url: str,
) -> None:
    with pytest.raises(SelectedMarketConfigurationError):
        _parse(_link(original_url=url))


@pytest.mark.parametrize(
    "url",
    (
        "https://issuer.ca/report?token=secret",
        "https://issuer.ca/report?API_KEY=secret",
        "https://issuer.ca/report?signature=secret",
        "https://issuer.ca/report?credential=secret",
        "https://issuer.ca/report?X-Amz-Credential=secret",
        "https://issuer.ca/report?X-Goog-Signature=secret",
    ),
)
def test_parser_rejects_credentials_in_link_queries(url: str) -> None:
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="credential_in_official_source_url",
    ):
        _parse(
            _link(
                official_host="issuer.ca",
                original_url=url,
            )
        )


@pytest.mark.parametrize(
    "url",
    (
        "https://issuer.ca/report?download=1",
        "https://issuer.ca/report?",
    ),
)
def test_parser_rejects_even_noncredential_queries_by_default(
    url: str,
) -> None:
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="query_not_allowed_in_official_source_url",
    ):
        _parse(
            _link(
                official_host="issuer.ca",
                original_url=url,
            )
        )


def test_ca_host_requires_separate_issuer_bound_provenance() -> None:
    record = _link()
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="official_host_not_approved_for_issuer",
    ):
        _parse(
            record,
            approved_hosts=[
                {
                    **_approved_host(record),
                    "issuer_identifier": "DIFFERENT",
                }
            ],
        )
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="invalid_host_provenance_evidence",
    ):
        _parse(
            record,
            approved_hosts=[
                {
                    **_approved_host(record),
                    "evidence_sha256": "not-a-sha",
                }
            ],
        )
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="unused_approved_host",
    ):
        _parse(
            record,
            approved_hosts=[
                _approved_host(record),
                {
                    "hostname": "investors.unused-issuer.ca",
                    "issuer_identifier_type": "SEDAR_ISSUER_ID",
                    "issuer_identifier": "CA999",
                    "evidence_sha256": "f" * 64,
                },
            ],
        )


def test_ca_approved_host_cannot_be_shared_across_issuers() -> None:
    first = _link()
    second = _link(
        suffix="2",
        official_host=first["official_host"],
        original_url=(
            f"https://{first['official_host']}/notices/second.pdf"
        ),
    )
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="ca_host_not_issuer_unique",
    ):
        _parse(first, second)


@pytest.mark.parametrize(
    "hostname",
    (
        "sedarplus.ca",
        "www.sedarplus.ca",
        "sedarplus.com",
        "www.sedarplus.com",
        "sedi.ca",
        "www.sedi.ca",
        "tmx.com",
        "www.tmx.com",
        "money.tmx.com",
        "tsx.com",
        "www.tsx.com",
        "asx.com.au",
        "www.asic.gov.au",
        "data.gov.au",
    ),
)
def test_ca_rejects_known_nonissuer_portal_hosts(hostname: str) -> None:
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="ca_host_must_be_issuer_controlled",
    ):
        _parse(
            _link(
                official_host=hostname,
                original_url=f"https://{hostname}/record",
            )
        )


def test_ca_manual_link_path_has_no_source_network_client() -> None:
    module = (
        Path(__file__).resolve().parents[1]
        / "curator"
        / "selected_market_ingest.py"
    ).read_text(encoding="utf-8")
    for network_symbol in (
        "urlopen(",
        "requests.get(",
        "httpx.get(",
        "httpx.Client(",
        "httpx.AsyncClient(",
    ):
        assert network_symbol not in module
    assert '"source_urls_requested": 0' in module
    assert "request_count=0" in module


@pytest.mark.parametrize(
    "hostname",
    (
        "www.asx.com.au",
        "data.gov.au",
        "issuer.example.com.au",
        "asic.gov.au.example.com",
    ),
)
def test_au_accepts_only_asic_official_hosts(hostname: str) -> None:
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="au_host_must_be_official_asic",
    ):
        _parse(
            _link(
                country="AU",
                official_host=hostname,
                original_url=f"https://{hostname}/record",
            ),
            country="AU",
        )


def test_parser_rejects_duplicate_url_and_external_id() -> None:
    first = _link()
    duplicate_url = {
        **first,
        "title": "Second record with the same URL",
    }
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="duplicate_selected_link",
    ):
        _parse(
            first,
            duplicate_url,
            approved_hosts=[_approved_host(first)],
        )

    with pytest.raises(
        SelectedMarketConfigurationError,
        match="duplicate_selected_link",
    ):
        _parse(
            _link(external_id="same"),
            _link(suffix="2", external_id="same"),
        )


def test_parser_rejects_unknown_fields_and_missing_observation_time() -> None:
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="malformed_selected_link",
    ):
        _parse(_link(unapproved_body="never store this"))

    missing = _link()
    del missing["first_observed_at"]
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="malformed_selected_link",
    ):
        _parse(missing)


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("title", " Original title"),
        ("original_language", "en "),
        ("original_url", " https://issuer.ca/report"),
    ),
)
def test_parser_does_not_silently_rewrite_exact_source_metadata(
    field: str,
    value: str,
) -> None:
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="malformed_selected_link",
    ):
        _parse(_link(**{field: value}))


def test_parser_enforces_fifty_issuer_host_mapping_limit() -> None:
    allowed = [
        _link(suffix=str(index))
        for index in range(MAX_ISSUERS_PER_COUNTRY)
    ]
    assert len(_parse(*allowed)) == MAX_ISSUERS_PER_COUNTRY
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="approved_host_limit_exceeded",
    ):
        _parse(*allowed, _link(suffix="overflow"))


def test_parser_enforces_fifty_issuer_host_mappings() -> None:
    assert MAX_APPROVED_HOSTS_PER_COUNTRY == MAX_ISSUERS_PER_COUNTRY
    approved_hosts = [
        {
            "hostname": f"investors.issuer{index}.ca",
            "issuer_identifier_type": "SEDAR_ISSUER_ID",
            "issuer_identifier": f"CA{index:04d}",
            "evidence_sha256": "e" * 64,
        }
        for index in range(MAX_APPROVED_HOSTS_PER_COUNTRY + 1)
    ]
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="approved_host_limit_exceeded",
    ):
        parse_selected_official_links(
            json.dumps(
                {
                    "schema_version": 1,
                    "approved_hosts": approved_hosts,
                    "records": [],
                }
            ),
            country_code="CA",
        )


def test_execution_rechecks_rights_once_before_and_after_batch() -> None:
    links = _parse(_link(), _link(suffix="2"))
    rights = _Rights()
    ingest = _Ingest()
    result = execute_selected_market_ingest(
        country_code="CA",
        links=links,
        code_revision=REVISION,
        rights_client=rights,
        ingest_client=ingest,
    )
    assert rights.calls == [
        ("official:ca-issuer-ir", "collect"),
        ("official:ca-issuer-ir", "public"),
        ("official:ca-issuer-ir", "collect"),
        ("official:ca-issuer-ir", "public"),
    ]
    assert result.record_count == 2
    assert result.issuer_count == 2
    assert len(result.batches) == 1
    assert result.batches[0].acknowledged_count == 2
    envelope, key, code_revision = ingest.calls[0]
    assert envelope.connector_id == "connector:ca:issuer-ir"
    assert envelope.source_right_id == "official:ca-issuer-ir"
    assert (
        envelope.source_manifest_sha256
        == links[0].approved_manifest_sha256
    )
    assert (
        envelope.to_payload()["source_manifest_sha256"]
        == links[0].approved_manifest_sha256
    )
    assert envelope.coverage_mode.value == "link-only"
    assert envelope.request_count == 0
    assert envelope.public_allowed is True
    assert envelope.ai_allowed is False
    assert all(record.body_text is None for record in envelope.records)
    assert all(record.record_kind == "link" for record in envelope.records)
    assert all(
        record.metadata["source_url_requested"] is False
        for record in envelope.records
    )
    assert all(
        record.metadata["ingest_mode"] == "manual-metadata"
        for record in envelope.records
    )
    assert all(
        record.metadata["title_provenance"] == "operator_metadata"
        for record in envelope.records
    )
    assert all(
        record.metadata["official_host"]
        == record.original_url.split("/", 3)[2]
        for record in envelope.records
    )
    assert all(
        record.content_hash
        == global_document_content_hash(
            record,
            source_type="official_issuer",
            public_allowed=True,
            ai_allowed=False,
        )
        for record in envelope.records
    )
    assert key.startswith("global-ingest-v2:ca:")
    assert code_revision == REVISION


def test_right_checks_are_constant_at_maximum_issuer_volume() -> None:
    links = _parse(
        *(
            _link(suffix=str(index))
            for index in range(MAX_ISSUERS_PER_COUNTRY)
        )
    )
    rights = _Rights()
    ingest = _Ingest()
    result = execute_selected_market_ingest(
        country_code="CA",
        links=links,
        code_revision=REVISION,
        rights_client=rights,
        ingest_client=ingest,
    )
    assert result.record_count == MAX_ISSUERS_PER_COUNTRY
    assert len(ingest.calls) == 1
    assert rights.calls == [
        ("official:ca-issuer-ir", "collect"),
        ("official:ca-issuer-ir", "public"),
        ("official:ca-issuer-ir", "collect"),
        ("official:ca-issuer-ir", "public"),
    ]


def test_au_uses_registered_asic_contract() -> None:
    links = _parse(_link(country="AU"), country="AU")
    rights = _Rights(country="AU")
    ingest = _Ingest()
    result = execute_selected_market_ingest(
        country_code="AU",
        links=links,
        code_revision=REVISION,
        rights_client=rights,
        ingest_client=ingest,
    )
    assert result.connector_id == "connector:au:asic-register"
    envelope = ingest.calls[0][0]
    assert envelope.source_right_id == "official:asic-register"
    assert envelope.records[0].source_key == "asic-register"


def test_exact_config_has_a_stable_content_idempotency_key() -> None:
    links = _parse(_link())
    first_ingest = _Ingest()
    second_ingest = _Ingest()
    execute_selected_market_ingest(
        country_code="CA",
        links=links,
        code_revision=REVISION,
        rights_client=_Rights(),
        ingest_client=first_ingest,
    )
    execute_selected_market_ingest(
        country_code="CA",
        links=links,
        code_revision=REVISION,
        rights_client=_Rights(),
        ingest_client=second_ingest,
    )
    assert first_ingest.calls[0][1] == second_ingest.calls[0][1]
    assert first_ingest.calls[0][1].startswith("global-ingest-v2:ca:")
    assert first_ingest.calls[0][0].to_payload() == (
        second_ingest.calls[0][0].to_payload()
    )


def test_manifest_digest_is_canonical_and_mixed_approval_is_rejected() -> None:
    payload = _config(_link())
    compact = parse_selected_official_links(
        json.dumps(payload, separators=(",", ":"), sort_keys=True),
        country_code="CA",
    )
    pretty = parse_selected_official_links(
        json.dumps(payload, indent=2, sort_keys=False),
        country_code="CA",
    )
    assert (
        compact[0].approved_manifest_sha256
        == pretty[0].approved_manifest_sha256
    )

    other = _parse(_link(suffix="other"))
    with pytest.raises(
        SelectedMarketConfigurationError,
        match="selected_market_manifest_mismatch",
    ):
        execute_selected_market_ingest(
            country_code="CA",
            links=(compact[0], other[0]),
            code_revision=REVISION,
            rights_client=_Rights(),
            ingest_client=_Ingest(),
        )


def test_execution_rejects_public_metadata_denial() -> None:
    with pytest.raises(
        SelectedMarketRightsError,
        match="source_right_contract_mismatch",
    ):
        execute_selected_market_ingest(
            country_code="CA",
            links=_parse(_link()),
            code_revision=REVISION,
            rights_client=_Rights(public_allowed=False),
            ingest_client=_Ingest(),
        )


def test_execution_rejects_stale_or_changed_rights() -> None:
    stale = (
        datetime.now(timezone.utc) - timedelta(minutes=6)
    ).replace(microsecond=0).isoformat()
    with pytest.raises(
        SelectedMarketRightsError,
        match="stale_source_right_grant",
    ):
        execute_selected_market_ingest(
            country_code="CA",
            links=_parse(_link()),
            code_revision=REVISION,
            rights_client=_Rights(checked_at=stale),
            ingest_client=_Ingest(),
        )

    with pytest.raises(
        SelectedMarketRightsError,
        match="source_right_revision_mismatch",
    ):
        execute_selected_market_ingest(
            country_code="CA",
            links=_parse(_link()),
            code_revision=REVISION,
            rights_client=_Rights(
                revisions=[RIGHTS_REVISION, "c" * 64],
            ),
            ingest_client=_Ingest(),
        )

    with pytest.raises(
        SelectedMarketRightsError,
        match="source_right_changed_during_ingest",
    ):
        execute_selected_market_ingest(
            country_code="CA",
            links=_parse(_link()),
            code_revision=REVISION,
            rights_client=_Rights(
                revisions=[
                    RIGHTS_REVISION,
                    RIGHTS_REVISION,
                    "c" * 64,
                    "c" * 64,
                ],
            ),
            ingest_client=_Ingest(),
        )


def test_execution_rejects_acknowledgment_mismatch() -> None:
    with pytest.raises(
        SelectedMarketApiError,
        match="api_acknowledgment_mismatch",
    ):
        execute_selected_market_ingest(
            country_code="CA",
            links=_parse(_link()),
            code_revision=REVISION,
            rights_client=_Rights(),
            ingest_client=_Ingest(acknowledged_delta=-1),
        )


def test_missing_config_writes_coverage_unavailable_without_api(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence_path = tmp_path / "evidence.json"
    monkeypatch.setenv("GOVERNANCE_PIPELINE_MODE", "shadow")
    monkeypatch.delenv("OFFICIAL_LINKS_JSON", raising=False)
    monkeypatch.delenv("CA_OFFICIAL_LINKS_JSON", raising=False)
    monkeypatch.delenv("BSIDE_API_BASE_URL", raising=False)
    assert main(
        [
            "--country",
            "CA",
            "--code-revision",
            REVISION,
            "--evidence",
            str(evidence_path),
            "--require-active-pipeline",
        ]
    ) == 0
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["status"] == "coverage_unavailable"
    assert evidence["eligible_for_release"] is False
    assert evidence["submitted_batch_count"] == 0
    assert evidence["source_urls_requested"] == 0
    assert evidence["body_storage"] is False
    assert evidence["coverage_mode"] == "link-only"
    assert evidence["ingest_mode"] == "manual-metadata"


def test_inactive_pipeline_fails_closed_and_still_writes_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence_path = tmp_path / "evidence.json"
    monkeypatch.setenv("GOVERNANCE_PIPELINE_MODE", "off")
    assert main(
        [
            "--country",
            "AU",
            "--code-revision",
            REVISION,
            "--evidence",
            str(evidence_path),
            "--require-active-pipeline",
        ]
    ) == 1
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["status"] == "failed"
    assert evidence["error"]["code"] == "governance_pipeline_not_active"


def test_configured_scope_without_ops_token_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence_path = tmp_path / "evidence.json"
    monkeypatch.setenv("GOVERNANCE_PIPELINE_MODE", "shadow")
    monkeypatch.setenv(
        "OFFICIAL_LINKS_JSON",
        json.dumps(_config(_link())),
    )
    monkeypatch.setenv(
        "BSIDE_API_BASE_URL",
        "https://alignpe.gabia.io/activist/api.php",
    )
    monkeypatch.delenv("BSIDE_OPS_TOKEN", raising=False)
    assert main(
        [
            "--country",
            "CA",
            "--code-revision",
            REVISION,
            "--evidence",
            str(evidence_path),
            "--require-active-pipeline",
        ]
    ) == 1
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    assert evidence["status"] == "failed"
    assert evidence["error"]["code"] == "missing_ops_token"
    assert evidence["source_urls_requested"] == 0
    assert evidence["body_storage"] is False


def test_malformed_config_evidence_does_not_echo_input(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence_path = tmp_path / "evidence.json"
    secret_url = (
        "https://investors.issuer1.ca/report?token=do-not-echo"
    )
    monkeypatch.setenv("GOVERNANCE_PIPELINE_MODE", "shadow")
    monkeypatch.setenv(
        "OFFICIAL_LINKS_JSON",
        json.dumps(_config(_link(original_url=secret_url))),
    )
    assert main(
        [
            "--country",
            "CA",
            "--code-revision",
            REVISION,
            "--evidence",
            str(evidence_path),
            "--require-active-pipeline",
        ]
    ) == 1
    serialized = evidence_path.read_text(encoding="utf-8")
    assert "do-not-echo" not in serialized
    assert "credential_in_official_source_url" in serialized


def test_workflow_is_default_branch_shadow_live_only_and_preserves_evidence() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "Ingest manual official-link metadata" in workflow
    assert 'cron: "7,37 * * * *"' in workflow
    assert "github.event.repository.default_branch" in workflow
    assert "GOVERNANCE_PIPELINE_MODE == 'shadow'" in workflow
    assert "GOVERNANCE_PIPELINE_MODE == 'live'" in workflow
    assert '["CA","AU"]' in workflow
    assert "vars.CA_OFFICIAL_LINKS_JSON" in workflow
    assert "vars.AU_OFFICIAL_LINKS_JSON" in workflow
    assert "curator.selected_market_ingest" in workflow
    assert "'coverage_mode':'link-only'" in workflow
    assert "'ingest_mode':'manual-metadata'" in workflow
    assert "--require-active-pipeline" in workflow
    assert "if: always()" in workflow
    assert "actions/upload-artifact@" in workflow
    assert "secrets.BSIDE_OPS_TOKEN" in workflow
    assert "BSIDE_ADMIN_TOKEN" not in workflow
    assert "telegram" not in workflow.casefold()
    assert "curl " not in workflow.casefold()
