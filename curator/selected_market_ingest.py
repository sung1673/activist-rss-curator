"""Config-driven, manual link-metadata ingestion for Canada and Australia.

This module never requests the configured source URLs.  It accepts only
explicitly approved official-host link metadata, revalidates collection and
public metadata rights before and after each right-scoped batch, and sends
draft observations to the v2 review queue.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import ipaddress
import json
import os
import re
import sys
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping, Protocol, Sequence
from urllib.parse import parse_qsl, urlsplit

from .global_connectors import (
    AUSTRALIA_ASIC_DESCRIPTOR,
    CANADA_IR_DESCRIPTOR,
    GlobalConnectorEnvelope,
    GlobalDocumentRecord,
    IssuerReference,
    global_document_content_hash,
)
from .global_ingest import (
    GlobalIngestError,
    GlobalIngestChunk,
    GlobalIngestReceipt,
    V2GlobalIngestClient,
    _api_configuration,
    _canonical_json,
    _validate_code_revision,
    content_idempotency_key,
    write_evidence,
)
from .global_market import (
    CoverageMode,
    SourceConnectorRecord,
    global_issuer_id,
)
from .official_source_rights import (
    GlobalOfficialSourceRightClient,
    OfficialSourceRightEligibility,
)


SUPPORTED_SELECTED_COUNTRIES = ("CA", "AU")
MAX_ISSUERS_PER_COUNTRY = 50
MAX_RECORDS_PER_COUNTRY = 500
MAX_APPROVED_HOSTS_PER_COUNTRY = 50
SELECTED_SOURCE_RIGHTS = {
    "CA": "official:ca-issuer-ir",
    "AU": "official:asic-register",
}
MANUAL_LINK_EVENT_FAMILIES = {
    "CA": (
        "meeting_and_vote",
        "tender_offer_and_mna",
        "capital_return",
        "board_and_compensation",
    ),
    "AU": (
        "board_and_compensation",
        "listing_status",
    ),
}
_RIGHT_ID = re.compile(r"^official:[a-z0-9_.:-]{1,48}$")
_IDENTIFIER_TYPE = re.compile(r"^[A-Z][A-Z0-9_]{1,39}$")
_NAMESPACE = re.compile(r"^[A-Z][A-Z0-9_:.-]{1,63}$")
_LANGUAGE = re.compile(r"^[a-z]{2,3}(?:-[A-Z]{2})?$")
_REVISION = re.compile(r"^[a-f0-9]{64}$")
_EVIDENCE_SHA256 = re.compile(r"^[a-f0-9]{64}$")
_HOSTNAME = re.compile(
    r"^(?=.{4,253}$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z]{2,63}$"
)
_DISALLOWED_HOST_SUFFIXES = (
    ".corp",
    ".example",
    ".home",
    ".internal",
    ".invalid",
    ".lan",
    ".local",
    ".localhost",
    ".test",
)
_CA_DISALLOWED_PORTAL_HOSTS = (
    "sedar.com",
    "sedarplus.ca",
    "asx.com.au",
    "asic.gov.au",
    "data.gov.au",
)
_QUERY_CREDENTIAL_MARKERS = (
    "access",
    "auth",
    "credential",
    "key",
    "secret",
    "sig",
    "signature",
    "token",
)
_TOP_LEVEL_KEYS = {"schema_version", "approved_hosts", "records"}
_APPROVED_HOST_KEYS = {
    "hostname",
    "issuer_identifier_type",
    "issuer_identifier",
    "evidence_sha256",
}
_REQUIRED_KEYS = {
    "country_code",
    "issuer_identifier_type",
    "issuer_identifier",
    "issuer_name",
    "source_right_id",
    "official_host",
    "original_url",
    "title",
    "original_language",
    "filed_at",
    "first_observed_at",
    "event_family",
}
_OPTIONAL_KEYS = {
    "issuer_namespace",
    "market",
    "ticker",
    "external_id",
    "document_type",
}


class SelectedMarketIngestError(RuntimeError):
    """A controlled error whose code is safe to include in evidence."""

    def __init__(self, code: str, *, http_status: int | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.http_status = http_status


class SelectedMarketConfigurationError(SelectedMarketIngestError):
    pass


class SelectedMarketRightsError(SelectedMarketIngestError):
    pass


class SelectedMarketApiError(SelectedMarketIngestError):
    pass


class RightsClient(Protocol):
    def check(
        self,
        source_right_id: str,
        *,
        use: str = "collect",
    ) -> OfficialSourceRightEligibility: ...


class IngestClient(Protocol):
    def submit(
        self,
        *,
        envelope: GlobalConnectorEnvelope,
        chunk: GlobalIngestChunk,
        idempotency_key: str,
        code_revision: str,
    ) -> GlobalIngestReceipt: ...


@dataclass(frozen=True)
class SelectedOfficialLink:
    country_code: str
    issuer_reference: IssuerReference
    source_right_id: str
    official_host: str
    host_evidence_sha256: str
    original_url: str
    title: str
    original_language: str
    filed_at: str
    first_observed_at: str
    event_family: str
    external_id: str
    document_type: str

    @property
    def issuer_id(self) -> str:
        return global_issuer_id(
            self.country_code,
            self.issuer_reference.namespace,
            self.issuer_reference.value,
        )


@dataclass(frozen=True)
class SourceRightGrant:
    source_right_id: str
    rights_revision: str
    source_type: str
    source_key: str


@dataclass(frozen=True)
class ApprovedHostProvenance:
    hostname: str
    issuer_identifier_type: str
    issuer_identifier: str
    evidence_sha256: str


@dataclass(frozen=True)
class SelectedBatchResult:
    source_right_id: str
    ingest_id: str
    idempotency_key: str
    raw_count: int
    acknowledged_count: int
    idempotent: bool

    def evidence(self) -> dict[str, object]:
        return {
            "source_right_id": self.source_right_id,
            "ingest_id": self.ingest_id,
            "idempotency_key": self.idempotency_key,
            "raw_count": self.raw_count,
            "acknowledged_count": self.acknowledged_count,
            "idempotent": self.idempotent,
        }


@dataclass(frozen=True)
class SelectedMarketResult:
    country_code: str
    connector_id: str
    code_revision: str
    issuer_count: int
    record_count: int
    batches: tuple[SelectedBatchResult, ...]

    def evidence(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "status": "succeeded",
            "coverage_mode": "link-only",
            "ingest_mode": "manual-metadata",
            "country_code": self.country_code,
            "connector_id": self.connector_id,
            "code_revision": self.code_revision,
            "issuer_count": self.issuer_count,
            "record_count": self.record_count,
            "source_right_count": len(self.batches),
            "submitted_batch_count": len(self.batches),
            "acknowledged_count": sum(
                batch.acknowledged_count for batch in self.batches
            ),
            "metadata_only": True,
            "source_urls_requested": 0,
            "body_storage": False,
            "batches": [batch.evidence() for batch in self.batches],
        }


def _timestamp(value: object, field_name: str) -> tuple[datetime, str]:
    text = str(value or "").strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise SelectedMarketConfigurationError(
            f"invalid_{field_name}"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SelectedMarketConfigurationError(f"invalid_{field_name}")
    value_utc = parsed.astimezone(timezone.utc).replace(microsecond=0)
    return value_utc, value_utc.isoformat()


def _required_text(
    item: Mapping[str, object],
    key: str,
    *,
    maximum: int,
) -> str:
    value = item.get(key)
    if not isinstance(value, str):
        raise SelectedMarketConfigurationError("malformed_selected_link")
    text = value
    if (
        not text
        or text != text.strip()
        or len(text) > maximum
        or any(ord(character) < 32 for character in text)
    ):
        raise SelectedMarketConfigurationError("malformed_selected_link")
    return text


def _optional_text(
    item: Mapping[str, object],
    key: str,
    *,
    maximum: int,
    default: str,
) -> str:
    if key not in item:
        return default
    return _required_text(item, key, maximum=maximum)


def _host_matches_suffix(hostname: str, suffix: str) -> bool:
    return hostname == suffix or hostname.endswith(f".{suffix}")


def _validated_hostname(value: object, *, country_code: str) -> str:
    if not isinstance(value, str):
        raise SelectedMarketConfigurationError("invalid_approved_host")
    hostname = value.strip().casefold().rstrip(".")
    if (
        value != hostname
        or _HOSTNAME.fullmatch(hostname) is None
        or hostname == "localhost"
        or hostname.endswith(_DISALLOWED_HOST_SUFFIXES)
    ):
        raise SelectedMarketConfigurationError("invalid_approved_host")
    if country_code == "AU" and not _host_matches_suffix(
        hostname,
        "asic.gov.au",
    ):
        raise SelectedMarketConfigurationError(
            "au_host_must_be_official_asic"
        )
    if country_code == "CA" and any(
        _host_matches_suffix(hostname, blocked)
        for blocked in _CA_DISALLOWED_PORTAL_HOSTS
    ):
        raise SelectedMarketConfigurationError(
            "ca_host_must_be_issuer_controlled"
        )
    return hostname


def _query_key_is_sensitive(name: str) -> bool:
    lowered = name.casefold()
    compact = re.sub(r"[^a-z0-9]+", "", lowered)
    return (
        lowered.startswith(("x-amz-", "x-goog-"))
        or compact.startswith(("xamz", "xgoog"))
        or any(marker in compact for marker in _QUERY_CREDENTIAL_MARKERS)
    )


def _official_https_url(
    value: str,
    *,
    expected_host: str,
    country_code: str,
) -> str:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise SelectedMarketConfigurationError(
            "invalid_official_source_url"
        ) from exc
    hostname = (parsed.hostname or "").casefold().rstrip(".")
    if (
        parsed.scheme != "https"
        or not hostname
        or hostname != expected_host
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or port not in (None, 443)
        or "." not in hostname
        or hostname == "localhost"
        or hostname.endswith(_DISALLOWED_HOST_SUFFIXES)
    ):
        raise SelectedMarketConfigurationError(
            "invalid_official_source_url"
        )
    try:
        address = ipaddress.ip_address(hostname.strip("[]"))
    except ValueError:
        address = None
    if address is not None and (
        not address.is_global
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_private
        or address.is_reserved
        or address.is_unspecified
    ):
        raise SelectedMarketConfigurationError(
            "private_official_source_url"
        )
    if parsed.query or "?" in value:
        for name, _ in parse_qsl(
            parsed.query,
            keep_blank_values=True,
            strict_parsing=False,
        ):
            if _query_key_is_sensitive(name):
                raise SelectedMarketConfigurationError(
                    "credential_in_official_source_url"
                )
        raise SelectedMarketConfigurationError(
            "query_not_allowed_in_official_source_url"
        )
    if country_code == "AU" and not _host_matches_suffix(
        hostname,
        "asic.gov.au",
    ):
        raise SelectedMarketConfigurationError(
            "au_host_must_be_official_asic"
        )
    if country_code == "CA" and any(
        _host_matches_suffix(hostname, blocked)
        for blocked in _CA_DISALLOWED_PORTAL_HOSTS
    ):
        raise SelectedMarketConfigurationError(
            "ca_host_must_be_issuer_controlled"
        )
    return value


def _descriptor(country_code: str, source_right_id: str) -> SourceConnectorRecord:
    if country_code == "CA":
        return replace(
            CANADA_IR_DESCRIPTOR,
            source_right_id=source_right_id,
            source_name="Canadian issuer IR manual link metadata",
            base_url="https://www.canada.ca",
            coverage_mode=CoverageMode.LINK_ONLY,
            schedule_minutes=30,
        )
    return replace(
        AUSTRALIA_ASIC_DESCRIPTOR,
        source_right_id=source_right_id,
        source_name="ASIC manual link metadata",
        base_url="https://www.asic.gov.au",
        coverage_mode=CoverageMode.LINK_ONLY,
        schedule_minutes=30,
    )


def parse_selected_official_links(
    raw: str,
    *,
    country_code: str,
) -> tuple[SelectedOfficialLink, ...]:
    """Parse exact manual metadata without fetching any configured source URL."""

    country = str(country_code or "").strip().upper()
    if country not in SUPPORTED_SELECTED_COUNTRIES:
        raise SelectedMarketConfigurationError(
            "unsupported_selected_market_country"
        )
    text = str(raw or "").strip()
    if not text:
        return ()
    try:
        payload = json.loads(text)
    except (TypeError, ValueError) as exc:
        raise SelectedMarketConfigurationError(
            "invalid_selected_market_json"
        ) from exc
    if (
        not isinstance(payload, dict)
        or set(payload) != _TOP_LEVEL_KEYS
        or payload.get("schema_version") != 1
        or not isinstance(payload.get("approved_hosts"), list)
        or not isinstance(payload.get("records"), list)
    ):
        raise SelectedMarketConfigurationError(
            "invalid_selected_market_json"
        )
    raw_hosts = payload["approved_hosts"]
    raw_records = payload["records"]
    assert isinstance(raw_hosts, list)
    assert isinstance(raw_records, list)
    if len(raw_hosts) > MAX_APPROVED_HOSTS_PER_COUNTRY:
        raise SelectedMarketConfigurationError(
            "approved_host_limit_exceeded"
        )
    if len(raw_records) > MAX_RECORDS_PER_COUNTRY:
        raise SelectedMarketConfigurationError(
            "selected_market_record_limit_exceeded"
        )
    # Reject a cross-market payload before interpreting its market-specific
    # hostname policy. This keeps the configuration error deterministic and
    # prevents a foreign row from being mistaken for a local host violation.
    for raw_record in raw_records:
        if (
            isinstance(raw_record, dict)
            and isinstance(raw_record.get("country_code"), str)
            and str(raw_record["country_code"]).strip().upper() != country
        ):
            raise SelectedMarketConfigurationError(
                "selected_link_country_mismatch"
            )

    approved_hosts: dict[
        tuple[str, str, str],
        ApprovedHostProvenance,
    ] = {}
    ca_host_owners: dict[str, tuple[str, str]] = {}
    for raw_host in raw_hosts:
        if (
            not isinstance(raw_host, dict)
            or set(raw_host) != _APPROVED_HOST_KEYS
        ):
            raise SelectedMarketConfigurationError(
                "malformed_approved_host"
            )
        host_item: Mapping[str, object] = raw_host
        hostname = _validated_hostname(
            host_item.get("hostname"),
            country_code=country,
        )
        identifier_type = _required_text(
            host_item,
            "issuer_identifier_type",
            maximum=40,
        ).upper()
        if _IDENTIFIER_TYPE.fullmatch(identifier_type) is None:
            raise SelectedMarketConfigurationError(
                "invalid_issuer_identifier_type"
            )
        identifier = _required_text(
            host_item,
            "issuer_identifier",
            maximum=191,
        )
        evidence_sha256 = _required_text(
            host_item,
            "evidence_sha256",
            maximum=64,
        )
        if _EVIDENCE_SHA256.fullmatch(evidence_sha256) is None:
            raise SelectedMarketConfigurationError(
                "invalid_host_provenance_evidence"
            )
        key = (identifier_type, identifier, hostname)
        if key in approved_hosts:
            raise SelectedMarketConfigurationError(
                "duplicate_approved_host"
            )
        if country == "CA":
            owner = (identifier_type, identifier)
            previous_owner = ca_host_owners.get(hostname)
            if previous_owner is not None and previous_owner != owner:
                raise SelectedMarketConfigurationError(
                    "ca_host_not_issuer_unique"
                )
            ca_host_owners[hostname] = owner
        approved_hosts[key] = ApprovedHostProvenance(
            hostname=hostname,
            issuer_identifier_type=identifier_type,
            issuer_identifier=identifier,
            evidence_sha256=evidence_sha256,
        )

    if not raw_records:
        if approved_hosts:
            raise SelectedMarketConfigurationError(
                "unused_approved_host"
            )
        return ()

    records: list[SelectedOfficialLink] = []
    issuer_references: dict[str, IssuerReference] = {}
    external_ids: set[str] = set()
    source_urls: set[str] = set()
    used_approved_hosts: set[tuple[str, str, str]] = set()
    for raw_item in raw_records:
        if (
            not isinstance(raw_item, dict)
            or set(raw_item) - (_REQUIRED_KEYS | _OPTIONAL_KEYS)
            or not _REQUIRED_KEYS.issubset(raw_item)
        ):
            raise SelectedMarketConfigurationError(
                "malformed_selected_link"
            )
        item: Mapping[str, object] = raw_item
        row_country = _required_text(
            item,
            "country_code",
            maximum=2,
        ).upper()
        if row_country != country:
            raise SelectedMarketConfigurationError(
                "selected_link_country_mismatch"
            )
        identifier_type = _required_text(
            item,
            "issuer_identifier_type",
            maximum=40,
        ).upper()
        if _IDENTIFIER_TYPE.fullmatch(identifier_type) is None:
            raise SelectedMarketConfigurationError(
                "invalid_issuer_identifier_type"
            )
        identifier = _required_text(
            item,
            "issuer_identifier",
            maximum=191,
        )
        issuer_name = _required_text(
            item,
            "issuer_name",
            maximum=255,
        )
        namespace = _optional_text(
            item,
            "issuer_namespace",
            maximum=64,
            default=f"{country}:OFFICIAL",
        ).upper()
        if (
            _NAMESPACE.fullmatch(namespace) is None
            or not namespace.startswith(f"{country}:")
        ):
            raise SelectedMarketConfigurationError(
                "invalid_issuer_namespace"
            )
        market = _optional_text(
            item,
            "market",
            maximum=40,
            default="TSX" if country == "CA" else "ASX",
        )
        ticker = _optional_text(
            item,
            "ticker",
            maximum=24,
            default="",
        )
        issuer = IssuerReference(
            namespace=namespace,
            identifier_type=identifier_type,
            value=identifier,
            legal_name=issuer_name,
            market=market,
            ticker=ticker,
        )
        issuer_id = global_issuer_id(country, namespace, identifier)
        previous_issuer = issuer_references.get(issuer_id)
        if previous_issuer is not None and previous_issuer != issuer:
            raise SelectedMarketConfigurationError(
                "conflicting_issuer_metadata"
            )
        issuer_references[issuer_id] = issuer
        if len(issuer_references) > MAX_ISSUERS_PER_COUNTRY:
            raise SelectedMarketConfigurationError(
                "selected_market_issuer_limit_exceeded"
            )

        source_right_id = _required_text(
            item,
            "source_right_id",
            maximum=64,
        ).casefold()
        if _RIGHT_ID.fullmatch(source_right_id) is None:
            raise SelectedMarketConfigurationError(
                "invalid_source_right_id"
            )
        if source_right_id != SELECTED_SOURCE_RIGHTS[country]:
            raise SelectedMarketConfigurationError(
                "selected_market_source_right_mismatch"
            )
        official_host = _validated_hostname(
            _required_text(
                item,
                "official_host",
                maximum=253,
            ),
            country_code=country,
        )
        approved_host_key = (
            identifier_type,
            identifier,
            official_host,
        )
        approved_host = approved_hosts.get(approved_host_key)
        if approved_host is None:
            raise SelectedMarketConfigurationError(
                "official_host_not_approved_for_issuer"
            )
        used_approved_hosts.add(approved_host_key)
        original_url = _official_https_url(
            _required_text(item, "original_url", maximum=4096),
            expected_host=official_host,
            country_code=country,
        )
        if original_url in source_urls:
            raise SelectedMarketConfigurationError(
                "duplicate_selected_link"
            )
        source_urls.add(original_url)
        title = _required_text(item, "title", maximum=700)
        language = _required_text(
            item,
            "original_language",
            maximum=8,
        )
        if _LANGUAGE.fullmatch(language) is None:
            raise SelectedMarketConfigurationError(
                "invalid_original_language"
            )
        filed_datetime, filed_at = _timestamp(
            item.get("filed_at"),
            "filed_at",
        )
        observed_datetime, first_observed_at = _timestamp(
            item.get("first_observed_at"),
            "first_observed_at",
        )
        if observed_datetime < filed_datetime:
            raise SelectedMarketConfigurationError(
                "first_observed_before_filed"
            )
        event_family = _required_text(
            item,
            "event_family",
            maximum=40,
        )
        if event_family not in MANUAL_LINK_EVENT_FAMILIES[country]:
            raise SelectedMarketConfigurationError(
                "unsupported_selected_event_family"
            )
        external_id = _optional_text(
            item,
            "external_id",
            maximum=191,
            default=(
                "link:"
                + hashlib.sha256(
                    (
                        country
                        + "\x1f"
                        + issuer_id
                        + "\x1f"
                        + original_url
                    ).encode("utf-8")
                ).hexdigest()[:40]
            ),
        )
        if external_id in external_ids:
            raise SelectedMarketConfigurationError(
                "duplicate_selected_link"
            )
        external_ids.add(external_id)
        document_type = _optional_text(
            item,
            "document_type",
            maximum=80,
            default=(
                "issuer_ir_link"
                if country == "CA"
                else "asic_register_link"
            ),
        )
        records.append(
            SelectedOfficialLink(
                country_code=country,
                issuer_reference=issuer,
                source_right_id=source_right_id,
                official_host=official_host,
                host_evidence_sha256=approved_host.evidence_sha256,
                original_url=original_url,
                title=title,
                original_language=language,
                filed_at=filed_at,
                first_observed_at=first_observed_at,
                event_family=event_family,
                external_id=external_id,
                document_type=document_type,
            )
        )
    unused_approved_hosts = set(approved_hosts) - used_approved_hosts
    if unused_approved_hosts:
        raise SelectedMarketConfigurationError(
            "unused_approved_host"
        )
    return tuple(
        sorted(
            records,
            key=lambda record: (
                record.source_right_id,
                record.filed_at,
                record.external_id,
            ),
        )
    )


def _eligibility_checked_at(
    eligibility: OfficialSourceRightEligibility,
) -> datetime:
    checked_at, _ = _timestamp(
        eligibility.checked_at,
        "source_right_checked_at",
    )
    age_seconds = (
        datetime.now(timezone.utc) - checked_at
    ).total_seconds()
    if age_seconds < -30 or age_seconds > 300:
        raise SelectedMarketRightsError("stale_source_right_grant")
    return checked_at


def _validate_eligibility(
    eligibility: OfficialSourceRightEligibility,
    *,
    source_right_id: str,
    descriptor: SourceConnectorRecord,
    use: str,
) -> SourceRightGrant:
    _eligibility_checked_at(eligibility)
    if (
        eligibility.source_right_id != source_right_id
        or eligibility.use != use
        or _REVISION.fullmatch(eligibility.rights_revision or "") is None
        or eligibility.source_type != descriptor.source_type
        or eligibility.source_key != descriptor.source_key
        or (
            use == "public"
            and eligibility.redistribution_allowed is not True
        )
    ):
        raise SelectedMarketRightsError(
            "source_right_contract_mismatch"
        )
    return SourceRightGrant(
        source_right_id=source_right_id,
        rights_revision=eligibility.rights_revision,
        source_type=str(eligibility.source_type),
        source_key=str(eligibility.source_key),
    )


def _check_grant_pair(
    rights_client: RightsClient,
    *,
    source_right_id: str,
    descriptor: SourceConnectorRecord,
    expected_revision: str | None = None,
) -> SourceRightGrant:
    collect = _validate_eligibility(
        rights_client.check(source_right_id, use="collect"),
        source_right_id=source_right_id,
        descriptor=descriptor,
        use="collect",
    )
    public = _validate_eligibility(
        rights_client.check(source_right_id, use="public"),
        source_right_id=source_right_id,
        descriptor=descriptor,
        use="public",
    )
    if not hmac.compare_digest(
        collect.rights_revision,
        public.rights_revision,
    ):
        raise SelectedMarketRightsError(
            "source_right_revision_mismatch"
        )
    if expected_revision is not None and not hmac.compare_digest(
        collect.rights_revision,
        expected_revision,
    ):
        raise SelectedMarketRightsError(
            "source_right_changed_during_ingest"
        )
    return collect


def _record_id(
    *,
    connector_id: str,
    issuer_id: str,
    external_id: str,
) -> str:
    digest = hashlib.sha256(
        (
            connector_id
            + "\x1f"
            + issuer_id
            + "\x1f"
            + external_id
        ).encode("utf-8")
    ).hexdigest()
    return f"globaldoc:{digest[:40]}"


def _document_record(
    link: SelectedOfficialLink,
    *,
    descriptor: SourceConnectorRecord,
) -> GlobalDocumentRecord:
    issuer_id = link.issuer_id
    record = GlobalDocumentRecord(
        record_id=_record_id(
            connector_id=descriptor.connector_id,
            issuer_id=issuer_id,
            external_id=link.external_id,
        ),
        external_id=link.external_id,
        issuer_id=issuer_id,
        issuer_reference=link.issuer_reference,
        country_code=link.country_code,
        source_key=descriptor.source_key,
        source_right_id=link.source_right_id,
        record_kind="link",
        document_type=link.document_type,
        event_family=link.event_family,
        title=link.title,
        original_language=link.original_language,
        filed_at=link.filed_at,
        first_observed_at=link.first_observed_at,
        original_url=link.original_url,
        content_hash="0" * 64,
        body_text=None,
        metadata={
            "approved_link_only": True,
            "ingest_mode": "manual-metadata",
            "title_provenance": "operator_metadata",
            "official_host": link.official_host,
            "host_provenance_evidence_sha256": (
                link.host_evidence_sha256
            ),
            "source_url_requested": False,
        },
    )
    return replace(
        record,
        content_hash=global_document_content_hash(
            record,
            source_type=descriptor.source_type,
            public_allowed=True,
            ai_allowed=False,
        ),
    )


def execute_selected_market_ingest(
    *,
    country_code: str,
    links: tuple[SelectedOfficialLink, ...],
    code_revision: str,
    rights_client: RightsClient,
    ingest_client: IngestClient,
) -> SelectedMarketResult:
    """Validate all rights, build right-scoped envelopes, and require exact ACKs."""

    country = str(country_code or "").strip().upper()
    if country not in SUPPORTED_SELECTED_COUNTRIES:
        raise SelectedMarketConfigurationError(
            "unsupported_selected_market_country"
        )
    if not links:
        raise SelectedMarketConfigurationError(
            "empty_selected_market_links"
        )
    if any(link.country_code != country for link in links):
        raise SelectedMarketConfigurationError(
            "selected_link_country_mismatch"
        )
    if any(
        link.source_right_id != SELECTED_SOURCE_RIGHTS[country]
        for link in links
    ):
        raise SelectedMarketConfigurationError(
            "selected_market_source_right_mismatch"
        )
    if len({link.issuer_id for link in links}) > MAX_ISSUERS_PER_COUNTRY:
        raise SelectedMarketConfigurationError(
            "selected_market_issuer_limit_exceeded"
        )
    revision = _validate_code_revision(code_revision)
    grouped: dict[str, list[SelectedOfficialLink]] = {}
    for link in links:
        grouped.setdefault(link.source_right_id, []).append(link)

    # Every distinct right is checked once before its batch is transformed.
    initial_grants: dict[str, SourceRightGrant] = {}
    for source_right_id in sorted(grouped):
        descriptor = _descriptor(country, source_right_id)
        initial_grants[source_right_id] = _check_grant_pair(
            rights_client,
            source_right_id=source_right_id,
            descriptor=descriptor,
        )

    batches: list[SelectedBatchResult] = []
    for source_right_id in sorted(grouped):
        descriptor = _descriptor(country, source_right_id)
        initial_grant = initial_grants[source_right_id]
        records = [
            _document_record(link, descriptor=descriptor)
            for link in grouped[source_right_id]
        ]
        records.sort(key=lambda record: (record.filed_at, record.record_id))
        retrieved_at = max(
            record.first_observed_at for record in records
        )
        window_start = datetime.fromisoformat(
            retrieved_at.replace("Z", "+00:00")
        ).date()
        window_end_exclusive = window_start + timedelta(days=1)
        envelope = GlobalConnectorEnvelope(
            schema_version=1,
            connector_id=descriptor.connector_id,
            country_code=country,
            source_right_id=source_right_id,
            rights_revision=initial_grant.rights_revision,
            retrieved_at=retrieved_at,
            coverage_mode=CoverageMode.LINK_ONLY,
            records=tuple(records),
            next_cursor=None,
            exhausted=True,
            request_count=0,
            raw_count=len(records),
            public_allowed=True,
            ai_allowed=False,
        )
        # One post-batch pair closes expiry/revocation races without making
        # eligibility HTTP calls proportional to the configured record count.
        final_grant = _check_grant_pair(
            rights_client,
            source_right_id=source_right_id,
            descriptor=descriptor,
            expected_revision=initial_grant.rights_revision,
        )
        if not hmac.compare_digest(
            envelope.rights_revision,
            final_grant.rights_revision,
        ):
            raise SelectedMarketRightsError(
                "source_right_changed_before_ingest"
            )
        idempotency_key = content_idempotency_key(
            envelope=envelope,
            code_revision=revision,
        )
        receipt = ingest_client.submit(
            envelope=envelope,
            chunk=GlobalIngestChunk(
                index=1,
                count=1,
                batch_raw_count=len(records),
                batch_acknowledged_count=len(records),
                batch_request_count=0,
                batch_id="global-batch:" + hashlib.sha256(
                    idempotency_key.encode("utf-8")
                ).hexdigest(),
                window_start=window_start.isoformat(),
                window_end_exclusive=window_end_exclusive.isoformat(),
            ),
            idempotency_key=idempotency_key,
            code_revision=revision,
        )
        expected_acknowledged = len(records)
        if (
            receipt.api_version != "v2"
            or receipt.connector_id != descriptor.connector_id
            or receipt.raw_count != len(records)
            or receipt.acknowledged_count != expected_acknowledged
        ):
            raise SelectedMarketApiError(
                "api_acknowledgment_mismatch"
            )
        batches.append(
            SelectedBatchResult(
                source_right_id=source_right_id,
                ingest_id=receipt.ingest_id,
                idempotency_key=idempotency_key,
                raw_count=len(records),
                acknowledged_count=receipt.acknowledged_count,
                idempotent=receipt.idempotent,
            )
        )
    return SelectedMarketResult(
        country_code=country,
        connector_id=_descriptor(
            country,
            sorted(grouped)[0],
        ).connector_id,
        code_revision=revision,
        issuer_count=len({link.issuer_id for link in links}),
        record_count=len(links),
        batches=tuple(batches),
    )


def coverage_unavailable_evidence(
    *,
    country_code: str,
    code_revision: str,
    started_at: str,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "status": "coverage_unavailable",
        "coverage_mode": "link-only",
        "ingest_mode": "manual-metadata",
        "country_code": country_code,
        "code_revision": code_revision,
        "reason": "approved_official_link_config_missing",
        "issuer_count": 0,
        "record_count": 0,
        "source_right_count": 0,
        "submitted_batch_count": 0,
        "acknowledged_count": 0,
        "metadata_only": True,
        "source_urls_requested": 0,
        "body_storage": False,
        "eligible_for_release": False,
        "started_at": started_at,
        "completed_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
    }


def failure_evidence(
    *,
    country_code: str,
    code_revision: str | None,
    started_at: str,
    error: BaseException,
) -> dict[str, object]:
    code = (
        error.code
        if isinstance(
            error,
            (SelectedMarketIngestError, GlobalIngestError),
        )
        else "selected_market_ingest_failed"
    )
    payload: dict[str, object] = {
        "schema_version": 1,
        "status": "failed",
        "coverage_mode": "link-only",
        "ingest_mode": "manual-metadata",
        "country_code": country_code,
        "code_revision": code_revision,
        "metadata_only": True,
        "source_urls_requested": 0,
        "body_storage": False,
        "started_at": started_at,
        "completed_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
        "error": {
            "code": code,
            "class": type(error).__name__,
        },
    }
    if (
        isinstance(
            error,
            (SelectedMarketIngestError, GlobalIngestError),
        )
        and error.http_status is not None
    ):
        error_payload = payload["error"]
        assert isinstance(error_payload, dict)
        error_payload["http_status"] = error.http_status
    return payload


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest explicitly approved CA/AU manual official-link metadata "
            "into the v2 review queue."
        )
    )
    parser.add_argument(
        "--country",
        required=True,
        choices=SUPPORTED_SELECTED_COUNTRIES,
    )
    parser.add_argument("--code-revision", default="")
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--require-active-pipeline", action="store_true")
    return parser


def _configured_json(
    environment: Mapping[str, str],
    country_code: str,
) -> str:
    explicit = str(environment.get("OFFICIAL_LINKS_JSON", "")).strip()
    if explicit:
        return explicit
    return str(
        environment.get(f"{country_code}_OFFICIAL_LINKS_JSON", "")
    ).strip()


def main(argv: Sequence[str] | None = None) -> int:
    args = _argument_parser().parse_args(argv)
    started_at = datetime.now(timezone.utc).replace(
        microsecond=0
    ).isoformat()
    revision: str | None = None
    try:
        if args.require_active_pipeline:
            pipeline_mode = str(
                os.environ.get("GOVERNANCE_PIPELINE_MODE", "")
            ).strip()
            if pipeline_mode not in {"shadow", "live"}:
                raise SelectedMarketConfigurationError(
                    "governance_pipeline_not_active"
                )
        revision = _validate_code_revision(
            args.code_revision or os.environ.get("GITHUB_SHA", "")
        )
        links = parse_selected_official_links(
            _configured_json(os.environ, args.country),
            country_code=args.country,
        )
        if not links:
            evidence = coverage_unavailable_evidence(
                country_code=args.country,
                code_revision=revision,
                started_at=started_at,
            )
            write_evidence(args.evidence, evidence)
            print(
                _canonical_json(
                    {
                        "ok": False,
                        "status": "coverage_unavailable",
                        "country_code": args.country,
                    }
                )
            )
            return 0
        base_url, token = _api_configuration(os.environ)
        rights_client = GlobalOfficialSourceRightClient(
            base_url=base_url,
            token=token,
        )
        ingest_client = V2GlobalIngestClient(
            base_url=base_url,
            token=token,
        )
        result = execute_selected_market_ingest(
            country_code=args.country,
            links=links,
            code_revision=revision,
            rights_client=rights_client,
            ingest_client=ingest_client,
        )
        evidence = result.evidence()
        evidence["started_at"] = started_at
        evidence["completed_at"] = datetime.now(timezone.utc).replace(
            microsecond=0
        ).isoformat()
        write_evidence(args.evidence, evidence)
        print(
            _canonical_json(
                {
                    "ok": True,
                    "country_code": result.country_code,
                    "record_count": result.record_count,
                    "acknowledged_count": sum(
                        batch.acknowledged_count
                        for batch in result.batches
                    ),
                }
            )
        )
        return 0
    except Exception as error:
        evidence = failure_evidence(
            country_code=str(args.country),
            code_revision=revision,
            started_at=started_at,
            error=error,
        )
        try:
            write_evidence(args.evidence, evidence)
        except OSError:
            pass
        error_payload = evidence["error"]
        assert isinstance(error_payload, dict)
        print(
            _canonical_json(
                {"ok": False, "error": error_payload["code"]}
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
