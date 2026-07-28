from __future__ import annotations

import base64
import calendar
import hashlib
import json
import math
import re
import time
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Iterable, Protocol, cast
from urllib.parse import parse_qsl, quote, urlparse
from zoneinfo import ZoneInfo

import httpx

from .global_market import (
    CoverageMode,
    GLOBAL_INGEST_EVENT_FAMILIES,
    SourceConnectorRecord,
    global_issuer_id,
)
from .official_source_rights import OfficialSourceRightEligibility
from .official_sources import (
    DartConnector,
    OfficialDisclosure,
    original_language,
    parse_dart_disclosure,
)


class GlobalConnectorError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.http_status = (
            http_status
            if isinstance(http_status, int)
            and not isinstance(http_status, bool)
            and 100 <= http_status <= 599
            else None
        )


class GlobalConnectorContractError(GlobalConnectorError):
    pass


class GlobalSourceRightDenied(GlobalConnectorError):
    pass


class GlobalConnectorPaginationError(GlobalConnectorError):
    pass


class GlobalConnectorIncomplete(GlobalConnectorError):
    """The official source has not finalized the requested window yet."""


TITLE_PROVENANCE_VALUES = frozenset(
    {"source", "generated_metadata", "operator_metadata"}
)


@dataclass(frozen=True)
class IssuerReference:
    namespace: str
    identifier_type: str
    value: str
    legal_name: str = ""
    market: str = ""
    ticker: str = ""

    def __post_init__(self) -> None:
        if re.fullmatch(r"[A-Z][A-Z0-9_:.-]{1,63}", self.namespace) is None:
            raise ValueError("issuer namespace is invalid")
        if re.fullmatch(r"[A-Z][A-Z0-9_]{1,39}", self.identifier_type) is None:
            raise ValueError("issuer identifier type is invalid")
        if not self.value.strip() or len(self.value) > 191:
            raise ValueError("issuer identifier value is invalid")


@dataclass(frozen=True)
class GlobalConnectorRequest:
    window_start: date
    window_end_exclusive: date
    issuers: tuple[IssuerReference, ...] = ()
    cursor: str | None = None
    page_size: int = 100
    max_pages: int = 100

    def __post_init__(self) -> None:
        if self.window_end_exclusive <= self.window_start:
            raise ValueError("connector window must be a non-empty half-open range")
        if self.page_size < 1 or self.page_size > 100:
            raise ValueError("page_size must be between 1 and 100")
        if self.max_pages < 1 or self.max_pages > 10_000:
            raise ValueError("max_pages is out of range")


@dataclass(frozen=True)
class GlobalDocumentRecord:
    record_id: str
    external_id: str
    issuer_id: str
    issuer_reference: IssuerReference
    country_code: str
    source_key: str
    source_right_id: str
    record_kind: str
    document_type: str
    event_family: str
    title: str
    original_language: str
    filed_at: str
    first_observed_at: str
    original_url: str
    content_hash: str
    body_text: str | None = None
    correction_of_external_id: str | None = None
    change_type: str = "new"
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.record_kind not in {"disclosure", "registry_filing", "link"}:
            raise ValueError("unsupported global connector record kind")
        if self.event_family not in GLOBAL_INGEST_EVENT_FAMILIES:
            raise ValueError("unsupported global event family")
        if not self.title:
            raise ValueError("record title must be non-empty")
        if re.fullmatch(r"[a-z]{2,3}(?:-[A-Z]{2})?", self.original_language) is None:
            raise ValueError("invalid source language")
        if re.fullmatch(r"[a-f0-9]{64}", self.content_hash) is None:
            raise ValueError("content_hash must be SHA-256")
        _safe_source_url(self.original_url)
        _explicit_utc(self.filed_at, "filed_at")
        _explicit_utc(self.first_observed_at, "first_observed_at")

    def public_payload(self, *, allow_body: bool) -> dict[str, Any]:
        payload = asdict(self)
        # Keep the wire representation identical to the cross-runtime
        # content-hash contract. PHP's associative JSON decoder cannot
        # distinguish a nested empty object from an empty list.
        payload["metadata"] = _cross_runtime_metadata(
            self.metadata,
            root=True,
        )
        if not allow_body:
            payload["body_text"] = None
        return payload


@dataclass(frozen=True)
class GlobalLifecycleObservation:
    observation_id: str
    country_code: str
    source_key: str
    external_id: str
    parent_external_id: str | None
    change_type: str
    observed_at: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.change_type not in {"updated", "corrected", "withdrawn"}:
            raise ValueError("unsupported lifecycle observation change_type")
        if not self.external_id or len(self.external_id) > 191:
            raise ValueError("lifecycle external_id is invalid")
        if not self.source_key:
            raise ValueError("lifecycle source_key is required")
        _explicit_utc(self.observed_at, "observed_at")


@dataclass(frozen=True)
class GlobalConnectorEnvelope:
    schema_version: int
    connector_id: str
    country_code: str
    source_right_id: str
    rights_revision: str
    retrieved_at: str
    coverage_mode: CoverageMode
    records: tuple[GlobalDocumentRecord, ...]
    next_cursor: str | None
    exhausted: bool
    request_count: int
    raw_count: int
    public_allowed: bool = False
    ai_allowed: bool = False
    lifecycle_observations: tuple[GlobalLifecycleObservation, ...] = ()
    source_manifest_sha256: str | None = None

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("unsupported connector envelope schema")
        _explicit_utc(self.retrieved_at, "retrieved_at")
        accepted_count = len(self.records) + len(self.lifecycle_observations)
        if self.request_count < 0 or self.raw_count < accepted_count:
            raise ValueError("connector envelope counts are inconsistent")
        identifiers = [record.record_id for record in self.records]
        if len(identifiers) != len(set(identifiers)):
            raise GlobalConnectorContractError("connector returned duplicate records")
        observation_ids = [
            observation.observation_id
            for observation in self.lifecycle_observations
        ]
        if len(observation_ids) != len(set(observation_ids)):
            raise GlobalConnectorContractError(
                "connector returned duplicate lifecycle observations"
            )
        if (
            self.source_manifest_sha256 is not None
            and re.fullmatch(r"[a-f0-9]{64}", self.source_manifest_sha256)
            is None
        ):
            raise GlobalConnectorContractError(
                "connector source manifest digest is invalid"
            )

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "schema_version": self.schema_version,
            "connector_id": self.connector_id,
            "country_code": self.country_code,
            "source_right_id": self.source_right_id,
            "rights_revision": self.rights_revision,
            "retrieved_at": self.retrieved_at,
            "coverage_mode": self.coverage_mode.value,
            "records": [
                record.public_payload(allow_body=self.public_allowed)
                for record in self.records
            ],
            "next_cursor": self.next_cursor,
            "exhausted": self.exhausted,
            "request_count": self.request_count,
            "raw_count": self.raw_count,
            "public_allowed": self.public_allowed,
            "ai_allowed": self.ai_allowed,
            "lifecycle_observations": [
                asdict(observation)
                for observation in self.lifecycle_observations
            ],
        }
        if self.source_manifest_sha256 is not None:
            payload["source_manifest_sha256"] = self.source_manifest_sha256
        return payload

    def to_public_payload(self) -> dict[str, Any]:
        if not self.public_allowed:
            raise GlobalSourceRightDenied(
                "source right does not permit public serialization"
            )
        payload = self.to_payload()
        payload["lifecycle_observations"] = []
        return payload


class GlobalSourceConnector(Protocol):
    descriptor: SourceConnectorRecord

    def fetch(
        self,
        request: GlobalConnectorRequest,
        *,
        eligibility: OfficialSourceRightEligibility,
        eligibility_provider: (
            Callable[[], OfficialSourceRightEligibility] | None
        ) = None,
        now: datetime | None = None,
    ) -> GlobalConnectorEnvelope: ...


def _explicit_utc(value: str | datetime, field_name: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be ISO-8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include an offset")
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _now(now: datetime | None) -> datetime:
    value = now or datetime.now(timezone.utc)
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("connector now must be timezone-aware")
    return value.astimezone(timezone.utc).replace(microsecond=0)


def _safe_source_url(value: str) -> str:
    candidate = str(value).strip()
    try:
        parsed = urlparse(candidate)
        _ = parsed.port
    except ValueError as exc:
        raise GlobalConnectorContractError("invalid source URL") from exc
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise GlobalConnectorContractError(
            "source URLs must be absolute HTTPS URLs without credentials or fragments"
        )
    return candidate


def _safe_link_only_url(value: str) -> str:
    candidate = _safe_source_url(value)
    parsed = urlparse(candidate)
    sensitive = {
        "access_token",
        "api_key",
        "apikey",
        "auth",
        "authorization",
        "key",
        "secret",
        "signature",
        "sig",
        "token",
    }
    for name, _ in parse_qsl(parsed.query, keep_blank_values=True):
        if name.casefold() in sensitive:
            raise GlobalConnectorContractError(
                "link-only source URL must not contain credentials"
            )
    return candidate


def _hash_record(*parts: object) -> str:
    value = "\x1f".join(str(part or "") for part in parts)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _cross_runtime_metadata(
    value: object,
    *,
    root: bool = False,
    depth: int = 0,
) -> object:
    if depth > 12:
        raise GlobalConnectorContractError("document metadata is too deeply nested")
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        if not -(2**63) <= value <= 2**63 - 1:
            raise GlobalConnectorContractError(
                "document metadata integer is outside signed 64-bit range"
            )
        return value
    if isinstance(value, float):
        raise GlobalConnectorContractError(
            "document metadata floats are not cross-runtime canonical"
        )
    if isinstance(value, list):
        return [
            _cross_runtime_metadata(item, depth=depth + 1)
            for item in value
        ]
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise GlobalConnectorContractError(
                "document metadata object keys must be strings"
            )
        # PHP's associative JSON decoder cannot distinguish nested empty
        # objects from empty lists. The metadata root is contractually an
        # object; nested empty containers use the shared [] representation.
        if not value and not root:
            return []
        return {
            key: _cross_runtime_metadata(child, depth=depth + 1)
            for key, child in value.items()
        }
    raise GlobalConnectorContractError(
        "document metadata contains a non-JSON value"
    )


def global_document_content_hash(
    record: GlobalDocumentRecord,
    *,
    source_type: str,
    public_allowed: bool,
    ai_allowed: bool,
) -> str:
    """Hash every canonical field that can change persisted document meaning.

    Observation time is deliberately excluded because it describes the
    collection attempt, not the source document.  The envelope rights flags
    are included because they control body retention and downstream AI use.
    """

    if not isinstance(public_allowed, bool) or not isinstance(ai_allowed, bool):
        raise GlobalConnectorContractError(
            "document rights flags must be boolean"
        )
    if not isinstance(source_type, str) or not source_type.strip():
        raise GlobalConnectorContractError("document source type is required")
    if not isinstance(record.metadata, dict):
        raise GlobalConnectorContractError("document metadata root must be an object")
    payload = {
        "schema_version": 1,
        "record_id": record.record_id,
        "external_id": record.external_id,
        "issuer_id": record.issuer_id,
        "issuer_reference": asdict(record.issuer_reference),
        "country_code": record.country_code,
        "source_key": record.source_key,
        "source_right_id": record.source_right_id,
        "source_type": source_type,
        "record_kind": record.record_kind,
        "document_type": record.document_type,
        "event_family": record.event_family,
        "title": record.title,
        "original_language": record.original_language,
        "filed_at": record.filed_at,
        "original_url": record.original_url,
        "body_text": record.body_text,
        "correction_of_external_id": record.correction_of_external_id,
        "change_type": record.change_type,
        "metadata": _cross_runtime_metadata(record.metadata, root=True),
        "public_allowed": public_allowed,
        "ai_allowed": ai_allowed,
    }
    try:
        canonical = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
        # PHP 7.3 keeps U+2028/U+2029 escaped unless the newer line-terminator
        # flag is requested. Match that stable server representation.
        canonical = canonical.replace("\u2028", "\\u2028").replace(
            "\u2029",
            "\\u2029",
        )
        encoded = canonical.encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise GlobalConnectorContractError(
            "document semantic fields must be canonical JSON"
        ) from exc
    return hashlib.sha256(encoded).hexdigest()


def _stable_record_id(connector_id: str, external_id: str) -> str:
    digest = _hash_record(connector_id, external_id)[:40]
    return f"globaldoc:{digest}"


def _require_source_right(
    descriptor: SourceConnectorRecord,
    *,
    eligibility: OfficialSourceRightEligibility,
    current: datetime,
) -> tuple[bool, bool, str]:
    checked_text = str(eligibility.checked_at or "").strip()
    try:
        checked_at = datetime.fromisoformat(
            checked_text[:-1] + "+00:00"
            if checked_text.endswith("Z")
            else checked_text
        )
    except ValueError as exc:
        raise GlobalSourceRightDenied(
            "source right eligibility checked_at is required"
        ) from exc
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        raise GlobalSourceRightDenied(
            "source right eligibility checked_at requires a timezone"
        )
    age_seconds = (
        current - checked_at.astimezone(timezone.utc)
    ).total_seconds()
    if age_seconds < -30 or age_seconds > 300:
        raise GlobalSourceRightDenied(
            "source right eligibility acknowledgment is stale"
        )
    if (
        eligibility.source_right_id != descriptor.source_right_id
        or eligibility.use != "collect"
        or re.fullmatch(r"[a-f0-9]{64}", eligibility.rights_revision) is None
        or eligibility.source_type != descriptor.source_type
        or eligibility.source_key != descriptor.source_key
    ):
        raise GlobalSourceRightDenied(
            "exact server-acknowledged source right eligibility is required"
        )
    if not descriptor.source_right_id:
        raise GlobalSourceRightDenied("connector has no registered source right")
    allow_body = eligibility.redistribution_allowed
    allow_ai = eligibility.ai_allowed
    return allow_body, allow_ai, eligibility.rights_revision


class _SourceRightGuard:
    def __init__(
        self,
        descriptor: SourceConnectorRecord,
        eligibility: OfficialSourceRightEligibility,
        *,
        initial_now: datetime,
        eligibility_provider: (
            Callable[[], OfficialSourceRightEligibility] | None
        ),
    ) -> None:
        self.descriptor = descriptor
        self.initial_eligibility = eligibility
        self.initial_now = initial_now
        self.eligibility_provider = eligibility_provider
        _, _, self.rights_revision = _require_source_right(
            descriptor,
            eligibility=eligibility,
            current=initial_now,
        )

    def assert_current(self) -> OfficialSourceRightEligibility:
        if self.eligibility_provider is None:
            eligibility = self.initial_eligibility
            current = self.initial_now
        else:
            eligibility = self.eligibility_provider()
            current = datetime.now(timezone.utc).replace(microsecond=0)
        _allow_body, _allow_ai, revision = _require_source_right(
            self.descriptor,
            eligibility=eligibility,
            current=current,
        )
        if not hashlib.sha256(revision.encode("ascii")).digest() == hashlib.sha256(
            self.rights_revision.encode("ascii")
        ).digest():
            raise GlobalSourceRightDenied(
                "source right changed during connector execution"
            )
        return eligibility


class BaseGlobalConnector:
    descriptor: SourceConnectorRecord

    def fetch(
        self,
        request: GlobalConnectorRequest,
        *,
        eligibility: OfficialSourceRightEligibility,
        eligibility_provider: (
            Callable[[], OfficialSourceRightEligibility] | None
        ) = None,
        now: datetime | None = None,
    ) -> GlobalConnectorEnvelope:
        current = _now(now)
        allow_body, allow_ai, revision = _require_source_right(
            self.descriptor,
            eligibility=eligibility,
            current=current,
        )
        rights_guard = _SourceRightGuard(
            self.descriptor,
            eligibility,
            initial_now=current,
            eligibility_provider=eligibility_provider,
        )
        envelope = self._fetch_authorized(
            request,
            rights_revision=revision,
            retrieved_at=current,
            rights_guard=rights_guard,
        )
        records = envelope.records
        if not allow_body:
            records = tuple(
                replace(
                    record,
                    body_text=None,
                    record_kind="link",
                )
                for record in records
            )
        records = tuple(
            replace(
                record,
                content_hash=global_document_content_hash(
                    record,
                    source_type=self.descriptor.source_type,
                    public_allowed=allow_body,
                    ai_allowed=allow_ai,
                ),
            )
            for record in records
        )
        return replace(
            envelope,
            public_allowed=allow_body,
            ai_allowed=allow_ai,
            records=records,
        )

    def _fetch_authorized(
        self,
        request: GlobalConnectorRequest,
        *,
        rights_revision: str,
        retrieved_at: datetime,
        rights_guard: _SourceRightGuard,
    ) -> GlobalConnectorEnvelope:
        raise NotImplementedError


def _official_record(
    *,
    connector: SourceConnectorRecord,
    issuer_reference: IssuerReference,
    external_id: str,
    record_kind: str,
    document_type: str,
    event_family: str,
    title: str,
    language: str,
    filed_at: str | datetime,
    retrieved_at: datetime,
    original_url: str,
    body_text: str | None = None,
    correction_of_external_id: str | None = None,
    change_type: str = "new",
    metadata: dict[str, Any] | None = None,
) -> GlobalDocumentRecord:
    filed = _explicit_utc(filed_at, "filed_at")
    issuer = global_issuer_id(
        connector.country_code,
        issuer_reference.namespace,
        issuer_reference.value,
    )
    metadata_value = dict(metadata or {})
    title_provenance = str(
        metadata_value.setdefault("title_provenance", "source")
    ).strip()
    if title_provenance not in TITLE_PROVENANCE_VALUES:
        raise GlobalConnectorContractError(
            "record metadata title_provenance is invalid"
        )
    record = GlobalDocumentRecord(
        record_id=_stable_record_id(
            connector.connector_id,
            f"{issuer}\x1f{external_id}",
        ),
        external_id=external_id,
        issuer_id=issuer,
        issuer_reference=issuer_reference,
        country_code=connector.country_code,
        source_key=connector.source_key,
        source_right_id=connector.source_right_id or "",
        record_kind=record_kind,
        document_type=document_type,
        event_family=event_family,
        title=title,
        original_language=language,
        filed_at=filed,
        first_observed_at=_explicit_utc(retrieved_at, "retrieved_at"),
        original_url=_safe_source_url(original_url),
        content_hash="0" * 64,
        body_text=body_text,
        correction_of_external_id=correction_of_external_id,
        change_type=change_type,
        metadata=metadata_value,
    )
    return replace(
        record,
        content_hash=global_document_content_hash(
            record,
            source_type=connector.source_type,
            public_allowed=False,
            ai_allowed=False,
        ),
    )


def _disclosure_event_family(disclosure: OfficialDisclosure) -> str | None:
    mapping = {
        "five_percent_holding": "large_ownership",
        "shareholder_proposal": "meeting_and_vote",
        "general_meeting": "meeting_and_vote",
        "tender_offer": "tender_offer_and_mna",
        "merger": "tender_offer_and_mna",
        "split": "tender_offer_and_mna",
        "duplicate_listing": "listing_status",
        "rights_issue": "capital_issuance",
        "convertible_bond": "capital_issuance",
        "bond_with_warrant": "capital_issuance",
        "exchangeable_bond": "capital_issuance",
        "dividend": "capital_return",
        "treasury_shares": "capital_return",
        "board": "board_and_compensation",
        "executive_compensation": "board_and_compensation",
        "trading_suspension": "listing_status",
        "delisting": "listing_status",
    }
    return mapping.get(disclosure.event_type.value)


class DartGlobalConnector(BaseGlobalConnector):
    descriptor = SourceConnectorRecord(
        connector_id="connector:kr:dart",
        country_code="KR",
        source_key="dart",
        source_name="OpenDART",
        source_type="official_disclosure",
        base_url="https://opendart.fss.or.kr",
        source_right_id="official:dart",
        coverage_mode=CoverageMode.MARKET_WIDE,
        schedule_minutes=15,
    )

    def __init__(self, connector: DartConnector) -> None:
        self.connector = connector

    def _fetch_authorized(
        self,
        request: GlobalConnectorRequest,
        *,
        rights_revision: str,
        retrieved_at: datetime,
        rights_guard: _SourceRightGuard,
    ) -> GlobalConnectorEnvelope:
        rights_guard.assert_current()
        before = self.connector.requests_made
        raw_count = 0
        records: list[GlobalDocumentRecord] = []
        inclusive_end = request.window_end_exclusive - timedelta(days=1)
        for row in self.connector.iter_disclosure_rows(
            request.window_start,
            inclusive_end,
            page_count=request.page_size,
            max_pages=request.max_pages,
        ):
            rights_guard.assert_current()
            raw_count += 1
            disclosure = parse_dart_disclosure(row)
            if disclosure is None:
                continue
            event_family = _disclosure_event_family(disclosure)
            if event_family is None:
                continue
            issuer_ref = IssuerReference(
                namespace="KR:DART",
                identifier_type="DART_CORP_CODE",
                value=disclosure.corp_code,
                legal_name=disclosure.corp_name,
                market=disclosure.market or "KRX",
                ticker=disclosure.stock_code,
            )
            change = (
                "withdrawn"
                if disclosure.is_cancelled
                else ("corrected" if disclosure.is_revision else "new")
            )
            records.append(
                _official_record(
                    connector=self.descriptor,
                    issuer_reference=issuer_ref,
                    external_id=disclosure.receipt_no,
                    record_kind="disclosure",
                    document_type=disclosure.event_type.value,
                    event_family=event_family,
                    title=disclosure.title,
                    language=original_language(disclosure.title),
                    filed_at=disclosure.received_at,
                    retrieved_at=retrieved_at,
                    original_url=disclosure.original_url,
                    change_type=change,
                    metadata={
                        "filer_name": disclosure.filer_name,
                        "remarks": disclosure.remarks,
                        "stock_code": disclosure.stock_code,
                        "market": disclosure.market,
                    },
                )
            )
        return GlobalConnectorEnvelope(
            schema_version=1,
            connector_id=self.descriptor.connector_id,
            country_code="KR",
            source_right_id=self.descriptor.source_right_id or "",
            rights_revision=rights_revision,
            retrieved_at=_explicit_utc(retrieved_at, "retrieved_at"),
            coverage_mode=self.descriptor.coverage_mode,
            records=tuple(sorted(records, key=lambda item: (item.filed_at, item.record_id))),
            next_cursor=None,
            exhausted=True,
            request_count=self.connector.requests_made - before,
            raw_count=raw_count,
        )


SEC_FORM_FAMILIES = {
    "SC 13D": "large_ownership",
    "SC 13D/A": "large_ownership",
    "SC 13G": "large_ownership",
    "SC 13G/A": "large_ownership",
    "DEF 14A": "meeting_and_vote",
    "PRE 14A": "meeting_and_vote",
    "DEFA14A": "meeting_and_vote",
    "DFAN14A": "meeting_and_vote",
    "DEFC14A": "meeting_and_vote",
    "PREC14A": "meeting_and_vote",
    "PRRN14A": "meeting_and_vote",
    "SC TO-I": "tender_offer_and_mna",
    "SC TO-T": "tender_offer_and_mna",
    "SC TO-C": "tender_offer_and_mna",
    "SC 14D9": "tender_offer_and_mna",
    "13E-3": "tender_offer_and_mna",
    "S-4": "tender_offer_and_mna",
    # The daily market-wide index does not expose 8-K item numbers. Preserve
    # these filings in the private review queue and require an editor to assign
    # one of the eight public families before publication.
    "8-K": "unclassified",
    "8-K/A": "unclassified",
}


def _observed_us_holiday(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _nth_weekday(year: int, month: int, weekday: int, ordinal: int) -> date:
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + (ordinal - 1) * 7)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    last_day = calendar.monthrange(year, month)[1]
    candidate = date(year, month, last_day)
    return candidate - timedelta(days=(candidate.weekday() - weekday) % 7)


def _us_federal_holidays(year: int) -> set[date]:
    holidays = {
        _observed_us_holiday(date(year, 1, 1)),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _last_weekday(year, 5, 0),
        _observed_us_holiday(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 10, 0, 2),
        _observed_us_holiday(date(year, 11, 11)),
        _nth_weekday(year, 11, 3, 4),
        _observed_us_holiday(date(year, 12, 25)),
    }
    if year >= 2021:
        holidays.add(_observed_us_holiday(date(year, 6, 19)))
    next_new_year = _observed_us_holiday(date(year + 1, 1, 1))
    if next_new_year.year == year:
        holidays.add(next_new_year)
    return holidays


def _sec_expected_daily_index(day: date) -> bool:
    return day.weekday() < 5 and day not in _us_federal_holidays(day.year)

SEC_8K_ITEM_FAMILIES = {
    "2.01": "tender_offer_and_mna",
    "3.01": "listing_status",
    "3.02": "capital_issuance",
    "5.02": "board_and_compensation",
    "5.07": "meeting_and_vote",
}

SEC_CURRENT_FILINGS_ATOM_ENDPOINT = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
)
SEC_DAILY_INDEX_BASE_URL = (
    "https://www.sec.gov/Archives/edgar/daily-index"
)
SEC_SUBMISSIONS_BASE_URL = "https://data.sec.gov/submissions"
SEC_ARCHIVES_BASE_URL = "https://www.sec.gov/Archives"
_SEC_USER_AGENT_EMAIL = re.compile(
    r"(?<![A-Z0-9._%+-])"
    r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,63}"
    r"(?![A-Z0-9._%+-])",
    re.IGNORECASE,
)


def _validated_sec_user_agent(value: str) -> str:
    """Return a declared SEC bot User-Agent with service and contact email."""

    user_agent = str(value or "").strip()
    if (
        not user_agent
        or len(user_agent) > 255
        or any(ord(character) < 32 or ord(character) == 127 for character in user_agent)
    ):
        raise ValueError(
            "SEC User-Agent must identify a service and contact email"
        )
    contact = _SEC_USER_AGENT_EMAIL.search(user_agent)
    service = user_agent[: contact.start()].strip(" ()[]<>;,:/") if contact else ""
    if contact is None or not service:
        raise ValueError(
            "SEC User-Agent must identify a service and contact email"
        )
    return user_agent


SEC_EDGAR_DESCRIPTOR = SourceConnectorRecord(
    connector_id="connector:us:sec-edgar",
    country_code="US",
    source_key="sec-edgar",
    source_name="SEC EDGAR current filings + daily index",
    source_type="official_disclosure",
    base_url="https://www.sec.gov",
    source_right_id="official:sec-edgar",
    coverage_mode=CoverageMode.MARKET_WIDE,
    schedule_minutes=30,
)

_SEC_ACCESSION = re.compile(r"^\d{10}-\d{2}-\d{6}$")
_SEC_CIK = re.compile(r"^\d{10}$")
_SEC_FILING_EXTERNAL_ID_PREFIX = "sec-accession-cik-v1:"


def _sec_filing_external_id(accession: str, cik: str) -> str:
    """Return a stable SEC identity scoped by official accession and issuer CIK."""

    if _SEC_ACCESSION.fullmatch(accession) is None:
        raise GlobalConnectorContractError("SEC accession number is invalid")
    if _SEC_CIK.fullmatch(cik) is None:
        raise GlobalConnectorContractError("SEC CIK is invalid")
    return f"{_SEC_FILING_EXTERNAL_ID_PREFIX}{accession}:{cik}"


@dataclass
class _SecRequestThrottle:
    minimum_interval: float = 0.12
    sleep: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic
    last_request_at: float | None = None

    def wait(self) -> None:
        if self.minimum_interval < 0.1:
            raise GlobalConnectorContractError(
                "SEC request interval must be at least 100 milliseconds"
            )
        current = self.clock()
        if self.last_request_at is not None:
            remaining = self.minimum_interval - (
                current - self.last_request_at
            )
            if remaining > 0:
                self.sleep(remaining)
                current = self.clock()
        self.last_request_at = current


class SecDailyIndexConnector(BaseGlobalConnector):
    """Market-wide governance filing discovery from EDGAR daily master indexes."""

    descriptor = SEC_EDGAR_DESCRIPTOR

    def __init__(
        self,
        *,
        user_agent: str,
        client: httpx.Client | None = None,
        timeout: float = 20.0,
        max_retries: int = 3,
        sleep: Callable[[float], None] = time.sleep,
        retry_sleep: Callable[[float], None] | None = None,
        clock: Callable[[], float] = time.monotonic,
        minimum_request_interval: float = 0.12,
        _throttle: _SecRequestThrottle | None = None,
    ) -> None:
        if max_retries < 0 or max_retries > 5:
            raise ValueError("SEC max_retries must be between 0 and 5")
        self.user_agent = _validated_sec_user_agent(user_agent)
        self.client = client
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_sleep = retry_sleep or sleep
        self.throttle = _throttle or _SecRequestThrottle(
            minimum_interval=minimum_request_interval,
            sleep=sleep,
            clock=clock,
        )

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> float | None:
        value = str(response.headers.get("Retry-After") or "").strip()
        if not value:
            return None
        try:
            delay = float(value)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
            except (TypeError, ValueError, OverflowError):
                return None
            if retry_at.tzinfo is None or retry_at.utcoffset() is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            delay = (
                retry_at.astimezone(timezone.utc)
                - datetime.now(timezone.utc)
            ).total_seconds()
        if not math.isfinite(delay):
            return None
        if delay < 0:
            return 0.0
        return min(delay, 60.0)

    def _get_day(
        self,
        day: date,
        *,
        before_request: Callable[[], object],
    ) -> tuple[httpx.Response, int]:
        quarter = ((day.month - 1) // 3) + 1
        url = (
            f"{SEC_DAILY_INDEX_BASE_URL}/{day.year}/QTR{quarter}/"
            f"master.{day:%Y%m%d}.idx"
        )
        headers = {
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "text/plain",
        }
        for attempt in range(self.max_retries + 1):
            before_request()
            self.throttle.wait()
            response = (
                self.client.get(
                    url,
                    headers=headers,
                    follow_redirects=False,
                )
                if self.client is not None
                else httpx.get(
                    url,
                    headers=headers,
                    timeout=self.timeout,
                    follow_redirects=False,
                )
            )
            attempts = attempt + 1
            retryable = (
                response.status_code == 429
                or 500 <= response.status_code < 600
            )
            if not retryable:
                return response, attempts
            if attempt >= self.max_retries:
                raise GlobalConnectorError(
                    "SEC EDGAR daily index request failed after retries",
                    http_status=response.status_code,
                )
            advertised = self._retry_after_seconds(response)
            delay = (
                advertised
                if advertised is not None
                else min(float(2**attempt), 8.0)
            )
            self.retry_sleep(delay)
        raise GlobalConnectorError(
            "SEC EDGAR daily index retry loop exhausted"
        )

    def _fetch_authorized(
        self,
        request: GlobalConnectorRequest,
        *,
        rights_revision: str,
        retrieved_at: datetime,
        rights_guard: _SourceRightGuard,
    ) -> GlobalConnectorEnvelope:
        day_count = (request.window_end_exclusive - request.window_start).days
        if day_count > request.max_pages:
            raise GlobalConnectorPaginationError(
                "SEC daily-index window exceeds max_pages request budget"
            )
        records_by_identity: dict[str, GlobalDocumentRecord] = {}
        raw_count = 0
        request_count = 0
        current = request.window_start
        while current < request.window_end_exclusive:
            response, attempts = self._get_day(
                current,
                before_request=rights_guard.assert_current,
            )
            request_count += attempts
            expected_daily_index = _sec_expected_daily_index(current)
            if response.status_code == 404 or (
                response.status_code == 403 and not expected_daily_index
            ):
                if expected_daily_index:
                    raise GlobalConnectorIncomplete(
                        "SEC daily index is missing for an expected filing day"
                    )
                current += timedelta(days=1)
                continue
            if response.status_code >= 400:
                raise GlobalConnectorError(
                    f"SEC EDGAR daily index HTTP {response.status_code}",
                    http_status=response.status_code,
                )
            lines = response.text.splitlines()
            # The published EDGAR master index currently spells the final
            # column ``File Name`` and emits compact YYYYMMDD filing dates.
            # Older fixtures/archives use ``Filename`` and ISO dates, so keep
            # both explicit official variants instead of loosening the parser.
            accepted_headers = {
                "CIK|Company Name|Form Type|Date Filed|File Name",
                "CIK|Company Name|Form Type|Date Filed|Filename",
            }
            header_index = next(
                (
                    index
                    for index, line in enumerate(lines)
                    if line in accepted_headers
                ),
                None,
            )
            if header_index is None:
                raise GlobalConnectorContractError(
                    "SEC daily master index header is missing"
                )
            data_lines = lines[header_index + 1 :]
            if not data_lines or re.fullmatch(r"-{20,}", data_lines[0].strip()) is None:
                raise GlobalConnectorContractError(
                    "SEC daily master index separator is missing"
                )
            for line in data_lines[1:]:
                if not line.strip():
                    continue
                columns = line.split("|")
                if len(columns) != 5:
                    raise GlobalConnectorContractError(
                        "SEC daily master index row shape is invalid"
                    )
                raw_count += 1
                cik_raw, company_name, form_raw, filed_raw, filename = (
                    value.strip() for value in columns
                )
                form = form_raw.upper()
                family = SEC_FORM_FAMILIES.get(form)
                if family is None:
                    continue
                digits = re.sub(r"\D", "", cik_raw)
                if not digits or len(digits) > 10 or not company_name:
                    raise GlobalConnectorContractError(
                        "SEC daily master issuer identity is invalid"
                    )
                try:
                    filed_day = (
                        datetime.strptime(filed_raw, "%Y%m%d").date()
                        if re.fullmatch(r"\d{8}", filed_raw)
                        else date.fromisoformat(filed_raw)
                    )
                except ValueError as exc:
                    raise GlobalConnectorContractError(
                        "SEC daily master filing date is invalid"
                    ) from exc
                # A daily dissemination index can legitimately contain a
                # late-added or reprocessed filing whose filing date predates
                # the index date. The receipt window is bound to the index
                # filename; preserve the source filing date on the document.
                # A future filing date, however, is an invalid source
                # contract and remains fail-closed.
                if filed_day > current:
                    raise GlobalConnectorContractError(
                        "SEC daily master row date exceeds requested index"
                    )
                if (
                    not filename.startswith("edgar/data/")
                    or ".." in filename
                    or "\\" in filename
                ):
                    raise GlobalConnectorContractError(
                        "SEC daily master filename is invalid"
                    )
                accession = filename.rsplit("/", 1)[-1]
                if accession.endswith(".txt"):
                    accession = accession[:-4]
                if _SEC_ACCESSION.fullmatch(accession) is None:
                    raise GlobalConnectorContractError(
                        "SEC daily master accession is invalid"
                    )
                cik = digits.zfill(10)
                reference = IssuerReference(
                    namespace="US:CIK",
                    identifier_type="CIK",
                    value=cik,
                    legal_name=company_name,
                    market="US",
                )
                record = _official_record(
                        connector=self.descriptor,
                        issuer_reference=reference,
                        external_id=_sec_filing_external_id(accession, cik),
                        record_kind="disclosure",
                        document_type=form,
                        event_family=family,
                        title=f"{form} — {company_name}",
                        language="en",
                        filed_at=datetime.combine(
                            filed_day,
                            datetime.min.time(),
                            tzinfo=ZoneInfo("America/New_York"),
                        ).astimezone(timezone.utc),
                        retrieved_at=retrieved_at,
                        original_url=f"{SEC_ARCHIVES_BASE_URL}/{quote(filename)}",
                        change_type=(
                            "corrected" if form.endswith("/A") else "new"
                        ),
                        metadata={
                            "accession_number": accession,
                            "cik": cik,
                            "form": form,
                            "filing_date": filed_raw,
                            "discovery": "daily-master-index",
                            "title_provenance": "generated_metadata",
                        },
                    )
                # SEC can re-disseminate the same accession/CIK association in
                # more than one daily index. Keep the last observed row in the
                # requested window so multi-day polling stays deterministic
                # and the envelope never emits duplicate document identities.
                records_by_identity[record.external_id] = record
            current += timedelta(days=1)
        return GlobalConnectorEnvelope(
            schema_version=1,
            connector_id=self.descriptor.connector_id,
            country_code="US",
            source_right_id=self.descriptor.source_right_id or "",
            rights_revision=rights_revision,
            retrieved_at=_explicit_utc(retrieved_at, "retrieved_at"),
            coverage_mode=self.descriptor.coverage_mode,
            records=tuple(
                sorted(
                    records_by_identity.values(),
                    key=lambda item: (item.filed_at, item.record_id),
                )
            ),
            next_cursor=None,
            exhausted=True,
            request_count=request_count,
            raw_count=raw_count,
        )


_ATOM_NAMESPACE = "http://www.w3.org/2005/Atom"
_SEC_CURRENT_CURSOR_PREFIX = "sec-current-v1:"
_SEC_CURRENT_CURSOR_OVERLAP = timedelta(minutes=90)
_SEC_CURRENT_TITLE = re.compile(
    r"^(?P<form>.+?) - (?P<company>.+) "
    r"\((?P<cik>\d{10})\) \((?P<role>[^()]{1,40})\)$"
)
_SEC_CURRENT_FORM_ALIASES = {
    "SCHEDULE 13D": "SC 13D",
    "SCHEDULE 13D/A": "SC 13D/A",
    "SCHEDULE 13G": "SC 13G",
    "SCHEDULE 13G/A": "SC 13G/A",
}
_SEC_CURRENT_ROLE_RANK = {
    "Subject": 4,
    "Filer": 3,
    "Issuer": 3,
    "Filed by": 1,
    "Reporting": 0,
}


def _sec_current_cursor(value: str | None) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text.startswith(_SEC_CURRENT_CURSOR_PREFIX):
        raise GlobalConnectorContractError("SEC current-filings cursor is invalid")
    encoded = text[len(_SEC_CURRENT_CURSOR_PREFIX) :]
    try:
        padding = "=" * (-len(encoded) % 4)
        payload = json.loads(
            base64.urlsafe_b64decode(encoded + padding).decode("utf-8")
        )
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GlobalConnectorContractError(
            "SEC current-filings cursor is invalid"
        ) from exc
    if (
        not isinstance(payload, dict)
        or set(payload) != {"schema_version", "updated_at"}
        or payload.get("schema_version") != 1
        or not isinstance(payload.get("updated_at"), str)
    ):
        raise GlobalConnectorContractError(
            "SEC current-filings cursor is invalid"
        )
    parsed = datetime.fromisoformat(payload["updated_at"].replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise GlobalConnectorContractError(
            "SEC current-filings cursor is invalid"
        )
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def _encode_sec_current_cursor(updated_at: datetime) -> str:
    payload = json.dumps(
        {
            "schema_version": 1,
            "updated_at": updated_at.astimezone(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    encoded = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    return _SEC_CURRENT_CURSOR_PREFIX + encoded


@dataclass(frozen=True)
class _SecCurrentEntry:
    accession: str
    form: str
    title: str
    company_name: str
    cik: str
    role: str
    updated_at: datetime
    original_url: str


class SecCurrentFilingsConnector(BaseGlobalConnector):
    """Market-wide intraday discovery from the SEC's official Atom feed."""

    descriptor = SEC_EDGAR_DESCRIPTOR
    endpoint = SEC_CURRENT_FILINGS_ATOM_ENDPOINT

    def __init__(
        self,
        *,
        user_agent: str,
        client: httpx.Client | None = None,
        timeout: float = 20.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        minimum_request_interval: float = 0.12,
        _throttle: _SecRequestThrottle | None = None,
    ) -> None:
        self.user_agent = _validated_sec_user_agent(user_agent)
        self.client = client
        self.timeout = timeout
        self.throttle = _throttle or _SecRequestThrottle(
            minimum_interval=minimum_request_interval,
            sleep=sleep,
            clock=clock,
        )

    def _get_page(self, *, start: int, count: int) -> httpx.Response:
        self.throttle.wait()
        headers = {
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/atom+xml, application/xml;q=0.9",
        }
        params = {
            "action": "getcurrent",
            "company": "",
            "count": str(count),
            "dateb": "",
            "output": "atom",
            "owner": "exclude",
            "start": str(start),
            "type": "",
        }
        return (
            self.client.get(
                self.endpoint,
                params=params,
                headers=headers,
                follow_redirects=False,
            )
            if self.client is not None
            else httpx.get(
                self.endpoint,
                params=params,
                headers=headers,
                timeout=self.timeout,
                follow_redirects=False,
            )
        )

    @staticmethod
    def _required_text(element: ET.Element, name: str) -> str:
        child = element.find(f"{{{_ATOM_NAMESPACE}}}{name}")
        value = "" if child is None or child.text is None else child.text.strip()
        if not value:
            raise GlobalConnectorContractError(
                f"SEC current-filings {name} is missing"
            )
        return value

    @classmethod
    def _parse_page(
        cls,
        content: bytes,
    ) -> tuple[
        datetime,
        int,
        datetime | None,
        tuple[_SecCurrentEntry, ...],
    ]:
        if not content or len(content) > 5_000_000:
            raise GlobalConnectorContractError(
                "SEC current-filings response size is invalid"
            )
        try:
            root = ET.fromstring(content)
        except ET.ParseError as exc:
            raise GlobalConnectorContractError(
                "SEC current-filings Atom is invalid"
            ) from exc
        if root.tag != f"{{{_ATOM_NAMESPACE}}}feed":
            raise GlobalConnectorContractError(
                "SEC current-filings Atom root is invalid"
            )
        feed_updated = datetime.fromisoformat(
            cls._required_text(root, "updated").replace("Z", "+00:00")
        )
        if feed_updated.tzinfo is None or feed_updated.utcoffset() is None:
            raise GlobalConnectorContractError(
                "SEC current-filings feed time is invalid"
            )
        entries: list[_SecCurrentEntry] = []
        entry_times: list[datetime] = []
        for element in root.findall(f"{{{_ATOM_NAMESPACE}}}entry"):
            updated_at = datetime.fromisoformat(
                cls._required_text(element, "updated").replace("Z", "+00:00")
            )
            if updated_at.tzinfo is None or updated_at.utcoffset() is None:
                raise GlobalConnectorContractError(
                    "SEC current-filings entry time is invalid"
                )
            updated_at = updated_at.astimezone(timezone.utc).replace(
                microsecond=0
            )
            entry_times.append(updated_at)
            category = element.find(f"{{{_ATOM_NAMESPACE}}}category")
            source_form = "" if category is None else str(category.get("term") or "").strip()
            if not source_form:
                raise GlobalConnectorContractError(
                    "SEC current-filings form is missing"
                )
            normalized_form = _SEC_CURRENT_FORM_ALIASES.get(
                source_form,
                source_form,
            )
            if normalized_form not in SEC_FORM_FAMILIES:
                continue
            title = cls._required_text(element, "title")
            match = _SEC_CURRENT_TITLE.fullmatch(title)
            if match is None:
                raise GlobalConnectorContractError(
                    "SEC current-filings title shape is invalid"
                )
            entry_id = cls._required_text(element, "id")
            prefix = "urn:tag:sec.gov,2008:accession-number="
            accession = entry_id[len(prefix) :] if entry_id.startswith(prefix) else ""
            if _SEC_ACCESSION.fullmatch(accession) is None:
                raise GlobalConnectorContractError(
                    "SEC current-filings accession is invalid"
                )
            title_form = match.group("form").strip()
            if source_form != title_form:
                raise GlobalConnectorContractError(
                    "SEC current-filings title and category form differ"
                )
            link = next(
                (
                    child
                    for child in element.findall(f"{{{_ATOM_NAMESPACE}}}link")
                    if child.get("rel") == "alternate"
                ),
                None,
            )
            original_url = "" if link is None else str(link.get("href") or "").strip()
            parsed_url = urlparse(_safe_source_url(original_url))
            if (
                parsed_url.hostname != "www.sec.gov"
                or not parsed_url.path.startswith("/Archives/edgar/data/")
                or not parsed_url.path.endswith("-index.htm")
                or parsed_url.query
            ):
                raise GlobalConnectorContractError(
                    "SEC current-filings document URL is invalid"
                )
            entries.append(
                _SecCurrentEntry(
                    accession=accession,
                    form=normalized_form,
                    title=title,
                    company_name=match.group("company").strip(),
                    cik=match.group("cik"),
                    role=match.group("role"),
                    updated_at=updated_at,
                    original_url=original_url,
                )
            )
        if any(left < right for left, right in zip(entry_times, entry_times[1:])):
            raise GlobalConnectorContractError(
                "SEC current-filings entries are not newest-first"
            )
        return (
            feed_updated.astimezone(timezone.utc).replace(microsecond=0),
            len(entry_times),
            entry_times[-1] if entry_times else None,
            tuple(entries),
        )

    def _fetch_authorized(
        self,
        request: GlobalConnectorRequest,
        *,
        rights_revision: str,
        retrieved_at: datetime,
        rights_guard: _SourceRightGuard,
    ) -> GlobalConnectorEnvelope:
        previous = _sec_current_cursor(request.cursor)
        cutoff = (
            previous - _SEC_CURRENT_CURSOR_OVERLAP
            if previous is not None
            else None
        )
        page_size = 100
        raw_count = 0
        request_count = 0
        crossed_cutoff = False
        feed_high_water = previous
        candidates: dict[str, _SecCurrentEntry] = {}
        previous_oldest: datetime | None = None
        while request_count < request.max_pages:
            rights_guard.assert_current()
            response = self._get_page(
                start=request_count * page_size,
                count=page_size,
            )
            request_count += 1
            if response.status_code != 200:
                raise GlobalConnectorError(
                    f"SEC current-filings HTTP {response.status_code}"
                )
            (
                feed_updated,
                page_raw_count,
                oldest_entry,
                entries,
            ) = self._parse_page(response.content)
            if (
                previous_oldest is not None
                and entries
                and entries[0].updated_at > previous_oldest
            ):
                raise GlobalConnectorPaginationError(
                    "SEC current-filings pagination drifted"
                )
            if oldest_entry is not None:
                previous_oldest = oldest_entry
            if feed_high_water is None or feed_updated > feed_high_water:
                feed_high_water = feed_updated
            raw_count += page_raw_count
            for entry in entries:
                if cutoff is not None and entry.updated_at < cutoff:
                    crossed_cutoff = True
                    continue
                external_id = _sec_filing_external_id(
                    entry.accession,
                    entry.cik,
                )
                existing = candidates.get(external_id)
                if (
                    existing is None
                    or _SEC_CURRENT_ROLE_RANK.get(entry.role, -1)
                    > _SEC_CURRENT_ROLE_RANK.get(existing.role, -1)
                ):
                    candidates[external_id] = entry
            if (
                cutoff is not None
                and oldest_entry is not None
                and oldest_entry < cutoff
            ):
                crossed_cutoff = True
            if page_raw_count < page_size or crossed_cutoff:
                break
        else:
            raise GlobalConnectorPaginationError(
                "SEC current-filings exceeded max_pages request budget"
            )

        records: list[GlobalDocumentRecord] = []
        for entry in candidates.values():
            family = SEC_FORM_FAMILIES.get(entry.form)
            if (
                family is None
                or _SEC_CURRENT_ROLE_RANK.get(entry.role, -1) < 3
            ):
                continue
            reference = IssuerReference(
                namespace="US:CIK",
                identifier_type="CIK",
                value=entry.cik,
                legal_name=entry.company_name,
                market="US",
            )
            records.append(
                _official_record(
                    connector=self.descriptor,
                    issuer_reference=reference,
                    external_id=_sec_filing_external_id(
                        entry.accession,
                        entry.cik,
                    ),
                    record_kind="disclosure",
                    document_type=entry.form,
                    event_family=family,
                    title=entry.title,
                    language="en",
                    filed_at=entry.updated_at,
                    retrieved_at=retrieved_at,
                    original_url=entry.original_url,
                    change_type=(
                        "corrected" if entry.form.endswith("/A") else "new"
                    ),
                    metadata={
                        "accession_number": entry.accession,
                        "cik": entry.cik,
                        "form": entry.form,
                        "role": entry.role,
                        "discovery": "current-filings-atom",
                        "title_provenance": "source",
                    },
                )
            )
        return GlobalConnectorEnvelope(
            schema_version=1,
            connector_id=self.descriptor.connector_id,
            country_code="US",
            source_right_id=self.descriptor.source_right_id or "",
            rights_revision=rights_revision,
            retrieved_at=_explicit_utc(retrieved_at, "retrieved_at"),
            coverage_mode=self.descriptor.coverage_mode,
            records=tuple(
                sorted(records, key=lambda item: (item.filed_at, item.record_id))
            ),
            next_cursor=(
                _encode_sec_current_cursor(feed_high_water)
                if feed_high_water is not None
                else request.cursor
            ),
            exhausted=True,
            request_count=request_count,
            raw_count=raw_count,
        )


class SecHybridConnector(BaseGlobalConnector):
    """Fail-closed intraday Atom discovery plus completed-day reconciliation."""

    descriptor = SEC_EDGAR_DESCRIPTOR

    def __init__(
        self,
        *,
        user_agent: str,
        client: httpx.Client | None = None,
        timeout: float = 20.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        minimum_request_interval: float = 0.12,
    ) -> None:
        throttle = _SecRequestThrottle(
            minimum_interval=minimum_request_interval,
            sleep=sleep,
            clock=clock,
        )
        self.daily = SecDailyIndexConnector(
            user_agent=user_agent,
            client=client,
            timeout=timeout,
            _throttle=throttle,
        )
        self.current = SecCurrentFilingsConnector(
            user_agent=user_agent,
            client=client,
            timeout=timeout,
            _throttle=throttle,
        )

    def _fetch_authorized(
        self,
        request: GlobalConnectorRequest,
        *,
        rights_revision: str,
        retrieved_at: datetime,
        rights_guard: _SourceRightGuard,
    ) -> GlobalConnectorEnvelope:
        daily_budget = (request.window_end_exclusive - request.window_start).days
        current_budget = request.max_pages - daily_budget
        if current_budget < 1:
            raise GlobalConnectorPaginationError(
                "SEC hybrid connector has no current-filings request budget"
            )
        daily = self.daily._fetch_authorized(
            request,
            rights_revision=rights_revision,
            retrieved_at=retrieved_at,
            rights_guard=rights_guard,
        )
        current = self.current._fetch_authorized(
            replace(request, max_pages=current_budget),
            rights_revision=rights_revision,
            retrieved_at=retrieved_at,
            rights_guard=rights_guard,
        )
        by_external_id = {
            record.external_id: record for record in daily.records
        }
        # The Atom record is authoritative for source title, exact acceptance
        # time and filing-index URL when both discovery paths overlap.
        by_external_id.update(
            {record.external_id: record for record in current.records}
        )
        return GlobalConnectorEnvelope(
            schema_version=1,
            connector_id=self.descriptor.connector_id,
            country_code="US",
            source_right_id=self.descriptor.source_right_id or "",
            rights_revision=rights_revision,
            retrieved_at=_explicit_utc(retrieved_at, "retrieved_at"),
            coverage_mode=self.descriptor.coverage_mode,
            records=tuple(
                sorted(
                    by_external_id.values(),
                    key=lambda item: (item.filed_at, item.record_id),
                )
            ),
            next_cursor=current.next_cursor,
            exhausted=daily.exhausted and current.exhausted,
            request_count=daily.request_count + current.request_count,
            raw_count=daily.raw_count + current.raw_count,
        )


class SecSubmissionsConnector(BaseGlobalConnector):
    descriptor = SourceConnectorRecord(
        connector_id="connector:us:sec-submissions",
        country_code="US",
        source_key="sec-edgar",
        source_name="SEC EDGAR",
        source_type="official_disclosure",
        base_url="https://data.sec.gov",
        source_right_id="official:sec-edgar",
        coverage_mode=CoverageMode.SELECTED_ISSUERS,
        schedule_minutes=15,
    )

    def __init__(
        self,
        *,
        user_agent: str,
        client: httpx.Client | None = None,
        timeout: float = 20.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        minimum_request_interval: float = 0.12,
        _throttle: _SecRequestThrottle | None = None,
    ) -> None:
        self.user_agent = _validated_sec_user_agent(user_agent)
        self.client = client
        self.timeout = timeout
        self.throttle = _throttle or _SecRequestThrottle(
            minimum_interval=minimum_request_interval,
            sleep=sleep,
            clock=clock,
        )

    def _get(self, url: str) -> httpx.Response:
        parsed = urlparse(url)
        if (
            parsed.scheme != "https"
            or parsed.hostname != "data.sec.gov"
            or not parsed.path.startswith("/submissions/CIK")
            or not parsed.path.endswith(".json")
            or parsed.query
            or parsed.fragment
        ):
            raise GlobalConnectorContractError(
                "SEC submissions URL is not an official structured endpoint"
            )
        self.throttle.wait()
        headers = {
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json",
        }
        response = (
            self.client.get(url, headers=headers, follow_redirects=False)
            if self.client is not None
            else httpx.get(url, headers=headers, timeout=self.timeout, follow_redirects=False)
        )
        if response.status_code >= 400:
            raise GlobalConnectorError(f"SEC EDGAR HTTP {response.status_code}")
        return response

    def _fetch_authorized(
        self,
        request: GlobalConnectorRequest,
        *,
        rights_revision: str,
        retrieved_at: datetime,
        rights_guard: _SourceRightGuard,
    ) -> GlobalConnectorEnvelope:
        records: list[GlobalDocumentRecord] = []
        request_count = 0
        raw_count = 0
        if not request.issuers:
            raise GlobalConnectorContractError(
                "SEC submissions connector requires an explicit issuer scope"
            )
        if len(request.issuers) > request.max_pages:
            raise GlobalConnectorPaginationError(
                "SEC submissions issuer scope exceeds max_pages request budget"
            )
        for issuer in request.issuers:
            rights_guard.assert_current()
            if issuer.identifier_type != "CIK":
                raise GlobalConnectorContractError("SEC connector requires CIK identifiers")
            digits = re.sub(r"\D", "", issuer.value)
            if not digits or len(digits) > 10:
                raise GlobalConnectorContractError("SEC CIK is invalid")
            cik = digits.zfill(10)
            url = f"{SEC_SUBMISSIONS_BASE_URL}/CIK{cik}.json"
            payload = self._get(url).json()
            request_count += 1
            if not isinstance(payload, dict):
                raise GlobalConnectorContractError("SEC submissions payload must be an object")
            response_cik = re.sub(r"\D", "", str(payload.get("cik") or ""))
            if response_cik and response_cik.zfill(10) != cik:
                raise GlobalConnectorContractError(
                    "SEC submissions response CIK does not match request"
                )
            filings = payload.get("filings")
            recent = filings.get("recent") if isinstance(filings, dict) else None
            if not isinstance(recent, dict):
                raise GlobalConnectorContractError("SEC submissions payload omitted filings.recent")
            historical_files = filings.get("files") if isinstance(filings, dict) else None
            if historical_files is not None and not isinstance(historical_files, list):
                raise GlobalConnectorContractError(
                    "SEC submissions filings.files must be a list"
                )
            for historical in historical_files or []:
                if not isinstance(historical, dict):
                    raise GlobalConnectorContractError(
                        "SEC submissions historical file descriptor is invalid"
                    )
                try:
                    history_from = date.fromisoformat(
                        str(historical.get("filingFrom") or "")
                    )
                    history_to = date.fromisoformat(
                        str(historical.get("filingTo") or "")
                    )
                except ValueError as exc:
                    raise GlobalConnectorContractError(
                        "SEC submissions historical file dates are invalid"
                    ) from exc
                overlaps = (
                    history_from < request.window_end_exclusive
                    and history_to >= request.window_start
                )
                if overlaps:
                    raise GlobalConnectorContractError(
                        "SEC historical submissions require the bulk backfill adapter"
                    )
            fields = (
                "accessionNumber",
                "filingDate",
                "acceptanceDateTime",
                "form",
                "primaryDocument",
                "primaryDocDescription",
                "items",
            )
            columns = {field: recent.get(field) for field in fields}
            if any(not isinstance(value, list) for value in columns.values()):
                raise GlobalConnectorContractError("SEC recent filing columns are invalid")
            column_lists = {
                field: cast(list[Any], columns[field])
                for field in fields
            }
            lengths = {len(value) for value in column_lists.values()}
            if len(lengths) != 1:
                raise GlobalConnectorContractError("SEC recent filing columns have different lengths")
            company_name = str(payload.get("name") or issuer.legal_name or "").strip()
            reference = IssuerReference(
                namespace="US:CIK",
                identifier_type="CIK",
                value=cik,
                legal_name=company_name,
                market=issuer.market,
                ticker=issuer.ticker,
            )
            for index in range(next(iter(lengths), 0)):
                raw_count += 1
                form = str(column_lists["form"][index] or "").strip().upper()
                event_family = SEC_FORM_FAMILIES.get(form)
                if form == "8-K":
                    filing_items = str(column_lists["items"][index] or "")
                    event_family = next(
                        (
                            family
                            for item, family in SEC_8K_ITEM_FAMILIES.items()
                            if item in filing_items
                        ),
                        None,
                    )
                if event_family is None:
                    continue
                filing_date = str(column_lists["filingDate"][index] or "").strip()
                try:
                    filed_day = date.fromisoformat(filing_date)
                except ValueError as exc:
                    raise GlobalConnectorContractError("SEC filingDate is invalid") from exc
                if not (request.window_start <= filed_day < request.window_end_exclusive):
                    continue
                accession = str(column_lists["accessionNumber"][index] or "").strip()
                if re.fullmatch(r"\d{10}-\d{2}-\d{6}", accession) is None:
                    raise GlobalConnectorContractError("SEC accession number is invalid")
                primary_document = str(column_lists["primaryDocument"][index] or "").strip()
                if not primary_document or "/" in primary_document or "\\" in primary_document:
                    raise GlobalConnectorContractError("SEC primary document name is invalid")
                accepted = str(column_lists["acceptanceDateTime"][index] or "").strip()
                try:
                    accepted_at = datetime.strptime(accepted, "%Y%m%d%H%M%S").replace(
                        tzinfo=ZoneInfo("America/New_York")
                    ).astimezone(timezone.utc)
                except ValueError:
                    accepted_at = datetime.combine(
                        filed_day,
                        datetime.min.time(),
                        tzinfo=ZoneInfo("America/New_York"),
                    ).astimezone(timezone.utc)
                accession_path = accession.replace("-", "")
                source_url = (
                    f"{SEC_ARCHIVES_BASE_URL}/edgar/data/{int(cik)}/"
                    f"{accession_path}/{quote(primary_document)}"
                )
                description = str(column_lists["primaryDocDescription"][index] or "").strip()
                title = description or f"{form} — {company_name or cik}"
                records.append(
                    _official_record(
                        connector=self.descriptor,
                        issuer_reference=reference,
                        external_id=_sec_filing_external_id(accession, cik),
                        record_kind="disclosure",
                        document_type=form,
                        event_family=event_family,
                        title=title,
                        language="en",
                        filed_at=accepted_at,
                        retrieved_at=retrieved_at,
                        original_url=source_url,
                        change_type=(
                            "corrected" if form.endswith("/A") else "new"
                        ),
                        metadata={
                            "accession_number": accession,
                            "cik": cik,
                            "form": form,
                            "filing_date": filing_date,
                            "title_provenance": (
                                "source" if description else "generated_metadata"
                            ),
                        },
                    )
                )
        return GlobalConnectorEnvelope(
            schema_version=1,
            connector_id=self.descriptor.connector_id,
            country_code="US",
            source_right_id=self.descriptor.source_right_id or "",
            rights_revision=rights_revision,
            retrieved_at=_explicit_utc(retrieved_at, "retrieved_at"),
            coverage_mode=self.descriptor.coverage_mode,
            records=tuple(sorted(records, key=lambda item: (item.filed_at, item.record_id))),
            next_cursor=None,
            exhausted=True,
            request_count=request_count,
            raw_count=raw_count,
        )


EDINET_TYPE_FAMILIES = {
    "220": "capital_return",
    "230": "capital_return",
    "240": "tender_offer_and_mna",
    "250": "tender_offer_and_mna",
    "260": "tender_offer_and_mna",
    "270": "tender_offer_and_mna",
    "280": "tender_offer_and_mna",
    "290": "tender_offer_and_mna",
    "300": "tender_offer_and_mna",
    "310": "tender_offer_and_mna",
    "320": "tender_offer_and_mna",
    "330": "tender_offer_and_mna",
    "340": "tender_offer_and_mna",
    "350": "large_ownership",
    "360": "large_ownership",
    "370": "meeting_and_vote",
    "380": "meeting_and_vote",
}

EDINET_CORRECTION_TYPES = {
    "230",
    "250",
    "280",
    "300",
    "320",
    "340",
    "360",
    "380",
}
EDINET_WITHDRAWAL_TYPES = {"260"}
EDINET_WITHDRAWAL_STATES = {"1", "2"}
EDINET_DISCLOSURE_STATES = {
    "1": "nonpublic_started",
    "2": "nonpublic",
    "3": "public_restored",
}


def _edinet_event_family(
    document_type: str,
    row: dict[str, Any],
) -> str | None:
    family = EDINET_TYPE_FAMILIES.get(document_type)
    if family is not None:
        return family
    if document_type not in {"180", "190"}:
        return None
    reason = str(row.get("currentReportReason") or "")
    keyword_families = (
        (
            "meeting_and_vote",
            ("株主総会", "議決権", "株主提案"),
        ),
        (
            "board_and_compensation",
            ("取締役", "監査役", "代表取締役", "役員", "報酬"),
        ),
        (
            "tender_offer_and_mna",
            ("合併", "会社分割", "株式交換", "株式移転", "公開買付", "買収"),
        ),
        (
            "capital_issuance",
            ("募集株式", "新株", "新株予約権", "転換社債", "社債"),
        ),
        (
            "capital_return",
            ("剰余金", "配当", "自己株式", "株主還元"),
        ),
        (
            "listing_status",
            ("上場廃止", "整理銘柄", "監理銘柄"),
        ),
        (
            "large_ownership",
            ("主要株主", "親会社", "大量保有"),
        ),
    )
    matched = {
        family_name
        for family_name, keywords in keyword_families
        if any(keyword in reason for keyword in keywords)
    }
    # Ambiguous or opaque extraordinary reports remain private until review.
    return matched.pop() if len(matched) == 1 else "unclassified"


def _edinet_timestamp(value: str, *, fallback: datetime) -> datetime:
    text = value.strip()
    if not text:
        return fallback
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise GlobalConnectorContractError(
            "EDINET timestamp is invalid"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=ZoneInfo("Asia/Tokyo"))
    return parsed.astimezone(timezone.utc)


def _edinet_stable_file_timestamp(file_day: date) -> datetime:
    return datetime.combine(
        file_day,
        datetime.min.time(),
        tzinfo=ZoneInfo("Asia/Tokyo"),
    ).astimezone(timezone.utc)


def _edinet_row_timestamps(
    row: dict[str, Any],
    *,
    file_day: date,
) -> tuple[datetime, datetime]:
    """Return the filing time and stable lifecycle-operation time.

    EDINET's ``opeDateTime`` is the timestamp of an information edit,
    withdrawal/non-disclosure operation, while ``submitDateTime`` remains the
    original filing time.  Some synthetic lifecycle rows omit one or both
    values, so the API file date is the only deterministic fallback.
    """

    stable_fallback = _edinet_stable_file_timestamp(file_day)
    submitted = str(row.get("submitDateTime") or "").strip()
    operated = str(row.get("opeDateTime") or "").strip()
    filed_at = _edinet_timestamp(
        submitted,
        fallback=(
            _edinet_timestamp(operated, fallback=stable_fallback)
            if operated
            else stable_fallback
        ),
    )
    observed_at = _edinet_timestamp(
        operated,
        fallback=(
            _edinet_timestamp(submitted, fallback=stable_fallback)
            if submitted
            else stable_fallback
        ),
    )
    return filed_at, observed_at


def _edinet_lifecycle_observation(
    *,
    document_id: str,
    parent_document_id: str,
    change_type: str,
    observed_at: datetime,
    file_day: date,
    row: dict[str, Any],
) -> GlobalLifecycleObservation:
    sequence = str(row.get("seqNumber") or "").strip()
    operation = "|".join(
        (
            str(row.get("withdrawalStatus") or ""),
            str(row.get("docInfoEditStatus") or ""),
            str(row.get("disclosureStatus") or ""),
        )
    )
    return GlobalLifecycleObservation(
        observation_id=(
            "globalobs:"
            + _hash_record(
                "edinet",
                file_day.isoformat(),
                sequence,
                document_id,
                parent_document_id,
                operation,
            )[:40]
        ),
        country_code="JP",
        source_key="edinet",
        external_id=document_id,
        parent_external_id=parent_document_id or None,
        change_type=change_type,
        observed_at=_explicit_utc(observed_at, "observed_at"),
        metadata={
            "file_date": file_day.isoformat(),
            "sequence_number": row.get("seqNumber"),
            "withdrawal_status": row.get("withdrawalStatus"),
            "document_info_edit_status": row.get("docInfoEditStatus"),
            "disclosure_status": row.get("disclosureStatus"),
            "disclosure_state": EDINET_DISCLOSURE_STATES.get(
                str(row.get("disclosureStatus") or "").strip()
            ),
        },
    )


def _store_lifecycle_observation(
    target: dict[str, GlobalLifecycleObservation],
    observation: GlobalLifecycleObservation,
) -> None:
    previous = target.get(observation.observation_id)
    if previous is not None and previous != observation:
        raise GlobalConnectorContractError(
            "lifecycle observation identity conflict"
        )
    target[observation.observation_id] = observation


class EdinetDocumentsConnector(BaseGlobalConnector):
    descriptor = SourceConnectorRecord(
        connector_id="connector:jp:edinet",
        country_code="JP",
        source_key="edinet",
        source_name="EDINET",
        source_type="official_disclosure",
        base_url="https://api.edinet-fsa.go.jp",
        source_right_id="official:edinet",
        coverage_mode=CoverageMode.MARKET_WIDE,
        schedule_minutes=15,
    )

    def __init__(
        self,
        *,
        api_key: str,
        client: httpx.Client | None = None,
        timeout: float = 20.0,
    ) -> None:
        if not api_key.strip() or "\r" in api_key or "\n" in api_key:
            raise ValueError("EDINET subscription key is required")
        self.api_key = api_key.strip()
        self.client = client
        self.timeout = timeout

    def _get_day(self, day: date) -> httpx.Response:
        url = "https://api.edinet-fsa.go.jp/api/v2/documents.json"
        headers = {"Accept": "application/json"}
        params = {
            "date": day.isoformat(),
            "type": "2",
            "Subscription-Key": self.api_key,
        }
        response = (
            self.client.get(
                url,
                params=params,
                headers=headers,
                follow_redirects=False,
            )
            if self.client is not None
            else httpx.get(
                url,
                params=params,
                headers=headers,
                timeout=self.timeout,
                follow_redirects=False,
            )
        )
        if response.status_code >= 400:
            raise GlobalConnectorError(f"EDINET HTTP {response.status_code}")
        return response

    def _fetch_authorized(
        self,
        request: GlobalConnectorRequest,
        *,
        rights_revision: str,
        retrieved_at: datetime,
        rights_guard: _SourceRightGuard,
    ) -> GlobalConnectorEnvelope:
        records_by_id: dict[str, GlobalDocumentRecord] = {}
        lifecycle_by_id: dict[str, GlobalLifecycleObservation] = {}
        raw_count = 0
        request_count = 0
        current = request.window_start
        if (request.window_end_exclusive - request.window_start).days > request.max_pages:
            raise GlobalConnectorPaginationError(
                "EDINET date window exceeds max_pages request budget"
            )
        while current < request.window_end_exclusive:
            rights_guard.assert_current()
            payload = self._get_day(current).json()
            request_count += 1
            if not isinstance(payload, dict):
                raise GlobalConnectorContractError("EDINET response must be an object")
            metadata = payload.get("metadata")
            if isinstance(metadata, dict):
                status = str(metadata.get("status") or "200")
                if status not in {"200", "0", "000"}:
                    raise GlobalConnectorError("EDINET returned a non-success status")
            rows = payload.get("results")
            if not isinstance(rows, list):
                raise GlobalConnectorContractError("EDINET response omitted results")
            seen_day: dict[str, str] = {}
            for row in rows:
                if not isinstance(row, dict):
                    raise GlobalConnectorContractError(
                        "EDINET result must be an object"
                    )
                raw_count += 1
                document_id = str(row.get("docID") or "").strip()
                edinet_code = str(row.get("edinetCode") or "").strip()
                filer_name = str(row.get("filerName") or "").strip()
                title = str(row.get("docDescription") or "").strip()
                document_type = str(row.get("docTypeCode") or "").strip()
                withdrawal_status = str(row.get("withdrawalStatus") or "").strip()
                disclosure_status = str(row.get("disclosureStatus") or "").strip()
                edit_status = str(row.get("docInfoEditStatus") or "").strip()
                parent_document_id = str(row.get("parentDocID") or "").strip()
                if withdrawal_status not in {"", "0", "1", "2"}:
                    raise GlobalConnectorContractError(
                        "EDINET withdrawalStatus is invalid"
                    )
                if edit_status not in {"", "0", "1", "2"}:
                    raise GlobalConnectorContractError(
                        "EDINET docInfoEditStatus is invalid"
                    )
                if disclosure_status not in {"", "0", "1", "2", "3"}:
                    raise GlobalConnectorContractError(
                        "EDINET disclosureStatus is invalid"
                    )
                withdrawn = (
                    withdrawal_status in EDINET_WITHDRAWAL_STATES
                    or document_type in EDINET_WITHDRAWAL_TYPES
                )
                disclosure_change = disclosure_status in EDINET_DISCLOSURE_STATES
                information_edit = edit_status in {"1", "2"}
                lifecycle_change = withdrawn or disclosure_change or information_edit
                filed_at, observed_at = _edinet_row_timestamps(
                    row,
                    file_day=current,
                )
                if (
                    re.fullmatch(r"[A-Z0-9]{8}", document_id) is None
                    or re.fullmatch(r"E\d{5}", edinet_code) is None
                    or not filer_name
                    or not title
                ):
                    if lifecycle_change:
                        if re.fullmatch(r"[A-Z0-9]{8}", document_id) is None:
                            raise GlobalConnectorContractError(
                                "EDINET lifecycle docID is invalid"
                            )
                        _store_lifecycle_observation(
                            lifecycle_by_id,
                            _edinet_lifecycle_observation(
                                document_id=document_id,
                                parent_document_id=parent_document_id,
                                change_type=(
                                    "withdrawn"
                                    if withdrawn
                                    else (
                                        "corrected"
                                        if information_edit
                                        else "updated"
                                    )
                                ),
                                observed_at=observed_at,
                                file_day=current,
                                row=row,
                            )
                        )
                        continue
                    raise GlobalConnectorContractError("EDINET result identity is invalid")
                family = _edinet_event_family(document_type, row)
                if family is None:
                    continue
                reference = IssuerReference(
                    namespace="JP:EDINET",
                    identifier_type="EDINET_CODE",
                    value=edinet_code,
                    legal_name=filer_name,
                    market="JPX",
                )
                change_type = (
                    "withdrawn"
                    if withdrawn
                    else (
                        "corrected"
                        if document_type in EDINET_CORRECTION_TYPES
                        or parent_document_id
                        or information_edit
                        else ("updated" if disclosure_change else "new")
                    )
                )
                record = _official_record(
                        connector=self.descriptor,
                        issuer_reference=reference,
                        external_id=document_id,
                        record_kind="disclosure",
                        document_type=document_type,
                        event_family=family,
                        title=title,
                        language="ja",
                        filed_at=filed_at,
                        retrieved_at=retrieved_at,
                        original_url=(
                            "https://disclosure2.edinet-fsa.go.jp/"
                            f"WZEK0040.aspx?{quote(document_id)}"
                        ),
                        correction_of_external_id=(
                            parent_document_id
                            if re.fullmatch(r"[A-Z0-9]{8}", parent_document_id)
                            else None
                        ),
                        change_type=change_type,
                        metadata={
                            "edinet_code": edinet_code,
                            "ordinance_code": row.get("ordinanceCode"),
                            "form_code": row.get("formCode"),
                            "withdrawal_status": withdrawal_status,
                            "disclosure_status": disclosure_status,
                            "disclosure_state": EDINET_DISCLOSURE_STATES.get(
                                disclosure_status
                            ),
                            "document_info_edit_status": edit_status,
                            "parent_document_id": parent_document_id,
                            "document_id": document_id,
                            "sequence_number": row.get("seqNumber"),
                            "lifecycle_observed_at": (
                                _explicit_utc(observed_at, "observed_at")
                                if lifecycle_change
                                else None
                            ),
                            "evidence_locator": f"EDINET docID={document_id}",
                        },
                )
                prior_hash = seen_day.get(document_id)
                if prior_hash is not None:
                    if prior_hash == record.content_hash:
                        continue
                    _store_lifecycle_observation(
                        lifecycle_by_id,
                        _edinet_lifecycle_observation(
                            document_id=document_id,
                            parent_document_id=parent_document_id,
                            change_type=(
                                change_type if change_type != "new" else "updated"
                            ),
                            observed_at=observed_at,
                            file_day=current,
                            row=row,
                        )
                    )
                seen_day[document_id] = record.content_hash
                records_by_id[record.record_id] = record
            current += timedelta(days=1)
        return GlobalConnectorEnvelope(
            schema_version=1,
            connector_id=self.descriptor.connector_id,
            country_code="JP",
            source_right_id=self.descriptor.source_right_id or "",
            rights_revision=rights_revision,
            retrieved_at=_explicit_utc(retrieved_at, "retrieved_at"),
            coverage_mode=self.descriptor.coverage_mode,
            records=tuple(
                sorted(
                    records_by_id.values(),
                    key=lambda item: (item.filed_at, item.record_id),
                )
            ),
            next_cursor=None,
            exhausted=True,
            request_count=request_count,
            raw_count=raw_count,
            lifecycle_observations=tuple(
                sorted(
                    lifecycle_by_id.values(),
                    key=lambda item: (item.observed_at, item.observation_id),
                )
            ),
        )


COMPANIES_HOUSE_FAMILIES = {
    "officers": "board_and_compensation",
    "capital": "capital_issuance",
    "liquidation": "listing_status",
    "resolution": "meeting_and_vote",
}


@dataclass
class _CompaniesHouseRequestBudget:
    limit: int
    used: int = 0

    def consume(self) -> None:
        if self.used >= self.limit:
            raise GlobalConnectorPaginationError(
                "Companies House run request budget exhausted"
            )
        self.used += 1


class CompaniesHouseFilingHistoryConnector(BaseGlobalConnector):
    descriptor = SourceConnectorRecord(
        connector_id="connector:gb:companies-house",
        country_code="GB",
        source_key="companies-house",
        source_name="Companies House",
        source_type="official_register",
        base_url="https://api.company-information.service.gov.uk",
        source_right_id="official:companies-house",
        coverage_mode=CoverageMode.OFFICIAL_REGISTER,
        schedule_minutes=30,
    )

    def __init__(
        self,
        *,
        api_key: str,
        client: httpx.Client | None = None,
        timeout: float = 20.0,
        max_retries: int = 2,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not api_key.strip() or "\r" in api_key or "\n" in api_key:
            raise ValueError("Companies House API key is required")
        if max_retries < 0 or max_retries > 5:
            raise ValueError("Companies House max_retries must be between 0 and 5")
        self.api_key = api_key.strip()
        self.client = client
        self.timeout = timeout
        self.max_retries = max_retries
        self.sleep = sleep

    @staticmethod
    def _retry_after_seconds(response: httpx.Response) -> float | None:
        value = str(response.headers.get("Retry-After") or "").strip()
        if not value:
            return None
        try:
            delay = float(value)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(value)
            except (TypeError, ValueError, OverflowError):
                return None
            if retry_at.tzinfo is None or retry_at.utcoffset() is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            delay = (
                retry_at.astimezone(timezone.utc) - datetime.now(timezone.utc)
            ).total_seconds()
        if not math.isfinite(delay):
            return None
        if delay < 0:
            return 0.0
        return min(delay, 60.0)

    def _get(
        self,
        company_number: str,
        start_index: int,
        page_size: int,
        *,
        request_budget: _CompaniesHouseRequestBudget,
        before_request: Callable[[], object],
    ) -> httpx.Response:
        url = (
            "https://api.company-information.service.gov.uk/company/"
            f"{quote(company_number)}/filing-history"
        )
        credential = base64.b64encode(f"{self.api_key}:".encode()).decode()
        headers = {"Authorization": f"Basic {credential}", "Accept": "application/json"}
        params = {"start_index": start_index, "items_per_page": page_size}
        for attempt in range(self.max_retries + 1):
            before_request()
            request_budget.consume()
            response = (
                self.client.get(
                    url,
                    params=params,
                    headers=headers,
                    follow_redirects=False,
                )
                if self.client is not None
                else httpx.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                    follow_redirects=False,
                )
            )
            retryable = response.status_code == 429 or 500 <= response.status_code < 600
            if not retryable:
                if response.status_code >= 400:
                    raise GlobalConnectorError(
                        f"Companies House HTTP {response.status_code}"
                    )
                return response
            if attempt >= self.max_retries:
                raise GlobalConnectorError(
                    f"Companies House HTTP {response.status_code} after retries"
                )
            advertised = self._retry_after_seconds(response)
            delay = (
                advertised
                if advertised is not None
                else min(float(2**attempt), 8.0)
            )
            self.sleep(delay)
        raise GlobalConnectorError("Companies House retry loop exhausted")

    def _fetch_authorized(
        self,
        request: GlobalConnectorRequest,
        *,
        rights_revision: str,
        retrieved_at: datetime,
        rights_guard: _SourceRightGuard,
    ) -> GlobalConnectorEnvelope:
        records: list[GlobalDocumentRecord] = []
        raw_count = 0
        request_count = 0
        if not request.issuers:
            raise GlobalConnectorContractError(
                "Companies House connector requires an explicit company scope"
            )
        request_budget = _CompaniesHouseRequestBudget(request.max_pages)
        for issuer in request.issuers:
            if issuer.identifier_type != "COMPANY_NUMBER":
                raise GlobalConnectorContractError(
                    "Companies House connector requires COMPANY_NUMBER"
                )
            company_number = issuer.value.strip().upper()
            if re.fullmatch(r"[A-Z0-9]{6,10}", company_number) is None:
                raise GlobalConnectorContractError("company number is invalid")
            start_index = 0
            expected_total: int | None = None
            previous_filed_day: date | None = None
            while True:
                payload = self._get(
                    company_number,
                    start_index,
                    request.page_size,
                    request_budget=request_budget,
                    before_request=rights_guard.assert_current,
                ).json()
                request_count = request_budget.used
                if not isinstance(payload, dict):
                    raise GlobalConnectorContractError(
                        "Companies House response must be an object"
                    )
                required_fields = {
                    "items",
                    "items_per_page",
                    "start_index",
                    "total_count",
                    "kind",
                }
                if not required_fields.issubset(payload):
                    raise GlobalConnectorContractError(
                        "Companies House response omitted pagination metadata"
                    )
                items = payload.get("items")
                if not isinstance(items, list):
                    raise GlobalConnectorContractError(
                        "Companies House response omitted items"
                    )
                if str(payload.get("kind") or "") != "filing-history":
                    raise GlobalConnectorContractError(
                        "Companies House response kind is invalid"
                    )
                response_start = int(payload.get("start_index", start_index))
                if response_start != start_index:
                    raise GlobalConnectorPaginationError(
                        "Companies House start_index drifted"
                    )
                total_count = int(payload.get("total_count", len(items)))
                items_per_page = int(payload.get("items_per_page", 0))
                if items_per_page < 1 or items_per_page > 100:
                    raise GlobalConnectorContractError(
                        "Companies House items_per_page is invalid"
                    )
                if expected_total is None:
                    expected_total = total_count
                elif total_count != expected_total:
                    raise GlobalConnectorPaginationError(
                        "Companies House total_count changed while paginating"
                    )
                reference = IssuerReference(
                    namespace="GB:COMPANIES_HOUSE",
                    identifier_type="COMPANY_NUMBER",
                    value=company_number,
                    legal_name=issuer.legal_name,
                    market=issuer.market or "LSE",
                    ticker=issuer.ticker,
                )
                oldest_filed_day: date | None = None
                for item in items:
                    if not isinstance(item, dict):
                        raise GlobalConnectorContractError(
                            "Companies House filing item must be an object"
                        )
                    transaction_id = str(item.get("transaction_id") or "").strip()
                    filed_date = str(item.get("date") or "").strip()
                    category = str(item.get("category") or "").strip()
                    filing_type = str(item.get("type") or "").strip()
                    description = str(item.get("description") or "").strip()
                    if (
                        not transaction_id
                        or not filed_date
                        or not category
                        or not filing_type
                    ):
                        raise GlobalConnectorContractError(
                            "Companies House filing identity is incomplete"
                        )
                    try:
                        filed_day = date.fromisoformat(filed_date)
                    except ValueError as exc:
                        raise GlobalConnectorContractError(
                            "Companies House filing date is invalid"
                        ) from exc
                    if previous_filed_day is not None and filed_day > previous_filed_day:
                        raise GlobalConnectorPaginationError(
                            "Companies House filing history is not descending"
                        )
                    previous_filed_day = filed_day
                    oldest_filed_day = filed_day
                    if not (
                        request.window_start
                        <= filed_day
                        < request.window_end_exclusive
                    ):
                        continue
                    # Historical replay identity covers the requested date
                    # window, not newer pages scanned to reach that window.
                    raw_count += 1
                    family = COMPANIES_HOUSE_FAMILIES.get(category)
                    if family is None:
                        continue
                    links = item.get("links")
                    if links is not None and not isinstance(links, dict):
                        raise GlobalConnectorContractError(
                            "Companies House filing links must be an object"
                        )
                    self_path = (
                        str(links.get("self") or "").strip()
                        if isinstance(links, dict)
                        else ""
                    )
                    document_meta = (
                        str(links.get("document_metadata") or "").strip()
                        if isinstance(links, dict)
                        else ""
                    )
                    for link_path in (self_path, document_meta):
                        if link_path and (
                            not link_path.startswith("/")
                            or ".." in link_path
                            or "\\" in link_path
                        ):
                            raise GlobalConnectorContractError(
                                "Companies House filing link is invalid"
                            )
                    original_url = (
                        "https://find-and-update.company-information.service.gov.uk"
                        f"/company/{quote(company_number)}/filing-history"
                    )
                    records.append(
                        _official_record(
                            connector=self.descriptor,
                            issuer_reference=reference,
                            external_id=transaction_id,
                            record_kind="registry_filing",
                            document_type=filing_type,
                            event_family=family,
                            title=description or filing_type,
                            language="en",
                            filed_at=datetime.combine(
                                filed_day,
                                datetime.min.time(),
                                tzinfo=timezone.utc,
                            ),
                            retrieved_at=retrieved_at,
                            original_url=original_url,
                            metadata={
                                "company_number": company_number,
                                "category": category,
                                "barcode": item.get("barcode"),
                                "transaction_id": transaction_id,
                                "title_provenance": (
                                    "source"
                                    if description
                                    else "generated_metadata"
                                ),
                                "api_self_path": self_path or None,
                                "document_metadata_path": document_meta or None,
                                "evidence_locator": (
                                    f"Companies House transaction={transaction_id}"
                                ),
                            },
                        )
                    )
                start_index += len(items)
                if not items and start_index < total_count:
                    raise GlobalConnectorPaginationError(
                        "Companies House returned an empty page before total_count"
                    )
                if start_index >= total_count:
                    break
                if (
                    oldest_filed_day is not None
                    and oldest_filed_day < request.window_start
                ):
                    break
        return GlobalConnectorEnvelope(
            schema_version=1,
            connector_id=self.descriptor.connector_id,
            country_code="GB",
            source_right_id=self.descriptor.source_right_id or "",
            rights_revision=rights_revision,
            retrieved_at=_explicit_utc(retrieved_at, "retrieved_at"),
            coverage_mode=self.descriptor.coverage_mode,
            records=tuple(sorted(records, key=lambda item: (item.filed_at, item.record_id))),
            next_cursor=None,
            exhausted=True,
            request_count=request_count,
            raw_count=raw_count,
        )


@dataclass(frozen=True)
class ApprovedOfficialLink:
    issuer: IssuerReference
    external_id: str
    document_type: str
    event_family: str
    title: str
    original_language: str
    filed_at: str
    original_url: str


class ApprovedLinkOnlyConnector(BaseGlobalConnector):
    def __init__(
        self,
        descriptor: SourceConnectorRecord,
        links: Iterable[ApprovedOfficialLink],
    ) -> None:
        if descriptor.coverage_mode not in {
            CoverageMode.SELECTED_ISSUERS,
            CoverageMode.LINK_ONLY,
        }:
            raise ValueError("approved-link connector must be rights-limited")
        self.descriptor = descriptor
        self.links = tuple(links)

    def _fetch_authorized(
        self,
        request: GlobalConnectorRequest,
        *,
        rights_revision: str,
        retrieved_at: datetime,
        rights_guard: _SourceRightGuard,
    ) -> GlobalConnectorEnvelope:
        if not request.issuers:
            raise GlobalConnectorContractError(
                "approved-link connector requires an explicit issuer allowlist"
            )
        allowed = {
            (issuer.namespace, issuer.value)
            for issuer in request.issuers
        }
        records: list[GlobalDocumentRecord] = []
        observed_count = 0
        for link in self.links:
            rights_guard.assert_current()
            if not link.issuer.namespace.startswith(
                f"{self.descriptor.country_code}:"
            ):
                raise GlobalConnectorContractError(
                    "approved link issuer country does not match connector"
                )
            if (link.issuer.namespace, link.issuer.value) not in allowed:
                continue
            _safe_link_only_url(link.original_url)
            filed = datetime.fromisoformat(
                link.filed_at[:-1] + "+00:00"
                if link.filed_at.endswith("Z")
                else link.filed_at
            )
            if filed.tzinfo is None:
                raise GlobalConnectorContractError(
                    "approved official link filed_at requires a timezone"
                )
            if not (
                request.window_start
                <= filed.date()
                < request.window_end_exclusive
            ):
                continue
            observed_count += 1
            records.append(
                _official_record(
                    connector=self.descriptor,
                    issuer_reference=link.issuer,
                    external_id=link.external_id,
                    record_kind="link",
                    document_type=link.document_type,
                    event_family=link.event_family,
                    title=link.title,
                    language=link.original_language,
                    filed_at=filed,
                    retrieved_at=retrieved_at,
                    original_url=link.original_url,
                    body_text=None,
                    metadata={"approved_link_only": True},
                )
            )
        return GlobalConnectorEnvelope(
            schema_version=1,
            connector_id=self.descriptor.connector_id,
            country_code=self.descriptor.country_code,
            source_right_id=self.descriptor.source_right_id or "",
            rights_revision=rights_revision,
            retrieved_at=_explicit_utc(retrieved_at, "retrieved_at"),
            coverage_mode=self.descriptor.coverage_mode,
            records=tuple(sorted(records, key=lambda item: (item.filed_at, item.record_id))),
            next_cursor=None,
            exhausted=True,
            request_count=0,
            raw_count=observed_count,
        )


CANADA_IR_DESCRIPTOR = SourceConnectorRecord(
    connector_id="connector:ca:issuer-ir",
    country_code="CA",
    source_key="issuer-ir",
    source_name="Canadian issuer IR manual links",
    source_type="official_issuer",
    base_url="https://www.canada.ca/",
    source_right_id="official:ca-issuer-ir",
    coverage_mode=CoverageMode.LINK_ONLY,
    schedule_minutes=30,
)

AUSTRALIA_ASIC_DESCRIPTOR = SourceConnectorRecord(
    connector_id="connector:au:asic-register",
    country_code="AU",
    source_key="asic-register",
    source_name="ASIC manual register links",
    source_type="official_register",
    base_url="https://www.asic.gov.au/",
    source_right_id="official:asic-register",
    coverage_mode=CoverageMode.LINK_ONLY,
    schedule_minutes=30,
)


GLOBAL_CONNECTOR_DESCRIPTORS = (
    DartGlobalConnector.descriptor,
    SecHybridConnector.descriptor,
    EdinetDocumentsConnector.descriptor,
    CompaniesHouseFilingHistoryConnector.descriptor,
    CANADA_IR_DESCRIPTOR,
    AUSTRALIA_ASIC_DESCRIPTOR,
)

# Enrichment adapters are deliberately excluded from the persisted connector
# registry because they share an official source identity with discovery but
# have a narrower, issuer-scoped execution contract.
GLOBAL_ENRICHMENT_CONNECTOR_DESCRIPTORS = (
    SecSubmissionsConnector.descriptor,
)
