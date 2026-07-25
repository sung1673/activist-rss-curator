from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Iterable
from urllib.parse import urlparse

from .governance import stable_id


GLOBAL_COUNTRIES = ("KR", "US", "JP", "GB", "CA", "AU")


class CoverageMode(StrEnum):
    MARKET_WIDE = "market-wide"
    OFFICIAL_REGISTER = "official-register"
    SELECTED_ISSUERS = "selected-issuers"
    LINK_ONLY = "link-only"
    UNAVAILABLE = "unavailable"


class ChangeType(StrEnum):
    NEW = "new"
    UPDATED = "updated"
    CORRECTED = "corrected"
    WITHDRAWN = "withdrawn"
    CLOSED = "closed"


class ConnectorStatus(StrEnum):
    INACTIVE = "inactive"
    CONFIGURED = "configured"
    ACTIVE = "active"
    DEGRADED = "degraded"
    PENDING_RIGHTS = "pending_rights"
    ERROR = "error"


class BriefLane(StrEnum):
    TOP = "top"
    WATCH = "watch"
    DEADLINE = "deadline"


GLOBAL_EVENT_FAMILIES = (
    "large_ownership",
    "meeting_and_vote",
    "tender_offer_and_mna",
    "capital_issuance",
    "capital_return",
    "board_and_compensation",
    "listing_status",
    "correction_and_withdrawal",
)

# Automated adapters may preserve an official observation before its public
# family can be determined safely.  ``unclassified`` is review-queue only:
# the public API continues to expose exactly the eight families above.
GLOBAL_INGEST_EVENT_FAMILIES = (*GLOBAL_EVENT_FAMILIES, "unclassified")


def _utc_iso(value: str | datetime, *, field_name: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include a UTC offset")
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _country(value: str) -> str:
    normalized = str(value).strip().upper()
    if normalized not in GLOBAL_COUNTRIES:
        raise ValueError(f"unsupported global market country: {value}")
    return normalized


def _language(value: str) -> str:
    normalized = str(value).strip()
    if re.fullmatch(r"[a-z]{2,3}(?:-[A-Z]{2})?", normalized) is None:
        raise ValueError("original_language must be an IETF-like language tag")
    return normalized


def _https_url(value: str, *, allow_query: bool = True) -> str:
    candidate = str(value).strip()
    try:
        parsed = urlparse(candidate)
        _ = parsed.port
    except ValueError as exc:
        raise ValueError("URL must be a valid absolute HTTPS URL") from exc
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or (not allow_query and parsed.query)
    ):
        raise ValueError("URL must be HTTPS without credentials or fragments")
    return candidate


def global_issuer_id(country: str, namespace: str, value: str) -> str:
    country_code = _country(country).casefold()
    namespace_parts = [
        part
        for part in re.split(r"[^a-z0-9]+", namespace.strip().casefold())
        if part and part != country_code
    ]
    normalized_namespace = "-".join(namespace_parts)
    normalized_value = re.sub(r"\s+", "", value.strip().casefold())
    if not normalized_namespace or not normalized_value:
        raise ValueError("issuer namespace and value are required")
    if re.fullmatch(r"[a-z0-9_.-]{1,40}", normalized_value) is None:
        normalized_value = stable_id(
            "hash",
            normalized_value,
            length=32,
        ).split(":", 1)[1]
    return f"issuer:{country_code}:{normalized_namespace}:{normalized_value}"


@dataclass(frozen=True)
class Jurisdiction:
    country_code: str
    display_name: str
    display_name_en: str
    default_market: str
    timezone_name: str
    launch_order: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "country_code", _country(self.country_code))
        if not self.display_name.strip() or not self.display_name_en.strip():
            raise ValueError("jurisdiction names are required")
        if not 1 <= int(self.launch_order) <= len(GLOBAL_COUNTRIES):
            raise ValueError("launch_order is out of range")


GLOBAL_JURISDICTIONS = (
    Jurisdiction("KR", "한국", "South Korea", "KRX", "Asia/Seoul", 1),
    Jurisdiction("US", "미국", "United States", "US", "America/New_York", 2),
    Jurisdiction("JP", "일본", "Japan", "JPX", "Asia/Tokyo", 3),
    Jurisdiction("GB", "영국", "United Kingdom", "LSE", "Europe/London", 4),
    Jurisdiction("CA", "캐나다", "Canada", "TSX", "America/Toronto", 5),
    Jurisdiction("AU", "호주", "Australia", "ASX", "Australia/Sydney", 6),
)


@dataclass(frozen=True)
class Issuer:
    issuer_id: str
    country_code: str
    legal_name: str
    original_language: str
    legal_name_en: str = ""
    short_name: str = ""
    homepage_url: str = ""
    listing_status: str = "unknown"
    record_status: str = "active"

    def __post_init__(self) -> None:
        object.__setattr__(self, "country_code", _country(self.country_code))
        object.__setattr__(
            self, "original_language", _language(self.original_language)
        )
        if re.fullmatch(
            r"issuer:[a-z]{2}:[a-z0-9][a-z0-9-]{0,31}:[a-z0-9_.-]{1,40}",
            self.issuer_id,
        ) is None:
            raise ValueError("issuer_id must be a stable global issuer ID")
        if self.issuer_id.split(":", 3)[1].upper() != self.country_code:
            raise ValueError("issuer_id country does not match country_code")
        if not self.legal_name.strip():
            raise ValueError("issuer legal_name is required")
        if self.homepage_url:
            _https_url(self.homepage_url)


@dataclass(frozen=True)
class IssuerIdentifier:
    issuer_id: str
    identifier_type: str
    identifier_value: str
    market: str = ""
    is_primary: bool = False

    def __post_init__(self) -> None:
        if not self.issuer_id.strip():
            raise ValueError("issuer_id is required")
        if re.fullmatch(r"[A-Z][A-Z0-9_]{1,39}", self.identifier_type) is None:
            raise ValueError("identifier_type is invalid")
        if not self.identifier_value.strip() or len(self.identifier_value) > 191:
            raise ValueError("identifier_value is invalid")


@dataclass(frozen=True)
class SourceConnectorRecord:
    connector_id: str
    country_code: str
    source_key: str
    source_name: str
    source_type: str
    base_url: str
    source_right_id: str | None
    coverage_mode: CoverageMode
    status: ConnectorStatus = ConnectorStatus.INACTIVE
    schedule_minutes: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "country_code", _country(self.country_code))
        object.__setattr__(
            self,
            "coverage_mode",
            CoverageMode(self.coverage_mode),
        )
        object.__setattr__(self, "status", ConnectorStatus(self.status))
        _https_url(self.base_url, allow_query=False)
        if re.fullmatch(r"connector:[a-z]{2}:[a-z0-9_.:-]+", self.connector_id) is None:
            raise ValueError("connector_id is invalid")
        if not self.source_key.strip() or not self.source_name.strip():
            raise ValueError("connector source identity is required")
        if self.schedule_minutes is not None and self.schedule_minutes < 1:
            raise ValueError("schedule_minutes must be positive")


@dataclass(frozen=True)
class SourceCoverage:
    coverage_id: str
    connector_id: str
    country_code: str
    coverage_mode: CoverageMode
    public_note: str
    event_family: str = "all"
    market: str = ""
    issuer_scope: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "country_code", _country(self.country_code))
        object.__setattr__(
            self,
            "coverage_mode",
            CoverageMode(self.coverage_mode),
        )
        object.__setattr__(self, "issuer_scope", tuple(self.issuer_scope))
        if self.event_family != "all" and self.event_family not in GLOBAL_EVENT_FAMILIES:
            raise ValueError("unsupported event_family")
        if not self.public_note.strip():
            raise ValueError("public coverage note is required")
        if self.coverage_mode is CoverageMode.SELECTED_ISSUERS and not self.issuer_scope:
            raise ValueError("selected-issuers coverage requires an issuer scope")


@dataclass(frozen=True)
class DocumentSection:
    section_id: str
    document_id: str
    section_key: str
    position_no: int
    original_language: str
    content_hash: str
    heading: str = ""
    body_text: str | None = None
    evidence_locator: str = ""
    publication_status: str = "draft"

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "original_language", _language(self.original_language)
        )
        if self.position_no < 0:
            raise ValueError("document section position cannot be negative")
        if re.fullmatch(r"[a-f0-9]{64}", self.content_hash) is None:
            raise ValueError("document section content_hash must be SHA-256")
        if not self.document_id.strip() or not self.section_key.strip():
            raise ValueError("document section identity is required")


@dataclass(frozen=True)
class BriefEdition:
    brief_id: str
    edition: str
    cutoff_at: str
    build_sha: str
    publication_status: str = "draft"
    published_at: str | None = None

    def __post_init__(self) -> None:
        if self.edition not in ("global", *GLOBAL_COUNTRIES):
            raise ValueError("unsupported brief edition")
        object.__setattr__(
            self, "cutoff_at", _utc_iso(self.cutoff_at, field_name="cutoff_at")
        )
        if self.published_at is not None:
            object.__setattr__(
                self,
                "published_at",
                _utc_iso(self.published_at, field_name="published_at"),
            )
        if re.fullmatch(r"[a-f0-9]{7,64}", self.build_sha) is None:
            raise ValueError("brief build_sha is invalid")


@dataclass(frozen=True)
class BriefItem:
    brief_id: str
    event_id: str
    lane: BriefLane
    position_no: int
    event_updated_at: str
    event_snapshot: dict[str, Any]
    selection_reason: str
    review_status: str = "pending"

    def __post_init__(self) -> None:
        object.__setattr__(self, "lane", BriefLane(self.lane))
        if self.position_no < 1 or self.position_no > 100:
            raise ValueError("brief item position is out of range")
        object.__setattr__(
            self,
            "event_updated_at",
            _utc_iso(self.event_updated_at, field_name="event_updated_at"),
        )
        if (
            not isinstance(self.event_snapshot, dict)
            or str(self.event_snapshot.get("event_id") or "") != self.event_id
            or str(self.event_snapshot.get("updated_at") or "")
            != self.event_updated_at
        ):
            raise ValueError(
                "brief event snapshot must freeze the selected event version"
            )
        if not self.selection_reason.strip():
            raise ValueError("brief item selection_reason is required")


@dataclass(frozen=True)
class GlobalMarketSnapshot:
    code_revision: str
    generated_at: str
    jurisdictions: tuple[Jurisdiction, ...] = GLOBAL_JURISDICTIONS
    issuers: tuple[Issuer, ...] = ()
    identifiers: tuple[IssuerIdentifier, ...] = ()
    connectors: tuple[SourceConnectorRecord, ...] = ()
    coverage: tuple[SourceCoverage, ...] = ()
    document_sections: tuple[DocumentSection, ...] = ()
    brief_editions: tuple[BriefEdition, ...] = ()
    brief_items: tuple[BriefItem, ...] = ()
    schema_version: int = 1
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("unsupported global snapshot schema_version")
        if re.fullmatch(r"[a-f0-9]{7,64}", self.code_revision) is None:
            raise ValueError("snapshot code_revision is invalid")
        object.__setattr__(
            self,
            "generated_at",
            _utc_iso(self.generated_at, field_name="generated_at"),
        )
        validate_global_snapshot(self)

    def to_payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "code_revision": self.code_revision,
            "generated_at": self.generated_at,
            "jurisdictions": [asdict(item) for item in self.jurisdictions],
            "issuers": [asdict(item) for item in self.issuers],
            "identifiers": [asdict(item) for item in self.identifiers],
            "connectors": [
                {
                    **asdict(item),
                    "coverage_mode": item.coverage_mode.value,
                    "status": item.status.value,
                }
                for item in self.connectors
            ],
            "coverage": [
                {
                    **asdict(item),
                    "coverage_mode": item.coverage_mode.value,
                    "issuer_scope": list(item.issuer_scope),
                }
                for item in self.coverage
            ],
            "document_sections": [
                asdict(item) for item in self.document_sections
            ],
            "brief_editions": [asdict(item) for item in self.brief_editions],
            "brief_items": [
                {**asdict(item), "lane": item.lane.value}
                for item in self.brief_items
            ],
            "metadata": dict(self.metadata),
        }


def _unique(values: Iterable[tuple[Any, ...]], label: str) -> None:
    seen: set[tuple[Any, ...]] = set()
    for value in values:
        if value in seen:
            raise ValueError(f"duplicate {label}: {value}")
        seen.add(value)


def validate_global_snapshot(snapshot: GlobalMarketSnapshot) -> None:
    jurisdiction_ids = {item.country_code for item in snapshot.jurisdictions}
    if jurisdiction_ids != set(GLOBAL_COUNTRIES):
        raise ValueError("snapshot must contain the six launch jurisdictions")
    _unique(
        ((item.launch_order,) for item in snapshot.jurisdictions),
        "jurisdiction launch order",
    )

    issuer_ids = {item.issuer_id for item in snapshot.issuers}
    _unique(((item.issuer_id,) for item in snapshot.issuers), "issuer")
    _unique(
        (
            (item.identifier_type, item.identifier_value.casefold(), item.market.casefold())
            for item in snapshot.identifiers
        ),
        "global issuer identifier",
    )
    for identifier in snapshot.identifiers:
        if identifier.issuer_id not in issuer_ids:
            raise ValueError("issuer identifier references an unknown issuer")

    connector_ids = {item.connector_id for item in snapshot.connectors}
    connectors_by_id = {item.connector_id: item for item in snapshot.connectors}
    _unique(((item.connector_id,) for item in snapshot.connectors), "connector")
    _unique(((item.coverage_id,) for item in snapshot.coverage), "source coverage")
    for item in snapshot.coverage:
        if item.connector_id not in connector_ids:
            raise ValueError("source coverage references an unknown connector")
        if connectors_by_id[item.connector_id].country_code != item.country_code:
            raise ValueError("source coverage country does not match connector")
        if connectors_by_id[item.connector_id].coverage_mode is not item.coverage_mode:
            raise ValueError("source coverage mode does not match connector")
        if item.coverage_mode is CoverageMode.SELECTED_ISSUERS:
            for issuer_id in item.issuer_scope:
                if issuer_id not in issuer_ids:
                    raise ValueError(
                        "selected source coverage references an unknown issuer"
                    )

    section_ids = {item.section_id for item in snapshot.document_sections}
    if len(section_ids) != len(snapshot.document_sections):
        raise ValueError("duplicate document section")
    _unique(
        (
            (item.document_id, item.section_key)
            for item in snapshot.document_sections
        ),
        "document section key",
    )
    _unique(
        (
            (item.document_id, item.position_no)
            for item in snapshot.document_sections
        ),
        "document section position",
    )

    brief_ids = {item.brief_id for item in snapshot.brief_editions}
    _unique(((item.brief_id,) for item in snapshot.brief_editions), "brief edition")
    _unique(
        (
            (item.edition, item.cutoff_at)
            for item in snapshot.brief_editions
        ),
        "brief edition cutoff",
    )
    _unique(
        (
            (item.brief_id, item.lane.value, item.position_no)
            for item in snapshot.brief_items
        ),
        "brief position",
    )
    _unique(
        (
            (item.brief_id, item.event_id, item.lane.value)
            for item in snapshot.brief_items
        ),
        "brief event lane",
    )
    for brief_item in snapshot.brief_items:
        if brief_item.brief_id not in brief_ids:
            raise ValueError("brief item references an unknown edition")
        if not brief_item.event_id.strip():
            raise ValueError("brief item event_id is required")
