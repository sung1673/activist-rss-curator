"""Protected manual bootstrap for metadata-only official SourceRight grants.

This operation intentionally supports only the two keyless core connectors
(OpenDART and SEC EDGAR) plus explicitly selected CA/AU link-only manifests.
It does not activate EDINET or Companies House while their API connectors are
unavailable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from types import TracebackType
from typing import Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit

import httpx

from .official_source_contracts import DART_METADATA_SOURCE_RIGHT
from .selected_market_ingest import parse_selected_official_links


CONFIRMATION = "BOOTSTRAP_DART_SEC_METADATA_RIGHTS_AT_EXACT_RELEASE_SHA"
_SHA40 = re.compile(r"^[a-f0-9]{40}$")
_SHA64 = re.compile(r"^[a-f0-9]{64}$")
_ENTITY_ID = re.compile(r"^[A-Za-z0-9_.:-]{1,96}$")
_DNS_HOST = re.compile(
    r"(?=.{1,253}\Z)"
    r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+"
    r"[a-z](?:[a-z0-9-]{0,61}[a-z0-9])?"
)
_MAX_RESPONSE_BYTES = 512_000
_MAX_ALLOWLIST_BYTES = 50_000
_IDEMPOTENT_GET_TRANSPORT_ATTEMPTS = 3
_PRODUCTION_V1_BASE = "https://alignpe.gabia.io/activist/api.php/api/v1"
_ALLOWED_EXISTING_CONNECTOR_STATUSES = {
    "active",
    "configured",
    "pending_rights",
}
_ALLOWED_EXISTING_RIGHT_STATUSES = {"active", "pending"}


class SourceRightBootstrapError(RuntimeError):
    """A protected bootstrap precondition or API contract was not satisfied."""


@dataclass(frozen=True)
class BootstrapSource:
    country_code: str
    source_right_id: str
    source_type: str
    source_key: str
    source_name: str
    connector_id: str
    coverage_mode: str
    permission_scope: str
    evidence_uri: str | None = None
    evidence_hash: str | None = None

    def source_right_payload(
        self,
        *,
        valid_from: str,
        code_revision: str,
    ) -> dict[str, object]:
        return {
            "source_right_id": self.source_right_id,
            "source_type": self.source_type,
            "source_key": self.source_key,
            "source_name": self.source_name,
            "permission_scope": self.permission_scope,
            "evidence_uri": self.evidence_uri,
            "evidence_hash": self.evidence_hash,
            "valid_from": valid_from,
            "valid_until": None,
            "revoked_at": None,
            "ai_allowed": False,
            "redistribution_allowed": True,
            "status": "active",
            "notes": (
                "Human-approved protected metadata-only bootstrap at release "
                f"{code_revision}; full text and document bodies are excluded."
            ),
        }


CORE_SOURCES = (
    BootstrapSource(
        country_code="KR",
        source_right_id=str(DART_METADATA_SOURCE_RIGHT["source_right_id"]),
        source_type=str(DART_METADATA_SOURCE_RIGHT["source_type"]),
        source_key=str(DART_METADATA_SOURCE_RIGHT["source_key"]),
        source_name=str(DART_METADATA_SOURCE_RIGHT["source_name"]),
        connector_id="connector:kr:dart",
        coverage_mode="market-wide",
        permission_scope=str(DART_METADATA_SOURCE_RIGHT["permission_scope"]),
        evidence_uri=str(DART_METADATA_SOURCE_RIGHT["evidence_uri"]),
    ),
    BootstrapSource(
        country_code="US",
        source_right_id="official:sec-edgar",
        source_type="official_disclosure",
        source_key="sec-edgar",
        source_name="SEC EDGAR",
        connector_id="connector:us:sec-edgar",
        coverage_mode="market-wide",
        permission_scope=(
            "Official SEC EDGAR metadata only: issuer and accession identifiers, "
            "original filing title or source description, form type, filing or "
            "acceptance date and time, official source URL, and amendment "
            "relationship. Full filing text, document bodies, exhibits, "
            "attachments, media, and third-party content are excluded."
        ),
        evidence_uri=(
            "https://www.sec.gov/search-filings/"
            "edgar-application-programming-interfaces"
        ),
    ),
)

_SELECTED_IDENTITIES = {
    "CA": {
        "source_right_id": "official:ca-issuer-ir",
        "source_type": "official_issuer",
        "source_key": "issuer-ir",
        "source_name": "Canadian issuer IR manual links",
        "connector_id": "connector:ca:issuer-ir",
        "coverage_mode": "link-only",
        "permission_scope": (
            "Human-approved issuer-controlled Canadian IR link metadata only: "
            "issuer identity, original title and language, filing date and time, "
            "official HTTPS URL, event family, and host-evidence hash. Source URLs "
            "are not fetched. Full text, document bodies, attachments, media, "
            "SEDAR+ content, and third-party content are excluded."
        ),
    },
    "AU": {
        "source_right_id": "official:asic-register",
        "source_type": "official_register",
        "source_key": "asic-register",
        "source_name": "ASIC manual register links",
        "connector_id": "connector:au:asic-register",
        "coverage_mode": "link-only",
        "permission_scope": (
            "Human-approved ASIC official-host link metadata only: issuer identity, "
            "original title and language, filing date and time, official HTTPS URL, "
            "event family, and host-evidence hash. Source URLs are not fetched. "
            "Full text, document bodies, attachments, media, ASX content, and "
            "third-party content are excluded."
        ),
    },
}


def _canonical_api_bases(raw: str) -> tuple[str, str]:
    value = raw.strip()
    if (
        not value
        or any(ord(character) <= 32 or ord(character) == 127 for character in value)
        or "\\" in value
        or "%" in value
    ):
        raise SourceRightBootstrapError("operational API base URL is invalid")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise SourceRightBootstrapError("operational API base URL is invalid") from exc
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or port not in (None, 443)
    ):
        raise SourceRightBootstrapError(
            "operational API base URL must be credential-free HTTPS"
        )
    hostname = parsed.hostname.casefold()
    if _DNS_HOST.fullmatch(hostname) is None:
        raise SourceRightBootstrapError(
            "operational API base URL must use a canonical DNS host"
        )
    path = parsed.path.rstrip("/")
    if (
        not path.startswith("/")
        or not path.endswith("/api/v1")
        or "//" in path
        or any(segment in {".", ".."} for segment in path.split("/"))
    ):
        raise SourceRightBootstrapError(
            "operational API base URL must end with /api/v1"
        )
    v1 = urlunsplit(("https", hostname, path, "", ""))
    if v1 != _PRODUCTION_V1_BASE:
        raise SourceRightBootstrapError(
            "operational API base URL must match the fixed production endpoint"
        )
    v2_path = path[: -len("/api/v1")] + "/api/v2"
    v2 = urlunsplit(("https", hostname, v2_path, "", ""))
    return v1, v2


def _api_boolean(value: object, *, field: str) -> bool:
    if value is True or value == 1 or value == "1":
        return True
    if value is False or value == 0 or value == "0":
        return False
    raise SourceRightBootstrapError(
        f"SourceRight {field} value is invalid"
    )


def _require_safe_existing_right(
    source: BootstrapSource,
    right: Mapping[str, object],
) -> str:
    status = right.get("status")
    if not isinstance(status, str) or status not in _ALLOWED_EXISTING_RIGHT_STATUSES:
        raise SourceRightBootstrapError(
            f"{source.country_code} SourceRight status cannot be bootstrapped"
        )
    if right.get("revoked_at") is not None:
        raise SourceRightBootstrapError(
            f"{source.country_code} SourceRight was revoked and cannot be bootstrapped"
        )
    if status == "pending":
        return status

    expected: dict[str, object] = {
        "source_type": source.source_type,
        "source_key": source.source_key,
        "source_name": source.source_name,
        "permission_scope": source.permission_scope,
        "evidence_uri": source.evidence_uri,
        "evidence_hash": source.evidence_hash,
        "valid_until": None,
        "revoked_at": None,
        "ai_allowed": False,
        "redistribution_allowed": True,
        "status": "active",
    }
    actual = dict(right)
    actual["ai_allowed"] = _api_boolean(
        actual.get("ai_allowed"),
        field="ai_allowed",
    )
    actual["redistribution_allowed"] = _api_boolean(
        actual.get("redistribution_allowed"),
        field="redistribution_allowed",
    )
    if any(actual.get(field) != value for field, value in expected.items()):
        raise SourceRightBootstrapError(
            f"{source.country_code} active SourceRight conflicts with the fixed metadata-only grant"
        )
    return status


def _selected_source(country: str, raw_allowlist: str) -> BootstrapSource:
    raw = raw_allowlist.strip()
    if not raw or len(raw.encode("utf-8")) > _MAX_ALLOWLIST_BYTES:
        raise SourceRightBootstrapError(
            f"{country} selected-link allowlist is missing or oversized"
        )
    try:
        decoded = json.loads(raw)
        links = parse_selected_official_links(raw, country_code=country)
    except (TypeError, ValueError, RuntimeError) as exc:
        raise SourceRightBootstrapError(
            f"{country} selected-link allowlist is invalid"
        ) from exc
    if not isinstance(decoded, dict) or not links:
        raise SourceRightBootstrapError(
            f"{country} selected-link allowlist must contain approved records"
        )
    canonical = json.dumps(
        decoded,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    evidence_hash = hashlib.sha256(canonical).hexdigest()
    identity = _SELECTED_IDENTITIES[country]
    return BootstrapSource(
        country_code=country,
        source_right_id=identity["source_right_id"],
        source_type=identity["source_type"],
        source_key=identity["source_key"],
        source_name=identity["source_name"],
        connector_id=identity["connector_id"],
        coverage_mode=identity["coverage_mode"],
        permission_scope=identity["permission_scope"],
        evidence_hash=evidence_hash,
    )


def bootstrap_sources(
    *,
    include_ca: bool,
    include_au: bool,
    environment: Mapping[str, str],
) -> tuple[BootstrapSource, ...]:
    sources = list(CORE_SOURCES)
    if include_ca:
        sources.append(
            _selected_source("CA", environment.get("CA_OFFICIAL_LINKS_JSON", ""))
        )
    if include_au:
        sources.append(
            _selected_source("AU", environment.get("AU_OFFICIAL_LINKS_JSON", ""))
        )
    return tuple(sources)


class _BootstrapClient:
    def __init__(
        self,
        *,
        base_url: str,
        admin_token: str,
        transport: httpx.BaseTransport | None,
    ) -> None:
        self.v1_base, self.v2_base = _canonical_api_bases(base_url)
        self._token = admin_token.strip()
        if len(self._token) < 32:
            raise SourceRightBootstrapError("protected admin token is unavailable")
        self._client = httpx.Client(
            timeout=20.0,
            transport=transport,
            follow_redirects=False,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self._token}",
                "Cache-Control": "no-cache",
                "Connection": "close",
            },
        )

    def __enter__(self) -> _BootstrapClient:
        self._client.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._client.__exit__(exc_type, exc_value, traceback)

    def request(
        self,
        method: str,
        url: str,
        *,
        operation: str,
        payload: dict[str, object] | None = None,
        params: Mapping[str, str | int] | None = None,
        api_version: str,
    ) -> dict[str, object]:
        content = (
            json.dumps(
                payload,
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode("utf-8")
            if payload is not None
            else None
        )
        request_headers = (
            {"Content-Type": "application/json; charset=utf-8"}
            if payload is not None
            else None
        )
        attempts = (
            _IDEMPOTENT_GET_TRANSPORT_ATTEMPTS
            if method.upper() == "GET"
            else 1
        )
        response: httpx.Response | None = None
        last_error: httpx.HTTPError | None = None
        for _attempt in range(attempts):
            try:
                response = self._client.request(
                    method,
                    url,
                    content=content,
                    params=params,
                    headers=request_headers,
                )
                break
            except httpx.HTTPError as exc:
                last_error = exc
        if response is None:
            assert last_error is not None
            raise SourceRightBootstrapError(
                f"{operation} transport failed ({type(last_error).__name__})"
            ) from None
        content = response.content
        content_type = response.headers.get("content-type", "").casefold()
        if (
            response.status_code != 200
            or len(content) > _MAX_RESPONSE_BYTES
            or "application/json" not in content_type
        ):
            raise SourceRightBootstrapError(
                f"{operation} was rejected (HTTP {response.status_code})"
            )
        if response.headers.get("X-BSIDE-API-Version") != api_version:
            raise SourceRightBootstrapError(f"{operation} API identity is invalid")
        try:
            decoded = response.json()
        except ValueError as exc:
            raise SourceRightBootstrapError(
                f"{operation} returned invalid JSON"
            ) from exc
        if not isinstance(decoded, dict) or decoded.get("ok") is not True:
            raise SourceRightBootstrapError(f"{operation} ACK is invalid")
        return decoded

    def health(self, expected_revision: str) -> datetime:
        payload = self.request(
            "GET",
            f"{self.v2_base}/health",
            operation="release identity preflight",
            api_version="v2",
        )
        if (
            payload.get("service") != "bside-global-market-terminal"
            or payload.get("schema_version") != 12
            or payload.get("code_revision") != expected_revision
        ):
            raise SourceRightBootstrapError(
                "deployed release identity does not match the approved SHA"
            )
        server_time = payload.get("time")
        if not isinstance(server_time, str):
            raise SourceRightBootstrapError(
                "deployed release health time is invalid"
            )
        try:
            parsed_time = datetime.fromisoformat(
                server_time[:-1] + "+00:00"
                if server_time.endswith("Z")
                else server_time
            )
        except ValueError as exc:
            raise SourceRightBootstrapError(
                "deployed release health time is invalid"
            ) from exc
        if parsed_time.tzinfo is None or parsed_time.utcoffset() is None:
            raise SourceRightBootstrapError(
                "deployed release health time is invalid"
            )
        return parsed_time.astimezone(timezone.utc)

    def require_closed_release_states(self) -> None:
        v1 = self.request(
            "GET",
            f"{self.v1_base}/admin/release-state",
            operation="v1 release-state preflight",
            api_version="v1",
        )
        v2 = self.request(
            "GET",
            f"{self.v2_base}/admin/release-state",
            operation="v2 release-state preflight",
            api_version="v2",
        )
        v2_data = v2.get("data")
        if (
            v1.get("release_state") != "closed"
            or not isinstance(v2_data, dict)
            or v2_data.get("release_state") != "closed"
        ):
            raise SourceRightBootstrapError(
                "both API release states must remain closed"
            )

    def current_rights(self) -> dict[str, dict[str, object]]:
        result: dict[str, dict[str, object]] = {}
        page = 1
        while page <= 100:
            payload = self.request(
                "GET",
                f"{self.v1_base}/admin/source-rights",
                operation="SourceRight identity preflight",
                params={"page": page, "limit": 25},
                api_version="v1",
            )
            rows = payload.get("data")
            pagination = payload.get("pagination")
            if not isinstance(rows, list) or not isinstance(pagination, dict):
                raise SourceRightBootstrapError(
                    "SourceRight identity preflight ACK is invalid"
                )
            for row in rows:
                if not isinstance(row, dict):
                    raise SourceRightBootstrapError(
                        "SourceRight identity preflight ACK is invalid"
                    )
                source_right_id = row.get("source_right_id")
                source_type = row.get("source_type")
                source_key = row.get("source_key")
                status = row.get("status")
                if not all(
                    isinstance(value, str) and value
                    for value in (
                        source_right_id,
                        source_type,
                        source_key,
                        status,
                    )
                ):
                    raise SourceRightBootstrapError(
                        "SourceRight identity preflight ACK is invalid"
                    )
                source_right_id_text = str(source_right_id)
                source_type_text = str(source_type)
                source_key_text = str(source_key)
                if source_right_id_text in result:
                    raise SourceRightBootstrapError(
                        "SourceRight identity preflight returned a duplicate"
                    )
                normalized = dict(row)
                normalized["source_right_id"] = source_right_id_text
                normalized["source_type"] = source_type_text
                normalized["source_key"] = source_key_text
                result[source_right_id_text] = normalized
            has_more = pagination.get("has_more")
            next_page = pagination.get("next_page")
            if has_more is False and next_page is None:
                return result
            if has_more is not True or next_page != page + 1:
                raise SourceRightBootstrapError(
                    "SourceRight pagination contract is invalid"
                )
            page += 1
        raise SourceRightBootstrapError("SourceRight pagination limit exceeded")

    def connector(self, source: BootstrapSource) -> dict[str, object]:
        payload = self.request(
            "GET",
            f"{self.v2_base}/admin/connectors/{source.connector_id}",
            operation=f"{source.country_code} connector preflight",
            api_version="v2",
        )
        data = payload.get("data")
        connector = data.get("connector") if isinstance(data, dict) else None
        if not isinstance(connector, dict):
            raise SourceRightBootstrapError(
                f"{source.country_code} connector ACK is invalid"
            )
        if (
            connector.get("connector_id") != source.connector_id
            or connector.get("country_code") != source.country_code
            or connector.get("source_right_id") != source.source_right_id
            or connector.get("source_type") != source.source_type
            or connector.get("source_key") != source.source_key
            or connector.get("coverage_mode") != source.coverage_mode
        ):
            raise SourceRightBootstrapError(
                f"{source.country_code} connector identity is invalid"
            )
        if (
            connector.get("connector_status")
            not in _ALLOWED_EXISTING_CONNECTOR_STATUSES
        ):
            raise SourceRightBootstrapError(
                f"{source.country_code} connector status cannot be bootstrapped"
            )
        updated_at = connector.get("updated_at")
        if not isinstance(updated_at, str) or not updated_at.endswith("Z"):
            raise SourceRightBootstrapError(
                f"{source.country_code} connector version is invalid"
            )
        return connector

    def upsert_right(
        self,
        source: BootstrapSource,
        *,
        valid_from: str,
        code_revision: str,
        expected_status: str,
        expected_updated_at: str | None,
    ) -> None:
        right_payload = source.source_right_payload(
            valid_from=valid_from,
            code_revision=code_revision,
        )
        right_payload["expected_status"] = expected_status
        right_payload["expected_updated_at"] = expected_updated_at
        payload = self.request(
            "POST",
            f"{self.v1_base}/admin/source-rights",
            operation=f"{source.country_code} SourceRight registration",
            payload=right_payload,
            api_version="v1",
        )
        if (
            payload.get("source_right_id") != source.source_right_id
            or payload.get("status") != "active"
        ):
            raise SourceRightBootstrapError(
                f"{source.country_code} SourceRight registration ACK is invalid"
            )

    def eligibility(
        self,
        source: BootstrapSource,
        *,
        use: str,
    ) -> str:
        payload = self.request(
            "GET",
            f"{self.v2_base}/ops/source-right-eligibility",
            operation=f"{source.country_code} {use} eligibility",
            params={"source_right_id": source.source_right_id, "use": use},
            api_version="v2",
        )
        revision = payload.get("rights_revision")
        if (
            payload.get("source_right_id") != source.source_right_id
            or payload.get("source_type") != source.source_type
            or payload.get("source_key") != source.source_key
            or payload.get("use") != use
            or payload.get("eligible") is not True
            or not isinstance(revision, str)
            or _SHA64.fullmatch(revision) is None
            or payload.get("redistribution_allowed") is not True
            or payload.get("ai_allowed") is not False
        ):
            raise SourceRightBootstrapError(
                f"{source.country_code} {use} eligibility ACK is invalid"
            )
        return revision

    def configure_connector(
        self,
        source: BootstrapSource,
        *,
        expected_updated_at: str,
        reason: str,
        expected_rights_revision: str,
    ) -> None:
        payload = self.request(
            "POST",
            f"{self.v2_base}/admin/connectors/{source.connector_id}",
            operation=f"{source.country_code} connector configuration",
            payload={
                "target_status": "configured",
                "expected_updated_at": expected_updated_at,
                "reason": reason,
            },
            api_version="v2",
        )
        data = payload.get("data")
        eligibility = (
            data.get("collect_eligibility") if isinstance(data, dict) else None
        )
        if (
            not isinstance(data, dict)
            or data.get("connector_id") != source.connector_id
            or data.get("connector_status") != "configured"
            or not isinstance(eligibility, dict)
            or eligibility.get("eligible") is not True
            or eligibility.get("identity_match") is not True
            or eligibility.get("rights_revision") != expected_rights_revision
            or not isinstance(data.get("audit_id"), str)
            or _ENTITY_ID.fullmatch(str(data["audit_id"])) is None
        ):
            raise SourceRightBootstrapError(
                f"{source.country_code} connector configuration ACK is invalid"
            )


def bootstrap_source_rights(
    *,
    base_url: str,
    admin_token: str,
    expected_release_sha: str,
    code_revision: str,
    reason: str,
    confirmation: str,
    include_ca: bool = False,
    include_au: bool = False,
    environment: Mapping[str, str] | None = None,
    transport: httpx.BaseTransport | None = None,
    now: datetime | None = None,
) -> dict[str, object]:
    expected = expected_release_sha.strip().casefold()
    actual = code_revision.strip().casefold()
    normalized_reason = reason.strip()
    if (
        _SHA40.fullmatch(expected) is None
        or actual != expected
        or not 20 <= len(normalized_reason) <= 500
        or confirmation != CONFIRMATION
    ):
        raise SourceRightBootstrapError(
            "release SHA, reason, or explicit confirmation is invalid"
        )
    sources = bootstrap_sources(
        include_ca=include_ca,
        include_au=include_au,
        environment=environment or {},
    )
    observed_at = now or datetime.now(timezone.utc)
    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise SourceRightBootstrapError("bootstrap time must include a timezone")
    observed_at = observed_at.astimezone(timezone.utc)
    completed: list[dict[str, object]] = []
    with _BootstrapClient(
        base_url=base_url,
        admin_token=admin_token,
        transport=transport,
    ) as client:
        server_time = client.health(expected)
        if abs((server_time - observed_at).total_seconds()) > 300:
            raise SourceRightBootstrapError(
                "runner and deployed API clocks differ by more than five minutes"
            )
        valid_from = (
            server_time.replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )
        client.require_closed_release_states()
        current_rights = client.current_rights()
        existing_right_statuses: dict[str, str | None] = {}
        existing_right_versions: dict[str, str | None] = {}
        connector_versions: dict[str, str] = {}
        connector_statuses: dict[str, str] = {}
        right_revisions: dict[str, str] = {}
        for source in sources:
            current_right = current_rights.get(source.source_right_id)
            if current_right is not None:
                current_identity = (
                    current_right.get("source_type"),
                    current_right.get("source_key"),
                )
                if current_identity != (
                    source.source_type,
                    source.source_key,
                ):
                    raise SourceRightBootstrapError(
                        f"{source.country_code} SourceRight identity is immutable and mismatched"
                    )
                existing_status = _require_safe_existing_right(
                    source,
                    current_right,
                )
                existing_right_statuses[source.source_right_id] = existing_status
                updated_at = current_right.get("updated_at")
                if not isinstance(updated_at, str) or not updated_at.strip():
                    raise SourceRightBootstrapError(
                        f"{source.country_code} SourceRight version is invalid"
                    )
                existing_right_versions[source.source_right_id] = updated_at
                if existing_status == "active":
                    collect_revision = client.eligibility(
                        source,
                        use="collect",
                    )
                    public_revision = client.eligibility(
                        source,
                        use="public",
                    )
                    if collect_revision != public_revision:
                        raise SourceRightBootstrapError(
                            f"{source.country_code} SourceRight changed during preflight"
                        )
                    right_revisions[source.source_right_id] = collect_revision
            else:
                existing_right_statuses[source.source_right_id] = None
                existing_right_versions[source.source_right_id] = None
            connector = client.connector(source)
            connector_versions[source.connector_id] = str(connector["updated_at"])
            connector_statuses[source.connector_id] = str(
                connector["connector_status"]
            )

        for source in sources:
            if existing_right_statuses[source.source_right_id] != "active":
                client.upsert_right(
                    source,
                    valid_from=valid_from,
                    code_revision=expected,
                    expected_status=(
                        existing_right_statuses[source.source_right_id]
                        or "missing"
                    ),
                    expected_updated_at=existing_right_versions[
                        source.source_right_id
                    ],
                )
            collect_revision = client.eligibility(source, use="collect")
            public_revision = client.eligibility(source, use="public")
            if collect_revision != public_revision:
                raise SourceRightBootstrapError(
                    f"{source.country_code} SourceRight changed during verification"
                )
            right_revisions[source.source_right_id] = collect_revision

        for source in sources:
            current = client.connector(source)
            expected_updated_at = connector_versions[source.connector_id]
            if current.get("updated_at") != expected_updated_at:
                raise SourceRightBootstrapError(
                    f"{source.country_code} connector changed during bootstrap"
                )
            eligibility = current.get("collect_eligibility")
            if (
                not isinstance(eligibility, dict)
                or eligibility.get("eligible") is not True
                or eligibility.get("identity_match") is not True
                or eligibility.get("rights_revision")
                != right_revisions[source.source_right_id]
            ):
                raise SourceRightBootstrapError(
                    f"{source.country_code} connector is not eligible after registration"
                )
            initial_status = connector_statuses[source.connector_id]
            expected_final_status = initial_status
            if initial_status == "pending_rights":
                client.configure_connector(
                    source,
                    expected_updated_at=expected_updated_at,
                    reason=normalized_reason,
                    expected_rights_revision=right_revisions[
                        source.source_right_id
                    ],
                )
                expected_final_status = "configured"
            final_connector = client.connector(source)
            final_eligibility = final_connector.get("collect_eligibility")
            final_collect_revision = client.eligibility(source, use="collect")
            final_public_revision = client.eligibility(source, use="public")
            if (
                final_connector.get("connector_status") != expected_final_status
                or not isinstance(final_eligibility, dict)
                or final_eligibility.get("eligible") is not True
                or final_eligibility.get("identity_match") is not True
                or final_eligibility.get("rights_revision")
                != right_revisions[source.source_right_id]
                or final_collect_revision != right_revisions[source.source_right_id]
                or final_public_revision != right_revisions[source.source_right_id]
            ):
                raise SourceRightBootstrapError(
                    f"{source.country_code} final connector verification failed"
                )
            completed.append(
                {
                    "country_code": source.country_code,
                    "source_right_id": source.source_right_id,
                    "connector_id": source.connector_id,
                    "connector_status": expected_final_status,
                    "collect_eligible": True,
                    "public_eligible": True,
                    "ai_allowed": False,
                    "full_text_redistribution": False,
                }
            )

        client.require_closed_release_states()
        client.health(expected)

    return {
        "ok": True,
        "operation": "metadata-only-source-right-bootstrap",
        "code_revision": expected,
        "release_states": {"v1": "closed", "v2": "closed"},
        "source_count": len(completed),
        "sources": completed,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Bootstrap protected metadata-only official SourceRight grants"
    )
    parser.add_argument("--expected-release-sha", required=True)
    parser.add_argument("--reason", required=True)
    parser.add_argument("--confirmation", required=True)
    parser.add_argument("--include-ca", action="store_true")
    parser.add_argument("--include-au", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = bootstrap_source_rights(
            base_url=os.environ.get("BSIDE_API_BASE_URL", ""),
            admin_token=os.environ.get("BSIDE_ADMIN_TOKEN", ""),
            expected_release_sha=args.expected_release_sha,
            code_revision=os.environ.get("GITHUB_SHA", ""),
            reason=args.reason,
            confirmation=args.confirmation,
            include_ca=args.include_ca,
            include_au=args.include_au,
            environment=os.environ,
        )
    except SourceRightBootstrapError as exc:
        print(f"SourceRight bootstrap failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
