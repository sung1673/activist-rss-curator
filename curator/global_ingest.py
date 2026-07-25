"""Fail-closed execution path for non-Korean global official connectors.

OpenDART deliberately does not use this module.  Korea continues through the
established official-ingest pipeline, while this runner sends the SEC
current-filings Atom feed plus completed-day index reconciliation to the v2
review queue.  EDINET and Companies House API collection must be explicitly
activated.  Without their credentials, this runner records the narrower
keyless/link-only coverage instead of scraping either public viewer.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import sys
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.parse import urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import httpx

from .global_connectors import (
    CompaniesHouseFilingHistoryConnector,
    EdinetDocumentsConnector,
    GlobalConnectorEnvelope,
    GlobalConnectorRequest,
    GlobalSourceConnector,
    IssuerReference,
    SecDailyIndexConnector,
    SecHybridConnector,
)
from .official_source_rights import (
    GlobalOfficialSourceRightClient,
    OfficialSourceRightEligibility,
)


SUPPORTED_COUNTRIES = ("US", "JP", "GB")
_CODE_REVISION = re.compile(r"^[a-f0-9]{7,64}$")
_COMPANY_NUMBER = re.compile(r"^[A-Z0-9]{6,10}$")
_CONNECTOR_ID = re.compile(r"^connector:[a-z]{2}:[a-z0-9_.:-]{1,64}$")
_GLOBAL_BATCH_ID = re.compile(r"^global-batch:[a-f0-9]{64}$")
_SAFE_SERVER_ERROR = re.compile(r"^[a-z][a-z0-9_]{1,63}$")
MAX_RECORDS_PER_INGEST = 500
MAX_COMPANIES_HOUSE_ISSUERS = 50
MAX_AUTOMATIC_WINDOW_DAYS = 31
AUTOMATIC_OVERLAP_DAYS = 1
MIN_AUTOMATIC_CHECKPOINT_DATE = date(2015, 1, 1)
EDINET_CONNECTOR_MODES = {"link-only", "active"}
COMPANIES_HOUSE_CONNECTOR_MODES = {"keyless", "active"}


@dataclass(frozen=True)
class GlobalIngestExecutionMode:
    country_code: str
    mode: str
    api_active: bool
    coverage_mode: str
    ingest_mode: str
    reason: str | None


class GlobalIngestError(RuntimeError):
    """A sanitized, evidence-safe failure."""

    def __init__(self, code: str, *, http_status: int | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.http_status = http_status


class GlobalIngestConfigurationError(GlobalIngestError):
    pass


class GlobalIngestApiError(GlobalIngestError):
    pass


class EligibilityClient(Protocol):
    def check(
        self,
        source_right_id: str,
        *,
        use: str = "collect",
    ) -> OfficialSourceRightEligibility: ...


class IngestApiClient(Protocol):
    def submit(
        self,
        *,
        envelope: GlobalConnectorEnvelope,
        chunk: "GlobalIngestChunk",
        idempotency_key: str,
        code_revision: str,
        replay_only: bool = False,
    ) -> "GlobalIngestReceipt": ...


class CheckpointClient(Protocol):
    def fetch_checkpoint(
        self,
        connector_id: str,
    ) -> "GlobalConnectorCheckpoint": ...


@dataclass(frozen=True)
class GlobalIngestReceipt:
    ingest_id: str
    connector_id: str
    raw_count: int
    acknowledged_count: int
    idempotent: bool
    api_version: str = "v2"


@dataclass(frozen=True)
class GlobalIngestChunk:
    index: int
    count: int
    batch_raw_count: int
    batch_acknowledged_count: int
    batch_request_count: int
    batch_id: str
    window_start: str
    window_end_exclusive: str

    def to_payload(self) -> dict[str, object]:
        return {
            "index": self.index,
            "count": self.count,
            "batch_raw_count": self.batch_raw_count,
            "batch_acknowledged_count": self.batch_acknowledged_count,
            "batch_request_count": self.batch_request_count,
            "batch_id": self.batch_id,
            "window_start": self.window_start,
            "window_end_exclusive": self.window_end_exclusive,
        }


@dataclass(frozen=True)
class GlobalConnectorCheckpoint:
    connector_id: str
    window_end_exclusive: date | None
    batch_id: str | None
    last_success_at: str | None
    last_checked_at: str | None
    code_revision: str | None
    source_cursor: str | None = None


@dataclass(frozen=True)
class GlobalIngestResult:
    country_code: str
    connector_id: str
    source_right_id: str
    window_start: str
    window_end_exclusive: str
    idempotency_key: str
    code_revision: str
    request_count: int
    raw_count: int
    record_count: int
    lifecycle_observation_count: int
    acknowledged_count: int
    ingest_id: str
    idempotent: bool
    api_version: str
    chunk_count: int = 1
    idempotency_keys: tuple[str, ...] = ()
    ingest_ids: tuple[str, ...] = ()
    idempotent_chunk_count: int = 0

    def evidence(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "status": "succeeded",
            "country_code": self.country_code,
            "connector_id": self.connector_id,
            "source_right_id": self.source_right_id,
            "window": {
                "start": self.window_start,
                "end_exclusive": self.window_end_exclusive,
            },
            "idempotency_key": self.idempotency_key,
            "code_revision": self.code_revision,
            "request_count": self.request_count,
            "raw_count": self.raw_count,
            "record_count": self.record_count,
            "lifecycle_observation_count": self.lifecycle_observation_count,
            "acknowledged_count": self.acknowledged_count,
            "ingest_id": self.ingest_id,
            "idempotent": self.idempotent,
            "api_version": self.api_version,
            "chunk_count": self.chunk_count,
            "idempotency_keys": list(self.idempotency_keys),
            "ingest_ids": list(self.ingest_ids),
            "idempotent_chunk_count": self.idempotent_chunk_count,
        }


class _ExactReplayConnector:
    """Fetch one source envelope and reuse it for one exact API replay."""

    def __init__(self, connector: GlobalSourceConnector) -> None:
        self._connector = connector
        self.descriptor = connector.descriptor
        self._request: GlobalConnectorRequest | None = None
        self._envelope: GlobalConnectorEnvelope | None = None
        self.fetch_calls = 0
        self.source_fetches = 0

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
        self.fetch_calls += 1
        if self._envelope is not None:
            if request != self._request:
                raise GlobalIngestConfigurationError(
                    "global_ingest_replay_request_changed"
                )
            return self._envelope
        self.source_fetches += 1
        envelope = self._connector.fetch(
            request,
            eligibility=eligibility,
            eligibility_provider=eligibility_provider,
            now=now,
        )
        self._request = request
        self._envelope = envelope
        return envelope


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


def content_idempotency_key(
    *,
    envelope: GlobalConnectorEnvelope,
    code_revision: str,
    window_start: date | None = None,
    window_end_exclusive: date | None = None,
    chunk_index: int = 0,
) -> str:
    """Return a stable key that excludes observation-time-only fields."""

    revision = _validate_code_revision(code_revision)
    stable_envelope = envelope.to_payload()
    stable_envelope.pop("retrieved_at", None)
    # Transport work can grow when a current-history endpoint gains newer
    # pages. It is operational telemetry, not the identity of a completed-day
    # source result.
    stable_envelope.pop("request_count", None)
    raw_records = stable_envelope.get("records")
    if isinstance(raw_records, list):
        for record in raw_records:
            if isinstance(record, dict):
                record.pop("first_observed_at", None)
    content = {
        "code_revision": revision,
        "window_start": window_start.isoformat() if window_start else None,
        "window_end_exclusive": (
            window_end_exclusive.isoformat()
            if window_end_exclusive
            else None
        ),
        "chunk_index": chunk_index,
        "envelope": stable_envelope,
    }
    digest = hashlib.sha256(_canonical_json(content).encode("utf-8")).hexdigest()
    return f"global-ingest-v2:{envelope.country_code.casefold()}:{digest}"


def global_ingest_batch_id(
    *,
    envelope: GlobalConnectorEnvelope,
    window_start: date,
    window_end_exclusive: date,
    code_revision: str,
) -> str:
    revision = _validate_code_revision(code_revision)
    stable_envelope = envelope.to_payload()
    stable_envelope.pop("retrieved_at", None)
    stable_envelope.pop("request_count", None)
    raw_records = stable_envelope.get("records")
    if isinstance(raw_records, list):
        for record in raw_records:
            if isinstance(record, dict):
                record.pop("first_observed_at", None)
    payload = {
        "code_revision": revision,
        "window_start": window_start.isoformat(),
        "window_end_exclusive": window_end_exclusive.isoformat(),
        "envelope": stable_envelope,
    }
    digest = hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    return f"global-batch:{digest}"


def global_ingest_chunk(
    *,
    envelope: GlobalConnectorEnvelope,
    window_start: date,
    window_end_exclusive: date,
    index: int,
    count: int,
    code_revision: str,
) -> GlobalIngestChunk:
    return GlobalIngestChunk(
        index=index + 1,
        count=count,
        batch_raw_count=envelope.raw_count,
        batch_acknowledged_count=(
            len(envelope.records) + len(envelope.lifecycle_observations)
        ),
        batch_request_count=envelope.request_count,
        batch_id=global_ingest_batch_id(
            envelope=envelope,
            window_start=window_start,
            window_end_exclusive=window_end_exclusive,
            code_revision=code_revision,
        ),
        window_start=window_start.isoformat(),
        window_end_exclusive=window_end_exclusive.isoformat(),
    )


def chunk_connector_envelope(
    envelope: GlobalConnectorEnvelope,
    *,
    window_start: date,
    window_end_exclusive: date,
    limit: int = MAX_RECORDS_PER_INGEST,
) -> tuple[GlobalConnectorEnvelope, ...]:
    if limit < 1 or limit > MAX_RECORDS_PER_INGEST:
        raise GlobalIngestConfigurationError("invalid_ingest_chunk_limit")
    record_chunks = max(1, (len(envelope.records) + limit - 1) // limit)
    lifecycle_chunks = max(
        1,
        (len(envelope.lifecycle_observations) + limit - 1) // limit,
    )
    chunk_count = max(record_chunks, lifecycle_chunks)
    stable_scope = {
        "connector_id": envelope.connector_id,
        "window_start": window_start.isoformat(),
        "window_end_exclusive": window_end_exclusive.isoformat(),
        "record_ids": [record.record_id for record in envelope.records],
        "observation_ids": [
            item.observation_id
            for item in envelope.lifecycle_observations
        ],
    }
    scope_digest = hashlib.sha256(
        _canonical_json(stable_scope).encode("utf-8")
    ).hexdigest()[:24]
    chunks: list[GlobalConnectorEnvelope] = []
    assigned_raw_count = 0
    for index in range(chunk_count):
        record_start = index * limit
        observation_start = index * limit
        is_last = index == chunk_count - 1
        next_cursor = (
            envelope.next_cursor
            if is_last
            else (
                "global-ingest-chunk:"
                f"{window_start.isoformat()}:{window_end_exclusive.isoformat()}:"
                f"{index + 1}:{chunk_count}:{scope_digest}"
            )
        )
        records = envelope.records[record_start : record_start + limit]
        lifecycle = envelope.lifecycle_observations[
            observation_start : observation_start + limit
        ]
        chunk_accepted_count = len(records) + len(lifecycle)
        chunk_raw_count = (
            envelope.raw_count - assigned_raw_count
            if is_last
            else chunk_accepted_count
        )
        assigned_raw_count += chunk_raw_count
        chunks.append(
            replace(
                envelope,
                records=records,
                lifecycle_observations=lifecycle,
                next_cursor=next_cursor,
                exhausted=envelope.exhausted if is_last else False,
                request_count=envelope.request_count if is_last else 0,
                raw_count=chunk_raw_count,
            )
        )
    return tuple(chunks)


def _validate_code_revision(value: str) -> str:
    revision = str(value or "").strip().casefold()
    if _CODE_REVISION.fullmatch(revision) is None:
        raise GlobalIngestConfigurationError("invalid_code_revision")
    return revision


def _validated_v2_base_url(raw: str) -> str:
    value = str(raw or "").strip()
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise GlobalIngestConfigurationError("invalid_v2_api_base_url")
    path = parsed.path.rstrip("/")
    for suffix in ("/api/v1", "/api/v2"):
        if path.endswith(suffix):
            path = path[: -len(suffix)]
            break
    return urlunsplit((parsed.scheme, parsed.netloc, path + "/api/v2", "", ""))


class V2GlobalIngestClient:
    """Small authenticated client with strict v2 response validation."""

    def __init__(
        self,
        *,
        base_url: str,
        token: str,
        timeout: float = 30.0,
        transport: httpx.BaseTransport | None = None,
        client_factory: Callable[..., httpx.Client] = httpx.Client,
    ) -> None:
        self.base_url = _validated_v2_base_url(base_url).rstrip("/")
        self.token = str(token or "").strip()
        if not self.token or "\r" in self.token or "\n" in self.token:
            raise GlobalIngestConfigurationError("missing_ops_token")
        self.timeout = timeout
        self.transport = transport
        self.client_factory = client_factory

    @staticmethod
    def _response_object(response: httpx.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise GlobalIngestApiError(
                "malformed_api_response",
                http_status=response.status_code,
            ) from exc
        if not isinstance(payload, dict):
            raise GlobalIngestApiError(
                "malformed_api_response",
                http_status=response.status_code,
            )
        return payload

    @staticmethod
    def _checkpoint_timestamp(value: object, field_name: str) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value:
            raise GlobalIngestApiError(
                f"malformed_checkpoint_{field_name}"
            )
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise GlobalIngestApiError(
                f"malformed_checkpoint_{field_name}"
            ) from exc
        if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
            raise GlobalIngestApiError(
                f"malformed_checkpoint_{field_name}"
            )
        return value

    def fetch_checkpoint(
        self,
        connector_id: str,
    ) -> GlobalConnectorCheckpoint:
        normalized_id = str(connector_id or "").strip()
        if _CONNECTOR_ID.fullmatch(normalized_id) is None:
            raise GlobalIngestConfigurationError("invalid_connector_id")
        try:
            with self.client_factory(
                timeout=self.timeout,
                transport=self.transport,
                follow_redirects=False,
            ) as client:
                response = client.get(
                    f"{self.base_url}/ops/connectors/"
                    f"{normalized_id}/checkpoint",
                    headers={
                        "Accept": "application/json",
                        "Authorization": f"Bearer {self.token}",
                    },
                )
        except httpx.HTTPError as exc:
            raise GlobalIngestApiError(
                "checkpoint_request_failed"
            ) from exc
        payload = self._response_object(response)
        if response.status_code != 200:
            server_code = payload.get("error")
            safe_code = (
                str(server_code)
                if isinstance(server_code, str)
                and _SAFE_SERVER_ERROR.fullmatch(server_code)
                else "checkpoint_request_rejected"
            )
            raise GlobalIngestApiError(
                safe_code,
                http_status=response.status_code,
            )
        data = payload.get("data")
        expected_fields = {
            "connector_id",
            "cursor_json",
            "last_success_at",
            "last_checked_at",
            "code_revision",
        }
        if (
            payload.get("ok") is not True
            or payload.get("api_version") != "v2"
            or not isinstance(data, dict)
            or set(data) != expected_fields
            or data.get("connector_id") != normalized_id
        ):
            raise GlobalIngestApiError("malformed_checkpoint_response")
        last_success_at = self._checkpoint_timestamp(
            data.get("last_success_at"),
            "last_success_at",
        )
        last_checked_at = self._checkpoint_timestamp(
            data.get("last_checked_at"),
            "last_checked_at",
        )
        code_revision_value = data.get("code_revision")
        if code_revision_value is not None and (
            not isinstance(code_revision_value, str)
            or _CODE_REVISION.fullmatch(code_revision_value) is None
        ):
            raise GlobalIngestApiError("malformed_checkpoint_revision")
        cursor = data.get("cursor_json")
        if cursor is None:
            if last_success_at is not None or code_revision_value is not None:
                raise GlobalIngestApiError(
                    "inconsistent_empty_connector_checkpoint"
                )
            return GlobalConnectorCheckpoint(
                connector_id=normalized_id,
                window_end_exclusive=None,
                batch_id=None,
                last_success_at=None,
                last_checked_at=last_checked_at,
                code_revision=None,
            )
        if not isinstance(cursor, dict):
            raise GlobalIngestApiError("malformed_checkpoint_response")
        cursor_keys = set(cursor)
        schema_version = cursor.get("schema_version")
        legacy_keys = {"schema_version", "window_end_exclusive", "batch_id"}
        live_keys = legacy_keys | {"source_cursor"}
        if (
            frozenset(cursor_keys)
            not in {frozenset(legacy_keys), frozenset(live_keys)}
            or schema_version not in {1, 2}
            or (schema_version == 1 and cursor_keys != legacy_keys)
            or (schema_version == 2 and cursor_keys != live_keys)
            or not isinstance(cursor.get("window_end_exclusive"), str)
            or not isinstance(cursor.get("batch_id"), str)
            or _GLOBAL_BATCH_ID.fullmatch(str(cursor["batch_id"])) is None
            or (
                schema_version == 2
                and (
                    not isinstance(cursor.get("source_cursor"), str)
                    or not str(cursor["source_cursor"]).startswith(
                        "sec-current-v1:"
                    )
                    or len(str(cursor["source_cursor"])) > 1000
                )
            )
            or last_success_at is None
            or last_checked_at is None
            or code_revision_value is None
        ):
            raise GlobalIngestApiError("malformed_checkpoint_response")
        try:
            completed_through = date.fromisoformat(
                str(cursor["window_end_exclusive"])
            )
        except ValueError as exc:
            raise GlobalIngestApiError(
                "malformed_checkpoint_window"
            ) from exc
        if completed_through.isoformat() != cursor["window_end_exclusive"]:
            raise GlobalIngestApiError("malformed_checkpoint_window")
        return GlobalConnectorCheckpoint(
            connector_id=normalized_id,
            window_end_exclusive=completed_through,
            batch_id=str(cursor["batch_id"]),
            last_success_at=last_success_at,
            last_checked_at=last_checked_at,
            code_revision=str(code_revision_value),
            source_cursor=(
                str(cursor["source_cursor"])
                if schema_version == 2
                else None
            ),
        )

    def submit(
        self,
        *,
        envelope: GlobalConnectorEnvelope,
        chunk: GlobalIngestChunk,
        idempotency_key: str,
        code_revision: str,
        replay_only: bool = False,
    ) -> GlobalIngestReceipt:
        revision = _validate_code_revision(code_revision)
        expected_acknowledged = (
            len(envelope.records) + len(envelope.lifecycle_observations)
        )
        body = {
            "idempotency_key": idempotency_key,
            "code_revision": revision,
            "envelope": {
                **envelope.to_payload(),
                "chunk": chunk.to_payload(),
            },
        }
        if replay_only:
            body["ingest_mode"] = "replay"
        try:
            with self.client_factory(
                timeout=self.timeout,
                transport=self.transport,
            ) as client:
                response = client.post(
                    f"{self.base_url}/ops/ingest",
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {self.token}",
                        "Idempotency-Key": idempotency_key,
                    },
                    content=_canonical_json(body).encode("utf-8"),
                )
        except httpx.HTTPError as exc:
            raise GlobalIngestApiError("api_request_failed") from exc

        payload = self._response_object(response)
        if response.status_code != 200:
            server_code = payload.get("error")
            safe_code = (
                str(server_code)
                if isinstance(server_code, str)
                and _SAFE_SERVER_ERROR.fullmatch(server_code)
                else "api_rejected_ingest"
            )
            raise GlobalIngestApiError(
                safe_code,
                http_status=response.status_code,
            )
        data = payload.get("data")
        if (
            payload.get("ok") is not True
            or payload.get("api_version") != "v2"
            or not isinstance(data, dict)
        ):
            raise GlobalIngestApiError(
                "malformed_api_response",
                http_status=response.status_code,
            )
        ingest_id = data.get("ingest_id")
        connector_id = data.get("connector_id")
        raw_count = data.get("raw_count")
        acknowledged_count = data.get("acknowledged_count")
        idempotent = data.get("idempotent")
        if (
            not isinstance(ingest_id, str)
            or not ingest_id
            or connector_id != envelope.connector_id
            or not isinstance(raw_count, int)
            or isinstance(raw_count, bool)
            or raw_count != envelope.raw_count
            or not isinstance(acknowledged_count, int)
            or isinstance(acknowledged_count, bool)
            or acknowledged_count != expected_acknowledged
            or not isinstance(idempotent, bool)
        ):
            raise GlobalIngestApiError(
                "api_acknowledgment_mismatch",
                http_status=response.status_code,
            )
        return GlobalIngestReceipt(
            ingest_id=ingest_id,
            connector_id=connector_id,
            raw_count=raw_count,
            acknowledged_count=acknowledged_count,
            idempotent=idempotent,
        )


def parse_companies_house_allowlist(raw: str) -> tuple[IssuerReference, ...]:
    """Parse an explicit, closed-schema Companies House issuer allowlist."""

    try:
        payload = json.loads(str(raw or ""))
    except (TypeError, ValueError) as exc:
        raise GlobalIngestConfigurationError(
            "invalid_companies_house_allowlist"
        ) from exc
    if not isinstance(payload, list) or not payload:
        raise GlobalIngestConfigurationError("empty_companies_house_allowlist")
    if len(payload) > MAX_COMPANIES_HOUSE_ISSUERS:
        raise GlobalIngestConfigurationError(
            "companies_house_allowlist_limit_exceeded"
        )
    allowed_keys = {"company_number", "legal_name", "market", "ticker"}
    issuers: list[IssuerReference] = []
    seen: set[str] = set()
    for item in payload:
        if not isinstance(item, dict) or set(item) - allowed_keys:
            raise GlobalIngestConfigurationError(
                "invalid_companies_house_allowlist"
            )
        company_number = str(item.get("company_number") or "").strip().upper()
        legal_name = str(item.get("legal_name") or "").strip()
        market = str(item.get("market") or "LSE").strip()
        ticker = str(item.get("ticker") or "").strip()
        if (
            _COMPANY_NUMBER.fullmatch(company_number) is None
            or not legal_name
            or len(legal_name) > 255
            or len(market) > 32
            or len(ticker) > 32
            or company_number in seen
        ):
            raise GlobalIngestConfigurationError(
                "invalid_companies_house_allowlist"
            )
        seen.add(company_number)
        issuers.append(
            IssuerReference(
                namespace="GB:COMPANIES_HOUSE",
                identifier_type="COMPANY_NUMBER",
                value=company_number,
                legal_name=legal_name,
                market=market,
                ticker=ticker,
            )
        )
    return tuple(sorted(issuers, key=lambda issuer: issuer.value))


def global_ingest_execution_mode(
    country_code: str,
    *,
    environment: Mapping[str, str],
) -> GlobalIngestExecutionMode:
    """Resolve an explicit API mode without treating public HTML as an API.

    Credentials alone never activate JP or GB API collection.  This prevents a
    repository/environment secret from silently widening the declared source
    coverage.  The keyless modes make no source request in this runner.
    """

    country = str(country_code or "").strip().upper()
    if country == "US":
        return GlobalIngestExecutionMode(
            country_code=country,
            mode="active",
            api_active=True,
            coverage_mode="market-wide",
            ingest_mode="official-api",
            reason=None,
        )
    if country == "JP":
        mode = str(
            environment.get("EDINET_CONNECTOR_MODE") or "link-only"
        ).strip().casefold()
        if mode not in EDINET_CONNECTOR_MODES:
            raise GlobalIngestConfigurationError(
                "invalid_edinet_connector_mode"
            )
        return GlobalIngestExecutionMode(
            country_code=country,
            mode=mode,
            api_active=mode == "active",
            coverage_mode=("market-wide" if mode == "active" else "link-only"),
            ingest_mode=(
                "official-api" if mode == "active" else "official-links-only"
            ),
            reason=(
                None
                if mode == "active"
                else "edinet_api_key_required_html_scraping_prohibited"
            ),
        )
    if country == "GB":
        mode = str(
            environment.get("COMPANIES_HOUSE_CONNECTOR_MODE") or "keyless"
        ).strip().casefold()
        if mode not in COMPANIES_HOUSE_CONNECTOR_MODES:
            raise GlobalIngestConfigurationError(
                "invalid_companies_house_connector_mode"
            )
        return GlobalIngestExecutionMode(
            country_code=country,
            mode=mode,
            api_active=mode == "active",
            coverage_mode=(
                "official-register" if mode == "active" else "link-only"
            ),
            ingest_mode=(
                "official-api"
                if mode == "active"
                else "official-bulk-basic-register-links"
            ),
            reason=(
                None
                if mode == "active"
                else "companies_house_api_key_required_for_filing_history"
            ),
        )
    raise GlobalIngestConfigurationError("unsupported_global_ingest_country")


def build_connector(
    country_code: str,
    *,
    environment: Mapping[str, str],
    completed_day_only: bool = False,
) -> tuple[GlobalSourceConnector, tuple[IssuerReference, ...]]:
    country = str(country_code or "").strip().upper()
    execution_mode = global_ingest_execution_mode(
        country,
        environment=environment,
    )
    if not execution_mode.api_active:
        raise GlobalIngestConfigurationError(
            f"{country.casefold()}_official_api_connector_not_active"
        )
    if country == "US":
        user_agent = str(environment.get("SEC_EDGAR_USER_AGENT", "")).strip()
        if not user_agent:
            raise GlobalIngestConfigurationError("missing_sec_user_agent")
        if completed_day_only:
            return SecDailyIndexConnector(user_agent=user_agent), ()
        return SecHybridConnector(user_agent=user_agent), ()
    if country == "JP":
        api_key = str(environment.get("EDINET_API_KEY", "")).strip()
        if not api_key:
            raise GlobalIngestConfigurationError("missing_edinet_api_key")
        return EdinetDocumentsConnector(api_key=api_key), ()
    if country == "GB":
        api_key = str(
            environment.get("COMPANIES_HOUSE_API_KEY", "")
        ).strip()
        if not api_key:
            raise GlobalIngestConfigurationError(
                "missing_companies_house_api_key"
            )
        issuers = parse_companies_house_allowlist(
            str(environment.get("COMPANIES_HOUSE_ISSUERS_JSON", ""))
        )
        return CompaniesHouseFilingHistoryConnector(api_key=api_key), issuers
    # This also explicitly keeps KR/DART away from /api/v2/ops/ingest.
    raise GlobalIngestConfigurationError("unsupported_global_ingest_country")


def default_completed_window(
    *,
    today: date | None = None,
) -> tuple[date, date]:
    """Return two completed dates as a half-open range."""

    end_exclusive = today or datetime.now(timezone.utc).date()
    return end_exclusive - timedelta(days=2), end_exclusive


def sec_completed_day_limit(
    *,
    now: datetime | None = None,
) -> date:
    """Return the conservative exclusive date for SEC daily-index files.

    The current Atom feed remains intraday.  Historical reconciliation does
    not require the prior filing day's daily index until 06:00 America/New_York,
    leaving a fail-closed publication buffer after the SEC's overnight build.
    """

    observed = now or datetime.now(timezone.utc)
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise GlobalIngestConfigurationError("sec_completed_limit_requires_timezone")
    eastern = observed.astimezone(ZoneInfo("America/New_York"))
    return eastern.date() - (timedelta(days=1) if eastern.hour < 6 else timedelta())


def automatic_completed_window(
    checkpoint: GlobalConnectorCheckpoint,
    *,
    today: date | None = None,
) -> tuple[date, date]:
    """Resume a completed-day window with one overlap day and no skipped backlog."""

    end_limit = today or datetime.now(timezone.utc).date()
    checkpoint_end = checkpoint.window_end_exclusive
    if checkpoint_end is None:
        if checkpoint.batch_id is not None:
            raise GlobalIngestConfigurationError(
                "inconsistent_empty_connector_checkpoint"
            )
        return default_completed_window(today=end_limit)
    if (
        checkpoint.batch_id is None
        or _GLOBAL_BATCH_ID.fullmatch(checkpoint.batch_id) is None
        or checkpoint_end < MIN_AUTOMATIC_CHECKPOINT_DATE
    ):
        raise GlobalIngestConfigurationError(
            "invalid_connector_checkpoint"
        )
    if checkpoint_end > end_limit:
        raise GlobalIngestConfigurationError(
            "future_connector_checkpoint"
        )
    window_start = checkpoint_end - timedelta(
        days=AUTOMATIC_OVERLAP_DAYS
    )
    window_end = min(
        end_limit,
        window_start + timedelta(days=MAX_AUTOMATIC_WINDOW_DAYS),
    )
    validate_window(window_start, window_end)
    return window_start, window_end


def select_completed_window(
    *,
    from_date: str,
    to_date: str,
    connector_id: str,
    checkpoint_client: CheckpointClient,
    checkpoint: GlobalConnectorCheckpoint | None = None,
    today: date | None = None,
) -> tuple[date, date]:
    """Use an explicit pair verbatim, otherwise load the durable checkpoint."""

    from_value = str(from_date or "").strip()
    to_value = str(to_date or "").strip()
    if bool(from_value) != bool(to_value):
        raise GlobalIngestConfigurationError(
            "partial_explicit_window"
        )
    if from_value:
        explicit_start = _parse_date(from_value, "from_date")
        explicit_end = _parse_date(to_value, "to_date")
        assert explicit_start is not None
        assert explicit_end is not None
        validate_window(explicit_start, explicit_end)
        return explicit_start, explicit_end
    durable = checkpoint or checkpoint_client.fetch_checkpoint(connector_id)
    if durable.connector_id != connector_id:
        raise GlobalIngestConfigurationError(
            "connector_checkpoint_mismatch"
        )
    return automatic_completed_window(durable, today=today)


def validate_window(
    window_start: date,
    window_end_exclusive: date,
) -> None:
    if window_end_exclusive <= window_start:
        raise GlobalIngestConfigurationError("invalid_half_open_window")
    if (window_end_exclusive - window_start).days > 31:
        raise GlobalIngestConfigurationError("window_exceeds_31_days")


def execute_global_ingest(
    *,
    country_code: str,
    connector: GlobalSourceConnector,
    issuers: tuple[IssuerReference, ...],
    window_start: date,
    window_end_exclusive: date,
    code_revision: str,
    rights_client: EligibilityClient,
    ingest_client: IngestApiClient,
    page_size: int = 100,
    max_pages: int = 100,
    source_cursor: str | None = None,
    replay_only: bool = False,
) -> GlobalIngestResult:
    country = str(country_code or "").strip().upper()
    if country not in SUPPORTED_COUNTRIES or country == "KR":
        raise GlobalIngestConfigurationError("unsupported_global_ingest_country")
    validate_window(window_start, window_end_exclusive)
    revision = _validate_code_revision(code_revision)
    descriptor = connector.descriptor
    if (
        descriptor.country_code != country
        or descriptor.connector_id == "connector:kr:dart"
        or not descriptor.source_right_id
    ):
        raise GlobalIngestConfigurationError("connector_country_mismatch")
    request = GlobalConnectorRequest(
        window_start=window_start,
        window_end_exclusive=window_end_exclusive,
        issuers=issuers,
        page_size=page_size,
        max_pages=max_pages,
        cursor=source_cursor,
    )

    def eligibility_provider() -> OfficialSourceRightEligibility:
        return rights_client.check(
            str(descriptor.source_right_id),
            use="collect",
        )

    # The first check happens before any official-source request.  The same
    # provider is passed into the connector, whose page loops recheck it before
    # every HTTP page/day.
    initial_eligibility = eligibility_provider()
    envelope = connector.fetch(
        request,
        eligibility=initial_eligibility,
        eligibility_provider=eligibility_provider,
    )
    if (
        envelope.connector_id != descriptor.connector_id
        or envelope.country_code != country
        or envelope.source_right_id != descriptor.source_right_id
    ):
        raise GlobalIngestError("connector_envelope_contract_mismatch")

    chunks = chunk_connector_envelope(
        envelope,
        window_start=window_start,
        window_end_exclusive=window_end_exclusive,
    )
    receipts: list[GlobalIngestReceipt] = []
    idempotency_keys: list[str] = []
    for chunk_index, chunk in enumerate(chunks):
        # Close the source-right race before every chunk. The API locks and
        # verifies the same revision again inside each transaction.
        final_eligibility = eligibility_provider()
        if (
            final_eligibility.source_right_id != descriptor.source_right_id
            or final_eligibility.use != "collect"
            or final_eligibility.source_type != descriptor.source_type
            or final_eligibility.source_key != descriptor.source_key
            or not hmac.compare_digest(
                chunk.rights_revision,
                final_eligibility.rights_revision,
            )
        ):
            raise GlobalIngestError("source_right_changed_before_ingest")

        key = content_idempotency_key(
            envelope=chunk,
            code_revision=revision,
            window_start=window_start,
            window_end_exclusive=window_end_exclusive,
            chunk_index=chunk_index,
        )
        chunk_metadata = global_ingest_chunk(
            envelope=envelope,
            window_start=window_start,
            window_end_exclusive=window_end_exclusive,
            index=chunk_index,
            count=len(chunks),
            code_revision=revision,
        )
        if replay_only:
            receipt = ingest_client.submit(
                envelope=chunk,
                chunk=chunk_metadata,
                idempotency_key=key,
                code_revision=revision,
                replay_only=True,
            )
        else:
            receipt = ingest_client.submit(
                envelope=chunk,
                chunk=chunk_metadata,
                idempotency_key=key,
                code_revision=revision,
            )
        expected_chunk_ack = (
            len(chunk.records) + len(chunk.lifecycle_observations)
        )
        if (
            receipt.api_version != "v2"
            or receipt.connector_id != chunk.connector_id
            or receipt.raw_count != chunk.raw_count
            or receipt.acknowledged_count != expected_chunk_ack
        ):
            raise GlobalIngestApiError("api_acknowledgment_mismatch")
        idempotency_keys.append(key)
        receipts.append(receipt)

    expected_ack = len(envelope.records) + len(envelope.lifecycle_observations)
    acknowledged = sum(receipt.acknowledged_count for receipt in receipts)
    if acknowledged != expected_ack:
        raise GlobalIngestApiError("api_acknowledgment_mismatch")
    if replay_only and not all(receipt.idempotent for receipt in receipts):
        raise GlobalIngestApiError("global_ingest_replay_not_idempotent")
    if len(idempotency_keys) == 1:
        batch_key = idempotency_keys[0]
    else:
        digest = hashlib.sha256(
            "\x1f".join(idempotency_keys).encode("utf-8")
        ).hexdigest()
        batch_key = f"global-ingest-v2:{country.casefold()}:batch:{digest}"
    final_receipt = receipts[-1]
    return GlobalIngestResult(
        country_code=country,
        connector_id=envelope.connector_id,
        source_right_id=envelope.source_right_id,
        window_start=window_start.isoformat(),
        window_end_exclusive=window_end_exclusive.isoformat(),
        idempotency_key=batch_key,
        code_revision=revision,
        request_count=envelope.request_count,
        raw_count=envelope.raw_count,
        record_count=len(envelope.records),
        lifecycle_observation_count=len(envelope.lifecycle_observations),
        acknowledged_count=acknowledged,
        ingest_id=final_receipt.ingest_id,
        idempotent=all(receipt.idempotent for receipt in receipts),
        api_version=final_receipt.api_version,
        chunk_count=len(chunks),
        idempotency_keys=tuple(idempotency_keys),
        ingest_ids=tuple(receipt.ingest_id for receipt in receipts),
        idempotent_chunk_count=sum(
            1 for receipt in receipts if receipt.idempotent
        ),
    )


def execute_global_ingest_with_replay(
    *,
    country_code: str,
    connector: GlobalSourceConnector,
    issuers: tuple[IssuerReference, ...],
    window_start: date,
    window_end_exclusive: date,
    code_revision: str,
    rights_client: EligibilityClient,
    ingest_client: IngestApiClient,
    page_size: int = 100,
    max_pages: int = 100,
    source_cursor: str | None = None,
) -> tuple[GlobalIngestResult, GlobalIngestResult]:
    """Submit one fetched envelope twice and require an exact idempotent replay."""

    replay_connector = _ExactReplayConnector(connector)
    def execute() -> GlobalIngestResult:
        return execute_global_ingest(
            country_code=country_code,
            connector=replay_connector,
            issuers=issuers,
            window_start=window_start,
            window_end_exclusive=window_end_exclusive,
            code_revision=code_revision,
            rights_client=rights_client,
            ingest_client=ingest_client,
            page_size=page_size,
            max_pages=max_pages,
            source_cursor=source_cursor,
        )

    initial = execute()
    replay = execute()
    stable_fields = (
        "country_code",
        "connector_id",
        "source_right_id",
        "window_start",
        "window_end_exclusive",
        "idempotency_key",
        "code_revision",
        "request_count",
        "raw_count",
        "record_count",
        "lifecycle_observation_count",
        "acknowledged_count",
        "ingest_id",
        "api_version",
        "chunk_count",
        "idempotency_keys",
        "ingest_ids",
    )
    if (
        replay_connector.fetch_calls != 2
        or replay_connector.source_fetches != 1
        or any(
            getattr(initial, field) != getattr(replay, field)
            for field in stable_fields
        )
    ):
        raise GlobalIngestApiError("global_ingest_replay_payload_mismatch")
    if (
        not replay.idempotent
        or replay.idempotent_chunk_count != replay.chunk_count
    ):
        raise GlobalIngestApiError("global_ingest_replay_not_idempotent")
    return initial, replay


def replay_verification_evidence(
    initial: GlobalIngestResult,
    replay: GlobalIngestResult,
) -> dict[str, object]:
    """Return credential-free proof for an already validated exact replay."""

    return {
        "attempted": True,
        "same_payload": True,
        "idempotent": replay.idempotent,
        "chunk_count": replay.chunk_count,
        "idempotent_chunk_count": replay.idempotent_chunk_count,
        "idempotency_keys_match": (
            initial.idempotency_keys == replay.idempotency_keys
        ),
        "ingest_ids_match": initial.ingest_ids == replay.ingest_ids,
        "raw_count": replay.raw_count,
        "acknowledged_count": replay.acknowledged_count,
    }


def replay_only_verification_evidence(
    result: GlobalIngestResult,
) -> dict[str, object]:
    """Return proof that the server accepted only pre-existing receipts."""

    return {
        "attempted": True,
        "same_payload": True,
        "idempotent": result.idempotent,
        "read_only": True,
        "chunk_count": result.chunk_count,
        "idempotent_chunk_count": result.idempotent_chunk_count,
        "idempotency_keys_match": True,
        "ingest_ids_match": True,
        "raw_count": result.raw_count,
        "acknowledged_count": result.acknowledged_count,
    }


def write_evidence(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(
            dict(payload),
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _parse_date(value: str, field_name: str) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise GlobalIngestConfigurationError(f"invalid_{field_name}") from exc


def _api_configuration(
    environment: Mapping[str, str],
) -> tuple[str, str]:
    base_url = ""
    for name in (
        "BSIDE_API_BASE_URL",
        "GOVERNANCE_API_BASE_URL",
        "ACTIVIST_API_URL",
    ):
        candidate = str(environment.get(name, "")).strip()
        if candidate:
            base_url = candidate
            break
    token = str(environment.get("BSIDE_OPS_TOKEN", "")).strip()
    if not base_url:
        raise GlobalIngestConfigurationError("missing_v2_api_base_url")
    if not token:
        raise GlobalIngestConfigurationError("missing_ops_token")
    return _validated_v2_base_url(base_url), token


def coverage_unavailable_evidence(
    *,
    execution_mode: GlobalIngestExecutionMode,
    code_revision: str,
    started_at: str,
) -> dict[str, object]:
    """Describe a keyless source boundary without making a source request."""

    if execution_mode.api_active or execution_mode.reason is None:
        raise GlobalIngestConfigurationError(
            "coverage_evidence_requires_inactive_connector"
        )
    keyless_capabilities = (
        ["official_viewer_links"]
        if execution_mode.country_code == "JP"
        else [
            "monthly_company_bulk_snapshot",
            "daily_electronic_accounts_bulk",
            "psc_snapshot",
            "basic_company_uri",
            "public_register_links",
        ]
    )
    return {
        "schema_version": 1,
        "status": "coverage_unavailable",
        "country_code": execution_mode.country_code,
        "connector_mode": execution_mode.mode,
        "coverage_mode": execution_mode.coverage_mode,
        "ingest_mode": execution_mode.ingest_mode,
        "reason": execution_mode.reason,
        "code_revision": code_revision,
        "api_connector_active": False,
        "eligible_for_release": False,
        "metadata_only": True,
        "html_scraping": False,
        "source_urls_requested": 0,
        "record_count": 0,
        "acknowledged_count": 0,
        "keyless_capabilities": keyless_capabilities,
        "started_at": started_at,
        "completed_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
    }


def _failure_evidence(
    *,
    country_code: str,
    window_start: date | None,
    window_end_exclusive: date | None,
    code_revision: str | None,
    started_at: str,
    error: BaseException,
) -> dict[str, object]:
    code = (
        error.code
        if isinstance(error, GlobalIngestError)
        else "global_ingest_failed"
    )
    payload: dict[str, object] = {
        "schema_version": 1,
        "status": "failed",
        "country_code": country_code,
        "window": {
            "start": window_start.isoformat() if window_start else None,
            "end_exclusive": (
                window_end_exclusive.isoformat()
                if window_end_exclusive
                else None
            ),
        },
        "code_revision": code_revision,
        "started_at": started_at,
        "completed_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat(),
        "error": {
            "code": code,
            "class": type(error).__name__,
        },
    }
    if isinstance(error, GlobalIngestError) and error.http_status is not None:
        payload["error"]["http_status"] = error.http_status  # type: ignore[index]
    return payload


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingest one non-Korean official source into the v2 review queue."
    )
    parser.add_argument("--country", required=True, choices=SUPPORTED_COUNTRIES)
    parser.add_argument("--from-date", default="")
    parser.add_argument("--to-date", default="", help="Exclusive YYYY-MM-DD bound")
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument("--code-revision", default="")
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument(
        "--completed-day-only",
        action="store_true",
        help=(
            "Use completed-day source material only. This is required for "
            "deterministic one-day historical replay and requires explicit dates."
        ),
    )
    parser.add_argument(
        "--verify-replay",
        action="store_true",
        help=(
            "Submit the exact in-memory completed-day payload a second time "
            "and require every API chunk to return an idempotent receipt."
        ),
    )
    parser.add_argument(
        "--replay-only",
        action="store_true",
        help=(
            "Require every exact receipt to exist already. The API rejects a "
            "missing or changed receipt before any document, event, receipt, "
            "or connector checkpoint can be written."
        ),
    )
    parser.add_argument("--require-active-pipeline", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _argument_parser().parse_args(argv)
    started_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    start: date | None = None
    end: date | None = None
    revision: str | None = None
    try:
        if args.require_active_pipeline:
            pipeline_mode = str(
                os.environ.get("GOVERNANCE_PIPELINE_MODE", "")
            ).strip()
            if pipeline_mode not in {"shadow", "live"}:
                raise GlobalIngestConfigurationError(
                    "governance_pipeline_not_active"
                )
        revision = _validate_code_revision(
            args.code_revision or os.environ.get("GITHUB_SHA", "")
        )
        if args.completed_day_only and (
            not str(args.from_date).strip()
            or not str(args.to_date).strip()
        ):
            raise GlobalIngestConfigurationError(
                "completed_day_only_requires_explicit_window"
            )
        if args.verify_replay and not args.completed_day_only:
            raise GlobalIngestConfigurationError(
                "verify_replay_requires_completed_day_only"
            )
        if args.replay_only and not args.verify_replay:
            raise GlobalIngestConfigurationError(
                "replay_only_requires_verify_replay"
            )
        execution_mode = global_ingest_execution_mode(
            args.country,
            environment=os.environ,
        )
        if not execution_mode.api_active:
            evidence = coverage_unavailable_evidence(
                execution_mode=execution_mode,
                code_revision=revision,
                started_at=started_at,
            )
            write_evidence(args.evidence, evidence)
            print(
                _canonical_json(
                    {
                        "ok": False,
                        "status": "coverage_unavailable",
                        "country_code": execution_mode.country_code,
                        "coverage_mode": execution_mode.coverage_mode,
                    }
                )
            )
            return 0
        connector, issuers = build_connector(
            args.country,
            environment=os.environ,
            completed_day_only=args.completed_day_only,
        )
        base_url, token = _api_configuration(os.environ)
        rights_client = GlobalOfficialSourceRightClient(
            base_url=base_url,
            token=token,
        )
        ingest_client = V2GlobalIngestClient(
            base_url=base_url,
            token=token,
        )
        checkpoint = ingest_client.fetch_checkpoint(
            connector.descriptor.connector_id
        )
        completed_today = (
            sec_completed_day_limit()
            if args.country == "US"
            else datetime.now(timezone.utc).date()
        )
        start, end = select_completed_window(
            from_date=args.from_date,
            to_date=args.to_date,
            connector_id=connector.descriptor.connector_id,
            checkpoint_client=ingest_client,
            checkpoint=checkpoint,
            today=completed_today,
        )
        replay_result: GlobalIngestResult | None = None
        if args.replay_only:
            result = execute_global_ingest(
                country_code=args.country,
                connector=connector,
                issuers=issuers,
                window_start=start,
                window_end_exclusive=end,
                code_revision=revision,
                rights_client=rights_client,
                ingest_client=ingest_client,
                page_size=args.page_size,
                max_pages=args.max_pages,
                source_cursor=(
                    None
                    if args.completed_day_only
                    else checkpoint.source_cursor
                ),
                replay_only=True,
            )
            replay_result = result
        elif args.verify_replay:
            result, replay_result = execute_global_ingest_with_replay(
                country_code=args.country,
                connector=connector,
                issuers=issuers,
                window_start=start,
                window_end_exclusive=end,
                code_revision=revision,
                rights_client=rights_client,
                ingest_client=ingest_client,
                page_size=args.page_size,
                max_pages=args.max_pages,
                source_cursor=(
                    None
                    if args.completed_day_only
                    else checkpoint.source_cursor
                ),
            )
        else:
            result = execute_global_ingest(
                country_code=args.country,
                connector=connector,
                issuers=issuers,
                window_start=start,
                window_end_exclusive=end,
                code_revision=revision,
                rights_client=rights_client,
                ingest_client=ingest_client,
                page_size=args.page_size,
                max_pages=args.max_pages,
                source_cursor=(
                    None
                    if args.completed_day_only
                    else checkpoint.source_cursor
                ),
            )
        evidence = result.evidence()
        evidence["collection_mode"] = (
            "completed-day"
            if args.completed_day_only
            else "incremental"
        )
        if args.replay_only:
            evidence["replay_verification"] = (
                replay_only_verification_evidence(result)
            )
        elif replay_result is not None:
            evidence["replay_verification"] = replay_verification_evidence(
                result,
                replay_result,
            )
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
                    "connector_id": result.connector_id,
                    "raw_count": result.raw_count,
                    "acknowledged_count": result.acknowledged_count,
                    "chunk_count": result.chunk_count,
                    "idempotent": result.idempotent,
                }
            )
        )
        return 0
    except Exception as error:
        # Artifact and stderr are intentionally limited to controlled codes and
        # exception class names.  URLs, HTTP bodies, source records, API keys,
        # and bearer tokens are never serialized.
        evidence = _failure_evidence(
            country_code=str(args.country),
            window_start=start,
            window_end_exclusive=end,
            code_revision=revision,
            started_at=started_at,
            error=error,
        )
        try:
            write_evidence(args.evidence, evidence)
        except OSError:
            pass
        code = evidence["error"]["code"]  # type: ignore[index]
        print(
            _canonical_json({"ok": False, "error": code}),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
