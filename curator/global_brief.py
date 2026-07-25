"""Human-approved publication path for the global daily brief.

The scheduled command only creates a secret-free candidate bundle.  Publication
is a separate, manually dispatched operation which requires an explicit human
approval document and revalidates every selected event against the v2 API.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence, cast
from urllib.parse import urlencode, urlsplit, urlunsplit
from zoneinfo import ZoneInfo

import httpx

from .global_market import GLOBAL_COUNTRIES, GLOBAL_EVENT_FAMILIES


API_VERSION = "v2"
BUNDLE_SCHEMA_VERSION = 1
BUNDLE_KIND = "bside-global-brief-candidate-bundle"
APPROVAL_KIND = "bside-global-brief-human-approval"
RECEIPT_KIND = "bside-global-brief-publication-receipt"
ACTIVE_PIPELINE_MODES = frozenset({"shadow", "live"})
EDITIONS = frozenset(("global", *GLOBAL_COUNTRIES))
PUBLIC_VERIFICATION_STATUSES = frozenset(
    {"official", "confirmed", "corroborated", "corrected", "withdrawn"}
)
PUBLIC_IMPORTANCE_LEVELS = frozenset(
    {"low", "medium", "high", "critical", "market_sensitive"}
)
SOURCE_STATUSES = frozenset(
    {
        "inactive",
        "configured",
        "active",
        "degraded",
        "pending_rights",
        "error",
        "blocked_rights",
    }
)
PUBLIC_SOURCE_STATUSES = frozenset(
    {
        "inactive",
        "configured",
        "active",
        "degraded",
        "pending_rights",
        "error",
        "blocked_rights",
        "redistribution_blocked",
        "excluded_source",
        "stale",
    }
)
COVERAGE_MODES = frozenset(
    {
        "market-wide",
        "official-register",
        "selected-issuers",
        "link-only",
        "unavailable",
    }
)
LANES = ("top", "watch", "deadline")
LANE_LIMITS = {"top": 5, "watch": 50, "deadline": 50}
EMPTY_REASONS = frozenset(
    {"no_confirmed_material_events", "coverage_unavailable"}
)
TITLE_PROVENANCE_VALUES = frozenset(
    {"source", "generated_metadata", "operator_metadata"}
)
EVENT_ID = re.compile(r"^[A-Za-z0-9_.:\-]{1,96}$")
BUILD_SHA = re.compile(r"^[a-f0-9]{7,64}$")
DIGEST = re.compile(r"^[a-f0-9]{64}$")
LANGUAGE = re.compile(r"^[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*$")
SAFE_SERVER_ERROR = re.compile(r"^[a-z][a-z0-9_]{1,63}$")
MYSQL_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")
KST = ZoneInfo("Asia/Seoul")

PUBLIC_EVENT_FIELDS = (
    "event_id",
    "issuer_id",
    "issuer_name",
    "ticker",
    "market",
    "country",
    "event_family",
    "importance",
    "verification_status",
    "change_type",
    "title",
    "title_provenance",
    "original_language",
    "change_summary",
    "current_status",
    "actor_name",
    "occurred_at",
    "filed_at",
    "first_observed_at",
    "updated_at",
    "deadline_at",
    "official_evidence_count",
    "media_count",
    "coverage_mode",
    "source_url",
)

PUBLIC_SOURCE_STATUS_FIELDS = (
    "connector_id",
    "country",
    "source_name",
    "coverage_mode",
    "status",
    "collect_status",
    "public_status",
    "last_success_at",
    "last_checked_at",
    "last_error_class",
    "public_note",
    "lag_minutes",
    "expected_cadence_minutes",
    "fresh",
    "collect_fresh",
    "public_ready",
    "raw_count",
    "acknowledged_count",
)


class GlobalBriefError(RuntimeError):
    """A safe error suitable for workflow logs."""

    def __init__(self, code: str, *, http_status: int | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.http_status = http_status


class GlobalBriefConfigurationError(GlobalBriefError):
    pass


class GlobalBriefApiError(GlobalBriefError):
    pass


class GlobalBriefValidationError(GlobalBriefError):
    pass


@dataclass(frozen=True)
class BriefPublicationReceipt:
    brief_id: str
    edition: str
    published: bool
    idempotent: bool
    top_count: int
    item_count: int
    empty_reason: str | None
    api_version: str = API_VERSION


def _canonical_json(value: object) -> str:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise GlobalBriefValidationError("non_canonical_json_value") from exc


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _require_exact_keys(
    value: Mapping[str, object],
    expected: set[str],
    *,
    code: str,
) -> None:
    if set(value) != expected:
        raise GlobalBriefValidationError(code)


def _validate_pipeline_mode(value: str) -> str:
    mode = str(value or "").strip().casefold()
    if mode not in ACTIVE_PIPELINE_MODES:
        raise GlobalBriefConfigurationError("global_brief_pipeline_inactive")
    return mode


def _validate_edition(value: str) -> str:
    raw = str(value or "").strip()
    edition = "global" if raw.casefold() == "global" else raw.upper()
    if edition not in EDITIONS:
        raise GlobalBriefValidationError("invalid_brief_edition")
    return edition


def _validate_build_sha(value: str) -> str:
    revision = str(value or "").strip().casefold()
    if BUILD_SHA.fullmatch(revision) is None:
        raise GlobalBriefValidationError("invalid_brief_build_sha")
    return revision


def _parse_timestamp(
    value: object,
    *,
    code: str,
    allow_mysql_utc: bool = True,
) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise GlobalBriefValidationError(code)
    text = value.strip()
    if allow_mysql_utc and MYSQL_TIMESTAMP.fullmatch(text):
        text = text.replace(" ", "T") + "+00:00"
    elif text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise GlobalBriefValidationError(code) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise GlobalBriefValidationError(code)
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def _utc_iso(value: object, *, code: str) -> str:
    return _parse_timestamp(value, code=code).isoformat()


def _mysql_utc(value: object, *, code: str) -> str:
    return _parse_timestamp(value, code=code).strftime("%Y-%m-%d %H:%M:%S")


def _validated_v2_base_url(raw: str) -> str:
    value = str(raw or "").strip()
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError as exc:
        raise GlobalBriefConfigurationError("invalid_v2_api_base_url") from exc
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise GlobalBriefConfigurationError("invalid_v2_api_base_url")
    path = parsed.path.rstrip("/")
    for suffix in ("/api/v1", "/api/v2"):
        if path.endswith(suffix):
            path = path[: -len(suffix)]
            break
    return urlunsplit((parsed.scheme, parsed.netloc, path + "/api/v2", "", ""))


def _validated_token(value: str, *, code: str, required: bool) -> str:
    token = str(value or "").strip()
    if not token and not required:
        return ""
    if len(token.encode("utf-8")) < 32 or "\r" in token or "\n" in token:
        raise GlobalBriefConfigurationError(code)
    return token


def _safe_text(value: object, *, code: str, maximum: int = 4000) -> str:
    if not isinstance(value, str):
        raise GlobalBriefValidationError(code)
    text = value.strip()
    if not text or len(text) > maximum:
        raise GlobalBriefValidationError(code)
    return text


def _optional_public_value(value: object, *, maximum: int = 4000) -> object:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, str) and len(value) <= maximum:
        return value
    raise GlobalBriefValidationError("invalid_public_api_value")


def _nonnegative_int(
    value: Mapping[str, object],
    field: str,
    *,
    code: str,
) -> int:
    candidate = value.get(field)
    if (
        not isinstance(candidate, int)
        or isinstance(candidate, bool)
        or candidate < 0
    ):
        raise GlobalBriefValidationError(code)
    return candidate


def normalize_candidate_event(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise GlobalBriefValidationError("invalid_brief_candidate")
    event_id = value.get("event_id")
    country = value.get("country")
    family = value.get("event_family")
    importance = value.get("importance")
    verification = value.get("verification_status")
    title_provenance = value.get("title_provenance")
    official_count = _nonnegative_int(
        value,
        "official_evidence_count",
        code="invalid_official_evidence_count",
    )
    media_count = _nonnegative_int(
        value,
        "media_count",
        code="invalid_media_count",
    )
    language = value.get("original_language")
    if not isinstance(event_id, str) or EVENT_ID.fullmatch(event_id) is None:
        raise GlobalBriefValidationError("invalid_brief_candidate_event_id")
    if country not in GLOBAL_COUNTRIES:
        raise GlobalBriefValidationError("invalid_brief_candidate_country")
    if family not in GLOBAL_EVENT_FAMILIES:
        raise GlobalBriefValidationError("invalid_brief_candidate_family")
    if importance not in PUBLIC_IMPORTANCE_LEVELS:
        raise GlobalBriefValidationError("invalid_brief_candidate_importance")
    if verification not in PUBLIC_VERIFICATION_STATUSES:
        raise GlobalBriefValidationError("ineligible_brief_candidate_status")
    if title_provenance not in TITLE_PROVENANCE_VALUES:
        raise GlobalBriefValidationError(
            "invalid_brief_candidate_title_provenance"
        )
    if not isinstance(language, str) or LANGUAGE.fullmatch(language) is None:
        raise GlobalBriefValidationError("invalid_candidate_original_language")
    _safe_text(value.get("title"), code="invalid_brief_candidate_title", maximum=1000)
    updated_at = _utc_iso(
        value.get("updated_at"),
        code="invalid_brief_candidate_updated_at",
    )
    sanitized: dict[str, object] = {}
    for field in PUBLIC_EVENT_FIELDS:
        if field in value:
            sanitized[field] = _optional_public_value(value[field])
    sanitized["event_id"] = event_id
    sanitized["country"] = country
    sanitized["event_family"] = family
    sanitized["importance"] = importance
    sanitized["verification_status"] = verification
    sanitized["title_provenance"] = title_provenance
    sanitized["official_evidence_count"] = official_count
    sanitized["media_count"] = media_count
    sanitized["original_language"] = language
    sanitized["updated_at"] = updated_at
    return sanitized


def normalize_source_status(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise GlobalBriefValidationError("invalid_source_status")
    country = value.get("country")
    status = value.get("status")
    collect_status = value.get("collect_status")
    public_status = value.get("public_status")
    coverage_mode = value.get("coverage_mode")
    fresh = value.get("fresh")
    collect_fresh = value.get("collect_fresh")
    public_ready = value.get("public_ready")
    connector_id = value.get("connector_id")
    if country not in GLOBAL_COUNTRIES:
        raise GlobalBriefValidationError("invalid_source_status_country")
    if status not in SOURCE_STATUSES:
        raise GlobalBriefValidationError("invalid_source_status_state")
    if collect_status not in SOURCE_STATUSES or status != collect_status:
        raise GlobalBriefValidationError("invalid_source_collect_status")
    if public_status not in PUBLIC_SOURCE_STATUSES:
        raise GlobalBriefValidationError("invalid_source_public_status")
    if coverage_mode not in COVERAGE_MODES:
        raise GlobalBriefValidationError("invalid_source_coverage_mode")
    if not isinstance(fresh, bool):
        raise GlobalBriefValidationError("invalid_source_freshness")
    if not isinstance(collect_fresh, bool) or fresh != collect_fresh:
        raise GlobalBriefValidationError("invalid_source_collect_freshness")
    if not isinstance(public_ready, bool):
        raise GlobalBriefValidationError("invalid_source_public_readiness")
    if public_ready and (
        public_status != "active" or collect_fresh is not True
    ):
        raise GlobalBriefValidationError("inconsistent_source_public_readiness")
    if connector_id is not None and (
        not isinstance(connector_id, str)
        or EVENT_ID.fullmatch(connector_id) is None
    ):
        raise GlobalBriefValidationError("invalid_source_connector_id")
    sanitized: dict[str, object] = {}
    for field in PUBLIC_SOURCE_STATUS_FIELDS:
        if field in value:
            sanitized[field] = _optional_public_value(value[field])
    sanitized.update(
        {
            "connector_id": connector_id,
            "country": country,
            "status": status,
            "collect_status": collect_status,
            "public_status": public_status,
            "coverage_mode": coverage_mode,
            "fresh": fresh,
            "collect_fresh": collect_fresh,
            "public_ready": public_ready,
        }
    )
    return sanitized


def source_readiness(
    statuses: Sequence[Mapping[str, object]],
    *,
    edition: str,
) -> dict[str, object]:
    normalized_edition = _validate_edition(edition)
    required = (
        list(GLOBAL_COUNTRIES)
        if normalized_edition == "global"
        else [normalized_edition]
    )
    ready_countries = sorted(
        {
            str(item["country"])
            for item in statuses
            if item.get("public_ready") is True
        }
    )
    missing = [country for country in required if country not in ready_countries]
    return {
        "ready": not missing,
        "required_countries": required,
        "ready_countries": ready_countries,
        "unavailable_countries": missing,
    }


def scheduled_cutoff(now: datetime | None = None) -> str:
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    local = current.astimezone(KST)
    cutoff_local = local.replace(hour=5, minute=45, second=0, microsecond=0)
    if cutoff_local > local + timedelta(minutes=5):
        cutoff_local -= timedelta(days=1)
    return cutoff_local.astimezone(timezone.utc).isoformat()


def expected_brief_id(*, edition: str, cutoff_at: object) -> str:
    normalized_edition = _validate_edition(edition)
    mysql_cutoff = _mysql_utc(cutoff_at, code="invalid_brief_cutoff")
    external = f"{normalized_edition}|{mysql_cutoff}"
    return "brief:" + hashlib.sha256(external.encode("utf-8")).hexdigest()


class V2GlobalBriefClient:
    """Strict, redirect-denying client for editorial brief operations."""

    def __init__(
        self,
        *,
        base_url: str,
        editor_token: str,
        preview_token: str = "",
        timeout: float = 30.0,
        transport: httpx.BaseTransport | None = None,
        client_factory: Callable[..., httpx.Client] = httpx.Client,
    ) -> None:
        self.base_url = _validated_v2_base_url(base_url).rstrip("/")
        self.editor_token = _validated_token(
            editor_token,
            code="missing_editor_token",
            required=True,
        )
        self.preview_token = _validated_token(
            preview_token,
            code="invalid_preview_token",
            required=False,
        )
        self.timeout = timeout
        self.transport = transport
        self.client_factory = client_factory

    @staticmethod
    def _object(response: httpx.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise GlobalBriefApiError(
                "malformed_api_response",
                http_status=response.status_code,
            ) from exc
        if not isinstance(payload, dict):
            raise GlobalBriefApiError(
                "malformed_api_response",
                http_status=response.status_code,
            )
        return payload

    @staticmethod
    def _raise_rejection(
        response: httpx.Response,
        payload: Mapping[str, object],
    ) -> None:
        server_error = payload.get("error")
        code = (
            str(server_error)
            if isinstance(server_error, str)
            and SAFE_SERVER_ERROR.fullmatch(server_error)
            else "global_brief_api_rejected"
        )
        raise GlobalBriefApiError(code, http_status=response.status_code)

    def _get(
        self,
        path: str,
        *,
        query: Mapping[str, str],
        token: str,
    ) -> dict[str, Any]:
        suffix = "?" + urlencode(query) if query else ""
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        try:
            with self.client_factory(
                timeout=self.timeout,
                transport=self.transport,
                follow_redirects=False,
            ) as client:
                response = client.get(
                    f"{self.base_url}{path}{suffix}",
                    headers=headers,
                )
        except httpx.HTTPError as exc:
            raise GlobalBriefApiError("global_brief_api_request_failed") from exc
        payload = self._object(response)
        if response.status_code != 200:
            self._raise_rejection(response, payload)
        if payload.get("ok") is not True or payload.get("api_version") != API_VERSION:
            raise GlobalBriefApiError(
                "malformed_api_response",
                http_status=response.status_code,
            )
        return payload

    def fetch_candidates(self, *, edition: str) -> tuple[dict[str, object], ...]:
        normalized_edition = _validate_edition(edition)
        query = {"limit": "100"}
        if normalized_edition != "global":
            query["country"] = normalized_edition
        payload = self._get(
            "/admin/brief-candidates",
            query=query,
            token=self.editor_token,
        )
        data = payload.get("data")
        meta = payload.get("meta")
        if (
            not isinstance(data, dict)
            or not isinstance(data.get("items"), list)
            or not isinstance(meta, dict)
            or not isinstance(meta.get("returned"), int)
            or isinstance(meta.get("returned"), bool)
            or meta.get("returned") != len(data["items"])
            or len(data["items"]) > 100
        ):
            raise GlobalBriefApiError("malformed_candidate_response")
        candidates = tuple(normalize_candidate_event(item) for item in data["items"])
        if normalized_edition != "global" and any(
            item["country"] != normalized_edition for item in candidates
        ):
            raise GlobalBriefApiError("candidate_country_mismatch")
        ids = [str(item["event_id"]) for item in candidates]
        if len(ids) != len(set(ids)):
            raise GlobalBriefApiError("duplicate_candidate_event")
        return candidates

    def fetch_source_status(
        self,
        *,
        edition: str,
    ) -> tuple[str, tuple[dict[str, object], ...]]:
        normalized_edition = _validate_edition(edition)
        query: dict[str, str] = {}
        if normalized_edition != "global":
            query["country"] = normalized_edition
        payload = self._get(
            "/sources/status",
            query=query,
            token=self.preview_token,
        )
        data = payload.get("data")
        meta = payload.get("meta")
        if (
            not isinstance(data, dict)
            or not isinstance(data.get("items"), list)
            or not isinstance(meta, dict)
            or not isinstance(meta.get("returned"), int)
            or isinstance(meta.get("returned"), bool)
            or meta.get("returned") != len(data["items"])
        ):
            raise GlobalBriefApiError("malformed_source_status_response")
        checked_at = _utc_iso(
            data.get("checked_at"),
            code="invalid_source_status_checked_at",
        )
        statuses = tuple(normalize_source_status(item) for item in data["items"])
        if normalized_edition != "global" and any(
            item["country"] != normalized_edition for item in statuses
        ):
            raise GlobalBriefApiError("source_status_country_mismatch")
        return checked_at, statuses

    def publish(self, publication: Mapping[str, object]) -> BriefPublicationReceipt:
        edition = _validate_edition(str(publication.get("edition") or ""))
        items = publication.get("items")
        if not isinstance(items, list):
            raise GlobalBriefValidationError("invalid_brief_items")
        top_count = sum(
            1
            for item in items
            if isinstance(item, dict) and item.get("lane") == "top"
        )
        expected_id = expected_brief_id(
            edition=edition,
            cutoff_at=publication.get("cutoff_at"),
        )
        try:
            with self.client_factory(
                timeout=self.timeout,
                transport=self.transport,
                follow_redirects=False,
            ) as client:
                response = client.post(
                    f"{self.base_url}/admin/briefs",
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {self.editor_token}",
                    },
                    content=_canonical_json(publication).encode("utf-8"),
                )
        except httpx.HTTPError as exc:
            raise GlobalBriefApiError("global_brief_api_request_failed") from exc
        payload = self._object(response)
        if response.status_code != 200:
            self._raise_rejection(response, payload)
        data = payload.get("data")
        if (
            payload.get("ok") is not True
            or payload.get("api_version") != API_VERSION
            or not isinstance(data, dict)
            or data.get("brief_id") != expected_id
            or data.get("edition") != edition
            or data.get("published") is not True
            or not isinstance(data.get("idempotent"), bool)
        ):
            raise GlobalBriefApiError("brief_publication_acknowledgment_mismatch")
        idempotent = bool(data["idempotent"])
        if not idempotent and (
            data.get("top_count") != top_count
            or data.get("item_count") != len(items)
            or data.get("empty_reason") != publication.get("empty_reason")
        ):
            raise GlobalBriefApiError("brief_publication_acknowledgment_mismatch")
        return BriefPublicationReceipt(
            brief_id=expected_id,
            edition=edition,
            published=True,
            idempotent=idempotent,
            top_count=top_count,
            item_count=len(items),
            empty_reason=(
                str(publication["empty_reason"])
                if publication.get("empty_reason") is not None
                else None
            ),
        )


def _candidate_payload(
    candidates: Sequence[Mapping[str, object]],
    *,
    edition: str,
    cutoff_at: str,
    build_sha: str,
    ready: bool,
) -> dict[str, object]:
    top = [
        item
        for item in candidates
        if _nonnegative_int(
            item,
            "official_evidence_count",
            code="invalid_official_evidence_count",
        )
        > 0
    ][: LANE_LIMITS["top"]]
    items = [
        {
            "event_id": str(item["event_id"]),
            "lane": "top",
            "position_no": position,
            "selection_reason": "Pending explicit human Top selection approval",
        }
        for position, item in enumerate(top, start=1)
    ]
    return {
        "edition": edition,
        "cutoff_at": cutoff_at,
        "build_sha": build_sha,
        "empty_reason": (
            None
            if items
            else (
                "no_confirmed_material_events"
                if ready
                else "coverage_unavailable"
            )
        ),
        "items": items,
    }


def build_candidate_bundle(
    *,
    edition: str,
    build_sha: str,
    candidates: Sequence[Mapping[str, object]],
    source_checked_at: str,
    source_statuses: Sequence[Mapping[str, object]],
    now: datetime | None = None,
    cutoff_at: str | None = None,
) -> dict[str, object]:
    normalized_edition = _validate_edition(edition)
    normalized_sha = _validate_build_sha(build_sha)
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(
        microsecond=0
    )
    normalized_candidates = tuple(normalize_candidate_event(item) for item in candidates)
    normalized_statuses = tuple(
        normalize_source_status(item) for item in source_statuses
    )
    checked_at = _utc_iso(
        source_checked_at,
        code="invalid_source_status_checked_at",
    )
    if _parse_timestamp(
        checked_at,
        code="invalid_source_status_checked_at",
    ) > current + timedelta(minutes=5):
        raise GlobalBriefValidationError("source_status_checked_in_future")
    cutoff = _utc_iso(
        cutoff_at or scheduled_cutoff(current),
        code="invalid_brief_cutoff",
    )
    if _parse_timestamp(cutoff, code="invalid_brief_cutoff") > current + timedelta(
        minutes=5
    ):
        raise GlobalBriefValidationError("brief_cutoff_in_future")
    readiness = source_readiness(
        normalized_statuses,
        edition=normalized_edition,
    )
    publication = _candidate_payload(
        normalized_candidates,
        edition=normalized_edition,
        cutoff_at=cutoff,
        build_sha=normalized_sha,
        ready=bool(readiness["ready"]),
    )
    publication_items = cast(list[dict[str, object]], publication["items"])
    selected_ids = {
        str(item["event_id"])
        for item in publication_items
    }
    versions = {
        str(item["event_id"]): str(item["updated_at"])
        for item in normalized_candidates
        if str(item["event_id"]) in selected_ids
    }
    basis = {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "edition": normalized_edition,
        "cutoff_at": cutoff,
        "build_sha": normalized_sha,
        "generated_at": current.isoformat(),
        "source_snapshot": {
            "checked_at": checked_at,
            "readiness": readiness,
            "items": list(normalized_statuses),
        },
        "candidates": list(normalized_candidates),
        "suggested_publication": publication,
    }
    basis_hash = _sha256(basis)
    return {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "kind": BUNDLE_KIND,
        "contains_secrets": False,
        "auto_publish": False,
        "human_approval_required": True,
        "candidate_bundle_sha256": basis_hash,
        "basis": basis,
        "approval_template": {
            "schema_version": BUNDLE_SCHEMA_VERSION,
            "kind": APPROVAL_KIND,
            "candidate_bundle_sha256": basis_hash,
            "approval": {
                "status": "pending",
                "approved_by": "",
                "approved_at": None,
            },
            "publication": publication,
            "selected_event_versions": versions,
        },
    }


def validate_publication(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        raise GlobalBriefValidationError("invalid_publication_payload")
    _require_exact_keys(
        value,
        {"edition", "cutoff_at", "build_sha", "empty_reason", "items"},
        code="invalid_publication_fields",
    )
    edition = _validate_edition(str(value.get("edition") or ""))
    cutoff = _utc_iso(value.get("cutoff_at"), code="invalid_brief_cutoff")
    build_sha = _validate_build_sha(str(value.get("build_sha") or ""))
    empty_reason = value.get("empty_reason")
    if empty_reason is not None and empty_reason not in EMPTY_REASONS:
        raise GlobalBriefValidationError("invalid_brief_empty_reason")
    raw_items = value.get("items")
    if not isinstance(raw_items, list) or len(raw_items) > 105:
        raise GlobalBriefValidationError("invalid_brief_items")
    items: list[dict[str, object]] = []
    lane_counts = {lane: 0 for lane in LANES}
    positions: set[tuple[str, int]] = set()
    event_ids: set[str] = set()
    for raw_item in raw_items:
        if not isinstance(raw_item, dict):
            raise GlobalBriefValidationError("invalid_brief_item")
        _require_exact_keys(
            raw_item,
            {"event_id", "lane", "position_no", "selection_reason"},
            code="invalid_brief_item_fields",
        )
        event_id = raw_item.get("event_id")
        lane = raw_item.get("lane")
        position = raw_item.get("position_no")
        reason = raw_item.get("selection_reason")
        if not isinstance(event_id, str) or EVENT_ID.fullmatch(event_id) is None:
            raise GlobalBriefValidationError("invalid_brief_item_event_id")
        if lane not in LANES:
            raise GlobalBriefValidationError("invalid_brief_item_lane")
        if (
            not isinstance(position, int)
            or isinstance(position, bool)
            or not 1 <= position <= 100
        ):
            raise GlobalBriefValidationError("invalid_brief_item_position")
        reason_text = _safe_text(
            reason,
            code="invalid_brief_selection_reason",
            maximum=500,
        )
        if (lane, position) in positions or event_id in event_ids:
            raise GlobalBriefValidationError("duplicate_brief_item")
        positions.add((lane, position))
        event_ids.add(event_id)
        lane_counts[str(lane)] += 1
        items.append(
            {
                "event_id": event_id,
                "lane": lane,
                "position_no": position,
                "selection_reason": reason_text,
            }
        )
    if any(lane_counts[lane] > LANE_LIMITS[lane] for lane in LANES):
        raise GlobalBriefValidationError("brief_lane_limit_exceeded")
    if (lane_counts["top"] == 0) != (empty_reason is not None):
        raise GlobalBriefValidationError("brief_empty_reason_mismatch")
    lane_order = {lane: index for index, lane in enumerate(LANES)}
    items.sort(key=lambda item: (lane_order[str(item["lane"])], item["position_no"]))
    return {
        "edition": edition,
        "cutoff_at": cutoff,
        "build_sha": build_sha,
        "empty_reason": empty_reason,
        "items": items,
    }


def validate_candidate_bundle(
    value: object,
    *,
    expected_revision: str,
) -> dict[str, object]:
    """Validate the immutable candidate artifact used for human approval."""

    if not isinstance(value, dict):
        raise GlobalBriefValidationError("invalid_candidate_bundle")
    _require_exact_keys(
        value,
        {
            "schema_version",
            "kind",
            "contains_secrets",
            "auto_publish",
            "human_approval_required",
            "candidate_bundle_sha256",
            "basis",
            "approval_template",
        },
        code="invalid_candidate_bundle_fields",
    )
    if value.get("schema_version") != BUNDLE_SCHEMA_VERSION:
        raise GlobalBriefValidationError("invalid_candidate_bundle_schema")
    if value.get("kind") != BUNDLE_KIND:
        raise GlobalBriefValidationError("invalid_candidate_bundle_kind")
    if (
        value.get("contains_secrets") is not False
        or value.get("auto_publish") is not False
        or value.get("human_approval_required") is not True
    ):
        raise GlobalBriefValidationError("unsafe_candidate_bundle_flags")
    basis = value.get("basis")
    if not isinstance(basis, dict):
        raise GlobalBriefValidationError("invalid_candidate_bundle_basis")
    _require_exact_keys(
        basis,
        {
            "schema_version",
            "edition",
            "cutoff_at",
            "build_sha",
            "generated_at",
            "source_snapshot",
            "candidates",
            "suggested_publication",
        },
        code="invalid_candidate_bundle_basis_fields",
    )
    if basis.get("schema_version") != BUNDLE_SCHEMA_VERSION:
        raise GlobalBriefValidationError("invalid_candidate_bundle_basis_schema")
    revision = _validate_build_sha(expected_revision)
    build_sha = _validate_build_sha(str(basis.get("build_sha") or ""))
    if build_sha != revision:
        raise GlobalBriefValidationError("candidate_bundle_revision_mismatch")
    edition = _validate_edition(str(basis.get("edition") or ""))
    cutoff_at = _utc_iso(
        basis.get("cutoff_at"),
        code="invalid_candidate_bundle_cutoff",
    )
    generated_at = _utc_iso(
        basis.get("generated_at"),
        code="invalid_candidate_bundle_generated_at",
    )
    source_snapshot = basis.get("source_snapshot")
    if not isinstance(source_snapshot, dict):
        raise GlobalBriefValidationError("invalid_candidate_source_snapshot")
    _require_exact_keys(
        source_snapshot,
        {"checked_at", "readiness", "items"},
        code="invalid_candidate_source_snapshot_fields",
    )
    checked_at = _utc_iso(
        source_snapshot.get("checked_at"),
        code="invalid_candidate_source_checked_at",
    )
    raw_statuses = source_snapshot.get("items")
    if not isinstance(raw_statuses, list):
        raise GlobalBriefValidationError("invalid_candidate_source_items")
    statuses = [normalize_source_status(item) for item in raw_statuses]
    readiness = source_readiness(statuses, edition=edition)
    if source_snapshot.get("readiness") != readiness:
        raise GlobalBriefValidationError("candidate_source_readiness_mismatch")
    raw_candidates = basis.get("candidates")
    if not isinstance(raw_candidates, list):
        raise GlobalBriefValidationError("invalid_candidate_bundle_candidates")
    candidates = [normalize_candidate_event(item) for item in raw_candidates]
    candidate_ids = [str(item["event_id"]) for item in candidates]
    if len(candidate_ids) != len(set(candidate_ids)):
        raise GlobalBriefValidationError("duplicate_candidate_bundle_event")
    suggested = validate_publication(basis.get("suggested_publication"))
    if (
        suggested["edition"] != edition
        or suggested["cutoff_at"] != cutoff_at
        or suggested["build_sha"] != build_sha
    ):
        raise GlobalBriefValidationError("candidate_suggested_publication_mismatch")
    template = value.get("approval_template")
    if not isinstance(template, dict):
        raise GlobalBriefValidationError("invalid_candidate_approval_template")
    _require_exact_keys(
        template,
        {
            "schema_version",
            "kind",
            "candidate_bundle_sha256",
            "approval",
            "publication",
            "selected_event_versions",
        },
        code="invalid_candidate_approval_template_fields",
    )
    bundle_hash = value.get("candidate_bundle_sha256")
    if not isinstance(bundle_hash, str) or DIGEST.fullmatch(bundle_hash) is None:
        raise GlobalBriefValidationError("invalid_candidate_bundle_hash")
    if bundle_hash != _sha256(basis):
        raise GlobalBriefValidationError("candidate_bundle_hash_mismatch")
    if (
        template.get("schema_version") != BUNDLE_SCHEMA_VERSION
        or template.get("kind") != APPROVAL_KIND
        or template.get("candidate_bundle_sha256") != bundle_hash
        or validate_publication(template.get("publication")) != suggested
    ):
        raise GlobalBriefValidationError("candidate_approval_template_mismatch")
    approval_record = template.get("approval")
    if approval_record != {
        "status": "pending",
        "approved_by": "",
        "approved_at": None,
    }:
        raise GlobalBriefValidationError("candidate_approval_template_not_pending")
    raw_versions = template.get("selected_event_versions")
    if not isinstance(raw_versions, dict):
        raise GlobalBriefValidationError("invalid_candidate_template_versions")
    candidate_map = {
        str(candidate["event_id"]): candidate for candidate in candidates
    }
    suggested_items = cast(list[dict[str, object]], suggested["items"])
    suggested_ids = {str(item["event_id"]) for item in suggested_items}
    if set(raw_versions) != suggested_ids:
        raise GlobalBriefValidationError("candidate_template_versions_mismatch")
    for event_id, event_version in raw_versions.items():
        candidate = candidate_map.get(str(event_id))
        if (
            candidate is None
            or _utc_iso(
                event_version,
                code="invalid_candidate_template_version",
            )
            != candidate["updated_at"]
        ):
            raise GlobalBriefValidationError("candidate_template_versions_mismatch")
    return {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "kind": BUNDLE_KIND,
        "candidate_bundle_sha256": bundle_hash,
        "basis": {
            "schema_version": BUNDLE_SCHEMA_VERSION,
            "edition": edition,
            "cutoff_at": cutoff_at,
            "build_sha": build_sha,
            "generated_at": generated_at,
            "source_snapshot": {
                "checked_at": checked_at,
                "readiness": readiness,
                "items": statuses,
            },
            "candidates": candidates,
            "suggested_publication": suggested,
        },
    }


def validate_human_approval(
    value: object,
    *,
    expected_revision: str,
    now: datetime | None = None,
) -> dict[str, object]:
    if not isinstance(value, dict):
        raise GlobalBriefValidationError("invalid_human_approval")
    _require_exact_keys(
        value,
        {
            "schema_version",
            "kind",
            "candidate_bundle_sha256",
            "approval",
            "publication",
            "selected_event_versions",
        },
        code="invalid_human_approval_fields",
    )
    if value.get("schema_version") != BUNDLE_SCHEMA_VERSION:
        raise GlobalBriefValidationError("invalid_human_approval_schema")
    if value.get("kind") != APPROVAL_KIND:
        raise GlobalBriefValidationError("invalid_human_approval_kind")
    basis_hash = value.get("candidate_bundle_sha256")
    if not isinstance(basis_hash, str) or DIGEST.fullmatch(basis_hash) is None:
        raise GlobalBriefValidationError("invalid_candidate_bundle_hash")
    approval = value.get("approval")
    if not isinstance(approval, dict):
        raise GlobalBriefValidationError("invalid_human_approval_record")
    _require_exact_keys(
        approval,
        {"status", "approved_by", "approved_at"},
        code="invalid_human_approval_record_fields",
    )
    if approval.get("status") != "approved":
        raise GlobalBriefValidationError("human_approval_required")
    approved_by = _safe_text(
        approval.get("approved_by"),
        code="invalid_human_approver",
        maximum=120,
    )
    approved_at = _utc_iso(
        approval.get("approved_at"),
        code="invalid_human_approval_timestamp",
    )
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(
        microsecond=0
    )
    approved_time = _parse_timestamp(
        approved_at,
        code="invalid_human_approval_timestamp",
    )
    if approved_time > current + timedelta(minutes=5):
        raise GlobalBriefValidationError("human_approval_in_future")
    if approved_time < current - timedelta(hours=36):
        raise GlobalBriefValidationError("human_approval_expired")
    publication = validate_publication(value.get("publication"))
    revision = _validate_build_sha(expected_revision)
    if publication["build_sha"] != revision:
        raise GlobalBriefValidationError("approved_revision_mismatch")
    raw_versions = value.get("selected_event_versions")
    if not isinstance(raw_versions, dict):
        raise GlobalBriefValidationError("invalid_selected_event_versions")
    publication_items = cast(list[dict[str, object]], publication["items"])
    selected_ids = {
        str(item["event_id"]) for item in publication_items
    }
    if set(raw_versions) != selected_ids:
        raise GlobalBriefValidationError("selected_event_versions_mismatch")
    versions: dict[str, str] = {}
    for event_id, event_updated_at in raw_versions.items():
        if EVENT_ID.fullmatch(str(event_id)) is None:
            raise GlobalBriefValidationError("invalid_selected_event_version_id")
        versions[str(event_id)] = _utc_iso(
            event_updated_at,
            code="invalid_selected_event_version_timestamp",
        )
    return {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "kind": APPROVAL_KIND,
        "candidate_bundle_sha256": basis_hash,
        "approval": {
            "status": "approved",
            "approved_by": approved_by,
            "approved_at": approved_at,
        },
        "publication": publication,
        "selected_event_versions": versions,
    }


def generate_candidate_bundle(
    *,
    client: V2GlobalBriefClient,
    edition: str,
    build_sha: str,
    now: datetime | None = None,
    cutoff_at: str | None = None,
) -> dict[str, object]:
    candidates = client.fetch_candidates(edition=edition)
    checked_at, statuses = client.fetch_source_status(edition=edition)
    return build_candidate_bundle(
        edition=edition,
        build_sha=build_sha,
        candidates=candidates,
        source_checked_at=checked_at,
        source_statuses=statuses,
        now=now,
        cutoff_at=cutoff_at,
    )


def publish_human_approval(
    *,
    client: V2GlobalBriefClient,
    candidate_bundle: object,
    approval: object,
    expected_revision: str,
    now: datetime | None = None,
) -> dict[str, object]:
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc).replace(
        microsecond=0
    )
    validated_bundle = validate_candidate_bundle(
        candidate_bundle,
        expected_revision=expected_revision,
    )
    validated = validate_human_approval(
        approval,
        expected_revision=expected_revision,
        now=current,
    )
    if (
        validated["candidate_bundle_sha256"]
        != validated_bundle["candidate_bundle_sha256"]
    ):
        raise GlobalBriefValidationError("approval_candidate_bundle_mismatch")
    publication = validated["publication"]
    assert isinstance(publication, dict)
    edition = str(publication["edition"])
    basis = validated_bundle["basis"]
    assert isinstance(basis, dict)
    if (
        edition != basis["edition"]
        or publication["cutoff_at"] != basis["cutoff_at"]
        or publication["build_sha"] != basis["build_sha"]
    ):
        raise GlobalBriefValidationError("approval_candidate_contract_mismatch")
    bundled_candidates = basis["candidates"]
    assert isinstance(bundled_candidates, list)
    bundled_candidate_map = {
        str(item["event_id"]): item
        for item in bundled_candidates
        if isinstance(item, dict)
    }
    versions = validated["selected_event_versions"]
    assert isinstance(versions, dict)
    publication_items = cast(list[dict[str, object]], publication["items"])
    for item in publication_items:
        event_id = str(item["event_id"])
        bundled = bundled_candidate_map.get(event_id)
        if bundled is None:
            raise GlobalBriefValidationError("approved_event_not_in_candidate_bundle")
        if bundled["updated_at"] != versions[event_id]:
            raise GlobalBriefValidationError("approved_version_not_in_candidate_bundle")
    fresh_candidates = client.fetch_candidates(edition=edition)
    candidate_map = {
        str(item["event_id"]): item for item in fresh_candidates
    }
    for item in publication_items:
        event_id = str(item["event_id"])
        candidate = candidate_map.get(event_id)
        if candidate is None:
            raise GlobalBriefValidationError("approved_event_no_longer_eligible")
        if candidate["updated_at"] != versions[event_id]:
            raise GlobalBriefValidationError("approved_event_version_changed")
        if (
            item["lane"] == "top"
            and _nonnegative_int(
                candidate,
                "official_evidence_count",
                code="invalid_official_evidence_count",
            )
            < 1
        ):
            raise GlobalBriefValidationError("approved_top_lacks_official_evidence")
    checked_at, statuses = client.fetch_source_status(edition=edition)
    readiness = source_readiness(statuses, edition=edition)
    if not publication_items:
        empty_reason = publication["empty_reason"]
        if (
            empty_reason == "no_confirmed_material_events"
            and not readiness["ready"]
        ):
            raise GlobalBriefValidationError("brief_sources_unavailable")
        if empty_reason == "coverage_unavailable" and readiness["ready"]:
            raise GlobalBriefValidationError("brief_coverage_is_available")
    receipt = client.publish(publication)
    return {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "kind": RECEIPT_KIND,
        "contains_secrets": False,
        "published_at": current.isoformat(),
        "candidate_bundle_sha256": validated["candidate_bundle_sha256"],
        "approval": validated["approval"],
        "source_snapshot": {
            "checked_at": checked_at,
            "readiness": readiness,
        },
        "publication": {
            "brief_id": receipt.brief_id,
            "edition": receipt.edition,
            "published": receipt.published,
            "idempotent": receipt.idempotent,
            "top_count": receipt.top_count,
            "item_count": receipt.item_count,
            "empty_reason": receipt.empty_reason,
            "api_version": receipt.api_version,
        },
    }


def write_json(path: str | Path, payload: Mapping[str, object]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    temporary.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
            allow_nan=False,
        )
        + "\n",
        encoding="utf-8",
    )
    temporary.replace(destination)


def _approval_from_sources(
    *,
    file_path: str,
    json_env_name: str,
    base64_env_name: str,
) -> object:
    sources: list[bytes] = []
    if file_path:
        try:
            sources.append(Path(file_path).read_bytes())
        except OSError as exc:
            raise GlobalBriefConfigurationError("approval_file_unreadable") from exc
    raw_json = os.environ.get(json_env_name, "")
    if raw_json.strip():
        sources.append(raw_json.encode("utf-8"))
    encoded = os.environ.get(base64_env_name, "")
    if encoded.strip():
        try:
            sources.append(base64.b64decode(encoded, validate=True))
        except (binascii.Error, ValueError) as exc:
            raise GlobalBriefConfigurationError("invalid_approval_base64") from exc
    if len(sources) != 1:
        raise GlobalBriefConfigurationError("exactly_one_approval_payload_required")
    if len(sources[0]) > 500_000:
        raise GlobalBriefConfigurationError("approval_payload_too_large")
    try:
        payload = json.loads(sources[0].decode("utf-8-sig"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise GlobalBriefConfigurationError("invalid_approval_json") from exc
    return payload


def _json_artifact_file(
    file_path: str,
    *,
    unreadable_code: str,
    invalid_code: str,
    maximum_bytes: int = 2_000_000,
) -> object:
    if not file_path:
        raise GlobalBriefConfigurationError(unreadable_code)
    try:
        raw = Path(file_path).read_bytes()
    except OSError as exc:
        raise GlobalBriefConfigurationError(unreadable_code) from exc
    if len(raw) > maximum_bytes:
        raise GlobalBriefConfigurationError(invalid_code)
    try:
        return json.loads(raw.decode("utf-8-sig"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise GlobalBriefConfigurationError(invalid_code) from exc


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate or publish a human-approved BSIDE global brief"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    def common(command: argparse.ArgumentParser) -> None:
        command.add_argument(
            "--api-base-url",
            default=os.environ.get("BSIDE_API_BASE_URL", ""),
        )
        command.add_argument(
            "--editor-token",
            default=os.environ.get("BSIDE_EDITOR_TOKEN", ""),
        )
        command.add_argument(
            "--preview-token",
            default=os.environ.get("GOVERNANCE_PREVIEW_TOKEN", ""),
        )
        command.add_argument(
            "--pipeline-mode",
            default=os.environ.get("GOVERNANCE_PIPELINE_MODE", ""),
        )
        command.add_argument("--edition", default="global")
        command.add_argument("--code-revision", required=True)

    candidates = subparsers.add_parser(
        "candidates",
        help="Create a secret-free bundle; never publishes",
    )
    common(candidates)
    candidates.add_argument("--cutoff-at", default="")
    candidates.add_argument("--output", required=True)

    publish = subparsers.add_parser(
        "publish",
        help="Publish only an explicitly human-approved JSON payload",
    )
    common(publish)
    publish.add_argument("--candidate-bundle-file", required=True)
    publish.add_argument("--approval-file", default="")
    publish.add_argument(
        "--approval-json-env",
        default="GLOBAL_BRIEF_APPROVAL_JSON",
    )
    publish.add_argument(
        "--approval-base64-env",
        default="GLOBAL_BRIEF_APPROVAL_B64",
    )
    publish.add_argument("--output", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        _validate_pipeline_mode(args.pipeline_mode)
        edition = _validate_edition(args.edition)
        revision = _validate_build_sha(args.code_revision)
        client = V2GlobalBriefClient(
            base_url=args.api_base_url,
            editor_token=args.editor_token,
            preview_token=args.preview_token,
        )
        if args.command == "candidates":
            bundle = generate_candidate_bundle(
                client=client,
                edition=edition,
                build_sha=revision,
                cutoff_at=args.cutoff_at or None,
            )
            write_json(args.output, bundle)
            return 0
        approval = _approval_from_sources(
            file_path=args.approval_file,
            json_env_name=args.approval_json_env,
            base64_env_name=args.approval_base64_env,
        )
        candidate_bundle = _json_artifact_file(
            args.candidate_bundle_file,
            unreadable_code="candidate_bundle_file_unreadable",
            invalid_code="invalid_candidate_bundle_json",
        )
        receipt = publish_human_approval(
            client=client,
            candidate_bundle=candidate_bundle,
            approval=approval,
            expected_revision=revision,
        )
        write_json(args.output, receipt)
        return 0
    except GlobalBriefError as exc:
        print(exc.code, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
