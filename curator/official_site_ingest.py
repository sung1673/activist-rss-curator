"""Fail-closed company and activist official-site adapter ingestion.

The public sites are selected from actual governance events by the private API.
Operators register one credential-free HTTPS JSON adapter and one valid
``SourceRight`` for every selected entity. Collection remains independently
auditable, while apply uses one HMAC-authenticated transaction per connector.
Every accepted record stays draft/pending and source deletes become review-only
tombstones; this module never deletes public or source content automatically.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import ipaddress
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Sequence
from urllib.parse import parse_qsl, urlsplit

import httpx

from .event_identity import EventIdentity, EventIdentityStatus, build_event_identity
from .governance import GovernanceEventType, stable_id
from .remote_api import post_remote_action, remote_api_configured


ADAPTER_SCHEMA_VERSION = 1
ARTIFACT_SCHEMA_VERSION = 1
APPLY_SCHEMA_VERSION = 1
MAX_ADAPTER_PAGES = 1000
MAX_ADAPTER_RESPONSE_BYTES = 5_000_000
MAX_ITEM_BODY_BYTES = 1_000_000
# Stay below the PHP/API request ceiling after headers and transport framing.
MAX_ATOMIC_APPLY_BYTES = 1_750_000
RIGHTS_PAGE_SIZE = 25
SOURCE_CLASS_BY_ENTITY = {
    "company": "company_statement",
    "actor": "activist_statement",
}
EVENT_TYPES = {value.value for value in GovernanceEventType}
ID_PATTERN = re.compile(r"^[A-Za-z0-9_.:\-]{1,64}$")
EXTERNAL_ID_PATTERN = re.compile(r"^[A-Za-z0-9_.:/\-]{1,191}$")
LANGUAGE_PATTERN = re.compile(r"^[a-z]{2,3}(?:-[A-Z]{2})?$")
HOST_PATTERN = re.compile(
    r"^(?=.{1,253}\.?$)(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z0-9]"
    r"(?:[a-z0-9-]{0,61}[a-z0-9])?\.?$"
)
SENSITIVE_QUERY_KEYS = {
    "access_token",
    "api_key",
    "auth",
    "authorization",
    "credential",
    "key",
    "password",
    "passwd",
    "session",
    "sessionid",
    "sig",
    "signature",
    "token",
}
PRIVATE_HOST_SUFFIXES = (".home", ".internal", ".lan", ".local", ".localhost")


class OfficialSiteIngestError(ValueError):
    """A source, permission, pagination, or normalization contract failed."""


@dataclass(frozen=True)
class Candidate:
    entity_type: str
    entity_id: str
    display_name: str

    @property
    def key(self) -> tuple[str, str]:
        return (self.entity_type, self.entity_id)


@dataclass(frozen=True)
class Connector:
    connector_id: str
    entity_type: str
    entity_id: str
    source_class: str
    source_right_id: str
    endpoint: str
    endpoint_host: str
    allowed_hosts: tuple[str, ...]
    page_size: int

    @property
    def candidate_key(self) -> tuple[str, str]:
        return (self.entity_type, self.entity_id)


@dataclass(frozen=True)
class AdapterResult:
    items: tuple[dict[str, object], ...]
    pages_fetched: int
    total_count: int
    payload_sha256: str


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(value: object) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _require_exact_keys(
    value: Mapping[str, object],
    *,
    required: set[str],
    optional: set[str] | None = None,
    location: str,
) -> None:
    optional = optional or set()
    keys = set(value)
    missing = sorted(required - keys)
    unknown = sorted(keys - required - optional)
    if missing:
        raise OfficialSiteIngestError(f"{location} is missing fields: {', '.join(missing)}")
    if unknown:
        raise OfficialSiteIngestError(f"{location} has unknown fields: {', '.join(unknown)}")


def _as_int(value: object, *, location: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise OfficialSiteIngestError(f"{location} must be an integer >= {minimum}")
    return value


def _as_bool(value: object, *, location: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) and value in {0, 1}:
        return bool(value)
    if isinstance(value, str) and value.strip().casefold() in {"0", "1", "false", "true"}:
        return value.strip().casefold() in {"1", "true"}
    raise OfficialSiteIngestError(f"{location} must be a boolean")


def _parse_datetime(value: object, *, location: str, require_offset: bool) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise OfficialSiteIngestError(f"{location} must be a timestamp")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError as exc:
        raise OfficialSiteIngestError(f"{location} is not an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        if require_offset:
            raise OfficialSiteIngestError(f"{location} must include an explicit UTC offset")
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0)


def _validate_host(host: str, *, location: str) -> str:
    normalized = host.strip().casefold().rstrip(".")
    if (
        not normalized
        or normalized in {"localhost", "localhost.localdomain"}
        or normalized.endswith(PRIVATE_HOST_SUFFIXES)
    ):
        raise OfficialSiteIngestError(f"{location} must be a public hostname")
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError:
        if not HOST_PATTERN.fullmatch(normalized):
            raise OfficialSiteIngestError(f"{location} must be a public ASCII hostname")
    else:
        if not address.is_global:
            raise OfficialSiteIngestError(f"{location} must not be a private or reserved address")
    return normalized


def validate_https_url(
    value: object,
    *,
    location: str,
    allow_query: bool,
) -> tuple[str, str]:
    text = str(value or "")
    if not text or text != text.strip() or "\\" in text or any(ord(char) < 32 for char in text):
        raise OfficialSiteIngestError(f"{location} is not a canonical HTTPS URL")
    try:
        parsed = urlsplit(text)
        port = parsed.port
    except ValueError as exc:
        raise OfficialSiteIngestError(f"{location} is not a valid HTTPS URL") from exc
    if parsed.scheme.casefold() != "https" or not parsed.hostname:
        raise OfficialSiteIngestError(f"{location} must use HTTPS")
    if parsed.username is not None or parsed.password is not None or "@" in parsed.netloc:
        raise OfficialSiteIngestError(f"{location} must not contain credentials")
    if port not in {None, 443}:
        raise OfficialSiteIngestError(f"{location} must use the default HTTPS port")
    if parsed.fragment:
        raise OfficialSiteIngestError(f"{location} must not contain a fragment")
    if parsed.query and not allow_query:
        raise OfficialSiteIngestError(f"{location} must not contain a query")
    if parsed.query and any(
        key.strip().casefold() in SENSITIVE_QUERY_KEYS for key, _ in parse_qsl(parsed.query)
    ):
        raise OfficialSiteIngestError(f"{location} must not contain credential-like query fields")
    host = _validate_host(parsed.hostname, location=f"{location} host")
    return text, host


def stable_connector_id(entity_type: str, entity_id: str) -> str:
    prefix = "company-site" if entity_type == "company" else "activist-site"
    readable = f"{prefix}:{entity_id}"
    if len(readable) <= 64 and ID_PATTERN.fullmatch(readable):
        return readable
    return f"{prefix}:{hashlib.sha256(entity_id.encode('utf-8')).hexdigest()[:32]}"


def stable_source_right_id(connector_id: str) -> str:
    readable = f"right:{connector_id}"
    if len(readable) <= 64 and ID_PATTERN.fullmatch(readable):
        return readable
    return f"right:official-site:{hashlib.sha256(connector_id.encode('utf-8')).hexdigest()[:32]}"


def _json_response(
    response: httpx.Response,
    *,
    location: str,
    expected_host: str,
) -> dict[str, object]:
    if response.history or response.is_redirect:
        raise OfficialSiteIngestError(f"{location} redirects are forbidden")
    _, response_host = validate_https_url(
        str(response.url),
        location=f"{location} response URL",
        allow_query=True,
    )
    if response_host != expected_host:
        raise OfficialSiteIngestError(f"{location} response escaped its allowlisted host")
    if response.status_code < 200 or response.status_code >= 300:
        raise OfficialSiteIngestError(f"{location} returned HTTP {response.status_code}")
    content_type = response.headers.get("content-type", "").split(";", 1)[0].strip().casefold()
    if content_type != "application/json":
        raise OfficialSiteIngestError(f"{location} must return application/json")
    if len(response.content) > MAX_ADAPTER_RESPONSE_BYTES:
        raise OfficialSiteIngestError(f"{location} exceeded the response byte limit")
    try:
        payload = response.json()
    except ValueError as exc:
        raise OfficialSiteIngestError(f"{location} returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise OfficialSiteIngestError(f"{location} must return a JSON object")
    return {str(key): item for key, item in payload.items()}


def _api_get(
    client: httpx.Client,
    *,
    api_base_url: str,
    token: str,
    path: str,
    params: Mapping[str, object] | None = None,
) -> dict[str, object]:
    if len(token) < 20 or any(char.isspace() for char in token):
        raise OfficialSiteIngestError("the private API token is missing or invalid")
    base, host = validate_https_url(
        api_base_url.rstrip("/"),
        location="private API base URL",
        allow_query=False,
    )
    try:
        response = client.get(
            f"{base}{path}",
            params=params,
            headers={"Accept": "application/json", "Authorization": f"Bearer {token}"},
            follow_redirects=False,
        )
    except httpx.HTTPError as exc:
        raise OfficialSiteIngestError(f"private API {path} request failed") from exc
    return _json_response(
        response,
        location=f"private API {path}",
        expected_host=host,
    )


def fetch_candidates(
    client: httpx.Client,
    *,
    api_base_url: str,
    token: str,
) -> tuple[list[Candidate], dict[str, object]]:
    payload = _api_get(
        client,
        api_base_url=api_base_url,
        token=token,
        path="/ops/official-site-candidates",
    )
    if payload.get("ok") is not True or payload.get("score_version") != "official-site-candidates-v1":
        raise OfficialSiteIngestError("official-site candidate response has an unsupported contract")
    raw_companies = payload.get("companies")
    raw_actors = payload.get("actors")
    if not isinstance(raw_companies, list) or not isinstance(raw_actors, list):
        raise OfficialSiteIngestError("official-site candidate lists are required")
    if len(raw_companies) > 20 or len(raw_actors) > 10:
        raise OfficialSiteIngestError("official-site candidate API exceeded its selection limits")
    candidates: list[Candidate] = []
    seen: set[tuple[str, str]] = set()
    for entity_type, rows, id_field, name_field in (
        ("company", raw_companies, "company_id", "company_name"),
        ("actor", raw_actors, "actor_id", "actor_name"),
    ):
        for index, row in enumerate(rows):
            if not isinstance(row, dict):
                raise OfficialSiteIngestError(f"{entity_type} candidate {index} must be an object")
            entity_id = str(row.get(id_field) or "").strip()
            name = str(row.get(name_field) or "")
            valid_id = (
                re.fullmatch(r"\d{8}", entity_id) is not None
                if entity_type == "company"
                else ID_PATTERN.fullmatch(entity_id) is not None
            )
            if not valid_id or not name:
                raise OfficialSiteIngestError(f"{entity_type} candidate {index} is incomplete")
            candidate = Candidate(entity_type, entity_id, name)
            if candidate.key in seen:
                raise OfficialSiteIngestError(f"duplicate candidate {entity_type}:{entity_id}")
            seen.add(candidate.key)
            candidates.append(candidate)
    snapshot = {
        "score_version": payload["score_version"],
        "generated_at": payload.get("generated_at"),
        "companies": raw_companies,
        "actors": raw_actors,
    }
    return candidates, snapshot


def fetch_source_rights(
    client: httpx.Client,
    *,
    api_base_url: str,
    token: str,
) -> dict[str, dict[str, object]]:
    page = 1
    rights: dict[str, dict[str, object]] = {}
    while page <= MAX_ADAPTER_PAGES:
        payload = _api_get(
            client,
            api_base_url=api_base_url,
            token=token,
            path="/ops/official-site-rights",
            params={"page": page, "limit": RIGHTS_PAGE_SIZE},
        )
        if payload.get("ok") is not True:
            raise OfficialSiteIngestError("source-right response is not successful")
        rows = payload.get("data")
        pagination = payload.get("pagination")
        if not isinstance(rows, list) or not isinstance(pagination, dict):
            raise OfficialSiteIngestError("source-right response omitted pagination")
        received_page = _as_int(pagination.get("page"), location="source-right page", minimum=1)
        returned = _as_int(
            pagination.get("returned"),
            location="source-right returned count",
            minimum=0,
        )
        has_more = pagination.get("has_more")
        if not isinstance(has_more, bool):
            raise OfficialSiteIngestError("source-right has_more must be a boolean")
        received_limit = _as_int(
            pagination.get("limit"),
            location="source-right page limit",
            minimum=1,
        )
        if received_page != page or received_limit != RIGHTS_PAGE_SIZE or returned != len(rows):
            raise OfficialSiteIngestError("source-right pagination drift or count mismatch")
        if len(rows) > RIGHTS_PAGE_SIZE:
            raise OfficialSiteIngestError("source-right page exceeded the requested limit")
        if has_more and not rows:
            raise OfficialSiteIngestError("source-right pagination returned an empty middle page")
        expected_next = page + 1 if has_more else None
        if pagination.get("next_page") != expected_next:
            raise OfficialSiteIngestError("source-right next_page does not match has_more")
        for index, raw in enumerate(rows):
            if not isinstance(raw, dict):
                raise OfficialSiteIngestError(f"source-right row {page}:{index} must be an object")
            source_right_id = str(raw.get("source_right_id") or "").strip()
            if not ID_PATTERN.fullmatch(source_right_id):
                raise OfficialSiteIngestError(f"source-right row {page}:{index} has an invalid ID")
            if source_right_id in rights:
                raise OfficialSiteIngestError(f"duplicate source right {source_right_id}")
            rights[source_right_id] = {str(key): value for key, value in raw.items()}
        if not has_more:
            return rights
        page += 1
    raise OfficialSiteIngestError("source-right pagination exceeded the page limit")


def parse_allowlist_manifest(
    payload: object,
    *,
    candidates: Sequence[Candidate],
) -> list[Connector]:
    if not isinstance(payload, dict):
        raise OfficialSiteIngestError("allowlist manifest must be a JSON object")
    manifest = {str(key): value for key, value in payload.items()}
    _require_exact_keys(
        manifest,
        required={"schema_version", "connectors"},
        location="allowlist manifest",
    )
    if (
        _as_int(
            manifest["schema_version"],
            location="allowlist manifest schema_version",
            minimum=1,
        )
        != ADAPTER_SCHEMA_VERSION
    ):
        raise OfficialSiteIngestError("allowlist manifest schema_version must be 1")
    raw_connectors = manifest["connectors"]
    if not isinstance(raw_connectors, list):
        raise OfficialSiteIngestError("allowlist manifest connectors must be a list")

    expected = {candidate.key for candidate in candidates}
    observed: set[tuple[str, str]] = set()
    connector_ids: set[str] = set()
    connectors: list[Connector] = []
    required = {
        "connector_id",
        "entity_type",
        "entity_id",
        "source_class",
        "source_right_id",
        "endpoint",
        "allowed_hosts",
        "page_size",
        "active",
    }
    for index, raw in enumerate(raw_connectors):
        if not isinstance(raw, dict):
            raise OfficialSiteIngestError(f"allowlist connector {index} must be an object")
        row = {str(key): value for key, value in raw.items()}
        _require_exact_keys(row, required=required, location=f"allowlist connector {index}")
        connector_id = str(row["connector_id"] or "").strip()
        entity_type = str(row["entity_type"] or "").strip().casefold()
        entity_id = str(row["entity_id"] or "").strip()
        source_class = str(row["source_class"] or "").strip().casefold()
        source_right_id = str(row["source_right_id"] or "").strip()
        if not ID_PATTERN.fullmatch(connector_id) or connector_id in connector_ids:
            raise OfficialSiteIngestError(f"allowlist connector {index} has an invalid or duplicate connector_id")
        if entity_type not in SOURCE_CLASS_BY_ENTITY:
            raise OfficialSiteIngestError(f"allowlist connector {index} has an invalid entity_type")
        if source_class != SOURCE_CLASS_BY_ENTITY[entity_type]:
            raise OfficialSiteIngestError(f"allowlist connector {index} has the wrong source_class")
        if not ID_PATTERN.fullmatch(source_right_id):
            raise OfficialSiteIngestError(f"allowlist connector {index} has an invalid source_right_id")
        if connector_id != stable_connector_id(entity_type, entity_id):
            raise OfficialSiteIngestError(f"allowlist connector {index} does not use its stable connector_id")
        if source_right_id != stable_source_right_id(connector_id):
            raise OfficialSiteIngestError(f"allowlist connector {index} does not use its stable source_right_id")
        if row["active"] is not True:
            raise OfficialSiteIngestError("every selected candidate must have one active connector")
        page_size = _as_int(row["page_size"], location=f"connector {connector_id} page_size", minimum=1)
        if page_size > 100:
            raise OfficialSiteIngestError(f"connector {connector_id} page_size exceeds 100")
        endpoint, endpoint_host = validate_https_url(
            row["endpoint"],
            location=f"connector {connector_id} endpoint",
            allow_query=False,
        )
        raw_hosts = row["allowed_hosts"]
        if not isinstance(raw_hosts, list) or not raw_hosts:
            raise OfficialSiteIngestError(f"connector {connector_id} allowed_hosts must be a non-empty list")
        allowed_hosts = tuple(
            _validate_host(str(host), location=f"connector {connector_id} allowed host")
            for host in raw_hosts
        )
        if len(set(allowed_hosts)) != len(allowed_hosts) or endpoint_host not in allowed_hosts:
            raise OfficialSiteIngestError(f"connector {connector_id} has invalid or duplicate allowed_hosts")
        candidate_key = (entity_type, entity_id)
        if candidate_key in observed:
            raise OfficialSiteIngestError(f"candidate {entity_type}:{entity_id} has multiple connectors")
        observed.add(candidate_key)
        connector_ids.add(connector_id)
        connectors.append(
            Connector(
                connector_id=connector_id,
                entity_type=entity_type,
                entity_id=entity_id,
                source_class=source_class,
                source_right_id=source_right_id,
                endpoint=endpoint,
                endpoint_host=endpoint_host,
                allowed_hosts=allowed_hosts,
                page_size=page_size,
            )
        )
    missing = sorted(expected - observed)
    extras = sorted(observed - expected)
    if missing or extras:
        raise OfficialSiteIngestError(
            f"allowlist/candidate mismatch: missing={missing!r}, extras={extras!r}"
        )
    return sorted(connectors, key=lambda connector: connector.connector_id)


def _right_is_valid(
    right: Mapping[str, object],
    connector: Connector,
    *,
    now: datetime,
) -> bool:
    if str(right.get("status") or "").strip().casefold() != "active":
        return False
    if str(right.get("source_type") or "").strip().casefold() != connector.source_class:
        return False
    if str(right.get("source_key") or "").strip() != connector.connector_id:
        return False
    if not str(right.get("permission_scope") or "").strip():
        return False
    evidence_present = right.get("evidence_present")
    if evidence_present is not True and (
        not str(right.get("evidence_uri") or "").strip()
        and not str(right.get("evidence_hash") or "").strip()
    ):
        return False
    evidence_hash = str(right.get("evidence_hash") or "").strip().casefold()
    if evidence_hash and re.fullmatch(r"[a-f0-9]{64}", evidence_hash) is None:
        return False
    try:
        if not _as_bool(right.get("redistribution_allowed"), location="redistribution_allowed"):
            return False
        valid_from = _parse_datetime(right.get("valid_from"), location="valid_from", require_offset=False)
        valid_until_value = right.get("valid_until")
        revoked_at_value = right.get("revoked_at")
        valid_until = (
            _parse_datetime(valid_until_value, location="valid_until", require_offset=False)
            if valid_until_value is not None and valid_until_value != ""
            else None
        )
        revoked_at = (
            _parse_datetime(revoked_at_value, location="revoked_at", require_offset=False)
            if revoked_at_value is not None and revoked_at_value != ""
            else None
        )
    except OfficialSiteIngestError:
        return False
    current = now.astimezone(timezone.utc)
    return (
        current >= valid_from
        and (valid_until is None or current < valid_until)
        and (revoked_at is None or current < revoked_at)
    )


def validate_source_rights(
    connectors: Sequence[Connector],
    rights: Mapping[str, Mapping[str, object]],
    *,
    now: datetime,
) -> None:
    if now.tzinfo is None or now.utcoffset() is None:
        raise OfficialSiteIngestError("SourceRight validation time must include an explicit UTC offset")
    for connector in connectors:
        right = rights.get(connector.source_right_id)
        if right is None or not _right_is_valid(right, connector, now=now):
            raise OfficialSiteIngestError(
                f"connector {connector.connector_id} lacks an active, unrevoked redistribution right"
            )


def _validate_adapter_item(
    raw: object,
    *,
    connector: Connector,
    page: int,
    index: int,
) -> dict[str, object]:
    location = f"connector {connector.connector_id} page {page} item {index}"
    if not isinstance(raw, dict):
        raise OfficialSiteIngestError(f"{location} must be an object")
    item = {str(key): value for key, value in raw.items()}
    operation = str(item.get("operation") or "").strip().casefold()
    if operation == "upsert":
        _require_exact_keys(
            item,
            required={
                "operation",
                "external_id",
                "title",
                "body",
                "language",
                "original_url",
                "published_at",
                "identity",
            },
            location=location,
        )
    elif operation == "delete":
        _require_exact_keys(
            item,
            required={"operation", "external_id", "deleted_at"},
            optional={"original_url"},
            location=location,
        )
    else:
        raise OfficialSiteIngestError(f"{location} operation must be upsert or delete")
    external_id = str(item.get("external_id") or "").strip()
    if not EXTERNAL_ID_PATTERN.fullmatch(external_id):
        raise OfficialSiteIngestError(f"{location} external_id is not stable ASCII")
    item["operation"] = operation
    item["external_id"] = external_id
    if operation == "delete":
        _parse_datetime(item["deleted_at"], location=f"{location} deleted_at", require_offset=True)
        if item.get("original_url") not in {None, ""}:
            _, host = validate_https_url(
                item["original_url"],
                location=f"{location} original_url",
                allow_query=True,
            )
            if host not in connector.allowed_hosts:
                raise OfficialSiteIngestError(f"{location} original_url escaped allowed_hosts")
        return item

    title = item["title"]
    body = item["body"]
    language = item["language"]
    if not isinstance(title, str) or not title or len(title) > 700:
        raise OfficialSiteIngestError(f"{location} title must be a non-empty original string")
    if not isinstance(body, str) or len(body.encode("utf-8")) > MAX_ITEM_BODY_BYTES:
        raise OfficialSiteIngestError(f"{location} body must be an original string within the byte limit")
    if not isinstance(language, str) or not LANGUAGE_PATTERN.fullmatch(language):
        raise OfficialSiteIngestError(f"{location} language must be an IETF-like original-language tag")
    _parse_datetime(item["published_at"], location=f"{location} published_at", require_offset=True)
    _, host = validate_https_url(
        item["original_url"],
        location=f"{location} original_url",
        allow_query=True,
    )
    if host not in connector.allowed_hosts:
        raise OfficialSiteIngestError(f"{location} original_url escaped allowed_hosts")
    identity = item["identity"]
    if not isinstance(identity, dict):
        raise OfficialSiteIngestError(f"{location} identity must be an object")
    _require_exact_keys(
        {str(key): value for key, value in identity.items()},
        required={
            "company_id",
            "event_type",
            "action",
            "target",
            "actor_id",
            "effective_at",
            "deadline_at",
        },
        location=f"{location} identity",
    )
    company_id = str(identity.get("company_id") or "").strip()
    actor_id = str(identity.get("actor_id") or "").strip()
    if connector.entity_type == "company" and company_id and company_id != connector.entity_id:
        raise OfficialSiteIngestError(f"{location} escaped its company candidate scope")
    if connector.entity_type == "actor" and actor_id and actor_id != connector.entity_id:
        raise OfficialSiteIngestError(f"{location} escaped its actor candidate scope")
    return item


def fetch_adapter(
    client: httpx.Client,
    connector: Connector,
) -> AdapterResult:
    page = 1
    expected_pages: int | None = None
    expected_count: int | None = None
    items: list[dict[str, object]] = []
    seen_ids: set[str] = set()
    raw_pages: list[dict[str, object]] = []
    while page <= MAX_ADAPTER_PAGES:
        request = client.build_request(
            "GET",
            connector.endpoint,
            params={"page": page, "per_page": connector.page_size},
            headers={"Accept": "application/json"},
        )
        if any(
            header in request.headers
            for header in ("authorization", "cookie", "proxy-authorization")
        ):
            raise OfficialSiteIngestError(
                f"connector {connector.connector_id} attempted a credentialed source request"
            )
        try:
            response = client.send(request, follow_redirects=False)
        except httpx.HTTPError as exc:
            raise OfficialSiteIngestError(f"connector {connector.connector_id} request failed") from exc
        payload = _json_response(
            response,
            location=f"connector {connector.connector_id} page {page}",
            expected_host=connector.endpoint_host,
        )
        _require_exact_keys(
            payload,
            required={"schema_version", "connector_id", "page", "total_pages", "total_count", "items"},
            location=f"connector {connector.connector_id} page {page}",
        )
        if _as_int(payload["schema_version"], location="adapter schema_version", minimum=1) != ADAPTER_SCHEMA_VERSION:
            raise OfficialSiteIngestError(f"connector {connector.connector_id} has an unsupported schema")
        if payload["connector_id"] != connector.connector_id:
            raise OfficialSiteIngestError(f"connector {connector.connector_id} returned the wrong connector_id")
        received_page = _as_int(payload["page"], location="adapter page", minimum=1)
        total_pages = _as_int(payload["total_pages"], location="adapter total_pages", minimum=1)
        total_count = _as_int(payload["total_count"], location="adapter total_count", minimum=0)
        raw_items = payload["items"]
        if not isinstance(raw_items, list):
            raise OfficialSiteIngestError(f"connector {connector.connector_id} items must be a list")
        if len(raw_items) > connector.page_size:
            raise OfficialSiteIngestError(f"connector {connector.connector_id} exceeded its page_size")
        if received_page != page:
            raise OfficialSiteIngestError(
                f"connector {connector.connector_id} requested page {page} but received page {received_page}"
            )
        if total_pages > MAX_ADAPTER_PAGES:
            raise OfficialSiteIngestError(f"connector {connector.connector_id} exceeded the page limit")
        if total_count > total_pages * connector.page_size:
            raise OfficialSiteIngestError(f"connector {connector.connector_id} has impossible pagination totals")
        if expected_pages is None:
            expected_pages, expected_count = total_pages, total_count
        elif total_pages != expected_pages or total_count != expected_count:
            raise OfficialSiteIngestError(f"connector {connector.connector_id} pagination totals drifted")
        if page > total_pages:
            raise OfficialSiteIngestError(f"connector {connector.connector_id} returned an impossible page")
        if not raw_items and (total_count > 0 or page < total_pages):
            raise OfficialSiteIngestError(f"connector {connector.connector_id} returned an empty middle page")
        normalized_page: list[dict[str, object]] = []
        for index, raw in enumerate(raw_items):
            item = _validate_adapter_item(raw, connector=connector, page=page, index=index)
            external_id = str(item["external_id"])
            if external_id in seen_ids:
                raise OfficialSiteIngestError(
                    f"connector {connector.connector_id} returned duplicate external_id {external_id}"
                )
            seen_ids.add(external_id)
            items.append(item)
            normalized_page.append(item)
        raw_pages.append(
            {
                "page": page,
                "total_pages": total_pages,
                "total_count": total_count,
                "items": normalized_page,
            }
        )
        if page == total_pages:
            if len(items) != total_count:
                raise OfficialSiteIngestError(
                    f"connector {connector.connector_id} count mismatch: expected {total_count}, got {len(items)}"
                )
            return AdapterResult(tuple(items), page, total_count, _sha256(raw_pages))
        page += 1
    raise OfficialSiteIngestError(f"connector {connector.connector_id} pagination did not terminate")


def _identity_for_item(item: Mapping[str, object]) -> tuple[EventIdentity, list[str]]:
    raw = item.get("identity")
    if not isinstance(raw, Mapping):
        raise OfficialSiteIngestError("validated upsert lost its identity object")
    event_type = str(raw.get("event_type") or "").strip()
    actor_id = str(raw.get("actor_id") or "").strip()
    reasons: list[str] = []
    if event_type and event_type not in EVENT_TYPES:
        reasons.append("invalid_event_type")
        event_type = ""
    if actor_id and not ID_PATTERN.fullmatch(actor_id):
        reasons.append("invalid_actor_id")
        actor_id = ""
    identity = build_event_identity(
        company_id=raw.get("company_id"),
        event_type=event_type,
        action=raw.get("action"),
        target=raw.get("target"),
        actor_id=actor_id,
        effective_at=raw.get("effective_at"),
        deadline_at=raw.get("deadline_at"),
    )
    reasons.extend(reason for reason in identity.review_reasons if reason not in reasons)
    return identity, reasons


def _document_candidate(
    connector: Connector,
    item: Mapping[str, object],
    *,
    retrieved_at: str,
    identity: EventIdentity,
) -> dict[str, object]:
    external_id = str(item["external_id"])
    title = item["title"]
    body = item["body"]
    original_url = item["original_url"]
    content_hash = hashlib.sha256(
        f"{title}\n{body}\n{original_url}".encode("utf-8")
    ).hexdigest()
    return {
        # The external ID identifies the logical source record while the
        # content hash identifies an immutable observed version.  Re-fetching
        # unchanged content therefore remains idempotent; a correction creates
        # a new draft document instead of overwriting an editor-approved row.
        "document_id": stable_id(
            "site-doc",
            connector.connector_id,
            external_id,
            content_hash,
            length=32,
        ),
        "external_id": external_id,
        "company_id": identity.company_id or None,
        "source_class": connector.source_class,
        "source_right_id": connector.source_right_id,
        "document_type": identity.event_type or None,
        "original_language": item["language"],
        "title": title,
        "body_text": body,
        "original_url": original_url,
        "content_hash": content_hash,
        "collection_key": stable_id("site-collection", connector.connector_id, external_id, length=32),
        "version_no": 1,
        "published_at": item["published_at"],
        "retrieved_at": retrieved_at,
        "verification_status": "unverified",
        "publication_status": "draft",
    }


def _event_candidate(
    connector: Connector,
    item: Mapping[str, object],
    document: Mapping[str, object],
    identity: EventIdentity,
) -> dict[str, object]:
    if identity.status is not EventIdentityStatus.COMPLETE or not identity.comparison_key:
        raise OfficialSiteIngestError("only complete identities may become draft event candidates")
    event: dict[str, object] = {
        "event_id": identity.comparison_key,
        "company_id": identity.company_id,
        "event_type": identity.event_type,
        "title": item["title"],
        "original_language": item["language"],
        "summary": "",
        "occurred_at": identity.effective_at,
        "deadline_at": identity.deadline_at,
        "importance": "medium",
        "verification_status": "unverified",
        "review_status": "pending",
        "publication_status": "draft",
        "review_required": True,
        "collection_key": stable_id(
            "site-event",
            connector.connector_id,
            item["external_id"],
            length=32,
        ),
        "document_ids": [document["document_id"]],
        "source_right_ids": [connector.source_right_id],
        "action": identity.action,
        "target": identity.target,
    }
    event.update(identity.to_payload())
    return event


def _merge_event(
    existing: dict[str, object],
    incoming: Mapping[str, object],
) -> None:
    identity_fields = (
        "company_id",
        "event_type",
        "identity_action",
        "identity_target",
        "identity_actor_id",
        "identity_effective_at",
        "identity_deadline_at",
        "comparison_key",
    )
    if any(existing.get(field) != incoming.get(field) for field in identity_fields):
        raise OfficialSiteIngestError("a shared comparison key carried conflicting identity fields")
    existing_documents = existing.get("document_ids")
    incoming_documents = incoming.get("document_ids")
    existing_rights = existing.get("source_right_ids")
    incoming_rights = incoming.get("source_right_ids")
    if not isinstance(existing_documents, list) or not isinstance(incoming_documents, list):
        raise OfficialSiteIngestError("an event candidate omitted document lineage")
    if not isinstance(existing_rights, list) or not isinstance(incoming_rights, list):
        raise OfficialSiteIngestError("an event candidate omitted SourceRight lineage")
    existing["document_ids"] = sorted(
        {
            *[str(value) for value in existing_documents if value],
            *[str(value) for value in incoming_documents if value],
        }
    )
    existing["source_right_ids"] = sorted(
        {
            *[str(value) for value in existing_rights if value],
            *[str(value) for value in incoming_rights if value],
        }
    )


def build_artifact(
    *,
    candidates: Sequence[Candidate],
    candidate_snapshot: Mapping[str, object],
    manifest_payload: Mapping[str, object],
    connectors: Sequence[Connector],
    adapter_results: Mapping[str, AdapterResult],
    now: datetime,
    code_revision: str,
) -> dict[str, object]:
    retrieved_at = now.astimezone(timezone.utc).replace(microsecond=0).isoformat()
    documents: dict[str, dict[str, object]] = {}
    events: dict[str, dict[str, object]] = {}
    review_items: list[dict[str, object]] = []
    tombstones: list[dict[str, object]] = []
    connector_receipts: list[dict[str, object]] = []

    for connector in connectors:
        result = adapter_results[connector.connector_id]
        connector_receipts.append(
            {
                "connector_id": connector.connector_id,
                "entity_type": connector.entity_type,
                "entity_id": connector.entity_id,
                "source_class": connector.source_class,
                "source_right_id": connector.source_right_id,
                "pages_fetched": result.pages_fetched,
                "total_count": result.total_count,
                "payload_sha256": result.payload_sha256,
            }
        )
        for item in result.items:
            external_id = str(item["external_id"])
            if item["operation"] == "delete":
                tombstones.append(
                    {
                        "tombstone_id": stable_id(
                            "site-tombstone",
                            connector.connector_id,
                            external_id,
                            item["deleted_at"],
                            length=32,
                        ),
                        "connector_id": connector.connector_id,
                        "entity_type": connector.entity_type,
                        "entity_id": connector.entity_id,
                        "source_class": connector.source_class,
                        "source_right_id": connector.source_right_id,
                        "external_id": external_id,
                        "deleted_at": item["deleted_at"],
                        "original_url": item.get("original_url"),
                        "action": "review_only_no_automatic_delete",
                    }
                )
                continue
            identity, reasons = _identity_for_item(item)
            document = _document_candidate(
                connector,
                item,
                retrieved_at=retrieved_at,
                identity=identity,
            )
            document_id = str(document["document_id"])
            previous = documents.get(document_id)
            if previous is not None and previous != document:
                raise OfficialSiteIngestError(f"document ID collision {document_id}")
            if identity.status is not EventIdentityStatus.COMPLETE or reasons:
                review_items.append(
                    {
                        "review_id": stable_id(
                            "site-review",
                            connector.connector_id,
                            external_id,
                            document["content_hash"],
                            length=32,
                        ),
                        "connector_id": connector.connector_id,
                        "entity_type": connector.entity_type,
                        "entity_id": connector.entity_id,
                        "source_class": connector.source_class,
                        "source_right_id": connector.source_right_id,
                        "external_id": external_id,
                        "review_reasons": reasons,
                        "draft_document": document,
                        "proposed_identity": item["identity"],
                        "action": "editor_identity_review_required",
                    }
                )
                continue
            documents[document_id] = document
            event = _event_candidate(connector, item, document, identity)
            event_id = str(event["event_id"])
            if event_id in events:
                _merge_event(events[event_id], event)
            else:
                events[event_id] = event

    companies = [
        {
            "company_id": candidate.entity_id,
            "legal_name": candidate.display_name,
            "record_status": "active",
        }
        for candidate in sorted(candidates, key=lambda row: row.key)
        if candidate.entity_type == "company"
    ]
    total_items = sum(result.total_count for result in adapter_results.values())
    artifact: dict[str, object] = {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "artifact_type": "official-site-ingest-receipt",
        "status": "succeeded",
        "code_revision": code_revision,
        "collected_at": retrieved_at,
        "candidate_snapshot": {
            "score_version": candidate_snapshot.get("score_version"),
            "generated_at": candidate_snapshot.get("generated_at"),
            "company_count": sum(1 for item in candidates if item.entity_type == "company"),
            "actor_count": sum(1 for item in candidates if item.entity_type == "actor"),
            "payload_sha256": _sha256(candidate_snapshot),
        },
        "manifest_sha256": _sha256(manifest_payload),
        "connectors": connector_receipts,
        "draft_payload": {
            "companies": companies,
            "documents": sorted(documents.values(), key=lambda row: str(row["document_id"])),
            "events": sorted(events.values(), key=lambda row: str(row["event_id"])),
            "source_rights": [],
        },
        "review_items": sorted(review_items, key=lambda row: str(row["review_id"])),
        "tombstones": sorted(tombstones, key=lambda row: str(row["tombstone_id"])),
        "counts": {
            "candidate_count": len(candidates),
            "connector_count": len(connectors),
            "raw_item_count": total_items,
            "draft_document_count": len(documents),
            "draft_event_count": len(events),
            "review_count": len(review_items),
            "tombstone_count": len(tombstones),
        },
        "apply": {
            "mode": "not_requested",
            "remote_mutation_performed": False,
            "reason": "collection_and_apply_are_separate_fail_closed_steps",
        },
    }
    artifact["receipt_sha256"] = _sha256(artifact)
    return artifact


def _artifact_rows(artifact: Mapping[str, object], name: str) -> list[dict[str, object]]:
    value = artifact.get(name)
    if not isinstance(value, list) or any(not isinstance(row, dict) for row in value):
        raise OfficialSiteIngestError(f"artifact {name} must be an object list")
    return [{str(key): item for key, item in row.items()} for row in value]


def build_apply_payloads(artifact: Mapping[str, object]) -> list[dict[str, object]]:
    """Build one bounded, atomic, idempotent payload for each connector."""

    if artifact.get("schema_version") != ARTIFACT_SCHEMA_VERSION:
        raise OfficialSiteIngestError("official-site artifact schema_version is unsupported")
    if artifact.get("artifact_type") != "official-site-ingest-receipt" or artifact.get("status") != "succeeded":
        raise OfficialSiteIngestError("official-site artifact is not a successful collection receipt")
    revision = _revision(str(artifact.get("code_revision") or ""))
    collected_at = _parse_datetime(
        artifact.get("collected_at"),
        location="artifact collected_at",
        require_offset=True,
    ).isoformat()
    receipt_sha256 = str(artifact.get("receipt_sha256") or "").strip().casefold()
    manifest_sha256 = str(artifact.get("manifest_sha256") or "").strip().casefold()
    if re.fullmatch(r"[a-f0-9]{64}", receipt_sha256) is None:
        raise OfficialSiteIngestError("artifact receipt_sha256 is invalid")
    if re.fullmatch(r"[a-f0-9]{64}", manifest_sha256) is None:
        raise OfficialSiteIngestError("artifact manifest_sha256 is invalid")

    connector_rows = _artifact_rows(artifact, "connectors")
    draft = artifact.get("draft_payload")
    if not isinstance(draft, dict):
        raise OfficialSiteIngestError("artifact draft_payload must be an object")
    companies = _artifact_rows(draft, "companies")
    documents = _artifact_rows(draft, "documents")
    events = _artifact_rows(draft, "events")
    reviews = _artifact_rows(artifact, "review_items")
    tombstones = _artifact_rows(artifact, "tombstones")
    payloads: list[dict[str, object]] = []
    seen_connectors: set[str] = set()

    for connector in connector_rows:
        connector_id = str(connector.get("connector_id") or "").strip()
        source_right_id = str(connector.get("source_right_id") or "").strip()
        entity_type = str(connector.get("entity_type") or "").strip()
        entity_id = str(connector.get("entity_id") or "").strip()
        if (
            not ID_PATTERN.fullmatch(connector_id)
            or connector_id in seen_connectors
            or not ID_PATTERN.fullmatch(source_right_id)
            or entity_type not in SOURCE_CLASS_BY_ENTITY
        ):
            raise OfficialSiteIngestError("artifact contains an invalid or duplicate connector receipt")
        seen_connectors.add(connector_id)
        connector_documents = [
            row for row in documents if str(row.get("source_right_id") or "") == source_right_id
        ]
        document_ids = {str(row.get("document_id") or "") for row in connector_documents}
        connector_events: list[dict[str, object]] = []
        expected_observations = 0
        for event in events:
            raw_ids = event.get("document_ids")
            if not isinstance(raw_ids, list):
                raise OfficialSiteIngestError("draft event omitted document_ids")
            selected_ids = sorted({str(value) for value in raw_ids if str(value) in document_ids})
            if not selected_ids:
                continue
            event_copy = dict(event)
            event_copy["document_ids"] = selected_ids
            event_copy["source_right_ids"] = [source_right_id]
            connector_events.append(event_copy)
            expected_observations += len(selected_ids)
        connector_reviews = [
            row for row in reviews if str(row.get("connector_id") or "") == connector_id
        ]
        connector_tombstones = [
            row for row in tombstones if str(row.get("connector_id") or "") == connector_id
        ]
        connector_companies = [
            row
            for row in companies
            if entity_type == "company" and str(row.get("company_id") or "") == entity_id
        ]
        core: dict[str, object] = {
            "schema_version": APPLY_SCHEMA_VERSION,
            "receipt_sha256": receipt_sha256,
            "code_revision": revision,
            "collected_at": collected_at,
            "manifest_sha256": manifest_sha256,
            "connector": connector,
            "companies": connector_companies,
            "documents": connector_documents,
            "events": connector_events,
            "review_items": connector_reviews,
            "tombstones": connector_tombstones,
            "expected": {
                "companies": len(connector_companies),
                "documents": len(connector_documents),
                "events": len(connector_events),
                "event_observations": expected_observations,
                "review_items": len(connector_reviews),
                "tombstones": len(connector_tombstones),
            },
        }
        payload_hash = _sha256(core)
        core["payload_sha256"] = payload_hash
        core["snapshot_id"] = stable_id(
            "official-site-snapshot",
            connector_id,
            receipt_sha256,
            payload_hash,
            length=64,
        )
        if len(_canonical_json(core)) > MAX_ATOMIC_APPLY_BYTES:
            raise OfficialSiteIngestError(
                f"connector {connector_id} exceeds the atomic apply byte limit"
            )
        payloads.append(core)
    return payloads


def _ack_count(response: Mapping[str, object], name: str) -> int:
    accepted = response.get("accepted")
    if not isinstance(accepted, dict):
        return -1
    value = accepted.get(name)
    if isinstance(value, bool):
        return -1
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return -1


def apply_artifact(artifact: Mapping[str, object]) -> dict[str, object]:
    """Atomically persist every connector snapshot and require an exact ACK."""

    if not remote_api_configured():
        raise OfficialSiteIngestError(
            "ACTIVIST_API_URL and ACTIVIST_API_SECRET are required for official-site apply"
        )
    payloads = build_apply_payloads(artifact)
    aggregate = {
        "companies": 0,
        "documents": 0,
        "events": 0,
        "event_observations": 0,
        "review_items": 0,
        "tombstones": 0,
    }
    idempotent_count = 0
    for payload in payloads:
        response = post_remote_action(
            "upsert_official_site_snapshot",
            payload,
            timeout=60.0,
        )
        expected = payload["expected"]
        if not isinstance(expected, dict):
            raise OfficialSiteIngestError("official-site apply payload omitted expected counts")
        exact = all(
            _ack_count(response, name) == int(expected[name])
            for name in aggregate
        )
        if (
            response.get("ok") is not True
            or str(response.get("snapshot_id") or "") != payload["snapshot_id"]
            or str(response.get("receipt_sha256") or "") != payload["receipt_sha256"]
            or int(response.get("rejected") or 0) != 0
            or not exact
        ):
            raise OfficialSiteIngestError(
                f"connector {payload['connector']['connector_id']} apply ACK mismatch"  # type: ignore[index]
            )
        for name in aggregate:
            aggregate[name] += int(expected[name])
        idempotent_count += int(response.get("idempotent") is True)

    result = json.loads(_canonical_json(artifact).decode("utf-8"))
    result["apply"] = {
        "mode": "remote_atomic_per_connector",
        "remote_mutation_performed": True,
        "connector_count": len(payloads),
        "idempotent_connector_count": idempotent_count,
        "accepted": aggregate,
    }
    return result


def collect(
    *,
    api_base_url: str,
    api_token: str,
    manifest_payload: Mapping[str, object],
    now: datetime,
    code_revision: str,
    client: httpx.Client,
) -> dict[str, object]:
    if now.tzinfo is None or now.utcoffset() is None:
        raise OfficialSiteIngestError("collection time must include an explicit UTC offset")
    candidates, candidate_snapshot = fetch_candidates(
        client,
        api_base_url=api_base_url,
        token=api_token,
    )
    connectors = parse_allowlist_manifest(manifest_payload, candidates=candidates)
    rights = fetch_source_rights(
        client,
        api_base_url=api_base_url,
        token=api_token,
    )
    validate_source_rights(connectors, rights, now=now)
    results = {connector.connector_id: fetch_adapter(client, connector) for connector in connectors}
    return build_artifact(
        candidates=candidates,
        candidate_snapshot=candidate_snapshot,
        manifest_payload=manifest_payload,
        connectors=connectors,
        adapter_results=results,
        now=now,
        code_revision=code_revision,
    )


def decode_manifest_base64(value: str) -> dict[str, object]:
    try:
        decoded = base64.b64decode(value, validate=True).decode("utf-8")
        payload = json.loads(decoded)
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise OfficialSiteIngestError("OFFICIAL_SITE_ALLOWLIST_B64 is not canonical base64 JSON") from exc
    if not isinstance(payload, dict):
        raise OfficialSiteIngestError("OFFICIAL_SITE_ALLOWLIST_B64 must decode to a JSON object")
    return {str(key): item for key, item in payload.items()}


def _revision(value: str) -> str:
    revision = value.strip().casefold()
    if not re.fullmatch(r"[a-f0-9]{40}", revision):
        raise OfficialSiteIngestError("a full 40-character code revision is required")
    return revision


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Collect and atomically apply official-site drafts.")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="persist connector snapshots through the HMAC write API after collection",
    )
    args = parser.parse_args(argv)
    mode = os.environ.get("GOVERNANCE_PIPELINE_MODE", "").strip().casefold()
    if mode not in {"shadow", "live"}:
        parser.error("GOVERNANCE_PIPELINE_MODE must resolve to shadow or live")
    try:
        manifest = decode_manifest_base64(os.environ.get("OFFICIAL_SITE_ALLOWLIST_B64", ""))
        revision = _revision(os.environ.get("GITHUB_SHA", ""))
        with httpx.Client(timeout=30.0, follow_redirects=False) as client:
            artifact = collect(
                api_base_url=os.environ.get("BSIDE_API_BASE_URL", ""),
                api_token=os.environ.get("BSIDE_OPS_TOKEN", ""),
                manifest_payload=manifest,
                now=datetime.now(timezone.utc),
                code_revision=revision,
                client=client,
            )
        if args.apply:
            artifact = apply_artifact(artifact)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_bytes(_canonical_json(artifact) + b"\n")
    except (OfficialSiteIngestError, httpx.HTTPError, OSError) as exc:
        parser.error(str(exc))
    counts = artifact["counts"]
    print(json.dumps({"ok": True, "counts": counts}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
