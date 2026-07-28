#!/usr/bin/env python3
"""End-to-end PHP 7.3/MySQL 8 smoke for the global terminal API v2.

This test runs only against the isolated GitHub Actions MySQL service.  It
exercises the real PHP HTTP entry point and verifies that automation cannot
publish an event before an editor has completed its identity.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from curator.global_connectors import (  # noqa: E402
    GlobalDocumentRecord,
    IssuerReference,
    global_document_content_hash,
)


def _deployed_code_revision() -> str:
    manifest_path = (
        REPOSITORY_ROOT / "deploy" / "activist" / "deployment-manifest.json"
    )
    if not manifest_path.is_file():
        return "a" * 40
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    revision = str(manifest.get("code_revision", "")).strip().lower()
    if len(revision) != 40 or any(
        character not in "0123456789abcdef" for character in revision
    ):
        raise RuntimeError("deployment manifest code_revision is invalid")
    return revision


ADMIN_TOKEN = "php73-ci-admin-token-00000000000000000000"
EDITOR_TOKEN = "php73-ci-editor-token-0000000000000000000"
OPS_TOKEN = "php73-ci-ops-token-000000000000000000000"
RELEASE_AUTHORIZER_TOKEN = "php73-ci-release-token-000000000000000000"
PREVIEW_TOKEN = "php73-ci-preview-token-000000000000000000"
API_SECRET = b"php73-ci-only-hmac-key-00000000000000000000000000000000"
MYSQL_ROOT_PASSWORD = "activist_ci_root_password"
DATABASE = "activist_ci"
TABLE_PREFIX = "ci_"
CODE_REVISION = _deployed_code_revision()
EVIDENCE_ARTIFACT_DIGEST = "sha256:" + ("b" * 64)
SEC_RIGHT_ID = "official:sec-edgar"
SEC_CONNECTOR_ID = "connector:us:sec-edgar"
SEC_SOURCE_KEY = "sec-edgar"
ALTERNATE_RIGHT_ID = "official:sec-ci-alternate"
ALTERNATE_URL = "https://www.sec.gov/Archives/edgar/data/320193/ci-alternate.txt"
TELEGRAM_RIGHT_ID = "telegram:ci-authorized"
TELEGRAM_URL = "https://t.me/ci_private_signal/42"
REQUIRED_ALPHA_RIGHT_IDS = (
    "official:dart",
    "official:sec-edgar",
    "official:ca-issuer-ir",
    "official:asic-register",
)
DART_CONTRACT_FIXTURE = json.loads(
    (
        REPOSITORY_ROOT
        / "tests"
        / "fixtures"
        / "dart_source_right_contract_v1.json"
    ).read_text(encoding="utf-8")
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def sec_current_cursor(updated_at: datetime) -> str:
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
    return "sec-current-v1:" + encoded


def set_sec_current_cursor(
    mysql_container_id: str,
    updated_at: datetime,
) -> None:
    cursor = sec_current_cursor(updated_at)
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_connectors SET cursor_json=JSON_SET("
        "COALESCE(cursor_json,JSON_OBJECT()),'$.schema_version',2,"
        f"'$.source_cursor','{cursor}') "
        "WHERE connector_id='connector:us:sec-edgar';",
    )


def request_json(
    base_url: str,
    path: str,
    *,
    method: str = "GET",
    token: str | None = None,
    preview_token: str | None = None,
    payload: dict[str, Any] | None = None,
    expected_status: int = 200,
) -> tuple[dict[str, Any], dict[str, str]]:
    body = None
    headers = {"Accept": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    if preview_token is not None:
        headers["X-BSIDE-Preview-Token"] = preview_token
    if payload is not None:
        body = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/{path.lstrip('/')}",
        data=body,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status = response.status
            raw = response.read()
            response_headers = dict(response.headers.items())
    except urllib.error.HTTPError as error:
        status = error.code
        raw = error.read()
        response_headers = dict(error.headers.items())
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise AssertionError(
            f"{method} {path} returned non-JSON status={status}: {raw[:500]!r}"
        ) from error
    require(
        isinstance(decoded, dict),
        f"{method} {path} did not return a JSON object: {decoded!r}",
    )
    require(
        status == expected_status,
        f"{method} {path} expected {expected_status}, got {status}: {decoded!r}",
    )
    return decoded, response_headers


def request_dart_hmac_write(
    base_url: str,
    payload: dict[str, Any],
    *,
    expected_status: int,
) -> dict[str, Any]:
    body = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    timestamp = str(int(datetime.now(timezone.utc).timestamp()))
    nonce = "global-live-dart-" + hashlib.sha256(body).hexdigest()[:32]
    signature = hmac.new(
        API_SECRET,
        timestamp.encode("ascii")
        + b"\n"
        + nonce.encode("ascii")
        + b"\n"
        + body,
        hashlib.sha256,
    ).hexdigest()
    request = urllib.request.Request(
        (
            f"{base_url.rstrip('/')}/api.php?"
            "action=upsert_governance_snapshot_dart_guarded"
        ),
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Activist-Timestamp": timestamp,
            "X-Activist-Nonce": nonce,
            "X-Activist-Signature": f"sha256={signature}",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status = response.status
            raw = response.read()
    except urllib.error.HTTPError as error:
        status = error.code
        raw = error.read()
    decoded = json.loads(raw.decode("utf-8"))
    require(
        status == expected_status and isinstance(decoded, dict),
        f"DART HMAC write expected {expected_status}, got {status}: {decoded!r}",
    )
    return decoded


def dart_guarded_metadata_payload(
    *,
    company_id: str,
    external_id: str,
    expected_release_state: str,
    rights_revision: str,
    contract_revision: str,
    backend_binding_id: str,
) -> dict[str, Any]:
    title = f"CI DART metadata-only {expected_release_state} filing"
    url = f"https://opendart.fss.or.kr/ci/{external_id}"
    return {
        "companies": [
            {
                "company_id": company_id,
                "stock_code": company_id[-6:],
                "market": "KOSPI",
                "legal_name": f"CI DART {expected_release_state} company",
                "record_status": "active",
            }
        ],
        "documents": [
            {
                "document_id": f"dart:{external_id}",
                "company_id": company_id,
                "source": "dart",
                "source_right_id": "official:dart",
                "source_class": "official_disclosure",
                "external_id": external_id,
                "document_type": "major_holding",
                "original_language": "ko",
                "title": title,
                "body_text": "",
                "original_url": url,
                "content_hash": hashlib.sha256(
                    f"{title}\n{url}\n{external_id}".encode("utf-8")
                ).hexdigest(),
                "collection_key": f"ci-dart-{expected_release_state}",
                "version_no": 1,
                "published_at": "2026-07-27T00:00:00Z",
                "retrieved_at": "2026-07-27T00:05:00Z",
                "verification_status": "official",
                "publication_status": "published",
            }
        ],
        "events": [],
        "source_rights": [],
        "expected_source_right_revisions": {
            "official:dart": {
                "rights_revision": rights_revision,
                "contract_revision": contract_revision,
            }
        },
        "expected_deployment_code_revision": CODE_REVISION,
        "expected_release_state": expected_release_state,
        "run": {},
        "expected_backend_binding_id": backend_binding_id,
    }


def request_bytes(
    base_url: str,
    path: str,
    *,
    token: str | None = None,
    expected_status: int = 200,
) -> tuple[bytes, dict[str, str]]:
    headers = {"Accept": "*/*"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/{path.lstrip('/')}",
        headers=headers,
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status = response.status
            raw = response.read()
            response_headers = dict(response.headers.items())
    except urllib.error.HTTPError as error:
        status = error.code
        raw = error.read()
        response_headers = dict(error.headers.items())
    require(
        status == expected_status,
        f"GET {path} expected {expected_status}, got {status}: {raw[:500]!r}",
    )
    return raw, response_headers


def mysql_execute(container_id: str, sql: str) -> str:
    completed = subprocess.run(
        [
            "docker",
            "exec",
            container_id,
            "mysql",
            "--user=root",
            f"--password={MYSQL_ROOT_PASSWORD}",
            "--batch",
            "--skip-column-names",
            DATABASE,
            f"--execute={sql}",
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return completed.stdout.strip()


def connector_runtime_state(container_id: str, connector_id: str) -> str:
    return mysql_execute(
        container_id,
        "SELECT CONCAT_WS('|',COALESCE(cursor_json,''),"
        "COALESCE(last_checked_at,''),COALESCE(last_success_at,''),"
        "COALESCE(last_observed_at,''),last_raw_count,"
        "last_acknowledged_count,COALESCE(last_error_class,''),"
        "COALESCE(code_revision,''),updated_at) "
        "FROM ci_source_connectors "
        f"WHERE connector_id='{connector_id}';",
    )


def utc_text(value: datetime) -> str:
    return (
        value.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace(
            "+00:00",
            "Z",
        )
    )


def require_rfc3339_utc(value: object, field: str) -> str:
    text = str(value)
    require(
        text.endswith("Z") and "T" in text,
        f"{field} must be an RFC 3339 UTC timestamp: {text!r}",
    )
    try:
        parsed = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as error:
        raise AssertionError(
            f"{field} must be an RFC 3339 UTC timestamp: {text!r}"
        ) from error
    require(
        parsed.utcoffset() == timedelta(0) and parsed.microsecond == 0,
        f"{field} must use whole-second UTC precision: {text!r}",
    )
    return text


def require_nullable_rfc3339_utc(value: object, field: str) -> None:
    if value is not None:
        require_rfc3339_utc(value, field)


def require_public_event_timestamps(event: object, field: str) -> None:
    require(isinstance(event, dict), f"{field} must be an object: {event!r}")
    for timestamp_field in (
        "occurred_at",
        "filed_at",
        "first_observed_at",
        "deadline_at",
    ):
        require(
            timestamp_field in event,
            f"{field}.{timestamp_field} is missing: {event!r}",
        )
        require_nullable_rfc3339_utc(
            event[timestamp_field],
            f"{field}.{timestamp_field}",
        )
    require_rfc3339_utc(event.get("updated_at"), f"{field}.updated_at")


def stable_record_id(connector_id: str, issuer_id: str, external_id: str) -> str:
    seed = f"{connector_id}\x1f{issuer_id}\x1f{external_id}"
    return "globaldoc:" + hashlib.sha256(seed.encode("utf-8")).hexdigest()[:40]


def refresh_record_content_hash(record: dict[str, Any]) -> None:
    """Recompute the cross-runtime hash after a deliberate fixture mutation."""
    semantic_record = GlobalDocumentRecord(
        record_id=record["record_id"],
        external_id=record["external_id"],
        issuer_id=record["issuer_id"],
        issuer_reference=IssuerReference(**record["issuer_reference"]),
        country_code=record["country_code"],
        source_key=record["source_key"],
        source_right_id=record["source_right_id"],
        record_kind=record["record_kind"],
        document_type=record["document_type"],
        event_family=record["event_family"],
        title=record["title"],
        original_language=record["original_language"],
        filed_at=record["filed_at"],
        first_observed_at=record["first_observed_at"],
        original_url=record["original_url"],
        content_hash="0" * 64,
        body_text=record["body_text"],
        correction_of_external_id=record["correction_of_external_id"],
        change_type=record["change_type"],
        metadata=record["metadata"],
    )
    record["content_hash"] = global_document_content_hash(
        semantic_record,
        source_type="official_disclosure",
        public_allowed=True,
        ai_allowed=True,
    )


def build_record(
    *,
    title: str,
    content_version: str,
    filed_at: str,
    observed_at: str,
    external_id: str = "0000320193-26-000999",
) -> dict[str, Any]:
    issuer_id = "issuer:us:cik:0000320193"
    canonical_filed_at = (
        datetime.fromisoformat(filed_at.replace("Z", "+00:00"))
        .astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
    )
    record = {
        "record_id": stable_record_id(
            SEC_CONNECTOR_ID,
            issuer_id,
            external_id,
        ),
        "external_id": external_id,
        "issuer_id": issuer_id,
        "issuer_reference": {
            "namespace": "US:CIK",
            "identifier_type": "CIK",
            "value": "0000320193",
            "legal_name": "Apple Inc.",
            "market": "NASDAQ",
            "ticker": "AAPL",
        },
        "country_code": "US",
        "source_key": SEC_SOURCE_KEY,
        "source_right_id": SEC_RIGHT_ID,
        "record_kind": "disclosure",
        "document_type": "SC 13D",
        "event_family": "large_ownership",
        "title": title,
        "original_language": "en",
        "filed_at": canonical_filed_at,
        "first_observed_at": observed_at,
        "original_url": (
            "https://www.sec.gov/Archives/edgar/data/320193/"
            "000032019326000999/ownership.txt"
        ),
        "content_hash": "",
        "body_text": None,
        "correction_of_external_id": None,
        "change_type": "new" if content_version == "v1" else "updated",
        "metadata": {
            "form": "SC 13D",
            "fixture_version": content_version,
            "title_provenance": "source",
            "canonical_fixture": {
                "verified": True,
                "ordinal": 1 if content_version == "v1" else 2,
                "labels": ["공식 근거", "source"],
                "empty_object": {},
            },
        },
    }
    refresh_record_content_hash(record)
    return record


def build_ca_link_record(
    *,
    filed_at: str,
    observed_at: str,
) -> dict[str, Any]:
    canonical_filed_at = (
        datetime.fromisoformat(filed_at.replace("Z", "+00:00"))
        .astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
    )
    issuer_reference = IssuerReference(
        namespace="CA:OFFICIAL",
        identifier_type="SEDAR_ISSUER_ID",
        value="CA0001",
        legal_name="CI Canadian Issuer",
        market="TSX",
        ticker="CICA",
    )
    issuer_id = "issuer:ca:official:ca0001"
    record = GlobalDocumentRecord(
        record_id=stable_record_id(
            "connector:ca:issuer-ir",
            issuer_id,
            "ci-ca-approved-link-1",
        ),
        external_id="ci-ca-approved-link-1",
        issuer_id=issuer_id,
        issuer_reference=issuer_reference,
        country_code="CA",
        source_key="issuer-ir",
        source_right_id="official:ca-issuer-ir",
        record_kind="link",
        document_type="issuer_notice",
        event_family="meeting_and_vote",
        title="CI Canadian issuer meeting notice",
        original_language="en",
        filed_at=canonical_filed_at,
        first_observed_at=observed_at,
        original_url=(
            "https://investors.ci-canadian-issuer.ca/notices/meeting.pdf"
        ),
        content_hash="0" * 64,
        body_text=None,
        metadata={
            "approved_link_only": True,
            "ingest_mode": "manual-metadata",
            "title_provenance": "operator_metadata",
            "official_host": "investors.ci-canadian-issuer.ca",
            "host_provenance_evidence_sha256": "e" * 64,
            "source_url_requested": False,
        },
    )
    record = replace(
        record,
        content_hash=global_document_content_hash(
            record,
            source_type="official_issuer",
            public_allowed=True,
            ai_allowed=False,
        ),
    )
    return record.public_payload(allow_body=True)


def ingest_payload(
    *,
    rights_revision: str,
    idempotency_key: str,
    record: dict[str, Any],
    retrieved_at: str,
) -> dict[str, Any]:
    return {
        "idempotency_key": idempotency_key,
        "code_revision": CODE_REVISION,
        "envelope": {
            "schema_version": 1,
            "connector_id": SEC_CONNECTOR_ID,
            "country_code": "US",
            "source_right_id": SEC_RIGHT_ID,
            "rights_revision": rights_revision,
            "retrieved_at": retrieved_at,
            "coverage_mode": "market-wide",
            "records": [record],
            "next_cursor": None,
            "exhausted": True,
            "request_count": 1,
            "raw_count": 1,
            "public_allowed": True,
            "ai_allowed": True,
            "lifecycle_observations": [],
            "chunk": {
                "index": 1,
                "count": 1,
                "batch_raw_count": 1,
                "batch_acknowledged_count": 1,
                "batch_request_count": 1,
                "batch_id": "global-batch:"
                + hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest(),
                "window_start": "2026-07-23",
                "window_end_exclusive": "2026-07-24",
            },
        },
    }


def bind_classified_ingest_key(
    payload: dict[str, Any],
    *,
    namespace: str,
) -> str:
    envelope = json.loads(json.dumps(payload["envelope"], ensure_ascii=False))
    chunk = envelope.pop("chunk")
    envelope.pop("retrieved_at", None)
    envelope.pop("request_count", None)
    for record in envelope.get("records", []):
        record.pop("first_observed_at", None)
    semantic = {
        "code_revision": payload["code_revision"],
        "window_start": chunk["window_start"],
        "window_end_exclusive": chunk["window_end_exclusive"],
        "chunk_index": chunk["index"] - 1,
        "envelope": envelope,
    }
    digest = hashlib.sha256(
        json.dumps(
            semantic,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
        .encode("utf-8")
    ).hexdigest()
    key = f"{namespace}:us:{digest}"
    payload["idempotency_key"] = key
    return key


def empty_chunk_payload(
    *,
    rights_revision: str,
    idempotency_key: str,
    retrieved_at: str,
    batch_id: str,
    index: int,
    count: int,
    batch_raw_count: int = 0,
    code_revision: str = CODE_REVISION,
) -> dict[str, Any]:
    is_final = index == count
    return {
        "idempotency_key": idempotency_key,
        "code_revision": code_revision,
        "envelope": {
            "schema_version": 1,
            "connector_id": SEC_CONNECTOR_ID,
            "country_code": "US",
            "source_right_id": SEC_RIGHT_ID,
            "rights_revision": rights_revision,
            "retrieved_at": retrieved_at,
            "coverage_mode": "market-wide",
            "records": [],
            "next_cursor": (None if is_final else f"ci-chunk:{batch_id}:{index}"),
            "exhausted": is_final,
            "request_count": 0,
            "raw_count": 0,
            "public_allowed": True,
            "ai_allowed": True,
            "lifecycle_observations": [],
            "chunk": {
                "index": index,
                "count": count,
                "batch_raw_count": batch_raw_count,
                "batch_acknowledged_count": 0,
                "batch_request_count": 0,
                "batch_id": batch_id,
                "window_start": "2026-07-23",
                "window_end_exclusive": "2026-07-24",
            },
        },
    }


def attach_single_chunk_lifecycle_observations(
    payload: dict[str, Any],
    observations: list[dict[str, Any]],
) -> None:
    envelope = payload["envelope"]
    chunk = envelope["chunk"]
    require(
        chunk["index"] == 1
        and chunk["count"] == 1
        and isinstance(envelope["records"], list),
        repr(payload),
    )
    envelope["lifecycle_observations"] = observations
    accepted_count = len(envelope["records"]) + len(observations)
    envelope["raw_count"] = accepted_count
    chunk["batch_raw_count"] = accepted_count
    chunk["batch_acknowledged_count"] = accepted_count


def transition(
    base_url: str,
    target: str,
    expected_version: int,
    reason: str,
) -> dict[str, Any]:
    payload, _ = request_json(
        base_url,
        "api.php/api/v2/admin/release-state",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "release_state": target,
            "expected_version": expected_version,
            "reason": reason,
        },
    )
    require(payload.get("api_version") == "v2", repr(payload))
    data = payload.get("data")
    require(isinstance(data, dict), repr(payload))
    return data


def transition_v1(
    base_url: str,
    target: str,
    expected_version: int,
    reason: str,
) -> dict[str, Any]:
    payload, _ = request_json(
        base_url,
        "api.php/api/v1/admin/release-state",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "release_state": target,
            "expected_version": expected_version,
            "reason": reason,
        },
    )
    require(payload.get("api_version") == "v1", repr(payload))
    return payload


def issue_release_authorization(
    base_url: str,
    *,
    nonce: str,
    expected_v1_version: int,
    expected_v2_version: int,
    expires_at: str,
) -> dict[str, Any]:
    payload, _ = request_json(
        base_url,
        "api.php/api/v2/admin/release-authorizations",
        method="POST",
        token=RELEASE_AUTHORIZER_TOKEN,
        payload={
            "candidate_sha": CODE_REVISION,
            "evidence_artifact_digest": EVIDENCE_ARTIFACT_DIGEST,
            "evidence_run_id": 101,
            "evidence_artifact_id": 202,
            "release_nonce": nonce,
            "expected_v1_state_version": expected_v1_version,
            "expected_v2_state_version": expected_v2_version,
            "expires_at": expires_at,
            "reason": "CI protected release workflow authorizes this exact evidence.",
        },
        expected_status=201,
    )
    require(payload.get("api_version") == "v2", repr(payload))
    data = payload.get("data")
    require(isinstance(data, dict), repr(payload))
    return data


def atomic_cutover(
    base_url: str,
    *,
    nonce: str,
    expected_v1_version: int,
    expected_v2_version: int,
    candidate_sha: str = CODE_REVISION,
    evidence_digest: str = EVIDENCE_ARTIFACT_DIGEST,
    expected_status: int = 200,
) -> dict[str, Any]:
    payload, _ = request_json(
        base_url,
        "api.php/api/v2/admin/cutover",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "candidate_sha": candidate_sha,
            "evidence_artifact_digest": evidence_digest,
            "release_nonce": nonce,
            "expected_v1_state_version": expected_v1_version,
            "expected_v2_state_version": expected_v2_version,
            "reason": "CI atomically activates both protected API release states.",
        },
        expected_status=expected_status,
    )
    require(payload.get("api_version") == "v2", repr(payload))
    return payload


def activate_sec_source_right(mysql_container_id: str) -> None:
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights "
        "SET permission_scope='CI-approved SEC filing metadata and source links only',"
        "evidence_uri='https://www.sec.gov/search-filings/edgar-application-programming-interfaces',"
        "evidence_hash=NULL,valid_from='2009-01-01 00:00:00',valid_until=NULL,"
        "revoked_at=NULL,ai_allowed=1,redistribution_allowed=1,status='active',"
        "updated_at=UTC_TIMESTAMP() "
        "WHERE source_right_id='official:sec-edgar';",
    )


def activate_exact_dart_source_right(mysql_container_id: str) -> None:
    source = DART_CONTRACT_FIXTURE["source_right"]

    def sql_value(value: object) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        return "'" + str(value).replace("'", "''") + "'"

    mysql_execute(
        mysql_container_id,
        "INSERT INTO ci_source_rights "
        "(source_right_id,source_type,source_key,source_name,permission_scope,"
        "evidence_uri,evidence_hash,valid_from,valid_until,revoked_at,ai_allowed,"
        "redistribution_allowed,status,notes,created_at,updated_at) VALUES ("
        + ",".join(
            (
                sql_value(source["source_right_id"]),
                sql_value(source["source_type"]),
                sql_value(source["source_key"]),
                sql_value(source["source_name"]),
                sql_value(source["permission_scope"]),
                sql_value(source["evidence_uri"]),
                sql_value(source["evidence_hash"]),
                "'2009-01-01 00:00:00'",
                sql_value(source["valid_until"]),
                sql_value(source["revoked_at"]),
                sql_value(source["ai_allowed"]),
                sql_value(source["redistribution_allowed"]),
                sql_value(source["status"]),
                "'cross-runtime source-right-contract-v1 fixture'",
                "UTC_TIMESTAMP()",
                "UTC_TIMESTAMP()",
            )
        )
        + ") ON DUPLICATE KEY UPDATE "
        "source_type=VALUES(source_type),source_key=VALUES(source_key),"
        "source_name=VALUES(source_name),permission_scope=VALUES(permission_scope),"
        "evidence_uri=VALUES(evidence_uri),evidence_hash=VALUES(evidence_hash),"
        "valid_from=VALUES(valid_from),valid_until=VALUES(valid_until),"
        "revoked_at=VALUES(revoked_at),ai_allowed=VALUES(ai_allowed),"
        "redistribution_allowed=VALUES(redistribution_allowed),"
        "status=VALUES(status),updated_at=UTC_TIMESTAMP();",
    )


def activate_required_alpha_cutover_sources(mysql_container_id: str) -> None:
    mysql_execute(
        mysql_container_id,
        "INSERT INTO ci_source_rights "
        "(source_right_id,source_type,source_key,source_name,permission_scope,"
        "evidence_uri,evidence_hash,valid_from,valid_until,revoked_at,ai_allowed,"
        "redistribution_allowed,status,notes,created_at,updated_at) VALUES "
        "('official:dart','official_disclosure','dart','OpenDART',"
        "'CI-approved OpenDART collection and public link redistribution',"
        "'https://opendart.fss.or.kr/',NULL,'2009-01-01 00:00:00',NULL,NULL,"
        "1,1,'active','CI required source fixture',UTC_TIMESTAMP(),UTC_TIMESTAMP()) "
        "ON DUPLICATE KEY UPDATE source_right_id=source_right_id;"
        "UPDATE ci_source_rights SET "
        "permission_scope=CONCAT('CI-approved collection and public redistribution: ',"
        "source_right_id),"
        "evidence_uri=CASE source_right_id "
        "WHEN 'official:dart' THEN 'https://opendart.fss.or.kr/' "
        "WHEN 'official:sec-edgar' THEN "
        "'https://www.sec.gov/search-filings/edgar-application-programming-interfaces' "
        "WHEN 'official:edinet' THEN "
        "'https://disclosure2.edinet-fsa.go.jp/guide/static/disclosure/WZEK0090.html' "
        "WHEN 'official:companies-house' THEN "
        "'https://developer.company-information.service.gov.uk/' "
        "WHEN 'official:ca-issuer-ir' THEN "
        "'https://example.invalid/ci-ca-issuer-right-evidence' "
        "WHEN 'official:asic-register' THEN "
        "'https://www.asic.gov.au/online-services/search-asic-registers/' END,"
        "evidence_hash=NULL,valid_from='2009-01-01 00:00:00',valid_until=NULL,"
        "revoked_at=NULL,ai_allowed=1,redistribution_allowed=1,status='active',"
        "updated_at=UTC_TIMESTAMP() WHERE source_right_id IN ("
        + ",".join(
            f"'{value}'"
            for value in REQUIRED_ALPHA_RIGHT_IDS
            if value not in {SEC_RIGHT_ID, "official:dart"}
        )
        + ");"
        "UPDATE ci_source_connectors SET connector_status='active',"
        "last_checked_at=UTC_TIMESTAMP(),last_success_at=UTC_TIMESTAMP(),"
        "last_observed_at=UTC_TIMESTAMP(),"
        "last_raw_count=CASE WHEN coverage_mode='link-only' THEN 1 ELSE 0 END,"
        "last_acknowledged_count=CASE WHEN coverage_mode='link-only' THEN 1 ELSE 0 END,"
        "last_error_class=NULL,updated_at=UTC_TIMESTAMP() WHERE connector_id IN ("
        "'connector:kr:dart','connector:us:sec-edgar','connector:ca:issuer-ir',"
        "'connector:au:asic-register');",
    )
    set_sec_current_cursor(mysql_container_id, datetime.now(timezone.utc))


def require_cutover_not_consumed(
    mysql_container_id: str,
    authorization_id: str,
) -> None:
    row = mysql_execute(
        mysql_container_id,
        "SELECT CONCAT("
        "(SELECT release_state FROM ci_governance_release_state "
        "WHERE state_key='governance_v1'),'|',"
        "(SELECT release_state FROM ci_governance_release_state "
        "WHERE state_key='global_terminal_v2'),'|',"
        "(SELECT fully_consumed_at IS NULL FROM ci_release_authorizations "
        f"WHERE authorization_id='{authorization_id}'));",
    )
    require(row == "preview|preview|1", row)


def backfill_job_fingerprint(job: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(
            job,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def replace_dart_checkpoint_fixture(
    mysql_container_id: str,
    *,
    checkpoint: dict[str, Any],
    row_fingerprint: str,
    now: datetime,
) -> None:
    checkpoint_json = json.dumps(
        checkpoint,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    payload_hash = hashlib.sha256(checkpoint_json.encode()).hexdigest()
    escaped_checkpoint = checkpoint_json.replace("\\", "\\\\").replace("'", "''")
    mysql_execute(
        mysql_container_id,
        "DELETE FROM ci_official_backfill_checkpoints;"
        "INSERT INTO ci_official_backfill_checkpoints "
        "(job_fingerprint,checkpoint_version,checkpoint_json,payload_hash,"
        "updated_by,created_at,updated_at) VALUES "
        f"('{row_fingerprint}',31,'{escaped_checkpoint}','{payload_hash}',"
        f"'ci-alpha-evidence','{now.strftime('%Y-%m-%d %H:%M:%S')}',"
        f"'{now.strftime('%Y-%m-%d %H:%M:%S')}');",
    )


def seed_alpha_automated_evidence(
    mysql_container_id: str,
    *,
    now: datetime,
) -> dict[str, Any]:
    end = now.date()
    start = end - timedelta(days=30)
    receipt_values: list[str] = []
    connector_ids = ("connector:us:sec-edgar",)
    for connector_id in connector_ids:
        cursor = start
        for index in range(30):
            next_cursor = cursor + timedelta(days=1)
            identity = f"{connector_id}:{cursor.isoformat()}:{CODE_REVISION}"
            digest = hashlib.sha256(identity.encode()).hexdigest()
            batch_id = "global-batch:" + digest
            ingest_id = "alpha:" + digest[:80]
            idempotency_key = "global-ingest-v2-day:us:" + digest
            completed = now.strftime("%Y-%m-%d %H:%M:%S")
            if connector_id == "connector:us:sec-edgar" and index == 0:
                for (
                    chunk_index,
                    request_count,
                    raw_count,
                    acknowledged_count,
                    batch_request_count,
                ) in (
                    (1, 0, 1, 0, 1),
                    (2, 1, 2, 1, 1),
                ):
                    chunk_digest = hashlib.sha256(
                        f"{identity}:chunk:{chunk_index}".encode()
                    ).hexdigest()
                    receipt_values.append(
                        "("
                        + ",".join(
                            (
                                f"'alpha:{chunk_digest[:80]}'",
                                f"'{connector_id}'",
                                f"'global-ingest-v2-day:us:{chunk_digest}'",
                                f"'{chunk_digest}'",
                                f"'{batch_id}'",
                                str(chunk_index),
                                "2",
                                f"'{cursor.isoformat()}'",
                                f"'{next_cursor.isoformat()}'",
                                str(request_count),
                                str(raw_count),
                                str(acknowledged_count),
                                "3",
                                "1",
                                str(batch_request_count),
                                f"'{CODE_REVISION}'",
                                f"'{completed}'",
                                f"'{completed}'",
                                f"'{completed}'",
                            )
                        )
                        + ")"
                    )
                cursor = next_cursor
                continue
            receipt_values.append(
                "("
                + ",".join(
                    (
                        f"'{ingest_id}'",
                        f"'{connector_id}'",
                        f"'{idempotency_key}'",
                        f"'{digest}'",
                        f"'{batch_id}'",
                        "1",
                        "1",
                        f"'{cursor.isoformat()}'",
                        f"'{next_cursor.isoformat()}'",
                        "1",
                        "3",
                        "1",
                        "3",
                        "1",
                        "1",
                        f"'{CODE_REVISION}'",
                        f"'{completed}'",
                        f"'{completed}'",
                        f"'{completed}'",
                    )
                )
                + ")"
            )
            cursor = next_cursor
    hybrid_digest = hashlib.sha256(
        f"hybrid:{CODE_REVISION}".encode()
    ).hexdigest()
    hybrid_completed = now.strftime("%Y-%m-%d %H:%M:%S")
    receipt_values.append(
        "("
        + ",".join(
            (
                f"'alpha-hybrid:{hybrid_digest}'",
                f"'{SEC_CONNECTOR_ID}'",
                f"'global-ingest-v2-current:us:{hybrid_digest}'",
                f"'{hybrid_digest}'",
                f"'global-batch:{hybrid_digest}'",
                "1",
                "1",
                f"'{(end - timedelta(days=2)).isoformat()}'",
                f"'{end.isoformat()}'",
                "2",
                "4",
                "1",
                "4",
                "1",
                "2",
                f"'{CODE_REVISION}'",
                f"'{hybrid_completed}'",
                f"'{hybrid_completed}'",
                f"'{hybrid_completed}'",
            )
        )
        + ")"
    )
    mysql_execute(mysql_container_id, "DELETE FROM ci_global_ingest_receipts;")
    mysql_execute(
        mysql_container_id,
        "INSERT INTO ci_global_ingest_receipts "
        "(ingest_id,connector_id,idempotency_key,payload_sha256,batch_id,"
        "chunk_index,chunk_count,window_start,window_end_exclusive,request_count,"
        "raw_count,acknowledged_count,batch_raw_count,batch_acknowledged_count,"
        "batch_request_count,code_revision,started_at,completed_at,created_at) VALUES "
        + ",".join(receipt_values)
        + ";",
    )

    job = {
        "range_start": start.isoformat(),
        "range_end_exclusive": end.isoformat(),
        "chunk_days": 1,
        "sources": ["dart"],
        "page_count": 100,
        "max_pages": 100,
        "sync_company_master": False,
        "code_revision": CODE_REVISION,
    }
    fingerprint = backfill_job_fingerprint(job)
    completed_windows: dict[str, object] = {}
    cursor = start
    for index in range(30):
        next_cursor = cursor + timedelta(days=1)
        key = f"{cursor.isoformat()}:{next_cursor.isoformat()}"
        accepted = 0 if index == 0 else 1
        fetched = 0 if index == 0 else 3
        idempotency_digest = hashlib.sha256(
            f"{fingerprint}|{key}".encode("utf-8")
        ).hexdigest()[:32]
        completed_windows[key] = {
            "window_start": cursor.isoformat(),
            "window_end_exclusive": next_cursor.isoformat(),
            "idempotency_key": f"official-backfill-v1:{idempotency_digest}",
            "attempt": 1,
            "code_revision": CODE_REVISION,
            "status": "succeeded",
            "summary": {
                "official_failed": 0,
                "official_skipped": 0,
                "official_remote_ack_mismatches": 0,
                "official_remote_run_persisted": 1,
                "official_remote_raw_count": accepted,
                "official_remote_ack_count": accepted,
                "official_remote_failed": 0,
                "official_remote_skipped": 0,
                "official_remote_synced": 1,
                "official_dart_requests": 9,
                "official_dart_fetched": fetched,
                "official_dart_accepted": accepted,
                "official_dart_errors": 0,
                "official_dart_quota_exhausted": 0,
            },
        }
        cursor = next_cursor
    checkpoint = {
        "schema_version": 1,
        "job": {**job, "fingerprint": fingerprint},
        "created_at": utc_text(now - timedelta(days=30)),
        "updated_at": utc_text(now),
        "company_master_synced": False,
        "dart_quota_blocked_until": None,
        "completed_windows": completed_windows,
        "failed_windows": {},
    }
    replace_dart_checkpoint_fixture(
        mysql_container_id,
        checkpoint=checkpoint,
        row_fingerprint=fingerprint,
        now=now,
    )
    return checkpoint


def attach_public_and_telegram_evidence(
    mysql_container_id: str,
    event_id: str,
) -> None:
    alternate_hash = hashlib.sha256(b"alternate official evidence").hexdigest()
    telegram_hash = hashlib.sha256(b"telegram internal signal").hexdigest()
    mysql_execute(
        mysql_container_id,
        "INSERT INTO ci_source_rights "
        "(source_right_id,source_type,source_key,source_name,permission_scope,"
        "evidence_uri,evidence_hash,valid_from,valid_until,revoked_at,ai_allowed,"
        "redistribution_allowed,status,notes,created_at,updated_at) VALUES "
        f"('{ALTERNATE_RIGHT_ID}','official_disclosure','sec-ci-alternate',"
        "'CI alternate SEC evidence','CI fixture redistribution grant',"
        "'https://www.sec.gov/',NULL,'2009-01-01 00:00:00',NULL,NULL,1,1,"
        "'active','CI fixture',UTC_TIMESTAMP(),UTC_TIMESTAMP()),"
        f"('{TELEGRAM_RIGHT_ID}','licensed_telegram','ci-authorized',"
        "'CI authorized Telegram','CI fixture internal signal grant',"
        "'https://example.invalid/telegram-right',NULL,'2009-01-01 00:00:00',"
        "NULL,NULL,1,1,'active','Must remain private in v2',UTC_TIMESTAMP(),"
        "UTC_TIMESTAMP());"
        "INSERT INTO ci_documents "
        "(document_id,company_id,issuer_id,country_code,source_right_id,"
        "source_class,source_key,external_id,document_type,original_language,"
        "title,body_text,original_url,content_hash,collection_key,"
        "correction_of_document_id,version_no,published_at,filed_at,retrieved_at,"
        "verification_status,publication_status,payload_json,created_at,updated_at)"
        " VALUES "
        f"('ci-doc-alternate',NULL,'issuer:us:cik:0000320193','US',"
        f"'{ALTERNATE_RIGHT_ID}','official_disclosure','sec-ci-alternate',"
        "'ci-alternate-0001','SC 13D','en','CI alternate official filing',NULL,"
        f"'{ALTERNATE_URL}','{alternate_hash}',NULL,NULL,1,UTC_TIMESTAMP(),"
        "UTC_TIMESTAMP(),UTC_TIMESTAMP(),'official','published',NULL,"
        "UTC_TIMESTAMP(),UTC_TIMESTAMP()),"
        f"('ci-doc-telegram',NULL,'issuer:us:cik:0000320193','US',"
        f"'{TELEGRAM_RIGHT_ID}','licensed_telegram','ci-authorized',"
        "'ci-telegram-0001','telegram_message','ko','CI Telegram signal',NULL,"
        f"'{TELEGRAM_URL}','{telegram_hash}',NULL,NULL,1,UTC_TIMESTAMP(),"
        "UTC_TIMESTAMP(),UTC_TIMESTAMP(),'confirmed','published',NULL,"
        "UTC_TIMESTAMP(),UTC_TIMESTAMP());"
        "INSERT INTO ci_event_documents "
        "(event_id,document_id,relation_type,position_no,created_at) VALUES "
        f"('{event_id}','ci-doc-telegram','evidence',-20,UTC_TIMESTAMP()),"
        f"('{event_id}','ci-doc-alternate','evidence',99,UTC_TIMESTAMP());",
    )


def add_byte_pagination_fixture_events(
    mysql_container_id: str,
    source_event_id: str,
    count: int = 100,
) -> set[str]:
    columns = [
        line.split("\t", 1)[0]
        for line in mysql_execute(
            mysql_container_id,
            "SHOW COLUMNS FROM ci_governance_events;",
        ).splitlines()
    ]
    require(columns and "event_id" in columns, repr(columns))
    expressions: list[str] = []
    for column in columns:
        quoted = f"`{column}`"
        if column == "event_id":
            expressions.append("CONCAT('ci-page:',LPAD(page_seq.n,3,'0'))")
        elif column == "title":
            expressions.append(
                "CONCAT('CI pagination ',LPAD(page_seq.n,3,'0'),' ',REPEAT('T',650))"
            )
        elif column == "summary":
            expressions.append("REPEAT('S',2000)")
        elif column in {"occurred_at", "updated_at"}:
            expressions.append(
                f"DATE_SUB(source_event.{quoted},INTERVAL page_seq.n SECOND)"
            )
        elif column in {"collection_key", "comparison_key"}:
            expressions.append("NULL")
        else:
            expressions.append(f"source_event.{quoted}")
    column_sql = ",".join(f"`{column}`" for column in columns)
    expression_sql = ",".join(expressions)
    mysql_execute(
        mysql_container_id,
        f"INSERT INTO ci_governance_events ({column_sql}) "
        "WITH RECURSIVE page_seq(n) AS ("
        "SELECT 1 UNION ALL SELECT n+1 FROM page_seq "
        f"WHERE n<{count}) "
        f"SELECT {expression_sql} FROM ci_governance_events source_event "
        f"CROSS JOIN page_seq WHERE source_event.event_id='{source_event_id}';"
        "INSERT INTO ci_event_documents "
        "(event_id,document_id,relation_type,position_no,created_at) "
        "SELECT event_id,'ci-doc-alternate','evidence',0,UTC_TIMESTAMP() "
        "FROM ci_governance_events WHERE event_id LIKE 'ci-page:%';",
    )
    return {f"ci-page:{index:03d}" for index in range(1, count + 1)}


def remove_byte_pagination_fixture_events(mysql_container_id: str) -> None:
    mysql_execute(
        mysql_container_id,
        "DELETE FROM ci_event_documents WHERE event_id LIKE 'ci-page:%';"
        "DELETE FROM ci_governance_events WHERE event_id LIKE 'ci-page:%';",
    )


def run(base_url: str, mysql_container_id: str) -> None:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    canonical_fixture = build_record(
        title="CI canonical fixture – 원문",
        content_version="v1",
        filed_at="2026-07-23T20:00:00+00:00",
        observed_at="2026-07-23T20:05:00Z",
    )
    require(
        canonical_fixture["content_hash"]
        == "c529b3a17704ccbb6d517396573fca1129408cb5cee5e6d555d7bb70f1d23cb0",
        repr(canonical_fixture),
    )
    require(
        token_hash(ADMIN_TOKEN)
        == "c8e80d02ecd972e840698ade74adc37d485b9c2077fe5fc1d1fde57f97de0a74",
        "CI admin token does not match tests/php73_config.php",
    )
    require(
        token_hash(EDITOR_TOKEN)
        == "957e0a84dd47002c3a093da30526279c213011fa06c606667b753ebe87f1c92b",
        "CI editor token does not match tests/php73_config.php",
    )
    require(
        token_hash(OPS_TOKEN)
        == "27bc3fddd68fd0f3a042dae1dd472d0d3d5b615c8a86e93473375c4fe21eeae2",
        "CI ops token does not match tests/php73_config.php",
    )
    require(
        token_hash(RELEASE_AUTHORIZER_TOKEN)
        == "83a00f2797d3a214080e86809cb2eba45e0163581c1612ee7699055fa109ecb7",
        "CI release token does not match tests/php73_config.php",
    )

    # Migration 011 must leave v1 functional and v2 independently closed.
    legacy_admin, _ = request_json(
        base_url,
        "api.php/api/v1/admin/release-state",
        token=ADMIN_TOKEN,
    )
    require(
        legacy_admin.get("api_version") == "v1"
        and legacy_admin.get("release_state") == "closed"
        and isinstance(legacy_admin.get("state_version"), int)
        and legacy_admin.get("state_version") >= 0,
        repr(legacy_admin),
    )
    v1_initial_version = int(legacy_admin["state_version"])
    v1_preview_version = v1_initial_version + 1
    v1_live_version = v1_preview_version + 1
    v1_closed_version = v1_live_version + 1
    v1_reopened_version = v1_closed_version + 1
    v1_final_closed_version = v1_reopened_version + 1

    closed, _ = request_json(
        base_url,
        "api.php/api/v2/events",
        expected_status=503,
    )
    require(
        closed.get("error") == "global_terminal_release_closed",
        repr(closed),
    )
    missing_admin, _ = request_json(
        base_url,
        "api.php/api/v2/admin/release-state",
        expected_status=401,
    )
    require(missing_admin.get("error") == "bearer_token_required", repr(missing_admin))
    wrong_admin, _ = request_json(
        base_url,
        "api.php/api/v2/admin/release-state",
        token=OPS_TOKEN,
        expected_status=403,
    )
    require(wrong_admin.get("error") == "insufficient_role", repr(wrong_admin))
    initial_state, _ = request_json(
        base_url,
        "api.php/api/v2/admin/release-state",
        token=ADMIN_TOKEN,
    )
    require(
        initial_state.get("data", {}).get("release_state") == "closed"
        and initial_state.get("data", {}).get("state_version") == 0,
        repr(initial_state),
    )
    ops_release_state, _ = request_json(
        base_url,
        "api.php/api/v2/ops/release-state",
        token=OPS_TOKEN,
    )
    require(
        ops_release_state.get("data", {}).get("release_state") == "closed"
        and ops_release_state.get("data", {}).get("state_version") == 0,
        repr(ops_release_state),
    )
    initial_state_data = initial_state["data"]
    require_nullable_rfc3339_utc(
        initial_state_data.get("cutover_at"),
        "initial_release_state.cutover_at",
    )
    require_nullable_rfc3339_utc(
        initial_state_data.get("sunset_at"),
        "initial_release_state.sunset_at",
    )
    require_rfc3339_utc(
        initial_state_data.get("updated_at"),
        "initial_release_state.updated_at",
    )
    unknown_release_field, _ = request_json(
        base_url,
        "api.php/api/v2/admin/release-state",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "release_state": "preview",
            "expected_version": 0,
            "reason": "CI rejects misspelled release-state fields.",
            "cutvoer_at": utc_text(now),
        },
        expected_status=400,
    )
    require(
        unknown_release_field.get("error") == "unknown_release_state_field",
        repr(unknown_release_field),
    )
    unchanged_state, _ = request_json(
        base_url,
        "api.php/api/v2/admin/release-state",
        token=ADMIN_TOKEN,
    )
    require(
        unchanged_state.get("data", {}).get("release_state") == "closed"
        and unchanged_state.get("data", {}).get("state_version") == 0,
        repr(unchanged_state),
    )
    wrong_review, _ = request_json(
        base_url,
        "api.php/api/v2/admin/review-queue",
        token=OPS_TOKEN,
        expected_status=403,
    )
    require(wrong_review.get("error") == "insufficient_role", repr(wrong_review))
    wrong_eligibility, _ = request_json(
        base_url,
        (
            "api.php/api/v2/ops/source-right-eligibility?"
            + urllib.parse.urlencode(
                {"source_right_id": SEC_RIGHT_ID, "use": "collect"}
            )
        ),
        token=EDITOR_TOKEN,
        expected_status=403,
    )
    require(
        wrong_eligibility.get("error") == "insufficient_role",
        repr(wrong_eligibility),
    )

    activate_exact_dart_source_right(mysql_container_id)
    dart_eligibility, _ = request_json(
        base_url,
        (
            "api.php/api/v2/ops/source-right-eligibility?"
            + urllib.parse.urlencode(
                {"source_right_id": "official:dart", "use": "collect"}
            )
        ),
        token=OPS_TOKEN,
    )
    require(
        dart_eligibility.get("eligible") is True
        and dart_eligibility.get("rights_revision")
        and dart_eligibility.get("contract_revision")
        == DART_CONTRACT_FIXTURE["expected_revision"]
        and dart_eligibility.get("ai_allowed") is False
        and dart_eligibility.get("redistribution_allowed") is True,
        repr(dart_eligibility),
    )

    activate_sec_source_right(mysql_container_id)
    identity_mutation, _ = request_json(
        base_url,
        "api.php/api/v1/admin/source-rights",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "source_right_id": SEC_RIGHT_ID,
            "source_type": "official_disclosure",
            "source_key": "sec-edgar-reassigned",
            "source_name": "SEC EDGAR reassigned identity",
            "permission_scope": (
                "CI must not move an existing grant to another source identity."
            ),
            "evidence_uri": (
                "https://www.sec.gov/search-filings/"
                "edgar-application-programming-interfaces"
            ),
            "evidence_hash": None,
            "valid_from": "2009-01-01T00:00:00Z",
            "valid_until": None,
            "revoked_at": None,
            "ai_allowed": True,
            "redistribution_allowed": True,
            "status": "active",
        },
        expected_status=409,
    )
    require(
        identity_mutation.get("error") == "source_right_identity_immutable"
        and identity_mutation.get("existing_source_type") == "official_disclosure"
        and identity_mutation.get("existing_source_key") == SEC_SOURCE_KEY
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT(source_type,':',source_key) FROM ci_source_rights "
            f"WHERE source_right_id='{SEC_RIGHT_ID}';",
        )
        == f"official_disclosure:{SEC_SOURCE_KEY}",
        repr(identity_mutation),
    )

    # A bootstrap that preflighted a pending grant cannot revive it after a
    # concurrent revocation. The status/version comparison happens under the
    # same SourceRight row lock as the write.
    edinet_preflight_updated_at = mysql_execute(
        mysql_container_id,
        "SELECT updated_at FROM ci_source_rights "
        "WHERE source_right_id='official:edinet';",
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET status='revoked',"
        "revoked_at=UTC_TIMESTAMP(),updated_at=UTC_TIMESTAMP() "
        "WHERE source_right_id='official:edinet';",
    )
    stale_right_activation, _ = request_json(
        base_url,
        "api.php/api/v1/admin/source-rights",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "source_right_id": "official:edinet",
            "source_type": "official_disclosure",
            "source_key": "edinet",
            "source_name": "EDINET",
            "permission_scope": "CI metadata-only EDINET fixture",
            "evidence_uri": (
                "https://disclosure2.edinet-fsa.go.jp/guide/static/"
                "disclosure/WZEK0090.html"
            ),
            "evidence_hash": None,
            "valid_from": "2013-09-17T00:00:00Z",
            "valid_until": None,
            "revoked_at": None,
            "ai_allowed": False,
            "redistribution_allowed": True,
            "status": "active",
            "expected_status": "pending",
            "expected_updated_at": edinet_preflight_updated_at,
        },
        expected_status=409,
    )
    require(
        stale_right_activation.get("error") == "stale_source_right"
        and mysql_execute(
            mysql_container_id,
            "SELECT status FROM ci_source_rights "
            "WHERE source_right_id='official:edinet';",
        )
        == "revoked",
        repr(stale_right_activation),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET status='pending',revoked_at=NULL,"
        "updated_at=UTC_TIMESTAMP() "
        "WHERE source_right_id='official:edinet';",
    )

    connector_admin_denied, _ = request_json(
        base_url,
        "api.php/api/v2/admin/connectors",
        token=OPS_TOKEN,
        expected_status=403,
    )
    require(
        connector_admin_denied.get("error") == "insufficient_role",
        repr(connector_admin_denied),
    )
    connectors, _ = request_json(
        base_url,
        "api.php/api/v2/admin/connectors",
        token=ADMIN_TOKEN,
    )
    connector_items = connectors.get("data", {}).get("items", [])
    sec_connector = next(
        (
            item
            for item in connector_items
            if item.get("connector_id") == SEC_CONNECTOR_ID
        ),
        None,
    )
    require(
        isinstance(sec_connector, dict)
        and sec_connector.get("connector_status") == "pending_rights"
        and sec_connector.get("collect_eligibility", {}).get("eligible") is True
        and sec_connector.get("collect_eligibility", {}).get("identity_match") is True,
        repr(connectors),
    )
    gb_connector = next(
        (
            item
            for item in connector_items
            if item.get("connector_id") == "connector:gb:companies-house"
        ),
        None,
    )
    require(isinstance(gb_connector, dict), repr(connectors))
    unapproved_configured, _ = request_json(
        base_url,
        "api.php/api/v2/admin/connectors/connector:gb:companies-house",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "target_status": "configured",
            "expected_updated_at": gb_connector["updated_at"],
            "reason": "CI must reject an unapproved source-right activation.",
        },
        expected_status=409,
    )
    require(
        unapproved_configured.get("error")
        == "connector_disabled_by_alpha_policy",
        repr(unapproved_configured),
    )
    inactive_without_rights, _ = request_json(
        base_url,
        "api.php/api/v2/admin/connectors/connector:gb:companies-house",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "target_status": "inactive",
            "expected_updated_at": gb_connector["updated_at"],
            "reason": "CI explicitly keeps the unapproved connector inactive.",
        },
    )
    require(
        inactive_without_rights.get("data", {}).get("connector_status") == "inactive"
        and inactive_without_rights.get("data", {}).get("changed") is True,
        repr(inactive_without_rights),
    )
    configured, _ = request_json(
        base_url,
        f"api.php/api/v2/admin/connectors/{SEC_CONNECTOR_ID}",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "target_status": "configured",
            "expected_updated_at": sec_connector["updated_at"],
            "reason": "CI approves exact SEC source-right identity.",
        },
    )
    require(
        configured.get("data", {}).get("connector_status") == "configured"
        and configured.get("data", {}).get("previous_status") == "pending_rights"
        and configured.get("data", {}).get("changed") is True
        and str(configured.get("data", {}).get("audit_id", "")).startswith(
            "connector-audit:"
        ),
        repr(configured),
    )
    stale_connector, _ = request_json(
        base_url,
        f"api.php/api/v2/admin/connectors/{SEC_CONNECTOR_ID}",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "target_status": "inactive",
            "expected_updated_at": sec_connector["updated_at"],
            "reason": "CI deliberately submits a stale status update.",
        },
        expected_status=409,
    )
    require(
        stale_connector.get("error") == "stale_connector_update",
        repr(stale_connector),
    )
    connector_detail, _ = request_json(
        base_url,
        f"api.php/api/v2/admin/connectors/{SEC_CONNECTOR_ID}",
        token=ADMIN_TOKEN,
    )
    require(
        connector_detail.get("data", {}).get("connector", {}).get("connector_status")
        == "configured"
        and len(connector_detail.get("data", {}).get("audit_log", [])) == 1,
        repr(connector_detail),
    )
    eligibility, _ = request_json(
        base_url,
        (
            "api.php/api/v2/ops/source-right-eligibility?"
            + urllib.parse.urlencode(
                {"source_right_id": SEC_RIGHT_ID, "use": "collect"}
            )
        ),
        token=OPS_TOKEN,
    )
    rights_revision = eligibility.get("rights_revision")
    contract_revision = eligibility.get("contract_revision")
    require(
        eligibility.get("eligible") is True
        and eligibility.get("source_type") == "official_disclosure"
        and eligibility.get("source_key") == SEC_SOURCE_KEY
        and isinstance(rights_revision, str)
        and len(rights_revision) == 64
        and isinstance(contract_revision, str)
        and len(contract_revision) == 64,
        repr(eligibility),
    )

    optional_receipts_before = mysql_execute(
        mysql_container_id,
        "SELECT COUNT(*) FROM ci_global_ingest_receipts;",
    )
    optional_ingest_disabled, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload={
            "idempotency_key": "php73-v2-gb-alpha-disabled",
            "code_revision": CODE_REVISION,
            "envelope": {
                "connector_id": "connector:gb:companies-house",
            },
        },
        expected_status=409,
    )
    require(
        optional_ingest_disabled.get("error")
        == "global_ingest_source_disabled"
        and optional_ingest_disabled.get("connector_id")
        == "connector:gb:companies-house"
        and mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_global_ingest_receipts;",
        )
        == optional_receipts_before,
        repr(optional_ingest_disabled),
    )

    # CA/AU link-only ingestion is bound to the exact human-approved
    # canonical manifest digest stored on the current SourceRight.
    ca_manifest_sha256 = "c" * 64
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET "
        "permission_scope='CI-approved Canadian issuer link metadata',"
        "evidence_uri=NULL,evidence_hash='" + ca_manifest_sha256 + "',"
        "valid_from='2015-01-01 00:00:00',valid_until=NULL,revoked_at=NULL,"
        "ai_allowed=0,redistribution_allowed=1,status='active',"
        "updated_at=UTC_TIMESTAMP() "
        "WHERE source_right_id='official:ca-issuer-ir';"
        "UPDATE ci_source_connectors SET connector_status='configured',"
        "last_error_class=NULL,updated_at=UTC_TIMESTAMP() "
        "WHERE connector_id='connector:ca:issuer-ir';",
    )
    ca_eligibility, _ = request_json(
        base_url,
        (
            "api.php/api/v2/ops/source-right-eligibility?"
            + urllib.parse.urlencode(
                {
                    "source_right_id": "official:ca-issuer-ir",
                    "use": "collect",
                }
            )
        ),
        token=OPS_TOKEN,
    )
    ca_rights_revision = ca_eligibility.get("rights_revision")
    ca_right_updated_at = mysql_execute(
        mysql_container_id,
        "SELECT updated_at FROM ci_source_rights "
        "WHERE source_right_id='official:ca-issuer-ir';",
    )
    require(
        ca_eligibility.get("eligible") is True
        and isinstance(ca_rights_revision, str)
        and len(ca_rights_revision) == 64,
        repr(ca_eligibility),
    )

    def ca_empty_payload(
        *,
        idempotency_key: str,
        manifest_sha256: str | None,
        record: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = empty_chunk_payload(
            rights_revision=ca_rights_revision,
            idempotency_key=idempotency_key,
            retrieved_at=utc_text(now),
            batch_id=(
                "global-batch:"
                + hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()
            ),
            index=1,
            count=1,
        )
        envelope = payload["envelope"]
        envelope.update(
            {
                "connector_id": "connector:ca:issuer-ir",
                "country_code": "CA",
                "source_right_id": "official:ca-issuer-ir",
                "coverage_mode": "link-only",
                "ai_allowed": False,
            }
        )
        if manifest_sha256 is not None:
            envelope["source_manifest_sha256"] = manifest_sha256
        if record is not None:
            envelope["records"] = [record]
            envelope["raw_count"] = 1
            envelope["chunk"]["batch_raw_count"] = 1
            envelope["chunk"]["batch_acknowledged_count"] = 1
        payload["expected_release_state"] = "closed"
        return payload

    for suffix, manifest_sha256 in (
        ("missing", None),
        ("mismatch", "d" * 64),
    ):
        rejected_manifest, _ = request_json(
            base_url,
            "api.php/api/v2/ops/ingest",
            method="POST",
            token=OPS_TOKEN,
            payload=ca_empty_payload(
                idempotency_key=f"php73-v2-ca-manifest-{suffix}",
                manifest_sha256=manifest_sha256,
            ),
            expected_status=400,
        )
        require(
            rejected_manifest.get("error") == "global_ingest_validation_failed"
            and "approved manifest mismatch"
            in str(rejected_manifest.get("detail")),
            repr(rejected_manifest),
        )
    ca_link_record = build_ca_link_record(
        filed_at=utc_text(now - timedelta(minutes=5)),
        observed_at=utc_text(now),
    )
    ca_link_payload = ca_empty_payload(
        idempotency_key="php73-v2-ca-manifest-approved",
        manifest_sha256=ca_manifest_sha256,
        record=ca_link_record,
    )
    unbound_ca_link_payload = json.loads(json.dumps(ca_link_payload))
    unbound_ca_link_payload.pop("expected_release_state")
    unbound_ca_link, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=unbound_ca_link_payload,
        expected_status=400,
    )
    require(
        unbound_ca_link.get("error") == "global_ingest_validation_failed"
        and "link-only apply requires release binding"
        in str(unbound_ca_link.get("detail")),
        repr(unbound_ca_link),
    )
    approved_manifest, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=ca_link_payload,
    )
    require(
        approved_manifest.get("data", {}).get("connector_id")
        == "connector:ca:issuer-ir"
        and approved_manifest.get("data", {}).get("raw_count") == 1
        and approved_manifest.get("data", {}).get("acknowledged_count") == 1,
        repr(approved_manifest),
    )
    ca_document_id = str(ca_link_record["record_id"])
    ca_event_id = "global-event:" + ca_document_id

    def ca_link_durable_content_state() -> str:
        return mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),ingest_id,payload_sha256,"
            "raw_count,acknowledged_count,batch_id,chunk_index,chunk_count,"
            "batch_raw_count,batch_acknowledged_count,code_revision,"
            "started_at,completed_at,created_at),256) "
            "FROM ci_global_ingest_receipts "
            "WHERE connector_id='connector:ca:issuer-ir' "
            "AND idempotency_key='php73-v2-ca-manifest-approved'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),document_id,content_hash,"
            "retrieved_at,updated_at,publication_status,version_no,"
            "payload_json),256) FROM ci_documents "
            f"WHERE document_id='{ca_document_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),event_id,title,"
            "COALESCE(summary,''),first_observed_at,updated_at,"
            "publication_status,review_status,identity_status),256) "
            "FROM ci_governance_events "
            f"WHERE event_id='{ca_event_id}'));"
        )

    # A scheduled link-only apply repeats the same approved manifest and
    # receipt. It must revalidate the current grant and refresh all three
    # readiness timestamps to the actual server verification time, even after
    # the previous observation has been stale for more than 45 minutes.
    stale_link_time = "2006-06-06 06:06:06"
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_connectors SET "
        f"last_checked_at='{stale_link_time}',"
        f"last_success_at='{stale_link_time}',"
        f"last_observed_at='{stale_link_time}',"
        "last_raw_count=1,last_acknowledged_count=1,"
        f"updated_at='{stale_link_time}' "
        "WHERE connector_id='connector:ca:issuer-ir';",
    )
    ca_content_before_heartbeat = ca_link_durable_content_state()
    scheduled_link_repeat, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=ca_link_payload,
    )
    require(
        scheduled_link_repeat.get("data", {}).get("idempotent") is True
        and scheduled_link_repeat.get("data", {}).get("raw_count") == 1
        and scheduled_link_repeat.get("data", {}).get(
            "acknowledged_count"
        ) == 1
        and mysql_execute(
            mysql_container_id,
            "SELECT "
            "last_checked_at=last_success_at,"
            "last_success_at=last_observed_at,"
            "TIMESTAMPDIFF(SECOND,last_checked_at,UTC_TIMESTAMP()) "
            "BETWEEN 0 AND 30,"
            "last_raw_count,last_acknowledged_count "
            "FROM ci_source_connectors "
            "WHERE connector_id='connector:ca:issuer-ir';",
        )
        == "1\t1\t1\t1\t1"
        and ca_link_durable_content_state() == ca_content_before_heartbeat,
        repr(scheduled_link_repeat),
    )

    # replay proves only that the exact receipt already exists. It must not
    # act as a collection heartbeat or mutate any connector field.
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_connectors SET "
        f"last_checked_at='{stale_link_time}',"
        f"last_success_at='{stale_link_time}',"
        f"last_observed_at='{stale_link_time}',"
        f"updated_at='{stale_link_time}' "
        "WHERE connector_id='connector:ca:issuer-ir';",
    )
    link_connector_before_replay = mysql_execute(
        mysql_container_id,
        "SELECT CONCAT_WS('|',connector_status,COALESCE(cursor_json,''),"
        "last_checked_at,last_success_at,last_observed_at,last_raw_count,"
        "last_acknowledged_count,COALESCE(last_error_class,''),"
        "COALESCE(code_revision,''),updated_at) "
        "FROM ci_source_connectors "
        "WHERE connector_id='connector:ca:issuer-ir';",
    )
    ca_content_before_replay = ca_link_durable_content_state()
    ca_link_replay_payload = json.loads(json.dumps(ca_link_payload))
    ca_link_replay_payload["ingest_mode"] = "replay"
    ca_link_replay, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=ca_link_replay_payload,
    )
    require(
        ca_link_replay.get("data", {}).get("idempotent") is True
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',connector_status,COALESCE(cursor_json,''),"
            "last_checked_at,last_success_at,last_observed_at,last_raw_count,"
            "last_acknowledged_count,COALESCE(last_error_class,''),"
            "COALESCE(code_revision,''),updated_at) "
            "FROM ci_source_connectors "
            "WHERE connector_id='connector:ca:issuer-ir';",
        )
        == link_connector_before_replay
        and ca_link_durable_content_state() == ca_content_before_replay,
        repr(ca_link_replay),
    )

    # Even an exact payload/receipt cannot refresh readiness if its durable
    # receipt is not a complete one-chunk selected-market batch.
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_global_ingest_receipts SET chunk_count=2 "
        "WHERE connector_id='connector:ca:issuer-ir' "
        "AND idempotency_key='php73-v2-ca-manifest-approved';",
    )
    ca_content_before_incomplete = ca_link_durable_content_state()
    incomplete_link_receipt, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=ca_link_payload,
        expected_status=409,
    )
    require(
        incomplete_link_receipt.get("error")
        == "global_ingest_batch_receipt_corrupt"
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',connector_status,COALESCE(cursor_json,''),"
            "last_checked_at,last_success_at,last_observed_at,last_raw_count,"
            "last_acknowledged_count,COALESCE(last_error_class,''),"
            "COALESCE(code_revision,''),updated_at) "
            "FROM ci_source_connectors "
            "WHERE connector_id='connector:ca:issuer-ir';",
        )
        == link_connector_before_replay
        and ca_link_durable_content_state() == ca_content_before_incomplete,
        repr(incomplete_link_receipt),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_global_ingest_receipts SET chunk_count=1 "
        "WHERE connector_id='connector:ca:issuer-ir' "
        "AND idempotency_key='php73-v2-ca-manifest-approved';",
    )

    # An impossible empty durable receipt must also fail closed. Previously it
    # returned an idempotent success before the locked completeness check.
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_global_ingest_receipts SET "
        "raw_count=0,acknowledged_count=0,"
        "batch_raw_count=0,batch_acknowledged_count=0 "
        "WHERE connector_id='connector:ca:issuer-ir' "
        "AND idempotency_key='php73-v2-ca-manifest-approved';",
    )
    empty_link_connector_before = mysql_execute(
        mysql_container_id,
        "SELECT CONCAT_WS('|',connector_status,COALESCE(cursor_json,''),"
        "last_checked_at,last_success_at,last_observed_at,last_raw_count,"
        "last_acknowledged_count,COALESCE(last_error_class,''),"
        "COALESCE(code_revision,''),updated_at) "
        "FROM ci_source_connectors "
        "WHERE connector_id='connector:ca:issuer-ir';",
    )
    ca_content_before_empty = ca_link_durable_content_state()
    empty_link_receipt, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=ca_link_payload,
        expected_status=409,
    )
    require(
        empty_link_receipt.get("error")
        == "global_ingest_batch_receipt_corrupt"
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',connector_status,COALESCE(cursor_json,''),"
            "last_checked_at,last_success_at,last_observed_at,last_raw_count,"
            "last_acknowledged_count,COALESCE(last_error_class,''),"
            "COALESCE(code_revision,''),updated_at) "
            "FROM ci_source_connectors "
            "WHERE connector_id='connector:ca:issuer-ir';",
        )
        == empty_link_connector_before
        and ca_link_durable_content_state() == ca_content_before_empty,
        repr(empty_link_receipt),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_global_ingest_receipts SET "
        "raw_count=1,acknowledged_count=1,"
        "batch_raw_count=1,batch_acknowledged_count=1 "
        "WHERE connector_id='connector:ca:issuer-ir' "
        "AND idempotency_key='php73-v2-ca-manifest-approved';",
    )

    # Manifest drift and revocation are checked before any heartbeat. These
    # direct corruption fixtures intentionally leave the connector snapshot
    # unchanged so a denied grant can never be made fresh by an old receipt.
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET evidence_hash='" + ("d" * 64) + "' "
        "WHERE source_right_id='official:ca-issuer-ir';",
    )
    manifest_drift, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=ca_link_payload,
        expected_status=400,
    )
    require(
        manifest_drift.get("error") == "global_ingest_validation_failed"
        and "approved manifest mismatch" in str(manifest_drift.get("detail"))
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',connector_status,COALESCE(cursor_json,''),"
            "last_checked_at,last_success_at,last_observed_at,last_raw_count,"
            "last_acknowledged_count,COALESCE(last_error_class,''),"
            "COALESCE(code_revision,''),updated_at) "
            "FROM ci_source_connectors "
            "WHERE connector_id='connector:ca:issuer-ir';",
        )
        == link_connector_before_replay
        and ca_link_durable_content_state() == ca_content_before_replay,
        repr(manifest_drift),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET evidence_hash='" + ca_manifest_sha256 + "' "
        "WHERE source_right_id='official:ca-issuer-ir';",
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET ai_allowed=1,updated_at=UTC_TIMESTAMP() "
        "WHERE source_right_id='official:ca-issuer-ir';",
    )
    stale_link_revision, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=ca_link_payload,
        expected_status=400,
    )
    require(
        stale_link_revision.get("error")
        == "global_ingest_validation_failed"
        and "current server grant mismatch" in str(
            stale_link_revision.get("detail")
        )
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',connector_status,COALESCE(cursor_json,''),"
            "last_checked_at,last_success_at,last_observed_at,last_raw_count,"
            "last_acknowledged_count,COALESCE(last_error_class,''),"
            "COALESCE(code_revision,''),updated_at) "
            "FROM ci_source_connectors "
            "WHERE connector_id='connector:ca:issuer-ir';",
        )
        == link_connector_before_replay
        and ca_link_durable_content_state() == ca_content_before_replay,
        repr(stale_link_revision),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET ai_allowed=0,"
        f"updated_at='{ca_right_updated_at}' "
        "WHERE source_right_id='official:ca-issuer-ir';",
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET status='revoked',"
        "revoked_at=UTC_TIMESTAMP(),updated_at=UTC_TIMESTAMP() "
        "WHERE source_right_id='official:ca-issuer-ir';",
    )
    revoked_link_apply, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=ca_link_payload,
        expected_status=400,
    )
    require(
        revoked_link_apply.get("error") == "global_ingest_validation_failed"
        and "not eligible for collection" in str(
            revoked_link_apply.get("detail")
        )
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',connector_status,COALESCE(cursor_json,''),"
            "last_checked_at,last_success_at,last_observed_at,last_raw_count,"
            "last_acknowledged_count,COALESCE(last_error_class,''),"
            "COALESCE(code_revision,''),updated_at) "
            "FROM ci_source_connectors "
            "WHERE connector_id='connector:ca:issuer-ir';",
        )
        == link_connector_before_replay
        and ca_link_durable_content_state() == ca_content_before_replay,
        repr(revoked_link_apply),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET status='active',revoked_at=NULL,"
        "updated_at=UTC_TIMESTAMP() "
        "WHERE source_right_id='official:ca-issuer-ir';",
    )

    # OpenDART remains on the established v1 official-ingest path.
    observed_at = utc_text(now)
    filed_at = utc_text(now - timedelta(minutes=5))
    dart_rejected, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload={
            "idempotency_key": "php73-v2-dart-rejected",
            "code_revision": CODE_REVISION,
            "envelope": {
                "schema_version": 1,
                "connector_id": "connector:kr:dart",
                "country_code": "KR",
                "source_right_id": "official:dart",
                "rights_revision": "0" * 64,
                "retrieved_at": observed_at,
                "coverage_mode": "market-wide",
                "records": [],
                "next_cursor": None,
                "exhausted": True,
                "request_count": 0,
                "raw_count": 0,
                "public_allowed": False,
                "ai_allowed": False,
                "lifecycle_observations": [],
            },
        },
        expected_status=400,
    )
    require(
        dart_rejected.get("error") == "global_ingest_validation_failed"
        and "established official-ingest pipeline" in str(dart_rejected.get("detail")),
        repr(dart_rejected),
    )

    # A final chunk can never create a checkpoint before every prior chunk is
    # durably present in the same batch.
    out_of_order_batch = (
        "global-batch:" + hashlib.sha256(b"php73-v2-final-before-prior").hexdigest()
    )
    final_before_prior, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=empty_chunk_payload(
            rights_revision=rights_revision,
            idempotency_key="php73-v2-final-before-prior",
            retrieved_at=observed_at,
            batch_id=out_of_order_batch,
            index=2,
            count=2,
        ),
        expected_status=409,
    )
    require(
        final_before_prior.get("error") == "global_ingest_chunk_out_of_order",
        repr(final_before_prior),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_global_ingest_receipts "
            f"WHERE batch_id='{out_of_order_batch}';",
        )
        == "0",
        out_of_order_batch,
    )

    # Identical batch metadata is necessary but not sufficient: finalization
    # also verifies the sum of every persisted chunk receipt.
    totals_batch = (
        "global-batch:" + hashlib.sha256(b"php73-v2-batch-totals").hexdigest()
    )
    totals_first, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=empty_chunk_payload(
            rights_revision=rights_revision,
            idempotency_key="php73-v2-totals-first",
            retrieved_at=observed_at,
            batch_id=totals_batch,
            index=1,
            count=2,
            batch_raw_count=1,
        ),
    )
    require(
        totals_first.get("data", {}).get("raw_count") == 0,
        repr(totals_first),
    )
    totals_final, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=empty_chunk_payload(
            rights_revision=rights_revision,
            idempotency_key="php73-v2-totals-final",
            retrieved_at=observed_at,
            batch_id=totals_batch,
            index=2,
            count=2,
            batch_raw_count=1,
        ),
        expected_status=409,
    )
    require(
        totals_final.get("error") == "global_ingest_batch_totals_mismatch",
        repr(totals_final),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*),SUM(raw_count),MAX(chunk_index) "
            "FROM ci_global_ingest_receipts "
            f"WHERE batch_id='{totals_batch}';",
        )
        == "1\t0\t1",
        totals_batch,
    )

    # A valid two-chunk zero-record batch still uses the same completeness
    # proof, survives changed request telemetry after an interrupted attempt,
    # and advances the checkpoint only after chunk 2 commits.
    complete_batch = (
        "global-batch:" + hashlib.sha256(b"php73-v2-complete-batch").hexdigest()
    )
    for chunk_index in (1, 2):
        complete_payload = empty_chunk_payload(
            rights_revision=rights_revision,
            idempotency_key=(f"php73-v2-complete-chunk-{chunk_index}"),
            retrieved_at=observed_at,
            batch_id=complete_batch,
            index=chunk_index,
            count=2,
        )
        complete_payload["envelope"]["chunk"]["batch_request_count"] = (
            2 if chunk_index == 1 else 3
        )
        if chunk_index == 2:
            complete_payload["envelope"]["request_count"] = 3
        complete_chunk, _ = request_json(
            base_url,
            "api.php/api/v2/ops/ingest",
            method="POST",
            token=OPS_TOKEN,
            payload=complete_payload,
        )
        require(
            complete_chunk.get("data", {}).get("acknowledged_count") == 0,
            repr(complete_chunk),
        )
        if chunk_index == 1:
            retried_first = json.loads(json.dumps(complete_payload))
            retried_first["envelope"]["chunk"]["batch_request_count"] = 3
            retried_chunk, _ = request_json(
                base_url,
                "api.php/api/v2/ops/ingest",
                method="POST",
                token=OPS_TOKEN,
                payload=retried_first,
            )
            require(
                retried_chunk.get("data", {}).get("idempotent") is True,
                repr(retried_chunk),
            )
        checkpoint_after_chunk, _ = request_json(
            base_url,
            (f"api.php/api/v2/ops/connectors/{SEC_CONNECTOR_ID}/checkpoint"),
            token=OPS_TOKEN,
        )
        cursor_after_chunk = checkpoint_after_chunk.get("data", {}).get("cursor_json")
        if chunk_index == 1:
            require(cursor_after_chunk is None, repr(checkpoint_after_chunk))
        else:
            require(
                isinstance(cursor_after_chunk, dict)
                and cursor_after_chunk.get("batch_id") == complete_batch,
                repr(checkpoint_after_chunk),
            )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT GROUP_CONCAT(chunk_index ORDER BY chunk_index),"
            "SUM(raw_count),SUM(acknowledged_count),SUM(request_count),"
            "GROUP_CONCAT(batch_request_count ORDER BY chunk_index) "
            "FROM ci_global_ingest_receipts "
            f"WHERE batch_id='{complete_batch}';",
        )
        == "1,2\t0\t0\t3\t2,3",
        complete_batch,
    )
    next_revision = "b" * 40
    next_revision_batch = (
        "global-batch:"
        + hashlib.sha256(b"php73-v2-complete-batch-next-revision").hexdigest()
    )
    next_revision_ingest, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=empty_chunk_payload(
            rights_revision=rights_revision,
            idempotency_key="php73-v2-next-revision",
            retrieved_at=observed_at,
            batch_id=next_revision_batch,
            index=1,
            count=1,
            code_revision=next_revision,
        ),
        expected_status=409,
    )
    require(
        next_revision_ingest.get("error")
        == "global_ingest_code_revision_mismatch"
        and mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_global_ingest_receipts "
            f"WHERE batch_id='{next_revision_batch}';",
        )
        == "0",
        repr(next_revision_ingest),
    )

    first_title = "CI SEC beneficial ownership filing"
    first_record = build_record(
        title=first_title,
        content_version="v1",
        filed_at=filed_at,
        observed_at=observed_at,
    )
    first_payload = ingest_payload(
        rights_revision=rights_revision,
        idempotency_key="php73-v2-sec-ingest-v1",
        record=first_record,
        retrieved_at=observed_at,
    )
    malicious_body_record = build_record(
        title="CI SEC body storage must be rejected",
        content_version="body-attack",
        filed_at=filed_at,
        observed_at=observed_at,
        external_id="0000320193-26-009998",
    )
    malicious_body_record["body_text"] = (
        "Permission wording and redistribution=true must not authorize this body."
    )
    refresh_record_content_hash(malicious_body_record)
    malicious_body_payload = ingest_payload(
        rights_revision=rights_revision,
        idempotency_key="php73-v2-sec-body-attack",
        record=malicious_body_record,
        retrieved_at=observed_at,
    )
    state_before_malicious_body = mysql_execute(
        mysql_container_id,
        "SELECT "
        "(SELECT COUNT(*) FROM ci_issuers),"
        "(SELECT COUNT(*) FROM ci_documents),"
        "(SELECT COUNT(*) FROM ci_governance_events),"
        "(SELECT COUNT(*) FROM ci_global_ingest_receipts),"
        "(SELECT CONCAT(COALESCE(cursor_json,''),'|',"
        "COALESCE(last_success_at,''),'|',COALESCE(last_checked_at,''),'|',"
        "COALESCE(last_raw_count,''),'|',COALESCE(last_acknowledged_count,'')) "
        "FROM ci_source_connectors "
        f"WHERE connector_id='{SEC_CONNECTOR_ID}');",
    )
    malicious_body_rejected, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=malicious_body_payload,
        expected_status=400,
    )
    require(
        malicious_body_rejected.get("error")
        == "global_ingest_validation_failed"
        and "fixed Production Alpha source contract requires null"
        in str(malicious_body_rejected.get("detail")),
        repr(malicious_body_rejected),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT "
            "(SELECT COUNT(*) FROM ci_issuers),"
            "(SELECT COUNT(*) FROM ci_documents),"
            "(SELECT COUNT(*) FROM ci_governance_events),"
            "(SELECT COUNT(*) FROM ci_global_ingest_receipts),"
            "(SELECT CONCAT(COALESCE(cursor_json,''),'|',"
            "COALESCE(last_success_at,''),'|',COALESCE(last_checked_at,''),'|',"
            "COALESCE(last_raw_count,''),'|',COALESCE(last_acknowledged_count,'')) "
            "FROM ci_source_connectors "
            f"WHERE connector_id='{SEC_CONNECTOR_ID}');",
        )
        == state_before_malicious_body
        and mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_documents WHERE document_id="
            f"'{malicious_body_record['record_id']}';",
        )
        == "0",
        "fixed metadata-only body rejection must leave MySQL unchanged",
    )
    replay_only_missing = json.loads(json.dumps(first_payload))
    replay_only_missing["ingest_mode"] = "replay"
    state_before_missing_replay = mysql_execute(
        mysql_container_id,
        "SELECT "
        "(SELECT COUNT(*) FROM ci_global_ingest_receipts),"
        "(SELECT COUNT(*) FROM ci_documents),"
        "(SELECT COUNT(*) FROM ci_governance_events);",
    )
    missing_replay, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=replay_only_missing,
        expected_status=409,
    )
    require(
        missing_replay.get("error") == "global_ingest_replay_missing"
        and mysql_execute(
            mysql_container_id,
            "SELECT "
            "(SELECT COUNT(*) FROM ci_global_ingest_receipts),"
            "(SELECT COUNT(*) FROM ci_documents),"
            "(SELECT COUNT(*) FROM ci_governance_events);",
        )
        == state_before_missing_replay,
        repr(missing_replay),
    )
    credential_url_payload = json.loads(json.dumps(first_payload))
    credential_url_payload["idempotency_key"] = "php73-v2-credential-url"
    credential_url_payload["envelope"]["chunk"]["batch_id"] = (
        "global-batch:" + hashlib.sha256(b"php73-v2-credential-url").hexdigest()
    )
    credential_url_payload["envelope"]["records"][0]["original_url"] = (
        "https://www.sec.gov/filing?api%5Fkey=must-not-be-stored"
    )
    credential_url_rejected, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=credential_url_payload,
        expected_status=400,
    )
    require(
        credential_url_rejected.get("error") == "global_ingest_validation_failed"
        and "credential query is forbidden"
        in str(credential_url_rejected.get("detail")),
        repr(credential_url_rejected),
    )
    first_ingest, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=first_payload,
    )
    first_data = first_ingest.get("data", {})
    require(
        first_ingest.get("api_version") == "v2"
        and first_data.get("connector_id") == SEC_CONNECTOR_ID
        and first_data.get("raw_count") == 1
        and first_data.get("acknowledged_count") == 1
        and first_data.get("idempotent") is False
        and first_data.get("public_events_created") == 0
        and first_data.get("review_required") == 1,
        repr(first_ingest),
    )
    replay_only_payload = json.loads(json.dumps(first_payload))
    replay_only_payload["ingest_mode"] = "replay"
    state_before_read_only_replay = mysql_execute(
        mysql_container_id,
        "SELECT "
        "(SELECT COUNT(*) FROM ci_global_ingest_receipts),"
        "(SELECT COUNT(*) FROM ci_documents),"
        "(SELECT COUNT(*) FROM ci_governance_events),"
        "(SELECT CONCAT(COALESCE(cursor_json,''),'|',"
        "COALESCE(last_success_at,''),'|',COALESCE(last_checked_at,'')) "
        "FROM ci_source_connectors "
        f"WHERE connector_id='{SEC_CONNECTOR_ID}');",
    )
    read_only_replay, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=replay_only_payload,
    )
    require(
        read_only_replay.get("data", {}).get("idempotent") is True
        and mysql_execute(
            mysql_container_id,
            "SELECT "
            "(SELECT COUNT(*) FROM ci_global_ingest_receipts),"
            "(SELECT COUNT(*) FROM ci_documents),"
            "(SELECT COUNT(*) FROM ci_governance_events),"
            "(SELECT CONCAT(COALESCE(cursor_json,''),'|',"
            "COALESCE(last_success_at,''),'|',COALESCE(last_checked_at,'')) "
            "FROM ci_source_connectors "
            f"WHERE connector_id='{SEC_CONNECTOR_ID}');",
        )
        == state_before_read_only_replay,
        repr(read_only_replay),
    )
    event_id = "global-event:" + first_record["record_id"]
    draft_row = mysql_execute(
        mysql_container_id,
        "SELECT publication_status,review_status,identity_status "
        "FROM ci_governance_events "
        f"WHERE event_id='{event_id}';",
    )
    require(draft_row == "draft\tpending\tneeds_review", draft_row)
    draft_document = mysql_execute(
        mysql_container_id,
        "SELECT company_id IS NULL,publication_status,version_no "
        "FROM ci_documents "
        f"WHERE document_id='{first_record['record_id']}';",
    )
    require(draft_document == "1\tdraft\t1", draft_document)

    replay, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=first_payload,
    )
    require(
        replay.get("data", {}).get("idempotent") is True
        and replay.get("data", {}).get("acknowledged_count") == 1,
        repr(replay),
    )
    retry_payload = json.loads(json.dumps(first_payload))
    retry_payload["envelope"]["retrieved_at"] = utc_text(now + timedelta(minutes=1))
    retry_payload["envelope"]["records"][0]["first_observed_at"] = utc_text(
        now + timedelta(minutes=1)
    )
    retry, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=retry_payload,
    )
    require(
        retry.get("data", {}).get("idempotent") is True
        and retry.get("data", {}).get("acknowledged_count") == 1,
        repr(retry),
    )
    telemetry_retry_payload = json.loads(json.dumps(first_payload))
    telemetry_retry_payload["envelope"]["request_count"] = 3
    telemetry_retry_payload["envelope"]["chunk"]["batch_request_count"] = 3
    telemetry_retry, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=telemetry_retry_payload,
    )
    require(
        telemetry_retry.get("data", {}).get("idempotent") is True
        and telemetry_retry.get("data", {}).get("acknowledged_count") == 1
        and mysql_execute(
            mysql_container_id,
            "SELECT request_count,batch_request_count "
            "FROM ci_global_ingest_receipts "
            "WHERE idempotency_key='php73-v2-sec-ingest-v1';",
        )
        == "1\t1",
        repr(telemetry_retry),
    )

    # Classified current receipts are bound to the same canonical semantic
    # digest in Python and PHP, including Unicode and nested empty objects.
    # A real unchanged poll may refresh connector readiness, but it must not
    # create another receipt, document version, or event.
    current_payload = json.loads(json.dumps(first_payload, ensure_ascii=False))
    current_payload["expected_release_state"] = "closed"
    current_observed = datetime.now(timezone.utc).replace(
        microsecond=0
    ) + timedelta(seconds=1)
    current_payload["envelope"]["retrieved_at"] = utc_text(current_observed)
    current_payload["envelope"]["records"][0]["first_observed_at"] = utc_text(
        current_observed
    )
    current_payload["envelope"]["records"][0]["title"] = (
        "CI canonical current\u2028line\u2029separator"
    )
    current_payload["envelope"]["records"][0]["metadata"][
        "canonical_fixture"
    ]["empty_object"] = []
    current_payload["envelope"]["records"][0]["metadata"][
        "line_separators"
    ] = "\u2028\u2029"
    refresh_record_content_hash(current_payload["envelope"]["records"][0])
    current_cursor = sec_current_cursor(now)
    current_payload["envelope"]["next_cursor"] = current_cursor
    current_payload["envelope"]["chunk"]["batch_id"] = (
        "global-batch:"
        + hashlib.sha256(b"php73-v2-current-heartbeat").hexdigest()
    )
    current_key = bind_classified_ingest_key(
        current_payload,
        namespace="global-ingest-v2-current",
    )
    unbound_current = json.loads(
        json.dumps(current_payload, ensure_ascii=False)
    )
    unbound_current.pop("expected_release_state")
    unbound_current_rejected, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=unbound_current,
        expected_status=400,
    )
    require(
        unbound_current_rejected.get("error")
        == "global_ingest_validation_failed"
        and "classified receipt requires release binding"
        in str(unbound_current_rejected.get("detail")),
        repr(unbound_current_rejected),
    )
    current_state_before_validation = connector_runtime_state(
        mysql_container_id,
        SEC_CONNECTOR_ID,
    )
    current_rows_before_validation = mysql_execute(
        mysql_container_id,
        "SELECT COUNT(*) FROM ci_global_ingest_receipts "
        f"WHERE idempotency_key='{current_key}';",
    )
    zero_request_current = json.loads(
        json.dumps(current_payload, ensure_ascii=False)
    )
    zero_request_current["envelope"]["request_count"] = 0
    zero_request_current["envelope"]["chunk"]["batch_request_count"] = 0
    require(
        bind_classified_ingest_key(
            zero_request_current,
            namespace="global-ingest-v2-current",
        )
        == current_key,
        "current request telemetry must stay outside content identity",
    )
    zero_current_rejected, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=zero_request_current,
        expected_status=400,
    )
    require(
        zero_current_rejected.get("error")
        == "global_ingest_validation_failed"
        and "current-poll request proof required"
        in str(zero_current_rejected.get("detail"))
        and connector_runtime_state(mysql_container_id, SEC_CONNECTOR_ID)
        == current_state_before_validation
        and mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_global_ingest_receipts "
            f"WHERE idempotency_key='{current_key}';",
        )
        == current_rows_before_validation,
        repr(zero_current_rejected),
    )
    stale_current = json.loads(
        json.dumps(current_payload, ensure_ascii=False)
    )
    stale_current["envelope"]["retrieved_at"] = utc_text(
        datetime.now(timezone.utc) - timedelta(minutes=16)
    )
    stale_current["envelope"]["records"][0]["first_observed_at"] = (
        stale_current["envelope"]["retrieved_at"]
    )
    require(
        bind_classified_ingest_key(
            stale_current,
            namespace="global-ingest-v2-current",
        )
        == current_key,
        "retrieval time must stay outside current content identity",
    )
    stale_current_rejected, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=stale_current,
        expected_status=400,
    )
    require(
        stale_current_rejected.get("error")
        == "global_ingest_validation_failed"
        and "current-poll freshness window mismatch"
        in str(stale_current_rejected.get("detail"))
        and connector_runtime_state(mysql_container_id, SEC_CONNECTOR_ID)
        == current_state_before_validation
        and mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_global_ingest_receipts "
            f"WHERE idempotency_key='{current_key}';",
        )
        == current_rows_before_validation,
        repr(stale_current_rejected),
    )
    future_current = json.loads(
        json.dumps(current_payload, ensure_ascii=False)
    )
    future_current["envelope"]["retrieved_at"] = utc_text(
        datetime.now(timezone.utc) + timedelta(minutes=2)
    )
    future_current["envelope"]["records"][0]["first_observed_at"] = (
        future_current["envelope"]["retrieved_at"]
    )
    require(
        bind_classified_ingest_key(
            future_current,
            namespace="global-ingest-v2-current",
        )
        == current_key,
        "retrieval time must stay outside current content identity",
    )
    future_current_rejected, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=future_current,
        expected_status=400,
    )
    require(
        future_current_rejected.get("error")
        == "global_ingest_validation_failed"
        and "current-poll freshness window mismatch"
        in str(future_current_rejected.get("detail"))
        and connector_runtime_state(mysql_container_id, SEC_CONNECTOR_ID)
        == current_state_before_validation
        and mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_global_ingest_receipts "
            f"WHERE idempotency_key='{current_key}';",
        )
        == current_rows_before_validation,
        repr(future_current_rejected),
    )
    current_ingest, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=current_payload,
    )
    require(
        current_ingest.get("data", {}).get("idempotent") is False,
        repr(current_ingest),
    )
    equal_current_state = connector_runtime_state(
        mysql_container_id,
        SEC_CONNECTOR_ID,
    )
    equal_current_retry, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=current_payload,
    )
    require(
        equal_current_retry.get("data", {}).get("idempotent") is True
        and connector_runtime_state(mysql_container_id, SEC_CONNECTOR_ID)
        == equal_current_state,
        repr(equal_current_retry),
    )
    new_equal_current = json.loads(
        json.dumps(current_payload, ensure_ascii=False)
    )
    new_equal_current["envelope"]["records"][0]["title"] = (
        "CI new current content at an equal observation"
    )
    refresh_record_content_hash(new_equal_current["envelope"]["records"][0])
    new_equal_current["envelope"]["chunk"]["batch_id"] = (
        "global-batch:"
        + hashlib.sha256(b"php73-v2-new-equal-current").hexdigest()
    )
    new_equal_key = bind_classified_ingest_key(
        new_equal_current,
        namespace="global-ingest-v2-current",
    )
    require(
        new_equal_key != current_key,
        "new current content must have a distinct receipt identity",
    )
    new_current_durable_state = mysql_execute(
        mysql_container_id,
        "SELECT CONCAT("
        "(SELECT COUNT(*) FROM ci_global_ingest_receipts),'|',"
        "(SELECT COUNT(*) FROM ci_documents),'|',"
        "(SELECT COUNT(*) FROM ci_governance_events));",
    )
    new_current_connector_state = connector_runtime_state(
        mysql_container_id,
        SEC_CONNECTOR_ID,
    )
    new_equal_rejected, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=new_equal_current,
        expected_status=409,
    )
    require(
        new_equal_rejected.get("error")
        == "global_ingest_current_observation_not_newer"
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT("
            "(SELECT COUNT(*) FROM ci_global_ingest_receipts),'|',"
            "(SELECT COUNT(*) FROM ci_documents),'|',"
            "(SELECT COUNT(*) FROM ci_governance_events));",
        )
        == new_current_durable_state
        and connector_runtime_state(mysql_container_id, SEC_CONNECTOR_ID)
        == new_current_connector_state,
        repr(new_equal_rejected),
    )
    new_older_current = json.loads(
        json.dumps(new_equal_current, ensure_ascii=False)
    )
    new_older_current["envelope"]["retrieved_at"] = utc_text(
        current_observed - timedelta(seconds=1)
    )
    new_older_current["envelope"]["records"][0]["first_observed_at"] = (
        new_older_current["envelope"]["retrieved_at"]
    )
    require(
        bind_classified_ingest_key(
            new_older_current,
            namespace="global-ingest-v2-current",
        )
        == new_equal_key,
        "current observation time must stay outside content identity",
    )
    new_older_rejected, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=new_older_current,
        expected_status=409,
    )
    require(
        new_older_rejected.get("error")
        == "global_ingest_current_observation_not_newer"
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT("
            "(SELECT COUNT(*) FROM ci_global_ingest_receipts),'|',"
            "(SELECT COUNT(*) FROM ci_documents),'|',"
            "(SELECT COUNT(*) FROM ci_governance_events));",
        )
        == new_current_durable_state
        and connector_runtime_state(mysql_container_id, SEC_CONNECTOR_ID)
        == new_current_connector_state,
        repr(new_older_rejected),
    )
    zero_repeat_state = connector_runtime_state(
        mysql_container_id,
        SEC_CONNECTOR_ID,
    )
    zero_repeat_rejected, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=zero_request_current,
        expected_status=400,
    )
    require(
        zero_repeat_rejected.get("error")
        == "global_ingest_validation_failed"
        and "current-poll request proof required"
        in str(zero_repeat_rejected.get("detail"))
        and connector_runtime_state(mysql_container_id, SEC_CONNECTOR_ID)
        == zero_repeat_state,
        repr(zero_repeat_rejected),
    )
    tampered_current = json.loads(
        json.dumps(current_payload, ensure_ascii=False)
    )
    tampered_current["idempotency_key"] = (
        current_key[:-1] + ("0" if current_key[-1] != "0" else "1")
    )
    tampered_current_rejected, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=tampered_current,
        expected_status=400,
    )
    require(
        tampered_current_rejected.get("error")
        == "global_ingest_validation_failed"
        and "classified semantic digest mismatch"
        in str(tampered_current_rejected.get("detail")),
        repr(tampered_current_rejected),
    )
    incomplete_current = json.loads(
        json.dumps(current_payload, ensure_ascii=False)
    )
    incomplete_current["envelope"]["exhausted"] = False
    bind_classified_ingest_key(
        incomplete_current,
        namespace="global-ingest-v2-current",
    )
    incomplete_current_rejected, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=incomplete_current,
        expected_status=400,
    )
    require(
        incomplete_current_rejected.get("error")
        == "global_ingest_validation_failed"
        and "current-poll contract mismatch"
        in str(incomplete_current_rejected.get("detail")),
        repr(incomplete_current_rejected),
    )
    malformed_cursor_current = json.loads(
        json.dumps(current_payload, ensure_ascii=False)
    )
    malformed_cursor_current["envelope"]["next_cursor"] = (
        "sec-current-v1:not-canonical"
    )
    bind_classified_ingest_key(
        malformed_cursor_current,
        namespace="global-ingest-v2-current",
    )
    malformed_cursor_rejected, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=malformed_cursor_current,
        expected_status=400,
    )
    require(
        malformed_cursor_rejected.get("error")
        == "global_ingest_validation_failed"
        and "current-poll contract mismatch"
        in str(malformed_cursor_rejected.get("detail")),
        repr(malformed_cursor_rejected),
    )
    stale_heartbeat = "2000-01-01 00:00:00"
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_connectors SET "
        f"last_checked_at='{stale_heartbeat}',"
        f"last_success_at='{stale_heartbeat}',"
        f"last_observed_at='{stale_heartbeat}',"
        f"updated_at='{stale_heartbeat}' "
        f"WHERE connector_id='{SEC_CONNECTOR_ID}';",
    )
    immutable_before_heartbeat = mysql_execute(
        mysql_container_id,
        "SELECT CONCAT("
        "(SELECT COUNT(*) FROM ci_global_ingest_receipts),'|',"
        "(SELECT COUNT(*) FROM ci_documents),'|',"
        "(SELECT COUNT(*) FROM ci_governance_events),'|',"
        "(SELECT cursor_json FROM ci_source_connectors "
        f"WHERE connector_id='{SEC_CONNECTOR_ID}'));",
    )
    current_retry = json.loads(json.dumps(current_payload, ensure_ascii=False))
    heartbeat_observed = datetime.now(timezone.utc).replace(
        microsecond=0
    ) + timedelta(seconds=1)
    current_retry["envelope"]["retrieved_at"] = utc_text(heartbeat_observed)
    current_retry["envelope"]["records"][0]["first_observed_at"] = utc_text(
        heartbeat_observed
    )
    current_retry["envelope"]["request_count"] = 3
    current_retry["envelope"]["chunk"]["batch_request_count"] = 3
    require(
        bind_classified_ingest_key(
            current_retry,
            namespace="global-ingest-v2-current",
        )
        == current_key,
        "current receipt identity must ignore attempt-only telemetry",
    )
    current_heartbeat, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=current_retry,
    )
    immutable_after_heartbeat = mysql_execute(
        mysql_container_id,
        "SELECT CONCAT("
        "(SELECT COUNT(*) FROM ci_global_ingest_receipts),'|',"
        "(SELECT COUNT(*) FROM ci_documents),'|',"
        "(SELECT COUNT(*) FROM ci_governance_events),'|',"
        "(SELECT cursor_json FROM ci_source_connectors "
        f"WHERE connector_id='{SEC_CONNECTOR_ID}'));",
    )
    require(
        current_heartbeat.get("data", {}).get("idempotent") is True
        and immutable_after_heartbeat == immutable_before_heartbeat
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT(last_checked_at<>'2000-01-01 00:00:00','|',"
            "last_success_at<>'2000-01-01 00:00:00','|',"
            f"JSON_UNQUOTE(JSON_EXTRACT(cursor_json,'$.source_cursor'))="
            f"'{current_cursor}','|',"
            "(SELECT COUNT(*) FROM ci_global_ingest_receipts "
            f"WHERE idempotency_key='{current_key}')) "
            "FROM ci_source_connectors "
            f"WHERE connector_id='{SEC_CONNECTOR_ID}';",
        )
        == "1|1|1|1",
        repr(current_heartbeat),
    )

    # A large current poll carries request proof on the final chunk only. All
    # non-final chunks remain writable while the connector heartbeat and source
    # cursor advance exactly once, after the complete three-chunk batch exists.
    multi_observed = datetime.now(timezone.utc).replace(
        microsecond=0
    ) + timedelta(seconds=2)
    multi_cursor = sec_current_cursor(multi_observed)
    multi_batch = (
        "global-batch:"
        + hashlib.sha256(b"php73-v2-current-three-chunk").hexdigest()
    )
    multi_scope = hashlib.sha256(
        b"php73-v2-current-three-chunk-cursor"
    ).hexdigest()[:24]
    multi_keys: list[str] = []
    for chunk_index in (1, 2, 3):
        multi_current = json.loads(
            json.dumps(current_payload, ensure_ascii=False)
        )
        multi_current["envelope"]["retrieved_at"] = utc_text(multi_observed)
        multi_current["envelope"]["records"] = []
        multi_current["envelope"]["raw_count"] = 0
        multi_current["envelope"]["request_count"] = (
            1 if chunk_index == 3 else 0
        )
        multi_current["envelope"]["exhausted"] = chunk_index == 3
        multi_current["envelope"]["next_cursor"] = (
            multi_cursor
            if chunk_index == 3
            else (
                "global-ingest-chunk:2026-07-23:2026-07-24:"
                f"{chunk_index}:3:{multi_scope}"
            )
        )
        multi_current["envelope"]["chunk"] = {
            "index": chunk_index,
            "count": 3,
            "batch_raw_count": 0,
            "batch_acknowledged_count": 0,
            "batch_request_count": 1,
            "batch_id": multi_batch,
            "window_start": "2026-07-23",
            "window_end_exclusive": "2026-07-24",
        }
        multi_key = bind_classified_ingest_key(
            multi_current,
            namespace="global-ingest-v2-current",
        )
        multi_keys.append(multi_key)
        multi_result, _ = request_json(
            base_url,
            "api.php/api/v2/ops/ingest",
            method="POST",
            token=OPS_TOKEN,
            payload=multi_current,
        )
        require(
            multi_result.get("data", {}).get("idempotent") is False
            and multi_result.get("data", {}).get("acknowledged_count") == 0,
            repr(multi_result),
        )
    require(
        len(set(multi_keys)) == 3
        and mysql_execute(
            mysql_container_id,
            "SELECT GROUP_CONCAT(chunk_index ORDER BY chunk_index),"
            "SUM(request_count),"
            "GROUP_CONCAT(batch_request_count ORDER BY chunk_index) "
            "FROM ci_global_ingest_receipts "
            f"WHERE batch_id='{multi_batch}';",
        )
        == "1,2,3\t1\t1,1,1"
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT(last_observed_at,'|',"
            "JSON_UNQUOTE(JSON_EXTRACT(cursor_json,'$.source_cursor'))) "
            "FROM ci_source_connectors "
            f"WHERE connector_id='{SEC_CONNECTOR_ID}';",
        )
        == (
            multi_observed.strftime("%Y-%m-%d %H:%M:%S")
            + "|"
            + multi_cursor
        ),
        multi_batch,
    )
    # Restore the earlier current fixture cursor for the later exact-receipt
    # preview-bound heartbeat checks.
    set_sec_current_cursor(mysql_container_id, now)

    # A completed-day evidence marker needs one actual daily-index request.
    # Exact replay remains read-only and cannot refresh the intraday heartbeat.
    zero_request_day = empty_chunk_payload(
        rights_revision=rights_revision,
        idempotency_key="placeholder-zero-day",
        retrieved_at=observed_at,
        batch_id=(
            "global-batch:"
            + hashlib.sha256(b"php73-v2-zero-request-day").hexdigest()
        ),
        index=1,
        count=1,
    )
    zero_request_day["expected_release_state"] = "closed"
    bind_classified_ingest_key(
        zero_request_day,
        namespace="global-ingest-v2-day",
    )
    zero_day_rejected, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=zero_request_day,
        expected_status=400,
    )
    require(
        zero_day_rejected.get("error") == "global_ingest_validation_failed"
        and "completed-day evidence contract mismatch"
        in str(zero_day_rejected.get("detail")),
        repr(zero_day_rejected),
    )
    wrong_provenance_day = json.loads(
        json.dumps(current_payload, ensure_ascii=False)
    )
    wrong_provenance_day["envelope"]["next_cursor"] = None
    wrong_provenance_day["envelope"]["exhausted"] = True
    wrong_provenance_day["envelope"]["chunk"]["batch_id"] = (
        "global-batch:"
        + hashlib.sha256(b"php73-v2-wrong-day-provenance").hexdigest()
    )
    bind_classified_ingest_key(
        wrong_provenance_day,
        namespace="global-ingest-v2-day",
    )
    wrong_provenance_rejected, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=wrong_provenance_day,
        expected_status=400,
    )
    require(
        wrong_provenance_rejected.get("error")
        == "global_ingest_validation_failed"
        and "completed-day source provenance mismatch"
        in str(wrong_provenance_rejected.get("detail")),
        repr(wrong_provenance_rejected),
    )
    completed_day = json.loads(json.dumps(zero_request_day))
    completed_day["envelope"]["request_count"] = 1
    completed_day["envelope"]["chunk"]["batch_request_count"] = 1
    completed_day["envelope"]["chunk"]["batch_id"] = (
        "global-batch:"
        + hashlib.sha256(b"php73-v2-empty-weekend-day").hexdigest()
    )
    completed_day["envelope"]["chunk"]["window_start"] = "2026-07-18"
    completed_day["envelope"]["chunk"]["window_end_exclusive"] = "2026-07-19"
    completed_day_key = bind_classified_ingest_key(
        completed_day,
        namespace="global-ingest-v2-day",
    )
    completed_day_ingest, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=completed_day,
    )
    require(
        completed_day_ingest.get("data", {}).get("idempotent") is False,
        repr(completed_day_ingest),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_connectors SET "
        f"last_checked_at='{stale_heartbeat}',"
        f"last_success_at='{stale_heartbeat}',"
        f"last_observed_at='{stale_heartbeat}',"
        f"updated_at='{stale_heartbeat}' "
        f"WHERE connector_id='{SEC_CONNECTOR_ID}';",
    )
    state_before_completed_replay = mysql_execute(
        mysql_container_id,
        "SELECT CONCAT(cursor_json,'|',last_checked_at,'|',last_success_at,'|',"
        "last_observed_at,'|',"
        "(SELECT COUNT(*) FROM ci_global_ingest_receipts), '|',"
        "(SELECT COUNT(*) FROM ci_documents), '|',"
        "(SELECT COUNT(*) FROM ci_governance_events)) "
        "FROM ci_source_connectors "
        f"WHERE connector_id='{SEC_CONNECTOR_ID}';",
    )
    completed_day_replay = json.loads(json.dumps(completed_day))
    completed_day_replay["ingest_mode"] = "replay"
    completed_day_replay["envelope"]["retrieved_at"] = utc_text(
        now + timedelta(minutes=10)
    )
    require(
        bind_classified_ingest_key(
            completed_day_replay,
            namespace="global-ingest-v2-day",
        )
        == completed_day_key,
        "completed-day replay identity must ignore retrieval time",
    )
    completed_replay_result, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=completed_day_replay,
    )
    require(
        completed_replay_result.get("data", {}).get("idempotent") is True
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT(cursor_json,'|',last_checked_at,'|',last_success_at,'|',"
            "last_observed_at,'|',"
            "(SELECT COUNT(*) FROM ci_global_ingest_receipts), '|',"
            "(SELECT COUNT(*) FROM ci_documents), '|',"
            "(SELECT COUNT(*) FROM ci_governance_events)) "
            "FROM ci_source_connectors "
            f"WHERE connector_id='{SEC_CONNECTOR_ID}';",
        )
        == state_before_completed_replay,
        repr(completed_replay_result),
    )

    conflicting_payload = json.loads(json.dumps(first_payload))
    conflicting_payload["envelope"]["raw_count"] = 2
    conflicting_payload["envelope"]["chunk"]["batch_raw_count"] = 2
    conflict, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=conflicting_payload,
        expected_status=409,
    )
    require(
        conflict.get("error") == "global_ingest_idempotency_conflict",
        repr(conflict),
    )
    reused_hash_payload = json.loads(json.dumps(first_payload))
    reused_hash_payload["idempotency_key"] = "php73-v2-reused-semantic-hash"
    reused_hash_payload["envelope"]["chunk"]["batch_id"] = (
        "global-batch:" + hashlib.sha256(b"php73-v2-reused-semantic-hash").hexdigest()
    )
    reused_hash_payload["envelope"]["records"][0]["event_family"] = "meeting_and_vote"
    reused_hash, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=reused_hash_payload,
        expected_status=400,
    )
    require(
        reused_hash.get("error") == "global_ingest_validation_failed"
        and "content_hash: semantic contract mismatch"
        in str(reused_hash.get("detail")),
        repr(reused_hash),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_documents "
        "SET original_url='https://www.sec.gov/ci-corrupted-stored-row' "
        f"WHERE source_right_id='{SEC_RIGHT_ID}' "
        f"AND external_id='{first_record['external_id']}';",
    )
    stored_core_conflict_payload = json.loads(json.dumps(first_payload))
    stored_core_conflict_payload["idempotency_key"] = "php73-v2-stored-core-conflict"
    stored_core_conflict_payload["envelope"]["chunk"]["batch_id"] = (
        "global-batch:" + hashlib.sha256(b"php73-v2-stored-core-conflict").hexdigest()
    )
    stored_core_conflict, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=stored_core_conflict_payload,
        expected_status=409,
    )
    require(
        stored_core_conflict.get("error") == "global_document_hash_contract_conflict",
        repr(stored_core_conflict),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_documents "
        f"SET original_url='{first_record['original_url']}' "
        f"WHERE source_right_id='{SEC_RIGHT_ID}' "
        f"AND external_id='{first_record['external_id']}';",
    )
    version_count_after_reused_hash = mysql_execute(
        mysql_container_id,
        "SELECT COUNT(*) FROM ci_documents "
        f"WHERE source_right_id='{SEC_RIGHT_ID}' "
        f"AND external_id='{first_record['external_id']}';",
    )
    require(version_count_after_reused_hash == "1", version_count_after_reused_hash)

    # A changed body for the same external ID creates an immutable second
    # version and sends the canonical event back to the editor.
    new_version_sentinel = "2001-01-01 00:00:00"
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_governance_events "
        f"SET updated_at='{new_version_sentinel}' "
        f"WHERE event_id='{event_id}';",
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT updated_at FROM ci_governance_events "
            f"WHERE event_id='{event_id}';",
        )
        == new_version_sentinel,
        event_id,
    )
    second_title = "CI SEC beneficial ownership filing — amended source"
    second_record = build_record(
        title=second_title,
        content_version="v2",
        filed_at=filed_at,
        observed_at=utc_text(now + timedelta(seconds=1)),
    )
    second_payload = ingest_payload(
        rights_revision=rights_revision,
        idempotency_key="php73-v2-sec-ingest-v2",
        record=second_record,
        retrieved_at=utc_text(now + timedelta(seconds=1)),
    )
    second_ingest, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=second_payload,
    )
    require(
        second_ingest.get("data", {}).get("acknowledged_count") == 1
        and second_ingest.get("data", {}).get("review_required") == 1,
        repr(second_ingest),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT updated_at<>"
            f"'{new_version_sentinel}' FROM ci_governance_events "
            f"WHERE event_id='{event_id}';",
        )
        == "1",
        "a semantic document version must advance event.updated_at",
    )
    checkpoint_auth, _ = request_json(
        base_url,
        f"api.php/api/v2/ops/connectors/{SEC_CONNECTOR_ID}/checkpoint",
        expected_status=401,
    )
    require(
        checkpoint_auth.get("error") == "bearer_token_required",
        repr(checkpoint_auth),
    )
    checkpoint, _ = request_json(
        base_url,
        f"api.php/api/v2/ops/connectors/{SEC_CONNECTOR_ID}/checkpoint",
        token=OPS_TOKEN,
    )
    checkpoint_data = checkpoint.get("data", {})
    require(
        checkpoint_data.get("connector_id") == SEC_CONNECTOR_ID
        and checkpoint_data.get("cursor_json", {}).get("schema_version") == 1
        and checkpoint_data.get("cursor_json", {}).get("window_end_exclusive")
        == "2026-07-24"
        and checkpoint_data.get("cursor_json", {}).get("batch_id")
        == complete_batch
        and checkpoint_data.get("code_revision") == CODE_REVISION,
        repr(checkpoint),
    )
    checkpoint_before_historical = mysql_execute(
        mysql_container_id,
        "SELECT CONCAT(cursor_json,'|',last_success_at,'|',last_checked_at,"
        "'|',last_observed_at,'|',code_revision) "
        "FROM ci_source_connectors "
        f"WHERE connector_id='{SEC_CONNECTOR_ID}';",
    )
    historical_batch = (
        "global-batch:"
        + hashlib.sha256(b"php73-v2-historical-no-rewind").hexdigest()
    )
    historical_payload = empty_chunk_payload(
        rights_revision=rights_revision,
        idempotency_key="php73-v2-historical-no-rewind",
        retrieved_at=utc_text(now + timedelta(seconds=2)),
        batch_id=historical_batch,
        index=1,
        count=1,
    )
    historical_payload["envelope"]["chunk"]["window_start"] = "2026-07-20"
    historical_payload["envelope"]["chunk"]["window_end_exclusive"] = (
        "2026-07-21"
    )
    historical_ingest, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=historical_payload,
    )
    require(
        historical_ingest.get("data", {}).get("idempotent") is False
        and mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_global_ingest_receipts "
            f"WHERE batch_id='{historical_batch}';",
        )
        == "1"
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT(cursor_json,'|',last_success_at,'|',last_checked_at,"
            "'|',last_observed_at,'|',code_revision) "
            "FROM ci_source_connectors "
            f"WHERE connector_id='{SEC_CONNECTOR_ID}';",
        )
        == checkpoint_before_historical,
        repr(historical_ingest),
    )
    versions = mysql_execute(
        mysql_container_id,
        "SELECT version_no,content_hash,correction_of_document_id IS NOT NULL,"
        "publication_status FROM ci_documents "
        "WHERE source_class='official_disclosure' "
        f"AND external_id='{first_record['external_id']}' "
        "ORDER BY version_no;",
    ).splitlines()
    require(len(versions) == 2, repr(versions))
    first_version = versions[0].split("\t")
    second_version = versions[1].split("\t")
    require(
        first_version[0] == "1"
        and first_version[1] == first_record["content_hash"]
        and first_version[2] == "0"
        and second_version[0] == "2"
        and second_version[1] == second_record["content_hash"]
        and second_version[2] == "1"
        and first_version[3] == second_version[3] == "draft",
        repr(versions),
    )

    editorial_sentinel = "2002-02-02 02:02:02"
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_governance_events "
        f"SET updated_at='{editorial_sentinel}' "
        f"WHERE event_id='{event_id}';",
    )
    review_queue, _ = request_json(
        base_url,
        "api.php/api/v2/admin/review-queue?country=US&limit=10",
        token=EDITOR_TOKEN,
    )
    queue_items = review_queue.get("data", {}).get("items", [])
    require(
        isinstance(queue_items, list)
        and len(queue_items) == 1
        and queue_items[0].get("event_id") == event_id
        and queue_items[0].get("title") == second_title
        and queue_items[0].get("visible_evidence_count") == 2,
        repr(review_queue),
    )
    expected_updated_at = require_rfc3339_utc(
        queue_items[0]["updated_at"],
        "review_queue.items[0].updated_at",
    )
    reviewed, _ = request_json(
        base_url,
        f"api.php/api/v2/admin/events/{event_id}/review",
        method="POST",
        token=EDITOR_TOKEN,
        payload={
            "decision": "approve",
            "expected_updated_at": expected_updated_at,
            "reason": "CI editor verified identity and official evidence.",
            "identity_action": "reported beneficial ownership",
            "identity_target": "voting securities",
            "identity_effective_at": filed_at,
            "identity_deadline_at": None,
            "importance": "high",
            "summary": "The reporting person amended an official beneficial ownership filing.",
            "current_status": "official filing active",
            "actor": {
                "actor_id": "actor:ci:sec-filer",
                "display_name": "CI Reporting Person",
                "actor_type": "institutional_investor",
                "actor_role": "reporting_person",
                "country_code": "US",
            },
        },
    )
    require(
        reviewed.get("data", {}).get("decision") == "approved"
        and reviewed.get("data", {}).get("published") is True
        and str(reviewed.get("data", {}).get("comparison_key", "")).startswith(
            "global:"
        ),
        repr(reviewed),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT updated_at<>"
            f"'{editorial_sentinel}' FROM ci_governance_events "
            f"WHERE event_id='{event_id}';",
        )
        == "1",
        "an editorial approval must advance event.updated_at",
    )
    attach_public_and_telegram_evidence(mysql_container_id, event_id)
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_governance_events "
        "SET updated_at=DATE_SUB(UTC_TIMESTAMP(),INTERVAL 1 DAY) "
        f"WHERE event_id='{event_id}';",
    )
    stable_live_updated_at_mysql = mysql_execute(
        mysql_container_id,
        "SELECT updated_at FROM ci_governance_events "
        f"WHERE event_id='{event_id}';",
    )
    stable_live_updated_at = (
        stable_live_updated_at_mysql.replace(" ", "T") + "Z"
    )

    preview = transition(
        base_url,
        "preview",
        0,
        "CI opens protected global terminal preview.",
    )
    require(
        preview.get("changed") is True
        and preview.get("release_state") == "preview"
        and preview.get("state_version") == 1,
        repr(preview),
    )
    v1_preview = transition_v1(
        base_url,
        "preview",
        v1_initial_version,
        "CI opens the protected v1 compatibility preview.",
    )
    require(
        v1_preview.get("changed") is True
        and v1_preview.get("release_state") == "preview"
        and v1_preview.get("state_version") == v1_preview_version,
        repr(v1_preview),
    )
    preview_current = json.loads(
        json.dumps(current_payload, ensure_ascii=False)
    )
    preview_current["expected_release_state"] = "preview"
    preview_observed = datetime.now(timezone.utc).replace(
        microsecond=0
    ) + timedelta(seconds=1)
    preview_current["envelope"]["retrieved_at"] = utc_text(preview_observed)
    preview_current["envelope"]["records"][0]["first_observed_at"] = utc_text(
        preview_observed
    )
    require(
        bind_classified_ingest_key(
            preview_current,
            namespace="global-ingest-v2-current",
        )
        == current_key,
        "release boundary must not change current content identity",
    )
    missing_preview_write_token, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=preview_current,
        expected_status=401,
    )
    require(
        missing_preview_write_token.get("error")
        == "ingest_preview_token_required",
        repr(missing_preview_write_token),
    )
    invalid_preview_write_token, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        preview_token="not-the-preview-token",
        payload=preview_current,
        expected_status=403,
    )
    require(
        invalid_preview_write_token.get("error")
        == "invalid_ingest_preview_token",
        repr(invalid_preview_write_token),
    )
    stale_boundary_current = json.loads(
        json.dumps(preview_current, ensure_ascii=False)
    )
    stale_boundary_current["expected_release_state"] = "closed"
    stale_boundary_rejected, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=stale_boundary_current,
        expected_status=409,
    )
    require(
        stale_boundary_rejected.get("error")
        == "global_ingest_release_state_mismatch",
        repr(stale_boundary_rejected),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_connectors SET "
        "last_checked_at='2000-01-01 00:00:00',"
        "last_success_at='2000-01-01 00:00:00',"
        "last_observed_at='2000-01-01 00:00:00',"
        "updated_at='2000-01-01 00:00:00' "
        f"WHERE connector_id='{SEC_CONNECTOR_ID}';",
    )
    preview_bound_current, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        preview_token=PREVIEW_TOKEN,
        payload=preview_current,
    )
    require(
        preview_bound_current.get("data", {}).get("idempotent") is True
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT(last_checked_at<>'2000-01-01 00:00:00','|',"
            "last_success_at<>'2000-01-01 00:00:00') "
            "FROM ci_source_connectors "
            f"WHERE connector_id='{SEC_CONNECTOR_ID}';",
        )
        == "1|1",
        repr(preview_bound_current),
    )
    server_uuid, database_name = mysql_execute(
        mysql_container_id,
        "SELECT LOWER(@@server_uuid),DATABASE()",
    ).split("\t")
    backend_binding_id = hashlib.sha256(
        f"mysql8\n{server_uuid}\n{database_name}\nci_".encode()
    ).hexdigest()
    preview_dart_payload = dart_guarded_metadata_payload(
        company_id="00999983",
        external_id="20260727999831",
        expected_release_state="preview",
        rights_revision=dart_eligibility["rights_revision"],
        contract_revision=dart_eligibility["contract_revision"],
        backend_binding_id=backend_binding_id,
    )
    preview_dart_write = request_dart_hmac_write(
        base_url,
        preview_dart_payload,
        expected_status=200,
    )
    require(
        preview_dart_write.get("ok") is True
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT("
            "(SELECT COUNT(*) FROM ci_companies WHERE company_id='00999983'),"
            "(SELECT COUNT(*) FROM ci_documents "
            "WHERE document_id='dart:20260727999831' AND body_text IS NULL))",
        )
        == "11",
        "matching preview state must accept metadata-only DART writes",
    )
    preview_state_mismatch_payload = dart_guarded_metadata_payload(
        company_id="00999984",
        external_id="20260727999841",
        expected_release_state="live",
        rights_revision=dart_eligibility["rights_revision"],
        contract_revision=dart_eligibility["contract_revision"],
        backend_binding_id=backend_binding_id,
    )
    preview_state_mismatch = request_dart_hmac_write(
        base_url,
        preview_state_mismatch_payload,
        expected_status=409,
    )
    require(
        preview_state_mismatch.get("error") == "dart_release_state_mismatch"
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT("
            "(SELECT COUNT(*) FROM ci_companies WHERE company_id='00999984'),"
            "(SELECT COUNT(*) FROM ci_documents "
            "WHERE document_id='dart:20260727999841'))",
        )
        == "00",
        "signed preview/live mismatch must leave MySQL unchanged",
    )
    preview_state, _ = request_json(
        base_url,
        "api.php/api/v2/admin/release-state",
        token=ADMIN_TOKEN,
    )
    preview_state_data = preview_state.get("data", {})
    require_rfc3339_utc(
        preview_state_data.get("updated_at"),
        "preview_release_state.updated_at",
    )
    preview_history = preview_state_data.get("history", [])
    require(
        isinstance(preview_history, list) and len(preview_history) >= 2,
        repr(preview_state),
    )
    for history_index, history_entry in enumerate(preview_history):
        for timestamp_field in ("cutover_at", "sunset_at"):
            require_nullable_rfc3339_utc(
                history_entry.get(timestamp_field),
                (f"preview_release_state.history[{history_index}].{timestamp_field}"),
            )
        require_rfc3339_utc(
            history_entry.get("created_at"),
            f"preview_release_state.history[{history_index}].created_at",
        )
    preview_missing, _ = request_json(
        base_url,
        "api.php/api/v2/events",
        expected_status=401,
    )
    require(
        preview_missing.get("error") == "preview_token_required",
        repr(preview_missing),
    )
    preview_events, preview_headers = request_json(
        base_url,
        "api.php/api/v2/events?country=US&limit=10",
        token=PREVIEW_TOKEN,
    )
    visible_items = preview_events.get("data", {}).get("items", [])
    require(
        len(visible_items) == 1
        and visible_items[0].get("event_id") == event_id
        and visible_items[0].get("title") == second_title
        and visible_items[0].get("importance") == "high"
        and visible_items[0].get("official_evidence_count") == 3
        and visible_items[0].get("source_url") == first_record["original_url"]
        and visible_items[0].get("updated_at") == stable_live_updated_at,
        repr(preview_events),
    )
    require_public_event_timestamps(
        visible_items[0],
        "preview_events.items[0]",
    )
    require(
        "private" in preview_headers.get("Cache-Control", "").lower(),
        repr(preview_headers),
    )

    # The automatic connector window intentionally overlaps a completed day.
    # A new receipt for the same semantic document may lower first_observed_at,
    # but it must not make the already-published canonical event look changed
    # or move it in the Live stream.
    unchanged_record = json.loads(json.dumps(second_record))
    unchanged_first_observed_at = utc_text(now - timedelta(hours=1))
    unchanged_retrieved_at = utc_text(now + timedelta(minutes=2))
    unchanged_record["first_observed_at"] = unchanged_first_observed_at
    unchanged_payload = ingest_payload(
        rights_revision=rights_revision,
        idempotency_key="php73-v2-sec-unchanged-overlap",
        record=unchanged_record,
        retrieved_at=unchanged_retrieved_at,
    )
    unchanged_ingest, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=unchanged_payload,
    )
    require(
        unchanged_ingest.get("data", {}).get("acknowledged_count") == 1
        and unchanged_ingest.get("data", {}).get("idempotent") is False,
        repr(unchanged_ingest),
    )
    unchanged_live, _ = request_json(
        base_url,
        "api.php/api/v2/live?country=US&limit=10",
        token=PREVIEW_TOKEN,
    )
    unchanged_live_items = unchanged_live.get("data", {}).get("items", [])
    require(
        len(unchanged_live_items) == 1
        and unchanged_live_items[0].get("event_id") == event_id
        and unchanged_live_items[0].get("updated_at")
        == stable_live_updated_at
        and unchanged_live_items[0].get("first_observed_at")
        == unchanged_first_observed_at,
        repr(unchanged_live),
    )
    unchanged_event_state = mysql_execute(
        mysql_container_id,
        "SELECT updated_at,publication_status,review_status,identity_status "
        "FROM ci_governance_events "
        f"WHERE event_id='{event_id}';",
    )
    require(
        unchanged_event_state
        == (
            f"{stable_live_updated_at_mysql}"
            "\tpublished\tapproved\tcomplete"
        )
        and mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_documents "
            f"WHERE source_right_id='{SEC_RIGHT_ID}' "
            f"AND external_id='{second_record['external_id']}';",
        )
        == "2",
        unchanged_event_state,
    )

    event_detail, _ = request_json(
        base_url,
        f"api.php/api/v2/events/{event_id}",
        token=PREVIEW_TOKEN,
    )
    detail_documents = event_detail.get("data", {}).get("documents", [])
    detail_actors = event_detail.get("data", {}).get("actors", [])
    detail_observations = event_detail.get("data", {}).get(
        "observations",
        [],
    )
    require_public_event_timestamps(
        event_detail.get("data", {}).get("event"),
        "event_detail.event",
    )
    for index, document in enumerate(detail_documents):
        for timestamp_field in ("filed_at", "published_at"):
            require_nullable_rfc3339_utc(
                document.get(timestamp_field),
                f"event_detail.documents[{index}].{timestamp_field}",
            )
    for index, observation in enumerate(detail_observations):
        require_rfc3339_utc(
            observation.get("first_observed_at"),
            f"event_detail.observations[{index}].first_observed_at",
        )
        require_rfc3339_utc(
            observation.get("observed_at"),
            f"event_detail.observations[{index}].observed_at",
        )
    require(
        len(detail_documents) == 3
        and detail_actors
        == [
            {
                "actor_id": "actor:ci:sec-filer",
                "display_name": "CI Reporting Person",
                "display_name_en": None,
                "actor_type": "institutional_investor",
                "country_code": "US",
                "actor_role": "reporting_person",
            }
        ]
        and all(
            document.get("source_class")
            not in {"licensed_telegram", "authorized_telegram"}
            for document in detail_documents
        )
        and TELEGRAM_URL not in json.dumps(event_detail, ensure_ascii=False),
        repr(event_detail),
    )
    for search_query in (
        "official filing active",
        "large_ownership",
        "AAPL",
        "0000320193",
        "CI Reporting Person",
        "CI alternate official filing",
        "SC 13D",
    ):
        search_result, _ = request_json(
            base_url,
            "api.php/api/v2/search?"
            + urllib.parse.urlencode({"q": search_query, "country": "US"}),
            token=PREVIEW_TOKEN,
        )
        require(
            [
                item.get("event_id")
                for item in search_result.get("data", {}).get("items", [])
            ]
            == [event_id],
            search_query + ": " + repr(search_result),
        )
    for literal_query in ("CI%", "CI_", "CI\\"):
        literal_result, _ = request_json(
            base_url,
            "api.php/api/v2/search?"
            + urllib.parse.urlencode({"q": literal_query, "country": "US"}),
            token=PREVIEW_TOKEN,
        )
        require(
            literal_result.get("data", {}).get("items") == [],
            literal_query + ": " + repr(literal_result),
        )
    private_search, _ = request_json(
        base_url,
        "api.php/api/v2/search?"
        + urllib.parse.urlencode({"q": "CI Telegram signal", "country": "US"}),
        token=PREVIEW_TOKEN,
    )
    require(
        private_search.get("data", {}).get("items") == [],
        repr(private_search),
    )

    cutoff_at = utc_text(now - timedelta(seconds=1))
    duplicate_lane_brief, _ = request_json(
        base_url,
        "api.php/api/v2/admin/briefs",
        method="POST",
        token=EDITOR_TOKEN,
        payload={
            "edition": "global",
            "cutoff_at": cutoff_at,
            "build_sha": CODE_REVISION,
            "empty_reason": None,
            "items": [
                {
                    "event_id": event_id,
                    "lane": "top",
                    "position_no": 1,
                    "selection_reason": "CI top selection.",
                },
                {
                    "event_id": event_id,
                    "lane": "watch",
                    "position_no": 1,
                    "selection_reason": "CI duplicate watch selection.",
                },
            ],
        },
        expected_status=400,
    )
    require(
        duplicate_lane_brief.get("error") == "brief_validation_failed"
        and "duplicate lane position or event"
        in str(duplicate_lane_brief.get("detail")),
        repr(duplicate_lane_brief),
    )
    published_brief, _ = request_json(
        base_url,
        "api.php/api/v2/admin/briefs",
        method="POST",
        token=EDITOR_TOKEN,
        payload={
            "edition": "global",
            "cutoff_at": cutoff_at,
            "build_sha": CODE_REVISION,
            "empty_reason": None,
            "items": [
                {
                    "event_id": event_id,
                    "lane": "top",
                    "position_no": 1,
                    "selection_reason": "CI editor selected official high-importance event.",
                }
            ],
        },
    )
    brief_id = published_brief.get("data", {}).get("brief_id")
    require(
        isinstance(brief_id, str)
        and published_brief.get("data", {}).get("top_count") == 1
        and published_brief.get("data", {}).get("published") is True,
        repr(published_brief),
    )
    snapshot_has_url = mysql_execute(
        mysql_container_id,
        "SELECT JSON_CONTAINS_PATH(event_snapshot_json,'one','$.source_url') "
        "FROM ci_brief_items "
        f"WHERE brief_id='{brief_id}' AND event_id='{event_id}';",
    )
    require(snapshot_has_url == "0", snapshot_has_url)
    brief_replay, _ = request_json(
        base_url,
        "api.php/api/v2/admin/briefs",
        method="POST",
        token=EDITOR_TOKEN,
        payload={
            "edition": "global",
            "cutoff_at": cutoff_at,
            "build_sha": CODE_REVISION,
            "empty_reason": None,
            "items": [
                {
                    "event_id": event_id,
                    "lane": "top",
                    "position_no": 1,
                    "selection_reason": "CI editor selected official high-importance event.",
                }
            ],
        },
    )
    require(
        brief_replay.get("data", {}).get("idempotent") is True,
        repr(brief_replay),
    )

    latest, _ = request_json(
        base_url,
        "api.php/api/v2/briefs/latest?edition=global",
        token=PREVIEW_TOKEN,
    )
    latest_data = latest.get("data", {})
    require(
        latest_data.get("brief_id") == brief_id
        and len(latest_data.get("top", [])) == 1
        and latest_data["top"][0].get("title") == second_title
        and latest_data["top"][0].get("importance") == "high"
        and latest_data["top"][0].get("source_url") == first_record["original_url"],
        repr(latest),
    )
    for timestamp_field in (
        "cutoff_at",
        "published_at",
        "last_updated_at",
    ):
        require_rfc3339_utc(
            latest_data.get(timestamp_field),
            f"latest_brief.{timestamp_field}",
        )
    require_public_event_timestamps(
        latest_data["top"][0],
        "latest_brief.top[0]",
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_governance_events "
        "SET title='CI mutable current event title',updated_at=DATE_ADD(UTC_TIMESTAMP(),INTERVAL 2 SECOND) "
        f"WHERE event_id='{event_id}';",
    )
    frozen, _ = request_json(
        base_url,
        "api.php/api/v2/briefs/latest?edition=global",
        token=PREVIEW_TOKEN,
    )
    require(
        frozen.get("data", {}).get("top", [])[0].get("title") == second_title,
        repr(frozen),
    )

    # Neither administrative release-state endpoint may bypass the protected
    # cutover authorization, even with the administrator credential.
    direct_v2_live, _ = request_json(
        base_url,
        "api.php/api/v2/admin/release-state",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "release_state": "live",
            "expected_version": 1,
            "reason": "CI rejects direct v2 preview promotion.",
        },
        expected_status=409,
    )
    require(
        direct_v2_live.get("error") == "protected_atomic_cutover_required",
        repr(direct_v2_live),
    )
    direct_v1_live, _ = request_json(
        base_url,
        "api.php/api/v1/admin/release-state",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "release_state": "live",
            "expected_version": v1_preview_version,
            "reason": "CI rejects direct v1 preview promotion.",
        },
        expected_status=409,
    )
    require(
        direct_v1_live.get("error") == "protected_atomic_cutover_required",
        repr(direct_v1_live),
    )
    admin_cannot_authorize, _ = request_json(
        base_url,
        "api.php/api/v2/admin/release-authorizations",
        method="POST",
        token=ADMIN_TOKEN,
        payload={},
        expected_status=403,
    )
    require(
        admin_cannot_authorize.get("error") == "insufficient_role",
        repr(admin_cannot_authorize),
    )
    release_cannot_activate, _ = request_json(
        base_url,
        "api.php/api/v2/admin/cutover",
        method="POST",
        token=RELEASE_AUTHORIZER_TOKEN,
        payload={},
        expected_status=403,
    )
    require(
        release_cannot_activate.get("error") == "insufficient_role",
        repr(release_cannot_activate),
    )

    # Expiry and every immutable binding fail closed before a valid
    # authorization is consumed.
    expired_nonce = "1" * 64
    expired_authorization = issue_release_authorization(
        base_url,
        nonce=expired_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        expires_at=utc_text(datetime.now(timezone.utc) + timedelta(minutes=10)),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_release_authorizations "
        "SET expires_at=DATE_SUB(UTC_TIMESTAMP(),INTERVAL 1 SECOND) "
        f"WHERE authorization_id='{expired_authorization['authorization_id']}';",
    )
    expired_cutover = atomic_cutover(
        base_url,
        nonce=expired_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        expected_status=410,
    )
    require(
        expired_cutover.get("error") == "release_authorization_expired",
        repr(expired_cutover),
    )

    release_nonce = "2" * 64
    authorization = issue_release_authorization(
        base_url,
        nonce=release_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        expires_at=utc_text(datetime.now(timezone.utc) + timedelta(minutes=10)),
    )
    wrong_sha = atomic_cutover(
        base_url,
        nonce=release_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        candidate_sha="c" * 40,
        expected_status=409,
    )
    require(
        wrong_sha.get("error") == "release_candidate_sha_mismatch",
        repr(wrong_sha),
    )
    wrong_nonce = atomic_cutover(
        base_url,
        nonce="3" * 64,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        expected_status=409,
    )
    require(
        wrong_nonce.get("error") == "release_authorization_invalid",
        repr(wrong_nonce),
    )
    wrong_digest = atomic_cutover(
        base_url,
        nonce=release_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        evidence_digest="sha256:" + ("c" * 64),
        expected_status=409,
    )
    require(
        wrong_digest.get("error") == "release_authorization_binding_mismatch",
        repr(wrong_digest),
    )

    # Required-source validation is independent of published documents. The
    # three still-pending non-SEC required grants block cutover without consuming
    # the otherwise valid one-time authorization.
    pending_sources = atomic_cutover(
        base_url,
        nonce=release_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        expected_status=409,
    )
    require(
        pending_sources.get("error") == "required_alpha_sources_invalid"
        and pending_sources.get("required_connector_count") == 4
        and pending_sources.get("optional_connector_count") == 2
        and pending_sources.get("invalid_required_connector_count", 0) >= 3,
        repr(pending_sources),
    )
    require_cutover_not_consumed(
        mysql_container_id,
        authorization["authorization_id"],
    )
    activate_required_alpha_cutover_sources(mysql_container_id)

    # Cutover locks and rechecks current readiness rather than trusting an
    # evidence artifact created before a source became stale.
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_connectors SET "
        "last_success_at=UTC_TIMESTAMP()-INTERVAL 46 MINUTE "
        "WHERE connector_id='connector:us:sec-edgar';",
    )
    stale_source = atomic_cutover(
        base_url,
        nonce=release_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        expected_status=409,
    )
    require(
        stale_source.get("error") == "required_alpha_sources_invalid"
        and any(
            item.get("connector_id") == "connector:us:sec-edgar"
            and "last_success_missing_or_stale" in item.get("reasons", [])
            for item in stale_source.get("invalid_sources", [])
            if isinstance(item, dict)
        ),
        repr(stale_source),
    )
    require_cutover_not_consumed(
        mysql_container_id,
        authorization["authorization_id"],
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_connectors SET last_success_at=UTC_TIMESTAMP() "
        "WHERE connector_id='connector:us:sec-edgar';",
    )

    # A fresh run timestamp cannot conceal an old or malformed SEC intraday
    # cursor. The embedded official-feed observation time is a separate gate.
    set_sec_current_cursor(
        mysql_container_id,
        datetime.now(timezone.utc) - timedelta(minutes=46),
    )
    stale_sec_cursor = atomic_cutover(
        base_url,
        nonce=release_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        expected_status=409,
    )
    require(
        stale_sec_cursor.get("error") == "required_alpha_sources_invalid"
        and any(
            item.get("connector_id") == "connector:us:sec-edgar"
            and "intraday_cursor_missing_or_stale" in item.get("reasons", [])
            for item in stale_sec_cursor.get("invalid_sources", [])
            if isinstance(item, dict)
        ),
        repr(stale_sec_cursor),
    )
    require_cutover_not_consumed(
        mysql_container_id,
        authorization["authorization_id"],
    )
    set_sec_current_cursor(mysql_container_id, datetime.now(timezone.utc))

    # Link-only readiness needs a recent explicit observation with a matching
    # non-zero ACK, even when the country has no published event.
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_connectors SET last_acknowledged_count=0 "
        "WHERE connector_id='connector:ca:issuer-ir';",
    )
    unacknowledged_link = atomic_cutover(
        base_url,
        nonce=release_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        expected_status=409,
    )
    require(
        unacknowledged_link.get("error") == "required_alpha_sources_invalid"
        and any(
            item.get("connector_id") == "connector:ca:issuer-ir"
            and "link_observation_not_acknowledged" in item.get("reasons", [])
            for item in unacknowledged_link.get("invalid_sources", [])
            if isinstance(item, dict)
        ),
        repr(unacknowledged_link),
    )
    require_cutover_not_consumed(
        mysql_container_id,
        authorization["authorization_id"],
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_connectors SET last_acknowledged_count=1 "
        "WHERE connector_id='connector:ca:issuer-ir';",
    )

    # A link-only country has no published document in this fixture. Its
    # connector identity must still be locked and validated at cutover.
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_connectors SET source_key='issuer-ir-corrupted',"
        "updated_at=UTC_TIMESTAMP() "
        "WHERE connector_id='connector:ca:issuer-ir';",
    )
    empty_country_identity = atomic_cutover(
        base_url,
        nonce=release_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        expected_status=409,
    )
    invalid_identity_sources = empty_country_identity.get("invalid_sources", [])
    require(
        empty_country_identity.get("error") == "required_alpha_sources_invalid"
        and any(
            item.get("connector_id") == "connector:ca:issuer-ir"
            and "connector_identity_mismatch" in item.get("reasons", [])
            for item in invalid_identity_sources
            if isinstance(item, dict)
        ),
        repr(empty_country_identity),
    )
    require_cutover_not_consumed(
        mysql_container_id,
        authorization["authorization_id"],
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_connectors SET source_key='issuer-ir',"
        "updated_at=UTC_TIMESTAMP() "
        "WHERE connector_id='connector:ca:issuer-ir';",
    )

    # Optional JP remains unavailable, but its stored identity is still locked
    # so a corrupted connector cannot masquerade as the declared keyless mode.
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_connectors SET source_key='edinet-corrupted',"
        "updated_at=UTC_TIMESTAMP() "
        "WHERE connector_id='connector:jp:edinet';",
    )
    optional_identity = atomic_cutover(
        base_url,
        nonce=release_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        expected_status=409,
    )
    require(
        optional_identity.get("error") == "required_alpha_sources_invalid"
        and optional_identity.get("invalid_required_connector_count") == 0
        and optional_identity.get("invalid_optional_connector_count") == 1
        and any(
            item.get("connector_id") == "connector:jp:edinet"
            and "connector_identity_mismatch" in item.get("reasons", [])
            for item in optional_identity.get("invalid_sources", [])
            if isinstance(item, dict)
        ),
        repr(optional_identity),
    )
    require_cutover_not_consumed(
        mysql_container_id,
        authorization["authorization_id"],
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_connectors SET source_key='edinet',"
        "updated_at=UTC_TIMESTAMP() "
        "WHERE connector_id='connector:jp:edinet';",
    )

    # Optional countries must be truly dormant. Activity cannot be rewritten
    # to zero and presented as an intentionally unavailable source.
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_connectors SET connector_status='active',"
        "last_checked_at=UTC_TIMESTAMP(),last_success_at=UTC_TIMESTAMP(),"
        "last_observed_at=UTC_TIMESTAMP(),cursor_json='unexpected-cursor',"
        "last_raw_count=100,last_acknowledged_count=100,"
        "updated_at=UTC_TIMESTAMP() "
        "WHERE connector_id='connector:jp:edinet';",
    )
    optional_activity_status, _ = request_json(
        base_url,
        "api.php/api/v2/sources/status?country=JP",
        token=PREVIEW_TOKEN,
    )
    optional_activity_items = optional_activity_status.get("data", {}).get(
        "items",
        [],
    )
    require(
        len(optional_activity_items) == 1
        and optional_activity_items[0].get("status")
        == "blocked_policy_activity"
        and optional_activity_items[0].get("collect_status")
        == "blocked_policy_activity"
        and optional_activity_items[0].get("public_status")
        == "blocked_policy_activity"
        and optional_activity_items[0].get("raw_count") == 100
        and optional_activity_items[0].get("acknowledged_count") == 100
        and optional_activity_items[0].get("last_success_at") is not None
        and optional_activity_items[0].get("last_checked_at") is not None
        and optional_activity_items[0].get("last_observed_at") is not None
        and optional_activity_items[0].get("public_ready") is False,
        repr(optional_activity_status),
    )
    optional_activity = atomic_cutover(
        base_url,
        nonce=release_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        expected_status=409,
    )
    require(
        optional_activity.get("error") == "required_alpha_sources_invalid"
        and optional_activity.get("invalid_required_connector_count") == 0
        and optional_activity.get("invalid_optional_connector_count") == 1
        and any(
            item.get("connector_id") == "connector:jp:edinet"
            and "connector_policy_activity" in item.get("reasons", [])
            for item in optional_activity.get("invalid_sources", [])
            if isinstance(item, dict)
        ),
        repr(optional_activity),
    )
    require_cutover_not_consumed(
        mysql_container_id,
        authorization["authorization_id"],
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_connectors SET connector_status='pending_rights',"
        "last_checked_at=NULL,last_success_at=NULL,last_observed_at=NULL,"
        "cursor_json=NULL,last_raw_count=0,last_acknowledged_count=0,"
        "last_error_class='source_right_required',updated_at=UTC_TIMESTAMP() "
        "WHERE connector_id='connector:jp:edinet';",
    )

    # An active SourceRight also violates the declared dormant policy even if
    # the connector itself has not emitted a receipt.
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET status='active',revoked_at=NULL,"
        "updated_at=UTC_TIMESTAMP() "
        "WHERE source_right_id='official:edinet';",
    )
    optional_right_active = atomic_cutover(
        base_url,
        nonce=release_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        expected_status=409,
    )
    require(
        optional_right_active.get("error") == "required_alpha_sources_invalid"
        and any(
            item.get("connector_id") == "connector:jp:edinet"
            and "source_right_policy_active" in item.get("reasons", [])
            for item in optional_right_active.get("invalid_sources", [])
            if isinstance(item, dict)
        ),
        repr(optional_right_active),
    )
    require_cutover_not_consumed(
        mysql_container_id,
        authorization["authorization_id"],
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET status='pending',revoked_at=NULL,"
        "updated_at=UTC_TIMESTAMP() "
        "WHERE source_right_id='official:edinet';",
    )

    # A revoked SourceRight for another document-empty link-only country must
    # fail the collect and public eligibility checks under the same locks.
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET status='revoked',"
        "revoked_at=UTC_TIMESTAMP(),updated_at=UTC_TIMESTAMP() "
        "WHERE source_right_id='official:asic-register';",
    )
    empty_country_revoked = atomic_cutover(
        base_url,
        nonce=release_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        expected_status=409,
    )
    revoked_sources = empty_country_revoked.get("invalid_sources", [])
    require(
        empty_country_revoked.get("error") == "required_alpha_sources_invalid"
        and any(
            item.get("connector_id") == "connector:au:asic-register"
            and "collect_not_allowed" in item.get("reasons", [])
            and "public_redistribution_not_allowed" in item.get("reasons", [])
            for item in revoked_sources
            if isinstance(item, dict)
        ),
        repr(empty_country_revoked),
    )
    require_cutover_not_consumed(
        mysql_container_id,
        authorization["authorization_id"],
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET status='active',revoked_at=NULL,"
        "updated_at=UTC_TIMESTAMP() "
        "WHERE source_right_id='official:asic-register';",
    )

    # Keep the pre-existing document rights guard: a non-required public
    # evidence grant can still block activation after required sources pass.
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET source_key='sec-ci-alternate-corrupted',"
        "updated_at=UTC_TIMESTAMP() "
        f"WHERE source_right_id='{ALTERNATE_RIGHT_ID}';",
    )
    published_document_identity = atomic_cutover(
        base_url,
        nonce=release_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        expected_status=409,
    )
    require(
        published_document_identity.get("error") == "current_source_rights_invalid"
        and published_document_identity.get(
            "v2_invalid_source_right_document_count",
            0,
        )
        >= 1,
        repr(published_document_identity),
    )
    require_cutover_not_consumed(
        mysql_container_id,
        authorization["authorization_id"],
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET source_key='sec-ci-alternate',"
        "updated_at=UTC_TIMESTAMP() "
        f"WHERE source_right_id='{ALTERNATE_RIGHT_ID}';",
    )
    live_response = atomic_cutover(
        base_url,
        nonce=release_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
    )
    live = live_response.get("data", {})
    require(
        live.get("changed") is True
        and live.get("states", {})
        .get("governance_v1", {})
        .get("release_state")
        == "live"
        and live.get("states", {})
        .get("governance_v1", {})
        .get("state_version")
        == v1_live_version
        and live.get("states", {})
        .get("global_terminal_v2", {})
        .get("release_state")
        == "live"
        and live.get("states", {})
        .get("global_terminal_v2", {})
        .get("state_version")
        == 2
        and live.get("authorization_id") == authorization.get("authorization_id"),
        repr(live),
    )
    server_uuid, database_name = mysql_execute(
        mysql_container_id,
        "SELECT LOWER(@@server_uuid),DATABASE()",
    ).split("\t")
    backend_binding_id = hashlib.sha256(
        f"mysql8\n{server_uuid}\n{database_name}\nci_".encode()
    ).hexdigest()
    live_dart_payload = dart_guarded_metadata_payload(
        company_id="00999985",
        external_id="20260727999851",
        expected_release_state="live",
        rights_revision=dart_eligibility["rights_revision"],
        contract_revision=dart_eligibility["contract_revision"],
        backend_binding_id=backend_binding_id,
    )
    live_dart_write = request_dart_hmac_write(
        base_url,
        live_dart_payload,
        expected_status=200,
    )
    require(
        live_dart_write.get("ok") is True
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT("
            "(SELECT COUNT(*) FROM ci_companies WHERE company_id='00999985'),"
            "(SELECT COUNT(*) FROM ci_documents "
            "WHERE document_id='dart:20260727999851' AND body_text IS NULL))",
        )
        == "11",
        "matching live state must keep DART freshness writable",
    )
    authorization_row = mysql_execute(
        mysql_container_id,
        "SELECT CONCAT(candidate_sha,'|',evidence_artifact_digest,'|',"
        "fully_consumed_at IS NOT NULL,'|',v1_consumed_state_version,'|',"
        "v2_consumed_state_version,'|',nonce_sha256=SHA2("
        f"'{release_nonce}',256)) FROM ci_release_authorizations "
        f"WHERE authorization_id='{authorization['authorization_id']}';",
    )
    require(
        authorization_row
        == (
            f"{CODE_REVISION}|{EVIDENCE_ARTIFACT_DIGEST}|1|"
            f"{v1_live_version}|2|1"
        ),
        authorization_row,
    )
    authorization_columns = mysql_execute(
        mysql_container_id,
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema=DATABASE() AND table_name='ci_release_authorizations' "
        "AND column_name='release_nonce';",
    )
    require(authorization_columns == "0", authorization_columns)
    audit_binding_count = mysql_execute(
        mysql_container_id,
        "SELECT COUNT(*) FROM ci_governance_release_audit "
        f"WHERE release_authorization_id='{authorization['authorization_id']}' "
        "AND new_state='live';",
    )
    require(audit_binding_count == "2", audit_binding_count)
    replayed = atomic_cutover(
        base_url,
        nonce=release_nonce,
        expected_v1_version=v1_preview_version,
        expected_v2_version=1,
        expected_status=409,
    )
    require(
        replayed.get("error") == "protected_cutover_requires_preview",
        repr(replayed),
    )
    live_events, live_headers = request_json(
        base_url,
        "api.php/api/v2/events?country=US&limit=10",
    )
    require(
        len(live_events.get("data", {}).get("items", [])) == 1,
        repr(live_events),
    )
    require(
        "public" in live_headers.get("Cache-Control", "").lower(),
        repr(live_headers),
    )

    valid_dart_checkpoint = seed_alpha_automated_evidence(
        mysql_container_id,
        now=now,
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT GROUP_CONCAT(batch_request_count ORDER BY chunk_index),"
            "SUM(request_count) FROM ci_global_ingest_receipts "
            "WHERE connector_id='connector:us:sec-edgar' "
            "AND chunk_count=2 GROUP BY batch_id;",
        )
        == "2,3\t3",
        "interrupted SEC receipt telemetry fixture is missing",
    )
    automated_with_mutation, _ = request_json(
        base_url,
        (
            "api.php/api/v2/ops/alpha-release-evidence?"
            + urllib.parse.urlencode({"code_revision": CODE_REVISION})
        ),
        token=OPS_TOKEN,
    )
    mutated_counts = (
        automated_with_mutation.get("data", {})
        .get("content_integrity", {})
        .get("raw_counts", {})
    )
    require(
        mutated_counts.get("public_event_count") == 1
        and mutated_counts.get("source_title_event_count") == 1
        and mutated_counts.get("source_title_preserved_count") == 0,
        repr(automated_with_mutation),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_governance_events e "
        "JOIN ci_event_documents ed ON ed.event_id=e.event_id "
        "JOIN ci_documents d ON d.document_id=ed.document_id "
        "SET e.title=d.title,e.updated_at=UTC_TIMESTAMP() "
        f"WHERE e.event_id='{event_id}' "
        f"AND d.source_right_id='{SEC_RIGHT_ID}' "
        f"AND d.external_id='{first_record['external_id']}' "
        "AND d.version_no=("
        "SELECT MAX(latest.version_no) FROM ci_documents latest "
        f"WHERE latest.source_right_id='{SEC_RIGHT_ID}' "
        f"AND latest.external_id='{first_record['external_id']}');",
    )
    restored_source_title = mysql_execute(
        mysql_container_id,
        "SELECT COUNT(*) FROM ci_governance_events e "
        "JOIN ci_event_documents ed ON ed.event_id=e.event_id "
        "JOIN ci_documents d ON d.document_id=ed.document_id "
        f"WHERE e.event_id='{event_id}' "
        f"AND d.source_right_id='{SEC_RIGHT_ID}' "
        f"AND d.external_id='{first_record['external_id']}' "
        "AND d.version_no=("
        "SELECT MAX(latest.version_no) FROM ci_documents latest "
        f"WHERE latest.source_right_id='{SEC_RIGHT_ID}' "
        f"AND latest.external_id='{first_record['external_id']}') "
        "AND BINARY e.title=BINARY d.title;",
    )
    require(restored_source_title == "1", restored_source_title)
    automated_preserved, _ = request_json(
        base_url,
        (
            "api.php/api/v2/ops/alpha-release-evidence?"
            + urllib.parse.urlencode({"code_revision": CODE_REVISION})
        ),
        token=OPS_TOKEN,
    )
    preserved_data = automated_preserved.get("data", {})
    preserved_counts = preserved_data.get("content_integrity", {}).get("raw_counts", {})
    evidence_windows = [
        window
        for connector in preserved_data.get("connector_coverage", [])
        for window in connector.get("completed_windows", [])
    ]
    require(
        preserved_data.get("evidence_source") == "production_database_export"
        and len(preserved_data.get("connector_coverage", [])) == 2
        and {
            connector.get("connector_family")
            for connector in preserved_data.get("connector_coverage", [])
        }
        == {"dart", "sec-edgar"}
        and all(
            connector.get("successful_window_count") == 30
            and len(connector.get("completed_windows", [])) == 30
            for connector in preserved_data.get("connector_coverage", [])
        )
        and len(evidence_windows) == 60
        and all(
            set(window) == {
                "window_start",
                "window_end_exclusive",
                "raw_count",
                "filtered_out_count",
                "accepted_count",
                "acknowledged_count",
                "status",
                "code_revision",
                "receipt_sha256",
            }
            and window.get("raw_count")
            == window.get("filtered_out_count") + window.get("accepted_count")
            and window.get("acknowledged_count")
            == window.get("accepted_count")
            for window in evidence_windows
        )
        and sum(
            window.get("filtered_out_count") == 0
            for window in evidence_windows
        )
        == 1
        and all(
            window.get("filtered_out_count") in {0, 2}
            for window in evidence_windows
        )
        and preserved_counts.get("source_title_preserved_count") == 1,
        repr(automated_preserved),
    )

    # DART release evidence is bound to the exact apply-job revision and to
    # the canonical Python job fingerprint. Legacy and tampered checkpoints
    # retain valid payload hashes below, so each rejection exercises the job
    # binding rather than the outer checkpoint-integrity check.
    invalid_dart_checkpoints: list[tuple[str, dict[str, Any], str]] = []

    legacy_checkpoint = json.loads(json.dumps(valid_dart_checkpoint))
    legacy_job = legacy_checkpoint["job"]
    legacy_job.pop("fingerprint")
    legacy_job.pop("code_revision")
    legacy_fingerprint = backfill_job_fingerprint(legacy_job)
    legacy_job["fingerprint"] = legacy_fingerprint
    invalid_dart_checkpoints.append(
        ("missing job revision", legacy_checkpoint, legacy_fingerprint)
    )

    wrong_revision_checkpoint = json.loads(json.dumps(valid_dart_checkpoint))
    wrong_revision_job = wrong_revision_checkpoint["job"]
    wrong_revision_job.pop("fingerprint")
    wrong_revision_job["code_revision"] = (
        "0" * 40 if CODE_REVISION != "0" * 40 else "1" * 40
    )
    wrong_revision_fingerprint = backfill_job_fingerprint(wrong_revision_job)
    wrong_revision_job["fingerprint"] = wrong_revision_fingerprint
    invalid_dart_checkpoints.append(
        (
            "wrong job revision",
            wrong_revision_checkpoint,
            wrong_revision_fingerprint,
        )
    )

    combined_sources_checkpoint = json.loads(json.dumps(valid_dart_checkpoint))
    combined_sources_job = combined_sources_checkpoint["job"]
    combined_sources_job.pop("fingerprint")
    combined_sources_job["sources"] = ["dart", "kind"]
    combined_sources_fingerprint = backfill_job_fingerprint(
        combined_sources_job
    )
    combined_sources_job["fingerprint"] = combined_sources_fingerprint
    invalid_dart_checkpoints.append(
        (
            "combined DART KIND sources",
            combined_sources_checkpoint,
            combined_sources_fingerprint,
        )
    )

    row_mismatch_checkpoint = json.loads(json.dumps(valid_dart_checkpoint))
    invalid_dart_checkpoints.append(
        (
            "row fingerprint mismatch",
            row_mismatch_checkpoint,
            hashlib.sha256(b"wrong DART checkpoint row").hexdigest(),
        )
    )

    stale_fingerprint_checkpoint = json.loads(json.dumps(valid_dart_checkpoint))
    stale_fingerprint_checkpoint["job"]["page_count"] = 99
    invalid_dart_checkpoints.append(
        (
            "canonical fingerprint mismatch",
            stale_fingerprint_checkpoint,
            stale_fingerprint_checkpoint["job"]["fingerprint"],
        )
    )

    wrong_window_key_checkpoint = json.loads(json.dumps(valid_dart_checkpoint))
    first_window = next(
        iter(wrong_window_key_checkpoint["completed_windows"].values())
    )
    first_window["idempotency_key"] = (
        "official-backfill-v1:"
        + hashlib.sha256(b"wrong DART window identity").hexdigest()[:32]
    )
    invalid_dart_checkpoints.append(
        (
            "window idempotency fingerprint mismatch",
            wrong_window_key_checkpoint,
            wrong_window_key_checkpoint["job"]["fingerprint"],
        )
    )

    partial_ack_checkpoint = json.loads(json.dumps(valid_dart_checkpoint))
    partial_ack_summary = next(
        iter(partial_ack_checkpoint["completed_windows"].values())
    )["summary"]
    partial_ack_summary["official_remote_raw_count"] = 2
    invalid_dart_checkpoints.append(
        (
            "partial remote ACK",
            partial_ack_checkpoint,
            partial_ack_checkpoint["job"]["fingerprint"],
        )
    )

    zero_request_checkpoint = json.loads(json.dumps(valid_dart_checkpoint))
    zero_request_summary = next(
        iter(zero_request_checkpoint["completed_windows"].values())
    )["summary"]
    zero_request_summary["official_dart_requests"] = 0
    invalid_dart_checkpoints.append(
        (
            "zero DART requests",
            zero_request_checkpoint,
            zero_request_checkpoint["job"]["fingerprint"],
        )
    )

    quota_checkpoint = json.loads(json.dumps(valid_dart_checkpoint))
    quota_summary = next(
        iter(quota_checkpoint["completed_windows"].values())
    )["summary"]
    quota_summary["official_dart_quota_exhausted"] = 1
    invalid_dart_checkpoints.append(
        (
            "DART quota exhausted",
            quota_checkpoint,
            quota_checkpoint["job"]["fingerprint"],
        )
    )

    outside_job_checkpoint = json.loads(json.dumps(valid_dart_checkpoint))
    first_window_key = next(iter(outside_job_checkpoint["completed_windows"]))
    outside_window = outside_job_checkpoint["completed_windows"].pop(
        first_window_key
    )
    outside_start = (
        datetime.fromisoformat(outside_window["window_start"]).date()
        - timedelta(days=1)
    )
    outside_end = outside_start + timedelta(days=1)
    outside_key = f"{outside_start.isoformat()}:{outside_end.isoformat()}"
    outside_window["window_start"] = outside_start.isoformat()
    outside_window["window_end_exclusive"] = outside_end.isoformat()
    outside_digest = hashlib.sha256(
        (
            outside_job_checkpoint["job"]["fingerprint"]
            + "|"
            + outside_key
        ).encode("utf-8")
    ).hexdigest()[:32]
    outside_window["idempotency_key"] = (
        f"official-backfill-v1:{outside_digest}"
    )
    outside_job_checkpoint["completed_windows"][outside_key] = outside_window
    invalid_dart_checkpoints.append(
        (
            "window outside job range",
            outside_job_checkpoint,
            outside_job_checkpoint["job"]["fingerprint"],
        )
    )

    for label, checkpoint, row_fingerprint in invalid_dart_checkpoints:
        replace_dart_checkpoint_fixture(
            mysql_container_id,
            checkpoint=checkpoint,
            row_fingerprint=row_fingerprint,
            now=now,
        )
        rejected_evidence, _ = request_json(
            base_url,
            (
                "api.php/api/v2/ops/alpha-release-evidence?"
                + urllib.parse.urlencode({"code_revision": CODE_REVISION})
            ),
            token=OPS_TOKEN,
            expected_status=409,
        )
        require(
            rejected_evidence.get("error") == "automated_evidence_unavailable",
            f"{label}: {rejected_evidence!r}",
        )

    valid_dart_fingerprint = valid_dart_checkpoint["job"]["fingerprint"]
    replace_dart_checkpoint_fixture(
        mysql_container_id,
        checkpoint=valid_dart_checkpoint,
        row_fingerprint=valid_dart_fingerprint,
        now=now,
    )

    duplicate_day = now.date() - timedelta(days=1)
    duplicate_end = now.date()
    duplicate_digest = hashlib.sha256(
        f"duplicate-day:{CODE_REVISION}".encode()
    ).hexdigest()
    duplicate_batch = "global-batch:" + duplicate_digest
    duplicate_completed = now.strftime("%Y-%m-%d %H:%M:%S")
    mysql_execute(
        mysql_container_id,
        "INSERT INTO ci_global_ingest_receipts "
        "(ingest_id,connector_id,idempotency_key,payload_sha256,batch_id,"
        "chunk_index,chunk_count,window_start,window_end_exclusive,request_count,"
        "raw_count,acknowledged_count,batch_raw_count,batch_acknowledged_count,"
        "batch_request_count,code_revision,started_at,completed_at,created_at) "
        "VALUES ("
        f"'alpha-duplicate:{duplicate_digest}','{SEC_CONNECTOR_ID}',"
        f"'global-ingest-v2-day:us:{duplicate_digest}','{duplicate_digest}',"
        f"'{duplicate_batch}',1,1,'{duplicate_day.isoformat()}',"
        f"'{duplicate_end.isoformat()}',1,3,1,3,1,1,'{CODE_REVISION}',"
        f"'{duplicate_completed}','{duplicate_completed}','{duplicate_completed}');",
    )
    duplicate_evidence, _ = request_json(
        base_url,
        (
            "api.php/api/v2/ops/alpha-release-evidence?"
            + urllib.parse.urlencode({"code_revision": CODE_REVISION})
        ),
        token=OPS_TOKEN,
        expected_status=409,
    )
    require(
        duplicate_evidence.get("error") == "automated_evidence_unavailable",
        repr(duplicate_evidence),
    )
    mysql_execute(
        mysql_container_id,
        "DELETE FROM ci_global_ingest_receipts "
        f"WHERE batch_id='{duplicate_batch}';",
    )

    # Public reads and source status use the same exact connector/document to
    # grant identity binding. A mismatched SEC grant cannot authorize SEC
    # documents, while independent matching evidence remains available.
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET source_key='sec-edgar-corrupted',"
        "updated_at=UTC_TIMESTAMP() "
        f"WHERE source_right_id='{SEC_RIGHT_ID}';",
    )
    identity_status, _ = request_json(
        base_url,
        "api.php/api/v2/sources/status?country=US",
    )
    identity_status_items = identity_status.get("data", {}).get("items", [])
    require(
        len(identity_status_items) == 1
        and identity_status_items[0].get("status") == "blocked_identity"
        and identity_status_items[0].get("collect_status") == "blocked_identity"
        and identity_status_items[0].get("public_status") == "blocked_identity"
        and identity_status_items[0].get("public_ready") is False,
        repr(identity_status),
    )
    identity_filtered_events, _ = request_json(
        base_url,
        "api.php/api/v2/events?country=US&limit=10",
    )
    identity_filtered_items = identity_filtered_events.get("data", {}).get("items", [])
    require(
        len(identity_filtered_items) == 1
        and identity_filtered_items[0].get("source_url") == ALTERNATE_URL
        and identity_filtered_items[0].get("official_evidence_count") == 1,
        repr(identity_filtered_events),
    )
    mysql_execute(
        mysql_container_id,
        f"UPDATE ci_source_rights SET source_key='{SEC_SOURCE_KEY}',"
        "updated_at=UTC_TIMESTAMP() "
        f"WHERE source_right_id='{SEC_RIGHT_ID}';",
    )
    stale_rights_revision = rights_revision
    restored_eligibility, _ = request_json(
        base_url,
        (
            "api.php/api/v2/ops/source-right-eligibility?"
            + urllib.parse.urlencode(
                {"source_right_id": SEC_RIGHT_ID, "use": "collect"}
            )
        ),
        token=OPS_TOKEN,
    )
    rights_revision = restored_eligibility.get("rights_revision")
    require(
        restored_eligibility.get("eligible") is True
        and restored_eligibility.get("source_key") == SEC_SOURCE_KEY
        and isinstance(rights_revision, str)
        and len(rights_revision) == 64
        and rights_revision != stale_rights_revision,
        repr(restored_eligibility),
    )

    pagination_ids = add_byte_pagination_fixture_events(
        mysql_container_id,
        event_id,
    )
    pagination_ids.add(event_id)
    seen_ids: list[str] = []
    next_offset = 0
    try:
        for _ in range(5):
            page_result, page_headers = request_json(
                base_url,
                "api.php/api/v2/events?"
                + urllib.parse.urlencode(
                    {"country": "US", "limit": 100, "offset": next_offset}
                ),
            )
            page_items = page_result.get("data", {}).get("items", [])
            page_meta = page_result.get("meta", {})
            page_ids = [str(item.get("event_id")) for item in page_items]
            require(
                page_ids
                and not set(page_ids).intersection(seen_ids)
                and int(page_headers.get("X-Response-Bytes", "250001")) <= 250000,
                repr(page_result),
            )
            seen_ids.extend(page_ids)
            if not page_meta.get("has_more"):
                require(page_meta.get("next_offset") is None, repr(page_meta))
                break
            expected_next = next_offset + len(page_ids)
            require(
                page_meta.get("next_offset") == expected_next,
                repr(page_meta),
            )
            next_offset = expected_next
        require(
            len(seen_ids) == len(set(seen_ids)) and set(seen_ids) == pagination_ids,
            repr((len(seen_ids), pagination_ids.difference(seen_ids))),
        )
        export_page, export_headers = request_json(
            base_url,
            "api.php/api/v2/exports/events.json?"
            + urllib.parse.urlencode({"country": "US", "limit": 100, "offset": 0}),
        )
        require(
            export_page.get("meta", {}).get("next_offset")
            == len(export_page.get("data", {}).get("items", []))
            and int(export_headers.get("X-Response-Bytes", "250001")) <= 250000,
            repr(export_page),
        )
        csv_page, csv_headers = request_bytes(
            base_url,
            "api.php/api/v2/exports/events.csv?"
            + urllib.parse.urlencode({"country": "US", "limit": 100, "offset": 0}),
        )
        require(
            len(csv_page) <= 250000
            and csv_headers.get("X-BSIDE-Has-More") in {"true", "false"}
            and int(csv_headers.get("X-BSIDE-Returned", "0")) > 0
            and (
                csv_headers.get("X-BSIDE-Has-More") == "false"
                or (
                    int(csv_headers.get("X-BSIDE-Next-Offset", "0")) > 0
                    and 'rel="next"' in csv_headers.get("Link", "")
                )
            ),
            repr(csv_headers),
        )
        atom_page, atom_headers = request_bytes(
            base_url,
            "api.php/api/v2/feeds/events.atom?"
            + urllib.parse.urlencode({"country": "US", "limit": 100, "offset": 0}),
        )
        require(
            len(atom_page) <= 250000
            and atom_headers.get("X-BSIDE-Has-More") == "true"
            and int(atom_headers.get("X-BSIDE-Next-Offset", "0")) > 0
            and b'rel="next"' in atom_page,
            repr(atom_headers),
        )
        for invalid_query in (
            "page=101",
            "offset=10001",
            "page=2&offset=1",
            "limit=101",
            "page=1x",
        ):
            invalid_page, _ = request_json(
                base_url,
                f"api.php/api/v2/events?{invalid_query}",
                expected_status=400,
            )
            require(
                invalid_page.get("error")
                in {
                    "invalid_page",
                    "invalid_offset",
                    "ambiguous_pagination",
                    "invalid_limit",
                },
                repr(invalid_page),
            )
    finally:
        remove_byte_pagination_fixture_events(mysql_container_id)

    # Reverting to an earlier content hash is a third immutable observation,
    # not a pointer back to the old row.
    reversion_records = [
        build_record(
            title="CI monotonic version A",
            content_version="monotonic-a",
            filed_at=filed_at,
            observed_at=utc_text(now + timedelta(minutes=10 + index)),
            external_id="0000320193-26-001111",
        )
        for index in (0, 2)
    ]
    reversion_records.insert(
        1,
        build_record(
            title="CI monotonic version B",
            content_version="monotonic-b",
            filed_at=filed_at,
            observed_at=utc_text(now + timedelta(minutes=11)),
            external_id="0000320193-26-001111",
        ),
    )
    for index, record in enumerate(reversion_records, start=1):
        payload = ingest_payload(
            rights_revision=rights_revision,
            idempotency_key=f"php73-v2-monotonic-{index}",
            record=record,
            retrieved_at=str(record["first_observed_at"]),
        )
        response, _ = request_json(
            base_url,
            "api.php/api/v2/ops/ingest",
            method="POST",
            token=OPS_TOKEN,
            payload=payload,
        )
        require(
            response.get("data", {}).get("acknowledged_count") == 1,
            repr(response),
        )
    reversion_rows = mysql_execute(
        mysql_container_id,
        "SELECT version_no,content_hash,document_id,"
        "COALESCE(correction_of_document_id,'') "
        "FROM ci_documents "
        f"WHERE source_right_id='{SEC_RIGHT_ID}' "
        "AND external_id='0000320193-26-001111' ORDER BY version_no;",
    ).splitlines()
    require(len(reversion_rows) == 3, repr(reversion_rows))
    parsed_reversions = [row.split("\t") for row in reversion_rows]
    require(
        [row[0] for row in parsed_reversions] == ["1", "2", "3"]
        and parsed_reversions[0][1] == parsed_reversions[2][1]
        and parsed_reversions[0][1] != parsed_reversions[1][1]
        and len({row[2] for row in parsed_reversions}) == 3
        and parsed_reversions[1][3] == parsed_reversions[0][2]
        and parsed_reversions[2][3] == parsed_reversions[1][2],
        repr(parsed_reversions),
    )

    # Lifecycle observations are semantic changes. Unlike an unchanged overlap
    # poll, they must continue to advance updated_at and return the event to
    # editorial review.
    lifecycle_event_id = (
        "global-event:" + reversion_records[0]["record_id"]
    )
    lifecycle_sentinel = "2003-03-03 03:03:03"
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_governance_events "
        f"SET updated_at='{lifecycle_sentinel}' "
        f"WHERE event_id='{lifecycle_event_id}';",
    )
    lifecycle_key = "php73-v2-monotonic-lifecycle"
    lifecycle_batch = (
        "global-batch:"
        + hashlib.sha256(lifecycle_key.encode("utf-8")).hexdigest()
    )
    lifecycle_payload = empty_chunk_payload(
        rights_revision=rights_revision,
        idempotency_key=lifecycle_key,
        retrieved_at=utc_text(now + timedelta(minutes=20)),
        batch_id=lifecycle_batch,
        index=1,
        count=1,
    )
    attach_single_chunk_lifecycle_observations(
        lifecycle_payload,
        [
            {
                "observation_id": (
                    "globalobs:"
                    + hashlib.sha256(lifecycle_key.encode("utf-8")).hexdigest()[:40]
                ),
                "country_code": "US",
                "source_key": SEC_SOURCE_KEY,
                "external_id": reversion_records[0]["external_id"],
                "parent_external_id": None,
                "change_type": "corrected",
                "observed_at": utc_text(now + timedelta(minutes=20)),
                "metadata": {"fixture": "semantic lifecycle correction"},
            }
        ],
    )
    lifecycle_ingest, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=lifecycle_payload,
    )
    require(
        lifecycle_ingest.get("data", {}).get("acknowledged_count") == 1,
        repr(lifecycle_ingest),
    )
    lifecycle_event_state = mysql_execute(
        mysql_container_id,
        "SELECT updated_at<>"
        f"'{lifecycle_sentinel}',verification_status,change_type,"
        "current_status,publication_status,review_status,identity_status "
        "FROM ci_governance_events "
        f"WHERE event_id='{lifecycle_event_id}';",
    )
    require(
        lifecycle_event_state
        == (
            "1\tcorrected\tcorrected\tcorrected"
            "\tdraft\tpending\tneeds_review"
        ),
        lifecycle_event_state,
    )

    # Once an editor approves the lifecycle change, an overlapping batch that
    # carries the exact same observation is a no-op. It must not refresh either
    # timestamp or send the canonical event back to review.
    lifecycle_observation_id = lifecycle_payload["envelope"][
        "lifecycle_observations"
    ][0]["observation_id"]
    lifecycle_replay_sentinel = "2004-04-04 04:04:04"
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_governance_events SET publication_status='published',"
        "review_status='approved',identity_status='complete',"
        f"updated_at='{lifecycle_replay_sentinel}' "
        f"WHERE event_id='{lifecycle_event_id}';",
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_global_lifecycle_observations "
        f"SET updated_at='{lifecycle_replay_sentinel}' "
        f"WHERE observation_id='{lifecycle_observation_id}';",
    )
    lifecycle_replay_key = "php73-v2-monotonic-lifecycle-replay"
    lifecycle_replay_payload = empty_chunk_payload(
        rights_revision=rights_revision,
        idempotency_key=lifecycle_replay_key,
        retrieved_at=utc_text(now + timedelta(minutes=21)),
        batch_id=(
            "global-batch:"
            + hashlib.sha256(
                lifecycle_replay_key.encode("utf-8")
            ).hexdigest()
        ),
        index=1,
        count=1,
    )
    attach_single_chunk_lifecycle_observations(
        lifecycle_replay_payload,
        json.loads(
            json.dumps(
                lifecycle_payload["envelope"]["lifecycle_observations"]
            )
        ),
    )
    lifecycle_replay, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=lifecycle_replay_payload,
    )
    require(
        lifecycle_replay.get("data", {}).get("acknowledged_count") == 1
        and lifecycle_replay.get("data", {}).get("idempotent") is False,
        repr(lifecycle_replay),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT updated_at,publication_status,review_status,"
            "identity_status,verification_status FROM ci_governance_events "
            f"WHERE event_id='{lifecycle_event_id}';",
        )
        == (
            f"{lifecycle_replay_sentinel}"
            "\tpublished\tapproved\tcomplete\tcorrected"
        )
        and mysql_execute(
            mysql_container_id,
            "SELECT updated_at,resolution_status,resolved_event_id "
            "FROM ci_global_lifecycle_observations "
            f"WHERE observation_id='{lifecycle_observation_id}';",
        )
        == (
            f"{lifecycle_replay_sentinel}"
            f"\tresolved\t{lifecycle_event_id}"
        ),
        "an exact lifecycle replay must be a canonical no-op",
    )

    # Reusing an observation ID for different semantics is an integrity
    # conflict. A genuinely new lifecycle state needs a new observation ID.
    lifecycle_conflict_payload = json.loads(
        json.dumps(lifecycle_replay_payload)
    )
    lifecycle_conflict_key = "php73-v2-monotonic-lifecycle-conflict"
    lifecycle_conflict_payload["idempotency_key"] = lifecycle_conflict_key
    lifecycle_conflict_payload["envelope"]["chunk"]["batch_id"] = (
        "global-batch:"
        + hashlib.sha256(
            lifecycle_conflict_key.encode("utf-8")
        ).hexdigest()
    )
    lifecycle_conflict_payload["envelope"]["lifecycle_observations"][0][
        "metadata"
    ] = {"fixture": "changed semantics under a reused observation id"}
    lifecycle_conflict, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=lifecycle_conflict_payload,
        expected_status=409,
    )
    require(
        lifecycle_conflict.get("error")
        == "global_lifecycle_observation_conflict",
        repr(lifecycle_conflict),
    )

    lifecycle_withdrawal_key = "php73-v2-monotonic-lifecycle-withdrawal"
    lifecycle_withdrawal_payload = empty_chunk_payload(
        rights_revision=rights_revision,
        idempotency_key=lifecycle_withdrawal_key,
        retrieved_at=utc_text(now + timedelta(minutes=22)),
        batch_id=(
            "global-batch:"
            + hashlib.sha256(
                lifecycle_withdrawal_key.encode("utf-8")
            ).hexdigest()
        ),
        index=1,
        count=1,
    )
    attach_single_chunk_lifecycle_observations(
        lifecycle_withdrawal_payload,
        [
            {
                "observation_id": (
                    "globalobs:"
                    + hashlib.sha256(
                        lifecycle_withdrawal_key.encode("utf-8")
                    ).hexdigest()[:40]
                ),
                "country_code": "US",
                "source_key": SEC_SOURCE_KEY,
                "external_id": reversion_records[0]["external_id"],
                "parent_external_id": None,
                "change_type": "withdrawn",
                "observed_at": utc_text(now + timedelta(minutes=22)),
                "metadata": {"fixture": "new lifecycle withdrawal"},
            }
        ],
    )
    lifecycle_withdrawal, _ = request_json(
        base_url,
        "api.php/api/v2/ops/ingest",
        method="POST",
        token=OPS_TOKEN,
        payload=lifecycle_withdrawal_payload,
    )
    require(
        lifecycle_withdrawal.get("data", {}).get("acknowledged_count") == 1,
        repr(lifecycle_withdrawal),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT updated_at<>"
            f"'{lifecycle_replay_sentinel}',verification_status,change_type,"
            "publication_status,review_status,identity_status "
            "FROM ci_governance_events "
            f"WHERE event_id='{lifecycle_event_id}';",
        )
        == "1\twithdrawn\twithdrawn\tdraft\tpending\tneeds_review"
        and mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_global_lifecycle_observations "
            f"WHERE resolved_event_id='{lifecycle_event_id}';",
        )
        == "2",
        "a new lifecycle semantic must return the event to review",
    )

    # Revocation is evaluated on every read. The revoked URL disappears
    # immediately while the independent official evidence keeps the event
    # public; licensed Telegram evidence never becomes a fallback.
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET status='revoked',revoked_at=UTC_TIMESTAMP(),"
        "updated_at=UTC_TIMESTAMP() WHERE source_right_id='official:sec-edgar';",
    )
    revoked_eligibility, _ = request_json(
        base_url,
        (
            "api.php/api/v2/ops/source-right-eligibility?"
            + urllib.parse.urlencode({"source_right_id": SEC_RIGHT_ID, "use": "public"})
        ),
        token=OPS_TOKEN,
        expected_status=409,
    )
    require(
        revoked_eligibility.get("eligible") is False
        and "revoked" in revoked_eligibility.get("reasons", [])
        and revoked_eligibility.get("rights_revision") != rights_revision,
        repr(revoked_eligibility),
    )
    revoked_events, _ = request_json(
        base_url,
        "api.php/api/v2/events?country=US&limit=10",
    )
    revoked_items = revoked_events.get("data", {}).get("items", [])
    require(
        len(revoked_items) == 1
        and revoked_items[0].get("source_url") == ALTERNATE_URL
        and revoked_items[0].get("official_evidence_count") == 1
        and TELEGRAM_URL not in json.dumps(revoked_events, ensure_ascii=False),
        repr(revoked_events),
    )
    revoked_brief, _ = request_json(
        base_url,
        "api.php/api/v2/briefs/latest?edition=global",
    )
    revoked_top = revoked_brief.get("data", {}).get("top", [])
    require(
        len(revoked_top) == 1
        and revoked_top[0].get("source_url") == ALTERNATE_URL
        and TELEGRAM_URL not in json.dumps(revoked_brief, ensure_ascii=False),
        repr(revoked_brief),
    )
    source_status, _ = request_json(
        base_url,
        "api.php/api/v2/sources/status?country=US",
    )
    source_items = source_status.get("data", {}).get("items", [])
    require(
        len(source_items) == 1
        and source_items[0].get("status") == "blocked_rights"
        and source_items[0].get("collect_status") == "blocked_rights"
        and source_items[0].get("public_status") == "blocked_rights"
        and source_items[0].get("fresh") is False
        and source_items[0].get("collect_fresh") is False
        and source_items[0].get("public_ready") is False,
        repr(source_status),
    )

    # With the SEC grant revoked, corrupting the only remaining official
    # evidence identity removes the event entirely instead of letting the
    # SourceRight ID alone authorize it.
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET source_key='sec-ci-alternate-corrupted',"
        "updated_at=UTC_TIMESTAMP() "
        f"WHERE source_right_id='{ALTERNATE_RIGHT_ID}';",
    )
    no_identity_evidence, _ = request_json(
        base_url,
        "api.php/api/v2/events?country=US&limit=10",
    )
    require(
        no_identity_evidence.get("data", {}).get("items", []) == [],
        repr(no_identity_evidence),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_source_rights SET source_key='sec-ci-alternate',"
        "updated_at=UTC_TIMESTAMP() "
        f"WHERE source_right_id='{ALTERNATE_RIGHT_ID}';",
    )

    closed_again = transition(
        base_url,
        "closed",
        2,
        "CI closes the global terminal after revocation.",
    )
    require(
        closed_again.get("release_state") == "closed"
        and closed_again.get("state_version") == 3,
        repr(closed_again),
    )
    v1_closed_again = transition_v1(
        base_url,
        "closed",
        v1_live_version,
        "CI closes the v1 compatibility surface after revocation.",
    )
    require(
        v1_closed_again.get("release_state") == "closed"
        and v1_closed_again.get("state_version") == v1_closed_version,
        repr(v1_closed_again),
    )
    reopened_v2 = transition(
        base_url,
        "preview",
        3,
        "CI reopens preview to prove rollback cannot reuse authorization.",
    )
    reopened_v1 = transition_v1(
        base_url,
        "preview",
        v1_closed_version,
        "CI reopens v1 preview to prove rollback cannot reuse authorization.",
    )
    require(
        reopened_v2.get("state_version") == 4
        and reopened_v1.get("state_version") == v1_reopened_version,
        repr((reopened_v1, reopened_v2)),
    )
    reuse_after_rollback = atomic_cutover(
        base_url,
        nonce=release_nonce,
        expected_v1_version=v1_reopened_version,
        expected_v2_version=4,
        expected_status=409,
    )
    require(
        reuse_after_rollback.get("error") == "release_authorization_replayed",
        repr(reuse_after_rollback),
    )
    final_v2_closed = transition(
        base_url,
        "closed",
        4,
        "CI leaves the global terminal closed after replay verification.",
    )
    final_v1_closed = transition_v1(
        base_url,
        "closed",
        v1_reopened_version,
        "CI leaves the v1 compatibility surface closed after replay verification.",
    )
    require(
        final_v2_closed.get("state_version") == 5
        and final_v1_closed.get("state_version") == v1_final_closed_version,
        repr((final_v1_closed, final_v2_closed)),
    )
    final_closed, _ = request_json(
        base_url,
        "api.php/api/v2/events",
        token=PREVIEW_TOKEN,
        expected_status=503,
    )
    require(
        final_closed.get("error") == "global_terminal_release_closed",
        repr(final_closed),
    )

    print("PHP 7.3 global terminal v2 smoke passed.", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8787")
    parser.add_argument("--mysql-container-id", required=True)
    args = parser.parse_args()
    run(args.base_url, args.mysql_container_id)


if __name__ == "__main__":
    main()
