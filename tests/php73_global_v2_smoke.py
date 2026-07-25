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
import json
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
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
    "official:edinet",
    "official:companies-house",
    "official:ca-issuer-ir",
    "official:asic-register",
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
    payload: dict[str, Any] | None = None,
    expected_status: int = 200,
) -> tuple[dict[str, Any], dict[str, str]]:
    body = None
    headers = {"Accept": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
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
        "body_text": f"CI source-preserved fixture body {content_version}.",
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
    return record


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
        "SET permission_scope='CI-approved SEC filing metadata, source links, and fixture body',"
        "evidence_uri='https://www.sec.gov/search-filings/edgar-application-programming-interfaces',"
        "evidence_hash=NULL,valid_from='2009-01-01 00:00:00',valid_until=NULL,"
        "revoked_at=NULL,ai_allowed=1,redistribution_allowed=1,status='active',"
        "updated_at=UTC_TIMESTAMP() "
        "WHERE source_right_id='official:sec-edgar';",
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
            if value != SEC_RIGHT_ID
        )
        + ");"
        "UPDATE ci_source_connectors SET connector_status='active',"
        "last_checked_at=UTC_TIMESTAMP(),last_success_at=UTC_TIMESTAMP(),"
        "last_observed_at=UTC_TIMESTAMP(),"
        "last_raw_count=CASE WHEN coverage_mode='link-only' THEN 1 ELSE 0 END,"
        "last_acknowledged_count=CASE WHEN coverage_mode='link-only' THEN 1 ELSE 0 END,"
        "last_error_class=NULL,updated_at=UTC_TIMESTAMP() WHERE connector_id IN ("
        "'connector:kr:dart','connector:us:sec-edgar','connector:jp:edinet',"
        "'connector:gb:companies-house','connector:ca:issuer-ir',"
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


def seed_alpha_automated_evidence(
    mysql_container_id: str,
    *,
    now: datetime,
) -> None:
    end = now.date()
    start = end - timedelta(days=30)
    receipt_values: list[str] = []
    connector_ids = (
        "connector:us:sec-edgar",
        "connector:jp:edinet",
        "connector:gb:companies-house",
    )
    for connector_id in connector_ids:
        cursor = start
        for index in range(30):
            next_cursor = cursor + timedelta(days=1)
            identity = f"{connector_id}:{cursor.isoformat()}:{CODE_REVISION}"
            digest = hashlib.sha256(identity.encode()).hexdigest()
            batch_id = "global-batch:" + digest
            ingest_id = "alpha:" + digest[:80]
            idempotency_key = "alpha-evidence:" + digest
            completed = now.strftime("%Y-%m-%d %H:%M:%S")
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

    completed_windows: dict[str, object] = {}
    cursor = start
    for index in range(30):
        next_cursor = cursor + timedelta(days=1)
        key = f"{cursor.isoformat()}:{next_cursor.isoformat()}"
        completed_windows[key] = {
            "window_start": cursor.isoformat(),
            "window_end_exclusive": next_cursor.isoformat(),
            "idempotency_key": f"official-backfill-v1:{index:032x}",
            "attempt": 1,
            "code_revision": CODE_REVISION,
            "status": "succeeded",
            "summary": {
                "official_failed": 0,
                "official_skipped": 0,
                "official_remote_ack_mismatches": 0,
                "official_remote_run_persisted": 1,
                "official_remote_raw_count": 3,
                "official_remote_ack_count": 1,
                "official_remote_failed": 0,
                "official_remote_skipped": 0,
                "official_remote_synced": 1,
            },
        }
        cursor = next_cursor
    job = {
        "range_start": start.isoformat(),
        "range_end_exclusive": end.isoformat(),
        "chunk_days": 1,
        "sources": ["dart"],
        "page_count": 100,
        "max_pages": 100,
        "sync_company_master": False,
    }
    fingerprint = hashlib.sha256(
        json.dumps(
            job,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    ).hexdigest()
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
        f"('{fingerprint}',31,'{escaped_checkpoint}','{payload_hash}',"
        f"'ci-alpha-evidence','{now.strftime('%Y-%m-%d %H:%M:%S')}',"
        f"'{now.strftime('%Y-%m-%d %H:%M:%S')}');",
    )


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
        == "19711d39dfe0fca47f6397cbbb6911b0ee7621b489ad8d72503c63da2d1514c4",
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
        unapproved_configured.get("error") == "connector_source_right_ineligible",
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
    require(
        eligibility.get("eligible") is True
        and eligibility.get("source_type") == "official_disclosure"
        and eligibility.get("source_key") == SEC_SOURCE_KEY
        and isinstance(rights_revision, str)
        and len(rights_revision) == 64,
        repr(eligibility),
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
    # proof, and advances the checkpoint only after chunk 2 commits.
    complete_batch = (
        "global-batch:" + hashlib.sha256(b"php73-v2-complete-batch").hexdigest()
    )
    for chunk_index in (1, 2):
        complete_chunk, _ = request_json(
            base_url,
            "api.php/api/v2/ops/ingest",
            method="POST",
            token=OPS_TOKEN,
            payload=empty_chunk_payload(
                rights_revision=rights_revision,
                idempotency_key=(f"php73-v2-complete-chunk-{chunk_index}"),
                retrieved_at=observed_at,
                batch_id=complete_batch,
                index=chunk_index,
                count=2,
            ),
        )
        require(
            complete_chunk.get("data", {}).get("acknowledged_count") == 0,
            repr(complete_chunk),
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
            "SUM(raw_count),SUM(acknowledged_count),SUM(request_count) "
            "FROM ci_global_ingest_receipts "
            f"WHERE batch_id='{complete_batch}';",
        )
        == "1,2\t0\t0\t0",
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
    )
    require(
        next_revision_ingest.get("data", {}).get("idempotent") is False
        and mysql_execute(
            mysql_container_id,
            "SELECT code_revision FROM ci_global_ingest_receipts "
            f"WHERE batch_id='{next_revision_batch}';",
        )
        == next_revision,
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
        == ("global-batch:" + hashlib.sha256(b"php73-v2-sec-ingest-v2").hexdigest())
        and checkpoint_data.get("code_revision") == CODE_REVISION,
        repr(checkpoint),
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
    # five still-pending non-SEC grants must block cutover without consuming
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
        and pending_sources.get("required_connector_count") == 6
        and pending_sources.get("invalid_required_connector_count", 0) >= 5,
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
    # evidence grant can still block activation after all six sources pass.
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

    seed_alpha_automated_evidence(mysql_container_id, now=now)
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
        "UPDATE ci_governance_events "
        f"SET title='{second_title}',updated_at=UTC_TIMESTAMP() "
        f"WHERE event_id='{event_id}';",
    )
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
        and len(preserved_data.get("connector_coverage", [])) == 4
        and all(
            connector.get("successful_window_count") == 30
            and len(connector.get("completed_windows", [])) == 30
            for connector in preserved_data.get("connector_coverage", [])
        )
        and len(evidence_windows) == 120
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
        and all(window.get("filtered_out_count") == 2 for window in evidence_windows)
        and preserved_counts.get("source_title_preserved_count") == 1,
        repr(automated_preserved),
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
    lifecycle_payload["envelope"]["lifecycle_observations"] = [
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
    ]
    lifecycle_payload["envelope"]["chunk"][
        "batch_acknowledged_count"
    ] = 1
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
    lifecycle_replay_payload["envelope"]["lifecycle_observations"] = (
        json.loads(
            json.dumps(
                lifecycle_payload["envelope"]["lifecycle_observations"]
            )
        )
    )
    lifecycle_replay_payload["envelope"]["chunk"][
        "batch_acknowledged_count"
    ] = 1
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
    lifecycle_withdrawal_payload["envelope"]["lifecycle_observations"] = [
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
    ]
    lifecycle_withdrawal_payload["envelope"]["chunk"][
        "batch_acknowledged_count"
    ] = 1
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
