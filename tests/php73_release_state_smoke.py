#!/usr/bin/env python3
"""End-to-end governance release-state checks against PHP 7.3 and MySQL."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ADMIN_TOKEN = "php73-ci-admin-token-00000000000000000000"
PREVIEW_TOKEN = "php73-ci-preview-token-000000000000000000"
API_SECRET = b"php73-ci-only-hmac-key-00000000000000000000000000000000"
KST = timezone(timedelta(hours=9))
EXPECTED_BACKEND_BINDING_ID = ""
DART_RIGHTS_REVISION = ""
DART_CONTRACT_REVISION = ""
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DART_CONTRACT_FIXTURE = json.loads(
    (
        REPOSITORY_ROOT
        / "tests"
        / "fixtures"
        / "dart_source_right_contract_v1.json"
    ).read_text(encoding="utf-8")
)
DEPLOYED_CODE_REVISION = json.loads(
    (
        REPOSITORY_ROOT
        / "deploy"
        / "activist"
        / "deployment-manifest.json"
    ).read_text(encoding="utf-8")
)["code_revision"]


class SmokeFailure(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SmokeFailure(message)


def canonical_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def stable_id(prefix: str, *parts: object, length: int) -> str:
    seed = "\x1f".join(str(part or "").strip() for part in parts)
    return f"{prefix}:{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:length]}"


def mysql_execute(container_id: str, sql: str) -> str:
    result = subprocess.run(
        [
            "docker",
            "exec",
            container_id,
            "mysql",
            "--user=root",
            "--password=activist_ci_root_password",
            "--batch",
            "--skip-column-names",
            "activist_ci",
            f"--execute={sql}",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def mysql_backend_binding_id(container_id: str) -> str:
    server_uuid, database_name = mysql_execute(
        container_id,
        "SELECT LOWER(@@server_uuid),DATABASE()",
    ).split("\t")
    return hashlib.sha256(
        f"mysql8\n{server_uuid}\n{database_name}\nci_".encode()
    ).hexdigest()


def activate_exact_dart_source_right(
    base_url: str,
    container_id: str,
) -> None:
    global DART_RIGHTS_REVISION, DART_CONTRACT_REVISION
    source = DART_CONTRACT_FIXTURE["source_right"]

    def sql_text(value: object) -> str:
        return str(value).replace("\\", "\\\\").replace("'", "''")

    mysql_execute(
        container_id,
        "INSERT INTO ci_source_rights "
        "(source_right_id,source_type,source_key,source_name,permission_scope,"
        "evidence_uri,evidence_hash,valid_from,valid_until,revoked_at,"
        "ai_allowed,redistribution_allowed,status,notes,created_at,updated_at) "
        "VALUES ("
        f"'{sql_text(source['source_right_id'])}',"
        f"'{sql_text(source['source_type'])}',"
        f"'{sql_text(source['source_key'])}',"
        f"'{sql_text(source['source_name'])}',"
        f"'{sql_text(source['permission_scope'])}',"
        f"'{sql_text(source['evidence_uri'])}',NULL,"
        "'2021-01-01 00:00:00',NULL,NULL,0,1,'active',"
        "'CI protected metadata-only fixture',UTC_TIMESTAMP(),UTC_TIMESTAMP()) "
        "ON DUPLICATE KEY UPDATE "
        f"source_type='{sql_text(source['source_type'])}',"
        f"source_key='{sql_text(source['source_key'])}',"
        f"source_name='{sql_text(source['source_name'])}',"
        f"permission_scope='{sql_text(source['permission_scope'])}',"
        f"evidence_uri='{sql_text(source['evidence_uri'])}',evidence_hash=NULL,"
        "valid_from='2021-01-01 00:00:00',valid_until=NULL,revoked_at=NULL,"
        "ai_allowed=0,redistribution_allowed=1,status='active',"
        "notes='CI protected metadata-only fixture',updated_at=UTC_TIMESTAMP();",
    )
    eligibility, _ = request_json(
        base_url,
        (
            "api.php/api/v2/ops/source-right-eligibility?"
            + urllib.parse.urlencode(
                {"source_right_id": "official:dart", "use": "collect"}
            )
        ),
        token=ADMIN_TOKEN,
    )
    DART_RIGHTS_REVISION = str(eligibility.get("rights_revision") or "")
    DART_CONTRACT_REVISION = str(
        eligibility.get("contract_revision") or ""
    )
    require(
        len(DART_RIGHTS_REVISION) == 64
        and DART_CONTRACT_REVISION
        == DART_CONTRACT_FIXTURE["expected_revision"],
        repr(eligibility),
    )
    require(
        eligibility.get("connector_id") == "connector:kr:dart"
        and eligibility.get("connector_ready") is True
        and "connector_status" not in eligibility,
        repr(eligibility),
    )


def official_slots_for_kst_day(local_day: datetime) -> list[datetime]:
    midnight = local_day.astimezone(KST).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    rows: list[datetime] = []
    for hour in range(24):
        minutes = (0, 30) if hour < 7 else (0, 15, 30, 45)
        for minute in minutes:
            rows.append(
                midnight.replace(hour=hour, minute=minute).astimezone(timezone.utc)
            )
    return rows


def current_official_slot() -> tuple[datetime, datetime]:
    while True:
        now = datetime.now(timezone.utc)
        slots = (
            official_slots_for_kst_day(now - timedelta(days=1))
            + official_slots_for_kst_day(now)
            + official_slots_for_kst_day(now + timedelta(days=1))
        )
        previous = max(slot for slot in slots if slot <= now)
        following = min(slot for slot in slots if slot > previous)
        remaining = (following - now).total_seconds()
        if remaining >= 60:
            return previous, following
        time.sleep(max(1.0, remaining + 1.0))


def official_schedule(slot: datetime) -> str:
    hour = slot.astimezone(timezone.utc).hour
    if hour >= 22:
        return "0,15,30,45 22-23 * * *"
    if hour <= 14:
        return "0,15,30,45 0-14 * * *"
    return "0,30 15-21 * * *"


def error_code(payload: dict[str, Any]) -> str:
    value = payload.get("error")
    if isinstance(value, dict):
        return str(value.get("code") or "")
    return str(value or "")


def iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def official_site_payload(
    *,
    title: str,
    body_text: str,
    collected_at: str,
    include_review: bool,
) -> tuple[dict[str, Any], str, str]:
    connector_id = "company-site:00123456"
    source_right_id = "right:company-site:00123456"
    external_id = "notice-1"
    original_url = "https://example.com/ir/notice-1"
    content_hash = hashlib.sha256(
        f"{title}\n{body_text}\n{original_url}".encode("utf-8")
    ).hexdigest()
    document_id = stable_id(
        "site-doc", connector_id, external_id, content_hash, length=32
    )
    effective_at = "2026-07-20"
    deadline_at = "2026-08-31"
    identity_values = (
        "00123456",
        "shareholder_proposal",
        "submit",
        "board seat",
        "actor:test",
        effective_at,
        deadline_at,
    )
    comparison_key = "eventcmp:v1:" + hashlib.sha256(
        "\x1f".join(("governance-event-identity-v1", *identity_values)).encode("utf-8")
    ).hexdigest()
    document = {
        "document_id": document_id,
        "external_id": external_id,
        "company_id": "00123456",
        "source_class": "company_statement",
        "source_right_id": source_right_id,
        "document_type": "shareholder_proposal",
        "original_language": "ko",
        "title": title,
        "body_text": body_text,
        "original_url": original_url,
        "content_hash": content_hash,
        "collection_key": stable_id(
            "site-collection", connector_id, external_id, length=32
        ),
        "version_no": 1,
        "published_at": "2026-07-20T09:00:00+09:00",
        "retrieved_at": collected_at,
        "verification_status": "unverified",
        "publication_status": "draft",
    }
    event = {
        "event_id": comparison_key,
        "company_id": "00123456",
        "event_type": "shareholder_proposal",
        "title": title,
        "original_language": "ko",
        "summary": "",
        "occurred_at": effective_at,
        "deadline_at": deadline_at,
        "importance": "medium",
        "verification_status": "unverified",
        "review_status": "pending",
        "publication_status": "draft",
        "review_required": True,
        "collection_key": stable_id(
            "site-event", connector_id, external_id, length=32
        ),
        "document_ids": [document_id],
        "source_right_ids": [source_right_id],
        "action": "submit",
        "target": "board seat",
        "identity_action": "submit",
        "identity_target": "board seat",
        "identity_actor_id": "actor:test",
        "identity_effective_at": effective_at,
        "identity_deadline_at": deadline_at,
        "identity_status": "complete",
        "comparison_key": comparison_key,
    }
    review_items: list[dict[str, Any]] = []
    if include_review:
        review_external_id = "notice-1-review"
        review_items.append(
            {
                "review_id": stable_id(
                    "site-review",
                    connector_id,
                    review_external_id,
                    content_hash,
                    length=32,
                ),
                "connector_id": connector_id,
                "entity_type": "company",
                "entity_id": "00123456",
                "external_id": review_external_id,
                "source_class": "company_statement",
                "source_right_id": source_right_id,
                "action": "editor_identity_review_required",
                "review_reasons": ["identity requires editor confirmation"],
                "draft_document": {
                    "content_hash": content_hash,
                    "retrieved_at": collected_at,
                },
                # Deliberately exercise the Python {} versus PHP [] canonical-JSON edge.
                "proposed_identity": {},
            }
        )
    connector = {
        "connector_id": connector_id,
        "entity_type": "company",
        "entity_id": "00123456",
        "source_class": "company_statement",
        "source_right_id": source_right_id,
        "pages_fetched": 1,
        "total_count": 1 + len(review_items),
        "payload_sha256": hashlib.sha256(
            f"connector:{content_hash}:{collected_at}".encode("utf-8")
        ).hexdigest(),
    }
    receipt_sha256 = hashlib.sha256(
        f"receipt:{content_hash}:{collected_at}".encode("utf-8")
    ).hexdigest()
    core: dict[str, Any] = {
        "schema_version": 1,
        "receipt_sha256": receipt_sha256,
        "code_revision": "c" * 40,
        "collected_at": collected_at,
        "manifest_sha256": "3" * 64,
        "connector": connector,
        "companies": [
            {
                "company_id": "00123456",
                "legal_name": "스모크 테스트 주식회사",
                "record_status": "active",
            }
        ],
        "documents": [document],
        "events": [event],
        "review_items": review_items,
        "tombstones": [],
        "expected": {
            "companies": 1,
            "documents": 1,
            "events": 1,
            "event_observations": 1,
            "review_items": len(review_items),
            "tombstones": 0,
        },
    }
    payload_sha256 = canonical_sha256(core)
    core["payload_sha256"] = payload_sha256
    core["snapshot_id"] = stable_id(
        "official-site-snapshot",
        connector_id,
        receipt_sha256,
        payload_sha256,
        length=64,
    )
    return core, document_id, content_hash


def request_json(
    base_url: str,
    path: str,
    *,
    method: str = "GET",
    token: str | None = None,
    payload: dict[str, Any] | None = None,
    expected_status: int = 200,
    request_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, str]]:
    body = None
    headers: dict[str, str] = {}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    if request_id is not None:
        headers["X-Request-ID"] = request_id
    if payload is not None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/{path.lstrip('/')}",
        data=body,
        method=method,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            status = response.status
            raw = response.read()
            response_headers = dict(response.headers.items())
    except urllib.error.HTTPError as exc:
        status = exc.code
        raw = exc.read()
        response_headers = dict(exc.headers.items())
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SmokeFailure(f"{path} returned non-JSON HTTP {status}: {raw[:300]!r}") from exc
    require(status == expected_status, f"{path}: expected HTTP {expected_status}, got {status}: {decoded!r}")
    require(isinstance(decoded, dict), f"{path}: response must be an object")
    return decoded, response_headers


def request_hmac_action(
    base_url: str,
    action: str,
    payload: dict[str, Any],
    *,
    expected_status: int,
    use_guarded_dart_action: bool = True,
    inject_dart_preconditions: bool = True,
    expected_release_state: str = "closed",
) -> dict[str, Any]:
    signed_payload = dict(payload)
    actual_action = action
    if action in {
        "upsert_governance_snapshot",
        "upsert_governance_snapshot_dart_guarded",
    }:
        require(
            len(EXPECTED_BACKEND_BINDING_ID) == 64,
            "CI backend binding must be initialized before governance HMAC writes",
        )
        signed_payload.setdefault(
            "expected_backend_binding_id",
            EXPECTED_BACKEND_BINDING_ID,
        )
        documents = signed_payload.get("documents")
        run = signed_payload.get("run")
        dart_document = any(
            isinstance(document, dict)
            and (
                str(document.get("document_id") or "").casefold().startswith(
                    "dart:"
                )
                or str(document.get("source") or "").casefold() == "dart"
                or str(
                    document.get("source_right_id") or ""
                ).casefold()
                == "official:dart"
            )
            for document in (
                documents if isinstance(documents, list) else []
            )
        )
        run_sources = (
            {
                token.strip()
                for token in str(run.get("source_key") or "")
                .casefold()
                .split("+")
                if token.strip()
            }
            if isinstance(run, dict)
            else set()
        )
        guarded_dart_payload = (
            dart_document
            or "dart" in run_sources
            or action == "upsert_governance_snapshot_dart_guarded"
        )
        if guarded_dart_payload and inject_dart_preconditions:
            require(
                len(DART_RIGHTS_REVISION) == 64
                and len(DART_CONTRACT_REVISION) == 64,
                "DART HMAC smoke requires protected SourceRight revisions",
            )
            signed_payload.setdefault(
                "expected_source_right_revisions",
                {
                    "official:dart": {
                        "rights_revision": DART_RIGHTS_REVISION,
                        "contract_revision": DART_CONTRACT_REVISION,
                    }
                },
            )
            signed_payload.setdefault(
                "expected_deployment_code_revision",
                DEPLOYED_CODE_REVISION,
            )
            signed_payload.setdefault(
                "expected_release_state",
                expected_release_state,
            )
            if (
                action == "upsert_governance_snapshot"
                and use_guarded_dart_action
            ):
                actual_action = (
                    "upsert_governance_snapshot_dart_guarded"
                )
    body = json.dumps(
        signed_payload,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    timestamp = str(int(time.time()))
    nonce = f"release-smoke-{action}-{time.time_ns()}"
    signature = hmac.new(
        API_SECRET,
        timestamp.encode("ascii") + b"\n" + nonce.encode("ascii") + b"\n" + body,
        hashlib.sha256,
    ).hexdigest()
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/api.php?{urllib.parse.urlencode({'action': actual_action})}",
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
        with urllib.request.urlopen(request, timeout=15) as response:
            status = response.status
            raw = response.read()
    except urllib.error.HTTPError as exc:
        status = exc.code
        raw = exc.read()
    decoded = json.loads(raw.decode("utf-8"))
    require(status == expected_status, f"{action}: expected {expected_status}, got {status}: {decoded!r}")
    require(isinstance(decoded, dict), f"{action}: response must be an object")
    return decoded


def transition(
    base_url: str,
    state: str,
    version: int,
    reason: str,
    *,
    expected_status: int = 200,
    request_id: str | None = None,
) -> dict[str, Any]:
    response, _ = request_json(
        base_url,
        "api.php/api/v1/admin/release-state",
        method="POST",
        token=ADMIN_TOKEN,
        payload={"release_state": state, "expected_version": version, "reason": reason},
        expected_status=expected_status,
        request_id=request_id,
    )
    return response


def exercise_official_slot_claims(base_url: str, mysql_container_id: str) -> None:
    revision = "c" * 40
    bootstrap_slot, _ = current_official_slot()
    bootstrap_request = {
        "action": "claim",
        "pipeline": "ingest-official",
        "github_run_id": "910001",
        "github_run_attempt": 1,
        "event_schedule": official_schedule(bootstrap_slot),
        "trigger_created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "code_revision": revision,
    }
    activated, _ = request_json(
        base_url,
        "api.php/api/v1/ops/official-slot-claims",
        method="POST",
        token=ADMIN_TOKEN,
        payload=bootstrap_request,
        expected_status=409,
    )
    require(error_code(activated) == "official_slot_claim_activated", repr(activated))
    require(isinstance(activated.get("active_from"), str), repr(activated))

    slot, next_slot = current_official_slot()
    slot_mysql = slot.strftime("%Y-%m-%d %H:%M:%S")
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_official_slot_claim_state "
        f"SET active_from='{slot_mysql}',updated_at=UTC_TIMESTAMP() "
        "WHERE pipeline='ingest-official';"
        "UPDATE ci_official_slot_claim_epochs "
        f"SET active_from='{slot_mysql}' "
        "WHERE pipeline='ingest-official' AND epoch_version=1;",
    )
    trigger = datetime.now(timezone.utc).replace(microsecond=0)
    claim_request = {
        **bootstrap_request,
        "event_schedule": official_schedule(slot),
        "trigger_created_at": trigger.isoformat(),
    }
    claim, _ = request_json(
        base_url,
        "api.php/api/v1/ops/official-slot-claims",
        method="POST",
        token=ADMIN_TOKEN,
        payload=claim_request,
    )
    require(claim.get("accepted") == 1 and claim.get("duplicate") is False, repr(claim))
    require(claim.get("scheduled_slot_at") == iso_z(slot), repr(claim))
    require(claim.get("next_cadence_slot_at") == iso_z(next_slot), repr(claim))
    require(claim.get("late") is False and claim.get("status") == "claimed", repr(claim))
    require(claim.get("terminal_reason") is None, repr(claim))

    run_id = stable_id("run", "ingest-official", claim["claim_id"], length=32)
    completed_run = {
        "run_id": run_id,
        "pipeline": "ingest-official",
        "source_key": "dart+kind",
        "code_revision": revision,
        "status": "succeeded",
        "started_at": claim["claimed_at"],
        "finished_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "first_observed_at": claim["claimed_at"],
        "raw_count": 0,
        "ack_count": 0,
        "fetched_count": 0,
        "resolved_count": 0,
        "accepted_count": 0,
        "error_count": 0,
        "run_kind": "scheduled_incremental",
        "event_schedule": claim["event_schedule"],
        "scheduled_slot_at": claim["scheduled_slot_at"],
        "trigger_created_at": claim["trigger_created_at"],
        "slot_claim_id": claim["claim_id"],
        "github_run_id": claim["github_run_id"],
        "github_run_attempt": 1,
        "slot_claimed_at": claim["claimed_at"],
        "next_cadence_slot_at": claim["next_cadence_slot_at"],
        "trigger_lag_seconds": claim["trigger_lag_seconds"],
        "claim_lag_seconds": claim["claim_lag_seconds"],
        "slot_claim_late": False,
        "company_master_sync": False,
        "source_ack_counts": {"dart": 0, "kind": 0},
        "source_outcomes": {
            "dart": {"status": "succeeded", "raw_count": 0, "acknowledged_count": 0},
            "kind": {"status": "succeeded", "raw_count": 0, "acknowledged_count": 0},
        },
    }
    failed_run = dict(completed_run)
    failed_run["status"] = "failed"
    failed_completion = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [],
            "documents": [],
            "events": [],
            "source_rights": [],
            "run": failed_run,
        },
        expected_status=200,
    )
    require(
        failed_completion.get("upserted", {}).get("runs") == 1,
        repr(failed_completion),
    )

    slot_day = slot.astimezone(KST).date().isoformat()
    failed_claims, _ = request_json(
        base_url,
        f"api.php/api/v1/ops/official-slot-claims?from={slot_day}&to={slot_day}&limit=100",
        token=ADMIN_TOKEN,
    )
    failed_rows = {
        row["claim_id"]: row for row in failed_claims.get("data", [])
    }
    failed_row = failed_rows.get(claim["claim_id"])
    require(isinstance(failed_row, dict), repr(failed_claims))
    require(
        failed_row.get("status") == "failed"
        and failed_row.get("terminal_reason") is None
        and failed_row.get("completed_at") is None,
        repr(failed_row),
    )

    retry_request = {**claim_request, "github_run_attempt": 2}
    retry_claim, _ = request_json(
        base_url,
        "api.php/api/v1/ops/official-slot-claims",
        method="POST",
        token=ADMIN_TOKEN,
        payload=retry_request,
    )
    require(
        retry_claim.get("duplicate") is True
        and retry_claim.get("status") == "failed"
        and retry_claim.get("terminal_reason") is None
        and retry_claim.get("github_run_attempt") == 2,
        repr(retry_claim),
    )
    completed_run["github_run_attempt"] = 2
    completion = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [],
            "documents": [],
            "events": [],
            "source_rights": [],
            "run": completed_run,
        },
        expected_status=200,
    )
    require(completion.get("upserted", {}).get("runs") == 1, repr(completion))
    claims_before, _ = request_json(
        base_url,
        f"api.php/api/v1/ops/official-slot-claims?from={slot_day}&to={slot_day}&limit=100",
        token=ADMIN_TOKEN,
    )
    rows_before = {row["claim_id"]: row for row in claims_before.get("data", [])}
    completed_row = rows_before.get(claim["claim_id"])
    require(isinstance(completed_row, dict), repr(claims_before))
    require(
        completed_row.get("status") == "completed"
        and completed_row.get("github_run_attempt") == 2
        and completed_row.get("completion_raw_count") == 0
        and completed_row.get("completion_ack_count") == 0,
        repr(completed_row),
    )

    rerun_request = {**claim_request, "github_run_attempt": 3}
    rerun_claim, _ = request_json(
        base_url,
        "api.php/api/v1/ops/official-slot-claims",
        method="POST",
        token=ADMIN_TOKEN,
        payload=rerun_request,
    )
    require(
        rerun_claim.get("duplicate") is True
        and rerun_claim.get("status") == "completed"
        and rerun_claim.get("github_run_attempt") == 3,
        repr(rerun_claim),
    )
    rerun_payload = dict(completed_run)
    rerun_payload["github_run_attempt"] = 3
    rerun_payload["started_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rerun_payload["finished_at"] = rerun_payload["started_at"]
    rerun_payload["first_observed_at"] = rerun_payload["started_at"]
    request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [],
            "documents": [],
            "events": [],
            "source_rights": [],
            "run": rerun_payload,
        },
        expected_status=200,
    )
    claims_after, _ = request_json(
        base_url,
        f"api.php/api/v1/ops/official-slot-claims?from={slot_day}&to={slot_day}&limit=100",
        token=ADMIN_TOKEN,
    )
    rows_after = {row["claim_id"]: row for row in claims_after.get("data", [])}
    require(rows_after.get(claim["claim_id"]) == completed_row, repr(rows_after))

    health, _ = request_json(
        base_url, "api.php/api/v1/ops/health", token=ADMIN_TOKEN
    )
    for source in ("dart", "kind"):
        source_health = health.get("official_sources", {}).get(source, {})
        require(
            source_health.get("last_scheduled_success_at") == iso_z(slot),
            repr(health),
        )

    cadence = sorted(
        set(
            official_slots_for_kst_day(slot - timedelta(days=1))
            + official_slots_for_kst_day(slot)
        )
    )
    slot_index = cadence.index(slot)
    require(slot_index >= 2, repr(cadence))
    repair_slot = cadence[slot_index - 2]
    repair_mysql = repair_slot.strftime("%Y-%m-%d %H:%M:%S")
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_official_slot_claim_state "
        f"SET active_from='{repair_mysql}',updated_at=UTC_TIMESTAMP() "
        "WHERE pipeline='ingest-official';"
        "UPDATE ci_official_slot_claim_epochs "
        f"SET active_from='{repair_mysql}' "
        "WHERE pipeline='ingest-official' AND epoch_version=1;",
    )
    repair_trigger = datetime.now(timezone.utc).replace(microsecond=0)
    repair_request = {
        "action": "repair",
        "pipeline": "ingest-official",
        "github_run_id": "910002",
        "github_run_attempt": 1,
        "event_schedule": official_schedule(repair_slot),
        "trigger_created_at": repair_trigger.isoformat(),
        "code_revision": revision,
        "expected_slot_at": repair_slot.isoformat(),
    }
    repaired, _ = request_json(
        base_url,
        "api.php/api/v1/ops/official-slot-claims",
        method="POST",
        token=ADMIN_TOKEN,
        payload=repair_request,
    )
    require(
        repaired.get("scheduled_slot_at") == iso_z(repair_slot)
        and repaired.get("event_schedule") == official_schedule(repair_slot)
        and repaired.get("late") is True,
        repr(repaired),
    )
    repair_run = dict(completed_run)
    repair_run.update(
        {
            "run_id": stable_id(
                "run", "ingest-official", repaired["claim_id"], length=32
            ),
            "started_at": repaired["claimed_at"],
            "finished_at": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
            "first_observed_at": repaired["claimed_at"],
            "event_schedule": repaired["event_schedule"],
            "scheduled_slot_at": repaired["scheduled_slot_at"],
            "trigger_created_at": repaired["trigger_created_at"],
            "slot_claim_id": repaired["claim_id"],
            "github_run_id": repaired["github_run_id"],
            "github_run_attempt": 1,
            "slot_claimed_at": repaired["claimed_at"],
            "next_cadence_slot_at": repaired["next_cadence_slot_at"],
            "trigger_lag_seconds": repaired["trigger_lag_seconds"],
            "claim_lag_seconds": repaired["claim_lag_seconds"],
            "slot_claim_late": True,
        }
    )
    terminal_completion = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [],
            "documents": [],
            "events": [],
            "source_rights": [],
            "run": repair_run,
        },
        expected_status=409,
    )
    require(
        terminal_completion.get("error")
        == "official_slot_completion_terminal_failure",
        repr(terminal_completion),
    )
    repair_day = repair_slot.astimezone(KST).date().isoformat()
    repaired_claims, _ = request_json(
        base_url,
        f"api.php/api/v1/ops/official-slot-claims?from={repair_day}&to={slot_day}&limit=100",
        token=ADMIN_TOKEN,
    )
    repaired_rows = {
        row["claim_id"]: row for row in repaired_claims.get("data", [])
    }
    terminal_row = repaired_rows.get(repaired["claim_id"])
    require(isinstance(terminal_row, dict), repr(repaired_claims))
    require(
        terminal_row.get("status") == "failed"
        and terminal_row.get("terminal_reason") == "claim_after_next_cadence"
        and terminal_row.get("completed_at") is None,
        repr(terminal_row),
    )

    invalid_reset, _ = request_json(
        base_url,
        "api.php/api/v1/admin/official-slot-epoch",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "action": "reset",
            "pipeline": "ingest-official",
            "expected_epoch_version": 1,
            "reason": "CI verifies an invalid confirmation is rejected",
            "code_revision": revision,
            "confirmation": "NO",
        },
        expected_status=400,
    )
    require(error_code(invalid_reset) == "official_slot_epoch_reset_invalid", repr(invalid_reset))
    reset, _ = request_json(
        base_url,
        "api.php/api/v1/admin/official-slot-epoch",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "action": "reset",
            "pipeline": "ingest-official",
            "expected_epoch_version": 1,
            "reason": "CI advances the durable slot epoch while preserving all claims",
            "code_revision": revision,
            "confirmation": "RESET_OFFICIAL_SLOT_EPOCH_AT_NEXT_KST_DAY",
        },
    )
    require(
        reset.get("epoch_version") == 2 and reset.get("claims_preserved") is True,
        repr(reset),
    )
    epoch, _ = request_json(
        base_url,
        "api.php/api/v1/admin/official-slot-epoch",
        token=ADMIN_TOKEN,
    )
    require(epoch.get("state", {}).get("epoch_version") == 2, repr(epoch))
    require([row.get("epoch_version") for row in epoch.get("history", [])[:2]] == [2, 1], repr(epoch))
    claims_preserved, _ = request_json(
        base_url,
        f"api.php/api/v1/ops/official-slot-claims?from={repair_day}&to={slot_day}&limit=100",
        token=ADMIN_TOKEN,
    )
    require(len(claims_preserved.get("data", [])) == 2, repr(claims_preserved))
    reset_active = datetime.fromisoformat(
        str(reset["active_from"]).replace("Z", "+00:00")
    )
    reset_day = reset_active.astimezone(KST).date().isoformat()
    reset_previous_day = (
        reset_active.astimezone(KST).date() - timedelta(days=1)
    ).isoformat()
    boundary, _ = request_json(
        base_url,
        f"api.php/api/v1/ops/release-evidence?from={reset_previous_day}&to={reset_day}&code_revision={revision}",
        token=ADMIN_TOKEN,
        expected_status=409,
    )
    require(
        error_code(boundary) == "official_slot_epoch_boundary_in_evidence_range",
        repr(boundary),
    )
    # This helper deliberately moved epoch 1 onto a recent cadence slot so it
    # could exercise repair and boundary rejection.  Re-home that synthetic
    # history at the already-future reset boundary before the later, unrelated
    # daily release-evidence fixture runs.  Without this cleanup, a CI job that
    # starts just after KST midnight can place the synthetic epoch inside the
    # immediately preceding complete KST day and fail for clock timing alone.
    reset_active_mysql = reset_active.astimezone(timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_official_slot_claim_epochs "
        f"SET active_from='{reset_active_mysql}' "
        "WHERE pipeline='ingest-official' AND epoch_version=1;",
    )


def event_identity_comparison_key(
    company_id: str,
    event_type: str,
    action: str,
    target: str,
    actor_id: str,
    effective_at: str,
    deadline_at: str,
) -> str:
    values = (
        company_id,
        event_type,
        action,
        target,
        actor_id,
        effective_at,
        deadline_at,
    )
    digest = hashlib.sha256(
        "\x1f".join(("governance-event-identity-v1", *values)).encode("utf-8")
    ).hexdigest()
    return f"eventcmp:v1:{digest}"


def exercise_event_identity_datetime_storage(
    base_url: str, mysql_container_id: str
) -> None:
    server_uuid, database_name = mysql_execute(
        mysql_container_id,
        "SELECT LOWER(@@server_uuid),DATABASE()",
    ).split("\t", 1)
    expected_backend_binding = hashlib.sha256(
        f"mysql8\n{server_uuid}\n{database_name}\nci_".encode()
    ).hexdigest()
    company_id = "00999991"
    event_type = "shareholder_proposal"
    action = "submit"
    target = "board seat"
    actor_id = "actor:identity-precision-smoke"
    effective_date = "2026-07-20"
    deadline_date = "2026-08-31"
    mysql_effective = f"{effective_date} 00:00:00"
    mysql_deadline = f"{deadline_date} 00:00:00"
    source_right_id = "official:dart"
    original_document_id = "dart:20260724999001"
    conflict_document_id = "dart:20260724999002"
    midnight_document_id = "dart:20260724999003"
    kind_document_id = "kind:20260724999004"

    company = {
        "company_id": company_id,
        "stock_code": "999991",
        "market": "KOSDAQ",
        "legal_name": "CI Identity Precision Corp",
        "record_status": "active",
    }
    kind_right = {
        "source_right_id": "official:kind",
        "source_type": "official_disclosure",
        "source_key": "kind",
        "source_name": "KIND",
        "permission_scope": "CI approved historical, incremental, AI and public redistribution scope",
        "evidence_hash": "9" * 64,
        "valid_from": "2021-01-01T00:00:00Z",
        "valid_until": None,
        "ai_allowed": True,
        "redistribution_allowed": True,
        "status": "active",
    }

    def document(
        document_id: str,
        source: str,
        source_right: str,
        title: str,
        *,
        correction_of: str | None = None,
        version_no: int = 1,
    ) -> dict[str, Any]:
        external_id = document_id.split(":", 1)[1]
        url = f"https://example.com/{source}/{external_id}"
        return {
            "document_id": document_id,
            "company_id": company_id,
            "source": source,
            "source_right_id": source_right,
            "source_class": "official_disclosure",
            "external_id": external_id,
            "document_type": event_type,
            "original_language": "ko",
            "title": title,
            "body_text": "",
            "original_url": url,
            "content_hash": hashlib.sha256(
                f"{title}\n{url}\n{external_id}".encode("utf-8")
            ).hexdigest(),
            "collection_key": "identity-precision-date-chain",
            "correction_of_document_id": correction_of,
            "version_no": version_no,
            "published_at": "2026-07-24T00:00:00Z",
            "retrieved_at": "2026-07-24T00:05:00Z",
            "verification_status": "official",
            "publication_status": "published",
            "is_correction": correction_of is not None,
        }

    def event(
        event_id: str,
        comparison_key: str,
        document_id: str,
        identity_target: str,
        effective_at: str,
        deadline_at: str,
        *,
        is_correction: bool = False,
    ) -> dict[str, Any]:
        return {
            "event_id": event_id,
            "company_id": company_id,
            "event_type": event_type,
            "title": "CI event identity precision smoke",
            "original_language": "ko",
            "summary": "",
            "occurred_at": effective_at,
            "deadline_at": deadline_at,
            "importance": "normal",
            "verification_status": "official",
            "collection_key": "identity-precision-date-event",
            "document_ids": [document_id],
            "is_correction": is_correction,
            "is_cancelled": False,
            "review_required": True,
            "actor_id": actor_id,
            "action": action,
            "target": identity_target,
            "identity_action": action,
            "identity_target": identity_target,
            "identity_actor_id": actor_id,
            "identity_effective_at": effective_at,
            "identity_deadline_at": deadline_at,
            "identity_status": "complete",
            "comparison_key": comparison_key,
        }

    date_key = event_identity_comparison_key(
        company_id,
        event_type,
        action,
        target,
        actor_id,
        effective_date,
        deadline_date,
    )
    original_document = document(
        original_document_id,
        "dart",
        source_right_id,
        "CI date-only identity filing",
    )
    original_event = event(
        date_key,
        date_key,
        original_document_id,
        target,
        effective_date,
        deadline_date,
    )
    original_payload = {
        "companies": [company],
        "documents": [original_document],
        "events": [original_event],
        "source_rights": [],
        "run": {},
    }
    first = request_hmac_action(
        base_url, "upsert_governance_snapshot", original_payload, expected_status=200
    )
    require(
        first.get("ok") is True
        and first.get("backend_binding_id") == expected_backend_binding
        and first.get("upserted", {}).get("events") == 1
        and first.get("upserted", {}).get("event_observations") == 1,
        repr(first),
    )
    replay = request_hmac_action(
        base_url, "upsert_governance_snapshot", original_payload, expected_status=200
    )
    require(
        replay.get("ok") is True
        and replay.get("backend_binding_id") == expected_backend_binding
        and replay.get("upserted", {}).get("events") == 1
        and replay.get("upserted", {}).get("event_observations") == 1,
        repr(replay),
    )
    company_master_only_id = "00999979"
    company_master_only = request_hmac_action(
        base_url,
        "upsert_governance_snapshot_dart_guarded",
        {
            "companies": [
                {
                    "company_id": company_master_only_id,
                    "stock_code": "999979",
                    "market": "KOSDAQ",
                    "legal_name": "CI DART company master only",
                    "listing_status": "listed",
                    "record_status": "active",
                }
            ],
            "documents": [],
            "events": [],
            "source_rights": [],
            "run": {},
        },
        expected_status=200,
    )
    require(
        company_master_only.get("ok") is True
        and company_master_only.get("backend_binding_id")
        == expected_backend_binding
        and company_master_only.get("upserted", {}).get("companies") == 1
        and company_master_only.get("upserted", {}).get("documents") == 0
        and company_master_only.get("upserted", {}).get("events") == 0
        and company_master_only.get("upserted", {}).get("runs") == 0,
        repr(company_master_only),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT "
            "(SELECT COUNT(*) FROM ci_companies "
            f"WHERE company_id='{company_master_only_id}'),"
            "(SELECT COUNT(*) FROM ci_issuers "
            f"WHERE issuer_id='issuer:kr:dart:{company_master_only_id}'),"
            "(SELECT COUNT(*) FROM ci_issuer_identifiers "
            f"WHERE issuer_id='issuer:kr:dart:{company_master_only_id}' "
            "AND identifier_type='DART_CORP_CODE' "
            f"AND identifier_value='{company_master_only_id}');",
        )
        == "1\t1\t1",
        "exact guarded DART company-master-only chunk was not projected",
    )
    for attack_index, attack_fields in enumerate(
        (
            {"body_text": "must never be stored"},
            {"body_text": "", "content": "hidden full-text alias"},
        ),
        start=1,
    ):
        attack_company_id = f"0099998{attack_index}"
        attack_document_id = f"dart:2026072499980{attack_index}"
        attack_company = {
            **company,
            "company_id": attack_company_id,
            "stock_code": f"99998{attack_index}",
            "legal_name": f"CI forbidden DART body {attack_index}",
        }
        attack_document = document(
            attack_document_id,
            "dart",
            source_right_id,
            f"CI forbidden DART body attack {attack_index}",
        )
        attack_document["company_id"] = attack_company_id
        attack_document.update(attack_fields)
        body_rejected = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            {
                "companies": [attack_company],
                "documents": [attack_document],
                "events": [],
                "source_rights": [],
                "run": {},
            },
            expected_status=409,
        )
        require(
            body_rejected.get("error") == "dart_body_text_forbidden",
            repr(body_rejected),
        )
        require(
            mysql_execute(
                mysql_container_id,
                "SELECT CONCAT("
                f"(SELECT COUNT(*) FROM ci_companies WHERE company_id='{attack_company_id}'),"
                f"(SELECT COUNT(*) FROM ci_documents WHERE document_id='{attack_document_id}'))",
            )
            == "00",
            "forbidden DART body payload must leave company/document rows unchanged",
        )
    stored_event_only = dict(original_event)
    stored_event_only.pop("document_ids", None)
    stored_event_only["title"] = "Generic HMAC must not rewrite DART event"
    generic_event_rewrite = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [],
            "documents": [],
            "events": [stored_event_only],
            "source_rights": [],
            "run": {},
        },
        expected_status=409,
        use_guarded_dart_action=False,
        inject_dart_preconditions=False,
    )
    require(
        generic_event_rewrite.get("error") == "dart_guarded_action_required",
        repr(generic_event_rewrite),
    )
    stored_title = mysql_execute(
        mysql_container_id,
        "SELECT title FROM ci_governance_events "
        f"WHERE event_id='{original_event['event_id']}';",
    )
    require(
        stored_title == original_event["title"],
        "generic action changed an existing DART event by event_id",
    )

    comparison_only = dict(stored_event_only)
    comparison_only["event_id"] = "event:generic-comparison-bypass"
    comparison_only["title"] = "Generic comparison-key rewrite must fail"
    generic_comparison_rewrite = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [],
            "documents": [],
            "events": [comparison_only],
            "source_rights": [],
            "run": {},
        },
        expected_status=409,
        use_guarded_dart_action=False,
        inject_dart_preconditions=False,
    )
    require(
        generic_comparison_rewrite.get("error")
        == "dart_guarded_action_required",
        repr(generic_comparison_rewrite),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_governance_events "
            "WHERE event_id='event:generic-comparison-bypass';",
        )
        == "0",
        "generic action inserted a comparison-key alias for a DART event",
    )

    disguised_document = dict(original_document)
    disguised_document["title"] = "Generic action must not rewrite DART document"
    disguised_document["source_right_id"] = "official:kind"
    disguised_document["source"] = "kind"
    generic_document_rewrite = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [],
            "documents": [disguised_document],
            "events": [],
            "source_rights": [],
            "run": {},
        },
        expected_status=409,
        use_guarded_dart_action=False,
        inject_dart_preconditions=False,
    )
    require(
        generic_document_rewrite.get("error")
        == "dart_guarded_action_required",
        repr(generic_document_rewrite),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT source_right_id FROM ci_documents "
            f"WHERE document_id='{original_document_id}';",
        )
        == "official:dart",
        "generic action changed existing DART document provenance",
    )

    derived_identity_document = dict(original_document)
    derived_identity_document.pop("document_id", None)
    derived_identity_document.pop("source", None)
    derived_identity_document.pop("source_key", None)
    derived_identity_document.pop("source_right_id", None)
    derived_identity_document["title"] = (
        "Generic missing-ID action must not rewrite derived DART document"
    )
    derived_identity_document["company_id"] = ""
    derived_identity_document["original_url"] = (
        "https://example.invalid/generic-derived-id-overwrite"
    )
    derived_identity_document["content_hash"] = "f" * 64
    derived_identity_rewrite = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [],
            "documents": [derived_identity_document],
            "events": [],
            "source_rights": [],
            "run": {},
        },
        expected_status=409,
        use_guarded_dart_action=False,
        inject_dart_preconditions=False,
    )
    require(
        derived_identity_rewrite.get("error")
        == "dart_document_requires_approved_source_right",
        repr(derived_identity_rewrite),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*),MAX(company_id),MAX(source_right_id),MAX(title),"
            "MAX(original_url),MAX(content_hash) FROM ci_documents "
            f"WHERE document_id='{original_document_id}';",
        )
        == (
            f"1\t{company_id}\tofficial:dart\t{original_document['title']}\t"
            f"{original_document['original_url']}\t"
            f"{original_document['content_hash']}"
        ),
        "missing document_id bypass partially changed a derived DART document",
    )

    generic_company_rewrite = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [
                {
                    **company,
                    "legal_name": "Generic company-only overwrite must fail",
                }
            ],
            "documents": [],
            "events": [],
            "source_rights": [],
            "run": {},
        },
        expected_status=409,
        use_guarded_dart_action=False,
        inject_dart_preconditions=False,
    )
    require(
        generic_company_rewrite.get("error")
        == "dart_guarded_action_required",
        repr(generic_company_rewrite),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT legal_name FROM ci_companies "
            f"WHERE company_id='{company_id}';",
        )
        == company["legal_name"],
        "company-only generic action changed an existing DART company",
    )

    protected_run_id = "run:dart-lineage-no-overwrite"
    mysql_execute(
        mysql_container_id,
        "DELETE FROM ci_collection_runs "
        f"WHERE run_id='{protected_run_id}';"
        "INSERT INTO ci_collection_runs "
        "(run_id,pipeline,source_key,code_revision,status,started_at,finished_at,"
        "first_observed_at,raw_count,acknowledged_count,fetched_count,"
        "resolved_count,accepted_count,error_count,lag_seconds_p95,metrics_json,"
        "created_at,updated_at) VALUES "
        f"('{protected_run_id}','ingest-official','dart',"
        f"'{DEPLOYED_CODE_REVISION}','succeeded','2026-07-24 00:00:00',"
        "'2026-07-24 00:01:00','2026-07-24 00:00:30',1,1,1,1,1,0,NULL,"
        "'{\"fixture\":\"dart-lineage\"}',UTC_TIMESTAMP(),UTC_TIMESTAMP());",
    )
    generic_run_rewrite = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [],
            "documents": [],
            "events": [],
            "source_rights": [],
            "run": {
                "run_id": protected_run_id,
                "pipeline": "generic-overwrite",
                "source_key": "media",
                "status": "failed",
                "raw_count": 9,
                "acknowledged_count": 0,
                "metrics": {"fixture": "generic-overwrite"},
            },
        },
        expected_status=409,
        use_guarded_dart_action=False,
        inject_dart_preconditions=False,
    )
    require(
        generic_run_rewrite.get("error")
        == "dart_guarded_action_required",
        repr(generic_run_rewrite),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT pipeline,source_key,status,raw_count,acknowledged_count,"
            "metrics_json FROM ci_collection_runs "
            f"WHERE run_id='{protected_run_id}';",
        )
        == (
            "ingest-official\tdart\tsucceeded\t1\t1\t"
            '{"fixture":"dart-lineage"}'
        ),
        "generic run-only action changed an existing DART collection run",
    )
    mysql_execute(
        mysql_container_id,
        "DELETE FROM ci_collection_runs "
        f"WHERE run_id='{protected_run_id}';",
    )

    inactive_company_id = "00999985"
    inactive_document_id = "dart:inactive-connector-smoke"
    inactive_event_id = "event:inactive-connector-smoke"
    inactive_run_id = "run:inactive-connector-smoke"
    previous_connector = mysql_execute(
        mysql_container_id,
        "SELECT connector_status,COALESCE(last_error_class,'<NULL>') "
        "FROM ci_source_connectors "
        "WHERE connector_id='connector:kr:dart';",
    ).split("\t", 1)
    require(
        len(previous_connector) == 2
        and previous_connector[0] in {"configured", "active"},
        repr(previous_connector),
    )
    mysql_execute(
        mysql_container_id,
        "DELETE FROM ci_event_documents "
        f"WHERE event_id='{inactive_event_id}';"
        "DELETE FROM ci_event_observations "
        f"WHERE event_id='{inactive_event_id}';"
        "DELETE FROM ci_governance_events "
        f"WHERE event_id='{inactive_event_id}';"
        "DELETE FROM ci_documents "
        f"WHERE document_id='{inactive_document_id}';"
        "DELETE FROM ci_collection_runs "
        f"WHERE run_id='{inactive_run_id}';"
        "DELETE FROM ci_companies "
        f"WHERE company_id='{inactive_company_id}';"
        "UPDATE ci_source_connectors "
        "SET connector_status='inactive',last_error_class='admin_inactive',"
        "updated_at=UTC_TIMESTAMP() "
        "WHERE connector_id='connector:kr:dart';",
    )
    try:
        inactive_eligibility, _ = request_json(
            base_url,
            (
                "api.php/api/v2/ops/source-right-eligibility?"
                + urllib.parse.urlencode(
                    {
                        "source_right_id": "official:dart",
                        "use": "collect",
                    }
                )
            ),
            token=ADMIN_TOKEN,
        )
        require(
            inactive_eligibility.get("eligible") is True
            and inactive_eligibility.get("connector_id")
            == "connector:kr:dart"
            and inactive_eligibility.get("connector_ready") is False
            and "connector_status" not in inactive_eligibility,
            repr(inactive_eligibility),
        )
        inactive_payload = {
            "companies": [
                {
                    "company_id": inactive_company_id,
                    "stock_code": "999985",
                    "market": "KOSDAQ",
                    "legal_name": "Must not cross inactive DART connector",
                    "record_status": "active",
                }
            ],
            "documents": [
                {
                    "document_id": inactive_document_id,
                    "company_id": inactive_company_id,
                    "source": "dart",
                    "source_right_id": "official:dart",
                    "source_class": "official_disclosure",
                    "external_id": "inactive-connector-smoke",
                    "document_type": "shareholder_proposal",
                    "original_language": "ko",
                    "title": "Must not cross inactive DART connector",
                    "body_text": "",
                    "original_url": (
                        "https://opendart.fss.or.kr/"
                        "inactive-connector-smoke"
                    ),
                    "content_hash": hashlib.sha256(
                        b"inactive-connector-smoke"
                    ).hexdigest(),
                    "collection_key": "inactive-connector-smoke",
                    "published_at": "2026-07-24T00:00:00Z",
                    "retrieved_at": "2026-07-24T00:01:00Z",
                    "verification_status": "official",
                    "publication_status": "draft",
                }
            ],
            "events": [
                {
                    "event_id": inactive_event_id,
                    "company_id": inactive_company_id,
                    "event_type": "shareholder_proposal",
                    "title": "Must not cross inactive DART connector",
                    "original_language": "ko",
                    "occurred_at": "2026-07-24T00:00:00Z",
                    "importance": "normal",
                    "verification_status": "official",
                    "collection_key": "inactive-connector-smoke",
                    "document_ids": [inactive_document_id],
                    "review_required": True,
                }
            ],
            "source_rights": [],
            "run": {
                "run_id": inactive_run_id,
                "pipeline": "ingest-official",
                "source_key": "dart",
                "code_revision": DEPLOYED_CODE_REVISION,
                "status": "succeeded",
                "started_at": "2026-07-24T00:00:00Z",
                "finished_at": "2026-07-24T00:01:00Z",
                "raw_count": 1,
                "acknowledged_count": 1,
                "source_outcomes": {
                    "dart": {
                        "status": "succeeded",
                        "raw_count": 1,
                        "error_count": 0,
                    }
                },
                "source_ack_counts": {"dart": 1},
                "window_start": "2026-07-24",
                "window_end": "2026-07-24",
            },
        }
        inactive_write = request_hmac_action(
            base_url,
            "upsert_governance_snapshot_dart_guarded",
            inactive_payload,
            expected_status=409,
        )
        require(
            inactive_write.get("error") == "dart_connector_inactive",
            repr(inactive_write),
        )
        require(
            mysql_execute(
                mysql_container_id,
                "SELECT "
                "(SELECT COUNT(*) FROM ci_companies "
                f"WHERE company_id='{inactive_company_id}'),"
                "(SELECT COUNT(*) FROM ci_documents "
                f"WHERE document_id='{inactive_document_id}'),"
                "(SELECT COUNT(*) FROM ci_governance_events "
                f"WHERE event_id='{inactive_event_id}'),"
                "(SELECT COUNT(*) FROM ci_collection_runs "
                f"WHERE run_id='{inactive_run_id}'),"
                "(SELECT connector_status FROM ci_source_connectors "
                "WHERE connector_id='connector:kr:dart');",
            )
            == "0\t0\t0\t0\tinactive",
            "inactive DART connector allowed a data mutation or was resurrected",
        )
    finally:
        previous_error_sql = (
            "NULL"
            if previous_connector[1] == "<NULL>"
            else "'"
            + previous_connector[1].replace("\\", "\\\\").replace("'", "''")
            + "'"
        )
        mysql_execute(
            mysql_container_id,
            "UPDATE ci_source_connectors "
            f"SET connector_status='{previous_connector[0]}',"
            f"last_error_class={previous_error_sql},updated_at=UTC_TIMESTAMP() "
            "WHERE connector_id='connector:kr:dart';",
        )

    cross_source_correction = document(
        "kind:20260724999005",
        "kind",
        "official:kind",
        "Generic correction reference must not claim DART lineage",
        correction_of=original_document_id,
        version_no=2,
    )
    generic_correction_rewrite = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [],
            "documents": [cross_source_correction],
            "events": [],
            "source_rights": [],
            "run": {},
        },
        expected_status=409,
        use_guarded_dart_action=False,
        inject_dart_preconditions=False,
    )
    require(
        generic_correction_rewrite.get("error")
        == "dart_guarded_action_required",
        repr(generic_correction_rewrite),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_documents "
            "WHERE document_id='kind:20260724999005';",
        )
        == "0",
        "generic correction reference inserted a DART-linked document",
    )

    mysql_execute(
        mysql_container_id,
        "DELETE FROM ci_event_documents "
        f"WHERE event_id='{original_event['event_id']}';"
        "UPDATE ci_governance_events SET issuer_id=NULL,country_code=NULL "
        f"WHERE event_id='{original_event['event_id']}';",
    )
    observation_only_rewrite = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [],
            "documents": [],
            "events": [stored_event_only],
            "source_rights": [],
            "run": {},
        },
        expected_status=409,
        use_guarded_dart_action=False,
        inject_dart_preconditions=False,
    )
    require(
        observation_only_rewrite.get("error")
        == "dart_guarded_action_required",
        repr(observation_only_rewrite),
    )

    mysql_execute(
        mysql_container_id,
        "DELETE FROM ci_event_observations "
        f"WHERE event_id='{original_event['event_id']}';"
        "UPDATE ci_governance_events "
        f"SET issuer_id='issuer:kr:dart:{company_id}',country_code='KR' "
        f"WHERE event_id='{original_event['event_id']}';",
    )
    projection_only_rewrite = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [],
            "documents": [],
            "events": [stored_event_only],
            "source_rights": [],
            "run": {},
        },
        expected_status=409,
        use_guarded_dart_action=False,
        inject_dart_preconditions=False,
    )
    require(
        projection_only_rewrite.get("error")
        == "dart_guarded_action_required",
        repr(projection_only_rewrite),
    )
    restored = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        original_payload,
        expected_status=200,
    )
    require(
        restored.get("ok") is True
        and restored.get("upserted", {}).get("event_observations") == 1,
        repr(restored),
    )

    conflict_target = "audit committee seat"
    conflict_key = event_identity_comparison_key(
        company_id,
        event_type,
        action,
        conflict_target,
        actor_id,
        effective_date,
        deadline_date,
    )
    conflict_payload = {
        "companies": [company],
        "documents": [
            document(
                conflict_document_id,
                "dart",
                source_right_id,
                "Correction: conflicting identity must fail closed",
                correction_of=original_document_id,
                version_no=2,
            )
        ],
        "events": [
            event(
                date_key,
                conflict_key,
                conflict_document_id,
                conflict_target,
                effective_date,
                deadline_date,
                is_correction=True,
            )
        ],
        "source_rights": [],
        "run": {},
    }
    conflict = request_hmac_action(
        base_url, "upsert_governance_snapshot", conflict_payload, expected_status=409
    )
    require(
        conflict.get("ok") is False
        and error_code(conflict) == "followup_event_identity_conflict"
        and conflict.get("validation_reason") == "followup_event_identity_conflict",
        repr(conflict),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_documents "
            f"WHERE document_id='{conflict_document_id}'",
        )
        == "0",
        "conflicting correction must roll back its document",
    )

    midnight_effective = f"{effective_date}T00:00:00+00:00"
    midnight_deadline = f"{deadline_date}T00:00:00+00:00"
    midnight_key = event_identity_comparison_key(
        company_id,
        event_type,
        action,
        target,
        actor_id,
        midnight_effective,
        midnight_deadline,
    )
    midnight_payload = {
        "companies": [company],
        "documents": [
            document(
                midnight_document_id,
                "dart",
                source_right_id,
                "CI explicit UTC-midnight identity filing",
            )
        ],
        "events": [
            event(
                midnight_key,
                midnight_key,
                midnight_document_id,
                target,
                "2026-07-20T00:00:00Z",
                "2026-08-31T00:00:00Z",
            )
        ],
        "source_rights": [],
        "run": {},
    }
    midnight = request_hmac_action(
        base_url, "upsert_governance_snapshot", midnight_payload, expected_status=200
    )
    require(midnight.get("ok") is True, repr(midnight))

    kind_payload = {
        "companies": [company],
        "documents": [
            document(
                kind_document_id,
                "kind",
                "official:kind",
                "CI KIND observation of the date-only identity",
            )
        ],
        "events": [
            event(
                date_key,
                date_key,
                kind_document_id,
                target,
                effective_date,
                deadline_date,
            )
        ],
        "source_rights": [kind_right],
        "run": {},
    }
    kind = request_hmac_action(
        base_url,
        "upsert_governance_snapshot_dart_guarded",
        kind_payload,
        expected_status=200,
    )
    require(kind.get("ok") is True, repr(kind))

    runtime_events, _ = request_json(
        base_url,
        "api.php/api/v1/ops/runtime-state?resource=governance_events&limit=100",
        token=ADMIN_TOKEN,
    )
    company_events = {
        str(row.get("event_id"))
        for row in runtime_events.get("data", {}).get("records", [])
        if row.get("company_id") == company_id
    }
    require(company_events == {date_key, midnight_key}, repr(runtime_events))

    stored_rows = mysql_execute(
        mysql_container_id,
        "SELECT event_id,identity_effective_at,identity_deadline_at,comparison_key "
        "FROM ci_governance_events "
        f"WHERE company_id='{company_id}' ORDER BY event_id",
    ).splitlines()
    parsed_rows = [row.split("\t") for row in stored_rows if row]
    require(len(parsed_rows) == 2, repr(parsed_rows))
    require(
        {row[0] for row in parsed_rows} == {date_key, midnight_key}
        and all(
            len(row) == 4
            and row[1] == mysql_effective
            and row[2] == mysql_deadline
            and row[3] == row[0]
            for row in parsed_rows
        ),
        repr(parsed_rows),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*),COUNT(DISTINCT source_key) "
            "FROM ci_event_observations "
            f"WHERE event_id='{date_key}'",
        )
        == "2\t2",
        "DART and KIND must contribute two observations to one date-only event",
    )
    incomplete_correction_event_id = "event:ci-incomplete-correction-self-replay"
    incomplete_cancellation_event_id = "event:ci-incomplete-cancellation-self-replay"
    incomplete_correction_document_id = "dart:20260724999005"
    incomplete_cancellation_document_id = "dart:20260724999006"

    def incomplete_followup(
        event_id: str,
        document_id: str,
        *,
        is_correction: bool,
        is_cancelled: bool,
    ) -> dict[str, Any]:
        return {
            "event_id": event_id,
            "company_id": company_id,
            "event_type": event_type,
            "title": (
                "CI incomplete correction self replay"
                if is_correction
                else "CI incomplete cancellation self replay"
            ),
            "metadata": {"title_provenance": "source"},
            "original_language": "ko",
            "summary": "",
            "occurred_at": "2026-07-22T00:00:00Z",
            "deadline_at": None,
            "importance": "normal",
            "verification_status": "official",
            "collection_key": f"identity-incomplete-{event_id}",
            "document_ids": [document_id],
            "is_correction": is_correction,
            "is_cancelled": is_cancelled,
            "review_required": True,
            "actor_id": None,
            "action": action,
            "target": target,
            "identity_action": action,
            "identity_target": target,
            "identity_actor_id": None,
            "identity_effective_at": "2026-07-22T00:00:00Z",
            "identity_deadline_at": None,
            "identity_status": "needs_review",
            "comparison_key": None,
        }

    correction_document = document(
        incomplete_correction_document_id,
        "dart",
        source_right_id,
        "CI incomplete correction receipt",
    )
    correction_document["is_correction"] = True
    correction_document["remarks"] = ""
    correction_document["has_later_correction"] = False
    correction_document["is_withdrawn_by_remark"] = False
    correction_document["content_hash"] = hashlib.sha256(
        (
            f"{correction_document['title']}\n"
            f"{correction_document['original_url']}\n"
        ).encode("utf-8")
    ).hexdigest()
    cancellation_document = document(
        incomplete_cancellation_document_id,
        "dart",
        source_right_id,
        "CI incomplete cancellation receipt",
    )
    cancellation_document["is_cancelled"] = True
    correction_event = incomplete_followup(
        incomplete_correction_event_id,
        incomplete_correction_document_id,
        is_correction=True,
        is_cancelled=False,
    )
    correction_event["has_later_correction"] = False
    cancellation_event = incomplete_followup(
        incomplete_cancellation_event_id,
        incomplete_cancellation_document_id,
        is_correction=False,
        is_cancelled=True,
    )

    def followup_payload(
        followup_document: dict[str, Any], followup_event: dict[str, Any]
    ) -> dict[str, Any]:
        return {
            "companies": [company],
            "documents": [followup_document],
            "events": [followup_event],
            "source_rights": [],
            "run": {},
        }

    def followup_row_signature(event_id: str, document_id: str) -> str:
        return mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',"
            f"(SELECT COUNT(*) FROM ci_governance_events WHERE event_id='{event_id}'),"
            f"(SELECT COUNT(*) FROM ci_documents WHERE document_id='{document_id}'),"
            f"(SELECT COUNT(*) FROM ci_event_documents WHERE event_id='{event_id}'),"
            f"(SELECT COUNT(*) FROM ci_event_observations WHERE event_id='{event_id}'),"
            f"(SELECT COUNT(*) FROM ci_timeline_entries WHERE event_id='{event_id}'),"
            "(SELECT COUNT(*) FROM ci_editorial_revisions "
            f"WHERE entity_type='event' AND entity_id='{event_id}'))",
        )

    def followup_semantic_signature(event_id: str, document_id: str) -> str:
        return mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),payload_json,verification_status,"
            "review_status,publication_status,event_type,title,original_language,"
            "occurred_at,COALESCE(deadline_at,'<NULL>'),identity_status,"
            "COALESCE(comparison_key,'<NULL>')) ,256) "
            f"FROM ci_governance_events WHERE event_id='{event_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),payload_json,verification_status,"
            "publication_status,source_class,COALESCE(source_right_id,'<NULL>'),"
            "external_id,COALESCE(document_type,'<NULL>'),original_language,title,"
            "COALESCE(body_text,'<NULL>'),original_url,content_hash,"
            "COALESCE(collection_key,'<NULL>'),"
            "COALESCE(correction_of_document_id,'<NULL>'),version_no,"
            "COALESCE(retrieved_at,'<NULL>')) ,256) "
            f"FROM ci_documents WHERE document_id='{document_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),document_id,relation_type,position_no),256) "
            f"FROM ci_event_documents WHERE event_id='{event_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),document_id,source_class,source_key,"
            "payload_hash,payload_json),256) "
            f"FROM ci_event_observations WHERE event_id='{event_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),COALESCE(document_id,'<NULL>'),"
            "occurred_at,entry_type,title,COALESCE(description,'<NULL>'),"
            "original_language,review_status,publication_status),256) "
            f"FROM ci_timeline_entries WHERE event_id='{event_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),field_name,"
            "COALESCE(previous_value,'<NULL>'),COALESCE(revised_value,'<NULL>'),"
            "reason,revision_status,requested_by,COALESCE(reviewed_by,'<NULL>')),256) "
            "FROM ci_editorial_revisions "
            f"WHERE entity_type='event' AND entity_id='{event_id}'))",
        )

    def reviewed_company_master_signature() -> str:
        issuer_id = f"issuer:kr:dart:{company_id}"
        return mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),stock_code,market,legal_name,"
            "COALESCE(legal_name_en,'<NULL>'),COALESCE(short_name,'<NULL>'),"
            "COALESCE(aliases_json,'<NULL>'),COALESCE(homepage_url,'<NULL>'),"
            "record_status,listing_status,"
            "COALESCE(master_modified_at,'<NULL>'),created_at,updated_at),256) "
            f"FROM ci_companies WHERE company_id='{company_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),country_code,legal_name,"
            "COALESCE(legal_name_en,'<NULL>'),COALESCE(short_name,'<NULL>'),"
            "original_language,COALESCE(homepage_url,'<NULL>'),listing_status,"
            "record_status,COALESCE(master_modified_at,'<NULL>'),"
            "COALESCE(payload_json,'<NULL>'),created_at,updated_at),256) "
            f"FROM ci_issuers WHERE issuer_id='{issuer_id}'),"
            "(SELECT COALESCE(SHA2(GROUP_CONCAT(CONCAT_WS(CHAR(31),"
            "identifier_type,identifier_value,market,is_primary,"
            "COALESCE(valid_from,'<NULL>'),COALESCE(valid_until,'<NULL>'),"
            "created_at,updated_at) ORDER BY identifier_type,identifier_value,"
            "market),256),SHA2('<NONE>',256)) FROM ci_issuer_identifiers "
            f"WHERE issuer_id='{issuer_id}'),"
            "(SELECT COALESCE(SHA2(GROUP_CONCAT(CONCAT_WS(CHAR(31),listing_id,"
            "country_code,market,COALESCE(ticker,'<NULL>'),"
            "COALESCE(isin,'<NULL>'),COALESCE(currency_code,'<NULL>'),"
            "listing_status,is_primary,created_at,updated_at) "
            "ORDER BY listing_id),256),SHA2('<NONE>',256)) "
            f"FROM ci_issuer_listings WHERE issuer_id='{issuer_id}'))",
        )

    def reviewed_company_master_rows() -> str:
        issuer_id = f"issuer:kr:dart:{company_id}"
        return mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',"
            f"(SELECT COUNT(*) FROM ci_companies WHERE company_id='{company_id}'),"
            f"(SELECT COUNT(*) FROM ci_issuers WHERE issuer_id='{issuer_id}'),"
            "(SELECT COUNT(*) FROM ci_issuer_identifiers "
            f"WHERE issuer_id='{issuer_id}'),"
            "(SELECT COUNT(*) FROM ci_issuer_listings "
            f"WHERE issuer_id='{issuer_id}'))",
        )

    for followup_document, followup_event in (
        (correction_document, correction_event),
        (cancellation_document, cancellation_event),
    ):
        first_followup = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            followup_payload(followup_document, followup_event),
            expected_status=200,
        )
        require(first_followup.get("ok") is True, repr(first_followup))
        mysql_execute(
            mysql_container_id,
            "UPDATE ci_governance_events "
            "SET review_status='approved',publication_status='published' "
            f"WHERE event_id='{followup_event['event_id']}';"
            "UPDATE ci_timeline_entries "
            "SET review_status='approved',publication_status='published' "
            f"WHERE event_id='{followup_event['event_id']}';"
            "UPDATE ci_editorial_revisions "
            "SET revision_status='rejected',reviewed_by='ci-sentinel-reviewer' "
            f"WHERE entity_type='event' AND entity_id='{followup_event['event_id']}';",
        )
        before_replay = followup_row_signature(
            str(followup_event["event_id"]),
            str(followup_document["document_id"]),
        )
        require(
            before_replay == "1|1|1|1|1|1",
            f"incomplete follow-up fixture is incomplete: {before_replay!r}",
        )
        semantic_before_replay = followup_semantic_signature(
            str(followup_event["event_id"]),
            str(followup_document["document_id"]),
        )
        require(
            len(semantic_before_replay.split("|")) == 6
            and all(len(value) == 64 for value in semantic_before_replay.split("|")),
            f"incomplete follow-up semantic signature is incomplete: {semantic_before_replay!r}",
        )
        require(
            mysql_execute(
                mysql_container_id,
                "SELECT CONCAT_WS('|',identity_status,"
                "JSON_UNQUOTE(JSON_EXTRACT(payload_json,'$.event_link_status'))) "
                "FROM ci_governance_events "
                f"WHERE event_id='{followup_event['event_id']}'",
            )
            == "needs_review|ambiguous_independent",
            "incomplete follow-up must retain its fail-closed isolation marker",
        )
        replay_followup = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            followup_payload(followup_document, followup_event),
            expected_status=200,
        )
        require(replay_followup.get("ok") is True, repr(replay_followup))
        require(
            followup_row_signature(
                str(followup_event["event_id"]),
                str(followup_document["document_id"]),
            )
            == before_replay,
            "exact incomplete correction/cancellation replay increased row counts",
        )
        require(
            followup_semantic_signature(
                str(followup_event["event_id"]),
                str(followup_document["document_id"]),
            )
            == semantic_before_replay,
            "exact incomplete correction/cancellation replay changed semantic state",
        )

    immutable_mutations: tuple[tuple[str, Any], ...] = (
        ("event_id", incomplete_correction_event_id.upper()),
        ("event_type", "annual_meeting"),
        ("title", "CI mutated incomplete correction"),
        ("summary", "mutated summary"),
        ("original_language", "en"),
        ("occurred_at", "2026-07-23T00:00:00Z"),
        ("deadline_at", "2026-09-01T00:00:00Z"),
        ("importance", "high"),
        ("verification_status", "signal"),
        ("review_required", False),
        ("collection_key", "identity-mutated-collection"),
        ("metadata", {"title_provenance": "derived"}),
        (
            "actor",
            {
                "actor_id": "actor:mutated-incomplete-followup",
                "actor_type": "institution",
                "display_name": "Mutated Actor",
                "company_id": None,
                "review_status": "pending",
                "record_status": "inactive",
            },
        ),
        (
            "event_actor",
            {
                "event_id": incomplete_correction_event_id,
                "actor_id": "actor:mutated-incomplete-followup",
                "actor_role": "filer",
                "review_status": "pending",
            },
        ),
        ("action", "withdraw"),
        ("target", "mutated alias target"),
        ("actor_id", "actor:mutated-alias"),
        ("identity_action", "withdraw"),
        ("identity_target", "audit committee seat"),
        ("identity_actor_id", "actor:mutated-incomplete-followup"),
        ("identity_effective_at", "2026-07-23T00:00:00Z"),
        ("identity_deadline_at", "2026-09-02T00:00:00Z"),
        ("identity_status", "complete"),
        ("comparison_key", "eventcmp:v1:" + ("a" * 64)),
        ("is_correction", False),
        ("is_cancelled", True),
        ("company_id", ""),
    )
    correction_signature = followup_row_signature(
        incomplete_correction_event_id,
        incomplete_correction_document_id,
    )
    correction_semantic_signature = followup_semantic_signature(
        incomplete_correction_event_id,
        incomplete_correction_document_id,
    )
    for field, changed_value in immutable_mutations:
        mutated_event = dict(correction_event)
        mutated_event[field] = changed_value
        rejected_mutation = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            followup_payload(correction_document, mutated_event),
            expected_status=409,
        )
        require(
            error_code(rejected_mutation) == "followup_event_identity_conflict",
            f"{field} mutation was not rejected fail-closed: {rejected_mutation!r}",
        )
        require(
            followup_row_signature(
                incomplete_correction_event_id,
                incomplete_correction_document_id,
            )
            == correction_signature,
            f"{field} mutation changed stored follow-up rows",
        )
        require(
            followup_semantic_signature(
                incomplete_correction_event_id,
                incomplete_correction_document_id,
            )
            == correction_semantic_signature,
            f"{field} mutation changed stored follow-up semantic state",
        )

    mismatched_document_event = dict(correction_event)
    mismatched_document_event["document_ids"] = [original_document_id]
    rejected_document_relation = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        followup_payload(correction_document, mismatched_document_event),
        expected_status=409,
    )
    require(
        error_code(rejected_document_relation) == "followup_event_identity_conflict",
        repr(rejected_document_relation),
    )
    require(
        followup_row_signature(
            incomplete_correction_event_id,
            incomplete_correction_document_id,
        )
        == correction_signature,
        "different evidence relation changed stored follow-up rows",
    )
    require(
        followup_semantic_signature(
            incomplete_correction_event_id,
            incomplete_correction_document_id,
        )
        == correction_semantic_signature,
        "different evidence relation changed stored follow-up semantic state",
    )

    for document_field, changed_value in (
        ("title", "CI mutated correction document title"),
        ("original_url", "https://example.com/dart/mutated-followup-url"),
        ("content_hash", "b" * 64),
    ):
        mutated_document = dict(correction_document)
        mutated_document[document_field] = changed_value
        rejected_document_mutation = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            followup_payload(mutated_document, correction_event),
            expected_status=409,
        )
        require(
            error_code(rejected_document_mutation)
            == "followup_event_identity_conflict",
            f"{document_field} document mutation was not rejected: "
            f"{rejected_document_mutation!r}",
        )
        require(
            followup_row_signature(
                incomplete_correction_event_id,
                incomplete_correction_document_id,
            )
            == correction_signature
            and followup_semantic_signature(
                incomplete_correction_event_id,
                incomplete_correction_document_id,
            )
            == correction_semantic_signature,
            f"{document_field} mutation changed stored document/event state",
        )

    document_only_variants = (
        correction_document,
        {
            **correction_document,
            "title": "CI protected document-only mutation",
        },
    )
    for protected_document in document_only_variants:
        rejected_document_only = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            {
                "companies": [company],
                "documents": [protected_document],
                "events": [],
                "source_rights": [],
                "run": {},
            },
            expected_status=409,
        )
        require(
            error_code(rejected_document_only)
            == "followup_event_identity_conflict",
            f"protected document-only write was not rejected: "
            f"{rejected_document_only!r}",
        )
    require(
        followup_semantic_signature(
            incomplete_correction_event_id,
            incomplete_correction_document_id,
        )
        == correction_semantic_signature,
        "protected document-only write changed stored state",
    )

    case_variant_document = {
        **correction_document,
        "document_id": incomplete_correction_document_id.upper(),
        "title": "CI case-variant protected document mutation",
    }
    rejected_case_variant_document = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        followup_payload(case_variant_document, correction_event),
        expected_status=409,
    )
    require(
        error_code(rejected_case_variant_document)
        == "followup_event_identity_conflict",
        repr(rejected_case_variant_document),
    )
    require(
        followup_semantic_signature(
            incomplete_correction_event_id,
            incomplete_correction_document_id,
        )
        == correction_semantic_signature,
        "case-variant protected document_id changed stored state",
    )

    reused_document_event = dict(correction_event)
    reused_document_event["event_id"] = "event:ci-reused-incomplete-document"
    reused_document_event["collection_key"] = "identity-reused-incomplete-document"
    for protected_document in document_only_variants:
        rejected_reused_document = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            followup_payload(protected_document, reused_document_event),
            expected_status=409,
        )
        require(
            error_code(rejected_reused_document)
            == "followup_event_identity_conflict",
            f"new event_id reused protected evidence: "
            f"{rejected_reused_document!r}",
        )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_governance_events "
            "WHERE event_id='event:ci-reused-incomplete-document'",
        )
        == "0"
        and followup_semantic_signature(
            incomplete_correction_event_id,
            incomplete_correction_document_id,
        )
        == correction_semantic_signature,
        "protected evidence reuse created or changed an event",
    )

    duplicated_document_replay = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [company],
            "documents": [correction_document, dict(correction_document)],
            "events": [correction_event],
            "source_rights": [],
            "run": {},
        },
        expected_status=409,
    )
    require(
        error_code(duplicated_document_replay)
        == "followup_event_identity_conflict",
        repr(duplicated_document_replay),
    )
    require(
        followup_row_signature(
            incomplete_correction_event_id,
            incomplete_correction_document_id,
        )
        == correction_signature
        and followup_semantic_signature(
            incomplete_correction_event_id,
            incomplete_correction_document_id,
        )
        == correction_semantic_signature,
        "duplicate submitted document_id changed stored follow-up state",
    )

    for relation_field, changed_value, restored_value in (
        ("relation_type", "context", "evidence"),
        ("position_no", 7, 0),
    ):
        mysql_execute(
            mysql_container_id,
            "UPDATE ci_event_documents "
            f"SET {relation_field}="
            + (
                f"'{changed_value}'"
                if isinstance(changed_value, str)
                else str(changed_value)
            )
            + f" WHERE event_id='{incomplete_correction_event_id}' "
            f"AND document_id='{incomplete_correction_document_id}';",
        )
        rejected_relation_replay = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            followup_payload(correction_document, correction_event),
            expected_status=409,
        )
        require(
            error_code(rejected_relation_replay)
            == "followup_event_identity_conflict",
            f"{relation_field} mismatch was not rejected: "
            f"{rejected_relation_replay!r}",
        )
        mysql_execute(
            mysql_container_id,
            "UPDATE ci_event_documents "
            f"SET {relation_field}="
            + (
                f"'{restored_value}'"
                if isinstance(restored_value, str)
                else str(restored_value)
            )
            + f" WHERE event_id='{incomplete_correction_event_id}' "
            f"AND document_id='{incomplete_correction_document_id}';",
        )
    require(
        followup_semantic_signature(
            incomplete_correction_event_id,
            incomplete_correction_document_id,
        )
        == correction_semantic_signature,
        "relation mismatch checks did not restore the semantic fixture",
    )

    later_correction_document = {
        **correction_document,
        "remarks": "정",
        "has_later_correction": True,
    }
    later_correction_document["content_hash"] = hashlib.sha256(
        (
            f"{later_correction_document['title']}\n"
            f"{later_correction_document['original_url']}\n"
            f"{later_correction_document['remarks']}"
        ).encode("utf-8")
    ).hexdigest()
    later_correction_event = {
        **correction_event,
        "has_later_correction": True,
    }
    rejected_published_marker = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        followup_payload(later_correction_document, later_correction_event),
        expected_status=409,
    )
    require(
        error_code(rejected_published_marker)
        == "followup_event_identity_conflict",
        "a reviewed/published isolated event accepted an automatic DART marker "
        f"change: {rejected_published_marker!r}",
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_global_lifecycle_observations "
            f"WHERE resolved_event_id='{incomplete_correction_event_id}' "
            f"AND resolved_document_id='{incomplete_correction_document_id}'",
        )
        == "0",
        "rejected reviewed/published marker change wrote lifecycle state",
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_governance_events "
        "SET review_status='pending',publication_status='draft' "
        f"WHERE event_id='{incomplete_correction_event_id}';",
    )
    marker_canonical_signature = followup_semantic_signature(
        incomplete_correction_event_id,
        incomplete_correction_document_id,
    )
    marker_lifecycle_state = mysql_execute(
        mysql_container_id,
        "SELECT CONCAT_WS('|',identity_status,review_status,publication_status,"
        "COALESCE(comparison_key,'<NULL>')) FROM ci_governance_events "
        f"WHERE event_id='{incomplete_correction_event_id}'",
    )
    require(
        marker_lifecycle_state == "needs_review|pending|draft|<NULL>",
        "DART lifecycle fixture must remain fail-closed before the marker change: "
        f"{marker_lifecycle_state!r}",
    )

    accepted_marker = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        followup_payload(later_correction_document, later_correction_event),
        expected_status=200,
    )
    require(accepted_marker.get("ok") is True, repr(accepted_marker))
    require(
        followup_semantic_signature(
            incomplete_correction_event_id,
            incomplete_correction_document_id,
        )
        == marker_canonical_signature,
        "DART later-correction marker changed first-seen canonical state",
    )
    marker_observation = mysql_execute(
        mysql_container_id,
        "SELECT COUNT(*),MIN(connector_id),MIN(country_code),MIN(source_key),"
        "MIN(change_type),MIN(resolution_status),MIN(resolved_document_id),"
        "MIN(resolved_event_id),"
        "MIN(JSON_UNQUOTE(JSON_EXTRACT(payload_json,'$.source_semantics'))),"
        "MIN(HEX(JSON_UNQUOTE(JSON_EXTRACT(payload_json,'$.marker')))),"
        "MIN(HEX(JSON_UNQUOTE(JSON_EXTRACT(payload_json,'$.previous_remarks')))),"
        "MIN(HEX(JSON_UNQUOTE(JSON_EXTRACT(payload_json,'$.current_remarks')))) "
        "FROM ci_global_lifecycle_observations "
        f"WHERE resolved_event_id='{incomplete_correction_event_id}' "
        f"AND resolved_document_id='{incomplete_correction_document_id}'",
    )
    require(
        marker_observation
        == (
            "1\tconnector:kr:dart\tKR\tdart\tupdated\tresolved\t"
            f"{incomplete_correction_document_id}\t"
            f"{incomplete_correction_event_id}\thas_later_correction\t"
            "ECA095\t\tECA095"
        ),
        f"DART marker lifecycle observation is incomplete: {marker_observation!r}",
    )
    marker_observation_signature = mysql_execute(
        mysql_container_id,
        "SELECT CONCAT_WS('|',observation_id,connector_id,country_code,source_key,"
        "external_id,COALESCE(parent_external_id,'<NULL>'),change_type,observed_at,"
        "SHA2(payload_json,256),resolution_status,resolved_document_id,"
        "resolved_event_id,created_at,updated_at) "
        "FROM ci_global_lifecycle_observations "
        f"WHERE resolved_event_id='{incomplete_correction_event_id}' "
        f"AND resolved_document_id='{incomplete_correction_document_id}'",
    )
    replay_marker = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        followup_payload(later_correction_document, later_correction_event),
        expected_status=200,
    )
    require(replay_marker.get("ok") is True, repr(replay_marker))
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',observation_id,connector_id,country_code,"
            "source_key,external_id,COALESCE(parent_external_id,'<NULL>'),"
            "change_type,observed_at,SHA2(payload_json,256),resolution_status,"
            "resolved_document_id,resolved_event_id,created_at,updated_at) "
            "FROM ci_global_lifecycle_observations "
            f"WHERE resolved_event_id='{incomplete_correction_event_id}' "
            f"AND resolved_document_id='{incomplete_correction_document_id}'",
        )
        == marker_observation_signature
        and followup_semantic_signature(
            incomplete_correction_event_id,
            incomplete_correction_document_id,
        )
        == marker_canonical_signature,
        "exact DART marker replay changed canonical or lifecycle state",
    )

    marker_rejection_payloads: list[
        tuple[str, dict[str, Any], dict[str, Any]]
    ] = []
    for label, remarks, marker_value, withdrawn_value in (
        ("withdrawal_marker", "철", True, False),
        ("duplicate_correction_marker", "정정", True, False),
        ("non_boolean_marker", "정", 1, False),
        ("withdrawal_flag", "정", True, True),
    ):
        invalid_marker_document = {
            **correction_document,
            "remarks": remarks,
            "has_later_correction": marker_value,
            "is_withdrawn_by_remark": withdrawn_value,
        }
        invalid_marker_document["content_hash"] = hashlib.sha256(
            (
                f"{invalid_marker_document['title']}\n"
                f"{invalid_marker_document['original_url']}\n"
                f"{invalid_marker_document['remarks']}"
            ).encode("utf-8")
        ).hexdigest()
        marker_rejection_payloads.append(
            (
                label,
                invalid_marker_document,
                later_correction_event,
            )
        )
    marker_rejection_payloads.extend(
        (
            (
                "event_marker_missing",
                later_correction_document,
                correction_event,
            ),
            (
                "document_marker_missing",
                correction_document,
                later_correction_event,
            ),
        )
    )
    for label, rejected_document, rejected_event in marker_rejection_payloads:
        rejected_marker = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            followup_payload(rejected_document, rejected_event),
            expected_status=409,
        )
        require(
            error_code(rejected_marker) == "followup_event_identity_conflict",
            f"{label} did not fail closed: {rejected_marker!r}",
        )
        require(
            followup_semantic_signature(
                incomplete_correction_event_id,
                incomplete_correction_document_id,
            )
            == marker_canonical_signature
            and mysql_execute(
                mysql_container_id,
                "SELECT CONCAT_WS('|',observation_id,connector_id,country_code,"
                "source_key,external_id,COALESCE(parent_external_id,'<NULL>'),"
                "change_type,observed_at,SHA2(payload_json,256),resolution_status,"
                "resolved_document_id,resolved_event_id,created_at,updated_at) "
                "FROM ci_global_lifecycle_observations "
                f"WHERE resolved_event_id='{incomplete_correction_event_id}' "
                f"AND resolved_document_id='{incomplete_correction_document_id}'",
            )
            == marker_observation_signature,
            f"{label} changed canonical or append-only lifecycle state",
        )

    canonical_false_replay = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        followup_payload(correction_document, correction_event),
        expected_status=200,
    )
    require(canonical_false_replay.get("ok") is True, repr(canonical_false_replay))
    require(
        followup_semantic_signature(
            incomplete_correction_event_id,
            incomplete_correction_document_id,
        )
        == marker_canonical_signature
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',observation_id,connector_id,country_code,"
            "source_key,external_id,COALESCE(parent_external_id,'<NULL>'),"
            "change_type,observed_at,SHA2(payload_json,256),resolution_status,"
            "resolved_document_id,resolved_event_id,created_at,updated_at) "
            "FROM ci_global_lifecycle_observations "
            f"WHERE resolved_event_id='{incomplete_correction_event_id}' "
            f"AND resolved_document_id='{incomplete_correction_document_id}'",
        )
        == marker_observation_signature,
        "first-seen canonical replay erased or rewrote append-only lifecycle state",
    )

    mysql_execute(
        mysql_container_id,
        "UPDATE ci_governance_events "
        "SET review_status='approved',publication_status='published' "
        f"WHERE event_id='{incomplete_correction_event_id}';",
    )
    approved_marker_signature = followup_semantic_signature(
        incomplete_correction_event_id,
        incomplete_correction_document_id,
    )
    approved_marker_replay = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        followup_payload(later_correction_document, later_correction_event),
        expected_status=200,
    )
    require(approved_marker_replay.get("ok") is True, repr(approved_marker_replay))
    require(
        followup_semantic_signature(
            incomplete_correction_event_id,
            incomplete_correction_document_id,
        )
        == approved_marker_signature
        and mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',observation_id,connector_id,country_code,"
            "source_key,external_id,COALESCE(parent_external_id,'<NULL>'),"
            "change_type,observed_at,SHA2(payload_json,256),resolution_status,"
            "resolved_document_id,resolved_event_id,created_at,updated_at) "
            "FROM ci_global_lifecycle_observations "
            f"WHERE resolved_event_id='{incomplete_correction_event_id}' "
            f"AND resolved_document_id='{incomplete_correction_document_id}'",
        )
        == marker_observation_signature,
        "an already-recorded exact marker replay changed later human approval",
    )

    mysql_execute(
        mysql_container_id,
        "UPDATE ci_governance_events "
        "SET payload_json=JSON_REMOVE(payload_json,'$.event_link_status') "
        f"WHERE event_id='{incomplete_cancellation_event_id}';",
    )
    missing_marker_replay = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        followup_payload(cancellation_document, cancellation_event),
        expected_status=409,
    )
    require(
        error_code(missing_marker_replay) == "followup_event_identity_conflict",
        repr(missing_marker_replay),
    )
    missing_marker_non_followup = dict(cancellation_event)
    missing_marker_non_followup["is_correction"] = False
    missing_marker_non_followup["is_cancelled"] = False
    missing_marker_bypass = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        followup_payload(cancellation_document, missing_marker_non_followup),
        expected_status=409,
    )
    require(
        error_code(missing_marker_bypass) == "followup_event_identity_conflict",
        repr(missing_marker_bypass),
    )

    reviewed_ordinary_event_id = "event:ci-reviewed-ordinary-read-only"
    reviewed_ordinary_document_id = "dart:20260724999009"
    rejected_ordinary_document_id = "dart:20260724999010"
    reviewed_ordinary_actor_id = "actor:ci-reviewed-ordinary"
    reviewed_ordinary_document = document(
        reviewed_ordinary_document_id,
        "dart",
        source_right_id,
        "CI reviewed ordinary source receipt",
    )
    reviewed_ordinary_event = incomplete_followup(
        reviewed_ordinary_event_id,
        reviewed_ordinary_document_id,
        is_correction=False,
        is_cancelled=False,
    )
    reviewed_ordinary_event.update(
        {
            "title": "CI reviewed ordinary source event",
            "deadline_at": f"{deadline_date}T00:00:00Z",
            "actor_id": reviewed_ordinary_actor_id,
            "identity_actor_id": reviewed_ordinary_actor_id,
            "identity_deadline_at": f"{deadline_date}T00:00:00Z",
            "actor": {
                "actor_id": reviewed_ordinary_actor_id,
                "actor_type": "institution",
                "display_name": "CI Reviewed Ordinary Filer",
                "company_id": None,
                "country_code": "KR",
                "review_status": "pending",
                "record_status": "inactive",
            },
            "event_actor": {
                "event_id": reviewed_ordinary_event_id,
                "actor_id": reviewed_ordinary_actor_id,
                "actor_role": "filer",
                "review_status": "pending",
            },
        }
    )
    reviewed_ordinary_source_write = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        followup_payload(
            reviewed_ordinary_document,
            reviewed_ordinary_event,
        ),
        expected_status=200,
    )
    require(
        reviewed_ordinary_source_write.get("ok") is True,
        repr(reviewed_ordinary_source_write),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',identity_status,review_status,"
            "publication_status,"
            "JSON_CONTAINS_PATH(payload_json,'one','$.event_link_status')) "
            "FROM ci_governance_events "
            f"WHERE event_id='{reviewed_ordinary_event_id}'",
        )
        == "needs_review|pending|draft|0",
        "reviewed ordinary source fixture did not retain its raw no-marker state",
    )

    reviewed_ordinary_family = "meeting_and_vote"
    reviewed_ordinary_action = "confirm meeting agenda"
    reviewed_ordinary_target = "editorial canonical voting item"
    reviewed_ordinary_effective_at = "2026-07-22 00:00:00"
    reviewed_ordinary_deadline_at = f"{deadline_date} 00:00:00"
    reviewed_ordinary_identity = {
        "issuer_id": f"issuer:kr:dart:{company_id}",
        "event_family": reviewed_ordinary_family,
        "action": reviewed_ordinary_action,
        "target": reviewed_ordinary_target,
        "actor_id": reviewed_ordinary_actor_id,
        "effective_at": reviewed_ordinary_effective_at,
        "deadline_at": reviewed_ordinary_deadline_at,
    }
    reviewed_ordinary_comparison_key = (
        "global:" + canonical_sha256(reviewed_ordinary_identity)
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_actors SET country_code='KR',review_status='approved',"
        "record_status='active',"
        "updated_at=GREATEST(UTC_TIMESTAMP(),DATE_ADD(updated_at,INTERVAL 1 SECOND)) "
        f"WHERE actor_id='{reviewed_ordinary_actor_id}';"
        "UPDATE ci_event_actors SET review_status='approved',"
        "updated_at=GREATEST(UTC_TIMESTAMP(),DATE_ADD(updated_at,INTERVAL 1 SECOND)) "
        f"WHERE event_id='{reviewed_ordinary_event_id}' "
        f"AND actor_id='{reviewed_ordinary_actor_id}' AND actor_role='filer';"
        "UPDATE ci_documents SET publication_status='published',"
        "updated_at=GREATEST(UTC_TIMESTAMP(),DATE_ADD(updated_at,INTERVAL 1 SECOND)) "
        f"WHERE document_id='{reviewed_ordinary_document_id}';"
        "UPDATE ci_governance_events SET "
        f"global_event_family='{reviewed_ordinary_family}',"
        f"event_type='{reviewed_ordinary_family}',"
        "title='CI editorial canonical ordinary event',"
        "summary='Human-reviewed canonical ordinary summary',"
        "importance='high',current_status='reviewed',"
        f"deadline_at='{reviewed_ordinary_deadline_at}',"
        f"identity_action='{reviewed_ordinary_action}',"
        f"identity_target='{reviewed_ordinary_target}',"
        f"identity_actor_id='{reviewed_ordinary_actor_id}',"
        f"identity_effective_at='{reviewed_ordinary_effective_at}',"
        f"identity_deadline_at='{reviewed_ordinary_deadline_at}',"
        "identity_status='complete',"
        f"comparison_key='{reviewed_ordinary_comparison_key}',"
        "verification_status='official',review_status='approved',"
        "publication_status='published',"
        "updated_at=GREATEST(UTC_TIMESTAMP(),DATE_ADD(updated_at,INTERVAL 1 SECOND)) "
        f"WHERE event_id='{reviewed_ordinary_event_id}';",
    )

    mysql_execute(
        mysql_container_id,
        "UPDATE ci_governance_events SET review_status='pending',"
        "publication_status='draft' "
        f"WHERE event_id='{reviewed_ordinary_event_id}';",
    )
    unreviewed_ordinary_replay = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        followup_payload(
            reviewed_ordinary_document,
            reviewed_ordinary_event,
        ),
        expected_status=409,
    )
    require(
        unreviewed_ordinary_replay.get("ok") is False,
        "unreviewed ordinary event entered the reviewed read-only ACK path",
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_governance_events SET review_status='approved',"
        "publication_status='published' "
        f"WHERE event_id='{reviewed_ordinary_event_id}';",
    )

    def reviewed_ordinary_canonical_signature() -> str:
        return mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),payload_json,"
            "COALESCE(issuer_id,'<NULL>'),COALESCE(country_code,'<NULL>'),"
            "COALESCE(global_event_family,'<NULL>'),event_type,title,"
            "COALESCE(summary,'<NULL>'),occurred_at,"
            "COALESCE(deadline_at,'<NULL>'),importance,"
            "COALESCE(current_status,'<NULL>'),verification_status,"
            "review_status,publication_status,identity_action,identity_target,"
            "identity_actor_id,identity_effective_at,"
            "COALESCE(identity_deadline_at,'<NULL>'),identity_status,"
            "comparison_key,created_at,updated_at),256) "
            "FROM ci_governance_events "
            f"WHERE event_id='{reviewed_ordinary_event_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),payload_json,company_id,"
            "source_right_id,source_class,external_id,document_type,"
            "original_language,title,COALESCE(body_text,'<NULL>'),original_url,"
            "content_hash,collection_key,"
            "COALESCE(correction_of_document_id,'<NULL>'),version_no,"
            "published_at,retrieved_at,verification_status,publication_status,"
            "created_at,updated_at),256) FROM ci_documents "
            f"WHERE document_id='{reviewed_ordinary_document_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),document_id,relation_type,"
            "position_no,created_at),256) FROM ci_event_documents "
            f"WHERE event_id='{reviewed_ordinary_event_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),document_id,source_class,"
            "source_key,first_observed_at,observed_at,payload_hash,payload_json,"
            "created_at,updated_at),256) FROM ci_event_observations "
            f"WHERE event_id='{reviewed_ordinary_event_id}'),"
            "COALESCE((SELECT SHA2(CONCAT_WS(CHAR(31),document_id,occurred_at,"
            "entry_type,title,COALESCE(description,'<NULL>'),original_language,"
            "review_status,publication_status,created_at,updated_at),256) "
            "FROM ci_timeline_entries "
            f"WHERE event_id='{reviewed_ordinary_event_id}'),SHA2('<NONE>',256)),"
            "(SELECT COALESCE(SHA2(GROUP_CONCAT(CONCAT_WS(CHAR(31),revision_id,"
            "field_name,COALESCE(previous_value,'<NULL>'),"
            "COALESCE(revised_value,'<NULL>'),reason,revision_status,"
            "requested_by,COALESCE(reviewed_by,'<NULL>'),"
            "COALESCE(reviewed_at,'<NULL>'),"
            "COALESCE(published_at,'<NULL>'),created_at,updated_at) "
            "ORDER BY revision_id),256),SHA2('<NONE>',256)) "
            "FROM ci_editorial_revisions "
            f"WHERE entity_type='event' AND entity_id='{reviewed_ordinary_event_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),review_status,record_status,"
            "country_code,created_at,updated_at),256) FROM ci_actors "
            f"WHERE actor_id='{reviewed_ordinary_actor_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),actor_role,review_status,"
            "created_at,updated_at),256) FROM ci_event_actors "
            f"WHERE event_id='{reviewed_ordinary_event_id}' "
            f"AND actor_id='{reviewed_ordinary_actor_id}'))",
        )

    reviewed_ordinary_before = reviewed_ordinary_canonical_signature()
    require(
        len(reviewed_ordinary_before.split("|")) == 8
        and all(
            len(value) == 64
            for value in reviewed_ordinary_before.split("|")
        ),
        "reviewed ordinary canonical fixture signature is incomplete: "
        f"{reviewed_ordinary_before!r}",
    )
    reviewed_ordinary_rows_before = followup_row_signature(
        reviewed_ordinary_event_id,
        reviewed_ordinary_document_id,
    )
    reviewed_ordinary_master_before = reviewed_company_master_signature()
    reviewed_ordinary_master_rows_before = reviewed_company_master_rows()
    require(
        reviewed_ordinary_rows_before == "1|1|1|1|0|0",
        "reviewed ordinary fixture row counts are incomplete: "
        f"{reviewed_ordinary_rows_before!r}",
    )
    require(
        len(reviewed_ordinary_master_before.split("|")) == 4
        and all(
            len(value) == 64
            for value in reviewed_ordinary_master_before.split("|")
        )
        and reviewed_ordinary_master_rows_before == "1|1|2|1",
        "reviewed ordinary company/issuer fixture is incomplete",
    )
    reviewed_ordinary_lifecycle_before = mysql_execute(
        mysql_container_id,
        "SELECT COUNT(*) FROM ci_global_lifecycle_observations "
        f"WHERE resolved_event_id='{reviewed_ordinary_event_id}'",
    )
    require(
        reviewed_ordinary_lifecycle_before == "0",
        "reviewed ordinary fixture unexpectedly has lifecycle writes",
    )
    reviewed_ordinary_document_without_correction_flag = dict(
        reviewed_ordinary_document
    )
    reviewed_ordinary_document_without_correction_flag.pop("is_correction")
    for replay_label, replay_document in (
        ("explicit_false", reviewed_ordinary_document),
        ("omitted", reviewed_ordinary_document_without_correction_flag),
    ):
        reviewed_ordinary_ack = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            followup_payload(
                replay_document,
                reviewed_ordinary_event,
            ),
            expected_status=200,
        )
        require(
            reviewed_ordinary_ack.get("ok") is True
            and reviewed_ordinary_ack.get("upserted", {}).get("documents") == 1
            and reviewed_ordinary_ack.get("upserted", {}).get("events") == 1
            and reviewed_ordinary_ack.get("upserted", {}).get(
                "event_documents"
            )
            == 1
            and reviewed_ordinary_ack.get("upserted", {}).get(
                "event_observations"
            )
            == 1,
            repr(reviewed_ordinary_ack),
        )
        require(
            reviewed_ordinary_canonical_signature()
            == reviewed_ordinary_before
            and followup_row_signature(
                reviewed_ordinary_event_id,
                reviewed_ordinary_document_id,
            )
            == reviewed_ordinary_rows_before
            and reviewed_company_master_signature()
            == reviewed_ordinary_master_before
            and reviewed_company_master_rows()
            == reviewed_ordinary_master_rows_before
            and mysql_execute(
                mysql_container_id,
                "SELECT COUNT(*) FROM ci_global_lifecycle_observations "
                f"WHERE resolved_event_id='{reviewed_ordinary_event_id}'",
            )
            == reviewed_ordinary_lifecycle_before,
            f"reviewed ordinary ACK {replay_label} changed canonical state",
        )

    reviewed_ordinary_payload_hex = mysql_execute(
        mysql_container_id,
        "SELECT HEX(payload_json) FROM ci_documents "
        f"WHERE document_id='{reviewed_ordinary_document_id}'",
    )
    require(
        reviewed_ordinary_payload_hex != "",
        "reviewed ordinary document payload fixture is missing",
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_documents SET "
        "payload_json=JSON_REMOVE(payload_json,'$.is_correction') "
        f"WHERE document_id='{reviewed_ordinary_document_id}';",
    )
    reviewed_ordinary_missing_flag_before = (
        reviewed_ordinary_canonical_signature()
    )
    for replay_label, replay_document in (
        ("omitted", reviewed_ordinary_document_without_correction_flag),
        ("explicit_false", reviewed_ordinary_document),
    ):
        reviewed_ordinary_missing_flag_ack = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            followup_payload(replay_document, reviewed_ordinary_event),
            expected_status=200,
        )
        require(
            reviewed_ordinary_missing_flag_ack.get("ok") is True
            and reviewed_ordinary_canonical_signature()
            == reviewed_ordinary_missing_flag_before,
            "reviewed ordinary stored-omitted document flag with "
            f"{replay_label} submission was not a read-only ACK",
        )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_documents SET "
        f"payload_json=UNHEX('{reviewed_ordinary_payload_hex}') "
        f"WHERE document_id='{reviewed_ordinary_document_id}';",
    )
    require(
        reviewed_ordinary_canonical_signature() == reviewed_ordinary_before,
        "reviewed ordinary fixture did not restore its strict false flag",
    )
    for stored_correction_json in ('\"false\"', "true"):
        mysql_execute(
            mysql_container_id,
            "UPDATE ci_documents SET payload_json=JSON_SET("
            "payload_json,'$.is_correction',JSON_EXTRACT("
            f"'{stored_correction_json}','$')) "
            f"WHERE document_id='{reviewed_ordinary_document_id}';",
        )
        rejected_stored_flag = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            followup_payload(
                reviewed_ordinary_document_without_correction_flag,
                reviewed_ordinary_event,
            ),
            expected_status=409,
        )
        require(
            error_code(rejected_stored_flag)
            == "followup_event_identity_conflict",
            "stored ordinary correction flag type/value mismatch entered "
            f"the ACK path: {rejected_stored_flag!r}",
        )
        mysql_execute(
            mysql_container_id,
            "UPDATE ci_documents SET "
            f"payload_json=UNHEX('{reviewed_ordinary_payload_hex}') "
            f"WHERE document_id='{reviewed_ordinary_document_id}';",
        )
        require(
            reviewed_ordinary_canonical_signature()
            == reviewed_ordinary_before,
            "reviewed ordinary fixture did not recover after stored flag "
            "rejection",
        )

    reviewed_company_mutations = (
        ("legal_name", "CI changed legal name"),
        ("stock_code", "888888"),
        ("market", "KOSPI"),
        ("legal_name_en", "CI Changed English Name"),
        ("short_name", "CI Changed"),
        ("aliases", ["CI changed alias"]),
        ("homepage_url", "https://example.com/changed-company"),
        ("record_status", "inactive"),
        ("listing_status", "listed"),
        ("master_modified_at", "2026-07-31T00:00:00Z"),
    )
    for field, mutated_value in reviewed_company_mutations:
        mutated_company_payload = followup_payload(
            reviewed_ordinary_document,
            reviewed_ordinary_event,
        )
        mutated_company_payload["companies"] = [
            {**company, field: mutated_value}
        ]
        rejected_company_replay = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            mutated_company_payload,
            expected_status=409,
        )
        require(
            error_code(rejected_company_replay)
            == "followup_event_identity_conflict",
            f"{field} company mutation entered the reviewed ACK path: "
            f"{rejected_company_replay!r}",
        )
        require(
            reviewed_ordinary_canonical_signature()
            == reviewed_ordinary_before
            and reviewed_company_master_signature()
            == reviewed_ordinary_master_before
            and reviewed_company_master_rows()
            == reviewed_ordinary_master_rows_before,
            f"{field} company mutation changed reviewed canonical state",
        )

    omitted_market_payload = followup_payload(
        reviewed_ordinary_document,
        reviewed_ordinary_event,
    )
    omitted_market_company = dict(company)
    omitted_market_company.pop("market")
    omitted_market_payload["companies"] = [omitted_market_company]
    rejected_omitted_market_replay = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        omitted_market_payload,
        expected_status=409,
    )
    require(
        error_code(rejected_omitted_market_replay)
        == "followup_event_identity_conflict",
        "omitted market entered the reviewed ACK path even though the "
        "generic projection would replace KOSDAQ with KRX",
    )
    require(
        reviewed_ordinary_canonical_signature()
        == reviewed_ordinary_before
        and reviewed_company_master_signature()
        == reviewed_ordinary_master_before
        and reviewed_company_master_rows()
        == reviewed_ordinary_master_rows_before,
        "omitted market replay changed reviewed canonical state",
    )

    projection_mutations = (
        (
            "issuer",
            "UPDATE ci_issuers SET legal_name='CI corrupted issuer' "
            f"WHERE issuer_id='issuer:kr:dart:{company_id}';",
            "UPDATE ci_issuers SET legal_name='CI Identity Precision Corp' "
            f"WHERE issuer_id='issuer:kr:dart:{company_id}';",
        ),
        (
            "dart_identifier",
            "UPDATE ci_issuer_identifiers SET is_primary=0 "
            f"WHERE issuer_id='issuer:kr:dart:{company_id}' "
            "AND identifier_type='DART_CORP_CODE' "
            f"AND identifier_value='{company_id}' AND market='KRX';",
            "UPDATE ci_issuer_identifiers SET is_primary=1 "
            f"WHERE issuer_id='issuer:kr:dart:{company_id}' "
            "AND identifier_type='DART_CORP_CODE' "
            f"AND identifier_value='{company_id}' AND market='KRX';",
        ),
        (
            "ticker_identifier",
            "UPDATE ci_issuer_identifiers SET is_primary=1 "
            f"WHERE issuer_id='issuer:kr:dart:{company_id}' "
            "AND identifier_type='TICKER' "
            "AND identifier_value='999991' AND market='KOSDAQ';",
            "UPDATE ci_issuer_identifiers SET is_primary=0 "
            f"WHERE issuer_id='issuer:kr:dart:{company_id}' "
            "AND identifier_type='TICKER' "
            "AND identifier_value='999991' AND market='KOSDAQ';",
        ),
        (
            "listing",
            "UPDATE ci_issuer_listings SET market='CORRUPTED' "
            f"WHERE listing_id='listing:kr:{company_id}';",
            "UPDATE ci_issuer_listings SET market='KOSDAQ' "
            f"WHERE listing_id='listing:kr:{company_id}';",
        ),
    )
    for label, corrupt_sql, restore_sql in projection_mutations:
        mysql_execute(mysql_container_id, corrupt_sql)
        rejected_projection_replay = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            followup_payload(
                reviewed_ordinary_document,
                reviewed_ordinary_event,
            ),
            expected_status=409,
        )
        require(
            error_code(rejected_projection_replay)
            == "followup_event_identity_conflict",
            f"{label} projection drift entered the reviewed ACK path: "
            f"{rejected_projection_replay!r}",
        )
        mysql_execute(mysql_container_id, restore_sql)
        require(
            reviewed_ordinary_canonical_signature()
            == reviewed_ordinary_before
            and reviewed_company_master_signature()
            == reviewed_ordinary_master_before
            and reviewed_company_master_rows()
            == reviewed_ordinary_master_rows_before,
            f"{label} projection drift was not restored cleanly",
        )

    ordinary_mutated_summary = {
        **reviewed_ordinary_event,
        "summary": "third-party ordinary summary mutation",
    }
    ordinary_mutated_title_document = {
        **reviewed_ordinary_document,
        "title": "third-party ordinary document title mutation",
    }
    ordinary_mutated_hash_document = {
        **reviewed_ordinary_document,
        "content_hash": "e" * 64,
    }
    ordinary_mutated_url_document = {
        **reviewed_ordinary_document,
        "original_url": "https://example.com/dart/mutated-reviewed-receipt",
    }
    ordinary_mutated_source_document = {
        **reviewed_ordinary_document,
        # Keep the approved DART right so this reaches the reviewed-receipt
        # identity comparison instead of being rejected earlier by the
        # independent source-right guard.
        "source": "dart-mutated",
    }
    ordinary_explicit_correction_document = {
        **reviewed_ordinary_document,
        "is_correction": True,
    }
    ordinary_string_false_correction_document = {
        **reviewed_ordinary_document,
        "is_correction": "false",
    }
    ordinary_mutated_actor_id = "actor:ci-reviewed-ordinary-mutated"
    ordinary_mutated_actor = {
        **reviewed_ordinary_event,
        "actor_id": ordinary_mutated_actor_id,
        "identity_actor_id": ordinary_mutated_actor_id,
        "actor": {
            **reviewed_ordinary_event["actor"],
            "actor_id": ordinary_mutated_actor_id,
        },
        "event_actor": {
            **reviewed_ordinary_event["event_actor"],
            "actor_id": ordinary_mutated_actor_id,
        },
    }
    ordinary_mutated_deadline = {
        **reviewed_ordinary_event,
        "deadline_at": "2026-09-02T00:00:00Z",
        "identity_deadline_at": "2026-09-02T00:00:00Z",
    }
    rejected_ordinary_event_id = "event:ci-reviewed-ordinary-mutated-id"
    ordinary_mutated_event_id = {
        **reviewed_ordinary_event,
        "event_id": rejected_ordinary_event_id,
        "event_actor": {
            **reviewed_ordinary_event["event_actor"],
            "event_id": rejected_ordinary_event_id,
        },
    }
    ordinary_cancellation = {
        **reviewed_ordinary_event,
        "is_cancelled": True,
    }
    ordinary_mutated_id_document = {
        **reviewed_ordinary_document,
        "document_id": rejected_ordinary_document_id,
        "external_id": rejected_ordinary_document_id.split(":", 1)[1],
        "original_url": (
            "https://example.com/dart/"
            + rejected_ordinary_document_id.split(":", 1)[1]
        ),
    }
    ordinary_mutated_id_document["content_hash"] = hashlib.sha256(
        (
            f"{ordinary_mutated_id_document['title']}\n"
            f"{ordinary_mutated_id_document['original_url']}\n"
            f"{ordinary_mutated_id_document['external_id']}"
        ).encode("utf-8")
    ).hexdigest()
    ordinary_mutated_id_event = {
        **reviewed_ordinary_event,
        "document_ids": [rejected_ordinary_document_id],
    }
    ordinary_rejections = (
        (
            "source_summary",
            reviewed_ordinary_document,
            ordinary_mutated_summary,
        ),
        (
            "document_title",
            ordinary_mutated_title_document,
            reviewed_ordinary_event,
        ),
        (
            "document_hash",
            ordinary_mutated_hash_document,
            reviewed_ordinary_event,
        ),
        (
            "document_url",
            ordinary_mutated_url_document,
            reviewed_ordinary_event,
        ),
        (
            "document_source",
            ordinary_mutated_source_document,
            reviewed_ordinary_event,
        ),
        (
            "document_correction_flag",
            ordinary_explicit_correction_document,
            reviewed_ordinary_event,
        ),
        (
            "document_correction_flag_string",
            ordinary_string_false_correction_document,
            reviewed_ordinary_event,
        ),
        (
            "document_id",
            ordinary_mutated_id_document,
            ordinary_mutated_id_event,
        ),
        (
            "event_id",
            reviewed_ordinary_document,
            ordinary_mutated_event_id,
        ),
        (
            "actor",
            reviewed_ordinary_document,
            ordinary_mutated_actor,
        ),
        (
            "deadline",
            reviewed_ordinary_document,
            ordinary_mutated_deadline,
        ),
        (
            "cancellation",
            reviewed_ordinary_document,
            ordinary_cancellation,
        ),
    )
    for label, rejected_document, rejected_event in ordinary_rejections:
        rejected_ordinary_replay = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            followup_payload(rejected_document, rejected_event),
            expected_status=409,
        )
        require(
            error_code(rejected_ordinary_replay)
            == "followup_event_identity_conflict",
            f"{label} mutation entered the reviewed ordinary ACK path: "
            f"{rejected_ordinary_replay!r}",
        )
        require(
            reviewed_ordinary_canonical_signature()
            == reviewed_ordinary_before
            and followup_row_signature(
                reviewed_ordinary_event_id,
                reviewed_ordinary_document_id,
            )
            == reviewed_ordinary_rows_before
            and reviewed_company_master_signature()
            == reviewed_ordinary_master_before
            and reviewed_company_master_rows()
            == reviewed_ordinary_master_rows_before
            and mysql_execute(
                mysql_container_id,
                "SELECT COUNT(*) FROM ci_global_lifecycle_observations "
                f"WHERE resolved_event_id='{reviewed_ordinary_event_id}'",
            )
            == reviewed_ordinary_lifecycle_before,
            f"{label} mutation changed canonical reviewed ordinary state",
        )
    rejected_ordinary_owner_event_id = (
        "event:ci-reviewed-ordinary-owner-mismatch"
    )
    ordinary_owner_mismatch_event = {
        **reviewed_ordinary_event,
        "event_id": rejected_ordinary_owner_event_id,
        "event_actor": {
            **reviewed_ordinary_event["event_actor"],
            "event_id": rejected_ordinary_owner_event_id,
        },
    }
    ordinary_owner_mismatch_payload = followup_payload(
        reviewed_ordinary_document,
        reviewed_ordinary_event,
    )
    ordinary_owner_mismatch_payload["events"].append(
        ordinary_owner_mismatch_event
    )
    rejected_owner_replay = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        ordinary_owner_mismatch_payload,
        expected_status=409,
    )
    require(
        error_code(rejected_owner_replay)
        == "followup_event_identity_conflict",
        "shared document owner mutation entered the reviewed ordinary ACK "
        f"path: {rejected_owner_replay!r}",
    )
    require(
        reviewed_ordinary_canonical_signature()
        == reviewed_ordinary_before
        and followup_row_signature(
            reviewed_ordinary_event_id,
            reviewed_ordinary_document_id,
        )
        == reviewed_ordinary_rows_before
        and mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_governance_events "
            f"WHERE event_id='{rejected_ordinary_owner_event_id}'",
        )
        == "0",
        "shared document owner mutation changed reviewed state",
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT "
            "(SELECT COUNT(*) FROM ci_documents "
            f"WHERE document_id='{rejected_ordinary_document_id}'),"
            "(SELECT COUNT(*) FROM ci_governance_events "
            f"WHERE event_id='{rejected_ordinary_event_id}')",
        )
        == "0\t0",
        "rejected reviewed ordinary event/document ID was persisted",
    )

    reviewed_correction_event_id = "event:ci-reviewed-correction-observation"
    reviewed_correction_document_id = "dart:20260724999007"
    rejected_correction_document_id = "dart:20260724999008"
    reviewed_actor_id = "actor:ci-reviewed-correction"
    reviewed_document = document(
        reviewed_correction_document_id,
        "dart",
        source_right_id,
        "CI reviewed correction source receipt",
    )
    reviewed_document["is_correction"] = True
    reviewed_event = incomplete_followup(
        reviewed_correction_event_id,
        reviewed_correction_document_id,
        is_correction=True,
        is_cancelled=False,
    )
    reviewed_event.update(
        {
            "deadline_at": f"{deadline_date}T00:00:00Z",
            "actor_id": reviewed_actor_id,
            "identity_actor_id": reviewed_actor_id,
            "identity_deadline_at": f"{deadline_date}T00:00:00Z",
            "actor": {
                "actor_id": reviewed_actor_id,
                "actor_type": "institution",
                "display_name": "CI Reviewed Correction Filer",
                "company_id": None,
                "country_code": "KR",
                "review_status": "pending",
                "record_status": "inactive",
            },
            "event_actor": {
                "event_id": reviewed_correction_event_id,
                "actor_id": reviewed_actor_id,
                "actor_role": "filer",
                "review_status": "pending",
            },
        }
    )
    reviewed_source_write = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        followup_payload(reviewed_document, reviewed_event),
        expected_status=200,
    )
    require(reviewed_source_write.get("ok") is True, repr(reviewed_source_write))
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',identity_status,review_status,"
            "publication_status,"
            "JSON_UNQUOTE(JSON_EXTRACT(payload_json,'$.event_link_status'))) "
            "FROM ci_governance_events "
            f"WHERE event_id='{reviewed_correction_event_id}'",
        )
        == "needs_review|pending|draft|ambiguous_independent",
        "reviewed correction source fixture was not isolated fail-closed",
    )

    reviewed_family = "correction_and_withdrawal"
    reviewed_action = "confirm corrected disclosure"
    reviewed_target = "editorial canonical board action"
    reviewed_effective_at = "2026-07-22 00:00:00"
    reviewed_deadline_at = f"{deadline_date} 00:00:00"
    reviewed_identity = {
        "issuer_id": f"issuer:kr:dart:{company_id}",
        "event_family": reviewed_family,
        "action": reviewed_action,
        "target": reviewed_target,
        "actor_id": reviewed_actor_id,
        "effective_at": reviewed_effective_at,
        "deadline_at": reviewed_deadline_at,
    }
    reviewed_comparison_key = "global:" + canonical_sha256(reviewed_identity)
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_actors SET country_code='KR',review_status='approved',"
        "record_status='active',"
        "updated_at=GREATEST(UTC_TIMESTAMP(),DATE_ADD(updated_at,INTERVAL 1 SECOND)) "
        f"WHERE actor_id='{reviewed_actor_id}';"
        "UPDATE ci_event_actors SET review_status='approved',"
        "updated_at=GREATEST(UTC_TIMESTAMP(),DATE_ADD(updated_at,INTERVAL 1 SECOND)) "
        f"WHERE event_id='{reviewed_correction_event_id}' "
        f"AND actor_id='{reviewed_actor_id}' AND actor_role='filer';"
        "UPDATE ci_documents SET publication_status='published',"
        "updated_at=GREATEST(UTC_TIMESTAMP(),DATE_ADD(updated_at,INTERVAL 1 SECOND)) "
        f"WHERE document_id='{reviewed_correction_document_id}';"
        "UPDATE ci_governance_events SET "
        f"global_event_family='{reviewed_family}',event_type='{reviewed_family}',"
        "title='CI editorial canonical correction',"
        "summary='Human-reviewed canonical correction summary',"
        "importance='high',current_status='reviewed',"
        f"deadline_at='{reviewed_deadline_at}',"
        f"identity_action='{reviewed_action}',"
        f"identity_target='{reviewed_target}',"
        f"identity_actor_id='{reviewed_actor_id}',"
        f"identity_effective_at='{reviewed_effective_at}',"
        f"identity_deadline_at='{reviewed_deadline_at}',"
        "identity_status='complete',"
        f"comparison_key='{reviewed_comparison_key}',"
        "verification_status='corrected',review_status='approved',"
        "publication_status='published',"
        "updated_at=GREATEST(UTC_TIMESTAMP(),DATE_ADD(updated_at,INTERVAL 1 SECOND)) "
        f"WHERE event_id='{reviewed_correction_event_id}';",
    )

    mysql_execute(
        mysql_container_id,
        "UPDATE ci_governance_events SET review_status='pending',"
        "publication_status='draft' "
        f"WHERE event_id='{reviewed_correction_event_id}';",
    )
    unreviewed_replay = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        followup_payload(reviewed_document, reviewed_event),
        expected_status=409,
    )
    require(
        error_code(unreviewed_replay) == "followup_event_identity_conflict",
        f"unreviewed complete correction entered the observation-only path: "
        f"{unreviewed_replay!r}",
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_governance_events SET review_status='approved',"
        "publication_status='published' "
        f"WHERE event_id='{reviewed_correction_event_id}';",
    )

    def reviewed_correction_canonical_signature() -> str:
        return mysql_execute(
            mysql_container_id,
            "SELECT CONCAT_WS('|',"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),payload_json,"
            "COALESCE(issuer_id,'<NULL>'),COALESCE(country_code,'<NULL>'),"
            "COALESCE(global_event_family,'<NULL>'),event_type,title,"
            "COALESCE(summary,'<NULL>'),occurred_at,"
            "COALESCE(deadline_at,'<NULL>'),importance,"
            "COALESCE(current_status,'<NULL>'),verification_status,"
            "review_status,publication_status,identity_action,identity_target,"
            "identity_actor_id,identity_effective_at,"
            "COALESCE(identity_deadline_at,'<NULL>'),identity_status,"
            "comparison_key,created_at,updated_at),256) "
            "FROM ci_governance_events "
            f"WHERE event_id='{reviewed_correction_event_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),payload_json,company_id,"
            "source_right_id,source_class,external_id,document_type,"
            "original_language,title,COALESCE(body_text,'<NULL>'),original_url,"
            "content_hash,collection_key,"
            "COALESCE(correction_of_document_id,'<NULL>'),version_no,"
            "published_at,retrieved_at,verification_status,publication_status,"
            "created_at,updated_at),256) FROM ci_documents "
            f"WHERE document_id='{reviewed_correction_document_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),document_id,relation_type,"
            "position_no,created_at),256) FROM ci_event_documents "
            f"WHERE event_id='{reviewed_correction_event_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),document_id,source_class,"
            "source_key,first_observed_at,observed_at,payload_hash,payload_json,"
            "created_at,updated_at),256) FROM ci_event_observations "
            f"WHERE event_id='{reviewed_correction_event_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),document_id,occurred_at,"
            "entry_type,title,COALESCE(description,'<NULL>'),original_language,"
            "review_status,publication_status,created_at,updated_at),256) "
            "FROM ci_timeline_entries "
            f"WHERE event_id='{reviewed_correction_event_id}'),"
            "(SELECT SHA2(GROUP_CONCAT(CONCAT_WS(CHAR(31),revision_id,"
            "field_name,COALESCE(previous_value,'<NULL>'),"
            "COALESCE(revised_value,'<NULL>'),reason,revision_status,"
            "requested_by,COALESCE(reviewed_by,'<NULL>'),"
            "COALESCE(reviewed_at,'<NULL>'),"
            "COALESCE(published_at,'<NULL>'),created_at,updated_at) "
            "ORDER BY revision_id),256) FROM ci_editorial_revisions "
            f"WHERE entity_type='event' AND entity_id='{reviewed_correction_event_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),review_status,record_status,"
            "country_code,created_at,updated_at),256) FROM ci_actors "
            f"WHERE actor_id='{reviewed_actor_id}'),"
            "(SELECT SHA2(CONCAT_WS(CHAR(31),actor_role,review_status,"
            "created_at,updated_at),256) FROM ci_event_actors "
            f"WHERE event_id='{reviewed_correction_event_id}' "
            f"AND actor_id='{reviewed_actor_id}'))",
        )

    reviewed_canonical_before = reviewed_correction_canonical_signature()
    require(
        len(reviewed_canonical_before.split("|")) == 8
        and all(
            len(value) == 64 for value in reviewed_canonical_before.split("|")
        ),
        "reviewed correction canonical fixture signature is incomplete: "
        f"{reviewed_canonical_before!r}",
    )
    reviewed_rows_before = followup_row_signature(
        reviewed_correction_event_id,
        reviewed_correction_document_id,
    )
    reviewed_master_before = reviewed_company_master_signature()
    reviewed_master_rows_before = reviewed_company_master_rows()
    require(
        reviewed_rows_before == "1|1|1|1|1|1",
        "reviewed correction fixture row counts are incomplete: "
        f"{reviewed_rows_before!r}",
    )
    require(
        len(reviewed_master_before.split("|")) == 4
        and all(
            len(value) == 64
            for value in reviewed_master_before.split("|")
        )
        and reviewed_master_rows_before == "1|1|2|1",
        "reviewed correction company/issuer fixture is incomplete",
    )
    reviewed_lifecycle_before = mysql_execute(
        mysql_container_id,
        "SELECT COUNT(*) FROM ci_global_lifecycle_observations "
        f"WHERE resolved_event_id='{reviewed_correction_event_id}'",
    )
    require(
        reviewed_lifecycle_before == "0",
        "reviewed correction fixture unexpectedly has lifecycle writes",
    )
    reviewed_document_without_correction_flag = dict(reviewed_document)
    reviewed_document_without_correction_flag.pop("is_correction")
    reviewed_read_only_ack = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        followup_payload(
            reviewed_document,
            reviewed_event,
        ),
        expected_status=200,
    )
    require(
        reviewed_read_only_ack.get("ok") is True
        and reviewed_read_only_ack.get("upserted", {}).get("documents") == 1
        and reviewed_read_only_ack.get("upserted", {}).get("events") == 1
        and reviewed_read_only_ack.get("upserted", {}).get(
            "event_documents"
        )
        == 1
        and reviewed_read_only_ack.get("upserted", {}).get(
            "event_observations"
        )
        == 1,
        repr(reviewed_read_only_ack),
    )
    require(
        reviewed_correction_canonical_signature() == reviewed_canonical_before
        and followup_row_signature(
            reviewed_correction_event_id,
            reviewed_correction_document_id,
        )
        == reviewed_rows_before
        and reviewed_company_master_signature() == reviewed_master_before
        and reviewed_company_master_rows() == reviewed_master_rows_before
        and mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_global_lifecycle_observations "
            f"WHERE resolved_event_id='{reviewed_correction_event_id}'",
        )
        == reviewed_lifecycle_before,
        "first reviewed correction ACK changed rows, canonical state, or timestamps",
    )
    reviewed_correction_replay = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        followup_payload(
            reviewed_document_without_correction_flag,
            reviewed_event,
        ),
        expected_status=200,
    )
    require(
        reviewed_correction_replay.get("ok") is True,
        repr(reviewed_correction_replay),
    )
    require(
        reviewed_correction_canonical_signature() == reviewed_canonical_before
        and followup_row_signature(
            reviewed_correction_event_id,
            reviewed_correction_document_id,
        )
        == reviewed_rows_before
        and reviewed_company_master_signature() == reviewed_master_before
        and reviewed_company_master_rows() == reviewed_master_rows_before
        and mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_global_lifecycle_observations "
            f"WHERE resolved_event_id='{reviewed_correction_event_id}'",
        )
        == reviewed_lifecycle_before,
        "reviewed correction replay was not idempotent",
    )
    reviewed_correction_payload_hex = mysql_execute(
        mysql_container_id,
        "SELECT HEX(payload_json) FROM ci_documents "
        f"WHERE document_id='{reviewed_correction_document_id}'",
    )
    require(
        reviewed_correction_payload_hex != "",
        "reviewed correction document payload fixture is missing",
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_documents SET "
        "payload_json=JSON_REMOVE(payload_json,'$.is_correction') "
        f"WHERE document_id='{reviewed_correction_document_id}';",
    )
    reviewed_correction_missing_flag_before = (
        reviewed_correction_canonical_signature()
    )
    for replay_label, replay_document in (
        ("omitted", reviewed_document_without_correction_flag),
        ("explicit_true", reviewed_document),
    ):
        reviewed_correction_missing_flag_ack = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            followup_payload(replay_document, reviewed_event),
            expected_status=200,
        )
        require(
            reviewed_correction_missing_flag_ack.get("ok") is True
            and reviewed_correction_canonical_signature()
            == reviewed_correction_missing_flag_before,
            "reviewed correction stored-omitted document flag with "
            f"{replay_label} submission was not a read-only ACK",
        )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_documents SET "
        f"payload_json=UNHEX('{reviewed_correction_payload_hex}') "
        f"WHERE document_id='{reviewed_correction_document_id}';",
    )
    require(
        reviewed_correction_canonical_signature() == reviewed_canonical_before,
        "reviewed correction fixture did not restore its strict true flag",
    )
    for stored_correction_json in ('\"true\"', "false"):
        mysql_execute(
            mysql_container_id,
            "UPDATE ci_documents SET payload_json=JSON_SET("
            "payload_json,'$.is_correction',JSON_EXTRACT("
            f"'{stored_correction_json}','$')) "
            f"WHERE document_id='{reviewed_correction_document_id}';",
        )
        rejected_stored_flag = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            followup_payload(
                reviewed_document_without_correction_flag,
                reviewed_event,
            ),
            expected_status=409,
        )
        require(
            error_code(rejected_stored_flag)
            == "followup_event_identity_conflict",
            "stored correction flag type/value mismatch entered the ACK "
            f"path: {rejected_stored_flag!r}",
        )
        mysql_execute(
            mysql_container_id,
            "UPDATE ci_documents SET "
            f"payload_json=UNHEX('{reviewed_correction_payload_hex}') "
            f"WHERE document_id='{reviewed_correction_document_id}';",
        )
        require(
            reviewed_correction_canonical_signature()
            == reviewed_canonical_before,
            "reviewed correction fixture did not recover after stored flag "
            "rejection",
        )

    mutated_summary_event = {
        **reviewed_event,
        "summary": "third-party summary mutation",
    }
    mutated_title_document = {
        **reviewed_document,
        "title": "third-party document title mutation",
    }
    mutated_hash_document = {
        **reviewed_document,
        "content_hash": "f" * 64,
    }
    explicit_ordinary_document = {
        **reviewed_document,
        "is_correction": False,
    }
    numeric_correction_document = {
        **reviewed_document,
        "is_correction": 0,
    }
    mutated_actor_id = "actor:ci-reviewed-correction-mutated"
    mutated_actor_event = {
        **reviewed_event,
        "actor_id": mutated_actor_id,
        "identity_actor_id": mutated_actor_id,
        "actor": {
            **reviewed_event["actor"],
            "actor_id": mutated_actor_id,
        },
        "event_actor": {
            **reviewed_event["event_actor"],
            "actor_id": mutated_actor_id,
        },
    }
    mutated_deadline_event = {
        **reviewed_event,
        "deadline_at": "2026-09-01T00:00:00Z",
        "identity_deadline_at": "2026-09-01T00:00:00Z",
    }
    rejected_correction_event_id = "event:ci-reviewed-correction-mutated-id"
    mutated_event_id_event = {
        **reviewed_event,
        "event_id": rejected_correction_event_id,
        "event_actor": {
            **reviewed_event["event_actor"],
            "event_id": rejected_correction_event_id,
        },
    }
    reviewed_cancellation_event = {
        **reviewed_event,
        "is_cancelled": True,
    }
    mutated_id_document = {
        **reviewed_document,
        "document_id": rejected_correction_document_id,
        "external_id": rejected_correction_document_id.split(":", 1)[1],
        "original_url": (
            "https://example.com/dart/"
            + rejected_correction_document_id.split(":", 1)[1]
        ),
    }
    mutated_id_document["content_hash"] = hashlib.sha256(
        (
            f"{mutated_id_document['title']}\n"
            f"{mutated_id_document['original_url']}\n"
            f"{mutated_id_document['external_id']}"
        ).encode("utf-8")
    ).hexdigest()
    mutated_id_event = {
        **reviewed_event,
        "document_ids": [rejected_correction_document_id],
    }
    reviewed_rejections = (
        (
            "source_summary",
            reviewed_document,
            mutated_summary_event,
        ),
        (
            "document_title",
            mutated_title_document,
            reviewed_event,
        ),
        (
            "document_hash",
            mutated_hash_document,
            reviewed_event,
        ),
        (
            "document_correction_flag",
            explicit_ordinary_document,
            reviewed_event,
        ),
        (
            "document_correction_flag_numeric",
            numeric_correction_document,
            reviewed_event,
        ),
        (
            "document_id",
            mutated_id_document,
            mutated_id_event,
        ),
        (
            "event_id",
            reviewed_document,
            mutated_event_id_event,
        ),
        (
            "actor",
            reviewed_document,
            mutated_actor_event,
        ),
        (
            "deadline",
            reviewed_document,
            mutated_deadline_event,
        ),
        (
            "cancellation",
            reviewed_document,
            reviewed_cancellation_event,
        ),
    )
    for label, rejected_document, rejected_event in reviewed_rejections:
        rejected_reviewed_replay = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            followup_payload(rejected_document, rejected_event),
            expected_status=409,
        )
        require(
            error_code(rejected_reviewed_replay)
            == "followup_event_identity_conflict",
            f"{label} mutation entered the reviewed correction ACK path: "
            f"{rejected_reviewed_replay!r}",
        )
        require(
            reviewed_correction_canonical_signature()
            == reviewed_canonical_before
            and followup_row_signature(
                reviewed_correction_event_id,
                reviewed_correction_document_id,
            )
            == reviewed_rows_before
            and reviewed_company_master_signature() == reviewed_master_before
            and reviewed_company_master_rows()
            == reviewed_master_rows_before
            and mysql_execute(
                mysql_container_id,
                "SELECT COUNT(*) FROM ci_global_lifecycle_observations "
                f"WHERE resolved_event_id='{reviewed_correction_event_id}'",
            )
            == reviewed_lifecycle_before,
            f"{label} mutation changed canonical reviewed state or row counts",
        )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT "
            "(SELECT COUNT(*) FROM ci_documents "
            f"WHERE document_id='{rejected_correction_document_id}'),"
            "(SELECT COUNT(*) FROM ci_governance_events "
            f"WHERE event_id='{rejected_correction_event_id}')",
        )
        == "0\t0",
        "rejected reviewed correction event/document ID was persisted",
    )

    event_ids = (
        f"'{date_key}','{midnight_key}','{incomplete_correction_event_id}',"
        f"'{incomplete_cancellation_event_id}','{reviewed_ordinary_event_id}',"
        f"'{rejected_ordinary_event_id}','{rejected_ordinary_owner_event_id}',"
        f"'{reviewed_correction_event_id}',"
        f"'{rejected_correction_event_id}'"
    )
    document_ids = (
        f"'{original_document_id}','{midnight_document_id}','{kind_document_id}',"
        f"'{incomplete_correction_document_id}','{incomplete_cancellation_document_id}',"
        f"'{reviewed_ordinary_document_id}','{rejected_ordinary_document_id}',"
        f"'{reviewed_correction_document_id}','{rejected_correction_document_id}'"
    )
    mysql_execute(
        mysql_container_id,
        "DELETE FROM ci_global_lifecycle_observations "
        f"WHERE resolved_event_id IN ({event_ids});"
        "DELETE FROM ci_timeline_entries "
        f"WHERE event_id IN ({event_ids});"
        "DELETE FROM ci_editorial_revisions "
        f"WHERE entity_type='event' AND entity_id IN ({event_ids});"
        "DELETE FROM ci_event_observations "
        f"WHERE event_id IN ({event_ids});"
        "DELETE FROM ci_event_documents "
        f"WHERE event_id IN ({event_ids});"
        "DELETE FROM ci_event_actors "
        f"WHERE event_id IN ({event_ids});"
        "DELETE FROM ci_documents "
        f"WHERE document_id IN ({document_ids});"
        "DELETE FROM ci_governance_events "
        f"WHERE event_id IN ({event_ids});"
        "DELETE FROM ci_actors WHERE actor_id IN "
        f"('{actor_id}','{reviewed_ordinary_actor_id}','{reviewed_actor_id}');"
        f"DELETE FROM ci_companies WHERE company_id='{company_id}';",
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT "
            f"(SELECT COUNT(*) FROM ci_governance_events WHERE event_id IN ({event_ids})),"
            f"(SELECT COUNT(*) FROM ci_documents WHERE document_id IN ({document_ids})),"
            f"(SELECT COUNT(*) FROM ci_companies WHERE company_id='{company_id}')",
        )
        == "0\t0\t0",
        "identity precision fixture must not leak into later corpus checks",
    )


def exercise_dart_review_corpus(base_url: str, mysql_container_id: str) -> None:
    company_id = "00999992"
    server_uuid, database_name = mysql_execute(
        mysql_container_id, "SELECT LOWER(@@server_uuid),DATABASE()"
    ).split("\t")
    expected_backend_binding = hashlib.sha256(
        f"mysql8\n{server_uuid}\n{database_name}\nci_".encode()
    ).hexdigest()
    to_day = datetime.now(KST).date()
    from_day = to_day - timedelta(days=2)
    receipt_date = from_day.strftime("%Y%m%d")
    external_ids = tuple(
        f"{receipt_date}{999101 + index:06d}"
        for index in range(5)
    )
    document_ids = tuple(
        ("dart:" if index < 4 else "kind:") + external_id
        for index, external_id in enumerate(external_ids)
    )
    event_ids = (
        "event:dart-corpus-original",
        "event:dart-corpus-correction",
        "event:dart-corpus-withdrawal",
        "event:dart-corpus-unlinked-correction",
        "event:dart-corpus-kind-control",
    )
    from_text = from_day.isoformat()
    to_text = to_day.isoformat()
    published = [
        datetime.combine(from_day, datetime.min.time(), KST)
        .replace(hour=10 + index)
        .astimezone(timezone.utc)
        .strftime("%Y-%m-%d %H:%M:%S")
        for index in range(5)
    ]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    sql = (
        "DELETE FROM ci_event_documents WHERE event_id IN "
        f"('{event_ids[0]}','{event_ids[1]}','{event_ids[2]}','{event_ids[3]}',"
        f"'{event_ids[4]}');"
        "DELETE FROM ci_documents WHERE document_id IN "
        f"('{document_ids[0]}','{document_ids[1]}','{document_ids[2]}',"
        f"'{document_ids[3]}','{document_ids[4]}');"
        "DELETE FROM ci_governance_events WHERE event_id IN "
        f"('{event_ids[0]}','{event_ids[1]}','{event_ids[2]}','{event_ids[3]}',"
        f"'{event_ids[4]}');"
        f"DELETE FROM ci_companies WHERE company_id='{company_id}';"
        "INSERT INTO ci_companies "
        "(company_id,stock_code,market,legal_name,record_status,listing_status,created_at,updated_at) VALUES "
        f"('{company_id}','999992','KOSDAQ','CI DART Corpus Corp','active','listed','{now}','{now}');"
    )
    for index, event_id in enumerate(event_ids):
        source = "kind" if index == 4 else "dart"
        event_payload = json.dumps(
            {
                "is_correction": index in {1, 3},
                "is_cancelled": index == 2,
            },
            separators=(",", ":"),
        )
        sql += (
            "INSERT INTO ci_governance_events "
            "(event_id,company_id,event_type,title,original_language,occurred_at,importance,"
            "verification_status,review_status,publication_status,collection_key,identity_status,"
            "payload_json,created_at,updated_at) VALUES "
            f"('{event_id}','{company_id}','shareholder_proposal','CI {source} corpus event',"
            f"'ko','{published[index]}','normal','official','pending','draft','ci-corpus','complete',"
            f"'{event_payload}','{now}','{now}');"
        )
    titles = (
        "CI original filing",
        "CI linked corrected filing",
        "CI withdrawn filing",
        "CI unlinked corrected filing",
        "CI KIND control filing",
    )
    for index, document_id in enumerate(document_ids):
        source_right = "official:kind" if index == 4 else "official:dart"
        source_url = "kind" if index == 4 else "dart"
        correction = (
            f"'{document_ids[0]}'" if index == 1 else "NULL"
        )
        version = 2 if index == 1 else 1
        document_payload = json.dumps(
            {"has_later_correction": index in {0, 1}},
            separators=(",", ":"),
        )
        content_hash = hashlib.sha256(
            f"{titles[index]}:{external_ids[index]}".encode("utf-8")
        ).hexdigest()
        sql += (
            "INSERT INTO ci_documents "
            "(document_id,company_id,source_right_id,source_class,external_id,document_type,"
            "original_language,title,original_url,content_hash,collection_key,"
            "correction_of_document_id,version_no,published_at,retrieved_at,verification_status,"
            "publication_status,payload_json,created_at,updated_at) VALUES "
            f"('{document_id}','{company_id}','{source_right}','official_disclosure',"
            f"'{external_ids[index]}','shareholder_proposal','ko','{titles[index]}',"
            f"'https://example.com/{source_url}/{external_ids[index]}','{content_hash}',"
            f"'ci-corpus',{correction},{version},'{published[index]}','{now}',"
            f"'official','published','{document_payload}','{now}','{now}');"
        )
        sql += (
            "INSERT INTO ci_event_documents "
            "(event_id,document_id,relation_type,position_no,created_at) VALUES "
            f"('{event_ids[0] if index == 1 else event_ids[index]}',"
            f"'{document_id}','evidence',0,'{now}');"
        )
    mysql_execute(mysql_container_id, sql)

    path = (
        "api.php/api/v1/ops/dart-review-corpus?"
        + urllib.parse.urlencode({"from": from_text, "to": to_text, "limit": 100})
    )
    unauthorized, _ = request_json(base_url, path, expected_status=401)
    require(unauthorized.get("error") == "bearer_token_required", repr(unauthorized))
    full, headers = request_json(base_url, path, token=ADMIN_TOKEN)
    expected_top_keys = {
        "ok",
        "contract_version",
        "range",
        "population_count",
        "corpus_sha256",
        "backend_binding_id",
        "items",
        "next_cursor",
        "api_version",
    }
    expected_item_keys = {
        "document_id",
        "event_id",
        "company_id",
        "company_name",
        "event_type",
        "revision_status",
        "external_id",
        "title",
        "original_language",
        "original_url",
        "published_at",
        "source_right_id",
        "correction_of_document_id",
        "version_no",
        "has_later_correction",
        "has_successor",
        "is_correction",
        "is_cancelled",
        "event_verification_status",
        "document_verification_status",
        "document_publication_status",
        "identity_status",
        "review_status",
        "importance",
    }
    items = full.get("items")
    require(set(full) == expected_top_keys, repr(full))
    require(
        full.get("ok") is True
        and full.get("contract_version") == "dart-review-corpus-v1"
        and full.get("api_version") == "v1"
        and full.get("range") == {"from": from_text, "to": to_text}
        and full.get("population_count") == 4
        and full.get("backend_binding_id") == expected_backend_binding
        and isinstance(items, list)
        and len(items) == 4
        and full.get("next_cursor") is None,
        repr(full),
    )
    require(
        int(headers.get("X-Response-Bytes", "250001")) <= 250000,
        repr(headers),
    )
    require(
        "no-store" in headers.get("Cache-Control", "").lower(),
        repr(headers),
    )
    require(
        all(
            set(item) == expected_item_keys
            and item.get("source_right_id") == "official:dart"
            and str(item.get("document_id", "")).startswith("dart:")
            for item in items
        ),
        repr(items),
    )
    ordered = sorted(
        items,
        key=lambda item: (
            str(item["published_at"]),
            str(item["document_id"]),
            str(item["event_id"]),
        ),
    )
    require(items == ordered, repr(items))
    statuses = {str(item["document_id"]): item["revision_status"] for item in items}
    require(
        statuses
        == {
            document_ids[0]: "original_superseded",
            document_ids[1]: "correction_linked",
            document_ids[2]: "withdrawal_unlinked",
            document_ids[3]: "correction_unlinked",
        },
        repr(statuses),
    )
    require(
        items[0]["has_later_correction"] is True
        and items[0]["has_successor"] is True
        and items[1]["has_later_correction"] is True
        and items[1]["has_successor"] is False
        and items[1]["is_correction"] is True
        and items[2]["is_cancelled"] is True
        and items[3]["is_correction"] is True
        and items[3]["correction_of_document_id"] is None,
        repr(items),
    )
    digest = hashlib.sha256()
    for item in items:
        digest.update(
            json.dumps(
                item,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            + b"\n"
        )
    require(full.get("corpus_sha256") == digest.hexdigest(), repr(full))

    first_path = (
        "api.php/api/v1/ops/dart-review-corpus?"
        + urllib.parse.urlencode({"from": from_text, "to": to_text, "limit": 2})
    )
    first, _ = request_json(base_url, first_path, token=ADMIN_TOKEN)
    require(
        len(first.get("items", [])) == 2
        and isinstance(first.get("next_cursor"), str)
        and first.get("corpus_sha256") == full.get("corpus_sha256")
        and first.get("backend_binding_id") == expected_backend_binding,
        repr(first),
    )
    second_path = (
        "api.php/api/v1/ops/dart-review-corpus?"
        + urllib.parse.urlencode(
            {
                "from": from_text,
                "to": to_text,
                "limit": 2,
                "cursor": first["next_cursor"],
            }
        )
    )
    second, _ = request_json(base_url, second_path, token=ADMIN_TOKEN)
    require(
        len(second.get("items", [])) == 2
        and second.get("next_cursor") is None
        and first["items"] + second["items"] == items
        and second.get("corpus_sha256") == full.get("corpus_sha256")
        and second.get("backend_binding_id") == expected_backend_binding,
        repr(second),
    )
    wrong_range_path = (
        "api.php/api/v1/ops/dart-review-corpus?"
        + urllib.parse.urlencode(
            {
                "from": (from_day - timedelta(days=1)).isoformat(),
                "to": to_text,
                "limit": 2,
                "cursor": first["next_cursor"],
            }
        )
    )
    invalid_cursor, _ = request_json(
        base_url,
        wrong_range_path,
        token=ADMIN_TOKEN,
        expected_status=400,
    )
    require(invalid_cursor.get("error") == "invalid_cursor", repr(invalid_cursor))

    metadata_mutations = (
        (
            f"UPDATE ci_companies SET legal_name='   ' WHERE company_id='{company_id}'",
            "UPDATE ci_companies SET legal_name='CI DART Corpus Corp' "
            f"WHERE company_id='{company_id}'",
        ),
        (
            f"UPDATE ci_documents SET title='   ' WHERE document_id='{document_ids[3]}'",
            f"UPDATE ci_documents SET title='{titles[3]}' "
            f"WHERE document_id='{document_ids[3]}'",
        ),
        (
            f"UPDATE ci_documents SET original_language='KO' "
            f"WHERE document_id='{document_ids[3]}'",
            f"UPDATE ci_documents SET original_language='ko' "
            f"WHERE document_id='{document_ids[3]}'",
        ),
        (
            f"UPDATE ci_documents SET original_url='http://example.com/unsafe' "
            f"WHERE document_id='{document_ids[3]}'",
            f"UPDATE ci_documents "
            f"SET original_url='https://example.com/dart/{external_ids[3]}' "
            f"WHERE document_id='{document_ids[3]}'",
        ),
        (
            f"UPDATE ci_governance_events SET event_type='Bad Type' "
            f"WHERE event_id='{event_ids[3]}'",
            f"UPDATE ci_governance_events SET event_type='shareholder_proposal' "
            f"WHERE event_id='{event_ids[3]}'",
        ),
    )
    for invalid_sql, restore_sql in metadata_mutations:
        mysql_execute(mysql_container_id, invalid_sql)
        invalid_metadata, _ = request_json(
            base_url,
            path,
            token=ADMIN_TOKEN,
            expected_status=503,
        )
        require(
            invalid_metadata.get("error") == "dart_review_corpus_metadata_error",
            repr(invalid_metadata),
        )
        mysql_execute(mysql_container_id, restore_sql)

    mismatched_published = (
        datetime.combine(
            from_day + timedelta(days=1),
            datetime.min.time(),
            timezone.utc,
        )
        .strftime("%Y-%m-%d %H:%M:%S")
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_documents "
        f"SET published_at='{mismatched_published}' "
        f"WHERE document_id='{document_ids[3]}'",
    )
    later_publication_date, _ = request_json(
        base_url,
        path,
        token=ADMIN_TOKEN,
    )
    require(
        later_publication_date.get("ok") is True
        and later_publication_date.get("population_count") == 4
        and any(
            item.get("document_id") == document_ids[3]
            and item.get("published_at")
            == mismatched_published.replace(" ", "T") + "Z"
            for item in later_publication_date.get("items", [])
        ),
        repr(later_publication_date),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_documents "
        f"SET published_at='{published[3]}' "
        f"WHERE document_id='{document_ids[3]}'",
    )

    mysql_execute(
        mysql_container_id,
        "UPDATE ci_documents SET correction_of_document_id=document_id,version_no=2 "
        f"WHERE document_id='{document_ids[0]}'",
    )
    self_lineage, _ = request_json(
        base_url,
        path,
        token=ADMIN_TOKEN,
        expected_status=503,
    )
    require(
        self_lineage.get("error") == "dart_review_corpus_lineage_error",
        repr(self_lineage),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_documents SET correction_of_document_id=NULL,version_no=1 "
        f"WHERE document_id='{document_ids[0]}'",
    )

    mysql_execute(
        mysql_container_id,
        "UPDATE ci_documents "
        f"SET correction_of_document_id='dart:{receipt_date}000001' "
        f"WHERE document_id='{document_ids[1]}'",
    )
    dangling_lineage, _ = request_json(
        base_url,
        path,
        token=ADMIN_TOKEN,
        expected_status=503,
    )
    require(
        dangling_lineage.get("error") == "dart_review_corpus_lineage_error",
        repr(dangling_lineage),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_documents "
        f"SET correction_of_document_id='{document_ids[0]}' "
        f"WHERE document_id='{document_ids[1]}'",
    )

    mysql_execute(
        mysql_container_id,
        "UPDATE ci_documents SET collection_key='ci-other-chain' "
        f"WHERE document_id='{document_ids[0]}'",
    )
    cross_collection_lineage, _ = request_json(
        base_url,
        path,
        token=ADMIN_TOKEN,
        expected_status=503,
    )
    require(
        cross_collection_lineage.get("error")
        == "dart_review_corpus_lineage_error",
        repr(cross_collection_lineage),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_documents SET collection_key='ci-corpus' "
        f"WHERE document_id='{document_ids[0]}'",
    )

    mysql_execute(
        mysql_container_id,
        "UPDATE ci_documents "
        f"SET correction_of_document_id='{document_ids[0]}',version_no=2 "
        f"WHERE document_id='{document_ids[3]}';"
        "UPDATE ci_event_documents "
        f"SET event_id='{event_ids[0]}' WHERE document_id='{document_ids[3]}'",
    )
    branching_lineage, _ = request_json(
        base_url,
        path,
        token=ADMIN_TOKEN,
        expected_status=503,
    )
    require(
        branching_lineage.get("error") == "dart_review_corpus_lineage_error",
        repr(branching_lineage),
    )
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_documents SET correction_of_document_id=NULL,version_no=1 "
        f"WHERE document_id='{document_ids[3]}';"
        "UPDATE ci_event_documents "
        f"SET event_id='{event_ids[3]}' WHERE document_id='{document_ids[3]}'",
    )

    mysql_execute(
        mysql_container_id,
        "INSERT INTO ci_event_documents "
        "(event_id,document_id,relation_type,position_no,created_at) VALUES "
        f"('{event_ids[1]}','{document_ids[0]}','secondary',1,'{now}')",
    )
    ambiguous, _ = request_json(
        base_url,
        path,
        token=ADMIN_TOKEN,
        expected_status=503,
    )
    require(
        ambiguous.get("error") == "dart_review_corpus_integrity_error",
        repr(ambiguous),
    )
    mysql_execute(
        mysql_container_id,
        "DELETE FROM ci_event_documents "
        f"WHERE event_id='{event_ids[1]}' AND document_id='{document_ids[0]}' "
        "AND relation_type='secondary'",
    )


def run(base_url: str, mysql_container_id: str) -> None:
    global EXPECTED_BACKEND_BINDING_ID
    EXPECTED_BACKEND_BINDING_ID = mysql_backend_binding_id(mysql_container_id)
    activate_exact_dart_source_right(base_url, mysql_container_id)

    root, _ = request_json(base_url, "api.php/api/v1/health")
    require(root.get("ok") is True, repr(root))

    descriptor, _ = request_json(base_url, "api.php/api/v1", expected_status=503)
    require(descriptor.get("error") == "governance_release_closed", repr(descriptor))

    closed, _ = request_json(
        base_url,
        "api.php/api/v1/events",
        expected_status=503,
    )
    require(closed.get("error") == "governance_release_closed", repr(closed))

    unauthorized, _ = request_json(
        base_url,
        "api.php/api/v1/admin/release-state",
        expected_status=401,
    )
    require(unauthorized.get("error") == "bearer_token_required", repr(unauthorized))

    initial, _ = request_json(
        base_url,
        "api.php/api/v1/admin/release-state",
        token=ADMIN_TOKEN,
    )
    require(initial.get("release_state") == "closed", repr(initial))
    require(initial.get("state_version") == 0, repr(initial))
    require(initial.get("schema_version") == 10, repr(initial))
    require(initial.get("preview_auth_configured") is True, repr(initial))
    require("preview_token_hash" not in initial, repr(initial))
    require(initial.get("cutover_at") is None and initial.get("sunset_at") is None, repr(initial))

    wrong_binding = (
        "0" * 64
        if EXPECTED_BACKEND_BINDING_ID != "0" * 64
        else "1" * 64
    )
    rejected_binding = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [
                {
                    "company_id": "00999991",
                    "legal_name": "Must not reach the wrong backend",
                }
            ],
            "documents": [],
            "events": [],
            "source_rights": [],
            "run": {},
            "expected_backend_binding_id": wrong_binding,
        },
        expected_status=409,
    )
    require(
        rejected_binding.get("error") == "backend_binding_mismatch",
        repr(rejected_binding),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_companies WHERE company_id='00999991'",
        )
        == "0",
        "backend binding mismatch must fail before the first governance mutation",
    )

    out_of_band_right = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [],
            "documents": [],
            "events": [],
            "source_rights": [
                {
                    **DART_CONTRACT_FIXTURE["source_right"],
                    "valid_from": "2021-01-01T00:00:00Z",
                }
            ],
            "run": {},
        },
        expected_status=409,
    )
    require(
        out_of_band_right.get("error")
        == "dart_source_right_managed_out_of_band",
        repr(out_of_band_right),
    )

    missing_deployment_company = "00999988"
    missing_deployment = request_hmac_action(
        base_url,
        "upsert_governance_snapshot_dart_guarded",
        {
            "companies": [
                {
                    "company_id": missing_deployment_company,
                    "legal_name": "Must not cross missing deployment guard",
                }
            ],
            "documents": [],
            "events": [],
            "source_rights": [],
            "run": {"source_key": "dart"},
            "expected_source_right_revisions": {
                "official:dart": {
                    "rights_revision": DART_RIGHTS_REVISION,
                    "contract_revision": DART_CONTRACT_REVISION,
                }
            },
            "expected_release_state": "closed",
        },
        expected_status=409,
        inject_dart_preconditions=False,
    )
    require(
        missing_deployment.get("error")
        == "dart_deployment_revision_required",
        repr(missing_deployment),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_companies "
            f"WHERE company_id='{missing_deployment_company}'",
        )
        == "0",
        "missing deployment precondition must fail before mutation",
    )

    wrong_deployment_company = "00999987"
    wrong_deployment_sha = (
        "0" * 40 if DEPLOYED_CODE_REVISION != "0" * 40 else "1" * 40
    )
    wrong_deployment = request_hmac_action(
        base_url,
        "upsert_governance_snapshot_dart_guarded",
        {
            "companies": [
                {
                    "company_id": wrong_deployment_company,
                    "legal_name": "Must not cross deployment mismatch",
                }
            ],
            "documents": [],
            "events": [],
            "source_rights": [],
            "run": {"source_key": "dart"},
            "expected_source_right_revisions": {
                "official:dart": {
                    "rights_revision": DART_RIGHTS_REVISION,
                    "contract_revision": DART_CONTRACT_REVISION,
                }
            },
            "expected_deployment_code_revision": wrong_deployment_sha,
            "expected_release_state": "closed",
        },
        expected_status=409,
        inject_dart_preconditions=False,
    )
    require(
        wrong_deployment.get("error")
        == "dart_deployment_revision_mismatch",
        repr(wrong_deployment),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_companies "
            f"WHERE company_id='{wrong_deployment_company}'",
        )
        == "0",
        "deployment revision mismatch must fail before mutation",
    )

    missing_release_state_company = "00999985"
    missing_release_state = request_hmac_action(
        base_url,
        "upsert_governance_snapshot_dart_guarded",
        {
            "companies": [
                {
                    "company_id": missing_release_state_company,
                    "legal_name": "Must not cross missing release-state guard",
                }
            ],
            "documents": [],
            "events": [],
            "source_rights": [],
            "run": {"source_key": "dart"},
            "expected_source_right_revisions": {
                "official:dart": {
                    "rights_revision": DART_RIGHTS_REVISION,
                    "contract_revision": DART_CONTRACT_REVISION,
                }
            },
            "expected_deployment_code_revision": DEPLOYED_CODE_REVISION,
        },
        expected_status=409,
        inject_dart_preconditions=False,
    )
    require(
        missing_release_state.get("error")
        == "dart_release_state_precondition_required",
        repr(missing_release_state),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_companies "
            f"WHERE company_id='{missing_release_state_company}'",
        )
        == "0",
        "missing release-state precondition must fail before mutation",
    )

    generic_dart_company = "00999986"
    generic_dart = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [
                {
                    "company_id": generic_dart_company,
                    "legal_name": "Generic action must reject DART",
                }
            ],
            "documents": [],
            "events": [],
            "source_rights": [],
            "run": {"source_key": "dart"},
        },
        expected_status=409,
        use_guarded_dart_action=False,
        inject_dart_preconditions=False,
    )
    require(
        generic_dart.get("error") == "dart_guarded_action_required",
        repr(generic_dart),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_companies "
            f"WHERE company_id='{generic_dart_company}'",
        )
        == "0",
        "generic HMAC action must reject DART before mutation",
    )

    migration_012_checksum = mysql_execute(
        mysql_container_id,
        "SELECT migration_checksum FROM ci_schema_migrations "
        "WHERE migration_version=12",
    )
    require(len(migration_012_checksum) == 64, migration_012_checksum)
    mysql_execute(
        mysql_container_id,
        "UPDATE ci_schema_migrations SET migration_checksum=REPEAT('0',64) "
        "WHERE migration_version=12",
    )
    try:
        drift_rejected = request_hmac_action(
            base_url,
            "upsert_governance_snapshot",
            {
                "companies": [
                    {
                        "company_id": "00999990",
                        "legal_name": "Must not cross schema drift",
                    }
                ],
                "documents": [],
                "events": [],
                "source_rights": [],
                "run": {"source_key": "dart"},
            },
            expected_status=503,
        )
        require(
            drift_rejected.get("error") == "dart_global_bridge_unavailable",
            repr(drift_rejected),
        )
        require(
            mysql_execute(
                mysql_container_id,
                "SELECT COUNT(*) FROM ci_companies "
                "WHERE company_id='00999990'",
            )
            == "0",
            "schema drift must fail before the first DART mutation",
        )
    finally:
        mysql_execute(
            mysql_container_id,
            "UPDATE ci_schema_migrations "
            f"SET migration_checksum='{migration_012_checksum}' "
            "WHERE migration_version=12",
        )

    exercise_official_slot_claims(base_url, mysql_container_id)

    for action, field, payload in (
        (
            "enqueue_delivery_outbox",
            "accepted",
            {"deliveries": [{"delivery_id": "must-never-be-enqueued"}]},
        ),
        ("claim_delivery_outbox", "claimed", {"worker_id": "must-never-claim"}),
    ):
        disabled = request_hmac_action(base_url, action, payload, expected_status=410)
        require(disabled.get("error") == "outbound_delivery_disabled", repr(disabled))
        require(disabled.get("distribution_mode") == "web_only", repr(disabled))
        require(disabled.get(field) == 0, repr(disabled))
    health_after_disabled, _ = request_json(
        base_url,
        "api.php/api/v1/ops/health",
        token=ADMIN_TOKEN,
    )
    require(health_after_disabled.get("pending_outbox") == 0, repr(health_after_disabled))

    ineligible_kind, _ = request_json(
        base_url,
        "api.php/api/v1/ops/source-right-eligibility?source_right_id=official:kind&use=ingest",
        token=ADMIN_TOKEN,
        expected_status=409,
    )
    require(ineligible_kind.get("error") == "source_right_ineligible", repr(ineligible_kind))
    registered_kind, _ = request_json(
        base_url,
        "api.php/api/v1/admin/source-rights",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "source_right_id": "official:kind",
            "source_type": "official_disclosure",
            "source_key": "kind",
            "source_name": "KIND",
            "permission_scope": "CI approved historical, incremental, AI and public redistribution scope",
            "evidence_hash": "9" * 64,
            "valid_from": "2021-01-01T00:00:00Z",
            "valid_until": None,
            "ai_allowed": True,
            "redistribution_allowed": True,
            "status": "active",
        },
    )
    require(registered_kind.get("source_right_id") == "official:kind", repr(registered_kind))
    eligible_kind, _ = request_json(
        base_url,
        "api.php/api/v1/ops/source-right-eligibility?source_right_id=official:kind&use=ingest",
        token=ADMIN_TOKEN,
    )
    require(eligible_kind.get("eligible") is True, repr(eligible_kind))
    require(
        isinstance(eligible_kind.get("rights_revision"), str)
        and len(str(eligible_kind["rights_revision"])) == 64,
        repr(eligible_kind),
    )

    official_right_id = "right:company-site:00123456"
    registered_official_right, _ = request_json(
        base_url,
        "api.php/api/v1/admin/source-rights",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "source_right_id": official_right_id,
            "source_type": "company_statement",
            "source_key": "company-site:00123456",
            "source_name": "스모크 테스트 회사 공식 IR",
            "permission_scope": "CI fixture: public redistribution permitted; AI use separately recorded",
            "evidence_hash": "8" * 64,
            "valid_from": "2021-01-01T00:00:00Z",
            "valid_until": None,
            "ai_allowed": False,
            "redistribution_allowed": True,
            "status": "active",
        },
    )
    require(
        registered_official_right.get("source_right_id") == official_right_id,
        repr(registered_official_right),
    )
    official_rights, _ = request_json(
        base_url,
        "api.php/api/v1/ops/official-site-rights",
        token=ADMIN_TOKEN,
    )
    require(len(official_rights.get("data", [])) == 1, repr(official_rights))
    require(official_rights["data"][0].get("source_right_id") == official_right_id, repr(official_rights))
    require(official_rights["data"][0].get("ai_allowed") is False, repr(official_rights))

    first_site_payload, first_document_id, first_content_hash = official_site_payload(
        title="  원문 제목 보존  ",
        body_text="첫 줄\n둘째 줄 – 원문 그대로",
        collected_at="2026-07-22T01:00:00Z",
        include_review=True,
    )
    first_site = request_hmac_action(
        base_url,
        "upsert_official_site_snapshot",
        first_site_payload,
        expected_status=200,
    )
    require(first_site.get("idempotent") is False, repr(first_site))
    require(first_site.get("snapshot_id") == first_site_payload["snapshot_id"], repr(first_site))
    require(first_site.get("accepted") == first_site_payload["expected"], repr(first_site))
    first_site_replay = request_hmac_action(
        base_url,
        "upsert_official_site_snapshot",
        first_site_payload,
        expected_status=200,
    )
    require(first_site_replay.get("idempotent") is True, repr(first_site_replay))

    repeated_payload, repeated_document_id, _ = official_site_payload(
        title="  원문 제목 보존  ",
        body_text="첫 줄\n둘째 줄 – 원문 그대로",
        collected_at="2026-07-22T01:30:00Z",
        include_review=True,
    )
    require(repeated_document_id == first_document_id, repr(repeated_payload))
    repeated_site = request_hmac_action(
        base_url,
        "upsert_official_site_snapshot",
        repeated_payload,
        expected_status=200,
    )
    require(repeated_site.get("idempotent") is False, repr(repeated_site))

    changed_payload, changed_document_id, changed_content_hash = official_site_payload(
        title="  정정된 원문 제목  ",
        body_text="정정 첫 줄\n정정 둘째 줄 – 원문 그대로",
        collected_at="2026-07-22T02:00:00Z",
        include_review=False,
    )
    require(changed_document_id != first_document_id, repr(changed_payload))
    changed_site = request_hmac_action(
        base_url,
        "upsert_official_site_snapshot",
        changed_payload,
        expected_status=200,
    )
    require(changed_site.get("idempotent") is False, repr(changed_site))

    runtime_documents, _ = request_json(
        base_url,
        "api.php/api/v1/ops/runtime-state?resource=documents&limit=100",
        token=ADMIN_TOKEN,
    )
    document_rows = {
        row["document_id"]: row
        for row in runtime_documents.get("data", {}).get("records", [])
        if row.get("document_id") in {first_document_id, changed_document_id}
    }
    require(set(document_rows) == {first_document_id, changed_document_id}, repr(runtime_documents))
    first_document = document_rows[first_document_id]
    changed_document = document_rows[changed_document_id]
    require(first_document.get("title") == "  원문 제목 보존  ", repr(first_document))
    require(first_document.get("original_language") == "ko", repr(first_document))
    require(first_document.get("content_hash") == first_content_hash, repr(first_document))
    require(
        first_document.get("body_sha256")
        == hashlib.sha256("첫 줄\n둘째 줄 – 원문 그대로".encode("utf-8")).hexdigest(),
        repr(first_document),
    )
    require(first_document.get("version_no") == 1, repr(first_document))
    require(changed_document.get("title") == "  정정된 원문 제목  ", repr(changed_document))
    require(changed_document.get("content_hash") == changed_content_hash, repr(changed_document))
    require(changed_document.get("version_no") == 2, repr(changed_document))
    require(changed_document.get("correction_of_document_id") == first_document_id, repr(changed_document))
    require(
        first_document.get("publication_status") == "draft"
        and changed_document.get("publication_status") == "draft",
        repr(document_rows),
    )
    source_identity_rows = mysql_execute(
        mysql_container_id,
        "SELECT CONCAT(COUNT(*),'|',SUM(BINARY source_key=BINARY "
        "'company-site:00123456')) FROM ci_documents WHERE document_id IN ("
        f"'{first_document_id}','{changed_document_id}');",
    )
    require(
        source_identity_rows == "2|2",
        "official-site documents must persist their exact SourceRight source_key",
    )

    legacy_before, legacy_before_headers = request_json(base_url, "api.php?action=reports")
    require(legacy_before.get("ok") is True, repr(legacy_before))
    require("Deprecation" not in legacy_before_headers and "Sunset" not in legacy_before_headers, repr(legacy_before_headers))

    fingerprint = "a" * 64
    missing_checkpoint, _ = request_json(
        base_url,
        f"api.php/api/v1/ops/backfill-checkpoints/{fingerprint}",
        token=ADMIN_TOKEN,
        expected_status=404,
    )
    require(missing_checkpoint.get("error") == "backfill_checkpoint_not_found", repr(missing_checkpoint))
    checkpoint = {
        "schema_version": 1,
        "job": {"fingerprint": fingerprint, "source": "dart"},
        "completed_windows": {},
        "failed_windows": {},
    }
    canonical_checkpoint = json.dumps(
        checkpoint, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    expected_checkpoint_hash = hashlib.sha256(canonical_checkpoint).hexdigest()
    created_checkpoint, _ = request_json(
        base_url,
        f"api.php/api/v1/ops/backfill-checkpoints/{fingerprint}",
        method="PUT",
        token=ADMIN_TOKEN,
        payload={"expected_version": 0, "checkpoint": checkpoint},
        expected_status=201,
    )
    require(created_checkpoint.get("checkpoint_version") == 1, repr(created_checkpoint))
    require(created_checkpoint.get("payload_hash") == expected_checkpoint_hash, repr(created_checkpoint))
    loaded_checkpoint, _ = request_json(
        base_url,
        f"api.php/api/v1/ops/backfill-checkpoints/{fingerprint}",
        token=ADMIN_TOKEN,
    )
    require(loaded_checkpoint.get("payload_hash") == expected_checkpoint_hash, repr(loaded_checkpoint))
    require(loaded_checkpoint.get("checkpoint") == checkpoint, repr(loaded_checkpoint))
    require(isinstance(loaded_checkpoint["checkpoint"]["completed_windows"], dict), repr(loaded_checkpoint))
    require(isinstance(loaded_checkpoint["checkpoint"]["failed_windows"], dict), repr(loaded_checkpoint))
    replayed_checkpoint, _ = request_json(
        base_url,
        f"api.php/api/v1/ops/backfill-checkpoints/{fingerprint}",
        method="PUT",
        token=ADMIN_TOKEN,
        payload={"expected_version": 1, "checkpoint": checkpoint},
    )
    require(
        replayed_checkpoint.get("unchanged") is True
        and replayed_checkpoint.get("checkpoint_version") == 1,
        repr(replayed_checkpoint),
    )
    stale_checkpoint, _ = request_json(
        base_url,
        f"api.php/api/v1/ops/backfill-checkpoints/{fingerprint}",
        method="PUT",
        token=ADMIN_TOKEN,
        payload={"expected_version": 0, "checkpoint": checkpoint},
        expected_status=409,
    )
    require(stale_checkpoint.get("error") == "backfill_checkpoint_version_conflict", repr(stale_checkpoint))

    # Use the immediately preceding complete KST date so both observations stay
    # safely inside the API's 48-hour acceptance window.  The next civil day's
    # 00:00:59 observation is attributed to this date's final 23:56 cadence slot.
    kst_date = ((datetime.now(timezone.utc) + timedelta(hours=9)).date() - timedelta(days=1)).isoformat()
    observed_at = f"{kst_date}T12:00:00+09:00"
    next_kst_date = (datetime.fromisoformat(kst_date) + timedelta(days=1)).date().isoformat()
    last_slot_observed_at = f"{next_kst_date}T00:00:59+09:00"
    revision = "c" * 40
    # Release evidence intentionally omits operation days that have no durable
    # collection run. Seed this historical fixture through the production HMAC
    # write path instead of relying on the unrelated current-slot claim above.
    evidence_run = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [],
            "documents": [],
            "events": [],
            "source_rights": [],
            "run": {
                "run_id": stable_id(
                    "run", "release-evidence-smoke", kst_date, length=32
                ),
                "pipeline": "ingest-official",
                "source_key": "dart",
                "code_revision": revision,
                "status": "succeeded",
                "started_at": observed_at,
                "finished_at": observed_at,
                "first_observed_at": observed_at,
                "raw_count": 0,
                "acknowledged_count": 0,
                "fetched_count": 0,
                "resolved_count": 0,
                "accepted_count": 0,
                "error_count": 0,
                "run_kind": "manual",
                "company_master_sync": False,
                "source_ack_counts": {"dart": 0},
                "source_outcomes": {
                    "dart": {
                        "status": "succeeded",
                        "raw_count": 0,
                        "acknowledged_count": 0,
                    }
                },
            },
        },
        expected_status=200,
    )
    require(
        evidence_run.get("upserted", {}).get("runs") == 1,
        repr(evidence_run),
    )
    # Promote the two company-statement documents into one approved public
    # event, then corrupt one stored title and revoke their otherwise complete
    # SourceRight. Both documents must remain in the quality denominator even
    # though the right failure removes the event from current public visibility.
    content_event_id = str(first_site_payload["events"][0]["event_id"])
    mysql_execute(
        mysql_container_id,
        "INSERT INTO ci_actors "
        "(actor_id,actor_type,display_name,display_name_en,company_id,country_code,aliases_json,homepage_url,"
        "review_status,record_status,created_at,updated_at) VALUES "
        "('actor:test','activist','CI Test Actor',NULL,NULL,'KR','[]',NULL,'approved','active',"
        "'2021-01-02 00:00:00','2021-01-02 00:00:00') "
        "ON DUPLICATE KEY UPDATE review_status='approved',record_status='active';"
        f"INSERT INTO ci_event_actors "
        "(event_id,actor_id,actor_role,review_status,created_at,updated_at) VALUES "
        f"('{content_event_id}','actor:test','proposer','approved','2021-01-02 00:00:00','2021-01-02 00:00:00') "
        "ON DUPLICATE KEY UPDATE review_status='approved',updated_at=VALUES(updated_at);"
        f"UPDATE ci_governance_events SET review_status='approved',publication_status='published',"
        f"verification_status='verified',created_at='2021-01-02 00:00:00' WHERE event_id='{content_event_id}';"
        f"UPDATE ci_documents SET publication_status='published',created_at='2021-01-02 00:00:00' "
        f"WHERE document_id IN ('{first_document_id}','{changed_document_id}');"
        f"UPDATE ci_event_documents SET created_at='2021-01-02 00:00:00' "
        f"WHERE event_id='{content_event_id}' AND document_id IN ('{first_document_id}','{changed_document_id}');"
        f"UPDATE ci_documents SET title=CONCAT(title,' [CI MUTATION]') WHERE document_id='{first_document_id}';"
        "UPDATE ci_source_rights SET status='revoked',ai_allowed=1,redistribution_allowed=1,"
        "revoked_at='2021-02-01 00:00:00' WHERE source_right_id='right:company-site:00123456';",
    )
    require(
        mysql_execute(
            mysql_container_id,
            f"SELECT COUNT(DISTINCT document_id) FROM ci_event_documents "
            f"WHERE event_id='{content_event_id}' "
            f"AND document_id IN ('{first_document_id}','{changed_document_id}')",
        )
        == "2",
        "expected two distinct non-official public-event document references",
    )
    availability, _ = request_json(
        base_url,
        "api.php/api/v1/ops/availability-observations",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "observations": [
                {
                    "observation_id": "availability:" + "b" * 48,
                    "route_template": "/api/v1/health",
                    "observed_at": observed_at,
                    "http_status": 200,
                    "duration_ms": 12,
                    "succeeded": True,
                    "build_sha": "c" * 40,
                    "source": "github_watchdog",
                    "error_class": None,
                },
                {
                    "observation_id": "availability:" + "e" * 48,
                    "route_template": "/api/v1/health",
                    "observed_at": last_slot_observed_at,
                    "http_status": 200,
                    "duration_ms": 13,
                    "succeeded": True,
                    "build_sha": "c" * 40,
                    "source": "github_watchdog",
                    "error_class": None,
                },
            ]
        },
        expected_status=202,
    )
    require(availability.get("accepted_count") == 2, repr(availability))

    distribution_payload = {
        "observations": [
            {
                "observation_id": "distribution:" + "d" * 48,
                "observed_at": observed_at,
                "distribution_target": "pages",
                "duration_ms": 321,
                "succeeded": True,
                "build_sha": revision,
                "workflow_run_id": 7001,
                "workflow_run_attempt": 1,
                "failure_detected_at": None,
                "source": "github_actions",
            }
        ]
    }
    distribution, _ = request_json(
        base_url,
        "api.php/api/v1/ops/web-distribution-observations",
        method="POST",
        token=ADMIN_TOKEN,
        payload=distribution_payload,
        expected_status=202,
    )
    require(distribution.get("accepted_count") == 1 and distribution.get("inserted_count") == 1, repr(distribution))
    distribution_replay, _ = request_json(
        base_url,
        "api.php/api/v1/ops/web-distribution-observations",
        method="POST",
        token=ADMIN_TOKEN,
        payload=distribution_payload,
        expected_status=202,
    )
    require(distribution_replay.get("duplicate_count") == 1 and distribution_replay.get("inserted_count") == 0, repr(distribution_replay))

    quality_counts = {
        "official_evidence_total_count": 1,
        "official_evidence_linked_count": 0,
        "top_sensitive_total_count": 0,
        "top_sensitive_reviewed_count": 0,
        "original_language_total_count": 2,
        "original_language_preserved_count": 1,
        "source_right_total_count": 2,
        "valid_source_right_count": 0,
    }
    false_pass, _ = request_json(
        base_url,
        "api.php/api/v1/ops/quality-observations",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "observations": [
                {
                    "observation_id": "quality:false-pass:" + "0" * 40,
                    "observation_date": kst_date,
                    "code_revision": revision,
                    "dart_success_poll_interval_p95_minutes": 15.0,
                    "kind_observation_lag_p95_minutes": None,
                    "raw_counts": {key: 0 for key in quality_counts},
                    "source": "production_quality_job",
                }
            ]
        },
        expected_status=409,
    )
    require(false_pass.get("error") == "quality_counts_not_actual", repr(false_pass))
    require(false_pass.get("actual_raw_counts") == quality_counts, repr(false_pass))
    quality, _ = request_json(
        base_url,
        "api.php/api/v1/ops/quality-observations",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "observations": [
                {
                    "observation_id": "quality:" + "e" * 48,
                    "observation_date": kst_date,
                    "code_revision": revision,
                    "dart_success_poll_interval_p95_minutes": 15.0,
                    "kind_observation_lag_p95_minutes": None,
                    "raw_counts": quality_counts,
                    "source": "production_quality_job",
                }
            ]
        },
        expected_status=202,
    )
    require(quality.get("accepted_count") == 1 and quality.get("inserted_count") == 1, repr(quality))

    comparison_key = "eventcmp:v1:" + "f" * 64
    shadow, _ = request_json(
        base_url,
        "api.php/api/v1/admin/shadow-runs",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "observation_date": kst_date,
            "code_revision": revision,
            "legacy_run": {"status": "succeeded", "events": [{"comparison_key": comparison_key}]},
            "candidate_run": {"status": "succeeded", "events": [{"comparison_key": comparison_key}]},
            "legacy_crosswalk": {
                "schema_version": 1,
                "eligible_legacy_record_count": 1,
                "crosswalked_legacy_record_count": 1,
                "unmatched_legacy_record_count": 0,
                "ambiguous_legacy_record_count": 0,
                "coverage_rate": 1.0,
                "crosswalk_sha256": hashlib.sha256(b"php73-ci-legacy-crosswalk").hexdigest(),
            },
        },
        expected_status=201,
    )
    require(shadow.get("unchanged") is False, repr(shadow))
    require(
        shadow.get("legacy_crosswalk", {}).get("eligible_legacy_record_count") == 1,
        repr(shadow),
    )
    shadow_list, _ = request_json(
        base_url,
        f"api.php/api/v1/admin/shadow-runs?from={kst_date}&to={kst_date}&code_revision={revision}",
        token=ADMIN_TOKEN,
    )
    require(len(shadow_list.get("data", [])) == 1, repr(shadow_list))
    require(
        shadow_list["data"][0].get("legacy_crosswalk", {}).get("eligible_legacy_record_count") == 1,
        repr(shadow_list),
    )

    provenance = {
        "schema_version": 1,
        "environment": "production",
        "evidence_source": "human_labeled_jsonl",
        "is_synthetic": False,
        "collected_at": observed_at,
        "code_revision": revision,
    }
    human_payload = {
        "code_revision": revision,
        "expected_version": 0,
        "benchmark": {"schema_version": 1, "evidence": provenance},
        "usability": {**provenance, "dataset_sha256": "1" * 64},
        "release_approval": {
            **provenance,
            "approved_revision": revision,
            "release_approved": False,
        },
    }
    human, _ = request_json(
        base_url,
        "api.php/api/v1/admin/release-evidence-inputs",
        method="POST",
        token=ADMIN_TOKEN,
        payload=human_payload,
        expected_status=201,
    )
    require(human.get("bundle_version") == 1, repr(human))

    candidates, _ = request_json(
        base_url,
        "api.php/api/v1/ops/official-site-candidates",
        token=ADMIN_TOKEN,
    )
    require(candidates.get("companies") == [] and candidates.get("actors") == [], repr(candidates))

    evidence, _ = request_json(
        base_url,
        f"api.php/api/v1/ops/release-evidence?from={kst_date}&to={kst_date}&code_revision={revision}",
        token=ADMIN_TOKEN,
    )
    require(evidence.get("distribution_mode") == "web_only", repr(evidence))
    require(len(evidence.get("web_distribution_days", [])) == 1, repr(evidence))
    require(len(evidence.get("quality_observations", [])) == 1, repr(evidence))
    require(
        evidence["quality_observations"][0].get("content_scope")
        == "governance_corpus_2021_plus_kst_day_end_v2",
        repr(evidence),
    )
    require(
        len(evidence.get("operations_days", [])) == 1
        and evidence["operations_days"][0].get("raw_counts") == quality_counts,
        repr(evidence),
    )
    require(len(evidence.get("shadow_days", [])) == 1, repr(evidence))
    require(evidence.get("human_release_evidence_status") == "available", repr(evidence))
    availability_evidence = evidence.get("availability", {})
    require(isinstance(availability_evidence, dict), repr(evidence))
    availability_groups = availability_evidence.get("daily_route_build_counts", [])
    require(len(availability_groups) == 1, repr(availability_evidence))
    availability_group = availability_groups[0]
    require(
        availability_group.get("cadence_id") == "watchdog-v1-kst-5m-minute01"
        and availability_group.get("expected_slot_count") == 288
        and availability_group.get("covered_slot_count") == 2
        and availability_group.get("missing_slot_count") == 286
        and len(availability_group.get("covered_slots_bitmap_hex", "")) == 72,
        repr(availability_group),
    )

    skipped = transition(base_url, "live", 0, "CI must not skip the preview state", expected_status=409)
    require(skipped.get("error") == "invalid_release_transition", repr(skipped))

    preview = transition(
        base_url,
        "preview",
        0,
        "CI enters a protected preview state",
        request_id="php73-release-preview",
    )
    require(preview.get("changed") is True and preview.get("state_version") == 1, repr(preview))

    preview_dart_write = request_hmac_action(
        base_url,
        "upsert_governance_snapshot",
        {
            "companies": [
                {
                    "company_id": "00999989",
                    "legal_name": "Must not write during preview",
                }
            ],
            "documents": [],
            "events": [],
            "source_rights": [],
            "run": {"source_key": "dart"},
        },
        expected_status=409,
        expected_release_state="preview",
    )
    require(
        preview_dart_write.get("error")
        == "dart_release_state_mismatch",
        repr(preview_dart_write),
    )
    require(
        mysql_execute(
            mysql_container_id,
            "SELECT COUNT(*) FROM ci_companies WHERE company_id='00999989'",
        )
        == "0",
        "v1/v2 preview race mismatch must fail before the first DART mutation",
    )

    missing_preview, _ = request_json(
        base_url,
        "api.php/api/v1/events",
        expected_status=401,
    )
    require(missing_preview.get("error") == "preview_token_required", repr(missing_preview))
    bad_preview, _ = request_json(
        base_url,
        "api.php/api/v1/events",
        token="invalid-preview-token-0000000000000000",
        expected_status=403,
    )
    require(bad_preview.get("error") == "invalid_preview_token", repr(bad_preview))
    preview_events, preview_headers = request_json(
        base_url,
        "api.php/api/v1/events",
        token=PREVIEW_TOKEN,
    )
    require(preview_events.get("ok") is True, repr(preview_events))
    require("private" in preview_headers.get("Cache-Control", "").lower(), repr(preview_headers))
    preview_today, _ = request_json(base_url, "api.php/api/v1/today", token=PREVIEW_TOKEN)
    require(preview_today.get("top") == [] and preview_today.get("watch") == [], repr(preview_today))
    filtered_calendar, _ = request_json(
        base_url,
        "api.php/api/v1/calendar?company_id=00126380&actor_id=actor:test&event_type=general_meeting&status=official&source_class=official_disclosure&evidence_document_id=doc:test",
        token=PREVIEW_TOKEN,
    )
    require(filtered_calendar.get("data") == [], repr(filtered_calendar))
    web_vital, _ = request_json(
        base_url,
        "api.php/api/v1/metrics/web-vitals",
        method="POST",
        token=PREVIEW_TOKEN,
        payload={
            "route_template": "/events/{event_id}",
            "metric": "LCP",
            "value": 1234.5,
            "device_class": "mobile",
            "build_sha": "c" * 40,
        },
        expected_status=202,
    )
    require(web_vital.get("accepted_count") == 1, repr(web_vital))

    replay = transition(base_url, "preview", 1, "CI idempotently repeats preview state")
    require(replay.get("changed") is False and replay.get("state_version") == 1, repr(replay))

    direct_live = transition(
        base_url,
        "live",
        1,
        "CI requires the protected atomic v1 and v2 cutover endpoint",
        expected_status=409,
    )
    require(
        direct_live.get("error") == "protected_atomic_cutover_required",
        repr(direct_live),
    )
    state_after_invalid_rights, _ = request_json(
        base_url,
        "api.php/api/v1/admin/release-state",
        token=ADMIN_TOKEN,
    )
    require(
        state_after_invalid_rights.get("release_state") == "preview"
        and state_after_invalid_rights.get("state_version") == 1,
        repr(state_after_invalid_rights),
    )
    restored_official_right, _ = request_json(
        base_url,
        "api.php/api/v1/admin/source-rights",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            "source_right_id": official_right_id,
            "source_type": "company_statement",
            "source_key": "company-site:00123456",
            "source_name": "CI company official IR",
            "permission_scope": "CI fixture: public redistribution and AI use permitted",
            "evidence_hash": "8" * 64,
            "valid_from": "2021-01-01T00:00:00Z",
            "valid_until": None,
            "ai_allowed": True,
            "redistribution_allowed": True,
            "status": "active",
        },
    )
    require(
        restored_official_right.get("source_right_id") == official_right_id
        and restored_official_right.get("status") == "active",
        repr(restored_official_right),
    )

    stale = transition(base_url, "closed", 0, "CI stale transition must be rejected", expected_status=409)
    require(stale.get("error") == "stale_release_state", repr(stale))

    rollback = transition(
        base_url,
        "closed",
        1,
        "CI closes preview without bypassing protected atomic cutover",
        request_id="php73-release-close",
    )
    require(
        rollback.get("changed") is True
        and rollback.get("state_version") == 2
        and rollback.get("cutover_at") is None
        and rollback.get("sunset_at") is None,
        repr(rollback),
    )
    closed_again, _ = request_json(
        base_url,
        "api.php/api/v1/events",
        token=PREVIEW_TOKEN,
        expected_status=503,
    )
    require(closed_again.get("error") == "governance_release_closed", repr(closed_again))

    final, _ = request_json(
        base_url,
        "api.php/api/v1/admin/release-state?history_limit=10",
        token=ADMIN_TOKEN,
    )
    versions = [entry.get("state_version") for entry in final.get("history", [])]
    require(versions[:3] == [2, 1, 0], repr(final))
    require(
        any(entry.get("request_id") == "php73-release-preview" for entry in final.get("history", [])),
        repr(final),
    )

    quota_day = (datetime.now(timezone.utc) + timedelta(hours=9)).date().isoformat()
    server_uuid, database_name = mysql_execute(
        mysql_container_id, "SELECT LOWER(@@server_uuid),DATABASE()"
    ).split("\t")
    expected_backend_binding = hashlib.sha256(
        f"mysql8\n{server_uuid}\n{database_name}\nci_".encode()
    ).hexdigest()
    quota_status, _ = request_json(
        base_url,
        f"api.php/api/v1/ops/dart-quota?quota_day={quota_day}",
        token=ADMIN_TOKEN,
    )
    require(
        quota_status.get("action") == "status"
        and quota_status.get("accepted") == 0
        and quota_status.get("limit_count") == 40000
        and quota_status.get("used_count") == 0
        and quota_status.get("remaining_count") == 40000
        and quota_status.get("credentials") == []
        and quota_status.get("backend_binding_id") == expected_backend_binding,
        repr(quota_status),
    )
    previous_quota_day = (
        datetime.now(KST).date() - timedelta(days=1)
    ).isoformat()
    previous_status, _ = request_json(
        base_url,
        f"api.php/api/v1/ops/dart-quota?quota_day={previous_quota_day}",
        token=ADMIN_TOKEN,
        expected_status=400,
    )
    require(
        previous_status.get("error", {}).get("code") == "quota_date_mismatch",
        repr(previous_status),
    )
    credential_id = "c" * 64
    attempt_id = "dart-list-smoke-attempt-0001"
    consume_payload = {
        "action": "consume",
        "attempt_id": attempt_id,
        "quota_day": quota_day,
        "credential_id": credential_id,
        "operation": "list",
        "code_revision": "c" * 40,
        "expected_backend_binding_id": expected_backend_binding,
    }
    wrong_binding_payload = dict(consume_payload)
    wrong_binding_payload["expected_backend_binding_id"] = "f" * 64
    rejected_quota_binding, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload=wrong_binding_payload,
        expected_status=409,
    )
    require(
        rejected_quota_binding.get("error", {}).get("code") == "backend_binding_mismatch",
        repr(rejected_quota_binding),
    )
    quota_after_rejection, _ = request_json(
        base_url,
        f"api.php/api/v1/ops/dart-quota?quota_day={quota_day}",
        token=ADMIN_TOKEN,
    )
    require(quota_after_rejection.get("used_count") == 0, repr(quota_after_rejection))
    consumed, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload=consume_payload,
    )
    require(
        consumed.get("action") == "consume"
        and consumed.get("attempt_id") == attempt_id
        and consumed.get("quota_day") == quota_day
        and consumed.get("accepted") == 1
        and consumed.get("used_count") == 1
        and consumed.get("remaining_count") == 39999
        and consumed.get("credential_id") == credential_id
        and consumed.get("credential_used_count") == 1
        and consumed.get("credential_remaining_count") == 39999
        and consumed.get("duplicate") is False
        and consumed.get("blocked_until") is None
        and consumed.get("backend_binding_id") == expected_backend_binding,
        repr(consumed),
    )
    consumed_replay, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload=consume_payload,
    )
    require(
        consumed_replay.get("duplicate") is True
        and consumed_replay.get("used_count") == 1
        and consumed_replay.get("backend_binding_id") == expected_backend_binding,
        repr(consumed_replay),
    )
    conflicting_consume = dict(consume_payload)
    conflicting_consume["operation"] = "corp_code"
    conflict, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload=conflicting_consume,
        expected_status=409,
    )
    require(
        conflict.get("error", {}).get("code") == "dart_quota_idempotency_conflict",
        repr(conflict),
    )
    block_payload = {
        "action": "block_020",
        "attempt_id": attempt_id,
        "quota_day": quota_day,
        "credential_id": credential_id,
        "reason": "opendart_status_020",
        "code_revision": "c" * 40,
        "expected_backend_binding_id": expected_backend_binding,
    }
    blocked, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload=block_payload,
    )
    require(
        blocked.get("action") == "block_020"
        and blocked.get("accepted") == 1
        and blocked.get("used_count") == 1
        and blocked.get("duplicate") is False
        and isinstance(blocked.get("blocked_until"), str)
        and blocked.get("backend_binding_id") == expected_backend_binding,
        repr(blocked),
    )
    blocked_replay, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload=block_payload,
    )
    require(
        blocked_replay.get("duplicate") is True
        and blocked_replay.get("backend_binding_id") == expected_backend_binding,
        repr(blocked_replay),
    )
    blocked_new, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload={
            **consume_payload,
            "attempt_id": "dart-list-smoke-attempt-0002",
        },
        expected_status=409,
    )
    require(
        blocked_new.get("error", {}).get("code") == "dart_credential_blocked",
        repr(blocked_new),
    )
    disable_after_block_payload = {
        **block_payload,
        "action": "disable_901",
        "reason": "opendart_status_901",
    }
    disabled_after_block, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload=disable_after_block_payload,
    )
    require(
        disabled_after_block.get("action") == "disable_901"
        and disabled_after_block.get("credential_status") == "disabled_901",
        repr(disabled_after_block),
    )
    terminal_block_replay, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload=block_payload,
    )
    require(
        terminal_block_replay.get("action") == "block_020"
        and terminal_block_replay.get("duplicate") is True
        and terminal_block_replay.get("credential_status") == "disabled_901",
        repr(terminal_block_replay),
    )
    second_credential = "d" * 64
    second_consume = {
        **consume_payload,
        "attempt_id": "dart-list-smoke-attempt-0003",
        "credential_id": second_credential,
    }
    second_consumed, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload=second_consume,
    )
    require(
        second_consumed.get("used_count") == 2
        and second_consumed.get("credential_used_count") == 1,
        repr(second_consumed),
    )
    disable_payload = {
        "action": "disable_901",
        "attempt_id": second_consume["attempt_id"],
        "quota_day": quota_day,
        "credential_id": second_credential,
        "reason": "opendart_status_901",
        "code_revision": "c" * 40,
        "expected_backend_binding_id": expected_backend_binding,
    }
    previous_consume = {
        **consume_payload,
        "attempt_id": "dart-prior-day-consume-must-fail",
        "quota_day": previous_quota_day,
    }
    rejected_previous_consume, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload=previous_consume,
        expected_status=400,
    )
    require(
        rejected_previous_consume.get("error", {}).get("code")
        == "quota_date_mismatch",
        repr(rejected_previous_consume),
    )
    missing_previous_followup = {
        "action": "block_020",
        "attempt_id": "dart-prior-day-missing-attempt",
        "quota_day": previous_quota_day,
        "credential_id": "a" * 64,
        "reason": "opendart_status_020",
        "code_revision": "a" * 40,
        "expected_backend_binding_id": expected_backend_binding,
    }
    rejected_previous_followup, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload=missing_previous_followup,
        expected_status=409,
    )
    require(
        rejected_previous_followup.get("error", {}).get("detail")
        == "consumed_attempt_required",
        repr(rejected_previous_followup),
    )
    disabled, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload=disable_payload,
    )
    require(
        disabled.get("action") == "disable_901"
        and disabled.get("credential_status") == "disabled_901",
        repr(disabled),
    )
    disabled_new, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload={**second_consume, "attempt_id": "dart-list-smoke-attempt-0004"},
        expected_status=409,
    )
    require(
        disabled_new.get("error", {}).get("code") == "dart_credential_disabled",
        repr(disabled_new),
    )
    previous_block_credential = "e" * 64
    previous_disable_credential = "f" * 64
    previous_block_attempt = "dart-prior-day-block-attempt"
    previous_disable_attempt = "dart-prior-day-disable-attempt"
    previous_block_revision = "e" * 40
    previous_disable_revision = "f" * 40
    prior_now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    mysql_execute(
        mysql_container_id,
        "INSERT INTO ci_dart_quota_days "
        "(quota_day,limit_count,used_count,blocked,block_reason,blocked_until,"
        "blocked_by_attempt_id,blocked_at,created_at,updated_at) VALUES "
        f"('{previous_quota_day}',40000,2,0,NULL,NULL,NULL,NULL,"
        f"'{prior_now}','{prior_now}');"
        "INSERT INTO ci_dart_quota_credentials "
        "(credential_id,status,disable_reason,disabled_by_attempt_id,disabled_at,"
        "created_at,updated_at) VALUES "
        f"('{previous_block_credential}','active',NULL,NULL,NULL,"
        f"'{prior_now}','{prior_now}'),"
        f"('{previous_disable_credential}','active',NULL,NULL,NULL,"
        f"'{prior_now}','{prior_now}');"
        "INSERT INTO ci_dart_quota_credential_days "
        "(quota_day,credential_id,limit_count,used_count,blocked,block_reason,"
        "blocked_until,blocked_by_attempt_id,blocked_at,created_at,updated_at) VALUES "
        f"('{previous_quota_day}','{previous_block_credential}',40000,1,0,NULL,"
        f"NULL,NULL,NULL,'{prior_now}','{prior_now}'),"
        f"('{previous_quota_day}','{previous_disable_credential}',40000,1,0,NULL,"
        f"NULL,NULL,NULL,'{prior_now}','{prior_now}');"
        "INSERT INTO ci_dart_quota_attempts "
        "(attempt_id,quota_day,credential_id,operation,code_revision,"
        "consume_request_sha256,block_request_sha256,disable_request_sha256,"
        "status,consumed_units,consumed_at,blocked_at,disabled_at,updated_at) VALUES "
        f"('{previous_block_attempt}','{previous_quota_day}',"
        f"'{previous_block_credential}','list','{previous_block_revision}',"
        f"'{hashlib.sha256(previous_block_attempt.encode()).hexdigest()}',"
        f"NULL,NULL,'consumed',1,'{prior_now}',NULL,NULL,'{prior_now}'),"
        f"('{previous_disable_attempt}','{previous_quota_day}',"
        f"'{previous_disable_credential}','list','{previous_disable_revision}',"
        f"'{hashlib.sha256(previous_disable_attempt.encode()).hexdigest()}',"
        f"NULL,NULL,'consumed',1,'{prior_now}',NULL,NULL,'{prior_now}');",
    )
    previous_block_payload = {
        "action": "block_020",
        "attempt_id": previous_block_attempt,
        "quota_day": previous_quota_day,
        "credential_id": previous_block_credential,
        "reason": "opendart_status_020",
        "code_revision": previous_block_revision,
        "expected_backend_binding_id": expected_backend_binding,
    }
    previous_blocked, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload=previous_block_payload,
    )
    expected_previous_block_until = (
        datetime.fromisoformat(previous_quota_day)
        .replace(tzinfo=KST)
        .replace(hour=0, minute=0, second=0, microsecond=0)
        + timedelta(days=1)
    ).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    require(
        previous_blocked.get("action") == "block_020"
        and previous_blocked.get("blocked_until") == expected_previous_block_until,
        repr(previous_blocked),
    )
    previous_disable_payload = {
        "action": "disable_901",
        "attempt_id": previous_disable_attempt,
        "quota_day": previous_quota_day,
        "credential_id": previous_disable_credential,
        "reason": "opendart_status_901",
        "code_revision": previous_disable_revision,
        "expected_backend_binding_id": expected_backend_binding,
    }
    previous_disabled, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload=previous_disable_payload,
    )
    require(
        previous_disabled.get("action") == "disable_901"
        and previous_disabled.get("credential_status") == "disabled_901",
        repr(previous_disabled),
    )
    exercise_event_identity_datetime_storage(base_url, mysql_container_id)
    exercise_dart_review_corpus(base_url, mysql_container_id)
    print("PHP 7.3 governance release-state smoke passed.", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8787")
    parser.add_argument("--mysql-container-id", required=True)
    args = parser.parse_args()
    run(args.base_url, args.mysql_container_id)


if __name__ == "__main__":
    main()
