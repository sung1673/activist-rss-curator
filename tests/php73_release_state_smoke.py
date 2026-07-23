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
from typing import Any


ADMIN_TOKEN = "php73-ci-admin-token-00000000000000000000"
PREVIEW_TOKEN = "php73-ci-preview-token-000000000000000000"
API_SECRET = b"php73-ci-only-hmac-key-00000000000000000000000000000000"
KST = timezone(timedelta(hours=9))


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
) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    timestamp = str(int(time.time()))
    nonce = f"release-smoke-{action}-{time.time_ns()}"
    signature = hmac.new(
        API_SECRET,
        timestamp.encode("ascii") + b"\n" + nonce.encode("ascii") + b"\n" + body,
        hashlib.sha256,
    ).hexdigest()
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/api.php?{urllib.parse.urlencode({'action': action})}",
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


def run(base_url: str, mysql_container_id: str) -> None:
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

    # Keep the synthetic evidence day clear of both the current claim boundary and
    # the epoch reset boundary, including runs that straddle KST midnight.
    kst_date = ((datetime.now(timezone.utc) + timedelta(hours=9)).date() - timedelta(days=2)).isoformat()
    observed_at = f"{kst_date}T12:00:00+09:00"
    next_kst_date = (datetime.fromisoformat(kst_date) + timedelta(days=1)).date().isoformat()
    last_slot_observed_at = f"{next_kst_date}T00:00:59+09:00"
    # Promote the two company-statement documents into one approved public
    # event, then corrupt one stored title and revoke their otherwise complete
    # SourceRight. Both documents must remain in the quality denominator even
    # though the right failure removes the event from current public visibility.
    content_event_id = str(first_site_payload["events"][0]["event_id"])
    mysql_execute(
        mysql_container_id,
        "INSERT INTO activist_actors "
        "(actor_id,actor_type,display_name,display_name_en,company_id,country_code,aliases_json,homepage_url,"
        "review_status,record_status,created_at,updated_at) VALUES "
        "('actor:test','activist','CI Test Actor',NULL,NULL,'KR','[]',NULL,'approved','active',"
        "'2021-01-02 00:00:00','2021-01-02 00:00:00') "
        "ON DUPLICATE KEY UPDATE review_status='approved',record_status='active';"
        f"INSERT INTO activist_event_actors "
        "(event_id,actor_id,actor_role,review_status,created_at,updated_at) VALUES "
        f"('{content_event_id}','actor:test','proposer','approved','2021-01-02 00:00:00','2021-01-02 00:00:00') "
        "ON DUPLICATE KEY UPDATE review_status='approved',updated_at=VALUES(updated_at);"
        f"UPDATE activist_governance_events SET review_status='approved',publication_status='published',"
        f"verification_status='verified',created_at='2021-01-02 00:00:00' WHERE event_id='{content_event_id}';"
        f"UPDATE activist_documents SET publication_status='published',created_at='2021-01-02 00:00:00' "
        f"WHERE document_id IN ('{first_document_id}','{changed_document_id}');"
        f"UPDATE activist_event_documents SET created_at='2021-01-02 00:00:00' "
        f"WHERE event_id='{content_event_id}' AND document_id IN ('{first_document_id}','{changed_document_id}');"
        f"UPDATE activist_documents SET title=CONCAT(title,' [CI MUTATION]') WHERE document_id='{first_document_id}';"
        "UPDATE activist_source_rights SET status='revoked',ai_allowed=1,redistribution_allowed=1,"
        "revoked_at='2021-02-01 00:00:00' WHERE source_right_id='right:company-site:00123456';",
    )
    require(
        mysql_execute(
            mysql_container_id,
            f"SELECT COUNT(DISTINCT document_id) FROM activist_event_documents "
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

    revision = "c" * 40
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

    invalid_rights = transition(
        base_url,
        "live",
        1,
        "CI rejects cutover while a referenced SourceRight is currently revoked",
        expected_status=409,
    )
    require(
        invalid_rights.get("error") == "current_source_rights_invalid"
        and invalid_rights.get("referenced_document_count") == 2
        and invalid_rights.get("invalid_source_right_document_count") == 2,
        repr(invalid_rights),
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

    live = transition(base_url, "live", 1, "CI promotes the reviewed preview state")
    require(live.get("changed") is True and live.get("state_version") == 2, repr(live))
    require(live.get("cutover_at") is not None and live.get("sunset_at") is not None, repr(live))
    cutover_at = datetime.fromisoformat(str(live["cutover_at"]).replace("Z", "+00:00"))
    sunset_at = datetime.fromisoformat(str(live["sunset_at"]).replace("Z", "+00:00"))
    require(sunset_at - cutover_at == timedelta(days=90), repr(live))
    live_events, live_headers = request_json(base_url, "api.php/api/v1/events")
    require(live_events.get("ok") is True, repr(live_events))
    require("public" in live_headers.get("Cache-Control", "").lower(), repr(live_headers))
    legacy_after, legacy_after_headers = request_json(base_url, "api.php?action=reports")
    require(legacy_after.get("ok") is True, repr(legacy_after))
    require(legacy_after_headers.get("Deprecation") == "true", repr(legacy_after_headers))
    require("Sunset" in legacy_after_headers, repr(legacy_after_headers))

    stale = transition(base_url, "closed", 1, "CI stale transition must be rejected", expected_status=409)
    require(stale.get("error") == "stale_release_state", repr(stale))

    rollback = transition(
        base_url,
        "closed",
        2,
        "CI verifies the emergency close transition",
        request_id="php73-release-close",
    )
    require(rollback.get("changed") is True and rollback.get("state_version") == 3, repr(rollback))
    require(rollback.get("cutover_at") == live.get("cutover_at") and rollback.get("sunset_at") == live.get("sunset_at"), repr(rollback))
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
    require(versions[:4] == [3, 2, 1, 0], repr(final))
    require(
        any(entry.get("request_id") == "php73-release-preview" for entry in final.get("history", [])),
        repr(final),
    )

    legacy, legacy_headers = request_json(base_url, "api.php?action=reports")
    require(legacy.get("ok") is True, repr(legacy))
    require(legacy_headers.get("Deprecation") == "true" and "Sunset" in legacy_headers, repr(legacy_headers))

    quota_day = (datetime.now(timezone.utc) + timedelta(hours=9)).date().isoformat()
    quota_status, _ = request_json(
        base_url,
        f"api.php/api/v1/ops/dart-quota?quota_day={quota_day}",
        token=ADMIN_TOKEN,
    )
    require(
        quota_status.get("action") == "status"
        and quota_status.get("accepted") == 0
        and quota_status.get("limit_count") == 10000
        and quota_status.get("used_count") == 0
        and quota_status.get("remaining_count") == 10000,
        repr(quota_status),
    )
    attempt_id = "dart-list-smoke-attempt-0001"
    consume_payload = {
        "action": "consume",
        "attempt_id": attempt_id,
        "quota_day": quota_day,
        "operation": "list",
        "code_revision": "c" * 40,
    }
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
        and consumed.get("remaining_count") == 9999
        and consumed.get("duplicate") is False
        and consumed.get("blocked_until") is None,
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
        and consumed_replay.get("used_count") == 1,
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
        "reason": "opendart_status_020",
        "code_revision": "c" * 40,
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
        and isinstance(blocked.get("blocked_until"), str),
        repr(blocked),
    )
    blocked_replay, _ = request_json(
        base_url,
        "api.php/api/v1/ops/dart-quota",
        method="POST",
        token=ADMIN_TOKEN,
        payload=block_payload,
    )
    require(blocked_replay.get("duplicate") is True, repr(blocked_replay))
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
    require(blocked_new.get("error", {}).get("code") == "dart_quota_blocked", repr(blocked_new))
    print("PHP 7.3 governance release-state smoke passed.", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8787")
    parser.add_argument("--mysql-container-id", required=True)
    args = parser.parse_args()
    run(args.base_url, args.mysql_container_id)


if __name__ == "__main__":
    main()
