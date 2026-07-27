from __future__ import annotations

import hmac
import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

from .config import load_config
from .governance import stable_id
from .dart_quota import (
    durable_dart_quota_client,
    durable_dart_quota_configured,
    durable_dart_quota_required,
)
from .official_sources import (
    DartConnector,
    DartInvocationQuota,
    DartQuotaExceededError,
    KindConnector,
    OfficialDisclosure,
    disclosure_payloads,
    parse_dart_disclosure,
    parse_kind_disclosure,
)
from .opendart_credentials import (
    OpenDartCredentialConfigurationError,
    load_opendart_credentials,
)
from .official_source_rights import (
    DartOfficialSourceRightClient,
    OfficialSourceRightClient,
    OfficialSourceRightEligibility,
    OfficialSourceRightError,
)
from .official_schedule import (
    COMPANY_MASTER_CRON_EXPRESSION,
    INCREMENTAL_CRON_EXPRESSIONS,
    next_incremental_slot,
    slot_iso,
    slot_matches_incremental_schedule,
)
from .remote_api import post_remote_action, remote_api_configured
from .shadow_engine import write_candidate_snapshot_from_events


PROJECT_ROOT = Path(__file__).resolve().parents[1]
_BACKEND_BINDING_RE = re.compile(r"^[a-f0-9]{64}$")
_DART_GUARDED_DOCUMENT_BATCH_SIZE = 40
_REMOTE_ERROR_CODE_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_REMOTE_EXCEPTION_CLASS_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")
_REMOTE_SQLSTATE_RE = re.compile(r"^[0-9A-Z]{5}$")
_BACKEND_BINDING_ERROR_CODES = {
    "backend_binding_required",
    "backend_binding_mismatch",
    "backend_binding_unavailable",
}
_TERMINAL_REMOTE_ERROR_CODES = {
    *_BACKEND_BINDING_ERROR_CODES,
    "auth_required",
    "dart_connector_inactive",
    "dart_connector_not_ready",
    "dart_connector_unavailable",
    "dart_deployment_revision_mismatch",
    "dart_deployment_revision_required",
    "dart_document_requires_approved_source_right",
    "dart_global_bridge_unavailable",
    "dart_guarded_action_required",
    "dart_guarded_payload_required",
    "dart_release_state_mismatch",
    "dart_release_state_precondition_required",
    "dart_source_right_ineligible_or_changed",
    "dart_source_right_managed_out_of_band",
    "dart_source_right_precondition_required",
    "global_dart_connector_not_writable",
    "global_release_state_guard_unavailable",
    "invalid_signature",
    "invalid_source_right_precondition",
    "release_state_unavailable",
    "secret_missing_or_too_short",
    "timestamp_expired",
}
_REMOTE_ERROR_CODE_ALLOWLIST = {
    *_BACKEND_BINDING_ERROR_CODES,
    "auth_required",
    "body_unreadable",
    "dart_connector_inactive",
    "dart_connector_not_ready",
    "dart_connector_unavailable",
    "dart_deployment_revision_mismatch",
    "dart_deployment_revision_required",
    "dart_document_requires_approved_source_right",
    "dart_global_bridge_unavailable",
    "dart_guarded_action_required",
    "dart_guarded_payload_required",
    "dart_release_state_mismatch",
    "dart_release_state_precondition_required",
    "dart_source_right_ineligible_or_changed",
    "dart_source_right_managed_out_of_band",
    "dart_source_right_precondition_required",
    "governance_snapshot_persistence_failed",
    "internal_error",
    "invalid_json",
    "invalid_json_response",
    "invalid_signature",
    "invalid_source_right_precondition",
    "kind_document_requires_approved_source_right",
    "kind_source_right_ineligible",
    "nonce_reused",
    "official_slot_completion_terminal_failure",
    "payload_too_large",
    "remote_ack_mismatch",
    "remote_error_unclassified",
    "remote_http_error",
    "remote_transport_exception",
    "secret_missing_or_too_short",
    "timestamp_expired",
    "too_many_lineage_candidates",
    "too_many_records",
}
_REMOTE_VALIDATION_REASON_ALLOWLIST = {
    "dart_event_metadata_invalid",
    "dart_document_title_provenance_conflict",
    "dart_title_provenance_conflict",
    "document_lineage_conflict",
    "event_identity_field_conflict",
    "event_identity_scope_conflict",
    "event_observation_document_missing",
    "event_observation_hash_invalid",
    "followup_event_identity_conflict",
    "global_dart_connector_not_writable",
    "global_release_state_guard_unavailable",
    "incomplete_event_identity_has_comparison_key",
    "invalid_scheduled_slot_claim_provenance",
    "invalid_collection_run_code_revision",
    "invalid_collection_run_counts",
    "invalid_complete_event_identity",
    "non_scheduled_run_has_slot_claim",
    "release_state_unavailable",
    "scheduled_slot_claim_conflict",
    "scheduled_slot_claim_completion_conflict",
}
_REMOTE_ERROR_CODE_ALLOWLIST.update(_REMOTE_VALIDATION_REASON_ALLOWLIST)


class GovernanceBackendBindingError(RuntimeError):
    """The signed write target cannot be bound to the expected MySQL backend."""


def _close_resources_independently(*resources: object | None) -> int:
    """Close every owned resource, retrying once without leaking cleanup errors."""

    failed_resources = 0
    for resource in resources:
        if resource is None:
            continue
        close = getattr(resource, "close", None)
        if not callable(close):
            continue
        for attempt in range(2):
            try:
                close()
            except Exception:  # noqa: BLE001 - cleanup must not mask the source failure.
                if attempt == 1:
                    failed_resources += 1
            else:
                break
    return failed_resources


def _required_backend_binding_id() -> str:
    binding_id = os.environ.get("BSIDE_BACKEND_BINDING_ID", "").strip()
    if _BACKEND_BINDING_RE.fullmatch(binding_id) is None:
        raise GovernanceBackendBindingError(
            "BSIDE_BACKEND_BINDING_ID must be 64 lowercase hexadecimal characters"
        )
    return binding_id


def _response_binding_matches(
    response: dict[str, object],
    expected_binding_id: str,
) -> bool:
    acknowledged = response.get("backend_binding_id")
    return bool(
        isinstance(acknowledged, str)
        and _BACKEND_BINDING_RE.fullmatch(acknowledged) is not None
        and hmac.compare_digest(acknowledged, expected_binding_id)
    )


def _response_has_terminal_remote_failure(
    response: dict[str, object],
    expected_binding_id: str,
) -> bool:
    error = response.get("error")
    error_code = (
        str(error.get("code") or "")
        if isinstance(error, dict)
        else str(error or "")
    )
    return error_code in _TERMINAL_REMOTE_ERROR_CODES or (
        response.get("ok") is True
        and not _response_binding_matches(response, expected_binding_id)
    )


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().casefold() in {"1", "true", "yes", "on"}


def _enabled_env(name: str, *, default: bool = True) -> bool:
    raw = os.environ.get(name)
    return default if raw is None else raw.strip().casefold() in {"1", "true", "yes", "on"}


def _date_env(name: str, default: date) -> date:
    value = os.environ.get(name, "").strip()
    return date.fromisoformat(value) if value else default


def source_right_payloads(config: dict[str, object], *, include_kind: bool) -> list[dict[str, object]]:
    # SourceRight grants are protected operational records. Neither OpenDART
    # nor KIND collection payloads may create, revive, or mutate one.
    _ = config, include_kind
    return []


def _dedupe_disclosures(disclosures: Iterable[OfficialDisclosure]) -> list[OfficialDisclosure]:
    by_id: dict[str, OfficialDisclosure] = {}
    for disclosure in disclosures:
        previous = by_id.get(disclosure.document_id)
        if previous is not None and previous != disclosure:
            raise ValueError(
                f"conflicting official rows share document ID {disclosure.document_id}"
            )
        by_id[disclosure.document_id] = disclosure
    return sorted(by_id.values(), key=lambda item: (item.received_at, item.document_id))


def _chunks(records: list[dict[str, object]], size: int = 1800) -> Iterable[list[dict[str, object]]]:
    for index in range(0, len(records), size):
        yield records[index : index + size]


def _payload_records(payload: dict[str, object], key: str) -> list[dict[str, object]]:
    value = payload.get(key)
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


def _int_value(value: object, default: int = 0) -> int:
    try:
        return int(str(value)) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default


def _code_revision() -> str | None:
    value = (os.environ.get("GITHUB_SHA") or os.environ.get("CURATOR_CODE_REVISION") or "").strip().casefold()
    if 7 <= len(value) <= 64 and all(character in "0123456789abcdef" for character in value):
        return value
    return None


def _run_provenance(
    *,
    current: datetime,
    idempotency_key: str | None,
    company_master_sync: bool,
) -> dict[str, object]:
    event_name = os.environ.get("GITHUB_EVENT_NAME", "").strip().casefold()
    event_schedule = os.environ.get("CURATOR_EVENT_SCHEDULE", "").strip()
    has_slot_claim = bool(
        os.environ.get("CURATOR_OFFICIAL_SLOT_CLAIM_ID", "").strip()
    )
    if idempotency_key and idempotency_key.startswith("official-backfill-v1:"):
        run_kind = "backfill"
        claim_values: dict[str, object] = {
            "scheduled_slot_at": None,
            "trigger_created_at": None,
            "slot_claim_id": None,
            "github_run_id": None,
            "github_run_attempt": None,
            "slot_claimed_at": None,
            "next_cadence_slot_at": None,
            "trigger_lag_seconds": None,
            "claim_lag_seconds": None,
            "slot_claim_late": None,
        }
    elif company_master_sync or (
        event_name == "schedule" and event_schedule == COMPANY_MASTER_CRON_EXPRESSION
    ):
        run_kind = "company_master"
        claim_values = {
            "scheduled_slot_at": None,
            "trigger_created_at": None,
            "slot_claim_id": None,
            "github_run_id": None,
            "github_run_attempt": None,
            "slot_claimed_at": None,
            "next_cadence_slot_at": None,
            "trigger_lag_seconds": None,
            "claim_lag_seconds": None,
            "slot_claim_late": None,
        }
    # GITHUB_EVENT_NAME is job-wide ambient context. Other scheduled
    # workflows (for example the legacy feed build) import and test this
    # module without being an official-ingest invocation. Only the explicit
    # official cadence or a durable slot claim may enter scheduled provenance.
    elif (event_name == "schedule" and event_schedule != "") or has_slot_claim:
        if event_schedule not in INCREMENTAL_CRON_EXPRESSIONS:
            raise ValueError("scheduled official ingest has an unknown event schedule")
        required = {
            "slot_claim_id": os.environ.get("CURATOR_OFFICIAL_SLOT_CLAIM_ID", "").strip(),
            "scheduled_slot_at": os.environ.get(
                "CURATOR_OFFICIAL_SCHEDULED_SLOT_AT", ""
            ).strip(),
            "trigger_created_at": os.environ.get(
                "CURATOR_GITHUB_RUN_CREATED_AT", ""
            ).strip(),
            "slot_claimed_at": os.environ.get(
                "CURATOR_OFFICIAL_SLOT_CLAIMED_AT", ""
            ).strip(),
            "next_cadence_slot_at": os.environ.get(
                "CURATOR_OFFICIAL_NEXT_CADENCE_SLOT_AT", ""
            ).strip(),
            "trigger_lag_seconds": os.environ.get(
                "CURATOR_OFFICIAL_TRIGGER_LAG_SECONDS", ""
            ).strip(),
            "claim_lag_seconds": os.environ.get(
                "CURATOR_OFFICIAL_CLAIM_LAG_SECONDS", ""
            ).strip(),
            "slot_claim_late": os.environ.get(
                "CURATOR_OFFICIAL_SLOT_LATE", ""
            ).strip(),
            "github_run_id": os.environ.get("CURATOR_GITHUB_RUN_ID", "").strip(),
            "github_run_attempt": os.environ.get(
                "CURATOR_GITHUB_RUN_ATTEMPT", ""
            ).strip(),
        }
        missing = sorted(key for key, value in required.items() if value == "")
        if missing:
            raise ValueError(
                "scheduled official ingest requires durable slot claim fields: "
                + ", ".join(missing)
            )
        claim_id = required["slot_claim_id"]
        github_run_id = required["github_run_id"]
        attempt_raw = required["github_run_attempt"]
        if (
            re.fullmatch(r"[0-9A-Za-z_.:-]{1,96}", claim_id) is None
            or not github_run_id.isdigit()
            or not attempt_raw.isdigit()
            or int(attempt_raw) < 1
            or github_run_id != os.environ.get("GITHUB_RUN_ID", "").strip()
            or attempt_raw != os.environ.get("GITHUB_RUN_ATTEMPT", "").strip()
        ):
            raise ValueError("scheduled official ingest has an invalid durable claim identity")

        def claim_timestamp(field: str) -> datetime:
            raw = required[field]
            try:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError as exc:
                raise ValueError(f"{field} must be an ISO timestamp") from exc
            if parsed.tzinfo is None:
                raise ValueError(f"{field} must include a timezone")
            return parsed.astimezone(timezone.utc).replace(microsecond=0)

        slot = claim_timestamp("scheduled_slot_at")
        trigger = claim_timestamp("trigger_created_at")
        claimed = claim_timestamp("slot_claimed_at")
        next_slot = claim_timestamp("next_cadence_slot_at")
        if not slot_matches_incremental_schedule(slot, event_schedule):
            raise ValueError("durable slot claim does not belong to event schedule")
        if next_slot != next_incremental_slot(slot) or trigger < slot or claimed < trigger:
            raise ValueError("durable slot claim timestamps are inconsistent")
        trigger_lag_raw = required["trigger_lag_seconds"]
        claim_lag_raw = required["claim_lag_seconds"]
        if not trigger_lag_raw.isdigit() or not claim_lag_raw.isdigit():
            raise ValueError("durable slot claim lag fields must be non-negative integers")
        trigger_lag = int(trigger_lag_raw)
        claim_lag = int(claim_lag_raw)
        late_raw = required["slot_claim_late"]
        if late_raw not in {"0", "1"}:
            raise ValueError("durable slot claim late field must be 0 or 1")
        late = late_raw == "1"
        if (
            trigger_lag != int((trigger - slot).total_seconds())
            or claim_lag != int((claimed - slot).total_seconds())
            or late is not (claimed >= next_slot)
        ):
            raise ValueError("durable slot claim lag/late fields are inconsistent")
        run_kind = "scheduled_incremental"
        claim_values = {
            "scheduled_slot_at": slot_iso(slot),
            "trigger_created_at": slot_iso(trigger),
            "slot_claim_id": claim_id,
            "github_run_id": github_run_id,
            "github_run_attempt": int(attempt_raw),
            "slot_claimed_at": slot_iso(claimed),
            "next_cadence_slot_at": slot_iso(next_slot),
            "trigger_lag_seconds": trigger_lag,
            "claim_lag_seconds": claim_lag,
            "slot_claim_late": late,
        }
    else:
        run_kind = "manual"
        claim_values = {
            "scheduled_slot_at": None,
            "trigger_created_at": None,
            "slot_claim_id": None,
            "github_run_id": None,
            "github_run_attempt": None,
            "slot_claimed_at": None,
            "next_cadence_slot_at": None,
            "trigger_lag_seconds": None,
            "claim_lag_seconds": None,
            "slot_claim_late": None,
        }
    return {
        "run_kind": run_kind,
        "event_schedule": event_schedule or None,
        **claim_values,
        "company_master_sync": company_master_sync,
    }


def _document_source(document: dict[str, object]) -> str:
    source_right_id = str(document.get("source_right_id") or "").strip().casefold()
    return source_right_id.split(":", 1)[1] if source_right_id.startswith("official:") else "unknown"


def _event_document_ids(event: dict[str, object]) -> set[str]:
    values = event.get("document_ids")
    if not isinstance(values, list):
        return set()
    return {str(value) for value in values if value}


def _validate_guarded_dart_event_document_contract(
    documents: list[dict[str, object]],
    events: list[dict[str, object]],
) -> None:
    """Guarantee every guarded DART event is transported exactly once."""

    document_ids = [str(row.get("document_id") or "") for row in documents]
    if any(not document_id for document_id in document_ids):
        raise OfficialSourceRightError("dart_guarded_document_identity_invalid")
    if len(set(document_ids)) != len(document_ids):
        raise OfficialSourceRightError("dart_guarded_document_identity_duplicate")
    document_id_set = set(document_ids)
    reference_counts = {document_id: 0 for document_id in document_ids}
    for event in events:
        referenced_document_ids = _event_document_ids(event)
        if len(referenced_document_ids) != 1:
            raise OfficialSourceRightError(
                "dart_guarded_event_document_cardinality_invalid"
            )
        if not referenced_document_ids <= document_id_set:
            raise OfficialSourceRightError(
                "dart_guarded_event_document_reference_missing"
            )
        referenced_document_id = next(iter(referenced_document_ids))
        reference_counts[referenced_document_id] += 1
    if len(events) != len(documents) or any(
        count != 1 for count in reference_counts.values()
    ):
        raise OfficialSourceRightError(
            "dart_guarded_event_document_bijection_invalid"
        )


def _remote_acknowledges_payload(
    response: dict[str, object],
    payload: dict[str, object],
) -> bool:
    """Require the server to acknowledge every submitted governance row.

    The write endpoint intentionally returns attempted upsert counts (including
    idempotent updates).  Treat a syntactically successful response with missing
    or partial counts as a failed batch so a backfill window is never checkpointed
    after silent data loss.
    """

    expected_binding_id = os.environ.get("BSIDE_BACKEND_BINDING_ID", "").strip()
    submitted_binding_id = payload.get("expected_backend_binding_id")
    if (
        response.get("ok") is not True
        or _BACKEND_BINDING_RE.fullmatch(expected_binding_id) is None
        or not isinstance(submitted_binding_id, str)
        or _BACKEND_BINDING_RE.fullmatch(submitted_binding_id) is None
        or not hmac.compare_digest(submitted_binding_id, expected_binding_id)
        or not _response_binding_matches(response, expected_binding_id)
    ):
        return False
    upserted = response.get("upserted")
    if not isinstance(upserted, dict):
        return False
    for key in ("companies", "documents", "events", "source_rights"):
        if key not in upserted:
            return False
        expected = len(_payload_records(payload, key))
        if _int_value(upserted.get(key), default=-1) != expected:
            return False
    run = payload.get("run")
    expected_runs = 1 if isinstance(run, dict) and bool(run) else 0
    if _int_value(upserted.get("runs"), default=-1) != expected_runs:
        return False
    return (
        "source_rights_rejected" in upserted
        and _int_value(upserted.get("source_rights_rejected"), default=-1) == 0
    )


def _allowlisted_remote_code(value: object, allowlist: set[str]) -> str | None:
    if not isinstance(value, str) or _REMOTE_ERROR_CODE_RE.fullmatch(value) is None:
        return None
    return value if value in allowlist else None


def _remote_response_error_code(response: dict[str, object]) -> str | None:
    error = response.get("error")
    candidate = error.get("code") if isinstance(error, dict) else error
    return _allowlisted_remote_code(candidate, _REMOTE_ERROR_CODE_ALLOWLIST)


def _safe_remote_status(response: dict[str, object]) -> int | None:
    value = response.get("status_code")
    if isinstance(value, int) and not isinstance(value, bool):
        status = value
    elif isinstance(value, str) and value.isascii() and value.isdigit():
        status = int(value)
    else:
        return None
    return status if 100 <= status <= 599 else None


def _safe_remote_body_bytes(response: dict[str, object]) -> int:
    value = response.get("_response_body_bytes")
    if isinstance(value, int) and not isinstance(value, bool):
        byte_count = value
    elif isinstance(value, str) and value.isascii() and value.isdigit():
        byte_count = int(value)
    else:
        return 0
    return max(0, byte_count)


def _safe_remote_driver_code(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        driver_code = value
    elif (
        isinstance(value, str)
        and value.isascii()
        and value.removeprefix("-").isdigit()
    ):
        driver_code = int(value)
    else:
        return None
    return driver_code if -(2**31) <= driver_code <= (2**31 - 1) else None


def _post_remote_attempt(
    action: str,
    payload: dict[str, object],
) -> tuple[dict[str, object], dict[str, object]]:
    started = time.perf_counter()
    exception_class: str | None = None
    try:
        response = post_remote_action(action, payload, timeout=45.0)
    except Exception as exc:  # noqa: BLE001 - retain class only; never the message.
        response = {"ok": False}
        candidate = type(exc).__name__
        exception_class = (
            candidate
            if _REMOTE_EXCEPTION_CLASS_RE.fullmatch(candidate) is not None
            else "Exception"
        )
    elapsed_ms = max(0, round((time.perf_counter() - started) * 1000))
    return response, {
        "http_status": _safe_remote_status(response),
        "response_body_bytes": _safe_remote_body_bytes(response),
        "elapsed_ms": elapsed_ms,
        "exception_class": exception_class,
    }


def _remote_failure_telemetry(
    response: dict[str, object],
    measurement: dict[str, object],
    *,
    scope: str,
    batch_number: int | None,
    ack_mismatch: bool,
) -> dict[str, object]:
    raw_error = response.get("error")
    error_code = (
        "remote_transport_exception"
        if measurement.get("exception_class") is not None
        else (
            "remote_ack_mismatch"
            if ack_mismatch
            else (
                _remote_response_error_code(response)
                or (
                    "remote_error_unclassified"
                    if raw_error not in (None, "")
                    else (
                        "remote_http_error"
                        if measurement.get("http_status") is not None
                        and _int_value(measurement["http_status"]) >= 400
                        else "remote_error_unclassified"
                    )
                )
            )
        )
    )
    detail: dict[str, object] = {
        "scope": scope,
        "batch_number": batch_number,
        "http_status": measurement.get("http_status"),
        "error_code": error_code,
        "response_body_bytes": _int_value(
            measurement.get("response_body_bytes")
        ),
        "elapsed_ms": _int_value(measurement.get("elapsed_ms")),
        "exception_class": measurement.get("exception_class"),
    }
    sqlstate_class = response.get("sqlstate_class")
    if (
        isinstance(sqlstate_class, str)
        and _REMOTE_SQLSTATE_RE.fullmatch(sqlstate_class) is not None
    ):
        detail["sqlstate_class"] = sqlstate_class
    driver_code = _safe_remote_driver_code(response.get("driver_code"))
    if driver_code is not None:
        detail["driver_code"] = driver_code
    validation_reason = _allowlisted_remote_code(
        response.get("validation_reason"),
        _REMOTE_VALIDATION_REASON_ALLOWLIST,
    )
    if validation_reason is not None:
        detail["validation_reason"] = validation_reason
    return detail


def _remote_failure_summary(
    details: list[dict[str, object]],
) -> dict[str, object]:
    return {
        "official_remote_failure_telemetry_count": len(details),
        "official_remote_failure_response_body_bytes": sum(
            _int_value(detail.get("response_body_bytes")) for detail in details
        ),
        "official_remote_failure_elapsed_ms": sum(
            _int_value(detail.get("elapsed_ms")) for detail in details
        ),
        "official_remote_failure_details": details,
    }


def sync_governance_payload(
    payload: dict[str, object],
    *,
    run: dict[str, object],
) -> dict[str, object]:
    companies = _payload_records(payload, "companies")
    documents = _payload_records(payload, "documents")
    events = _payload_records(payload, "events")
    rights = _payload_records(payload, "source_rights")
    preconditions_value = payload.get("expected_source_right_revisions")
    source_right_preconditions = (
        dict(preconditions_value)
        if isinstance(preconditions_value, dict)
        else {}
    )
    guarded_dart_write = "official:dart" in source_right_preconditions
    expected_deployment_code_revision = payload.get(
        "expected_deployment_code_revision"
    )
    expected_release_state = payload.get("expected_release_state")
    if guarded_dart_write and (
        not isinstance(expected_deployment_code_revision, str)
        or len(expected_deployment_code_revision) != 40
        or any(
            character not in "0123456789abcdef"
            for character in expected_deployment_code_revision
        )
    ):
        raise OfficialSourceRightError(
            "OpenDART remote write requires the exact deployed release SHA"
        )
    if guarded_dart_write and expected_release_state not in {
        "closed",
        "preview",
        "live",
    }:
        raise OfficialSourceRightError(
            "OpenDART remote write requires the exact preflighted release state"
        )
    remote_action = (
        "upsert_governance_snapshot_dart_guarded"
        if guarded_dart_write
        else "upsert_governance_snapshot"
    )
    guarded_preconditions: dict[str, object] = (
        {
            "expected_source_right_revisions": source_right_preconditions,
            "expected_deployment_code_revision": expected_deployment_code_revision,
            "expected_release_state": expected_release_state,
        }
        if guarded_dart_write
        else {}
    )
    if not remote_api_configured():
        return {
            "official_remote_synced": 0,
            "official_remote_failed": 0,
            "official_remote_skipped": 1,
            "official_remote_batches_attempted": 0,
            "official_remote_run_persisted": 0,
            "official_remote_ack_mismatches": 0,
            "official_remote_raw_count": len(documents),
            "official_remote_ack_count": 0,
            **_remote_failure_summary([]),
        }
    expected_backend_binding_id = _required_backend_binding_id()
    if guarded_dart_write:
        _validate_guarded_dart_event_document_contract(documents, events)

    # Event/document chunks stay aligned by document_id. Company master-only
    # chunks are sent first so foreign keys are available for later batches.
    company_by_id = {str(row.get("company_id") or ""): row for row in companies}
    document_batch_size = (
        _DART_GUARDED_DOCUMENT_BATCH_SIZE if guarded_dart_write else 1800
    )
    document_chunks = list(_chunks(documents, size=document_batch_size)) or [[]]
    synced = failed = attempted = ack_mismatches = 0
    failure_details: list[dict[str, object]] = []
    acknowledged_documents = 0
    selected_sources = {
        value.strip().casefold()
        for value in str(run.get("source_key") or "").split("+")
        if value.strip().casefold() in {"dart", "kind"}
    }
    source_ack_counts: dict[str, int] = {
        source: 0 for source in sorted(selected_sources)
    }
    covered_companies: set[str] = set()
    terminal_remote_failure = False
    for index, document_chunk in enumerate(document_chunks):
        document_ids = {str(row.get("document_id") or "") for row in document_chunk}
        event_chunk = [
            row
            for row in events
            if document_ids & _event_document_ids(row)
        ]
        company_ids = {
            str(row.get("company_id") or "")
            for row in [*document_chunk, *event_chunk]
            if row.get("company_id")
        }
        covered_companies.update(company_ids)
        attempted += 1
        submitted: dict[str, object] = {
            "companies": [
                company_by_id[company_id]
                for company_id in sorted(company_ids)
                if company_id in company_by_id
            ],
            "documents": document_chunk,
            "events": event_chunk,
            "source_rights": rights if index == 0 else [],
            **guarded_preconditions,
            # The collection run is written only after every data chunk has
            # returned, so an early partial failure cannot be hidden by a
            # successful final data chunk.
            "run": {},
            # This value is inside the HMAC-signed JSON body. The PHP endpoint
            # compares it with its own MySQL identity before beginTransaction,
            # preventing a same-secret routing error from mutating another DB.
            "expected_backend_binding_id": expected_backend_binding_id,
        }
        response, measurement = _post_remote_attempt(remote_action, submitted)
        if _remote_acknowledges_payload(response, submitted):
            synced += 1
            acknowledged_documents += len(document_chunk)
            for document in document_chunk:
                source = _document_source(document)
                source_ack_counts[source] = source_ack_counts.get(source, 0) + 1
        else:
            failed += 1
            ack_mismatch = response.get("ok") is True
            if ack_mismatch:
                ack_mismatches += 1
            failure_details.append(
                _remote_failure_telemetry(
                    response,
                    measurement,
                    scope="data_batch",
                    batch_number=index + 1,
                    ack_mismatch=ack_mismatch,
                )
            )
            if _response_has_terminal_remote_failure(
                response,
                expected_backend_binding_id,
            ):
                terminal_remote_failure = True
                break

    remaining = [row for company_id, row in company_by_id.items() if company_id not in covered_companies]
    if not terminal_remote_failure:
        for company_batch_number, company_chunk in enumerate(
            _chunks(remaining),
            start=1,
        ):
            attempted += 1
            submitted = {
                "companies": company_chunk,
                "documents": [],
                "events": [],
                "source_rights": [],
                **guarded_preconditions,
                "run": {},
                "expected_backend_binding_id": expected_backend_binding_id,
            }
            response, measurement = _post_remote_attempt(remote_action, submitted)
            if _remote_acknowledges_payload(response, submitted):
                synced += 1
            else:
                failed += 1
                ack_mismatch = response.get("ok") is True
                if ack_mismatch:
                    ack_mismatches += 1
                failure_details.append(
                    _remote_failure_telemetry(
                        response,
                        measurement,
                        scope="company_batch",
                        batch_number=company_batch_number,
                        ack_mismatch=ack_mismatch,
                    )
                )
                if _response_has_terminal_remote_failure(
                    response,
                    expected_backend_binding_id,
                ):
                    terminal_remote_failure = True
                    break

    final_run = dict(run)
    initial_error_count = _int_value(final_run.get("error_count"))
    initial_status = str(final_run.get("status") or "succeeded").strip().casefold()
    final_run["status"] = "failed" if failed or initial_status not in {"success", "succeeded"} else "succeeded"
    final_run["error_count"] = initial_error_count + failed
    final_run["remote_data_batches_attempted"] = attempted
    final_run["remote_data_batches_succeeded"] = synced
    final_run["remote_data_batches_failed"] = failed
    final_run["raw_count"] = _int_value(final_run.get("raw_count"), len(documents))
    final_run["ack_count"] = acknowledged_documents
    final_run["source_ack_counts"] = source_ack_counts
    final_run.update(_remote_failure_summary(failure_details))
    run_persisted = 0
    final_payload: dict[str, object] = {
        "companies": [],
        "documents": [],
        "events": [],
        "source_rights": [],
        **guarded_preconditions,
        "run": final_run,
        "expected_backend_binding_id": expected_backend_binding_id,
    }
    if not terminal_remote_failure:
        run_response, run_measurement = _post_remote_attempt(
            remote_action,
            final_payload,
        )
        if _remote_acknowledges_payload(run_response, final_payload):
            run_persisted = 1
        else:
            failed += 1
            ack_mismatch = run_response.get("ok") is True
            if ack_mismatch:
                ack_mismatches += 1
            failure_details.append(
                _remote_failure_telemetry(
                    run_response,
                    run_measurement,
                    scope="final_run",
                    batch_number=None,
                    ack_mismatch=ack_mismatch,
                )
            )
    return {
        "official_remote_synced": synced,
        "official_remote_failed": failed,
        "official_remote_skipped": 0,
        "official_remote_batches_attempted": attempted,
        "official_remote_run_persisted": run_persisted,
        "official_remote_ack_mismatches": ack_mismatches,
        "official_remote_raw_count": len(documents),
        "official_remote_ack_count": acknowledged_documents,
        **_remote_failure_summary(failure_details),
    }


def run(
    root: Path | None = None,
    *,
    now: datetime | None = None,
    start: date | None = None,
    end: date | None = None,
    settings_overrides: dict[str, object] | None = None,
    dry_run: bool = False,
    idempotency_key: str | None = None,
) -> dict[str, object]:
    """Collect and normalize one inclusive official-disclosure date window.

    ``start``/``end`` and ``settings_overrides`` are intentionally optional so
    scheduled ingestion keeps its existing environment/config contract.  The
    official backfill runner supplies them explicitly to make every chunk
    reproducible.  ``dry_run`` performs source reads and normalization but never
    calls the remote MySQL API.
    """
    project_root = root or PROJECT_ROOT
    config = load_config(project_root / "config.yaml")
    configured_settings = config.get("official_ingest", {})
    settings = dict(configured_settings) if isinstance(configured_settings, dict) else {}
    if settings_overrides:
        settings.update(settings_overrides)
    collection_started_at = datetime.now(timezone.utc)
    current = now or collection_started_at
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)
    current_kst_date = current.astimezone(ZoneInfo("Asia/Seoul")).date()
    claimed_slot_raw = os.environ.get(
        "CURATOR_OFFICIAL_SCHEDULED_SLOT_AT", ""
    ).strip()
    if claimed_slot_raw:
        try:
            claimed_slot = datetime.fromisoformat(
                claimed_slot_raw.replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise ValueError(
                "CURATOR_OFFICIAL_SCHEDULED_SLOT_AT must be an ISO timestamp"
            ) from exc
        if claimed_slot.tzinfo is None:
            raise ValueError(
                "CURATOR_OFFICIAL_SCHEDULED_SLOT_AT must include a timezone"
            )
        # A GitHub rerun of the same durable claim must read the same date
        # window even when it is executed on a later wall-clock day.
        current_kst_date = claimed_slot.astimezone(ZoneInfo("Asia/Seoul")).date()
    window_end = end or _date_env("OFFICIAL_INGEST_END", current_kst_date)
    lookback = max(0, int(settings.get("lookback_days", 2)))
    window_start = start or _date_env("OFFICIAL_INGEST_START", window_end - timedelta(days=lookback))
    if window_start > window_end:
        raise ValueError("OFFICIAL_INGEST_START must not be after OFFICIAL_INGEST_END")
    company_master_sync_requested = _truthy_env("DART_SYNC_COMPANY_MASTER") or bool(
        settings.get("sync_company_master", False)
    )
    run_provenance = _run_provenance(
        current=current,
        idempotency_key=idempotency_key,
        company_master_sync=company_master_sync_requested,
    )
    dart_enabled = bool(settings.get("dart_enabled", True))
    dart_rights_client: DartOfficialSourceRightClient | None = None
    dart_rights_initial: OfficialSourceRightEligibility | None = None
    dart_deployment_code_revision: str | None = None
    if not dry_run and dart_enabled:
        # This occurs before OpenDART network access and before any checkpoint,
        # data, or run write performed by this collector. Missing protected API
        # configuration is itself a terminal apply error.
        dart_rights_client = DartOfficialSourceRightClient()
        candidate_revision = _code_revision()
        if candidate_revision is None or len(candidate_revision) != 40:
            raise OfficialSourceRightError(
                "OpenDART apply requires the exact 40-character deployed release SHA"
            )
        dart_deployment_code_revision = candidate_revision
        dart_rights_initial = dart_rights_client.preflight(
            dart_deployment_code_revision
        )

    dart_credential_configuration_error = 0
    try:
        dart_credentials = load_opendart_credentials()
    except OpenDartCredentialConfigurationError:
        # Credential values and parser details are intentionally absent from
        # run payloads. A bad or ambiguous pool is a failed source, never a
        # silent legacy fallback.
        dart_credentials = ()
        dart_credential_configuration_error = 1
    page_count = min(100, max(1, int(settings.get("page_count", 100))))
    max_pages = max(1, int(settings.get("max_pages", 100)))
    kind_endpoint = os.environ.get("KIND_DISCLOSURE_ENDPOINT", "").strip()
    kind_requested = bool(settings.get("kind_enabled", True))
    kind_selected = kind_requested and _enabled_env("CURATOR_ENABLE_KIND")
    require_kind = _truthy_env("CURATOR_REQUIRE_KIND")
    kind_enabled = False
    kind_rights: OfficialSourceRightEligibility | None = None
    kind_configuration_error = int(
        (kind_selected and not kind_endpoint) or (require_kind and not kind_selected)
    )
    source_fetched = {"dart": 0, "kind": 0}
    source_rejected = {"dart": 0, "kind": 0}
    source_duplicates = {"dart": 0, "kind": 0}
    source_discarded = {"dart": 0, "kind": 0}
    source_errors = {
        "dart": int(
            dart_enabled
            and (not dart_credentials or dart_credential_configuration_error)
        ),
        "kind": kind_configuration_error,
    }
    source_failure_kinds: dict[str, dict[str, int]] = {
        "dart": {
            "configuration": source_errors["dart"],
            "connector": 0,
            "quota": 0,
            "parse": 0,
            "conflict": 0,
        },
        "kind": {
            "configuration": source_errors["kind"],
            "rights": 0,
            "connector": 0,
            "parse": 0,
            "conflict": 0,
        },
    }
    source_metrics: dict[str, dict[str, int]] = {
        "dart": {"list_requests": 0, "pages_fetched": 0, "rows_fetched": 0, "elapsed_ms": 0},
        "kind": {"list_requests": 0, "pages_fetched": 0, "rows_fetched": 0, "elapsed_ms": 0},
    }
    raw_fetched = 0
    disclosures: list[OfficialDisclosure] = []
    company_master: list[dict[str, object]] = []
    if dart_credentials and dart_enabled and not dart_credential_configuration_error:
        shared_budget = settings.get("dart_request_budget")
        owned_shared_budget: DartInvocationQuota | None = None
        if shared_budget is None and (
            durable_dart_quota_required() or durable_dart_quota_configured()
        ):
            owned_shared_budget = DartInvocationQuota(
                durable_dart_quota_client(
                    phase=os.environ.get(
                        "CURATOR_DART_QUOTA_PHASE",
                        "official-ingest",
                    )
                ),
                limit=10_000,
                close_delegate=True,
            )
            shared_budget = owned_shared_budget
        try:
            dart_connector = (
                DartConnector(dart_credentials, request_budget=shared_budget)
                if shared_budget is not None
                else DartConnector(dart_credentials)
            )
        except Exception:
            _close_resources_independently(owned_shared_budget)
            raise
        source_started = time.perf_counter()
        source_buffer: list[OfficialDisclosure] = []
        try:
            for row in dart_connector.iter_disclosure_rows(
                window_start,
                window_end,
                page_count=page_count,
                max_pages=max_pages,
            ):
                raw_fetched += 1
                source_fetched["dart"] += 1
                try:
                    disclosure = parse_dart_disclosure(row)
                except (TypeError, ValueError):
                    source_errors["dart"] += 1
                    source_failure_kinds["dart"]["parse"] += 1
                    continue
                if disclosure is not None:
                    source_buffer.append(disclosure)
                else:
                    source_rejected["dart"] += 1
            if source_errors["dart"] == 0 and company_master_sync_requested:
                company_master = list(dart_connector.fetch_company_master())
        except DartQuotaExceededError:
            source_errors["dart"] += 1
            source_failure_kinds["dart"]["quota"] += 1
        except Exception:  # noqa: BLE001 - the source outcome records the failed contract.
            source_errors["dart"] += 1
            source_failure_kinds["dart"]["connector"] += 1
        finally:
            cleanup_failures = 0
            try:
                source_metrics["dart"] = {
                    "list_requests": dart_connector.list_requests,
                    "pages_fetched": dart_connector.pages_fetched,
                    "rows_fetched": dart_connector.rows_fetched,
                    "elapsed_ms": max(
                        0,
                        round((time.perf_counter() - source_started) * 1000),
                    ),
                    "requests_made": getattr(dart_connector, "requests_made", 0),
                }
            finally:
                cleanup_failures = _close_resources_independently(
                    dart_connector,
                    owned_shared_budget,
                )
        if cleanup_failures:
            source_errors["dart"] += cleanup_failures
            source_failure_kinds["dart"]["connector"] += cleanup_failures
        if source_errors["dart"]:
            source_discarded["dart"] = len(source_buffer)
            company_master = []
        else:
            try:
                normalized_source = _dedupe_disclosures(source_buffer)
            except ValueError:
                source_errors["dart"] += 1
                source_failure_kinds["dart"]["conflict"] += 1
                source_discarded["dart"] = len(source_buffer)
            else:
                source_duplicates["dart"] = len(source_buffer) - len(normalized_source)
                disclosures.extend(normalized_source)

    if kind_selected and kind_endpoint:
        try:
            kind_rights = OfficialSourceRightClient().check_kind_ingest()
        except OfficialSourceRightError:
            source_errors["kind"] += 1
            source_failure_kinds["kind"]["rights"] += 1
        else:
            kind_enabled = True

    if kind_enabled:
        kind_connector = KindConnector(kind_endpoint, api_key=os.environ.get("KIND_API_KEY", ""))
        source_started = time.perf_counter()
        source_buffer = []
        try:
            for row in kind_connector.iter_disclosure_rows(
                window_start,
                window_end,
                page_count=page_count,
                max_pages=max_pages,
            ):
                raw_fetched += 1
                source_fetched["kind"] += 1
                try:
                    disclosure = parse_kind_disclosure(row)
                except (TypeError, ValueError):
                    source_errors["kind"] += 1
                    source_failure_kinds["kind"]["parse"] += 1
                    continue
                if disclosure is not None:
                    source_buffer.append(disclosure)
                else:
                    source_rejected["kind"] += 1
        except Exception:  # noqa: BLE001 - the source outcome records the failed contract.
            source_errors["kind"] += 1
            source_failure_kinds["kind"]["connector"] += 1
        finally:
            source_metrics["kind"] = {
                "list_requests": kind_connector.list_requests,
                "pages_fetched": kind_connector.pages_fetched,
                "rows_fetched": kind_connector.rows_fetched,
                "elapsed_ms": max(0, round((time.perf_counter() - source_started) * 1000)),
            }
        if source_errors["kind"]:
            source_discarded["kind"] = len(source_buffer)
        else:
            try:
                normalized_source = _dedupe_disclosures(source_buffer)
            except ValueError:
                source_errors["kind"] += 1
                source_failure_kinds["kind"]["conflict"] += 1
                source_discarded["kind"] = len(source_buffer)
            else:
                source_duplicates["kind"] = len(source_buffer) - len(normalized_source)
                disclosures.extend(normalized_source)

    normalized = _dedupe_disclosures(disclosures)
    errors = sum(source_errors.values())
    source_accepted = {
        source: sum(1 for disclosure in normalized if disclosure.source.casefold() == source)
        for source in ("dart", "kind")
    }
    source_outcomes = {
        "dart": {
            "enabled": dart_enabled,
            "rights_checked": dart_rights_initial is not None,
            "rights_eligible": dart_rights_initial is not None,
            "rights_revision": (
                dart_rights_initial.rights_revision
                if dart_rights_initial is not None
                else None
            ),
            "contract_revision": (
                dart_rights_initial.contract_revision
                if dart_rights_initial is not None
                else None
            ),
            "configured": bool(
                dart_credentials and not dart_credential_configuration_error
            ),
            "fetched": source_fetched["dart"],
            # ``fetched`` is connector volume, while ``raw_count`` is the
            # deduplicated governance-document denominator actually submitted
            # to MySQL.  Release evidence must compare ACKs against the latter;
            # otherwise non-governance rows make a healthy write look lossy.
            "raw_count": source_accepted["dart"],
            "accepted": source_accepted["dart"],
            "rejected_non_governance": source_rejected["dart"],
            "duplicate_count": source_duplicates["dart"],
            "discarded_valid_count": source_discarded["dart"],
            "error_count": source_errors["dart"],
            "failure_kinds": source_failure_kinds["dart"],
            **source_metrics["dart"],
            "status": "failed" if source_errors["dart"] else ("succeeded" if dart_enabled else "disabled"),
        },
        "kind": {
            "requested": kind_requested,
            "enabled": kind_selected,
            "required": require_kind,
            "configured": bool(kind_endpoint),
            "rights_checked": bool(kind_selected and kind_endpoint),
            "rights_eligible": kind_rights is not None,
            "rights_revision": kind_rights.rights_revision if kind_rights is not None else None,
            "fetched": source_fetched["kind"],
            "raw_count": source_accepted["kind"],
            "accepted": source_accepted["kind"],
            "rejected_non_governance": source_rejected["kind"],
            "duplicate_count": source_duplicates["kind"],
            "discarded_valid_count": source_discarded["kind"],
            "error_count": source_errors["kind"],
            "failure_kinds": source_failure_kinds["kind"],
            **source_metrics["kind"],
            "status": (
                "failed"
                if source_errors["kind"]
                else ("succeeded" if kind_enabled else "disabled")
            ),
        },
    }
    generated_payload = disclosure_payloads(normalized, retrieved_at=current)
    payload: dict[str, object] = {key: value for key, value in generated_payload.items()}
    if company_master:
        by_id = {
            str(row.get("company_id") or ""): row
            for row in _payload_records(payload, "companies")
        }
        for company in company_master:
            by_id[str(company.get("company_id") or "")] = company
        payload["companies"] = list(by_id.values())
    rights = source_right_payloads(config, include_kind=kind_enabled)
    payload["source_rights"] = rights
    slot_claim_id = str(run_provenance.get("slot_claim_id") or "")
    run_id = (
        stable_id("run", "ingest-official", slot_claim_id, length=32)
        if slot_claim_id
        else stable_id(
            "run",
            "ingest-official",
            window_start.isoformat(),
            window_end.isoformat(),
            idempotency_key or current.isoformat(),
            length=32,
        )
    )
    run_record: dict[str, object] = {
        "run_id": run_id,
        "pipeline": "ingest-official",
        "source_key": "+".join(
            source
            for source, enabled in (("dart", dart_enabled), ("kind", kind_selected or require_kind))
            if enabled
        ) or "none",
        "status": "succeeded" if errors == 0 else "failed",
        "started_at": collection_started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "fetched_count": raw_fetched,
        "resolved_count": len(normalized),
        "accepted_count": len(payload["events"]),  # type: ignore[arg-type]
        "error_count": errors,
        "code_revision": _code_revision(),
        "first_observed_at": collection_started_at.isoformat(),
        # The durable ACK contract covers submitted documents, not every row
        # examined by a source connector.  Preserve connector volume in
        # ``fetched_count`` and use the exact write denominator here.
        "raw_count": len(payload["documents"]),  # type: ignore[arg-type]
        "ack_count": 0,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "idempotency_key": idempotency_key,
        **run_provenance,
        # list.json exposes only a receipt date, not a stable receipt time.
        # Do not publish a fabricated p95 collection lag from midnight.
        "lag_seconds_p95": None,
        "source_outcomes": source_outcomes,
        "metrics": {
            "source_outcomes": source_outcomes,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "retrieved_at": current.isoformat(),
            "kind_rights_revision": (
                kind_rights.rights_revision if kind_rights is not None else None
            ),
            "dart_rights_revision": (
                dart_rights_initial.rights_revision
                if dart_rights_initial is not None
                else None
            ),
            "dart_contract_revision": (
                dart_rights_initial.contract_revision
                if dart_rights_initial is not None
                else None
            ),
            **run_provenance,
        },
    }
    if dart_rights_initial is not None:
        assert dart_rights_client is not None
        # Recheck immediately before the first remote write. The signed payload
        # carries both digests and PHP checks them again under FOR UPDATE.
        assert dart_deployment_code_revision is not None
        dart_rights_final = dart_rights_client.preflight(
            dart_deployment_code_revision
        )
        if (
            dart_rights_final.rights_revision
            != dart_rights_initial.rights_revision
            or dart_rights_final.contract_revision
            != dart_rights_initial.contract_revision
            or dart_rights_final.release_state
            != dart_rights_initial.release_state
        ):
            raise OfficialSourceRightError(
                "OpenDART SourceRight or release state changed between "
                "collection and write"
            )
        payload["expected_source_right_revisions"] = {
            "official:dart": {
                "rights_revision": dart_rights_final.rights_revision,
                "contract_revision": dart_rights_final.contract_revision,
            }
        }
        payload[
            "expected_deployment_code_revision"
        ] = dart_deployment_code_revision
        if dart_rights_final.release_state not in {
            "closed",
            "preview",
            "live",
        }:
            raise OfficialSourceRightError(
                "OpenDART preflight did not bind an exact release state"
            )
        payload["expected_release_state"] = dart_rights_final.release_state
    remote_summary = (
        {
            "official_remote_synced": 0,
            "official_remote_failed": 0,
            "official_remote_skipped": 1,
            "official_remote_batches_attempted": 0,
            "official_remote_run_persisted": 0,
            "official_remote_ack_mismatches": 0,
            "official_remote_raw_count": len(payload["documents"]),  # type: ignore[arg-type]
            "official_remote_ack_count": 0,
            **_remote_failure_summary([]),
        }
        if dry_run
        else sync_governance_payload(payload, run=run_record)
    )
    require_remote = _truthy_env("CURATOR_REQUIRE_REMOTE_API")
    remote_failed = _int_value(remote_summary.get("official_remote_failed"))
    remote_skipped = _int_value(remote_summary.get("official_remote_skipped"))
    failed = errors + remote_failed + (1 if require_remote and remote_skipped and not dry_run else 0)
    shadow_output_path = os.environ.get("CURATOR_SHADOW_ENGINE_OUTPUT_PATH", "").strip()
    if shadow_output_path:
        write_candidate_snapshot_from_events(
            _payload_records(payload, "events"),
            observation_date=current_kst_date,
            status="failed" if failed else "succeeded",
            output_path=shadow_output_path,
            code_revision=_code_revision(),
            source_run_id=run_id,
            generated_at=current,
        )
    return {
        "official_fetched": raw_fetched,
        "official_documents": len(payload["documents"]),  # type: ignore[arg-type]
        "official_events": len(payload["events"]),  # type: ignore[arg-type]
        "official_companies": len(payload["companies"]),  # type: ignore[arg-type]
        "official_source_rights": len(rights),
        "official_failed": failed,
        "official_skipped": 0,
        "official_dry_run": int(dry_run),
        "official_dart_fetched": source_fetched["dart"],
        "official_dart_accepted": source_accepted["dart"],
        "official_dart_rejected": source_rejected["dart"],
        "official_dart_duplicates": source_duplicates["dart"],
        "official_dart_discarded": source_discarded["dart"],
        "official_dart_pages": source_metrics["dart"]["pages_fetched"],
        "official_dart_requests": source_metrics["dart"].get("requests_made", 0),
        "official_dart_errors": source_errors["dart"],
        "official_dart_quota_exhausted": source_failure_kinds["dart"]["quota"],
        "official_kind_required": int(require_kind),
        "official_kind_enabled": int(kind_selected),
        "official_kind_configured": int(bool(kind_endpoint)),
        "official_kind_rights_verified": int(kind_rights is not None),
        "official_kind_fetched": source_fetched["kind"],
        "official_kind_accepted": source_accepted["kind"],
        "official_kind_rejected": source_rejected["kind"],
        "official_kind_duplicates": source_duplicates["kind"],
        "official_kind_discarded": source_discarded["kind"],
        "official_kind_pages": source_metrics["kind"]["pages_fetched"],
        "official_kind_errors": source_errors["kind"],
        **remote_summary,
    }


def main() -> None:
    summary = run()
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    if _int_value(summary.get("official_failed")):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
