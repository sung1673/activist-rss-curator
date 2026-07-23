from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import math
import os
import re
import shutil
import sys
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from .official_schedule import (
    INCREMENTAL_CRON_EXPRESSIONS,
    OFFICIAL_RUN_KINDS,
    expected_incremental_slots,
    next_incremental_slot,
    slot_matches_incremental_schedule,
)


KST = ZoneInfo("Asia/Seoul")
REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
COMPARISON_KEY_RE = re.compile(r"^eventcmp:v1:[0-9a-f]{64}$")
HUMAN_FILES = ("benchmark.json", "usability.json", "release-approval.json")
GENERATED_FILES = ("shadow.jsonl", "operations.jsonl", "performance.jsonl")
EVIDENCE_FILES = GENERATED_FILES + HUMAN_FILES
TEMPORARY_SECRET_NAMES = {
    "benchmark.json": "GOVERNANCE_BENCHMARK_EVIDENCE_B64",
    "usability.json": "GOVERNANCE_USABILITY_EVIDENCE_B64",
    "release-approval.json": "GOVERNANCE_RELEASE_APPROVAL_B64",
}
AVAILABILITY_ROUTES = ("/", "/governance/", "/feed.xml", "/api/v1/health")
AVAILABILITY_CADENCE_ID = "watchdog-v1-kst-5m-minute01"
AVAILABILITY_SLOTS_PER_ROUTE_DAY = 288
AVAILABILITY_ROUTES_PER_DAY = len(AVAILABILITY_ROUTES)
AVAILABILITY_SLOTS_PER_DAY = AVAILABILITY_SLOTS_PER_ROUTE_DAY * AVAILABILITY_ROUTES_PER_DAY
AVAILABILITY_SLOTS_PER_7_DAYS = AVAILABILITY_SLOTS_PER_DAY * 7
AVAILABILITY_BITMAP_RE = re.compile(r"^[0-9a-f]{72}$")
AVAILABILITY_MAX_GAP_SECONDS = 600.0
WEB_VITAL_ROUTES = ("/today", "/events", "/companies", "/calendar")
WEB_VITAL_METRICS = ("lcp", "inp", "cls")
REVIEWED_STATUSES = frozenset({"reviewed", "resolved", "dismissed"})
MAX_API_BYTES = 32 * 1024 * 1024
MAX_LEDGER_PAGE_BYTES = 512 * 1024
MAX_LEDGER_PAGES = 1000
# Base64 stays comfortably below GitHub's 48 KiB encrypted-secret value limit.
MAX_HUMAN_FILE_BYTES = 35 * 1024


class EvidenceInputError(ValueError):
    """Raised when production observations cannot form fail-closed evidence inputs."""


def _mapping(value: object, location: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise EvidenceInputError(f"{location}: expected an object")
    return dict(value)


def _sequence(value: object, location: str) -> list[object]:
    if not isinstance(value, list):
        raise EvidenceInputError(f"{location}: expected an array")
    return list(value)


def _text(value: object, field: str, location: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise EvidenceInputError(f"{location}: {field} must be a non-empty string")
    return result


def _integer(value: object, field: str, location: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise EvidenceInputError(f"{location}: {field} must be a non-negative integer")
    if positive and value < 1:
        raise EvidenceInputError(f"{location}: {field} must be non-zero")
    return value


def _number(value: object, field: str, location: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EvidenceInputError(f"{location}: {field} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise EvidenceInputError(f"{location}: {field} must be finite and non-negative")
    return result


def _rate(value: object, field: str, location: str) -> float:
    result = _number(value, field, location)
    if result > 1:
        raise EvidenceInputError(f"{location}: {field} must be between zero and one")
    return result


def _revision(value: object, field: str, location: str, expected: str) -> str:
    result = _text(value, field, location).casefold()
    if not REVISION_RE.fullmatch(result):
        raise EvidenceInputError(f"{location}: {field} must be a full 40-character Git SHA")
    if result != expected:
        raise EvidenceInputError(
            f"{location}: {field} {result} does not match expected revision {expected}"
        )
    return result


def _day(value: object, field: str, location: str) -> date:
    raw = _text(value, field, location)
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise EvidenceInputError(f"{location}: {field} must be YYYY-MM-DD") from exc


def _timestamp(value: object, field: str, location: str) -> datetime:
    raw = _text(value, field, location)
    try:
        result = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EvidenceInputError(f"{location}: {field} must be ISO-8601") from exc
    if result.tzinfo is None:
        raise EvidenceInputError(f"{location}: {field} must include a timezone")
    return result


def _required_days(through_date: date, count: int) -> list[date]:
    return [through_date - timedelta(days=offset) for offset in range(count - 1, -1, -1)]


def _common_record(day: date, revision: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "date": day.isoformat(),
        "environment": "production",
        "evidence_source": "production_db_export",
        "is_synthetic": False,
        # This is the closed KST evidence interval, not a fabricated source receipt time.
        "collected_at": f"{day.isoformat()}T23:59:59+09:00",
        "code_revision": revision,
    }


def _canonical_jsonl(records: Sequence[Mapping[str, object]]) -> bytes:
    return "".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        for record in records
    ).encode("utf-8")


def _read_human_files(source_dir: Path, expected_revision: str) -> dict[str, bytes]:
    source = source_dir.resolve()
    if not source.is_dir() or source_dir.is_symlink():
        raise EvidenceInputError("human evidence path must be a real directory")
    actual = {path.name for path in source.iterdir()}
    expected = set(HUMAN_FILES)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        detail = []
        if missing:
            detail.append("missing=" + ",".join(missing))
        if extra:
            detail.append("extra=" + ",".join(extra))
        raise EvidenceInputError("human artifact must contain exactly three files: " + "; ".join(detail))

    result: dict[str, bytes] = {}
    decoded: dict[str, dict[str, object]] = {}
    for filename in HUMAN_FILES:
        path = source / filename
        if path.is_symlink() or not path.is_file():
            raise EvidenceInputError(f"{filename}: symbolic links are forbidden")
        size = path.stat().st_size
        if size < 1 or size > MAX_HUMAN_FILE_BYTES:
            raise EvidenceInputError(
                f"{filename}: size must be between 1 and {MAX_HUMAN_FILE_BYTES} bytes"
            )
        raw = path.read_bytes()
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EvidenceInputError(f"{filename}: invalid UTF-8 JSON") from exc
        decoded[filename] = _mapping(value, filename)
        result[filename] = raw

    benchmark = _mapping(decoded["benchmark.json"].get("evidence"), "benchmark.json.evidence")
    provenance = {
        "benchmark.json": benchmark,
        "usability.json": decoded["usability.json"],
        "release-approval.json": decoded["release-approval.json"],
    }
    for filename, record in provenance.items():
        location = filename if filename != "benchmark.json" else "benchmark.json.evidence"
        if record.get("schema_version") != 1:
            raise EvidenceInputError(f"{location}: schema_version must be 1")
        if record.get("environment") != "production":
            raise EvidenceInputError(f"{location}: environment must be production")
        if record.get("is_synthetic") is not False:
            raise EvidenceInputError(f"{location}: synthetic evidence is forbidden")
        _timestamp(record.get("collected_at"), "collected_at", location)
        _revision(record.get("code_revision"), "code_revision", location, expected_revision)
    _revision(
        decoded["release-approval.json"].get("approved_revision"),
        "approved_revision",
        "release-approval.json",
        expected_revision,
    )
    return result


def validate_human_evidence(source_dir: Path, expected_revision: str) -> dict[str, bytes]:
    revision = expected_revision.strip().casefold()
    if not REVISION_RE.fullmatch(revision):
        raise EvidenceInputError("expected_revision must be a full 40-character Git SHA")
    return _read_human_files(source_dir, revision)


def materialize_human_secrets(
    *, output_dir: Path, expected_revision: str, environment: Mapping[str, str]
) -> dict[str, bytes]:
    output = output_dir.resolve()
    if output.exists():
        raise EvidenceInputError("output_dir already exists; human evidence artifacts are immutable")
    temporary = Path(tempfile.mkdtemp(prefix=f".{output.name}-", dir=output.parent.resolve()))
    try:
        for filename, secret_name in TEMPORARY_SECRET_NAMES.items():
            encoded = str(environment.get(secret_name, "")).strip()
            if not encoded:
                raise EvidenceInputError(f"temporary environment secret {secret_name} is missing")
            try:
                raw = base64.b64decode(encoded, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise EvidenceInputError(f"temporary environment secret {secret_name} is not base64") from exc
            if len(raw) < 1 or len(raw) > MAX_HUMAN_FILE_BYTES:
                raise EvidenceInputError(
                    f"decoded {filename} must be between 1 and {MAX_HUMAN_FILE_BYTES} bytes"
                )
            (temporary / filename).write_bytes(raw)
        validated = validate_human_evidence(temporary, expected_revision)
        temporary.replace(output)
        return validated
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def _validate_api_envelope(
    payload: Mapping[str, object], *, expected_revision: str, through_date: date
) -> datetime:
    location = "release-evidence API"
    if payload.get("ok") is not True:
        raise EvidenceInputError(f"{location}: ok must be true")
    if payload.get("evidence_source") != "production_db_export":
        raise EvidenceInputError(f"{location}: evidence_source must be production_db_export")
    if payload.get("is_synthetic") is not False:
        raise EvidenceInputError(f"{location}: synthetic values are forbidden")
    if payload.get("distribution_mode") != "web_only":
        raise EvidenceInputError(f"{location}: distribution_mode must be web_only")
    schema_version = _integer(payload.get("schema_version"), "schema_version", location, positive=True)
    if schema_version < 7:
        raise EvidenceInputError(f"{location}: schema_version must be at least 7")
    if payload.get("release_state") not in {"preview", "live"}:
        raise EvidenceInputError(f"{location}: release_state must be preview or live")
    expected_from = through_date - timedelta(days=13)
    reported_range = _mapping(payload.get("range"), f"{location}.range")
    if reported_range.get("from") != expected_from.isoformat() or reported_range.get(
        "to"
    ) != through_date.isoformat():
        raise EvidenceInputError(
            f"{location}: range must be {expected_from.isoformat()} through {through_date.isoformat()}"
        )
    generated_at = _timestamp(payload.get("generated_at"), "generated_at", location)
    revisions = _sequence(payload.get("code_revisions"), f"{location}.code_revisions")
    normalized = {
        _revision(value, "code_revision", f"{location}.code_revisions[{index}]", expected_revision)
        for index, value in enumerate(revisions)
    }
    if normalized != {expected_revision}:
        raise EvidenceInputError(f"{location}: exactly one code revision is required")
    return generated_at


def _collection_days(
    payload: Mapping[str, object], *, expected_revision: str, through_date: date
) -> dict[date, dict[str, int]]:
    records = _sequence(payload.get("official_run_ledger"), "official_run_ledger")
    ledger_days = _required_days(through_date, 7)
    required_days = set(ledger_days)
    expected_by_day = {
        day: frozenset(expected_incremental_slots(day)) for day in required_days
    }
    observed: dict[date, dict[str, dict[datetime, bool]]] = {
        day: {"dart": {}, "kind": {}} for day in required_days
    }
    totals = {
        day: {"raw": 0, "ack": 0} for day in required_days
    }
    scheduled_run_count = 0
    observed_run_slots: set[datetime] = set()
    claim_ids: set[str] = set()
    github_run_ids: set[str] = set()
    late_claim_count = 0
    incomplete_claim_count = 0
    terminal_failure_count = 0
    for index, raw in enumerate(records):
        location = f"official_run_ledger[{index}]"
        record = _mapping(raw, location)
        _text(record.get("run_id"), "run_id", location)
        pipeline = _text(record.get("pipeline"), "pipeline", location).casefold()
        if pipeline != "ingest-official":
            raise EvidenceInputError(f"{location}: official run pipeline is invalid")
        source_key = str(record.get("source_key") or "").strip().casefold()
        sources = {part.strip() for part in source_key.split("+") if part.strip()}
        if not sources or not sources <= {"dart", "kind"}:
            raise EvidenceInputError(f"{location}: source_key must contain only DART/KIND")
        _revision(record.get("code_revision"), "code_revision", location, expected_revision)
        started = _timestamp(record.get("started_at"), "started_at", location)
        finished = _timestamp(record.get("finished_at"), "finished_at", location)
        if finished < started:
            raise EvidenceInputError(f"{location}: finished_at precedes started_at")
        raw_count = _integer(record.get("raw_count"), "raw_count", location)
        ack_count = _integer(record.get("acknowledged_count"), "acknowledged_count", location)
        if ack_count > raw_count:
            raise EvidenceInputError(f"{location}: ACK count exceeds raw count")
        run_kind = _text(record.get("run_kind"), "run_kind", location).casefold()
        if run_kind not in OFFICIAL_RUN_KINDS:
            raise EvidenceInputError(f"{location}: run_kind is invalid")
        company_master_sync = record.get("company_master_sync")
        if not isinstance(company_master_sync, bool):
            raise EvidenceInputError(f"{location}: company_master_sync must be boolean")
        event_schedule_raw = record.get("event_schedule")
        event_schedule = str(event_schedule_raw or "").strip()
        scheduled_raw = record.get("scheduled_slot_at")
        if run_kind != "scheduled_incremental":
            if scheduled_raw not in (None, ""):
                raise EvidenceInputError(f"{location}: non-scheduled run claims a scheduled slot")
            continue
        scheduled_run_count += 1
        if company_master_sync:
            raise EvidenceInputError(f"{location}: incremental slot cannot be a company-master run")
        if event_schedule not in INCREMENTAL_CRON_EXPRESSIONS:
            raise EvidenceInputError(f"{location}: scheduled run has an unknown event_schedule")
        slot = _timestamp(scheduled_raw, "scheduled_slot_at", location)
        trigger = _timestamp(record.get("trigger_created_at"), "trigger_created_at", location)
        claim_id = _text(record.get("slot_claim_id"), "slot_claim_id", location)
        github_run_id = _text(record.get("github_run_id"), "github_run_id", location)
        github_run_attempt = _integer(
            record.get("github_run_attempt"), "github_run_attempt", location, positive=True
        )
        del github_run_attempt
        claimed = _timestamp(record.get("slot_claimed_at"), "slot_claimed_at", location)
        next_slot = _timestamp(
            record.get("next_cadence_slot_at"), "next_cadence_slot_at", location
        )
        trigger_lag = _integer(
            record.get("trigger_lag_seconds"), "trigger_lag_seconds", location
        )
        claim_lag = _integer(
            record.get("claim_lag_seconds"), "claim_lag_seconds", location
        )
        late = record.get("slot_claim_late")
        claim_status = _text(
            record.get("slot_claim_status"), "slot_claim_status", location
        ).casefold()
        terminal_raw = record.get("slot_claim_terminal_reason")
        terminal_reason = (
            str(terminal_raw).strip().casefold()
            if terminal_raw not in (None, "")
            else None
        )
        if (
            re.fullmatch(r"[0-9A-Za-z_.:-]{1,96}", claim_id) is None
            or re.fullmatch(r"[0-9]{1,64}", github_run_id) is None
            or claim_id in claim_ids
            or github_run_id in github_run_ids
        ):
            raise EvidenceInputError(f"{location}: slot claim identity is invalid or repeated")
        claim_ids.add(claim_id)
        github_run_ids.add(github_run_id)
        if claim_status not in {"claimed", "failed", "completed"}:
            raise EvidenceInputError(f"{location}: slot_claim_status is invalid")
        if (
            terminal_reason is not None
            and re.fullmatch(r"[a-z][a-z0-9_]{0,63}", terminal_reason) is None
        ) or (terminal_reason is not None and claim_status != "failed"):
            raise EvidenceInputError(f"{location}: slot claim terminal reason is invalid")
        if not isinstance(late, bool):
            raise EvidenceInputError(f"{location}: slot_claim_late must be boolean")
        if slot.second != 0 or slot.microsecond != 0:
            raise EvidenceInputError(f"{location}: scheduled_slot_at must be minute-aligned")
        slot_day = slot.astimezone(KST).date()
        expected_slots = frozenset(expected_incremental_slots(slot_day))
        if slot not in expected_slots:
            raise EvidenceInputError(f"{location}: scheduled_slot_at is outside the official cadence")
        if not slot_matches_incremental_schedule(slot, event_schedule):
            raise EvidenceInputError(
                f"{location}: scheduled_slot_at does not belong to event_schedule"
            )
        if next_slot != next_incremental_slot(slot):
            raise EvidenceInputError(f"{location}: next cadence boundary is invalid")
        if trigger < slot or claimed < trigger or started < claimed:
            raise EvidenceInputError(f"{location}: slot claim timestamps are not monotonic")
        if trigger_lag != int((trigger - slot).total_seconds()):
            raise EvidenceInputError(f"{location}: trigger lag does not match claim timestamps")
        if claim_lag != int((claimed - slot).total_seconds()):
            raise EvidenceInputError(f"{location}: claim lag does not match claim timestamps")
        if late is not (claimed >= next_slot):
            raise EvidenceInputError(f"{location}: late flag does not match next cadence boundary")
        run_status = _text(record.get("status"), "status", location).casefold()
        if run_status not in {"success", "succeeded", "failed", "incomplete"}:
            raise EvidenceInputError(f"{location}: scheduled run status is invalid")
        if late:
            late_claim_count += 1
        if claim_status != "completed":
            incomplete_claim_count += 1
        if terminal_reason is not None:
            terminal_failure_count += 1
        observed_run_slots.add(slot)
        if slot_day not in required_days:
            continue
        outcomes = _mapping(record.get("source_outcomes"), f"{location}.source_outcomes")
        totals[slot_day]["raw"] += raw_count
        totals[slot_day]["ack"] += ack_count
        source_raw_total = 0
        source_ack_total = 0
        for source in ("dart", "kind"):
            outcome_raw = outcomes.get(source)
            if source not in sources:
                continue
            if not isinstance(outcome_raw, dict):
                raise EvidenceInputError(
                    f"{location}.source_outcomes.{source}: selected source outcome is required"
                )
            outcome = _mapping(outcome_raw, f"{location}.source_outcomes.{source}")
            status = _text(
                outcome.get("status"), "status", f"{location}.source_outcomes.{source}"
            ).casefold()
            if status not in {"success", "succeeded", "failed", "missing"}:
                raise EvidenceInputError(
                    f"{location}.source_outcomes.{source}: status is invalid"
                )
            if "raw_count" not in outcome or "acknowledged_count" not in outcome:
                raise EvidenceInputError(
                    f"{location}.source_outcomes.{source}: raw and ACK counts are required"
                )
            source_raw = _integer(
                outcome.get("raw_count"),
                "raw_count",
                f"{location}.source_outcomes.{source}",
            )
            source_ack = _integer(
                outcome.get("acknowledged_count"),
                "acknowledged_count",
                f"{location}.source_outcomes.{source}",
            )
            if source_ack > source_raw:
                raise EvidenceInputError(
                    f"{location}.source_outcomes.{source}: ACK count exceeds raw count"
                )
            source_raw_total += source_raw
            source_ack_total += source_ack
            if slot in observed[slot_day][source]:
                raise EvidenceInputError(
                    f"{location}: duplicate scheduled {source.upper()} slot {slot.isoformat()}"
                )
            observed[slot_day][source][slot] = (
                run_status in {"success", "succeeded"}
                and status in {"success", "succeeded"}
                and claim_status == "completed"
                and not late
                and ack_count == raw_count
                and source_ack == source_raw
            )
        if source_raw_total != raw_count or source_ack_total != ack_count:
            raise EvidenceInputError(
                f"{location}: source raw and ACK counts do not reconcile with the run totals"
            )

    result: dict[date, dict[str, int]] = {}
    for day in ledger_days:
        expected_slots = expected_by_day[day]
        missing_by_source = {
            source: expected_slots - set(observed[day][source]) for source in ("dart", "kind")
        }
        failed_by_source = {
            source: sum(not succeeded for succeeded in observed[day][source].values())
            for source in ("dart", "kind")
        }
        expected_count = len(expected_slots)
        dart_successes = expected_count - len(missing_by_source["dart"]) - failed_by_source["dart"]
        kind_successes = expected_count - len(missing_by_source["kind"]) - failed_by_source["kind"]
        result[day] = {
            "attempts": expected_count * 2,
            "successes": dart_successes + kind_successes,
            "dart_attempts": expected_count,
            "dart_successes": dart_successes,
            "kind_attempts": expected_count,
            "kind_successes": kind_successes,
            "raw": totals[day]["raw"],
            "ack": totals[day]["ack"],
        }
    summary = _mapping(payload.get("official_schedule"), "official_schedule")
    expected_slot_count = len(ledger_days) * 82
    expected_all_slots = {
        slot for day in ledger_days for slot in expected_by_day[day]
    }
    dart_observed = {
        slot: succeeded
        for day in ledger_days
        for slot, succeeded in observed[day]["dart"].items()
    }
    kind_observed = {
        slot: succeeded
        for day in ledger_days
        for slot, succeeded in observed[day]["kind"].items()
    }
    dart_missing_count = len(expected_all_slots - set(dart_observed))
    kind_missing_count = len(expected_all_slots - set(kind_observed))
    dart_failed_count = sum(not succeeded for succeeded in dart_observed.values())
    kind_failed_count = sum(not succeeded for succeeded in kind_observed.values())
    present_run_slots = observed_run_slots & expected_all_slots
    expected_summary: dict[str, object] = {
        "contract_version": 1,
        "timezone": "Asia/Seoul",
        "cadence_id": "official-v1-82-slots",
        "from": ledger_days[0].isoformat(),
        "to": ledger_days[-1].isoformat(),
        "expected_slot_count": expected_slot_count,
        "ledger_row_count": len(records),
        "ledger_sha256": hashlib.sha256(
            _canonical_jsonl([_mapping(record, "official_run_ledger row") for record in records])
        ).hexdigest(),
        "scheduled_run_count": scheduled_run_count,
        "observed_slot_count": len(present_run_slots),
        "claimed_slot_count": len(present_run_slots),
        "missing_slot_count": len(expected_all_slots - present_run_slots),
        "late_claim_count": late_claim_count,
        "incomplete_claim_count": incomplete_claim_count,
        "terminal_failure_count": terminal_failure_count,
        "duplicate_slot_count": 0,
        "invalid_scheduled_run_count": 0,
        "invalid_run_metadata_count": 0,
        "dart_expected_count": expected_slot_count,
        "dart_succeeded_count": expected_slot_count - dart_missing_count - dart_failed_count,
        "dart_missing_count": dart_missing_count,
        "dart_failed_count": dart_failed_count,
        "kind_expected_count": expected_slot_count,
        "kind_succeeded_count": expected_slot_count - kind_missing_count - kind_failed_count,
        "kind_missing_count": kind_missing_count,
        "kind_failed_count": kind_failed_count,
    }
    for field, expected_value in expected_summary.items():
        if summary.get(field) != expected_value:
            raise EvidenceInputError(
                f"official_schedule: {field} does not match the complete run ledger"
            )
    return result


def _availability_days(
    payload: Mapping[str, object], *, expected_revision: str, through_date: date
) -> dict[date, dict[str, object]]:
    availability = _mapping(payload.get("availability"), "availability")
    groups = _sequence(
        availability.get("daily_route_build_counts"), "availability.daily_route_build_counts"
    )
    reported_attempts = _integer(
        availability.get("raw_attempt_count"), "raw_attempt_count", "availability", positive=True
    )
    reported_successes = _integer(
        availability.get("raw_success_count"), "raw_success_count", "availability"
    )
    reported_failures = _integer(
        availability.get("raw_failure_count"), "raw_failure_count", "availability"
    )
    reported_denominator = _integer(
        availability.get("success_rate_denominator"),
        "success_rate_denominator",
        "availability",
        positive=True,
    )
    if reported_denominator != reported_attempts or reported_successes + reported_failures != reported_attempts:
        raise EvidenceInputError("availability: top-level raw counts do not reconcile")
    top_rate = _rate(availability.get("success_rate"), "success_rate", "availability")
    if not math.isclose(top_rate, reported_successes / reported_attempts, abs_tol=0.000001):
        raise EvidenceInputError("availability: success_rate does not match raw counts")

    required_days = _required_days(through_date, 7)
    expected_group_count = len(required_days) * AVAILABILITY_ROUTES_PER_DAY
    if len(groups) != expected_group_count:
        raise EvidenceInputError(
            "availability: daily_route_build_counts must contain exactly "
            f"{expected_group_count} seven-day route groups"
        )

    grouped: dict[date, dict[str, dict[str, object]]] = {}
    sum_attempts = sum_successes = sum_failures = 0
    seen: set[tuple[date, str]] = set()
    for index, raw in enumerate(groups):
        location = f"availability.daily_route_build_counts[{index}]"
        item = _mapping(raw, location)
        day = _day(item.get("observation_date"), "observation_date", location)
        revision = _revision(item.get("build_sha"), "build_sha", location, expected_revision)
        del revision
        route = _text(item.get("route_template"), "route_template", location)
        key = (day, route)
        if key in seen:
            raise EvidenceInputError(f"{location}: duplicate day/route/build group")
        seen.add(key)
        attempted = _integer(item.get("raw_attempt_count"), "raw_attempt_count", location, positive=True)
        succeeded = _integer(item.get("raw_success_count"), "raw_success_count", location)
        failed = _integer(item.get("raw_failure_count"), "raw_failure_count", location)
        denominator = _integer(
            item.get("success_rate_denominator"), "success_rate_denominator", location, positive=True
        )
        if denominator != attempted or succeeded + failed != attempted:
            raise EvidenceInputError(f"{location}: raw counts do not reconcile")
        rate = _rate(item.get("success_rate"), "success_rate", location)
        if not math.isclose(rate, succeeded / attempted, abs_tol=0.000001):
            raise EvidenceInputError(f"{location}: success_rate does not match raw counts")
        duration = _number(item.get("duration_ms_p95"), "duration_ms_p95", location)
        cadence_id = _text(item.get("cadence_id"), "cadence_id", location)
        if cadence_id != AVAILABILITY_CADENCE_ID:
            raise EvidenceInputError(f"{location}: unsupported availability cadence_id")
        expected_slots = _integer(
            item.get("expected_slot_count"), "expected_slot_count", location, positive=True
        )
        covered_slots = _integer(
            item.get("covered_slot_count"), "covered_slot_count", location
        )
        missing_slots = _integer(
            item.get("missing_slot_count"), "missing_slot_count", location
        )
        duplicate_slots = _integer(
            item.get("duplicate_slot_count"), "duplicate_slot_count", location
        )
        off_cadence = _integer(
            item.get("off_cadence_count"), "off_cadence_count", location
        )
        if expected_slots != AVAILABILITY_SLOTS_PER_ROUTE_DAY:
            raise EvidenceInputError(
                f"{location}: expected_slot_count must be {AVAILABILITY_SLOTS_PER_ROUTE_DAY}"
            )
        if missing_slots != expected_slots - covered_slots:
            raise EvidenceInputError(f"{location}: slot coverage counts do not reconcile")
        if attempted != covered_slots + duplicate_slots + off_cadence:
            raise EvidenceInputError(
                f"{location}: attempts must equal covered + duplicate + off-cadence counts"
            )
        bitmap = _text(
            item.get("covered_slots_bitmap_hex"), "covered_slots_bitmap_hex", location
        ).casefold()
        if AVAILABILITY_BITMAP_RE.fullmatch(bitmap) is None:
            raise EvidenceInputError(
                f"{location}: covered_slots_bitmap_hex must be exactly 72 lowercase hex characters"
            )
        if int(bitmap, 16).bit_count() != covered_slots:
            raise EvidenceInputError(f"{location}: availability bitmap popcount does not reconcile")
        first_observed = _timestamp(
            item.get("first_observed_at"), "first_observed_at", location
        )
        last_observed = _timestamp(
            item.get("last_observed_at"), "last_observed_at", location
        )
        cadence_start = datetime(day.year, day.month, day.day, 0, 1, tzinfo=KST)
        cadence_end = cadence_start + timedelta(days=1)
        if not cadence_start <= first_observed < cadence_start + timedelta(minutes=5):
            raise EvidenceInputError(
                f"{location}: first_observed_at must fall in the KST 00:01 cadence slot"
            )
        if not cadence_end - timedelta(minutes=5) <= last_observed < cadence_end:
            raise EvidenceInputError(
                f"{location}: last_observed_at must fall in the KST 23:56 cadence slot"
            )
        if first_observed > last_observed:
            raise EvidenceInputError(f"{location}: observation timestamps are reversed")
        actual_interval = _number(
            item.get("actual_interval_seconds_p95"),
            "actual_interval_seconds_p95",
            location,
        )
        compatibility_interval = _number(
            item.get("observation_interval_seconds_p95"),
            "observation_interval_seconds_p95",
            location,
        )
        if not math.isclose(actual_interval, compatibility_interval, abs_tol=0.000001):
            raise EvidenceInputError(
                f"{location}: observation interval compatibility alias does not match actual value"
            )
        actual_max_gap = _number(
            item.get("actual_max_gap_seconds"), "actual_max_gap_seconds", location
        )
        if actual_interval > AVAILABILITY_MAX_GAP_SECONDS:
            raise EvidenceInputError(f"{location}: actual interval p95 exceeds 600 seconds")
        if actual_max_gap > AVAILABILITY_MAX_GAP_SECONDS:
            raise EvidenceInputError(f"{location}: actual maximum gap exceeds 600 seconds")
        if (
            covered_slots != expected_slots
            or missing_slots != 0
            or off_cadence != 0
        ):
            raise EvidenceInputError(f"{location}: complete cadence coverage is required")
        grouped.setdefault(day, {})[route] = {
            "attempted": attempted,
            "succeeded": succeeded,
            "failed": failed,
            "duration": duration,
            "cadence_id": cadence_id,
            "expected_slots": expected_slots,
            "covered_slots": covered_slots,
            "missing_slots": missing_slots,
            "duplicate_slots": duplicate_slots,
            "off_cadence": off_cadence,
            "first_observed": first_observed,
            "last_observed": last_observed,
            "actual_interval": actual_interval,
            "actual_max_gap": actual_max_gap,
        }
        sum_attempts += attempted
        sum_successes += succeeded
        sum_failures += failed
    if (sum_attempts, sum_successes, sum_failures) != (
        reported_attempts,
        reported_successes,
        reported_failures,
    ):
        raise EvidenceInputError("availability: grouped raw counts do not match top-level counts")

    result: dict[date, dict[str, object]] = {}
    total_expected = total_covered = total_missing = total_off_cadence = 0
    for day in required_days:
        by_route = grouped.get(day)
        if by_route is None:
            raise EvidenceInputError(f"availability: missing required date {day.isoformat()}")
        missing = sorted(set(AVAILABILITY_ROUTES) - set(by_route))
        extra = sorted(set(by_route) - set(AVAILABILITY_ROUTES))
        if missing or extra:
            raise EvidenceInputError(
                f"availability[{day.isoformat()}]: route set mismatch; "
                f"missing={','.join(missing)} extra={','.join(extra)}"
            )
        selected = [by_route[route] for route in AVAILABILITY_ROUTES]
        aggregate_location = f"availability[{day.isoformat()}]"
        attempts = sum(
            _integer(item.get("attempted"), "attempted", aggregate_location)
            for item in selected
        )
        successes = sum(
            _integer(item.get("succeeded"), "succeeded", aggregate_location)
            for item in selected
        )
        expected_slots = sum(
            _integer(item.get("expected_slots"), "expected_slots", aggregate_location)
            for item in selected
        )
        covered_slots = sum(
            _integer(item.get("covered_slots"), "covered_slots", aggregate_location)
            for item in selected
        )
        missing_slots = sum(
            _integer(item.get("missing_slots"), "missing_slots", aggregate_location)
            for item in selected
        )
        duplicate_slots = sum(
            _integer(item.get("duplicate_slots"), "duplicate_slots", aggregate_location)
            for item in selected
        )
        off_cadence = sum(
            _integer(item.get("off_cadence"), "off_cadence", aggregate_location)
            for item in selected
        )
        if attempts < 1:
            raise EvidenceInputError(f"availability[{day.isoformat()}]: zero denominator")
        if (
            expected_slots != AVAILABILITY_SLOTS_PER_DAY
            or covered_slots != AVAILABILITY_SLOTS_PER_DAY
            or missing_slots != 0
            or off_cadence != 0
        ):
            raise EvidenceInputError(
                f"availability[{day.isoformat()}]: expected and covered cadence totals must "
                f"both equal {AVAILABILITY_SLOTS_PER_DAY} with no missing/off-cadence slots"
            )
        first_observed = min(
            item["first_observed"] for item in selected if isinstance(item["first_observed"], datetime)
        )
        last_observed = max(
            item["last_observed"] for item in selected if isinstance(item["last_observed"], datetime)
        )
        actual_interval = max(
            _number(item.get("actual_interval"), "actual_interval", aggregate_location)
            for item in selected
        )
        actual_max_gap = max(
            _number(item.get("actual_max_gap"), "actual_max_gap", aggregate_location)
            for item in selected
        )
        result[day] = {
            "attempts": attempts,
            "successes": successes,
            "success_rate": successes / attempts,
            "cadence_id": AVAILABILITY_CADENCE_ID,
            "expected_slots": expected_slots,
            "covered_slots": covered_slots,
            "missing_slots": missing_slots,
            "duplicate_slots": duplicate_slots,
            "off_cadence": off_cadence,
            "first_observed_at": first_observed.isoformat(),
            "last_observed_at": last_observed.isoformat(),
            "actual_interval_seconds_p95": actual_interval,
            "actual_max_gap_seconds": actual_max_gap,
            "coverage_rate": covered_slots / expected_slots,
            "failure_detection_p95_minutes": actual_max_gap / 60.0,
        }
        total_expected += expected_slots
        total_covered += covered_slots
        total_missing += missing_slots
        total_off_cadence += off_cadence
    if (
        total_expected != AVAILABILITY_SLOTS_PER_7_DAYS
        or total_covered != AVAILABILITY_SLOTS_PER_7_DAYS
        or total_missing != 0
        or total_off_cadence != 0
    ):
        raise EvidenceInputError(
            "availability: seven-day expected and covered totals must both equal "
            f"{AVAILABILITY_SLOTS_PER_7_DAYS} with no missing/off-cadence slots"
        )
    return result


def _web_vitals_days(
    payload: Mapping[str, object], *, expected_revision: str, through_date: date
) -> dict[date, dict[str, float | int]]:
    vitals = _mapping(payload.get("web_vitals"), "web_vitals")
    groups = _sequence(vitals.get("groups"), "web_vitals.groups")
    reported_samples = _integer(vitals.get("raw_sample_count"), "raw_sample_count", "web_vitals")
    grouped: dict[date, dict[tuple[str, str], tuple[int, float]]] = {}
    summed_samples = 0
    for index, raw in enumerate(groups):
        location = f"web_vitals.groups[{index}]"
        item = _mapping(raw, location)
        day = _day(item.get("observation_date"), "observation_date", location)
        _revision(item.get("build_sha"), "build_sha", location, expected_revision)
        route = _text(item.get("route_template"), "route_template", location)
        metric = _text(item.get("metric_name"), "metric_name", location).casefold()
        if metric not in WEB_VITAL_METRICS:
            raise EvidenceInputError(f"{location}: unsupported metric {metric!r}")
        device = _text(item.get("device_class"), "device_class", location).casefold()
        samples = _integer(item.get("sample_count"), "sample_count", location, positive=True)
        p75 = _number(item.get("p75"), "p75", location)
        summed_samples += samples
        if device != "mobile":
            continue
        key = (route, metric)
        if key in grouped.setdefault(day, {}):
            raise EvidenceInputError(f"{location}: duplicate day/route/metric/mobile/build group")
        grouped[day][key] = (samples, p75)
    if summed_samples != reported_samples:
        raise EvidenceInputError("web_vitals: group sample counts do not match raw_sample_count")

    result: dict[date, dict[str, float | int]] = {}
    for day in _required_days(through_date, 7):
        day_groups = grouped.get(day)
        if day_groups is None:
            raise EvidenceInputError(f"web_vitals: missing required date {day.isoformat()}")
        values: dict[str, float | int] = {}
        for metric in WEB_VITAL_METRICS:
            metric_groups: list[tuple[int, float]] = []
            for route in WEB_VITAL_ROUTES:
                group = day_groups.get((route, metric))
                if group is None:
                    raise EvidenceInputError(
                        f"web_vitals[{day.isoformat()}]: missing mobile {metric} for {route}"
                    )
                if group[0] < 5:
                    raise EvidenceInputError(
                        f"web_vitals[{day.isoformat()}]: {route} {metric} requires at least five samples"
                    )
                metric_groups.append(group)
            # The gate uses the worst route p75, so every required route must satisfy the budget.
            values[f"{metric}_p75"] = max(group[1] for group in metric_groups)
            values[f"{metric}_samples"] = sum(group[0] for group in metric_groups)
        result[day] = values
    return result


def _distribution_days(
    payload: Mapping[str, object], *, expected_revision: str, through_date: date
) -> dict[date, dict[str, float | int | None]]:
    records = _sequence(payload.get("web_distribution_days"), "web_distribution_days")
    by_day: dict[date, dict[str, float | int | None]] = {}
    for index, raw in enumerate(records):
        location = f"web_distribution_days[{index}]"
        item = _mapping(raw, location)
        day = _day(item.get("observation_date"), "observation_date", location)
        if day in by_day:
            raise EvidenceInputError(f"{location}: duplicate observation_date/code_revision group")
        _revision(item.get("code_revision"), "code_revision", location, expected_revision)
        attempted = _integer(
            item.get("raw_attempt_count"), "raw_attempt_count", location, positive=True
        )
        succeeded = _integer(item.get("raw_success_count"), "raw_success_count", location)
        failed = _integer(item.get("raw_failure_count"), "raw_failure_count", location)
        denominator = _integer(
            item.get("success_rate_denominator"),
            "success_rate_denominator",
            location,
            positive=True,
        )
        if denominator != attempted or succeeded + failed != attempted:
            raise EvidenceInputError(f"{location}: web distribution counts do not reconcile")
        success_rate = _rate(item.get("success_rate"), "success_rate", location)
        if not math.isclose(success_rate, succeeded / attempted, abs_tol=0.000001):
            raise EvidenceInputError(f"{location}: success_rate does not match raw counts")
        targets_raw = _sequence(item.get("distribution_targets"), f"{location}.distribution_targets")
        targets = {_text(value, "distribution_target", location) for value in targets_raw}
        if not targets or not targets <= {"pages", "api"} or "pages" not in targets:
            raise EvidenceInputError(
                f"{location}: distribution_targets must contain pages and only pages/api"
            )
        _number(item.get("duration_ms_p95"), "duration_ms_p95", location)
        failure_raw = item.get("failure_detection_seconds_p95")
        failure_minutes = (
            None
            if failure_raw is None
            else _number(failure_raw, "failure_detection_seconds_p95", location) / 60.0
        )
        if failed > 0 and failure_minutes is None:
            raise EvidenceInputError(f"{location}: failed distributions require detection evidence")
        by_day[day] = {
            "attempts": attempted,
            "successes": succeeded,
            "failures": failed,
            "success_rate": success_rate,
            "failure_detection_p95_minutes": failure_minutes,
        }
    for day in _required_days(through_date, 7):
        if day not in by_day:
            raise EvidenceInputError(
                f"web_distribution_days: missing required date {day.isoformat()}"
            )
    return by_day


def _quality_days(
    payload: Mapping[str, object], *, expected_revision: str, through_date: date
) -> dict[date, dict[str, object]]:
    records = _sequence(payload.get("operations_days"), "operations_days")
    required_count_fields = (
        "official_evidence_total_count",
        "official_evidence_linked_count",
        "top_sensitive_total_count",
        "top_sensitive_reviewed_count",
        "original_language_total_count",
        "original_language_preserved_count",
        "source_right_total_count",
        "valid_source_right_count",
    )
    by_day: dict[date, dict[str, object]] = {}
    for index, raw in enumerate(records):
        location = f"operations_days[{index}]"
        item = _mapping(raw, location)
        day = _day(item.get("observation_date"), "observation_date", location)
        if day in by_day:
            raise EvidenceInputError(f"{location}: duplicate observation_date")
        _revision(item.get("code_revision"), "code_revision", location, expected_revision)
        if item.get("content_metric_assignment") != "immutable_quality_observation":
            raise EvidenceInputError(
                f"{location}: an immutable production quality observation is required"
            )
        _text(item.get("quality_observation_id"), "quality_observation_id", location)
        quality_hash = _text(
            item.get("quality_payload_sha256"), "quality_payload_sha256", location
        ).casefold()
        if not re.fullmatch(r"[0-9a-f]{64}", quality_hash):
            raise EvidenceInputError(f"{location}: quality_payload_sha256 must be SHA-256")
        if item.get("content_scope") != "governance_corpus_2021_plus_kst_day_end_v2":
            raise EvidenceInputError(f"{location}: invalid content_scope")
        snapshot_at = _timestamp(item.get("content_snapshot_at"), "content_snapshot_at", location)
        snapshot_kst = snapshot_at.astimezone(KST)
        if snapshot_kst.date() != day or snapshot_kst.strftime("%H:%M:%S") != "23:59:59":
            raise EvidenceInputError(f"{location}: content_snapshot_at must be the KST day end")
        if "dart_success_poll_interval_p95_minutes" in item:
            dart_poll = _number(
                item.get("dart_success_poll_interval_p95_minutes"),
                "dart_success_poll_interval_p95_minutes",
                location,
            )
        else:
            dart_poll = _number(
                item.get("dart_success_poll_interval_seconds_p95"),
                "dart_success_poll_interval_seconds_p95",
                location,
            ) / 60.0
        if "kind_observation_lag_p95_minutes" in item:
            kind_value = item.get("kind_observation_lag_p95_minutes")
        else:
            seconds = item.get("kind_first_observed_lag_seconds_p95")
            kind_value = None if seconds is None else _number(
                seconds, "kind_first_observed_lag_seconds_p95", location
            ) / 60.0
        kind_observation_count = _integer(
            item.get("kind_observation_count"), "kind_observation_count", location
        )
        kind_lag_sample_count = _integer(
            item.get("kind_lag_sample_count"), "kind_lag_sample_count", location
        )
        if kind_observation_count == 0:
            if kind_lag_sample_count != 0 or kind_value is not None:
                raise EvidenceInputError(f"{location}: invalid KIND no-disclosure observation")
            kind_lag: float | None = None
        else:
            if kind_lag_sample_count != kind_observation_count or kind_value is None:
                raise EvidenceInputError(f"{location}: incomplete KIND lag observations")
            kind_lag = _number(kind_value, "kind_observation_lag_p95_minutes", location)
        raw_value = item.get("raw_counts")
        raw_counts = _mapping(raw_value, f"{location}.raw_counts") if raw_value is not None else item
        counts = {
            field: _integer(raw_counts.get(field), field, f"{location}.raw_counts")
            for field in required_count_fields
        }
        source_count_aliases = {
            "official_ingest_expected_count": ("official_ingest_expected_count",),
            "official_ingest_succeeded_count": ("official_ingest_succeeded_count",),
            "dart_ingest_expected_count": ("dart_ingest_expected_count", "dart_expected_count"),
            "dart_ingest_succeeded_count": (
                "dart_ingest_succeeded_count",
                "dart_succeeded_count",
            ),
            "kind_ingest_expected_count": ("kind_ingest_expected_count", "kind_expected_count"),
            "kind_ingest_succeeded_count": (
                "kind_ingest_succeeded_count",
                "kind_succeeded_count",
            ),
        }
        source_counts: dict[str, int] = {}
        for output, aliases in source_count_aliases.items():
            found_name = ""
            found_value: object = None
            for container in (item, raw_counts):
                for alias in aliases:
                    if alias in container:
                        found_name = alias
                        found_value = container[alias]
                        break
                if found_name:
                    break
            source_counts[output] = _integer(
                found_value,
                found_name or output,
                location,
                positive=output
                in {
                    "official_ingest_expected_count",
                    "dart_ingest_expected_count",
                    "kind_ingest_expected_count",
                },
            )
        if source_counts["official_ingest_expected_count"] != (
            source_counts["dart_ingest_expected_count"]
            + source_counts["kind_ingest_expected_count"]
        ) or source_counts["official_ingest_succeeded_count"] != (
            source_counts["dart_ingest_succeeded_count"]
            + source_counts["kind_ingest_succeeded_count"]
        ):
            raise EvidenceInputError(f"{location}: official DART/KIND run counts do not reconcile")
        for source in ("official", "dart", "kind"):
            if source_counts[f"{source}_ingest_succeeded_count"] > source_counts[
                f"{source}_ingest_expected_count"
            ]:
                raise EvidenceInputError(f"{location}: {source} successes exceed attempts")
        for source in ("dart", "kind"):
            raw_field = f"{source}_raw_count"
            ack_field = f"{source}_acknowledged_count"
            if raw_field in item or ack_field in item:
                raw_count = _integer(item.get(raw_field), raw_field, location)
                ack_count = _integer(item.get(ack_field), ack_field, location)
                if ack_count > raw_count:
                    raise EvidenceInputError(f"{location}: {source} ACK count exceeds raw count")
        pairs = (
            ("official_evidence_linked_count", "official_evidence_total_count"),
            ("top_sensitive_reviewed_count", "top_sensitive_total_count"),
            ("original_language_preserved_count", "original_language_total_count"),
            ("valid_source_right_count", "source_right_total_count"),
        )
        for numerator, denominator in pairs:
            if counts[numerator] > counts[denominator]:
                raise EvidenceInputError(
                    f"{location}.raw_counts: {numerator} exceeds {denominator}"
                )
        by_day[day] = {
            "dart_success_poll_interval_p95_minutes": dart_poll,
            "kind_observation_lag_p95_minutes": kind_lag,
            "kind_observation_count": kind_observation_count,
            "kind_lag_sample_count": kind_lag_sample_count,
            "content_snapshot_at": snapshot_at.isoformat(),
            "content_scope": "governance_corpus_2021_plus_kst_day_end_v2",
            "official_lag_p95_minutes": dart_poll if kind_lag is None else max(dart_poll, kind_lag),
            "raw_counts": {**counts, **source_counts},
        }
    for day in _required_days(through_date, 7):
        if day not in by_day:
            raise EvidenceInputError(f"operations_days: missing required date {day.isoformat()}")
    return by_day


def _event_list(value: object, location: str) -> list[dict[str, str]]:
    records = _sequence(value, location)
    result: list[dict[str, str]] = []
    seen: set[str] = set()
    for index, raw in enumerate(records):
        if isinstance(raw, str):
            key = _text(raw, "comparison_key", f"{location}[{index}]").casefold()
        else:
            item = _mapping(raw, f"{location}[{index}]")
            key = _text(
                item.get("comparison_key"), "comparison_key", f"{location}[{index}]"
            ).casefold()
        if not COMPARISON_KEY_RE.fullmatch(key):
            raise EvidenceInputError(f"{location}[{index}]: invalid comparison_key")
        if key in seen:
            raise EvidenceInputError(f"{location}[{index}]: duplicate comparison_key")
        seen.add(key)
        result.append({"comparison_key": key})
    return result


def _shadow_records(
    payload: Mapping[str, object], *, expected_revision: str, through_date: date
) -> list[dict[str, object]]:
    records = _sequence(payload.get("shadow_days"), "shadow_days")
    discrepancy_value = payload.get("shadow_discrepancies")
    discrepancy_counts: dict[date, dict[str, int]] = {}
    overall_discrepancy_counts: dict[str, int] | None = None
    if isinstance(discrepancy_value, dict):
        overall_raw = _mapping(
            discrepancy_value.get("review_status_counts"),
            "shadow_discrepancies.review_status_counts",
        )
        overall_discrepancy_counts = {
            str(status).casefold(): _integer(
                count, str(status), "shadow_discrepancies.review_status_counts"
            )
            for status, count in overall_raw.items()
        }
    else:
        discrepancy_rows = _sequence(discrepancy_value, "shadow_discrepancies")
        for index, raw in enumerate(discrepancy_rows):
            location = f"shadow_discrepancies[{index}]"
            item = _mapping(raw, location)
            day = _day(item.get("observation_date"), "observation_date", location)
            _revision(item.get("code_revision"), "code_revision", location, expected_revision)
            status = _text(item.get("review_status"), "review_status", location).casefold()
            count = _integer(item.get("raw_count"), "raw_count", location)
            if status not in REVIEWED_STATUSES | {"pending"}:
                raise EvidenceInputError(f"{location}: unsupported review_status {status!r}")
            bucket = discrepancy_counts.setdefault(day, {})
            if status in bucket:
                raise EvidenceInputError(f"{location}: duplicate day/review_status group")
            bucket[status] = count

    by_day: dict[date, dict[str, object]] = {}
    summed_reported_counts: dict[str, int] = {}
    total_eligible_legacy_records = 0
    for index, raw in enumerate(records):
        location = f"shadow_days[{index}]"
        item = _mapping(raw, location)
        day = _day(item.get("observation_date"), "observation_date", location)
        if day in by_day:
            raise EvidenceInputError(f"{location}: duplicate observation_date")
        _revision(item.get("code_revision"), "code_revision", location, expected_revision)
        if "legacy_run" in item or "candidate_run" in item:
            legacy_run = _mapping(item.get("legacy_run"), f"{location}.legacy_run")
            candidate_run = _mapping(item.get("candidate_run"), f"{location}.candidate_run")
            legacy_status = _text(
                legacy_run.get("status"), "status", f"{location}.legacy_run"
            ).casefold()
            candidate_status = _text(
                candidate_run.get("status"), "status", f"{location}.candidate_run"
            ).casefold()
            legacy_events = _event_list(
                legacy_run.get("comparison_keys", legacy_run.get("events")),
                f"{location}.legacy_run.comparison_keys",
            )
            candidate_events = _event_list(
                candidate_run.get("comparison_keys", candidate_run.get("events")),
                f"{location}.candidate_run.comparison_keys",
            )
            for run_name, run, events in (
                ("legacy_run", legacy_run, legacy_events),
                ("candidate_run", candidate_run, candidate_events),
            ):
                if "event_count" in run and _integer(
                    run.get("event_count"), "event_count", f"{location}.{run_name}"
                ) != len(events):
                    raise EvidenceInputError(f"{location}.{run_name}: event_count does not match")
                if "events_sha256" in run:
                    keys = [event["comparison_key"] for event in events]
                    encoded = json.dumps(
                        keys, ensure_ascii=False, separators=(",", ":")
                    ).encode("utf-8")
                    expected_hash = _text(
                        run.get("events_sha256"), "events_sha256", f"{location}.{run_name}"
                    ).casefold()
                    if (
                        not re.fullmatch(r"[0-9a-f]{64}", expected_hash)
                        or hashlib.sha256(encoded).hexdigest() != expected_hash
                    ):
                        raise EvidenceInputError(f"{location}.{run_name}: events_sha256 mismatch")
        else:
            legacy_status = _text(
                item.get("legacy_status"), "legacy_status", location
            ).casefold()
            candidate_status = _text(
                item.get("candidate_status"), "candidate_status", location
            ).casefold()
            legacy_events = _event_list(item.get("legacy_events"), f"{location}.legacy_events")
            candidate_events = _event_list(
                item.get("candidate_events"), f"{location}.candidate_events"
            )
        if legacy_status != "succeeded" or candidate_status != "succeeded":
            raise EvidenceInputError(f"{location}: both shadow engines must succeed")
        crosswalk = _mapping(item.get("legacy_crosswalk"), f"{location}.legacy_crosswalk")
        schema_version = _integer(
            crosswalk.get("schema_version"),
            "schema_version",
            f"{location}.legacy_crosswalk",
        )
        eligible = _integer(
            crosswalk.get("eligible_legacy_record_count"),
            "eligible_legacy_record_count",
            f"{location}.legacy_crosswalk",
            positive=True,
        )
        crosswalked = _integer(
            crosswalk.get("crosswalked_legacy_record_count"),
            "crosswalked_legacy_record_count",
            f"{location}.legacy_crosswalk",
        )
        unmatched = _integer(
            crosswalk.get("unmatched_legacy_record_count"),
            "unmatched_legacy_record_count",
            f"{location}.legacy_crosswalk",
        )
        ambiguous = _integer(
            crosswalk.get("ambiguous_legacy_record_count"),
            "ambiguous_legacy_record_count",
            f"{location}.legacy_crosswalk",
        )
        coverage = _number(
            crosswalk.get("coverage_rate"), "coverage_rate", f"{location}.legacy_crosswalk"
        )
        crosswalk_sha = _text(
            crosswalk.get("crosswalk_sha256"),
            "crosswalk_sha256",
            f"{location}.legacy_crosswalk",
        ).casefold()
        if (
            schema_version != 1
            or crosswalked != eligible
            or unmatched != 0
            or ambiguous != 0
            or not math.isclose(coverage, 1.0, rel_tol=0.0, abs_tol=0.000001)
            or re.fullmatch(r"[0-9a-f]{64}", crosswalk_sha) is None
        ):
            raise EvidenceInputError(f"{location}: incomplete legacy crosswalk")
        total_eligible_legacy_records += eligible
        reported = _mapping(item.get("review_status_counts"), f"{location}.review_status_counts")
        normalized_reported = {
            str(status).casefold(): _integer(count, str(status), f"{location}.review_status_counts")
            for status, count in reported.items()
        }
        actual = discrepancy_counts.get(day, {})
        if overall_discrepancy_counts is None and normalized_reported != actual:
            raise EvidenceInputError(f"{location}: review status counts do not match raw discrepancies")
        if normalized_reported.get("pending", 0) > 0:
            raise EvidenceInputError(f"{location}: unreviewed discrepancies remain")
        if item.get("discrepancies_reviewed") is not None and item.get(
            "discrepancies_reviewed"
        ) is not True:
            raise EvidenceInputError(f"{location}: discrepancies_reviewed must be true")
        for status, count in normalized_reported.items():
            summed_reported_counts[status] = summed_reported_counts.get(status, 0) + count
        by_day[day] = {
            **_common_record(day, expected_revision),
            "legacy_run": {"status": legacy_status, "events": legacy_events},
            "candidate_run": {"status": candidate_status, "events": candidate_events},
            "legacy_crosswalk": {
                "schema_version": 1,
                "eligible_legacy_record_count": eligible,
                "crosswalked_legacy_record_count": crosswalked,
                "unmatched_legacy_record_count": unmatched,
                "ambiguous_legacy_record_count": ambiguous,
                "coverage_rate": 1.0,
                "crosswalk_sha256": crosswalk_sha,
            },
            "discrepancies_reviewed": True,
        }

    if overall_discrepancy_counts is not None and summed_reported_counts != overall_discrepancy_counts:
        raise EvidenceInputError(
            "shadow_discrepancies: overall review counts do not match shadow day raw counts"
        )

    expected_days = _required_days(through_date, 14)
    missing = [day.isoformat() for day in expected_days if day not in by_day]
    if missing:
        raise EvidenceInputError(f"shadow_days: missing required dates: {', '.join(missing)}")
    previous_legacy_keys: set[str] | None = None
    previous_candidate_keys: set[str] | None = None
    previous_eligible: int | None = None
    for day in expected_days:
        record = by_day[day]
        legacy_run = _mapping(record.get("legacy_run"), "shadow legacy_run")
        candidate_run = _mapping(record.get("candidate_run"), "shadow candidate_run")
        legacy_run_events = _sequence(legacy_run.get("events"), "shadow legacy events")
        candidate_run_events = _sequence(candidate_run.get("events"), "shadow candidate events")
        legacy_keys = {
            _text(
                _mapping(event, "shadow legacy event").get("comparison_key"),
                "comparison_key",
                "shadow legacy event",
            )
            for event in legacy_run_events
        }
        candidate_keys = {
            _text(
                _mapping(event, "shadow candidate event").get("comparison_key"),
                "comparison_key",
                "shadow candidate event",
            )
            for event in candidate_run_events
        }
        crosswalk = _mapping(record.get("legacy_crosswalk"), "shadow legacy_crosswalk")
        eligible = _integer(
            crosswalk.get("eligible_legacy_record_count"),
            "eligible_legacy_record_count",
            "shadow legacy_crosswalk",
            positive=True,
        )
        if not legacy_keys or not candidate_keys:
            raise EvidenceInputError(
                f"shadow_days[{day.isoformat()}]: cumulative corpus must be non-empty"
            )
        if previous_legacy_keys is not None and not previous_legacy_keys.issubset(legacy_keys):
            raise EvidenceInputError(
                f"shadow_days[{day.isoformat()}]: legacy cumulative corpus regressed"
            )
        if previous_candidate_keys is not None and not previous_candidate_keys.issubset(
            candidate_keys
        ):
            raise EvidenceInputError(
                f"shadow_days[{day.isoformat()}]: candidate cumulative corpus regressed"
            )
        if previous_eligible is not None and eligible < previous_eligible:
            raise EvidenceInputError(
                f"shadow_days[{day.isoformat()}]: legacy crosswalk denominator regressed"
            )
        previous_legacy_keys = legacy_keys
        previous_candidate_keys = candidate_keys
        previous_eligible = eligible
    if total_eligible_legacy_records < 1:
        raise EvidenceInputError("shadow_days: legacy crosswalk has no eligible denominator")
    return [by_day[day] for day in expected_days]


def _operations_records(
    payload: Mapping[str, object], *, expected_revision: str, through_date: date
) -> list[dict[str, object]]:
    # Reconstruct the denominator from the immutable cadence, not from rows
    # that happened to arrive. Manual, backfill and company-master runs were
    # validated above but are intentionally absent from these scheduled rates.
    collection = _collection_days(
        payload, expected_revision=expected_revision, through_date=through_date
    )
    quality = _quality_days(payload, expected_revision=expected_revision, through_date=through_date)
    distribution = _distribution_days(
        payload, expected_revision=expected_revision, through_date=through_date
    )
    availability = _availability_days(
        payload, expected_revision=expected_revision, through_date=through_date
    )
    records: list[dict[str, object]] = []
    for day in _required_days(through_date, 7):
        quality_day = quality[day]
        web = distribution[day]
        watchdog = availability[day]
        raw_counts = dict(_mapping(quality_day["raw_counts"], "quality raw_counts"))
        scheduled = collection[day]
        scheduled_counts = {
            "official_ingest_expected_count": scheduled["attempts"],
            "official_ingest_succeeded_count": scheduled["successes"],
            "dart_ingest_expected_count": scheduled["dart_attempts"],
            "dart_ingest_succeeded_count": scheduled["dart_successes"],
            "kind_ingest_expected_count": scheduled["kind_attempts"],
            "kind_ingest_succeeded_count": scheduled["kind_successes"],
        }
        reported_counts = {
            name: _integer(raw_counts.get(name), name, f"operations_days[{day.isoformat()}]")
            for name in scheduled_counts
        }
        if reported_counts != scheduled_counts:
            raise EvidenceInputError(
                f"operations_days[{day.isoformat()}]: official schedule counts do not match "
                "the run ledger"
            )
        raw_counts.update(scheduled_counts)
        raw_counts["official_scheduled_slot_count"] = scheduled["dart_attempts"]
        failure_detection = web["failure_detection_p95_minutes"]
        if failure_detection is None:
            # With no real failure, the actual watchdog interval is the measured detection upper bound.
            failure_detection = _number(
                watchdog.get("failure_detection_p95_minutes"),
                "failure_detection_p95_minutes",
                f"availability[{day.isoformat()}]",
            )
        def corpus_rate(numerator: str, denominator: str) -> float | None:
            denominator_value = _integer(
                raw_counts.get(denominator), denominator, f"operations_days[{day.isoformat()}]"
            )
            numerator_value = _integer(
                raw_counts.get(numerator), numerator, f"operations_days[{day.isoformat()}]"
            )
            return None if denominator_value == 0 else numerator_value / denominator_value

        raw_counts["kind_observation_count"] = quality_day["kind_observation_count"]
        raw_counts["kind_lag_sample_count"] = quality_day["kind_lag_sample_count"]
        records.append(
            {
                **_common_record(day, expected_revision),
                "metrics": {
                    "metrics_contract_version": 2,
                    "distribution_mode": "web_only",
                    "official_ingest_success_rate": scheduled["successes"]
                    / scheduled["attempts"],
                    "dart_ingest_success_rate": scheduled["dart_successes"]
                    / scheduled["dart_attempts"],
                    "kind_ingest_success_rate": scheduled["kind_successes"]
                    / scheduled["kind_attempts"],
                    "dart_success_poll_interval_p95_minutes": quality_day[
                        "dart_success_poll_interval_p95_minutes"
                    ],
                    "kind_observation_lag_p95_minutes": quality_day[
                        "kind_observation_lag_p95_minutes"
                    ],
                    "content_snapshot_at": quality_day["content_snapshot_at"],
                    "content_scope": quality_day["content_scope"],
                    "official_lag_p95_minutes": quality_day["official_lag_p95_minutes"],
                    "web_distribution_attempted_count": web["attempts"],
                    "web_distribution_succeeded_count": web["successes"],
                    "web_distribution_success_rate": web["success_rate"],
                    "web_distribution_failure_detection_p95_minutes": failure_detection,
                    "telegram_delivery_attempted_count": 0,
                    "raw_counts": raw_counts,
                    "official_evidence_link_rate": corpus_rate(
                        "official_evidence_linked_count", "official_evidence_total_count"
                    ),
                    "top_sensitive_human_review_rate": corpus_rate(
                        "top_sensitive_reviewed_count", "top_sensitive_total_count"
                    ),
                    "original_language_preservation_rate": corpus_rate(
                        "original_language_preserved_count", "original_language_total_count"
                    ),
                    "valid_source_right_rate": corpus_rate(
                        "valid_source_right_count", "source_right_total_count"
                    ),
                },
            }
        )
    return records


def _performance_records(
    payload: Mapping[str, object], *, expected_revision: str, through_date: date
) -> list[dict[str, object]]:
    availability = _availability_days(payload, expected_revision=expected_revision, through_date=through_date)
    vitals = _web_vitals_days(payload, expected_revision=expected_revision, through_date=through_date)
    records: list[dict[str, object]] = []
    for day in _required_days(through_date, 7):
        web = availability[day]
        vital = vitals[day]
        records.append(
            {
                **_common_record(day, expected_revision),
                "metrics": {
                    "availability_rate": web["success_rate"],
                    "availability_cadence_id": web["cadence_id"],
                    "availability_actual_interval_seconds_p95": web[
                        "actual_interval_seconds_p95"
                    ],
                    "availability_actual_max_gap_seconds": web[
                        "actual_max_gap_seconds"
                    ],
                    "availability_first_observed_at": web["first_observed_at"],
                    "availability_last_observed_at": web["last_observed_at"],
                    "availability_coverage_rate": web["coverage_rate"],
                    "mobile_lcp_p75_seconds": float(vital["lcp_p75"]) / 1000.0,
                    "mobile_inp_p75_ms": vital["inp_p75"],
                    "mobile_cls_p75": vital["cls_p75"],
                    "raw_counts": {
                        "availability_attempted_count": web["attempts"],
                        "availability_succeeded_count": web["successes"],
                        "availability_expected_slot_count": web["expected_slots"],
                        "availability_covered_slot_count": web["covered_slots"],
                        "availability_missing_slot_count": web["missing_slots"],
                        "availability_duplicate_slot_count": web["duplicate_slots"],
                        "availability_off_cadence_count": web["off_cadence"],
                        "mobile_lcp_sample_count": vital["lcp_samples"],
                        "mobile_inp_sample_count": vital["inp_samples"],
                        "mobile_cls_sample_count": vital["cls_samples"],
                    },
                },
            }
        )
    return records


def build_evidence_inputs(
    *,
    api_export: Mapping[str, object],
    human_dir: Path,
    output_dir: Path,
    expected_revision: str,
    through_date: date,
) -> dict[str, object]:
    revision = expected_revision.strip().casefold()
    if not REVISION_RE.fullmatch(revision):
        raise EvidenceInputError("expected_revision must be a full 40-character Git SHA")
    generated_at = _validate_api_envelope(
        api_export, expected_revision=revision, through_date=through_date
    )
    human = validate_human_evidence(human_dir, revision)
    shadow = _shadow_records(api_export, expected_revision=revision, through_date=through_date)
    operations = _operations_records(api_export, expected_revision=revision, through_date=through_date)
    performance = _performance_records(api_export, expected_revision=revision, through_date=through_date)

    output = output_dir.resolve()
    source = human_dir.resolve()
    if output.exists():
        raise EvidenceInputError("output_dir already exists; evidence inputs are immutable")
    if output == source or output.is_relative_to(source) or source.is_relative_to(output):
        raise EvidenceInputError("human_dir and output_dir must be disjoint")
    rendered = {
        "shadow.jsonl": _canonical_jsonl(shadow),
        "operations.jsonl": _canonical_jsonl(operations),
        "performance.jsonl": _canonical_jsonl(performance),
        **human,
    }
    temporary = Path(tempfile.mkdtemp(prefix=f".{output.name}-", dir=output.parent.resolve()))
    try:
        for filename in EVIDENCE_FILES:
            (temporary / filename).write_bytes(rendered[filename])
        temporary.replace(output)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return {
        "status": "release-evidence-inputs-ready",
        "through_date": through_date.isoformat(),
        "code_revision": revision,
        "generated_at": generated_at.isoformat(),
        "files": list(EVIDENCE_FILES),
    }


def _api_url(base_url: str, *, from_date: date, to_date: date) -> str:
    raw = base_url.strip().rstrip("/")
    parsed = urlsplit(raw)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise EvidenceInputError(
            "BSIDE_API_BASE_URL must be a credential-free, query-free HTTPS URL"
        )
    path = parsed.path.rstrip("/")
    if not path.endswith("/api/v1"):
        raise EvidenceInputError("BSIDE_API_BASE_URL must end with /api/v1")
    query = urlencode({"from": from_date.isoformat(), "to": to_date.isoformat()})
    return urlunsplit((parsed.scheme, parsed.netloc, path + "/ops/release-evidence", query, ""))


def _official_ledger_url(
    base_url: str,
    *,
    from_date: date,
    to_date: date,
    cursor: str | None,
) -> str:
    raw = base_url.strip().rstrip("/")
    parsed = urlsplit(raw)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise EvidenceInputError(
            "BSIDE_API_BASE_URL must be a credential-free, query-free HTTPS URL"
        )
    path = parsed.path.rstrip("/")
    if not path.endswith("/api/v1"):
        raise EvidenceInputError("BSIDE_API_BASE_URL must end with /api/v1")
    parameters = {
        "from": from_date.isoformat(),
        "to": to_date.isoformat(),
        "limit": "100",
    }
    if cursor is not None:
        parameters["cursor"] = cursor
    return urlunsplit(
        (parsed.scheme, parsed.netloc, path + "/ops/official-run-ledger", urlencode(parameters), "")
    )


def _fetch_json_object(
    *, url: str, token: str, timeout_seconds: float, byte_limit: int, location: str
) -> dict[str, object]:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
            "User-Agent": "bside-release-evidence-inputs/1",
        },
        method="GET",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - HTTPS enforced.
            content_length = response.headers.get("Content-Length")
            if content_length and int(content_length) > byte_limit:
                raise EvidenceInputError(f"{location} response is too large")
            raw = response.read(byte_limit + 1)
    except HTTPError as exc:
        raise EvidenceInputError(f"{location} returned HTTP {exc.code}") from exc
    except EvidenceInputError:
        raise
    except (URLError, OSError, ValueError) as exc:
        raise EvidenceInputError(f"{location} request failed") from exc
    if len(raw) > byte_limit:
        raise EvidenceInputError(f"{location} response is too large")
    try:
        return _mapping(json.loads(raw.decode("utf-8")), location)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceInputError(f"{location} returned invalid UTF-8 JSON") from exc


def fetch_official_run_ledger(
    *,
    base_url: str,
    ops_token: str,
    from_date: date,
    to_date: date,
    timeout_seconds: float = 30.0,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    token = ops_token.strip()
    if len(token) < 32:
        raise EvidenceInputError("BSIDE_OPS_TOKEN must contain at least 32 characters")
    cursor: str | None = None
    seen_cursors: set[str] = set()
    rows: list[dict[str, object]] = []
    seen_run_ids: set[str] = set()
    contract: dict[str, object] | None = None
    for page_number in range(1, MAX_LEDGER_PAGES + 1):
        payload = _fetch_json_object(
            url=_official_ledger_url(
                base_url, from_date=from_date, to_date=to_date, cursor=cursor
            ),
            token=token,
            timeout_seconds=timeout_seconds,
            byte_limit=MAX_LEDGER_PAGE_BYTES,
            location="official-run-ledger API",
        )
        if payload.get("ok") is not True:
            raise EvidenceInputError("official-run-ledger API did not acknowledge the request")
        page_range = _mapping(payload.get("range"), "official-run-ledger.range")
        if page_range != {"from": from_date.isoformat(), "to": to_date.isoformat()}:
            raise EvidenceInputError("official-run-ledger API range mismatch")
        page_contract = {
            "ledger_row_count": _integer(
                payload.get("ledger_row_count"),
                "ledger_row_count",
                "official-run-ledger",
                positive=True,
            ),
            "ledger_sha256": _text(
                payload.get("ledger_sha256"), "ledger_sha256", "official-run-ledger"
            ).casefold(),
        }
        if re.fullmatch(r"[0-9a-f]{64}", str(page_contract["ledger_sha256"])) is None:
            raise EvidenceInputError("official-run-ledger ledger_sha256 must be SHA-256")
        if contract is None:
            contract = page_contract
        elif page_contract != contract:
            raise EvidenceInputError("official-run-ledger contract changed during pagination")
        page_rows = _sequence(payload.get("data"), "official-run-ledger.data")
        if len(page_rows) > 100:
            raise EvidenceInputError("official-run-ledger page exceeds the requested limit")
        for index, raw_row in enumerate(page_rows):
            row = _mapping(raw_row, f"official-run-ledger.page[{page_number}].data[{index}]")
            run_id = _text(row.get("run_id"), "run_id", "official-run-ledger row")
            if run_id in seen_run_ids:
                raise EvidenceInputError("official-run-ledger repeated a run_id across pages")
            seen_run_ids.add(run_id)
            rows.append(row)
        pagination = _mapping(payload.get("pagination"), "official-run-ledger.pagination")
        returned = _integer(
            pagination.get("returned"), "returned", "official-run-ledger.pagination"
        )
        if returned != len(page_rows):
            raise EvidenceInputError("official-run-ledger returned count mismatch")
        has_more = pagination.get("has_more")
        if not isinstance(has_more, bool):
            raise EvidenceInputError("official-run-ledger has_more must be boolean")
        next_value = pagination.get("next_cursor")
        if not has_more:
            if next_value not in (None, ""):
                raise EvidenceInputError("official-run-ledger terminal page has a cursor")
            break
        if not isinstance(next_value, str) or not next_value or len(next_value) > 512:
            raise EvidenceInputError("official-run-ledger next cursor is invalid")
        if not page_rows or next_value in seen_cursors:
            raise EvidenceInputError("official-run-ledger pagination did not advance")
        seen_cursors.add(next_value)
        cursor = next_value
    else:
        raise EvidenceInputError("official-run-ledger exceeded the page safety limit")
    assert contract is not None
    if len(rows) != contract["ledger_row_count"]:
        raise EvidenceInputError("official-run-ledger total row count mismatch")
    digest = hashlib.sha256(_canonical_jsonl(rows)).hexdigest()
    if digest != contract["ledger_sha256"]:
        raise EvidenceInputError("official-run-ledger digest mismatch")
    return rows, contract


def fetch_api_export(
    *, base_url: str, ops_token: str, from_date: date, to_date: date, timeout_seconds: float = 30.0
) -> dict[str, object]:
    token = ops_token.strip()
    if len(token) < 32:
        raise EvidenceInputError("BSIDE_OPS_TOKEN must contain at least 32 characters")
    payload = _fetch_json_object(
        url=_api_url(base_url, from_date=from_date, to_date=to_date),
        token=token,
        timeout_seconds=timeout_seconds,
        byte_limit=MAX_API_BYTES,
        location="release-evidence API",
    )
    ledger_from = to_date - timedelta(days=6)
    ledger, contract = fetch_official_run_ledger(
        base_url=base_url,
        ops_token=token,
        from_date=ledger_from,
        to_date=to_date,
        timeout_seconds=timeout_seconds,
    )
    schedule = _mapping(payload.get("official_schedule"), "official_schedule")
    if (
        schedule.get("from") != ledger_from.isoformat()
        or schedule.get("to") != to_date.isoformat()
        or schedule.get("ledger_row_count") != contract["ledger_row_count"]
        or schedule.get("ledger_sha256") != contract["ledger_sha256"]
    ):
        raise EvidenceInputError("release-evidence official schedule does not match the run ledger")
    payload["official_run_ledger"] = ledger
    return payload


def _load_api_json(path: Path) -> dict[str, object]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_API_BYTES:
        raise EvidenceInputError("api_json must be a regular JSON file within the size limit")
    try:
        return _mapping(json.loads(path.read_text(encoding="utf-8")), "api_json")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceInputError("api_json contains invalid UTF-8 JSON") from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build fail-closed production release-evidence inputs.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate-human", help="validate the exact protected human artifact")
    validate.add_argument("--human-dir", type=Path, required=True)
    validate.add_argument("--expected-revision", default=os.environ.get("GITHUB_SHA", ""))

    materialize = subparsers.add_parser(
        "materialize-human", help="decode one-use environment secrets into an exact human artifact"
    )
    materialize.add_argument("--output-dir", type=Path, required=True)
    materialize.add_argument("--expected-revision", default=os.environ.get("GITHUB_SHA", ""))

    build = subparsers.add_parser("build", help="query production observations and combine human evidence")
    build.add_argument("--api-base-url", default=os.environ.get("BSIDE_API_BASE_URL", ""))
    build.add_argument("--ops-token", default=os.environ.get("BSIDE_OPS_TOKEN", ""))
    build.add_argument("--api-json", type=Path)
    build.add_argument("--human-dir", type=Path, required=True)
    build.add_argument("--output-dir", type=Path, required=True)
    build.add_argument("--expected-revision", default=os.environ.get("GITHUB_SHA", ""))
    build.add_argument(
        "--through-date",
        default=os.environ.get("EVIDENCE_THROUGH_DATE", ""),
        help="final KST evidence date (default: previous KST day)",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "validate-human":
            validated = validate_human_evidence(args.human_dir, args.expected_revision)
            result: dict[str, object] = {"status": "valid-human-evidence", "files": sorted(validated)}
        elif args.command == "materialize-human":
            validated = materialize_human_secrets(
                output_dir=args.output_dir,
                expected_revision=args.expected_revision,
                environment=os.environ,
            )
            result = {"status": "human-evidence-materialized", "files": sorted(validated)}
        else:
            through_date = (
                date.fromisoformat(str(args.through_date).strip())
                if str(args.through_date).strip()
                else datetime.now(KST).date() - timedelta(days=1)
            )
            api_export = (
                _load_api_json(args.api_json)
                if args.api_json is not None
                else fetch_api_export(
                    base_url=args.api_base_url,
                    ops_token=args.ops_token,
                    from_date=through_date - timedelta(days=13),
                    to_date=through_date,
                )
            )
            result = build_evidence_inputs(
                api_export=api_export,
                human_dir=args.human_dir,
                output_dir=args.output_dir,
                expected_revision=args.expected_revision,
                through_date=through_date,
            )
    except (EvidenceInputError, OSError, ValueError) as exc:
        print(
            json.dumps(
                {"status": "invalid-release-evidence-input", "error": str(exc)},
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        raise SystemExit(2) from exc
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
