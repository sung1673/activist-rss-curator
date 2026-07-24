import base64
import hashlib
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import yaml

from curator import release_evidence_inputs
from curator.release_evidence_inputs import (
    EVIDENCE_FILES,
    EvidenceInputError,
    _api_url,
    build_evidence_inputs,
    fetch_official_run_ledger,
    materialize_human_secrets,
    validate_human_evidence,
)
from curator.official_schedule import expected_incremental_slots, next_incremental_slot
from curator.release_gate import (
    GateThresholds,
    build_operations_gates,
    build_performance_gates,
    build_shadow_comparison,
)


REVISION = "a" * 40
THROUGH = date(2026, 7, 14)
ROOT = Path(__file__).resolve().parents[1]
AVAILABILITY_ROUTES = ("/", "/governance/", "/feed.xml", "/api/v1/health")
WEB_VITAL_ROUTES = ("/today", "/events", "/companies", "/calendar")


def _human_reports() -> dict[str, dict[str, object]]:
    provenance = {
        "schema_version": 1,
        "environment": "production",
        "evidence_source": "human_labeled_jsonl",
        "is_synthetic": False,
        "collected_at": "2026-07-14T15:00:00Z",
        "code_revision": REVISION,
    }
    return {
        "benchmark.json": {"schema_version": 1, "evidence": provenance},
        "usability.json": {**provenance, "dataset_sha256": "b" * 64},
        "release-approval.json": {
            **provenance,
            "approved_revision": REVISION,
            "release_approved": True,
        },
    }


def _write_human(path: Path) -> dict[str, bytes]:
    path.mkdir()
    rendered: dict[str, bytes] = {}
    for filename, report in _human_reports().items():
        raw = (json.dumps(report, sort_keys=True) + "\n").encode()
        (path / filename).write_bytes(raw)
        rendered[filename] = raw
    return rendered


def _quality_counts() -> dict[str, int]:
    return {
        "official_ingest_expected_count": 164,
        "official_ingest_succeeded_count": 164,
        "dart_ingest_expected_count": 82,
        "dart_ingest_succeeded_count": 82,
        "kind_ingest_expected_count": 82,
        "kind_ingest_succeeded_count": 82,
        "official_evidence_total_count": 100,
        "official_evidence_linked_count": 98,
        "top_sensitive_total_count": 5,
        "top_sensitive_reviewed_count": 5,
        "original_language_total_count": 100,
        "original_language_preserved_count": 100,
        "source_right_total_count": 10,
        "valid_source_right_count": 10,
    }


def _schedule_for_slot(slot: datetime) -> str:
    hour = slot.astimezone(ZoneInfo("Asia/Seoul")).hour
    if hour < 7:
        return "0,30 15-21 * * *"
    if hour < 9:
        return "0,15,30,45 22-23 * * *"
    return "0,15,30,45 0-14 * * *"


def _official_run_ledger(day: date) -> list[dict[str, object]]:
    rows = []
    for index, slot in enumerate(expected_incremental_slots(day)):
        trigger = slot + timedelta(seconds=30)
        claimed = slot + timedelta(seconds=45)
        rows.append(
            {
                "run_id": f"run:{day.isoformat()}:{index:02d}",
                "pipeline": "ingest-official",
                "source_key": "dart+kind",
                "code_revision": REVISION,
                "status": "succeeded",
                "trigger_created_at": trigger.isoformat(),
                "started_at": (slot + timedelta(minutes=1)).isoformat(),
                "finished_at": (slot + timedelta(minutes=2)).isoformat(),
                "raw_count": 10,
                "acknowledged_count": 10,
                "run_kind": "scheduled_incremental",
                "event_schedule": _schedule_for_slot(slot),
                "scheduled_slot_at": slot.isoformat(),
                "slot_claim_id": f"official-slot:{day.isoformat()}:{index:02d}",
                "github_run_id": str(int(slot.timestamp())),
                "github_run_attempt": 1,
                "slot_claimed_at": claimed.isoformat(),
                "next_cadence_slot_at": next_incremental_slot(slot).isoformat(),
                "trigger_lag_seconds": 30,
                "claim_lag_seconds": 45,
                "slot_claim_late": False,
                "slot_claim_status": "completed",
                "slot_claim_terminal_reason": None,
                "company_master_sync": False,
                "source_outcomes": {
                    "dart": {
                        "status": "succeeded",
                        "raw_count": 5,
                        "acknowledged_count": 5,
                    },
                    "kind": {
                        "status": "succeeded",
                        "raw_count": 5,
                        "acknowledged_count": 5,
                    },
                },
            }
        )
    return rows


def _refresh_ledger_identity(payload: dict[str, object]) -> None:
    ledger = payload["official_run_ledger"]
    summary = payload["official_schedule"]
    assert isinstance(ledger, list) and isinstance(summary, dict)
    raw = "".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        for row in ledger
    ).encode("utf-8")
    summary["ledger_row_count"] = len(ledger)
    summary["ledger_sha256"] = hashlib.sha256(raw).hexdigest()
    start = date.fromisoformat(str(summary["from"]))
    end = date.fromisoformat(str(summary["to"]))
    days = [start + timedelta(days=offset) for offset in range((end - start).days + 1)]
    expected = {slot for day in days for slot in expected_incremental_slots(day)}
    scheduled = [row for row in ledger if row.get("run_kind") == "scheduled_incremental"]
    observed_slots: set[datetime] = set()
    observed_by_source: dict[str, dict[datetime, bool]] = {"dart": {}, "kind": {}}
    for row in scheduled:
        slot = datetime.fromisoformat(str(row["scheduled_slot_at"]).replace("Z", "+00:00"))
        if slot not in expected:
            continue
        observed_slots.add(slot)
        source_key = {part.strip() for part in str(row.get("source_key") or "").split("+")}
        outcomes = row.get("source_outcomes")
        assert isinstance(outcomes, dict)
        for source in ("dart", "kind"):
            outcome = outcomes.get(source)
            if source not in source_key or not isinstance(outcome, dict):
                continue
            status = str(outcome.get("status") or "").casefold()
            if status == "disabled":
                continue
            observed_by_source[source][slot] = status in {"success", "succeeded"}
    summary["scheduled_run_count"] = len(scheduled)
    summary["claimed_slot_count"] = len(observed_slots)
    summary["late_claim_count"] = sum(row.get("slot_claim_late") is True for row in scheduled)
    summary["incomplete_claim_count"] = sum(
        row.get("slot_claim_status") != "completed" for row in scheduled
    )
    summary["terminal_failure_count"] = sum(
        row.get("slot_claim_terminal_reason") not in (None, "") for row in scheduled
    )
    summary["observed_slot_count"] = len(observed_slots)
    summary["missing_slot_count"] = len(expected - observed_slots)
    for source in ("dart", "kind"):
        observed = observed_by_source[source]
        missing = len(expected - set(observed))
        failed = sum(not succeeded for succeeded in observed.values())
        summary[f"{source}_succeeded_count"] = len(expected) - missing - failed
        summary[f"{source}_missing_count"] = missing
        summary[f"{source}_failed_count"] = failed


def _set_ingest_counts(
    payload: dict[str, object], day: date, *, dart_successes: int, kind_successes: int
) -> None:
    operations = payload["operations_days"]
    assert isinstance(operations, list)
    row = next(item for item in operations if item["observation_date"] == day.isoformat())
    counts = row["raw_counts"]
    assert isinstance(counts, dict)
    counts["official_ingest_expected_count"] = 164
    counts["official_ingest_succeeded_count"] = dart_successes + kind_successes
    counts["dart_ingest_expected_count"] = 82
    counts["dart_ingest_succeeded_count"] = dart_successes
    counts["kind_ingest_expected_count"] = 82
    counts["kind_ingest_succeeded_count"] = kind_successes


def _api_export() -> dict[str, object]:
    latest_seven = [THROUGH - timedelta(days=offset) for offset in range(6, -1, -1)]
    shadow_days = [THROUGH - timedelta(days=offset) for offset in range(13, -1, -1)]
    collection_runs = []
    official_run_ledger = []
    availability_groups = []
    vital_groups = []
    operations_days = []
    web_distribution_days = []
    for day in latest_seven:
        official_run_ledger.extend(_official_run_ledger(day))
        collection_runs.append(
            {
                "observation_date": day.isoformat(),
                "source_key": "dart+kind",
                "code_revision": REVISION,
                "attempt_count": 1,
                "success_count": 1,
                "raw_count": 10,
                "acknowledged_count": 8,
                "first_observed_at": f"{day.isoformat()} 00:01:00",
                "last_finished_at": f"{day.isoformat()} 23:46:00",
            }
        )
        operations_days.append(
            {
                "observation_date": day.isoformat(),
                "code_revision": REVISION,
                "dart_success_poll_interval_p95_minutes": 15.0,
                "kind_observation_lag_p95_minutes": 30.0,
                "kind_observation_count": 4,
                "kind_lag_sample_count": 4,
                "content_snapshot_at": f"{day.isoformat()}T14:59:59Z",
                "content_scope": "governance_corpus_2021_plus_kst_day_end_v2",
                "content_metric_assignment": "immutable_quality_observation",
                "quality_observation_id": f"quality:{day.isoformat()}",
                "quality_payload_sha256": "d" * 64,
                "raw_counts": _quality_counts(),
            }
        )
        web_distribution_days.append(
            {
                "observation_date": day.isoformat(),
                "code_revision": REVISION,
                "raw_attempt_count": 1,
                "raw_success_count": 1,
                "raw_failure_count": 0,
                "success_rate_denominator": 1,
                "success_rate": 1.0,
                "distribution_targets": ["pages"],
                "duration_ms_p95": 1000.0,
                "failure_detection_seconds_p95": None,
            }
        )
        for route in AVAILABILITY_ROUTES:
            availability_groups.append(
                {
                    "observation_date": day.isoformat(),
                    "route_template": route,
                    "build_sha": REVISION,
                    "raw_attempt_count": 288,
                    "raw_success_count": 288,
                    "raw_failure_count": 0,
                    "success_rate_denominator": 288,
                    "success_rate": 1.0,
                    "duration_ms_p95": 250.0,
                    "cadence_id": "watchdog-v1-kst-5m-minute01",
                    "expected_slot_count": 288,
                    "covered_slot_count": 288,
                    "missing_slot_count": 0,
                    "duplicate_slot_count": 0,
                    "off_cadence_count": 0,
                    "covered_slots_bitmap_hex": "f" * 72,
                    "first_observed_at": f"{day.isoformat()}T00:01:00+09:00",
                    "last_observed_at": f"{day.isoformat()}T23:56:00+09:00",
                    "actual_interval_seconds_p95": 300.0,
                    "actual_max_gap_seconds": 300.0,
                    "observation_interval_seconds_p95": 300.0,
                }
            )
        for route in WEB_VITAL_ROUTES:
            for metric, value in (("lcp", 2100.0), ("inp", 175.0), ("cls", 0.05)):
                vital_groups.append(
                    {
                        "observation_date": day.isoformat(),
                        "route_template": route,
                        "metric_name": metric,
                        "device_class": "mobile",
                        "build_sha": REVISION,
                        "sample_count": 5,
                        "p75": value,
                    }
                )
    ledger_bytes = "".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        for row in official_run_ledger
    ).encode("utf-8")
    expected_slots = len(latest_seven) * 82
    return {
        "ok": True,
        "evidence_source": "production_db_export",
        "is_synthetic": False,
        "distribution_mode": "web_only",
        "range": {
            "from": (THROUGH - timedelta(days=13)).isoformat(),
            "to": THROUGH.isoformat(),
        },
        "generated_at": "2026-07-14T15:35:00Z",
        "schema_version": 7,
        "release_state": "preview",
        "code_revisions": [REVISION],
        "collection_runs": collection_runs,
        "official_run_ledger": official_run_ledger,
        "official_schedule": {
            "contract_version": 1,
            "timezone": "Asia/Seoul",
            "cadence_id": "official-v1-82-slots",
            "from": latest_seven[0].isoformat(),
            "to": latest_seven[-1].isoformat(),
            "expected_slot_count": expected_slots,
            "ledger_row_count": len(official_run_ledger),
            "ledger_sha256": hashlib.sha256(ledger_bytes).hexdigest(),
            "scheduled_run_count": expected_slots,
            "claimed_slot_count": expected_slots,
            "late_claim_count": 0,
            "incomplete_claim_count": 0,
            "terminal_failure_count": 0,
            "observed_slot_count": expected_slots,
            "missing_slot_count": 0,
            "duplicate_slot_count": 0,
            "invalid_scheduled_run_count": 0,
            "invalid_run_metadata_count": 0,
            "dart_expected_count": expected_slots,
            "dart_succeeded_count": expected_slots,
            "dart_missing_count": 0,
            "dart_failed_count": 0,
            "kind_expected_count": expected_slots,
            "kind_succeeded_count": expected_slots,
            "kind_missing_count": 0,
            "kind_failed_count": 0,
        },
        "availability": {
            "raw_attempt_count": len(availability_groups) * 288,
            "raw_success_count": len(availability_groups) * 288,
            "raw_failure_count": 0,
            "success_rate_denominator": len(availability_groups) * 288,
            "success_rate": 1.0,
            "daily_route_build_counts": availability_groups,
        },
        "web_vitals": {
            "raw_sample_count": len(vital_groups) * 5,
            "groups": vital_groups,
        },
        "shadow_discrepancies": [],
        "shadow_days": [
            {
                "observation_date": day.isoformat(),
                "code_revision": REVISION,
                "legacy_status": "succeeded",
                "candidate_status": "succeeded",
                "legacy_events": [
                    {"comparison_key": f"eventcmp:v1:{event_index:064x}"}
                    for event_index in range(1, index + 1)
                ],
                "candidate_events": [
                    {"comparison_key": f"eventcmp:v1:{event_index:064x}"}
                    for event_index in range(1, index + 1)
                ],
                "legacy_crosswalk": {
                    "schema_version": 1,
                    "eligible_legacy_record_count": index,
                    "crosswalked_legacy_record_count": index,
                    "unmatched_legacy_record_count": 0,
                    "ambiguous_legacy_record_count": 0,
                    "coverage_rate": 1.0,
                    "crosswalk_sha256": hashlib.sha256(
                        f"legacy-crosswalk:{day.isoformat()}".encode()
                    ).hexdigest(),
                },
                "review_status_counts": {},
            }
            for index, day in enumerate(shadow_days, start=1)
        ],
        "operations_days": operations_days,
        "web_distribution_days": web_distribution_days,
    }


def test_builds_exact_six_files_from_actual_counts(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    human_raw = _write_human(human_dir)
    output = tmp_path / "output"

    report = build_evidence_inputs(
        api_export=_api_export(),
        human_dir=human_dir,
        output_dir=output,
        expected_revision=REVISION,
        through_date=THROUGH,
    )

    assert report["status"] == "release-evidence-inputs-ready"
    assert {path.name for path in output.iterdir()} == set(EVIDENCE_FILES)
    assert (output / "benchmark.json").read_bytes() == human_raw["benchmark.json"]
    operations = [json.loads(line) for line in (output / "operations.jsonl").read_text().splitlines()]
    assert len(operations) == 7
    metrics = operations[-1]["metrics"]
    assert metrics["distribution_mode"] == "web_only"
    assert metrics["telegram_delivery_attempted_count"] == 0
    assert metrics["dart_ingest_success_rate"] == 1.0
    assert metrics["kind_ingest_success_rate"] == 1.0
    assert metrics["dart_success_poll_interval_p95_minutes"] == 15.0
    assert metrics["kind_observation_lag_p95_minutes"] == 30.0
    assert metrics["official_lag_p95_minutes"] == 30.0
    assert metrics["raw_counts"]["dart_ingest_expected_count"] == 82
    assert metrics["raw_counts"]["kind_ingest_succeeded_count"] == 82
    assert metrics["raw_counts"]["official_scheduled_slot_count"] == 82
    performance = [
        json.loads(line) for line in (output / "performance.jsonl").read_text().splitlines()
    ]
    assert performance[-1]["metrics"]["mobile_lcp_p75_seconds"] == 2.1
    assert performance[-1]["metrics"]["availability_cadence_id"] == (
        "watchdog-v1-kst-5m-minute01"
    )
    assert performance[-1]["metrics"]["availability_actual_max_gap_seconds"] == 300.0
    assert performance[-1]["metrics"]["availability_coverage_rate"] == 1.0
    assert performance[-1]["metrics"]["raw_counts"]["availability_expected_slot_count"] == 1152
    assert performance[-1]["metrics"]["raw_counts"]["availability_covered_slot_count"] == 1152
    assert performance[-1]["metrics"]["raw_counts"]["mobile_lcp_sample_count"] == 20
    shadow = [json.loads(line) for line in (output / "shadow.jsonl").read_text().splitlines()]
    for builder, records in (
        (build_shadow_comparison, shadow),
        (build_performance_gates, performance),
    ):
        _report, gates, _revisions, _days = builder(records, GateThresholds())
        assert all(gate["passed"] for gate in gates)


def test_true_kind_no_disclosure_day_is_exported_as_null_lag(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    operations = payload["operations_days"]
    assert isinstance(operations, list)
    operations[0]["kind_observation_count"] = 0
    operations[0]["kind_lag_sample_count"] = 0
    operations[0]["kind_observation_lag_p95_minutes"] = None
    build_evidence_inputs(
        api_export=payload,
        human_dir=human_dir,
        output_dir=tmp_path / "output",
        expected_revision=REVISION,
        through_date=THROUGH,
    )
    first = json.loads((tmp_path / "output" / "operations.jsonl").read_text().splitlines()[0])
    assert first["metrics"]["kind_observation_lag_p95_minutes"] is None
    assert first["metrics"]["raw_counts"]["kind_observation_count"] == 0


def test_shadow_no_disclosure_day_reuses_actual_nonempty_cumulative_corpus(
    tmp_path: Path,
) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    shadow_days = payload["shadow_days"]
    assert isinstance(shadow_days, list)
    previous = shadow_days[4]
    no_disclosure = shadow_days[5]
    no_disclosure["legacy_events"] = list(previous["legacy_events"])
    no_disclosure["candidate_events"] = list(previous["candidate_events"])
    no_disclosure["legacy_crosswalk"] = dict(previous["legacy_crosswalk"])

    build_evidence_inputs(
        api_export=payload,
        human_dir=human_dir,
        output_dir=tmp_path / "output",
        expected_revision=REVISION,
        through_date=THROUGH,
    )
    records = [
        json.loads(line)
        for line in (tmp_path / "output" / "shadow.jsonl").read_text().splitlines()
    ]
    assert records[5]["legacy_run"]["events"] == records[4]["legacy_run"]["events"]
    assert records[5]["candidate_run"]["events"] == records[4]["candidate_run"]["events"]


def test_shadow_cumulative_corpus_regression_is_rejected(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    shadow_days = payload["shadow_days"]
    assert isinstance(shadow_days, list)
    shadow_days[6]["legacy_events"] = list(shadow_days[4]["legacy_events"])
    with pytest.raises(EvidenceInputError, match="legacy cumulative corpus regressed"):
        build_evidence_inputs(
            api_export=payload,
            human_dir=human_dir,
            output_dir=tmp_path / "output",
            expected_revision=REVISION,
            through_date=THROUGH,
        )


def test_accepts_production_api_flat_operations_and_nested_shadow_contract(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    operations = payload["operations_days"]
    shadow = payload["shadow_days"]
    assert isinstance(operations, list) and isinstance(shadow, list)
    for operation in operations:
        counts = operation.pop("raw_counts")
        operation.update(counts)
        operation["dart_expected_count"] = operation.pop("dart_ingest_expected_count")
        operation["dart_succeeded_count"] = operation.pop("dart_ingest_succeeded_count")
        operation["kind_expected_count"] = operation.pop("kind_ingest_expected_count")
        operation["kind_succeeded_count"] = operation.pop("kind_ingest_succeeded_count")
        operation["dart_success_poll_interval_seconds_p95"] = (
            operation.pop("dart_success_poll_interval_p95_minutes") * 60
        )
        operation["kind_first_observed_lag_seconds_p95"] = (
            operation.pop("kind_observation_lag_p95_minutes") * 60
        )
        operation["dart_raw_count"] = 10
        operation["dart_acknowledged_count"] = 8
        operation["kind_raw_count"] = 4
        operation["kind_acknowledged_count"] = 4
    for day in shadow:
        legacy_keys = [event["comparison_key"] for event in day.pop("legacy_events")]
        candidate_keys = [event["comparison_key"] for event in day.pop("candidate_events")]
        day["legacy_run"] = {
            "status": day.pop("legacy_status"),
            "comparison_keys": legacy_keys,
            "event_count": len(legacy_keys),
            "events_sha256": hashlib.sha256(
                json.dumps(legacy_keys, separators=(",", ":")).encode()
            ).hexdigest(),
        }
        day["candidate_run"] = {
            "status": day.pop("candidate_status"),
            "comparison_keys": candidate_keys,
            "event_count": len(candidate_keys),
            "events_sha256": hashlib.sha256(
                json.dumps(candidate_keys, separators=(",", ":")).encode()
            ).hexdigest(),
        }
        day["discrepancies_reviewed"] = True
    payload["shadow_discrepancies"] = {"review_status_counts": {}}

    build_evidence_inputs(
        api_export=payload,
        human_dir=human_dir,
        output_dir=tmp_path / "output",
        expected_revision=REVISION,
        through_date=THROUGH,
    )
    operations_output = [
        json.loads(line)
        for line in (tmp_path / "output" / "operations.jsonl").read_text().splitlines()
    ]
    assert operations_output[-1]["metrics"]["kind_observation_lag_p95_minutes"] == 30.0
    assert operations_output[-1]["metrics"]["raw_counts"]["kind_ingest_expected_count"] == 82


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (
            lambda payload: payload["shadow_days"].pop(),  # type: ignore[union-attr]
            "missing required dates",
        ),
        (
            lambda payload: payload.update(code_revisions=["b" * 40]),
            "does not match expected revision",
        ),
        (
            lambda payload: payload["operations_days"].pop(),  # type: ignore[union-attr]
            "missing required date",
        ),
        (
            lambda payload: payload["operations_days"][0].update(  # type: ignore[index,union-attr]
                {"content_scope": "governance_corpus_2021_plus_kst_day_end_v1"}
            ),
            "invalid content_scope",
        ),
    ],
)
def test_missing_days_mixed_sha_and_missing_quality_fail_closed(
    tmp_path: Path, mutate, message: str
) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    mutate(payload)
    with pytest.raises(EvidenceInputError, match=message):
        build_evidence_inputs(
            api_export=payload,
            human_dir=human_dir,
            output_dir=tmp_path / "output",
            expected_revision=REVISION,
            through_date=THROUGH,
        )


def test_zero_denominator_is_rejected(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    availability = payload["availability"]
    assert isinstance(availability, dict)
    groups = availability["daily_route_build_counts"]
    assert isinstance(groups, list)
    groups[0]["raw_attempt_count"] = 0
    groups[0]["success_rate_denominator"] = 0
    with pytest.raises(EvidenceInputError, match="must be non-zero"):
        build_evidence_inputs(
            api_export=payload,
            human_dir=human_dir,
            output_dir=tmp_path / "output",
            expected_revision=REVISION,
            through_date=THROUGH,
        )


def test_duplicate_observation_cannot_hide_a_missing_watchdog_slot(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    availability = payload["availability"]
    assert isinstance(availability, dict)
    groups = availability["daily_route_build_counts"]
    assert isinstance(groups, list)
    group = groups[0]
    group["covered_slot_count"] = 287
    group["missing_slot_count"] = 1
    group["duplicate_slot_count"] = 1
    group["covered_slots_bitmap_hex"] = "f" * 71 + "e"

    with pytest.raises(EvidenceInputError, match="complete cadence coverage"):
        build_evidence_inputs(
            api_export=payload,
            human_dir=human_dir,
            output_dir=tmp_path / "output",
            expected_revision=REVISION,
            through_date=THROUGH,
        )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("covered_slots_bitmap_hex", "0" * 72, "bitmap popcount"),
        ("actual_interval_seconds_p95", 601.0, "interval p95 exceeds 600"),
        ("actual_max_gap_seconds", 601.0, "maximum gap exceeds 600"),
        ("first_observed_at", "2026-07-08T00:00:59+09:00", "KST 00:01"),
    ),
)
def test_availability_cadence_tampering_fails_closed(
    tmp_path: Path, field: str, value: object, message: str
) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    availability = payload["availability"]
    assert isinstance(availability, dict)
    groups = availability["daily_route_build_counts"]
    assert isinstance(groups, list)
    groups[0][field] = value
    if field == "actual_interval_seconds_p95":
        groups[0]["observation_interval_seconds_p95"] = value

    with pytest.raises(EvidenceInputError, match=message):
        build_evidence_inputs(
            api_export=payload,
            human_dir=human_dir,
            output_dir=tmp_path / "output",
            expected_revision=REVISION,
            through_date=THROUGH,
        )


def test_availability_last_slot_accepts_the_next_kst_midnight_minute(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    availability = payload["availability"]
    assert isinstance(availability, dict)
    groups = availability["daily_route_build_counts"]
    assert isinstance(groups, list)
    first_group = groups[0]
    group_day = date.fromisoformat(first_group["observation_date"])
    first_group["last_observed_at"] = (
        f"{(group_day + timedelta(days=1)).isoformat()}T00:00:59+09:00"
    )

    build_evidence_inputs(
        api_export=payload,
        human_dir=human_dir,
        output_dir=tmp_path / "output",
        expected_revision=REVISION,
        through_date=THROUGH,
    )


def test_availability_off_cadence_count_cannot_be_ignored(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    availability = payload["availability"]
    assert isinstance(availability, dict)
    groups = availability["daily_route_build_counts"]
    assert isinstance(groups, list)
    group = groups[0]
    group["raw_attempt_count"] += 1
    group["raw_success_count"] += 1
    group["success_rate_denominator"] += 1
    group["off_cadence_count"] = 1
    availability["raw_attempt_count"] += 1
    availability["raw_success_count"] += 1
    availability["success_rate_denominator"] += 1

    with pytest.raises(EvidenceInputError, match="complete cadence coverage"):
        build_evidence_inputs(
            api_export=payload,
            human_dir=human_dir,
            output_dir=tmp_path / "output",
            expected_revision=REVISION,
            through_date=THROUGH,
        )


def test_closed_release_state_cannot_create_cutover_evidence(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    payload["release_state"] = "closed"
    with pytest.raises(EvidenceInputError, match="release_state must be preview or live"):
        build_evidence_inputs(
            api_export=payload,
            human_dir=human_dir,
            output_dir=tmp_path / "output",
            expected_revision=REVISION,
            through_date=THROUGH,
        )


def test_web_vitals_require_html_journeys_not_xml_or_health_routes(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    vitals = payload["web_vitals"]
    assert isinstance(vitals, dict) and isinstance(vitals["groups"], list)
    for group in vitals["groups"]:
        if group["route_template"] == "/today":
            group["route_template"] = "/feed.xml"
    with pytest.raises(EvidenceInputError, match="missing mobile lcp for /today"):
        build_evidence_inputs(
            api_export=payload,
            human_dir=human_dir,
            output_dir=tmp_path / "output",
            expected_revision=REVISION,
            through_date=THROUGH,
        )


def test_unreviewed_shadow_discrepancy_is_rejected(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    first_day = (THROUGH - timedelta(days=13)).isoformat()
    discrepancies = payload["shadow_discrepancies"]
    shadow_days = payload["shadow_days"]
    assert isinstance(discrepancies, list) and isinstance(shadow_days, list)
    discrepancies.append(
        {
            "observation_date": first_day,
            "code_revision": REVISION,
            "review_status": "pending",
            "raw_count": 1,
        }
    )
    shadow_days[0]["review_status_counts"] = {"pending": 1}
    with pytest.raises(EvidenceInputError, match="unreviewed discrepancies"):
        build_evidence_inputs(
            api_export=payload,
            human_dir=human_dir,
            output_dir=tmp_path / "output",
            expected_revision=REVISION,
            through_date=THROUGH,
        )


def test_shadow_day_without_lossless_nonempty_legacy_crosswalk_is_rejected(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    shadow_days = payload["shadow_days"]
    assert isinstance(shadow_days, list)
    shadow_days[0]["legacy_crosswalk"]["unmatched_legacy_record_count"] = 1
    with pytest.raises(EvidenceInputError, match="incomplete legacy crosswalk"):
        build_evidence_inputs(
            api_export=payload,
            human_dir=human_dir,
            output_dir=tmp_path / "output",
            expected_revision=REVISION,
            through_date=THROUGH,
        )


def test_dart_only_collection_creates_failing_fixed_denominator_evidence(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    runs = payload["official_run_ledger"]
    assert isinstance(runs, list)
    for run in runs:
        run["source_key"] = "dart"
        run["raw_count"] = 5
        run["acknowledged_count"] = 5
        run["source_outcomes"] = {
            "dart": {"status": "succeeded", "raw_count": 5, "acknowledged_count": 5}
        }
    for offset in range(7):
        _set_ingest_counts(
            payload,
            THROUGH - timedelta(days=offset),
            dart_successes=82,
            kind_successes=0,
        )
    _refresh_ledger_identity(payload)
    build_evidence_inputs(
        api_export=payload,
        human_dir=human_dir,
        output_dir=tmp_path / "output",
        expected_revision=REVISION,
        through_date=THROUGH,
    )
    operations = [
        json.loads(line)
        for line in (tmp_path / "output" / "operations.jsonl").read_text().splitlines()
    ]
    assert all(row["metrics"]["kind_ingest_success_rate"] == 0.0 for row in operations)
    _report, gates, _revisions, _days = build_operations_gates(operations, GateThresholds())
    assert any(
        gate["name"].endswith(".kind_ingest_success_rate") and not gate["passed"]
        for gate in gates
    )


def test_missing_scheduled_slot_cannot_hide_outside_observed_run_denominator(
    tmp_path: Path,
) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    ledger = payload["official_run_ledger"]
    assert isinstance(ledger, list)
    removed = ledger.pop()
    removed_slot = datetime.fromisoformat(str(removed["scheduled_slot_at"]))
    removed_day = removed_slot.astimezone(ZoneInfo("Asia/Seoul")).date()
    _set_ingest_counts(payload, removed_day, dart_successes=81, kind_successes=81)
    _refresh_ledger_identity(payload)

    build_evidence_inputs(
        api_export=payload,
        human_dir=human_dir,
        output_dir=tmp_path / "output",
        expected_revision=REVISION,
        through_date=THROUGH,
    )
    operations = [
        json.loads(line)
        for line in (tmp_path / "output" / "operations.jsonl").read_text().splitlines()
    ]
    affected = next(row for row in operations if row["date"] == removed_day.isoformat())
    assert affected["metrics"]["dart_ingest_success_rate"] == 81 / 82
    assert affected["metrics"]["raw_counts"]["dart_ingest_expected_count"] == 82
    _report, gates, _revisions, _days = build_operations_gates(operations, GateThresholds())
    assert any(
        gate["name"] == f"operations.{removed_day.isoformat()}.dart_ingest_success_rate"
        and not gate["passed"]
        for gate in gates
    )


def test_scheduled_source_counts_must_reconcile_with_run_totals(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    ledger = payload["official_run_ledger"]
    assert isinstance(ledger, list)
    ledger[0]["source_outcomes"]["dart"]["raw_count"] = 4
    ledger[0]["source_outcomes"]["dart"]["acknowledged_count"] = 4

    with pytest.raises(EvidenceInputError, match="do not reconcile with the run totals"):
        build_evidence_inputs(
            api_export=payload,
            human_dir=human_dir,
            output_dir=tmp_path / "output",
            expected_revision=REVISION,
            through_date=THROUGH,
        )


def test_scheduled_slot_must_match_the_claimed_cron_family(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    ledger = payload["official_run_ledger"]
    assert isinstance(ledger, list)
    ledger[0]["event_schedule"] = "0,15,30,45 0-14 * * *"

    with pytest.raises(EvidenceInputError, match="does not belong to event_schedule"):
        build_evidence_inputs(
            api_export=payload,
            human_dir=human_dir,
            output_dir=tmp_path / "output",
            expected_revision=REVISION,
            through_date=THROUGH,
        )


def test_scheduled_trigger_lag_must_match_immutable_claim_provenance(
    tmp_path: Path,
) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    ledger = payload["official_run_ledger"]
    assert isinstance(ledger, list)
    slot = datetime.fromisoformat(str(ledger[20]["scheduled_slot_at"]))
    ledger[20]["trigger_created_at"] = (slot + timedelta(seconds=31)).isoformat()
    _refresh_ledger_identity(payload)

    with pytest.raises(EvidenceInputError, match="trigger lag does not match"):
        build_evidence_inputs(
            api_export=payload,
            human_dir=human_dir,
            output_dir=tmp_path / "output",
            expected_revision=REVISION,
            through_date=THROUGH,
        )


def test_duplicate_scheduled_slot_remains_structurally_fatal(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    ledger = payload["official_run_ledger"]
    assert isinstance(ledger, list)
    duplicate = dict(ledger[0])
    duplicate["run_id"] = "run:duplicate"
    duplicate["slot_claim_id"] = "official-slot:duplicate"
    duplicate["github_run_id"] = "999999999999"
    duplicate["source_outcomes"] = dict(ledger[0]["source_outcomes"])
    ledger.append(duplicate)
    _refresh_ledger_identity(payload)

    with pytest.raises(EvidenceInputError, match="duplicate scheduled DART slot"):
        build_evidence_inputs(
            api_export=payload,
            human_dir=human_dir,
            output_dir=tmp_path / "output",
            expected_revision=REVISION,
            through_date=THROUGH,
        )


def test_manual_backfill_and_company_master_rows_never_inflate_schedule_rates(
    tmp_path: Path,
) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    payload = _api_export()
    ledger = payload["official_run_ledger"]
    assert isinstance(ledger, list)
    for index, run_kind in enumerate(("manual", "backfill", "company_master")):
        ledger.append(
            {
                "run_id": f"run:excluded:{index}",
                "pipeline": "ingest-official",
                "source_key": "dart+kind",
                "code_revision": REVISION,
                "status": "succeeded",
                "started_at": "2026-07-14T01:00:00+00:00",
                "finished_at": "2026-07-14T01:01:00+00:00",
                "raw_count": 999,
                "acknowledged_count": 999,
                "run_kind": run_kind,
                "event_schedule": None,
                "scheduled_slot_at": None,
                "company_master_sync": run_kind == "company_master",
                "source_outcomes": {
                    "dart": {"status": "succeeded"},
                    "kind": {"status": "succeeded"},
                },
            }
        )
    _refresh_ledger_identity(payload)

    build_evidence_inputs(
        api_export=payload,
        human_dir=human_dir,
        output_dir=tmp_path / "output",
        expected_revision=REVISION,
        through_date=THROUGH,
    )
    operations = [
        json.loads(line)
        for line in (tmp_path / "output" / "operations.jsonl").read_text().splitlines()
    ]
    assert operations[-1]["metrics"]["raw_counts"]["official_ingest_expected_count"] == 164


def test_human_artifact_must_have_exact_files_and_same_revision(tmp_path: Path) -> None:
    human_dir = tmp_path / "human"
    _write_human(human_dir)
    (human_dir / "notes.txt").write_text("private notes must not be uploaded")
    with pytest.raises(EvidenceInputError, match="exactly three files"):
        validate_human_evidence(human_dir, REVISION)

    (human_dir / "notes.txt").unlink()
    approval = json.loads((human_dir / "release-approval.json").read_text())
    approval["approved_revision"] = "b" * 40
    (human_dir / "release-approval.json").write_text(json.dumps(approval))
    with pytest.raises(EvidenceInputError, match="does not match expected revision"):
        validate_human_evidence(human_dir, REVISION)


def test_materializes_one_use_environment_secrets_without_logging_values(tmp_path: Path) -> None:
    reports = _human_reports()
    environment = {
        "GOVERNANCE_BENCHMARK_EVIDENCE_B64": base64.b64encode(
            json.dumps(reports["benchmark.json"]).encode()
        ).decode(),
        "GOVERNANCE_USABILITY_EVIDENCE_B64": base64.b64encode(
            json.dumps(reports["usability.json"]).encode()
        ).decode(),
        "GOVERNANCE_RELEASE_APPROVAL_B64": base64.b64encode(
            json.dumps(reports["release-approval.json"]).encode()
        ).decode(),
    }
    output = tmp_path / "human"
    result = materialize_human_secrets(
        output_dir=output, expected_revision=REVISION, environment=environment
    )
    assert set(result) == set(reports)
    assert {path.name for path in output.iterdir()} == set(reports)


def test_api_url_requires_query_free_https_v1_base() -> None:
    assert _api_url(
        "https://alignpe.gabia.io/activist/api.php/api/v1",
        from_date=date(2026, 7, 1),
        to_date=date(2026, 7, 14),
    ).endswith("/ops/release-evidence?from=2026-07-01&to=2026-07-14")
    with pytest.raises(EvidenceInputError, match="query-free HTTPS"):
        _api_url(
            "http://localhost/api/v1?token=bad",
            from_date=date(2026, 7, 1),
            to_date=date(2026, 7, 14),
        )
    with pytest.raises(EvidenceInputError, match="query-free HTTPS"):
        _api_url(
            "https://user:password@example.test/api/v1",
            from_date=date(2026, 7, 1),
            to_date=date(2026, 7, 14),
        )


def test_official_run_ledger_fetches_every_page_and_verifies_digest(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    rows = [
        {"run_id": "run:1", "run_kind": "manual"},
        {"run_id": "run:2", "run_kind": "company_master"},
    ]
    raw = "".join(
        json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
        for row in rows
    ).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()

    class Response:
        headers: dict[str, str] = {}

        def __init__(self, payload: dict[str, object]) -> None:
            self.payload = payload

        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *_args):  # type: ignore[no-untyped-def]
            return None

        def read(self, _limit: int) -> bytes:
            return json.dumps(self.payload, separators=(",", ":")).encode()

    calls = 0

    def fake_open(request, **_kwargs):  # type: ignore[no-untyped-def]
        nonlocal calls
        calls += 1
        has_more = calls == 1
        return Response(
            {
                "ok": True,
                "range": {"from": "2026-07-08", "to": "2026-07-14"},
                "ledger_row_count": 2,
                "ledger_sha256": digest,
                "data": [rows[calls - 1]],
                "pagination": {
                    "limit": 100,
                    "returned": 1,
                    "has_more": has_more,
                    "next_cursor": "next-page" if has_more else None,
                },
            }
        )

    monkeypatch.setattr(release_evidence_inputs, "urlopen", fake_open)
    fetched, contract = fetch_official_run_ledger(
        base_url="https://api.example.test/api/v1",
        ops_token="x" * 32,
        from_date=date(2026, 7, 8),
        to_date=date(2026, 7, 14),
    )

    assert calls == 2
    assert fetched == rows
    assert contract == {"ledger_row_count": 2, "ledger_sha256": digest}


def test_official_run_ledger_rejects_digest_mismatch(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    class Response:
        headers: dict[str, str] = {}

        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *_args):  # type: ignore[no-untyped-def]
            return None

        def read(self, _limit: int) -> bytes:
            return json.dumps(
                {
                    "ok": True,
                    "range": {"from": "2026-07-08", "to": "2026-07-14"},
                    "ledger_row_count": 1,
                    "ledger_sha256": "f" * 64,
                    "data": [{"run_id": "run:1"}],
                    "pagination": {
                        "limit": 100,
                        "returned": 1,
                        "has_more": False,
                        "next_cursor": None,
                    },
                }
            ).encode()

    monkeypatch.setattr(release_evidence_inputs, "urlopen", lambda *_args, **_kwargs: Response())
    with pytest.raises(EvidenceInputError, match="digest mismatch"):
        fetch_official_run_ledger(
            base_url="https://api.example.test/api/v1",
            ops_token="x" * 32,
            from_date=date(2026, 7, 8),
            to_date=date(2026, 7, 14),
        )


def test_workflows_are_same_sha_protected_and_ordered() -> None:
    producer_path = ROOT / ".github" / "workflows" / "release-evidence-inputs.yml"
    human_path = ROOT / ".github" / "workflows" / "human-evidence-artifact.yml"
    exporter_path = ROOT / ".github" / "workflows" / "release-evidence.yml"
    producer_text = producer_path.read_text(encoding="utf-8")
    human_text = human_path.read_text(encoding="utf-8")
    producer = yaml.load(producer_text, Loader=yaml.BaseLoader)
    human = yaml.load(human_text, Loader=yaml.BaseLoader)

    assert producer["on"]["schedule"] == [{"cron": "35 15 * * *"}]
    assert "45 15 * * *" in exporter_path.read_text(encoding="utf-8")
    assert producer["permissions"] == {"actions": "read", "contents": "read"}
    assert "REQUIRED_WORKFLOW_PATH: .github/workflows/human-evidence-artifact.yml" in producer_text
    assert "run.head_sha" in producer_text
    assert "run.head_branch" in producer_text
    assert "artifact.digest" in producer_text
    assert "governance-release-evidence-inputs" in producer_text
    assert "secrets.BSIDE_OPS_TOKEN" in producer_text
    assert "TELEGRAM_BOT_TOKEN" not in producer_text
    assert "TELEGRAM_CHAT_ID" not in producer_text
    assert human["on"] == {"workflow_dispatch": {"inputs": human["on"]["workflow_dispatch"]["inputs"]}}
    assert human["jobs"]["upload"]["environment"]["name"] == "governance-release"
    assert "governance-human-evidence" in human_text
    assert "PERSONAL_ACCESS_TOKEN" not in human_text
    assert "git push" not in human_text
