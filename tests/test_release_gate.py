from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from curator.release_gate import ReleaseEvidenceError, build_release_gate_report, main


REVISION = "a" * 40


def provenance(day: date, *, synthetic: bool = False, revision: str = REVISION) -> dict[str, object]:
    return {
        "schema_version": 1,
        "date": day.isoformat(),
        "environment": "production",
        "evidence_source": "mysql_export",
        "is_synthetic": synthetic,
        "collected_at": f"{day.isoformat()}T23:59:00+09:00",
        "code_revision": revision,
    }


def shadow_records(start: date = date(2026, 7, 1)) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for offset in range(14):
        day = start + timedelta(days=offset)
        common = {"comparison_key": f"corp:event:{offset}"}
        legacy_events = [common]
        candidate_events = [common]
        if offset == 4:
            candidate_events.append({"comparison_key": "corp:new:reviewed"})
        records.append(
            {
                **provenance(day),
                "legacy_run": {"status": "succeeded", "events": legacy_events},
                "candidate_run": {"status": "succeeded", "events": candidate_events},
                "discrepancies_reviewed": True,
            }
        )
    return records


def operations_records(start: date = date(2026, 7, 8)) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for offset in range(7):
        day = start + timedelta(days=offset)
        records.append(
            {
                **provenance(day),
                "metrics": {
                    "official_ingest_success_rate": 0.999,
                    "official_lag_p95_minutes": 30.0,
                    "delivery_success_rate": 0.999,
                    "delivery_failure_detection_p95_minutes": 5.0,
                    "official_evidence_link_rate": 0.98,
                    "same_story_precision": 0.98,
                    "same_story_evaluated_pair_count": 20,
                    "top_sensitive_human_review_rate": 1.0,
                    "original_language_preservation_rate": 1.0,
                    "valid_source_right_rate": 1.0,
                },
            }
        )
    return records


def performance_records(start: date = date(2026, 7, 8)) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for offset in range(7):
        day = start + timedelta(days=offset)
        record = provenance(day)
        record["evidence_source"] = "production_rum_export"
        record["metrics"] = {
            "availability_rate": 0.9999,
            "mobile_lcp_p75_seconds": 2.1,
            "mobile_inp_p75_ms": 175.0,
            "mobile_cls_p75": 0.05,
        }
        records.append(record)
    return records


def benchmark_report(*, synthetic: bool = False, revision: str = REVISION) -> dict[str, object]:
    return {
        "schema_version": 1,
        "evaluated_at": "2026-07-14T15:00:00+00:00",
        "release_gate_passed": True,
        "failed_gates": [],
        "evidence": {
            "schema_version": 1,
            "environment": "production",
            "evidence_source": "fixture" if synthetic else "human_labeled_jsonl",
            "is_synthetic": synthetic,
            "collected_at": "2026-07-14T15:00:00+00:00",
            "code_revision": revision,
            "release_eligible": not synthetic,
            "same_story_label_sources": ["human"],
            "relevance_label_sources": ["human", "adjudicated"],
            "same_story_dataset_sha256": "b" * 64,
            "relevance_dataset_sha256": "c" * 64,
        },
        "thresholds": {
            "min_article_pairs": 500,
            "min_events": 300,
            "same_story_min_precision": 0.97,
            "relevance_min_recall": 0.95,
        },
        "same_story": {
            "sample_count": 500,
            "actual_positive": 250,
            "actual_negative": 250,
            "precision": 0.98,
        },
        "relevance": {
            "unique_event_count": 300,
            "actual_positive": 150,
            "actual_negative": 150,
            "recall": 0.96,
        },
    }


def write_evidence_bundle(tmp_path: Path, operations: list[dict[str, object]] | None = None) -> list[str]:
    inputs = {
        "shadow": shadow_records(),
        "operations": operations or operations_records(),
        "performance": performance_records(),
    }
    paths: dict[str, Path] = {}
    for name, records in inputs.items():
        path = tmp_path / f"{name}.jsonl"
        path.write_text(
            "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n",
            encoding="utf-8",
        )
        paths[name] = path
    benchmark = tmp_path / "benchmark.json"
    benchmark.write_text(json.dumps(benchmark_report(), ensure_ascii=False), encoding="utf-8")
    output = tmp_path / "release-gate.json"
    shadow_output = tmp_path / "shadow-comparison.json"
    return [
        "--shadow",
        str(paths["shadow"]),
        "--operations",
        str(paths["operations"]),
        "--performance",
        str(paths["performance"]),
        "--benchmark",
        str(benchmark),
        "--expected-revision",
        REVISION,
        "--output",
        str(output),
        "--shadow-output",
        str(shadow_output),
    ]


def test_complete_production_evidence_passes_and_reports_shadow_differences() -> None:
    report = build_release_gate_report(
        shadow_records(),
        operations_records(),
        performance_records(),
        benchmark_report(),
        REVISION,
    )
    assert report["release_gate_passed"] is True
    shadow = report["shadow_comparison"]
    assert isinstance(shadow, dict)
    assert shadow["candidate_event_count"] == 15
    assert shadow["legacy_event_count"] == 14
    daily = shadow["daily"]
    assert isinstance(daily, list)
    assert daily[4]["candidate_only_keys"] == ["corp:new:reviewed"]


def test_seven_consecutive_days_fail_when_a_day_is_missing() -> None:
    operations = operations_records()
    del operations[3]
    report = build_release_gate_report(
        shadow_records(),
        operations,
        performance_records(),
        benchmark_report(),
        REVISION,
    )
    assert report["release_gate_passed"] is False
    assert "operations.consecutive_days" in report["failed_gates"]


def test_daily_metric_below_threshold_names_the_failed_day() -> None:
    operations = operations_records()
    metrics = operations[-1]["metrics"]
    assert isinstance(metrics, dict)
    metrics["delivery_success_rate"] = 0.994
    report = build_release_gate_report(
        shadow_records(),
        operations,
        performance_records(),
        benchmark_report(),
        REVISION,
    )
    assert report["release_gate_passed"] is False
    assert "operations.2026-07-14.delivery_success_rate" in report["failed_gates"]


@pytest.mark.parametrize("evidence_kind", ["shadow", "operations", "performance", "benchmark"])
def test_synthetic_or_fixture_evidence_can_never_pass(evidence_kind: str) -> None:
    shadow = shadow_records()
    operations = operations_records()
    performance = performance_records()
    benchmark = benchmark_report()
    if evidence_kind == "shadow":
        shadow[-1]["is_synthetic"] = True
    elif evidence_kind == "operations":
        operations[-1]["evidence_source"] = "fixture"
    elif evidence_kind == "performance":
        performance[-1]["environment"] = "test"
    else:
        benchmark = benchmark_report(synthetic=True)
    with pytest.raises(ReleaseEvidenceError):
        build_release_gate_report(shadow, operations, performance, benchmark, REVISION)


def test_all_evidence_must_reference_one_code_revision() -> None:
    performance = performance_records()
    performance[-1]["code_revision"] = "d" * 40
    report = build_release_gate_report(
        shadow_records(),
        operations_records(),
        performance,
        benchmark_report(),
        REVISION,
    )
    assert report["release_gate_passed"] is False
    assert "evidence.single_code_revision" in report["failed_gates"]


def test_shadow_requires_reviewed_discrepancies_and_observed_events() -> None:
    shadow = shadow_records()
    shadow[-1]["discrepancies_reviewed"] = False
    report = build_release_gate_report(
        shadow,
        operations_records(),
        performance_records(),
        benchmark_report(),
        REVISION,
    )
    assert report["release_gate_passed"] is False
    assert "shadow.all_discrepancies_reviewed" in report["failed_gates"]


def test_checked_out_revision_must_match_evidence_but_accepts_sha_prefix() -> None:
    prefixed = build_release_gate_report(
        shadow_records(),
        operations_records(),
        performance_records(),
        benchmark_report(),
        REVISION[:12],
    )
    assert prefixed["release_gate_passed"] is True

    mismatched = build_release_gate_report(
        shadow_records(),
        operations_records(),
        performance_records(),
        benchmark_report(),
        "f" * 40,
    )
    assert mismatched["release_gate_passed"] is False
    assert "evidence.matches_checked_out_revision" in mismatched["failed_gates"]


def test_formal_gate_rejects_stale_production_windows() -> None:
    fresh = build_release_gate_report(
        shadow_records(),
        operations_records(),
        performance_records(),
        benchmark_report(),
        REVISION,
        evidence_as_of=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    assert fresh["release_gate_passed"] is True

    stale = build_release_gate_report(
        shadow_records(),
        operations_records(),
        performance_records(),
        benchmark_report(),
        REVISION,
        evidence_as_of=datetime(2026, 7, 20, tzinfo=timezone.utc),
    )
    assert stale["release_gate_passed"] is False
    assert "evidence.operations_are_recent" in stale["failed_gates"]
    assert "evidence.performance_is_recent" in stale["failed_gates"]


def test_daily_evidence_collected_at_must_match_its_calendar_date() -> None:
    operations = operations_records()
    operations[0]["collected_at"] = "2026-07-09T00:01:00+09:00"

    with pytest.raises(ReleaseEvidenceError, match="collected_at calendar date must match"):
        build_release_gate_report(
            shadow_records(),
            operations,
            performance_records(),
            benchmark_report(),
            REVISION,
        )


def test_evidence_collected_after_producer_run_is_rejected() -> None:
    benchmark = benchmark_report()
    evidence = benchmark["evidence"]
    assert isinstance(evidence, dict)
    evidence["collected_at"] = "2026-07-17T00:00:00+00:00"

    with pytest.raises(ReleaseEvidenceError, match="later than evidence_as_of"):
        build_release_gate_report(
            shadow_records(),
            operations_records(),
            performance_records(),
            benchmark,
            REVISION,
            evidence_as_of=datetime(2026, 7, 16, tzinfo=timezone.utc),
        )


def test_cli_writes_complete_and_shadow_reports(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    args = write_evidence_bundle(tmp_path)
    main(args)
    report = json.loads((tmp_path / "release-gate.json").read_text(encoding="utf-8"))
    shadow = json.loads((tmp_path / "shadow-comparison.json").read_text(encoding="utf-8"))
    assert report["release_gate_passed"] is True
    assert shadow["candidate_event_count"] == 15
    assert '"release_gate_passed": true' in capsys.readouterr().out


def test_cli_writes_failure_report_before_exit_one(tmp_path: Path) -> None:
    operations = operations_records()
    metrics = operations[-1]["metrics"]
    assert isinstance(metrics, dict)
    metrics["same_story_precision"] = 0.5
    args = write_evidence_bundle(tmp_path, operations)
    with pytest.raises(SystemExit) as raised:
        main(args)
    assert raised.value.code == 1
    report = json.loads((tmp_path / "release-gate.json").read_text(encoding="utf-8"))
    assert report["release_gate_passed"] is False
