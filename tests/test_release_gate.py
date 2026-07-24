from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from curator.release_gate import (
    GateThresholds,
    ReleaseEvidenceError,
    build_operations_gates,
    build_release_gate_report,
    main,
)


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
                "legacy_crosswalk": {
                    "schema_version": 1,
                    "eligible_legacy_record_count": 10,
                    "crosswalked_legacy_record_count": 10,
                    "unmatched_legacy_record_count": 0,
                    "ambiguous_legacy_record_count": 0,
                    "coverage_rate": 1.0,
                    "crosswalk_sha256": hashlib.sha256(
                        f"legacy-crosswalk:{day.isoformat()}".encode()
                    ).hexdigest(),
                },
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
                    "metrics_contract_version": 2,
                    "distribution_mode": "web_only",
                    "official_ingest_success_rate": 0.999,
                    "dart_ingest_success_rate": 0.999,
                    "kind_ingest_success_rate": 0.999,
                    "official_lag_p95_minutes": 30.0,
                    "dart_success_poll_interval_p95_minutes": 30.0,
                    "kind_observation_lag_p95_minutes": 30.0,
                    "content_snapshot_at": f"{day.isoformat()}T14:59:59+00:00",
                    "content_scope": "governance_corpus_2021_plus_kst_day_end_v2",
                    "web_distribution_attempted_count": 1000,
                    "web_distribution_succeeded_count": 999,
                    "web_distribution_success_rate": 0.999,
                    "web_distribution_failure_detection_p95_minutes": 5.0,
                    "telegram_delivery_attempted_count": 0,
                    "raw_counts": {
                        "official_ingest_expected_count": 2000,
                        "official_ingest_succeeded_count": 1998,
                        "dart_ingest_expected_count": 1000,
                        "dart_ingest_succeeded_count": 999,
                        "kind_ingest_expected_count": 1000,
                        "kind_ingest_succeeded_count": 999,
                        "kind_observation_count": 10,
                        "kind_lag_sample_count": 10,
                        "official_evidence_total_count": 100,
                        "official_evidence_linked_count": 98,
                        "top_sensitive_total_count": 5,
                        "top_sensitive_reviewed_count": 5,
                        "original_language_total_count": 100,
                        "original_language_preserved_count": 100,
                        "source_right_total_count": 10,
                        "valid_source_right_count": 10,
                    },
                    "official_evidence_link_rate": 0.98,
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
            "raw_counts": {
                "availability_attempted_count": 10000,
                "availability_succeeded_count": 9999,
                "mobile_lcp_sample_count": 20,
                "mobile_inp_sample_count": 20,
                "mobile_cls_sample_count": 20,
            },
        }
        records.append(record)
    return records


def benchmark_review_process() -> dict[str, object]:
    process: dict[str, object] = {
        "schema_version": 1,
        "contract": "independent-human-review-v1",
        "candidate_manifest_sha256": "1" * 64,
        "candidate_files": {
            "same_story": {
                "sha256": "2" * 64,
                "item_count": 650,
                "strata": {
                    "predicted_same": 300,
                    "hard_negative": 250,
                    "easy_negative": 100,
                },
            },
            "relevance": {
                "sha256": "3" * 64,
                "item_count": 420,
                "strata": {
                    "official_event": 300,
                    "non_governance_hard_negative": 120,
                },
            },
        },
        "pilot": {
            "report_sha256": "4" * 64,
            "same_event": {
                "item_count": 50,
                "reviewer_ids": ["reviewer-a", "reviewer-b"],
                "cohen_kappa": 0.9,
                "threshold": 0.8,
                "item_ids_sha256": "5" * 64,
            },
            "core_event": {
                "item_count": 30,
                "reviewer_ids": ["reviewer-a", "reviewer-b"],
                "cohen_kappa": 0.85,
                "threshold": 0.8,
                "item_ids_sha256": "6" * 64,
            },
        },
        "reviewers": {
            "reviewer_count": 2,
            "reviewer_ids": ["reviewer-a", "reviewer-b"],
            "same_story_reviewer_a_sha256": "7" * 64,
            "same_story_reviewer_b_sha256": "8" * 64,
            "relevance_reviewer_a_sha256": "9" * 64,
            "relevance_reviewer_b_sha256": "a" * 64,
        },
        "final": {
            "same_story_item_count": 650,
            "relevance_item_count": 420,
            "same_story_strata": {
                "predicted_same": 300,
                "hard_negative": 250,
                "easy_negative": 100,
            },
            "relevance_strata": {
                "official_event": 300,
                "non_governance_hard_negative": 120,
            },
        },
        "adjudication": {
            "dataset_sha256": "b" * 64,
            "disagreement_count": 20,
            "adjudicated_count": 20,
            "unresolved_count": 0,
            "task_counts": {"same_story": 12, "relevance": 8},
        },
    }
    process["process_sha256"] = hashlib.sha256(
        json.dumps(process, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return process


def benchmark_report(*, synthetic: bool = False, revision: str = REVISION) -> dict[str, object]:
    review_process = benchmark_review_process()
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
            "benchmark_process_sha256": review_process["process_sha256"],
        },
        "thresholds": {
            "min_article_pairs": 500,
            "min_events": 300,
            "min_relevance_hard_negatives": 120,
            "same_story_min_precision": 0.97,
            "relevance_min_precision": 0.90,
            "relevance_min_recall": 0.95,
        },
        "same_story": {
            "sample_count": 650,
            "actual_positive": 300,
            "actual_negative": 350,
            "precision": 0.98,
            "strata": {
                "predicted_same": 300,
                "hard_negative": 250,
                "easy_negative": 100,
            },
        },
        "relevance": {
            "sample_count": 420,
            "unique_event_count": 300,
            "official_linked_event_count": 300,
            "hard_negative_count": 120,
            "actual_positive": 300,
            "actual_negative": 120,
            "precision": 0.91,
            "recall": 0.96,
        },
        "review_process": review_process,
    }


def reseal_benchmark_process(benchmark: dict[str, object]) -> None:
    process = benchmark["review_process"]
    evidence = benchmark["evidence"]
    assert isinstance(process, dict)
    assert isinstance(evidence, dict)
    process.pop("process_sha256", None)
    digest = hashlib.sha256(
        json.dumps(process, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    process["process_sha256"] = digest
    evidence["benchmark_process_sha256"] = digest


def usability_report(
    *,
    succeeded_count: int = 15,
    revision: str = REVISION,
    source: str = "human_usability_export",
) -> dict[str, object]:
    evaluations: list[dict[str, object]] = []
    for segment in ("institution", "high_net_worth", "international_institution"):
        for index in range(5):
            succeeded = len(evaluations) < succeeded_count
            evaluations.append(
                {
                    "evaluation_id": f"{segment}-{index}",
                    "segment": segment,
                    "completed_at": "2026-07-14T14:00:00+00:00",
                    "duration_seconds": 120.0 if succeeded else 200.0,
                    "identified_event": succeeded,
                    "identified_actors": succeeded,
                    "identified_official_evidence": succeeded,
                    "identified_current_status": succeeded,
                    "succeeded": succeeded,
                }
            )
    canonical = "\n".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        for record in evaluations
    ).encode("utf-8")
    return {
        "schema_version": 1,
        "environment": "production",
        "evidence_source": source,
        "is_synthetic": False,
        "collected_at": "2026-07-14T15:00:00+00:00",
        "code_revision": revision,
        "dataset_sha256": hashlib.sha256(canonical).hexdigest(),
        "target_seconds": 180,
        "evaluator_count": 15,
        "succeeded_evaluator_count": succeeded_count,
        "success_rate": succeeded_count / 15,
        "evaluations": evaluations,
    }


def approval_report(
    *,
    approved: bool = True,
    revision: str = REVISION,
    usability: dict[str, object] | None = None,
    benchmark: dict[str, object] | None = None,
) -> dict[str, object]:
    usability_value = usability or usability_report()
    benchmark_value = benchmark or benchmark_report()
    evidence = benchmark_value["evidence"]
    assert isinstance(evidence, dict)
    approvals = [
        {
            "role": role,
            "decision": "approved" if approved else "rejected",
            "decided_at": "2026-07-14T15:10:00+00:00",
            "approver_reference": f"internal-register:{role}",
            "evidence_uri": f"urn:bside:approval:{role}:2026-07-14",
            "evidence_sha256": str(index) * 64,
        }
        for index, role in enumerate(("legal", "editorial", "product"), start=1)
    ]
    return {
        "schema_version": 1,
        "environment": "production",
        "evidence_source": "signed_release_approval_export",
        "is_synthetic": False,
        "collected_at": "2026-07-14T15:15:00+00:00",
        "code_revision": revision,
        "approved_revision": revision,
        "usability_dataset_sha256": usability_value["dataset_sha256"],
        "same_story_dataset_sha256": evidence["same_story_dataset_sha256"],
        "relevance_dataset_sha256": evidence["relevance_dataset_sha256"],
        "approvals": approvals,
        "release_approved": approved,
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
    benchmark_value = benchmark_report()
    usability_value = usability_report()
    benchmark = tmp_path / "benchmark.json"
    benchmark.write_text(json.dumps(benchmark_value, ensure_ascii=False), encoding="utf-8")
    usability = tmp_path / "usability.json"
    usability.write_text(json.dumps(usability_value, ensure_ascii=False), encoding="utf-8")
    approval = tmp_path / "release-approval.json"
    approval.write_text(
        json.dumps(
            approval_report(usability=usability_value, benchmark=benchmark_value),
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
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
        "--usability",
        str(usability),
        "--approval",
        str(approval),
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
        usability_report(),
        approval_report(),
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


def test_benchmark_rejects_low_relevance_precision_and_weak_declared_threshold() -> None:
    benchmark = benchmark_report()
    relevance = benchmark["relevance"]
    assert isinstance(relevance, dict)
    relevance["precision"] = 0.89
    report = build_release_gate_report(
        shadow_records(),
        operations_records(),
        performance_records(),
        benchmark,
        usability_report(),
        approval_report(benchmark=benchmark),
        REVISION,
    )
    assert "benchmark.actual.relevance_precision" in report["failed_gates"]

    benchmark = benchmark_report()
    thresholds = benchmark["thresholds"]
    assert isinstance(thresholds, dict)
    thresholds["relevance_min_precision"] = 0.89
    report = build_release_gate_report(
        shadow_records(),
        operations_records(),
        performance_records(),
        benchmark,
        usability_report(),
        approval_report(benchmark=benchmark),
        REVISION,
    )
    assert "benchmark.threshold.relevance_precision" in report["failed_gates"]


def test_benchmark_requires_three_hundred_linked_events_and_120_confirmed_hard_negatives() -> None:
    benchmark = benchmark_report()
    relevance = benchmark["relevance"]
    assert isinstance(relevance, dict)
    relevance["official_linked_event_count"] = 299
    relevance["hard_negative_count"] = 119
    report = build_release_gate_report(
        shadow_records(),
        operations_records(),
        performance_records(),
        benchmark,
        usability_report(),
        approval_report(benchmark=benchmark),
        REVISION,
    )
    assert "benchmark.actual.relevance_events" in report["failed_gates"]
    assert "benchmark.actual.relevance_hard_negatives" in report["failed_gates"]


def test_benchmark_review_process_digest_tampering_is_rejected() -> None:
    benchmark = benchmark_report()
    process = benchmark["review_process"]
    assert isinstance(process, dict)
    process["candidate_manifest_sha256"] = "f" * 64
    with pytest.raises(ReleaseEvidenceError, match="process SHA-256 mismatch"):
        build_release_gate_report(
            shadow_records(),
            operations_records(),
            performance_records(),
            benchmark,
            usability_report(),
            approval_report(benchmark=benchmark),
            REVISION,
        )


@pytest.mark.parametrize("tamper", ["reviewer", "kappa", "strata", "unresolved"])
def test_benchmark_review_process_semantics_are_revalidated(tamper: str) -> None:
    benchmark = benchmark_report()
    process = benchmark["review_process"]
    assert isinstance(process, dict)
    if tamper == "reviewer":
        reviewers = process["reviewers"]
        assert isinstance(reviewers, dict)
        reviewers["reviewer_count"] = 1
        pattern = "exactly two distinct reviewers"
    elif tamper == "kappa":
        pilot = process["pilot"]
        assert isinstance(pilot, dict)
        same_event = pilot["same_event"]
        assert isinstance(same_event, dict)
        same_event["cohen_kappa"] = 0.79
        pattern = "must be >= 0.8"
    elif tamper == "strata":
        candidate_files = process["candidate_files"]
        assert isinstance(candidate_files, dict)
        same_story = candidate_files["same_story"]
        assert isinstance(same_story, dict)
        strata = same_story["strata"]
        assert isinstance(strata, dict)
        strata["hard_negative"] = 249
        pattern = "must equal 250"
    else:
        adjudication = process["adjudication"]
        assert isinstance(adjudication, dict)
        adjudication["unresolved_count"] = 1
        pattern = "none unresolved"
    reseal_benchmark_process(benchmark)
    with pytest.raises(ReleaseEvidenceError, match=pattern):
        build_release_gate_report(
            shadow_records(),
            operations_records(),
            performance_records(),
            benchmark,
            usability_report(),
            approval_report(benchmark=benchmark),
            REVISION,
        )


def test_seven_consecutive_days_fail_when_a_day_is_missing() -> None:
    operations = operations_records()
    del operations[3]
    report = build_release_gate_report(
        shadow_records(),
        operations,
        performance_records(),
        benchmark_report(),
        usability_report(),
        approval_report(),
        REVISION,
    )
    assert report["release_gate_passed"] is False
    assert "operations.consecutive_days" in report["failed_gates"]


def test_daily_metric_below_threshold_names_the_failed_day() -> None:
    operations = operations_records()
    metrics = operations[-1]["metrics"]
    assert isinstance(metrics, dict)
    metrics["web_distribution_attempted_count"] = 1000
    metrics["web_distribution_succeeded_count"] = 994
    metrics["web_distribution_success_rate"] = 0.994
    report = build_release_gate_report(
        shadow_records(),
        operations,
        performance_records(),
        benchmark_report(),
        usability_report(),
        approval_report(),
        REVISION,
    )
    assert report["release_gate_passed"] is False
    assert "operations.2026-07-14.web_distribution_success_rate" in report["failed_gates"]


def test_kind_must_have_its_own_nonzero_success_and_lag_evidence() -> None:
    operations = operations_records()
    metrics = operations[-1]["metrics"]
    assert isinstance(metrics, dict)
    raw_counts = metrics["raw_counts"]
    assert isinstance(raw_counts, dict)
    raw_counts["kind_ingest_expected_count"] = 0
    raw_counts["kind_ingest_succeeded_count"] = 0
    with pytest.raises(ReleaseEvidenceError, match="kind_ingest_expected_count must be non-zero"):
        build_release_gate_report(
            shadow_records(),
            operations,
            performance_records(),
            benchmark_report(),
            usability_report(),
            approval_report(),
            REVISION,
        )

    operations = operations_records()
    metrics = operations[-1]["metrics"]
    assert isinstance(metrics, dict)
    metrics["kind_observation_lag_p95_minutes"] = 46.0
    metrics["official_lag_p95_minutes"] = 46.0
    report = build_release_gate_report(
        shadow_records(),
        operations,
        performance_records(),
        benchmark_report(),
        usability_report(),
        approval_report(),
        REVISION,
    )
    assert report["release_gate_passed"] is False
    assert "operations.2026-07-14.kind_observation_lag_p95_minutes" in report["failed_gates"]


def test_true_kind_no_disclosure_day_is_na_while_window_uses_real_samples() -> None:
    operations = operations_records()
    metrics = operations[0]["metrics"]
    assert isinstance(metrics, dict)
    raw_counts = metrics["raw_counts"]
    assert isinstance(raw_counts, dict)
    raw_counts["kind_observation_count"] = 0
    raw_counts["kind_lag_sample_count"] = 0
    metrics["kind_observation_lag_p95_minutes"] = None
    metrics["official_lag_p95_minutes"] = metrics["dart_success_poll_interval_p95_minutes"]

    report = build_release_gate_report(
        shadow_records(),
        operations,
        performance_records(),
        benchmark_report(),
        usability_report(),
        approval_report(),
        REVISION,
    )
    assert report["release_gate_passed"] is True
    assert report["operations"]["daily"][0]["kind_observation_lag_p95_minutes"] is None
    assert report["operations"]["window"]["kind_observation_count"] == 60


def test_all_kind_no_disclosure_days_fail_the_seven_day_denominator_gate() -> None:
    operations = operations_records()
    for record in operations:
        metrics = record["metrics"]
        assert isinstance(metrics, dict)
        raw_counts = metrics["raw_counts"]
        assert isinstance(raw_counts, dict)
        raw_counts["kind_observation_count"] = 0
        raw_counts["kind_lag_sample_count"] = 0
        metrics["kind_observation_lag_p95_minutes"] = None
        metrics["official_lag_p95_minutes"] = metrics["dart_success_poll_interval_p95_minutes"]
    report = build_release_gate_report(
        shadow_records(),
        operations,
        performance_records(),
        benchmark_report(),
        usability_report(),
        approval_report(),
        REVISION,
    )
    assert report["release_gate_passed"] is False
    assert "operations.window.kind_observation_samples" in report["failed_gates"]


def test_zero_daily_corpus_denominators_are_aggregated_over_the_seven_day_window() -> None:
    operations = operations_records()
    metrics = operations[0]["metrics"]
    assert isinstance(metrics, dict)
    raw_counts = metrics["raw_counts"]
    assert isinstance(raw_counts, dict)
    for rate, numerator, denominator in (
        ("official_evidence_link_rate", "official_evidence_linked_count", "official_evidence_total_count"),
        ("top_sensitive_human_review_rate", "top_sensitive_reviewed_count", "top_sensitive_total_count"),
        ("original_language_preservation_rate", "original_language_preserved_count", "original_language_total_count"),
        ("valid_source_right_rate", "valid_source_right_count", "source_right_total_count"),
    ):
        metrics[rate] = None
        raw_counts[numerator] = 0
        raw_counts[denominator] = 0
    report = build_release_gate_report(
        shadow_records(),
        operations,
        performance_records(),
        benchmark_report(),
        usability_report(),
        approval_report(),
        REVISION,
    )
    assert report["release_gate_passed"] is True
    assert (
        report["operations"]["window"]["corpus_counts"]["official_evidence_link_rate"]
        == {"numerator": 588, "denominator": 600}
    )


def test_performance_rate_must_match_nonzero_raw_availability_counts() -> None:
    performance = performance_records()
    metrics = performance[-1]["metrics"]
    assert isinstance(metrics, dict)
    raw_counts = metrics["raw_counts"]
    assert isinstance(raw_counts, dict)
    raw_counts["availability_attempted_count"] = 0
    raw_counts["availability_succeeded_count"] = 0

    with pytest.raises(ReleaseEvidenceError, match="must be non-zero"):
        build_release_gate_report(
            shadow_records(),
            operations_records(),
            performance,
            benchmark_report(),
            usability_report(),
            approval_report(),
            REVISION,
        )

    performance = performance_records()
    metrics = performance[-1]["metrics"]
    assert isinstance(metrics, dict)
    metrics["availability_rate"] = 1.0
    with pytest.raises(ReleaseEvidenceError, match="does not match"):
        build_release_gate_report(
            shadow_records(),
            operations_records(),
            performance,
            benchmark_report(),
            usability_report(),
            approval_report(),
            REVISION,
        )


def test_performance_requires_four_routes_times_five_real_samples_per_metric() -> None:
    performance = performance_records()
    metrics = performance[-1]["metrics"]
    assert isinstance(metrics, dict)
    raw_counts = metrics["raw_counts"]
    assert isinstance(raw_counts, dict)
    raw_counts["mobile_inp_sample_count"] = 19

    with pytest.raises(ReleaseEvidenceError, match="at least 20 real route measurements"):
        build_release_gate_report(
            shadow_records(),
            operations_records(),
            performance,
            benchmark_report(),
            usability_report(),
            approval_report(),
            REVISION,
        )


def test_legacy_content_corpus_scope_fails_closed() -> None:
    operations = operations_records()
    metrics = operations[-1]["metrics"]
    assert isinstance(metrics, dict)
    metrics["content_scope"] = "governance_corpus_2021_plus_kst_day_end_v1"

    with pytest.raises(ReleaseEvidenceError, match="invalid content_scope"):
        build_operations_gates(operations, GateThresholds())


@pytest.mark.parametrize(
    "evidence_kind", ["shadow", "operations", "performance", "benchmark", "usability", "approval"]
)
def test_synthetic_or_fixture_evidence_can_never_pass(evidence_kind: str) -> None:
    shadow = shadow_records()
    operations = operations_records()
    performance = performance_records()
    benchmark = benchmark_report()
    usability = usability_report()
    approval = approval_report(usability=usability, benchmark=benchmark)
    if evidence_kind == "shadow":
        shadow[-1]["is_synthetic"] = True
    elif evidence_kind == "operations":
        operations[-1]["evidence_source"] = "fixture"
    elif evidence_kind == "performance":
        performance[-1]["environment"] = "test"
    elif evidence_kind == "benchmark":
        benchmark = benchmark_report(synthetic=True)
    elif evidence_kind == "usability":
        usability["evidence_source"] = "fixture_human_export"
    else:
        approval["is_synthetic"] = True
    with pytest.raises(ReleaseEvidenceError):
        build_release_gate_report(
            shadow,
            operations,
            performance,
            benchmark,
            usability,
            approval,
            REVISION,
        )


def test_all_evidence_must_reference_one_code_revision() -> None:
    performance = performance_records()
    performance[-1]["code_revision"] = "d" * 40
    report = build_release_gate_report(
        shadow_records(),
        operations_records(),
        performance,
        benchmark_report(),
        usability_report(),
        approval_report(),
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
        usability_report(),
        approval_report(),
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
        usability_report(),
        approval_report(),
        REVISION[:12],
    )
    assert prefixed["release_gate_passed"] is True

    mismatched = build_release_gate_report(
        shadow_records(),
        operations_records(),
        performance_records(),
        benchmark_report(),
        usability_report(),
        approval_report(),
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
        usability_report(),
        approval_report(),
        REVISION,
        evidence_as_of=datetime(2026, 7, 16, tzinfo=timezone.utc),
    )
    assert fresh["release_gate_passed"] is True

    stale = build_release_gate_report(
        shadow_records(),
        operations_records(),
        performance_records(),
        benchmark_report(),
        usability_report(),
        approval_report(),
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
            usability_report(),
            approval_report(),
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
            usability_report(),
            approval_report(benchmark=benchmark),
            REVISION,
            evidence_as_of=datetime(2026, 7, 16, tzinfo=timezone.utc),
        )


def test_usability_requires_twelve_of_fifteen_and_five_per_segment() -> None:
    usability = usability_report(succeeded_count=11)
    report = build_release_gate_report(
        shadow_records(),
        operations_records(),
        performance_records(),
        benchmark_report(),
        usability,
        approval_report(usability=usability),
        REVISION,
    )
    assert report["release_gate_passed"] is False
    assert "usability.successful_evaluators" in report["failed_gates"]

    imbalanced = usability_report(succeeded_count=15)
    evaluations = imbalanced["evaluations"]
    assert isinstance(evaluations, list)
    first = evaluations[0]
    assert isinstance(first, dict)
    first["segment"] = "high_net_worth"
    canonical = "\n".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        for record in evaluations
    ).encode("utf-8")
    imbalanced["dataset_sha256"] = hashlib.sha256(canonical).hexdigest()
    imbalanced_report = build_release_gate_report(
        shadow_records(),
        operations_records(),
        performance_records(),
        benchmark_report(),
        imbalanced,
        approval_report(usability=imbalanced),
        REVISION,
    )
    assert imbalanced_report["release_gate_passed"] is False
    assert "usability.segment.institution" in imbalanced_report["failed_gates"]
    assert "usability.segment.high_net_worth" in imbalanced_report["failed_gates"]


def test_usability_requires_explicit_human_source_and_exact_dataset_digest() -> None:
    wrong_source = usability_report(source="mysql_export")
    with pytest.raises(ReleaseEvidenceError, match="human_usability_export"):
        build_release_gate_report(
            shadow_records(),
            operations_records(),
            performance_records(),
            benchmark_report(),
            wrong_source,
            approval_report(usability=wrong_source),
            REVISION,
        )

    wrong_digest = usability_report()
    wrong_digest["dataset_sha256"] = "f" * 64
    with pytest.raises(ReleaseEvidenceError, match="dataset_sha256 does not match"):
        build_release_gate_report(
            shadow_records(),
            operations_records(),
            performance_records(),
            benchmark_report(),
            wrong_digest,
            approval_report(usability=wrong_digest),
            REVISION,
        )


def test_release_requires_positive_legal_editorial_and_product_approval() -> None:
    rejected = approval_report(approved=False)
    report = build_release_gate_report(
        shadow_records(),
        operations_records(),
        performance_records(),
        benchmark_report(),
        usability_report(),
        rejected,
        REVISION,
    )
    assert report["release_gate_passed"] is False
    assert "release_approval.release_approved" in report["failed_gates"]
    assert "release_approval.role.legal" in report["failed_gates"]
    assert "release_approval.role.editorial" in report["failed_gates"]
    assert "release_approval.role.product" in report["failed_gates"]


def test_usability_and_approval_must_match_the_checked_out_revision() -> None:
    usability = usability_report(revision="f" * 40)
    approval = approval_report(revision="f" * 40, usability=usability)
    report = build_release_gate_report(
        shadow_records(),
        operations_records(),
        performance_records(),
        benchmark_report(),
        usability,
        approval,
        REVISION,
    )
    assert report["release_gate_passed"] is False
    assert "evidence.single_code_revision" in report["failed_gates"]
    assert "evidence.matches_checked_out_revision" in report["failed_gates"]


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
    metrics["official_evidence_link_rate"] = 0.5
    raw_counts = metrics["raw_counts"]
    assert isinstance(raw_counts, dict)
    raw_counts["official_evidence_linked_count"] = 50
    args = write_evidence_bundle(tmp_path, operations)
    with pytest.raises(SystemExit) as raised:
        main(args)
    assert raised.value.code == 1
    report = json.loads((tmp_path / "release-gate.json").read_text(encoding="utf-8"))
    assert report["release_gate_passed"] is False
