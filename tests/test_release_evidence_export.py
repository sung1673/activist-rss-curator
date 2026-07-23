from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from curator.release_evidence import (
    EvidenceExportError,
    SourceArtifact,
    export_release_evidence,
    main,
)


REVISION = "a" * 40
THROUGH_DATE = date(2026, 7, 14)
ROOT = Path(__file__).resolve().parents[1]


def provenance(day: date, *, source: str = "mysql_export") -> dict[str, object]:
    return {
        "schema_version": 1,
        "date": day.isoformat(),
        "environment": "production",
        "evidence_source": source,
        "is_synthetic": False,
        "collected_at": f"{day.isoformat()}T23:59:00+09:00",
        "code_revision": REVISION,
    }


def shadow_records() -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for offset in range(14):
        day = date(2026, 7, 1) + timedelta(days=offset)
        event = {"comparison_key": f"eventcmp:v1:{offset:064x}"}
        result.append(
            {
                **provenance(day),
                "legacy_run": {"status": "succeeded", "events": [event]},
                "candidate_run": {"status": "succeeded", "events": [event]},
                "legacy_crosswalk": {
                    "schema_version": 1,
                    "eligible_legacy_record_count": 1,
                    "crosswalked_legacy_record_count": 1,
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
    return result


def operations_records() -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for offset in range(7):
        day = date(2026, 7, 8) + timedelta(days=offset)
        result.append(
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
    return result


def performance_records() -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    for offset in range(7):
        day = date(2026, 7, 8) + timedelta(days=offset)
        result.append(
            {
                **provenance(day, source="production_rum_and_watchdog_export"),
                "metrics": {
                    "availability_rate": 1.0,
                    "availability_cadence_id": "watchdog-v1-kst-5m-minute01",
                    "availability_actual_interval_seconds_p95": 300.0,
                    "availability_actual_max_gap_seconds": 300.0,
                    "availability_first_observed_at": f"{day.isoformat()}T00:01:00+09:00",
                    "availability_last_observed_at": f"{day.isoformat()}T23:56:00+09:00",
                    "availability_coverage_rate": 1.0,
                    "mobile_lcp_p75_seconds": 2.1,
                    "mobile_inp_p75_ms": 175.0,
                    "mobile_cls_p75": 0.05,
                    "raw_counts": {
                        "availability_attempted_count": 1152,
                        "availability_succeeded_count": 1152,
                        "availability_expected_slot_count": 1152,
                        "availability_covered_slot_count": 1152,
                        "availability_missing_slot_count": 0,
                        "availability_duplicate_slot_count": 0,
                        "availability_off_cadence_count": 0,
                        "mobile_lcp_sample_count": 20,
                        "mobile_inp_sample_count": 20,
                        "mobile_cls_sample_count": 20,
                    },
                },
            }
        )
    return result


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


def benchmark_report() -> dict[str, object]:
    review_process = benchmark_review_process()
    return {
        "schema_version": 1,
        "evaluated_at": "2026-07-14T15:00:00+00:00",
        "release_gate_passed": True,
        "failed_gates": [],
        "evidence": {
            "schema_version": 1,
            "environment": "production",
            "evidence_source": "human_labeled_jsonl",
            "is_synthetic": False,
            "collected_at": "2026-07-14T15:00:00+00:00",
            "code_revision": REVISION,
            "release_eligible": True,
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


def usability_report() -> dict[str, object]:
    evaluations: list[dict[str, object]] = []
    segments = ("institution", "high_net_worth", "international_institution")
    for segment in segments:
        for index in range(5):
            evaluations.append(
                {
                    "evaluation_id": f"{segment}-{index}",
                    "segment": segment,
                    "completed_at": "2026-07-14T14:00:00+00:00",
                    "duration_seconds": 120.0,
                    "identified_event": True,
                    "identified_actors": True,
                    "identified_official_evidence": True,
                    "identified_current_status": True,
                    "succeeded": True,
                }
            )
    canonical = "\n".join(
        json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        for record in evaluations
    ).encode("utf-8")
    dataset_sha256 = hashlib.sha256(canonical).hexdigest()
    return {
        "schema_version": 1,
        "environment": "production",
        "evidence_source": "human_usability_export",
        "is_synthetic": False,
        "collected_at": "2026-07-14T15:00:00+00:00",
        "code_revision": REVISION,
        "dataset_sha256": dataset_sha256,
        "target_seconds": 180,
        "evaluator_count": 15,
        "succeeded_evaluator_count": 15,
        "success_rate": 1.0,
        "evaluations": evaluations,
    }


def approval_report() -> dict[str, object]:
    approvals = []
    for index, role in enumerate(("legal", "editorial", "product"), start=1):
        approvals.append(
            {
                "role": role,
                "decision": "approved",
                "decided_at": "2026-07-14T15:10:00+00:00",
                "approver_reference": f"internal-register:{role}",
                "evidence_uri": f"urn:bside:approval:{role}:2026-07-14",
                "evidence_sha256": str(index) * 64,
            }
        )
    return {
        "schema_version": 1,
        "environment": "production",
        "evidence_source": "signed_release_approval_export",
        "is_synthetic": False,
        "collected_at": "2026-07-14T15:15:00+00:00",
        "code_revision": REVISION,
        "approved_revision": REVISION,
        "usability_dataset_sha256": usability_report()["dataset_sha256"],
        "same_story_dataset_sha256": "b" * 64,
        "relevance_dataset_sha256": "c" * 64,
        "approvals": approvals,
        "release_approved": True,
    }


def write_source(source: Path) -> None:
    source.mkdir()
    for filename, records in (
        ("shadow.jsonl", shadow_records()),
        ("operations.jsonl", operations_records()),
        ("performance.jsonl", performance_records()),
    ):
        (source / filename).write_text(
            "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records),
            encoding="utf-8",
        )
    for filename, report in (
        ("benchmark.json", benchmark_report()),
        ("usability.json", usability_report()),
        ("release-approval.json", approval_report()),
    ):
        (source / filename).write_text(json.dumps(report, ensure_ascii=False), encoding="utf-8")


def export(source: Path, output: Path) -> dict[str, object]:
    return export_release_evidence(
        source_dir=source,
        output_dir=output,
        expected_revision=REVISION,
        through_date=THROUGH_DATE,
        source_artifact=SourceArtifact(1234, 5678, "sha256:" + "e" * 64),
        exported_at=datetime(2026, 7, 14, 16, 0, tzinfo=timezone.utc),
    )


def rewrite_jsonl(path: Path, records: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in records),
        encoding="utf-8",
    )


def test_export_writes_six_same_revision_files_and_manifest(tmp_path: Path) -> None:
    source = tmp_path / "source"
    output = tmp_path / "output"
    write_source(source)

    manifest = export(source, output)

    expected = {
        "shadow.jsonl",
        "operations.jsonl",
        "performance.jsonl",
        "benchmark.json",
        "usability.json",
        "release-approval.json",
        "bundle-manifest.json",
    }
    assert {path.name for path in output.iterdir()} == expected
    assert manifest["code_revision"] == REVISION
    assert manifest["through_date"] == THROUGH_DATE.isoformat()
    assert manifest["source_artifact"] == {
        "workflow_run_id": 1234,
        "artifact_id": 5678,
        "digest": "sha256:" + "e" * 64,
    }
    shadow = [json.loads(line) for line in (output / "shadow.jsonl").read_text(encoding="utf-8").splitlines()]
    operations = [
        json.loads(line) for line in (output / "operations.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert [record["date"] for record in shadow] == [
        (date(2026, 7, 1) + timedelta(days=offset)).isoformat() for offset in range(14)
    ]
    assert len(operations) == 7
    assert manifest["files"]["benchmark.json"]["source_sha256"] != ""  # type: ignore[index]


def test_missing_daily_date_is_rejected_without_partial_output(tmp_path: Path) -> None:
    source = tmp_path / "source"
    output = tmp_path / "output"
    write_source(source)
    records = shadow_records()
    rewrite_jsonl(source / "shadow.jsonl", records[:6] + records[7:])

    with pytest.raises(EvidenceExportError, match="missing required dates"):
        export(source, output)

    assert not output.exists()


@pytest.mark.parametrize(
    ("filename", "mutation", "message"),
    [
        (
            "operations.jsonl",
            lambda records: records[0]["metrics"]["raw_counts"].update(  # type: ignore[index,union-attr]
                {"official_ingest_expected_count": 0}
            ),
            "must be non-zero",
        ),
        (
            "performance.jsonl",
            lambda records: records[0]["metrics"]["raw_counts"].update(  # type: ignore[index,union-attr]
                {"mobile_lcp_sample_count": 0}
            ),
            "must contain at least 20 real route measurements",
        ),
    ],
)
def test_zero_denominators_are_rejected(
    tmp_path: Path,
    filename: str,
    mutation: object,
    message: str,
) -> None:
    source = tmp_path / "source"
    write_source(source)
    records = operations_records() if filename == "operations.jsonl" else performance_records()
    mutation(records)  # type: ignore[operator]
    rewrite_jsonl(source / filename, records)

    with pytest.raises(EvidenceExportError, match=message):
        export(source, tmp_path / "output")


@pytest.mark.parametrize("filename", ["shadow.jsonl", "operations.jsonl", "performance.jsonl"])
def test_synthetic_or_fixture_daily_sources_are_rejected(tmp_path: Path, filename: str) -> None:
    source = tmp_path / "source"
    write_source(source)
    records = {
        "shadow.jsonl": shadow_records,
        "operations.jsonl": operations_records,
        "performance.jsonl": performance_records,
    }[filename]()
    records[-1]["evidence_source"] = "fixture_mysql_export"
    rewrite_jsonl(source / filename, records)

    with pytest.raises(EvidenceExportError, match="not production evidence"):
        export(source, tmp_path / "output")


@pytest.mark.parametrize(
    "filename",
    [
        "shadow.jsonl",
        "operations.jsonl",
        "performance.jsonl",
        "benchmark.json",
        "usability.json",
        "release-approval.json",
    ],
)
def test_every_output_must_match_the_exact_release_sha(tmp_path: Path, filename: str) -> None:
    source = tmp_path / "source"
    write_source(source)
    path = source / filename
    if filename.endswith(".jsonl"):
        records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        records[-1]["code_revision"] = "f" * 40
        rewrite_jsonl(path, records)
    else:
        report = json.loads(path.read_text(encoding="utf-8"))
        if filename == "benchmark.json":
            report["evidence"]["code_revision"] = "f" * 40
        else:
            report["code_revision"] = "f" * 40
        path.write_text(json.dumps(report), encoding="utf-8")

    with pytest.raises(EvidenceExportError, match="does not match expected revision"):
        export(source, tmp_path / "output")


def test_usability_raw_counts_and_approval_links_are_verified(tmp_path: Path) -> None:
    source = tmp_path / "source"
    write_source(source)
    usability_path = source / "usability.json"
    usability = json.loads(usability_path.read_text(encoding="utf-8"))
    usability["success_rate"] = 0.8
    usability_path.write_text(json.dumps(usability), encoding="utf-8")

    with pytest.raises(EvidenceExportError, match="success_rate does not match raw counts"):
        export(source, tmp_path / "first-output")

    usability["success_rate"] = 1.0
    usability_path.write_text(json.dumps(usability), encoding="utf-8")
    approval_path = source / "release-approval.json"
    approval = json.loads(approval_path.read_text(encoding="utf-8"))
    approval["usability_dataset_sha256"] = "9" * 64
    approval_path.write_text(json.dumps(approval), encoding="utf-8")
    with pytest.raises(EvidenceExportError, match="does not match referenced evidence"):
        export(source, tmp_path / "second-output")


def test_cli_rejects_unverified_source_artifact_digest(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    source = tmp_path / "source"
    write_source(source)

    with pytest.raises(SystemExit) as exc:
        main(
            [
                "--source-dir",
                str(source),
                "--output-dir",
                str(tmp_path / "output"),
                "--expected-revision",
                REVISION,
                "--through-date",
                THROUGH_DATE.isoformat(),
                "--source-run-id",
                "1234",
                "--source-artifact-id",
                "5678",
                "--source-artifact-digest",
                "missing",
            ]
        )

    assert exc.value.code == 2
    assert "source artifact digest must be a SHA-256 digest" in capsys.readouterr().err


def test_scheduled_export_workflow_uses_protected_same_sha_source_artifact() -> None:
    workflow_path = ROOT / ".github" / "workflows" / "release-evidence.yml"
    text = workflow_path.read_text(encoding="utf-8")
    workflow = yaml.load(text, Loader=yaml.BaseLoader)

    assert 'cron: "45 15 * * *"' in text
    assert "KST 00:45" in text
    assert "github.event.repository.default_branch" in text
    assert "artifact.workflow_run?.head_sha" in text
    assert "run.data.conclusion !== \"success\"" in text
    assert "run.data.head_branch !== defaultBranch" in text
    assert 'run.data.path !== expectedWorkflowPath' in text
    assert 'const expectedWorkflowPath = ".github/workflows/release-evidence-inputs.yml"' in text
    assert "artifact.digest" in text
    download = next(
        step
        for step in workflow["jobs"]["export"]["steps"]
        if step.get("uses") == "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
    )
    assert download["with"]["artifact-ids"] == "${{ steps.source_artifact.outputs.artifact_id }}"
    assert download["with"]["digest-mismatch"] == "error"
    upload = next(
        step
        for step in workflow["jobs"]["export"]["steps"]
        if step.get("uses") == "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
    )
    assert upload["with"] == {
        "name": "governance-release-evidence",
        "path": "evidence-output",
        "if-no-files-found": "error",
        "retention-days": "90",
        "compression-level": "9",
    }
    assert "--expected-revision \"$GITHUB_SHA\"" in text
    assert "--source-artifact-digest \"$SOURCE_ARTIFACT_DIGEST\"" in text
