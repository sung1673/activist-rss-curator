from __future__ import annotations

import base64
import gzip
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from curator.global_alpha_evidence_bundle import (
    EvidenceBundleError,
    _validate_exact_evidence_file,
    decode_input_bundle,
    finalize_bundle,
    prepare_candidate_bundle,
    verify_materialized_review,
)


REVISION = "a" * 40
DIGEST = "c" * 64


def canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def write_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def provenance(
    kind: str,
    *,
    collected_at: datetime,
    evidence_source: str = "production_database_export",
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "kind": kind,
        "environment": "production",
        "evidence_source": evidence_source,
        "is_synthetic": False,
        "code_revision": REVISION,
        "collected_at": collected_at.isoformat(),
    }


def completed_windows(
    family: str,
    *,
    end: datetime,
) -> list[dict[str, object]]:
    start_date = end.date() - timedelta(days=30)
    return [
        {
            "window_start": (start_date + timedelta(days=index)).isoformat(),
            "window_end_exclusive": (
                start_date + timedelta(days=index + 1)
            ).isoformat(),
            "raw_count": 5,
            "filtered_out_count": 1,
            "accepted_count": 4,
            "acknowledged_count": 4,
            "status": "complete",
            "code_revision": REVISION,
            "receipt_sha256": hashlib.sha256(
                f"{family}:{index}:{REVISION}".encode()
            ).hexdigest(),
        }
        for index in range(30)
    ]


def content_integrity(collected_at: datetime) -> dict[str, object]:
    return {
        **provenance(
            "bside-global-alpha-content-integrity",
            collected_at=collected_at,
        ),
        "raw_counts": {
            "public_event_count": 60,
            "original_language_preserved_count": 60,
            "official_url_preserved_count": 60,
            "title_provenance_labeled_count": 60,
            "source_title_event_count": 50,
            "source_title_preserved_count": 50,
            "generated_metadata_title_count": 5,
            "operator_metadata_title_count": 5,
            "unknown_title_provenance_count": 0,
            "scanned_response_count": 600,
            "telegram_exposure_count": 0,
            "internal_field_exposure_count": 0,
            "persisted_snapshot_forbidden_key_count": 0,
        },
    }


def test_content_integrity_exact_contract_requires_snapshot_hygiene_count() -> None:
    report = content_integrity(datetime.now(timezone.utc))
    _validate_exact_evidence_file("content-integrity.json", report)

    del report["raw_counts"]["persisted_snapshot_forbidden_key_count"]  # type: ignore[index]
    with pytest.raises(
        EvidenceBundleError,
        match="persisted_snapshot_forbidden_key_count",
    ):
        _validate_exact_evidence_file("content-integrity.json", report)


def automated_response(collected_at: datetime) -> dict[str, object]:
    coverage = []
    for family, country in (
        ("dart", "KR"),
        ("sec-edgar", "US"),
    ):
        windows = completed_windows(family, end=collected_at)
        coverage.append(
            {
                "connector_family": family,
                "country": country,
                "coverage_started_at": (
                    datetime.combine(
                        collected_at.date() - timedelta(days=30),
                        datetime.min.time(),
                        timezone.utc,
                    )
                ).isoformat(),
                "coverage_ended_at": (
                    datetime.combine(
                        collected_at.date(),
                        datetime.min.time(),
                        timezone.utc,
                    )
                ).isoformat(),
                "successful_window_count": 30,
                "failed_window_count": 0,
                "completed_windows": windows,
            }
        )
    return {
        "ok": True,
        "api_version": "v2",
        "data": {
            **provenance(
                "bside-global-alpha-automated-evidence",
                collected_at=collected_at,
            ),
            "connector_coverage": coverage,
            "content_integrity": content_integrity(collected_at),
        },
    }


def review_export(collected_at: datetime) -> dict[str, object]:
    return {
        **provenance(
            "bside-global-alpha-review-candidate-export",
            collected_at=collected_at,
        ),
        "event_candidates": [
            {
                "event_id": f"event-{index:03d}",
                "title": f"Official event {index}",
                "issuer_name": f"Issuer {index % 12}",
                "country": ("KR", "US", "JP", "GB", "CA", "AU")[index % 6],
                "event_family": f"family-{index % 8}",
                "importance": ("high", "medium", "low")[index % 3],
                "verification_status": "official",
                "official_document_ids": [f"event-document-{index:03d}"],
                "official_urls": [f"https://example.invalid/event/{index}"],
            }
            for index in range(66)
        ],
        "same_event_pair_candidates": [
            {
                "pair_id": f"pair-{index:03d}",
                "left_document_id": f"left-{index:03d}",
                "right_document_id": f"right-{index:03d}",
                "left_title": f"Left {index}",
                "right_title": f"Right {index}",
                "left_url": f"https://example.invalid/left/{index}",
                "right_url": f"https://example.invalid/right/{index}",
                "stratum": (
                    "predicted_same",
                    "hard_negative",
                    "easy_negative",
                )[index % 3],
            }
            for index in range(126)
        ],
        "top5_candidates": [
            {
                "edition_id": "brief-global-20260725",
                "event_id": f"top-{position}",
                "position_no": position,
                "title": f"Top {position}",
                "official_url": f"https://example.invalid/top/{position}",
            }
            for position in range(1, 6)
        ],
    }


def prepare(tmp_path: Path) -> tuple[Path, Path, Path, datetime]:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    automated_path = tmp_path / "automated.json"
    review_path = tmp_path / "review-export.json"
    write_json(automated_path, automated_response(now))
    write_json(review_path, review_export(now))
    output = tmp_path / "candidate"
    prepare_candidate_bundle(
        automated_evidence_path=automated_path,
        review_candidate_export_path=review_path,
        output_dir=output,
        expected_revision=REVISION,
    )
    return output, automated_path, review_path, now


def fill_human_review(path: Path, reviewed_at: datetime) -> None:
    value = json.loads(path.read_text(encoding="utf-8"))
    value.update(
        {
            "evidence_source": "independent_human_oversight_record",
            "collected_at": reviewed_at.isoformat(),
            "ground_truth_source": "human",
            "ai_generated_ground_truth": False,
            "human_attestation": True,
        }
    )
    for record in value["event_reviews"]:
        record.update(
            {
                "decision": "approved",
                "reviewer_type": "human",
                "reviewer_reference": "oversight-1",
                "reviewed_at": reviewed_at.isoformat(),
            }
        )
    for index, record in enumerate(value["same_event_pair_reviews"]):
        record.update(
            {
                "decision": index % 2 == 0,
                "reviewer_type": "human",
                "reviewer_reference": "oversight-1",
                "reviewed_at": reviewed_at.isoformat(),
            }
        )
    for record in value["top5_reviews"]:
        record.update(
            {
                "decision": "approved",
                "reviewer_type": "human",
                "reviewer_reference": "oversight-1",
                "reviewed_at": reviewed_at.isoformat(),
            }
        )
    value["raw_counts"]["top5_human_reviewed_count"] = 5
    write_json(path, value)


def fill_connector(path: Path) -> None:
    value = json.loads(path.read_text(encoding="utf-8"))
    value["evidence_source"] = (
        "production_database_export_with_protected_replay_audit"
    )
    for item in value["connectors"]:
        payload = hashlib.sha256(
            str(item["connector_family"]).encode()
        ).hexdigest()
        item.update(
            {
                "payload_sha256": payload,
                "first_run": {
                    "raw_count": 5,
                    "filtered_out_count": 1,
                    "accepted_count": 4,
                    "acknowledged_count": 4,
                    "idempotent": False,
                },
                "replay_run": {
                    "raw_count": 5,
                    "filtered_out_count": 1,
                    "accepted_count": 4,
                    "acknowledged_count": 4,
                    "idempotent": True,
                    "payload_sha256": payload,
                },
                "row_count_after_first": 4,
                "row_count_after_replay": 4,
                "duplicate_row_count": 0,
                "checkpoint_after_first": "checkpoint-30",
                "checkpoint_after_replay": "checkpoint-30",
            }
        )
    write_json(path, value)


def fill_experience(path: Path, collected_at: datetime) -> None:
    value = json.loads(path.read_text(encoding="utf-8"))
    value.update(
        {
            "evidence_source": "production_browser_and_operations_observation",
            "collected_at": collected_at.isoformat(),
        }
    )
    for item in value["viewports"]:
        item.update(
            {
                "visual_regression_passed": True,
                "axe_serious_count": 0,
                "axe_critical_count": 0,
            }
        )
        if item["viewport"] == "390x844":
            item["first_important_event_top_px"] = 250
    value["web_vitals"] = {
        "lcp": {"p75_seconds": 2.1, "sample_count": 20},
        "inp": {"p75_ms": 150, "sample_count": 20},
        "cls": {"p75": 0.05, "sample_count": 20},
    }
    for item in value["api_responses"]:
        item["size_bytes"] = 120_000
        item["http_status"] = 200
    value["failure_detection_drill"] = {
        "incident_started_at": collected_at.isoformat(),
        "detected_at": (collected_at + timedelta(minutes=7)).isoformat(),
        "detection_minutes": 7,
    }
    value["rollback_drill"] = {
        "succeeded": True,
        "duration_minutes": 8,
        "started_at": collected_at.isoformat(),
        "completed_at": (collected_at + timedelta(minutes=8)).isoformat(),
        "legacy_artifact_sha256": DIGEST,
    }
    write_json(path, value)


def fill_approval(path: Path, collected_at: datetime) -> None:
    value = json.loads(path.read_text(encoding="utf-8"))
    value.update(
        {
            "evidence_source": "independent_human_release_approval",
            "collected_at": collected_at.isoformat(),
        }
    )
    for record in value["approvals"]:
        record.update(
            {
                "decision": "approved",
                "approver_type": "human",
                "approver_reference": "oversight-1",
                "decided_at": collected_at.isoformat(),
                "evidence_sha256": DIGEST,
            }
        )
    for record in value["source_right_scope"]:
        record.update(
            {
                "decision": "approved",
                "valid_source_right_count": 1,
                "invalid_source_right_count": 0,
            }
        )
    write_json(path, value)


def fill_experience_manifest(
    path: Path,
    artifact_root: Path,
    approved_at: datetime,
) -> None:
    value = json.loads(path.read_text(encoding="utf-8"))
    artifact_root.mkdir()
    for item in value["viewports"]:
        viewport = item["viewport"]
        screenshot = artifact_root / f"{viewport}.png"
        axe = artifact_root / f"{viewport}-axe.json"
        screenshot.write_bytes(f"screenshot:{viewport}".encode())
        axe.write_bytes(f'{{"viewport":"{viewport}","violations":[]}}'.encode())
        item.update(
            {
                "screenshot_path": screenshot.name,
                "screenshot_sha256": hashlib.sha256(
                    screenshot.read_bytes()
                ).hexdigest(),
                "axe_report_path": axe.name,
                "axe_report_sha256": hashlib.sha256(
                    axe.read_bytes()
                ).hexdigest(),
            }
        )
    for item in value["measurements"]:
        artifact = artifact_root / f"{item['name']}.json"
        artifact.write_bytes(f'{{"name":"{item["name"]}"}}'.encode())
        item.update(
            {
                "path": artifact.name,
                "sha256": hashlib.sha256(artifact.read_bytes()).hexdigest(),
            }
        )
    value["release_eligible"] = True
    value["human_approval"] = {
        "decision": "approved",
        "approver_type": "human",
        "approver_reference": "oversight-1",
        "decided_at": approved_at.isoformat(),
    }
    write_json(path, value)


def fill_all(
    candidate_dir: Path,
    *,
    collected_at: datetime,
    artifact_root: Path,
) -> None:
    fill_connector(candidate_dir / "connector-idempotency.json")
    fill_human_review(candidate_dir / "human-review.json", collected_at)
    fill_experience(candidate_dir / "experience.json", collected_at)
    fill_approval(candidate_dir / "approval.json", collected_at)
    fill_experience_manifest(
        candidate_dir / "experience-artifact-manifest.json",
        artifact_root,
        collected_at,
    )


def test_prepare_generates_exact_blank_human_candidates(
    tmp_path: Path,
) -> None:
    candidate_dir, automated_path, _, _ = prepare(tmp_path)
    assert automated_path.is_file()
    human = json.loads(
        (candidate_dir / "human-review.json").read_text(encoding="utf-8")
    )
    assert len(human["event_reviews"]) == 60
    assert len(human["same_event_pair_reviews"]) == 120
    assert len(human["top5_reviews"]) == 5
    assert human["ground_truth_source"] is None
    assert human["human_attestation"] is False
    assert human["ai_generated_ground_truth"] is False
    assert {record["decision"] for record in human["event_reviews"]} == {None}
    assert {record["reviewer_type"] for record in human["event_reviews"]} == {
        None
    }
    manifest = json.loads(
        (candidate_dir / "experience-artifact-manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["release_eligible"] is False
    assert manifest["human_approval"]["decision"] is None
    assert {item["viewport"] for item in manifest["viewports"]} == {
        "390x844",
        "768x1024",
        "1440x900",
    }
    connector = json.loads(
        (candidate_dir / "connector-idempotency.json").read_text(
            encoding="utf-8"
        )
    )
    assert {item["connector_family"] for item in connector["connectors"]} == {
        "dart",
        "sec-edgar",
    }
    assert connector["connectors"][0]["payload_sha256"] is None


def test_verify_materialized_review_binds_exact_immutable_export(
    tmp_path: Path,
) -> None:
    candidate_dir, _, review_path, collected_at = prepare(tmp_path)
    immutable_export = (
        tmp_path / "global-alpha-review-candidate-export.json"
    )
    review_path.replace(immutable_export)
    human_review = candidate_dir / "human-review.json"
    fill_human_review(human_review, collected_at)
    output = tmp_path / "review-candidate-provenance.json"

    report = verify_materialized_review(
        review_candidate_export_path=immutable_export,
        human_review_path=human_review,
        expected_revision=REVISION,
        producer_run_id="12345",
        producer_run_attempt="2",
        producer_run_created_at=(collected_at - timedelta(hours=1)).isoformat(),
        artifact_id="67890",
        artifact_name=f"global-alpha-review-candidates-{REVISION}",
        artifact_digest=f"sha256:{DIGEST}",
        output_path=output,
        verified_at=collected_at + timedelta(seconds=10),
    )

    export_value = json.loads(immutable_export.read_text(encoding="utf-8"))
    human_value = json.loads(human_review.read_text(encoding="utf-8"))
    assert report["producer_workflow"] == (
        ".github/workflows/global-alpha-review-candidates.yml"
    )
    assert report["producer_run_id"] == "12345"
    assert report["artifact_id"] == "67890"
    assert report["artifact_digest"] == f"sha256:{DIGEST}"
    assert report["review_candidate_export_bytes"] == len(
        immutable_export.read_bytes()
    )
    assert report["review_candidate_export_file_sha256"] == hashlib.sha256(
        immutable_export.read_bytes()
    ).hexdigest()
    assert report["review_candidate_export_canonical_sha256"] == hashlib.sha256(
        canonical(export_value)
    ).hexdigest()
    assert report["human_review_canonical_sha256"] == hashlib.sha256(
        canonical(human_value)
    ).hexdigest()
    assert report["event_review_count"] == 60
    assert report["same_event_pair_review_count"] == 120
    assert report["top5_review_count"] == 5
    assert report["human_review_verified"] is True
    assert json.loads(output.read_text(encoding="utf-8")) == report


def test_verify_materialized_review_rejects_changed_review_identity(
    tmp_path: Path,
) -> None:
    candidate_dir, _, review_path, collected_at = prepare(tmp_path)
    immutable_export = (
        tmp_path / "global-alpha-review-candidate-export.json"
    )
    review_path.replace(immutable_export)
    human_review = candidate_dir / "human-review.json"
    fill_human_review(human_review, collected_at)
    human_value = json.loads(human_review.read_text(encoding="utf-8"))
    human_value["event_reviews"][0]["event_id"] = "substituted-event"
    write_json(human_review, human_value)

    with pytest.raises(EvidenceBundleError, match="immutable candidate selection"):
        verify_materialized_review(
            review_candidate_export_path=immutable_export,
            human_review_path=human_review,
            expected_revision=REVISION,
            producer_run_id="12345",
            producer_run_attempt="1",
            producer_run_created_at=collected_at.isoformat(),
            artifact_id="67890",
            artifact_name=f"global-alpha-review-candidates-{REVISION}",
            artifact_digest=f"sha256:{DIGEST}",
            output_path=tmp_path / "provenance.json",
            verified_at=collected_at + timedelta(seconds=10),
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    (
        ({"producer_run_id": "0"}, "positive decimal identifier"),
        ({"artifact_name": "wrong-artifact"}, "artifact_name must be"),
        ({"artifact_digest": DIGEST}, "sha256:<digest>"),
        (
            {
                "producer_run_created_at": (
                    datetime.now(timezone.utc) - timedelta(hours=73)
                ).isoformat()
            },
            "72-hour freshness window",
        ),
    ),
)
def test_verify_materialized_review_rejects_invalid_producer_provenance(
    tmp_path: Path,
    overrides: dict[str, str],
    message: str,
) -> None:
    candidate_dir, _, review_path, collected_at = prepare(tmp_path)
    immutable_export = (
        tmp_path / "global-alpha-review-candidate-export.json"
    )
    review_path.replace(immutable_export)
    human_review = candidate_dir / "human-review.json"
    fill_human_review(human_review, collected_at)
    arguments: dict[str, object] = {
        "review_candidate_export_path": immutable_export,
        "human_review_path": human_review,
        "expected_revision": REVISION,
        "producer_run_id": "12345",
        "producer_run_attempt": "1",
        "producer_run_created_at": collected_at.isoformat(),
        "artifact_id": "67890",
        "artifact_name": f"global-alpha-review-candidates-{REVISION}",
        "artifact_digest": f"sha256:{DIGEST}",
        "output_path": tmp_path / "provenance.json",
        "verified_at": collected_at + timedelta(seconds=10),
    }
    arguments.update(overrides)

    with pytest.raises(EvidenceBundleError, match=message):
        verify_materialized_review(**arguments)  # type: ignore[arg-type]


def test_unedited_candidate_cannot_be_finalized(tmp_path: Path) -> None:
    candidate_dir, automated_path, review_path, collected_at = prepare(tmp_path)
    with pytest.raises(EvidenceBundleError, match="human release eligibility"):
        finalize_bundle(
            input_dir=candidate_dir,
            automated_evidence_path=automated_path,
            review_candidate_export_path=review_path,
            experience_artifact_root=tmp_path / "artifacts",
            expected_revision=REVISION,
            evidence_as_of=collected_at + timedelta(minutes=10),
        )


def test_finalize_is_deterministic_and_reuses_release_gate_validation(
    tmp_path: Path,
) -> None:
    candidate_dir, automated_path, review_path, collected_at = prepare(tmp_path)
    artifact_root = tmp_path / "artifacts"
    fill_all(
        candidate_dir,
        collected_at=collected_at,
        artifact_root=artifact_root,
    )
    first, first_summary = finalize_bundle(
        input_dir=candidate_dir,
        automated_evidence_path=automated_path,
        review_candidate_export_path=review_path,
        experience_artifact_root=artifact_root,
        expected_revision=REVISION,
        evidence_as_of=collected_at + timedelta(minutes=10),
    )
    second, second_summary = finalize_bundle(
        input_dir=candidate_dir,
        automated_evidence_path=automated_path,
        review_candidate_export_path=review_path,
        experience_artifact_root=artifact_root,
        expected_revision=REVISION,
        evidence_as_of=collected_at + timedelta(minutes=10),
    )
    assert first == second
    assert first_summary == second_summary
    assert first_summary["encoded_bytes"] <= 48_000
    assert first_summary["compressed_bytes"] <= 48_000
    assert first_summary["uncompressed_bytes"] <= 2_000_000
    decoded = decode_input_bundle(first)
    assert decoded["code_revision"] == REVISION
    assert set(decoded["files"]) == {
        "connector-idempotency.json",
        "human-review.json",
        "content-integrity.json",
        "experience.json",
        "approval.json",
    }


def test_finalize_rejects_extra_fields_and_changed_sample(
    tmp_path: Path,
) -> None:
    candidate_dir, automated_path, review_path, collected_at = prepare(tmp_path)
    artifact_root = tmp_path / "artifacts"
    fill_all(
        candidate_dir,
        collected_at=collected_at,
        artifact_root=artifact_root,
    )
    approval_path = candidate_dir / "approval.json"
    approval = json.loads(approval_path.read_text(encoding="utf-8"))
    approval["unexpected"] = True
    write_json(approval_path, approval)
    with pytest.raises(EvidenceBundleError, match="unexpected"):
        finalize_bundle(
            input_dir=candidate_dir,
            automated_evidence_path=automated_path,
            review_candidate_export_path=review_path,
            experience_artifact_root=artifact_root,
            expected_revision=REVISION,
            evidence_as_of=collected_at + timedelta(minutes=10),
        )

    del approval["unexpected"]
    write_json(approval_path, approval)
    human_path = candidate_dir / "human-review.json"
    human = json.loads(human_path.read_text(encoding="utf-8"))
    human["event_reviews"][0]["event_id"] = "substituted-event"
    write_json(human_path, human)
    with pytest.raises(EvidenceBundleError, match="immutable candidate selection"):
        finalize_bundle(
            input_dir=candidate_dir,
            automated_evidence_path=automated_path,
            review_candidate_export_path=review_path,
            experience_artifact_root=artifact_root,
            expected_revision=REVISION,
            evidence_as_of=collected_at + timedelta(minutes=10),
        )


def test_finalize_rejects_changed_original_review_export(tmp_path: Path) -> None:
    candidate_dir, automated_path, review_path, collected_at = prepare(tmp_path)
    artifact_root = tmp_path / "artifacts"
    fill_all(
        candidate_dir,
        collected_at=collected_at,
        artifact_root=artifact_root,
    )
    altered = json.loads(review_path.read_text(encoding="utf-8"))
    altered["event_candidates"][0]["title"] = "altered after candidate preparation"
    altered_path = tmp_path / "altered-review-export.json"
    write_json(altered_path, altered)
    with pytest.raises(EvidenceBundleError, match="original review candidate export changed"):
        finalize_bundle(
            input_dir=candidate_dir,
            automated_evidence_path=automated_path,
            review_candidate_export_path=altered_path,
            experience_artifact_root=artifact_root,
            expected_revision=REVISION,
            evidence_as_of=collected_at + timedelta(minutes=10),
        )


def test_finalize_rejects_selection_and_human_review_changed_together(
    tmp_path: Path,
) -> None:
    candidate_dir, automated_path, review_path, collected_at = prepare(tmp_path)
    artifact_root = tmp_path / "artifacts"
    fill_all(
        candidate_dir,
        collected_at=collected_at,
        artifact_root=artifact_root,
    )
    selection_path = candidate_dir / "review-candidates.json"
    human_path = candidate_dir / "human-review.json"
    selection = json.loads(selection_path.read_text(encoding="utf-8"))
    human = json.loads(human_path.read_text(encoding="utf-8"))
    selection["event_candidates"][0]["event_id"] = "jointly-substituted-event"
    human["event_reviews"][0]["event_id"] = "jointly-substituted-event"
    write_json(selection_path, selection)
    write_json(human_path, human)
    with pytest.raises(
        EvidenceBundleError,
        match="differs from deterministic selection of the original export",
    ):
        finalize_bundle(
            input_dir=candidate_dir,
            automated_evidence_path=automated_path,
            review_candidate_export_path=review_path,
            experience_artifact_root=artifact_root,
            expected_revision=REVISION,
            evidence_as_of=collected_at + timedelta(minutes=10),
        )


def test_decoder_rejects_concatenated_gzip_and_trailing_json() -> None:
    minimal = canonical(
        {
            "schema_version": 1,
            "kind": "bside-global-production-alpha-release-inputs",
            "code_revision": REVISION,
            "files": {},
        }
    )
    member = gzip.compress(minimal, mtime=0)
    concatenated = base64.b64encode(member + member).decode()
    with pytest.raises(EvidenceBundleError, match="concatenated"):
        decode_input_bundle(concatenated)

    trailing_json = base64.b64encode(gzip.compress(b"{}{}", mtime=0)).decode()
    with pytest.raises(EvidenceBundleError, match="concatenated, or trailing JSON"):
        decode_input_bundle(trailing_json)
