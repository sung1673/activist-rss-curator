from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest

from curator.config import DEFAULT_CONFIG
from curator.quality_benchmark import (
    BenchmarkDataError,
    ReleaseThresholds,
    build_release_report,
    build_review_process_evidence,
    calculate_binary_metrics,
    load_jsonl,
    main as benchmark_main,
    predict_relevance,
    predict_same_story,
)


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "quality_benchmark"


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _production_review_files(tmp_path: Path) -> dict[str, Path]:
    same_candidates: list[dict[str, object]] = []
    same_strata = [
        *("predicted_same" for _ in range(300)),
        *("hard_negative" for _ in range(250)),
        *("easy_negative" for _ in range(100)),
    ]
    for index, stratum in enumerate(same_strata):
        same_candidates.append(
            {
                "schema_version": 1,
                "task": "same_event_candidate",
                "pair_id": f"pair-{index:04d}",
                "left": {
                    "article_id": f"left-{index:04d}",
                    "title": f"Left {index}",
                    "published_at": "2026-07-01T00:00:00+09:00",
                },
                "right": {
                    "article_id": f"right-{index:04d}",
                    "title": f"Right {index}",
                    "published_at": "2026-07-01T00:05:00+09:00",
                },
                "stratum": stratum,
                "label": None,
                "label_source": None,
            }
        )
    relevance_candidates: list[dict[str, object]] = []
    for index in range(420):
        official = index < 300
        relevance_candidates.append(
            {
                "schema_version": 1,
                "task": "relevance",
                "sample_id": f"sample-{index:04d}",
                "event_id": f"event-{index:04d}",
                "article": {
                    "article_id": f"relevance-article-{index:04d}",
                    "title": f"Event evidence {index}",
                    "published_at": "2026-07-01T00:00:00+09:00",
                },
                "stratum": "official_event"
                if official
                else "non_governance_hard_negative",
                "linked_document_ids": [f"document-{index:04d}"] if official else [],
                "label": None,
                "label_source": None,
                "annotator_id": None,
                "labeled_at": None,
            }
        )
    paths = {
        "manifest": tmp_path / "candidate_manifest.json",
        "same_candidates": tmp_path / "same_candidates.jsonl",
        "relevance_candidates": tmp_path / "relevance_candidates.jsonl",
        "same_a": tmp_path / "same_a.jsonl",
        "same_b": tmp_path / "same_b.jsonl",
        "relevance_a": tmp_path / "relevance_a.jsonl",
        "relevance_b": tmp_path / "relevance_b.jsonl",
        "same_final": tmp_path / "same_final.jsonl",
        "relevance_final": tmp_path / "relevance_final.jsonl",
        "pilot": tmp_path / "pilot.json",
        "adjudication": tmp_path / "adjudication.jsonl",
    }
    _write_jsonl(paths["same_candidates"], same_candidates)
    _write_jsonl(paths["relevance_candidates"], relevance_candidates)
    manifest = {
        "schema_version": 1,
        "release_eligible": False,
        "same_event": {
            "required": {
                "predicted_same": 300,
                "hard_negative": 250,
                "easy_negative": 100,
            },
            "selected": {
                "predicted_same": 300,
                "hard_negative": 250,
                "easy_negative": 100,
            },
            "sha256": _sha256(paths["same_candidates"]),
        },
        "relevance": {
            "required": {
                "official_event": 300,
                "non_governance_hard_negative": 120,
            },
            "selected": {
                "official_event": 300,
                "non_governance_hard_negative": 120,
            },
            "sha256": _sha256(paths["relevance_candidates"]),
        },
    }
    paths["manifest"].write_text(json.dumps(manifest), encoding="utf-8")

    def labeled(
        candidates: list[dict[str, object]], *, task: str, reviewer: str, second: bool
    ) -> list[dict[str, object]]:
        result: list[dict[str, object]] = []
        for index, candidate in enumerate(candidates):
            row = deepcopy(candidate)
            if task == "same_story":
                row["task"] = "same_story"
                if index < 300:
                    label = "same_story"
                elif index < 550:
                    label = "related_but_different"
                else:
                    label = "different"
                if second and index == 60:
                    label = "different"
            else:
                label = "relevant" if index < 300 else "not_relevant"
                if second and index == 60:
                    label = "not_relevant"
            row["label"] = label
            row["label_source"] = "human"
            row["annotator_id"] = reviewer
            row["labeled_at"] = "2026-07-02T00:00:00+09:00"
            result.append(row)
        return result

    same_a = labeled(same_candidates, task="same_story", reviewer="reviewer-a", second=False)
    same_b = labeled(same_candidates, task="same_story", reviewer="reviewer-b", second=True)
    relevance_a = labeled(
        relevance_candidates, task="relevance", reviewer="reviewer-a", second=False
    )
    relevance_b = labeled(
        relevance_candidates, task="relevance", reviewer="reviewer-b", second=True
    )
    same_final = deepcopy(same_a)
    same_final[60]["label_source"] = "adjudicated"
    same_final[60]["annotator_id"] = "reviewer-a+reviewer-b"
    relevance_final = deepcopy(relevance_a)
    relevance_final[60]["label_source"] = "adjudicated"
    relevance_final[60]["annotator_id"] = "reviewer-a+reviewer-b"
    for key, rows in (
        ("same_a", same_a),
        ("same_b", same_b),
        ("relevance_a", relevance_a),
        ("relevance_b", relevance_b),
        ("same_final", same_final),
        ("relevance_final", relevance_final),
    ):
        _write_jsonl(paths[key], rows)

    same_pilot_ids = [f"pair-{index:04d}" for index in range(50)]
    core_pilot_ids = [f"event-{index:04d}" for index in range(30)]
    pilot = {
        "schema_version": 1,
        "evaluated_at": "2026-07-02T01:00:00+09:00",
        "release_eligible": False,
        "pilot_passed": True,
        "guide_revision_required": False,
        "tasks": [
            {
                "task": "same_event",
                "item_count": 50,
                "item_ids": same_pilot_ids,
                "item_ids_sha256": hashlib.sha256(
                    json.dumps(
                        same_pilot_ids, ensure_ascii=False, separators=(",", ":")
                    ).encode()
                ).hexdigest(),
                "annotators": ["reviewer-a", "reviewer-b"],
                "reviewer_a_dataset_sha256": "a" * 64,
                "reviewer_b_dataset_sha256": "b" * 64,
                "cohen_kappa": 1.0,
                "threshold": 0.8,
                "passed": True,
                "disagreement_count": 0,
                "disagreement_ids": [],
            },
            {
                "task": "core_event",
                "item_count": 30,
                "item_ids": core_pilot_ids,
                "item_ids_sha256": hashlib.sha256(
                    json.dumps(
                        core_pilot_ids, ensure_ascii=False, separators=(",", ":")
                    ).encode()
                ).hexdigest(),
                "annotators": ["reviewer-a", "reviewer-b"],
                "reviewer_a_dataset_sha256": "c" * 64,
                "reviewer_b_dataset_sha256": "d" * 64,
                "cohen_kappa": 1.0,
                "threshold": 0.8,
                "passed": True,
                "disagreement_count": 0,
                "disagreement_ids": [],
            },
        ],
    }
    paths["pilot"].write_text(json.dumps(pilot), encoding="utf-8")
    adjudications = [
        {
            "schema_version": 1,
            "task": task,
            "item_id": item_id,
            "reviewer_a_label": label_a,
            "reviewer_b_label": label_b,
            "final_label": label_a,
            "decision_mode": "reviewer_consensus",
            "decided_by": ["reviewer-a", "reviewer-b"],
            "reason": "Independent reviewers reached consensus.",
            "decided_at": "2026-07-02T02:00:00+09:00",
            "unresolved": False,
        }
        for task, item_id, label_a, label_b in (
            ("same_story", "pair-0060", "same_story", "different"),
            ("relevance", "sample-0060", "relevant", "not_relevant"),
        )
    ]
    _write_jsonl(paths["adjudication"], adjudications)
    return paths


def _build_process(paths: dict[str, Path]) -> dict[str, object]:
    return build_review_process_evidence(
        candidate_manifest_path=paths["manifest"],
        same_story_candidates_path=paths["same_candidates"],
        relevance_candidates_path=paths["relevance_candidates"],
        pilot_agreement_path=paths["pilot"],
        same_story_reviewer_a_path=paths["same_a"],
        same_story_reviewer_b_path=paths["same_b"],
        relevance_reviewer_a_path=paths["relevance_a"],
        relevance_reviewer_b_path=paths["relevance_b"],
        adjudication_path=paths["adjudication"],
        final_same_story_records=load_jsonl(paths["same_final"], expected_task="same_story"),
        final_relevance_records=load_jsonl(paths["relevance_final"], expected_task="relevance"),
    )


def test_production_review_process_proves_exact_candidates_reviewers_and_adjudication(
    tmp_path: Path,
) -> None:
    process = _build_process(_production_review_files(tmp_path))
    assert process["contract"] == "independent-human-review-v1"
    assert process["candidate_files"]["same_story"]["strata"] == {  # type: ignore[index]
        "predicted_same": 300,
        "hard_negative": 250,
        "easy_negative": 100,
    }
    assert process["reviewers"]["reviewer_count"] == 2  # type: ignore[index]
    assert process["pilot"]["same_event"]["cohen_kappa"] == 1.0  # type: ignore[index]
    assert process["adjudication"]["disagreement_count"] == 2  # type: ignore[index]
    assert process["adjudication"]["unresolved_count"] == 0  # type: ignore[index]


def test_same_or_single_reviewer_fails_closed(tmp_path: Path) -> None:
    paths = _production_review_files(tmp_path)
    rows = [
        {**row, "annotator_id": "reviewer-a"}
        for row in load_jsonl(paths["same_b"], expected_task="same_story")
    ]
    _write_jsonl(paths["same_b"], rows)
    with pytest.raises(BenchmarkDataError, match="two different reviewers"):
        _build_process(paths)


def test_low_pilot_kappa_fails_closed(tmp_path: Path) -> None:
    paths = _production_review_files(tmp_path)
    pilot = json.loads(paths["pilot"].read_text(encoding="utf-8"))
    pilot["tasks"][0]["cohen_kappa"] = 0.79
    pilot["tasks"][0]["passed"] = False
    paths["pilot"].write_text(json.dumps(pilot), encoding="utf-8")
    with pytest.raises(BenchmarkDataError, match="kappa >= 0.8"):
        _build_process(paths)


def test_candidate_strata_shortage_fails_closed(tmp_path: Path) -> None:
    paths = _production_review_files(tmp_path)
    rows = paths["same_candidates"].read_text(encoding="utf-8").splitlines()
    paths["same_candidates"].write_text("\n".join(rows[:-1]) + "\n", encoding="utf-8")
    manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    manifest["same_event"]["sha256"] = _sha256(paths["same_candidates"])
    paths["manifest"].write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(BenchmarkDataError, match="candidate strata must be exact"):
        _build_process(paths)


@pytest.mark.parametrize("tamper", ["hash", "content", "id", "label"])
def test_candidate_or_final_tampering_fails_closed(tmp_path: Path, tamper: str) -> None:
    paths = _production_review_files(tmp_path)
    if tamper == "hash":
        manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
        manifest["same_event"]["sha256"] = "0" * 64
        paths["manifest"].write_text(json.dumps(manifest), encoding="utf-8")
        pattern = "SHA-256 mismatch"
    else:
        rows = load_jsonl(paths["same_a"], expected_task="same_story")
        if tamper == "content":
            rows[0]["left"]["title"] = "tampered title"  # type: ignore[index]
            pattern = "content/stratum changed"
        elif tamper == "id":
            rows[0]["pair_id"] = "unknown-pair"
            pattern = "ID mismatch"
        else:
            final_rows = load_jsonl(paths["same_final"], expected_task="same_story")
            final_rows[0]["label"] = "different"
            _write_jsonl(paths["same_final"], final_rows)
            pattern = "changed despite reviewer agreement"
            rows = []
        if rows:
            _write_jsonl(paths["same_a"], [dict(row) for row in rows])
    with pytest.raises(BenchmarkDataError, match=pattern):
        _build_process(paths)


@pytest.mark.parametrize("tamper", ["missing", "extra", "unresolved"])
def test_adjudication_must_exactly_resolve_every_disagreement(
    tmp_path: Path, tamper: str
) -> None:
    paths = _production_review_files(tmp_path)
    rows = [
        json.loads(line)
        for line in paths["adjudication"].read_text(encoding="utf-8").splitlines()
        if line
    ]
    if tamper == "missing":
        rows.pop()
        pattern = "exactly cover disagreements"
    elif tamper == "extra":
        extra = deepcopy(rows[0])
        extra["item_id"] = "pair-0001"
        rows.append(extra)
        pattern = "extra or duplicate"
    else:
        rows[0]["unresolved"] = True
        pattern = "unresolved adjudication is forbidden"
    _write_jsonl(paths["adjudication"], rows)
    with pytest.raises(BenchmarkDataError, match=pattern):
        _build_process(paths)


def test_binary_metrics_report_precision_recall_and_f1() -> None:
    metrics = calculate_binary_metrics(
        [True, True, False, False],
        [True, False, True, False],
    )
    assert metrics.true_positive == 1
    assert metrics.false_positive == 1
    assert metrics.true_negative == 1
    assert metrics.false_negative == 1
    assert metrics.precision == pytest.approx(0.5)
    assert metrics.recall == pytest.approx(0.5)
    assert metrics.f1 == pytest.approx(0.5)


def test_fixture_labels_are_rejected_by_release_loader() -> None:
    path = FIXTURE_ROOT / "same_story_pairs.sample.jsonl"
    with pytest.raises(BenchmarkDataError, match="not release-eligible"):
        load_jsonl(path, expected_task="same_story")
    records = load_jsonl(path, expected_task="same_story", allow_fixture_labels=True)
    assert len(records) == 3


def test_duplicate_sample_ids_fail_validation(tmp_path: Path) -> None:
    source = (FIXTURE_ROOT / "relevance_events.sample.jsonl").read_text(encoding="utf-8").splitlines()[0]
    path = tmp_path / "duplicates.jsonl"
    path.write_text(source + "\n" + source + "\n", encoding="utf-8")
    with pytest.raises(BenchmarkDataError, match="duplicate sample_id"):
        load_jsonl(path, expected_task="relevance", allow_fixture_labels=True)


def test_human_metrics_without_protected_review_process_are_not_release_eligible() -> None:
    pairs = deepcopy(
        load_jsonl(
            FIXTURE_ROOT / "same_story_pairs.sample.jsonl",
            expected_task="same_story",
            allow_fixture_labels=True,
        )
    )
    relevance = deepcopy(
        load_jsonl(
            FIXTURE_ROOT / "relevance_events.sample.jsonl",
            expected_task="relevance",
            allow_fixture_labels=True,
        )
    )
    for index, record in enumerate(pairs):
        record["label_source"] = "human"
        record["stratum"] = ("predicted_same", "hard_negative", "easy_negative")[index]
    for record in relevance:
        record["label_source"] = "human"
    for index, record in enumerate(relevance):
        if record["label"] == "not_relevant":
            record["stratum"] = "non_governance_hard_negative"
            record["linked_document_ids"] = []
        else:
            record["stratum"] = "official_event"
            record["linked_document_ids"] = [f"fixture-document-{index}"]
    thresholds = ReleaseThresholds(
        min_article_pairs=3,
        min_events=2,
        min_relevance_hard_negatives=1,
    )
    report = build_release_report(
        pairs,
        relevance,
        DEFAULT_CONFIG,
        thresholds,
        same_story_predictor=lambda row, _config: row["label"] == "same_story",
        relevance_predictor=lambda row, _config: row["label"] == "relevant",
    )
    assert report["release_gate_passed"] is False
    assert report["same_story"]["precision"] == 1.0  # type: ignore[index]
    assert report["relevance"]["recall"] == 1.0  # type: ignore[index]
    assert report["relevance"]["precision"] == 1.0  # type: ignore[index]
    assert report["relevance"]["official_linked_event_count"] == 2  # type: ignore[index]
    assert report["relevance"]["hard_negative_count"] == 1  # type: ignore[index]
    assert report["evidence"]["is_synthetic"] is True  # type: ignore[index]
    assert report["evidence"]["release_eligible"] is False  # type: ignore[index]
    assert "evidence.release_eligible" in report["failed_gates"]


def test_fixture_report_cannot_pass_even_with_lowered_sample_thresholds() -> None:
    pairs = load_jsonl(
        FIXTURE_ROOT / "same_story_pairs.sample.jsonl",
        expected_task="same_story",
        allow_fixture_labels=True,
    )
    relevance = load_jsonl(
        FIXTURE_ROOT / "relevance_events.sample.jsonl",
        expected_task="relevance",
        allow_fixture_labels=True,
    )
    report = build_release_report(
        pairs,
        relevance,
        DEFAULT_CONFIG,
        ReleaseThresholds(min_article_pairs=3, min_events=3),
        same_story_predictor=lambda row, _config: row["label"] == "same_story",
        relevance_predictor=lambda row, _config: row["label"] == "relevant",
    )
    assert report["release_gate_passed"] is False
    assert "evidence.release_eligible" in report["failed_gates"]


def test_cli_fixture_mode_is_explicit_report_only_and_never_release_eligible(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    output = tmp_path / "fixture-report.json"
    benchmark_main(
        [
            "--same-story",
            str(FIXTURE_ROOT / "same_story_pairs.sample.jsonl"),
            "--relevance",
            str(FIXTURE_ROOT / "relevance_events.sample.jsonl"),
            "--allow-fixture-labels",
            "--report-only",
            "--output",
            str(output),
        ]
    )
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["release_gate_passed"] is False
    assert report["evidence"]["fixture_mode"] is True
    assert report["evidence"]["release_eligible"] is False
    assert report["review_process"] is None
    assert json.loads(capsys.readouterr().out)["release_gate_passed"] is False


def test_cli_production_mode_requires_complete_review_process(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    paths = _production_review_files(tmp_path)
    with pytest.raises(SystemExit) as exc_info:
        benchmark_main(
            [
                "--same-story",
                str(paths["same_final"]),
                "--relevance",
                str(paths["relevance_final"]),
                "--environment",
                "production",
                "--code-revision",
                "a" * 40,
            ]
        )
    assert exc_info.value.code == 2
    error = json.loads(capsys.readouterr().err)
    assert error["status"] == "invalid-benchmark"
    assert "requires review-process inputs" in error["error"]


def test_default_release_gate_fails_small_fixture_even_with_perfect_predictions() -> None:
    pairs = load_jsonl(
        FIXTURE_ROOT / "same_story_pairs.sample.jsonl",
        expected_task="same_story",
        allow_fixture_labels=True,
    )
    relevance = load_jsonl(
        FIXTURE_ROOT / "relevance_events.sample.jsonl",
        expected_task="relevance",
        allow_fixture_labels=True,
    )
    report = build_release_report(
        pairs,
        relevance,
        DEFAULT_CONFIG,
        same_story_predictor=lambda row, _config: row["label"] == "same_story",
        relevance_predictor=lambda row, _config: row["label"] == "relevant",
    )
    assert report["release_gate_passed"] is False
    assert "same_story.article_pair_count" in report["failed_gates"]
    assert "relevance.official_linked_event_count" in report["failed_gates"]


def test_relevance_precision_default_fails_even_when_recall_is_perfect() -> None:
    pairs = deepcopy(
        load_jsonl(
            FIXTURE_ROOT / "same_story_pairs.sample.jsonl",
            expected_task="same_story",
            allow_fixture_labels=True,
        )
    )
    relevance = deepcopy(
        load_jsonl(
            FIXTURE_ROOT / "relevance_events.sample.jsonl",
            expected_task="relevance",
            allow_fixture_labels=True,
        )
    )
    for record in pairs:
        record["label_source"] = "human"
    for index, record in enumerate(pairs):
        record["stratum"] = ("predicted_same", "hard_negative", "easy_negative")[index]
    for index, record in enumerate(relevance):
        record["label_source"] = "human"
        record["stratum"] = "official_event" if record["label"] == "relevant" else "non_governance_hard_negative"
        record["linked_document_ids"] = [f"document-{index}"] if record["label"] == "relevant" else []
    report = build_release_report(
        pairs,
        relevance,
        DEFAULT_CONFIG,
        ReleaseThresholds(min_article_pairs=3, min_events=2, min_relevance_hard_negatives=1),
        same_story_predictor=lambda row, _config: row["label"] == "same_story",
        relevance_predictor=lambda _row, _config: True,
    )
    assert report["relevance"]["recall"] == 1.0  # type: ignore[index]
    assert report["relevance"]["precision"] == pytest.approx(2 / 3)  # type: ignore[index]
    assert "relevance.precision" in report["failed_gates"]


def test_production_predictors_use_current_clustering_and_relevance_rules() -> None:
    pairs = load_jsonl(
        FIXTURE_ROOT / "same_story_pairs.sample.jsonl",
        expected_task="same_story",
        allow_fixture_labels=True,
    )
    relevance = load_jsonl(
        FIXTURE_ROOT / "relevance_events.sample.jsonl",
        expected_task="relevance",
        allow_fixture_labels=True,
    )
    assert predict_same_story(pairs[0], DEFAULT_CONFIG)
    assert predict_relevance(relevance[0], DEFAULT_CONFIG)
    assert not predict_relevance(relevance[-1], DEFAULT_CONFIG)


def test_published_at_requires_timezone(tmp_path: Path) -> None:
    record = json.loads((FIXTURE_ROOT / "relevance_events.sample.jsonl").read_text(encoding="utf-8").splitlines()[0])
    record["article"]["published_at"] = "2026-07-16T09:00:00"
    path = tmp_path / "naive-time.jsonl"
    path.write_text(json.dumps(record, ensure_ascii=False) + "\n", encoding="utf-8")
    with pytest.raises(BenchmarkDataError, match="include a timezone"):
        load_jsonl(path, expected_task="relevance", allow_fixture_labels=True)


def test_json_schema_contract_files_are_valid_json() -> None:
    schema_root = Path(__file__).parents[1] / "docs" / "schemas"
    for name in (
        "same-story-pair.schema.json",
        "relevance-event.schema.json",
        "official-backfill-checkpoint.schema.json",
        "shadow-comparison-day.schema.json",
        "operations-gate-day.schema.json",
        "performance-gate-day.schema.json",
        "benchmark-adjudication.schema.json",
    ):
        schema = json.loads((schema_root / name).read_text(encoding="utf-8"))
        assert schema["$schema"].endswith("2020-12/schema")
