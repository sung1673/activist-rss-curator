from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest

from curator.config import DEFAULT_CONFIG
from curator.quality_benchmark import (
    BenchmarkDataError,
    ReleaseThresholds,
    build_release_report,
    calculate_binary_metrics,
    load_jsonl,
    predict_relevance,
    predict_same_story,
)


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "quality_benchmark"


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


def test_release_report_passes_only_when_samples_and_metrics_meet_thresholds() -> None:
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
    for record in [*pairs, *relevance]:
        record["label_source"] = "human"
    thresholds = ReleaseThresholds(min_article_pairs=3, min_events=3)
    report = build_release_report(
        pairs,
        relevance,
        DEFAULT_CONFIG,
        thresholds,
        same_story_predictor=lambda row, _config: row["label"] == "same_story",
        relevance_predictor=lambda row, _config: row["label"] == "relevant",
    )
    assert report["release_gate_passed"] is True
    assert report["same_story"]["precision"] == 1.0  # type: ignore[index]
    assert report["relevance"]["recall"] == 1.0  # type: ignore[index]
    assert report["evidence"]["is_synthetic"] is False  # type: ignore[index]
    assert report["evidence"]["release_eligible"] is True  # type: ignore[index]


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
    assert "relevance.unique_event_count" in report["failed_gates"]


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
    ):
        schema = json.loads((schema_root / name).read_text(encoding="utf-8"))
        assert schema["$schema"].endswith("2020-12/schema")
