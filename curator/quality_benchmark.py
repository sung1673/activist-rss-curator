from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence

from .cluster import can_join_cluster, create_cluster, enrich_article_for_clustering
from .config import load_config
from .normalize import normalize_title_parts
from .relevance import relevance_details


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMA_VERSION = 1
RELEASE_LABEL_SOURCES = {"human", "adjudicated"}
ALL_LABEL_SOURCES = RELEASE_LABEL_SOURCES | {"fixture"}
SAME_STORY_LABELS = {"same_story", "related_but_different", "different"}
RELEVANCE_LABELS = {"relevant", "not_relevant"}
REVISION_RE = re.compile(r"^[0-9a-f]{7,64}$")


class BenchmarkDataError(ValueError):
    pass


@dataclass(frozen=True)
class BinaryMetrics:
    true_positive: int
    false_positive: int
    true_negative: int
    false_negative: int
    precision: float
    recall: float
    f1: float
    accuracy: float

    @property
    def sample_count(self) -> int:
        return self.true_positive + self.false_positive + self.true_negative + self.false_negative


@dataclass(frozen=True)
class ReleaseThresholds:
    min_article_pairs: int = 500
    min_events: int = 300
    same_story_min_precision: float = 0.97
    same_story_min_recall: float = 0.0
    same_story_min_f1: float = 0.0
    relevance_min_precision: float = 0.0
    relevance_min_recall: float = 0.95
    relevance_min_f1: float = 0.0

    def validate(self) -> None:
        if self.min_article_pairs < 1 or self.min_events < 1:
            raise BenchmarkDataError("minimum sample gates must be positive")
        for name, value in asdict(self).items():
            if name.startswith("min_"):
                continue
            if not 0.0 <= float(value) <= 1.0:
                raise BenchmarkDataError(f"{name} must be between 0 and 1")


def calculate_binary_metrics(actual: Iterable[bool], predicted: Iterable[bool]) -> BinaryMetrics:
    actual_values = list(actual)
    predicted_values = list(predicted)
    if len(actual_values) != len(predicted_values):
        raise BenchmarkDataError("actual and predicted lengths differ")
    true_positive = sum(1 for truth, guess in zip(actual_values, predicted_values) if truth and guess)
    false_positive = sum(1 for truth, guess in zip(actual_values, predicted_values) if not truth and guess)
    true_negative = sum(1 for truth, guess in zip(actual_values, predicted_values) if not truth and not guess)
    false_negative = sum(1 for truth, guess in zip(actual_values, predicted_values) if truth and not guess)
    precision_denominator = true_positive + false_positive
    recall_denominator = true_positive + false_negative
    precision = true_positive / precision_denominator if precision_denominator else 0.0
    recall = true_positive / recall_denominator if recall_denominator else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    count = len(actual_values)
    accuracy = (true_positive + true_negative) / count if count else 0.0
    return BinaryMetrics(
        true_positive=true_positive,
        false_positive=false_positive,
        true_negative=true_negative,
        false_negative=false_negative,
        precision=precision,
        recall=recall,
        f1=f1,
        accuracy=accuracy,
    )


def _require_text(value: object, field: str, location: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise BenchmarkDataError(f"{location}: {field} must be a non-empty string")
    return text


def _require_timestamp(value: object, field: str, location: str) -> str:
    text = _require_text(value, field, location)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BenchmarkDataError(f"{location}: {field} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise BenchmarkDataError(f"{location}: {field} must include a timezone")
    return text


def _validate_article(value: object, field: str, location: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise BenchmarkDataError(f"{location}: {field} must be an object")
    _require_text(value.get("article_id"), f"{field}.article_id", location)
    _require_text(value.get("title"), f"{field}.title", location)
    _require_timestamp(value.get("published_at"), f"{field}.published_at", location)
    for array_field in ("company_candidates", "topic_keywords"):
        array_value = value.get(array_field)
        if array_value is not None and (
            not isinstance(array_value, list) or any(not isinstance(item, str) for item in array_value)
        ):
            raise BenchmarkDataError(f"{location}: {field}.{array_field} must be an array of strings")
    return value


def _validate_record(
    value: object,
    *,
    expected_task: str,
    location: str,
    allow_fixture_labels: bool,
) -> dict[str, object]:
    if not isinstance(value, dict):
        raise BenchmarkDataError(f"{location}: row must be a JSON object")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise BenchmarkDataError(f"{location}: schema_version must be {SCHEMA_VERSION}")
    if value.get("task") != expected_task:
        raise BenchmarkDataError(f"{location}: task must be {expected_task!r}")
    label_source = _require_text(value.get("label_source"), "label_source", location)
    if label_source not in ALL_LABEL_SOURCES:
        raise BenchmarkDataError(f"{location}: unsupported label_source {label_source!r}")
    if label_source not in RELEASE_LABEL_SOURCES and not allow_fixture_labels:
        raise BenchmarkDataError(
            f"{location}: label_source {label_source!r} is not release-eligible human evidence"
        )
    _require_text(value.get("annotator_id"), "annotator_id", location)
    _require_timestamp(value.get("labeled_at"), "labeled_at", location)

    if expected_task == "same_story":
        _require_text(value.get("pair_id"), "pair_id", location)
        left = _validate_article(value.get("left"), "left", location)
        right = _validate_article(value.get("right"), "right", location)
        if str(left.get("article_id")) == str(right.get("article_id")):
            raise BenchmarkDataError(f"{location}: a pair must contain two different article IDs")
        label = _require_text(value.get("label"), "label", location)
        if label not in SAME_STORY_LABELS:
            raise BenchmarkDataError(f"{location}: invalid same-story label {label!r}")
    elif expected_task == "relevance":
        _require_text(value.get("sample_id"), "sample_id", location)
        _require_text(value.get("event_id"), "event_id", location)
        _validate_article(value.get("article"), "article", location)
        label = _require_text(value.get("label"), "label", location)
        if label not in RELEVANCE_LABELS:
            raise BenchmarkDataError(f"{location}: invalid relevance label {label!r}")
    else:
        raise BenchmarkDataError(f"unsupported benchmark task {expected_task!r}")
    return value


def load_jsonl(
    path: Path,
    *,
    expected_task: str,
    allow_fixture_labels: bool = False,
) -> list[dict[str, object]]:
    if not path.exists():
        raise BenchmarkDataError(f"benchmark file does not exist: {path}")
    records: list[dict[str, object]] = []
    identifiers: set[str] = set()
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        location = f"{path}:{line_number}"
        try:
            raw_value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise BenchmarkDataError(f"{location}: invalid JSON: {exc.msg}") from exc
        record = _validate_record(
            raw_value,
            expected_task=expected_task,
            location=location,
            allow_fixture_labels=allow_fixture_labels,
        )
        identifier_field = "pair_id" if expected_task == "same_story" else "sample_id"
        identifier = str(record.get(identifier_field))
        if identifier in identifiers:
            raise BenchmarkDataError(f"{location}: duplicate {identifier_field} {identifier!r}")
        identifiers.add(identifier)
        records.append(record)
    if not records:
        raise BenchmarkDataError(f"benchmark file has no records: {path}")
    return records


def _article_for_clustering(value: Mapping[str, object]) -> dict[str, object]:
    title = str(value.get("title") or "")
    parts = normalize_title_parts(title)
    article_id = str(value.get("article_id") or "")
    published_at = str(value.get("published_at") or "")
    url = str(value.get("canonical_url") or f"https://benchmark.invalid/articles/{article_id}")
    return {
        "article_id": article_id,
        "title": title,
        "clean_title": parts["clean_title"],
        "normalized_title": parts["normalized_title"],
        "title_hash": parts["title_hash"],
        "summary": str(value.get("summary") or ""),
        "source": str(value.get("source") or "benchmark"),
        "canonical_url": url,
        "link": url,
        "published_at": published_at,
        "article_published_at": published_at,
        "feed_published_at": published_at,
        "company_candidates": list(value.get("company_candidates") or []),
        "topic_keywords": list(value.get("topic_keywords") or []),
        "relevance_level": "medium",
    }


def _parsed_article_time(value: Mapping[str, object]) -> datetime:
    return datetime.fromisoformat(str(value.get("published_at") or "").replace("Z", "+00:00"))


def rules_only_config(config: Mapping[str, object]) -> dict[str, object]:
    benchmark_config = deepcopy(dict(config))
    ai = benchmark_config.get("ai")
    ai_settings = dict(ai) if isinstance(ai, dict) else {}
    ai_settings["enabled"] = False
    ai_settings["story_judge_enabled"] = False
    benchmark_config["ai"] = ai_settings
    return benchmark_config


def predict_same_story(record: Mapping[str, object], config: Mapping[str, object]) -> bool:
    left_value = record.get("left")
    right_value = record.get("right")
    if not isinstance(left_value, dict) or not isinstance(right_value, dict):
        raise BenchmarkDataError("same-story record is missing article objects")
    left = _article_for_clustering(left_value)
    right = enrich_article_for_clustering(_article_for_clustering(right_value))
    now = max(_parsed_article_time(left_value), _parsed_article_time(right_value))
    local_config = rules_only_config(config)
    cluster = create_cluster(left, now, {"pending_clusters": [], "published_clusters": []})
    return can_join_cluster(right, cluster, local_config, now)


def relevant_levels(config: Mapping[str, object]) -> set[str]:
    publish = config.get("publish")
    values = publish.get("publish_levels") if isinstance(publish, dict) else None
    levels = {str(value) for value in values} if isinstance(values, list) else {"high", "medium"}
    return levels or {"high", "medium"}


def predict_relevance(record: Mapping[str, object], config: Mapping[str, object]) -> bool:
    article = record.get("article")
    if not isinstance(article, dict):
        raise BenchmarkDataError("relevance record is missing article")
    details = relevance_details(str(article.get("title") or ""), str(article.get("summary") or ""))
    return str(details.get("level") or "low") in relevant_levels(config)


SameStoryPredictor = Callable[[Mapping[str, object], Mapping[str, object]], bool]
RelevancePredictor = Callable[[Mapping[str, object], Mapping[str, object]], bool]


def _metrics_dict(metrics: BinaryMetrics) -> dict[str, object]:
    return {
        "sample_count": metrics.sample_count,
        "confusion_matrix": {
            "true_positive": metrics.true_positive,
            "false_positive": metrics.false_positive,
            "true_negative": metrics.true_negative,
            "false_negative": metrics.false_negative,
        },
        "precision": round(metrics.precision, 6),
        "recall": round(metrics.recall, 6),
        "f1": round(metrics.f1, 6),
        "accuracy": round(metrics.accuracy, 6),
    }


def _dataset_sha256(records: Sequence[Mapping[str, object]]) -> str:
    canonical = "\n".join(
        json.dumps(dict(record), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        for record in records
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _label_sources(records: Sequence[Mapping[str, object]]) -> list[str]:
    return sorted({str(record.get("label_source") or "") for record in records})


def evaluate_same_story(
    records: Sequence[Mapping[str, object]],
    config: Mapping[str, object],
    *,
    predictor: SameStoryPredictor = predict_same_story,
) -> dict[str, object]:
    actual = [str(record.get("label")) == "same_story" for record in records]
    predicted = [predictor(record, config) for record in records]
    metrics = calculate_binary_metrics(actual, predicted)
    misclassified = [
        str(record.get("pair_id"))
        for record, truth, guess in zip(records, actual, predicted)
        if truth != guess
    ]
    return {
        "task": "same_story",
        "predictor": "production clustering rules with network AI disabled",
        "positive_label": "same_story",
        "actual_positive": sum(actual),
        "actual_negative": len(actual) - sum(actual),
        "misclassified_ids": misclassified[:100],
        "misclassified_count": len(misclassified),
        **_metrics_dict(metrics),
    }


def evaluate_relevance(
    records: Sequence[Mapping[str, object]],
    config: Mapping[str, object],
    *,
    predictor: RelevancePredictor = predict_relevance,
) -> dict[str, object]:
    actual = [str(record.get("label")) == "relevant" for record in records]
    predicted = [predictor(record, config) for record in records]
    metrics = calculate_binary_metrics(actual, predicted)
    event_ids = {str(record.get("event_id") or "") for record in records}
    misclassified = [
        str(record.get("sample_id"))
        for record, truth, guess in zip(records, actual, predicted)
        if truth != guess
    ]
    return {
        "task": "relevance",
        "predictor": "production relevance keyword classifier and publish levels",
        "positive_label": "relevant",
        "unique_event_count": len(event_ids),
        "actual_positive": sum(actual),
        "actual_negative": len(actual) - sum(actual),
        "misclassified_ids": misclassified[:100],
        "misclassified_count": len(misclassified),
        **_metrics_dict(metrics),
    }


def _gate(name: str, actual: float, minimum: float) -> dict[str, object]:
    return {
        "name": name,
        "actual": round(actual, 6),
        "minimum": round(minimum, 6),
        "passed": actual >= minimum,
    }


def build_release_report(
    same_story_records: Sequence[Mapping[str, object]],
    relevance_records: Sequence[Mapping[str, object]],
    config: Mapping[str, object],
    thresholds: ReleaseThresholds | None = None,
    *,
    same_story_predictor: SameStoryPredictor = predict_same_story,
    relevance_predictor: RelevancePredictor = predict_relevance,
    environment: str = "test",
    code_revision: str = "",
) -> dict[str, object]:
    release_thresholds = thresholds or ReleaseThresholds()
    release_thresholds.validate()
    same_story = evaluate_same_story(same_story_records, config, predictor=same_story_predictor)
    relevance = evaluate_relevance(relevance_records, config, predictor=relevance_predictor)
    gates = [
        _gate("same_story.article_pair_count", float(same_story["sample_count"]), release_thresholds.min_article_pairs),
        _gate("same_story.has_positive_class", float(int(same_story["actual_positive"]) > 0), 1.0),
        _gate("same_story.has_negative_class", float(int(same_story["actual_negative"]) > 0), 1.0),
        _gate("same_story.precision", float(same_story["precision"]), release_thresholds.same_story_min_precision),
        _gate("same_story.recall", float(same_story["recall"]), release_thresholds.same_story_min_recall),
        _gate("same_story.f1", float(same_story["f1"]), release_thresholds.same_story_min_f1),
        _gate("relevance.unique_event_count", float(relevance["unique_event_count"]), release_thresholds.min_events),
        _gate("relevance.has_positive_class", float(int(relevance["actual_positive"]) > 0), 1.0),
        _gate("relevance.has_negative_class", float(int(relevance["actual_negative"]) > 0), 1.0),
        _gate("relevance.precision", float(relevance["precision"]), release_thresholds.relevance_min_precision),
        _gate("relevance.recall", float(relevance["recall"]), release_thresholds.relevance_min_recall),
        _gate("relevance.f1", float(relevance["f1"]), release_thresholds.relevance_min_f1),
    ]
    same_story_sources = _label_sources(same_story_records)
    relevance_sources = _label_sources(relevance_records)
    release_eligible = bool(same_story_sources and relevance_sources) and (
        set(same_story_sources) | set(relevance_sources)
    ) <= RELEASE_LABEL_SOURCES
    gates.append(_gate("evidence.release_eligible", float(release_eligible), 1.0))
    failed_gates = [str(gate["name"]) for gate in gates if not gate["passed"]]
    evaluated_at = datetime.now(timezone.utc).isoformat()
    return {
        "schema_version": 1,
        "evaluated_at": evaluated_at,
        "release_gate_passed": not failed_gates,
        "failed_gates": failed_gates,
        "evidence": {
            "schema_version": 1,
            "environment": environment,
            "evidence_source": "human_labeled_jsonl" if release_eligible else "fixture",
            "is_synthetic": not release_eligible,
            "collected_at": evaluated_at,
            "code_revision": code_revision.strip().casefold(),
            "release_eligible": release_eligible,
            "same_story_label_sources": same_story_sources,
            "relevance_label_sources": relevance_sources,
            "same_story_dataset_sha256": _dataset_sha256(same_story_records),
            "relevance_dataset_sha256": _dataset_sha256(relevance_records),
        },
        "thresholds": asdict(release_thresholds),
        "gates": gates,
        "same_story": same_story,
        "relevance": relevance,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate relevance and same-story release gates from JSONL labels.")
    parser.add_argument("--root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--same-story", type=Path, required=True, help="same-story article-pair JSONL")
    parser.add_argument("--relevance", type=Path, required=True, help="relevance event JSONL")
    parser.add_argument("--min-article-pairs", type=int, default=500)
    parser.add_argument("--min-events", type=int, default=300)
    parser.add_argument("--same-story-min-precision", type=float, default=0.97)
    parser.add_argument("--same-story-min-recall", type=float, default=0.0)
    parser.add_argument("--same-story-min-f1", type=float, default=0.0)
    parser.add_argument("--relevance-min-precision", type=float, default=0.0)
    parser.add_argument("--relevance-min-recall", type=float, default=0.95)
    parser.add_argument("--relevance-min-f1", type=float, default=0.0)
    parser.add_argument(
        "--report-only",
        action="store_true",
        help="print metrics without making an unmet release gate fail the command",
    )
    parser.add_argument(
        "--allow-fixture-labels",
        action="store_true",
        help="accept label_source=fixture (only valid with --report-only)",
    )
    parser.add_argument(
        "--environment",
        choices=("production", "staging", "test"),
        default="test",
        help="evidence environment; formal release evidence must use production",
    )
    parser.add_argument(
        "--code-revision",
        default=os.environ.get("GITHUB_SHA", ""),
        help="7-64 character hexadecimal revision evaluated by this benchmark",
    )
    parser.add_argument("--output", type=Path, help="optionally write the UTF-8 JSON report to this path")
    return parser


def _resolved_input(project_root: Path, value: Path) -> Path:
    return value if value.is_absolute() else project_root / value


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    try:
        if args.allow_fixture_labels and not args.report_only:
            raise BenchmarkDataError("fixture labels are forbidden in release-gate mode")
        if not args.report_only and args.environment != "production":
            raise BenchmarkDataError("release-gate mode requires --environment production")
        if not args.report_only and not REVISION_RE.fullmatch(str(args.code_revision).strip().casefold()):
            raise BenchmarkDataError("release-gate mode requires a hexadecimal --code-revision")
        project_root = args.root.resolve()
        config = load_config(project_root / "config.yaml")
        same_story_records = load_jsonl(
            _resolved_input(project_root, args.same_story),
            expected_task="same_story",
            allow_fixture_labels=args.allow_fixture_labels,
        )
        relevance_records = load_jsonl(
            _resolved_input(project_root, args.relevance),
            expected_task="relevance",
            allow_fixture_labels=args.allow_fixture_labels,
        )
        thresholds = ReleaseThresholds(
            min_article_pairs=args.min_article_pairs,
            min_events=args.min_events,
            same_story_min_precision=args.same_story_min_precision,
            same_story_min_recall=args.same_story_min_recall,
            same_story_min_f1=args.same_story_min_f1,
            relevance_min_precision=args.relevance_min_precision,
            relevance_min_recall=args.relevance_min_recall,
            relevance_min_f1=args.relevance_min_f1,
        )
        report = build_release_report(
            same_story_records,
            relevance_records,
            config,
            thresholds,
            environment=args.environment,
            code_revision=args.code_revision,
        )
    except (BenchmarkDataError, OSError, ValueError) as exc:
        print(json.dumps({"status": "invalid-benchmark", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(2) from exc
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        output_path = _resolved_input(project_root, args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    if not args.report_only and not report["release_gate_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
