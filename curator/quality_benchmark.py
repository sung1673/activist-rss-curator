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
SAME_STORY_STRATA = {"predicted_same", "hard_negative", "easy_negative"}
RELEVANCE_LABELS = {"relevant", "not_relevant"}
RELEVANCE_STRATA = {"official_event", "non_governance_hard_negative"}
REVISION_RE = re.compile(r"^[0-9a-f]{7,64}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
PRODUCTION_SAME_STORY_STRATA = {
    "predicted_same": 300,
    "hard_negative": 250,
    "easy_negative": 100,
}
PRODUCTION_RELEVANCE_STRATA = {
    "official_event": 300,
    "non_governance_hard_negative": 120,
}
PILOT_COUNTS = {"same_event": 50, "core_event": 30}
PILOT_KAPPA_MINIMUM = 0.8


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
    min_relevance_hard_negatives: int = 120
    same_story_min_precision: float = 0.97
    same_story_min_recall: float = 0.0
    same_story_min_f1: float = 0.0
    relevance_min_precision: float = 0.90
    relevance_min_recall: float = 0.95
    relevance_min_f1: float = 0.0

    def validate(self) -> None:
        for name, value in asdict(self).items():
            if name.startswith("min_"):
                if int(value) < 1:
                    raise BenchmarkDataError("minimum sample gates must be positive")
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
        stratum = str(value.get("stratum") or "").strip()
        fixture_without_stratum = label_source == "fixture" and allow_fixture_labels and not stratum
        if not fixture_without_stratum and stratum not in SAME_STORY_STRATA:
            raise BenchmarkDataError(f"{location}: invalid same-story stratum {stratum!r}")
        label = _require_text(value.get("label"), "label", location)
        if label not in SAME_STORY_LABELS:
            raise BenchmarkDataError(f"{location}: invalid same-story label {label!r}")
    elif expected_task == "relevance":
        _require_text(value.get("sample_id"), "sample_id", location)
        _require_text(value.get("event_id"), "event_id", location)
        _validate_article(value.get("article"), "article", location)
        # Historical fixture rows predate the release-only provenance strata.
        # They remain usable for structural report-only tests, but a human or
        # adjudicated release row must always carry the explicit linkage fields.
        stratum = str(value.get("stratum") or "").strip()
        linked_document_ids = value.get("linked_document_ids")
        fixture_without_stratum = label_source == "fixture" and allow_fixture_labels and not stratum
        if not fixture_without_stratum:
            if stratum not in RELEVANCE_STRATA:
                raise BenchmarkDataError(f"{location}: invalid relevance stratum {stratum!r}")
            if not isinstance(linked_document_ids, list) or any(
                not isinstance(item, str) or not item.strip() for item in linked_document_ids
            ):
                raise BenchmarkDataError(f"{location}: linked_document_ids must be an array of non-empty strings")
            if len(linked_document_ids) != len(set(linked_document_ids)):
                raise BenchmarkDataError(f"{location}: linked_document_ids must be unique")
            if stratum == "official_event" and not linked_document_ids:
                raise BenchmarkDataError(f"{location}: official_event must have linked_document_ids")
            if stratum == "non_governance_hard_negative" and linked_document_ids:
                raise BenchmarkDataError(
                    f"{location}: non_governance_hard_negative must not claim an official document link"
                )
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


def _canonical_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _raw_file_sha256(path: Path) -> str:
    if not path.exists():
        raise BenchmarkDataError(f"evidence file does not exist: {path}")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _load_json_object(path: Path, *, location: str) -> dict[str, object]:
    if not path.exists():
        raise BenchmarkDataError(f"{location} does not exist: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise BenchmarkDataError(f"{location} must be UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise BenchmarkDataError(f"{location} must be a JSON object")
    return value


def _load_unlabeled_candidates(path: Path, *, task: str) -> list[dict[str, object]]:
    if not path.exists():
        raise BenchmarkDataError(f"{task} candidate file does not exist: {path}")
    records: list[dict[str, object]] = []
    identifiers: set[str] = set()
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw_line.strip():
            continue
        location = f"{path}:{line_number}"
        try:
            value = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            raise BenchmarkDataError(f"{location}: invalid JSON") from exc
        if not isinstance(value, dict) or value.get("schema_version") != SCHEMA_VERSION:
            raise BenchmarkDataError(f"{location}: invalid candidate schema_version")
        if task == "same_story":
            expected_keys = {
                "schema_version",
                "task",
                "pair_id",
                "left",
                "right",
                "stratum",
                "label",
                "label_source",
            }
            if set(value) != expected_keys or value.get("task") != "same_event_candidate":
                raise BenchmarkDataError(f"{location}: invalid same-event candidate contract")
            identifier = _require_text(value.get("pair_id"), "pair_id", location)
            left = _validate_article(value.get("left"), "left", location)
            right = _validate_article(value.get("right"), "right", location)
            if str(left.get("article_id")) == str(right.get("article_id")):
                raise BenchmarkDataError(f"{location}: candidate pair contains the same article twice")
            stratum = str(value.get("stratum") or "").strip()
            if stratum not in SAME_STORY_STRATA:
                raise BenchmarkDataError(f"{location}: invalid same-event candidate stratum")
        elif task == "relevance":
            expected_keys = {
                "schema_version",
                "task",
                "sample_id",
                "event_id",
                "article",
                "stratum",
                "linked_document_ids",
                "label",
                "label_source",
                "annotator_id",
                "labeled_at",
            }
            if set(value) != expected_keys or value.get("task") != "relevance":
                raise BenchmarkDataError(f"{location}: invalid relevance candidate contract")
            identifier = _require_text(value.get("sample_id"), "sample_id", location)
            _require_text(value.get("event_id"), "event_id", location)
            _validate_article(value.get("article"), "article", location)
            stratum = str(value.get("stratum") or "").strip()
            linked = value.get("linked_document_ids")
            if stratum not in RELEVANCE_STRATA or not isinstance(linked, list) or any(
                not isinstance(item, str) or not item.strip() for item in linked
            ):
                raise BenchmarkDataError(f"{location}: invalid relevance candidate provenance")
            if len(linked) != len(set(linked)):
                raise BenchmarkDataError(f"{location}: linked_document_ids must be unique")
            if (stratum == "official_event") is not bool(linked):
                raise BenchmarkDataError(f"{location}: candidate stratum/document linkage mismatch")
        else:
            raise BenchmarkDataError(f"unsupported candidate task {task}")
        if value.get("label") is not None or value.get("label_source") is not None:
            raise BenchmarkDataError(f"{location}: candidate labels must be empty")
        if task == "relevance" and (
            value.get("annotator_id") is not None or value.get("labeled_at") is not None
        ):
            raise BenchmarkDataError(f"{location}: candidate reviewer fields must be empty")
        if identifier in identifiers:
            raise BenchmarkDataError(f"{location}: duplicate candidate ID {identifier!r}")
        identifiers.add(identifier)
        records.append(value)
    if not records:
        raise BenchmarkDataError(f"{task} candidate file has no records")
    return records


def _candidate_core(record: Mapping[str, object], *, task: str) -> dict[str, object]:
    if task == "same_story":
        return {
            "schema_version": record.get("schema_version"),
            "pair_id": record.get("pair_id"),
            "left": record.get("left"),
            "right": record.get("right"),
            "stratum": record.get("stratum"),
        }
    return {
        "schema_version": record.get("schema_version"),
        "sample_id": record.get("sample_id"),
        "event_id": record.get("event_id"),
        "article": record.get("article"),
        "stratum": record.get("stratum"),
        "linked_document_ids": record.get("linked_document_ids"),
    }


def _records_by_id(
    records: Sequence[Mapping[str, object]], *, task: str, location: str
) -> dict[str, Mapping[str, object]]:
    id_field = "pair_id" if task == "same_story" else "sample_id"
    result: dict[str, Mapping[str, object]] = {}
    for record in records:
        identifier = _require_text(record.get(id_field), id_field, location)
        if identifier in result:
            raise BenchmarkDataError(f"{location}: duplicate {id_field} {identifier!r}")
        result[identifier] = record
    return result


def _stratum_counts(
    records: Sequence[Mapping[str, object]], *, allowed: set[str], location: str
) -> dict[str, int]:
    counts = {stratum: 0 for stratum in sorted(allowed)}
    for record in records:
        stratum = str(record.get("stratum") or "").strip()
        if stratum not in allowed:
            raise BenchmarkDataError(f"{location}: invalid stratum {stratum!r}")
        counts[stratum] += 1
    return counts


def _same_story_strata_report(
    records: Sequence[Mapping[str, object]],
) -> dict[str, int]:
    if all(str(record.get("stratum") or "").strip() in SAME_STORY_STRATA for record in records):
        return _stratum_counts(
            records, allowed=SAME_STORY_STRATA, location="same-story benchmark"
        )
    if all(
        str(record.get("label_source") or "") == "fixture"
        and not str(record.get("stratum") or "").strip()
        for record in records
    ):
        return {
            **{stratum: 0 for stratum in sorted(SAME_STORY_STRATA)},
            "fixture_unstratified": len(records),
        }
    raise BenchmarkDataError("same-story benchmark contains missing or invalid production strata")


def _require_exact_counts(
    value: object, expected: Mapping[str, int], *, location: str
) -> dict[str, int]:
    if not isinstance(value, dict) or set(value) != set(expected):
        raise BenchmarkDataError(f"{location}: exact stratum keys are required")
    normalized: dict[str, int] = {}
    for key, expected_count in expected.items():
        actual = value.get(key)
        if isinstance(actual, bool) or not isinstance(actual, int) or actual != expected_count:
            raise BenchmarkDataError(
                f"{location}: {key} must equal {expected_count}, got {actual!r}"
            )
        normalized[key] = actual
    return normalized


def _validate_candidate_bundle(
    manifest_path: Path,
    same_story_path: Path,
    relevance_path: Path,
) -> tuple[
    dict[str, Mapping[str, object]],
    dict[str, Mapping[str, object]],
    dict[str, object],
]:
    manifest = _load_json_object(manifest_path, location="candidate manifest")
    if manifest.get("schema_version") != 1 or manifest.get("release_eligible") is not False:
        raise BenchmarkDataError("candidate manifest must be schema v1 and release_eligible=false")
    same_story_records = _load_unlabeled_candidates(same_story_path, task="same_story")
    relevance_records = _load_unlabeled_candidates(relevance_path, task="relevance")
    same_story_sha = _raw_file_sha256(same_story_path)
    relevance_sha = _raw_file_sha256(relevance_path)

    same_manifest = manifest.get("same_event")
    relevance_manifest = manifest.get("relevance")
    if not isinstance(same_manifest, dict) or not isinstance(relevance_manifest, dict):
        raise BenchmarkDataError("candidate manifest requires same_event and relevance sections")
    for section, digest, expected in (
        (same_manifest, same_story_sha, PRODUCTION_SAME_STORY_STRATA),
        (relevance_manifest, relevance_sha, PRODUCTION_RELEVANCE_STRATA),
    ):
        reported_sha = str(section.get("sha256") or "").strip().casefold()
        if reported_sha != digest:
            raise BenchmarkDataError("candidate manifest/file SHA-256 mismatch")
        _require_exact_counts(section.get("required"), expected, location="candidate.required")
        _require_exact_counts(section.get("selected"), expected, location="candidate.selected")

    same_counts = _stratum_counts(
        same_story_records, allowed=SAME_STORY_STRATA, location="same-event candidates"
    )
    relevance_counts = _stratum_counts(
        relevance_records, allowed=RELEVANCE_STRATA, location="relevance candidates"
    )
    if same_counts != PRODUCTION_SAME_STORY_STRATA:
        raise BenchmarkDataError(
            f"same-event candidate strata must be exact: {PRODUCTION_SAME_STORY_STRATA}"
        )
    if relevance_counts != PRODUCTION_RELEVANCE_STRATA:
        raise BenchmarkDataError(
            f"relevance candidate strata must be exact: {PRODUCTION_RELEVANCE_STRATA}"
        )
    same_by_id = _records_by_id(
        same_story_records, task="same_story", location="same-event candidates"
    )
    relevance_by_id = _records_by_id(
        relevance_records, task="relevance", location="relevance candidates"
    )
    process = {
        "candidate_manifest_sha256": _raw_file_sha256(manifest_path),
        "candidate_files": {
            "same_story": {
                "sha256": same_story_sha,
                "item_count": len(same_story_records),
                "strata": same_counts,
            },
            "relevance": {
                "sha256": relevance_sha,
                "item_count": len(relevance_records),
                "strata": relevance_counts,
            },
        },
    }
    return same_by_id, relevance_by_id, process


def _uniform_human_reviewer(
    records: Sequence[Mapping[str, object]], *, location: str
) -> str:
    annotators = {
        _require_text(record.get("annotator_id"), "annotator_id", location) for record in records
    }
    if len(annotators) != 1:
        raise BenchmarkDataError(f"{location}: exactly one reviewer is required per blind file")
    if any(str(record.get("label_source") or "").strip() != "human" for record in records):
        raise BenchmarkDataError(f"{location}: blind reviewer labels must use label_source=human")
    return next(iter(annotators))


def _assert_review_matches_candidates(
    records: Sequence[Mapping[str, object]],
    candidates: Mapping[str, Mapping[str, object]],
    *,
    task: str,
    location: str,
) -> dict[str, Mapping[str, object]]:
    by_id = _records_by_id(records, task=task, location=location)
    if set(by_id) != set(candidates):
        missing = sorted(set(candidates) - set(by_id))
        extra = sorted(set(by_id) - set(candidates))
        raise BenchmarkDataError(
            f"{location}: reviewer/candidate ID mismatch missing={missing[:5]} extra={extra[:5]}"
        )
    for identifier, record in by_id.items():
        if _canonical_json(_candidate_core(record, task=task)) != _canonical_json(
            _candidate_core(candidates[identifier], task=task)
        ):
            raise BenchmarkDataError(f"{location}: candidate content/stratum changed for {identifier}")
    return by_id


def _sha256_text_list(values: Sequence[str]) -> str:
    return hashlib.sha256(
        json.dumps(list(values), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _validate_pilot_report(
    pilot_path: Path,
    *,
    reviewers: set[str],
    same_story_reviews_a: Mapping[str, Mapping[str, object]],
    same_story_reviews_b: Mapping[str, Mapping[str, object]],
    relevance_reviews_a: Mapping[str, Mapping[str, object]],
    relevance_reviews_b: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    pilot = _load_json_object(pilot_path, location="pilot agreement")
    if (
        pilot.get("schema_version") != 1
        or pilot.get("release_eligible") is not False
        or pilot.get("pilot_passed") is not True
        or pilot.get("guide_revision_required") is not False
    ):
        raise BenchmarkDataError("pilot agreement must be a passed, non-release schema-v1 report")
    _require_timestamp(pilot.get("evaluated_at"), "evaluated_at", "pilot agreement")
    tasks = pilot.get("tasks")
    if not isinstance(tasks, list) or len(tasks) != 2:
        raise BenchmarkDataError("pilot agreement must contain exactly two tasks")
    by_task: dict[str, dict[str, object]] = {}
    for raw_task in tasks:
        if not isinstance(raw_task, dict):
            raise BenchmarkDataError("pilot agreement task must be an object")
        task_name = str(raw_task.get("task") or "").strip()
        if task_name not in PILOT_COUNTS or task_name in by_task:
            raise BenchmarkDataError("pilot agreement task names must be same_event and core_event")
        expected_count = PILOT_COUNTS[task_name]
        item_count = raw_task.get("item_count")
        item_ids = raw_task.get("item_ids")
        if (
            item_count != expected_count
            or not isinstance(item_ids, list)
            or len(item_ids) != expected_count
            or any(not isinstance(value, str) or not value for value in item_ids)
            or len(set(item_ids)) != expected_count
        ):
            raise BenchmarkDataError(f"pilot {task_name} requires {expected_count} unique IDs")
        expected_ids_sha = _sha256_text_list(sorted(item_ids))
        if str(raw_task.get("item_ids_sha256") or "").casefold() != expected_ids_sha:
            raise BenchmarkDataError(f"pilot {task_name} item_ids_sha256 mismatch")
        pilot_reviewers = raw_task.get("annotators")
        if (
            not isinstance(pilot_reviewers, list)
            or len(pilot_reviewers) != 2
            or any(not isinstance(value, str) or not value for value in pilot_reviewers)
            or set(pilot_reviewers) != reviewers
        ):
            raise BenchmarkDataError(f"pilot {task_name} must use the two blind reviewers")
        for digest_field in ("reviewer_a_dataset_sha256", "reviewer_b_dataset_sha256"):
            if SHA256_RE.fullmatch(str(raw_task.get(digest_field) or "").casefold()) is None:
                raise BenchmarkDataError(f"pilot {task_name} has invalid {digest_field}")
        threshold = raw_task.get("threshold")
        kappa = raw_task.get("cohen_kappa")
        if (
            isinstance(threshold, bool)
            or not isinstance(threshold, (int, float))
            or isinstance(kappa, bool)
            or not isinstance(kappa, (int, float))
            or float(threshold) < PILOT_KAPPA_MINIMUM
            or float(kappa) < PILOT_KAPPA_MINIMUM
            or raw_task.get("passed") is not True
        ):
            raise BenchmarkDataError(f"pilot {task_name} requires Cohen's kappa >= 0.8")
        disagreement_ids = raw_task.get("disagreement_ids")
        disagreement_count = raw_task.get("disagreement_count")
        if (
            not isinstance(disagreement_ids, list)
            or any(not isinstance(value, str) for value in disagreement_ids)
            or disagreement_count != len(disagreement_ids)
        ):
            raise BenchmarkDataError(f"pilot {task_name} disagreement count mismatch")

        if task_name == "same_event":
            if not set(item_ids) <= set(same_story_reviews_a):
                raise BenchmarkDataError("pilot same_event IDs are not in the reviewed candidate set")
            labels_a = {
                item_id: str(same_story_reviews_a[item_id].get("label")) for item_id in item_ids
            }
            labels_b = {
                item_id: str(same_story_reviews_b[item_id].get("label")) for item_id in item_ids
            }
        else:
            official_a: dict[str, Mapping[str, object]] = {}
            official_b: dict[str, Mapping[str, object]] = {}
            for record in relevance_reviews_a.values():
                if record.get("stratum") == "official_event":
                    event_id = str(record.get("event_id") or "")
                    if event_id in official_a:
                        raise BenchmarkDataError("core-event pilot event IDs must be unique")
                    official_a[event_id] = record
            for record in relevance_reviews_b.values():
                if record.get("stratum") == "official_event":
                    event_id = str(record.get("event_id") or "")
                    if event_id in official_b:
                        raise BenchmarkDataError("core-event pilot event IDs must be unique")
                    official_b[event_id] = record
            if not set(item_ids) <= set(official_a) or not set(item_ids) <= set(official_b):
                raise BenchmarkDataError("pilot core_event IDs are not reviewed official events")
            labels_a = {item_id: str(official_a[item_id].get("label")) for item_id in item_ids}
            labels_b = {item_id: str(official_b[item_id].get("label")) for item_id in item_ids}

        from .label_agreement import cohen_kappa

        actual_kappa = round(cohen_kappa(labels_a, labels_b), 6)
        actual_disagreements = sorted(
            item_id for item_id in item_ids if labels_a[item_id] != labels_b[item_id]
        )
        if not abs(float(kappa) - actual_kappa) <= 0.000001:
            raise BenchmarkDataError(f"pilot {task_name} kappa does not match full blind labels")
        if sorted(disagreement_ids) != actual_disagreements:
            raise BenchmarkDataError(f"pilot {task_name} disagreement IDs do not match labels")
        by_task[task_name] = {
            "item_count": expected_count,
            "reviewer_ids": sorted(reviewers),
            "cohen_kappa": actual_kappa,
            "threshold": float(threshold),
            "item_ids_sha256": expected_ids_sha,
        }
    if set(by_task) != set(PILOT_COUNTS):
        raise BenchmarkDataError("pilot agreement task set is incomplete")
    return {
        "report_sha256": _raw_file_sha256(pilot_path),
        "same_event": by_task["same_event"],
        "core_event": by_task["core_event"],
    }


def _load_adjudication(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        raise BenchmarkDataError(f"adjudication file does not exist: {path}")
    records: list[dict[str, object]] = []
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not raw_line.strip():
            continue
        location = f"{path}:{line_number}"
        try:
            value = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            raise BenchmarkDataError(f"{location}: invalid adjudication JSON") from exc
        if not isinstance(value, dict):
            raise BenchmarkDataError(f"{location}: adjudication row must be an object")
        expected_keys = {
            "schema_version",
            "task",
            "item_id",
            "reviewer_a_label",
            "reviewer_b_label",
            "final_label",
            "decision_mode",
            "decided_by",
            "reason",
            "decided_at",
            "unresolved",
        }
        if set(value) != expected_keys or value.get("schema_version") != 1:
            raise BenchmarkDataError(f"{location}: invalid adjudication contract")
        records.append(value)
    return records


def _validate_adjudication(
    path: Path,
    *,
    reviewer_a: str,
    reviewer_b: str,
    same_story_a: Mapping[str, Mapping[str, object]],
    same_story_b: Mapping[str, Mapping[str, object]],
    relevance_a: Mapping[str, Mapping[str, object]],
    relevance_b: Mapping[str, Mapping[str, object]],
    final_same_story: Mapping[str, Mapping[str, object]],
    final_relevance: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    datasets = {
        "same_story": (same_story_a, same_story_b, final_same_story, SAME_STORY_LABELS),
        "relevance": (relevance_a, relevance_b, final_relevance, RELEVANCE_LABELS),
    }
    expected: set[tuple[str, str]] = set()
    for task, (left, right, _final, _labels) in datasets.items():
        expected.update(
            (task, item_id)
            for item_id in left
            if str(left[item_id].get("label")) != str(right[item_id].get("label"))
        )
    rows = _load_adjudication(path)
    actual: set[tuple[str, str]] = set()
    task_counts = {"same_story": 0, "relevance": 0}
    for index, row in enumerate(rows):
        location = f"adjudication[{index}]"
        task = str(row.get("task") or "").strip()
        item_id = _require_text(row.get("item_id"), "item_id", location)
        key = (task, item_id)
        if task not in datasets or key in actual or key not in expected:
            raise BenchmarkDataError(f"{location}: extra or duplicate adjudication row")
        left, right, final, allowed_labels = datasets[task]
        if item_id not in left or item_id not in right or item_id not in final:
            raise BenchmarkDataError(f"{location}: adjudication item is not in all datasets")
        label_a = str(row.get("reviewer_a_label") or "")
        label_b = str(row.get("reviewer_b_label") or "")
        final_label = str(row.get("final_label") or "")
        if (
            label_a != str(left[item_id].get("label"))
            or label_b != str(right[item_id].get("label"))
            or final_label != str(final[item_id].get("label"))
            or final_label not in allowed_labels
        ):
            raise BenchmarkDataError(f"{location}: adjudication label mismatch")
        if row.get("unresolved") is not False:
            raise BenchmarkDataError(f"{location}: unresolved adjudication is forbidden")
        decision_mode = str(row.get("decision_mode") or "")
        decided_by = row.get("decided_by")
        if (
            not isinstance(decided_by, list)
            or not decided_by
            or any(not isinstance(value, str) or not value.strip() for value in decided_by)
            or len(decided_by) != len(set(decided_by))
        ):
            raise BenchmarkDataError(f"{location}: decided_by must contain unique reviewer IDs")
        if decision_mode == "reviewer_consensus":
            if set(decided_by) != {reviewer_a, reviewer_b}:
                raise BenchmarkDataError(f"{location}: reviewer consensus requires both reviewers")
        elif decision_mode == "product_owner":
            if len(decided_by) != 1 or decided_by[0] in {reviewer_a, reviewer_b}:
                raise BenchmarkDataError(f"{location}: product-owner decision requires an independent ID")
        else:
            raise BenchmarkDataError(f"{location}: invalid decision_mode")
        if len(_require_text(row.get("reason"), "reason", location)) < 5:
            raise BenchmarkDataError(f"{location}: adjudication reason is too short")
        _require_timestamp(row.get("decided_at"), "decided_at", location)
        if str(final[item_id].get("label_source") or "") != "adjudicated":
            raise BenchmarkDataError(f"{location}: disagreed final row must be adjudicated")
        actual.add(key)
        task_counts[task] += 1
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise BenchmarkDataError(
            f"adjudication must exactly cover disagreements missing={missing[:5]} extra={extra[:5]}"
        )
    return {
        "dataset_sha256": _raw_file_sha256(path),
        "disagreement_count": len(expected),
        "adjudicated_count": len(actual),
        "unresolved_count": 0,
        "task_counts": task_counts,
    }


def build_review_process_evidence(
    *,
    candidate_manifest_path: Path,
    same_story_candidates_path: Path,
    relevance_candidates_path: Path,
    pilot_agreement_path: Path,
    same_story_reviewer_a_path: Path,
    same_story_reviewer_b_path: Path,
    relevance_reviewer_a_path: Path,
    relevance_reviewer_b_path: Path,
    adjudication_path: Path,
    final_same_story_records: Sequence[Mapping[str, object]],
    final_relevance_records: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    same_candidates, relevance_candidates, candidate_evidence = _validate_candidate_bundle(
        candidate_manifest_path,
        same_story_candidates_path,
        relevance_candidates_path,
    )
    same_a_records = load_jsonl(same_story_reviewer_a_path, expected_task="same_story")
    same_b_records = load_jsonl(same_story_reviewer_b_path, expected_task="same_story")
    relevance_a_records = load_jsonl(relevance_reviewer_a_path, expected_task="relevance")
    relevance_b_records = load_jsonl(relevance_reviewer_b_path, expected_task="relevance")
    reviewer_a = _uniform_human_reviewer(same_a_records, location="same-story reviewer A")
    reviewer_b = _uniform_human_reviewer(same_b_records, location="same-story reviewer B")
    relevance_reviewer_a = _uniform_human_reviewer(
        relevance_a_records, location="relevance reviewer A"
    )
    relevance_reviewer_b = _uniform_human_reviewer(
        relevance_b_records, location="relevance reviewer B"
    )
    if reviewer_a == reviewer_b:
        raise BenchmarkDataError("blind review requires two different reviewers")
    if reviewer_a != relevance_reviewer_a or reviewer_b != relevance_reviewer_b:
        raise BenchmarkDataError("the same reviewer A/B identities must be used for both tasks")
    reviewers = {reviewer_a, reviewer_b}

    same_a = _assert_review_matches_candidates(
        same_a_records, same_candidates, task="same_story", location="same-story reviewer A"
    )
    same_b = _assert_review_matches_candidates(
        same_b_records, same_candidates, task="same_story", location="same-story reviewer B"
    )
    relevance_a = _assert_review_matches_candidates(
        relevance_a_records,
        relevance_candidates,
        task="relevance",
        location="relevance reviewer A",
    )
    relevance_b = _assert_review_matches_candidates(
        relevance_b_records,
        relevance_candidates,
        task="relevance",
        location="relevance reviewer B",
    )
    final_same = _assert_review_matches_candidates(
        final_same_story_records,
        same_candidates,
        task="same_story",
        location="final same-story benchmark",
    )
    final_relevance = _assert_review_matches_candidates(
        final_relevance_records,
        relevance_candidates,
        task="relevance",
        location="final relevance benchmark",
    )
    if len(final_same) < 500:
        raise BenchmarkDataError("final same-story benchmark requires at least 500 pairs")

    for task, left, right, final in (
        ("same_story", same_a, same_b, final_same),
        ("relevance", relevance_a, relevance_b, final_relevance),
    ):
        for item_id in left:
            label_a = str(left[item_id].get("label"))
            label_b = str(right[item_id].get("label"))
            final_label = str(final[item_id].get("label"))
            if label_a == label_b:
                if final_label != label_a:
                    raise BenchmarkDataError(
                        f"{task} final label changed despite reviewer agreement for {item_id}"
                    )
                if str(final[item_id].get("label_source") or "") != "human":
                    raise BenchmarkDataError(
                        f"{task} agreed final row must use label_source=human for {item_id}"
                    )

    pilot = _validate_pilot_report(
        pilot_agreement_path,
        reviewers=reviewers,
        same_story_reviews_a=same_a,
        same_story_reviews_b=same_b,
        relevance_reviews_a=relevance_a,
        relevance_reviews_b=relevance_b,
    )
    adjudication = _validate_adjudication(
        adjudication_path,
        reviewer_a=reviewer_a,
        reviewer_b=reviewer_b,
        same_story_a=same_a,
        same_story_b=same_b,
        relevance_a=relevance_a,
        relevance_b=relevance_b,
        final_same_story=final_same,
        final_relevance=final_relevance,
    )
    process = {
        "schema_version": 1,
        "contract": "independent-human-review-v1",
        **candidate_evidence,
        "pilot": pilot,
        "reviewers": {
            "reviewer_count": 2,
            "reviewer_ids": sorted(reviewers),
            "same_story_reviewer_a_sha256": _raw_file_sha256(same_story_reviewer_a_path),
            "same_story_reviewer_b_sha256": _raw_file_sha256(same_story_reviewer_b_path),
            "relevance_reviewer_a_sha256": _raw_file_sha256(relevance_reviewer_a_path),
            "relevance_reviewer_b_sha256": _raw_file_sha256(relevance_reviewer_b_path),
        },
        "final": {
            "same_story_item_count": len(final_same),
            "relevance_item_count": len(final_relevance),
            "same_story_strata": _stratum_counts(
                final_same_story_records,
                allowed=SAME_STORY_STRATA,
                location="final same-story benchmark",
            ),
            "relevance_strata": _stratum_counts(
                final_relevance_records,
                allowed=RELEVANCE_STRATA,
                location="final relevance benchmark",
            ),
        },
        "adjudication": adjudication,
    }
    process["process_sha256"] = hashlib.sha256(
        _canonical_json(process).encode("utf-8")
    ).hexdigest()
    return process


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
        "company_candidates": _string_array(value.get("company_candidates")),
        "topic_keywords": _string_array(value.get("topic_keywords")),
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
        "strata": _same_story_strata_report(records),
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
    official_event_ids = {
        str(record.get("event_id") or "")
        for record in records
        if record.get("stratum") == "official_event" and record.get("linked_document_ids")
    }
    hard_negative_candidate_count = sum(
        record.get("stratum") == "non_governance_hard_negative" for record in records
    )
    hard_negative_count = sum(
        record.get("stratum") == "non_governance_hard_negative"
        and record.get("label") == "not_relevant"
        for record in records
    )
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
        "official_linked_event_count": len(official_event_ids),
        "hard_negative_candidate_count": hard_negative_candidate_count,
        "hard_negative_count": hard_negative_count,
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


def _string_array(value: object) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        return []
    return list(value)


def _metric_float(report: Mapping[str, object], field: str) -> float:
    value = report.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise BenchmarkDataError(f"benchmark metric {field} must be numeric")
    return float(value)


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
    review_process: Mapping[str, object] | None = None,
    fixture_mode: bool = False,
) -> dict[str, object]:
    release_thresholds = thresholds or ReleaseThresholds()
    release_thresholds.validate()
    same_story = evaluate_same_story(same_story_records, config, predictor=same_story_predictor)
    relevance = evaluate_relevance(relevance_records, config, predictor=relevance_predictor)
    gates = [
        _gate("same_story.article_pair_count", _metric_float(same_story, "sample_count"), release_thresholds.min_article_pairs),
        _gate("same_story.has_positive_class", float(_metric_float(same_story, "actual_positive") > 0), 1.0),
        _gate("same_story.has_negative_class", float(_metric_float(same_story, "actual_negative") > 0), 1.0),
        _gate("same_story.precision", _metric_float(same_story, "precision"), release_thresholds.same_story_min_precision),
        _gate("same_story.recall", _metric_float(same_story, "recall"), release_thresholds.same_story_min_recall),
        _gate("same_story.f1", _metric_float(same_story, "f1"), release_thresholds.same_story_min_f1),
        _gate(
            "relevance.official_linked_event_count",
            _metric_float(relevance, "official_linked_event_count"),
            release_thresholds.min_events,
        ),
        _gate(
            "relevance.hard_negative_count",
            _metric_float(relevance, "hard_negative_count"),
            release_thresholds.min_relevance_hard_negatives,
        ),
        _gate("relevance.has_positive_class", float(_metric_float(relevance, "actual_positive") > 0), 1.0),
        _gate("relevance.has_negative_class", float(_metric_float(relevance, "actual_negative") > 0), 1.0),
        _gate("relevance.precision", _metric_float(relevance, "precision"), release_thresholds.relevance_min_precision),
        _gate("relevance.recall", _metric_float(relevance, "recall"), release_thresholds.relevance_min_recall),
        _gate("relevance.f1", _metric_float(relevance, "f1"), release_thresholds.relevance_min_f1),
    ]
    same_story_sources = _label_sources(same_story_records)
    relevance_sources = _label_sources(relevance_records)
    human_labels_eligible = bool(same_story_sources and relevance_sources) and (
        set(same_story_sources) | set(relevance_sources)
    ) <= RELEASE_LABEL_SOURCES
    process_eligible = (
        isinstance(review_process, Mapping)
        and review_process.get("contract") == "independent-human-review-v1"
        and review_process.get("process_sha256")
        == hashlib.sha256(
            _canonical_json(
                {key: value for key, value in review_process.items() if key != "process_sha256"}
            ).encode("utf-8")
        ).hexdigest()
    )
    release_eligible = human_labels_eligible and process_eligible and not fixture_mode
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
            "benchmark_process_sha256": (
                str(review_process.get("process_sha256"))
                if isinstance(review_process, Mapping) and review_process.get("process_sha256")
                else None
            ),
        },
        "thresholds": asdict(release_thresholds),
        "gates": gates,
        "same_story": same_story,
        "relevance": relevance,
        "review_process": dict(review_process) if isinstance(review_process, Mapping) else None,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate relevance and same-story release gates from JSONL labels.")
    parser.add_argument("--root", type=Path, default=PROJECT_ROOT)
    parser.add_argument("--same-story", type=Path, required=True, help="same-story article-pair JSONL")
    parser.add_argument("--relevance", type=Path, required=True, help="relevance event JSONL")
    parser.add_argument("--candidate-manifest", type=Path, help="immutable benchmark candidate manifest")
    parser.add_argument("--same-story-candidates", type=Path, help="unlabeled 650-pair candidate JSONL")
    parser.add_argument("--relevance-candidates", type=Path, help="unlabeled relevance candidate JSONL")
    parser.add_argument("--pilot-agreement", type=Path, help="passed independent pilot agreement JSON")
    parser.add_argument("--same-story-reviewer-a", type=Path, help="blind same-story labels from reviewer A")
    parser.add_argument("--same-story-reviewer-b", type=Path, help="blind same-story labels from reviewer B")
    parser.add_argument("--relevance-reviewer-a", type=Path, help="blind relevance labels from reviewer A")
    parser.add_argument("--relevance-reviewer-b", type=Path, help="blind relevance labels from reviewer B")
    parser.add_argument("--adjudication", type=Path, help="complete disagreement adjudication JSONL")
    parser.add_argument("--min-article-pairs", type=int, default=500)
    parser.add_argument("--min-events", type=int, default=300)
    parser.add_argument("--min-relevance-hard-negatives", type=int, default=120)
    parser.add_argument("--same-story-min-precision", type=float, default=0.97)
    parser.add_argument("--same-story-min-recall", type=float, default=0.0)
    parser.add_argument("--same-story-min-f1", type=float, default=0.0)
    parser.add_argument("--relevance-min-precision", type=float, default=0.90)
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
        help="explicit fixture/dev mode; accepts fixture labels only with --report-only",
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
        process_paths = {
            "candidate_manifest_path": args.candidate_manifest,
            "same_story_candidates_path": args.same_story_candidates,
            "relevance_candidates_path": args.relevance_candidates,
            "pilot_agreement_path": args.pilot_agreement,
            "same_story_reviewer_a_path": args.same_story_reviewer_a,
            "same_story_reviewer_b_path": args.same_story_reviewer_b,
            "relevance_reviewer_a_path": args.relevance_reviewer_a,
            "relevance_reviewer_b_path": args.relevance_reviewer_b,
            "adjudication_path": args.adjudication,
        }
        review_process: dict[str, object] | None = None
        if args.allow_fixture_labels:
            if any(value is not None for value in process_paths.values()):
                raise BenchmarkDataError(
                    "fixture mode cannot be combined with production review-process evidence"
                )
        else:
            missing_process = sorted(
                name.removesuffix("_path")
                for name, value in process_paths.items()
                if value is None
            )
            if missing_process:
                raise BenchmarkDataError(
                    "production benchmark requires review-process inputs: "
                    + ", ".join(missing_process)
                )
            if not all(isinstance(value, Path) for value in process_paths.values()):
                raise BenchmarkDataError("production review-process paths are incomplete")
            review_process = build_review_process_evidence(
                candidate_manifest_path=_resolved_input(project_root, args.candidate_manifest),
                same_story_candidates_path=_resolved_input(
                    project_root, args.same_story_candidates
                ),
                relevance_candidates_path=_resolved_input(
                    project_root, args.relevance_candidates
                ),
                pilot_agreement_path=_resolved_input(project_root, args.pilot_agreement),
                same_story_reviewer_a_path=_resolved_input(
                    project_root, args.same_story_reviewer_a
                ),
                same_story_reviewer_b_path=_resolved_input(
                    project_root, args.same_story_reviewer_b
                ),
                relevance_reviewer_a_path=_resolved_input(
                    project_root, args.relevance_reviewer_a
                ),
                relevance_reviewer_b_path=_resolved_input(
                    project_root, args.relevance_reviewer_b
                ),
                adjudication_path=_resolved_input(project_root, args.adjudication),
                final_same_story_records=same_story_records,
                final_relevance_records=relevance_records,
            )
        thresholds = ReleaseThresholds(
            min_article_pairs=args.min_article_pairs,
            min_events=args.min_events,
            min_relevance_hard_negatives=args.min_relevance_hard_negatives,
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
            review_process=review_process,
            fixture_mode=args.allow_fixture_labels,
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
