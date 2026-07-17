from __future__ import annotations

import argparse
import json
import math
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence


SCHEMA_VERSION = 1
REVISION_RE = re.compile(r"^[0-9a-f]{7,64}$")
RELEASE_LABEL_SOURCES = {"human", "adjudicated"}
NON_RELEASE_EVIDENCE_SOURCES = {"fixture", "synthetic", "test", "sample"}


class ReleaseEvidenceError(ValueError):
    """Raised when supplied release evidence is malformed or ineligible."""


@dataclass(frozen=True)
class GateThresholds:
    shadow_days: int = 14
    consecutive_days: int = 7
    official_ingest_success_rate: float = 0.99
    official_lag_p95_minutes: float = 45.0
    delivery_success_rate: float = 0.995
    delivery_failure_detection_p95_minutes: float = 10.0
    official_evidence_link_rate: float = 0.95
    same_story_precision: float = 0.97
    top_sensitive_human_review_rate: float = 1.0
    original_language_preservation_rate: float = 1.0
    valid_source_right_rate: float = 1.0
    availability_rate: float = 0.999
    mobile_lcp_p75_seconds: float = 2.5
    mobile_inp_p75_ms: float = 200.0
    mobile_cls_p75: float = 0.1
    same_story_min_pairs: int = 500
    relevance_min_events: int = 300
    relevance_recall: float = 0.95
    max_evidence_lag_days: int = 2


def _require_mapping(value: object, location: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise ReleaseEvidenceError(f"{location}: expected a JSON object")
    return value


def _require_text(value: object, field: str, location: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ReleaseEvidenceError(f"{location}: {field} must be a non-empty string")
    return text


def _require_bool(value: object, field: str, location: str) -> bool:
    if not isinstance(value, bool):
        raise ReleaseEvidenceError(f"{location}: {field} must be a boolean")
    return value


def _require_float(value: object, field: str, location: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReleaseEvidenceError(f"{location}: {field} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ReleaseEvidenceError(f"{location}: {field} must be finite")
    return number


def _require_nonnegative_int(value: object, field: str, location: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ReleaseEvidenceError(f"{location}: {field} must be a non-negative integer")
    return value


def _require_nonnegative_float(value: object, field: str, location: str) -> float:
    number = _require_float(value, field, location)
    if number < 0:
        raise ReleaseEvidenceError(f"{location}: {field} must be non-negative")
    return number


def _require_rate(value: object, field: str, location: str) -> float:
    rate = _require_float(value, field, location)
    if not 0.0 <= rate <= 1.0:
        raise ReleaseEvidenceError(f"{location}: {field} must be between 0 and 1")
    return rate


def _require_day(value: object, field: str, location: str) -> date:
    text = _require_text(value, field, location)
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ReleaseEvidenceError(f"{location}: {field} must be YYYY-MM-DD") from exc


def _require_timestamp(value: object, field: str, location: str) -> datetime:
    text = _require_text(value, field, location)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ReleaseEvidenceError(f"{location}: {field} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise ReleaseEvidenceError(f"{location}: {field} must include a timezone")
    return parsed


def _validate_revision(value: object, field: str, location: str) -> str:
    revision = _require_text(value, field, location).casefold()
    if not REVISION_RE.fullmatch(revision):
        raise ReleaseEvidenceError(f"{location}: {field} must be a 7-64 character hexadecimal revision")
    return revision


def _revisions_are_compatible(revisions: Iterable[str]) -> bool:
    values = sorted(set(revisions), key=len, reverse=True)
    return bool(values) and all(values[0].startswith(value) for value in values[1:])


def _validate_production_provenance(record: Mapping[str, object], location: str) -> str:
    if record.get("schema_version") != SCHEMA_VERSION:
        raise ReleaseEvidenceError(f"{location}: schema_version must be {SCHEMA_VERSION}")
    if record.get("environment") != "production":
        raise ReleaseEvidenceError(f"{location}: environment must be production")
    if _require_bool(record.get("is_synthetic"), "is_synthetic", location):
        raise ReleaseEvidenceError(f"{location}: synthetic evidence is never release-eligible")
    evidence_source = _require_text(record.get("evidence_source"), "evidence_source", location).casefold()
    if any(marker in evidence_source for marker in NON_RELEASE_EVIDENCE_SOURCES):
        raise ReleaseEvidenceError(f"{location}: evidence_source {evidence_source!r} is not release-eligible")
    _require_timestamp(record.get("collected_at"), "collected_at", location)
    return _validate_revision(record.get("code_revision"), "code_revision", location)


def load_json(path: Path) -> dict[str, object]:
    if not path.exists():
        raise ReleaseEvidenceError(f"evidence file does not exist: {path}")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ReleaseEvidenceError(f"{path}: invalid JSON: {exc.msg}") from exc
    return dict(_require_mapping(value, str(path)))


def load_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        raise ReleaseEvidenceError(f"evidence file does not exist: {path}")
    records: list[dict[str, object]] = []
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        location = f"{path}:{line_number}"
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ReleaseEvidenceError(f"{location}: invalid JSON: {exc.msg}") from exc
        records.append(dict(_require_mapping(value, location)))
    if not records:
        raise ReleaseEvidenceError(f"evidence file has no records: {path}")
    return records


def _event_keys(value: object, field: str, location: str) -> set[str]:
    if not isinstance(value, list):
        raise ReleaseEvidenceError(f"{location}: {field} must be an array")
    keys: set[str] = set()
    for index, raw_event in enumerate(value):
        event = _require_mapping(raw_event, f"{location}.{field}[{index}]")
        key = _require_text(event.get("comparison_key"), "comparison_key", f"{location}.{field}[{index}]")
        if key in keys:
            raise ReleaseEvidenceError(f"{location}: duplicate {field} comparison_key {key!r}")
        keys.add(key)
    return keys


def _sorted_unique_days(
    records: Sequence[Mapping[str, object]],
    *,
    kind: str,
) -> list[tuple[date, Mapping[str, object], str]]:
    by_day: dict[date, tuple[Mapping[str, object], str]] = {}
    for index, record in enumerate(records, start=1):
        location = f"{kind}[{index}]"
        day = _require_day(record.get("date"), "date", location)
        if day in by_day:
            raise ReleaseEvidenceError(f"{location}: duplicate date {day.isoformat()}")
        collected_at = _require_timestamp(record.get("collected_at"), "collected_at", location)
        if collected_at.date() != day:
            raise ReleaseEvidenceError(
                f"{location}: collected_at calendar date must match date {day.isoformat()}"
            )
        revision = _validate_production_provenance(record, location)
        by_day[day] = (record, revision)
    return [(day, *by_day[day]) for day in sorted(by_day)]


def _latest_window[T](values: Sequence[T], size: int) -> list[T]:
    return list(values[-size:]) if len(values) >= size else list(values)


def _dates_are_consecutive(days: Sequence[date], size: int) -> bool:
    return len(days) == size and all(right - left == timedelta(days=1) for left, right in zip(days, days[1:]))


def _minimum_gate(name: str, actual: float, minimum: float) -> dict[str, object]:
    return {
        "name": name,
        "actual": round(actual, 6),
        "minimum": round(minimum, 6),
        "passed": actual >= minimum,
    }


def _maximum_gate(name: str, actual: float, maximum: float) -> dict[str, object]:
    return {
        "name": name,
        "actual": round(actual, 6),
        "maximum": round(maximum, 6),
        "passed": actual <= maximum,
    }


def _boolean_gate(name: str, passed: bool, detail: object | None = None) -> dict[str, object]:
    gate: dict[str, object] = {"name": name, "passed": passed}
    if detail is not None:
        gate["detail"] = detail
    return gate


def build_shadow_comparison(
    records: Sequence[Mapping[str, object]],
    thresholds: GateThresholds,
) -> tuple[dict[str, object], list[dict[str, object]], set[str], set[date]]:
    dated = _sorted_unique_days(records, kind="shadow")
    selected = _latest_window(dated, thresholds.shadow_days)
    revisions = {revision for _, _, revision in selected}
    days = [day for day, _, _ in selected]
    comparisons: list[dict[str, object]] = []
    successful = True
    reviewed = True
    total_legacy = 0
    total_candidate = 0
    total_matched = 0
    for day, record, _revision in selected:
        location = f"shadow[{day.isoformat()}]"
        legacy_run = _require_mapping(record.get("legacy_run"), f"{location}.legacy_run")
        candidate_run = _require_mapping(record.get("candidate_run"), f"{location}.candidate_run")
        legacy_status = _require_text(legacy_run.get("status"), "status", f"{location}.legacy_run")
        candidate_status = _require_text(candidate_run.get("status"), "status", f"{location}.candidate_run")
        day_reviewed = _require_bool(record.get("discrepancies_reviewed"), "discrepancies_reviewed", location)
        successful = successful and legacy_status == "succeeded" and candidate_status == "succeeded"
        reviewed = reviewed and day_reviewed
        legacy_keys = _event_keys(legacy_run.get("events"), "events", f"{location}.legacy_run")
        candidate_keys = _event_keys(candidate_run.get("events"), "events", f"{location}.candidate_run")
        matched = legacy_keys & candidate_keys
        legacy_only = sorted(legacy_keys - candidate_keys)
        candidate_only = sorted(candidate_keys - legacy_keys)
        union_count = len(legacy_keys | candidate_keys)
        total_legacy += len(legacy_keys)
        total_candidate += len(candidate_keys)
        total_matched += len(matched)
        comparisons.append(
            {
                "date": day.isoformat(),
                "legacy_event_count": len(legacy_keys),
                "candidate_event_count": len(candidate_keys),
                "matched_event_count": len(matched),
                "candidate_delta": len(candidate_keys) - len(legacy_keys),
                "agreement_jaccard": round(len(matched) / union_count, 6) if union_count else 1.0,
                "legacy_only_keys": legacy_only[:200],
                "candidate_only_keys": candidate_only[:200],
                "discrepancies_reviewed": day_reviewed,
                "runs_succeeded": legacy_status == "succeeded" and candidate_status == "succeeded",
            }
        )
    overall_union = total_legacy + total_candidate - total_matched
    gates = [
        _boolean_gate(
            "shadow.consecutive_days",
            _dates_are_consecutive(days, thresholds.shadow_days),
            {"required": thresholds.shadow_days, "actual": len(days)},
        ),
        _boolean_gate("shadow.all_runs_succeeded", successful),
        _boolean_gate("shadow.all_discrepancies_reviewed", reviewed),
        _boolean_gate("shadow.observed_candidate_events", total_candidate > 0, total_candidate),
    ]
    report: dict[str, object] = {
        "required_days": thresholds.shadow_days,
        "start_date": days[0].isoformat() if days else None,
        "end_date": days[-1].isoformat() if days else None,
        "legacy_event_count": total_legacy,
        "candidate_event_count": total_candidate,
        "matched_event_count": total_matched,
        "candidate_precision_vs_legacy": round(total_matched / total_candidate, 6) if total_candidate else 0.0,
        "candidate_recall_vs_legacy": round(total_matched / total_legacy, 6) if total_legacy else 0.0,
        "agreement_jaccard": round(total_matched / overall_union, 6) if overall_union else 1.0,
        "daily": comparisons,
    }
    return report, gates, revisions, set(days)


def build_operations_gates(
    records: Sequence[Mapping[str, object]],
    thresholds: GateThresholds,
) -> tuple[dict[str, object], list[dict[str, object]], set[str], set[date]]:
    dated = _sorted_unique_days(records, kind="operations")
    selected = _latest_window(dated, thresholds.consecutive_days)
    revisions = {revision for _, _, revision in selected}
    days = [day for day, _, _ in selected]
    gates: list[dict[str, object]] = [
        _boolean_gate(
            "operations.consecutive_days",
            _dates_are_consecutive(days, thresholds.consecutive_days),
            {"required": thresholds.consecutive_days, "actual": len(days)},
        )
    ]
    daily: list[dict[str, object]] = []
    for day, record, _revision in selected:
        location = f"operations[{day.isoformat()}]"
        metrics = _require_mapping(record.get("metrics"), f"{location}.metrics")
        values = {
            "official_ingest_success_rate": _require_rate(
                metrics.get("official_ingest_success_rate"), "official_ingest_success_rate", location
            ),
            "official_lag_p95_minutes": _require_nonnegative_float(
                metrics.get("official_lag_p95_minutes"), "official_lag_p95_minutes", location
            ),
            "delivery_success_rate": _require_rate(
                metrics.get("delivery_success_rate"), "delivery_success_rate", location
            ),
            "delivery_failure_detection_p95_minutes": _require_nonnegative_float(
                metrics.get("delivery_failure_detection_p95_minutes"),
                "delivery_failure_detection_p95_minutes",
                location,
            ),
            "official_evidence_link_rate": _require_rate(
                metrics.get("official_evidence_link_rate"), "official_evidence_link_rate", location
            ),
            "same_story_precision": _require_rate(
                metrics.get("same_story_precision"), "same_story_precision", location
            ),
            "same_story_evaluated_pair_count": _require_nonnegative_int(
                metrics.get("same_story_evaluated_pair_count"), "same_story_evaluated_pair_count", location
            ),
            "top_sensitive_human_review_rate": _require_rate(
                metrics.get("top_sensitive_human_review_rate"), "top_sensitive_human_review_rate", location
            ),
            "original_language_preservation_rate": _require_rate(
                metrics.get("original_language_preservation_rate"), "original_language_preservation_rate", location
            ),
            "valid_source_right_rate": _require_rate(
                metrics.get("valid_source_right_rate"), "valid_source_right_rate", location
            ),
        }
        daily.append({"date": day.isoformat(), **values})
        prefix = f"operations.{day.isoformat()}"
        gates.extend(
            [
                _minimum_gate(
                    f"{prefix}.official_ingest_success_rate",
                    values["official_ingest_success_rate"],
                    thresholds.official_ingest_success_rate,
                ),
                _maximum_gate(
                    f"{prefix}.official_lag_p95_minutes",
                    values["official_lag_p95_minutes"],
                    thresholds.official_lag_p95_minutes,
                ),
                _minimum_gate(
                    f"{prefix}.delivery_success_rate",
                    values["delivery_success_rate"],
                    thresholds.delivery_success_rate,
                ),
                _maximum_gate(
                    f"{prefix}.delivery_failure_detection_p95_minutes",
                    values["delivery_failure_detection_p95_minutes"],
                    thresholds.delivery_failure_detection_p95_minutes,
                ),
                _minimum_gate(
                    f"{prefix}.official_evidence_link_rate",
                    values["official_evidence_link_rate"],
                    thresholds.official_evidence_link_rate,
                ),
                _minimum_gate(
                    f"{prefix}.same_story_precision",
                    values["same_story_precision"],
                    thresholds.same_story_precision,
                ),
                _minimum_gate(
                    f"{prefix}.same_story_evaluated_pair_count",
                    values["same_story_evaluated_pair_count"],
                    1.0,
                ),
                _minimum_gate(
                    f"{prefix}.top_sensitive_human_review_rate",
                    values["top_sensitive_human_review_rate"],
                    thresholds.top_sensitive_human_review_rate,
                ),
                _minimum_gate(
                    f"{prefix}.original_language_preservation_rate",
                    values["original_language_preservation_rate"],
                    thresholds.original_language_preservation_rate,
                ),
                _minimum_gate(
                    f"{prefix}.valid_source_right_rate",
                    values["valid_source_right_rate"],
                    thresholds.valid_source_right_rate,
                ),
            ]
        )
    return {"daily": daily}, gates, revisions, set(days)


def build_performance_gates(
    records: Sequence[Mapping[str, object]],
    thresholds: GateThresholds,
) -> tuple[dict[str, object], list[dict[str, object]], set[str], set[date]]:
    dated = _sorted_unique_days(records, kind="performance")
    selected = _latest_window(dated, thresholds.consecutive_days)
    revisions = {revision for _, _, revision in selected}
    days = [day for day, _, _ in selected]
    gates: list[dict[str, object]] = [
        _boolean_gate(
            "performance.consecutive_days",
            _dates_are_consecutive(days, thresholds.consecutive_days),
            {"required": thresholds.consecutive_days, "actual": len(days)},
        )
    ]
    daily: list[dict[str, object]] = []
    for day, record, _revision in selected:
        location = f"performance[{day.isoformat()}]"
        metrics = _require_mapping(record.get("metrics"), f"{location}.metrics")
        values = {
            "availability_rate": _require_rate(metrics.get("availability_rate"), "availability_rate", location),
            "mobile_lcp_p75_seconds": _require_nonnegative_float(
                metrics.get("mobile_lcp_p75_seconds"), "mobile_lcp_p75_seconds", location
            ),
            "mobile_inp_p75_ms": _require_nonnegative_float(
                metrics.get("mobile_inp_p75_ms"), "mobile_inp_p75_ms", location
            ),
            "mobile_cls_p75": _require_nonnegative_float(
                metrics.get("mobile_cls_p75"), "mobile_cls_p75", location
            ),
        }
        daily.append({"date": day.isoformat(), **values})
        prefix = f"performance.{day.isoformat()}"
        gates.extend(
            [
                _minimum_gate(
                    f"{prefix}.availability_rate", values["availability_rate"], thresholds.availability_rate
                ),
                _maximum_gate(
                    f"{prefix}.mobile_lcp_p75_seconds",
                    values["mobile_lcp_p75_seconds"],
                    thresholds.mobile_lcp_p75_seconds,
                ),
                _maximum_gate(
                    f"{prefix}.mobile_inp_p75_ms",
                    values["mobile_inp_p75_ms"],
                    thresholds.mobile_inp_p75_ms,
                ),
                _maximum_gate(
                    f"{prefix}.mobile_cls_p75", values["mobile_cls_p75"], thresholds.mobile_cls_p75
                ),
            ]
        )
    return {"daily": daily}, gates, revisions, set(days)


def build_benchmark_gates(
    report: Mapping[str, object],
    thresholds: GateThresholds,
) -> tuple[dict[str, object], list[dict[str, object]], set[str]]:
    location = "benchmark"
    if report.get("schema_version") != 1:
        raise ReleaseEvidenceError("benchmark: schema_version must be 1")
    _require_timestamp(report.get("evaluated_at"), "evaluated_at", location)
    evidence = _require_mapping(report.get("evidence"), "benchmark.evidence")
    revision = _validate_production_provenance(evidence, "benchmark.evidence")
    for digest_field in ("same_story_dataset_sha256", "relevance_dataset_sha256"):
        digest = _require_text(evidence.get(digest_field), digest_field, "benchmark.evidence").casefold()
        if not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ReleaseEvidenceError(f"benchmark.evidence: {digest_field} must be SHA-256")
    label_sources: set[str] = set()
    for field in ("same_story_label_sources", "relevance_label_sources"):
        raw_sources = evidence.get(field)
        if not isinstance(raw_sources, list) or not raw_sources:
            raise ReleaseEvidenceError(f"benchmark.evidence: {field} must be a non-empty array")
        sources = {_require_text(value, field, "benchmark.evidence") for value in raw_sources}
        if not sources <= RELEASE_LABEL_SOURCES:
            raise ReleaseEvidenceError(f"benchmark.evidence: {field} contains non-human evidence")
        label_sources.update(sources)
    if not _require_bool(evidence.get("release_eligible"), "release_eligible", "benchmark.evidence"):
        raise ReleaseEvidenceError("benchmark.evidence: release_eligible must be true")
    benchmark_thresholds = _require_mapping(report.get("thresholds"), "benchmark.thresholds")
    same_story = _require_mapping(report.get("same_story"), "benchmark.same_story")
    relevance = _require_mapping(report.get("relevance"), "benchmark.relevance")
    failed_benchmark_gates = report.get("failed_gates")
    if not isinstance(failed_benchmark_gates, list) or any(not isinstance(value, str) for value in failed_benchmark_gates):
        raise ReleaseEvidenceError("benchmark: failed_gates must be an array of strings")
    gates = [
        _boolean_gate("benchmark.report_passed", report.get("release_gate_passed") is True),
        _boolean_gate("benchmark.no_failed_gates", not failed_benchmark_gates, failed_benchmark_gates),
        _minimum_gate(
            "benchmark.threshold.same_story_min_pairs",
            float(
                _require_nonnegative_int(
                    benchmark_thresholds.get("min_article_pairs"), "min_article_pairs", "benchmark.thresholds"
                )
            ),
            thresholds.same_story_min_pairs,
        ),
        _minimum_gate(
            "benchmark.threshold.same_story_precision",
            _require_rate(
                benchmark_thresholds.get("same_story_min_precision"),
                "same_story_min_precision",
                "benchmark.thresholds",
            ),
            thresholds.same_story_precision,
        ),
        _minimum_gate(
            "benchmark.threshold.relevance_min_events",
            float(
                _require_nonnegative_int(benchmark_thresholds.get("min_events"), "min_events", "benchmark.thresholds")
            ),
            thresholds.relevance_min_events,
        ),
        _minimum_gate(
            "benchmark.threshold.relevance_recall",
            _require_rate(
                benchmark_thresholds.get("relevance_min_recall"), "relevance_min_recall", "benchmark.thresholds"
            ),
            thresholds.relevance_recall,
        ),
        _minimum_gate(
            "benchmark.actual.same_story_pairs",
            float(_require_nonnegative_int(same_story.get("sample_count"), "sample_count", "benchmark.same_story")),
            thresholds.same_story_min_pairs,
        ),
        _minimum_gate(
            "benchmark.actual.same_story_precision",
            _require_rate(same_story.get("precision"), "precision", "benchmark.same_story"),
            thresholds.same_story_precision,
        ),
        _minimum_gate(
            "benchmark.actual.same_story_positive_class",
            float(
                _require_nonnegative_int(
                    same_story.get("actual_positive"), "actual_positive", "benchmark.same_story"
                )
                > 0
            ),
            1.0,
        ),
        _minimum_gate(
            "benchmark.actual.same_story_negative_class",
            float(
                _require_nonnegative_int(
                    same_story.get("actual_negative"), "actual_negative", "benchmark.same_story"
                )
                > 0
            ),
            1.0,
        ),
        _minimum_gate(
            "benchmark.actual.relevance_events",
            float(
                _require_nonnegative_int(
                    relevance.get("unique_event_count"), "unique_event_count", "benchmark.relevance"
                )
            ),
            thresholds.relevance_min_events,
        ),
        _minimum_gate(
            "benchmark.actual.relevance_recall",
            _require_rate(relevance.get("recall"), "recall", "benchmark.relevance"),
            thresholds.relevance_recall,
        ),
        _minimum_gate(
            "benchmark.actual.relevance_positive_class",
            float(
                _require_nonnegative_int(
                    relevance.get("actual_positive"), "actual_positive", "benchmark.relevance"
                )
                > 0
            ),
            1.0,
        ),
        _minimum_gate(
            "benchmark.actual.relevance_negative_class",
            float(
                _require_nonnegative_int(
                    relevance.get("actual_negative"), "actual_negative", "benchmark.relevance"
                )
                > 0
            ),
            1.0,
        ),
    ]
    summary = {
        "evaluated_at": report.get("evaluated_at"),
        "label_sources": sorted(label_sources),
        "same_story_sample_count": same_story.get("sample_count"),
        "same_story_precision": same_story.get("precision"),
        "relevance_unique_event_count": relevance.get("unique_event_count"),
        "relevance_recall": relevance.get("recall"),
    }
    return summary, gates, {revision}


def build_release_gate_report(
    shadow_records: Sequence[Mapping[str, object]],
    operations_records: Sequence[Mapping[str, object]],
    performance_records: Sequence[Mapping[str, object]],
    benchmark_report: Mapping[str, object],
    expected_revision: str,
    thresholds: GateThresholds | None = None,
    evidence_as_of: datetime | None = None,
) -> dict[str, object]:
    gate_thresholds = thresholds or GateThresholds()
    shadow, shadow_gates, shadow_revisions, shadow_days = build_shadow_comparison(shadow_records, gate_thresholds)
    operations, operations_gates, operations_revisions, operations_days = build_operations_gates(
        operations_records, gate_thresholds
    )
    performance, performance_gates, performance_revisions, performance_days = build_performance_gates(
        performance_records, gate_thresholds
    )
    benchmark, benchmark_gates, benchmark_revisions = build_benchmark_gates(benchmark_report, gate_thresholds)
    revisions = shadow_revisions | operations_revisions | performance_revisions | benchmark_revisions
    expected = _validate_revision(expected_revision, "expected_revision", "release_gate")
    all_revisions = revisions | {expected}
    freshness_gates: list[dict[str, object]] = []
    if evidence_as_of is not None:
        if evidence_as_of.tzinfo is None:
            raise ReleaseEvidenceError("release_gate: evidence_as_of must include a timezone")
        as_of_utc = evidence_as_of.astimezone(timezone.utc)
        timestamped_records: list[tuple[str, Mapping[str, object]]] = []
        for kind, records in (
            ("shadow", shadow_records),
            ("operations", operations_records),
            ("performance", performance_records),
        ):
            timestamped_records.extend(
                (f"{kind}[{index}]", record) for index, record in enumerate(records, start=1)
            )
        benchmark_evidence = _require_mapping(
            benchmark_report.get("evidence"), "benchmark.evidence"
        )
        timestamped_records.append(("benchmark.evidence", benchmark_evidence))
        for location, record in timestamped_records:
            collected_at = _require_timestamp(record.get("collected_at"), "collected_at", location)
            if collected_at.astimezone(timezone.utc) > as_of_utc:
                raise ReleaseEvidenceError(
                    f"{location}: collected_at cannot be later than evidence_as_of"
                )
        as_of_day = evidence_as_of.date()

        def freshness_gate(name: str, days: set[date]) -> dict[str, object]:
            latest = max(days) if days else None
            lag = (as_of_day - latest).days if latest is not None else None
            return _boolean_gate(
                name,
                lag is not None and 0 <= lag <= gate_thresholds.max_evidence_lag_days,
                {
                    "as_of": evidence_as_of.isoformat(),
                    "latest_date": latest.isoformat() if latest is not None else None,
                    "lag_days": lag,
                    "maximum_lag_days": gate_thresholds.max_evidence_lag_days,
                },
            )

        freshness_gates = [
            freshness_gate("evidence.shadow_is_recent", shadow_days),
            freshness_gate("evidence.operations_are_recent", operations_days),
            freshness_gate("evidence.performance_is_recent", performance_days),
        ]
    alignment_gates = [
        _boolean_gate(
            "evidence.single_code_revision",
            _revisions_are_compatible(revisions),
            sorted(revisions),
        ),
        _boolean_gate(
            "evidence.matches_checked_out_revision",
            _revisions_are_compatible(all_revisions),
            {"expected": expected, "evidence": sorted(revisions)},
        ),
        _boolean_gate(
            "evidence.operations_performance_dates_match",
            operations_days == performance_days,
            {
                "operations": sorted(day.isoformat() for day in operations_days),
                "performance": sorted(day.isoformat() for day in performance_days),
            },
        ),
        _boolean_gate(
            "evidence.seven_day_window_within_shadow",
            bool(operations_days) and operations_days <= shadow_days,
        ),
    ]
    gates = (
        shadow_gates
        + operations_gates
        + performance_gates
        + benchmark_gates
        + alignment_gates
        + freshness_gates
    )
    failed_gates = [str(gate.get("name")) for gate in gates if gate.get("passed") is not True]
    return {
        "schema_version": SCHEMA_VERSION,
        "evaluated_at": datetime.now().astimezone().isoformat(),
        "release_gate_passed": not failed_gates,
        "failed_gates": failed_gates,
        "expected_revision": expected,
        "code_revisions": sorted(revisions),
        "thresholds": gate_thresholds.__dict__,
        "gates": gates,
        "shadow_comparison": shadow,
        "operations": operations,
        "performance": performance,
        "benchmark": benchmark,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate the 14-day shadow and 7-day production transition gates from exported evidence."
    )
    parser.add_argument("--shadow", type=Path, required=True, help="14+ daily shadow comparison JSONL")
    parser.add_argument("--operations", type=Path, required=True, help="7+ daily run metric JSONL")
    parser.add_argument("--performance", type=Path, required=True, help="7+ daily performance JSONL")
    parser.add_argument("--benchmark", type=Path, required=True, help="quality_benchmark JSON report")
    parser.add_argument(
        "--expected-revision",
        required=True,
        help="checked-out 7-64 character hexadecimal revision; short/full SHA prefixes are accepted",
    )
    parser.add_argument(
        "--evidence-as-of",
        help="timezone-aware timestamp of the evidence-producing run; enforces a recent production window",
    )
    parser.add_argument("--output", type=Path, required=True, help="write the complete release gate JSON report")
    parser.add_argument("--shadow-output", type=Path, help="optionally write a separate shadow comparison report")
    return parser


def _write_json(path: Path, value: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    try:
        evidence_as_of = (
            _require_timestamp(args.evidence_as_of, "evidence_as_of", "release_gate")
            if args.evidence_as_of
            else None
        )
        report = build_release_gate_report(
            load_jsonl(args.shadow),
            load_jsonl(args.operations),
            load_jsonl(args.performance),
            load_json(args.benchmark),
            args.expected_revision,
            evidence_as_of=evidence_as_of,
        )
        _write_json(args.output, report)
        if args.shadow_output:
            shadow = _require_mapping(report.get("shadow_comparison"), "shadow_comparison")
            _write_json(args.shadow_output, shadow)
    except (OSError, ReleaseEvidenceError, ValueError) as exc:
        print(json.dumps({"status": "invalid-release-evidence", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(2) from exc
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    if not report["release_gate_passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
