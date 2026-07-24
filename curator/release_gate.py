from __future__ import annotations

import argparse
import hashlib
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
USABILITY_EVIDENCE_SOURCE = "human_usability_export"
APPROVAL_EVIDENCE_SOURCE = "signed_release_approval_export"
USABILITY_SEGMENTS = ("institution", "high_net_worth", "international_institution")
APPROVAL_ROLES = ("legal", "editorial", "product")
BENCHMARK_SAME_STORY_STRATA = {
    "predicted_same": 300,
    "hard_negative": 250,
    "easy_negative": 100,
}
BENCHMARK_RELEVANCE_STRATA = {
    "official_event": 300,
    "non_governance_hard_negative": 120,
}
BENCHMARK_PILOT_COUNTS = {"same_event": 50, "core_event": 30}


class ReleaseEvidenceError(ValueError):
    """Raised when supplied release evidence is malformed or ineligible."""


@dataclass(frozen=True)
class GateThresholds:
    shadow_days: int = 14
    consecutive_days: int = 7
    official_ingest_success_rate: float = 0.99
    official_lag_p95_minutes: float = 45.0
    web_distribution_success_rate: float = 0.995
    web_distribution_failure_detection_p95_minutes: float = 10.0
    official_evidence_link_rate: float = 0.95
    same_story_precision: float = 0.97
    top_sensitive_human_review_rate: float = 1.0
    original_language_preservation_rate: float = 1.0
    valid_source_right_rate: float = 1.0
    availability_rate: float = 0.999
    mobile_lcp_p75_seconds: float = 2.5
    mobile_inp_p75_ms: float = 200.0
    mobile_cls_p75: float = 0.1
    mobile_metric_min_samples: int = 20
    same_story_min_pairs: int = 500
    relevance_min_events: int = 300
    relevance_min_hard_negatives: int = 120
    relevance_precision: float = 0.90
    relevance_recall: float = 0.95
    usability_evaluator_count: int = 15
    usability_successful_evaluators: int = 12
    usability_target_seconds: int = 180
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


def _verified_count_rate(
    metrics: Mapping[str, object],
    raw_counts: Mapping[str, object],
    *,
    rate_field: str,
    numerator_field: str,
    denominator_field: str,
    location: str,
) -> tuple[float, int, int]:
    numerator = _require_nonnegative_int(raw_counts.get(numerator_field), numerator_field, location)
    denominator = _require_nonnegative_int(raw_counts.get(denominator_field), denominator_field, location)
    if denominator < 1:
        raise ReleaseEvidenceError(f"{location}: {denominator_field} must be non-zero")
    if numerator > denominator:
        raise ReleaseEvidenceError(f"{location}: {numerator_field} exceeds {denominator_field}")
    rate = _require_rate(metrics.get(rate_field), rate_field, location)
    calculated = numerator / denominator
    if not math.isclose(rate, calculated, rel_tol=0.0, abs_tol=0.000001):
        raise ReleaseEvidenceError(f"{location}: {rate_field} does not match its raw counts")
    return rate, numerator, denominator


def _verified_optional_count_rate(
    metrics: Mapping[str, object],
    raw_counts: Mapping[str, object],
    *,
    rate_field: str,
    numerator_field: str,
    denominator_field: str,
    location: str,
) -> tuple[float | None, int, int]:
    numerator = _require_nonnegative_int(raw_counts.get(numerator_field), numerator_field, location)
    denominator = _require_nonnegative_int(raw_counts.get(denominator_field), denominator_field, location)
    if numerator > denominator:
        raise ReleaseEvidenceError(f"{location}: {numerator_field} exceeds {denominator_field}")
    raw_rate = metrics.get(rate_field)
    if denominator == 0:
        if numerator != 0 or raw_rate is not None:
            raise ReleaseEvidenceError(f"{location}: zero {denominator_field} requires a null rate")
        return None, numerator, denominator
    rate = _require_rate(raw_rate, rate_field, location)
    calculated = numerator / denominator
    if not math.isclose(rate, calculated, rel_tol=0.0, abs_tol=0.000001):
        raise ReleaseEvidenceError(f"{location}: {rate_field} does not match its raw counts")
    return rate, numerator, denominator


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
    total_eligible_legacy_records = 0
    for day, record, _revision in selected:
        location = f"shadow[{day.isoformat()}]"
        legacy_run = _require_mapping(record.get("legacy_run"), f"{location}.legacy_run")
        candidate_run = _require_mapping(record.get("candidate_run"), f"{location}.candidate_run")
        legacy_status = _require_text(legacy_run.get("status"), "status", f"{location}.legacy_run")
        candidate_status = _require_text(candidate_run.get("status"), "status", f"{location}.candidate_run")
        day_reviewed = _require_bool(record.get("discrepancies_reviewed"), "discrepancies_reviewed", location)
        crosswalk = _require_mapping(record.get("legacy_crosswalk"), f"{location}.legacy_crosswalk")
        crosswalk_schema = _require_nonnegative_int(
            crosswalk.get("schema_version"), "schema_version", f"{location}.legacy_crosswalk"
        )
        eligible_legacy = _require_nonnegative_int(
            crosswalk.get("eligible_legacy_record_count"),
            "eligible_legacy_record_count",
            f"{location}.legacy_crosswalk",
        )
        crosswalked_legacy = _require_nonnegative_int(
            crosswalk.get("crosswalked_legacy_record_count"),
            "crosswalked_legacy_record_count",
            f"{location}.legacy_crosswalk",
        )
        unmatched_legacy = _require_nonnegative_int(
            crosswalk.get("unmatched_legacy_record_count"),
            "unmatched_legacy_record_count",
            f"{location}.legacy_crosswalk",
        )
        ambiguous_legacy = _require_nonnegative_int(
            crosswalk.get("ambiguous_legacy_record_count"),
            "ambiguous_legacy_record_count",
            f"{location}.legacy_crosswalk",
        )
        coverage_rate = _require_rate(
            crosswalk.get("coverage_rate"), "coverage_rate", f"{location}.legacy_crosswalk"
        )
        crosswalk_sha = _require_text(
            crosswalk.get("crosswalk_sha256"),
            "crosswalk_sha256",
            f"{location}.legacy_crosswalk",
        ).casefold()
        if (
            crosswalk_schema != 1
            or eligible_legacy < 1
            or crosswalked_legacy != eligible_legacy
            or unmatched_legacy != 0
            or ambiguous_legacy != 0
            or not math.isclose(coverage_rate, 1.0, rel_tol=0.0, abs_tol=0.000001)
            or re.fullmatch(r"[0-9a-f]{64}", crosswalk_sha) is None
        ):
            raise ReleaseEvidenceError(f"{location}: incomplete legacy crosswalk")
        total_eligible_legacy_records += eligible_legacy
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
                "legacy_crosswalk": {
                    "schema_version": 1,
                    "eligible_legacy_record_count": eligible_legacy,
                    "crosswalked_legacy_record_count": crosswalked_legacy,
                    "unmatched_legacy_record_count": unmatched_legacy,
                    "ambiguous_legacy_record_count": ambiguous_legacy,
                    "coverage_rate": coverage_rate,
                    "crosswalk_sha256": crosswalk_sha,
                },
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
        _boolean_gate(
            "shadow.legacy_crosswalk_denominator",
            total_eligible_legacy_records > 0,
            total_eligible_legacy_records,
        ),
    ]
    report: dict[str, object] = {
        "required_days": thresholds.shadow_days,
        "start_date": days[0].isoformat() if days else None,
        "end_date": days[-1].isoformat() if days else None,
        "legacy_event_count": total_legacy,
        "candidate_event_count": total_candidate,
        "matched_event_count": total_matched,
        "eligible_legacy_record_count": total_eligible_legacy_records,
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
    corpus_pairs: dict[str, tuple[str, str]] = {
        "official_evidence_link_rate": (
            "official_evidence_linked_count",
            "official_evidence_total_count",
        ),
        "top_sensitive_human_review_rate": (
            "top_sensitive_reviewed_count",
            "top_sensitive_total_count",
        ),
        "original_language_preservation_rate": (
            "original_language_preserved_count",
            "original_language_total_count",
        ),
        "valid_source_right_rate": ("valid_source_right_count", "source_right_total_count"),
    }
    corpus_totals = {rate_name: [0, 0] for rate_name in corpus_pairs}
    kind_window_observations = 0
    kind_window_samples = 0
    kind_window_lags: list[float] = []
    for day, record, _revision in selected:
        location = f"operations[{day.isoformat()}]"
        metrics = _require_mapping(record.get("metrics"), f"{location}.metrics")
        contract_version = _require_nonnegative_int(
            metrics.get("metrics_contract_version"), "metrics_contract_version", location
        )
        if contract_version != 2:
            raise ReleaseEvidenceError(f"{location}: metrics_contract_version must be 2")
        distribution_mode = _require_text(
            metrics.get("distribution_mode"), "distribution_mode", location
        )
        if distribution_mode != "web_only":
            raise ReleaseEvidenceError(f"{location}: distribution_mode must be 'web_only'")
        web_attempted = _require_nonnegative_int(
            metrics.get("web_distribution_attempted_count"),
            "web_distribution_attempted_count",
            location,
        )
        web_succeeded = _require_nonnegative_int(
            metrics.get("web_distribution_succeeded_count"),
            "web_distribution_succeeded_count",
            location,
        )
        if web_attempted < 1:
            raise ReleaseEvidenceError(f"{location}: web distribution requires a non-zero denominator")
        if web_succeeded > web_attempted:
            raise ReleaseEvidenceError(f"{location}: web distribution successes exceed attempts")
        web_success_rate = _require_rate(
            metrics.get("web_distribution_success_rate"), "web_distribution_success_rate", location
        )
        calculated_web_rate = web_succeeded / web_attempted
        if not math.isclose(web_success_rate, calculated_web_rate, rel_tol=0.0, abs_tol=0.000001):
            raise ReleaseEvidenceError(
                f"{location}: web_distribution_success_rate does not match its raw counts"
            )
        telegram_attempted = _require_nonnegative_int(
            metrics.get("telegram_delivery_attempted_count"),
            "telegram_delivery_attempted_count",
            location,
        )
        raw_counts = _require_mapping(metrics.get("raw_counts"), f"{location}.raw_counts")
        official_ingest_rate, _, _ = _verified_count_rate(
            metrics,
            raw_counts,
            rate_field="official_ingest_success_rate",
            numerator_field="official_ingest_succeeded_count",
            denominator_field="official_ingest_expected_count",
            location=location,
        )
        dart_ingest_rate, _, _ = _verified_count_rate(
            metrics,
            raw_counts,
            rate_field="dart_ingest_success_rate",
            numerator_field="dart_ingest_succeeded_count",
            denominator_field="dart_ingest_expected_count",
            location=location,
        )
        kind_ingest_rate, _, _ = _verified_count_rate(
            metrics,
            raw_counts,
            rate_field="kind_ingest_success_rate",
            numerator_field="kind_ingest_succeeded_count",
            denominator_field="kind_ingest_expected_count",
            location=location,
        )
        official_evidence_rate, official_linked, official_total = _verified_optional_count_rate(
            metrics,
            raw_counts,
            rate_field="official_evidence_link_rate",
            numerator_field="official_evidence_linked_count",
            denominator_field="official_evidence_total_count",
            location=location,
        )
        top_review_rate, top_reviewed, top_total = _verified_optional_count_rate(
            metrics,
            raw_counts,
            rate_field="top_sensitive_human_review_rate",
            numerator_field="top_sensitive_reviewed_count",
            denominator_field="top_sensitive_total_count",
            location=location,
        )
        language_rate, language_preserved, language_total = _verified_optional_count_rate(
            metrics,
            raw_counts,
            rate_field="original_language_preservation_rate",
            numerator_field="original_language_preserved_count",
            denominator_field="original_language_total_count",
            location=location,
        )
        rights_rate, rights_valid, rights_total = _verified_optional_count_rate(
            metrics,
            raw_counts,
            rate_field="valid_source_right_rate",
            numerator_field="valid_source_right_count",
            denominator_field="source_right_total_count",
            location=location,
        )
        official_lag = _require_nonnegative_float(
            metrics.get("official_lag_p95_minutes"), "official_lag_p95_minutes", location
        )
        dart_poll_interval = _require_nonnegative_float(
            metrics.get("dart_success_poll_interval_p95_minutes"),
            "dart_success_poll_interval_p95_minutes",
            location,
        )
        kind_observation_count = _require_nonnegative_int(
            raw_counts.get("kind_observation_count"), "kind_observation_count", location
        )
        kind_lag_sample_count = _require_nonnegative_int(
            raw_counts.get("kind_lag_sample_count"), "kind_lag_sample_count", location
        )
        raw_kind_lag = metrics.get("kind_observation_lag_p95_minutes")
        if kind_observation_count == 0:
            if kind_lag_sample_count != 0 or raw_kind_lag is not None:
                raise ReleaseEvidenceError(f"{location}: invalid KIND no-disclosure N/A evidence")
            kind_observation_lag: float | None = None
        else:
            if kind_lag_sample_count != kind_observation_count or raw_kind_lag is None:
                raise ReleaseEvidenceError(f"{location}: incomplete KIND observation timestamps")
            kind_observation_lag = _require_nonnegative_float(
                raw_kind_lag, "kind_observation_lag_p95_minutes", location
            )
        expected_official_lag = (
            dart_poll_interval
            if kind_observation_lag is None
            else max(dart_poll_interval, kind_observation_lag)
        )
        if not math.isclose(official_lag, expected_official_lag, rel_tol=0.0, abs_tol=0.000001):
            raise ReleaseEvidenceError(f"{location}: official_lag_p95_minutes does not match actual source lags")
        if metrics.get("content_scope") != "governance_corpus_2021_plus_kst_day_end_v2":
            raise ReleaseEvidenceError(f"{location}: invalid content_scope")
        content_snapshot_at = _require_timestamp(
            metrics.get("content_snapshot_at"), "content_snapshot_at", location
        )
        snapshot_kst = content_snapshot_at.astimezone(timezone(timedelta(hours=9)))
        if snapshot_kst.date() != day or snapshot_kst.strftime("%H:%M:%S") != "23:59:59":
            raise ReleaseEvidenceError(f"{location}: content_snapshot_at must be the KST day end")
        failure_detection_lag = _require_nonnegative_float(
            metrics.get("web_distribution_failure_detection_p95_minutes"),
            "web_distribution_failure_detection_p95_minutes",
            location,
        )
        values = {
            "metrics_contract_version": contract_version,
            "distribution_mode": distribution_mode,
            "official_ingest_success_rate": official_ingest_rate,
            "dart_ingest_success_rate": dart_ingest_rate,
            "kind_ingest_success_rate": kind_ingest_rate,
            "official_lag_p95_minutes": official_lag,
            "dart_success_poll_interval_p95_minutes": dart_poll_interval,
            "kind_observation_lag_p95_minutes": kind_observation_lag,
            "kind_observation_count": kind_observation_count,
            "kind_lag_sample_count": kind_lag_sample_count,
            "content_snapshot_at": content_snapshot_at.isoformat(),
            "content_scope": "governance_corpus_2021_plus_kst_day_end_v2",
            "web_distribution_attempted_count": web_attempted,
            "web_distribution_succeeded_count": web_succeeded,
            "web_distribution_success_rate": web_success_rate,
            "web_distribution_failure_detection_p95_minutes": failure_detection_lag,
            "telegram_delivery_attempted_count": telegram_attempted,
            "official_evidence_link_rate": official_evidence_rate,
            "top_sensitive_human_review_rate": top_review_rate,
            "original_language_preservation_rate": language_rate,
            "valid_source_right_rate": rights_rate,
            "raw_counts": dict(raw_counts),
        }
        daily_corpus_counts = {
            "official_evidence_link_rate": (official_linked, official_total),
            "top_sensitive_human_review_rate": (top_reviewed, top_total),
            "original_language_preservation_rate": (language_preserved, language_total),
            "valid_source_right_rate": (rights_valid, rights_total),
        }
        for rate_name, (numerator, denominator) in daily_corpus_counts.items():
            corpus_totals[rate_name][0] += numerator
            corpus_totals[rate_name][1] += denominator
        kind_window_observations += kind_observation_count
        kind_window_samples += kind_lag_sample_count
        if kind_observation_lag is not None:
            kind_window_lags.append(kind_observation_lag)
        daily.append({"date": day.isoformat(), **values})
        prefix = f"operations.{day.isoformat()}"
        day_gates = [
                _minimum_gate(
                    f"{prefix}.official_ingest_success_rate",
                    official_ingest_rate,
                    thresholds.official_ingest_success_rate,
                ),
                _minimum_gate(
                    f"{prefix}.dart_ingest_success_rate",
                    dart_ingest_rate,
                    thresholds.official_ingest_success_rate,
                ),
                _minimum_gate(
                    f"{prefix}.kind_ingest_success_rate",
                    kind_ingest_rate,
                    thresholds.official_ingest_success_rate,
                ),
                _maximum_gate(
                    f"{prefix}.official_lag_p95_minutes",
                    official_lag,
                    thresholds.official_lag_p95_minutes,
                ),
                _maximum_gate(
                    f"{prefix}.dart_success_poll_interval_p95_minutes",
                    dart_poll_interval,
                    thresholds.official_lag_p95_minutes,
                ),
                _minimum_gate(
                    f"{prefix}.web_distribution_attempted_count",
                    float(web_attempted),
                    1.0,
                ),
                _minimum_gate(
                    f"{prefix}.web_distribution_success_rate",
                    web_success_rate,
                    thresholds.web_distribution_success_rate,
                ),
                _maximum_gate(
                    f"{prefix}.web_distribution_failure_detection_p95_minutes",
                    failure_detection_lag,
                    thresholds.web_distribution_failure_detection_p95_minutes,
                ),
                _maximum_gate(
                    f"{prefix}.telegram_delivery_attempted_count",
                    float(telegram_attempted),
                    0.0,
                ),
            ]
        if kind_observation_lag is not None:
            day_gates.append(
                _maximum_gate(
                    f"{prefix}.kind_observation_lag_p95_minutes",
                    kind_observation_lag,
                    thresholds.official_lag_p95_minutes,
                )
            )
        else:
            day_gates.append(
                _boolean_gate(
                    f"{prefix}.kind_no_disclosure_n_a",
                    kind_observation_count == 0 and kind_lag_sample_count == 0,
                    {"kind_observation_count": kind_observation_count},
                )
            )
        gates.extend(day_gates)

    aggregate_rates: dict[str, float | None] = {}
    thresholds_by_rate = {
        "official_evidence_link_rate": thresholds.official_evidence_link_rate,
        "top_sensitive_human_review_rate": thresholds.top_sensitive_human_review_rate,
        "original_language_preservation_rate": thresholds.original_language_preservation_rate,
        "valid_source_right_rate": thresholds.valid_source_right_rate,
    }
    aggregate_counts: dict[str, dict[str, int]] = {}
    for rate_name, (numerator, denominator) in corpus_totals.items():
        aggregate_counts[rate_name] = {"numerator": numerator, "denominator": denominator}
        aggregate_rates[rate_name] = None if denominator == 0 else numerator / denominator
        gates.append(
            _boolean_gate(
                f"operations.window.{rate_name}.nonzero_denominator",
                denominator > 0,
                aggregate_counts[rate_name],
            )
        )
        if denominator > 0:
            gates.append(
                _minimum_gate(
                    f"operations.window.{rate_name}",
                    numerator / denominator,
                    thresholds_by_rate[rate_name],
                )
            )
    gates.append(
        _boolean_gate(
            "operations.window.kind_observation_samples",
            kind_window_observations > 0 and kind_window_samples == kind_window_observations,
            {"observations": kind_window_observations, "samples": kind_window_samples},
        )
    )
    if kind_window_lags:
        gates.append(
            _maximum_gate(
                "operations.window.kind_observation_lag_p95_minutes",
                max(kind_window_lags),
                thresholds.official_lag_p95_minutes,
            )
        )
    return {
        "daily": daily,
        "window": {
            "corpus_rates": aggregate_rates,
            "corpus_counts": aggregate_counts,
            "kind_observation_count": kind_window_observations,
            "kind_lag_sample_count": kind_window_samples,
            "kind_observation_lag_p95_minutes_conservative": max(kind_window_lags) if kind_window_lags else None,
        },
    }, gates, revisions, set(days)


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
        raw_counts = _require_mapping(metrics.get("raw_counts"), f"{location}.metrics.raw_counts")
        availability_rate, availability_succeeded, availability_attempted = _verified_count_rate(
            metrics,
            raw_counts,
            rate_field="availability_rate",
            numerator_field="availability_succeeded_count",
            denominator_field="availability_attempted_count",
            location=location,
        )
        sample_counts: dict[str, int] = {}
        for metric_name in ("lcp", "inp", "cls"):
            field = f"mobile_{metric_name}_sample_count"
            count = _require_nonnegative_int(raw_counts.get(field), field, location)
            if count < thresholds.mobile_metric_min_samples:
                raise ReleaseEvidenceError(
                    f"{location}: {field} must contain at least "
                    f"{thresholds.mobile_metric_min_samples} real route measurements"
                )
            sample_counts[field] = count
        mobile_lcp = _require_nonnegative_float(
            metrics.get("mobile_lcp_p75_seconds"), "mobile_lcp_p75_seconds", location
        )
        mobile_inp = _require_nonnegative_float(
            metrics.get("mobile_inp_p75_ms"), "mobile_inp_p75_ms", location
        )
        mobile_cls = _require_nonnegative_float(
            metrics.get("mobile_cls_p75"), "mobile_cls_p75", location
        )
        values: dict[str, object] = {
            "availability_rate": availability_rate,
            "mobile_lcp_p75_seconds": mobile_lcp,
            "mobile_inp_p75_ms": mobile_inp,
            "mobile_cls_p75": mobile_cls,
            "raw_counts": {
                "availability_attempted_count": availability_attempted,
                "availability_succeeded_count": availability_succeeded,
                **sample_counts,
            },
        }
        daily.append({"date": day.isoformat(), **values})
        prefix = f"performance.{day.isoformat()}"
        gates.extend(
            [
                _minimum_gate(
                    f"{prefix}.availability_rate", availability_rate, thresholds.availability_rate
                ),
                _maximum_gate(
                    f"{prefix}.mobile_lcp_p75_seconds",
                    mobile_lcp,
                    thresholds.mobile_lcp_p75_seconds,
                ),
                _maximum_gate(
                    f"{prefix}.mobile_inp_p75_ms",
                    mobile_inp,
                    thresholds.mobile_inp_p75_ms,
                ),
                _maximum_gate(
                    f"{prefix}.mobile_cls_p75", mobile_cls, thresholds.mobile_cls_p75
                ),
            ]
        )
    return {"daily": daily}, gates, revisions, set(days)


def _exact_benchmark_strata(
    value: object,
    expected: Mapping[str, int],
    *,
    location: str,
) -> dict[str, int]:
    strata = _require_mapping(value, location)
    if set(strata) != set(expected):
        raise ReleaseEvidenceError(f"{location}: exact stratum keys are required")
    result: dict[str, int] = {}
    for name, expected_count in expected.items():
        count = _require_nonnegative_int(strata.get(name), name, location)
        if count != expected_count:
            raise ReleaseEvidenceError(
                f"{location}: {name} must equal {expected_count}, got {count}"
            )
        result[name] = count
    return result


def _benchmark_digest_fields(
    value: Mapping[str, object],
    fields: Sequence[str],
    *,
    location: str,
) -> None:
    for field in fields:
        _sha256_digest(value.get(field), field, location)


def _validate_benchmark_review_process(
    value: object,
    *,
    evidence: Mapping[str, object],
    same_story: Mapping[str, object],
    relevance: Mapping[str, object],
) -> dict[str, object]:
    location = "benchmark.review_process"
    process = _require_mapping(value, location)
    if process.get("schema_version") != 1:
        raise ReleaseEvidenceError(f"{location}: schema_version must be 1")
    if process.get("contract") != "independent-human-review-v1":
        raise ReleaseEvidenceError(f"{location}: unsupported review contract")
    process_sha = _sha256_digest(process.get("process_sha256"), "process_sha256", location)
    expected_evidence_sha = _sha256_digest(
        evidence.get("benchmark_process_sha256"),
        "benchmark_process_sha256",
        "benchmark.evidence",
    )
    canonical_process = dict(process)
    canonical_process.pop("process_sha256", None)
    calculated_process_sha = hashlib.sha256(
        json.dumps(
            canonical_process,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    if process_sha != calculated_process_sha or process_sha != expected_evidence_sha:
        raise ReleaseEvidenceError(f"{location}: process SHA-256 mismatch")
    _sha256_digest(
        process.get("candidate_manifest_sha256"),
        "candidate_manifest_sha256",
        location,
    )

    candidate_files = _require_mapping(process.get("candidate_files"), f"{location}.candidate_files")
    if set(candidate_files) != {"same_story", "relevance"}:
        raise ReleaseEvidenceError(f"{location}.candidate_files: exact task set is required")
    candidate_summary: dict[str, object] = {}
    for task, expected_strata in (
        ("same_story", BENCHMARK_SAME_STORY_STRATA),
        ("relevance", BENCHMARK_RELEVANCE_STRATA),
    ):
        candidate = _require_mapping(
            candidate_files.get(task), f"{location}.candidate_files.{task}"
        )
        _sha256_digest(candidate.get("sha256"), "sha256", f"{location}.candidate_files.{task}")
        expected_count = sum(expected_strata.values())
        count = _require_nonnegative_int(
            candidate.get("item_count"), "item_count", f"{location}.candidate_files.{task}"
        )
        if count != expected_count:
            raise ReleaseEvidenceError(
                f"{location}.candidate_files.{task}: item_count must equal {expected_count}"
            )
        strata = _exact_benchmark_strata(
            candidate.get("strata"),
            expected_strata,
            location=f"{location}.candidate_files.{task}.strata",
        )
        candidate_summary[task] = {"item_count": count, "strata": strata}

    reviewers = _require_mapping(process.get("reviewers"), f"{location}.reviewers")
    reviewer_count = _require_nonnegative_int(
        reviewers.get("reviewer_count"), "reviewer_count", f"{location}.reviewers"
    )
    reviewer_ids = reviewers.get("reviewer_ids")
    if (
        reviewer_count != 2
        or not isinstance(reviewer_ids, list)
        or len(reviewer_ids) != 2
        or any(not isinstance(item, str) or not item.strip() for item in reviewer_ids)
        or len(set(reviewer_ids)) != 2
    ):
        raise ReleaseEvidenceError(f"{location}.reviewers: exactly two distinct reviewers required")
    _benchmark_digest_fields(
        reviewers,
        (
            "same_story_reviewer_a_sha256",
            "same_story_reviewer_b_sha256",
            "relevance_reviewer_a_sha256",
            "relevance_reviewer_b_sha256",
        ),
        location=f"{location}.reviewers",
    )

    pilot = _require_mapping(process.get("pilot"), f"{location}.pilot")
    _sha256_digest(pilot.get("report_sha256"), "report_sha256", f"{location}.pilot")
    pilot_summary: dict[str, object] = {}
    for task, expected_count in BENCHMARK_PILOT_COUNTS.items():
        pilot_task = _require_mapping(pilot.get(task), f"{location}.pilot.{task}")
        count = _require_nonnegative_int(
            pilot_task.get("item_count"), "item_count", f"{location}.pilot.{task}"
        )
        if count != expected_count:
            raise ReleaseEvidenceError(
                f"{location}.pilot.{task}: item_count must equal {expected_count}"
            )
        task_reviewers = pilot_task.get("reviewer_ids")
        if (
            not isinstance(task_reviewers, list)
            or len(task_reviewers) != 2
            or set(task_reviewers) != set(reviewer_ids)
        ):
            raise ReleaseEvidenceError(
                f"{location}.pilot.{task}: pilot reviewers do not match full blind reviewers"
            )
        threshold = _require_rate(
            pilot_task.get("threshold"), "threshold", f"{location}.pilot.{task}"
        )
        kappa = _require_rate(
            pilot_task.get("cohen_kappa"), "cohen_kappa", f"{location}.pilot.{task}"
        )
        if threshold < 0.8 or kappa < 0.8:
            raise ReleaseEvidenceError(
                f"{location}.pilot.{task}: threshold and Cohen's kappa must be >= 0.8"
            )
        _sha256_digest(
            pilot_task.get("item_ids_sha256"),
            "item_ids_sha256",
            f"{location}.pilot.{task}",
        )
        pilot_summary[task] = {
            "item_count": count,
            "cohen_kappa": kappa,
            "threshold": threshold,
        }

    final = _require_mapping(process.get("final"), f"{location}.final")
    final_same_count = _require_nonnegative_int(
        final.get("same_story_item_count"), "same_story_item_count", f"{location}.final"
    )
    final_relevance_count = _require_nonnegative_int(
        final.get("relevance_item_count"), "relevance_item_count", f"{location}.final"
    )
    if final_same_count < 500:
        raise ReleaseEvidenceError(f"{location}.final: at least 500 final same-story pairs required")
    if final_same_count != _require_nonnegative_int(
        same_story.get("sample_count"), "sample_count", "benchmark.same_story"
    ):
        raise ReleaseEvidenceError(f"{location}.final: same-story count does not match metrics")
    if final_relevance_count != _require_nonnegative_int(
        relevance.get("sample_count"), "sample_count", "benchmark.relevance"
    ):
        raise ReleaseEvidenceError(f"{location}.final: relevance count does not match metrics")
    final_same_strata = _exact_benchmark_strata(
        final.get("same_story_strata"),
        BENCHMARK_SAME_STORY_STRATA,
        location=f"{location}.final.same_story_strata",
    )
    final_relevance_strata = _exact_benchmark_strata(
        final.get("relevance_strata"),
        BENCHMARK_RELEVANCE_STRATA,
        location=f"{location}.final.relevance_strata",
    )
    if final_same_count != sum(final_same_strata.values()):
        raise ReleaseEvidenceError(f"{location}.final: same-story strata/count mismatch")
    if final_relevance_count != sum(final_relevance_strata.values()):
        raise ReleaseEvidenceError(f"{location}.final: relevance strata/count mismatch")
    reported_same_strata = _exact_benchmark_strata(
        same_story.get("strata"),
        BENCHMARK_SAME_STORY_STRATA,
        location="benchmark.same_story.strata",
    )
    if reported_same_strata != final_same_strata:
        raise ReleaseEvidenceError(f"{location}.final: same-story strata do not match metrics")

    adjudication = _require_mapping(
        process.get("adjudication"), f"{location}.adjudication"
    )
    _sha256_digest(
        adjudication.get("dataset_sha256"),
        "dataset_sha256",
        f"{location}.adjudication",
    )
    disagreement_count = _require_nonnegative_int(
        adjudication.get("disagreement_count"),
        "disagreement_count",
        f"{location}.adjudication",
    )
    adjudicated_count = _require_nonnegative_int(
        adjudication.get("adjudicated_count"),
        "adjudicated_count",
        f"{location}.adjudication",
    )
    unresolved_count = _require_nonnegative_int(
        adjudication.get("unresolved_count"),
        "unresolved_count",
        f"{location}.adjudication",
    )
    task_counts = _require_mapping(
        adjudication.get("task_counts"), f"{location}.adjudication.task_counts"
    )
    if set(task_counts) != {"same_story", "relevance"}:
        raise ReleaseEvidenceError(
            f"{location}.adjudication.task_counts: exact task set required"
        )
    task_total = sum(
        _require_nonnegative_int(
            task_counts.get(task), task, f"{location}.adjudication.task_counts"
        )
        for task in ("same_story", "relevance")
    )
    if (
        unresolved_count != 0
        or disagreement_count != adjudicated_count
        or task_total != adjudicated_count
    ):
        raise ReleaseEvidenceError(
            f"{location}.adjudication: every disagreement must be adjudicated with none unresolved"
        )
    return {
        "contract": "independent-human-review-v1",
        "process_sha256": process_sha,
        "candidate_files": candidate_summary,
        "reviewer_count": reviewer_count,
        "reviewer_ids": list(reviewer_ids),
        "pilot": pilot_summary,
        "final": {
            "same_story_item_count": final_same_count,
            "relevance_item_count": final_relevance_count,
            "same_story_strata": final_same_strata,
            "relevance_strata": final_relevance_strata,
        },
        "adjudication": {
            "disagreement_count": disagreement_count,
            "adjudicated_count": adjudicated_count,
            "unresolved_count": unresolved_count,
        },
    }


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
    for digest_field in (
        "same_story_dataset_sha256",
        "relevance_dataset_sha256",
        "benchmark_process_sha256",
    ):
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
    review_process = _validate_benchmark_review_process(
        report.get("review_process"),
        evidence=evidence,
        same_story=same_story,
        relevance=relevance,
    )
    failed_benchmark_gates = report.get("failed_gates")
    if not isinstance(failed_benchmark_gates, list) or any(not isinstance(value, str) for value in failed_benchmark_gates):
        raise ReleaseEvidenceError("benchmark: failed_gates must be an array of strings")
    gates = [
        _boolean_gate("benchmark.report_passed", report.get("release_gate_passed") is True),
        _boolean_gate("benchmark.no_failed_gates", not failed_benchmark_gates, failed_benchmark_gates),
        _boolean_gate("benchmark.independent_review_process", bool(review_process)),
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
            "benchmark.threshold.relevance_min_hard_negatives",
            float(
                _require_nonnegative_int(
                    benchmark_thresholds.get("min_relevance_hard_negatives"),
                    "min_relevance_hard_negatives",
                    "benchmark.thresholds",
                )
            ),
            thresholds.relevance_min_hard_negatives,
        ),
        _minimum_gate(
            "benchmark.threshold.relevance_precision",
            _require_rate(
                benchmark_thresholds.get("relevance_min_precision"),
                "relevance_min_precision",
                "benchmark.thresholds",
            ),
            thresholds.relevance_precision,
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
                    relevance.get("official_linked_event_count"),
                    "official_linked_event_count",
                    "benchmark.relevance",
                )
            ),
            thresholds.relevance_min_events,
        ),
        _minimum_gate(
            "benchmark.actual.relevance_hard_negatives",
            float(
                _require_nonnegative_int(
                    relevance.get("hard_negative_count"),
                    "hard_negative_count",
                    "benchmark.relevance",
                )
            ),
            thresholds.relevance_min_hard_negatives,
        ),
        _minimum_gate(
            "benchmark.actual.relevance_precision",
            _require_rate(relevance.get("precision"), "precision", "benchmark.relevance"),
            thresholds.relevance_precision,
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
        "relevance_official_linked_event_count": relevance.get("official_linked_event_count"),
        "relevance_hard_negative_count": relevance.get("hard_negative_count"),
        "relevance_precision": relevance.get("precision"),
        "relevance_recall": relevance.get("recall"),
        "review_process": review_process,
    }
    return summary, gates, {revision}


def _sha256_digest(value: object, field: str, location: str) -> str:
    digest = _require_text(value, field, location).casefold()
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ReleaseEvidenceError(f"{location}: {field} must be SHA-256")
    return digest


def build_usability_gates(
    report: Mapping[str, object],
    thresholds: GateThresholds,
) -> tuple[dict[str, object], list[dict[str, object]], set[str]]:
    location = "usability"
    revision = _validate_production_provenance(report, location)
    if len(revision) != 40:
        raise ReleaseEvidenceError("usability: code_revision must be a full 40-character Git SHA")
    source = _require_text(report.get("evidence_source"), "evidence_source", location)
    if source != USABILITY_EVIDENCE_SOURCE:
        raise ReleaseEvidenceError(
            f"usability: evidence_source must be {USABILITY_EVIDENCE_SOURCE!r}"
        )
    dataset_digest = _sha256_digest(report.get("dataset_sha256"), "dataset_sha256", location)
    collected_at = _require_timestamp(report.get("collected_at"), "collected_at", location)
    target_seconds = _require_nonnegative_int(report.get("target_seconds"), "target_seconds", location)
    evaluator_count = _require_nonnegative_int(report.get("evaluator_count"), "evaluator_count", location)
    succeeded_count = _require_nonnegative_int(
        report.get("succeeded_evaluator_count"), "succeeded_evaluator_count", location
    )
    if evaluator_count < 1:
        raise ReleaseEvidenceError("usability: evaluator_count must be non-zero")
    if succeeded_count > evaluator_count:
        raise ReleaseEvidenceError("usability: succeeded_evaluator_count exceeds evaluator_count")
    success_rate = _require_rate(report.get("success_rate"), "success_rate", location)
    if not math.isclose(success_rate, succeeded_count / evaluator_count, rel_tol=0, abs_tol=0.000001):
        raise ReleaseEvidenceError("usability: success_rate does not match raw counts")
    raw_evaluations = report.get("evaluations")
    if not isinstance(raw_evaluations, list):
        raise ReleaseEvidenceError("usability: evaluations must be an array")
    if len(raw_evaluations) != evaluator_count:
        raise ReleaseEvidenceError("usability: evaluator_count does not match evaluations")

    identifiers: set[str] = set()
    segment_counts = {segment: 0 for segment in USABILITY_SEGMENTS}
    calculated_successes = 0
    canonical_records: list[str] = []
    for index, value in enumerate(raw_evaluations):
        item_location = f"usability.evaluations[{index}]"
        evaluation = _require_mapping(value, item_location)
        identifier = _require_text(evaluation.get("evaluation_id"), "evaluation_id", item_location)
        if identifier in identifiers:
            raise ReleaseEvidenceError(f"{item_location}: duplicate evaluation_id")
        identifiers.add(identifier)
        segment = _require_text(evaluation.get("segment"), "segment", item_location)
        if segment not in segment_counts:
            raise ReleaseEvidenceError(f"{item_location}: unsupported segment {segment!r}")
        segment_counts[segment] += 1
        completed_at = _require_timestamp(
            evaluation.get("completed_at"), "completed_at", item_location
        )
        if completed_at > collected_at:
            raise ReleaseEvidenceError(f"{item_location}: completed_at is later than collected_at")
        duration = _require_nonnegative_float(
            evaluation.get("duration_seconds"), "duration_seconds", item_location
        )
        checks = [
            _require_bool(evaluation.get(field), field, item_location)
            for field in (
                "identified_event",
                "identified_actors",
                "identified_official_evidence",
                "identified_current_status",
            )
        ]
        succeeded = _require_bool(evaluation.get("succeeded"), "succeeded", item_location)
        calculated = duration <= target_seconds and all(checks)
        if succeeded != calculated:
            raise ReleaseEvidenceError(
                f"{item_location}: succeeded does not match duration and required findings"
            )
        calculated_successes += int(succeeded)
        canonical_records.append(
            json.dumps(dict(evaluation), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        )
    if calculated_successes != succeeded_count:
        raise ReleaseEvidenceError("usability: succeeded_evaluator_count does not match evaluations")
    calculated_digest = hashlib.sha256("\n".join(canonical_records).encode("utf-8")).hexdigest()
    if calculated_digest != dataset_digest:
        raise ReleaseEvidenceError("usability: dataset_sha256 does not match evaluations")

    gates = [
        _boolean_gate(
            "usability.exact_evaluator_count",
            evaluator_count == thresholds.usability_evaluator_count,
            {"required": thresholds.usability_evaluator_count, "actual": evaluator_count},
        ),
        _minimum_gate(
            "usability.successful_evaluators",
            float(succeeded_count),
            float(thresholds.usability_successful_evaluators),
        ),
        _boolean_gate(
            "usability.target_seconds",
            target_seconds == thresholds.usability_target_seconds,
            {"required": thresholds.usability_target_seconds, "actual": target_seconds},
        ),
    ]
    expected_per_segment = thresholds.usability_evaluator_count // len(USABILITY_SEGMENTS)
    for segment in USABILITY_SEGMENTS:
        gates.append(
            _boolean_gate(
                f"usability.segment.{segment}",
                segment_counts[segment] == expected_per_segment,
                {"required": expected_per_segment, "actual": segment_counts[segment]},
            )
        )
    summary = {
        "evidence_source": source,
        "dataset_sha256": dataset_digest,
        "target_seconds": target_seconds,
        "evaluator_count": evaluator_count,
        "succeeded_evaluator_count": succeeded_count,
        "success_rate": success_rate,
        "segment_counts": segment_counts,
    }
    return summary, gates, {revision}


def build_release_approval_gates(
    report: Mapping[str, object],
    *,
    benchmark_report: Mapping[str, object],
    usability_report: Mapping[str, object],
) -> tuple[dict[str, object], list[dict[str, object]], set[str]]:
    location = "release-approval"
    revision = _validate_production_provenance(report, location)
    if len(revision) != 40:
        raise ReleaseEvidenceError(
            "release-approval: code_revision must be a full 40-character Git SHA"
        )
    source = _require_text(report.get("evidence_source"), "evidence_source", location)
    if source != APPROVAL_EVIDENCE_SOURCE:
        raise ReleaseEvidenceError(
            f"release-approval: evidence_source must be {APPROVAL_EVIDENCE_SOURCE!r}"
        )
    approved_revision = _validate_revision(
        report.get("approved_revision"), "approved_revision", location
    )
    if len(approved_revision) != 40:
        raise ReleaseEvidenceError(
            "release-approval: approved_revision must be a full 40-character Git SHA"
        )
    benchmark_evidence = _require_mapping(benchmark_report.get("evidence"), "benchmark.evidence")
    references = {
        "usability_dataset_sha256": usability_report.get("dataset_sha256"),
        "same_story_dataset_sha256": benchmark_evidence.get("same_story_dataset_sha256"),
        "relevance_dataset_sha256": benchmark_evidence.get("relevance_dataset_sha256"),
    }
    for field, expected_value in references.items():
        actual = _sha256_digest(report.get(field), field, location)
        expected = _sha256_digest(expected_value, field, "referenced evidence")
        if actual != expected:
            raise ReleaseEvidenceError(f"release-approval: {field} does not match referenced evidence")
    raw_approvals = report.get("approvals")
    if not isinstance(raw_approvals, list):
        raise ReleaseEvidenceError("release-approval: approvals must be an array")
    decisions: dict[str, bool] = {}
    collected_at = _require_timestamp(report.get("collected_at"), "collected_at", location)
    for index, value in enumerate(raw_approvals):
        item_location = f"release-approval.approvals[{index}]"
        approval = _require_mapping(value, item_location)
        role = _require_text(approval.get("role"), "role", item_location)
        if role not in APPROVAL_ROLES:
            raise ReleaseEvidenceError(f"{item_location}: unsupported role {role!r}")
        if role in decisions:
            raise ReleaseEvidenceError(f"{item_location}: duplicate role {role!r}")
        decision = _require_text(approval.get("decision"), "decision", item_location)
        if decision not in {"approved", "rejected"}:
            raise ReleaseEvidenceError(f"{item_location}: decision must be approved or rejected")
        decided_at = _require_timestamp(approval.get("decided_at"), "decided_at", item_location)
        if decided_at > collected_at:
            raise ReleaseEvidenceError(f"{item_location}: decided_at is later than collected_at")
        _require_text(approval.get("approver_reference"), "approver_reference", item_location)
        _require_text(approval.get("evidence_uri"), "evidence_uri", item_location)
        _sha256_digest(approval.get("evidence_sha256"), "evidence_sha256", item_location)
        decisions[role] = decision == "approved"
    release_approved = _require_bool(report.get("release_approved"), "release_approved", location)
    all_roles_present = set(decisions) == set(APPROVAL_ROLES)
    all_roles_approved = all_roles_present and all(decisions.values())
    if release_approved != all_roles_approved:
        raise ReleaseEvidenceError(
            "release-approval: release_approved does not match required role decisions"
        )
    gates = [
        _boolean_gate("release_approval.release_approved", release_approved),
        _boolean_gate("release_approval.all_roles_present", all_roles_present, sorted(decisions)),
    ]
    for role in APPROVAL_ROLES:
        gates.append(_boolean_gate(f"release_approval.role.{role}", decisions.get(role) is True))
    summary = {
        "evidence_source": source,
        "approved_revision": approved_revision,
        "release_approved": release_approved,
        "decisions": {role: decisions.get(role) for role in APPROVAL_ROLES},
    }
    return summary, gates, {revision, approved_revision}


def build_release_gate_report(
    shadow_records: Sequence[Mapping[str, object]],
    operations_records: Sequence[Mapping[str, object]],
    performance_records: Sequence[Mapping[str, object]],
    benchmark_report: Mapping[str, object],
    usability_report: Mapping[str, object],
    approval_report: Mapping[str, object],
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
    usability, usability_gates, usability_revisions = build_usability_gates(
        usability_report, gate_thresholds
    )
    approval, approval_gates, approval_revisions = build_release_approval_gates(
        approval_report,
        benchmark_report=benchmark_report,
        usability_report=usability_report,
    )
    revisions = (
        shadow_revisions
        | operations_revisions
        | performance_revisions
        | benchmark_revisions
        | usability_revisions
        | approval_revisions
    )
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
        timestamped_records.append(("usability", usability_report))
        timestamped_records.append(("release-approval", approval_report))
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
        + usability_gates
        + approval_gates
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
        "usability": usability,
        "release_approval": approval,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate the 14-day shadow and 7-day production transition gates from exported evidence."
    )
    parser.add_argument("--shadow", type=Path, required=True, help="14+ daily shadow comparison JSONL")
    parser.add_argument("--operations", type=Path, required=True, help="7+ daily run metric JSONL")
    parser.add_argument("--performance", type=Path, required=True, help="7+ daily performance JSONL")
    parser.add_argument("--benchmark", type=Path, required=True, help="quality_benchmark JSON report")
    parser.add_argument("--usability", type=Path, required=True, help="15-person usability evidence JSON")
    parser.add_argument("--approval", type=Path, required=True, help="legal/editorial/product approval JSON")
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
            load_json(args.usability),
            load_json(args.approval),
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
