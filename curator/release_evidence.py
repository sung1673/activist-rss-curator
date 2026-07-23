from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo

from .release_gate import (
    GateThresholds,
    ReleaseEvidenceError,
    build_benchmark_gates,
    build_operations_gates,
    build_performance_gates,
    build_shadow_comparison,
)


SCHEMA_VERSION = 1
KST = ZoneInfo("Asia/Seoul")
REVISION_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^(?:sha256:)?([0-9a-f]{64})$")
NON_PRODUCTION_MARKERS = ("fixture", "synthetic", "sample", "test")
JSONL_FILES = ("shadow.jsonl", "operations.jsonl", "performance.jsonl")
JSON_FILES = ("benchmark.json", "usability.json", "release-approval.json")
EVIDENCE_FILES = JSONL_FILES + JSON_FILES
MAX_INPUT_BYTES = 32 * 1024 * 1024
USABILITY_SEGMENTS = ("institution", "high_net_worth", "international_institution")
APPROVAL_ROLES = ("legal", "editorial", "product")


class EvidenceExportError(ValueError):
    """Raised when source evidence cannot form an immutable production bundle."""


@dataclass(frozen=True)
class SourceArtifact:
    workflow_run_id: int
    artifact_id: int
    digest: str

    def validated(self) -> "SourceArtifact":
        if self.workflow_run_id < 1:
            raise EvidenceExportError("source workflow run id must be positive")
        if self.artifact_id < 1:
            raise EvidenceExportError("source artifact id must be positive")
        digest = _sha256_text(self.digest, "source artifact digest")
        return SourceArtifact(self.workflow_run_id, self.artifact_id, f"sha256:{digest}")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_text(value: object, location: str) -> str:
    text = str(value or "").strip().casefold()
    match = SHA256_RE.fullmatch(text)
    if not match:
        raise EvidenceExportError(f"{location} must be a SHA-256 digest")
    return match.group(1)


def _require_mapping(value: object, location: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise EvidenceExportError(f"{location}: expected an object")
    return dict(value)


def _require_list(value: object, location: str) -> list[object]:
    if not isinstance(value, list):
        raise EvidenceExportError(f"{location}: expected an array")
    return list(value)


def _require_text(value: object, field: str, location: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise EvidenceExportError(f"{location}: {field} must be a non-empty string")
    return text


def _require_bool(value: object, field: str, location: str) -> bool:
    if not isinstance(value, bool):
        raise EvidenceExportError(f"{location}: {field} must be a boolean")
    return value


def _require_nonnegative_int(value: object, field: str, location: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise EvidenceExportError(f"{location}: {field} must be a non-negative integer")
    return value


def _require_positive_int(value: object, field: str, location: str) -> int:
    result = _require_nonnegative_int(value, field, location)
    if result < 1:
        raise EvidenceExportError(f"{location}: {field} must be non-zero")
    return result


def _require_nonnegative_float(value: object, field: str, location: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise EvidenceExportError(f"{location}: {field} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result < 0:
        raise EvidenceExportError(f"{location}: {field} must be finite and non-negative")
    return result


def _require_rate(value: object, field: str, location: str) -> float:
    result = _require_nonnegative_float(value, field, location)
    if result > 1:
        raise EvidenceExportError(f"{location}: {field} must be between 0 and 1")
    return result


def _require_timestamp(value: object, field: str, location: str) -> datetime:
    text = _require_text(value, field, location)
    try:
        result = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EvidenceExportError(f"{location}: {field} must be ISO-8601") from exc
    if result.tzinfo is None:
        raise EvidenceExportError(f"{location}: {field} must include a timezone")
    return result


def _validate_revision(value: object, field: str, location: str, expected_revision: str) -> str:
    revision = _require_text(value, field, location).casefold()
    if not REVISION_RE.fullmatch(revision):
        raise EvidenceExportError(f"{location}: {field} must be a full 40-character Git SHA")
    if revision != expected_revision:
        raise EvidenceExportError(
            f"{location}: {field} {revision} does not match expected revision {expected_revision}"
        )
    return revision


def _validate_provenance(
    record: Mapping[str, object], location: str, expected_revision: str
) -> datetime:
    if record.get("schema_version") != SCHEMA_VERSION:
        raise EvidenceExportError(f"{location}: schema_version must be {SCHEMA_VERSION}")
    if record.get("environment") != "production":
        raise EvidenceExportError(f"{location}: environment must be production")
    if _require_bool(record.get("is_synthetic"), "is_synthetic", location):
        raise EvidenceExportError(f"{location}: synthetic evidence is forbidden")
    source = _require_text(record.get("evidence_source"), "evidence_source", location).casefold()
    if any(marker in source for marker in NON_PRODUCTION_MARKERS):
        raise EvidenceExportError(f"{location}: evidence_source {source!r} is not production evidence")
    _validate_revision(record.get("code_revision"), "code_revision", location, expected_revision)
    return _require_timestamp(record.get("collected_at"), "collected_at", location)


def _read_bytes(path: Path) -> bytes:
    if path.is_symlink():
        raise EvidenceExportError(f"symbolic-link evidence input is forbidden: {path.name}")
    if not path.is_file():
        raise EvidenceExportError(f"missing evidence input: {path.name}")
    size = path.stat().st_size
    if size < 1:
        raise EvidenceExportError(f"empty evidence input: {path.name}")
    if size > MAX_INPUT_BYTES:
        raise EvidenceExportError(f"evidence input exceeds {MAX_INPUT_BYTES} bytes: {path.name}")
    return path.read_bytes()


def _read_json(path: Path) -> tuple[dict[str, object], bytes]:
    raw = _read_bytes(path)
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceExportError(f"{path.name}: invalid UTF-8 JSON") from exc
    return _require_mapping(value, path.name), raw


def _read_jsonl(path: Path) -> tuple[list[dict[str, object]], bytes]:
    raw = _read_bytes(path)
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise EvidenceExportError(f"{path.name}: invalid UTF-8") from exc
    records: list[dict[str, object]] = []
    for line_number, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise EvidenceExportError(f"{path.name}:{line_number}: invalid JSON") from exc
        records.append(_require_mapping(value, f"{path.name}:{line_number}"))
    if not records:
        raise EvidenceExportError(f"{path.name}: no evidence records")
    return records, raw


def _expected_dates(through_date: date, count: int) -> list[date]:
    return [through_date - timedelta(days=offset) for offset in range(count - 1, -1, -1)]


def _select_daily_window(
    records: Sequence[Mapping[str, object]],
    *,
    filename: str,
    expected_revision: str,
    through_date: date,
    day_count: int,
) -> list[dict[str, object]]:
    by_day: dict[date, dict[str, object]] = {}
    for index, raw_record in enumerate(records, start=1):
        record = dict(raw_record)
        location = f"{filename}:{index}"
        collected_at = _validate_provenance(record, location, expected_revision)
        day_text = _require_text(record.get("date"), "date", location)
        try:
            day = date.fromisoformat(day_text)
        except ValueError as exc:
            raise EvidenceExportError(f"{location}: date must be YYYY-MM-DD") from exc
        if collected_at.date() != day:
            raise EvidenceExportError(f"{location}: collected_at date must equal {day.isoformat()}")
        if day in by_day:
            raise EvidenceExportError(f"{location}: duplicate date {day.isoformat()}")
        by_day[day] = record
    required = _expected_dates(through_date, day_count)
    missing = [day.isoformat() for day in required if day not in by_day]
    if missing:
        raise EvidenceExportError(f"{filename}: missing required dates: {', '.join(missing)}")
    return [by_day[day] for day in required]


def _validate_performance_denominators(records: Sequence[Mapping[str, object]]) -> None:
    for record in records:
        day = str(record.get("date") or "unknown")
        location = f"performance[{day}]"
        metrics = _require_mapping(record.get("metrics"), f"{location}.metrics")
        raw_counts = _require_mapping(metrics.get("raw_counts"), f"{location}.metrics.raw_counts")
        attempted = _require_positive_int(
            raw_counts.get("availability_attempted_count"),
            "availability_attempted_count",
            location,
        )
        succeeded = _require_nonnegative_int(
            raw_counts.get("availability_succeeded_count"),
            "availability_succeeded_count",
            location,
        )
        if succeeded > attempted:
            raise EvidenceExportError(f"{location}: availability successes exceed attempts")
        availability_rate = _require_rate(metrics.get("availability_rate"), "availability_rate", location)
        if not math.isclose(availability_rate, succeeded / attempted, rel_tol=0, abs_tol=0.000001):
            raise EvidenceExportError(f"{location}: availability_rate does not match raw counts")
        for field in (
            "mobile_lcp_sample_count",
            "mobile_inp_sample_count",
            "mobile_cls_sample_count",
        ):
            sample_count = _require_positive_int(raw_counts.get(field), field, location)
            if sample_count < 20:
                raise EvidenceExportError(
                    f"{location}: {field} must contain at least 20 real route measurements"
                )


def _validate_benchmark(report: Mapping[str, object], expected_revision: str) -> None:
    evidence = _require_mapping(report.get("evidence"), "benchmark.evidence")
    _validate_provenance(evidence, "benchmark.evidence", expected_revision)
    try:
        build_benchmark_gates(report, GateThresholds())
    except ReleaseEvidenceError as exc:
        raise EvidenceExportError(str(exc)) from exc


def _validate_usability(report: Mapping[str, object], expected_revision: str) -> None:
    location = "usability"
    _validate_provenance(report, location, expected_revision)
    reported_dataset_digest = _sha256_text(
        report.get("dataset_sha256"), f"{location}.dataset_sha256"
    )
    target_seconds = _require_positive_int(report.get("target_seconds"), "target_seconds", location)
    evaluator_count = _require_positive_int(report.get("evaluator_count"), "evaluator_count", location)
    if evaluator_count < 15:
        raise EvidenceExportError("usability: evaluator_count must include at least 15 real evaluators")
    succeeded_count = _require_nonnegative_int(
        report.get("succeeded_evaluator_count"), "succeeded_evaluator_count", location
    )
    if succeeded_count > evaluator_count:
        raise EvidenceExportError("usability: succeeded_evaluator_count exceeds evaluator_count")
    success_rate = _require_rate(report.get("success_rate"), "success_rate", location)
    if not math.isclose(success_rate, succeeded_count / evaluator_count, rel_tol=0, abs_tol=0.000001):
        raise EvidenceExportError("usability: success_rate does not match raw counts")
    evaluations = _require_list(report.get("evaluations"), "usability.evaluations")
    if len(evaluations) != evaluator_count:
        raise EvidenceExportError("usability: evaluator_count does not match evaluations")
    identifiers: set[str] = set()
    segment_counts = {segment: 0 for segment in USABILITY_SEGMENTS}
    calculated_success = 0
    for index, value in enumerate(evaluations):
        item_location = f"usability.evaluations[{index}]"
        evaluation = _require_mapping(value, item_location)
        identifier = _require_text(evaluation.get("evaluation_id"), "evaluation_id", item_location)
        if identifier in identifiers:
            raise EvidenceExportError(f"{item_location}: duplicate evaluation_id")
        identifiers.add(identifier)
        segment = _require_text(evaluation.get("segment"), "segment", item_location)
        if segment not in segment_counts:
            raise EvidenceExportError(f"{item_location}: unsupported segment {segment!r}")
        segment_counts[segment] += 1
        _require_timestamp(evaluation.get("completed_at"), "completed_at", item_location)
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
            raise EvidenceExportError(f"{item_location}: succeeded does not match duration and checks")
        calculated_success += int(succeeded)
    if calculated_success != succeeded_count:
        raise EvidenceExportError("usability: succeeded_evaluator_count does not match evaluations")
    if any(segment_counts[segment] < 5 for segment in USABILITY_SEGMENTS):
        raise EvidenceExportError("usability: each evaluator segment requires at least five evaluations")
    canonical_dataset = "\n".join(
        json.dumps(
            _require_mapping(value, f"usability.evaluations[{index}]"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        for index, value in enumerate(evaluations)
    ).encode("utf-8")
    if _sha256_bytes(canonical_dataset) != reported_dataset_digest:
        raise EvidenceExportError("usability: dataset_sha256 does not match evaluations")


def _validate_release_approval(
    report: Mapping[str, object],
    *,
    expected_revision: str,
    benchmark: Mapping[str, object],
    usability: Mapping[str, object],
) -> None:
    location = "release-approval"
    _validate_provenance(report, location, expected_revision)
    _validate_revision(report.get("approved_revision"), "approved_revision", location, expected_revision)
    benchmark_evidence = _require_mapping(benchmark.get("evidence"), "benchmark.evidence")
    digest_links = {
        "usability_dataset_sha256": usability.get("dataset_sha256"),
        "same_story_dataset_sha256": benchmark_evidence.get("same_story_dataset_sha256"),
        "relevance_dataset_sha256": benchmark_evidence.get("relevance_dataset_sha256"),
    }
    for field, expected in digest_links.items():
        actual_digest = _sha256_text(report.get(field), f"{location}.{field}")
        expected_digest = _sha256_text(expected, f"referenced {field}")
        if actual_digest != expected_digest:
            raise EvidenceExportError(f"{location}: {field} does not match referenced evidence")
    approvals = _require_list(report.get("approvals"), "release-approval.approvals")
    by_role: dict[str, bool] = {}
    for index, value in enumerate(approvals):
        item_location = f"release-approval.approvals[{index}]"
        approval = _require_mapping(value, item_location)
        role = _require_text(approval.get("role"), "role", item_location)
        if role not in APPROVAL_ROLES:
            raise EvidenceExportError(f"{item_location}: unsupported role {role!r}")
        if role in by_role:
            raise EvidenceExportError(f"{item_location}: duplicate role {role!r}")
        decision = _require_text(approval.get("decision"), "decision", item_location)
        if decision not in {"approved", "rejected"}:
            raise EvidenceExportError(f"{item_location}: decision must be approved or rejected")
        _require_timestamp(approval.get("decided_at"), "decided_at", item_location)
        _require_text(approval.get("approver_reference"), "approver_reference", item_location)
        _require_text(approval.get("evidence_uri"), "evidence_uri", item_location)
        _sha256_text(approval.get("evidence_sha256"), f"{item_location}.evidence_sha256")
        by_role[role] = decision == "approved"
    missing_roles = sorted(set(APPROVAL_ROLES) - set(by_role))
    if missing_roles:
        raise EvidenceExportError(f"release-approval: missing roles: {', '.join(missing_roles)}")
    release_approved = _require_bool(report.get("release_approved"), "release_approved", location)
    if release_approved != all(by_role.values()):
        raise EvidenceExportError("release-approval: release_approved does not match role decisions")


def _canonical_json(value: Mapping[str, object]) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _canonical_jsonl(records: Sequence[Mapping[str, object]]) -> bytes:
    return (
        "".join(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n" for record in records)
    ).encode("utf-8")


def export_release_evidence(
    *,
    source_dir: Path,
    output_dir: Path,
    expected_revision: str,
    through_date: date,
    source_artifact: SourceArtifact,
    exported_at: datetime | None = None,
) -> dict[str, object]:
    expected_revision = expected_revision.strip().casefold()
    if not REVISION_RE.fullmatch(expected_revision):
        raise EvidenceExportError("expected_revision must be a full 40-character Git SHA")
    artifact = source_artifact.validated()
    source = source_dir.resolve()
    output = output_dir.resolve()
    if not source.is_dir() or source_dir.is_symlink():
        raise EvidenceExportError("source_dir must be a real directory")
    if output.exists():
        raise EvidenceExportError("output_dir already exists; release evidence is immutable")
    if output == source or output.is_relative_to(source) or source.is_relative_to(output):
        raise EvidenceExportError("source_dir and output_dir must be disjoint")

    shadow_all, shadow_raw = _read_jsonl(source / "shadow.jsonl")
    operations_all, operations_raw = _read_jsonl(source / "operations.jsonl")
    performance_all, performance_raw = _read_jsonl(source / "performance.jsonl")
    benchmark, benchmark_raw = _read_json(source / "benchmark.json")
    usability, usability_raw = _read_json(source / "usability.json")
    approval, approval_raw = _read_json(source / "release-approval.json")

    shadow = _select_daily_window(
        shadow_all,
        filename="shadow.jsonl",
        expected_revision=expected_revision,
        through_date=through_date,
        day_count=14,
    )
    operations = _select_daily_window(
        operations_all,
        filename="operations.jsonl",
        expected_revision=expected_revision,
        through_date=through_date,
        day_count=7,
    )
    performance = _select_daily_window(
        performance_all,
        filename="performance.jsonl",
        expected_revision=expected_revision,
        through_date=through_date,
        day_count=7,
    )
    try:
        build_shadow_comparison(shadow, GateThresholds())
        build_operations_gates(operations, GateThresholds())
        build_performance_gates(performance, GateThresholds())
    except ReleaseEvidenceError as exc:
        raise EvidenceExportError(str(exc)) from exc
    _validate_performance_denominators(performance)
    _validate_benchmark(benchmark, expected_revision)
    _validate_usability(usability, expected_revision)
    _validate_release_approval(
        approval,
        expected_revision=expected_revision,
        benchmark=benchmark,
        usability=usability,
    )

    rendered = {
        "shadow.jsonl": _canonical_jsonl(shadow),
        "operations.jsonl": _canonical_jsonl(operations),
        "performance.jsonl": _canonical_jsonl(performance),
        "benchmark.json": _canonical_json(benchmark),
        "usability.json": _canonical_json(usability),
        "release-approval.json": _canonical_json(approval),
    }
    input_raw = {
        "shadow.jsonl": shadow_raw,
        "operations.jsonl": operations_raw,
        "performance.jsonl": performance_raw,
        "benchmark.json": benchmark_raw,
        "usability.json": usability_raw,
        "release-approval.json": approval_raw,
    }
    timestamp = (exported_at or datetime.now(timezone.utc)).astimezone(timezone.utc)
    manifest: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "environment": "production",
        "is_synthetic": False,
        "evidence_source": "protected_github_artifact",
        "exported_at": timestamp.isoformat().replace("+00:00", "Z"),
        "through_date": through_date.isoformat(),
        "code_revision": expected_revision,
        "source_artifact": {
            "workflow_run_id": artifact.workflow_run_id,
            "artifact_id": artifact.artifact_id,
            "digest": artifact.digest,
        },
        "files": {
            name: {
                "source_sha256": _sha256_bytes(input_raw[name]),
                "output_sha256": _sha256_bytes(rendered[name]),
                "bytes": len(rendered[name]),
            }
            for name in EVIDENCE_FILES
        },
    }
    rendered["bundle-manifest.json"] = _canonical_json(manifest)

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=f".{output.name}-", dir=output.parent))
    try:
        for filename, contents in rendered.items():
            (temporary / filename).write_bytes(contents)
        temporary.replace(output)
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise
    return manifest


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Assemble immutable same-revision production release evidence from a protected source artifact."
    )
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--expected-revision", default=os.environ.get("GITHUB_SHA", ""), required=False)
    parser.add_argument(
        "--through-date",
        default=os.environ.get("EVIDENCE_THROUGH_DATE", ""),
        help="last KST evidence date (default: the previous KST calendar day)",
    )
    parser.add_argument("--source-run-id", type=int, required=True)
    parser.add_argument("--source-artifact-id", type=int, required=True)
    parser.add_argument("--source-artifact-digest", required=True)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    try:
        through_date = (
            date.fromisoformat(str(args.through_date).strip())
            if str(args.through_date).strip()
            else datetime.now(KST).date() - timedelta(days=1)
        )
        manifest = export_release_evidence(
            source_dir=args.source_dir,
            output_dir=args.output_dir,
            expected_revision=args.expected_revision,
            through_date=through_date,
            source_artifact=SourceArtifact(
                workflow_run_id=args.source_run_id,
                artifact_id=args.source_artifact_id,
                digest=args.source_artifact_digest,
            ),
        )
    except (EvidenceExportError, OSError, ValueError) as exc:
        print(
            json.dumps({"status": "invalid-release-evidence-source", "error": str(exc)}, ensure_ascii=False),
            file=sys.stderr,
        )
        raise SystemExit(2) from exc
    print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
