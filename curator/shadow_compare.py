from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import stat
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Mapping, Sequence
from urllib.parse import urlsplit, urlunsplit

import httpx

from .shadow_engine import (
    COMPARISON_KEY_RE,
    DAY_DELTA_SCOPE,
    REVISION_RE,
    ShadowEngineError,
    canonical_json_bytes,
    comparison_keys_sha256,
    validate_engine_snapshot,
)


ARTIFACT_DIGEST_RE = re.compile(r"^sha256:([0-9a-f]{64})$")
CORPUS_SCOPE = "same_sha_cumulative_kst_day_end_v1"
VALID_REVIEW_STATUSES = {"pending", "reviewed", "resolved", "dismissed"}
CROSSWALK_FIELDS = {
    "schema_version",
    "eligible_legacy_record_count",
    "crosswalked_legacy_record_count",
    "unmatched_legacy_record_count",
    "ambiguous_legacy_record_count",
    "coverage_rate",
    "crosswalk_sha256",
}


class ShadowComparisonError(RuntimeError):
    """Fail-closed shadow orchestration error."""


def normalize_api_base(value: str) -> str:
    raw = value.strip().rstrip("/")
    parsed = urlsplit(raw)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ShadowComparisonError("GOVERNANCE_API_BASE_URL is invalid")
    if parsed.scheme != "https" and parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        raise ShadowComparisonError("GOVERNANCE_API_BASE_URL must use HTTPS")
    path = parsed.path.rstrip("/")
    if not path.endswith("/api/v1"):
        path += "/api/v1"
    return urlunsplit((parsed.scheme, parsed.netloc, path, "", ""))


def _manifest(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ShadowComparisonError("artifact manifest is missing or invalid JSON") from exc
    if not isinstance(payload, dict):
        raise ShadowComparisonError("artifact manifest must be an object")
    if set(payload) != {
        "schema_version",
        "observation_date",
        "code_revision",
        "corpus_scope",
        "artifacts",
        "previous_comparison",
    }:
        raise ShadowComparisonError("artifact manifest fields are invalid")
    if payload.get("schema_version") != 2:
        raise ShadowComparisonError("artifact manifest schema_version must be 2")
    if payload.get("corpus_scope") != CORPUS_SCOPE:
        raise ShadowComparisonError("artifact manifest corpus_scope is invalid")
    try:
        observed = date.fromisoformat(str(payload.get("observation_date") or ""))
    except ValueError as exc:
        raise ShadowComparisonError("artifact manifest observation_date is invalid") from exc
    if observed.isoformat() != payload.get("observation_date"):
        raise ShadowComparisonError("artifact manifest observation_date is invalid")
    revision = str(payload.get("code_revision") or "").strip().casefold()
    if not REVISION_RE.fullmatch(revision):
        raise ShadowComparisonError("artifact manifest code_revision must be a full Git SHA")
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, list) or not artifacts:
        raise ShadowComparisonError("artifact manifest must contain engine artifacts")
    seen_ids: set[int] = set()
    engines: set[str] = set()
    for index, item in enumerate(artifacts):
        location = f"artifacts[{index}]"
        if not isinstance(item, dict) or set(item) != {
            "engine",
            "artifact_id",
            "artifact_name",
            "artifact_digest",
            "workflow_run_id",
            "workflow_path",
            "head_sha",
        }:
            raise ShadowComparisonError(f"{location} fields are invalid")
        engine = str(item.get("engine") or "").strip().casefold()
        if engine not in {"legacy", "candidate"}:
            raise ShadowComparisonError(f"{location}.engine is invalid")
        engines.add(engine)
        artifact_id = item.get("artifact_id")
        workflow_run_id = item.get("workflow_run_id")
        if (
            not isinstance(artifact_id, int)
            or isinstance(artifact_id, bool)
            or artifact_id < 1
            or artifact_id in seen_ids
            or not isinstance(workflow_run_id, int)
            or isinstance(workflow_run_id, bool)
            or workflow_run_id < 1
        ):
            raise ShadowComparisonError(f"{location} identifiers are invalid")
        seen_ids.add(artifact_id)
        expected_prefix = f"shadow-engine-{engine}-{observed.isoformat()}-"
        artifact_name = str(item.get("artifact_name") or "")
        artifact_suffix = artifact_name.removeprefix(expected_prefix)
        artifact_run = re.fullmatch(r"([1-9][0-9]*)-([1-9][0-9]*)", artifact_suffix)
        if (
            not artifact_name.startswith(expected_prefix)
            or artifact_run is None
            or int(artifact_run.group(1)) != workflow_run_id
        ):
            raise ShadowComparisonError(f"{location}.artifact_name is invalid")
        digest = str(item.get("artifact_digest") or "").strip().casefold()
        if not ARTIFACT_DIGEST_RE.fullmatch(digest):
            raise ShadowComparisonError(f"{location}.artifact_digest is invalid")
        expected_workflow = (
            ".github/workflows/build-feed.yml"
            if engine == "legacy"
            else ".github/workflows/ingest-official.yml"
        )
        if item.get("workflow_path") != expected_workflow:
            raise ShadowComparisonError(f"{location}.workflow_path is invalid")
        if str(item.get("head_sha") or "").casefold() != revision:
            raise ShadowComparisonError("mixed-SHA shadow artifacts are forbidden")
    if engines != {"legacy", "candidate"}:
        raise ShadowComparisonError("both legacy and candidate artifacts are required")
    previous = payload.get("previous_comparison")
    if previous is not None:
        if not isinstance(previous, dict) or set(previous) != {
            "artifact_id",
            "artifact_name",
            "artifact_digest",
            "workflow_run_id",
            "workflow_path",
            "head_sha",
            "observation_date",
        }:
            raise ShadowComparisonError("previous_comparison fields are invalid")
        previous_day = observed - timedelta(days=1)
        if previous.get("observation_date") != previous_day.isoformat():
            raise ShadowComparisonError(
                "previous_comparison must be the immediately preceding KST day"
            )
        artifact_id = previous.get("artifact_id")
        workflow_run_id = previous.get("workflow_run_id")
        if (
            not isinstance(artifact_id, int)
            or isinstance(artifact_id, bool)
            or artifact_id < 1
            or artifact_id in seen_ids
            or not isinstance(workflow_run_id, int)
            or isinstance(workflow_run_id, bool)
            or workflow_run_id < 1
        ):
            raise ShadowComparisonError("previous_comparison identifiers are invalid")
        expected_name = (
            f"governance-shadow-comparison-{previous_day.isoformat()}-{revision}"
        )
        if previous.get("artifact_name") != expected_name:
            raise ShadowComparisonError("previous_comparison artifact_name is invalid")
        digest = str(previous.get("artifact_digest") or "").strip().casefold()
        if not ARTIFACT_DIGEST_RE.fullmatch(digest):
            raise ShadowComparisonError("previous_comparison artifact_digest is invalid")
        if previous.get("workflow_path") != ".github/workflows/shadow-compare.yml":
            raise ShadowComparisonError("previous_comparison workflow_path is invalid")
        if str(previous.get("head_sha") or "").casefold() != revision:
            raise ShadowComparisonError("mixed-SHA previous comparison is forbidden")
    return payload


def extract_artifacts(manifest_path: Path, archives: Path, destination: Path) -> None:
    payload = _manifest(manifest_path)
    artifacts = payload["artifacts"]
    assert isinstance(artifacts, list)
    archive_items: list[tuple[dict[str, object], str, int]] = [
        (item, "engine-output.json", 5_000_000)
        for item in artifacts
        if isinstance(item, dict)
    ]
    previous = payload.get("previous_comparison")
    if isinstance(previous, dict):
        archive_items.append((previous, "shadow-comparison.json", 20_000_000))
    destination.mkdir(parents=True, exist_ok=True)
    for item, expected_filename, maximum_size in archive_items:
        artifact_id = int(str(item["artifact_id"]))
        archive_path = archives / f"{artifact_id}.zip"
        try:
            archive_bytes = archive_path.read_bytes()
        except OSError as exc:
            raise ShadowComparisonError(f"artifact archive {artifact_id} is missing") from exc
        expected = ARTIFACT_DIGEST_RE.fullmatch(str(item["artifact_digest"]).casefold())
        assert expected is not None
        if not hmac.compare_digest(hashlib.sha256(archive_bytes).hexdigest(), expected.group(1)):
            raise ShadowComparisonError(f"artifact archive {artifact_id} digest mismatch")
        try:
            with zipfile.ZipFile(archive_path) as archive:
                regular: list[zipfile.ZipInfo] = []
                total_size = 0
                for member in archive.infolist():
                    member_path = PurePosixPath(member.filename)
                    if (
                        member_path.is_absolute()
                        or ".." in member_path.parts
                        or "" in member_path.parts
                    ):
                        raise ShadowComparisonError(
                            f"artifact archive {artifact_id} contains an unsafe path"
                        )
                    mode = member.external_attr >> 16
                    if stat.S_ISLNK(mode):
                        raise ShadowComparisonError(
                            f"artifact archive {artifact_id} contains a symbolic link"
                        )
                    if member.is_dir():
                        continue
                    regular.append(member)
                    total_size += member.file_size
                if (
                    len(regular) != 1
                    or PurePosixPath(regular[0].filename).name != expected_filename
                    or total_size > maximum_size
                ):
                    raise ShadowComparisonError(
                        f"artifact archive {artifact_id} must contain only {expected_filename}"
                    )
                output_dir = destination / str(artifact_id)
                output_dir.mkdir(parents=True, exist_ok=True)
                output = output_dir / expected_filename
                output.write_bytes(archive.read(regular[0]))
        except (OSError, zipfile.BadZipFile) as exc:
            raise ShadowComparisonError(f"artifact archive {artifact_id} is invalid") from exc


def _source_evidence(value: object, location: str) -> set[tuple[str, str]]:
    if not isinstance(value, list):
        raise ShadowComparisonError(f"{location} is invalid")
    result: set[tuple[str, str]] = set()
    for index, item in enumerate(value):
        if not isinstance(item, dict) or set(item) != {"kind", "value"}:
            raise ShadowComparisonError(f"{location}[{index}] is invalid")
        kind = str(item.get("kind") or "")
        evidence_value = str(item.get("value") or "")
        if not kind or not evidence_value or (kind, evidence_value) in result:
            raise ShadowComparisonError(f"{location}[{index}] is invalid or duplicate")
        result.add((kind, evidence_value))
    return result


def _crosswalk_legacy_records(
    legacy_records: Mapping[str, Mapping[str, object]],
    candidate_events: Mapping[str, set[tuple[str, str]]],
) -> tuple[list[str], dict[str, object], list[dict[str, object]]]:
    evidence_index: dict[tuple[str, str], set[str]] = {}
    for comparison_key, evidence in candidate_events.items():
        if not evidence:
            raise ShadowComparisonError(
                f"candidate event {comparison_key!r} has no stable source evidence"
            )
        for token in evidence:
            evidence_index.setdefault(token, set()).add(comparison_key)

    ambiguous_candidate_evidence = sorted(
        token for token, keys in evidence_index.items() if len(keys) != 1
    )
    if ambiguous_candidate_evidence:
        kind, value = ambiguous_candidate_evidence[0]
        raise ShadowComparisonError(
            "candidate source evidence maps to multiple canonical events: "
            f"{kind}:{value}"
        )

    eligible = 0
    crosswalked = 0
    unmatched: list[str] = []
    ambiguous: list[str] = []
    mapped_keys: set[str] = set()
    rows: list[dict[str, object]] = []
    for record_id in sorted(legacy_records):
        record = legacy_records[record_id]
        evidence = _source_evidence(
            record.get("source_evidence"),
            f"legacy record {record_id!r}.source_evidence",
        )
        raw_explicit_key = record.get("comparison_key")
        explicit_key = str(raw_explicit_key) if raw_explicit_key is not None else ""
        matched_by_evidence: set[str] = set()
        matched_tokens: list[tuple[str, str]] = []
        for token in sorted(evidence):
            keys = evidence_index.get(token, set())
            if keys:
                matched_by_evidence.update(keys)
                matched_tokens.append(token)

        has_identifier = any(
            kind in {"document_id", "official_receipt"} for kind, _value in evidence
        )
        is_eligible = bool(explicit_key or has_identifier or matched_by_evidence)
        if not is_eligible:
            # A general news URL absent from the candidate evidence graph is
            # outside this official-event comparison.  It is never title-
            # matched and cannot silently inflate the denominator.
            continue
        eligible += 1

        possible_keys = set(matched_by_evidence)
        if explicit_key:
            possible_keys.add(explicit_key)
        if not possible_keys:
            unmatched.append(record_id)
            continue
        if len(possible_keys) != 1:
            ambiguous.append(record_id)
            continue
        comparison_key = next(iter(possible_keys))
        if not COMPARISON_KEY_RE.fullmatch(comparison_key):
            raise ShadowComparisonError(
                f"legacy record {record_id!r} has an invalid explicit comparison key"
            )
        crosswalked += 1
        mapped_keys.add(comparison_key)
        rows.append(
            {
                "legacy_record_id": record_id,
                "comparison_key": comparison_key,
                "matched_source_evidence": [
                    {"kind": kind, "value": value}
                    for kind, value in matched_tokens
                ],
                "mapping_basis": "explicit_comparison_key"
                if explicit_key
                else "stable_source_evidence",
            }
        )

    if eligible < 1:
        raise ShadowComparisonError(
            "legacy crosswalk eligible denominator must be non-zero; title/theme inference is forbidden"
        )
    if unmatched:
        raise ShadowComparisonError(
            f"{len(unmatched)} eligible legacy records have no canonical crosswalk key"
        )
    if ambiguous:
        raise ShadowComparisonError(
            f"{len(ambiguous)} eligible legacy records map to multiple canonical crosswalk keys"
        )
    if crosswalked != eligible:
        raise ShadowComparisonError("legacy crosswalk coverage is incomplete")

    rows = sorted(rows, key=lambda row: str(row["legacy_record_id"]))
    metrics: dict[str, object] = {
        "schema_version": 1,
        "eligible_legacy_record_count": eligible,
        "crosswalked_legacy_record_count": crosswalked,
        "unmatched_legacy_record_count": len(unmatched),
        "ambiguous_legacy_record_count": len(ambiguous),
        "coverage_rate": 1.0,
        "crosswalk_sha256": hashlib.sha256(canonical_json_bytes(rows)).hexdigest(),
    }
    return sorted(mapped_keys), metrics, rows


def _corpus_payload(
    candidate_events: Mapping[str, set[tuple[str, str]]],
    legacy_records: Mapping[str, Mapping[str, object]],
) -> dict[str, object]:
    candidate_rows = [
        {
            "comparison_key": comparison_key,
            "source_evidence": [
                {"kind": kind, "value": value}
                for kind, value in sorted(candidate_events[comparison_key])
            ],
        }
        for comparison_key in sorted(candidate_events)
    ]
    legacy_rows = [dict(legacy_records[record_id]) for record_id in sorted(legacy_records)]
    content = {
        "candidate_events": candidate_rows,
        "legacy_records": legacy_rows,
    }
    return {
        "schema_version": 1,
        **content,
        "candidate_events_sha256": hashlib.sha256(
            canonical_json_bytes(candidate_rows)
        ).hexdigest(),
        "legacy_records_sha256": hashlib.sha256(
            canonical_json_bytes(legacy_rows)
        ).hexdigest(),
        "corpus_payload_sha256": hashlib.sha256(
            canonical_json_bytes(content)
        ).hexdigest(),
    }


def _validated_corpus_payload(
    value: object,
    location: str,
) -> tuple[dict[str, set[tuple[str, str]]], dict[str, dict[str, object]], dict[str, object]]:
    fields = {
        "schema_version",
        "candidate_events",
        "candidate_events_sha256",
        "legacy_records",
        "legacy_records_sha256",
        "corpus_payload_sha256",
    }
    if not isinstance(value, dict) or set(value) != fields or value.get("schema_version") != 1:
        raise ShadowComparisonError(f"{location} fields are invalid")
    candidate_rows = value.get("candidate_events")
    legacy_rows = value.get("legacy_records")
    if (
        not isinstance(candidate_rows, list)
        or len(candidate_rows) > 50_000
        or not isinstance(legacy_rows, list)
        or len(legacy_rows) > 50_000
    ):
        raise ShadowComparisonError(f"{location} corpus arrays are invalid")

    candidate_events: dict[str, set[tuple[str, str]]] = {}
    candidate_keys: list[str] = []
    for index, row in enumerate(candidate_rows):
        row_location = f"{location}.candidate_events[{index}]"
        if not isinstance(row, dict) or set(row) != {"comparison_key", "source_evidence"}:
            raise ShadowComparisonError(f"{row_location} is invalid")
        comparison_key = str(row.get("comparison_key") or "").strip().casefold()
        if not COMPARISON_KEY_RE.fullmatch(comparison_key) or row.get(
            "comparison_key"
        ) != comparison_key:
            raise ShadowComparisonError(f"{row_location}.comparison_key is invalid")
        evidence = _source_evidence(row.get("source_evidence"), f"{row_location}.source_evidence")
        if not evidence:
            raise ShadowComparisonError(f"{row_location} has no stable source evidence")
        candidate_keys.append(comparison_key)
        candidate_events[comparison_key] = evidence
    if candidate_keys != sorted(set(candidate_keys)):
        raise ShadowComparisonError(f"{location}.candidate_events must be sorted and unique")

    legacy_records: dict[str, dict[str, object]] = {}
    legacy_ids: list[str] = []
    for index, row in enumerate(legacy_rows):
        row_location = f"{location}.legacy_records[{index}]"
        if not isinstance(row, dict) or set(row) != {
            "legacy_record_id",
            "comparison_key",
            "source_evidence",
        }:
            raise ShadowComparisonError(f"{row_location} is invalid")
        record_id = str(row.get("legacy_record_id") or "").strip().casefold()
        if (
            not record_id
            or len(record_id) > 191
            or row.get("legacy_record_id") != record_id
            or re.search(r"[\x00-\x1f\x7f]", record_id)
        ):
            raise ShadowComparisonError(f"{row_location}.legacy_record_id is invalid")
        raw_key = row.get("comparison_key")
        if raw_key is not None:
            comparison_key = str(raw_key).strip().casefold()
            if not COMPARISON_KEY_RE.fullmatch(comparison_key) or raw_key != comparison_key:
                raise ShadowComparisonError(f"{row_location}.comparison_key is invalid")
        evidence = _source_evidence(row.get("source_evidence"), f"{row_location}.source_evidence")
        normalized = {
            "legacy_record_id": record_id,
            "comparison_key": raw_key,
            "source_evidence": [
                {"kind": kind, "value": evidence_value}
                for kind, evidence_value in sorted(evidence)
            ],
        }
        legacy_ids.append(record_id)
        legacy_records[record_id] = normalized
    if legacy_ids != sorted(set(legacy_ids)):
        raise ShadowComparisonError(f"{location}.legacy_records must be sorted and unique")

    expected_candidate_hash = hashlib.sha256(canonical_json_bytes(candidate_rows)).hexdigest()
    expected_legacy_hash = hashlib.sha256(canonical_json_bytes(legacy_rows)).hexdigest()
    expected_payload_hash = hashlib.sha256(
        canonical_json_bytes(
            {"candidate_events": candidate_rows, "legacy_records": legacy_rows}
        )
    ).hexdigest()
    for field, expected in (
        ("candidate_events_sha256", expected_candidate_hash),
        ("legacy_records_sha256", expected_legacy_hash),
        ("corpus_payload_sha256", expected_payload_hash),
    ):
        actual = str(value.get(field) or "").casefold()
        if not re.fullmatch(r"[0-9a-f]{64}", actual) or not hmac.compare_digest(
            actual, expected
        ):
            raise ShadowComparisonError(f"{location}.{field} mismatch")
    return candidate_events, legacy_records, dict(value)


def _validated_previous_comparison(
    manifest: Mapping[str, object],
    snapshots_root: Path,
    *,
    observed: date,
    revision: str,
) -> tuple[
    dict[str, set[tuple[str, str]]],
    dict[str, dict[str, object]],
    dict[str, object] | None,
    date,
]:
    previous = manifest.get("previous_comparison")
    if not isinstance(previous, dict):
        return {}, {}, None, observed
    artifact_id = int(previous["artifact_id"])
    receipt_path = snapshots_root / str(artifact_id) / "shadow-comparison.json"
    try:
        report_raw = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise ShadowComparisonError("previous comparison receipt is missing or invalid JSON") from exc
    if not isinstance(report_raw, dict):
        raise ShadowComparisonError("previous comparison receipt must be an object")
    claimed_report_hash = str(report_raw.get("report_sha256") or "").casefold()
    without_hash = dict(report_raw)
    without_hash.pop("report_sha256", None)
    actual_report_hash = hashlib.sha256(canonical_json_bytes(without_hash)).hexdigest()
    if not re.fullmatch(r"[0-9a-f]{64}", claimed_report_hash) or not hmac.compare_digest(
        claimed_report_hash, actual_report_hash
    ):
        raise ShadowComparisonError("previous comparison report_sha256 mismatch")
    previous_day = observed - timedelta(days=1)
    if (
        report_raw.get("schema_version") != 3
        or report_raw.get("observation_date") != previous_day.isoformat()
        or str(report_raw.get("code_revision") or "").casefold() != revision
        or report_raw.get("distribution_mode") != "web_only"
    ):
        raise ShadowComparisonError("previous comparison receipt identity is invalid")

    corpus = report_raw.get("corpus")
    corpus_fields = {
        "schema_version",
        "scope",
        "start_date",
        "end_date",
        "day_count",
        "previous_observation_date",
        "previous_report_sha256",
        "current_source_artifact_count",
        "current_source_artifacts_sha256",
        "corpus_payload_sha256",
    }
    if not isinstance(corpus, dict) or set(corpus) != corpus_fields:
        raise ShadowComparisonError("previous comparison corpus fields are invalid")
    try:
        start_date = date.fromisoformat(str(corpus.get("start_date") or ""))
        end_date = date.fromisoformat(str(corpus.get("end_date") or ""))
    except ValueError as exc:
        raise ShadowComparisonError("previous comparison corpus dates are invalid") from exc
    expected_days = (end_date - start_date).days + 1
    if (
        corpus.get("schema_version") != 1
        or corpus.get("scope") != CORPUS_SCOPE
        or end_date != previous_day
        or start_date > end_date
        or corpus.get("day_count") != expected_days
    ):
        raise ShadowComparisonError("previous comparison corpus continuity is invalid")

    candidate_events, legacy_records, corpus_payload = _validated_corpus_payload(
        report_raw.get("corpus_payload"), "previous comparison corpus_payload"
    )
    if corpus.get("corpus_payload_sha256") != corpus_payload["corpus_payload_sha256"]:
        raise ShadowComparisonError("previous comparison corpus payload commitment mismatch")
    legacy_keys, crosswalk, rows = _crosswalk_legacy_records(
        legacy_records, candidate_events
    )
    candidate_keys = sorted(candidate_events)
    engines = report_raw.get("engines")
    if not isinstance(engines, dict):
        raise ShadowComparisonError("previous comparison engines are invalid")
    for engine, keys in (("legacy", legacy_keys), ("candidate", candidate_keys)):
        engine_row = engines.get(engine)
        if not isinstance(engine_row, dict) or (
            engine_row.get("status") != "succeeded"
            or engine_row.get("event_count") != len(keys)
            or engine_row.get("events_sha256") != comparison_keys_sha256(keys)
            or engine_row.get("comparison_keys") != keys
        ):
            raise ShadowComparisonError(f"previous comparison {engine} corpus mismatch")
    if report_raw.get("legacy_crosswalk") != crosswalk or report_raw.get(
        "legacy_crosswalk_records"
    ) != rows:
        raise ShadowComparisonError("previous comparison legacy crosswalk mismatch")
    previous_summary: dict[str, object] = {
        "observation_date": previous_day.isoformat(),
        "report_sha256": claimed_report_hash,
        "legacy_comparison_keys": legacy_keys,
        "candidate_comparison_keys": candidate_keys,
        "legacy_crosswalk": crosswalk,
    }
    return candidate_events, legacy_records, previous_summary, start_date


def aggregate_engine_outputs(
    manifest_path: Path,
    snapshots_root: Path,
) -> dict[str, object]:
    manifest = _manifest(manifest_path)
    observed = date.fromisoformat(str(manifest["observation_date"]))
    revision = str(manifest["code_revision"])
    artifacts = manifest["artifacts"]
    assert isinstance(artifacts, list)
    (
        candidate_events,
        legacy_records,
        previous_summary,
        corpus_start_date,
    ) = _validated_previous_comparison(
        manifest,
        snapshots_root,
        observed=observed,
        revision=revision,
    )
    provenance: dict[str, list[dict[str, object]]] = {"legacy": [], "candidate": []}
    for item in artifacts:
        assert isinstance(item, dict)
        engine = str(item["engine"])
        artifact_id = int(item["artifact_id"])
        snapshot_path = snapshots_root / str(artifact_id) / "engine-output.json"
        try:
            snapshot_raw = json.loads(snapshot_path.read_text(encoding="utf-8"))
            snapshot = validate_engine_snapshot(
                snapshot_raw,
                expected_engine=engine,
                expected_date=observed,
                expected_revision=revision,
                require_succeeded=True,
            )
            if snapshot.get("record_scope") != DAY_DELTA_SCOPE:
                raise ShadowEngineError(
                    "shadow comparison requires a KST observation-day delta snapshot"
                )
        except (OSError, ValueError, ShadowEngineError) as exc:
            raise ShadowComparisonError(
                f"{engine} artifact {artifact_id} engine output is invalid: {exc}"
            ) from exc
        expected_source_run = re.fullmatch(
            r"github:([1-9][0-9]*):([1-9][0-9]*)",
            str(snapshot.get("producer_run_id") or ""),
        )
        if (
            expected_source_run is None
            or int(expected_source_run.group(1)) != int(item["workflow_run_id"])
        ):
            raise ShadowComparisonError(
                f"{engine} artifact {artifact_id} source-run provenance mismatch"
            )
        events = snapshot.get("events")
        assert isinstance(events, list)
        if engine == "candidate":
            for event_index, event in enumerate(events):
                assert isinstance(event, dict)
                comparison_key = str(event["comparison_key"])
                evidence = _source_evidence(
                    event.get("source_evidence"),
                    f"candidate artifact {artifact_id} events[{event_index}].source_evidence",
                )
                candidate_events.setdefault(comparison_key, set()).update(evidence)
        else:
            raw_legacy_records = snapshot.get("legacy_records")
            assert isinstance(raw_legacy_records, list)
            for record_index, record in enumerate(raw_legacy_records):
                assert isinstance(record, dict)
                record_id = str(record["legacy_record_id"])
                normalized_record = {
                    "legacy_record_id": record_id,
                    "comparison_key": record.get("comparison_key"),
                    "source_evidence": record.get("source_evidence"),
                }
                previous = legacy_records.get(record_id)
                if previous is not None and previous != normalized_record:
                    raise ShadowComparisonError(
                        f"legacy record {record_id!r} changed across immutable corpus artifacts"
                    )
                legacy_records[record_id] = normalized_record
        provenance[engine].append(
            {
                "artifact_id": artifact_id,
                "artifact_digest": item["artifact_digest"],
                "workflow_run_id": item["workflow_run_id"],
                "source_run_id": snapshot["source_run_id"],
                "producer_run_id": snapshot["producer_run_id"],
                "snapshot_sha256": snapshot["snapshot_sha256"],
                "event_count": snapshot["event_count"],
                "legacy_record_count": snapshot["legacy_record_count"],
            }
        )
    legacy_keys, crosswalk, crosswalk_rows = _crosswalk_legacy_records(
        legacy_records, candidate_events
    )
    candidate_keys = sorted(candidate_events)
    corpus_payload = _corpus_payload(candidate_events, legacy_records)
    source_artifacts = sorted(
        provenance["legacy"] + provenance["candidate"],
        key=lambda row: (
            str(row.get("producer_run_id")),
            int(str(row["artifact_id"])),
        ),
    )
    prior_hash = (
        str(previous_summary["report_sha256"])
        if isinstance(previous_summary, dict)
        else None
    )
    result: dict[str, object] = {
        "schema_version": 3,
        "observation_date": observed.isoformat(),
        "code_revision": revision,
        "corpus": {
            "schema_version": 1,
            "scope": CORPUS_SCOPE,
            "start_date": corpus_start_date.isoformat(),
            "end_date": observed.isoformat(),
            "day_count": (observed - corpus_start_date).days + 1,
            "previous_observation_date": (
                str(previous_summary["observation_date"])
                if isinstance(previous_summary, dict)
                else None
            ),
            "previous_report_sha256": prior_hash,
            "current_source_artifact_count": len(source_artifacts),
            "current_source_artifacts_sha256": hashlib.sha256(
                canonical_json_bytes(source_artifacts)
            ).hexdigest(),
            "corpus_payload_sha256": corpus_payload["corpus_payload_sha256"],
        },
        "corpus_payload": corpus_payload,
        "previous_shadow": previous_summary,
        "engines": {},
        "legacy_crosswalk": crosswalk,
        "legacy_crosswalk_records": crosswalk_rows,
    }
    engines = result["engines"]
    assert isinstance(engines, dict)
    for engine in ("legacy", "candidate"):
        keys = legacy_keys if engine == "legacy" else candidate_keys
        engines[engine] = {
            "status": "succeeded",
            "event_count": len(keys),
            "events_sha256": comparison_keys_sha256(keys),
            "comparison_keys": keys,
            "artifacts": sorted(provenance[engine], key=lambda row: int(row["artifact_id"])),
        }
    return result


class AdminApi:
    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.base_url = normalize_api_base(base_url)
        if len(token.strip()) < 32 or "\n" in token or "\r" in token:
            raise ShadowComparisonError("BSIDE_EDITOR_TOKEN is missing or invalid")
        self.client = httpx.Client(
            timeout=30.0,
            transport=transport,
            headers={
                "Authorization": f"Bearer {token.strip()}",
                "Accept": "application/json",
                "User-Agent": "bside-shadow-compare/1",
            },
        )

    def close(self) -> None:
        self.client.close()

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: Mapping[str, str | int | float | bool | None] | None = None,
        payload: Mapping[str, object] | None = None,
        expected_statuses: set[int] | None = None,
    ) -> dict[str, object]:
        response = self.client.request(
            method,
            f"{self.base_url}{path}",
            params=params,
            json=payload,
        )
        try:
            body = response.json()
        except ValueError as exc:
            raise ShadowComparisonError(
                f"{method} {path} returned invalid JSON ({response.status_code})"
            ) from exc
        if not isinstance(body, dict):
            raise ShadowComparisonError(f"{method} {path} returned a non-object response")
        allowed = expected_statuses or {200}
        if response.status_code not in allowed or body.get("ok") is not True:
            error = str(body.get("error") or "request_failed")
            raise ShadowComparisonError(
                f"{method} {path} failed with HTTP {response.status_code}: {error}"
            )
        return body

    def get_pages(
        self,
        path: str,
        params: Mapping[str, str | int | float | bool | None],
    ) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        page = 1
        while True:
            body = self._request(
                "GET",
                path,
                params={**params, "page": page, "limit": 100},
            )
            data = body.get("data")
            pagination = body.get("pagination")
            if (
                not isinstance(data, list)
                or any(not isinstance(row, dict) for row in data)
                or not isinstance(pagination, dict)
                or not isinstance(pagination.get("has_more"), bool)
            ):
                raise ShadowComparisonError(f"GET {path} returned an invalid page")
            rows.extend(data)
            if not pagination["has_more"]:
                return rows
            next_page = pagination.get("next_page")
            if not isinstance(next_page, int) or next_page != page + 1 or page >= 1000:
                raise ShadowComparisonError(f"GET {path} pagination did not advance")
            page = next_page

    def post(self, path: str, payload: Mapping[str, object]) -> dict[str, object]:
        return self._request(
            "POST",
            path,
            payload=payload,
            expected_statuses={200, 201},
        )


def _engine_api_run(engine: Mapping[str, object]) -> dict[str, object]:
    keys = engine.get("comparison_keys")
    if not isinstance(keys, list):
        raise ShadowComparisonError("aggregated engine comparison_keys are invalid")
    return {
        "status": "succeeded",
        "events": [{"comparison_key": key} for key in keys],
    }


def _validated_crosswalk(value: object, location: str = "legacy_crosswalk") -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != CROSSWALK_FIELDS:
        raise ShadowComparisonError(f"{location} fields are invalid")
    if value.get("schema_version") != 1:
        raise ShadowComparisonError(f"{location}.schema_version must be 1")
    counts: dict[str, int] = {}
    for field in (
        "eligible_legacy_record_count",
        "crosswalked_legacy_record_count",
        "unmatched_legacy_record_count",
        "ambiguous_legacy_record_count",
    ):
        raw = value.get(field)
        if not isinstance(raw, int) or isinstance(raw, bool) or raw < 0:
            raise ShadowComparisonError(f"{location}.{field} is invalid")
        counts[field] = raw
    if counts["eligible_legacy_record_count"] < 1:
        raise ShadowComparisonError(
            f"{location}.eligible_legacy_record_count must be non-zero"
        )
    if (
        counts["crosswalked_legacy_record_count"]
        != counts["eligible_legacy_record_count"]
        or counts["unmatched_legacy_record_count"] != 0
        or counts["ambiguous_legacy_record_count"] != 0
        or value.get("coverage_rate") != 1.0
    ):
        raise ShadowComparisonError(f"{location} is incomplete")
    digest = str(value.get("crosswalk_sha256") or "").casefold()
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ShadowComparisonError(f"{location}.crosswalk_sha256 is invalid")
    return {
        "schema_version": 1,
        **counts,
        "coverage_rate": 1.0,
        "crosswalk_sha256": digest,
    }


def _api_event_keys(run: object, location: str) -> list[str]:
    if not isinstance(run, dict) or run.get("status") != "succeeded":
        raise ShadowComparisonError(f"{location} did not succeed")
    events = run.get("events")
    if not isinstance(events, list):
        raise ShadowComparisonError(f"{location}.events is invalid")
    keys: list[str] = []
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            raise ShadowComparisonError(f"{location}.events[{index}] is invalid")
        key = str(event.get("comparison_key") or "").strip().casefold()
        if not COMPARISON_KEY_RE.fullmatch(key):
            raise ShadowComparisonError(f"{location}.events[{index}] key is invalid")
        keys.append(key)
    if keys != sorted(set(keys)):
        raise ShadowComparisonError(f"{location}.events must be sorted and unique")
    claimed_count = run.get("event_count")
    if claimed_count is not None and claimed_count != len(keys):
        raise ShadowComparisonError(f"{location}.event_count mismatch")
    claimed_digest = run.get("events_sha256")
    if claimed_digest is not None and claimed_digest != comparison_keys_sha256(keys):
        raise ShadowComparisonError(f"{location}.events_sha256 mismatch")
    return keys


def _shadow_run_matches(
    row: Mapping[str, object],
    *,
    observed: date,
    revision: str,
    legacy_keys: Sequence[str],
    candidate_keys: Sequence[str],
    legacy_crosswalk: Mapping[str, object],
) -> bool:
    if row.get("observation_date") != observed.isoformat() or row.get("code_revision") != revision:
        return False
    try:
        actual_legacy = _api_event_keys(row.get("legacy_run"), "legacy_run")
        actual_candidate = _api_event_keys(row.get("candidate_run"), "candidate_run")
    except ShadowComparisonError:
        return False
    try:
        actual_crosswalk = _validated_crosswalk(
            row.get("legacy_crosswalk"), "shadow_run.legacy_crosswalk"
        )
    except ShadowComparisonError:
        return False
    return (
        actual_legacy == list(legacy_keys)
        and actual_candidate == list(candidate_keys)
        and actual_crosswalk == dict(legacy_crosswalk)
    )


def _discrepancy_id(observed: date, revision: str, key: str, kind: str) -> str:
    external = "|".join((observed.isoformat(), revision, key, kind))
    return "shadow:" + hashlib.sha256(external.encode("utf-8")).hexdigest()


def _discrepancy_payload(
    observed: date,
    revision: str,
    key: str,
    kind: str,
    *,
    expected_updated_at: str | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "discrepancy_id": _discrepancy_id(observed, revision, key, kind),
        "observation_date": observed.isoformat(),
        "code_revision": revision,
        "comparison_key": key,
        "discrepancy_type": kind,
        "legacy_event": {"comparison_key": key} if kind == "candidate_missing" else None,
        "candidate_event": {"comparison_key": key} if kind == "candidate_added" else None,
        "review_status": "pending",
        "review_note": "",
    }
    if expected_updated_at is not None:
        payload["expected_updated_at"] = expected_updated_at
    return payload


def _same_discrepancy(existing: Mapping[str, object], expected: Mapping[str, object]) -> bool:
    for field in (
        "discrepancy_id",
        "observation_date",
        "code_revision",
        "comparison_key",
        "discrepancy_type",
        "legacy_event",
        "candidate_event",
    ):
        if existing.get(field) != expected.get(field):
            return False
    return True


def post_shadow_comparison(
    api: AdminApi,
    aggregate: Mapping[str, object],
) -> dict[str, object]:
    try:
        observed = date.fromisoformat(str(aggregate.get("observation_date") or ""))
    except ValueError as exc:
        raise ShadowComparisonError("aggregate observation_date is invalid") from exc
    revision = str(aggregate.get("code_revision") or "").casefold()
    if not REVISION_RE.fullmatch(revision):
        raise ShadowComparisonError("aggregate code_revision is invalid")
    engines = aggregate.get("engines")
    if not isinstance(engines, dict):
        raise ShadowComparisonError("aggregate engines are invalid")
    legacy = engines.get("legacy")
    candidate = engines.get("candidate")
    if not isinstance(legacy, dict) or not isinstance(candidate, dict):
        raise ShadowComparisonError("aggregate requires both engines")
    legacy_keys = _api_event_keys(_engine_api_run(legacy), "legacy")
    candidate_keys = _api_event_keys(_engine_api_run(candidate), "candidate")
    legacy_crosswalk = _validated_crosswalk(aggregate.get("legacy_crosswalk"))

    previous_day = observed - timedelta(days=1)
    previous_shadow = aggregate.get("previous_shadow")
    prior_runs = api.get_pages(
        "/admin/shadow-runs",
        {
            "code_revision": revision,
            "from": previous_day.isoformat(),
            "to": previous_day.isoformat(),
        },
    )
    if previous_shadow is None:
        if prior_runs:
            raise ShadowComparisonError(
                "previous-day shadow run exists but its immutable comparison receipt was not chained"
            )
    else:
        if not isinstance(previous_shadow, dict):
            raise ShadowComparisonError("aggregate previous_shadow is invalid")
        prior_legacy_keys = previous_shadow.get("legacy_comparison_keys")
        prior_candidate_keys = previous_shadow.get("candidate_comparison_keys")
        prior_crosswalk = previous_shadow.get("legacy_crosswalk")
        if (
            previous_shadow.get("observation_date") != previous_day.isoformat()
            or not isinstance(prior_legacy_keys, list)
            or not isinstance(prior_candidate_keys, list)
            or len(prior_runs) != 1
            or not _shadow_run_matches(
                prior_runs[0],
                observed=previous_day,
                revision=revision,
                legacy_keys=[str(key) for key in prior_legacy_keys],
                candidate_keys=[str(key) for key in prior_candidate_keys],
                legacy_crosswalk=_validated_crosswalk(
                    prior_crosswalk, "previous_shadow.legacy_crosswalk"
                ),
            )
        ):
            raise ShadowComparisonError(
                "previous comparison receipt does not match the durable previous-day shadow run"
            )
    pending_prior = api.get_pages(
        "/admin/shadow-discrepancies",
        {
            "code_revision": revision,
            "review_status": "pending",
            "to": previous_day.isoformat(),
        },
    )
    if pending_prior:
        raise ShadowComparisonError(
            f"{len(pending_prior)} prior-day shadow discrepancies remain pending"
        )

    existing_runs = api.get_pages(
        "/admin/shadow-runs",
        {
            "code_revision": revision,
            "from": observed.isoformat(),
            "to": observed.isoformat(),
        },
    )
    if len(existing_runs) > 1:
        raise ShadowComparisonError("multiple shadow runs exist for the same day and SHA")
    run_payload: dict[str, object] = {
        "observation_date": observed.isoformat(),
        "code_revision": revision,
        "legacy_run": _engine_api_run(legacy),
        "candidate_run": _engine_api_run(candidate),
        "legacy_crosswalk": legacy_crosswalk,
    }
    if existing_runs:
        existing_run = existing_runs[0]
        if not _shadow_run_matches(
            existing_run,
            observed=observed,
            revision=revision,
            legacy_keys=legacy_keys,
            candidate_keys=candidate_keys,
            legacy_crosswalk=legacy_crosswalk,
        ):
            raise ShadowComparisonError(
                "an immutable shadow run already exists with different engine outputs"
            )
        updated_at = existing_run.get("updated_at")
        if not isinstance(updated_at, str) or not updated_at:
            raise ShadowComparisonError("existing shadow run omitted updated_at")
        run_payload["expected_updated_at"] = updated_at
    run_ack = api.post("/admin/shadow-runs", run_payload)
    for field, expected_value in (
        ("observation_date", observed.isoformat()),
        ("code_revision", revision),
    ):
        if run_ack.get(field) != expected_value:
            raise ShadowComparisonError(f"shadow run ACK {field} mismatch")
    for field, expected_count in (
        ("legacy_event_count", len(legacy_keys)),
        ("candidate_event_count", len(candidate_keys)),
    ):
        if field in run_ack and run_ack.get(field) != expected_count:
            raise ShadowComparisonError(f"shadow run ACK {field} mismatch")
    if _validated_crosswalk(
        run_ack.get("legacy_crosswalk"), "shadow run ACK legacy_crosswalk"
    ) != legacy_crosswalk:
        raise ShadowComparisonError("shadow run ACK legacy_crosswalk mismatch")

    verified_runs = api.get_pages(
        "/admin/shadow-runs",
        {
            "code_revision": revision,
            "from": observed.isoformat(),
            "to": observed.isoformat(),
        },
    )
    if len(verified_runs) != 1 or not _shadow_run_matches(
        verified_runs[0],
        observed=observed,
        revision=revision,
        legacy_keys=legacy_keys,
        candidate_keys=candidate_keys,
        legacy_crosswalk=legacy_crosswalk,
    ):
        raise ShadowComparisonError("shadow run read-after-write ACK mismatch")

    expected_discrepancies: dict[str, dict[str, object]] = {}
    legacy_set = set(legacy_keys)
    candidate_set = set(candidate_keys)
    for key in sorted(legacy_set - candidate_set):
        payload = _discrepancy_payload(observed, revision, key, "candidate_missing")
        expected_discrepancies[str(payload["discrepancy_id"])] = payload
    for key in sorted(candidate_set - legacy_set):
        payload = _discrepancy_payload(observed, revision, key, "candidate_added")
        expected_discrepancies[str(payload["discrepancy_id"])] = payload

    existing_rows = api.get_pages(
        "/admin/shadow-discrepancies",
        {
            "code_revision": revision,
            "from": observed.isoformat(),
            "to": observed.isoformat(),
        },
    )
    existing_by_id: dict[str, dict[str, object]] = {}
    for row in existing_rows:
        discrepancy_id = str(row.get("discrepancy_id") or "")
        if not discrepancy_id or discrepancy_id in existing_by_id:
            raise ShadowComparisonError("duplicate or invalid existing discrepancy")
        status = str(row.get("review_status") or "")
        if status not in VALID_REVIEW_STATUSES:
            raise ShadowComparisonError("existing discrepancy has invalid review_status")
        existing_by_id[discrepancy_id] = row
    unexpected = sorted(set(existing_by_id) - set(expected_discrepancies))
    if unexpected:
        # A changed daily set must be reconciled by a human. The runner never
        # turns a disappeared difference into resolved/dismissed on its own.
        raise ShadowComparisonError(
            "same-day discrepancies exist outside the immutable comparison set"
        )

    created = updated = retained = 0
    acknowledged_ids: list[str] = []
    for discrepancy_id in sorted(expected_discrepancies):
        expected_discrepancy = expected_discrepancies[discrepancy_id]
        existing = existing_by_id.get(discrepancy_id)
        if existing is not None:
            if not _same_discrepancy(existing, expected_discrepancy):
                status = str(existing.get("review_status") or "")
                if status != "pending":
                    raise ShadowComparisonError(
                        "a human-reviewed discrepancy differs from immutable engine output"
                    )
                updated_at = existing.get("updated_at")
                if not isinstance(updated_at, str) or not updated_at:
                    raise ShadowComparisonError("existing discrepancy omitted updated_at")
                update_payload = dict(expected_discrepancy)
                update_payload["expected_updated_at"] = updated_at
                ack = api.post("/admin/shadow-discrepancies", update_payload)
                if (
                    ack.get("discrepancy_id") != discrepancy_id
                    or ack.get("review_status") != "pending"
                ):
                    raise ShadowComparisonError("shadow discrepancy update ACK mismatch")
                updated += 1
            else:
                retained += 1
            acknowledged_ids.append(discrepancy_id)
            continue
        ack = api.post("/admin/shadow-discrepancies", expected_discrepancy)
        if ack.get("discrepancy_id") != discrepancy_id or ack.get("review_status") != "pending":
            raise ShadowComparisonError("shadow discrepancy create ACK mismatch")
        acknowledged_ids.append(discrepancy_id)
        created += 1

    verified_rows = api.get_pages(
        "/admin/shadow-discrepancies",
        {
            "code_revision": revision,
            "from": observed.isoformat(),
            "to": observed.isoformat(),
        },
    )
    verified_by_id = {str(row.get("discrepancy_id") or ""): row for row in verified_rows}
    if set(verified_by_id) != set(expected_discrepancies):
        raise ShadowComparisonError("shadow discrepancy read-after-write ACK count mismatch")
    for discrepancy_id, expected_discrepancy in expected_discrepancies.items():
        if not _same_discrepancy(
            verified_by_id[discrepancy_id], expected_discrepancy
        ):
            raise ShadowComparisonError("shadow discrepancy read-after-write ACK mismatch")

    return {
        "shadow_run_updated_at": verified_runs[0].get("updated_at"),
        "legacy_event_count": len(legacy_keys),
        "candidate_event_count": len(candidate_keys),
        "legacy_events_sha256": comparison_keys_sha256(legacy_keys),
        "candidate_events_sha256": comparison_keys_sha256(candidate_keys),
        "legacy_crosswalk": legacy_crosswalk,
        "discrepancy_count": len(expected_discrepancies),
        "discrepancies_created": created,
        "discrepancies_updated": updated,
        "discrepancies_retained": retained,
        "discrepancy_ids": acknowledged_ids,
    }


def run(
    *,
    manifest_path: Path,
    snapshots_root: Path,
    api_base_url: str,
    token: str,
    output_path: Path,
    transport: httpx.BaseTransport | None = None,
) -> dict[str, object]:
    aggregate = aggregate_engine_outputs(manifest_path, snapshots_root)
    api = AdminApi(api_base_url, token, transport=transport)
    try:
        api_ack = post_shadow_comparison(api, aggregate)
    finally:
        api.close()
    report = {
        **aggregate,
        "api_ack": api_ack,
        "generated_at": datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
        "distribution_mode": "web_only",
    }
    report["report_sha256"] = hashlib.sha256(canonical_json_bytes(report)).hexdigest()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary = output_path.with_suffix(output_path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(output_path)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Compare real immutable legacy/candidate outputs and persist one KST shadow day."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    extract = subparsers.add_parser("extract-artifacts")
    extract.add_argument("--manifest", type=Path, required=True)
    extract.add_argument("--archives", type=Path, required=True)
    extract.add_argument("--destination", type=Path, required=True)
    compare = subparsers.add_parser("compare")
    compare.add_argument("--manifest", type=Path, required=True)
    compare.add_argument("--snapshots-root", type=Path, required=True)
    compare.add_argument(
        "--api-base-url", default=os.environ.get("GOVERNANCE_API_BASE_URL", "")
    )
    compare.add_argument("--token", default=os.environ.get("BSIDE_EDITOR_TOKEN", ""))
    compare.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "extract-artifacts":
            extract_artifacts(args.manifest, args.archives, args.destination)
        elif args.command == "compare":
            report = run(
                manifest_path=args.manifest,
                snapshots_root=args.snapshots_root,
                api_base_url=args.api_base_url,
                token=args.token,
                output_path=args.output,
            )
            print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    except ShadowComparisonError as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    main()
