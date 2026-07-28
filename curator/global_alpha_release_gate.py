from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import re
import sys
import zipfile
import zlib
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping, Sequence
from urllib.parse import urlsplit

from .global_alpha_observation_segment import (
    MAX_SLOT_LATENESS_SECONDS,
    OBSERVATION_INTERVAL_SECONDS,
    SEGMENT_COUNT,
    SEGMENT_COUNTS,
    SEGMENT_KIND,
    SEGMENT_SCHEMA_VERSION,
    TOTAL_OBSERVATIONS,
    canonical_jsonl,
    segment_slot_bounds,
)
from .global_alpha_pages_identity import (
    PagesArtifactIdentityError,
    validate_pages_artifact_binding,
    validate_terminal_content_identity,
)


SCHEMA_VERSION = 1
REPORT_KIND = "bside-global-production-alpha-release-report"
INPUT_BUNDLE_KIND = "bside-global-production-alpha-release-inputs"
AUTOMATED_EVIDENCE_KIND = "bside-global-alpha-automated-evidence"
EXPECTED_COUNTRY_COVERAGE = {
    "KR": "market-wide",
    "US": "market-wide",
    "JP": "link-only",
    "GB": "link-only",
    "CA": "link-only",
    "AU": "link-only",
}
REQUIRED_ALPHA_COUNTRIES = frozenset(("KR", "US", "CA", "AU"))
OPTIONAL_UNAVAILABLE_COUNTRIES = frozenset(("JP", "GB"))
EXPECTED_CONNECTORS = {
    "dart": "KR",
    "sec-edgar": "US",
}
MINIMUM_CONNECTOR_COVERAGE = timedelta(days=30)
MAXIMUM_CONNECTOR_COVERAGE_END_AGE = timedelta(hours=24)
EXPECTED_VIEWPORTS = {"390x844", "768x1024", "1440x900"}
REQUIRED_API_ROUTE_PREFIXES = {
    "/briefs/latest",
    "/live",
    "/events",
    "/issuers",
    "/calendar",
    "/search",
    "/sources/status",
    "/exports/events.json",
    "/exports/events.csv",
    "/feeds/events.atom",
}
INPUT_FILENAMES = (
    "connector-idempotency.json",
    "human-review.json",
    "content-integrity.json",
    "experience.json",
    "approval.json",
)
FORBIDDEN_EVIDENCE_MARKERS = ("fixture", "synthetic", "sample", "test")
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class AlphaReleaseEvidenceError(ValueError):
    """Raised when Production Alpha evidence is malformed or ineligible."""


def _mapping(value: object, location: str) -> Mapping[str, object]:
    if not isinstance(value, dict):
        raise AlphaReleaseEvidenceError(f"{location}: expected an object")
    return value


def _list(value: object, location: str) -> list[object]:
    if not isinstance(value, list):
        raise AlphaReleaseEvidenceError(f"{location}: expected an array")
    return value


def _text(value: object, field: str, location: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise AlphaReleaseEvidenceError(
            f"{location}: {field} must be a non-empty string"
        )
    return result


def _bool(value: object, field: str, location: str) -> bool:
    if not isinstance(value, bool):
        raise AlphaReleaseEvidenceError(f"{location}: {field} must be boolean")
    return value


def _int(value: object, field: str, location: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise AlphaReleaseEvidenceError(
            f"{location}: {field} must be an integer >= {minimum}"
        )
    return value


def _receipt_count_partition(
    value: Mapping[str, object],
    *,
    location: str,
    minimum_accepted: int = 0,
) -> tuple[int, int, int, int]:
    raw = _int(value.get("raw_count"), "raw_count", location)
    filtered_out = _int(
        value.get("filtered_out_count"),
        "filtered_out_count",
        location,
    )
    accepted = _int(
        value.get("accepted_count"),
        "accepted_count",
        location,
        minimum=minimum_accepted,
    )
    acknowledged = _int(
        value.get("acknowledged_count"),
        "acknowledged_count",
        location,
        minimum=minimum_accepted,
    )
    if raw != filtered_out + accepted:
        raise AlphaReleaseEvidenceError(
            f"{location}: raw_count must equal "
            "filtered_out_count + accepted_count"
        )
    if acknowledged != accepted:
        raise AlphaReleaseEvidenceError(
            f"{location}: acknowledged_count must equal accepted_count"
        )
    return raw, filtered_out, accepted, acknowledged


def _number(
    value: object,
    field: str,
    location: str,
    *,
    minimum: float = 0.0,
) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AlphaReleaseEvidenceError(f"{location}: {field} must be numeric")
    result = float(value)
    if not math.isfinite(result) or result < minimum:
        raise AlphaReleaseEvidenceError(
            f"{location}: {field} must be finite and >= {minimum}"
        )
    return result


def _timestamp(value: object, field: str, location: str) -> datetime:
    raw = _text(value, field, location).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise AlphaReleaseEvidenceError(
            f"{location}: {field} must be ISO-8601"
        ) from exc
    if parsed.tzinfo is None:
        raise AlphaReleaseEvidenceError(
            f"{location}: {field} must include a timezone"
        )
    return parsed.astimezone(timezone.utc)


def _date(value: object, field: str, location: str) -> date:
    raw = _text(value, field, location)
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise AlphaReleaseEvidenceError(
            f"{location}: {field} must be an ISO-8601 date"
        ) from exc
    if parsed.isoformat() != raw:
        raise AlphaReleaseEvidenceError(
            f"{location}: {field} must use YYYY-MM-DD"
        )
    return parsed


def _revision(value: object, field: str, location: str) -> str:
    result = _text(value, field, location).casefold()
    if SHA_RE.fullmatch(result) is None:
        raise AlphaReleaseEvidenceError(
            f"{location}: {field} must be a full 40-character Git SHA"
        )
    return result


def _digest(value: object, field: str, location: str) -> str:
    result = _text(value, field, location).casefold()
    if SHA256_RE.fullmatch(result) is None:
        raise AlphaReleaseEvidenceError(
            f"{location}: {field} must be a SHA-256 digest"
        )
    return result


def _v2_api_base(value: object, field: str, location: str) -> str:
    raw = _text(value, field, location)
    parsed = urlsplit(raw)
    path = parsed.path.rstrip("/")
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not (
            path.endswith("/api/v2")
            or path.endswith("/api.php/api/v2")
        )
    ):
        raise AlphaReleaseEvidenceError(
            f"{location}: {field} must be a canonical HTTPS v2 API base"
        )
    return f"https://{parsed.netloc.casefold()}{path}"


def _rate_counts(
    *,
    numerator: object,
    denominator: object,
    location: str,
) -> tuple[int, int, float]:
    num = _int(numerator, "numerator", location)
    den = _int(denominator, "denominator", location, minimum=1)
    if num > den:
        raise AlphaReleaseEvidenceError(
            f"{location}: numerator exceeds denominator"
        )
    return num, den, num / den


def _provenance(
    value: Mapping[str, object],
    *,
    kind: str,
    expected_revision: str,
    location: str,
    evidence_as_of: datetime,
) -> datetime:
    if value.get("schema_version") != SCHEMA_VERSION:
        raise AlphaReleaseEvidenceError(
            f"{location}: schema_version must be {SCHEMA_VERSION}"
        )
    if value.get("kind") != kind:
        raise AlphaReleaseEvidenceError(f"{location}: unexpected kind")
    if value.get("environment") != "production":
        raise AlphaReleaseEvidenceError(
            f"{location}: environment must be production"
        )
    if _bool(value.get("is_synthetic"), "is_synthetic", location):
        raise AlphaReleaseEvidenceError(
            f"{location}: synthetic evidence is not release eligible"
        )
    source = _text(value.get("evidence_source"), "evidence_source", location)
    if any(marker in source.casefold() for marker in FORBIDDEN_EVIDENCE_MARKERS):
        raise AlphaReleaseEvidenceError(
            f"{location}: evidence_source is not release eligible"
        )
    revision = _revision(value.get("code_revision"), "code_revision", location)
    if revision != expected_revision:
        raise AlphaReleaseEvidenceError(
            f"{location}: code_revision does not match expected revision"
        )
    collected_at = _timestamp(value.get("collected_at"), "collected_at", location)
    if collected_at > evidence_as_of + timedelta(minutes=1):
        raise AlphaReleaseEvidenceError(
            f"{location}: collected_at is after evidence_as_of"
        )
    if evidence_as_of - collected_at > timedelta(days=7):
        raise AlphaReleaseEvidenceError(
            f"{location}: evidence is older than seven days"
        )
    return collected_at


def _gate(
    name: str,
    passed: bool,
    *,
    required: object,
    actual: object,
) -> dict[str, object]:
    return {
        "name": name,
        "passed": bool(passed),
        "required": required,
        "actual": actual,
    }


def _sha256_json(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_json(path: Path) -> dict[str, object]:
    if not path.is_file():
        raise AlphaReleaseEvidenceError(f"{path}: evidence file is missing")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AlphaReleaseEvidenceError(f"{path}: invalid JSON") from exc
    return dict(_mapping(value, str(path)))


def _load_observations(path: Path) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    files = sorted(path.glob("*.json")) if path.is_dir() else [path]
    if not files:
        raise AlphaReleaseEvidenceError(
            f"{path}: no observation evidence files were found"
        )
    for source in files:
        try:
            text = source.read_text(encoding="utf-8")
        except OSError as exc:
            raise AlphaReleaseEvidenceError(
                f"{source}: observation file is unreadable"
            ) from exc
        if source.suffix.casefold() == ".jsonl":
            raw_values: Iterable[object] = (
                json.loads(line)
                for line in text.splitlines()
                if line.strip()
            )
        else:
            try:
                decoded = json.loads(text)
            except json.JSONDecodeError as exc:
                raise AlphaReleaseEvidenceError(
                    f"{source}: invalid observation JSON"
                ) from exc
            raw_values = decoded if isinstance(decoded, list) else [decoded]
        try:
            records.extend(
                dict(_mapping(value, f"{source}: observation"))
                for value in raw_values
            )
        except json.JSONDecodeError as exc:
            raise AlphaReleaseEvidenceError(
                f"{source}: invalid observation JSONL"
            ) from exc
    if not records:
        raise AlphaReleaseEvidenceError(
            f"{path}: observation evidence is empty"
        )
    return records


def _source_snapshot(
    value: object,
    *,
    location: str,
) -> tuple[str, str, str, bool, int, int]:
    item = _mapping(value, location)
    country = _text(item.get("country"), "country", location)
    coverage = _text(item.get("coverage_mode"), "coverage_mode", location)
    public_status = _text(
        item.get("public_status"),
        "public_status",
        location,
    )
    public_ready = _bool(item.get("public_ready"), "public_ready", location)
    raw_count = _int(item.get("raw_count"), "raw_count", location)
    acknowledged_count = _int(
        item.get("acknowledged_count"),
        "acknowledged_count",
        location,
    )
    if acknowledged_count > raw_count:
        raise AlphaReleaseEvidenceError(
            f"{location}: acknowledged_count exceeds raw_count"
        )
    return (
        country,
        coverage,
        public_status,
        public_status == "active" and public_ready,
        raw_count,
        acknowledged_count,
    )


def validate_observations(
    records: Sequence[Mapping[str, object]],
    *,
    expected_revision: str,
    expected_terminal_content: Mapping[str, object],
) -> tuple[dict[str, object], list[dict[str, object]]]:
    parsed: list[tuple[datetime, Mapping[str, object]]] = []
    observation_ids: set[str] = set()
    observed_values: set[datetime] = set()
    window_starts: set[datetime] = set()
    window_ends: set[datetime] = set()
    deployed_api_bases: set[str] = set()
    terminal_content_digests: set[str] = set()
    healthy_count = 0
    complete_probe_count = 0
    complete_coverage_count = 0
    required_probes = {
        "public_root",
        "health",
        "release_state",
        "deployed_build",
        "terminal_app",
        "terminal_styles",
        "sources_status",
        "live",
        "search",
        "event_detail",
    }
    always_required_probes = required_probes - {"event_detail"}

    for index, record in enumerate(records):
        location = f"observations[{index}]"
        if record.get("schema_version") != SCHEMA_VERSION:
            raise AlphaReleaseEvidenceError(
                f"{location}: schema_version must be {SCHEMA_VERSION}"
            )
        observation_id = _text(
            record.get("observation_id"),
            "observation_id",
            location,
        )
        if not observation_id.startswith("global-alpha:"):
            raise AlphaReleaseEvidenceError(
                f"{location}: observation_id has an invalid namespace"
            )
        if observation_id in observation_ids:
            raise AlphaReleaseEvidenceError(
                f"{location}: duplicate observation_id"
            )
        observation_ids.add(observation_id)
        observed_at = _timestamp(
            record.get("observed_at"),
            "observed_at",
            location,
        )
        if observed_at in observed_values:
            raise AlphaReleaseEvidenceError(
                f"{location}: duplicate observed_at"
            )
        observed_values.add(observed_at)
        parsed.append((observed_at, record))
        if _revision(
            record.get("workflow_revision"),
            "workflow_revision",
            location,
        ) != expected_revision:
            raise AlphaReleaseEvidenceError(
                f"{location}: workflow revision mismatch"
            )
        if _revision(
            record.get("deployed_build_sha"),
            "deployed_build_sha",
            location,
        ) != expected_revision:
            raise AlphaReleaseEvidenceError(
                f"{location}: deployed build revision mismatch"
            )
        if _revision(
            record.get("api_code_revision"),
            "api_code_revision",
            location,
        ) != expected_revision:
            raise AlphaReleaseEvidenceError(
                f"{location}: API code revision mismatch"
            )
        deployed_api_bases.add(
            _v2_api_base(
                record.get("deployed_api_base"),
                "deployed_api_base",
                location,
            )
        )
        if record.get("pipeline_mode") != "shadow":
            raise AlphaReleaseEvidenceError(
                f"{location}: pipeline_mode must be shadow"
            )
        if record.get("web_surface") != "governance-preview":
            raise AlphaReleaseEvidenceError(
                f"{location}: shadow evidence must identify governance-preview"
            )
        if record.get("release_state") != "preview":
            raise AlphaReleaseEvidenceError(
                f"{location}: release_state must be preview"
            )
        try:
            terminal_content = validate_terminal_content_identity(
                record.get("terminal_content")
            )
        except PagesArtifactIdentityError as exc:
            raise AlphaReleaseEvidenceError(
                f"{location}: terminal content identity is invalid"
            ) from exc
        if terminal_content != expected_terminal_content:
            raise AlphaReleaseEvidenceError(
                f"{location}: observed terminal bytes do not match "
                "the evidence-bound daily Pages artifact"
            )
        terminal_content_digests.add(str(terminal_content["sha256"]))

        window = _mapping(
            record.get("observation_window"),
            f"{location}.observation_window",
        )
        if _int(
            window.get("duration_hours"),
            "duration_hours",
            f"{location}.observation_window",
        ) != 24:
            raise AlphaReleaseEvidenceError(
                f"{location}: observation window must be 24 hours"
            )
        if not _bool(
            window.get("within_window"),
            "within_window",
            f"{location}.observation_window",
        ):
            raise AlphaReleaseEvidenceError(
                f"{location}: observation is outside the candidate window"
            )
        window_start = _timestamp(
            window.get("started_at"),
            "started_at",
            f"{location}.observation_window",
        )
        window_end = _timestamp(
            window.get("ends_at"),
            "ends_at",
            f"{location}.observation_window",
        )
        if window_end - window_start != timedelta(hours=24):
            raise AlphaReleaseEvidenceError(
                f"{location}: observation window boundaries are invalid"
            )
        if not window_start <= observed_at <= window_end:
            raise AlphaReleaseEvidenceError(
                f"{location}: observed_at is outside the declared window"
            )
        window_starts.add(window_start)
        window_ends.add(window_end)

        reasons = _list(record.get("reasons"), f"{location}.reasons")
        warnings = _list(record.get("warnings"), f"{location}.warnings")
        if (
            record.get("status") == "healthy"
            and not reasons
            and not warnings
        ):
            healthy_count += 1

        probes = _mapping(record.get("probes"), f"{location}.probes")
        probes_ok = set(probes) == required_probes
        for probe_name in always_required_probes:
            probe = _mapping(
                probes.get(probe_name),
                f"{location}.probes.{probe_name}",
            )
            probes_ok = probes_ok and (
                probe.get("http_status") == 200
                and probe.get("transport_succeeded") is True
                and probe.get("contract_valid") is True
            )
        event_availability = _mapping(
            record.get("event_availability"),
            f"{location}.event_availability",
        )
        event_state = event_availability.get("state")
        event_detail = _mapping(
            probes.get("event_detail"),
            f"{location}.probes.event_detail",
        )
        if event_state == "events_present":
            probes_ok = probes_ok and (
                event_detail.get("http_status") == 200
                and event_detail.get("transport_succeeded") is True
                and event_detail.get("contract_valid") is True
                and event_availability.get("returned") == 1
            )
        elif event_state == "no_events":
            probes_ok = probes_ok and (
                dict(event_detail)
                == {
                    "skipped": True,
                    "reason": "no_live_event_available",
                }
                and event_availability.get("returned") == 0
            )
        else:
            probes_ok = False
        complete_probe_count += int(probes_ok)

        sources = _list(record.get("sources"), f"{location}.sources")
        source_results: dict[str, tuple[str, str, bool, int, int]] = {}
        for source_index, source in enumerate(sources):
            (
                country,
                coverage,
                public_status,
                ready,
                raw_count,
                acknowledged_count,
            ) = _source_snapshot(
                source,
                location=f"{location}.sources[{source_index}]",
            )
            if country in source_results:
                raise AlphaReleaseEvidenceError(
                    f"{location}: duplicate country source status"
                )
            source_results[country] = (
                coverage,
                public_status,
                ready,
                raw_count,
                acknowledged_count,
            )
        coverage_ok = set(source_results) == set(EXPECTED_COUNTRY_COVERAGE)
        coverage_ok = coverage_ok and all(
            source_results[country][0] == expected_coverage
            for country, expected_coverage in EXPECTED_COUNTRY_COVERAGE.items()
        )
        coverage_ok = coverage_ok and all(
            source_results[country][1] == "active"
            and source_results[country][2]
            for country in REQUIRED_ALPHA_COUNTRIES
        )
        coverage_ok = coverage_ok and all(
            source_results[country][1] == "coverage_unavailable"
            and source_results[country][2] is False
            and source_results[country][3] == 0
            and source_results[country][4] == 0
            for country in OPTIONAL_UNAVAILABLE_COUNTRIES
        )
        complete_coverage_count += int(coverage_ok)

    if len(window_starts) != 1 or len(window_ends) != 1:
        raise AlphaReleaseEvidenceError(
            "observations: every record must reference one candidate window"
        )
    if len(deployed_api_bases) != 1:
        raise AlphaReleaseEvidenceError(
            "observations: deployed_api_base changed within the candidate window"
        )
    if len(terminal_content_digests) != 1:
        raise AlphaReleaseEvidenceError(
            "observations: terminal content identity changed within the candidate window"
        )
    parsed.sort(key=lambda item: item[0])
    timestamps = [item[0] for item in parsed]
    window_start = next(iter(window_starts))
    window_end = next(iter(window_ends))
    intervals = [
        (right - left).total_seconds() / 60
        for left, right in zip(timestamps, timestamps[1:])
    ]
    span_minutes = (timestamps[-1] - timestamps[0]).total_seconds() / 60
    max_gap = max(intervals, default=0.0)
    min_gap = min(intervals, default=0.0)
    sample_count = len(records)
    boundary_covered = (
        timestamps[0] <= window_start + timedelta(minutes=5)
        and timestamps[-1] >= window_end - timedelta(minutes=5)
    )
    cadence_ok = bool(intervals) and min_gap >= 2.0 and max_gap <= 8.0
    gates = [
        _gate(
            "observation.window_duration",
            span_minutes >= 1430.0 and boundary_covered,
            required="24 hours with at most five-minute boundary tolerance",
            actual={
                "span_minutes": span_minutes,
                "boundary_covered": boundary_covered,
            },
        ),
        _gate(
            "observation.five_minute_cadence",
            sample_count >= 287 and cadence_ok,
            required={
                "minimum_samples": 287,
                "interval_minutes": {"minimum": 2, "maximum": 8},
            },
            actual={
                "sample_count": sample_count,
                "minimum_interval_minutes": min_gap,
                "maximum_interval_minutes": max_gap,
            },
        ),
        _gate(
            "observation.no_incident_or_degradation",
            healthy_count == sample_count,
            required=sample_count,
            actual=healthy_count,
        ),
        _gate(
            "observation.probes_complete",
            complete_probe_count == sample_count,
            required=sample_count,
            actual=complete_probe_count,
        ),
        _gate(
            "observation.six_country_coverage",
            complete_coverage_count == sample_count,
            required=sample_count,
            actual=complete_coverage_count,
        ),
    ]
    summary = {
        "started_at": timestamps[0].isoformat(),
        "ended_at": timestamps[-1].isoformat(),
        "sample_count": sample_count,
        "span_minutes": span_minutes,
        "minimum_interval_minutes": min_gap,
        "maximum_interval_minutes": max_gap,
        "country_coverage": EXPECTED_COUNTRY_COVERAGE,
        "deployed_api_base": next(iter(deployed_api_bases)),
        "terminal_content_sha256": next(iter(terminal_content_digests)),
        "api_code_revision": expected_revision,
    }
    return summary, gates


def validate_connector_idempotency(
    report: Mapping[str, object],
    *,
    expected_revision: str,
    evidence_as_of: datetime,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    _provenance(
        report,
        kind="bside-global-alpha-connector-idempotency",
        expected_revision=expected_revision,
        location="connector-idempotency",
        evidence_as_of=evidence_as_of,
    )
    connectors = _list(report.get("connectors"), "connector-idempotency.connectors")
    seen: set[str] = set()
    passed = 0
    coverage_passed = 0
    summaries: list[dict[str, object]] = []
    for index, value in enumerate(connectors):
        location = f"connector-idempotency.connectors[{index}]"
        item = _mapping(value, location)
        family = _text(item.get("connector_family"), "connector_family", location)
        country = _text(item.get("country"), "country", location)
        if family in seen:
            raise AlphaReleaseEvidenceError(
                f"{location}: duplicate connector_family"
            )
        seen.add(family)
        payload_digest = _digest(
            item.get("payload_sha256"),
            "payload_sha256",
            location,
        )
        first = _mapping(item.get("first_run"), f"{location}.first_run")
        replay = _mapping(item.get("replay_run"), f"{location}.replay_run")
        (
            first_raw,
            first_filtered_out,
            first_accepted,
            first_ack,
        ) = _receipt_count_partition(
            first,
            location=f"{location}.first_run",
            minimum_accepted=1,
        )
        (
            replay_raw,
            replay_filtered_out,
            replay_accepted,
            replay_ack,
        ) = _receipt_count_partition(
            replay,
            location=f"{location}.replay_run",
            minimum_accepted=1,
        )
        first_rows = _int(
            item.get("row_count_after_first"),
            "row_count_after_first",
            location,
            minimum=1,
        )
        replay_rows = _int(
            item.get("row_count_after_replay"),
            "row_count_after_replay",
            location,
            minimum=1,
        )
        duplicate_rows = _int(
            item.get("duplicate_row_count"),
            "duplicate_row_count",
            location,
        )
        first_checkpoint = _text(
            item.get("checkpoint_after_first"),
            "checkpoint_after_first",
            location,
        )
        replay_checkpoint = _text(
            item.get("checkpoint_after_replay"),
            "checkpoint_after_replay",
            location,
        )
        coverage_started_at = _timestamp(
            item.get("coverage_started_at"),
            "coverage_started_at",
            location,
        )
        coverage_ended_at = _timestamp(
            item.get("coverage_ended_at"),
            "coverage_ended_at",
            location,
        )
        successful_window_count = _int(
            item.get("successful_window_count"),
            "successful_window_count",
            location,
            minimum=1,
        )
        failed_window_count = _int(
            item.get("failed_window_count"),
            "failed_window_count",
            location,
        )
        completed_windows = _list(
            item.get("completed_windows"),
            f"{location}.completed_windows",
        )
        if len(completed_windows) < 30 or len(completed_windows) > 366:
            raise AlphaReleaseEvidenceError(
                f"{location}: completed_windows must contain 30..366 windows"
            )
        completed_ranges: list[tuple[date, date]] = []
        receipt_digests: set[str] = set()
        for window_index, window_value in enumerate(completed_windows):
            window_location = (
                f"{location}.completed_windows[{window_index}]"
            )
            window = _mapping(window_value, window_location)
            expected_fields = {
                "window_start",
                "window_end_exclusive",
                "raw_count",
                "filtered_out_count",
                "accepted_count",
                "acknowledged_count",
                "status",
                "code_revision",
                "receipt_sha256",
            }
            if set(window) != expected_fields:
                raise AlphaReleaseEvidenceError(
                    f"{window_location}: fields do not match the receipt contract"
                )
            window_start = _date(
                window.get("window_start"),
                "window_start",
                window_location,
            )
            window_end = _date(
                window.get("window_end_exclusive"),
                "window_end_exclusive",
                window_location,
            )
            if window_end - window_start != timedelta(days=1):
                raise AlphaReleaseEvidenceError(
                    f"{window_location}: window must cover exactly one day"
                )
            _receipt_count_partition(
                window,
                location=window_location,
            )
            if window.get("status") != "complete":
                raise AlphaReleaseEvidenceError(
                    f"{window_location}: status must be complete"
                )
            if _revision(
                window.get("code_revision"),
                "code_revision",
                window_location,
            ) != expected_revision:
                raise AlphaReleaseEvidenceError(
                    f"{window_location}: code revision mismatch"
                )
            receipt_digest = _digest(
                window.get("receipt_sha256"),
                "receipt_sha256",
                window_location,
            )
            if receipt_digest in receipt_digests:
                raise AlphaReleaseEvidenceError(
                    f"{window_location}: duplicate receipt digest"
                )
            receipt_digests.add(receipt_digest)
            completed_ranges.append((window_start, window_end))
        for previous, current in zip(
            completed_ranges,
            completed_ranges[1:],
        ):
            if previous[1] != current[0]:
                raise AlphaReleaseEvidenceError(
                    f"{location}: completed windows contain a gap or overlap"
                )
        coverage_duration = coverage_ended_at - coverage_started_at
        coverage_current = (
            coverage_duration >= MINIMUM_CONNECTOR_COVERAGE
            and completed_ranges[0][0] == coverage_started_at.date()
            and completed_ranges[-1][1] == coverage_ended_at.date()
            and coverage_started_at
            <= evidence_as_of - MINIMUM_CONNECTOR_COVERAGE
            and coverage_ended_at
            >= evidence_as_of - MAXIMUM_CONNECTOR_COVERAGE_END_AGE
            and coverage_ended_at <= evidence_as_of + timedelta(minutes=1)
            and successful_window_count == len(completed_ranges)
            and failed_window_count == 0
        )
        idempotent = (
            EXPECTED_CONNECTORS.get(family) == country
            and first_raw == replay_raw
            and first_filtered_out == replay_filtered_out
            and first_accepted == replay_accepted
            and first_ack == replay_ack
            and first_rows == replay_rows
            and duplicate_rows == 0
            and first_checkpoint == replay_checkpoint
            and first.get("idempotent") is False
            and replay.get("idempotent") is True
            and _digest(
                replay.get("payload_sha256"),
                "payload_sha256",
                f"{location}.replay_run",
            )
            == payload_digest
        )
        passed += int(idempotent)
        coverage_passed += int(coverage_current)
        summaries.append(
            {
                "connector_family": family,
                "country": country,
                "raw_count": first_raw,
                "filtered_out_count": first_filtered_out,
                "accepted_count": first_accepted,
                "acknowledged_count": first_ack,
                "idempotent": idempotent,
                "coverage_started_at": coverage_started_at.isoformat(),
                "coverage_ended_at": coverage_ended_at.isoformat(),
                "coverage_days": coverage_duration.total_seconds() / 86_400,
                "successful_window_count": successful_window_count,
                "failed_window_count": failed_window_count,
                "completed_window_count": len(completed_ranges),
                "minimum_30_day_horizon": coverage_current,
            }
        )
    exact_set = seen == set(EXPECTED_CONNECTORS)
    gates = [
        _gate(
            "connectors.exact_official_set",
            exact_set,
            required=sorted(EXPECTED_CONNECTORS),
            actual=sorted(seen),
        ),
        _gate(
            "connectors.idempotent_replay",
            exact_set and passed == len(EXPECTED_CONNECTORS),
            required=len(EXPECTED_CONNECTORS),
            actual=passed,
        ),
        _gate(
            "connectors.minimum_30_day_horizon",
            exact_set and coverage_passed == len(EXPECTED_CONNECTORS),
            required=len(EXPECTED_CONNECTORS),
            actual=coverage_passed,
        ),
    ]
    return {"connectors": summaries}, gates


def _review_record(
    value: object,
    *,
    location: str,
    identity_fields: Sequence[str],
    allowed_decisions: set[object],
    boolean_decision: bool = False,
) -> tuple[str, ...]:
    item = _mapping(value, location)
    identities = tuple(_text(item.get(field), field, location) for field in identity_fields)
    decision = item.get("decision")
    if (
        (boolean_decision and not isinstance(decision, bool))
        or decision not in allowed_decisions
    ):
        raise AlphaReleaseEvidenceError(f"{location}: invalid review decision")
    if item.get("reviewer_type") != "human":
        raise AlphaReleaseEvidenceError(
            f"{location}: reviewer_type must be human"
        )
    _text(item.get("reviewer_reference"), "reviewer_reference", location)
    _timestamp(item.get("reviewed_at"), "reviewed_at", location)
    return identities


def validate_human_review(
    report: Mapping[str, object],
    *,
    expected_revision: str,
    evidence_as_of: datetime,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    _provenance(
        report,
        kind="bside-global-alpha-human-review",
        expected_revision=expected_revision,
        location="human-review",
        evidence_as_of=evidence_as_of,
    )
    if report.get("ground_truth_source") != "human":
        raise AlphaReleaseEvidenceError(
            "human-review: ground_truth_source must be human"
        )
    if _bool(
        report.get("ai_generated_ground_truth"),
        "ai_generated_ground_truth",
        "human-review",
    ):
        raise AlphaReleaseEvidenceError(
            "human-review: AI output cannot be ground truth"
        )
    if not _bool(
        report.get("human_attestation"),
        "human_attestation",
        "human-review",
    ):
        raise AlphaReleaseEvidenceError(
            "human-review: explicit human attestation is required"
        )
    raw_counts = _mapping(report.get("raw_counts"), "human-review.raw_counts")
    event_count = _int(
        raw_counts.get("event_review_count"),
        "event_review_count",
        "human-review.raw_counts",
        minimum=1,
    )
    pair_count = _int(
        raw_counts.get("same_event_pair_review_count"),
        "same_event_pair_review_count",
        "human-review.raw_counts",
        minimum=1,
    )
    top_reviewed, top_total, top_rate = _rate_counts(
        numerator=raw_counts.get("top5_human_reviewed_count"),
        denominator=raw_counts.get("top5_published_count"),
        location="human-review.raw_counts.top5",
    )
    event_reviews = _list(report.get("event_reviews"), "human-review.event_reviews")
    pair_reviews = _list(
        report.get("same_event_pair_reviews"),
        "human-review.same_event_pair_reviews",
    )
    top_reviews = _list(report.get("top5_reviews"), "human-review.top5_reviews")
    if event_count != len(event_reviews):
        raise AlphaReleaseEvidenceError(
            "human-review: event raw count does not match records"
        )
    if pair_count != len(pair_reviews):
        raise AlphaReleaseEvidenceError(
            "human-review: pair raw count does not match records"
        )
    if top_total != len(top_reviews):
        raise AlphaReleaseEvidenceError(
            "human-review: Top 5 raw count does not match records"
        )
    event_ids = {
        _review_record(
            value,
            location=f"human-review.event_reviews[{index}]",
            identity_fields=("event_id",),
            allowed_decisions={"approved", "rejected"},
        )[0]
        for index, value in enumerate(event_reviews)
    }
    pair_ids: set[str] = set()
    document_pairs: set[tuple[str, str]] = set()
    for index, value in enumerate(pair_reviews):
        identities = _review_record(
            value,
            location=f"human-review.same_event_pair_reviews[{index}]",
            identity_fields=("pair_id", "left_document_id", "right_document_id"),
            allowed_decisions={True, False},
            boolean_decision=True,
        )
        if identities[1] == identities[2]:
            raise AlphaReleaseEvidenceError(
                "human-review: a same-event pair must contain two documents"
            )
        pair_ids.add(identities[0])
        left_document, right_document = identities[1], identities[2]
        document_pairs.add(
            (left_document, right_document)
            if left_document < right_document
            else (right_document, left_document)
        )
    approved_top = 0
    top_ids: set[tuple[str, str]] = set()
    for index, value in enumerate(top_reviews):
        identities = _review_record(
            value,
            location=f"human-review.top5_reviews[{index}]",
            identity_fields=("edition_id", "event_id"),
            allowed_decisions={"approved"},
        )
        top_ids.add((identities[0], identities[1]))
        approved_top += 1
    if len(event_ids) != len(event_reviews):
        raise AlphaReleaseEvidenceError("human-review: duplicate event review")
    if len(pair_ids) != len(pair_reviews):
        raise AlphaReleaseEvidenceError("human-review: duplicate pair review")
    if len(document_pairs) != len(pair_reviews):
        raise AlphaReleaseEvidenceError(
            "human-review: duplicate document pair review"
        )
    if len(top_ids) != len(top_reviews):
        raise AlphaReleaseEvidenceError("human-review: duplicate Top 5 review")
    gates = [
        _gate(
            "human_review.events",
            event_count >= 60,
            required=60,
            actual=event_count,
        ),
        _gate(
            "human_review.same_event_pairs",
            pair_count >= 120,
            required=120,
            actual=pair_count,
        ),
        _gate(
            "human_review.top5",
            top_rate == 1.0 and approved_top == top_total == top_reviewed,
            required=1.0,
            actual=top_rate,
        ),
    ]
    return {
        "event_review_count": event_count,
        "same_event_pair_review_count": pair_count,
        "top5_human_reviewed_count": top_reviewed,
        "top5_published_count": top_total,
        "top5_human_review_rate": top_rate,
    }, gates


def validate_content_integrity(
    report: Mapping[str, object],
    *,
    expected_revision: str,
    evidence_as_of: datetime,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    _provenance(
        report,
        kind="bside-global-alpha-content-integrity",
        expected_revision=expected_revision,
        location="content-integrity",
        evidence_as_of=evidence_as_of,
    )
    counts = _mapping(report.get("raw_counts"), "content-integrity.raw_counts")
    language_count, event_count, language_rate = _rate_counts(
        numerator=counts.get("original_language_preserved_count"),
        denominator=counts.get("public_event_count"),
        location="content-integrity.original-language",
    )
    url_count, url_denominator, url_rate = _rate_counts(
        numerator=counts.get("official_url_preserved_count"),
        denominator=counts.get("public_event_count"),
        location="content-integrity.official-url",
    )
    provenance_count, provenance_denominator, provenance_rate = _rate_counts(
        numerator=counts.get("title_provenance_labeled_count"),
        denominator=counts.get("public_event_count"),
        location="content-integrity.title-provenance",
    )
    source_title_count, source_title_denominator, source_title_rate = _rate_counts(
        numerator=counts.get("source_title_preserved_count"),
        denominator=counts.get("source_title_event_count"),
        location="content-integrity.source-title",
    )
    generated_metadata_title_count = _int(
        counts.get("generated_metadata_title_count"),
        "generated_metadata_title_count",
        "content-integrity.raw_counts",
    )
    operator_metadata_title_count = _int(
        counts.get("operator_metadata_title_count"),
        "operator_metadata_title_count",
        "content-integrity.raw_counts",
    )
    unknown_title_provenance_count = _int(
        counts.get("unknown_title_provenance_count"),
        "unknown_title_provenance_count",
        "content-integrity.raw_counts",
    )
    scanned = _int(
        counts.get("scanned_response_count"),
        "scanned_response_count",
        "content-integrity.raw_counts",
        minimum=1,
    )
    telegram = _int(
        counts.get("telegram_exposure_count"),
        "telegram_exposure_count",
        "content-integrity.raw_counts",
    )
    internal = _int(
        counts.get("internal_field_exposure_count"),
        "internal_field_exposure_count",
        "content-integrity.raw_counts",
    )
    if url_denominator != event_count:
        raise AlphaReleaseEvidenceError(
            "content-integrity: public event denominators disagree"
        )
    if provenance_denominator != event_count:
        raise AlphaReleaseEvidenceError(
            "content-integrity: title provenance denominator disagrees"
        )
    if (
        source_title_denominator
        + generated_metadata_title_count
        + operator_metadata_title_count
        != provenance_count
        or provenance_count + unknown_title_provenance_count != event_count
    ):
        raise AlphaReleaseEvidenceError(
            "content-integrity: title provenance counts do not partition public events"
        )
    gates = [
        _gate(
            "content.original_language",
            language_rate == 1.0,
            required=1.0,
            actual=language_rate,
        ),
        _gate(
            "content.official_url",
            url_rate == 1.0,
            required=1.0,
            actual=url_rate,
        ),
        _gate(
            "content.title_provenance",
            provenance_rate == 1.0,
            required=1.0,
            actual=provenance_rate,
        ),
        _gate(
            "content.source_title_preservation",
            source_title_rate == 1.0,
            required=1.0,
            actual=source_title_rate,
        ),
        _gate(
            "content.no_unknown_title_provenance",
            unknown_title_provenance_count == 0,
            required=0,
            actual=unknown_title_provenance_count,
        ),
        _gate(
            "content.no_telegram_exposure",
            telegram == 0,
            required=0,
            actual=telegram,
        ),
        _gate(
            "content.no_internal_field_exposure",
            internal == 0,
            required=0,
            actual=internal,
        ),
    ]
    return {
        "public_event_count": event_count,
        "original_language_preserved_count": language_count,
        "official_url_preserved_count": url_count,
        "title_provenance_labeled_count": provenance_count,
        "source_title_event_count": source_title_denominator,
        "source_title_preserved_count": source_title_count,
        "generated_metadata_title_count": generated_metadata_title_count,
        "operator_metadata_title_count": operator_metadata_title_count,
        "unknown_title_provenance_count": unknown_title_provenance_count,
        "scanned_response_count": scanned,
        "telegram_exposure_count": telegram,
        "internal_field_exposure_count": internal,
    }, gates


def _metric(
    metrics: Mapping[str, object],
    name: str,
    *,
    value_field: str,
) -> tuple[float, int]:
    location = f"experience.web_vitals.{name}"
    value = _mapping(metrics.get(name), location)
    return (
        _number(value.get(value_field), value_field, location),
        _int(value.get("sample_count"), "sample_count", location, minimum=1),
    )


def validate_experience(
    report: Mapping[str, object],
    *,
    expected_revision: str,
    evidence_as_of: datetime,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    _provenance(
        report,
        kind="bside-global-alpha-experience",
        expected_revision=expected_revision,
        location="experience",
        evidence_as_of=evidence_as_of,
    )
    viewports = _list(report.get("viewports"), "experience.viewports")
    seen_viewports: set[str] = set()
    viewport_passed = 0
    mobile_top: float | None = None
    for index, value in enumerate(viewports):
        location = f"experience.viewports[{index}]"
        item = _mapping(value, location)
        viewport = _text(item.get("viewport"), "viewport", location)
        if viewport in seen_viewports:
            raise AlphaReleaseEvidenceError(
                f"{location}: duplicate viewport"
            )
        seen_viewports.add(viewport)
        visual_passed = _bool(
            item.get("visual_regression_passed"),
            "visual_regression_passed",
            location,
        )
        serious = _int(
            item.get("axe_serious_count"),
            "axe_serious_count",
            location,
        )
        critical = _int(
            item.get("axe_critical_count"),
            "axe_critical_count",
            location,
        )
        viewport_passed += int(visual_passed and serious == 0 and critical == 0)
        if viewport == "390x844":
            mobile_top = _number(
                item.get("first_important_event_top_px"),
                "first_important_event_top_px",
                location,
            )
    metrics = _mapping(report.get("web_vitals"), "experience.web_vitals")
    lcp, lcp_samples = _metric(metrics, "lcp", value_field="p75_seconds")
    inp, inp_samples = _metric(metrics, "inp", value_field="p75_ms")
    cls, cls_samples = _metric(metrics, "cls", value_field="p75")
    api_records = _list(
        report.get("api_responses"),
        "experience.api_responses",
    )
    route_prefixes: set[str] = set()
    largest_response = 0
    api_ok = bool(api_records)
    for index, value in enumerate(api_records):
        location = f"experience.api_responses[{index}]"
        item = _mapping(value, location)
        route = _text(item.get("route"), "route", location)
        size = _int(item.get("size_bytes"), "size_bytes", location, minimum=1)
        status = _int(item.get("http_status"), "http_status", location, minimum=100)
        largest_response = max(largest_response, size)
        api_ok = api_ok and status == 200 and size <= 250_000
        for prefix in REQUIRED_API_ROUTE_PREFIXES:
            if route.startswith(prefix):
                route_prefixes.add(prefix)
    detection = _mapping(
        report.get("failure_detection_drill"),
        "experience.failure_detection_drill",
    )
    detection_minutes = _number(
        detection.get("detection_minutes"),
        "detection_minutes",
        "experience.failure_detection_drill",
    )
    incident_started_at = _timestamp(
        detection.get("incident_started_at"),
        "incident_started_at",
        "experience.failure_detection_drill",
    )
    detected_at = _timestamp(
        detection.get("detected_at"),
        "detected_at",
        "experience.failure_detection_drill",
    )
    measured_detection_minutes = (
        detected_at - incident_started_at
    ).total_seconds() / 60
    if measured_detection_minutes < 0 or not math.isclose(
        detection_minutes,
        measured_detection_minutes,
        rel_tol=0,
        abs_tol=0.01,
    ):
        raise AlphaReleaseEvidenceError(
            "experience.failure_detection_drill: duration does not match timestamps"
        )
    rollback = _mapping(
        report.get("rollback_drill"),
        "experience.rollback_drill",
    )
    rollback_minutes = _number(
        rollback.get("duration_minutes"),
        "duration_minutes",
        "experience.rollback_drill",
    )
    rollback_succeeded = _bool(
        rollback.get("succeeded"),
        "succeeded",
        "experience.rollback_drill",
    )
    _digest(
        rollback.get("legacy_artifact_sha256"),
        "legacy_artifact_sha256",
        "experience.rollback_drill",
    )
    rollback_started_at = _timestamp(
        rollback.get("started_at"),
        "started_at",
        "experience.rollback_drill",
    )
    rollback_completed_at = _timestamp(
        rollback.get("completed_at"),
        "completed_at",
        "experience.rollback_drill",
    )
    measured_rollback_minutes = (
        rollback_completed_at - rollback_started_at
    ).total_seconds() / 60
    if measured_rollback_minutes < 0 or not math.isclose(
        rollback_minutes,
        measured_rollback_minutes,
        rel_tol=0,
        abs_tol=0.01,
    ):
        raise AlphaReleaseEvidenceError(
            "experience.rollback_drill: duration does not match timestamps"
        )
    gates = [
        _gate(
            "experience.viewports_and_axe",
            seen_viewports == EXPECTED_VIEWPORTS
            and viewport_passed == len(EXPECTED_VIEWPORTS),
            required=sorted(EXPECTED_VIEWPORTS),
            actual={
                "viewports": sorted(seen_viewports),
                "passing": viewport_passed,
            },
        ),
        _gate(
            "experience.mobile_first_event",
            mobile_top is not None and mobile_top <= 300,
            required="<=300px",
            actual=mobile_top,
        ),
        _gate(
            "experience.lcp",
            lcp <= 2.5,
            required="<=2.5s",
            actual={"p75_seconds": lcp, "sample_count": lcp_samples},
        ),
        _gate(
            "experience.inp",
            inp <= 200,
            required="<=200ms",
            actual={"p75_ms": inp, "sample_count": inp_samples},
        ),
        _gate(
            "experience.cls",
            cls <= 0.1,
            required="<=0.1",
            actual={"p75": cls, "sample_count": cls_samples},
        ),
        _gate(
            "experience.api_budget",
            api_ok and route_prefixes == REQUIRED_API_ROUTE_PREFIXES,
            required={
                "maximum_bytes": 250_000,
                "route_prefixes": sorted(REQUIRED_API_ROUTE_PREFIXES),
            },
            actual={
                "maximum_bytes": largest_response,
                "route_prefixes": sorted(route_prefixes),
            },
        ),
        _gate(
            "experience.failure_detection",
            detection_minutes <= 10,
            required="<=10 minutes",
            actual=detection_minutes,
        ),
        _gate(
            "experience.rollback",
            rollback_succeeded and rollback_minutes <= 10,
            required={"succeeded": True, "duration_minutes": "<=10"},
            actual={
                "succeeded": rollback_succeeded,
                "duration_minutes": rollback_minutes,
            },
        ),
    ]
    return {
        "viewports": sorted(seen_viewports),
        "web_vitals": {
            "lcp_p75_seconds": lcp,
            "inp_p75_ms": inp,
            "cls_p75": cls,
        },
        "maximum_api_response_bytes": largest_response,
        "failure_detection_minutes": detection_minutes,
        "rollback_minutes": rollback_minutes,
    }, gates


def validate_approval(
    report: Mapping[str, object],
    *,
    expected_revision: str,
    evidence_as_of: datetime,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    _provenance(
        report,
        kind="bside-global-alpha-release-approval",
        expected_revision=expected_revision,
        location="approval",
        evidence_as_of=evidence_as_of,
    )
    if report.get("release_tier_acknowledged") != "production-alpha":
        raise AlphaReleaseEvidenceError(
            "approval: release_tier_acknowledged must be production-alpha"
        )
    if _bool(
        report.get("ga_certification_claimed"),
        "ga_certification_claimed",
        "approval",
    ):
        raise AlphaReleaseEvidenceError(
            "approval: Production Alpha cannot claim GA certification"
        )
    approvals = _list(report.get("approvals"), "approval.approvals")
    roles: dict[str, bool] = {}
    for index, value in enumerate(approvals):
        location = f"approval.approvals[{index}]"
        item = _mapping(value, location)
        role = _text(item.get("role"), "role", location)
        if role in roles:
            raise AlphaReleaseEvidenceError(f"{location}: duplicate role")
        if item.get("approver_type") != "human":
            raise AlphaReleaseEvidenceError(
                f"{location}: approver_type must be human"
            )
        _text(item.get("approver_reference"), "approver_reference", location)
        _timestamp(item.get("decided_at"), "decided_at", location)
        _digest(item.get("evidence_sha256"), "evidence_sha256", location)
        roles[role] = item.get("decision") == "approved"
    rights = _list(
        report.get("source_right_scope"),
        "approval.source_right_scope",
    )
    right_countries: set[str] = set()
    rights_approved = 0
    total_rights = 0
    invalid_rights = 0
    for index, value in enumerate(rights):
        location = f"approval.source_right_scope[{index}]"
        item = _mapping(value, location)
        country = _text(item.get("country"), "country", location)
        if country in right_countries:
            raise AlphaReleaseEvidenceError(f"{location}: duplicate country")
        right_countries.add(country)
        count = _int(
            item.get("valid_source_right_count"),
            "valid_source_right_count",
            location,
            minimum=1,
        )
        invalid = _int(
            item.get("invalid_source_right_count"),
            "invalid_source_right_count",
            location,
        )
        total_rights += count
        invalid_rights += invalid
        rights_approved += int(
            item.get("decision") == "approved" and invalid == 0
        )
    gates = [
        _gate(
            "approval.human_oversight",
            roles.get("oversight") is True,
            required=True,
            actual=roles.get("oversight"),
        ),
        _gate(
            "approval.source_rights",
            roles.get("source-rights") is True
            and right_countries == set(REQUIRED_ALPHA_COUNTRIES)
            and rights_approved == len(REQUIRED_ALPHA_COUNTRIES)
            and invalid_rights == 0,
            required={
                "role": "source-rights",
                "countries": sorted(REQUIRED_ALPHA_COUNTRIES),
                "invalid_source_right_count": 0,
            },
            actual={
                "role_approved": roles.get("source-rights"),
                "countries": sorted(right_countries),
                "approved_country_count": rights_approved,
                "invalid_source_right_count": invalid_rights,
            },
        ),
    ]
    return {
        "roles": roles,
        "source_right_country_count": len(right_countries),
        "valid_source_right_count": total_rights,
        "invalid_source_right_count": invalid_rights,
    }, gates


def build_alpha_release_report(
    observations: Sequence[Mapping[str, object]],
    pages_artifact_identity: Mapping[str, object],
    connector_idempotency: Mapping[str, object],
    human_review: Mapping[str, object],
    content_integrity: Mapping[str, object],
    experience: Mapping[str, object],
    approval: Mapping[str, object],
    *,
    expected_revision: str,
    evidence_as_of: datetime,
) -> dict[str, object]:
    revision = _revision(expected_revision, "expected_revision", "release-gate")
    if evidence_as_of.tzinfo is None:
        raise AlphaReleaseEvidenceError(
            "release-gate: evidence_as_of must include a timezone"
        )
    as_of = evidence_as_of.astimezone(timezone.utc)
    try:
        pages_binding = validate_pages_artifact_binding(
            pages_artifact_identity,
            expected_revision=revision,
        )
    except PagesArtifactIdentityError as exc:
        raise AlphaReleaseEvidenceError(
            f"pages-artifact-identity: {exc}"
        ) from exc
    content_identity = pages_binding["content_identity"]
    if not isinstance(content_identity, dict):
        raise AlphaReleaseEvidenceError(
            "pages-artifact-identity: content_identity is invalid"
        )
    expected_terminal_content = content_identity.get("terminal")
    if not isinstance(expected_terminal_content, dict):
        raise AlphaReleaseEvidenceError(
            "pages-artifact-identity: terminal content identity is invalid"
        )
    observation_summary, observation_gates = validate_observations(
        observations,
        expected_revision=revision,
        expected_terminal_content=expected_terminal_content,
    )
    connector_summary, connector_gates = validate_connector_idempotency(
        connector_idempotency,
        expected_revision=revision,
        evidence_as_of=as_of,
    )
    review_summary, review_gates = validate_human_review(
        human_review,
        expected_revision=revision,
        evidence_as_of=as_of,
    )
    content_summary, content_gates = validate_content_integrity(
        content_integrity,
        expected_revision=revision,
        evidence_as_of=as_of,
    )
    experience_summary, experience_gates = validate_experience(
        experience,
        expected_revision=revision,
        evidence_as_of=as_of,
    )
    approval_summary, approval_gates = validate_approval(
        approval,
        expected_revision=revision,
        evidence_as_of=as_of,
    )
    gates = (
        observation_gates
        + connector_gates
        + review_gates
        + content_gates
        + experience_gates
        + approval_gates
    )
    failed = [str(gate["name"]) for gate in gates if gate["passed"] is not True]
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": REPORT_KIND,
        "release_tier": "production-alpha",
        "ga_certification_claimed": False,
        "quality_statement": (
            "This report verifies the bounded Production Alpha launch contract; "
            "it does not certify final recall or same-event precision."
        ),
        "code_revision": revision,
        "evidence_as_of": as_of.isoformat(),
        "evidence_sha256": {
            "observations": _sha256_json(list(observations)),
            "pages_artifact_identity": _sha256_json(pages_binding),
            "connector_idempotency": _sha256_json(connector_idempotency),
            "human_review": _sha256_json(human_review),
            "content_integrity": _sha256_json(content_integrity),
            "experience": _sha256_json(experience),
            "approval": _sha256_json(approval),
        },
        "observation": observation_summary,
        "pages_artifact": pages_binding,
        "connector_idempotency": connector_summary,
        "human_review": review_summary,
        "content_integrity": content_summary,
        "experience": experience_summary,
        "approval": approval_summary,
        "gates": gates,
        "failed_gates": failed,
        "release_gate_passed": not failed,
    }


def materialize_input_bundle(
    encoded: str,
    *,
    output_dir: Path,
    expected_revision: str,
    automated_evidence_path: Path,
) -> None:
    if not encoded.strip():
        raise AlphaReleaseEvidenceError("input bundle secret is empty")
    if len(encoded.encode("utf-8")) > 48_000:
        raise AlphaReleaseEvidenceError(
            "encoded input bundle exceeds the 48KB secret budget"
        )
    try:
        compressed = base64.b64decode(encoded, validate=True)
    except (ValueError, TypeError) as exc:
        raise AlphaReleaseEvidenceError("input bundle is not valid base64") from exc
    try:
        inflater = zlib.decompressobj(16 + zlib.MAX_WBITS)
        raw = inflater.decompress(compressed, 2_000_001)
        if len(raw) > 2_000_000 or inflater.unconsumed_tail:
            raise AlphaReleaseEvidenceError(
                "input bundle gzip exceeds the 2MB decompressed budget"
            )
        raw += inflater.flush(2_000_001 - len(raw))
    except zlib.error as exc:
        raise AlphaReleaseEvidenceError(
            "input bundle is not valid gzip"
        ) from exc
    if (
        len(raw) > 2_000_000
        or not inflater.eof
        or inflater.unused_data
    ):
        raise AlphaReleaseEvidenceError(
            "input bundle gzip is oversized, truncated, concatenated, or has trailing data"
        )
    try:
        bundle = _mapping(json.loads(raw.decode("utf-8")), "input-bundle")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AlphaReleaseEvidenceError("input bundle is not valid UTF-8 JSON") from exc
    if bundle.get("schema_version") != SCHEMA_VERSION:
        raise AlphaReleaseEvidenceError(
            f"input-bundle: schema_version must be {SCHEMA_VERSION}"
        )
    if bundle.get("kind") != INPUT_BUNDLE_KIND:
        raise AlphaReleaseEvidenceError("input-bundle: unexpected kind")
    revision = _revision(
        bundle.get("code_revision"),
        "code_revision",
        "input-bundle",
    )
    if revision != _revision(
        expected_revision,
        "expected_revision",
        "input-bundle",
    ):
        raise AlphaReleaseEvidenceError("input-bundle: revision mismatch")
    files = _mapping(bundle.get("files"), "input-bundle.files")
    if set(files) != set(INPUT_FILENAMES):
        raise AlphaReleaseEvidenceError(
            "input-bundle: exact evidence file set is required"
        )

    try:
        automated_response = _mapping(
            json.loads(automated_evidence_path.read_text(encoding="utf-8")),
            "automated-evidence-response",
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AlphaReleaseEvidenceError(
            "automated-evidence-response: invalid UTF-8 JSON"
        ) from exc
    if set(automated_response) != {"ok", "data", "api_version"}:
        raise AlphaReleaseEvidenceError(
            "automated-evidence-response: exact v2 response fields are required"
        )
    if (
        automated_response.get("ok") is not True
        or automated_response.get("api_version") != "v2"
    ):
        raise AlphaReleaseEvidenceError(
            "automated-evidence-response: successful v2 response required"
        )
    automated = _mapping(
        automated_response.get("data"),
        "automated-evidence-response.data",
    )
    automated_collected_at = _provenance(
        automated,
        kind=AUTOMATED_EVIDENCE_KIND,
        expected_revision=revision,
        location="automated-evidence",
        evidence_as_of=datetime.now(timezone.utc) + timedelta(minutes=1),
    )
    if automated.get("evidence_source") != "production_database_export":
        raise AlphaReleaseEvidenceError(
            "automated-evidence: production database export required"
        )
    coverage = _list(
        automated.get("connector_coverage"),
        "automated-evidence.connector_coverage",
    )
    coverage_by_family: dict[str, Mapping[str, object]] = {}
    expected_coverage_fields = {
        "connector_family",
        "country",
        "coverage_started_at",
        "coverage_ended_at",
        "successful_window_count",
        "failed_window_count",
        "completed_windows",
    }
    for index, raw_coverage in enumerate(coverage):
        location = f"automated-evidence.connector_coverage[{index}]"
        item = _mapping(raw_coverage, location)
        if set(item) != expected_coverage_fields:
            raise AlphaReleaseEvidenceError(
                f"{location}: exact database coverage fields are required"
            )
        family = _text(item.get("connector_family"), "connector_family", location)
        country = _text(item.get("country"), "country", location)
        if family in coverage_by_family:
            raise AlphaReleaseEvidenceError(
                f"{location}: duplicate connector_family"
            )
        if EXPECTED_CONNECTORS.get(family) != country:
            raise AlphaReleaseEvidenceError(
                f"{location}: unexpected connector family or country"
            )
        coverage_by_family[family] = item
    if set(coverage_by_family) != set(EXPECTED_CONNECTORS):
        raise AlphaReleaseEvidenceError(
            "automated-evidence: exact official connector coverage is required"
        )

    protected_connector = dict(
        _mapping(
            files["connector-idempotency.json"],
            "input-bundle.files.connector-idempotency.json",
        )
    )
    protected_items = _list(
        protected_connector.get("connectors"),
        "input-bundle.files.connector-idempotency.json.connectors",
    )
    merged_items: list[dict[str, object]] = []
    seen_protected: set[str] = set()
    coverage_fields = expected_coverage_fields - {"connector_family", "country"}
    for index, raw_item in enumerate(protected_items):
        location = (
            "input-bundle.files.connector-idempotency.json"
            f".connectors[{index}]"
        )
        item = dict(_mapping(raw_item, location))
        family = _text(item.get("connector_family"), "connector_family", location)
        country = _text(item.get("country"), "country", location)
        if family in seen_protected or EXPECTED_CONNECTORS.get(family) != country:
            raise AlphaReleaseEvidenceError(
                f"{location}: unexpected or duplicate connector"
            )
        seen_protected.add(family)
        database_coverage = coverage_by_family[family]
        for field in coverage_fields:
            item[field] = database_coverage[field]
        merged_items.append(item)
    if seen_protected != set(EXPECTED_CONNECTORS):
        raise AlphaReleaseEvidenceError(
            "input-bundle: exact protected replay connector set is required"
        )
    protected_connector.update(
        {
            "schema_version": SCHEMA_VERSION,
            "kind": "bside-global-alpha-connector-idempotency",
            "environment": "production",
            "evidence_source": (
                "production_database_export_with_protected_replay_audit"
            ),
            "is_synthetic": False,
            "code_revision": revision,
            "collected_at": automated_collected_at.isoformat(),
            "connectors": merged_items,
        }
    )
    validate_connector_idempotency(
        protected_connector,
        expected_revision=revision,
        evidence_as_of=automated_collected_at,
    )

    content_integrity = dict(
        _mapping(
            automated.get("content_integrity"),
            "automated-evidence.content_integrity",
        )
    )
    if content_integrity.get("evidence_source") != "production_database_export":
        raise AlphaReleaseEvidenceError(
            "automated-evidence.content_integrity: database export required"
        )
    validate_content_integrity(
        content_integrity,
        expected_revision=revision,
        evidence_as_of=automated_collected_at,
    )
    materialized_files = dict(files)
    materialized_files["connector-idempotency.json"] = protected_connector
    materialized_files["content-integrity.json"] = content_integrity

    output_dir.mkdir(parents=True, exist_ok=False)
    for filename in INPUT_FILENAMES:
        value = _mapping(
            materialized_files[filename],
            f"materialized-inputs.{filename}",
        )
        destination = output_dir / filename
        destination.write_text(
            json.dumps(
                value,
                ensure_ascii=False,
                sort_keys=True,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
            newline="\n",
        )


def compile_observation_archives(
    *,
    archive_dir: Path,
    manifest_path: Path,
    output_path: Path,
    expected_revision: str,
) -> int:
    revision = _revision(
        expected_revision,
        "expected_revision",
        "observation-segments",
    )
    try:
        decoded_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AlphaReleaseEvidenceError(
            "observation-segment-archive-manifest: invalid JSON"
        ) from exc
    manifest = _mapping(
        decoded_manifest,
        "observation-segment-archive-manifest",
    )
    if (
        _int(
            manifest.get("schema_version"),
            "schema_version",
            "observation-segment-archive-manifest",
        )
        != SEGMENT_SCHEMA_VERSION
        or manifest.get("kind")
        != "bside-global-alpha-observation-segment-archive-manifest"
    ):
        raise AlphaReleaseEvidenceError(
            "observation-segment-archive-manifest: unsupported contract"
        )
    chain_id = _text(
        manifest.get("chain_id"),
        "chain_id",
        "observation-segment-archive-manifest",
    )
    if not chain_id.isdigit() or chain_id.startswith("0"):
        raise AlphaReleaseEvidenceError(
            "observation-segment-archive-manifest: invalid chain_id"
        )
    if _revision(
        manifest.get("code_revision"),
        "code_revision",
        "observation-segment-archive-manifest",
    ) != revision:
        raise AlphaReleaseEvidenceError(
            "observation-segment-archive-manifest: revision mismatch"
        )
    segment_entries = _list(
        manifest.get("segments"),
        "observation-segment-archive-manifest.segments",
    )
    if len(segment_entries) != SEGMENT_COUNT:
        raise AlphaReleaseEvidenceError(
            "observation-segment-archive-manifest: exactly five segments required"
        )

    records: list[dict[str, object]] = []
    artifact_ids: set[str] = set()
    run_ids: set[str] = set()
    previous_run_id: str | None = None
    previous_artifact_digest: str | None = None
    candidate_started_at: datetime | None = None
    candidate_ends_at: datetime | None = None
    cadence_anchor: datetime | None = None
    for index, raw_entry in enumerate(segment_entries, start=1):
        location = f"observation-segment-archive-manifest.segments[{index - 1}]"
        entry = _mapping(raw_entry, location)
        segment_index = _int(
            entry.get("segment_index"),
            "segment_index",
            location,
        )
        if segment_index != index:
            raise AlphaReleaseEvidenceError(
                f"{location}: segment order is incomplete or overlapping"
            )
        if entry.get("chain_id") != chain_id:
            raise AlphaReleaseEvidenceError(
                f"{location}: chain_id mismatch"
            )
        if _revision(
            entry.get("code_revision"),
            "code_revision",
            location,
        ) != revision:
            raise AlphaReleaseEvidenceError(
                f"{location}: revision mismatch"
            )
        if (
            entry.get("run_conclusion") != "success"
            or entry.get("run_event") != "workflow_dispatch"
            or entry.get("workflow_path")
            != ".github/workflows/global-alpha-observation-chain.yml"
            or _int(entry.get("run_attempt"), "run_attempt", location) != 1
        ):
            raise AlphaReleaseEvidenceError(
                f"{location}: successful first-attempt chain workflow required"
            )
        run_id = _text(entry.get("run_id"), "run_id", location)
        if (
            not run_id.isdigit()
            or run_id.startswith("0")
            or run_id in run_ids
            or (index == 1 and run_id != chain_id)
        ):
            raise AlphaReleaseEvidenceError(
                f"{location}: invalid or duplicate run_id"
            )
        run_ids.add(run_id)
        artifact_id = _text(entry.get("artifact_id"), "artifact_id", location)
        if artifact_id in artifact_ids:
            raise AlphaReleaseEvidenceError(
                f"{location}: duplicate artifact_id"
            )
        artifact_ids.add(artifact_id)
        expected_artifact_name = (
            f"global-alpha-observation-segment-{chain_id}-{index}"
        )
        if entry.get("artifact_name") != expected_artifact_name:
            raise AlphaReleaseEvidenceError(
                f"{location}: artifact_name mismatch"
            )
        archive_name = _text(entry.get("archive_name"), "archive_name", location)
        if Path(archive_name).name != archive_name or not archive_name.endswith(".zip"):
            raise AlphaReleaseEvidenceError(
                f"{location}: archive_name must be a plain ZIP filename"
            )
        expected_digest = _digest(
            str(entry.get("artifact_digest") or "").removeprefix("sha256:"),
            "artifact_digest",
            location,
        )
        archive = archive_dir / archive_name
        if not archive.is_file():
            raise AlphaReleaseEvidenceError(f"{location}: archive is missing")
        actual_digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        if actual_digest != expected_digest:
            raise AlphaReleaseEvidenceError(
                f"{location}: archive digest mismatch"
            )
        try:
            with zipfile.ZipFile(archive) as bundle:
                members = [item for item in bundle.infolist() if not item.is_dir()]
                if len(members) != 2:
                    raise AlphaReleaseEvidenceError(
                        f"{location}: segment artifact must contain exactly two files"
                    )
                by_name = {item.filename: item for item in members}
                if set(by_name) != {
                    "observations.jsonl",
                    "segment-manifest.json",
                }:
                    raise AlphaReleaseEvidenceError(
                        f"{location}: unexpected segment archive members"
                    )
                for member in members:
                    member_path = Path(member.filename)
                    file_type = (member.external_attr >> 16) & 0o170000
                    maximum_size = (
                        50_000_000
                        if member.filename == "observations.jsonl"
                        else 100_000
                    )
                    if (
                        member_path.name != member.filename
                        or member.file_size < 2
                        or member.file_size > maximum_size
                        or file_type == 0o120000
                    ):
                        raise AlphaReleaseEvidenceError(
                            f"{location}: unsafe segment archive member"
                        )
                observation_bytes = bundle.read(by_name["observations.jsonl"])
                segment_decoded = json.loads(
                    bundle.read(by_name["segment-manifest.json"]).decode("utf-8")
                )
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, zipfile.BadZipFile) as exc:
            raise AlphaReleaseEvidenceError(
                f"{location}: invalid observation segment archive"
            ) from exc
        segment = _mapping(segment_decoded, f"{location}.segment-manifest")
        if (
            _int(
                segment.get("schema_version"),
                "schema_version",
                f"{location}.segment-manifest",
            )
            != SEGMENT_SCHEMA_VERSION
            or segment.get("kind") != SEGMENT_KIND
            or segment.get("status") != "complete"
            or segment.get("error_code") is not None
            or segment.get("chain_id") != chain_id
            or _int(
                segment.get("segment_index"),
                "segment_index",
                f"{location}.segment-manifest",
            )
            != index
            or _int(
                segment.get("segment_count"),
                "segment_count",
                f"{location}.segment-manifest",
            )
            != SEGMENT_COUNT
            or _revision(
                segment.get("code_revision"),
                "code_revision",
                f"{location}.segment-manifest",
            )
            != revision
            or segment.get("run_id") != run_id
            or _int(
                segment.get("run_attempt"),
                "run_attempt",
                f"{location}.segment-manifest",
            )
            != 1
        ):
            raise AlphaReleaseEvidenceError(
                f"{location}: segment manifest identity mismatch"
            )
        expected_predecessor_run = previous_run_id if index > 1 else None
        expected_predecessor_digest = (
            previous_artifact_digest if index > 1 else None
        )
        if (
            segment.get("predecessor_run_id") != expected_predecessor_run
            or segment.get("predecessor_artifact_digest")
            != expected_predecessor_digest
        ):
            raise AlphaReleaseEvidenceError(
                f"{location}: predecessor artifact chain mismatch"
            )
        first_slot, last_slot = segment_slot_bounds(index)
        expected_count = SEGMENT_COUNTS[index - 1]
        if (
            _int(
                segment.get("first_slot_index"),
                "first_slot_index",
                f"{location}.segment-manifest",
            )
            != first_slot
            or _int(
                segment.get("last_slot_index"),
                "last_slot_index",
                f"{location}.segment-manifest",
            )
            != last_slot
            or _int(
                segment.get("expected_record_count"),
                "expected_record_count",
                f"{location}.segment-manifest",
            )
            != expected_count
            or _int(
                segment.get("record_count"),
                "record_count",
                f"{location}.segment-manifest",
            )
            != expected_count
        ):
            raise AlphaReleaseEvidenceError(
                f"{location}: incomplete or overlapping segment slots"
            )
        expected_observations_digest = _digest(
            segment.get("observations_sha256"),
            "observations_sha256",
            f"{location}.segment-manifest",
        )
        if hashlib.sha256(observation_bytes).hexdigest() != expected_observations_digest:
            raise AlphaReleaseEvidenceError(
                f"{location}: observations digest mismatch"
            )
        try:
            observation_text = observation_bytes.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise AlphaReleaseEvidenceError(
                f"{location}: observations are not UTF-8"
            ) from exc
        if not observation_text.endswith("\n"):
            raise AlphaReleaseEvidenceError(
                f"{location}: observations JSONL must end with a newline"
            )
        segment_records: list[dict[str, object]] = []
        for line_index, line in enumerate(observation_text.splitlines()):
            if not line:
                raise AlphaReleaseEvidenceError(
                    f"{location}: blank observation JSONL line"
                )
            try:
                decoded_record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise AlphaReleaseEvidenceError(
                    f"{location}: invalid observation JSONL"
                ) from exc
            segment_records.append(
                dict(
                    _mapping(
                        decoded_record,
                        f"{location}.observations[{line_index}]",
                    )
                )
            )
        if (
            len(segment_records) != expected_count
            or canonical_jsonl(segment_records) != observation_bytes
        ):
            raise AlphaReleaseEvidenceError(
                f"{location}: non-canonical or incomplete observations"
            )
        segment_start = _timestamp(
            segment.get("candidate_started_at"),
            "candidate_started_at",
            f"{location}.segment-manifest",
        )
        segment_end = _timestamp(
            segment.get("candidate_ends_at"),
            "candidate_ends_at",
            f"{location}.segment-manifest",
        )
        segment_anchor = _timestamp(
            segment.get("cadence_anchor"),
            "cadence_anchor",
            f"{location}.segment-manifest",
        )
        segment_completed_at = _timestamp(
            segment.get("completed_at"),
            "completed_at",
            f"{location}.segment-manifest",
        )
        if (
            segment_end - segment_start != timedelta(hours=24)
            or not segment_start
            <= segment_anchor
            <= segment_start + timedelta(minutes=5)
            or segment_anchor
            + timedelta(
                seconds=(TOTAL_OBSERVATIONS - 1)
                * OBSERVATION_INTERVAL_SECONDS
            )
            > segment_end
        ):
            raise AlphaReleaseEvidenceError(
                f"{location}: candidate window or cadence anchor invalid"
            )
        if index == SEGMENT_COUNT and not (
            segment_end
            <= segment_completed_at
            <= segment_end
            + timedelta(seconds=MAX_SLOT_LATENESS_SECONDS)
        ):
            raise AlphaReleaseEvidenceError(
                f"{location}: final segment was not sealed at the candidate end"
            )
        if candidate_started_at is None:
            candidate_started_at = segment_start
            candidate_ends_at = segment_end
            cadence_anchor = segment_anchor
        elif (
            segment_start != candidate_started_at
            or segment_end != candidate_ends_at
            or segment_anchor != cadence_anchor
        ):
            raise AlphaReleaseEvidenceError(
                f"{location}: candidate window changed between segments"
            )
        for offset, record in enumerate(segment_records):
            record_location = f"{location}.observations[{offset}]"
            slot_index = first_slot + offset
            chain = _mapping(
                record.get("observation_chain"),
                f"{record_location}.observation_chain",
            )
            if (
                _int(
                    chain.get("schema_version"),
                    "schema_version",
                    f"{record_location}.observation_chain",
                )
                != SEGMENT_SCHEMA_VERSION
                or chain.get("chain_id") != chain_id
                or _int(
                    chain.get("segment_index"),
                    "segment_index",
                    f"{record_location}.observation_chain",
                )
                != index
                or _int(
                    chain.get("segment_count"),
                    "segment_count",
                    f"{record_location}.observation_chain",
                )
                != SEGMENT_COUNT
                or _int(
                    chain.get("slot_index"),
                    "slot_index",
                    f"{record_location}.observation_chain",
                )
                != slot_index
                or chain.get("run_id") != run_id
                or _int(
                    chain.get("run_attempt"),
                    "run_attempt",
                    f"{record_location}.observation_chain",
                )
                != 1
                or _timestamp(
                    chain.get("candidate_started_at"),
                    "candidate_started_at",
                    f"{record_location}.observation_chain",
                )
                != segment_start
                or _timestamp(
                    chain.get("candidate_ends_at"),
                    "candidate_ends_at",
                    f"{record_location}.observation_chain",
                )
                != segment_end
                or _timestamp(
                    chain.get("cadence_anchor"),
                    "cadence_anchor",
                    f"{record_location}.observation_chain",
                )
                != segment_anchor
            ):
                raise AlphaReleaseEvidenceError(
                    f"{record_location}: observation chain metadata mismatch"
                )
            observed_at = _timestamp(
                record.get("observed_at"),
                "observed_at",
                record_location,
            )
            expected_at = segment_anchor + timedelta(
                seconds=slot_index * OBSERVATION_INTERVAL_SECONDS
            )
            lateness = (observed_at - expected_at).total_seconds()
            if (
                lateness < -1
                or lateness > MAX_SLOT_LATENESS_SECONDS
                or observed_at > segment_end
                or _revision(
                    record.get("workflow_revision"),
                    "workflow_revision",
                    record_location,
                )
                != revision
            ):
                raise AlphaReleaseEvidenceError(
                    f"{record_location}: observation slot timing or revision mismatch"
                )
        if (
            segment.get("first_observed_at")
            != segment_records[0].get("observed_at")
            or segment.get("last_observed_at")
            != segment_records[-1].get("observed_at")
        ):
            raise AlphaReleaseEvidenceError(
                f"{location}: observed boundary metadata mismatch"
            )
        records.extend(segment_records)
        previous_run_id = run_id
        previous_artifact_digest = "sha256:" + expected_digest

    if len(records) != TOTAL_OBSERVATIONS:
        raise AlphaReleaseEvidenceError(
            "observation-segments: complete 288-record chain required"
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        "".join(
            json.dumps(
                record,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
            for record in records
        ),
        encoding="utf-8",
        newline="\n",
    )
    return len(records)


def _write_report(path: Path, report: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Evaluate the separate BSIDE Global Terminal Production Alpha gate",
    )
    subparsers = parser.add_subparsers(dest="command")
    evaluate = subparsers.add_parser("evaluate")
    evaluate.add_argument("--observations", type=Path, required=True)
    evaluate.add_argument("--pages-artifact-identity", type=Path, required=True)
    evaluate.add_argument("--connector-idempotency", type=Path, required=True)
    evaluate.add_argument("--human-review", type=Path, required=True)
    evaluate.add_argument("--content-integrity", type=Path, required=True)
    evaluate.add_argument("--experience", type=Path, required=True)
    evaluate.add_argument("--approval", type=Path, required=True)
    evaluate.add_argument("--expected-revision", required=True)
    evaluate.add_argument("--evidence-as-of", required=True)
    evaluate.add_argument("--output", type=Path, required=True)
    materialize = subparsers.add_parser("materialize-inputs")
    materialize.add_argument("--encoded-env", required=True)
    materialize.add_argument("--output-dir", type=Path, required=True)
    materialize.add_argument("--expected-revision", required=True)
    materialize.add_argument(
        "--automated-evidence",
        type=Path,
        required=True,
    )
    compile_observations = subparsers.add_parser("compile-observations")
    compile_observations.add_argument("--archive-dir", type=Path, required=True)
    compile_observations.add_argument("--manifest", type=Path, required=True)
    compile_observations.add_argument("--output", type=Path, required=True)
    compile_observations.add_argument("--expected-revision", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    if args.command is None:
        args = build_arg_parser().parse_args(["evaluate", *(argv or sys.argv[1:])])
    try:
        if args.command == "materialize-inputs":
            materialize_input_bundle(
                os.environ.get(args.encoded_env, ""),
                output_dir=args.output_dir,
                expected_revision=args.expected_revision,
                automated_evidence_path=args.automated_evidence,
            )
            print(
                json.dumps(
                    {
                        "status": "global-alpha-inputs-materialized",
                        "files": list(INPUT_FILENAMES),
                    },
                    ensure_ascii=False,
                )
            )
            return 0
        if args.command == "compile-observations":
            count = compile_observation_archives(
                archive_dir=args.archive_dir,
                manifest_path=args.manifest,
                output_path=args.output,
                expected_revision=args.expected_revision,
            )
            print(
                json.dumps(
                    {
                        "status": "global-alpha-observations-compiled",
                        "observation_count": count,
                    },
                    ensure_ascii=False,
                )
            )
            return 0
        as_of = _timestamp(
            args.evidence_as_of,
            "evidence_as_of",
            "release-gate",
        )
        report = build_alpha_release_report(
            _load_observations(args.observations),
            _load_json(args.pages_artifact_identity),
            _load_json(args.connector_idempotency),
            _load_json(args.human_review),
            _load_json(args.content_integrity),
            _load_json(args.experience),
            _load_json(args.approval),
            expected_revision=args.expected_revision,
            evidence_as_of=as_of,
        )
        _write_report(args.output, report)
    except AlphaReleaseEvidenceError as exc:
        print(
            json.dumps(
                {
                    "status": "invalid-production-alpha-evidence",
                    "error": str(exc),
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["release_gate_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
