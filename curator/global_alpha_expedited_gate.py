from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Mapping, Sequence

from .global_alpha_pages_identity import (
    PagesArtifactIdentityError,
    validate_terminal_content_identity,
)
from .global_alpha_release_gate import (
    AlphaReleaseEvidenceError,
    _bool,
    _date,
    _digest,
    _gate,
    _int,
    _list,
    _mapping,
    _number,
    _provenance,
    _receipt_count_partition,
    _revision,
    _sha256_json,
    _text,
    _timestamp,
    _v2_api_base,
    validate_content_integrity,
    validate_experience,
)


SCHEMA_VERSION = 1
INPUT_KIND = "bside-global-production-alpha-expedited-inputs"
REPORT_KIND = "bside-global-production-alpha-expedited-release-report"
RELEASE_CHANNEL = "production_alpha_early_access"
CONNECTOR_KIND = "bside-global-alpha-expedited-connector-receipts-v2"
SOURCE_READINESS_KIND = "bside-global-alpha-expedited-source-readiness"
HUMAN_REVIEW_KIND = "bside-global-alpha-human-review"
APPROVAL_KIND = "bside-global-alpha-expedited-release-approval"
LEGACY_ARCHIVE_KIND = "bside-global-alpha-expedited-legacy-archive"

EXPECTED_CONNECTORS = {
    "dart": "KR",
    "sec-edgar": "US",
}
EXPECTED_SOURCES = {
    "KR": ("official:dart", "connector:kr:dart", "market-wide"),
    "US": ("official:sec-edgar", "connector:us:sec-edgar", "market-wide"),
    "JP": ("official:edinet", "connector:jp:edinet", "link-only"),
    "GB": (
        "official:companies-house",
        "connector:gb:companies-house",
        "link-only",
    ),
    "CA": ("official:ca-issuer-ir", "connector:ca:issuer-ir", "link-only"),
    "AU": ("official:asic-register", "connector:au:asic-register", "link-only"),
}
ACTIVE_COUNTRIES = frozenset(("KR", "US", "CA", "AU"))
UNAVAILABLE_COUNTRIES = frozenset(("JP", "GB"))
DART_DRIFT_RELEASE_GATE_POLICY = (
    "stable-public-payload-source-count-diagnostic-v1"
)
MINIMUM_REVIEWED_EVENTS = 20
MINIMUM_REVIEWED_PAIRS = 40
REQUIRED_TOP5 = 5
MINIMUM_OBSERVATIONS = 7
MINIMUM_OBSERVATION_SPAN_MINUTES = 30.0
MINIMUM_INTERVAL_MINUTES = 2.0
MAXIMUM_INTERVAL_MINUTES = 8.0
MAXIMUM_LAST_OBSERVATION_AGE_MINUTES = 60.0
WAIVER_FIRST_DATE = date(2026, 5, 1)
WAIVER_LAST_DATE = date(2026, 7, 28)
WAIVER_DAY_COUNT = 89
WAIVER_CUTOFF = datetime(2026, 7, 28, 20, 45, tzinfo=timezone.utc)
WAIVER_EXCEPTION_ID = "production-alpha-early-access-89-day-2026-07-28"
EXPEDITED_LEGACY_MANIFEST_KIND = (
    "bside-expedited-legacy-feed-compatibility"
)
EXPEDITED_LEGACY_MANIFEST_SCHEMA_VERSION = 1
MAXIMUM_HUMAN_REVIEW_AGE = timedelta(hours=72)
KST = timezone(timedelta(hours=9))
DAILY_RECOVERY_READY_AT = time(5, 45)
FORBIDDEN_PRODUCTION_MARKERS = ("fixture", "synthetic", "sample", "test")
REQUIRED_PROBES = frozenset(
    (
        "public_root",
        "health",
        "release_state",
        "deployed_build",
        "terminal_app",
        "terminal_styles",
        "sources_status",
        "live",
        "search",
    )
)
ROOT_FIELDS = frozenset(
    (
        "schema_version",
        "kind",
        "environment",
        "evidence_source",
        "is_synthetic",
        "code_revision",
        "collected_at",
        "evidence_as_of",
        "release_channel",
        "observations",
        "connector_receipts",
        "source_readiness",
        "human_review",
        "content_integrity",
        "experience",
        "approval",
        "legacy_archive",
    )
)
WINDOW_FIELDS = frozenset(
    (
        "window_start",
        "window_end_exclusive",
        "status",
        "code_revision",
        "raw_count",
        "filtered_out_count",
        "accepted_count",
        "acknowledged_count",
        "payload_sha256",
        "receipt_sha256",
        "idempotency_key",
        "ingest_id",
        "idempotent",
        "replay_verified",
    )
)
HUMAN_REVIEW_SECTION_FIELDS = (
    "ground_truth_source",
    "ai_generated_ground_truth",
    "human_attestation",
    "raw_counts",
    "event_reviews",
    "same_event_pair_reviews",
    "top5_reviews",
)
APPROVAL_SECTION_FIELDS = (
    "release_tier_acknowledged",
    "ga_certification_claimed",
    "expedited_waiver_acknowledged",
    "evidence_binding",
    "approvals",
)


# The standard and expedited evaluators intentionally share one evidence error
# type so callers can handle either protected gate without branching.
ExpeditedAlphaEvidenceError = AlphaReleaseEvidenceError


def _production_identifier(value: object, field: str, location: str) -> str:
    result = _text(value, field, location)
    if any(marker in result.casefold() for marker in FORBIDDEN_PRODUCTION_MARKERS):
        raise ExpeditedAlphaEvidenceError(
            f"{location}: {field} is not production provenance"
        )
    return result


def _prefixed_digest(value: object, field: str, location: str) -> str:
    result = _text(value, field, location).casefold()
    if not result.startswith("sha256:"):
        raise ExpeditedAlphaEvidenceError(
            f"{location}: {field} must be a sha256-prefixed digest"
        )
    return _digest(result.removeprefix("sha256:"), field, location)


def _evidence_artifact_identity(
    report: Mapping[str, object],
    *,
    location: str,
) -> dict[str, object]:
    return {
        "artifact_id": _int(
            report.get("artifact_id"),
            "artifact_id",
            location,
            minimum=1,
        ),
        "artifact_name": _production_identifier(
            report.get("artifact_name"),
            "artifact_name",
            location,
        ),
        "artifact_sha256": _digest(
            report.get("artifact_sha256"),
            "artifact_sha256",
            location,
        ),
    }


def _section_digest(
    report: Mapping[str, object],
    *,
    fields: Sequence[str],
    location: str,
) -> str:
    payload = {field: report.get(field) for field in fields}
    calculated = _sha256_json(payload)
    claimed = _digest(
        report.get("section_sha256"),
        "section_sha256",
        location,
    )
    if claimed != calculated:
        raise ExpeditedAlphaEvidenceError(
            f"{location}: section_sha256 does not match the protected records"
        )
    return calculated


def _validate_human_timestamp(
    value: object,
    *,
    field: str,
    location: str,
    evidence_as_of: datetime,
) -> datetime:
    timestamp = _timestamp(value, field, location)
    if timestamp > evidence_as_of + timedelta(minutes=1):
        raise ExpeditedAlphaEvidenceError(
            f"{location}: {field} is after evidence_as_of"
        )
    if evidence_as_of - timestamp > MAXIMUM_HUMAN_REVIEW_AGE:
        raise ExpeditedAlphaEvidenceError(
            f"{location}: {field} is older than 72 hours"
        )
    return timestamp


def _latest_recovery_date(evidence_as_of: datetime) -> date:
    local = evidence_as_of.astimezone(KST)
    if local.timetz().replace(tzinfo=None) < DAILY_RECOVERY_READY_AT:
        return local.date() - timedelta(days=1)
    return local.date()


def _validate_root(
    bundle: Mapping[str, object],
    *,
    expected_revision: str,
) -> tuple[str, datetime]:
    if set(bundle) != ROOT_FIELDS:
        missing = sorted(ROOT_FIELDS - set(bundle))
        extra = sorted(set(bundle) - ROOT_FIELDS)
        raise ExpeditedAlphaEvidenceError(
            "expedited-inputs: fields do not match the input contract "
            f"(missing={missing}, extra={extra})"
        )
    revision = _revision(
        expected_revision,
        "expected_revision",
        "expedited-release-gate",
    )
    as_of = _timestamp(
        bundle.get("evidence_as_of"),
        "evidence_as_of",
        "expedited-inputs",
    )
    _provenance(
        bundle,
        kind=INPUT_KIND,
        expected_revision=revision,
        location="expedited-inputs",
        evidence_as_of=as_of,
    )
    if bundle.get("release_channel") != RELEASE_CHANNEL:
        raise ExpeditedAlphaEvidenceError(
            f"expedited-inputs: release_channel must be {RELEASE_CHANNEL}"
        )
    return revision, as_of


def _validate_probe(
    value: object,
    *,
    location: str,
) -> bool:
    probe = _mapping(value, location)
    return (
        probe.get("http_status") == 200
        and probe.get("transport_succeeded") is True
        and probe.get("contract_valid") is True
    )


def _validate_observation_sources(
    value: object,
    *,
    location: str,
) -> bool:
    sources = _list(value, location)
    seen: set[str] = set()
    valid = len(sources) == len(EXPECTED_SOURCES)
    for index, raw_source in enumerate(sources):
        item_location = f"{location}[{index}]"
        source = _mapping(raw_source, item_location)
        country = _text(source.get("country"), "country", item_location)
        if country in seen or country not in EXPECTED_SOURCES:
            raise ExpeditedAlphaEvidenceError(
                f"{item_location}: duplicate or unexpected country"
            )
        seen.add(country)
        _, connector_id, coverage = EXPECTED_SOURCES[country]
        raw_count = _int(source.get("raw_count"), "raw_count", item_location)
        acknowledged = _int(
            source.get("acknowledged_count"),
            "acknowledged_count",
            item_location,
        )
        if acknowledged > raw_count:
            raise ExpeditedAlphaEvidenceError(
                f"{item_location}: acknowledged_count exceeds raw_count"
            )
        if country in {"CA", "AU"} and acknowledged != raw_count:
            raise ExpeditedAlphaEvidenceError(
                f"{item_location}: link-only raw and acknowledged counts differ"
            )
        valid = valid and source.get("connector_id") == connector_id
        valid = valid and source.get("coverage_mode") == coverage
        if country in ACTIVE_COUNTRIES:
            valid = valid and (
                source.get("public_status") == "active"
                and source.get("public_ready") is True
                and raw_count >= 1
                and acknowledged >= 1
            )
        else:
            valid = valid and (
                source.get("public_status") == "coverage_unavailable"
                and source.get("public_ready") is False
                and raw_count == 0
                and acknowledged == 0
            )
    return valid and seen == set(EXPECTED_SOURCES)


def validate_expedited_observations(
    records: Sequence[Mapping[str, object]],
    *,
    expected_revision: str,
    evidence_as_of: datetime,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    if not records:
        raise ExpeditedAlphaEvidenceError(
            "observations: production observations are required"
        )
    parsed: list[tuple[datetime, Mapping[str, object]]] = []
    observation_ids: set[str] = set()
    observed_values: set[datetime] = set()
    terminal_identities: set[str] = set()
    api_bases: set[str] = set()
    release_versions: set[int] = set()
    healthy = 0
    probe_complete = 0
    coverage_complete = 0

    for index, record in enumerate(records):
        location = f"observations[{index}]"
        if record.get("schema_version") != SCHEMA_VERSION:
            raise ExpeditedAlphaEvidenceError(
                f"{location}: schema_version must be {SCHEMA_VERSION}"
            )
        observation_id = _text(
            record.get("observation_id"),
            "observation_id",
            location,
        )
        if (
            not observation_id.startswith("global-alpha:")
            or observation_id in observation_ids
        ):
            raise ExpeditedAlphaEvidenceError(
                f"{location}: invalid or duplicate observation_id"
            )
        observation_ids.add(observation_id)
        observed_at = _timestamp(
            record.get("observed_at"),
            "observed_at",
            location,
        )
        if observed_at in observed_values:
            raise ExpeditedAlphaEvidenceError(
                f"{location}: duplicate observed_at"
            )
        observed_values.add(observed_at)
        parsed.append((observed_at, record))
        for field in (
            "workflow_revision",
            "deployed_build_sha",
            "api_code_revision",
        ):
            if _revision(record.get(field), field, location) != expected_revision:
                raise ExpeditedAlphaEvidenceError(
                    f"{location}: {field} does not match expected revision"
                )
        api_bases.add(
            _v2_api_base(
                record.get("deployed_api_base"),
                "deployed_api_base",
                location,
            )
        )
        if (
            record.get("pipeline_mode") != "shadow"
            or record.get("web_surface") != "governance-preview"
            or record.get("release_state") != "preview"
        ):
            raise ExpeditedAlphaEvidenceError(
                f"{location}: observation must be shadow/preview"
            )
        release_versions.add(
            _int(
                record.get("release_state_version"),
                "release_state_version",
                location,
                minimum=1,
            )
        )
        try:
            terminal = validate_terminal_content_identity(
                record.get("terminal_content")
            )
        except PagesArtifactIdentityError as exc:
            raise ExpeditedAlphaEvidenceError(
                f"{location}: terminal content identity is invalid"
            ) from exc
        terminal_identities.add(_sha256_json(terminal))
        reasons = _list(record.get("reasons"), f"{location}.reasons")
        warnings = _list(record.get("warnings"), f"{location}.warnings")
        healthy += int(
            record.get("status") == "healthy" and not reasons and not warnings
        )
        probes = _mapping(record.get("probes"), f"{location}.probes")
        probes_ok = REQUIRED_PROBES.issubset(probes)
        probes_ok = probes_ok and all(
            _validate_probe(
                probes.get(name),
                location=f"{location}.probes.{name}",
            )
            for name in REQUIRED_PROBES
        )
        event_detail = probes.get("event_detail")
        if event_detail is not None:
            event_probe = _mapping(
                event_detail,
                f"{location}.probes.event_detail",
            )
            event_ok = (
                event_probe.get("skipped") is True
                and event_probe.get("reason") == "no_live_event_available"
            ) or _validate_probe(
                event_probe,
                location=f"{location}.probes.event_detail",
            )
            probes_ok = probes_ok and event_ok
        probe_complete += int(probes_ok)
        coverage_complete += int(
            _validate_observation_sources(
                record.get("sources"),
                location=f"{location}.sources",
            )
        )

    if len(api_bases) != 1:
        raise ExpeditedAlphaEvidenceError(
            "observations: deployed API base changed during observation"
        )
    if len(release_versions) != 1:
        raise ExpeditedAlphaEvidenceError(
            "observations: release state version changed during observation"
        )
    if len(terminal_identities) != 1:
        raise ExpeditedAlphaEvidenceError(
            "observations: terminal bytes changed during observation"
        )
    parsed.sort(key=lambda pair: pair[0])
    timestamps = [pair[0] for pair in parsed]
    intervals = [
        (right - left).total_seconds() / 60
        for left, right in zip(timestamps, timestamps[1:])
    ]
    span_minutes = (timestamps[-1] - timestamps[0]).total_seconds() / 60
    minimum_interval = min(intervals, default=0.0)
    maximum_interval = max(intervals, default=0.0)
    cadence_ok = bool(intervals) and (
        minimum_interval >= MINIMUM_INTERVAL_MINUTES
        and maximum_interval <= MAXIMUM_INTERVAL_MINUTES
    )
    last_age_minutes = (
        evidence_as_of - timestamps[-1]
    ).total_seconds() / 60
    freshness_ok = -1.0 <= last_age_minutes <= MAXIMUM_LAST_OBSERVATION_AGE_MINUTES
    count = len(records)
    gates = [
        _gate(
            "expedited_observation.minimum_window",
            count >= MINIMUM_OBSERVATIONS
            and span_minutes >= MINIMUM_OBSERVATION_SPAN_MINUTES
            and freshness_ok,
            required={
                "minimum_samples": MINIMUM_OBSERVATIONS,
                "minimum_span_minutes": MINIMUM_OBSERVATION_SPAN_MINUTES,
                "maximum_last_observation_age_minutes": (
                    MAXIMUM_LAST_OBSERVATION_AGE_MINUTES
                ),
            },
            actual={
                "sample_count": count,
                "span_minutes": span_minutes,
                "last_observation_age_minutes": last_age_minutes,
            },
        ),
        _gate(
            "expedited_observation.cadence",
            cadence_ok,
            required={"minimum_minutes": 2, "maximum_minutes": 8},
            actual={
                "minimum_minutes": minimum_interval,
                "maximum_minutes": maximum_interval,
            },
        ),
        _gate(
            "expedited_observation.immutable_candidate",
            len(api_bases) == len(release_versions) == len(terminal_identities) == 1,
            required=True,
            actual=True,
        ),
        _gate(
            "expedited_observation.no_incident",
            healthy == count,
            required=count,
            actual=healthy,
        ),
        _gate(
            "expedited_observation.probes",
            probe_complete == count,
            required=count,
            actual=probe_complete,
        ),
        _gate(
            "expedited_observation.coverage",
            coverage_complete == count,
            required=count,
            actual=coverage_complete,
        ),
    ]
    return {
        "started_at": timestamps[0].isoformat(),
        "ended_at": timestamps[-1].isoformat(),
        "sample_count": count,
        "span_minutes": span_minutes,
        "minimum_interval_minutes": minimum_interval,
        "maximum_interval_minutes": maximum_interval,
        "last_observation_age_minutes": last_age_minutes,
        "api_base": next(iter(api_bases)),
        "release_state_version": next(iter(release_versions)),
        "terminal_content_sha256": next(iter(terminal_identities)),
    }, gates


def _validate_connector_run(
    value: object,
    *,
    location: str,
    mode: str,
    expected_revision: str,
) -> tuple[dict[date, dict[str, object]], dict[str, object]]:
    run = _mapping(value, location)
    run_id = _int(run.get("run_id"), "run_id", location, minimum=1)
    artifact_id = _int(
        run.get("artifact_id"),
        "artifact_id",
        location,
        minimum=1,
    )
    artifact_name = _production_identifier(
        run.get("artifact_name"),
        "artifact_name",
        location,
    )
    artifact_sha256 = _digest(
        run.get("artifact_sha256"),
        "artifact_sha256",
        location,
    )
    if run.get("status") != "succeeded":
        raise ExpeditedAlphaEvidenceError(
            f"{location}: status must be succeeded"
        )
    if _revision(
        run.get("code_revision"),
        "code_revision",
        location,
    ) != expected_revision:
        raise ExpeditedAlphaEvidenceError(
            f"{location}: code_revision does not match expected revision"
        )
    windows = _list(run.get("windows"), f"{location}.windows")
    if len(windows) != 30:
        raise ExpeditedAlphaEvidenceError(
            f"{location}: exactly 30 daily windows are required"
        )
    by_date: dict[date, dict[str, object]] = {}
    ranges: list[tuple[date, date]] = []
    receipt_digests: set[str] = set()
    accepted_total = 0
    raw_total = 0
    acknowledged_total = 0
    for index, raw_window in enumerate(windows):
        window_location = f"{location}.windows[{index}]"
        window = _mapping(raw_window, window_location)
        if set(window) != WINDOW_FIELDS:
            raise ExpeditedAlphaEvidenceError(
                f"{window_location}: fields do not match the receipt contract"
            )
        start = _date(
            window.get("window_start"),
            "window_start",
            window_location,
        )
        end = _date(
            window.get("window_end_exclusive"),
            "window_end_exclusive",
            window_location,
        )
        if end - start != timedelta(days=1):
            raise ExpeditedAlphaEvidenceError(
                f"{window_location}: window must cover exactly one day"
            )
        if start in by_date:
            raise ExpeditedAlphaEvidenceError(
                f"{window_location}: duplicate daily window"
            )
        if window.get("status") != "complete":
            raise ExpeditedAlphaEvidenceError(
                f"{window_location}: status must be complete"
            )
        if _revision(
            window.get("code_revision"),
            "code_revision",
            window_location,
        ) != expected_revision:
            raise ExpeditedAlphaEvidenceError(
                f"{window_location}: code_revision mismatch"
            )
        raw, filtered_out, accepted, acknowledged = _receipt_count_partition(
            window,
            location=window_location,
        )
        payload_digest = _digest(
            window.get("payload_sha256"),
            "payload_sha256",
            window_location,
        )
        receipt_digest = _digest(
            window.get("receipt_sha256"),
            "receipt_sha256",
            window_location,
        )
        if receipt_digest in receipt_digests:
            raise ExpeditedAlphaEvidenceError(
                f"{window_location}: duplicate receipt digest"
            )
        receipt_digests.add(receipt_digest)
        idempotency_key = _production_identifier(
            window.get("idempotency_key"),
            "idempotency_key",
            window_location,
        )
        ingest_id = _production_identifier(
            window.get("ingest_id"),
            "ingest_id",
            window_location,
        )
        idempotent = _bool(
            window.get("idempotent"),
            "idempotent",
            window_location,
        )
        replay_verified = _bool(
            window.get("replay_verified"),
            "replay_verified",
            window_location,
        )
        expected_idempotent = mode == "replay"
        if idempotent is not expected_idempotent:
            raise ExpeditedAlphaEvidenceError(
                f"{window_location}: idempotent does not match {mode} mode"
            )
        if replay_verified is not expected_idempotent:
            raise ExpeditedAlphaEvidenceError(
                f"{window_location}: replay_verified does not match {mode} mode"
            )
        by_date[start] = {
            "window_start": start.isoformat(),
            "window_end_exclusive": end.isoformat(),
            "raw_count": raw,
            "filtered_out_count": filtered_out,
            "accepted_count": accepted,
            "acknowledged_count": acknowledged,
            "payload_sha256": payload_digest,
            "idempotency_key": idempotency_key,
            "ingest_id": ingest_id,
        }
        ranges.append((start, end))
        raw_total += raw
        accepted_total += accepted
        acknowledged_total += acknowledged
    for previous, current in zip(ranges, ranges[1:]):
        if previous[1] != current[0]:
            raise ExpeditedAlphaEvidenceError(
                f"{location}: windows contain a gap, overlap, or are out of order"
            )
    if accepted_total < 1:
        raise ExpeditedAlphaEvidenceError(
            f"{location}: production run accepted no official records"
        )
    return by_date, {
        "run_id": run_id,
        "artifact_id": artifact_id,
        "artifact_name": artifact_name,
        "artifact_sha256": artifact_sha256,
        "window_count": len(windows),
        "raw_count": raw_total,
        "accepted_count": accepted_total,
        "acknowledged_count": acknowledged_total,
        "started_on": ranges[0][0].isoformat(),
        "ended_on_exclusive": ranges[-1][1].isoformat(),
    }


def validate_connector_receipts(
    report: Mapping[str, object],
    *,
    expected_revision: str,
    evidence_as_of: datetime,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    _provenance(
        report,
        kind=CONNECTOR_KIND,
        expected_revision=expected_revision,
        location="connector-receipts",
        evidence_as_of=evidence_as_of,
    )
    connectors = _list(
        report.get("connectors"),
        "connector-receipts.connectors",
    )
    seen: set[str] = set()
    summaries: list[dict[str, object]] = []
    replay_verified = 0
    current_coverage = 0
    frozen_replay_verified = 0
    for index, raw_connector in enumerate(connectors):
        location = f"connector-receipts.connectors[{index}]"
        connector = _mapping(raw_connector, location)
        family = _text(
            connector.get("connector_family"),
            "connector_family",
            location,
        )
        country = _text(connector.get("country"), "country", location)
        if family in seen:
            raise ExpeditedAlphaEvidenceError(
                f"{location}: duplicate connector_family"
            )
        seen.add(family)
        apply_by_date, apply_summary = _validate_connector_run(
            connector.get("apply_run"),
            location=f"{location}.apply_run",
            mode="apply",
            expected_revision=expected_revision,
        )
        replay_by_date, replay_summary = _validate_connector_run(
            connector.get("replay_run"),
            location=f"{location}.replay_run",
            mode="replay",
            expected_revision=expected_revision,
        )
        producer_identity_fields = (
            "run_id",
            "artifact_id",
            "artifact_name",
            "artifact_sha256",
        )
        reused_identity_fields = [
            field
            for field in producer_identity_fields
            if apply_summary[field] == replay_summary[field]
        ]
        if reused_identity_fields:
            raise ExpeditedAlphaEvidenceError(
                f"{location}: apply and replay reused producer identity fields "
                f"{reused_identity_fields}"
            )
        if apply_by_date.keys() != replay_by_date.keys():
            raise ExpeditedAlphaEvidenceError(
                f"{location}: apply and replay window dates differ"
            )
        matches = 0
        for window_date in apply_by_date:
            if apply_by_date[window_date] != replay_by_date[window_date]:
                raise ExpeditedAlphaEvidenceError(
                    f"{location}: apply/replay payload differs on {window_date}"
                )
            matches += 1
        if family == "dart":
            apply_run = _mapping(
                connector.get("apply_run"),
                f"{location}.apply_run",
            )
            replay_run = _mapping(
                connector.get("replay_run"),
                f"{location}.replay_run",
            )
            apply_execution_windows = _int(
                apply_run.get("execution_window_count"),
                "execution_window_count",
                f"{location}.apply_run",
                minimum=0,
            )
            apply_preexisting_windows = _int(
                apply_run.get("preexisting_window_count"),
                "preexisting_window_count",
                f"{location}.apply_run",
                minimum=0,
            )
            apply_evidenced_windows = _int(
                apply_run.get("evidenced_window_count"),
                "evidenced_window_count",
                f"{location}.apply_run",
                minimum=1,
            )
            replay_execution_windows = _int(
                replay_run.get("execution_window_count"),
                "execution_window_count",
                f"{location}.replay_run",
                minimum=1,
            )
            replay_preexisting_windows = _int(
                replay_run.get("preexisting_window_count"),
                "preexisting_window_count",
                f"{location}.replay_run",
                minimum=0,
            )
            replay_evidenced_windows = _int(
                replay_run.get("evidenced_window_count"),
                "evidenced_window_count",
                f"{location}.replay_run",
                minimum=1,
            )
            if (
                apply_execution_windows > 30
                or apply_preexisting_windows > 30
                or apply_execution_windows + apply_preexisting_windows != 30
                or apply_evidenced_windows != 30
                or replay_execution_windows != 30
                or replay_preexisting_windows != 30
                or replay_evidenced_windows != 30
            ):
                raise ExpeditedAlphaEvidenceError(
                    f"{location}: DART execution and authoritative frozen "
                    "30-window evidence are inconsistent"
                )
            apply_summary.update(
                {
                    "execution_window_count": apply_execution_windows,
                    "preexisting_window_count": apply_preexisting_windows,
                    "evidenced_window_count": apply_evidenced_windows,
                }
            )
            replay_summary.update(
                {
                    "execution_window_count": replay_execution_windows,
                    "preexisting_window_count": replay_preexisting_windows,
                    "evidenced_window_count": replay_evidenced_windows,
                }
            )
            apply_manifest = _digest(
                apply_run.get("frozen_bundle_manifest_sha256"),
                "frozen_bundle_manifest_sha256",
                f"{location}.apply_run",
            )
            replay_manifest = _digest(
                replay_run.get("frozen_bundle_manifest_sha256"),
                "frozen_bundle_manifest_sha256",
                f"{location}.replay_run",
            )
            _digest(
                replay_run.get("frozen_artifact_binding_sha256"),
                "frozen_artifact_binding_sha256",
                f"{location}.replay_run",
            )
            if apply_manifest != replay_manifest:
                raise ExpeditedAlphaEvidenceError(
                    f"{location}: apply/replay frozen bundle manifest differs"
                )
            if _bool(
                replay_run.get("source_network_accessed"),
                "source_network_accessed",
                f"{location}.replay_run",
            ):
                raise ExpeditedAlphaEvidenceError(
                    f"{location}: frozen replay accessed the source network"
                )
            drift = _mapping(
                replay_run.get("fresh_drift_probe"),
                f"{location}.replay_run.fresh_drift_probe",
            )
            drift_location = f"{location}.replay_run.fresh_drift_probe"
            drift_status = _text(
                drift.get("status"),
                "status",
                drift_location,
            )
            drift_policy = _text(
                drift.get("release_gate_policy"),
                "release_gate_policy",
                drift_location,
            )
            drift_release_gate_matched = _bool(
                drift.get("release_gate_matched"),
                "release_gate_matched",
                drift_location,
            )
            diagnostic_only_window_count = _int(
                drift.get("diagnostic_only_window_count"),
                "diagnostic_only_window_count",
                drift_location,
                minimum=0,
            )
            blocking_drift_window_count = _int(
                drift.get("blocking_drift_window_count"),
                "blocking_drift_window_count",
                drift_location,
                minimum=0,
            )
            if (
                drift_status not in {"matched", "drift_detected"}
                or drift_policy != DART_DRIFT_RELEASE_GATE_POLICY
                or not drift_release_gate_matched
                or diagnostic_only_window_count > 30
                or blocking_drift_window_count != 0
                or (
                    drift_status == "matched"
                    and diagnostic_only_window_count != 0
                )
                or (
                    drift_status == "drift_detected"
                    and diagnostic_only_window_count < 1
                )
                or not _bool(drift.get("read_only"), "read_only", drift_location)
                or _bool(
                    drift.get("governance_write_attempted"),
                    "governance_write_attempted",
                    drift_location,
                )
                or _bool(
                    drift.get("checkpoint_write_attempted"),
                    "checkpoint_write_attempted",
                    drift_location,
                )
                or not _bool(
                    drift.get("quota_ledger_write_attempted"),
                    "quota_ledger_write_attempted",
                    drift_location,
                )
            ):
                raise ExpeditedAlphaEvidenceError(
                    f"{location}: fresh DART drift probe is not release-gate safe "
                    "and read-only"
                )
            drift_sha256 = _digest(
                drift.get("sha256"),
                "sha256",
                drift_location,
            )
            replay_summary["fresh_drift_probe"] = {
                "status": drift_status,
                "release_gate_policy": drift_policy,
                "release_gate_matched": drift_release_gate_matched,
                "diagnostic_only_window_count": diagnostic_only_window_count,
                "blocking_drift_window_count": blocking_drift_window_count,
                "sha256": drift_sha256,
                "read_only": True,
                "governance_write_attempted": False,
                "checkpoint_write_attempted": False,
                "quota_ledger_write_attempted": True,
            }
            frozen_replay_verified += 1
        last_end = date.fromisoformat(
            str(apply_summary["ended_on_exclusive"])
        )
        age = evidence_as_of - datetime.combine(
            last_end,
            datetime.min.time(),
            tzinfo=timezone.utc,
        )
        coverage_ok = (
            timedelta(0) <= age <= timedelta(hours=24, minutes=1)
            and EXPECTED_CONNECTORS.get(family) == country
        )
        replay_verified += int(matches == 30)
        current_coverage += int(coverage_ok)
        summaries.append(
            {
                "connector_family": family,
                "country": country,
                "matched_window_count": matches,
                "coverage_current": coverage_ok,
                "apply_run": apply_summary,
                "replay_run": replay_summary,
            }
        )
    exact_set = seen == set(EXPECTED_CONNECTORS)
    gates = [
        _gate(
            "expedited_connectors.exact_set",
            exact_set,
            required=sorted(EXPECTED_CONNECTORS),
            actual=sorted(seen),
        ),
        _gate(
            "expedited_connectors.apply_replay",
            exact_set and replay_verified == len(EXPECTED_CONNECTORS),
            required={"connectors": 2, "matched_windows_each": 30},
            actual={
                "connectors": len(seen),
                "fully_matched_connectors": replay_verified,
            },
        ),
        _gate(
            "expedited_connectors.current_30_day_horizon",
            exact_set and current_coverage == len(EXPECTED_CONNECTORS),
            required=len(EXPECTED_CONNECTORS),
            actual=current_coverage,
        ),
        _gate(
            "expedited_connectors.dart_frozen_replay",
            frozen_replay_verified == 1,
            required=1,
            actual=frozen_replay_verified,
        ),
    ]
    return {"connectors": summaries}, gates


def validate_source_readiness(
    report: Mapping[str, object],
    *,
    expected_revision: str,
    evidence_as_of: datetime,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    _provenance(
        report,
        kind=SOURCE_READINESS_KIND,
        expected_revision=expected_revision,
        location="source-readiness",
        evidence_as_of=evidence_as_of,
    )
    values = _list(report.get("sources"), "source-readiness.sources")
    seen: set[str] = set()
    valid_rights = 0
    active_ready = 0
    unavailable_ready = 0
    summaries: list[dict[str, object]] = []
    for index, raw_source in enumerate(values):
        location = f"source-readiness.sources[{index}]"
        source = _mapping(raw_source, location)
        country = _text(source.get("country"), "country", location)
        if country in seen or country not in EXPECTED_SOURCES:
            raise ExpeditedAlphaEvidenceError(
                f"{location}: duplicate or unexpected country"
            )
        seen.add(country)
        expected_right, expected_connector, expected_coverage = (
            EXPECTED_SOURCES[country]
        )
        source_right_valid = _bool(
            source.get("source_right_valid"),
            "source_right_valid",
            location,
        )
        raw_count = _int(source.get("raw_count"), "raw_count", location)
        acknowledged = _int(
            source.get("acknowledged_count"),
            "acknowledged_count",
            location,
        )
        if acknowledged > raw_count:
            raise ExpeditedAlphaEvidenceError(
                f"{location}: acknowledged_count exceeds raw_count"
            )
        if country in {"CA", "AU"} and acknowledged != raw_count:
            raise ExpeditedAlphaEvidenceError(
                f"{location}: link-only raw and acknowledged counts differ"
            )
        identity_ok = (
            source.get("source_right_id") == expected_right
            and source.get("connector_id") == expected_connector
            and source.get("coverage_mode") == expected_coverage
        )
        right_ok = source_right_valid and identity_ok
        valid_rights += int(right_ok)
        if country in ACTIVE_COUNTRIES:
            ready = (
                right_ok
                and source.get("public_status") == "active"
                and source.get("public_ready") is True
                and raw_count >= 1
                and acknowledged >= 1
            )
            active_ready += int(ready)
        else:
            ready = (
                right_ok
                and source.get("public_status") == "coverage_unavailable"
                and source.get("public_ready") is False
                and raw_count == acknowledged == 0
            )
            unavailable_ready += int(ready)
        summaries.append(
            {
                "country": country,
                "coverage_mode": source.get("coverage_mode"),
                "public_status": source.get("public_status"),
                "public_ready": source.get("public_ready"),
                "source_right_valid": source_right_valid,
                "raw_count": raw_count,
                "acknowledged_count": acknowledged,
            }
        )
    exact = seen == set(EXPECTED_SOURCES)
    gates = [
        _gate(
            "expedited_sources.exact_six_country_set",
            exact,
            required=sorted(EXPECTED_SOURCES),
            actual=sorted(seen),
        ),
        _gate(
            "expedited_sources.valid_rights",
            exact and valid_rights == len(EXPECTED_SOURCES),
            required=len(EXPECTED_SOURCES),
            actual=valid_rights,
        ),
        _gate(
            "expedited_sources.active_ready",
            active_ready == len(ACTIVE_COUNTRIES),
            required=sorted(ACTIVE_COUNTRIES),
            actual=active_ready,
        ),
        _gate(
            "expedited_sources.jp_gb_unavailable",
            unavailable_ready == len(UNAVAILABLE_COUNTRIES),
            required=sorted(UNAVAILABLE_COUNTRIES),
            actual=unavailable_ready,
        ),
    ]
    return {"sources": summaries}, gates


def _validate_human_record(
    value: object,
    *,
    location: str,
    identity_fields: Sequence[str],
    allowed_decisions: set[object],
    evidence_as_of: datetime,
    boolean_decision: bool = False,
) -> tuple[str, ...]:
    item = _mapping(value, location)
    identities = tuple(
        _text(item.get(field), field, location) for field in identity_fields
    )
    decision = item.get("decision")
    if (
        (boolean_decision and not isinstance(decision, bool))
        or decision not in allowed_decisions
    ):
        raise ExpeditedAlphaEvidenceError(
            f"{location}: invalid review decision"
        )
    if item.get("reviewer_type") != "human":
        raise ExpeditedAlphaEvidenceError(
            f"{location}: reviewer_type must be human"
        )
    _production_identifier(
        item.get("reviewer_reference"),
        "reviewer_reference",
        location,
    )
    _validate_human_timestamp(
        item.get("reviewed_at"),
        field="reviewed_at",
        location=location,
        evidence_as_of=evidence_as_of,
    )
    return identities


def validate_expedited_human_review(
    report: Mapping[str, object],
    *,
    expected_revision: str,
    evidence_as_of: datetime,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    _provenance(
        report,
        kind=HUMAN_REVIEW_KIND,
        expected_revision=expected_revision,
        location="human-review",
        evidence_as_of=evidence_as_of,
    )
    if (
        report.get("ground_truth_source") != "human"
        or _bool(
            report.get("ai_generated_ground_truth"),
            "ai_generated_ground_truth",
            "human-review",
        )
        or not _bool(
            report.get("human_attestation"),
            "human_attestation",
            "human-review",
        )
    ):
        raise ExpeditedAlphaEvidenceError(
            "human-review: human-only attested ground truth is required"
        )
    artifact = _evidence_artifact_identity(
        report,
        location="human-review",
    )
    section_sha256 = _section_digest(
        report,
        fields=HUMAN_REVIEW_SECTION_FIELDS,
        location="human-review",
    )
    counts = _mapping(report.get("raw_counts"), "human-review.raw_counts")
    event_count = _int(
        counts.get("event_review_count"),
        "event_review_count",
        "human-review.raw_counts",
    )
    pair_count = _int(
        counts.get("same_event_pair_review_count"),
        "same_event_pair_review_count",
        "human-review.raw_counts",
    )
    top_reviewed = _int(
        counts.get("top5_human_reviewed_count"),
        "top5_human_reviewed_count",
        "human-review.raw_counts",
    )
    top_published = _int(
        counts.get("top5_published_count"),
        "top5_published_count",
        "human-review.raw_counts",
    )
    events = _list(report.get("event_reviews"), "human-review.event_reviews")
    pairs = _list(
        report.get("same_event_pair_reviews"),
        "human-review.same_event_pair_reviews",
    )
    top5 = _list(report.get("top5_reviews"), "human-review.top5_reviews")
    if (
        event_count != len(events)
        or pair_count != len(pairs)
        or top_published != len(top5)
    ):
        raise ExpeditedAlphaEvidenceError(
            "human-review: raw counts do not match review records"
        )
    event_decisions: dict[str, object] = {}
    for index, value in enumerate(events):
        event_id = _validate_human_record(
            value,
            location=f"human-review.event_reviews[{index}]",
            identity_fields=("event_id",),
            allowed_decisions={"approved", "rejected"},
            evidence_as_of=evidence_as_of,
        )[0]
        if event_id in event_decisions:
            raise ExpeditedAlphaEvidenceError(
                "human-review: duplicate event review"
            )
        event_decisions[event_id] = _mapping(
            value,
            f"human-review.event_reviews[{index}]",
        ).get("decision")
    event_ids = set(event_decisions)
    pair_ids: set[str] = set()
    document_pairs: set[tuple[str, str]] = set()
    for index, value in enumerate(pairs):
        identities = _validate_human_record(
            value,
            location=f"human-review.same_event_pair_reviews[{index}]",
            identity_fields=("pair_id", "left_document_id", "right_document_id"),
            allowed_decisions={True, False},
            evidence_as_of=evidence_as_of,
            boolean_decision=True,
        )
        if identities[1] == identities[2]:
            raise ExpeditedAlphaEvidenceError(
                "human-review: a pair must contain two documents"
            )
        pair_ids.add(identities[0])
        left, right = identities[1], identities[2]
        document_pairs.add(
            (left, right) if left < right else (right, left)
        )
    top_ids: set[tuple[str, str]] = set()
    approved_top = 0
    for index, value in enumerate(top5):
        identities = _validate_human_record(
            value,
            location=f"human-review.top5_reviews[{index}]",
            identity_fields=("edition_id", "event_id"),
            allowed_decisions={"approved"},
            evidence_as_of=evidence_as_of,
        )
        top_item = _mapping(
            value,
            f"human-review.top5_reviews[{index}]",
        )
        official_evidence_count = _int(
            top_item.get("official_evidence_count"),
            "official_evidence_count",
            f"human-review.top5_reviews[{index}]",
            minimum=1,
        )
        public_eligible = _bool(
            top_item.get("public_eligible"),
            "public_eligible",
            f"human-review.top5_reviews[{index}]",
        )
        _digest(
            top_item.get("event_evidence_sha256"),
            "event_evidence_sha256",
            f"human-review.top5_reviews[{index}]",
        )
        if not public_eligible or official_evidence_count < 1:
            raise ExpeditedAlphaEvidenceError(
                "human-review: every Top 5 item must be public eligible "
                "and have official evidence"
            )
        top_ids.add((identities[0], identities[1]))
        approved_top += 1
    if len(event_ids) != len(events):
        raise ExpeditedAlphaEvidenceError(
            "human-review: duplicate event review"
        )
    if len(pair_ids) != len(pairs) or len(document_pairs) != len(pairs):
        raise ExpeditedAlphaEvidenceError(
            "human-review: duplicate same-event pair review"
        )
    if len(top_ids) != len(top5):
        raise ExpeditedAlphaEvidenceError(
            "human-review: duplicate Top 5 review"
        )
    if any(
        event_decisions.get(event_id) != "approved"
        for _, event_id in top_ids
    ):
        raise ExpeditedAlphaEvidenceError(
            "human-review: every Top 5 event must have an approved event review"
        )
    gates = [
        _gate(
            "expedited_human_review.events",
            event_count >= MINIMUM_REVIEWED_EVENTS,
            required=MINIMUM_REVIEWED_EVENTS,
            actual=event_count,
        ),
        _gate(
            "expedited_human_review.same_event_pairs",
            pair_count >= MINIMUM_REVIEWED_PAIRS,
            required=MINIMUM_REVIEWED_PAIRS,
            actual=pair_count,
        ),
        _gate(
            "expedited_human_review.top5",
            top_reviewed
            == top_published
            == approved_top
            == REQUIRED_TOP5,
            required=REQUIRED_TOP5,
            actual={
                "published": top_published,
                "human_reviewed": top_reviewed,
                "approved": approved_top,
            },
        ),
    ]
    return {
        **artifact,
        "section_sha256": section_sha256,
        "event_review_count": event_count,
        "same_event_pair_review_count": pair_count,
        "top5_published_count": top_published,
        "top5_human_reviewed_count": top_reviewed,
    }, gates


def validate_expedited_approval(
    report: Mapping[str, object],
    *,
    expected_revision: str,
    evidence_as_of: datetime,
    required_evidence_binding: Mapping[str, object],
) -> tuple[dict[str, object], list[dict[str, object]]]:
    _provenance(
        report,
        kind=APPROVAL_KIND,
        expected_revision=expected_revision,
        location="approval",
        evidence_as_of=evidence_as_of,
    )
    if report.get("release_tier_acknowledged") != (
        "production-alpha-early-access"
    ):
        raise ExpeditedAlphaEvidenceError(
            "approval: Early Access release tier must be acknowledged"
        )
    if _bool(
        report.get("ga_certification_claimed"),
        "ga_certification_claimed",
        "approval",
    ):
        raise ExpeditedAlphaEvidenceError(
            "approval: expedited Alpha cannot claim GA certification"
        )
    if not _bool(
        report.get("expedited_waiver_acknowledged"),
        "expedited_waiver_acknowledged",
        "approval",
    ):
        raise ExpeditedAlphaEvidenceError(
            "approval: expedited waiver acknowledgement is required"
        )
    artifact = _evidence_artifact_identity(
        report,
        location="approval",
    )
    binding = _mapping(
        report.get("evidence_binding"),
        "approval.evidence_binding",
    )
    if dict(binding) != dict(required_evidence_binding):
        raise ExpeditedAlphaEvidenceError(
            "approval: evidence_binding does not match the evaluated sections"
        )
    binding_sha256 = _digest(
        binding.get("binding_sha256"),
        "binding_sha256",
        "approval.evidence_binding",
    )
    section_sha256 = _section_digest(
        report,
        fields=APPROVAL_SECTION_FIELDS,
        location="approval",
    )
    values = _list(report.get("approvals"), "approval.approvals")
    roles: dict[str, bool] = {}
    for index, raw_approval in enumerate(values):
        location = f"approval.approvals[{index}]"
        approval = _mapping(raw_approval, location)
        role = _text(approval.get("role"), "role", location)
        if role in roles:
            raise ExpeditedAlphaEvidenceError(
                f"{location}: duplicate approval role"
            )
        if approval.get("approver_type") != "human":
            raise ExpeditedAlphaEvidenceError(
                f"{location}: approver_type must be human"
            )
        _production_identifier(
            approval.get("approver_reference"),
            "approver_reference",
            location,
        )
        _validate_human_timestamp(
            approval.get("decided_at"),
            field="decided_at",
            location=location,
            evidence_as_of=evidence_as_of,
        )
        approved_digest = _digest(
            approval.get("evidence_sha256"),
            "evidence_sha256",
            location,
        )
        if approved_digest != binding_sha256:
            raise ExpeditedAlphaEvidenceError(
                f"{location}: evidence_sha256 does not match evidence_binding"
            )
        roles[role] = approval.get("decision") == "approved"
    required_roles = {"oversight", "source-rights", "expedited-risk"}
    gates = [
        _gate(
            "expedited_approval.human_roles",
            required_roles.issubset(roles)
            and all(roles.get(role) is True for role in required_roles),
            required=sorted(required_roles),
            actual=sorted(role for role, approved in roles.items() if approved),
        )
    ]
    return {
        **artifact,
        "section_sha256": section_sha256,
        "evidence_binding": dict(binding),
        "roles": roles,
    }, gates


def validate_legacy_archive(
    report: Mapping[str, object],
    *,
    expected_revision: str,
    evidence_as_of: datetime,
    rollback_artifact_sha256: str,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    _provenance(
        report,
        kind=LEGACY_ARCHIVE_KIND,
        expected_revision=expected_revision,
        location="legacy-archive",
        evidence_as_of=evidence_as_of,
    )
    archive_digest = _digest(
        report.get("archive_sha256"),
        "archive_sha256",
        "legacy-archive",
    )
    artifact_id = _int(
        report.get("artifact_id"),
        "artifact_id",
        "legacy-archive",
        minimum=1,
    )
    artifact_name = _production_identifier(
        report.get("artifact_name"),
        "artifact_name",
        "legacy-archive",
    )
    day_count = _int(
        report.get("consecutive_day_count"),
        "consecutive_day_count",
        "legacy-archive",
        minimum=1,
    )
    first_date = _date(
        report.get("first_date"),
        "first_date",
        "legacy-archive",
    )
    last_date = _date(
        report.get("last_date"),
        "last_date",
        "legacy-archive",
    )
    generated_at = _timestamp(
        report.get("generated_at"),
        "generated_at",
        "legacy-archive",
    )
    if generated_at > evidence_as_of + timedelta(minutes=1):
        raise ExpeditedAlphaEvidenceError(
            "legacy-archive: generated_at is after evidence_as_of"
        )
    contains_placeholder = _bool(
        report.get("contains_placeholder"),
        "contains_placeholder",
        "legacy-archive",
    )
    duplicate_content_count = _int(
        report.get("duplicate_content_count"),
        "duplicate_content_count",
        "legacy-archive",
    )
    if (last_date - first_date).days + 1 != day_count:
        raise ExpeditedAlphaEvidenceError(
            "legacy-archive: dates are not a complete consecutive-day archive"
        )
    if archive_digest != rollback_artifact_sha256:
        raise ExpeditedAlphaEvidenceError(
            "legacy-archive: archive digest does not match rollback drill"
        )
    manifest = _mapping(
        report.get("compatibility_manifest"),
        "legacy-archive.compatibility_manifest",
    )
    manifest_digest = _digest(
        report.get("compatibility_manifest_sha256"),
        "compatibility_manifest_sha256",
        "legacy-archive",
    )
    if manifest_digest != _sha256_json(manifest):
        raise ExpeditedAlphaEvidenceError(
            "legacy-archive: compatibility manifest digest does not match"
        )
    if (
        manifest.get("schema_version")
        != EXPEDITED_LEGACY_MANIFEST_SCHEMA_VERSION
        or manifest.get("kind") != EXPEDITED_LEGACY_MANIFEST_KIND
        or manifest.get("release_channel") != RELEASE_CHANNEL
    ):
        raise ExpeditedAlphaEvidenceError(
            "legacy-archive: compatibility manifest identity is invalid"
        )
    mode = _text(
        manifest.get("mode"),
        "mode",
        "legacy-archive.compatibility_manifest",
    )
    if mode not in {"89_day_human_waiver", "standard_90_day"}:
        raise ExpeditedAlphaEvidenceError(
            "legacy-archive: compatibility manifest mode is invalid"
        )
    prepared_at = _timestamp(
        manifest.get("prepared_at"),
        "prepared_at",
        "legacy-archive.compatibility_manifest",
    )
    if (
        prepared_at > evidence_as_of + timedelta(minutes=1)
        or generated_at != prepared_at
    ):
        raise ExpeditedAlphaEvidenceError(
            "legacy-archive: manifest preparation time is invalid"
        )
    source = _mapping(
        manifest.get("source"),
        "legacy-archive.compatibility_manifest.source",
    )
    source_location = "legacy-archive.compatibility_manifest.source"
    source_artifact_id_text = _text(
        source.get("artifact_id"),
        "artifact_id",
        source_location,
    )
    if (
        not source_artifact_id_text.isdigit()
        or int(source_artifact_id_text) < 1
    ):
        raise ExpeditedAlphaEvidenceError(
            f"{source_location}: artifact_id must be a positive integer string"
        )
    source_artifact_name = _production_identifier(
        source.get("artifact_name"),
        "artifact_name",
        source_location,
    )
    _production_identifier(
        source.get("run_id"),
        "run_id",
        source_location,
    )
    _revision(
        source.get("code_revision"),
        "code_revision",
        source_location,
    )
    source_archive_digest = _prefixed_digest(
        source.get("artifact_digest"),
        "artifact_digest",
        source_location,
    )
    if source.get("workflow") != ".github/workflows/build-feed.yml":
        raise ExpeditedAlphaEvidenceError(
            "legacy-archive: compatibility manifest source workflow is invalid"
        )
    if (
        int(source_artifact_id_text) != artifact_id
        or source_artifact_name != artifact_name
        or source_archive_digest != archive_digest
    ):
        raise ExpeditedAlphaEvidenceError(
            "legacy-archive: compatibility source does not match the pinned artifact"
        )
    manifest_day_count = _int(
        manifest.get("window_days"),
        "window_days",
        "legacy-archive.compatibility_manifest",
        minimum=1,
    )
    manifest_report_count = _int(
        manifest.get("dated_report_count"),
        "dated_report_count",
        "legacy-archive.compatibility_manifest",
        minimum=1,
    )
    manifest_first_date = _date(
        manifest.get("window_start"),
        "window_start",
        "legacy-archive.compatibility_manifest",
    )
    manifest_last_date = _date(
        manifest.get("window_end"),
        "window_end",
        "legacy-archive.compatibility_manifest",
    )
    content_sha256 = _prefixed_digest(
        manifest.get("content_sha256"),
        "content_sha256",
        "legacy-archive.compatibility_manifest",
    )
    waiver = _mapping(
        manifest.get("waiver"),
        "legacy-archive.compatibility_manifest.waiver",
    )
    waiver_digest = _digest(
        report.get("waiver_sha256"),
        "waiver_sha256",
        "legacy-archive",
    )
    if waiver_digest != _sha256_json(waiver):
        raise ExpeditedAlphaEvidenceError(
            "legacy-archive: waiver digest does not match the compatibility manifest"
        )
    if (
        manifest_day_count != day_count
        or manifest_report_count != day_count
        or manifest_first_date != first_date
        or manifest_last_date != last_date
    ):
        raise ExpeditedAlphaEvidenceError(
            "legacy-archive: summary does not match the compatibility manifest"
        )
    if (manifest_last_date - manifest_first_date).days + 1 != manifest_day_count:
        raise ExpeditedAlphaEvidenceError(
            "legacy-archive: compatibility window is not consecutive"
        )

    waiver_used = mode == "89_day_human_waiver"
    if waiver_used:
        waiver_approved_at = _timestamp(
            waiver.get("approved_at"),
            "approved_at",
            "legacy-archive.compatibility_manifest.waiver",
        )
        waiver_expires_at = _timestamp(
            waiver.get("expires_at"),
            "expires_at",
            "legacy-archive.compatibility_manifest.waiver",
        )
        if (
            day_count != WAIVER_DAY_COUNT
            or first_date != WAIVER_FIRST_DATE
            or last_date != WAIVER_LAST_DATE
            or evidence_as_of >= WAIVER_CUTOFF
            or prepared_at >= WAIVER_CUTOFF
            or waiver_approved_at >= WAIVER_CUTOFF
            or waiver_approved_at > prepared_at
            or manifest.get("entire_legacy_site_snapshot") is not True
            or waiver.get("exception_id") != WAIVER_EXCEPTION_ID
            or waiver.get("release_channel") != RELEASE_CHANNEL
            or waiver.get("status") != "active"
            or waiver.get("approved") is not True
            or waiver.get("reviewer_type") != "human"
            or waiver.get("ai_generated_ground_truth") is not False
            or waiver.get("is_synthetic") is not False
            or waiver_expires_at != WAIVER_CUTOFF
        ):
            raise ExpeditedAlphaEvidenceError(
                "legacy-archive: the bound 89-day human waiver is not eligible"
            )
        _production_identifier(
            waiver.get("reviewer_id"),
            "reviewer_id",
            "legacy-archive.compatibility_manifest.waiver",
        )
        _text(
            waiver.get("reason"),
            "reason",
            "legacy-archive.compatibility_manifest.waiver",
        )
    else:
        latest_required = _latest_recovery_date(evidence_as_of)
        current_kst_date = evidence_as_of.astimezone(KST).date()
        if (
            day_count != 90
            or (last_date - first_date).days != 89
            or last_date < latest_required
            or last_date > current_kst_date
            or waiver.get("status") != "not_required"
        ):
            raise ExpeditedAlphaEvidenceError(
                "legacy-archive: standard recovery must be the latest real 90-day window"
            )
        _prefixed_digest(
            manifest.get("standard_manifest_sha256"),
            "standard_manifest_sha256",
            "legacy-archive.compatibility_manifest",
        )
    gates = [
        _gate(
            "expedited_legacy_archive.real_consecutive_days",
            not contains_placeholder and duplicate_content_count == 0,
            required={
                "contains_placeholder": False,
                "duplicate_content_count": 0,
            },
            actual={
                "contains_placeholder": contains_placeholder,
                "duplicate_content_count": duplicate_content_count,
            },
        ),
        _gate(
            "expedited_legacy_archive.compatibility",
            (
                waiver_used
                and day_count == WAIVER_DAY_COUNT
                and evidence_as_of < WAIVER_CUTOFF
            )
            or (not waiver_used and day_count == 90),
            required=(
                "bound eligible 89-day human waiver before cutoff or "
                "latest standard 90-day manifest"
            ),
            actual={
                "consecutive_day_count": day_count,
                "waiver_used": waiver_used,
                "mode": mode,
            },
        ),
    ]
    return {
        "archive_sha256": archive_digest,
        "artifact_id": artifact_id,
        "artifact_name": artifact_name,
        "consecutive_day_count": day_count,
        "first_date": first_date.isoformat(),
        "last_date": last_date.isoformat(),
        "generated_at": generated_at.isoformat(),
        "mode": mode,
        "compatibility_manifest_sha256": manifest_digest,
        "content_sha256": content_sha256,
        "waiver_sha256": waiver_digest,
        "waiver_used": waiver_used,
        "waiver_cutoff": WAIVER_CUTOFF.isoformat(),
    }, gates


def build_expedited_release_report(
    bundle: Mapping[str, object],
    *,
    expected_revision: str,
) -> dict[str, object]:
    revision, evidence_as_of = _validate_root(
        bundle,
        expected_revision=expected_revision,
    )
    observations = [
        dict(_mapping(value, f"observations[{index}]"))
        for index, value in enumerate(
            _list(bundle.get("observations"), "observations")
        )
    ]
    observation_summary, observation_gates = validate_expedited_observations(
        observations,
        expected_revision=revision,
        evidence_as_of=evidence_as_of,
    )
    connector_receipts = _mapping(
        bundle.get("connector_receipts"),
        "connector_receipts",
    )
    connector_summary, connector_gates = validate_connector_receipts(
        connector_receipts,
        expected_revision=revision,
        evidence_as_of=evidence_as_of,
    )
    source_readiness = _mapping(
        bundle.get("source_readiness"),
        "source_readiness",
    )
    source_summary, source_gates = validate_source_readiness(
        source_readiness,
        expected_revision=revision,
        evidence_as_of=evidence_as_of,
    )
    human_review = _mapping(bundle.get("human_review"), "human_review")
    human_summary, human_gates = validate_expedited_human_review(
        human_review,
        expected_revision=revision,
        evidence_as_of=evidence_as_of,
    )
    content_integrity = _mapping(
        bundle.get("content_integrity"),
        "content_integrity",
    )
    content_summary, content_gates = validate_content_integrity(
        content_integrity,
        expected_revision=revision,
        evidence_as_of=evidence_as_of,
    )
    public_event_count = _int(
        content_summary.get("public_event_count"),
        "public_event_count",
        "content-integrity.summary",
    )
    content_gates.append(
        _gate(
            "expedited_content.minimum_reviewed_events",
            public_event_count >= MINIMUM_REVIEWED_EVENTS,
            required=MINIMUM_REVIEWED_EVENTS,
            actual=public_event_count,
        )
    )
    experience = _mapping(bundle.get("experience"), "experience")
    experience_summary, experience_gates = validate_experience(
        experience,
        expected_revision=revision,
        evidence_as_of=evidence_as_of,
    )
    rollback = _mapping(
        experience.get("rollback_drill"),
        "experience.rollback_drill",
    )
    rollback_seconds = _number(
        rollback.get("duration_minutes"),
        "duration_minutes",
        "experience.rollback_drill",
    ) * 60
    rollback_digest = _digest(
        rollback.get("legacy_artifact_sha256"),
        "legacy_artifact_sha256",
        "experience.rollback_drill",
    )
    rollback_gates = [
        _gate(
            "expedited_rollback.maximum_seconds",
            rollback.get("succeeded") is True and rollback_seconds <= 600,
            required={"succeeded": True, "maximum_seconds": 600},
            actual={
                "succeeded": rollback.get("succeeded"),
                "duration_seconds": rollback_seconds,
            },
        )
    ]
    legacy_archive = _mapping(
        bundle.get("legacy_archive"),
        "legacy_archive",
    )
    archive_summary, archive_gates = validate_legacy_archive(
        legacy_archive,
        expected_revision=revision,
        evidence_as_of=evidence_as_of,
        rollback_artifact_sha256=rollback_digest,
    )
    binding_sections = {
        "human_review_section_sha256": human_summary["section_sha256"],
        "legacy_manifest_sha256": archive_summary[
            "compatibility_manifest_sha256"
        ],
        "pages_terminal_content_sha256": observation_summary[
            "terminal_content_sha256"
        ],
        "content_integrity_sha256": _sha256_json(content_integrity),
        "experience_sha256": _sha256_json(experience),
        "rollback_drill_sha256": _sha256_json(
            _mapping(
                experience.get("rollback_drill_receipt"),
                "experience.rollback_drill_receipt",
            )
        ),
        "observations_sha256": _sha256_json(observations),
        "legacy_source_artifact_sha256": archive_summary["archive_sha256"],
    }
    required_evidence_binding = {
        **binding_sections,
        "binding_sha256": _sha256_json(binding_sections),
    }
    approval = _mapping(bundle.get("approval"), "approval")
    approval_summary, approval_gates = validate_expedited_approval(
        approval,
        expected_revision=revision,
        evidence_as_of=evidence_as_of,
        required_evidence_binding=required_evidence_binding,
    )
    gates = (
        observation_gates
        + connector_gates
        + source_gates
        + human_gates
        + content_gates
        + experience_gates
        + rollback_gates
        + approval_gates
        + archive_gates
    )
    failed = [str(gate["name"]) for gate in gates if gate["passed"] is not True]
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": REPORT_KIND,
        "release_tier": "production-alpha-early-access",
        "release_channel": RELEASE_CHANNEL,
        "ga_certification_claimed": False,
        "quality_statement": (
            "This expedited report verifies the bounded Early Access contract; "
            "the standard 24-hour Production Alpha gate remains independent."
        ),
        "code_revision": revision,
        "evidence_as_of": evidence_as_of.isoformat(),
        "input_sha256": _sha256_json(bundle),
        "evidence_sha256": {
            "observations": _sha256_json(observations),
            "connector_receipts": _sha256_json(connector_receipts),
            "source_readiness": _sha256_json(source_readiness),
            "human_review": _sha256_json(human_review),
            "content_integrity": _sha256_json(content_integrity),
            "experience": _sha256_json(experience),
            "approval": _sha256_json(approval),
            "legacy_archive": _sha256_json(legacy_archive),
        },
        "observation": observation_summary,
        "connector_receipts": connector_summary,
        "source_readiness": source_summary,
        "human_review": human_summary,
        "content_integrity": content_summary,
        "experience": experience_summary,
        "rollback": {
            "duration_seconds": rollback_seconds,
            "legacy_artifact_sha256": rollback_digest,
        },
        "approval": approval_summary,
        "legacy_archive": archive_summary,
        "gates": gates,
        "failed_gates": failed,
        "release_gate_passed": not failed,
    }


def _load_bundle(path: Path) -> dict[str, object]:
    if not path.is_file():
        raise ExpeditedAlphaEvidenceError(
            f"{path}: expedited input bundle is missing"
        )
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExpeditedAlphaEvidenceError(
            f"{path}: invalid expedited input JSON"
        ) from exc
    return dict(_mapping(value, str(path)))


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
        description="Evaluate the BSIDE expedited Production Alpha gate",
    )
    subparsers = parser.add_subparsers(dest="command")
    evaluate = subparsers.add_parser("evaluate")
    evaluate.add_argument("--input", type=Path, required=True)
    evaluate.add_argument("--expected-revision", required=True)
    evaluate.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    if args.command is None:
        args = build_arg_parser().parse_args(
            ["evaluate", *(argv or sys.argv[1:])]
        )
    try:
        report = build_expedited_release_report(
            _load_bundle(args.input),
            expected_revision=args.expected_revision,
        )
        _write_report(args.output, report)
    except AlphaReleaseEvidenceError as exc:
        print(
            json.dumps(
                {
                    "status": "invalid-expedited-production-alpha-evidence",
                    "error": str(exc),
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 2
    print(
        json.dumps(
            report,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    return 0 if report["release_gate_passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
