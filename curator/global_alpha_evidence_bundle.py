from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import io
import json
import sys
import tempfile
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Mapping, Sequence

from .global_alpha_release_gate import (
    AlphaReleaseEvidenceError,
    EXPECTED_CONNECTORS,
    EXPECTED_VIEWPORTS,
    INPUT_BUNDLE_KIND,
    INPUT_FILENAMES,
    REQUIRED_ALPHA_COUNTRIES,
    SCHEMA_VERSION,
    materialize_input_bundle,
    validate_approval,
    validate_connector_idempotency,
    validate_content_integrity,
    validate_experience,
    validate_human_review,
)


AUTOMATED_EVIDENCE_KIND = "bside-global-alpha-automated-evidence"
REVIEW_CANDIDATE_EXPORT_KIND = "bside-global-alpha-review-candidate-export"
REVIEW_SELECTION_KIND = "bside-global-alpha-review-selection"
REVIEW_PROVENANCE_KIND = "bside-global-alpha-review-candidate-provenance"
REVIEW_PRODUCER_WORKFLOW = ".github/workflows/global-alpha-review-candidates.yml"
REVIEW_EXPORT_FILENAME = "global-alpha-review-candidate-export.json"
EXPERIENCE_MANIFEST_KIND = "bside-global-alpha-experience-artifact-manifest"
CANDIDATE_MANIFEST_KIND = "bside-global-alpha-evidence-candidate-manifest"
MAX_UNCOMPRESSED_BYTES = 2_000_000
MAX_SECRET_BYTES = 48_000
EVENT_REVIEW_COUNT = 60
PAIR_REVIEW_COUNT = 120
TOP5_REVIEW_COUNT = 5
SHA_RE_LENGTH = 40
SHA256_RE_LENGTH = 64

COMMON_FIELDS = {
    "schema_version",
    "kind",
    "environment",
    "evidence_source",
    "is_synthetic",
    "code_revision",
    "collected_at",
}
CONNECTOR_FIELDS = COMMON_FIELDS | {"connectors"}
CONNECTOR_ITEM_FIELDS = {
    "connector_family",
    "country",
    "payload_sha256",
    "first_run",
    "replay_run",
    "row_count_after_first",
    "row_count_after_replay",
    "duplicate_row_count",
    "checkpoint_after_first",
    "checkpoint_after_replay",
    "coverage_started_at",
    "coverage_ended_at",
    "successful_window_count",
    "failed_window_count",
    "completed_windows",
}
FIRST_RUN_FIELDS = {
    "raw_count",
    "filtered_out_count",
    "accepted_count",
    "acknowledged_count",
    "idempotent",
}
REPLAY_RUN_FIELDS = FIRST_RUN_FIELDS | {"payload_sha256"}
WINDOW_FIELDS = {
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
HUMAN_REVIEW_FIELDS = COMMON_FIELDS | {
    "ground_truth_source",
    "ai_generated_ground_truth",
    "human_attestation",
    "raw_counts",
    "event_reviews",
    "same_event_pair_reviews",
    "top5_reviews",
}
HUMAN_REVIEW_COUNT_FIELDS = {
    "event_review_count",
    "same_event_pair_review_count",
    "top5_human_reviewed_count",
    "top5_published_count",
}
EVENT_REVIEW_FIELDS = {
    "event_id",
    "decision",
    "reviewer_type",
    "reviewer_reference",
    "reviewed_at",
}
PAIR_REVIEW_FIELDS = {
    "pair_id",
    "left_document_id",
    "right_document_id",
    "decision",
    "reviewer_type",
    "reviewer_reference",
    "reviewed_at",
}
TOP5_REVIEW_FIELDS = {
    "edition_id",
    "event_id",
    "decision",
    "reviewer_type",
    "reviewer_reference",
    "reviewed_at",
}
CONTENT_FIELDS = COMMON_FIELDS | {"raw_counts"}
CONTENT_COUNT_FIELDS = {
    "public_event_count",
    "original_language_preserved_count",
    "official_url_preserved_count",
    "title_provenance_labeled_count",
    "source_title_event_count",
    "source_title_preserved_count",
    "generated_metadata_title_count",
    "operator_metadata_title_count",
    "unknown_title_provenance_count",
    "scanned_response_count",
    "telegram_exposure_count",
    "internal_field_exposure_count",
    "persisted_snapshot_forbidden_key_count",
}
EXPERIENCE_FIELDS = COMMON_FIELDS | {
    "viewports",
    "web_vitals",
    "api_responses",
    "failure_detection_drill",
    "rollback_drill",
}
VIEWPORT_FIELDS = {
    "viewport",
    "visual_regression_passed",
    "axe_serious_count",
    "axe_critical_count",
}
MOBILE_VIEWPORT_FIELDS = VIEWPORT_FIELDS | {"first_important_event_top_px"}
WEB_VITAL_FIELDS = {"lcp", "inp", "cls"}
LCP_FIELDS = {"p75_seconds", "sample_count"}
INP_FIELDS = {"p75_ms", "sample_count"}
CLS_FIELDS = {"p75", "sample_count"}
API_RESPONSE_FIELDS = {"route", "size_bytes", "http_status"}
DETECTION_FIELDS = {"incident_started_at", "detected_at", "detection_minutes"}
ROLLBACK_FIELDS = {
    "succeeded",
    "duration_minutes",
    "started_at",
    "completed_at",
    "legacy_artifact_sha256",
}
APPROVAL_FIELDS = COMMON_FIELDS | {
    "release_tier_acknowledged",
    "ga_certification_claimed",
    "approvals",
    "source_right_scope",
}
APPROVAL_RECORD_FIELDS = {
    "role",
    "decision",
    "approver_type",
    "approver_reference",
    "decided_at",
    "evidence_sha256",
}
SOURCE_RIGHT_FIELDS = {
    "country",
    "decision",
    "valid_source_right_count",
    "invalid_source_right_count",
}
REVIEW_EXPORT_FIELDS = COMMON_FIELDS | {
    "event_candidates",
    "same_event_pair_candidates",
    "top5_candidates",
}
AUTOMATED_DATA_FIELDS = COMMON_FIELDS | {
    "connector_coverage",
    "content_integrity",
}
COVERAGE_FIELDS = {
    "connector_family",
    "country",
    "coverage_started_at",
    "coverage_ended_at",
    "successful_window_count",
    "failed_window_count",
    "completed_windows",
}
EXPERIENCE_ROUTES = (
    "/briefs/latest?edition=global",
    "/live?limit=100",
    "/events?limit=100",
    "/issuers?limit=100",
    "/calendar?limit=100",
    "/search?q=governance",
    "/sources/status",
    "/exports/events.json?limit=100",
    "/exports/events.csv?limit=100",
    "/feeds/events.atom?limit=100",
)
EXPERIENCE_MEASUREMENTS = (
    "web-vitals",
    "api-responses",
    "failure-detection-drill",
    "rollback-drill",
)


class EvidenceBundleError(ValueError):
    """Raised when candidate or protected Production Alpha evidence is unsafe."""


def _mapping(value: object, location: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise EvidenceBundleError(f"{location}: expected an object")
    return dict(value)


def _list(value: object, location: str) -> list[object]:
    if not isinstance(value, list):
        raise EvidenceBundleError(f"{location}: expected an array")
    return value


def _text(value: object, field: str, location: str) -> str:
    result = str(value or "").strip()
    if not result:
        raise EvidenceBundleError(f"{location}: {field} must be a non-empty string")
    return result


def _revision(value: object, field: str, location: str) -> str:
    result = _text(value, field, location).casefold()
    if (
        len(result) != SHA_RE_LENGTH
        or any(character not in "0123456789abcdef" for character in result)
    ):
        raise EvidenceBundleError(
            f"{location}: {field} must be a full 40-character Git SHA"
        )
    return result


def _digest(value: object, field: str, location: str) -> str:
    result = _text(value, field, location).casefold()
    if (
        len(result) != SHA256_RE_LENGTH
        or any(character not in "0123456789abcdef" for character in result)
    ):
        raise EvidenceBundleError(f"{location}: {field} must be a SHA-256 digest")
    return result


def _positive_identifier(value: object, field: str, location: str) -> str:
    result = _text(value, field, location)
    if not result.isascii() or not result.isdecimal() or int(result) <= 0:
        raise EvidenceBundleError(
            f"{location}: {field} must be a positive decimal identifier"
        )
    return result


def _github_artifact_digest(value: object, field: str, location: str) -> str:
    result = _text(value, field, location).casefold()
    prefix = "sha256:"
    if not result.startswith(prefix):
        raise EvidenceBundleError(
            f"{location}: {field} must use the sha256:<digest> format"
        )
    return prefix + _digest(result[len(prefix) :], field, location)


def _timestamp(value: object, field: str, location: str) -> datetime:
    raw = _text(value, field, location).replace("Z", "+00:00")
    try:
        result = datetime.fromisoformat(raw)
    except ValueError as exc:
        raise EvidenceBundleError(f"{location}: {field} must be ISO-8601") from exc
    if result.tzinfo is None:
        raise EvidenceBundleError(f"{location}: {field} must include a timezone")
    return result.astimezone(timezone.utc)


def _exact_keys(
    value: Mapping[str, object],
    expected: set[str],
    location: str,
) -> None:
    actual = set(value)
    if actual != expected:
        missing = sorted(expected - actual)
        unexpected = sorted(actual - expected)
        raise EvidenceBundleError(
            f"{location}: exact fields required; "
            f"missing={missing}, unexpected={unexpected}"
        )


def _strict_json_bytes(raw: bytes, location: str) -> dict[str, object]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise EvidenceBundleError(f"{location}: invalid UTF-8") from exc
    try:
        decoded = json.loads(text)
    except json.JSONDecodeError as exc:
        raise EvidenceBundleError(
            f"{location}: invalid, concatenated, or trailing JSON"
        ) from exc
    return _mapping(decoded, location)


def _load_json(path: Path, location: str) -> dict[str, object]:
    try:
        return _strict_json_bytes(path.read_bytes(), location)
    except OSError as exc:
        raise EvidenceBundleError(f"{location}: cannot read {path}") from exc


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _write_json(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _validate_common(
    value: Mapping[str, object],
    *,
    expected_kind: str,
    expected_revision: str,
    location: str,
) -> datetime:
    if value.get("schema_version") != SCHEMA_VERSION:
        raise EvidenceBundleError(
            f"{location}: schema_version must be {SCHEMA_VERSION}"
        )
    if value.get("kind") != expected_kind:
        raise EvidenceBundleError(f"{location}: unexpected kind")
    if value.get("environment") != "production":
        raise EvidenceBundleError(f"{location}: environment must be production")
    if value.get("is_synthetic") is not False:
        raise EvidenceBundleError(f"{location}: real production data is required")
    revision = _revision(value.get("code_revision"), "code_revision", location)
    if revision != expected_revision:
        raise EvidenceBundleError(f"{location}: code_revision mismatch")
    source = _text(value.get("evidence_source"), "evidence_source", location)
    if any(marker in source.casefold() for marker in ("fixture", "synthetic", "test")):
        raise EvidenceBundleError(f"{location}: synthetic evidence source is forbidden")
    return _timestamp(value.get("collected_at"), "collected_at", location)


def _parse_automated_evidence(
    path: Path,
    *,
    expected_revision: str,
) -> tuple[dict[str, object], dict[str, object], datetime]:
    response = _load_json(path, "automated-evidence-response")
    _exact_keys(
        response,
        {"ok", "api_version", "data"},
        "automated-evidence-response",
    )
    if response.get("ok") is not True or response.get("api_version") != "v2":
        raise EvidenceBundleError(
            "automated-evidence-response: successful v2 response required"
        )
    data = _mapping(response.get("data"), "automated-evidence-response.data")
    _exact_keys(data, AUTOMATED_DATA_FIELDS, "automated-evidence-response.data")
    collected_at = _validate_common(
        data,
        expected_kind=AUTOMATED_EVIDENCE_KIND,
        expected_revision=expected_revision,
        location="automated-evidence-response.data",
    )
    if data.get("evidence_source") != "production_database_export":
        raise EvidenceBundleError(
            "automated-evidence-response.data: production database export required"
        )
    coverage = _list(
        data.get("connector_coverage"),
        "automated-evidence-response.data.connector_coverage",
    )
    families: set[str] = set()
    for index, raw_item in enumerate(coverage):
        location = f"automated-evidence-response.data.connector_coverage[{index}]"
        item = _mapping(raw_item, location)
        _exact_keys(item, COVERAGE_FIELDS, location)
        family = _text(item.get("connector_family"), "connector_family", location)
        country = _text(item.get("country"), "country", location)
        if family in families or EXPECTED_CONNECTORS.get(family) != country:
            raise EvidenceBundleError(f"{location}: unexpected or duplicate connector")
        families.add(family)
        for window_index, raw_window in enumerate(
            _list(item.get("completed_windows"), f"{location}.completed_windows")
        ):
            window_location = f"{location}.completed_windows[{window_index}]"
            window = _mapping(raw_window, window_location)
            _exact_keys(window, WINDOW_FIELDS, window_location)
    if families != set(EXPECTED_CONNECTORS):
        raise EvidenceBundleError(
            "automated-evidence-response.data: exact official connector set required"
        )
    content = _mapping(
        data.get("content_integrity"),
        "automated-evidence-response.data.content_integrity",
    )
    _validate_exact_evidence_file("content-integrity.json", content)
    return data, response, collected_at


def _parse_review_export(
    path: Path,
    *,
    expected_revision: str,
) -> tuple[dict[str, object], datetime]:
    export = _load_json(path, "review-candidate-export")
    _exact_keys(export, REVIEW_EXPORT_FIELDS, "review-candidate-export")
    collected_at = _validate_common(
        export,
        expected_kind=REVIEW_CANDIDATE_EXPORT_KIND,
        expected_revision=expected_revision,
        location="review-candidate-export",
    )
    return export, collected_at


def _string_list(value: object, field: str, location: str) -> list[str]:
    items = _list(value, f"{location}.{field}")
    result = sorted({_text(item, field, location) for item in items})
    if not result:
        raise EvidenceBundleError(f"{location}: {field} cannot be empty")
    return result


def _safe_event_candidate(value: object, location: str) -> dict[str, object]:
    item = _mapping(value, location)
    event_id = _text(item.get("event_id"), "event_id", location)
    document_ids = _string_list(
        item.get("official_document_ids"),
        "official_document_ids",
        location,
    )
    official_urls = _string_list(
        item.get("official_urls"),
        "official_urls",
        location,
    )
    return {
        "event_id": event_id,
        "title": str(item.get("title") or "").strip(),
        "issuer_name": str(item.get("issuer_name") or "").strip(),
        "country": str(item.get("country") or "").strip(),
        "event_family": str(item.get("event_family") or "").strip(),
        "importance": str(item.get("importance") or "").strip(),
        "verification_status": str(item.get("verification_status") or "").strip(),
        "official_document_ids": document_ids,
        "official_urls": official_urls,
    }


def _candidate_document_id(
    item: Mapping[str, object],
    side: str,
    location: str,
) -> str:
    direct = str(item.get(f"{side}_document_id") or "").strip()
    if direct:
        return direct
    nested = item.get(side)
    if isinstance(nested, dict):
        return _text(
            nested.get("document_id") or nested.get("article_id"),
            f"{side}_document_id",
            location,
        )
    raise EvidenceBundleError(f"{location}: {side}_document_id is required")


def _safe_pair_candidate(value: object, location: str) -> dict[str, object]:
    item = _mapping(value, location)
    left_id = _candidate_document_id(item, "left", location)
    right_id = _candidate_document_id(item, "right", location)
    if left_id == right_id:
        raise EvidenceBundleError(f"{location}: pair documents must differ")
    left = item.get("left") if isinstance(item.get("left"), dict) else {}
    right = item.get("right") if isinstance(item.get("right"), dict) else {}
    return {
        "pair_id": _text(item.get("pair_id"), "pair_id", location),
        "left_document_id": left_id,
        "right_document_id": right_id,
        "left_title": str(
            item.get("left_title")
            or (left.get("title") if isinstance(left, dict) else "")
            or ""
        ).strip(),
        "right_title": str(
            item.get("right_title")
            or (right.get("title") if isinstance(right, dict) else "")
            or ""
        ).strip(),
        "left_url": str(
            item.get("left_url")
            or (left.get("canonical_url") if isinstance(left, dict) else "")
            or ""
        ).strip(),
        "right_url": str(
            item.get("right_url")
            or (right.get("canonical_url") if isinstance(right, dict) else "")
            or ""
        ).strip(),
        "stratum": str(item.get("stratum") or "unstratified").strip(),
    }


def _safe_top5_candidate(value: object, location: str) -> dict[str, object]:
    item = _mapping(value, location)
    position = item.get("position_no")
    if isinstance(position, bool) or not isinstance(position, int):
        raise EvidenceBundleError(f"{location}: position_no must be an integer")
    return {
        "edition_id": _text(item.get("edition_id"), "edition_id", location),
        "event_id": _text(item.get("event_id"), "event_id", location),
        "position_no": position,
        "title": str(item.get("title") or "").strip(),
        "official_url": str(item.get("official_url") or "").strip(),
    }


def _round_robin_select(
    values: Sequence[dict[str, object]],
    *,
    count: int,
    group_fields: Sequence[str],
    identity_field: str,
    location: str,
) -> list[dict[str, object]]:
    unique: dict[str, dict[str, object]] = {}
    for value in values:
        identity = _text(value.get(identity_field), identity_field, location)
        if identity in unique:
            raise EvidenceBundleError(f"{location}: duplicate {identity_field}")
        unique[identity] = value
    groups: dict[tuple[str, ...], list[dict[str, object]]] = {}
    for value in unique.values():
        key = tuple(str(value.get(field) or "") for field in group_fields)
        groups.setdefault(key, []).append(value)
    queues = [
        sorted(group, key=lambda item: str(item[identity_field]))
        for _, group in sorted(groups.items())
    ]
    selected: list[dict[str, object]] = []
    while len(selected) < count and queues:
        remaining: list[list[dict[str, object]]] = []
        for queue in queues:
            if queue and len(selected) < count:
                selected.append(queue.pop(0))
            if queue:
                remaining.append(queue)
        queues = remaining
    if len(selected) != count:
        raise EvidenceBundleError(
            f"{location}: exactly {count} real candidates required; "
            f"available={len(unique)}"
        )
    return selected


def _select_review_candidates(
    export: Mapping[str, object],
) -> tuple[list[dict[str, object]], list[dict[str, object]], list[dict[str, object]]]:
    events = [
        _safe_event_candidate(value, f"review-candidate-export.event_candidates[{index}]")
        for index, value in enumerate(
            _list(export.get("event_candidates"), "review-candidate-export.event_candidates")
        )
    ]
    pairs = [
        _safe_pair_candidate(
            value,
            f"review-candidate-export.same_event_pair_candidates[{index}]",
        )
        for index, value in enumerate(
            _list(
                export.get("same_event_pair_candidates"),
                "review-candidate-export.same_event_pair_candidates",
            )
        )
    ]
    top5 = [
        _safe_top5_candidate(value, f"review-candidate-export.top5_candidates[{index}]")
        for index, value in enumerate(
            _list(export.get("top5_candidates"), "review-candidate-export.top5_candidates")
        )
    ]
    selected_events = _round_robin_select(
        events,
        count=EVENT_REVIEW_COUNT,
        group_fields=("country", "event_family", "importance"),
        identity_field="event_id",
        location="review-candidate-export.event_candidates",
    )
    selected_pairs = _round_robin_select(
        pairs,
        count=PAIR_REVIEW_COUNT,
        group_fields=("stratum",),
        identity_field="pair_id",
        location="review-candidate-export.same_event_pair_candidates",
    )
    document_pairs = {
        tuple(
            sorted(
                (
                    str(item["left_document_id"]),
                    str(item["right_document_id"]),
                )
            )
        )
        for item in selected_pairs
    }
    if len(document_pairs) != PAIR_REVIEW_COUNT:
        raise EvidenceBundleError(
            "review-candidate-export: duplicate unordered document pair"
        )
    editions = {str(item["edition_id"]) for item in top5}
    positions = {int(str(item["position_no"])) for item in top5}
    if (
        len(top5) != TOP5_REVIEW_COUNT
        or len(editions) != 1
        or positions != set(range(1, TOP5_REVIEW_COUNT + 1))
        or len({str(item["event_id"]) for item in top5}) != TOP5_REVIEW_COUNT
    ):
        raise EvidenceBundleError(
            "review-candidate-export.top5_candidates: exactly one edition "
            "with unique positions 1..5 is required"
        )
    selected_top5 = sorted(
        top5,
        key=lambda item: int(str(item["position_no"])),
    )
    return selected_events, selected_pairs, selected_top5


def _common_candidate(
    *,
    kind: str,
    revision: str,
    evidence_source: str,
    collected_at: object,
) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": kind,
        "environment": "production",
        "evidence_source": evidence_source,
        "is_synthetic": False,
        "code_revision": revision,
        "collected_at": collected_at,
    }


def _connector_candidate(
    automated: Mapping[str, object],
    *,
    revision: str,
    collected_at: str,
) -> dict[str, object]:
    connectors: list[dict[str, object]] = []
    for raw_coverage in _list(
        automated.get("connector_coverage"),
        "automated-evidence.connector_coverage",
    ):
        coverage = _mapping(raw_coverage, "automated-evidence.connector_coverage[]")
        connectors.append(
            {
                "connector_family": coverage["connector_family"],
                "country": coverage["country"],
                "payload_sha256": None,
                "first_run": {
                    "raw_count": None,
                    "filtered_out_count": None,
                    "accepted_count": None,
                    "acknowledged_count": None,
                    "idempotent": None,
                },
                "replay_run": {
                    "raw_count": None,
                    "filtered_out_count": None,
                    "accepted_count": None,
                    "acknowledged_count": None,
                    "idempotent": None,
                    "payload_sha256": None,
                },
                "row_count_after_first": None,
                "row_count_after_replay": None,
                "duplicate_row_count": None,
                "checkpoint_after_first": None,
                "checkpoint_after_replay": None,
                "coverage_started_at": coverage["coverage_started_at"],
                "coverage_ended_at": coverage["coverage_ended_at"],
                "successful_window_count": coverage["successful_window_count"],
                "failed_window_count": coverage["failed_window_count"],
                "completed_windows": coverage["completed_windows"],
            }
        )
    connectors.sort(key=lambda item: str(item["connector_family"]))
    return {
        **_common_candidate(
            kind="bside-global-alpha-connector-idempotency",
            revision=revision,
            evidence_source=(
                "production_database_export_pending_protected_replay_audit"
            ),
            collected_at=collected_at,
        ),
        "connectors": connectors,
    }


def _human_review_candidate(
    events: Sequence[Mapping[str, object]],
    pairs: Sequence[Mapping[str, object]],
    top5: Sequence[Mapping[str, object]],
    *,
    revision: str,
) -> dict[str, object]:
    return {
        **_common_candidate(
            kind="bside-global-alpha-human-review",
            revision=revision,
            evidence_source="pending_human_oversight",
            collected_at=None,
        ),
        "ground_truth_source": None,
        "ai_generated_ground_truth": False,
        "human_attestation": False,
        "raw_counts": {
            "event_review_count": len(events),
            "same_event_pair_review_count": len(pairs),
            "top5_human_reviewed_count": 0,
            "top5_published_count": len(top5),
        },
        "event_reviews": [
            {
                "event_id": item["event_id"],
                "decision": None,
                "reviewer_type": None,
                "reviewer_reference": None,
                "reviewed_at": None,
            }
            for item in events
        ],
        "same_event_pair_reviews": [
            {
                "pair_id": item["pair_id"],
                "left_document_id": item["left_document_id"],
                "right_document_id": item["right_document_id"],
                "decision": None,
                "reviewer_type": None,
                "reviewer_reference": None,
                "reviewed_at": None,
            }
            for item in pairs
        ],
        "top5_reviews": [
            {
                "edition_id": item["edition_id"],
                "event_id": item["event_id"],
                "decision": None,
                "reviewer_type": None,
                "reviewer_reference": None,
                "reviewed_at": None,
            }
            for item in top5
        ],
    }


def _experience_candidate(*, revision: str) -> dict[str, object]:
    return {
        **_common_candidate(
            kind="bside-global-alpha-experience",
            revision=revision,
            evidence_source="pending_production_experience_evidence",
            collected_at=None,
        ),
        "viewports": [
            {
                "viewport": viewport,
                "visual_regression_passed": None,
                "axe_serious_count": None,
                "axe_critical_count": None,
                **(
                    {"first_important_event_top_px": None}
                    if viewport == "390x844"
                    else {}
                ),
            }
            for viewport in sorted(EXPECTED_VIEWPORTS)
        ],
        "web_vitals": {
            "lcp": {"p75_seconds": None, "sample_count": None},
            "inp": {"p75_ms": None, "sample_count": None},
            "cls": {"p75": None, "sample_count": None},
        },
        "api_responses": [
            {"route": route, "size_bytes": None, "http_status": None}
            for route in EXPERIENCE_ROUTES
        ],
        "failure_detection_drill": {
            "incident_started_at": None,
            "detected_at": None,
            "detection_minutes": None,
        },
        "rollback_drill": {
            "succeeded": None,
            "duration_minutes": None,
            "started_at": None,
            "completed_at": None,
            "legacy_artifact_sha256": None,
        },
    }


def _approval_candidate(*, revision: str) -> dict[str, object]:
    return {
        **_common_candidate(
            kind="bside-global-alpha-release-approval",
            revision=revision,
            evidence_source="pending_human_release_approval",
            collected_at=None,
        ),
        "release_tier_acknowledged": "production-alpha",
        "ga_certification_claimed": False,
        "approvals": [
            {
                "role": role,
                "decision": None,
                "approver_type": None,
                "approver_reference": None,
                "decided_at": None,
                "evidence_sha256": None,
            }
            for role in ("oversight", "source-rights")
        ],
        "source_right_scope": [
            {
                "country": country,
                "decision": None,
                "valid_source_right_count": None,
                "invalid_source_right_count": None,
            }
            for country in sorted(REQUIRED_ALPHA_COUNTRIES)
        ],
    }


def _experience_manifest_candidate(*, revision: str) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": EXPERIENCE_MANIFEST_KIND,
        "code_revision": revision,
        "release_eligible": False,
        "viewports": [
            {
                "viewport": viewport,
                "screenshot_path": None,
                "screenshot_sha256": None,
                "axe_report_path": None,
                "axe_report_sha256": None,
            }
            for viewport in sorted(EXPECTED_VIEWPORTS)
        ],
        "measurements": [
            {"name": name, "path": None, "sha256": None}
            for name in EXPERIENCE_MEASUREMENTS
        ],
        "human_approval": {
            "decision": None,
            "approver_type": None,
            "approver_reference": None,
            "decided_at": None,
        },
    }


def prepare_candidate_bundle(
    *,
    automated_evidence_path: Path,
    review_candidate_export_path: Path,
    output_dir: Path,
    expected_revision: str,
) -> dict[str, object]:
    revision = _revision(
        expected_revision,
        "expected_revision",
        "candidate-bundle",
    )
    automated, automated_response, automated_collected_at = (
        _parse_automated_evidence(
            automated_evidence_path,
            expected_revision=revision,
        )
    )
    review_export, review_collected_at = _parse_review_export(
        review_candidate_export_path,
        expected_revision=revision,
    )
    if abs((automated_collected_at - review_collected_at).total_seconds()) > 86_400:
        raise EvidenceBundleError(
            "candidate-bundle: automated and review exports must be within 24 hours"
        )
    events, pairs, top5 = _select_review_candidates(review_export)
    collected_at = automated_collected_at.isoformat()
    files: dict[str, dict[str, object]] = {
        "connector-idempotency.json": _connector_candidate(
            automated,
            revision=revision,
            collected_at=collected_at,
        ),
        "human-review.json": _human_review_candidate(
            events,
            pairs,
            top5,
            revision=revision,
        ),
        "content-integrity.json": _mapping(
            automated.get("content_integrity"),
            "automated-evidence.content_integrity",
        ),
        "experience.json": _experience_candidate(revision=revision),
        "approval.json": _approval_candidate(revision=revision),
    }
    for filename, value in files.items():
        _validate_exact_evidence_file(filename, value)
    review_selection = {
        "schema_version": SCHEMA_VERSION,
        "kind": REVIEW_SELECTION_KIND,
        "code_revision": revision,
        "automated_evidence_sha256": _sha256(
            _canonical_bytes(automated_response)
        ),
        "review_candidate_export_sha256": _sha256(
            _canonical_bytes(review_export)
        ),
        "event_candidates": events,
        "same_event_pair_candidates": pairs,
        "top5_candidates": top5,
    }
    experience_manifest = _experience_manifest_candidate(revision=revision)
    candidate_outputs: dict[str, object] = {
        **files,
        "review-candidates.json": review_selection,
        "experience-artifact-manifest.json": experience_manifest,
    }
    hashes = {
        filename: _sha256(_canonical_bytes(value))
        for filename, value in candidate_outputs.items()
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "kind": CANDIDATE_MANIFEST_KIND,
        "code_revision": revision,
        "generated_at": collected_at,
        "release_eligible": False,
        "reason": (
            "Human labels, screenshot approval, replay audit, experience metrics, "
            "and release approvals are intentionally blank."
        ),
        "counts": {
            "event_reviews": len(events),
            "same_event_pair_reviews": len(pairs),
            "top5_reviews": len(top5),
        },
        "files": hashes,
    }
    candidate_outputs["candidate-manifest.json"] = manifest
    if output_dir.exists():
        raise EvidenceBundleError(
            f"candidate-bundle: output directory already exists: {output_dir}"
        )
    output_dir.mkdir(parents=True)
    for filename, candidate_value in candidate_outputs.items():
        _write_json(output_dir / filename, candidate_value)
    return manifest


def _validate_exact_evidence_file(
    filename: str,
    value: Mapping[str, object],
) -> None:
    if filename == "connector-idempotency.json":
        _exact_keys(value, CONNECTOR_FIELDS, filename)
        for index, raw_item in enumerate(_list(value.get("connectors"), filename)):
            location = f"{filename}.connectors[{index}]"
            item = _mapping(raw_item, location)
            _exact_keys(item, CONNECTOR_ITEM_FIELDS, location)
            first = _mapping(item.get("first_run"), f"{location}.first_run")
            replay = _mapping(item.get("replay_run"), f"{location}.replay_run")
            _exact_keys(first, FIRST_RUN_FIELDS, f"{location}.first_run")
            _exact_keys(replay, REPLAY_RUN_FIELDS, f"{location}.replay_run")
            for window_index, raw_window in enumerate(
                _list(item.get("completed_windows"), f"{location}.completed_windows")
            ):
                window_location = f"{location}.completed_windows[{window_index}]"
                _exact_keys(
                    _mapping(raw_window, window_location),
                    WINDOW_FIELDS,
                    window_location,
                )
        return
    if filename == "human-review.json":
        _exact_keys(value, HUMAN_REVIEW_FIELDS, filename)
        counts = _mapping(value.get("raw_counts"), f"{filename}.raw_counts")
        _exact_keys(counts, HUMAN_REVIEW_COUNT_FIELDS, f"{filename}.raw_counts")
        contracts = (
            ("event_reviews", EVENT_REVIEW_FIELDS),
            ("same_event_pair_reviews", PAIR_REVIEW_FIELDS),
            ("top5_reviews", TOP5_REVIEW_FIELDS),
        )
        for field, expected in contracts:
            for index, raw_item in enumerate(_list(value.get(field), f"{filename}.{field}")):
                location = f"{filename}.{field}[{index}]"
                _exact_keys(_mapping(raw_item, location), expected, location)
        return
    if filename == "content-integrity.json":
        _exact_keys(value, CONTENT_FIELDS, filename)
        counts = _mapping(value.get("raw_counts"), f"{filename}.raw_counts")
        _exact_keys(counts, CONTENT_COUNT_FIELDS, f"{filename}.raw_counts")
        return
    if filename == "experience.json":
        _exact_keys(value, EXPERIENCE_FIELDS, filename)
        for index, raw_item in enumerate(
            _list(value.get("viewports"), f"{filename}.viewports")
        ):
            location = f"{filename}.viewports[{index}]"
            item = _mapping(raw_item, location)
            expected = (
                MOBILE_VIEWPORT_FIELDS
                if item.get("viewport") == "390x844"
                else VIEWPORT_FIELDS
            )
            _exact_keys(item, expected, location)
        metrics = _mapping(value.get("web_vitals"), f"{filename}.web_vitals")
        _exact_keys(metrics, WEB_VITAL_FIELDS, f"{filename}.web_vitals")
        nested_metrics = (
            ("lcp", LCP_FIELDS),
            ("inp", INP_FIELDS),
            ("cls", CLS_FIELDS),
        )
        for field, expected in nested_metrics:
            metric = _mapping(metrics.get(field), f"{filename}.web_vitals.{field}")
            _exact_keys(metric, expected, f"{filename}.web_vitals.{field}")
        for index, raw_item in enumerate(
            _list(value.get("api_responses"), f"{filename}.api_responses")
        ):
            location = f"{filename}.api_responses[{index}]"
            _exact_keys(_mapping(raw_item, location), API_RESPONSE_FIELDS, location)
        detection = _mapping(
            value.get("failure_detection_drill"),
            f"{filename}.failure_detection_drill",
        )
        _exact_keys(
            detection,
            DETECTION_FIELDS,
            f"{filename}.failure_detection_drill",
        )
        rollback = _mapping(value.get("rollback_drill"), f"{filename}.rollback_drill")
        _exact_keys(rollback, ROLLBACK_FIELDS, f"{filename}.rollback_drill")
        return
    if filename == "approval.json":
        _exact_keys(value, APPROVAL_FIELDS, filename)
        for index, raw_item in enumerate(
            _list(value.get("approvals"), f"{filename}.approvals")
        ):
            location = f"{filename}.approvals[{index}]"
            _exact_keys(_mapping(raw_item, location), APPROVAL_RECORD_FIELDS, location)
        for index, raw_item in enumerate(
            _list(value.get("source_right_scope"), f"{filename}.source_right_scope")
        ):
            location = f"{filename}.source_right_scope[{index}]"
            _exact_keys(_mapping(raw_item, location), SOURCE_RIGHT_FIELDS, location)
        return
    raise EvidenceBundleError(f"unexpected evidence filename: {filename}")


def _validate_review_selection(
    selection: Mapping[str, object],
    human_review: Mapping[str, object],
    *,
    expected_revision: str,
) -> None:
    _exact_keys(
        selection,
        {
            "schema_version",
            "kind",
            "code_revision",
            "automated_evidence_sha256",
            "review_candidate_export_sha256",
            "event_candidates",
            "same_event_pair_candidates",
            "top5_candidates",
        },
        "review-candidates.json",
    )
    if selection.get("schema_version") != SCHEMA_VERSION:
        raise EvidenceBundleError("review-candidates.json: schema version mismatch")
    if selection.get("kind") != REVIEW_SELECTION_KIND:
        raise EvidenceBundleError("review-candidates.json: unexpected kind")
    if (
        _revision(
            selection.get("code_revision"),
            "code_revision",
            "review-candidates.json",
        )
        != expected_revision
    ):
        raise EvidenceBundleError("review-candidates.json: revision mismatch")
    _digest(
        selection.get("automated_evidence_sha256"),
        "automated_evidence_sha256",
        "review-candidates.json",
    )
    _digest(
        selection.get("review_candidate_export_sha256"),
        "review_candidate_export_sha256",
        "review-candidates.json",
    )
    expected_events = [
        str(_mapping(item, "review-candidates.event[]").get("event_id"))
        for item in _list(
            selection.get("event_candidates"),
            "review-candidates.event_candidates",
        )
    ]
    expected_pairs = [
        (
            str(_mapping(item, "review-candidates.pair[]").get("pair_id")),
            str(_mapping(item, "review-candidates.pair[]").get("left_document_id")),
            str(_mapping(item, "review-candidates.pair[]").get("right_document_id")),
        )
        for item in _list(
            selection.get("same_event_pair_candidates"),
            "review-candidates.same_event_pair_candidates",
        )
    ]
    expected_top5 = [
        (
            str(_mapping(item, "review-candidates.top5[]").get("edition_id")),
            str(_mapping(item, "review-candidates.top5[]").get("event_id")),
        )
        for item in _list(
            selection.get("top5_candidates"),
            "review-candidates.top5_candidates",
        )
    ]
    actual_events = [
        str(_mapping(item, "human-review.event[]").get("event_id"))
        for item in _list(human_review.get("event_reviews"), "human-review.event_reviews")
    ]
    actual_pairs = [
        (
            str(_mapping(item, "human-review.pair[]").get("pair_id")),
            str(_mapping(item, "human-review.pair[]").get("left_document_id")),
            str(_mapping(item, "human-review.pair[]").get("right_document_id")),
        )
        for item in _list(
            human_review.get("same_event_pair_reviews"),
            "human-review.same_event_pair_reviews",
        )
    ]
    actual_top5 = [
        (
            str(_mapping(item, "human-review.top5[]").get("edition_id")),
            str(_mapping(item, "human-review.top5[]").get("event_id")),
        )
        for item in _list(human_review.get("top5_reviews"), "human-review.top5_reviews")
    ]
    if expected_events != actual_events:
        raise EvidenceBundleError(
            "human-review.json: event reviews differ from immutable candidate selection"
        )
    if expected_pairs != actual_pairs:
        raise EvidenceBundleError(
            "human-review.json: pair reviews differ from immutable candidate selection"
        )
    if expected_top5 != actual_top5:
        raise EvidenceBundleError(
            "human-review.json: Top 5 reviews differ from immutable candidate selection"
        )
    if (
        len(actual_events) != EVENT_REVIEW_COUNT
        or len(actual_pairs) != PAIR_REVIEW_COUNT
        or len(actual_top5) != TOP5_REVIEW_COUNT
    ):
        raise EvidenceBundleError("human-review.json: exact 60/120/5 sample required")


def _validate_review_export_binding(
    selection: Mapping[str, object],
    review_export: Mapping[str, object],
) -> None:
    expected_digest = _digest(
        selection.get("review_candidate_export_sha256"),
        "review_candidate_export_sha256",
        "review-candidates.json",
    )
    actual_digest = _sha256(_canonical_bytes(review_export))
    if expected_digest != actual_digest:
        raise EvidenceBundleError(
            "review-candidates.json: original review candidate export changed"
        )
    events, pairs, top5 = _select_review_candidates(review_export)
    regenerated = {
        "event_candidates": events,
        "same_event_pair_candidates": pairs,
        "top5_candidates": top5,
    }
    for field, expected in regenerated.items():
        selected = _list(
            selection.get(field),
            f"review-candidates.json.{field}",
        )
        if _canonical_bytes(selected) != _canonical_bytes(expected):
            raise EvidenceBundleError(
                f"review-candidates.json: {field} differs from deterministic "
                "selection of the original export"
            )


def verify_materialized_review(
    *,
    review_candidate_export_path: Path,
    human_review_path: Path,
    expected_revision: str,
    producer_run_id: object,
    producer_run_attempt: object,
    producer_run_created_at: object,
    artifact_id: object,
    artifact_name: str,
    artifact_digest: str,
    output_path: Path,
    verified_at: datetime | None = None,
) -> dict[str, object]:
    """Bind completed human labels to one immutable producer artifact."""

    location = "verify-materialized-review"
    revision = _revision(expected_revision, "expected_revision", location)
    run_id = _positive_identifier(producer_run_id, "producer_run_id", location)
    run_attempt = _positive_identifier(
        producer_run_attempt,
        "producer_run_attempt",
        location,
    )
    github_artifact_id = _positive_identifier(artifact_id, "artifact_id", location)
    expected_artifact_name = f"global-alpha-review-candidates-{revision}"
    if artifact_name != expected_artifact_name:
        raise EvidenceBundleError(
            f"{location}: artifact_name must be {expected_artifact_name}"
        )
    github_digest = _github_artifact_digest(
        artifact_digest,
        "artifact_digest",
        location,
    )
    checked_at = verified_at or datetime.now(timezone.utc).replace(microsecond=0)
    if checked_at.tzinfo is None:
        raise EvidenceBundleError(f"{location}: verified_at must include a timezone")
    checked_at = checked_at.astimezone(timezone.utc)
    run_created_at = _timestamp(
        producer_run_created_at,
        "producer_run_created_at",
        location,
    )
    if (
        run_created_at > checked_at + timedelta(minutes=1)
        or checked_at - run_created_at > timedelta(hours=72)
    ):
        raise EvidenceBundleError(
            f"{location}: producer run is outside the 72-hour freshness window"
        )
    if review_candidate_export_path.name != REVIEW_EXPORT_FILENAME:
        raise EvidenceBundleError(
            f"{location}: source filename must be {REVIEW_EXPORT_FILENAME}"
        )
    if output_path.exists():
        raise EvidenceBundleError(f"{location}: output already exists: {output_path}")

    try:
        export_raw = review_candidate_export_path.read_bytes()
        human_raw = human_review_path.read_bytes()
    except OSError as exc:
        raise EvidenceBundleError(f"{location}: cannot read review evidence") from exc
    review_export = _strict_json_bytes(export_raw, "review-candidate-export")
    _exact_keys(review_export, REVIEW_EXPORT_FIELDS, "review-candidate-export")
    _validate_common(
        review_export,
        expected_kind=REVIEW_CANDIDATE_EXPORT_KIND,
        expected_revision=revision,
        location="review-candidate-export",
    )
    human_review = _strict_json_bytes(human_raw, "human-review.json")
    _validate_exact_evidence_file("human-review.json", human_review)
    if (
        _revision(
            human_review.get("code_revision"),
            "code_revision",
            "human-review.json",
        )
        != revision
    ):
        raise EvidenceBundleError("human-review.json: revision mismatch")

    events, pairs, top5 = _select_review_candidates(review_export)
    selection = {
        "schema_version": SCHEMA_VERSION,
        "kind": REVIEW_SELECTION_KIND,
        "code_revision": revision,
        "automated_evidence_sha256": "0" * SHA256_RE_LENGTH,
        "review_candidate_export_sha256": _sha256(
            _canonical_bytes(review_export)
        ),
        "event_candidates": events,
        "same_event_pair_candidates": pairs,
        "top5_candidates": top5,
    }
    _validate_review_selection(
        selection,
        human_review,
        expected_revision=revision,
    )
    _validate_review_export_binding(selection, review_export)

    _, human_review_gates = validate_human_review(
        human_review,
        expected_revision=revision,
        evidence_as_of=checked_at,
    )
    _require_passed_gates(human_review_gates, "human-review.json")

    identity_binding = {
        "event_reviews": [
            {"event_id": str(item["event_id"])}
            for item in events
        ],
        "same_event_pair_reviews": [
            {
                "pair_id": str(item["pair_id"]),
                "left_document_id": str(item["left_document_id"]),
                "right_document_id": str(item["right_document_id"]),
            }
            for item in pairs
        ],
        "top5_reviews": [
            {
                "edition_id": str(item["edition_id"]),
                "event_id": str(item["event_id"]),
            }
            for item in top5
        ],
    }
    selected_candidates = {
        "event_candidates": events,
        "same_event_pair_candidates": pairs,
        "top5_candidates": top5,
    }
    report: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "kind": REVIEW_PROVENANCE_KIND,
        "code_revision": revision,
        "verified_at": checked_at.isoformat().replace("+00:00", "Z"),
        "producer_workflow": REVIEW_PRODUCER_WORKFLOW,
        "producer_run_id": run_id,
        "producer_run_attempt": run_attempt,
        "producer_run_created_at": run_created_at.isoformat().replace(
            "+00:00",
            "Z",
        ),
        "artifact_id": github_artifact_id,
        "artifact_name": artifact_name,
        "artifact_digest": github_digest,
        "review_candidate_export_filename": REVIEW_EXPORT_FILENAME,
        "review_candidate_export_bytes": len(export_raw),
        "review_candidate_export_file_sha256": _sha256(export_raw),
        "review_candidate_export_canonical_sha256": _sha256(
            _canonical_bytes(review_export)
        ),
        "human_review_bytes": len(human_raw),
        "human_review_file_sha256": _sha256(human_raw),
        "human_review_canonical_sha256": _sha256(
            _canonical_bytes(human_review)
        ),
        "selected_candidates_canonical_sha256": _sha256(
            _canonical_bytes(selected_candidates)
        ),
        "review_identity_canonical_sha256": _sha256(
            _canonical_bytes(identity_binding)
        ),
        "event_review_count": len(events),
        "same_event_pair_review_count": len(pairs),
        "top5_review_count": len(top5),
        "human_review_verified": True,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _write_json(output_path, report)
    return report


def _relative_artifact_path(value: object, field: str, location: str) -> Path:
    raw = _text(value, field, location)
    pure = PurePosixPath(raw)
    if pure.is_absolute() or ".." in pure.parts or "\\" in raw or raw.endswith("/"):
        raise EvidenceBundleError(f"{location}: {field} must be a safe relative path")
    return Path(*pure.parts)


def _verify_artifact(
    *,
    artifact_root: Path,
    relative_path: Path,
    expected_digest: str,
    location: str,
) -> None:
    root = artifact_root.resolve()
    candidate = (root / relative_path).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise EvidenceBundleError(f"{location}: artifact path escapes root") from exc
    if not candidate.is_file():
        raise EvidenceBundleError(f"{location}: artifact file does not exist")
    actual = _sha256(candidate.read_bytes())
    if actual != expected_digest:
        raise EvidenceBundleError(f"{location}: artifact digest mismatch")


def _validate_experience_manifest(
    manifest: Mapping[str, object],
    *,
    expected_revision: str,
    artifact_root: Path,
) -> None:
    _exact_keys(
        manifest,
        {
            "schema_version",
            "kind",
            "code_revision",
            "release_eligible",
            "viewports",
            "measurements",
            "human_approval",
        },
        "experience-artifact-manifest.json",
    )
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise EvidenceBundleError(
            "experience-artifact-manifest.json: schema version mismatch"
        )
    if manifest.get("kind") != EXPERIENCE_MANIFEST_KIND:
        raise EvidenceBundleError(
            "experience-artifact-manifest.json: unexpected kind"
        )
    if (
        _revision(
            manifest.get("code_revision"),
            "code_revision",
            "experience-artifact-manifest.json",
        )
        != expected_revision
    ):
        raise EvidenceBundleError(
            "experience-artifact-manifest.json: revision mismatch"
        )
    if manifest.get("release_eligible") is not True:
        raise EvidenceBundleError(
            "experience-artifact-manifest.json: explicit human release eligibility required"
        )
    viewports: set[str] = set()
    for index, raw_item in enumerate(
        _list(manifest.get("viewports"), "experience-artifact-manifest.viewports")
    ):
        location = f"experience-artifact-manifest.viewports[{index}]"
        item = _mapping(raw_item, location)
        _exact_keys(
            item,
            {
                "viewport",
                "screenshot_path",
                "screenshot_sha256",
                "axe_report_path",
                "axe_report_sha256",
            },
            location,
        )
        viewport = _text(item.get("viewport"), "viewport", location)
        if viewport in viewports:
            raise EvidenceBundleError(f"{location}: duplicate viewport")
        viewports.add(viewport)
        screenshot_path = _relative_artifact_path(
            item.get("screenshot_path"),
            "screenshot_path",
            location,
        )
        axe_path = _relative_artifact_path(
            item.get("axe_report_path"),
            "axe_report_path",
            location,
        )
        _verify_artifact(
            artifact_root=artifact_root,
            relative_path=screenshot_path,
            expected_digest=_digest(
                item.get("screenshot_sha256"),
                "screenshot_sha256",
                location,
            ),
            location=f"{location}.screenshot",
        )
        _verify_artifact(
            artifact_root=artifact_root,
            relative_path=axe_path,
            expected_digest=_digest(
                item.get("axe_report_sha256"),
                "axe_report_sha256",
                location,
            ),
            location=f"{location}.axe",
        )
    if viewports != EXPECTED_VIEWPORTS:
        raise EvidenceBundleError(
            "experience-artifact-manifest.json: exact viewport set required"
        )
    measurement_names: set[str] = set()
    for index, raw_item in enumerate(
        _list(
            manifest.get("measurements"),
            "experience-artifact-manifest.measurements",
        )
    ):
        location = f"experience-artifact-manifest.measurements[{index}]"
        item = _mapping(raw_item, location)
        _exact_keys(item, {"name", "path", "sha256"}, location)
        name = _text(item.get("name"), "name", location)
        if name in measurement_names:
            raise EvidenceBundleError(f"{location}: duplicate measurement")
        measurement_names.add(name)
        path = _relative_artifact_path(item.get("path"), "path", location)
        _verify_artifact(
            artifact_root=artifact_root,
            relative_path=path,
            expected_digest=_digest(item.get("sha256"), "sha256", location),
            location=location,
        )
    if measurement_names != set(EXPERIENCE_MEASUREMENTS):
        raise EvidenceBundleError(
            "experience-artifact-manifest.json: exact measurement artifact set required"
        )
    approval = _mapping(
        manifest.get("human_approval"),
        "experience-artifact-manifest.human_approval",
    )
    _exact_keys(
        approval,
        {"decision", "approver_type", "approver_reference", "decided_at"},
        "experience-artifact-manifest.human_approval",
    )
    if (
        approval.get("decision") != "approved"
        or approval.get("approver_type") != "human"
    ):
        raise EvidenceBundleError(
            "experience-artifact-manifest.json: human approval required"
        )
    _text(
        approval.get("approver_reference"),
        "approver_reference",
        "experience-artifact-manifest.human_approval",
    )
    _timestamp(
        approval.get("decided_at"),
        "decided_at",
        "experience-artifact-manifest.human_approval",
    )


def _validate_authoritative_fields(
    files: Mapping[str, Mapping[str, object]],
    automated: Mapping[str, object],
) -> None:
    if _canonical_bytes(files["content-integrity.json"]) != _canonical_bytes(
        automated["content_integrity"]
    ):
        raise EvidenceBundleError(
            "content-integrity.json: must exactly match the production ops export"
        )
    coverage_by_family = {
        str(_mapping(item, "automated.coverage[]").get("connector_family")): _mapping(
            item,
            "automated.coverage[]",
        )
        for item in _list(
            automated.get("connector_coverage"),
            "automated.connector_coverage",
        )
    }
    for raw_item in _list(
        files["connector-idempotency.json"].get("connectors"),
        "connector-idempotency.connectors",
    ):
        item = _mapping(raw_item, "connector-idempotency.connectors[]")
        family = str(item.get("connector_family"))
        coverage = coverage_by_family.get(family)
        if coverage is None:
            raise EvidenceBundleError(
                "connector-idempotency.json: unexpected connector family"
            )
        for field in COVERAGE_FIELDS - {"connector_family", "country"}:
            if _canonical_bytes(item.get(field)) != _canonical_bytes(
                coverage.get(field)
            ):
                raise EvidenceBundleError(
                    f"connector-idempotency.json: {family}.{field} "
                    "does not match the production ops export"
                )


def _require_passed_gates(
    gates: Sequence[Mapping[str, object]],
    location: str,
) -> None:
    failed = [
        str(gate.get("name"))
        for gate in gates
        if gate.get("passed") is not True
    ]
    if failed:
        raise EvidenceBundleError(f"{location}: failed release gates: {failed}")


def _deterministic_gzip(raw: bytes) -> bytes:
    output = io.BytesIO()
    with gzip.GzipFile(
        filename="",
        mode="wb",
        compresslevel=9,
        fileobj=output,
        mtime=0,
    ) as bundle:
        bundle.write(raw)
    return output.getvalue()


def decode_input_bundle(encoded: str) -> dict[str, object]:
    encoded_text = encoded.strip()
    if not encoded_text:
        raise EvidenceBundleError("input bundle is empty")
    if len(encoded_text.encode("ascii", errors="ignore")) > MAX_SECRET_BYTES:
        raise EvidenceBundleError("encoded input bundle exceeds 48KB")
    try:
        compressed = base64.b64decode(encoded_text, validate=True)
    except (ValueError, TypeError) as exc:
        raise EvidenceBundleError("input bundle is not valid base64") from exc
    if len(compressed) > MAX_SECRET_BYTES:
        raise EvidenceBundleError("compressed input bundle exceeds 48KB")
    try:
        inflater = zlib.decompressobj(16 + zlib.MAX_WBITS)
        raw = inflater.decompress(compressed, MAX_UNCOMPRESSED_BYTES + 1)
        if len(raw) > MAX_UNCOMPRESSED_BYTES or inflater.unconsumed_tail:
            raise EvidenceBundleError("input bundle exceeds 2MB decompressed")
        raw += inflater.flush(MAX_UNCOMPRESSED_BYTES + 1 - len(raw))
    except zlib.error as exc:
        raise EvidenceBundleError("input bundle is not valid gzip") from exc
    if (
        len(raw) > MAX_UNCOMPRESSED_BYTES
        or not inflater.eof
        or inflater.unused_data
    ):
        raise EvidenceBundleError(
            "input bundle gzip is truncated, concatenated, or has trailing data"
        )
    bundle = _strict_json_bytes(raw, "input-bundle")
    _exact_keys(
        bundle,
        {"schema_version", "kind", "code_revision", "files"},
        "input-bundle",
    )
    if bundle.get("schema_version") != SCHEMA_VERSION:
        raise EvidenceBundleError("input-bundle: schema version mismatch")
    if bundle.get("kind") != INPUT_BUNDLE_KIND:
        raise EvidenceBundleError("input-bundle: unexpected kind")
    files = _mapping(bundle.get("files"), "input-bundle.files")
    if set(files) != set(INPUT_FILENAMES):
        raise EvidenceBundleError("input-bundle: exact five evidence files required")
    for filename in INPUT_FILENAMES:
        _validate_exact_evidence_file(
            filename,
            _mapping(files.get(filename), f"input-bundle.files.{filename}"),
        )
    return bundle


def finalize_bundle(
    *,
    input_dir: Path,
    automated_evidence_path: Path,
    review_candidate_export_path: Path,
    experience_artifact_root: Path,
    expected_revision: str,
    evidence_as_of: datetime,
) -> tuple[str, dict[str, object]]:
    revision = _revision(
        expected_revision,
        "expected_revision",
        "finalize",
    )
    if evidence_as_of.tzinfo is None:
        raise EvidenceBundleError("finalize: evidence_as_of must include a timezone")
    as_of = evidence_as_of.astimezone(timezone.utc)
    automated, automated_response, _ = _parse_automated_evidence(
        automated_evidence_path,
        expected_revision=revision,
    )
    review_export, _ = _parse_review_export(
        review_candidate_export_path,
        expected_revision=revision,
    )
    files: dict[str, Mapping[str, object]] = {}
    for filename in INPUT_FILENAMES:
        value = _load_json(input_dir / filename, filename)
        _validate_exact_evidence_file(filename, value)
        if (
            _revision(value.get("code_revision"), "code_revision", filename)
            != revision
        ):
            raise EvidenceBundleError(f"{filename}: revision mismatch")
        files[filename] = value
    selection = _load_json(
        input_dir / "review-candidates.json",
        "review-candidates.json",
    )
    _validate_review_selection(
        selection,
        files["human-review.json"],
        expected_revision=revision,
    )
    _validate_review_export_binding(selection, review_export)
    expected_automated_digest = _digest(
        selection.get("automated_evidence_sha256"),
        "automated_evidence_sha256",
        "review-candidates.json",
    )
    if expected_automated_digest != _sha256(_canonical_bytes(automated_response)):
        raise EvidenceBundleError(
            "review-candidates.json: automated evidence export changed"
        )
    experience_manifest = _load_json(
        input_dir / "experience-artifact-manifest.json",
        "experience-artifact-manifest.json",
    )
    _validate_experience_manifest(
        experience_manifest,
        expected_revision=revision,
        artifact_root=experience_artifact_root,
    )
    _validate_authoritative_fields(files, automated)
    bundle = {
        "schema_version": SCHEMA_VERSION,
        "kind": INPUT_BUNDLE_KIND,
        "code_revision": revision,
        "files": files,
    }
    raw = _canonical_bytes(bundle)
    if len(raw) > MAX_UNCOMPRESSED_BYTES:
        raise EvidenceBundleError("input bundle exceeds 2MB uncompressed")
    compressed = _deterministic_gzip(raw)
    if len(compressed) > MAX_SECRET_BYTES:
        raise EvidenceBundleError("input bundle exceeds 48KB compressed")
    encoded = base64.b64encode(compressed).decode("ascii")
    if len(encoded.encode("ascii")) > MAX_SECRET_BYTES:
        raise EvidenceBundleError("input bundle exceeds the 48KB secret budget")
    decoded = decode_input_bundle(encoded)
    if _canonical_bytes(decoded) != raw:
        raise EvidenceBundleError("input bundle deterministic round-trip failed")

    with tempfile.TemporaryDirectory(prefix="bside-alpha-evidence-") as temp_dir:
        materialized_dir = Path(temp_dir) / "materialized"
        materialize_input_bundle(
            encoded,
            output_dir=materialized_dir,
            expected_revision=revision,
            automated_evidence_path=automated_evidence_path,
        )
        materialized = {
            filename: _load_json(
                materialized_dir / filename,
                f"materialized.{filename}",
            )
            for filename in INPUT_FILENAMES
        }
    for filename, value in materialized.items():
        _validate_exact_evidence_file(filename, value)
    _, connector_gates = validate_connector_idempotency(
        materialized["connector-idempotency.json"],
        expected_revision=revision,
        evidence_as_of=as_of,
    )
    _, review_gates = validate_human_review(
        materialized["human-review.json"],
        expected_revision=revision,
        evidence_as_of=as_of,
    )
    _, content_gates = validate_content_integrity(
        materialized["content-integrity.json"],
        expected_revision=revision,
        evidence_as_of=as_of,
    )
    _, experience_gates = validate_experience(
        materialized["experience.json"],
        expected_revision=revision,
        evidence_as_of=as_of,
    )
    _, approval_gates = validate_approval(
        materialized["approval.json"],
        expected_revision=revision,
        evidence_as_of=as_of,
    )
    _require_passed_gates(connector_gates, "connector-idempotency.json")
    _require_passed_gates(review_gates, "human-review.json")
    _require_passed_gates(content_gates, "content-integrity.json")
    _require_passed_gates(experience_gates, "experience.json")
    _require_passed_gates(approval_gates, "approval.json")
    summary = {
        "status": "global-alpha-release-inputs-ready",
        "code_revision": revision,
        "uncompressed_bytes": len(raw),
        "compressed_bytes": len(compressed),
        "encoded_bytes": len(encoded.encode("ascii")),
        "bundle_sha256": _sha256(raw),
        "experience_manifest_sha256": _sha256(
            _canonical_bytes(experience_manifest)
        ),
        "event_review_count": EVENT_REVIEW_COUNT,
        "same_event_pair_review_count": PAIR_REVIEW_COUNT,
        "top5_review_count": TOP5_REVIEW_COUNT,
    }
    return encoded, summary


def _parse_evidence_as_of(value: str) -> datetime:
    return _timestamp(value, "evidence_as_of", "finalize")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Prepare blank human-review candidates and finalize the exact "
            "Production Alpha protected evidence secret."
        )
    )
    commands = parser.add_subparsers(dest="command")
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--automated-evidence", type=Path, required=True)
    prepare.add_argument("--review-candidate-export", type=Path, required=True)
    prepare.add_argument("--output-dir", type=Path, required=True)
    prepare.add_argument("--expected-revision", required=True)
    finalize = commands.add_parser("finalize")
    finalize.add_argument("--input-dir", type=Path, required=True)
    finalize.add_argument("--automated-evidence", type=Path, required=True)
    finalize.add_argument("--review-candidate-export", type=Path, required=True)
    finalize.add_argument("--experience-artifact-root", type=Path, required=True)
    finalize.add_argument("--expected-revision", required=True)
    finalize.add_argument("--evidence-as-of", required=True)
    finalize.add_argument("--output", type=Path, required=True)
    verify_review = commands.add_parser("verify-materialized-review")
    verify_review.add_argument(
        "--review-candidate-export",
        type=Path,
        required=True,
    )
    verify_review.add_argument("--human-review", type=Path, required=True)
    verify_review.add_argument("--expected-revision", required=True)
    verify_review.add_argument("--producer-run-id", required=True)
    verify_review.add_argument("--producer-run-attempt", required=True)
    verify_review.add_argument("--producer-run-created-at", required=True)
    verify_review.add_argument("--artifact-id", required=True)
    verify_review.add_argument("--artifact-name", required=True)
    verify_review.add_argument("--artifact-digest", required=True)
    verify_review.add_argument("--output", type=Path, required=True)
    verify = commands.add_parser("verify-encoded")
    verify.add_argument("--encoded-file", type=Path, required=True)
    verify.add_argument("--expected-revision", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        if args.command == "prepare":
            manifest = prepare_candidate_bundle(
                automated_evidence_path=args.automated_evidence,
                review_candidate_export_path=args.review_candidate_export,
                output_dir=args.output_dir,
                expected_revision=args.expected_revision,
            )
            print(json.dumps(manifest, ensure_ascii=False, sort_keys=True))
            return 0
        if args.command == "finalize":
            encoded, summary = finalize_bundle(
                input_dir=args.input_dir,
                automated_evidence_path=args.automated_evidence,
                review_candidate_export_path=args.review_candidate_export,
                experience_artifact_root=args.experience_artifact_root,
                expected_revision=args.expected_revision,
                evidence_as_of=_parse_evidence_as_of(args.evidence_as_of),
            )
            if args.output.exists():
                raise EvidenceBundleError(
                    f"finalize: output already exists: {args.output}"
                )
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(encoded + "\n", encoding="ascii", newline="\n")
            print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
            return 0
        if args.command == "verify-materialized-review":
            report = verify_materialized_review(
                review_candidate_export_path=args.review_candidate_export,
                human_review_path=args.human_review,
                expected_revision=args.expected_revision,
                producer_run_id=args.producer_run_id,
                producer_run_attempt=args.producer_run_attempt,
                producer_run_created_at=args.producer_run_created_at,
                artifact_id=args.artifact_id,
                artifact_name=args.artifact_name,
                artifact_digest=args.artifact_digest,
                output_path=args.output,
            )
            print(json.dumps(report, ensure_ascii=False, sort_keys=True))
            return 0
        if args.command == "verify-encoded":
            try:
                encoded = args.encoded_file.read_text(encoding="ascii")
            except OSError as exc:
                raise EvidenceBundleError(
                    f"verify-encoded: cannot read {args.encoded_file}"
                ) from exc
            bundle = decode_input_bundle(encoded)
            revision = _revision(
                bundle.get("code_revision"),
                "code_revision",
                "input-bundle",
            )
            if revision != _revision(
                args.expected_revision,
                "expected_revision",
                "verify-encoded",
            ):
                raise EvidenceBundleError("verify-encoded: revision mismatch")
            print(
                json.dumps(
                    {
                        "status": "valid-encoded-input-bundle",
                        "code_revision": revision,
                    },
                    sort_keys=True,
                )
            )
            return 0
        build_arg_parser().error("a command is required")
    except (EvidenceBundleError, AlphaReleaseEvidenceError) as exc:
        print(
            json.dumps(
                {"status": "invalid-global-alpha-evidence", "error": str(exc)},
                ensure_ascii=False,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
