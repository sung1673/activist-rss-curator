from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from curator.expedited_legacy_compat import (
    PINNED_SNAPSHOT_ARTIFACT_DIGEST,
    PINNED_SNAPSHOT_ARTIFACT_ID,
    PINNED_SNAPSHOT_ARTIFACT_NAME,
    PINNED_SNAPSHOT_CODE_REVISION,
    PINNED_SNAPSHOT_DAY_COUNT,
    PINNED_SNAPSHOT_MAX_PAGE_BYTES,
    PINNED_SNAPSHOT_MIN_PAGE_BYTES,
    PINNED_SNAPSHOT_MODE,
    PINNED_SNAPSHOT_RUN_ID,
    PINNED_SNAPSHOT_WAIVER,
)
from curator.global_alpha_expedited_gate import (
    APPROVAL_KIND,
    CONNECTOR_KIND,
    INPUT_KIND,
    LEGACY_ARCHIVE_KIND,
    RELEASE_CHANNEL,
    REPORT_KIND,
    SOURCE_READINESS_KIND,
    ExpeditedAlphaEvidenceError,
    build_expedited_release_report,
    main,
    validate_connector_receipts,
    validate_editorial_canonical_event_targets,
)
from curator.global_alpha_pages_identity import build_terminal_content_identity


REVISION = "a" * 40
OTHER_REVISION = "b" * 40
DIGEST = "c" * 64
AS_OF = datetime(2026, 7, 28, 19, 35, tzinfo=timezone.utc)
WINDOW_START = date(2026, 6, 28)
TERMINAL_CONTENT = build_terminal_content_identity(
    {
        "index.html": (
            '<main id="app"></main><a data-nav="today"></a>'
            '<script src="./config.js"></script>'
            '<script src="./app.js"></script>'
        ).encode(),
        "config.js": (
            "window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze("
            f'{{"apiBase":"https://example.invalid/api/v2",'
            f'"buildSha":"{REVISION}",'
            f'"releaseChannel":"{RELEASE_CHANNEL}"}});\n'
        ).encode(),
        "app.js": b"window.__BSIDE_ALPHA_APP__=true;\n",
        "styles.css": b":root{color:#111827}\n",
    }
)


def provenance(kind: str, *, as_of: datetime = AS_OF) -> dict[str, object]:
    return {
        "schema_version": 1,
        "kind": kind,
        "environment": "production",
        "evidence_source": "protected_production_export",
        "is_synthetic": False,
        "code_revision": REVISION,
        "collected_at": as_of.isoformat(),
    }


def digest(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def json_digest(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def source(
    country: str,
    *,
    readiness: bool = False,
) -> dict[str, object]:
    identities = {
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
    source_right, connector, coverage = identities[country]
    unavailable = country in {"JP", "GB"}
    result: dict[str, object] = {
        "connector_id": connector,
        "country": country,
        "coverage_mode": coverage,
        "public_status": "coverage_unavailable" if unavailable else "active",
        "public_ready": not unavailable,
        "raw_count": 0 if unavailable else 4,
        "acknowledged_count": 0 if unavailable else 4,
    }
    if readiness:
        result.update(
            {
                "source_right_id": source_right,
                "source_right_valid": True,
            }
        )
    return result


def observation(
    index: int,
    *,
    as_of: datetime = AS_OF,
) -> dict[str, object]:
    observed_at = as_of - timedelta(minutes=30) + timedelta(minutes=index * 5)
    probe = {
        "http_status": 200,
        "transport_succeeded": True,
        "contract_valid": True,
        "error_class": None,
    }
    probes = {
        name: dict(probe)
        for name in (
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
    }
    probes["event_detail"] = {
        "skipped": True,
        "reason": "no_live_event_available",
    }
    return {
        "schema_version": 1,
        "observation_id": f"global-alpha:{index:048x}",
        "observed_at": observed_at.isoformat(),
        "status": "healthy",
        "reasons": [],
        "warnings": [],
        "pipeline_mode": "shadow",
        "web_surface": "governance-preview",
        "release_state": "preview",
        "release_state_version": 4,
        "workflow_revision": REVISION,
        "deployed_build_sha": REVISION,
        "api_code_revision": REVISION,
        "deployed_api_base": "https://alignpe.gabia.io/activist/api.php/api/v2",
        "terminal_content": deepcopy(TERMINAL_CONTENT),
        "probes": probes,
        "sources": [
            source(country)
            for country in ("KR", "US", "JP", "GB", "CA", "AU")
        ],
    }


def receipt_window(
    family: str,
    mode: str,
    index: int,
) -> dict[str, object]:
    start = WINDOW_START + timedelta(days=index)
    end = start + timedelta(days=1)
    return {
        "window_start": start.isoformat(),
        "window_end_exclusive": end.isoformat(),
        "status": "complete",
        "code_revision": REVISION,
        "raw_count": 3,
        "filtered_out_count": 1,
        "accepted_count": 2,
        "acknowledged_count": 2,
        "payload_sha256": digest(f"{family}:{start}:payload"),
        "receipt_sha256": digest(f"{family}:{mode}:{start}:receipt"),
        "idempotency_key": f"{family}:{start}:production",
        "ingest_id": f"{family}:{start}:ingest",
        "idempotent": mode == "replay",
        "replay_verified": mode == "replay",
    }


def connector_run(family: str, mode: str) -> dict[str, object]:
    result: dict[str, object] = {
        "run_id": 100 if mode == "apply" else 101,
        "artifact_id": 200 if mode == "apply" else 201,
        "artifact_name": f"global-backfill-{family}-{mode}-production",
        "artifact_sha256": digest(f"{family}:{mode}:artifact"),
        "status": "succeeded",
        "code_revision": REVISION,
        "windows": [
            receipt_window(family, mode, index) for index in range(30)
        ],
    }
    if family == "dart":
        result.update(
            {
                "execution_window_count": 30,
                "preexisting_window_count": 0 if mode == "apply" else 30,
                "evidenced_window_count": 30,
            }
        )
        result["frozen_bundle_manifest_sha256"] = digest(
            "dart:frozen-bundle-manifest"
        )
        if mode == "replay":
            result.update(
                {
                    "frozen_artifact_binding_sha256": digest(
                        "dart:frozen-artifact-binding"
                    ),
                    "source_network_accessed": False,
                    "fresh_drift_probe": {
                        "status": "matched",
                        "release_gate_policy": (
                            "stable-public-payload-source-count-diagnostic-v1"
                        ),
                        "release_gate_matched": True,
                        "diagnostic_only_window_count": 0,
                        "blocking_drift_window_count": 0,
                        "sha256": digest("dart:fresh-drift-probe"),
                        "read_only": True,
                        "governance_write_attempted": False,
                        "checkpoint_write_attempted": False,
                        "quota_ledger_write_attempted": True,
                    },
                }
            )
    return result


def connector_report(*, as_of: datetime = AS_OF) -> dict[str, object]:
    result = provenance(CONNECTOR_KIND, as_of=as_of)
    result["connectors"] = [
        {
            "connector_family": family,
            "country": country,
            "apply_run": connector_run(family, "apply"),
            "replay_run": connector_run(family, "replay"),
        }
        for family, country in (("dart", "KR"), ("sec-edgar", "US"))
    ]
    return result


def retime_connector_windows(
    connector: dict[str, object],
    *,
    start: date,
) -> None:
    family = str(connector["connector_family"])
    for mode in ("apply", "replay"):
        run = connector[f"{mode}_run"]  # type: ignore[index]
        for index, item in enumerate(run["windows"]):  # type: ignore[index]
            day = start + timedelta(days=index)
            item.update(
                {
                    "window_start": day.isoformat(),
                    "window_end_exclusive": (day + timedelta(days=1)).isoformat(),
                    "payload_sha256": digest(f"{family}:{day}:payload"),
                    "receipt_sha256": digest(
                        f"{family}:{mode}:{day}:receipt"
                    ),
                    "idempotency_key": f"{family}:{day}:production",
                    "ingest_id": f"{family}:{day}:ingest",
                }
            )


def test_sec_horizon_uses_latest_provable_business_day_on_weekend() -> None:
    as_of = datetime.fromisoformat("2026-08-02T10:30:00+00:00")
    report = connector_report(as_of=as_of)
    connectors = report["connectors"]  # type: ignore[assignment]
    retime_connector_windows(connectors[0], start=date(2026, 7, 3))
    retime_connector_windows(connectors[1], start=date(2026, 7, 2))

    summary, gates = validate_connector_receipts(
        report,
        expected_revision=REVISION,
        evidence_as_of=as_of,
    )

    assert gate({"gates": gates}, "expedited_connectors.current_30_day_horizon")[
        "passed"
    ] is True
    sec = next(
        item
        for item in summary["connectors"]  # type: ignore[index]
        if item["connector_family"] == "sec-edgar"
    )
    assert sec["apply_run"]["ended_on_exclusive"] == "2026-08-01"


def test_sec_weekend_horizon_stays_fail_closed_for_stale_or_future_end() -> None:
    as_of = datetime.fromisoformat("2026-08-02T10:30:00+00:00")
    for sec_start in (date(2026, 7, 1), date(2026, 7, 3)):
        report = connector_report(as_of=as_of)
        connectors = report["connectors"]  # type: ignore[assignment]
        retime_connector_windows(connectors[0], start=date(2026, 7, 3))
        retime_connector_windows(connectors[1], start=sec_start)
        _, gates = validate_connector_receipts(
            report,
            expected_revision=REVISION,
            evidence_as_of=as_of,
        )

        assert gate(
            {"gates": gates},
            "expedited_connectors.current_30_day_horizon",
        )["passed"] is False


def test_dart_horizon_keeps_the_existing_24_hour_limit_on_weekend() -> None:
    as_of = datetime.fromisoformat("2026-08-02T10:30:00+00:00")
    report = connector_report(as_of=as_of)
    connectors = report["connectors"]  # type: ignore[assignment]
    retime_connector_windows(connectors[0], start=date(2026, 7, 2))
    retime_connector_windows(connectors[1], start=date(2026, 7, 2))
    _, gates = validate_connector_receipts(
        report,
        expected_revision=REVISION,
        evidence_as_of=as_of,
    )

    assert gate({"gates": gates}, "expedited_connectors.current_30_day_horizon")[
        "passed"
    ] is False


def source_readiness(*, as_of: datetime = AS_OF) -> dict[str, object]:
    result = provenance(SOURCE_READINESS_KIND, as_of=as_of)
    result["sources"] = [
        source(country, readiness=True)
        for country in ("KR", "US", "JP", "GB", "CA", "AU")
    ]
    return result


def human_review(*, as_of: datetime = AS_OF) -> dict[str, object]:
    result = provenance("bside-global-alpha-human-review", as_of=as_of)
    event_reviews = [
        {
            "event_id": f"event-{index}",
            "decision": "approved",
            "reviewer_type": "human",
            "reviewer_reference": "oversight-1",
            "reviewed_at": as_of.isoformat(),
        }
        for index in range(20)
    ]
    pair_reviews = [
        {
            "pair_id": f"pair-{index}",
            "left_document_id": f"left-{index}",
            "right_document_id": f"right-{index}",
            "decision": index % 2 == 0,
            "reviewer_type": "human",
            "reviewer_reference": "oversight-1",
            "reviewed_at": as_of.isoformat(),
        }
        for index in range(40)
    ]
    top5 = [
        {
            "edition_id": "brief-global-20260728",
            "event_id": f"event-{index}",
            "decision": "approved",
            "reviewer_type": "human",
            "reviewer_reference": "oversight-1",
            "reviewed_at": as_of.isoformat(),
            "official_evidence_count": 1,
            "public_eligible": True,
            "event_evidence_sha256": digest(f"event-{index}:evidence"),
        }
        for index in range(5)
    ]
    section = {
        "ground_truth_source": "human",
        "ai_generated_ground_truth": False,
        "human_attestation": True,
        "raw_counts": {
            "event_review_count": len(event_reviews),
            "same_event_pair_review_count": len(pair_reviews),
            "top5_human_reviewed_count": len(top5),
            "top5_published_count": len(top5),
        },
        "event_reviews": event_reviews,
        "same_event_pair_reviews": pair_reviews,
        "top5_reviews": top5,
    }
    result.update(section)
    result.update(
        {
            "artifact_id": 7101,
            "artifact_name": "human-review-production",
            "artifact_sha256": digest("human-review-artifact"),
            "section_sha256": json_digest(section),
            "carry_forward": {
                "human_approval_chain_sha256": digest(
                    "human-approval-chain"
                ),
            },
        }
    )
    return result


def content_integrity(*, as_of: datetime = AS_OF) -> dict[str, object]:
    result = provenance("bside-global-alpha-content-integrity", as_of=as_of)
    result["raw_counts"] = {
        "public_event_count": 20,
        "original_language_preserved_count": 20,
        "official_url_preserved_count": 20,
        "title_provenance_labeled_count": 20,
        "source_title_event_count": 18,
        "source_title_preserved_count": 18,
        "generated_metadata_title_count": 1,
        "operator_metadata_title_count": 1,
        "unknown_title_provenance_count": 0,
        "scanned_response_count": 180,
        "telegram_exposure_count": 0,
        "internal_field_exposure_count": 0,
        "persisted_snapshot_forbidden_key_count": 0,
    }
    return result


def experience(*, as_of: datetime = AS_OF) -> dict[str, object]:
    result = provenance("bside-global-alpha-experience", as_of=as_of)
    rollback_started = as_of - timedelta(minutes=9)
    rollback_receipt = {
        "schema_version": 1,
        "kind": "bside-expedited-actual-rollback-drill",
        "code_revision": REVISION,
        "status": "succeeded",
        "started_at": rollback_started.isoformat(),
        "legacy_deployed_at": as_of.isoformat(),
        "preview_restored_at": as_of.isoformat(),
        "duration_seconds": 540,
        "duration_minutes": 9,
        "succeeded": True,
        "legacy_artifact_sha256": DIGEST,
        "preview_restore_verified": True,
        "preview_terminal_content_sha256": json_digest(TERMINAL_CONTENT),
    }
    result.update(
        {
            "viewports": [
                {
                    "viewport": viewport,
                    "visual_regression_passed": True,
                    "axe_serious_count": 0,
                    "axe_critical_count": 0,
                    **(
                        {"first_important_event_top_px": 250}
                        if viewport == "390x844"
                        else {}
                    ),
                }
                for viewport in ("390x844", "768x1024", "1440x900")
            ],
            "web_vitals": {
                "lcp": {"p75_seconds": 2.1, "sample_count": 5},
                "inp": {"p75_ms": 150, "sample_count": 5},
                "cls": {"p75": 0.05, "sample_count": 5},
            },
            "api_responses": [
                {"route": route, "size_bytes": 120_000, "http_status": 200}
                for route in (
                    "/briefs/latest?edition=global",
                    "/live?limit=100",
                    "/events?limit=100",
                    "/issuers?limit=100",
                    "/calendar?limit=100",
                    "/search?q=capital",
                    "/sources/status",
                    "/exports/events.json?limit=100",
                    "/exports/events.csv?limit=100",
                    "/feeds/events.atom?limit=100",
                )
            ],
            "failure_detection_drill": {
                "incident_started_at": (
                    as_of - timedelta(minutes=7)
                ).isoformat(),
                "detected_at": as_of.isoformat(),
                "detection_minutes": 7,
            },
            "rollback_drill": {
                "succeeded": True,
                "duration_minutes": 9,
                "started_at": rollback_started.isoformat(),
                "completed_at": as_of.isoformat(),
                "legacy_artifact_sha256": DIGEST,
            },
            "rollback_drill_receipt": rollback_receipt,
        }
    )
    return result


def approval(
    *,
    as_of: datetime = AS_OF,
    evidence_binding: dict[str, object],
) -> dict[str, object]:
    result = provenance(APPROVAL_KIND, as_of=as_of)
    section = {
        "release_tier_acknowledged": "production-alpha-early-access",
        "ga_certification_claimed": False,
        "expedited_waiver_acknowledged": True,
        "evidence_binding": evidence_binding,
        "approvals": [
            {
                "role": role,
                "decision": "approved",
                "approver_type": "human",
                "approver_reference": "oversight-1",
                "decided_at": as_of.isoformat(),
                "evidence_sha256": evidence_binding["binding_sha256"],
            }
            for role in ("oversight", "source-rights", "expedited-risk")
        ],
    }
    result.update(section)
    result.update(
        {
            "artifact_id": 7201,
            "artifact_name": "release-approval-production",
            "artifact_sha256": digest("release-approval-artifact"),
            "section_sha256": json_digest(section),
        }
    )
    return result


def legacy_archive(
    *,
    as_of: datetime = AS_OF,
    day_count: int = 89,
) -> dict[str, object]:
    result = provenance(LEGACY_ARCHIVE_KIND, as_of=as_of)
    first = date(2026, 5, 1)
    last = first + timedelta(days=day_count - 1)
    waiver = (
        {
            "exception_id": (
                "production-alpha-early-access-89-day-2026-07-28"
            ),
            "release_channel": RELEASE_CHANNEL,
            "status": "active",
            "approved": True,
            "reviewer_type": "human",
            "reviewer_id": "production-owner",
            "approved_at": (as_of - timedelta(minutes=10)).isoformat(),
            "reason": (
                "Approve the exact immutable 89-day rollback artifact "
                "for Early Access."
            ),
            "ai_generated_ground_truth": False,
            "is_synthetic": False,
            "expires_at": "2026-07-28T20:45:00+00:00",
        }
        if day_count == 89
        else {
            "exception_id": (
                "production-alpha-early-access-89-day-2026-07-28"
            ),
            "status": "not_required",
            "reason": "standard_90_day_window_available",
            "expires_at": "2026-07-28T20:45:00+00:00",
        }
    )
    compatibility_manifest: dict[str, object] = {
        "schema_version": 1,
        "kind": "bside-expedited-legacy-feed-compatibility",
        "mode": (
            "89_day_human_waiver"
            if day_count == 89
            else "standard_90_day"
        ),
        "release_channel": RELEASE_CHANNEL,
        "prepared_at": as_of.isoformat(),
        "source": {
            "run_id": "8001",
            "artifact_id": "9001",
            "artifact_name": "legacy-pages-archive-production",
            "code_revision": "d" * 40,
            "artifact_digest": f"sha256:{DIGEST}",
            "workflow": ".github/workflows/build-feed.yml",
        },
        "window_days": day_count,
        "window_start": first.isoformat(),
        "window_end": last.isoformat(),
        "dated_report_count": day_count,
        "content_sha256": f"sha256:{digest(f'legacy-content:{day_count}')}",
        "waiver": waiver,
    }
    if day_count == 89:
        compatibility_manifest["entire_legacy_site_snapshot"] = True
    else:
        compatibility_manifest["standard_manifest_sha256"] = (
            f"sha256:{digest(f'standard-manifest:{day_count}')}"
        )
    result.update(
        {
            "archive_sha256": DIGEST,
            "artifact_id": 9001,
            "artifact_name": "legacy-pages-archive-production",
            "consecutive_day_count": day_count,
            "first_date": first.isoformat(),
            "last_date": last.isoformat(),
            "generated_at": as_of.isoformat(),
            "contains_placeholder": False,
            "duplicate_content_count": 0,
            "compatibility_manifest": compatibility_manifest,
            "compatibility_manifest_sha256": json_digest(
                compatibility_manifest
            ),
            "waiver_sha256": json_digest(waiver),
        }
    )
    return result


def valid_bundle(
    *,
    as_of: datetime = AS_OF,
    day_count: int = 89,
) -> dict[str, object]:
    result = provenance(INPUT_KIND, as_of=as_of)
    human = human_review(as_of=as_of)
    content = content_integrity(as_of=as_of)
    experience_record = experience(as_of=as_of)
    legacy = legacy_archive(
        as_of=as_of,
        day_count=day_count,
    )
    binding_sections = {
        "human_review_section_sha256": human["section_sha256"],
        "human_approval_chain_sha256": human["carry_forward"][  # type: ignore[index]
            "human_approval_chain_sha256"
        ],
        "legacy_manifest_sha256": legacy[
            "compatibility_manifest_sha256"
        ],
        "pages_terminal_content_sha256": json_digest(TERMINAL_CONTENT),
        "content_integrity_sha256": json_digest(content),
        "experience_sha256": json_digest(experience_record),
        "rollback_drill_sha256": json_digest(
            experience_record["rollback_drill_receipt"]
        ),
        "observations_sha256": json_digest(
            [observation(index, as_of=as_of) for index in range(7)]
        ),
        "legacy_source_artifact_sha256": legacy["archive_sha256"],
    }
    evidence_binding = {
        **binding_sections,
        "binding_sha256": json_digest(binding_sections),
    }
    result.update(
        {
            "evidence_as_of": as_of.isoformat(),
            "release_channel": RELEASE_CHANNEL,
            "observations": [observation(index, as_of=as_of) for index in range(7)],
            "connector_receipts": connector_report(as_of=as_of),
            "source_readiness": source_readiness(as_of=as_of),
            "human_review": human,
            "content_integrity": content,
            "experience": experience_record,
            "approval": approval(
                as_of=as_of,
                evidence_binding=evidence_binding,
            ),
            "legacy_archive": legacy,
        }
    )
    return result


def pinned_snapshot_bundle() -> dict[str, object]:
    as_of = datetime(2026, 8, 4, 10, 0, tzinfo=timezone.utc)
    bundle = valid_bundle(as_of=as_of, day_count=94)
    connectors = bundle["connector_receipts"]["connectors"]  # type: ignore[index]
    for connector in connectors:
        retime_connector_windows(connector, start=date(2026, 7, 5))
    legacy = bundle["legacy_archive"]  # type: ignore[assignment]
    archive_digest = PINNED_SNAPSHOT_ARTIFACT_DIGEST.removeprefix("sha256:")
    legacy.update(  # type: ignore[union-attr]
        {
            "archive_sha256": archive_digest,
            "artifact_id": int(PINNED_SNAPSHOT_ARTIFACT_ID),
            "artifact_name": PINNED_SNAPSHOT_ARTIFACT_NAME,
            "first_date": "2026-05-01",
            "last_date": "2026-08-02",
        }
    )
    manifest = legacy["compatibility_manifest"]  # type: ignore[index]
    manifest.update(
        {
            "mode": PINNED_SNAPSHOT_MODE,
            "source": {
                "run_id": PINNED_SNAPSHOT_RUN_ID,
                "artifact_id": PINNED_SNAPSHOT_ARTIFACT_ID,
                "artifact_name": PINNED_SNAPSHOT_ARTIFACT_NAME,
                "code_revision": PINNED_SNAPSHOT_CODE_REVISION,
                "artifact_digest": PINNED_SNAPSHOT_ARTIFACT_DIGEST,
                "workflow": ".github/workflows/build-feed.yml",
            },
            "complete_legacy_feed_window": True,
            "pinned_snapshot_audit": {
                "actual_dated_report_count": PINNED_SNAPSHOT_DAY_COUNT,
                "actual_window_start": "2026-05-01",
                "actual_window_end": "2026-08-02",
                "gap_count": 0,
                "unique_dated_report_content_count": PINNED_SNAPSHOT_DAY_COUNT,
                "duplicate_content_group_count": 0,
                "contains_placeholder": False,
                "is_synthetic": False,
                "audited_min_page_bytes": PINNED_SNAPSHOT_MIN_PAGE_BYTES,
                "audited_max_page_bytes": PINNED_SNAPSHOT_MAX_PAGE_BYTES,
            },
            "waiver": dict(PINNED_SNAPSHOT_WAIVER),
        }
    )
    manifest.pop("standard_manifest_sha256", None)
    experience_record = bundle["experience"]  # type: ignore[assignment]
    experience_record["rollback_drill"][  # type: ignore[index]
        "legacy_artifact_sha256"
    ] = archive_digest
    rollback = experience_record["rollback_drill_receipt"]  # type: ignore[index]
    rollback["legacy_artifact_sha256"] = archive_digest
    refresh_protected_bindings(bundle)
    return bundle


def refresh_protected_bindings(bundle: dict[str, object]) -> None:
    human = bundle["human_review"]  # type: ignore[assignment]
    human_section = {
        key: human[key]  # type: ignore[index]
        for key in (
            "ground_truth_source",
            "ai_generated_ground_truth",
            "human_attestation",
            "raw_counts",
            "event_reviews",
            "same_event_pair_reviews",
            "top5_reviews",
        )
    }
    human["section_sha256"] = json_digest(human_section)  # type: ignore[index]
    legacy = bundle["legacy_archive"]  # type: ignore[assignment]
    manifest = legacy["compatibility_manifest"]  # type: ignore[index]
    legacy["compatibility_manifest_sha256"] = json_digest(manifest)  # type: ignore[index]
    legacy["waiver_sha256"] = json_digest(manifest["waiver"])  # type: ignore[index]
    observations = bundle["observations"]  # type: ignore[assignment]
    content = bundle["content_integrity"]
    experience_record = bundle["experience"]
    binding_sections = {
        "human_review_section_sha256": human["section_sha256"],  # type: ignore[index]
        "human_approval_chain_sha256": human["carry_forward"][  # type: ignore[index]
            "human_approval_chain_sha256"
        ],
        "legacy_manifest_sha256": legacy[  # type: ignore[index]
            "compatibility_manifest_sha256"
        ],
        "pages_terminal_content_sha256": json_digest(
            observations[0]["terminal_content"]  # type: ignore[index]
        ),
        "content_integrity_sha256": json_digest(content),
        "experience_sha256": json_digest(experience_record),
        "rollback_drill_sha256": json_digest(
            experience_record["rollback_drill_receipt"]  # type: ignore[index]
        ),
        "observations_sha256": json_digest(observations),
        "legacy_source_artifact_sha256": legacy["archive_sha256"],  # type: ignore[index]
    }
    binding = {
        **binding_sections,
        "binding_sha256": json_digest(binding_sections),
    }
    approval = bundle["approval"]  # type: ignore[assignment]
    approval["evidence_binding"] = binding  # type: ignore[index]
    for record in approval["approvals"]:  # type: ignore[index]
        record["evidence_sha256"] = binding["binding_sha256"]
    approval_section = {
        key: approval[key]  # type: ignore[index]
        for key in (
            "release_tier_acknowledged",
            "ga_certification_claimed",
            "expedited_waiver_acknowledged",
            "evidence_binding",
            "approvals",
        )
    }
    approval["section_sha256"] = json_digest(approval_section)  # type: ignore[index]


def gate(report: dict[str, object], name: str) -> dict[str, object]:
    return next(
        value
        for value in report["gates"]  # type: ignore[union-attr]
        if value["name"] == name  # type: ignore[index]
    )


def test_expedited_report_passes_and_is_deterministic() -> None:
    bundle = valid_bundle()
    first = build_expedited_release_report(
        bundle,
        expected_revision=REVISION,
    )
    second = build_expedited_release_report(
        deepcopy(bundle),
        expected_revision=REVISION,
    )

    assert first == second
    assert first["kind"] == REPORT_KIND
    assert first["release_channel"] == RELEASE_CHANNEL
    assert first["release_gate_passed"] is True
    assert first["failed_gates"] == []
    assert first["legacy_archive"]["waiver_used"] is True  # type: ignore[index]
    assert len(first["connector_receipts"]["connectors"]) == 2  # type: ignore[index]
    chain = bundle["human_review"]["carry_forward"][  # type: ignore[index]
        "human_approval_chain_sha256"
    ]
    assert first["human_review"]["human_approval_chain_sha256"] == chain  # type: ignore[index]
    assert first["approval"]["evidence_binding"]["binding_sha256"]  # type: ignore[index]


@pytest.mark.parametrize(
    "mutation",
    ("omitted", "malformed", "uppercase", "mismatched"),
)
def test_expedited_report_fails_closed_on_human_approval_chain(
    mutation: str,
) -> None:
    bundle = valid_bundle()
    human = bundle["human_review"]  # type: ignore[assignment]
    carry_forward = human["carry_forward"]  # type: ignore[index]
    if mutation == "omitted":
        carry_forward.pop("human_approval_chain_sha256")  # type: ignore[union-attr]
    elif mutation == "malformed":
        carry_forward["human_approval_chain_sha256"] = "not-a-digest"  # type: ignore[index]
    elif mutation == "uppercase":
        carry_forward["human_approval_chain_sha256"] = (  # type: ignore[index]
            str(carry_forward["human_approval_chain_sha256"]).upper()  # type: ignore[index]
        )
    else:
        carry_forward["human_approval_chain_sha256"] = "f" * 64  # type: ignore[index]

    with pytest.raises(
        ExpeditedAlphaEvidenceError,
        match=(
            "human_approval_chain_sha256"
            if mutation != "mismatched"
            else "evidence_binding"
        ),
    ):
        build_expedited_release_report(
            bundle,
            expected_revision=REVISION,
        )


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (
            lambda value: value.update({"is_synthetic": True}),
            "synthetic evidence",
        ),
        (
            lambda value: value.update({"code_revision": OTHER_REVISION}),
            "code_revision does not match",
        ),
        (
            lambda value: value["connector_receipts"]["connectors"][0][  # type: ignore[index]
                "apply_run"
            ]["windows"][0].update({"code_revision": OTHER_REVISION}),  # type: ignore[index]
            "code_revision mismatch",
        ),
    ],
)
def test_exact_revision_and_production_provenance_are_required(
    mutate: object,
    match: str,
) -> None:
    bundle = valid_bundle()
    mutate(bundle)  # type: ignore[operator]

    with pytest.raises(ExpeditedAlphaEvidenceError, match=match):
        build_expedited_release_report(bundle, expected_revision=REVISION)


def test_connectors_require_exact_contiguous_matching_30_day_replay() -> None:
    fewer = valid_bundle()
    fewer["connector_receipts"]["connectors"][0]["apply_run"]["windows"].pop()  # type: ignore[index]
    with pytest.raises(ExpeditedAlphaEvidenceError, match="exactly 30"):
        build_expedited_release_report(fewer, expected_revision=REVISION)

    mismatch = valid_bundle()
    mismatch["connector_receipts"]["connectors"][1]["replay_run"]["windows"][5][  # type: ignore[index]
        "accepted_count"
    ] = 1
    mismatch["connector_receipts"]["connectors"][1]["replay_run"]["windows"][5][  # type: ignore[index]
        "filtered_out_count"
    ] = 2
    mismatch["connector_receipts"]["connectors"][1]["replay_run"]["windows"][5][  # type: ignore[index]
        "acknowledged_count"
    ] = 1
    with pytest.raises(ExpeditedAlphaEvidenceError, match="payload differs"):
        build_expedited_release_report(mismatch, expected_revision=REVISION)

    gap = valid_bundle()
    windows = gap["connector_receipts"]["connectors"][0]["replay_run"]["windows"]  # type: ignore[index]
    windows[5]["window_start"] = "2026-07-04"
    windows[5]["window_end_exclusive"] = "2026-07-05"
    with pytest.raises(
        ExpeditedAlphaEvidenceError,
        match="duplicate daily window|gap",
    ):
        build_expedited_release_report(gap, expected_revision=REVISION)


@pytest.mark.parametrize(
    "field",
    ["run_id", "artifact_id", "artifact_name", "artifact_sha256"],
)
def test_connector_apply_and_replay_require_distinct_producers(
    field: str,
) -> None:
    bundle = valid_bundle()
    connector = bundle["connector_receipts"]["connectors"][0]  # type: ignore[index]
    connector["replay_run"][field] = connector["apply_run"][field]
    with pytest.raises(
        ExpeditedAlphaEvidenceError,
        match="reused producer identity fields",
    ):
        build_expedited_release_report(bundle, expected_revision=REVISION)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("source_network_accessed", True, "source network"),
        (
            "frozen_bundle_manifest_sha256",
            digest("different-frozen-manifest"),
            "frozen bundle manifest differs",
        ),
    ],
)
def test_dart_release_requires_exact_offline_frozen_replay(
    field: str,
    value: object,
    message: str,
) -> None:
    bundle = valid_bundle()
    dart = bundle["connector_receipts"]["connectors"][0]  # type: ignore[index]
    dart["replay_run"][field] = value
    with pytest.raises(ExpeditedAlphaEvidenceError, match=message):
        build_expedited_release_report(bundle, expected_revision=REVISION)


def test_dart_release_accepts_transparent_partial_apply_resume() -> None:
    bundle = valid_bundle()
    dart = bundle["connector_receipts"]["connectors"][0]  # type: ignore[index]
    dart["apply_run"]["execution_window_count"] = 12
    dart["apply_run"]["preexisting_window_count"] = 18
    refresh_protected_bindings(bundle)

    report = build_expedited_release_report(bundle, expected_revision=REVISION)

    assert report["release_gate_passed"] is True
    summary = report["connector_receipts"]["connectors"][0]["apply_run"]  # type: ignore[index]
    assert summary["execution_window_count"] == 12
    assert summary["preexisting_window_count"] == 18
    assert summary["evidenced_window_count"] == 30


@pytest.mark.parametrize(
    ("execution", "preexisting", "evidenced"),
    [
        (12, 17, 30),
        (12, 18, 29),
        (0, 29, 30),
    ],
)
def test_dart_release_rejects_unbound_partial_apply_resume(
    execution: int,
    preexisting: int,
    evidenced: int,
) -> None:
    bundle = valid_bundle()
    dart = bundle["connector_receipts"]["connectors"][0]  # type: ignore[index]
    dart["apply_run"]["execution_window_count"] = execution
    dart["apply_run"]["preexisting_window_count"] = preexisting
    dart["apply_run"]["evidenced_window_count"] = evidenced
    with pytest.raises(
        ExpeditedAlphaEvidenceError,
        match="execution and authoritative frozen 30-window evidence",
    ):
        build_expedited_release_report(bundle, expected_revision=REVISION)


def test_dart_release_accepts_complete_checkpoint_after_zero_window_resume() -> None:
    bundle = valid_bundle()
    dart = bundle["connector_receipts"]["connectors"][0]  # type: ignore[index]
    dart["apply_run"]["execution_window_count"] = 0
    dart["apply_run"]["preexisting_window_count"] = 30
    refresh_protected_bindings(bundle)

    report = build_expedited_release_report(bundle, expected_revision=REVISION)

    assert report["release_gate_passed"] is True
    summary = report["connector_receipts"]["connectors"][0]["apply_run"]  # type: ignore[index]
    assert summary["execution_window_count"] == 0
    assert summary["preexisting_window_count"] == 30
    assert summary["evidenced_window_count"] == 30


def test_dart_release_accepts_source_count_only_diagnostic_drift() -> None:
    bundle = valid_bundle()
    dart = bundle["connector_receipts"]["connectors"][0]  # type: ignore[index]
    drift = dart["replay_run"]["fresh_drift_probe"]
    drift["status"] = "drift_detected"
    drift["diagnostic_only_window_count"] = 2
    refresh_protected_bindings(bundle)

    report = build_expedited_release_report(bundle, expected_revision=REVISION)

    assert report["release_gate_passed"] is True
    replay_summary = report["connector_receipts"]["connectors"][0][  # type: ignore[index]
        "replay_run"
    ]
    drift_summary = replay_summary["fresh_drift_probe"]
    assert drift_summary["status"] == "drift_detected"
    assert drift_summary["release_gate_policy"] == (
        "stable-public-payload-source-count-diagnostic-v1"
    )
    assert drift_summary["release_gate_matched"] is True
    assert drift_summary["diagnostic_only_window_count"] == 2
    assert drift_summary["blocking_drift_window_count"] == 0


@pytest.mark.parametrize(
    ("status", "release_gate_matched", "diagnostic_count", "blocking_count"),
    [
        ("probe_failed", False, 0, 0),
        ("drift_detected", False, 0, 1),
        ("drift_detected", True, 0, 0),
        ("matched", True, 1, 0),
    ],
)
def test_dart_release_rejects_unsafe_or_inconsistent_fresh_drift_probe(
    status: str,
    release_gate_matched: bool,
    diagnostic_count: int,
    blocking_count: int,
) -> None:
    bundle = valid_bundle()
    dart = bundle["connector_receipts"]["connectors"][0]  # type: ignore[index]
    drift = dart["replay_run"]["fresh_drift_probe"]
    drift["status"] = status
    drift["release_gate_matched"] = release_gate_matched
    drift["diagnostic_only_window_count"] = diagnostic_count
    drift["blocking_drift_window_count"] = blocking_count
    with pytest.raises(ExpeditedAlphaEvidenceError, match="not release-gate safe"):
        build_expedited_release_report(bundle, expected_revision=REVISION)


def test_dart_release_rejects_unknown_drift_release_gate_policy() -> None:
    bundle = valid_bundle()
    dart = bundle["connector_receipts"]["connectors"][0]  # type: ignore[index]
    dart["replay_run"]["fresh_drift_probe"]["release_gate_policy"] = (
        "unapproved-policy"
    )
    with pytest.raises(ExpeditedAlphaEvidenceError, match="not release-gate safe"):
        build_expedited_release_report(bundle, expected_revision=REVISION)


def test_source_readiness_and_unavailable_markets_fail_closed() -> None:
    bundle = valid_bundle()
    sources = bundle["source_readiness"]["sources"]  # type: ignore[index]
    ca = next(item for item in sources if item["country"] == "CA")
    ca["raw_count"] = 0
    ca["acknowledged_count"] = 0
    report = build_expedited_release_report(bundle, expected_revision=REVISION)
    assert report["release_gate_passed"] is False
    assert gate(report, "expedited_sources.active_ready")["passed"] is False

    unavailable = valid_bundle()
    jp = next(
        item
        for item in unavailable["source_readiness"]["sources"]  # type: ignore[index]
        if item["country"] == "JP"
    )
    jp["public_status"] = "active"
    jp["public_ready"] = True
    report = build_expedited_release_report(
        unavailable,
        expected_revision=REVISION,
    )
    assert gate(report, "expedited_sources.jp_gb_unavailable")["passed"] is False


def test_link_only_ca_au_require_complete_acknowledgement() -> None:
    readiness = valid_bundle()
    ca = next(
        item
        for item in readiness["source_readiness"]["sources"]  # type: ignore[index]
        if item["country"] == "CA"
    )
    ca["acknowledged_count"] = ca["raw_count"] - 1
    with pytest.raises(ExpeditedAlphaEvidenceError, match="raw and acknowledged"):
        build_expedited_release_report(readiness, expected_revision=REVISION)

    observations = valid_bundle()
    au = next(
        item
        for item in observations["observations"][0]["sources"]  # type: ignore[index]
        if item["country"] == "AU"
    )
    au["acknowledged_count"] = au["raw_count"] - 1
    with pytest.raises(ExpeditedAlphaEvidenceError, match="raw and acknowledged"):
        build_expedited_release_report(observations, expected_revision=REVISION)


def test_human_review_requires_20_40_and_exact_approved_top5() -> None:
    bundle = valid_bundle()
    review = bundle["human_review"]  # type: ignore[assignment]
    review["event_reviews"].pop()  # type: ignore[index]
    review["raw_counts"]["event_review_count"] = 19  # type: ignore[index]
    with pytest.raises(ExpeditedAlphaEvidenceError, match="section_sha256"):
        build_expedited_release_report(bundle, expected_revision=REVISION)

    top4 = valid_bundle()
    review = top4["human_review"]  # type: ignore[assignment]
    review["top5_reviews"].pop()  # type: ignore[index]
    review["raw_counts"]["top5_human_reviewed_count"] = 4  # type: ignore[index]
    review["raw_counts"]["top5_published_count"] = 4  # type: ignore[index]
    with pytest.raises(ExpeditedAlphaEvidenceError, match="section_sha256"):
        build_expedited_release_report(top4, expected_revision=REVISION)


def _editorial_canonical_events() -> list[dict[str, object]]:
    return [
        {
            "event_id": f"event:{position:02d}",
            "decision": "approved",
            "issuer_name": f"Issuer {position}",
            "title": f"Official title {position}",
            "identity_target": (
                f"Issuer {position} — Official title {position}"
            ),
            "publication_status": "published",
            "review_status": "approved",
            "identity_status": "complete",
        }
        for position in range(1, 21)
    ]


def _editorial_decisions(
    events: list[dict[str, object]],
) -> list[dict[str, object]]:
    return [
        {
            "event_id": event["event_id"],
            "decision": event["decision"],
        }
        for event in events
    ]


def test_editorial_target_gate_keeps_rejected_candidate_non_public() -> None:
    events = _editorial_canonical_events()
    events[14].update(
        {
            "decision": "rejected",
            "identity_target": None,
            "publication_status": "draft",
            "review_status": "rejected",
            "identity_status": "rejected",
        }
    )
    decisions = _editorial_decisions(events)

    validate_editorial_canonical_event_targets(
        events,
        decision_basis=decisions,
        human_event_reviews=decisions,
    )


def test_editorial_target_gate_rejects_bad_approved_target() -> None:
    events = _editorial_canonical_events()
    events[14]["identity_target"] = None
    decisions = _editorial_decisions(events)

    with pytest.raises(
        ExpeditedAlphaEvidenceError,
        match="approved identity target mismatch",
    ):
        validate_editorial_canonical_event_targets(
            events,
            decision_basis=decisions,
            human_event_reviews=decisions,
        )


def test_editorial_target_gate_rejects_published_rejection() -> None:
    events = _editorial_canonical_events()
    events[14].update(
        {
            "decision": "rejected",
            "identity_target": None,
            "publication_status": "published",
            "review_status": "rejected",
            "identity_status": "rejected",
        }
    )
    decisions = _editorial_decisions(events)

    with pytest.raises(
        ExpeditedAlphaEvidenceError,
        match="escaped the draft boundary",
    ):
        validate_editorial_canonical_event_targets(
            events,
            decision_basis=decisions,
            human_event_reviews=decisions,
        )


def test_editorial_target_gate_binds_canonical_and_human_decisions() -> None:
    events = _editorial_canonical_events()
    decisions = _editorial_decisions(events)
    human_decisions = deepcopy(decisions)
    human_decisions[14]["decision"] = "rejected"

    with pytest.raises(
        ExpeditedAlphaEvidenceError,
        match="do not match human review",
    ):
        validate_editorial_canonical_event_targets(
            events,
            decision_basis=decisions,
            human_event_reviews=human_decisions,
        )


@pytest.mark.parametrize(
    "reviewed_at",
    [
        AS_OF + timedelta(minutes=2),
        AS_OF - timedelta(hours=72, seconds=1),
    ],
)
def test_human_review_timestamp_must_be_recent_and_not_future(
    reviewed_at: datetime,
) -> None:
    bundle = valid_bundle()
    bundle["human_review"]["event_reviews"][0]["reviewed_at"] = (  # type: ignore[index]
        reviewed_at.isoformat()
    )
    refresh_protected_bindings(bundle)
    with pytest.raises(
        ExpeditedAlphaEvidenceError,
        match="after evidence_as_of|older than 72 hours",
    ):
        build_expedited_release_report(bundle, expected_revision=REVISION)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("official_evidence_count", 0),
        ("public_eligible", False),
        ("event_evidence_sha256", "not-a-digest"),
    ],
)
def test_top5_requires_bound_official_public_evidence(
    field: str,
    value: object,
) -> None:
    bundle = valid_bundle()
    bundle["human_review"]["top5_reviews"][0][field] = value  # type: ignore[index]
    refresh_protected_bindings(bundle)
    with pytest.raises(
        ExpeditedAlphaEvidenceError,
        match=(
            "official evidence|official_evidence_count|"
            "event_evidence_sha256"
        ),
    ):
        build_expedited_release_report(bundle, expected_revision=REVISION)


def test_top5_requires_corresponding_approved_event_review() -> None:
    bundle = valid_bundle()
    bundle["human_review"]["event_reviews"][0]["decision"] = "rejected"  # type: ignore[index]
    refresh_protected_bindings(bundle)
    with pytest.raises(
        ExpeditedAlphaEvidenceError,
        match="approved event review",
    ):
        build_expedited_release_report(bundle, expected_revision=REVISION)


def test_approval_digest_must_bind_all_evaluated_sections() -> None:
    bundle = valid_bundle()
    bundle["approval"]["approvals"][0]["evidence_sha256"] = "d" * 64  # type: ignore[index]
    approval = bundle["approval"]  # type: ignore[assignment]
    approval_section = {
        key: approval[key]  # type: ignore[index]
        for key in (
            "release_tier_acknowledged",
            "ga_certification_claimed",
            "expedited_waiver_acknowledged",
            "evidence_binding",
            "approvals",
        )
    }
    approval["section_sha256"] = json_digest(approval_section)  # type: ignore[index]
    with pytest.raises(ExpeditedAlphaEvidenceError, match="evidence_sha256"):
        build_expedited_release_report(bundle, expected_revision=REVISION)


def test_observation_window_requires_seven_fresh_immutable_samples() -> None:
    six = valid_bundle()
    six["observations"].pop()  # type: ignore[union-attr]
    refresh_protected_bindings(six)
    report = build_expedited_release_report(six, expected_revision=REVISION)
    assert gate(report, "expedited_observation.minimum_window")["passed"] is False

    changed = valid_bundle()
    changed["observations"][-1]["terminal_content"]["sha256"] = "d" * 64  # type: ignore[index]
    with pytest.raises(
        ExpeditedAlphaEvidenceError,
        match="terminal content identity is invalid|terminal bytes changed",
    ):
        build_expedited_release_report(changed, expected_revision=REVISION)

    gap = valid_bundle()
    gap["observations"][-1]["observed_at"] = (  # type: ignore[index]
        AS_OF + timedelta(minutes=5)
    ).isoformat()
    refresh_protected_bindings(gap)
    report = build_expedited_release_report(gap, expected_revision=REVISION)
    assert gate(report, "expedited_observation.cadence")["passed"] is False


def test_final_approval_can_follow_preparation_but_observations_expire_at_sixty_minutes(
) -> None:
    fresh = valid_bundle()
    fresh_as_of = AS_OF + timedelta(minutes=8)
    fresh["evidence_as_of"] = fresh_as_of.isoformat()
    fresh["approval"] = approval(
        as_of=fresh_as_of,
        evidence_binding=fresh["approval"]["evidence_binding"],  # type: ignore[index]
    )
    refresh_protected_bindings(fresh)
    fresh_report = build_expedited_release_report(
        fresh,
        expected_revision=REVISION,
    )
    assert fresh_report["release_gate_passed"] is True
    assert fresh_report["observation"]["last_observation_age_minutes"] == 8  # type: ignore[index]

    stale = valid_bundle()
    stale_as_of = AS_OF + timedelta(minutes=61)
    stale["evidence_as_of"] = stale_as_of.isoformat()
    stale["approval"] = approval(
        as_of=stale_as_of,
        evidence_binding=stale["approval"]["evidence_binding"],  # type: ignore[index]
    )
    refresh_protected_bindings(stale)
    stale_report = build_expedited_release_report(
        stale,
        expected_revision=REVISION,
    )
    assert gate(
        stale_report,
        "expedited_observation.minimum_window",
    )["passed"] is False
    assert stale_report["observation"]["last_observation_age_minutes"] == 61  # type: ignore[index]


def test_content_performance_and_rollback_gates_are_enforced() -> None:
    content = valid_bundle()
    content["content_integrity"]["raw_counts"]["telegram_exposure_count"] = 1  # type: ignore[index]
    with pytest.raises(ExpeditedAlphaEvidenceError, match="evidence_binding"):
        build_expedited_release_report(content, expected_revision=REVISION)

    slow = valid_bundle()
    slow["experience"]["web_vitals"]["lcp"]["p75_seconds"] = 2.6  # type: ignore[index]
    refresh_protected_bindings(slow)
    report = build_expedited_release_report(slow, expected_revision=REVISION)
    assert gate(report, "experience.lcp")["passed"] is False

    persisted_snapshot = valid_bundle()
    persisted_snapshot["content_integrity"]["raw_counts"][  # type: ignore[index]
        "persisted_snapshot_forbidden_key_count"
    ] = 1
    refresh_protected_bindings(persisted_snapshot)
    report = build_expedited_release_report(
        persisted_snapshot,
        expected_revision=REVISION,
    )
    assert gate(
        report,
        "content.no_persisted_snapshot_forbidden_keys",
    )["passed"] is False
    assert (
        report["content_integrity"]["persisted_snapshot_forbidden_key_count"]  # type: ignore[index]
        == 1
    )

    rollback = valid_bundle()
    exp = rollback["experience"]  # type: ignore[assignment]
    exp["rollback_drill"]["duration_minutes"] = 10.1  # type: ignore[index]
    exp["rollback_drill"]["started_at"] = (  # type: ignore[index]
        AS_OF - timedelta(minutes=10, seconds=6)
    ).isoformat()
    refresh_protected_bindings(rollback)
    report = build_expedited_release_report(
        rollback,
        expected_revision=REVISION,
    )
    assert gate(report, "expedited_rollback.maximum_seconds")["passed"] is False


def test_expedited_content_requires_at_least_twenty_public_events() -> None:
    bundle = valid_bundle()
    counts = bundle["content_integrity"]["raw_counts"]  # type: ignore[index]
    counts.update(
        {
            "public_event_count": 1,
            "original_language_preserved_count": 1,
            "official_url_preserved_count": 1,
            "title_provenance_labeled_count": 1,
            "source_title_event_count": 1,
            "source_title_preserved_count": 1,
            "generated_metadata_title_count": 0,
            "operator_metadata_title_count": 0,
        }
    )
    refresh_protected_bindings(bundle)
    report = build_expedited_release_report(
        bundle,
        expected_revision=REVISION,
    )
    assert report["release_gate_passed"] is False
    assert gate(
        report,
        "expedited_content.minimum_reviewed_events",
    )["passed"] is False


def test_89_day_waiver_expires_and_real_90_day_archive_passes() -> None:
    after_cutoff = datetime(2026, 7, 28, 21, 0, tzinfo=timezone.utc)
    with pytest.raises(ExpeditedAlphaEvidenceError, match="89-day.*waiver"):
        build_expedited_release_report(
            valid_bundle(as_of=after_cutoff),
            expected_revision=REVISION,
        )

    report = build_expedited_release_report(
        valid_bundle(as_of=after_cutoff, day_count=90),
        expected_revision=REVISION,
    )
    assert report["release_gate_passed"] is True
    assert report["legacy_archive"]["waiver_used"] is False  # type: ignore[index]

    placeholder = valid_bundle()
    placeholder["legacy_archive"]["contains_placeholder"] = True  # type: ignore[index]
    report = build_expedited_release_report(
        placeholder,
        expected_revision=REVISION,
    )
    assert gate(
        report,
        "expedited_legacy_archive.real_consecutive_days",
    )["passed"] is False


def test_legacy_manifest_and_human_waiver_are_digest_bound() -> None:
    manifest_tamper = valid_bundle()
    manifest_tamper["legacy_archive"]["compatibility_manifest"][  # type: ignore[index]
        "content_sha256"
    ] = f"sha256:{'e' * 64}"
    with pytest.raises(
        ExpeditedAlphaEvidenceError,
        match="manifest digest",
    ):
        build_expedited_release_report(
            manifest_tamper,
            expected_revision=REVISION,
        )

    fake_human = valid_bundle()
    waiver = fake_human["legacy_archive"]["compatibility_manifest"]["waiver"]  # type: ignore[index]
    waiver["reviewer_type"] = "ai"
    refresh_protected_bindings(fake_human)
    with pytest.raises(
        ExpeditedAlphaEvidenceError,
        match="human waiver",
    ):
        build_expedited_release_report(fake_human, expected_revision=REVISION)


def test_standard_90_day_manifest_must_be_current_at_kst_boundary() -> None:
    as_of = datetime(2026, 7, 28, 21, 0, tzinfo=timezone.utc)
    stale = valid_bundle(as_of=as_of, day_count=90)
    legacy = stale["legacy_archive"]  # type: ignore[assignment]
    legacy["first_date"] = "2026-01-01"  # type: ignore[index]
    legacy["last_date"] = "2026-03-31"  # type: ignore[index]
    manifest = legacy["compatibility_manifest"]  # type: ignore[index]
    manifest["window_start"] = "2026-01-01"
    manifest["window_end"] = "2026-03-31"
    refresh_protected_bindings(stale)
    with pytest.raises(
        ExpeditedAlphaEvidenceError,
        match="latest real 90-day window",
    ):
        build_expedited_release_report(stale, expected_revision=REVISION)


def test_exact_pinned_snapshot_fallback_is_manifest_bound_and_risk_approved() -> None:
    bundle = pinned_snapshot_bundle()
    report = build_expedited_release_report(bundle, expected_revision=REVISION)

    assert report["release_gate_passed"] is True
    archive = report["legacy_archive"]
    assert archive["mode"] == PINNED_SNAPSHOT_MODE  # type: ignore[index]
    assert archive["consecutive_day_count"] == 94  # type: ignore[index]
    assert archive["pinned_snapshot_used"] is True  # type: ignore[index]
    assert archive["waiver_used"] is False  # type: ignore[index]
    assert (
        report["approval"]["evidence_binding"]["legacy_manifest_sha256"]  # type: ignore[index]
        == archive["compatibility_manifest_sha256"]  # type: ignore[index]
    )
    assert report["approval"]["roles"]["expedited-risk"] is True  # type: ignore[index]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_id", "30743899773"),
        ("artifact_id", "8832612653"),
        ("artifact_name", "legacy-pages-archive-other"),
        ("code_revision", "f" * 40),
        ("artifact_digest", f"sha256:{'e' * 64}"),
    ],
)
def test_pinned_snapshot_fallback_rejects_every_nonexact_pin(
    field: str,
    value: str,
) -> None:
    bundle = pinned_snapshot_bundle()
    manifest = bundle["legacy_archive"]["compatibility_manifest"]  # type: ignore[index]
    manifest["source"][field] = value
    refresh_protected_bindings(bundle)

    with pytest.raises(
        ExpeditedAlphaEvidenceError,
        match="exact immutable pin|pinned artifact",
    ):
        build_expedited_release_report(bundle, expected_revision=REVISION)


def test_pinned_snapshot_fallback_rejects_waiver_and_missing_risk_approval() -> None:
    waiver = pinned_snapshot_bundle()
    manifest = waiver["legacy_archive"]["compatibility_manifest"]  # type: ignore[index]
    manifest["waiver"] = {
        "status": "active",
        "approved": True,
        "reviewer_type": "human",
    }
    refresh_protected_bindings(waiver)
    with pytest.raises(ExpeditedAlphaEvidenceError, match="exact immutable pin"):
        build_expedited_release_report(waiver, expected_revision=REVISION)

    missing_risk = pinned_snapshot_bundle()
    approvals = missing_risk["approval"]["approvals"]  # type: ignore[index]
    risk = next(item for item in approvals if item["role"] == "expedited-risk")
    risk["decision"] = "rejected"
    refresh_protected_bindings(missing_risk)
    report = build_expedited_release_report(
        missing_risk,
        expected_revision=REVISION,
    )
    assert report["release_gate_passed"] is False
    assert gate(report, "expedited_approval.human_roles")["passed"] is False


def test_archive_must_match_the_drilled_artifact() -> None:
    bundle = valid_bundle()
    bundle["legacy_archive"]["archive_sha256"] = "d" * 64  # type: ignore[index]
    with pytest.raises(ExpeditedAlphaEvidenceError, match="rollback drill"):
        build_expedited_release_report(bundle, expected_revision=REVISION)


def test_cli_writes_deterministic_report_and_uses_exit_codes(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "input.json"
    first_output = tmp_path / "first.json"
    second_output = tmp_path / "second.json"
    input_path.write_text(
        json.dumps(valid_bundle(), ensure_ascii=False, sort_keys=True),
        encoding="utf-8",
    )

    assert (
        main(
            [
                "evaluate",
                "--input",
                str(input_path),
                "--expected-revision",
                REVISION,
                "--output",
                str(first_output),
            ]
        )
        == 0
    )
    capsys.readouterr()
    assert (
        main(
            [
                "evaluate",
                "--input",
                str(input_path),
                "--expected-revision",
                REVISION,
                "--output",
                str(second_output),
            ]
        )
        == 0
    )
    assert first_output.read_bytes() == second_output.read_bytes()

    failed = valid_bundle()
    failed["human_review"]["event_reviews"].pop()  # type: ignore[index]
    failed["human_review"]["raw_counts"]["event_review_count"] = 19  # type: ignore[index]
    refresh_protected_bindings(failed)
    input_path.write_text(json.dumps(failed), encoding="utf-8")
    assert (
        main(
            [
                "evaluate",
                "--input",
                str(input_path),
                "--expected-revision",
                REVISION,
                "--output",
                str(tmp_path / "failed.json"),
            ]
        )
        == 1
    )

    invalid = valid_bundle()
    invalid["is_synthetic"] = True
    input_path.write_text(json.dumps(invalid), encoding="utf-8")
    assert (
        main(
            [
                "evaluate",
                "--input",
                str(input_path),
                "--expected-revision",
                REVISION,
                "--output",
                str(tmp_path / "invalid.json"),
            ]
        )
        == 2
    )
    assert "invalid-expedited-production-alpha-evidence" in capsys.readouterr().err
