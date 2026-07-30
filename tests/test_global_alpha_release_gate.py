from __future__ import annotations

import base64
import gzip
import hashlib
import json
import zipfile
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import yaml

from curator.global_alpha_monitor import (
    HttpProbe,
    MonitorConfig,
    run_monitor,
)
from curator.global_alpha_observation_segment import (
    SEGMENT_COUNTS,
    SEGMENT_KIND,
    canonical_jsonl,
    segment_slot_bounds,
)
from curator.global_alpha_pages_identity import (
    CONTENT_ALGORITHM,
    build_terminal_content_identity,
)
from curator.global_alpha_release_gate import (
    AlphaReleaseEvidenceError,
    INPUT_FILENAMES,
    build_alpha_release_report,
    compile_observation_archives,
    main,
    materialize_input_bundle,
)


REVISION = "a" * 40
OTHER_REVISION = "b" * 40
START = datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc)
AS_OF = START + timedelta(hours=24, minutes=1)
DIGEST = "c" * 64
ROOT = Path(__file__).resolve().parents[1]
TERMINAL_ASSET_BYTES = {
    "index.html": (
        '<main id="app"></main><a data-nav="today"></a>'
        '<script src="./config.js"></script><script src="./app.js"></script>'
    ).encode(),
    "config.js": (
        "window.__BSIDE_GOVERNANCE_CONFIG__=Object.freeze("
        f'{{"apiBase":"https://example.invalid/api/v1",'
        f'"buildSha":"{REVISION}",'
        '"releaseChannel":"production_alpha_early_access"});\n'
    ).encode(),
    "app.js": b"window.__BSIDE_ALPHA_APP__=true;\n",
    "styles.css": b":root{color:#111827}\n",
}
TERMINAL_CONTENT = build_terminal_content_identity(TERMINAL_ASSET_BYTES)


def provenance(kind: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "kind": kind,
        "environment": "production",
        "evidence_source": "protected_production_export",
        "is_synthetic": False,
        "code_revision": REVISION,
        "collected_at": AS_OF.isoformat(),
    }


def source(country: str, coverage_mode: str) -> dict[str, object]:
    connector_ids = {
        "KR": "connector:kr:dart",
        "US": "connector:us:sec-edgar",
        "JP": "connector:jp:edinet",
        "GB": "connector:gb:companies-house",
        "CA": "connector:ca:issuer-ir",
        "AU": "connector:au:asic-register",
    }
    optional = country in {"JP", "GB"}
    return {
        "connector_id": connector_ids[country],
        "country": country,
        "coverage_mode": coverage_mode,
        "public_status": "coverage_unavailable" if optional else "active",
        "public_ready": not optional,
        "raw_count": 0 if optional else 4,
        "acknowledged_count": 0 if optional else 4,
    }


def observation(index: int) -> dict[str, object]:
    observed = START + timedelta(minutes=index * 5)
    sources = [
        source("KR", "market-wide"),
        source("US", "market-wide"),
        source("JP", "link-only"),
        source("GB", "link-only"),
        source("CA", "link-only"),
        source("AU", "link-only"),
    ]
    probe = {
        "http_status": 200,
        "transport_succeeded": True,
        "contract_valid": True,
        "error_class": None,
    }
    return {
        "schema_version": 1,
        "observation_id": f"global-alpha:{index:048x}",
        "observed_at": observed.isoformat(),
        "status": "healthy",
        "pipeline_mode": "shadow",
        "web_surface": "governance-preview",
        "release_state": "preview",
        "release_state_version": 4,
        "deployed_build_sha": REVISION,
        "api_code_revision": REVISION,
        "deployed_api_base": "https://example.invalid/api/v2",
        "terminal_content": deepcopy(TERMINAL_CONTENT),
        "workflow_revision": REVISION,
        "observation_window": {
            "duration_hours": 24,
            "started_at": START.isoformat(),
            "ends_at": (START + timedelta(hours=24)).isoformat(),
            "within_window": True,
            "elapsed_minutes": index * 5,
        },
        "sources": sources,
        "probes": {
            **{
                name: deepcopy(probe)
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
            },
            "event_detail": {
                "skipped": True,
                "reason": "no_live_event_available",
            },
        },
        "event_availability": {
            "state": "no_events",
            "returned": 0,
            "meaning": (
                "No public event matched, while all monitored connectors were healthy."
            ),
        },
        "reasons": [],
        "warnings": [],
    }


def observations() -> list[dict[str, object]]:
    return [observation(index) for index in range(289)]


class MonitorIntegrationClient:
    def _source(self, country: str, coverage: str) -> dict[str, object]:
        connector_ids = {
            "KR": "connector:kr:dart",
            "US": "connector:us:sec-edgar",
            "JP": "connector:jp:edinet",
            "GB": "connector:gb:companies-house",
            "CA": "connector:ca:issuer-ir",
            "AU": "connector:au:asic-register",
        }
        optional = country in {"JP", "GB"}
        return {
            "connector_id": connector_ids[country],
            "country": country,
            "coverage_mode": coverage,
            "status": "inactive" if optional else "active",
            "fresh": not optional,
            "public_status": (
                "coverage_unavailable" if optional else "active"
            ),
            "public_ready": not optional,
            "lag_minutes": None if optional else 5,
            "expected_cadence_minutes": 30,
            "raw_count": 0 if optional else 4,
            "acknowledged_count": 0 if optional else 4,
            "last_success_at": None if optional else START.isoformat(),
            "last_checked_at": None if optional else START.isoformat(),
        }

    def get_text(self, url: str) -> HttpProbe:
        path = urlsplit(url).path
        if path.endswith("/governance/config.js"):
            asset = "config.js"
        elif path.endswith("/governance/app.js"):
            asset = "app.js"
        elif path.endswith("/governance/styles.css"):
            asset = "styles.css"
        else:
            asset = "index.html"
        return HttpProbe(200, 1, text=TERMINAL_ASSET_BYTES[asset].decode())

    def get_json(self, url: str, *, token: str = "") -> HttpProbe:
        del token
        parsed = urlsplit(url)
        path = parsed.path
        if path.endswith("/health"):
            payload: object = {
                "ok": True,
                "service": "bside-global-market-terminal",
                "code_revision": REVISION,
                "time": START.isoformat(),
                "api_version": "v2",
            }
        elif path.endswith("/ops/release-state"):
            payload = {
                "ok": True,
                "api_version": "v2",
                "data": {
                    "release_state": "preview",
                    "state_version": 1,
                    "updated_at": START.isoformat(),
                    "cutover_at": None,
                },
            }
        elif path.endswith("/sources/status"):
            items = [
                self._source("KR", "market-wide"),
                self._source("US", "market-wide"),
                self._source("JP", "link-only"),
                self._source("GB", "link-only"),
                self._source("CA", "link-only"),
                self._source("AU", "link-only"),
            ]
            required_ready = {
                "connector:kr:dart": True,
                "connector:us:sec-edgar": True,
                "connector:ca:issuer-ir": True,
                "connector:au:asic-register": True,
            }
            payload = {
                "ok": True,
                "api_version": "v2",
                "data": {
                    "checked_at": START.isoformat(),
                    "items": items,
                    "required_source_ready": required_ready,
                    "all_required_ready": True,
                },
                "meta": {"returned": len(items)},
            }
        elif path.endswith("/live"):
            payload = {
                "ok": True,
                "api_version": "v2",
                "data": {"items": []},
                "meta": {"returned": 0},
            }
        elif path.endswith("/search"):
            payload = {
                "ok": True,
                "api_version": "v2",
                "data": {"items": []},
                "meta": {"returned": 0},
            }
        else:
            return HttpProbe(404, 1, error_class="http_error")
        return HttpProbe(200, 1, payload=payload)


def connector_report() -> dict[str, object]:
    result = provenance("bside-global-alpha-connector-idempotency")
    result["connectors"] = [
        {
            "connector_family": family,
            "country": country,
            "payload_sha256": DIGEST,
            "first_run": {
                "raw_count": 5,
                "filtered_out_count": 1,
                "accepted_count": 4,
                "acknowledged_count": 4,
                "idempotent": False,
            },
            "replay_run": {
                "raw_count": 5,
                "filtered_out_count": 1,
                "accepted_count": 4,
                "acknowledged_count": 4,
                "payload_sha256": DIGEST,
                "idempotent": True,
            },
            "row_count_after_first": 4,
            "row_count_after_replay": 4,
            "duplicate_row_count": 0,
            "checkpoint_after_first": "2026-07-24T00:00:00Z",
            "checkpoint_after_replay": "2026-07-24T00:00:00Z",
            "coverage_started_at": (AS_OF - timedelta(days=31)).isoformat(),
            "coverage_ended_at": AS_OF.isoformat(),
            "successful_window_count": 31,
            "failed_window_count": 0,
            "completed_windows": [
                {
                    "window_start": (
                        AS_OF.date() - timedelta(days=31 - index)
                    ).isoformat(),
                    "window_end_exclusive": (
                        AS_OF.date() - timedelta(days=30 - index)
                    ).isoformat(),
                    "raw_count": 5,
                    "filtered_out_count": 1,
                    "accepted_count": 4,
                    "acknowledged_count": 4,
                    "status": "complete",
                    "code_revision": REVISION,
                    "receipt_sha256": hashlib.sha256(
                        f"{family}:{index}".encode()
                    ).hexdigest(),
                }
                for index in range(31)
            ],
        }
        for family, country in (
            ("dart", "KR"),
            ("sec-edgar", "US"),
        )
    ]
    return result


def automated_evidence_response(
    *,
    revision: str = REVISION,
) -> dict[str, object]:
    collected = datetime.now(timezone.utc)
    end = collected.date()
    start = end - timedelta(days=30)
    coverage: list[dict[str, object]] = []
    for family, country in (
        ("dart", "KR"),
        ("sec-edgar", "US"),
    ):
        windows = []
        cursor = start
        for index in range(30):
            next_cursor = cursor + timedelta(days=1)
            windows.append(
                {
                    "window_start": cursor.isoformat(),
                    "window_end_exclusive": next_cursor.isoformat(),
                    "raw_count": 5,
                    "filtered_out_count": 1,
                    "accepted_count": 4,
                    "acknowledged_count": 4,
                    "status": "complete",
                    "code_revision": revision,
                    "receipt_sha256": hashlib.sha256(
                        f"{family}:{index}:{revision}".encode()
                    ).hexdigest(),
                }
            )
            cursor = next_cursor
        coverage.append(
            {
                "connector_family": family,
                "country": country,
                "coverage_started_at": (
                    datetime.combine(start, datetime.min.time(), timezone.utc)
                ).isoformat(),
                "coverage_ended_at": (
                    datetime.combine(end, datetime.min.time(), timezone.utc)
                ).isoformat(),
                "successful_window_count": 30,
                "failed_window_count": 0,
                "completed_windows": windows,
            }
        )
    content = content_report()
    content.update(
        {
            "evidence_source": "production_database_export",
            "code_revision": revision,
            "collected_at": collected.isoformat(),
        }
    )
    return {
        "ok": True,
        "api_version": "v2",
        "data": {
            "schema_version": 1,
            "kind": "bside-global-alpha-automated-evidence",
            "environment": "production",
            "evidence_source": "production_database_export",
            "is_synthetic": False,
            "code_revision": revision,
            "collected_at": collected.isoformat(),
            "connector_coverage": coverage,
            "content_integrity": content,
        },
    }


def review_report() -> dict[str, object]:
    result = provenance("bside-global-alpha-human-review")
    event_reviews = [
        {
            "event_id": f"event-{index}",
            "decision": "approved",
            "reviewer_type": "human",
            "reviewer_reference": "oversight-1",
            "reviewed_at": AS_OF.isoformat(),
        }
        for index in range(60)
    ]
    pair_reviews = [
        {
            "pair_id": f"pair-{index}",
            "left_document_id": f"left-{index}",
            "right_document_id": f"right-{index}",
            "decision": index % 2 == 0,
            "reviewer_type": "human",
            "reviewer_reference": "oversight-1",
            "reviewed_at": AS_OF.isoformat(),
        }
        for index in range(120)
    ]
    top_reviews = [
        {
            "edition_id": "brief-global-20260724",
            "event_id": f"top-{index}",
            "decision": "approved",
            "reviewer_type": "human",
            "reviewer_reference": "oversight-1",
            "reviewed_at": AS_OF.isoformat(),
        }
        for index in range(5)
    ]
    result.update(
        {
            "ground_truth_source": "human",
            "ai_generated_ground_truth": False,
            "human_attestation": True,
            "raw_counts": {
                "event_review_count": len(event_reviews),
                "same_event_pair_review_count": len(pair_reviews),
                "top5_human_reviewed_count": len(top_reviews),
                "top5_published_count": len(top_reviews),
            },
            "event_reviews": event_reviews,
            "same_event_pair_reviews": pair_reviews,
            "top5_reviews": top_reviews,
        }
    )
    return result


def content_report() -> dict[str, object]:
    result = provenance("bside-global-alpha-content-integrity")
    result["raw_counts"] = {
        "public_event_count": 100,
        "original_language_preserved_count": 100,
        "official_url_preserved_count": 100,
        "title_provenance_labeled_count": 100,
        "source_title_event_count": 80,
        "source_title_preserved_count": 80,
        "generated_metadata_title_count": 15,
        "operator_metadata_title_count": 5,
        "unknown_title_provenance_count": 0,
        "scanned_response_count": 800,
        "telegram_exposure_count": 0,
        "internal_field_exposure_count": 0,
        "persisted_snapshot_forbidden_key_count": 0,
    }
    return result


def experience_report() -> dict[str, object]:
    result = provenance("bside-global-alpha-experience")
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
                "lcp": {"p75_seconds": 2.1, "sample_count": 20},
                "inp": {"p75_ms": 150, "sample_count": 20},
                "cls": {"p75": 0.05, "sample_count": 20},
            },
            "api_responses": [
                {"route": route, "size_bytes": 120_000, "http_status": 200}
                for route in (
                    "/briefs/latest?edition=global",
                    "/live?limit=100",
                    "/events?limit=100",
                    "/issuers?limit=100",
                    "/calendar?limit=100",
                    "/search?q=sample",
                    "/sources/status",
                    "/exports/events.json?limit=100",
                    "/exports/events.csv?limit=100",
                    "/feeds/events.atom?limit=100",
                )
            ],
            "failure_detection_drill": {
                "incident_started_at": START.isoformat(),
                "detected_at": (START + timedelta(minutes=7)).isoformat(),
                "detection_minutes": 7,
            },
            "rollback_drill": {
                "succeeded": True,
                "duration_minutes": 8,
                "started_at": START.isoformat(),
                "completed_at": (START + timedelta(minutes=8)).isoformat(),
                "legacy_artifact_sha256": DIGEST,
            },
        }
    )
    return result


def approval_report() -> dict[str, object]:
    result = provenance("bside-global-alpha-release-approval")
    result.update(
        {
            "release_tier_acknowledged": "production-alpha",
            "ga_certification_claimed": False,
            "approvals": [
                {
                    "role": role,
                    "decision": "approved",
                    "approver_type": "human",
                    "approver_reference": "oversight-1",
                    "decided_at": AS_OF.isoformat(),
                    "evidence_sha256": DIGEST,
                }
                for role in ("oversight", "source-rights")
            ],
            "source_right_scope": [
                {
                    "country": country,
                    "decision": "approved",
                    "valid_source_right_count": 1,
                    "invalid_source_right_count": 0,
                }
                for country in ("KR", "US", "CA", "AU")
            ],
        }
    )
    return result


def pages_artifact_identity() -> dict[str, object]:
    return {
        "schema_version": 1,
        "kind": "bside-global-alpha-pages-artifact-binding",
        "code_revision": REVISION,
        "producer_workflow": ".github/workflows/daily.yml",
        "producer_run_id": 1234,
        "producer_run_attempt": 1,
        "artifact_id": 5678,
        "artifact_name": "pages-1234-1",
        "artifact_digest": "sha256:" + DIGEST,
        "content_identity": {
            "schema_version": 1,
            "kind": "bside-global-alpha-pages-content-identity",
            "algorithm": CONTENT_ALGORITHM,
            "site": {
                "file_count": 12,
                "total_bytes": int(TERMINAL_CONTENT["total_bytes"]) * 2 + 100,
                "sha256": DIGEST,
            },
            "terminal": deepcopy(TERMINAL_CONTENT),
        },
    }


def build(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "observations": observations(),
        "pages_artifact_identity": pages_artifact_identity(),
        "connector_idempotency": connector_report(),
        "human_review": review_report(),
        "content_integrity": content_report(),
        "experience": experience_report(),
        "approval": approval_report(),
    }
    values.update(overrides)
    return build_alpha_release_report(
        values["observations"],  # type: ignore[arg-type]
        values["pages_artifact_identity"],  # type: ignore[arg-type]
        values["connector_idempotency"],  # type: ignore[arg-type]
        values["human_review"],  # type: ignore[arg-type]
        values["content_integrity"],  # type: ignore[arg-type]
        values["experience"],  # type: ignore[arg-type]
        values["approval"],  # type: ignore[arg-type]
        expected_revision=REVISION,
        evidence_as_of=AS_OF,
    )


def test_valid_evidence_passes_as_production_alpha_without_ga_claim() -> None:
    report = build()
    assert report["release_gate_passed"] is True
    assert report["release_tier"] == "production-alpha"
    assert report["ga_certification_claimed"] is False
    assert "does not certify final recall or same-event precision" in str(
        report["quality_statement"]
    )
    assert report["failed_gates"] == []
    assert report["pages_artifact"]["artifact_id"] == 5678
    assert (
        report["content_integrity"]["persisted_snapshot_forbidden_key_count"]  # type: ignore[index]
        == 0
    )
    assert next(
        gate
        for gate in report["gates"]
        if gate["name"] == "content.no_persisted_snapshot_forbidden_keys"
    )["passed"] is True
    assert (
        report["observation"]["terminal_content_sha256"]
        == TERMINAL_CONTENT["sha256"]
    )


def test_observed_terminal_bytes_must_match_exact_daily_artifact() -> None:
    pages = pages_artifact_identity()
    changed_terminal = build_terminal_content_identity(
        {
            **TERMINAL_ASSET_BYTES,
            "app.js": b"window.__BSIDE_ALPHA_APP__='different';\n",
        }
    )
    pages["content_identity"]["terminal"] = changed_terminal  # type: ignore[index]
    with pytest.raises(
        AlphaReleaseEvidenceError,
        match="do not match the evidence-bound daily Pages artifact",
    ):
        build(pages_artifact_identity=pages)


def test_pages_artifact_binding_must_match_release_sha_and_run_name() -> None:
    pages = pages_artifact_identity()
    pages["code_revision"] = OTHER_REVISION
    with pytest.raises(AlphaReleaseEvidenceError, match="release candidate"):
        build(pages_artifact_identity=pages)

    pages = pages_artifact_identity()
    pages["artifact_name"] = "pages-9999-1"
    with pytest.raises(AlphaReleaseEvidenceError, match="not run-bound"):
        build(pages_artifact_identity=pages)


def test_real_monitor_output_contract_flows_into_alpha_gate() -> None:
    actual = run_monitor(
        MonitorConfig(
            api_base_url="https://example.invalid/api/v2",
            web_base_url="https://example.invalid/governance",
            web_surface="governance-preview",
            pipeline_mode="shadow",
            ops_token="ops",
            preview_token="preview",
            code_revision=REVISION,
        ),
        client=MonitorIntegrationClient(),
        now=START,
    )
    assert actual["status"] == "healthy"
    assert set(actual["probes"]) == {
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
    records = []
    for index in range(289):
        record = deepcopy(actual)
        observed = START + timedelta(minutes=index * 5)
        record["observation_id"] = f"global-alpha:{index:048x}"
        record["observed_at"] = observed.isoformat()
        record["observation_window"]["elapsed_minutes"] = index * 5  # type: ignore[index]
        records.append(record)
    report = build(observations=records)
    assert report["release_gate_passed"] is True
    assert report["observation"]["sample_count"] == 289  # type: ignore[index]


@pytest.mark.parametrize("marker", ["fixture", "synthetic", "sample", "test"])
def test_non_production_or_synthetic_provenance_is_rejected(marker: str) -> None:
    connectors = connector_report()
    connectors["evidence_source"] = f"{marker}_export"
    with pytest.raises(AlphaReleaseEvidenceError, match="not release eligible"):
        build(connector_idempotency=connectors)


def test_different_sha_is_rejected_across_every_evidence_boundary() -> None:
    records = observations()
    records[100]["workflow_revision"] = OTHER_REVISION
    with pytest.raises(AlphaReleaseEvidenceError, match="revision mismatch"):
        build(observations=records)

    approval = approval_report()
    approval["code_revision"] = OTHER_REVISION
    with pytest.raises(AlphaReleaseEvidenceError, match="does not match"):
        build(approval=approval)

    records = observations()
    records[100]["api_code_revision"] = OTHER_REVISION
    with pytest.raises(AlphaReleaseEvidenceError, match="API code revision mismatch"):
        build(observations=records)


def test_deployed_api_base_must_be_one_canonical_https_v2_endpoint() -> None:
    records = observations()
    records[100]["deployed_api_base"] = "http://example.invalid/api/v2"
    with pytest.raises(AlphaReleaseEvidenceError, match="canonical HTTPS v2"):
        build(observations=records)

    records = observations()
    records[100]["deployed_api_base"] = "https://other.invalid/api/v2"
    with pytest.raises(AlphaReleaseEvidenceError, match="changed within"):
        build(observations=records)


def test_missing_cadence_incident_and_duplicate_observation_fail_closed() -> None:
    missing = observations()
    del missing[100]
    report = build(observations=missing)
    assert report["release_gate_passed"] is False
    assert "observation.five_minute_cadence" in report["failed_gates"]

    incident = observations()
    incident[100]["status"] = "incident"
    incident[100]["reasons"] = ["api_failure"]
    report = build(observations=incident)
    assert "observation.no_incident_or_degradation" in report["failed_gates"]

    duplicated = observations()
    duplicated[101]["observation_id"] = duplicated[100]["observation_id"]
    with pytest.raises(AlphaReleaseEvidenceError, match="duplicate observation_id"):
        build(observations=duplicated)


def test_six_country_coverage_and_connector_idempotency_are_required() -> None:
    records = observations()
    records[0]["sources"][4]["coverage_mode"] = "selected-issuers"  # type: ignore[index]
    report = build(observations=records)
    assert "observation.six_country_coverage" in report["failed_gates"]

    connectors = connector_report()
    connectors["connectors"][0]["row_count_after_replay"] = 6  # type: ignore[index]
    report = build(connector_idempotency=connectors)
    assert "connectors.idempotent_replay" in report["failed_gates"]


def test_each_official_connector_requires_a_current_thirty_day_horizon() -> None:
    connectors = connector_report()
    connectors["connectors"][1]["coverage_started_at"] = (  # type: ignore[index]
        AS_OF - timedelta(days=2)
    ).isoformat()
    report = build(connector_idempotency=connectors)
    assert "connectors.minimum_30_day_horizon" in report["failed_gates"]
    summaries = report["connector_idempotency"]["connectors"]  # type: ignore[index]
    sec = next(
        item for item in summaries if item["connector_family"] == "sec-edgar"
    )
    assert sec["minimum_30_day_horizon"] is False


def test_connector_horizon_rejects_gaps_and_unacknowledged_windows() -> None:
    connectors = connector_report()
    completed = connectors["connectors"][1]["completed_windows"]  # type: ignore[index]
    completed[10]["window_start"] = completed[9]["window_start"]  # type: ignore[index]
    completed[10]["window_end_exclusive"] = completed[9][  # type: ignore[index]
        "window_end_exclusive"
    ]
    with pytest.raises(AlphaReleaseEvidenceError, match="gap or overlap"):
        build(connector_idempotency=connectors)

    connectors = connector_report()
    completed = connectors["connectors"][1]["completed_windows"]  # type: ignore[index]
    completed[10]["acknowledged_count"] = 3  # type: ignore[index]
    with pytest.raises(
        AlphaReleaseEvidenceError,
        match="acknowledged_count must equal accepted_count",
    ):
        build(connector_idempotency=connectors)

    connectors = connector_report()
    completed = connectors["connectors"][1]["completed_windows"]  # type: ignore[index]
    completed[10]["filtered_out_count"] = 0  # type: ignore[index]
    with pytest.raises(
        AlphaReleaseEvidenceError,
        match="raw_count must equal filtered_out_count \\+ accepted_count",
    ):
        build(connector_idempotency=connectors)


def test_human_review_raw_counts_top5_and_zero_denominators_are_enforced() -> None:
    review = review_report()
    review["event_reviews"] = review["event_reviews"][:-1]  # type: ignore[index]
    review["raw_counts"]["event_review_count"] = 59  # type: ignore[index]
    report = build(human_review=review)
    assert "human_review.events" in report["failed_gates"]

    zero_top = review_report()
    zero_top["top5_reviews"] = []
    zero_top["raw_counts"]["top5_human_reviewed_count"] = 0  # type: ignore[index]
    zero_top["raw_counts"]["top5_published_count"] = 0  # type: ignore[index]
    with pytest.raises(AlphaReleaseEvidenceError, match="integer >= 1"):
        build(human_review=zero_top)


def test_content_integrity_performance_accessibility_and_recovery_gates() -> None:
    content = content_report()
    content["raw_counts"]["original_language_preserved_count"] = 99  # type: ignore[index]
    content["raw_counts"]["telegram_exposure_count"] = 1  # type: ignore[index]
    content["raw_counts"]["title_provenance_labeled_count"] = 99  # type: ignore[index]
    content["raw_counts"]["source_title_preserved_count"] = 79  # type: ignore[index]
    content["raw_counts"]["operator_metadata_title_count"] = 4  # type: ignore[index]
    content["raw_counts"]["unknown_title_provenance_count"] = 1  # type: ignore[index]
    report = build(content_integrity=content)
    assert "content.original_language" in report["failed_gates"]
    assert "content.title_provenance" in report["failed_gates"]
    assert "content.source_title_preservation" in report["failed_gates"]
    assert "content.no_unknown_title_provenance" in report["failed_gates"]
    assert "content.no_telegram_exposure" in report["failed_gates"]

    persisted_snapshot = content_report()
    persisted_snapshot["raw_counts"][  # type: ignore[index]
        "persisted_snapshot_forbidden_key_count"
    ] = 1
    report = build(content_integrity=persisted_snapshot)
    assert (
        "content.no_persisted_snapshot_forbidden_keys"
        in report["failed_gates"]
    )
    assert (
        report["content_integrity"]["persisted_snapshot_forbidden_key_count"]  # type: ignore[index]
        == 1
    )

    experience = experience_report()
    experience["web_vitals"]["lcp"]["p75_seconds"] = 2.6  # type: ignore[index]
    experience["viewports"][0]["axe_serious_count"] = 1  # type: ignore[index]
    experience["api_responses"][0]["size_bytes"] = 250_001  # type: ignore[index]
    experience["failure_detection_drill"]["detection_minutes"] = 11  # type: ignore[index]
    experience["failure_detection_drill"]["detected_at"] = (  # type: ignore[index]
        START + timedelta(minutes=11)
    ).isoformat()
    experience["rollback_drill"]["duration_minutes"] = 11  # type: ignore[index]
    experience["rollback_drill"]["completed_at"] = (  # type: ignore[index]
        START + timedelta(minutes=11)
    ).isoformat()
    report = build(experience=experience)
    assert {
        "experience.viewports_and_axe",
        "experience.lcp",
        "experience.api_budget",
        "experience.failure_detection",
        "experience.rollback",
    }.issubset(set(report["failed_gates"]))

    zero_samples = experience_report()
    zero_samples["web_vitals"]["inp"]["sample_count"] = 0  # type: ignore[index]
    with pytest.raises(AlphaReleaseEvidenceError, match="integer >= 1"):
        build(experience=zero_samples)


def test_human_oversight_and_all_source_right_countries_are_required() -> None:
    approval = approval_report()
    approval["approvals"][0]["approver_type"] = "ai"  # type: ignore[index]
    with pytest.raises(AlphaReleaseEvidenceError, match="must be human"):
        build(approval=approval)

    approval = approval_report()
    approval["source_right_scope"] = approval["source_right_scope"][:-1]  # type: ignore[index]
    report = build(approval=approval)
    assert "approval.source_rights" in report["failed_gates"]


def write_inputs(root: Path) -> dict[str, Path]:
    paths: dict[str, Path] = {}
    values = {
        "connector-idempotency.json": connector_report(),
        "human-review.json": review_report(),
        "content-integrity.json": content_report(),
        "experience.json": experience_report(),
        "approval.json": approval_report(),
    }
    for filename, value in values.items():
        path = root / filename
        path.write_text(json.dumps(value), encoding="utf-8")
        paths[filename] = path
    observation_path = root / "observations.jsonl"
    observation_path.write_text(
        "".join(json.dumps(value) + "\n" for value in observations()),
        encoding="utf-8",
    )
    paths["observations.jsonl"] = observation_path
    pages_path = root / "pages-artifact-identity.json"
    pages_path.write_text(
        json.dumps(pages_artifact_identity()),
        encoding="utf-8",
    )
    paths["pages-artifact-identity.json"] = pages_path
    return paths


def test_cli_writes_deterministic_report_and_returns_failure_for_failed_gate(
    tmp_path: Path,
) -> None:
    paths = write_inputs(tmp_path)
    output = tmp_path / "report.json"
    args = [
        "evaluate",
        "--observations",
        str(paths["observations.jsonl"]),
        "--pages-artifact-identity",
        str(paths["pages-artifact-identity.json"]),
        "--connector-idempotency",
        str(paths["connector-idempotency.json"]),
        "--human-review",
        str(paths["human-review.json"]),
        "--content-integrity",
        str(paths["content-integrity.json"]),
        "--experience",
        str(paths["experience.json"]),
        "--approval",
        str(paths["approval.json"]),
        "--expected-revision",
        REVISION,
        "--evidence-as-of",
        AS_OF.isoformat(),
        "--output",
        str(output),
    ]
    assert main(args) == 0
    assert json.loads(output.read_text(encoding="utf-8"))["release_gate_passed"]

    content = json.loads(paths["content-integrity.json"].read_text(encoding="utf-8"))
    content["raw_counts"]["internal_field_exposure_count"] = 1
    paths["content-integrity.json"].write_text(
        json.dumps(content),
        encoding="utf-8",
    )
    assert main(args) == 1


def test_protected_base64_bundle_requires_exact_same_sha_file_set(
    tmp_path: Path,
) -> None:
    bundle = {
        "schema_version": 1,
        "kind": "bside-global-production-alpha-release-inputs",
        "code_revision": REVISION,
        "files": {
            "connector-idempotency.json": connector_report(),
            "human-review.json": review_report(),
            "content-integrity.json": content_report(),
            "experience.json": experience_report(),
            "approval.json": approval_report(),
        },
    }
    encoded = base64.b64encode(
        gzip.compress(
            json.dumps(bundle, separators=(",", ":")).encode(),
            mtime=0,
        )
    ).decode()
    assert len(encoded.encode("ascii")) < 48_000
    automated_path = tmp_path / "automated-evidence.json"
    automated_path.write_text(
        json.dumps(automated_evidence_response()),
        encoding="utf-8",
    )
    output = tmp_path / "materialized"
    materialize_input_bundle(
        encoded,
        output_dir=output,
        expected_revision=REVISION,
        automated_evidence_path=automated_path,
    )
    assert {path.name for path in output.iterdir()} == set(INPUT_FILENAMES)
    materialized_connector = json.loads(
        (output / "connector-idempotency.json").read_text(encoding="utf-8")
    )
    assert all(
        item["successful_window_count"] == 30
        for item in materialized_connector["connectors"]
    )
    assert (
        json.loads(
            (output / "content-integrity.json").read_text(encoding="utf-8")
        )["evidence_source"]
        == "production_database_export"
    )

    wrong = deepcopy(bundle)
    wrong["code_revision"] = OTHER_REVISION
    encoded_wrong = base64.b64encode(
        gzip.compress(
            json.dumps(wrong, separators=(",", ":")).encode(),
            mtime=0,
        )
    ).decode()
    with pytest.raises(AlphaReleaseEvidenceError, match="revision mismatch"):
        materialize_input_bundle(
            encoded_wrong,
            output_dir=tmp_path / "wrong",
            expected_revision=REVISION,
            automated_evidence_path=automated_path,
        )

    trailing = base64.b64encode(
        base64.b64decode(encoded) + b"forbidden-trailing-data"
    ).decode()
    with pytest.raises(AlphaReleaseEvidenceError, match="trailing data"):
        materialize_input_bundle(
            trailing,
            output_dir=tmp_path / "trailing",
            expected_revision=REVISION,
            automated_evidence_path=automated_path,
        )

    forged_bundle = deepcopy(bundle)
    forged_bundle["files"]["connector-idempotency.json"]["connectors"][0][
        "successful_window_count"
    ] = 999
    forged_bundle["files"]["content-integrity.json"]["raw_counts"][
        "source_title_preserved_count"
    ] = 0
    forged_encoded = base64.b64encode(
        gzip.compress(
            json.dumps(forged_bundle, separators=(",", ":")).encode(),
            mtime=0,
        )
    ).decode()
    forged_output = tmp_path / "forged-overridden"
    materialize_input_bundle(
        forged_encoded,
        output_dir=forged_output,
        expected_revision=REVISION,
        automated_evidence_path=automated_path,
    )
    forged_connector = json.loads(
        (forged_output / "connector-idempotency.json").read_text(encoding="utf-8")
    )
    assert forged_connector["connectors"][0]["successful_window_count"] == 30
    forged_content = json.loads(
        (forged_output / "content-integrity.json").read_text(encoding="utf-8")
    )
    assert forged_content["raw_counts"]["source_title_preserved_count"] == 80

    wrong_automated = automated_evidence_response(revision=OTHER_REVISION)
    wrong_automated_path = tmp_path / "wrong-automated.json"
    wrong_automated_path.write_text(
        json.dumps(wrong_automated),
        encoding="utf-8",
    )
    with pytest.raises(AlphaReleaseEvidenceError, match="code_revision"):
        materialize_input_bundle(
            encoded,
            output_dir=tmp_path / "wrong-automated-output",
            expected_revision=REVISION,
            automated_evidence_path=wrong_automated_path,
        )


def _write_segment_archives(
    tmp_path: Path,
    *,
    slot_override: tuple[int, int, int] | None = None,
    internal_digest_override_segment: int | None = None,
) -> tuple[Path, Path, dict[str, object]]:
    archive_dir = tmp_path / "archives"
    archive_dir.mkdir()
    records = observations()[:288]
    chain_id = "9001"
    entries: list[dict[str, object]] = []
    previous_run_id: str | None = None
    previous_digest: str | None = None
    offset = 0
    for segment_index, count in enumerate(SEGMENT_COUNTS, start=1):
        run_id = str(9000 + segment_index)
        first_slot, last_slot = segment_slot_bounds(segment_index)
        segment_records = deepcopy(records[offset : offset + count])
        for local_index, record in enumerate(segment_records):
            slot_index = first_slot + local_index
            if (
                slot_override is not None
                and slot_override[0] == segment_index
                and slot_override[1] == local_index
            ):
                slot_index = slot_override[2]
            record["observation_chain"] = {
                "schema_version": 1,
                "chain_id": chain_id,
                "segment_index": segment_index,
                "segment_count": 5,
                "slot_index": slot_index,
                "cadence_anchor": START.isoformat(),
                "candidate_started_at": START.isoformat(),
                "candidate_ends_at": (START + timedelta(hours=24)).isoformat(),
                "run_id": run_id,
                "run_attempt": 1,
            }
        observation_bytes = canonical_jsonl(segment_records)
        segment_manifest = {
            "schema_version": 1,
            "kind": SEGMENT_KIND,
            "status": "complete",
            "error_code": None,
            "chain_id": chain_id,
            "segment_index": segment_index,
            "segment_count": 5,
            "code_revision": REVISION,
            "run_id": run_id,
            "run_attempt": 1,
            "predecessor_run_id": previous_run_id,
            "predecessor_artifact_digest": previous_digest,
            "candidate_started_at": START.isoformat(),
            "candidate_ends_at": (START + timedelta(hours=24)).isoformat(),
            "cadence_anchor": START.isoformat(),
            "first_slot_index": first_slot,
            "last_slot_index": last_slot,
            "expected_record_count": count,
            "record_count": count,
            "first_observed_at": segment_records[0]["observed_at"],
            "last_observed_at": segment_records[-1]["observed_at"],
            "observations_sha256": (
                "0" * 64
                if internal_digest_override_segment == segment_index
                else hashlib.sha256(observation_bytes).hexdigest()
            ),
            "completed_at": (
                (START + timedelta(hours=24)).isoformat()
                if segment_index == 5
                else segment_records[-1]["observed_at"]
            ),
        }
        archive = archive_dir / f"{segment_index}.zip"
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
            bundle.writestr(
                "observations.jsonl",
                observation_bytes,
            )
            bundle.writestr(
                "segment-manifest.json",
                json.dumps(segment_manifest, sort_keys=True),
            )
        digest = "sha256:" + hashlib.sha256(archive.read_bytes()).hexdigest()
        entries.append(
            {
                "chain_id": chain_id,
                "segment_index": segment_index,
                "run_id": run_id,
                "run_attempt": 1,
                "run_conclusion": "success",
                "run_event": "workflow_dispatch",
                "workflow_path": (
                    ".github/workflows/global-alpha-observation-chain.yml"
                ),
                "run_created_at": (
                    START + timedelta(minutes=offset * 5)
                ).isoformat(),
                "artifact_id": str(1000 + segment_index),
                "artifact_name": (
                    f"global-alpha-observation-segment-{chain_id}-"
                    f"{segment_index}"
                ),
                "artifact_digest": digest,
                "archive_name": archive.name,
                "code_revision": REVISION,
            }
        )
        previous_run_id = run_id
        previous_digest = digest
        offset += count
    manifest: dict[str, object] = {
        "schema_version": 1,
        "kind": "bside-global-alpha-observation-segment-archive-manifest",
        "chain_id": chain_id,
        "code_revision": REVISION,
        "segments": entries,
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return archive_dir, manifest_path, manifest


def test_observation_segments_are_chained_digest_checked_and_compiled(
    tmp_path: Path,
) -> None:
    archive_dir, manifest_path, manifest = _write_segment_archives(tmp_path)
    output = tmp_path / "observations.jsonl"
    assert (
        compile_observation_archives(
            archive_dir=archive_dir,
            manifest_path=manifest_path,
            output_path=output,
            expected_revision=REVISION,
        )
        == 288
    )
    assert len(output.read_text(encoding="utf-8").splitlines()) == 288

    segments = manifest["segments"]
    assert isinstance(segments, list)
    assert isinstance(segments[2], dict)
    segments[2]["artifact_digest"] = "sha256:" + ("0" * 64)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(AlphaReleaseEvidenceError, match="digest mismatch"):
        compile_observation_archives(
            archive_dir=archive_dir,
            manifest_path=manifest_path,
            output_path=output,
            expected_revision=REVISION,
        )


def test_observation_segments_reject_missing_neutral_and_overlapping_chain(
    tmp_path: Path,
) -> None:
    missing_root = tmp_path / "missing"
    missing_root.mkdir()
    archive_dir, manifest_path, manifest = _write_segment_archives(missing_root)
    segments = manifest["segments"]
    assert isinstance(segments, list)
    segments.pop()
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(AlphaReleaseEvidenceError, match="exactly five"):
        compile_observation_archives(
            archive_dir=archive_dir,
            manifest_path=manifest_path,
            output_path=tmp_path / "missing.jsonl",
            expected_revision=REVISION,
        )

    neutral_root = tmp_path / "neutral"
    neutral_root.mkdir()
    archive_dir, manifest_path, manifest = _write_segment_archives(neutral_root)
    segments = manifest["segments"]
    assert isinstance(segments, list)
    assert isinstance(segments[3], dict)
    segments[3]["run_conclusion"] = "neutral"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(AlphaReleaseEvidenceError, match="successful first-attempt"):
        compile_observation_archives(
            archive_dir=archive_dir,
            manifest_path=manifest_path,
            output_path=tmp_path / "neutral.jsonl",
            expected_revision=REVISION,
        )

    cancelled_root = tmp_path / "cancelled"
    cancelled_root.mkdir()
    archive_dir, manifest_path, manifest = _write_segment_archives(cancelled_root)
    segments = manifest["segments"]
    assert isinstance(segments, list)
    assert isinstance(segments[1], dict)
    segments[1]["run_conclusion"] = "cancelled"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(AlphaReleaseEvidenceError, match="successful first-attempt"):
        compile_observation_archives(
            archive_dir=archive_dir,
            manifest_path=manifest_path,
            output_path=tmp_path / "cancelled.jsonl",
            expected_revision=REVISION,
        )

    overlap_root = tmp_path / "overlap"
    overlap_root.mkdir()
    archive_dir, manifest_path, _manifest = _write_segment_archives(
        overlap_root,
        slot_override=(2, 0, 57),
    )
    with pytest.raises(AlphaReleaseEvidenceError, match="chain metadata mismatch"):
        compile_observation_archives(
            archive_dir=archive_dir,
            manifest_path=manifest_path,
            output_path=tmp_path / "overlap.jsonl",
            expected_revision=REVISION,
        )

    tampered_root = tmp_path / "internal-tamper"
    tampered_root.mkdir()
    archive_dir, manifest_path, _manifest = _write_segment_archives(
        tampered_root,
        internal_digest_override_segment=3,
    )
    with pytest.raises(AlphaReleaseEvidenceError, match="observations digest"):
        compile_observation_archives(
            archive_dir=archive_dir,
            manifest_path=manifest_path,
            output_path=tmp_path / "tampered.jsonl",
            expected_revision=REVISION,
        )


def test_alpha_evidence_workflows_and_cutover_use_exact_immutable_contract() -> None:
    inputs_text = (
        ROOT / ".github" / "workflows" / "global-alpha-evidence-inputs.yml"
    ).read_text(encoding="utf-8")
    evidence_text = (
        ROOT / ".github" / "workflows" / "global-alpha-release-evidence.yml"
    ).read_text(encoding="utf-8")
    cutover_text = (
        ROOT / ".github" / "workflows" / "governance-cutover.yml"
    ).read_text(encoding="utf-8")
    evidence = yaml.load(evidence_text, Loader=yaml.BaseLoader)
    assert evidence["permissions"] == {"actions": "read", "contents": "read"}
    evidence_downloads = [
        step
        for step in evidence["jobs"]["evaluate"]["steps"]
        if step.get("uses")
        == "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
    ]
    assert len(evidence_downloads) == 2
    assert all(
        step["with"]["digest-mismatch"] == "error"
        and step["with"]["merge-multiple"] == "true"
        for step in evidence_downloads
    )
    assert "GLOBAL_ALPHA_RELEASE_INPUTS_GZIP_B64" in inputs_text
    assert "actual human reviewer" in inputs_text
    assert ".github/workflows/global-alpha-evidence-inputs.yml" in evidence_text
    assert ".github/workflows/global-alpha-observation-chain.yml" in evidence_text
    assert ".github/workflows/global-alpha-watchdog.yml" not in evidence_text
    assert "exact self-chained observation segments" in evidence_text
    assert 'run.conclusion !== "success"' in evidence_text
    assert "run_conclusion: run.conclusion" in evidence_text
    assert "digest.toLowerCase()" in evidence_text
    assert "segmentIndex <= 5" in evidence_text
    assert "observation_chain_run_id" in evidence_text
    assert "listArtifactsForRepo" in evidence_text
    assert "governance_pages_run_id" in evidence_text
    assert "governance_pages_artifact_name" in evidence_text
    assert "run.path !== \".github/workflows/daily.yml\"" in evidence_text
    assert "python -m curator.global_alpha_pages_identity create" in evidence_text
    assert "pages-artifact-identity.json" in evidence_text
    assert "--pages-artifact-identity" in evidence_text
    assert "python -m curator.global_alpha_release_gate evaluate" in evidence_text
    assert "name: global-alpha-release-evidence" in evidence_text
    assert "not final recall or same-event precision certification" in evidence_text
    assert '"global-alpha-release-evidence.yml"' in cutover_text
    assert "global-alpha-release-evidence" in cutover_text
    assert "ageHours > 48" in cutover_text
    assert "Evaluate immutable Production Alpha evidence" in cutover_text
    assert "Resolve evidence-bound daily Pages artifact" in cutover_text
    assert "Only the exact evidence-bound daily Pages artifact may be cut over" in cutover_text
    assert "python -m curator.global_alpha_pages_identity verify" in cutover_text
    assert "governance_pages_run_id" not in cutover_text
    assert "governance_pages_artifact_name" not in cutover_text
    assert "cmp --silent" in cutover_text


def test_existing_fourteen_day_ga_gate_contract_is_unchanged_and_separate() -> None:
    ga_gate = (ROOT / "curator" / "release_gate.py").read_text(encoding="utf-8")
    ga_export = (
        ROOT / ".github" / "workflows" / "release-evidence.yml"
    ).read_text(encoding="utf-8")
    alpha = (
        ROOT / "curator" / "global_alpha_release_gate.py"
    ).read_text(encoding="utf-8")
    assert "shadow_days: int = 14" in ga_gate
    assert "same_story_min_pairs: int = 500" in ga_gate
    assert "relevance_min_events: int = 300" in ga_gate
    assert "name: governance-release-evidence" in ga_export
    assert "from .release_gate import" not in alpha
