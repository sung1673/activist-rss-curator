from __future__ import annotations

import json
from datetime import date

import httpx
import pytest

from curator.quality_snapshot import (
    COUNT_FIELDS,
    QualitySnapshotClient,
    QualitySnapshotError,
    build_observation,
    freeze_quality_snapshot,
)


REVISION = "a" * 40
DAY = date(2026, 7, 21)


def export_payload(
    *,
    assignment: str = "database_corpus_snapshot",
    kind_lag: object = 12.5,
    kind_observation_count: int = 3,
    kind_lag_sample_count: int = 3,
) -> dict[str, object]:
    counts = {
        "official_evidence_total_count": 20,
        "official_evidence_linked_count": 19,
        "top_sensitive_total_count": 4,
        "top_sensitive_reviewed_count": 4,
        "original_language_total_count": 20,
        "original_language_preserved_count": 20,
        "source_right_total_count": 20,
        "valid_source_right_count": 20,
        "same_story_evaluated_pair_count": None,
        "same_story_predicted_same_count": None,
        "same_story_true_positive_count": None,
    }
    operation: dict[str, object] = {
        "observation_date": DAY.isoformat(),
        "code_revision": REVISION,
        "dart_success_poll_interval_p95_minutes": 15.0,
        "kind_observation_lag_p95_minutes": kind_lag,
        "kind_observation_count": kind_observation_count,
        "kind_lag_sample_count": kind_lag_sample_count,
        "content_snapshot_at": f"{DAY.isoformat()}T14:59:59Z",
        "content_scope": "governance_corpus_2021_plus_kst_day_end_v2",
        "content_metric_assignment": assignment,
        "raw_counts": counts,
    }
    if assignment == "immutable_quality_observation":
        operation.update(
            {
                "quality_observation_id": f"quality:{DAY.isoformat()}:{REVISION}",
                "quality_payload_sha256": "b" * 64,
            }
        )
    return {
        "ok": True,
        "evidence_source": "production_db_export",
        "is_synthetic": False,
        "distribution_mode": "web_only",
        "release_state": "preview",
        "range": {"from": DAY.isoformat(), "to": DAY.isoformat()},
        "code_revisions": [REVISION],
        "operations_days": [operation],
    }


def test_build_observation_freezes_only_actual_daily_metrics() -> None:
    result = build_observation(export_payload(), observation_date=DAY, revision=REVISION)
    assert result["observation_id"] == f"quality:{DAY.isoformat()}:{REVISION}"
    assert result["kind_observation_lag_p95_minutes"] == 12.5
    assert result["source"] == "production_quality_job"
    raw = result["raw_counts"]
    assert isinstance(raw, dict)
    assert {field: raw[field] for field in COUNT_FIELDS}["official_evidence_linked_count"] == 19
    assert raw["same_story_evaluated_pair_count"] == 0


def test_build_observation_accepts_true_kind_no_disclosure_day() -> None:
    result = build_observation(
        export_payload(kind_lag=None, kind_observation_count=0, kind_lag_sample_count=0),
        observation_date=DAY,
        revision=REVISION,
    )
    assert result["kind_observation_lag_p95_minutes"] is None


@pytest.mark.parametrize(
    ("mutator", "message"),
    [
        (lambda value: value.update({"is_synthetic": True}), "synthetic"),
        (lambda value: value.update({"release_state": "closed"}), "preview or live"),
        (lambda value: value.update({"code_revisions": ["b" * 40]}), "expected revision"),
        (
            lambda value: value["operations_days"][0].update(  # type: ignore[index,union-attr]
                {"content_metric_assignment": "ambiguous_multiple_revisions"}
            ),
            "ambiguous",
        ),
        (
            lambda value: value["operations_days"][0].update(  # type: ignore[index,union-attr]
                {"kind_observation_lag_p95_minutes": None}
            ),
            "KIND",
        ),
        (
            lambda value: value["operations_days"][0].update(  # type: ignore[index,union-attr]
                {"content_snapshot_at": f"{DAY.isoformat()}T14:59:58Z"}
            ),
            "day end",
        ),
        (
            lambda value: value["operations_days"][0].update(  # type: ignore[index,union-attr]
                {"content_scope": "governance_corpus_2021_plus_kst_day_end_v1"}
            ),
            "corpus scope",
        ),
    ],
)
def test_build_observation_rejects_unverifiable_inputs(mutator, message: str) -> None:  # type: ignore[no-untyped-def]
    payload = export_payload()
    mutator(payload)
    with pytest.raises(QualitySnapshotError, match=message):
        build_observation(payload, observation_date=DAY, revision=REVISION)


def test_freeze_posts_exact_ack_and_reexports_immutable_snapshot() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.headers["Authorization"] == "Bearer " + "token-" * 8
        if request.method == "GET":
            assignment = (
                "database_corpus_snapshot"
                if len(calls) == 1
                else "immutable_quality_observation"
            )
            return httpx.Response(200, json=export_payload(assignment=assignment))
        body = json.loads(request.content)
        assert body["observations"][0]["code_revision"] == REVISION
        return httpx.Response(
            202,
            json={
                "ok": True,
                "accepted_count": 1,
                "inserted_count": 1,
                "duplicate_count": 0,
            },
        )

    client = QualitySnapshotClient(
        "https://api.example/activist/api.php",
        "token-" * 8,
        transport=httpx.MockTransport(handler),
    )
    result = freeze_quality_snapshot(client, observation_date=DAY, revision=REVISION)
    assert result["ok"] is True
    assert result["quality_payload_sha256"] == "b" * 64
    assert [request.method for request in calls] == ["GET", "POST", "GET"]


def test_freeze_rejects_partial_ack() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            return httpx.Response(200, json=export_payload())
        return httpx.Response(
            202,
            json={"ok": True, "accepted_count": 1, "inserted_count": 0, "duplicate_count": 0},
        )

    client = QualitySnapshotClient(
        "https://api.example/api/v1",
        "x" * 32,
        transport=httpx.MockTransport(handler),
    )
    with pytest.raises(QualitySnapshotError, match="ACK mismatch"):
        freeze_quality_snapshot(client, observation_date=DAY, revision=REVISION)


@pytest.mark.parametrize(
    "url",
    [
        "http://api.example/api/v1",
        "https://user:secret@api.example/api/v1",
        "https://api.example/api/v1?token=secret",
    ],
)
def test_client_rejects_unsafe_api_urls(url: str) -> None:
    with pytest.raises(QualitySnapshotError):
        QualitySnapshotClient(url, "x" * 32)
