from __future__ import annotations

import hashlib
import json
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Mapping

import httpx
import pytest

from curator.shadow_compare import (
    ShadowComparisonError,
    aggregate_engine_outputs,
    extract_artifacts,
    run,
)
from curator.shadow_engine import (
    ShadowEngineError,
    build_engine_snapshot,
    canonical_json_bytes,
    comparison_keys_sha256,
    validate_engine_snapshot,
    write_candidate_snapshot_from_events,
    write_engine_snapshot,
)


REVISION = "a" * 40
OBSERVED = date(2026, 7, 21)
TOKEN = "editor-token-" + "x" * 40


def key(number: int) -> str:
    return f"eventcmp:v1:{number:064x}"


def snapshot(
    engine: str,
    keys: list[str],
    *,
    status: str = "succeeded",
    source_run_id: str | None = None,
    observed: date = OBSERVED,
) -> dict[str, object]:
    resolved_source_run_id = source_run_id or (
        f"github:{111 if engine == 'legacy' else 122}:1"
    )
    records = [
        {
            "comparison_key": value,
            "identity_status": "complete",
            "occurred_at": f"{observed.isoformat()}T03:00:00+09:00",
            "document_ids": [f"dart:{int(value.rsplit(':', 1)[1], 16):014d}"],
            **(
                {"guid": f"legacy:{int(value.rsplit(':', 1)[1], 16)}"}
                if engine == "legacy"
                else {}
            ),
        }
        for value in keys
    ]
    return build_engine_snapshot(
        engine=engine,
        observation_date=observed,
        code_revision=REVISION,
        status=status,
        records=records,
        source_run_id=resolved_source_run_id,
        producer_run_id=resolved_source_run_id,
        generated_at=datetime.combine(observed, datetime.min.time(), timezone.utc),
    )


def artifact_row(
    engine: str, artifact_id: int, *, observed: date = OBSERVED
) -> dict[str, object]:
    workflow_run_id = artifact_id + 100
    return {
        "engine": engine,
        "artifact_id": artifact_id,
        "artifact_name": (
            f"shadow-engine-{engine}-{observed.isoformat()}-{workflow_run_id}-1"
        ),
        "artifact_digest": "sha256:" + "b" * 64,
        "workflow_run_id": workflow_run_id,
        "workflow_path": (
            ".github/workflows/build-feed.yml"
            if engine == "legacy"
            else ".github/workflows/ingest-official.yml"
        ),
        "head_sha": REVISION,
    }


def write_inputs(
    root: Path,
    *,
    legacy_keys: list[str] | None = None,
    candidate_keys: list[str] | None = None,
    candidate_status: str = "succeeded",
    observed: date = OBSERVED,
) -> tuple[Path, Path]:
    manifest = {
        "schema_version": 2,
        "observation_date": observed.isoformat(),
        "code_revision": REVISION,
        "corpus_scope": "same_sha_cumulative_kst_day_end_v1",
        "artifacts": [
            artifact_row("legacy", 11, observed=observed),
            artifact_row("candidate", 22, observed=observed),
        ],
        "previous_comparison": None,
    }
    manifest_path = root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    snapshots = root / "snapshots"
    for artifact_id, payload in (
        (
            11,
            snapshot(
                "legacy",
                [key(1), key(2)] if legacy_keys is None else legacy_keys,
                observed=observed,
            ),
        ),
        (
            22,
            snapshot(
                "candidate",
                [key(2), key(3)] if candidate_keys is None else candidate_keys,
                status=candidate_status,
                observed=observed,
            ),
        ),
    ):
        destination = snapshots / str(artifact_id) / "engine-output.json"
        write_engine_snapshot(destination, payload)
    return manifest_path, snapshots


def write_record_inputs(
    root: Path,
    *,
    legacy_records: list[dict[str, object]],
    candidate_records: list[dict[str, object]],
) -> tuple[Path, Path]:
    manifest = {
        "schema_version": 2,
        "observation_date": OBSERVED.isoformat(),
        "code_revision": REVISION,
        "corpus_scope": "same_sha_cumulative_kst_day_end_v1",
        "artifacts": [artifact_row("legacy", 11), artifact_row("candidate", 22)],
        "previous_comparison": None,
    }
    manifest_path = root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    snapshots = root / "snapshots"
    for artifact_id, engine, records in (
        (11, "legacy", legacy_records),
        (22, "candidate", candidate_records),
    ):
        payload = build_engine_snapshot(
            engine=engine,
            observation_date=OBSERVED,
            code_revision=REVISION,
            status="succeeded",
            records=records,
            source_run_id=f"github:{111 if engine == 'legacy' else 122}:1",
            producer_run_id=f"github:{111 if engine == 'legacy' else 122}:1",
            generated_at=datetime(2026, 7, 21, 15, tzinfo=timezone.utc),
        )
        write_engine_snapshot(
            snapshots / str(artifact_id) / "engine-output.json", payload
        )
    return manifest_path, snapshots


def chain_previous_receipt(
    root: Path,
    manifest_path: Path,
    snapshots: Path,
) -> dict[str, object]:
    previous_day = OBSERVED - timedelta(days=1)
    (root / "previous").mkdir(parents=True, exist_ok=True)
    previous_manifest, previous_snapshots = write_inputs(
        root / "previous",
        observed=previous_day,
    )
    previous_report = {
        **aggregate_engine_outputs(previous_manifest, previous_snapshots),
        "api_ack": {},
        "generated_at": "2026-07-20T15:30:00Z",
        "distribution_mode": "web_only",
    }
    previous_report["report_sha256"] = hashlib.sha256(
        canonical_json_bytes(previous_report)
    ).hexdigest()
    artifact_id = 99
    destination = snapshots / str(artifact_id) / "shadow-comparison.json"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(previous_report), encoding="utf-8")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["previous_comparison"] = {
        "artifact_id": artifact_id,
        "artifact_name": (
            f"governance-shadow-comparison-{previous_day.isoformat()}-{REVISION}"
        ),
        "artifact_digest": "sha256:" + "c" * 64,
        "workflow_run_id": 999,
        "workflow_path": ".github/workflows/shadow-compare.yml",
        "head_sha": REVISION,
        "observation_date": previous_day.isoformat(),
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return previous_report


def candidate_record(
    comparison_key: str,
    *,
    document_id: str | None = None,
    canonical_url: str | None = None,
    title: str = "candidate title",
) -> dict[str, object]:
    record: dict[str, object] = {
        "comparison_key": comparison_key,
        "identity_status": "complete",
        "occurred_at": "2026-07-21T03:00:00+09:00",
        "title": title,
    }
    if document_id is not None:
        record["document_ids"] = [document_id]
    if canonical_url is not None:
        record["canonical_url"] = canonical_url
    return record


def legacy_record(
    record_id: str,
    *,
    document_id: str | None = None,
    canonical_url: str | None = None,
    title: str = "legacy title",
) -> dict[str, object]:
    record: dict[str, object] = {
        "guid": record_id,
        "published_at": "2026-07-21T03:00:00+09:00",
        "representative_title": title,
    }
    if document_id is not None:
        record["document_id"] = document_id
    if canonical_url is not None:
        record["articles"] = [{"canonical_url": canonical_url, "title": title}]
    return record


def test_engine_snapshot_keeps_only_explicit_same_day_canonical_keys() -> None:
    payload = build_engine_snapshot(
        engine="candidate",
        observation_date=OBSERVED,
        code_revision=REVISION,
        status="succeeded",
        records=[
            {
                "comparison_key": key(2),
                "identity_status": "complete",
                "occurred_at": "2026-07-21T08:00:00+09:00",
                "document_ids": ["dart:20260721000002"],
            },
            {
                "comparison_key": key(1),
                "identity_status": "complete",
                "occurred_at": "2026-07-21T01:00:00+09:00",
                "document_ids": ["dart:20260721000001"],
            },
            {
                "comparison_key": key(2),
                "identity_status": "complete",
                "occurred_at": "2026-07-21T09:00:00+09:00",
                "document_ids": ["dart:20260721000002"],
            },
            {
                "comparison_key": key(9),
                "identity_status": "complete",
                "occurred_at": "2026-07-20T23:59:59+09:00",
                "document_ids": ["dart:20260720000009"],
            },
            {
                "identity_status": "needs_review",
                "occurred_at": "2026-07-21T10:00:00+09:00",
            },
        ],
        source_run_id="github:100:1",
        generated_at=datetime(2026, 7, 21, 15, tzinfo=timezone.utc),
    )
    assert payload["events"] == [
        {
            "comparison_key": key(1),
            "source_evidence": [
                {"kind": "document_id", "value": "dart:20260721000001"},
                {"kind": "official_receipt", "value": "dart:20260721000001"},
            ],
        },
        {
            "comparison_key": key(2),
            "source_evidence": [
                {"kind": "document_id", "value": "dart:20260721000002"},
                {"kind": "official_receipt", "value": "dart:20260721000002"},
            ],
        },
    ]
    assert payload["event_count"] == 2
    assert payload["noncanonical_record_count"] == 1
    assert payload["events_sha256"] == comparison_keys_sha256([key(1), key(2)])
    assert payload["record_scope"] == "kst_observation_day_delta_v1"
    validate_engine_snapshot(payload)


@pytest.mark.parametrize(
    "record",
    [
        {"identity_status": "complete", "occurred_at": "2026-07-21T00:00:00+09:00"},
        {
            "comparison_key": "eventcmp:v1:not-a-key",
            "identity_status": "needs_review",
            "occurred_at": "2026-07-21T00:00:00+09:00",
        },
    ],
)
def test_engine_snapshot_rejects_missing_or_invalid_canonical_keys(record: dict[str, object]) -> None:
    with pytest.raises(ShadowEngineError, match="comparison_key"):
        build_engine_snapshot(
            engine="candidate",
            observation_date=OBSERVED,
            code_revision=REVISION,
            status="succeeded",
            records=[record],
            source_run_id="github:1:1",
        )


def test_engine_snapshot_detects_content_tampering() -> None:
    payload = snapshot("candidate", [key(1)])
    events = payload["events"]
    assert isinstance(events, list) and isinstance(events[0], dict)
    events[0]["comparison_key"] = key(2)
    with pytest.raises(ShadowEngineError, match="events_sha256"):
        validate_engine_snapshot(payload)


def test_candidate_snapshot_preserves_collection_run_and_github_producer_provenance(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GITHUB_RUN_ID", "122")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "3")
    output = tmp_path / "candidate.json"
    payload = write_candidate_snapshot_from_events(
        [candidate_record(key(1), document_id="dart:20260721000123")],
        observation_date=OBSERVED,
        status="succeeded",
        output_path=output,
        code_revision=REVISION,
        source_run_id="officialrun:collection-1",
        generated_at=datetime(2026, 7, 21, 15, tzinfo=timezone.utc),
    )
    assert payload["source_run_id"] == "officialrun:collection-1"
    assert payload["producer_run_id"] == "github:122:3"
    assert json.loads(output.read_text(encoding="utf-8")) == payload


def test_aggregate_unions_real_artifacts_into_sorted_unique_engine_sets(tmp_path: Path) -> None:
    manifest_path, snapshots = write_inputs(tmp_path)
    aggregate = aggregate_engine_outputs(manifest_path, snapshots)
    engines = aggregate["engines"]
    assert isinstance(engines, dict)
    assert engines["legacy"]["comparison_keys"] == [key(1), key(2)]  # type: ignore[index]
    assert engines["candidate"]["comparison_keys"] == [key(2), key(3)]  # type: ignore[index]
    assert engines["candidate"]["events_sha256"] == comparison_keys_sha256(  # type: ignore[index]
        [key(2), key(3)]
    )
    crosswalk = aggregate["legacy_crosswalk"]
    assert isinstance(crosswalk, dict)
    assert crosswalk["eligible_legacy_record_count"] == 2
    assert crosswalk["crosswalked_legacy_record_count"] == 2
    assert crosswalk["coverage_rate"] == 1.0


def test_no_disclosure_day_chains_verified_nonempty_cumulative_corpus(
    tmp_path: Path,
) -> None:
    current = tmp_path / "current"
    current.mkdir()
    manifest_path, snapshots = write_inputs(
        current,
        legacy_keys=[],
        candidate_keys=[],
    )
    previous_report = chain_previous_receipt(tmp_path, manifest_path, snapshots)
    aggregate = aggregate_engine_outputs(manifest_path, snapshots)

    assert aggregate["engines"]["legacy"]["comparison_keys"] == [key(1), key(2)]  # type: ignore[index]
    assert aggregate["engines"]["candidate"]["comparison_keys"] == [key(2), key(3)]  # type: ignore[index]
    assert aggregate["legacy_crosswalk"]["eligible_legacy_record_count"] == 2  # type: ignore[index]
    assert aggregate["corpus"] == {
        "schema_version": 1,
        "scope": "same_sha_cumulative_kst_day_end_v1",
        "start_date": "2026-07-20",
        "end_date": "2026-07-21",
        "day_count": 2,
        "previous_observation_date": "2026-07-20",
        "previous_report_sha256": previous_report["report_sha256"],
        "current_source_artifact_count": 2,
        "current_source_artifacts_sha256": aggregate["corpus"][  # type: ignore[index]
            "current_source_artifacts_sha256"
        ],
        "corpus_payload_sha256": previous_report["corpus"][  # type: ignore[index]
            "corpus_payload_sha256"
        ],
    }


def test_empty_bootstrap_day_still_rejects_zero_denominator(tmp_path: Path) -> None:
    manifest_path, snapshots = write_inputs(
        tmp_path,
        legacy_keys=[],
        candidate_keys=[],
    )
    with pytest.raises(ShadowComparisonError, match="denominator must be non-zero"):
        aggregate_engine_outputs(manifest_path, snapshots)


def test_aggregate_crosswalks_exact_document_receipt_and_canonical_url(
    tmp_path: Path,
) -> None:
    dart_receipt = "20260721000123"
    canonical = "https://example.test/disclosure/2?b=2&a=1&utm_source=ignored"
    manifest_path, snapshots = write_record_inputs(
        tmp_path,
        legacy_records=[
            legacy_record(
                "legacy:dart",
                canonical_url=(
                    "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=" + dart_receipt
                ),
            ),
            legacy_record(
                "legacy:url",
                canonical_url="https://example.test/disclosure/2?a=1&b=2",
            ),
        ],
        candidate_records=[
            candidate_record(key(1), document_id=f"dart:{dart_receipt}"),
            candidate_record(key(2), canonical_url=canonical),
        ],
    )

    aggregate = aggregate_engine_outputs(manifest_path, snapshots)
    engines = aggregate["engines"]
    assert isinstance(engines, dict)
    assert engines["legacy"]["comparison_keys"] == [key(1), key(2)]  # type: ignore[index]
    crosswalk = aggregate["legacy_crosswalk"]
    assert isinstance(crosswalk, dict)
    assert crosswalk == {
        "schema_version": 1,
        "eligible_legacy_record_count": 2,
        "crosswalked_legacy_record_count": 2,
        "unmatched_legacy_record_count": 0,
        "ambiguous_legacy_record_count": 0,
        "coverage_rate": 1.0,
        "crosswalk_sha256": crosswalk["crosswalk_sha256"],
    }
    assert len(str(crosswalk["crosswalk_sha256"])) == 64
    rows = aggregate["legacy_crosswalk_records"]
    assert isinstance(rows, list)
    assert [row["mapping_basis"] for row in rows] == [  # type: ignore[index]
        "stable_source_evidence",
        "stable_source_evidence",
    ]


def test_aggregate_rejects_ambiguous_candidate_evidence(tmp_path: Path) -> None:
    shared = "dart:20260721000123"
    manifest_path, snapshots = write_record_inputs(
        tmp_path,
        legacy_records=[legacy_record("legacy:1", document_id=shared)],
        candidate_records=[
            candidate_record(key(1), document_id=shared),
            candidate_record(key(2), document_id=shared),
        ],
    )
    with pytest.raises(ShadowComparisonError, match="multiple canonical events"):
        aggregate_engine_outputs(manifest_path, snapshots)


def test_aggregate_rejects_one_legacy_record_linking_two_events(tmp_path: Path) -> None:
    first = "dart:20260721000123"
    second = "dart:20260721000456"
    legacy = legacy_record("legacy:1")
    legacy["document_ids"] = [first, second]
    manifest_path, snapshots = write_record_inputs(
        tmp_path,
        legacy_records=[legacy],
        candidate_records=[
            candidate_record(key(1), document_id=first),
            candidate_record(key(2), document_id=second),
        ],
    )
    with pytest.raises(ShadowComparisonError, match="map to multiple canonical"):
        aggregate_engine_outputs(manifest_path, snapshots)


def test_aggregate_rejects_missing_official_crosswalk_link(tmp_path: Path) -> None:
    manifest_path, snapshots = write_record_inputs(
        tmp_path,
        legacy_records=[
            legacy_record("legacy:1", document_id="dart:20260721000123")
        ],
        candidate_records=[
            candidate_record(key(1), document_id="dart:20260721000999")
        ],
    )
    with pytest.raises(ShadowComparisonError, match="no canonical crosswalk key"):
        aggregate_engine_outputs(manifest_path, snapshots)


def test_aggregate_rejects_zero_eligible_legacy_denominator(tmp_path: Path) -> None:
    manifest_path, snapshots = write_record_inputs(
        tmp_path,
        legacy_records=[
            legacy_record(
                "legacy:1", canonical_url="https://news.example.test/unrelated"
            )
        ],
        candidate_records=[
            candidate_record(key(1), document_id="dart:20260721000123")
        ],
    )
    with pytest.raises(ShadowComparisonError, match="denominator must be non-zero"):
        aggregate_engine_outputs(manifest_path, snapshots)


def test_aggregate_never_guesses_crosswalk_from_equal_titles(tmp_path: Path) -> None:
    shared_title = "Same company value-up announcement"
    manifest_path, snapshots = write_record_inputs(
        tmp_path,
        legacy_records=[
            legacy_record(
                "legacy:1",
                canonical_url="https://news.example.test/legacy-only",
                title=shared_title,
            )
        ],
        candidate_records=[
            candidate_record(
                key(1),
                canonical_url="https://official.example.test/candidate-only",
                title=shared_title,
            )
        ],
    )
    with pytest.raises(ShadowComparisonError, match="denominator must be non-zero"):
        aggregate_engine_outputs(manifest_path, snapshots)


def test_aggregate_rejects_failed_engine_and_mixed_sha(tmp_path: Path) -> None:
    manifest_path, snapshots = write_inputs(tmp_path, candidate_status="failed")
    with pytest.raises(ShadowComparisonError, match="did not succeed"):
        aggregate_engine_outputs(manifest_path, snapshots)

    payload = json.loads(manifest_path.read_text())
    payload["artifacts"][1]["head_sha"] = "c" * 40
    manifest_path.write_text(json.dumps(payload))
    with pytest.raises(ShadowComparisonError, match="mixed-SHA"):
        aggregate_engine_outputs(manifest_path, snapshots)


def test_aggregate_rejects_source_run_provenance_mismatch(tmp_path: Path) -> None:
    manifest_path, snapshots = write_inputs(tmp_path)
    candidate_path = snapshots / "22" / "engine-output.json"
    write_engine_snapshot(
        candidate_path,
        snapshot(
            "candidate",
            [key(2), key(3)],
            source_run_id="github:999:1",
        ),
    )
    with pytest.raises(ShadowComparisonError, match="source-run provenance mismatch"):
        aggregate_engine_outputs(manifest_path, snapshots)


def test_extract_artifacts_verifies_archive_digest_and_single_safe_file(tmp_path: Path) -> None:
    manifest_path, _ = write_inputs(tmp_path)
    manifest = json.loads(manifest_path.read_text())
    archives = tmp_path / "archives"
    archives.mkdir()
    for item in manifest["artifacts"]:
        archive_path = archives / f"{item['artifact_id']}.zip"
        with zipfile.ZipFile(archive_path, "w") as archive:
            archive.writestr(
                "engine-output.json",
                json.dumps(snapshot(item["engine"], [key(item["artifact_id"])])),
            )
        item["artifact_digest"] = "sha256:" + hashlib.sha256(archive_path.read_bytes()).hexdigest()
    manifest_path.write_text(json.dumps(manifest))

    destination = tmp_path / "extracted"
    extract_artifacts(manifest_path, archives, destination)
    assert (destination / "11" / "engine-output.json").is_file()
    assert (destination / "22" / "engine-output.json").is_file()

    manifest["artifacts"][0]["artifact_digest"] = "sha256:" + "0" * 64
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ShadowComparisonError, match="digest mismatch"):
        extract_artifacts(manifest_path, archives, tmp_path / "bad")


class FakeShadowApi:
    def __init__(self) -> None:
        self.shadow_run: dict[str, object] | None = None
        self.shadow_runs: dict[tuple[str, str], dict[str, object]] = {}
        self.discrepancies: dict[str, dict[str, object]] = {}
        self.clock = 0
        self.corrupt_run_ack = False

    def timestamp(self) -> str:
        self.clock += 1
        return f"2026-07-21T15:00:{self.clock:02d}Z"

    @staticmethod
    def response(status: int, payload: Mapping[str, object]) -> httpx.Response:
        return httpx.Response(status, json=dict(payload))

    @staticmethod
    def page(rows: list[dict[str, object]]) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "ok": True,
                "data": rows,
                "pagination": {
                    "page": 1,
                    "limit": 100,
                    "returned": len(rows),
                    "has_more": False,
                    "next_page": None,
                },
            },
        )

    def __call__(self, request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == f"Bearer {TOKEN}"
        path = request.url.path
        if request.method == "GET" and path.endswith("/admin/shadow-runs"):
            params = request.url.params
            rows = list(self.shadow_runs.values())
            if params.get("from"):
                rows = [row for row in rows if row["observation_date"] >= params["from"]]
            if params.get("to"):
                rows = [row for row in rows if row["observation_date"] <= params["to"]]
            if params.get("code_revision"):
                rows = [row for row in rows if row["code_revision"] == params["code_revision"]]
            return self.page([dict(row) for row in rows])
        if request.method == "POST" and path.endswith("/admin/shadow-runs"):
            body = json.loads(request.content)
            run_key = (body["observation_date"], body["code_revision"])
            existing_run = self.shadow_runs.get(run_key)
            legacy_keys = [row["comparison_key"] for row in body["legacy_run"]["events"]]
            candidate_keys = [row["comparison_key"] for row in body["candidate_run"]["events"]]
            if existing_run is not None:
                assert body["expected_updated_at"] == existing_run["updated_at"]
            updated_at = self.timestamp()
            self.shadow_run = {
                "observation_date": body["observation_date"],
                "code_revision": body["code_revision"],
                "legacy_crosswalk": body["legacy_crosswalk"],
                "legacy_run": {
                    "status": "succeeded",
                    "events": [{"comparison_key": value} for value in legacy_keys],
                    "event_count": len(legacy_keys),
                    "events_sha256": comparison_keys_sha256(legacy_keys),
                },
                "candidate_run": {
                    "status": "succeeded",
                    "events": [{"comparison_key": value} for value in candidate_keys],
                    "event_count": len(candidate_keys),
                    "events_sha256": comparison_keys_sha256(candidate_keys),
                },
                "updated_at": updated_at,
            }
            self.shadow_runs[run_key] = self.shadow_run
            legacy_count = len(legacy_keys) + (1 if self.corrupt_run_ack else 0)
            return self.response(
                201,
                {
                    "ok": True,
                    "observation_date": body["observation_date"],
                    "code_revision": body["code_revision"],
                    "legacy_event_count": legacy_count,
                    "candidate_event_count": len(candidate_keys),
                    "legacy_crosswalk": body["legacy_crosswalk"],
                    "updated_at": updated_at,
                },
            )
        if request.method == "GET" and path.endswith("/admin/shadow-discrepancies"):
            params = request.url.params
            rows = list(self.discrepancies.values())
            if params.get("review_status"):
                rows = [row for row in rows if row["review_status"] == params["review_status"]]
            if params.get("from"):
                rows = [row for row in rows if row["observation_date"] >= params["from"]]
            if params.get("to"):
                rows = [row for row in rows if row["observation_date"] <= params["to"]]
            if params.get("code_revision"):
                rows = [row for row in rows if row["code_revision"] == params["code_revision"]]
            return self.page(sorted(rows, key=lambda row: str(row["discrepancy_id"])))
        if request.method == "POST" and path.endswith("/admin/shadow-discrepancies"):
            body = json.loads(request.content)
            existing = self.discrepancies.get(body["discrepancy_id"])
            if existing is not None:
                assert body["expected_updated_at"] == existing["updated_at"]
            updated_at = self.timestamp()
            row = {
                **{key: value for key, value in body.items() if key != "expected_updated_at"},
                "updated_at": updated_at,
            }
            self.discrepancies[body["discrepancy_id"]] = row
            return self.response(
                200 if existing else 201,
                {
                    "ok": True,
                    "discrepancy_id": body["discrepancy_id"],
                    "review_status": body["review_status"],
                    "updated_at": updated_at,
                },
            )
        return self.response(404, {"ok": False, "error": "not_found"})


def test_runner_persists_idempotent_snapshot_and_pending_differences(tmp_path: Path) -> None:
    manifest_path, snapshots = write_inputs(tmp_path)
    fake = FakeShadowApi()
    transport = httpx.MockTransport(fake)
    output = tmp_path / "report.json"

    report = run(
        manifest_path=manifest_path,
        snapshots_root=snapshots,
        api_base_url="https://example.test/activist/api.php/api/v1",
        token=TOKEN,
        output_path=output,
        transport=transport,
    )
    ack = report["api_ack"]
    assert isinstance(ack, dict)
    assert ack["legacy_event_count"] == 2
    assert ack["candidate_event_count"] == 2
    assert ack["discrepancy_count"] == 2
    assert ack["discrepancies_created"] == 2
    assert {row["discrepancy_type"] for row in fake.discrepancies.values()} == {
        "candidate_missing",
        "candidate_added",
    }
    assert {row["review_status"] for row in fake.discrepancies.values()} == {"pending"}
    assert report["distribution_mode"] == "web_only"
    assert ack["legacy_crosswalk"]["eligible_legacy_record_count"] == 2  # type: ignore[index]
    assert output.is_file()

    replay = run(
        manifest_path=manifest_path,
        snapshots_root=snapshots,
        api_base_url="https://example.test/activist/api.php/api/v1",
        token=TOKEN,
        output_path=tmp_path / "replay.json",
        transport=httpx.MockTransport(fake),
    )
    replay_ack = replay["api_ack"]
    assert isinstance(replay_ack, dict)
    assert replay_ack["discrepancies_created"] == 0
    assert replay_ack["discrepancies_retained"] == 2


def test_runner_verifies_previous_receipt_against_durable_shadow_run(
    tmp_path: Path,
) -> None:
    current = tmp_path / "current"
    current.mkdir()
    manifest_path, snapshots = write_inputs(
        current,
        legacy_keys=[],
        candidate_keys=[],
    )
    previous_report = chain_previous_receipt(tmp_path, manifest_path, snapshots)
    previous_day = (OBSERVED - timedelta(days=1)).isoformat()
    previous_engines = previous_report["engines"]
    assert isinstance(previous_engines, dict)
    legacy_keys = previous_engines["legacy"]["comparison_keys"]
    candidate_keys = previous_engines["candidate"]["comparison_keys"]
    fake = FakeShadowApi()
    fake.shadow_runs[(previous_day, REVISION)] = {
        "observation_date": previous_day,
        "code_revision": REVISION,
        "legacy_crosswalk": previous_report["legacy_crosswalk"],
        "legacy_run": {
            "status": "succeeded",
            "events": [{"comparison_key": value} for value in legacy_keys],
            "event_count": len(legacy_keys),
            "events_sha256": comparison_keys_sha256(legacy_keys),
        },
        "candidate_run": {
            "status": "succeeded",
            "events": [{"comparison_key": value} for value in candidate_keys],
            "event_count": len(candidate_keys),
            "events_sha256": comparison_keys_sha256(candidate_keys),
        },
        "updated_at": "2026-07-20T15:45:00Z",
    }

    report = run(
        manifest_path=manifest_path,
        snapshots_root=snapshots,
        api_base_url="https://example.test/api/v1",
        token=TOKEN,
        output_path=tmp_path / "report.json",
        transport=httpx.MockTransport(fake),
    )
    assert report["corpus"]["day_count"] == 2  # type: ignore[index]
    assert report["api_ack"]["legacy_event_count"] == 2  # type: ignore[index]
    assert report["api_ack"]["candidate_event_count"] == 2  # type: ignore[index]


def test_runner_rejects_silent_corpus_reset_when_previous_run_exists(
    tmp_path: Path,
) -> None:
    manifest_path, snapshots = write_inputs(tmp_path)
    fake = FakeShadowApi()
    previous_day = (OBSERVED - timedelta(days=1)).isoformat()
    fake.shadow_runs[(previous_day, REVISION)] = {
        "observation_date": previous_day,
        "code_revision": REVISION,
    }
    with pytest.raises(ShadowComparisonError, match="receipt was not chained"):
        run(
            manifest_path=manifest_path,
            snapshots_root=snapshots,
            api_base_url="https://example.test/api/v1",
            token=TOKEN,
            output_path=tmp_path / "report.json",
            transport=httpx.MockTransport(fake),
        )


def test_runner_rejects_pending_prior_day_before_writing(tmp_path: Path) -> None:
    manifest_path, snapshots = write_inputs(tmp_path)
    fake = FakeShadowApi()
    fake.discrepancies["shadow:" + "f" * 64] = {
        "discrepancy_id": "shadow:" + "f" * 64,
        "observation_date": "2026-07-20",
        "code_revision": REVISION,
        "comparison_key": key(9),
        "discrepancy_type": "candidate_added",
        "legacy_event": None,
        "candidate_event": {"comparison_key": key(9)},
        "review_status": "pending",
        "updated_at": "2026-07-20T15:00:00Z",
    }
    with pytest.raises(ShadowComparisonError, match="prior-day"):
        run(
            manifest_path=manifest_path,
            snapshots_root=snapshots,
            api_base_url="https://example.test/api/v1",
            token=TOKEN,
            output_path=tmp_path / "report.json",
            transport=httpx.MockTransport(fake),
        )
    assert fake.shadow_run is None


def test_runner_rejects_ack_mismatch(tmp_path: Path) -> None:
    manifest_path, snapshots = write_inputs(tmp_path)
    fake = FakeShadowApi()
    fake.corrupt_run_ack = True
    with pytest.raises(ShadowComparisonError, match="ACK legacy_event_count mismatch"):
        run(
            manifest_path=manifest_path,
            snapshots_root=snapshots,
            api_base_url="https://example.test/api/v1",
            token=TOKEN,
            output_path=tmp_path / "report.json",
            transport=httpx.MockTransport(fake),
        )


def test_workflow_is_daily_non_cancelling_same_sha_and_web_only() -> None:
    workflow = Path(".github/workflows/shadow-compare.yml").read_text(encoding="utf-8")
    assert 'cron: "20 15 * * *"' in workflow
    assert "cancel-in-progress: false" in workflow
    assert "GOVERNANCE_PIPELINE_MODE == 'shadow'" in workflow
    assert "GOVERNANCE_PIPELINE_MODE == 'live'" in workflow
    assert "mixed SHA" in workflow
    assert "shadow-engine-legacy-${observationDate}-" in workflow
    assert "shadow-engine-candidate-${observationDate}-" in workflow
    assert "governance-shadow-comparison-${previousDate}-${expectedSha}" in workflow
    assert "same_sha_cumulative_kst_day_end_v1" in workflow
    assert ".previous_comparison.artifact_id // empty" in workflow
    assert "BSIDE_EDITOR_TOKEN" in workflow
    assert "eligible_legacy_record_count" in workflow
    assert "crosswalked_legacy_record_count" in workflow
    assert "Legacy crosswalk receipt is incomplete" in workflow
    assert "CURATOR_DISABLE_TELEGRAM_SEND: \"1\"" in workflow
    assert "TELEGRAM_BOT_TOKEN" not in workflow
    assert "TELEGRAM_CHAT_ID" not in workflow


def test_producer_workflows_preserve_actual_engine_outputs() -> None:
    legacy = Path(".github/workflows/build-feed.yml").read_text(encoding="utf-8")
    candidate = Path(".github/workflows/ingest-official.yml").read_text(encoding="utf-8")
    assert "snapshot-legacy-state" in legacy
    assert "data/state.json" in legacy
    assert "shadow-engine-legacy-" in legacy
    assert "CURATOR_SHADOW_ENGINE_OUTPUT_PATH" in candidate
    assert "shadow-engine-candidate-" in candidate
    assert "retention-days: 21" in legacy
    assert "retention-days: 21" in candidate
