from __future__ import annotations

import copy
import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from dataclasses import replace

import pytest

from curator import dart_frozen_replay_bundle as frozen
from curator import official_ingest
from curator import official_backfill
from curator.backfill_checkpoint_api import (
    RemoteCheckpointSnapshot,
    RemoteCheckpointWrite,
    canonical_checkpoint,
    checkpoint_payload_hash,
)


REVISION = "a" * 40
FINGERPRINT = "b" * 64
CHECKPOINT = "c" * 64


def matched_probe() -> dict[str, object]:
    start = date(2026, 7, 1)
    windows = []
    for index in range(30):
        window_start = start + timedelta(days=index)
        digest = hashlib.sha256(
            f"probe:{window_start.isoformat()}".encode()
        ).hexdigest()
        windows.append(
            {
                "index": index,
                "window_start": window_start.isoformat(),
                "window_end_exclusive": (
                    window_start + timedelta(days=1)
                ).isoformat(),
                "matched": True,
                "expected_stable_payload_sha256": digest,
                "actual_stable_payload_sha256": digest,
                "changed_entity_count": 0,
                "changes": [],
                "probe_execution": {
                    "source_requests": 1,
                    "source_pages": 1,
                    "source_rows_fetched": 0,
                },
            }
        )
    return {
        "schema_version": frozen.SCHEMA_VERSION,
        "kind": frozen.PROBE_KIND,
        "source": "dart",
        "code_revision": REVISION,
        "range_start": start.isoformat(),
        "range_end_exclusive": (start + timedelta(days=30)).isoformat(),
        "job_fingerprint": FINGERPRINT,
        "read_only": True,
        "governance_write_attempted": False,
        "checkpoint_write_attempted": False,
        "quota_ledger_write_attempted": True,
        "status": "matched",
        "window_count": 30,
        "windows": windows,
        "error_code": None,
        "artifact_sanitization": {
            "status": "verified",
            "contains_provider_response_body": False,
            "contains_credentials": False,
        },
    }


def validate_release_probe(tmp_path: Path, probe: dict[str, object]):
    path = tmp_path / "probe.json"
    path.write_text(json.dumps(probe), encoding="utf-8")
    return frozen._validate_probe(
        SimpleNamespace(
            path=path,
            expected_code_revision=REVISION,
            expected_from_date="2026-07-01",
            expected_to_date="2026-07-31",
            expected_job_fingerprint=FINGERPRINT,
        )
    )


def payload(*, title: str = "원문 제목", retrieved_at: str = "2026-07-01T00:00:00Z"):
    return {
        "companies": [
            {
                "company_id": "00126380",
                "legal_name": "삼성전자",
                "stock_code": "005930",
                "market": "KOSPI",
                "aliases": [],
            }
        ],
        "documents": [
            {
                "document_id": "dart:20260701000001",
                "company_id": "00126380",
                "source_right_id": "official:dart",
                "title": title,
                "original_language": "ko",
                "original_url": "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20260701000001",
                "retrieved_at": retrieved_at,
            }
        ],
        "events": [
            {
                "event_id": "event:dart:20260701000001",
                "company_id": "00126380",
                "title": title,
                "document_ids": ["dart:20260701000001"],
            }
        ],
        "source_rights": [],
    }


def run_record(window: int = 0):
    return {
        "run_id": f"run:{window:02d}",
        "source_key": "dart",
        "code_revision": REVISION,
        "idempotency_key": f"official-backfill-v1:{window:032x}",
        "ingest_mode": "apply",
        "stable_payload_contract_version": 1,
        "stable_payload_sha256": frozen.stable_payload_sha256(payload()),
        "fetched_count": 3,
        "resolved_count": 1,
        "accepted_count": 1,
        "error_count": 0,
        "source_outcomes": {
            "dart": {
                "fetched": 3,
                "accepted": 1,
                "rejected_non_governance": 2,
                "duplicate_count": 0,
                "discarded_valid_count": 0,
                "error_count": 0,
            }
        },
    }


def leaf(index: int = 0):
    return frozen.build_window_leaf(
        code_revision=REVISION,
        job_fingerprint=FINGERPRINT,
        window_start=f"2026-07-{index + 1:02d}",
        window_end_exclusive=f"2026-07-{index + 2:02d}",
        idempotency_key=f"official-backfill-v1:{index:032x}",
        payload=payload(),
        run=run_record(index),
    )


def test_semantic_digest_excludes_only_retrieval_clock_and_reports_hash_only_drift():
    expected = leaf()
    assert frozen.stable_payload_sha256(payload()) == (
        official_ingest._stable_dart_payload_sha256(payload())
    )
    same = payload(retrieved_at="2026-07-01T02:00:00Z")
    assert frozen.public_payload_semantic_sha256(payload()) == (
        frozen.public_payload_semantic_sha256(same)
    )
    comparison = frozen.compare_fresh_payload(
        expected,
        same,
        actual_source_counts=expected["source_semantic_counts"],
    )
    assert comparison["matched"] is True

    changed = payload(title="변경된 원문 제목")
    drift = frozen.compare_fresh_payload(
        expected,
        changed,
        actual_source_counts=expected["source_semantic_counts"],
    )
    assert drift["matched"] is False
    rendered = json.dumps(drift, ensure_ascii=False)
    assert "변경된 원문 제목" not in rendered
    assert {row["entity"] for row in drift["changes"]} == {"document", "event"}
    assert all("expected_leaf_sha256" in row for row in drift["changes"])


def test_frozen_database_replay_never_touches_opendart_connector_or_quota(
    monkeypatch: pytest.MonkeyPatch,
):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("frozen replay must not access OpenDART or quota")

    eligibility = SimpleNamespace(
        rights_revision="1" * 64,
        contract_revision="2" * 64,
        release_state="closed",
    )

    class Rights:
        def preflight(self, revision):
            assert revision == REVISION
            return eligibility

    observed = {}

    def sync(submitted, *, run):
        observed["payload"] = submitted
        observed["run"] = run
        return {
            "official_remote_synced": 1,
            "official_remote_failed": 0,
            "official_remote_skipped": 0,
            "official_remote_batches_attempted": 1,
            "official_remote_run_persisted": 1,
            "official_remote_ack_mismatches": 0,
            "official_remote_raw_count": 1,
            "official_remote_ack_count": 1,
            "official_remote_failure_telemetry_count": 0,
            "official_remote_failure_response_body_bytes": 0,
            "official_remote_failure_elapsed_ms": 0,
        }

    monkeypatch.setenv("GITHUB_SHA", REVISION)
    monkeypatch.setenv("CURATOR_REQUIRE_REMOTE_API", "1")
    monkeypatch.setattr(official_ingest, "load_opendart_credentials", forbidden)
    monkeypatch.setattr(official_ingest, "DartConnector", forbidden)
    monkeypatch.setattr(official_ingest, "DartInvocationQuota", forbidden)
    monkeypatch.setattr(official_ingest, "DartOfficialSourceRightClient", Rights)
    monkeypatch.setattr(official_ingest, "sync_governance_payload", sync)

    result = official_ingest.replay_frozen_dart_window(
        leaf(),
        now=datetime(2026, 7, 29, 4, 0, tzinfo=timezone.utc),
    )

    assert result["official_frozen_replay"] == 1
    assert result["official_dart_requests"] == 0
    assert result["official_dart_pages"] == 0
    assert result["official_remote_run_persisted"] == 1
    assert observed["run"]["stable_payload_sha256"] == leaf()[
        "stable_payload_sha256"
    ]


def test_bundle_is_exactly_bound_and_tampering_or_extra_files_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    root = tmp_path / "bundle"
    metadata = [
        frozen.write_window_leaf(root, index=index, leaf=leaf(index))
        for index in range(30)
    ]
    manifest = frozen.finalize_apply_bundle(
        root,
        code_revision=REVISION,
        job_fingerprint=FINGERPRINT,
        range_start="2026-07-01",
        range_end_exclusive="2026-07-31",
        checkpoint_payload_sha256=CHECKPOINT,
        checkpoint_version=31,
        window_metadata=metadata,
    )
    manifest_raw = (root / "manifest.json").read_bytes()
    binding = {
        "schema_version": 1,
        "kind": frozen.ARTIFACT_BINDING_KIND,
        "consumer": {"code_revision": REVISION},
        "producer": {"run_id": 123, "run_attempt": 1},
        "artifact": {
            "name": "official-dart-frozen-replay-apply-123-1",
            "digest": "sha256:" + "d" * 64,
        },
        "job_fingerprint": FINGERPRINT,
        "range_start": "2026-07-01",
        "range_end_exclusive": "2026-07-31",
        "checkpoint_payload_sha256": CHECKPOINT,
        "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
        "leaf_sha256": [row["sha256"] for row in metadata],
    }
    binding["binding_sha256"] = frozen.canonical_sha256(binding)
    binding_path = tmp_path / "binding.json"
    binding_path.write_text(json.dumps(binding), encoding="utf-8")

    loaded = frozen.load_bundle(
        root,
        binding_path,
        expected_code_revision=REVISION,
        expected_job_fingerprint=FINGERPRINT,
        expected_checkpoint_sha256=CHECKPOINT,
    )
    assert loaded.manifest == manifest
    assert frozen.load_window(loaded, index=0)["payload"] == leaf()["payload"]

    (root / "extra.txt").write_text("x", encoding="utf-8")
    with pytest.raises(frozen.FrozenReplayBundleError, match="extra or missing"):
        frozen.load_bundle(
            root,
            binding_path,
            expected_code_revision=REVISION,
            expected_job_fingerprint=FINGERPRINT,
            expected_checkpoint_sha256=CHECKPOINT,
        )


def test_build_binding_checks_producer_artifact_and_production_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    root = tmp_path / "bundle"
    metadata = [
        frozen.write_window_leaf(root, index=index, leaf=leaf(index))
        for index in range(30)
    ]
    frozen.finalize_apply_bundle(
        root,
        code_revision=REVISION,
        job_fingerprint=FINGERPRINT,
        range_start="2026-07-01",
        range_end_exclusive="2026-07-31",
        checkpoint_payload_sha256=CHECKPOINT,
        checkpoint_version=31,
        window_metadata=metadata,
    )
    replay_state = tmp_path / "state.json"
    replay_state.write_text(
        json.dumps(
            {
                "checkpoint": {
                    "job_fingerprint": FINGERPRINT,
                    "checkpoint_payload_sha256": CHECKPOINT,
                    "completed_window_count": 30,
                    "failed_window_count": 0,
                }
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "binding.json"
    binding = frozen.build_artifact_binding(
        bundle_root=root,
        replay_state_path=replay_state,
        output_path=output,
        artifact_id=99,
        artifact_name="official-dart-frozen-replay-apply-123-1",
        artifact_digest="d" * 64,
        artifact_created_at="2026-07-29T03:00:00Z",
        producer_run_id=123,
        producer_run_attempt=1,
        producer_run_started_at="2026-07-29T02:00:00Z",
        consumer_repository="owner/repo",
        consumer_workflow="official-backfill.yml",
        consumer_run_id=456,
        consumer_run_attempt=1,
        consumer_code_revision=REVISION,
        expected_range_start="2026-07-01",
        expected_range_end_exclusive="2026-07-31",
        now=datetime(2026, 7, 29, 4, tzinfo=timezone.utc),
    )
    assert output.is_file()
    assert binding["job_fingerprint"] == FINGERPRINT
    assert binding["leaf_sha256"] == [row["sha256"] for row in metadata]


def test_strict_json_rejects_duplicate_keys_and_non_finite_values(tmp_path: Path):
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text('{"kind":"x","kind":"y"}', encoding="utf-8")
    with pytest.raises(frozen.FrozenReplayBundleError, match="strict"):
        frozen._strict_object(duplicate, max_bytes=1000)

    non_finite = tmp_path / "nan.json"
    non_finite.write_text('{"value":NaN}', encoding="utf-8")
    with pytest.raises(frozen.FrozenReplayBundleError, match="strict"):
        frozen._strict_object(non_finite, max_bytes=1000)


def test_probe_contract_admits_quota_ledger_but_no_governance_or_checkpoint_write(
    tmp_path: Path,
):
    report = frozen.write_probe_report(
        tmp_path / "probe.json",
        code_revision=REVISION,
        job_fingerprint=FINGERPRINT,
        range_start="2026-07-01",
        range_end_exclusive="2026-07-31",
        status="probe_failed",
        windows=[
            {
                "index": 0,
                "window_start": "2026-07-01",
                "window_end_exclusive": "2026-07-02",
                "matched": False,
                "status": "probe_failed",
                "error_code": "source_probe_failed",
            }
        ],
        error_code="source_probe_failed",
    )
    assert report["governance_write_attempted"] is False
    assert report["checkpoint_write_attempted"] is False
    assert report["quota_ledger_write_attempted"] is True
    assert "database_write_attempted" not in report
    diagnostic = frozen.validate_probe_contract(report)
    assert diagnostic["status"] == "probe_failed"
    assert diagnostic["fully_matched"] is False
    with pytest.raises(
        frozen.FrozenReplayBundleError,
        match="not fully matched",
    ):
        validate_release_probe(tmp_path, report)


def test_release_probe_accepts_only_exact_fully_matched_30_day_evidence(
    tmp_path: Path,
):
    result = validate_release_probe(tmp_path, matched_probe())

    assert result["status"] == "matched"
    assert result["window_count"] == 30
    assert result["matched_window_count"] == 30
    assert result["fully_matched"] is True


@pytest.mark.parametrize(
    "mutation",
    [
        "empty",
        "partial",
        "duplicate_index",
        "gap",
        "hash_mismatch",
        "zero_requests",
        "zero_pages",
        "negative_rows",
        "quota_not_durable",
        "top_error",
        "unverified_sanitization",
        "provider_body",
        "credentials",
    ],
)
def test_release_probe_rejects_incomplete_or_self_asserted_matched_evidence(
    tmp_path: Path,
    mutation: str,
):
    probe = matched_probe()
    windows = probe["windows"]
    assert isinstance(windows, list)
    if mutation == "empty":
        windows.clear()
        probe["window_count"] = 0
    elif mutation == "partial":
        windows.pop()
        probe["window_count"] = 29
    elif mutation == "duplicate_index":
        windows[1]["index"] = 0
    elif mutation == "gap":
        windows[8]["window_start"] = "2026-07-10"
        windows[8]["window_end_exclusive"] = "2026-07-11"
    elif mutation == "hash_mismatch":
        windows[12]["actual_stable_payload_sha256"] = "e" * 64
    elif mutation == "zero_requests":
        windows[3]["probe_execution"]["source_requests"] = 0
    elif mutation == "zero_pages":
        windows[3]["probe_execution"]["source_pages"] = 0
    elif mutation == "negative_rows":
        windows[3]["probe_execution"]["source_rows_fetched"] = -1
    elif mutation == "quota_not_durable":
        probe["quota_ledger_write_attempted"] = False
    elif mutation == "top_error":
        probe["error_code"] = "self_asserted_error"
    elif mutation == "unverified_sanitization":
        probe["artifact_sanitization"]["status"] = "unchecked"
    elif mutation == "provider_body":
        probe["artifact_sanitization"][
            "contains_provider_response_body"
        ] = True
    elif mutation == "credentials":
        probe["artifact_sanitization"]["contains_credentials"] = True
    else:  # pragma: no cover - the parameter list is closed above.
        raise AssertionError(mutation)

    with pytest.raises(frozen.FrozenReplayBundleError):
        validate_release_probe(tmp_path, probe)


def test_partial_resume_inventory_rejects_extra_files_and_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("GITHUB_RUN_ID", "321")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    root = tmp_path / "partial-inventory"
    metadata = frozen.write_window_leaf(root, index=0, leaf=leaf(0))
    frozen.write_partial_apply_bundle(
        root,
        code_revision=REVISION,
        job_fingerprint=FINGERPRINT,
        range_start="2026-07-01",
        range_end_exclusive="2026-07-31",
        checkpoint_payload_sha256=CHECKPOINT,
        checkpoint_version=1,
        completed_window_metadata=[metadata],
    )
    args = SimpleNamespace(
        path=root,
        expected_code_revision=REVISION,
        expected_job_fingerprint=FINGERPRINT,
    )
    assert frozen._validate_resume_tree(args)["completed_window_count"] == 1

    extra_file = root / "unexpected.txt"
    extra_file.write_text("not evidence", encoding="utf-8")
    with pytest.raises(
        frozen.FrozenReplayBundleError,
        match="inventory",
    ):
        frozen._validate_resume_tree(args)
    extra_file.unlink()

    extra_directory = root / "unexpected"
    extra_directory.mkdir()
    with pytest.raises(
        frozen.FrozenReplayBundleError,
        match="inventory",
    ):
        frozen._validate_resume_tree(args)
    extra_directory.rmdir()

    nested_directory = root / "windows" / "nested"
    nested_directory.mkdir()
    with pytest.raises(
        frozen.FrozenReplayBundleError,
        match="windows inventory",
    ):
        frozen._validate_resume_tree(args)


def test_partial_resume_inventory_rejects_symlink_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("GITHUB_RUN_ID", "322")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    root = tmp_path / "partial-links"
    metadata = frozen.write_window_leaf(root, index=0, leaf=leaf(0))
    frozen.write_partial_apply_bundle(
        root,
        code_revision=REVISION,
        job_fingerprint=FINGERPRINT,
        range_start="2026-07-01",
        range_end_exclusive="2026-07-31",
        checkpoint_payload_sha256=CHECKPOINT,
        checkpoint_version=1,
        completed_window_metadata=[metadata],
    )
    target = tmp_path / "outside.json"
    target.write_text("{}", encoding="utf-8")
    linked_leaf = root / "windows" / "01-2026-07-02.json"
    try:
        linked_leaf.symlink_to(target)
    except OSError:
        pytest.skip("this platform does not permit test symlink creation")

    with pytest.raises(
        frozen.FrozenReplayBundleError,
        match="windows inventory",
    ):
        frozen._validate_resume_tree(
            SimpleNamespace(
                path=root,
                expected_code_revision=REVISION,
                expected_job_fingerprint=FINGERPRINT,
            )
        )
    linked_leaf.unlink()
    target_directory = tmp_path / "outside-directory"
    target_directory.mkdir()
    linked_directory = root / "linked-directory"
    try:
        linked_directory.symlink_to(
            target_directory,
            target_is_directory=True,
        )
    except OSError:
        return
    with pytest.raises(
        frozen.FrozenReplayBundleError,
        match="inventory",
    ):
        frozen._validate_resume_tree(
            SimpleNamespace(
                path=root,
                expected_code_revision=REVISION,
                expected_job_fingerprint=FINGERPRINT,
            )
        )


class MemoryCheckpointStore:
    def __init__(self):
        self.record = None

    def get(self, _fingerprint):
        if self.record is None:
            return RemoteCheckpointSnapshot(checkpoint=None, version=0)
        version, checkpoint, digest = self.record
        return RemoteCheckpointSnapshot(
            checkpoint=copy.deepcopy(checkpoint),
            version=version,
            payload_hash=digest,
        )

    def put(self, _fingerprint, *, expected_version, checkpoint):
        normalized = canonical_checkpoint(checkpoint)
        digest = checkpoint_payload_hash(normalized)
        actual = 0 if self.record is None else self.record[0]
        assert actual == expected_version
        version = actual + 1
        self.record = (version, copy.deepcopy(normalized), digest)
        return RemoteCheckpointWrite(
            version=version,
            payload_hash=digest,
            unchanged=False,
        )


def _successful_summary(*, frozen_replay: bool = False):
    return {
        "official_fetched": 3,
        "official_documents": 1,
        "official_events": 1,
        "official_companies": 1,
        "official_source_rights": 0,
        "official_failed": 0,
        "official_skipped": 0,
        "official_dry_run": 0,
        "official_remote_synced": 1,
        "official_remote_run_persisted": 1,
        "official_remote_failed": 0,
        "official_remote_skipped": 0,
        "official_remote_ack_mismatches": 0,
        "official_remote_raw_count": 1,
        "official_remote_ack_count": 1,
        "official_dart_fetched": 3,
        "official_dart_accepted": 1,
        "official_dart_rejected": 2,
        "official_dart_duplicates": 0,
        "official_dart_discarded": 0,
        "official_dart_pages": 0 if frozen_replay else 1,
        "official_dart_requests": 0 if frozen_replay else 1,
        "official_dart_errors": 0,
        "official_dart_quota_exhausted": 0,
        "official_kind_required": 0,
        "official_kind_enabled": 0,
        "official_kind_configured": 0,
        "official_kind_rights_verified": 0,
        "official_kind_fetched": 0,
        "official_kind_accepted": 0,
        "official_kind_rejected": 0,
        "official_kind_duplicates": 0,
        "official_kind_discarded": 0,
        "official_kind_pages": 0,
        "official_kind_errors": 0,
        "official_remote_batches_attempted": 1,
        "official_remote_failure_telemetry_count": 0,
        "official_remote_failure_response_body_bytes": 0,
        "official_remote_failure_elapsed_ms": 0,
        **({"official_frozen_replay": 1} if frozen_replay else {}),
    }


def test_apply_bundle_and_network_free_replay_are_checkpoint_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("GITHUB_SHA", REVISION)
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "456")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    store = MemoryCheckpointStore()
    bundle_root = tmp_path / "bundle"
    options = official_backfill.BackfillOptions(
        start=date(2026, 6, 1),
        end_exclusive=date(2026, 7, 1),
        checkpoint_path=tmp_path / "checkpoint.json",
        sources=("dart",),
        max_chunks=30,
        write_frozen_bundle_dir=bundle_root,
    )

    def apply_runner(_root, **kwargs):
        captured = kwargs["payload_capture"]
        record = run_record()
        record["idempotency_key"] = kwargs["idempotency_key"]
        record["run_id"] = "run:" + str(kwargs["idempotency_key"]).split(":")[-1]
        captured(payload(), record)
        return _successful_summary()

    apply_report = official_backfill.run_backfill(
        tmp_path,
        options,
        ingest_runner=apply_runner,
        now_provider=lambda: datetime(2026, 7, 2, tzinfo=timezone.utc),
        checkpoint_store=store,
    )
    assert apply_report["status"] == "succeeded"
    assert len(apply_report["window_results"]) == 30
    assert (bundle_root / "manifest.json").is_file()
    apply_manifest_sha256 = hashlib.sha256(
        (bundle_root / "manifest.json").read_bytes()
    ).hexdigest()
    assert (
        apply_report["receipt_contract"]["frozen_bundle_manifest_sha256"]
        == apply_manifest_sha256
    )
    apply_receipt_windows = apply_report["receipt_contract"]["windows"]
    assert all(
        receipt["payload_sha256"]
        == result["stable_payload_sha256"]
        for receipt, result in zip(
            apply_receipt_windows,
            apply_report["window_results"],
            strict=True,
        )
    )
    checkpoint_digest = apply_report["checkpoint_payload_sha256"]
    manifest_raw = (bundle_root / "manifest.json").read_bytes()
    binding = {
        "schema_version": 1,
        "kind": frozen.ARTIFACT_BINDING_KIND,
        "consumer": {"code_revision": REVISION},
        "producer": {"run_id": 456, "run_attempt": 1},
        "artifact": {
            "name": "official-dart-frozen-replay-apply-456-1",
            "digest": "sha256:" + "d" * 64,
        },
        "job_fingerprint": apply_report["job_fingerprint"],
        "range_start": "2026-06-01",
        "range_end_exclusive": "2026-07-01",
        "checkpoint_payload_sha256": checkpoint_digest,
        "manifest_sha256": hashlib.sha256(manifest_raw).hexdigest(),
        "leaf_sha256": [
            row["sha256"]
            for row in json.loads(manifest_raw)["windows"]
        ],
    }
    binding["binding_sha256"] = frozen.canonical_sha256(binding)
    binding_path = tmp_path / "binding.json"
    binding_path.write_text(json.dumps(binding), encoding="utf-8")

    calls = []

    def frozen_runner(frozen_leaf, *, now):
        calls.append((frozen_leaf["window_start"], now))
        return _successful_summary(frozen_replay=True)

    monkeypatch.setattr(
        official_backfill,
        "replay_frozen_dart_window",
        frozen_runner,
    )

    def forbidden_live_runner(*_args, **_kwargs):
        raise AssertionError("frozen replay called the live ingest runner")

    replay_report = official_backfill.run_backfill(
        tmp_path,
        replace(
            options,
            replay=True,
            write_frozen_bundle_dir=None,
            frozen_bundle_dir=bundle_root,
            frozen_artifact_binding=binding_path,
        ),
        ingest_runner=forbidden_live_runner,
        now_provider=lambda: datetime(2026, 7, 2, 1, tzinfo=timezone.utc),
        checkpoint_store=store,
    )
    assert replay_report["status"] == "succeeded"
    assert replay_report["checkpoint_before"] == replay_report["checkpoint_after"]
    assert replay_report["windows_succeeded"] == 30
    assert len(calls) == 30
    assert (
        replay_report["apply_receipt_contract"]
        == apply_report["receipt_contract"]
    )
    replay_receipt = replay_report["receipt_contract"]
    assert (
        replay_receipt["frozen_bundle_manifest_sha256"]
        == apply_manifest_sha256
    )
    assert replay_receipt["frozen_artifact_binding_sha256"] == hashlib.sha256(
        binding_path.read_bytes()
    ).hexdigest()
    assert replay_receipt["source_network_accessed"] is False
    assert [
        receipt["payload_sha256"]
        for receipt in replay_receipt["windows"]
    ] == [
        receipt["payload_sha256"]
        for receipt in apply_receipt_windows
    ]
    assert all(
        row["summary"]["official_dart_requests"] == 0
        for row in replay_report["window_results"]
    )


def test_partial_apply_manifest_survives_failure_and_changed_resume_cannot_overwrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("GITHUB_SHA", REVISION)
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "789")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    store = MemoryCheckpointStore()
    root = tmp_path / "partial"
    options = official_backfill.BackfillOptions(
        start=date(2026, 6, 1),
        end_exclusive=date(2026, 7, 1),
        checkpoint_path=tmp_path / "checkpoint.json",
        sources=("dart",),
        max_chunks=30,
        write_frozen_bundle_dir=root,
    )
    calls = 0

    def failing_runner(_root, **kwargs):
        nonlocal calls
        record = run_record(calls)
        record["idempotency_key"] = kwargs["idempotency_key"]
        record["run_id"] = f"run:{calls}"
        kwargs["payload_capture"](payload(), record)
        calls += 1
        summary = _successful_summary()
        if calls == 2:
            summary["official_failed"] = 1
            summary["official_remote_run_persisted"] = 0
            summary["official_remote_synced"] = 0
        return summary

    with pytest.raises(
        official_backfill.CheckpointError,
        match="cannot be finalized",
    ):
        official_backfill.run_backfill(
            tmp_path,
            options,
            ingest_runner=failing_runner,
            now_provider=lambda: datetime(2026, 7, 2, tzinfo=timezone.utc),
            checkpoint_store=store,
        )
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["bundle_status"] == "partial"
    assert len(manifest["windows"]) == 1
    resume = frozen._validate_resume_tree(
        SimpleNamespace(
            path=root,
            expected_code_revision=REVISION,
            expected_job_fingerprint=manifest["job"]["fingerprint"],
        )
    )
    assert resume["completed_window_count"] == 1
    assert resume["window_count"] == 2
    failed_leaf = root / "windows" / "01-2026-06-02.json"
    original_digest = hashlib.sha256(failed_leaf.read_bytes()).hexdigest()

    def changed_resume(_root, **kwargs):
        record = run_record(1)
        record["idempotency_key"] = kwargs["idempotency_key"]
        record["run_id"] = "run:1"
        changed = payload(title="source changed after failed ACK")
        record["stable_payload_sha256"] = frozen.stable_payload_sha256(changed)
        kwargs["payload_capture"](changed, record)
        return _successful_summary()

    with pytest.raises(
        official_backfill.CheckpointError,
        match="cannot be finalized",
    ):
        official_backfill.run_backfill(
            tmp_path,
            options,
            ingest_runner=changed_resume,
            now_provider=lambda: datetime(2026, 7, 2, 1, tzinfo=timezone.utc),
            checkpoint_store=store,
        )
    assert hashlib.sha256(failed_leaf.read_bytes()).hexdigest() == original_digest


def test_partial_apply_resumes_to_complete_authoritative_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("GITHUB_SHA", REVISION)
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setenv("GITHUB_RUN_ID", "900")
    monkeypatch.setenv("GITHUB_RUN_ATTEMPT", "1")
    store = MemoryCheckpointStore()
    root = tmp_path / "resumable"
    options = official_backfill.BackfillOptions(
        start=date(2026, 6, 1),
        end_exclusive=date(2026, 7, 1),
        checkpoint_path=tmp_path / "checkpoint.json",
        sources=("dart",),
        max_chunks=30,
        write_frozen_bundle_dir=root,
    )
    first_calls = 0

    def first_runner(_root, **kwargs):
        nonlocal first_calls
        day_index = (kwargs["start"] - date(2026, 6, 1)).days
        record = run_record(day_index)
        record["idempotency_key"] = kwargs["idempotency_key"]
        record["run_id"] = f"run:{day_index}"
        kwargs["payload_capture"](payload(), record)
        first_calls += 1
        summary = _successful_summary()
        if first_calls == 2:
            summary["official_failed"] = 1
            summary["official_remote_run_persisted"] = 0
            summary["official_remote_synced"] = 0
        return summary

    with pytest.raises(
        official_backfill.CheckpointError,
        match="cannot be finalized",
    ):
        official_backfill.run_backfill(
            tmp_path,
            options,
            ingest_runner=first_runner,
            now_provider=lambda: datetime(2026, 7, 2, tzinfo=timezone.utc),
            checkpoint_store=store,
        )
    partial = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    assert partial["bundle_status"] == "partial"
    assert len(partial["windows"]) == 1

    monkeypatch.setenv("GITHUB_RUN_ID", "901")

    def resume_runner(_root, **kwargs):
        day_index = (kwargs["start"] - date(2026, 6, 1)).days
        record = run_record(day_index)
        record["idempotency_key"] = kwargs["idempotency_key"]
        record["run_id"] = f"run:{day_index}"
        kwargs["payload_capture"](payload(), record)
        return _successful_summary()

    report = official_backfill.run_backfill(
        tmp_path,
        options,
        ingest_runner=resume_runner,
        now_provider=lambda: datetime(2026, 7, 2, 1, tzinfo=timezone.utc),
        checkpoint_store=store,
    )

    manifest_raw = (root / "manifest.json").read_bytes()
    manifest = json.loads(manifest_raw)
    assert report["status"] == "succeeded"
    assert report["windows_attempted"] == 29
    assert report["windows_already_completed"] == 1
    assert report["windows_succeeded"] == 29
    assert report["windows_remaining"] == 0
    assert report["receipt_contract"]["window_count"] == 30
    assert (
        report["receipt_contract"]["frozen_bundle_manifest_sha256"]
        == hashlib.sha256(manifest_raw).hexdigest()
    )
    assert manifest["bundle_status"] == "complete"
    assert manifest["producer"]["run_id"] == 901
    assert len(manifest["windows"]) == 30
