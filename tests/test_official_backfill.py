from __future__ import annotations

import copy
import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from curator import official_ingest
from curator.backfill_checkpoint_api import (
    RemoteCheckpointConflictError,
    RemoteCheckpointSnapshot,
    RemoteCheckpointWrite,
    canonical_checkpoint,
    checkpoint_payload_hash,
)
from curator.official_backfill import (
    BackfillConfigurationError,
    BackfillOptions,
    CheckpointError,
    build_date_windows,
    load_checkpoint,
    run_backfill,
    validate_runtime,
)
from curator.official_sources import DartRequestBudget


def successful_summary(*, dry_run: bool = False) -> dict[str, int]:
    return {
        "official_fetched": 9,
        "official_documents": 4,
        "official_events": 4,
        "official_companies": 2,
        "official_source_rights": 2,
        "official_failed": 0,
        "official_skipped": 0,
        "official_dry_run": int(dry_run),
        "official_remote_synced": 0 if dry_run else 1,
        "official_remote_run_persisted": 0 if dry_run else 1,
        "official_remote_failed": 0,
        "official_remote_skipped": 1 if dry_run else 0,
        "official_remote_ack_mismatches": 0,
        "official_remote_raw_count": 4,
        "official_remote_ack_count": 4,
    }


class MemoryCheckpointStore:
    def __init__(self) -> None:
        self.records: dict[str, tuple[int, dict[str, object], str]] = {}
        self.get_calls: list[str] = []
        self.put_calls: list[tuple[str, int, dict[str, object]]] = []
        self.conflict_on_put: int | None = None

    def get(self, fingerprint: str) -> RemoteCheckpointSnapshot:
        self.get_calls.append(fingerprint)
        record = self.records.get(fingerprint)
        if record is None:
            return RemoteCheckpointSnapshot(checkpoint=None, version=0)
        version, checkpoint, payload_hash = record
        return RemoteCheckpointSnapshot(
            checkpoint=copy.deepcopy(checkpoint),
            version=version,
            payload_hash=payload_hash,
        )

    def put(
        self,
        fingerprint: str,
        *,
        expected_version: int,
        checkpoint: dict[str, object],
    ) -> RemoteCheckpointWrite:
        normalized = canonical_checkpoint(checkpoint)
        self.put_calls.append((fingerprint, expected_version, copy.deepcopy(normalized)))
        current = self.records.get(fingerprint)
        actual_version = current[0] if current is not None else 0
        if self.conflict_on_put == len(self.put_calls) or actual_version != expected_version:
            raise RemoteCheckpointConflictError(
                expected_version=expected_version,
                actual_version=actual_version + int(actual_version == expected_version),
            )
        payload_hash = checkpoint_payload_hash(normalized)
        if current is not None and current[2] == payload_hash:
            return RemoteCheckpointWrite(
                version=actual_version,
                payload_hash=payload_hash,
                unchanged=True,
            )
        version = actual_version + 1
        self.records[fingerprint] = (version, copy.deepcopy(normalized), payload_hash)
        return RemoteCheckpointWrite(
            version=version,
            payload_hash=payload_hash,
            unchanged=False,
        )


def fixed_now() -> datetime:
    return datetime(2026, 7, 16, 0, 0, tzinfo=timezone.utc)


def test_operational_defaults_are_one_day_and_dart_only(tmp_path: Path) -> None:
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 2),
        checkpoint_path=tmp_path / "official-checkpoint.json",
    )

    assert options.chunk_days == 1
    assert options.sources == ("dart",)


def test_backfill_rejects_future_empty_windows(tmp_path: Path) -> None:
    tomorrow_kst = datetime.now(ZoneInfo("Asia/Seoul")).date() + timedelta(days=1)
    options = BackfillOptions(
        start=tomorrow_kst,
        end_exclusive=tomorrow_kst + timedelta(days=1),
        checkpoint_path=tmp_path / "official-checkpoint.json",
    )

    with pytest.raises(BackfillConfigurationError, match="tomorrow in KST"):
        options.validate()


def test_date_windows_are_half_open_and_connector_end_is_inclusive() -> None:
    windows = build_date_windows(date(2021, 1, 1), date(2021, 1, 6), 2)
    assert [window.key for window in windows] == [
        "2021-01-01:2021-01-03",
        "2021-01-03:2021-01-05",
        "2021-01-05:2021-01-06",
    ]
    assert [window.source_end_inclusive.isoformat() for window in windows] == [
        "2021-01-02",
        "2021-01-04",
        "2021-01-05",
    ]


def test_backfill_resumes_completed_chunks_and_keeps_stable_idempotency_keys(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "official-checkpoint.json"
    store = MemoryCheckpointStore()
    calls: list[dict[str, object]] = []

    def runner(root: Path, **kwargs: object) -> dict[str, int]:
        calls.append({"root": root, **kwargs})
        return successful_summary()

    limited = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 6),
        checkpoint_path=checkpoint_path,
        chunk_days=2,
        sources=("dart",),
        max_chunks=2,
    )
    first = run_backfill(
        tmp_path,
        limited,
        ingest_runner=runner,
        now_provider=fixed_now,
        checkpoint_store=store,
    )
    assert first["status"] == "succeeded"
    assert first["windows_attempted"] == 2
    assert first["windows_remaining"] == 1
    assert calls[0]["start"] == date(2021, 1, 1)
    assert calls[0]["end"] == date(2021, 1, 2)
    first_keys = [row["idempotency_key"] for row in first["window_results"]]  # type: ignore[index]

    calls.clear()
    unlimited = BackfillOptions(
        start=limited.start,
        end_exclusive=limited.end_exclusive,
        checkpoint_path=checkpoint_path,
        chunk_days=limited.chunk_days,
        sources=limited.sources,
    )
    checkpoint_path.write_text('{"corrupt":"local-only"}', encoding="utf-8")
    second = run_backfill(
        tmp_path,
        unlimited,
        ingest_runner=runner,
        now_provider=fixed_now,
        checkpoint_store=store,
    )
    assert second["windows_already_completed"] == 2
    assert second["windows_attempted"] == 1
    assert second["windows_remaining"] == 0
    assert calls[0]["start"] == date(2021, 1, 5)

    checkpoint = load_checkpoint(checkpoint_path)
    assert checkpoint is not None
    completed = checkpoint["completed_windows"]
    assert isinstance(completed, dict) and len(completed) == 3
    assert [completed[key]["idempotency_key"] for key in list(completed)[:2]] == first_keys  # type: ignore[index]
    assert first["checkpoint_source"] == "mysql_remote"
    assert second["checkpoint_version"] == 4


def test_backfill_shares_one_bounded_dart_request_budget_across_windows(tmp_path: Path) -> None:
    budgets: list[DartRequestBudget] = []
    store = MemoryCheckpointStore()

    def runner(_root: Path, **kwargs: object) -> dict[str, int]:
        overrides = kwargs["settings_overrides"]
        assert isinstance(overrides, dict)
        budget = overrides["dart_request_budget"]
        assert isinstance(budget, DartRequestBudget)
        budgets.append(budget)
        budget.consume()
        return successful_summary()

    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 3),
        checkpoint_path=tmp_path / "official-checkpoint.json",
        chunk_days=1,
        sources=("dart",),
        request_budget=2,
    )
    report = run_backfill(
        tmp_path,
        options,
        ingest_runner=runner,
        now_provider=fixed_now,
        checkpoint_store=store,
    )

    assert report["status"] == "succeeded"
    assert len(budgets) == 2
    assert budgets[0] is budgets[1]
    assert budgets[0].used == 2


def test_checkpoint_fingerprint_rejects_changed_job_without_restart(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "official-checkpoint.json"
    store = MemoryCheckpointStore()
    base = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 5),
        checkpoint_path=checkpoint_path,
        chunk_days=2,
        sources=("dart",),
    )
    run_backfill(
        tmp_path,
        base,
        ingest_runner=lambda *_args, **_kwargs: successful_summary(),
        checkpoint_store=store,
    )
    changed = BackfillOptions(
        start=base.start,
        end_exclusive=base.end_exclusive,
        checkpoint_path=checkpoint_path,
        chunk_days=3,
        sources=base.sources,
    )
    existing = next(iter(store.records.values()))

    class WrongCheckpointStore(MemoryCheckpointStore):
        def get(self, fingerprint: str) -> RemoteCheckpointSnapshot:
            version, checkpoint, payload_hash = existing
            return RemoteCheckpointSnapshot(
                checkpoint=copy.deepcopy(checkpoint),
                version=version,
                payload_hash=payload_hash,
            )

    with pytest.raises(CheckpointError, match="fingerprint"):
        run_backfill(
            tmp_path,
            changed,
            ingest_runner=lambda *_args, **_kwargs: successful_summary(),
            checkpoint_store=WrongCheckpointStore(),
        )


def test_dry_run_reads_and_normalizes_without_checkpoint_or_remote_success(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "must-not-exist.json"
    checkpoint_path.write_text("local evidence must remain unchanged", encoding="utf-8")
    store = MemoryCheckpointStore()
    seen: list[dict[str, object]] = []

    def runner(_root: Path, **kwargs: object) -> dict[str, int]:
        seen.append(kwargs)
        return successful_summary(dry_run=True)

    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 3),
        checkpoint_path=checkpoint_path,
        sources=("dart",),
        dry_run=True,
    )
    report = run_backfill(
        tmp_path,
        options,
        ingest_runner=runner,
        now_provider=fixed_now,
        checkpoint_store=store,
    )
    assert report["status"] == "succeeded"
    assert seen[0]["dry_run"] is True
    assert report["checkpoint_path"] is None
    assert checkpoint_path.read_text(encoding="utf-8") == "local evidence must remain unchanged"
    assert store.get_calls == []
    assert store.put_calls == []


def test_failed_remote_sync_is_checkpointed_but_not_completed(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "official-checkpoint.json"
    store = MemoryCheckpointStore()
    failed_summary = successful_summary()
    failed_summary.update(official_failed=1, official_remote_synced=0, official_remote_failed=1)
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 3),
        checkpoint_path=checkpoint_path,
        sources=("dart",),
    )
    report = run_backfill(
        tmp_path,
        options,
        ingest_runner=lambda *_args, **_kwargs: failed_summary,
        checkpoint_store=store,
    )
    assert report["status"] == "failed"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert checkpoint["completed_windows"] == {}
    assert list(checkpoint["failed_windows"]) == ["2021-01-01:2021-01-02"]
    remote_checkpoint = next(iter(store.records.values()))[1]
    assert remote_checkpoint["completed_windows"] == {}
    assert list(remote_checkpoint["failed_windows"]) == ["2021-01-01:2021-01-02"]


def test_kind_rights_failure_is_checkpointed_but_never_completed(tmp_path: Path) -> None:
    store = MemoryCheckpointStore()
    rights_failure = successful_summary()
    rights_failure.update(
        official_failed=1,
        official_kind_errors=1,
        official_kind_rights_verified=0,
    )
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 2),
        checkpoint_path=tmp_path / "official-checkpoint.json",
        sources=("kind",),
    )

    report = run_backfill(
        tmp_path,
        options,
        ingest_runner=lambda *_args, **_kwargs: rights_failure,
        checkpoint_store=store,
        now_provider=fixed_now,
    )

    assert report["status"] == "failed"
    assert report["windows_succeeded"] == 0
    checkpoint = next(iter(store.records.values()))[1]
    assert checkpoint["completed_windows"] == {}
    assert list(checkpoint["failed_windows"]) == ["2021-01-01:2021-01-02"]


def test_kind_dry_run_requires_rights_preflight_runtime_config(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("KIND_DISCLOSURE_ENDPOINT", "https://kind-adapter.example/v1/disclosures")
    for name in (
        "BSIDE_API_BASE_URL",
        "GOVERNANCE_API_BASE_URL",
        "ACTIVIST_API_URL",
        "BSIDE_OPS_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 2),
        checkpoint_path=tmp_path / "unused.json",
        sources=("kind",),
        dry_run=True,
    )

    with pytest.raises(BackfillConfigurationError, match="SourceRight preflight"):
        validate_runtime(options)


def test_dart_only_dry_run_does_not_require_source_right_ops_token(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("DART_API_KEY", "dart-key")
    for name in (
        "BSIDE_API_BASE_URL",
        "GOVERNANCE_API_BASE_URL",
        "ACTIVIST_API_URL",
        "BSIDE_OPS_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 2),
        checkpoint_path=tmp_path / "unused.json",
        sources=("dart",),
        dry_run=True,
    )

    validate_runtime(options)


def test_dart_020_blocks_same_kst_day_and_resumes_same_checkpoint_next_day(
    tmp_path: Path,
) -> None:
    checkpoint_path = tmp_path / "official-checkpoint.json"
    store = MemoryCheckpointStore()
    quota_summary = successful_summary()
    quota_summary.update(
        official_failed=1,
        official_remote_synced=0,
        official_dart_quota_exhausted=1,
    )
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 3),
        checkpoint_path=checkpoint_path,
        chunk_days=1,
        sources=("dart",),
        continue_on_error=True,
    )
    attempts: list[str] = []

    first = run_backfill(
        tmp_path,
        options,
        ingest_runner=lambda *_args, **_kwargs: (attempts.append("quota") or quota_summary),
        now_provider=fixed_now,
        checkpoint_store=store,
    )
    assert first["windows_attempted"] == 1
    assert attempts == ["quota"]
    remote = next(iter(store.records.values()))[1]
    assert remote["dart_quota_blocked_until"] == "2026-07-17"

    with pytest.raises(BackfillConfigurationError, match="status 020 already exhausted"):
        run_backfill(
            tmp_path,
            options,
            ingest_runner=lambda *_args, **_kwargs: successful_summary(),
            now_provider=fixed_now,
            checkpoint_store=store,
        )

    def next_quota_period() -> datetime:
        return datetime(2026, 7, 17, 0, 0, tzinfo=timezone.utc)

    resumed = run_backfill(
        tmp_path,
        options,
        ingest_runner=lambda *_args, **_kwargs: successful_summary(),
        now_provider=next_quota_period,
        checkpoint_store=store,
    )
    assert resumed["status"] == "succeeded"
    assert resumed["windows_succeeded"] == 2
    remote = next(iter(store.records.values()))[1]
    assert remote["dart_quota_blocked_until"] is None


def test_exact_ack_mismatch_never_advances_completed_windows(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "official-checkpoint.json"
    store = MemoryCheckpointStore()
    mismatched = successful_summary()
    mismatched["official_remote_ack_count"] = 3
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 2),
        checkpoint_path=checkpoint_path,
        sources=("dart",),
    )

    report = run_backfill(
        tmp_path,
        options,
        ingest_runner=lambda *_args, **_kwargs: mismatched,
        checkpoint_store=store,
        now_provider=fixed_now,
    )

    assert report["status"] == "failed"
    assert report["windows_succeeded"] == 0
    remote_checkpoint = next(iter(store.records.values()))[1]
    assert remote_checkpoint["completed_windows"] == {}
    failed = remote_checkpoint["failed_windows"]
    assert isinstance(failed, dict) and list(failed) == ["2021-01-01:2021-01-02"]


def test_missing_collection_run_ack_never_advances_completed_windows(tmp_path: Path) -> None:
    store = MemoryCheckpointStore()
    missing_run_ack = successful_summary()
    missing_run_ack["official_remote_run_persisted"] = 0
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 2),
        checkpoint_path=tmp_path / "official-checkpoint.json",
        sources=("dart",),
    )

    report = run_backfill(
        tmp_path,
        options,
        ingest_runner=lambda *_args, **_kwargs: missing_run_ack,
        checkpoint_store=store,
        now_provider=fixed_now,
    )

    assert report["status"] == "failed"
    remote_checkpoint = next(iter(store.records.values()))[1]
    assert remote_checkpoint["completed_windows"] == {}


def test_checkpoint_409_after_ingest_fails_without_local_or_remote_advance(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "official-checkpoint.json"
    store = MemoryCheckpointStore()
    # PUT 1 establishes the empty job; PUT 2 would commit the first window.
    store.conflict_on_put = 2
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 2),
        checkpoint_path=checkpoint_path,
        sources=("dart",),
    )

    with pytest.raises(RemoteCheckpointConflictError, match="version conflict"):
        run_backfill(
            tmp_path,
            options,
            ingest_runner=lambda *_args, **_kwargs: successful_summary(),
            checkpoint_store=store,
            now_provider=fixed_now,
        )

    local_checkpoint = load_checkpoint(checkpoint_path)
    assert local_checkpoint is not None
    assert local_checkpoint["completed_windows"] == {}
    remote_checkpoint = next(iter(store.records.values()))[1]
    assert remote_checkpoint["completed_windows"] == {}


def test_failed_window_retry_increments_attempt_and_reuses_idempotency_key(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "official-checkpoint.json"
    store = MemoryCheckpointStore()
    failed_summary = successful_summary()
    failed_summary.update(official_failed=1, official_remote_synced=0, official_remote_failed=1)
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 3),
        checkpoint_path=checkpoint_path,
        sources=("dart",),
    )
    first = run_backfill(
        tmp_path,
        options,
        ingest_runner=lambda *_args, **_kwargs: failed_summary,
        now_provider=fixed_now,
        checkpoint_store=store,
    )
    first_result = first["window_results"][0]  # type: ignore[index]
    seen: list[dict[str, object]] = []

    def succeeds(_root: Path, **kwargs: object) -> dict[str, int]:
        seen.append(kwargs)
        return successful_summary()

    second = run_backfill(
        tmp_path,
        options,
        ingest_runner=succeeds,
        now_provider=fixed_now,
        checkpoint_store=store,
    )
    second_result = second["window_results"][0]  # type: ignore[index]
    assert first_result["attempt"] == 1
    assert second_result["attempt"] == 2
    assert second_result["idempotency_key"] == first_result["idempotency_key"]
    assert seen[0]["idempotency_key"] == first_result["idempotency_key"]
    assert seen[0]["now"] == fixed_now()


def test_checkpoint_rejects_inconsistent_completed_and_failed_records(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "official-checkpoint.json"
    store = MemoryCheckpointStore()
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 3),
        checkpoint_path=checkpoint_path,
        sources=("dart",),
    )
    run_backfill(
        tmp_path,
        options,
        ingest_runner=lambda *_args, **_kwargs: successful_summary(),
        now_provider=fixed_now,
        checkpoint_store=store,
    )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    key = next(iter(checkpoint["completed_windows"]))
    checkpoint["completed_windows"][key]["status"] = "failed"
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(CheckpointError, match="inconsistent"):
        load_checkpoint(checkpoint_path)


def test_checkpoint_rejects_completed_window_without_exact_remote_ack(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "official-checkpoint.json"
    store = MemoryCheckpointStore()
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 2),
        checkpoint_path=checkpoint_path,
        sources=("dart",),
    )
    run_backfill(
        tmp_path,
        options,
        ingest_runner=lambda *_args, **_kwargs: successful_summary(),
        now_provider=fixed_now,
        checkpoint_store=store,
    )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    key = next(iter(checkpoint["completed_windows"]))
    checkpoint["completed_windows"][key]["summary"]["official_remote_ack_count"] = 3
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(CheckpointError, match="lacks an exact remote ACK"):
        load_checkpoint(checkpoint_path)


def test_request_budget_change_keeps_the_same_logical_job_checkpoint(tmp_path: Path) -> None:
    store = MemoryCheckpointStore()
    first = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 2),
        checkpoint_path=tmp_path / "official-checkpoint.json",
        sources=("dart",),
        request_budget=100,
    )
    second = replace(first, request_budget=200)
    first_report = run_backfill(
        tmp_path,
        first,
        ingest_runner=lambda *_args, **_kwargs: successful_summary(),
        now_provider=fixed_now,
        checkpoint_store=store,
    )
    second_report = run_backfill(
        tmp_path,
        second,
        ingest_runner=lambda *_args, **_kwargs: successful_summary(),
        now_provider=fixed_now,
        checkpoint_store=store,
    )
    assert first_report["job_fingerprint"] == second_report["job_fingerprint"]
    assert second_report["windows_attempted"] == 0


def test_company_master_sync_requires_dart_source(tmp_path: Path) -> None:
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 3),
        checkpoint_path=tmp_path / "official-checkpoint.json",
        sources=("kind",),
        sync_company_master=True,
    )
    with pytest.raises(BackfillConfigurationError, match="requires the dart source"):
        options.validate()


def test_official_ingest_dry_run_never_calls_remote_sync(monkeypatch: pytest.MonkeyPatch) -> None:
    def forbidden_sync(*_args: object, **_kwargs: object) -> dict[str, int]:
        raise AssertionError("dry run must not write to the remote API")

    monkeypatch.setattr(official_ingest, "sync_governance_payload", forbidden_sync)
    summary = official_ingest.run(
        now=fixed_now(),
        start=date(2021, 1, 1),
        end=date(2021, 1, 2),
        settings_overrides={"dart_enabled": False, "kind_enabled": False},
        dry_run=True,
    )
    assert summary["official_dry_run"] == 1
    assert summary["official_remote_skipped"] == 1
    assert summary["official_failed"] == 0
