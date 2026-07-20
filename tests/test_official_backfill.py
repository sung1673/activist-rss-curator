from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from curator import official_ingest
from curator.official_backfill import (
    BackfillConfigurationError,
    BackfillOptions,
    CheckpointError,
    build_date_windows,
    load_checkpoint,
    run_backfill,
)


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
        "official_remote_failed": 0,
        "official_remote_skipped": 1 if dry_run else 0,
    }


def fixed_now() -> datetime:
    return datetime(2026, 7, 16, 0, 0, tzinfo=timezone.utc)


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
    first = run_backfill(tmp_path, limited, ingest_runner=runner, now_provider=fixed_now)
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
    second = run_backfill(tmp_path, unlimited, ingest_runner=runner, now_provider=fixed_now)
    assert second["windows_already_completed"] == 2
    assert second["windows_attempted"] == 1
    assert second["windows_remaining"] == 0
    assert calls[0]["start"] == date(2021, 1, 5)

    checkpoint = load_checkpoint(checkpoint_path)
    assert checkpoint is not None
    completed = checkpoint["completed_windows"]
    assert isinstance(completed, dict) and len(completed) == 3
    assert [completed[key]["idempotency_key"] for key in list(completed)[:2]] == first_keys  # type: ignore[index]


def test_checkpoint_fingerprint_rejects_changed_job_without_restart(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "official-checkpoint.json"
    base = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 5),
        checkpoint_path=checkpoint_path,
        chunk_days=2,
        sources=("dart",),
    )
    run_backfill(tmp_path, base, ingest_runner=lambda *_args, **_kwargs: successful_summary())
    changed = BackfillOptions(
        start=base.start,
        end_exclusive=base.end_exclusive,
        checkpoint_path=checkpoint_path,
        chunk_days=3,
        sources=base.sources,
    )
    with pytest.raises(CheckpointError, match="fingerprint"):
        run_backfill(tmp_path, changed, ingest_runner=lambda *_args, **_kwargs: successful_summary())


def test_dry_run_reads_and_normalizes_without_checkpoint_or_remote_success(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "must-not-exist.json"
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
    report = run_backfill(tmp_path, options, ingest_runner=runner, now_provider=fixed_now)
    assert report["status"] == "succeeded"
    assert seen[0]["dry_run"] is True
    assert report["checkpoint_path"] is None
    assert not checkpoint_path.exists()


def test_failed_remote_sync_is_checkpointed_but_not_completed(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "official-checkpoint.json"
    failed_summary = successful_summary()
    failed_summary.update(official_failed=1, official_remote_synced=0, official_remote_failed=1)
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 3),
        checkpoint_path=checkpoint_path,
        sources=("dart",),
    )
    report = run_backfill(tmp_path, options, ingest_runner=lambda *_args, **_kwargs: failed_summary)
    assert report["status"] == "failed"
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    assert checkpoint["completed_windows"] == {}
    assert list(checkpoint["failed_windows"]) == ["2021-01-01:2021-01-03"]


def test_failed_window_retry_increments_attempt_and_reuses_idempotency_key(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "official-checkpoint.json"
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
    )
    second_result = second["window_results"][0]  # type: ignore[index]
    assert first_result["attempt"] == 1
    assert second_result["attempt"] == 2
    assert second_result["idempotency_key"] == first_result["idempotency_key"]
    assert seen[0]["idempotency_key"] == first_result["idempotency_key"]
    assert seen[0]["now"] == fixed_now()


def test_checkpoint_rejects_inconsistent_completed_and_failed_records(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "official-checkpoint.json"
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
    )
    checkpoint = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    key = next(iter(checkpoint["completed_windows"]))
    checkpoint["completed_windows"][key]["status"] = "failed"
    checkpoint_path.write_text(json.dumps(checkpoint), encoding="utf-8")

    with pytest.raises(CheckpointError, match="inconsistent"):
        load_checkpoint(checkpoint_path)


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
