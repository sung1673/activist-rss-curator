from __future__ import annotations

import copy
import json
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import pytest

from curator import official_backfill, official_ingest
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
    _completed_kst_end_exclusive,
    build_parser,
    build_date_windows,
    load_checkpoint,
    options_from_args,
    run_backfill,
    validate_runtime,
)
from curator.official_sources import DartRequestBudget


CODE_REVISION = "a" * 40


@pytest.fixture(autouse=True)
def applied_backfill_revision(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GITHUB_SHA", CODE_REVISION)
    monkeypatch.delenv("CURATOR_CODE_REVISION", raising=False)


def successful_summary(*, dry_run: bool = False) -> dict[str, object]:
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
        "official_dart_fetched": 9,
        "official_dart_accepted": 4,
        "official_dart_rejected": 5,
        "official_dart_duplicates": 0,
        "official_dart_discarded": 0,
        "official_dart_pages": 1,
        "official_dart_requests": 1,
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


def test_completed_kst_boundary_is_the_current_kst_date() -> None:
    before_midnight_utc = datetime(2026, 7, 15, 14, 59, 59, tzinfo=timezone.utc)
    after_midnight_utc = datetime(2026, 7, 15, 15, 0, tzinfo=timezone.utc)

    assert _completed_kst_end_exclusive(before_midnight_utc) == date(2026, 7, 15)
    assert _completed_kst_end_exclusive(after_midnight_utc) == date(2026, 7, 16)


def test_backfill_accepts_only_completed_kst_dates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    today_kst = date(2026, 7, 16)
    monkeypatch.setattr(
        official_backfill,
        "_completed_kst_end_exclusive",
        lambda: today_kst,
    )
    BackfillOptions(
        start=today_kst - timedelta(days=1),
        end_exclusive=today_kst,
        checkpoint_path=tmp_path / "completed.json",
    ).validate()

    options = BackfillOptions(
        start=today_kst,
        end_exclusive=today_kst + timedelta(days=1),
        checkpoint_path=tmp_path / "current-or-future.json",
    )

    with pytest.raises(BackfillConfigurationError, match="current KST date"):
        options.validate()


def test_cli_defaults_to_completed_kst_boundary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    today_kst = date(2026, 7, 16)
    monkeypatch.setattr(
        official_backfill,
        "_completed_kst_end_exclusive",
        lambda: today_kst,
    )
    args = build_parser().parse_args(
        [
            "--root",
            str(Path(__file__).resolve().parents[1]),
            "--from-date",
            "2026-07-15",
        ]
    )

    _, options = options_from_args(args)

    assert options.end_exclusive == today_kst


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
    assert {
        completed[key]["code_revision"] for key in completed  # type: ignore[index]
    } == {CODE_REVISION}
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


def test_backfill_reuses_and_closes_one_owned_durable_quota(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DurableQuota:
        limit = 40_000

        def __init__(self) -> None:
            self.used = 0
            self.close_calls = 0

        def consume(self, *, operation: str, credential_id: str) -> object:
            self.used += 1
            return operation, credential_id

        def block_020(self, permit: object) -> None:
            del permit

        def disable_901(self, permit: object) -> None:
            del permit

        def close(self) -> None:
            self.close_calls += 1

    durable = DurableQuota()
    budgets: list[object] = []
    monkeypatch.setattr(
        official_backfill,
        "durable_dart_quota_required",
        lambda: True,
    )
    monkeypatch.setattr(
        official_backfill,
        "durable_dart_quota_configured",
        lambda: False,
    )
    monkeypatch.setattr(
        official_backfill,
        "durable_dart_quota_client",
        lambda **_kwargs: durable,
    )

    def runner(_root: Path, **kwargs: object) -> dict[str, int]:
        overrides = kwargs["settings_overrides"]
        assert isinstance(overrides, dict)
        budget = overrides["dart_request_budget"]
        budgets.append(budget)
        budget.consume(  # type: ignore[attr-defined]
            operation="list",
            credential_id="c" * 64,
        )
        return successful_summary()

    report = run_backfill(
        tmp_path,
        BackfillOptions(
            start=date(2021, 1, 1),
            end_exclusive=date(2021, 1, 3),
            checkpoint_path=tmp_path / "official-checkpoint.json",
            sources=("dart",),
        ),
        ingest_runner=runner,
        now_provider=fixed_now,
        checkpoint_store=MemoryCheckpointStore(),
    )

    assert report["status"] == "succeeded"
    assert len(budgets) == 2
    assert budgets[0] is budgets[1]
    assert durable.used == 2
    assert durable.close_calls == 1


def test_backfill_closes_owned_durable_quota_when_checkpoint_write_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DurableQuota:
        limit = 40_000
        used = 0

        def __init__(self) -> None:
            self.close_calls = 0

        def consume(self, *, operation: str, credential_id: str) -> object:
            return operation, credential_id

        def block_020(self, permit: object) -> None:
            del permit

        def disable_901(self, permit: object) -> None:
            del permit

        def close(self) -> None:
            self.close_calls += 1

    class FailingCheckpointStore(MemoryCheckpointStore):
        def put(
            self,
            fingerprint: str,
            *,
            expected_version: int,
            checkpoint: dict[str, object],
        ) -> RemoteCheckpointWrite:
            if self.put_calls:
                raise RuntimeError("checkpoint write failed")
            return super().put(
                fingerprint,
                expected_version=expected_version,
                checkpoint=checkpoint,
            )

    durable = DurableQuota()
    monkeypatch.setattr(
        official_backfill,
        "durable_dart_quota_required",
        lambda: True,
    )
    monkeypatch.setattr(
        official_backfill,
        "durable_dart_quota_configured",
        lambda: False,
    )
    monkeypatch.setattr(
        official_backfill,
        "durable_dart_quota_client",
        lambda **_kwargs: durable,
    )

    with pytest.raises(RuntimeError, match="checkpoint write failed"):
        run_backfill(
            tmp_path,
            BackfillOptions(
                start=date(2021, 1, 1),
                end_exclusive=date(2021, 1, 2),
                checkpoint_path=tmp_path / "official-checkpoint.json",
                sources=("dart",),
            ),
            ingest_runner=lambda *_args, **_kwargs: successful_summary(),
            now_provider=fixed_now,
            checkpoint_store=FailingCheckpointStore(),
        )

    assert durable.close_calls == 1


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


def test_dry_run_reads_and_normalizes_without_checkpoint_or_remote_success(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GITHUB_SHA")
    monkeypatch.delenv("CURATOR_CODE_REVISION", raising=False)
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
    assert report["code_revision"] is None
    assert seen[0]["dry_run"] is True
    assert report["checkpoint_path"] is None
    assert checkpoint_path.read_text(encoding="utf-8") == "local evidence must remain unchanged"
    assert store.get_calls == []
    assert store.put_calls == []


def test_workflow_dry_run_binds_report_to_dispatch_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_SHA", CODE_REVISION)
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 2),
        checkpoint_path=tmp_path / "must-not-exist.json",
        sources=("dart",),
        dry_run=True,
    )
    store = MemoryCheckpointStore()

    report = run_backfill(
        tmp_path,
        options,
        ingest_runner=lambda *_args, **_kwargs: successful_summary(dry_run=True),
        now_provider=fixed_now,
        checkpoint_store=store,
    )

    assert report["status"] == "succeeded"
    assert report["code_revision"] == CODE_REVISION
    assert report["job_fingerprint"] == official_backfill.job_fingerprint(
        official_backfill.job_contract(options, code_revision=None)
    )
    assert "code_revision" not in report["window_results"][0]
    assert not options.checkpoint_path.exists()
    assert store.get_calls == []
    assert store.put_calls == []


def test_workflow_dry_run_rejects_malformed_dispatch_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_SHA", "not-a-revision")
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 2),
        checkpoint_path=tmp_path / "must-not-exist.json",
        sources=("dart",),
        dry_run=True,
    )

    with pytest.raises(
        BackfillConfigurationError,
        match="GITHUB_SHA or CURATOR_CODE_REVISION",
    ):
        run_backfill(
            tmp_path,
            options,
            ingest_runner=lambda *_args, **_kwargs: successful_summary(dry_run=True),
            now_provider=fixed_now,
            checkpoint_store=MemoryCheckpointStore(),
        )


def test_failed_remote_sync_is_checkpointed_but_not_completed(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / "official-checkpoint.json"
    store = MemoryCheckpointStore()
    failed_summary = successful_summary()
    failed_summary.update(
        official_failed=1,
        official_remote_synced=2,
        official_remote_failed=1,
        official_remote_raw_count=120,
        official_remote_ack_count=80,
        official_remote_failure_telemetry_count=1,
        official_remote_failure_response_body_bytes=321,
        official_remote_failure_elapsed_ms=9,
        official_remote_failure_details=[
            {
                "scope": "data_batch",
                "batch_number": 2,
                "http_status": 503,
                "error_code": "governance_snapshot_persistence_failed",
                "response_body_bytes": 321,
                "elapsed_ms": 9,
                "exception_class": None,
                "sqlstate_class": "HY000",
                "driver_code": 1205,
            }
        ],
    )
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
    failed_window = checkpoint["failed_windows"]["2021-01-01:2021-01-02"]
    assert (
        failed_window["summary"]["official_remote_failure_details"]
        == failed_summary["official_remote_failure_details"]
    )
    remote_checkpoint = next(iter(store.records.values()))[1]
    assert remote_checkpoint["completed_windows"] == {}
    assert list(remote_checkpoint["failed_windows"]) == ["2021-01-01:2021-01-02"]
    assert (
        remote_checkpoint["failed_windows"]["2021-01-01:2021-01-02"]["summary"][
            "official_remote_failure_details"
        ]
        == failed_summary["official_remote_failure_details"]
    )


def test_checkpoint_schema_keeps_quota_block_optional_and_nullable() -> None:
    schema_path = (
        Path(official_backfill.__file__).resolve().parents[1]
        / "docs"
        / "schemas"
        / "official-backfill-checkpoint.schema.json"
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    quota_contract = schema["properties"]["dart_quota_blocked_until"]
    assert quota_contract == {
        "anyOf": [
            {"type": "string", "format": "date"},
            {"type": "null"},
        ]
    }
    assert "dart_quota_blocked_until" not in schema["required"]

    checkpoint = official_backfill.new_checkpoint(
        {},
        "f" * 64,
        now_provider=fixed_now,
    )
    assert (
        official_backfill.validate_checkpoint(
            checkpoint,
            label="nullable quota checkpoint",
        )["dart_quota_blocked_until"]
        is None
    )
    legacy_checkpoint = dict(checkpoint)
    legacy_checkpoint.pop("dart_quota_blocked_until")
    assert (
        official_backfill.validate_checkpoint(
            legacy_checkpoint,
            label="legacy quota checkpoint",
        ).get("dart_quota_blocked_until")
        is None
    )
    checkpoint["dart_quota_blocked_until"] = "2026-07-29"
    assert (
        official_backfill.validate_checkpoint(
            checkpoint,
            label="blocked quota checkpoint",
        )["dart_quota_blocked_until"]
        == "2026-07-29"
    )


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
    monkeypatch.setenv("DART_API_KEY", "a" * 40)
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


def test_dart_apply_runtime_requires_exact_protected_preflight(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    calls: list[str] = []

    class EligibleDartClient:
        def preflight(self, expected_release_sha: str) -> object:
            calls.append(expected_release_sha)
            return object()

    monkeypatch.setenv("DART_API_KEY", "a" * 40)
    monkeypatch.setenv(
        "ACTIVIST_API_URL",
        "https://alignpe.gabia.io/activist/api.php",
    )
    monkeypatch.setenv("ACTIVIST_API_SECRET", "hmac-secret")
    monkeypatch.setenv(
        "BSIDE_API_BASE_URL",
        "https://alignpe.gabia.io/activist/api.php/api/v1",
    )
    monkeypatch.setenv("BSIDE_OPS_TOKEN", "ops-token")
    monkeypatch.setenv("BSIDE_BACKEND_BINDING_ID", "b" * 64)
    monkeypatch.setattr(
        official_backfill,
        "DartOfficialSourceRightClient",
        EligibleDartClient,
    )
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 2),
        checkpoint_path=tmp_path / "unused.json",
        sources=("dart",),
        dry_run=False,
    )

    validate_runtime(options)
    assert calls == [CODE_REVISION]


def test_dart_apply_runtime_rejects_failed_preflight(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class RejectedDartClient:
        def preflight(self, _expected_release_sha: str) -> object:
            raise official_backfill.OfficialSourceRightError("revoked")

    monkeypatch.setenv("DART_API_KEY", "a" * 40)
    monkeypatch.setenv(
        "ACTIVIST_API_URL",
        "https://alignpe.gabia.io/activist/api.php",
    )
    monkeypatch.setenv("ACTIVIST_API_SECRET", "hmac-secret")
    monkeypatch.setenv(
        "BSIDE_API_BASE_URL",
        "https://alignpe.gabia.io/activist/api.php/api/v1",
    )
    monkeypatch.setenv("BSIDE_OPS_TOKEN", "ops-token")
    monkeypatch.setenv("BSIDE_BACKEND_BINDING_ID", "b" * 64)
    monkeypatch.setattr(
        official_backfill,
        "DartOfficialSourceRightClient",
        RejectedDartClient,
    )
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 2),
        checkpoint_path=tmp_path / "unused.json",
        sources=("dart",),
        dry_run=False,
    )

    with pytest.raises(
        BackfillConfigurationError,
        match="protected SourceRight preflight failed: revoked",
    ):
        validate_runtime(options)


def test_backfill_rejects_conflicting_pool_and_legacy_key_without_secret(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    key_a, key_b = "a" * 40, "b" * 40
    monkeypatch.setenv("OPENDART_API_KEYS", key_a)
    monkeypatch.setenv("DART_API_KEY", key_b)
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 2),
        checkpoint_path=tmp_path / "unused.json",
        sources=("dart",),
        dry_run=True,
    )

    with pytest.raises(BackfillConfigurationError) as captured:
        validate_runtime(options)

    assert str(captured.value) == "OpenDART credential configuration is invalid"
    assert key_a not in str(captured.value)
    assert key_b not in str(captured.value)


def test_apply_requires_exact_backend_binding_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("DART_API_KEY", "a" * 40)
    monkeypatch.setenv("ACTIVIST_API_URL", "https://example.test/api.php")
    monkeypatch.setenv("ACTIVIST_API_SECRET", "secret")
    monkeypatch.setenv("BSIDE_API_BASE_URL", "https://example.test/api.php/api/v1")
    monkeypatch.setenv("BSIDE_OPS_TOKEN", "ops-token")
    monkeypatch.setenv("BSIDE_BACKEND_BINDING_ID", "INVALID")
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 2),
        checkpoint_path=tmp_path / "unused.json",
        sources=("dart",),
        dry_run=False,
    )

    with pytest.raises(BackfillConfigurationError, match="BSIDE_BACKEND_BINDING_ID"):
        validate_runtime(options)


def test_apply_requires_exact_code_revision(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("DART_API_KEY", "a" * 40)
    monkeypatch.setenv("ACTIVIST_API_URL", "https://example.test/api.php")
    monkeypatch.setenv("ACTIVIST_API_SECRET", "secret")
    monkeypatch.setenv("BSIDE_API_BASE_URL", "https://example.test/api.php/api/v1")
    monkeypatch.setenv("BSIDE_OPS_TOKEN", "ops-token")
    monkeypatch.setenv("BSIDE_BACKEND_BINDING_ID", "b" * 64)
    monkeypatch.delenv("GITHUB_SHA")
    monkeypatch.setenv("CURATOR_CODE_REVISION", "not-a-revision")
    options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 2),
        checkpoint_path=tmp_path / "unused.json",
        sources=("dart",),
        dry_run=False,
    )

    with pytest.raises(
        BackfillConfigurationError,
        match="GITHUB_SHA/CURATOR_CODE_REVISION",
    ):
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


def test_revision_change_creates_a_new_exact_release_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = MemoryCheckpointStore()
    checkpoint_path = tmp_path / "official-checkpoint.json"
    first_options = BackfillOptions(
        start=date(2021, 1, 1),
        end_exclusive=date(2021, 1, 3),
        checkpoint_path=checkpoint_path,
        sources=("dart",),
        max_chunks=1,
    )
    first_report = run_backfill(
        tmp_path,
        first_options,
        ingest_runner=lambda *_args, **_kwargs: successful_summary(),
        now_provider=fixed_now,
        checkpoint_store=store,
    )

    next_revision = "b" * 40
    monkeypatch.setenv("GITHUB_SHA", next_revision.upper())
    second_report = run_backfill(
        tmp_path,
        replace(first_options, max_chunks=0),
        ingest_runner=lambda *_args, **_kwargs: successful_summary(),
        now_provider=fixed_now,
        checkpoint_store=store,
    )

    assert first_report["job_fingerprint"] != second_report["job_fingerprint"]
    assert second_report["windows_attempted"] == 2
    assert len(store.records) == 2
    checkpoint = load_checkpoint(checkpoint_path)
    assert checkpoint is not None
    completed = checkpoint["completed_windows"]
    assert isinstance(completed, dict)
    assert {row["code_revision"] for row in completed.values()} == {
        next_revision,
    }


def test_dart_replay_refetches_all_30_windows_and_preserves_checkpoint(
    tmp_path: Path,
) -> None:
    store = MemoryCheckpointStore()
    checkpoint_path = tmp_path / "official-checkpoint.json"
    apply_options = BackfillOptions(
        start=date(2026, 6, 1),
        end_exclusive=date(2026, 7, 1),
        checkpoint_path=checkpoint_path,
        sources=("dart",),
    )
    apply_report = run_backfill(
        tmp_path,
        apply_options,
        ingest_runner=lambda *_args, **_kwargs: successful_summary(),
        now_provider=fixed_now,
        checkpoint_store=store,
    )
    assert apply_report["windows_attempted"] == 30
    assert apply_report["receipt_contract"]["window_count"] == 30  # type: ignore[index]
    before_put_count = len(store.put_calls)
    before_record = copy.deepcopy(next(iter(store.records.values())))
    replay_calls: list[dict[str, object]] = []

    def replay_runner(root: Path, **kwargs: object) -> dict[str, object]:
        replay_calls.append({"root": root, **kwargs})
        return successful_summary()

    replay_report = run_backfill(
        tmp_path,
        replace(apply_options, replay=True, max_chunks=30),
        ingest_runner=replay_runner,
        now_provider=fixed_now,
        checkpoint_store=store,
    )

    assert replay_report["status"] == "succeeded"
    assert replay_report["mode"] == "replay"
    assert replay_report["windows_already_completed"] == 30
    assert replay_report["windows_attempted"] == 30
    assert replay_report["windows_succeeded"] == 30
    assert replay_report["windows_failed"] == 0
    assert replay_report["windows_remaining"] == 0
    assert replay_report["idempotent"] is True
    assert replay_report["replay_verified"] is True
    assert replay_report["checkpoint_before"] == replay_report["checkpoint_after"]
    assert (
        replay_report["checkpoint_payload_sha256"]
        == replay_report["checkpoint_before"]["payload_sha256"]  # type: ignore[index]
    )
    assert len(store.put_calls) == before_put_count
    assert next(iter(store.records.values())) == before_record
    assert len(replay_calls) == 30
    assert all(call["dry_run"] is False for call in replay_calls)
    assert {
        str(call["idempotency_key"]) for call in replay_calls
    } == {
        row["idempotency_key"]  # type: ignore[index]
        for row in apply_report["window_results"]  # type: ignore[union-attr]
    }
    contract = replay_report["receipt_contract"]
    assert contract["mode"] == "replay"  # type: ignore[index]
    assert contract["window_count"] == 30  # type: ignore[index]
    assert all(  # type: ignore[union-attr]
        row["idempotent"] is True and row["replay_verified"] is True
        for row in contract["windows"]  # type: ignore[index]
    )
    assert all(
        row["apply_summary_counts_sha256"]
        == row["replay_summary_counts_sha256"]
        for row in replay_report["window_results"]  # type: ignore[union-attr]
    )


def test_dart_replay_rejects_count_drift_without_mutating_checkpoint(
    tmp_path: Path,
) -> None:
    store = MemoryCheckpointStore()
    apply_options = BackfillOptions(
        start=date(2026, 6, 1),
        end_exclusive=date(2026, 7, 1),
        checkpoint_path=tmp_path / "official-checkpoint.json",
        sources=("dart",),
    )
    run_backfill(
        tmp_path,
        apply_options,
        ingest_runner=lambda *_args, **_kwargs: successful_summary(),
        now_provider=fixed_now,
        checkpoint_store=store,
    )
    before_put_count = len(store.put_calls)
    before_record = copy.deepcopy(next(iter(store.records.values())))
    call_count = 0

    def drifting_runner(_root: Path, **_kwargs: object) -> dict[str, object]:
        nonlocal call_count
        call_count += 1
        summary = successful_summary()
        if call_count == 1:
            summary["official_dart_rejected"] = 6
        return summary

    report = run_backfill(
        tmp_path,
        replace(apply_options, replay=True),
        ingest_runner=drifting_runner,
        now_provider=fixed_now,
        checkpoint_store=store,
    )

    assert report["status"] == "failed"
    assert report["replay_verified"] is False
    assert report["idempotent"] is False
    assert report["windows_attempted"] == 1
    assert "do not exactly match" in report["window_results"][0]["error"]  # type: ignore[index]
    assert report["receipt_contract"] is None
    assert len(store.put_calls) == before_put_count
    assert next(iter(store.records.values())) == before_record


def test_dart_replay_rejects_checkpoint_mutation_during_replay(
    tmp_path: Path,
) -> None:
    class MutatingCheckpointStore(MemoryCheckpointStore):
        def get(self, fingerprint: str) -> RemoteCheckpointSnapshot:
            snapshot = super().get(fingerprint)
            if len(self.get_calls) == 2 and snapshot.checkpoint is not None:
                changed = copy.deepcopy(snapshot.checkpoint)
                changed["updated_at"] = "2026-07-16T00:00:01+00:00"
                normalized = canonical_checkpoint(changed)
                payload_hash = checkpoint_payload_hash(normalized)
                self.records[fingerprint] = (
                    snapshot.version + 1,
                    normalized,
                    payload_hash,
                )
                return RemoteCheckpointSnapshot(
                    checkpoint=copy.deepcopy(normalized),
                    version=snapshot.version + 1,
                    payload_hash=payload_hash,
                )
            return snapshot

    seed_store = MemoryCheckpointStore()
    apply_options = BackfillOptions(
        start=date(2026, 6, 1),
        end_exclusive=date(2026, 7, 1),
        checkpoint_path=tmp_path / "official-checkpoint.json",
        sources=("dart",),
    )
    run_backfill(
        tmp_path,
        apply_options,
        ingest_runner=lambda *_args, **_kwargs: successful_summary(),
        now_provider=fixed_now,
        checkpoint_store=seed_store,
    )
    store = MutatingCheckpointStore()
    store.records = copy.deepcopy(seed_store.records)

    with pytest.raises(CheckpointError, match="mutated"):
        run_backfill(
            tmp_path,
            replace(apply_options, replay=True),
            ingest_runner=lambda *_args, **_kwargs: successful_summary(),
            now_provider=fixed_now,
            checkpoint_store=store,
        )

    assert store.put_calls == []


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"sources": ("kind",)}, "source=dart"),
        ({"chunk_days": 2}, "one-day"),
        ({"end_exclusive": date(2026, 6, 30)}, "exact completed 30-day"),
        ({"max_chunks": 29}, "all 30"),
        ({"sync_company_master": True}, "company master"),
        ({"dry_run": True}, "dry_run"),
        ({"restart": True}, "apply checkpoint"),
    ],
)
def test_dart_replay_requires_exact_apply_contract(
    tmp_path: Path,
    changes: dict[str, object],
    message: str,
) -> None:
    base = BackfillOptions(
        start=date(2026, 6, 1),
        end_exclusive=date(2026, 7, 1),
        checkpoint_path=tmp_path / "unused.json",
        sources=("dart",),
        replay=True,
    )

    with pytest.raises(BackfillConfigurationError, match=message):
        replace(base, **changes).validate()


def test_cli_exposes_dart_replay_without_changing_the_job_fingerprint(
    tmp_path: Path,
) -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "--root",
            str(Path(__file__).resolve().parents[1]),
            "--from-date",
            "2026-06-01",
            "--to-date",
            "2026-07-01",
            "--source",
            "dart",
            "--max-chunks",
            "30",
            "--checkpoint",
            str(tmp_path / "checkpoint.json"),
            "--replay",
        ]
    )
    _, replay_options = options_from_args(args)
    apply_options = replace(replay_options, replay=False)

    assert replay_options.replay is True
    assert official_backfill.job_contract(replay_options, code_revision=CODE_REVISION) == (
        official_backfill.job_contract(apply_options, code_revision=CODE_REVISION)
    )


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
