from __future__ import annotations

import copy
from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from curator import official_ingest
from curator.backfill_checkpoint_api import (
    RemoteCheckpointSnapshot,
    RemoteCheckpointWrite,
    canonical_checkpoint,
    checkpoint_payload_hash,
)
from curator.official_backfill import (
    BackfillOptions,
    run_backfill,
)
ROOT = Path(__file__).resolve().parents[1]
V1 = (ROOT / "deploy" / "activist" / "governance_v1.php").read_text(
    encoding="utf-8"
)
V2 = (ROOT / "deploy" / "activist" / "governance_v2.php").read_text(
    encoding="utf-8"
)
BACKEND_BINDING_ID = "b" * 64
CODE_REVISION = "a" * 40


class MemoryCheckpointStore:
    def __init__(self) -> None:
        self.records: dict[str, tuple[int, dict[str, object], str]] = {}

    def get(self, fingerprint: str) -> RemoteCheckpointSnapshot:
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
        current = self.records.get(fingerprint)
        assert expected_version == (0 if current is None else current[0])
        normalized = canonical_checkpoint(checkpoint)
        payload_hash = checkpoint_payload_hash(normalized)
        if current is not None and current[2] == payload_hash:
            return RemoteCheckpointWrite(
                version=expected_version,
                payload_hash=payload_hash,
                unchanged=True,
            )
        version = expected_version + 1
        self.records[fingerprint] = (
            version,
            copy.deepcopy(normalized),
            payload_hash,
        )
        return RemoteCheckpointWrite(
            version=version,
            payload_hash=payload_hash,
            unchanged=False,
        )

    def close(self) -> None:
        return None


def successful_summary() -> dict[str, object]:
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


def _dart_payload(retrieved_at: str) -> dict[str, object]:
    return {
        "companies": [{"company_id": "00126380"}],
        "documents": [
            {
                "document_id": "dart:20260728000001",
                "company_id": "00126380",
                "source_right_id": "official:dart",
                "title": "원문 제목",
                "original_language": "ko",
                "retrieved_at": retrieved_at,
            }
        ],
        "events": [
            {
                "event_id": "event:dart:20260728000001",
                "company_id": "00126380",
                "title": "원문 제목",
                "document_ids": ["dart:20260728000001"],
            }
        ],
        "source_rights": [],
        "expected_source_right_revisions": {
            "official:dart": {
                "rights_revision": "1" * 64,
                "contract_revision": "2" * 64,
            }
        },
        "expected_deployment_code_revision": CODE_REVISION,
        "expected_release_state": "closed",
    }


def _replay_run(payload: dict[str, object]) -> dict[str, object]:
    return {
        "run_id": "run:read-only-replay",
        "pipeline": "ingest-official",
        "source_key": "dart",
        "status": "succeeded",
        "code_revision": CODE_REVISION,
        "idempotency_key": "official-backfill-v1:" + "3" * 32,
        "ingest_mode": "replay",
        "stable_payload_contract_version": 1,
        "stable_payload_sha256": official_ingest._stable_dart_payload_sha256(
            payload
        ),
        "replay_attempted_at": "2026-07-28T10:30:00+00:00",
        "raw_count": 1,
        "fetched_count": 3,
        "resolved_count": 1,
        "accepted_count": 1,
        "error_count": 0,
    }


def test_stable_dart_digest_ignores_only_retrieval_clock() -> None:
    first = _dart_payload("2026-07-28T10:00:00+00:00")
    second = _dart_payload("2026-07-28T10:30:00+00:00")
    assert official_ingest._stable_dart_payload_sha256(
        first
    ) == official_ingest._stable_dart_payload_sha256(second)

    changed = copy.deepcopy(second)
    changed["documents"][0]["title"] = "변경된 제목"  # type: ignore[index]
    assert official_ingest._stable_dart_payload_sha256(
        first
    ) != official_ingest._stable_dart_payload_sha256(changed)


def test_replay_contract_is_signed_into_every_remote_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _dart_payload("2026-07-28T10:30:00+00:00")
    run = _replay_run(payload)
    calls: list[dict[str, object]] = []

    def fake_post(_action: str, submitted: dict[str, object], **_kwargs: object):
        calls.append(copy.deepcopy(submitted))
        replay = submitted["dart_replay"]
        assert isinstance(replay, dict)
        return {
            "ok": True,
            "backend_binding_id": BACKEND_BINDING_ID,
            "replay_verified": True,
            "replay_run_id": replay["run_id"],
            "stable_payload_sha256": replay["stable_payload_sha256"],
            "replay_attempted_at": replay["attempted_at"],
            "upserted": {
                key: len(submitted[key])
                for key in ("companies", "documents", "events", "source_rights")
            }
            | {
                "source_rights_rejected": 0,
                "runs": int(bool(submitted["run"])),
            },
        }

    monkeypatch.setenv("BSIDE_BACKEND_BINDING_ID", BACKEND_BINDING_ID)
    monkeypatch.setattr(official_ingest, "remote_api_configured", lambda: True)
    monkeypatch.setattr(official_ingest, "post_remote_action", fake_post)

    summary = official_ingest.sync_governance_payload(payload, run=run)

    assert len(calls) == 2
    assert all(call["ingest_mode"] == "replay" for call in calls)
    assert all(
        call["dart_replay"]["stable_payload_sha256"]  # type: ignore[index]
        == run["stable_payload_sha256"]
        for call in calls
    )
    assert summary["official_remote_run_persisted"] == 1
    assert summary["official_remote_failed"] == 0


def test_replay_ack_without_server_noop_proof_is_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = _dart_payload("2026-07-28T10:30:00+00:00")
    run = _replay_run(payload)
    calls = 0

    def unsafe_old_server(
        _action: str, submitted: dict[str, object], **_kwargs: object
    ) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {
            "ok": True,
            "backend_binding_id": BACKEND_BINDING_ID,
            "upserted": {
                key: len(submitted[key])
                for key in ("companies", "documents", "events", "source_rights")
            }
            | {
                "source_rights_rejected": 0,
                "runs": int(bool(submitted["run"])),
            },
        }

    monkeypatch.setenv("BSIDE_BACKEND_BINDING_ID", BACKEND_BINDING_ID)
    monkeypatch.setattr(official_ingest, "remote_api_configured", lambda: True)
    monkeypatch.setattr(official_ingest, "post_remote_action", unsafe_old_server)

    summary = official_ingest.sync_governance_payload(payload, run=run)

    assert calls == 1
    assert summary["official_remote_run_persisted"] == 0
    assert summary["official_remote_failed"] == 1
    assert summary["official_remote_ack_mismatches"] == 1


def test_backfill_replay_uses_real_attempt_time_and_receipts_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GITHUB_SHA", CODE_REVISION)
    store = MemoryCheckpointStore()
    options = BackfillOptions(
        start=date(2026, 6, 1),
        end_exclusive=date(2026, 7, 1),
        checkpoint_path=tmp_path / "checkpoint.json",
        sources=("dart",),
    )
    apply_clock = datetime(2026, 7, 2, 1, 0, tzinfo=timezone.utc)
    replay_clock = datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc)
    run_backfill(
        tmp_path,
        options,
        ingest_runner=lambda *_args, **_kwargs: successful_summary(),
        now_provider=lambda: apply_clock,
        checkpoint_store=store,
    )
    calls: list[dict[str, object]] = []

    def replay_runner(_root: Path, **kwargs: object) -> dict[str, object]:
        calls.append(kwargs)
        return successful_summary()

    report = run_backfill(
        tmp_path,
        replace(options, replay=True, max_chunks=30),
        ingest_runner=replay_runner,
        now_provider=lambda: replay_clock,
        checkpoint_store=store,
    )

    assert all(call["replay"] is True for call in calls)
    assert all(call["now"] == replay_clock for call in calls)
    assert all(
        row["replay_attempted_at"] == replay_clock.isoformat()
        for row in report["receipt_contract"]["windows"]  # type: ignore[index]
    )


def test_php_replay_preflight_is_read_only_and_before_upserts() -> None:
    helper = V1[
        V1.index("function v1_dart_replay_read_only_ack") :
        V1.index("function upsert_governance_snapshot")
    ]
    ingest = V1[
        V1.index("function upsert_governance_snapshot") :
        V1.index("function ", V1.index("function upsert_governance_snapshot") + 20)
    ]
    assert "FOR UPDATE" in helper
    assert "stable_payload_sha256" in helper
    assert "dart_replay_count_mismatch" in helper
    assert "$pdo->commit();" in helper
    assert "INSERT INTO" not in helper
    assert "UPDATE " not in helper
    assert ingest.index("v1_dart_replay_read_only_ack(") < ingest.index(
        "$companyStmt ="
    )


def test_replay_state_uses_consistent_unbuffered_snapshot_and_closes_cursor() -> None:
    digest = V2[
        V2.index("function v2_alpha_replay_table_digest") :
        V2.index("function v2_alpha_replay_state")
    ]
    snapshot = V2[
        V2.index("function v2_alpha_snapshot_begin") :
        V2.index("function v2_alpha_replay_state")
    ]
    endpoint = V2[
        V2.index("function v2_ops_alpha_replay_state") :
        V2.index("function handle_v2_request")
    ]
    assert "finally" in digest
    assert "$statement->closeCursor();" in digest
    assert "PDO::MYSQL_ATTR_USE_BUFFERED_QUERY" in snapshot
    assert "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ" in snapshot
    assert "SET TRANSACTION READ ONLY" in snapshot
    assert "$pdo->beginTransaction()" in snapshot
    assert "$pdo->rollBack()" in snapshot
    assert "v2_alpha_snapshot_begin($pdo)" in endpoint
    assert "v2_alpha_snapshot_finish($pdo,$snapshot,true)" in endpoint
    assert "v2_alpha_snapshot_finish($pdo,$snapshot,false)" in endpoint
