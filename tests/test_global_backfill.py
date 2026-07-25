from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Sequence

import pytest

from curator.global_backfill import (
    GlobalBackfillError,
    plan_global_backfill,
    run_global_backfill,
)


REVISION = "a" * 40
NOW = datetime(2026, 7, 25, 12, 0, tzinfo=timezone.utc)


def test_plan_is_exact_bounded_and_oldest_first() -> None:
    plan = plan_global_backfill(
        country_code="us",
        mode="replay",
        from_date="2026-06-24",
        to_date="2026-07-24",
        max_windows=30,
        now=NOW,
    )

    assert plan.country_code == "US"
    assert plan.mode == "replay"
    assert len(plan.windows) == 30
    assert plan.windows[0].start == date(2026, 6, 24)
    assert plan.windows[-1].end_exclusive == date(2026, 7, 24)
    assert all(
        window.end_exclusive - window.start
        == plan.windows[0].end_exclusive - plan.windows[0].start
        for window in plan.windows
    )


@pytest.mark.parametrize(
    ("overrides", "code"),
    [
        ({"from_date": "2026-7-01"}, "invalid_from_date"),
        ({"to_date": "2026-07-01"}, "invalid_global_backfill_window"),
        ({"to_date": "2026-08-02"}, "global_backfill_exceeds_31_windows"),
        ({"max_windows": 0}, "invalid_global_backfill_max_windows"),
        ({"max_windows": 2}, "global_backfill_exceeds_max_windows"),
        (
            {
                "from_date": "2026-07-25",
                "to_date": "2026-07-26",
                "max_windows": 1,
            },
            "global_backfill_requires_completed_days",
        ),
    ],
)
def test_plan_rejects_invalid_unbounded_or_incomplete_dates(
    overrides: dict[str, object],
    code: str,
) -> None:
    arguments: dict[str, object] = {
        "country_code": "US",
        "mode": "apply",
        "from_date": "2026-07-01",
        "to_date": "2026-07-04",
        "max_windows": 3,
        "now": NOW,
    }
    arguments.update(overrides)
    with pytest.raises(GlobalBackfillError, match=code):
        plan_global_backfill(**arguments)  # type: ignore[arg-type]


def _receipt_from_arguments(
    arguments: Sequence[str],
    *,
    replay_valid: bool,
) -> tuple[Path, dict[str, object]]:
    values = list(arguments)

    def value(name: str) -> str:
        return values[values.index(name) + 1]

    start = value("--from-date")
    end = value("--to-date")
    receipt_path = Path(value("--evidence"))
    payload: dict[str, object] = {
        "schema_version": 1,
        "status": "succeeded",
        "country_code": value("--country"),
        "connector_id": "connector:us:sec-edgar",
        "source_right_id": "official:sec-edgar",
        "window": {"start": start, "end_exclusive": end},
        "code_revision": value("--code-revision"),
        "collection_mode": "completed-day",
        "raw_count": 7,
        "acknowledged_count": 2,
        "idempotent": "--replay-only" in values,
    }
    if "--verify-replay" in values:
        payload["replay_verification"] = {
            "attempted": True,
            "same_payload": True,
            "idempotent": replay_valid,
            "read_only": "--replay-only" in values,
            "chunk_count": 1,
            "idempotent_chunk_count": 1 if replay_valid else 0,
            "idempotency_keys_match": True,
            "ingest_ids_match": True,
            "raw_count": 7,
            "acknowledged_count": 2,
        }
    receipt_path.write_text(
        json.dumps(payload, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return receipt_path, payload


def test_runner_preserves_one_receipt_per_day_and_exact_replay_summary(
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, str]] = []

    def ingest(arguments: Sequence[str] | None) -> int:
        assert arguments is not None
        assert "--verify-replay" in arguments
        assert "--replay-only" in arguments
        assert "--require-active-pipeline" not in arguments
        receipt_path, payload = _receipt_from_arguments(
            arguments,
            replay_valid=True,
        )
        assert receipt_path.parent == tmp_path / "receipts"
        window = payload["window"]
        assert isinstance(window, dict)
        calls.append((str(window["start"]), str(window["end_exclusive"])))
        return 0

    plan = plan_global_backfill(
        country_code="US",
        mode="replay",
        from_date="2026-07-21",
        to_date="2026-07-24",
        max_windows=3,
        now=NOW,
    )
    summary_path = tmp_path / "summary.json"
    summary = run_global_backfill(
        plan=plan,
        code_revision=REVISION,
        evidence_dir=tmp_path / "receipts",
        summary_path=summary_path,
        max_pages=200,
        ingest_entrypoint=ingest,
    )

    assert calls == [
        ("2026-07-21", "2026-07-22"),
        ("2026-07-22", "2026-07-23"),
        ("2026-07-23", "2026-07-24"),
    ]
    assert summary["status"] == "succeeded"
    assert summary["processed_windows"] == 3
    assert summary["total_raw_count"] == 21
    assert summary["total_acknowledged_count"] == 6
    receipts = summary["receipts"]
    assert isinstance(receipts, list)
    assert all(receipt["replay_verified"] is True for receipt in receipts)
    assert all(len(receipt["receipt_sha256"]) == 64 for receipt in receipts)
    assert json.loads(summary_path.read_text(encoding="utf-8")) == summary


def test_runner_fails_closed_and_keeps_partial_summary_on_bad_replay(
    tmp_path: Path,
) -> None:
    calls = 0

    def ingest(arguments: Sequence[str] | None) -> int:
        nonlocal calls
        assert arguments is not None
        assert "--require-active-pipeline" not in arguments
        calls += 1
        _receipt_from_arguments(
            arguments,
            replay_valid=calls == 1,
        )
        return 0

    plan = plan_global_backfill(
        country_code="US",
        mode="replay",
        from_date="2026-07-21",
        to_date="2026-07-23",
        max_windows=2,
        now=NOW,
    )
    summary_path = tmp_path / "summary.json"
    with pytest.raises(
        GlobalBackfillError,
        match="global_backfill_replay_verification_failed",
    ):
        run_global_backfill(
            plan=plan,
            code_revision=REVISION,
            evidence_dir=tmp_path / "receipts",
            summary_path=summary_path,
            max_pages=200,
            ingest_entrypoint=ingest,
        )

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["status"] == "failed"
    assert summary["processed_windows"] == 1
    assert summary["failed_window"] == "2026-07-22"
    assert summary["error"]["code"] == (
        "global_backfill_replay_verification_failed"
    )
