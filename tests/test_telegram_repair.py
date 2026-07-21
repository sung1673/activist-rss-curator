from __future__ import annotations

import json
from pathlib import Path

import pytest

from curator import telegram_repair


def test_repair_hydrates_remote_state_before_backfill(config, now, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    calls: list[str] = []

    monkeypatch.setattr(telegram_repair, "load_config", lambda _path: config)
    monkeypatch.setattr(telegram_repair, "load_state", lambda _path: {})
    monkeypatch.setattr(telegram_repair, "now_in_timezone", lambda _timezone: now)

    def fake_hydrate(state, _config, _now):  # type: ignore[no-untyped-def]
        calls.append("hydrate")
        state["telegram_source_channels"] = [
            {"handle": "licensed", "last_message_id": 10}
        ]
        return {"runtime_hydrated": 1}

    def fake_backfill(state, _config, _now, **kwargs):  # type: ignore[no-untyped-def]
        calls.append("backfill")
        assert state["telegram_source_channels"][0]["handle"] == "licensed"
        assert kwargs["sync_remote"] is True
        assert kwargs["force_remote_resync"] is True
        assert kwargs["rebuild_remote_signals"] is True
        assert kwargs["before_message_id"] == 0
        kwargs["checkpoint_callback"](
            {"handle": "licensed", "status": "ok", "remote_checkpoint_complete": 1}
        )
        return {
            "telegram_remote_failed": 0,
            "telegram_backfill_messages_seen": 7,
            "telegram_source_rights_blocked": 0,
        }

    monkeypatch.setattr(telegram_repair, "hydrate_runtime_state", fake_hydrate)
    monkeypatch.setattr(telegram_repair, "backfill_telegram_messages", fake_backfill)
    monkeypatch.setattr(
        telegram_repair, "save_state", lambda *_args: calls.append("save")
    )

    summary = telegram_repair.run_repair(Path("."), days=365, limit_per_channel=3000)

    assert calls == ["hydrate", "backfill", "save", "save"]
    assert summary["runtime_hydrated"] == 1
    assert summary["telegram_backfill_messages_seen"] == 7
    assert summary["telegram_repair_checkpoints"] == 1


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"days": 0}, "days_out_of_bounds"),
        ({"days": 366}, "days_out_of_bounds"),
        ({"limit_per_channel": 3001}, "limit_per_channel_out_of_bounds"),
        ({"channel_limit": 501}, "channel_limit_out_of_bounds"),
        ({"max_messages": 0}, "max_messages_out_of_bounds"),
        ({"max_messages": 300_001}, "max_messages_out_of_bounds"),
        ({"only_handles": {"not-a-handle"}}, "only_handles_invalid"),
        ({"start_after_handle": "not-a-handle"}, "start_after_handle_invalid"),
        ({"before_message_id": -1}, "before_message_id_out_of_bounds"),
        ({"before_message_id": 42}, "before_message_id_requires_one_handle"),
        (
            {
                "before_message_id": 42,
                "only_handles": {"licensed"},
                "start_after_handle": "licensed",
            },
            "before_message_id_requires_one_handle",
        ),
    ],
)
def test_repair_request_is_bounded(overrides: dict[str, object], error: str) -> None:
    request: dict[str, object] = {
        "days": 365,
        "limit_per_channel": 3000,
        "channel_limit": 0,
        "max_messages": 300_000,
        "only_handles": set(),
        "start_after_handle": "",
        "before_message_id": 0,
    }
    request.update(overrides)

    with pytest.raises(ValueError, match=error):
        telegram_repair.validate_repair_request(**request)  # type: ignore[arg-type]


def test_repair_exception_writes_failure_metrics(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    metrics_path = tmp_path / "repair-metrics.json"
    metrics_path.write_text(
        json.dumps(
            {
                "ok": False,
                "status": "running",
                "telegram_repair_last_handle": "licensed",
                "telegram_repair_remote_checkpoint_complete": 1,
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("CURATOR_RUN_METRICS_PATH", str(metrics_path))
    monkeypatch.setattr(
        telegram_repair,
        "run_repair",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("sensitive detail")
        ),
    )

    with pytest.raises(RuntimeError, match="sensitive detail"):
        telegram_repair.main(["--root", str(tmp_path)])

    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert metrics == {
        "error_type": "RuntimeError",
        "ok": False,
        "status": "failed",
        "telegram_repair_last_handle": "licensed",
        "telegram_repair_remote_checkpoint_complete": 1,
    }


def test_repair_cli_fails_closed_on_truncated_history(tmp_path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    metrics_path = tmp_path / "repair-metrics.json"
    monkeypatch.setenv("CURATOR_RUN_METRICS_PATH", str(metrics_path))
    monkeypatch.setattr(
        telegram_repair,
        "run_repair",
        lambda *_args, **_kwargs: {
            "telegram_channel_failed": 0,
            "telegram_remote_failed": 0,
            "telegram_remote_pending": 0,
            "telegram_backfill_truncated_channels": 2,
            "telegram_backfill_resume_handle": "next_channel",
            "telegram_backfill_resume_after_handle": "completed_channel",
        },
    )

    with pytest.raises(SystemExit, match="1"):
        telegram_repair.main(["--root", str(tmp_path)])

    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert metrics["ok"] is False
    assert metrics["status"] == "failed"
    assert metrics["telegram_backfill_truncated_channels"] == 2
    assert metrics["telegram_backfill_resume_handle"] == "next_channel"
