from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import ModuleType


ROOT = Path(__file__).resolve().parents[1]


def load_watchdog() -> ModuleType:
    path = ROOT / ".github" / "scripts" / "watchdog.py"
    spec = importlib.util.spec_from_file_location("bside_watchdog", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parse_timestamp_normalizes_utc() -> None:
    watchdog = load_watchdog()
    assert watchdog.parse_timestamp("2026-07-16T00:00:00Z") == datetime(2026, 7, 16, tzinfo=timezone.utc)
    assert watchdog.parse_timestamp("not-a-date") is None


def test_missing_configuration_emits_incident_output(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    watchdog = load_watchdog()
    output_path = tmp_path / "github-output.txt"
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("BSIDE_API_BASE_URL", raising=False)
    monkeypatch.delenv("BSIDE_OPS_TOKEN", raising=False)
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_path))
    assert watchdog.main() == 0
    assert "incident=true" in output_path.read_text(encoding="utf-8")
    report = (tmp_path / ".watchdog-report.md").read_text(encoding="utf-8")
    assert "Missing operational configuration" in report


def test_report_marks_old_delivery_and_dead_letter() -> None:
    watchdog = load_watchdog()
    report = watchdog.build_report(
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        payload={
            "last_success_at": "2026-07-15T23:55:00Z",
            "pending_outbox": 3,
            "oldest_pending_at": "2026-07-15T23:30:00Z",
            "dead_letter_count": 1,
        },
        reasons=["Oldest delivery has waited 30 minutes", "Delivery dead-letter queue contains 1 item"],
        ingest_age=5,
        outbox_age=30,
    )
    assert "INCIDENT" in report
    assert "Pending outbox: `3`" in report
    assert "Dead-letter count: `1`" in report


def test_default_outbox_budget_is_five_minutes() -> None:
    source = (ROOT / ".github" / "scripts" / "watchdog.py").read_text(encoding="utf-8")
    assert 'WATCHDOG_MAX_OUTBOX_AGE_MINUTES"), 5' in source


def test_future_timestamps_and_negative_counts_fail_closed(tmp_path: Path, monkeypatch) -> None:  # type: ignore[no-untyped-def]
    watchdog = load_watchdog()
    output_path = tmp_path / "github-output.txt"
    future = datetime.now(timezone.utc) + timedelta(minutes=15)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BSIDE_API_BASE_URL", "https://api.example.test")
    monkeypatch.setenv("BSIDE_OPS_TOKEN", "token")
    monkeypatch.setenv("GITHUB_OUTPUT", str(output_path))
    monkeypatch.setattr(
        watchdog,
        "fetch_health",
        lambda *_args: {
            "last_success_at": future.isoformat(),
            "pending_outbox": -1,
            "oldest_pending_at": None,
            "dead_letter_count": -1,
        },
    )

    assert watchdog.main() == 0
    assert "incident=true" in output_path.read_text(encoding="utf-8")
    report = (tmp_path / ".watchdog-report.md").read_text(encoding="utf-8")
    assert "timestamp is" in report and "in the future" in report
    assert "negative pending_outbox" in report
    assert "negative dead_letter_count" in report
