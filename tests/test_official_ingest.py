from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from curator import official_ingest


def test_remote_sync_persists_one_final_failed_run_after_all_data_chunks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []
    responses = iter(({"ok": True}, {"ok": False}, {"ok": True}, {"ok": True}))

    def fake_post(action: str, payload: dict[str, object], *, timeout: float) -> dict[str, object]:
        assert action == "upsert_governance_snapshot"
        assert timeout == 45.0
        calls.append(payload)
        return next(responses)

    monkeypatch.setattr(official_ingest, "remote_api_configured", lambda: True)
    monkeypatch.setattr(official_ingest, "post_remote_action", fake_post)
    payload: dict[str, object] = {
        "companies": [],
        "documents": [{"document_id": f"dart:{index}"} for index in range(3601)],
        "events": [],
        "source_rights": [{"source_right_id": "official:dart"}],
    }
    run = {
        "run_id": "run:official-test",
        "status": "succeeded",
        "error_count": 2,
        "source_outcomes": {"dart": {"status": "succeeded"}},
    }

    summary = official_ingest.sync_governance_payload(payload, run=run)

    assert len(calls) == 4
    assert all(call["run"] == {} for call in calls[:-1])
    assert calls[0]["source_rights"] == [{"source_right_id": "official:dart"}]
    assert all(call["source_rights"] == [] for call in calls[1:])
    final_run = calls[-1]["run"]
    assert isinstance(final_run, dict)
    assert final_run["status"] == "failed"
    assert final_run["error_count"] == 3
    assert final_run["remote_data_batches_attempted"] == 3
    assert final_run["remote_data_batches_succeeded"] == 2
    assert final_run["remote_data_batches_failed"] == 1
    assert final_run["source_outcomes"] == run["source_outcomes"]
    assert summary == {
        "official_remote_synced": 2,
        "official_remote_failed": 1,
        "official_remote_skipped": 0,
        "official_remote_batches_attempted": 3,
        "official_remote_run_persisted": 1,
    }


def test_required_kind_without_endpoint_fails_and_records_source_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_run: dict[str, object] = {}

    def fake_sync(payload: dict[str, object], *, run: dict[str, object]) -> dict[str, int]:
        assert payload["documents"] == []
        captured_run.update(run)
        return {
            "official_remote_synced": 1,
            "official_remote_failed": 0,
            "official_remote_skipped": 0,
            "official_remote_batches_attempted": 1,
            "official_remote_run_persisted": 1,
        }

    monkeypatch.setenv("CURATOR_REQUIRE_KIND", "1")
    monkeypatch.delenv("KIND_DISCLOSURE_ENDPOINT", raising=False)
    monkeypatch.setattr(official_ingest, "sync_governance_payload", fake_sync)

    summary = official_ingest.run(
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        start=date(2026, 7, 15),
        end=date(2026, 7, 16),
        settings_overrides={"dart_enabled": False, "kind_enabled": True},
    )

    assert summary["official_failed"] == 1
    assert summary["official_kind_required"] == 1
    assert summary["official_kind_configured"] == 0
    assert summary["official_kind_errors"] == 1
    assert captured_run["status"] == "failed"
    assert captured_run["source_key"] == "kind"
    outcomes = captured_run["source_outcomes"]
    assert isinstance(outcomes, dict)
    assert {key: outcomes["kind"][key] for key in (
        "enabled",
        "required",
        "configured",
        "fetched",
        "accepted",
        "error_count",
        "status",
    )} == {
        "enabled": True,
        "required": True,
        "configured": False,
        "fetched": 0,
        "accepted": 0,
        "error_count": 1,
        "status": "failed",
    }
    assert outcomes["kind"]["failure_kinds"]["configuration"] == 1


def test_explicit_kind_disable_skips_configured_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_payload: dict[str, object] = {}
    captured_run: dict[str, object] = {}

    class UnexpectedKindConnector:
        def __init__(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("KIND connector must not run in explicit DART-only mode")

    def fake_sync(payload: dict[str, object], *, run: dict[str, object]) -> dict[str, int]:
        captured_payload.update(payload)
        captured_run.update(run)
        return {
            "official_remote_synced": 1,
            "official_remote_failed": 0,
            "official_remote_skipped": 0,
            "official_remote_batches_attempted": 1,
            "official_remote_run_persisted": 1,
        }

    monkeypatch.setenv("KIND_DISCLOSURE_ENDPOINT", "https://kind-adapter.invalid/v1/disclosures")
    monkeypatch.setenv("CURATOR_ENABLE_KIND", "0")
    monkeypatch.setenv("CURATOR_REQUIRE_KIND", "0")
    monkeypatch.setattr(official_ingest, "KindConnector", UnexpectedKindConnector)
    monkeypatch.setattr(official_ingest, "sync_governance_payload", fake_sync)

    summary = official_ingest.run(
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        start=date(2026, 7, 15),
        end=date(2026, 7, 16),
        settings_overrides={"dart_enabled": False, "kind_enabled": True},
    )

    assert summary["official_failed"] == 0
    assert summary["official_kind_enabled"] == 0
    assert summary["official_kind_configured"] == 1
    assert captured_payload["source_rights"] == [
        official_ingest.source_right_payloads({}, include_kind=False)[0]
    ]
    outcomes = captured_run["source_outcomes"]
    assert isinstance(outcomes, dict)
    assert outcomes["kind"]["requested"] is True
    assert outcomes["kind"]["enabled"] is False
    assert outcomes["kind"]["status"] == "disabled"


def test_connector_failure_discards_partial_dart_window_before_remote_sync(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_payload: dict[str, object] = {}
    captured_run: dict[str, object] = {}

    class PartialDartConnector:
        list_requests = 2
        pages_fetched = 1
        rows_fetched = 1

        def __init__(self, _api_key: str) -> None:
            pass

        def iter_disclosure_rows(self, *_args: object, **_kwargs: object):  # type: ignore[no-untyped-def]
            yield {
                "corp_code": "00126380",
                "corp_name": "삼성전자",
                "stock_code": "005930",
                "corp_cls": "Y",
                "report_nm": "주요사항보고서(자기주식취득결정)",
                "rcept_no": "20260716000123",
                "flr_nm": "삼성전자",
                "rcept_dt": "20260716",
                "rm": "",
            }
            raise RuntimeError("page 2 failed")

        def fetch_company_master(self) -> list[dict[str, object]]:
            return []

    def fake_sync(payload: dict[str, object], *, run: dict[str, object]) -> dict[str, int]:
        captured_payload.update(payload)
        captured_run.update(run)
        return {
            "official_remote_synced": 1,
            "official_remote_failed": 0,
            "official_remote_skipped": 0,
            "official_remote_batches_attempted": 1,
            "official_remote_run_persisted": 1,
        }

    monkeypatch.setenv("DART_API_KEY", "x" * 40)
    monkeypatch.setattr(official_ingest, "DartConnector", PartialDartConnector)
    monkeypatch.setattr(official_ingest, "sync_governance_payload", fake_sync)

    summary = official_ingest.run(
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        start=date(2026, 7, 15),
        end=date(2026, 7, 16),
        settings_overrides={"kind_enabled": False},
    )

    assert captured_payload["documents"] == []
    assert captured_payload["events"] == []
    assert summary["official_fetched"] == 1
    assert summary["official_dart_accepted"] == 0
    assert summary["official_dart_discarded"] == 1
    assert summary["official_dart_errors"] == 1
    assert summary["official_failed"] == 1
    outcomes = captured_run["source_outcomes"]
    assert isinstance(outcomes, dict)
    assert outcomes["dart"]["failure_kinds"]["connector"] == 1
    assert outcomes["dart"]["pages_fetched"] == 1


def test_enabled_dart_without_api_key_is_a_failed_source_not_a_skip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_run: dict[str, object] = {}

    def fake_sync(_payload: dict[str, object], *, run: dict[str, object]) -> dict[str, int]:
        captured_run.update(run)
        return {
            "official_remote_synced": 1,
            "official_remote_failed": 0,
            "official_remote_skipped": 0,
            "official_remote_batches_attempted": 1,
            "official_remote_run_persisted": 1,
        }

    monkeypatch.delenv("DART_API_KEY", raising=False)
    monkeypatch.setattr(official_ingest, "sync_governance_payload", fake_sync)

    summary = official_ingest.run(
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        start=date(2026, 7, 15),
        end=date(2026, 7, 16),
        settings_overrides={"dart_enabled": True, "kind_enabled": False},
    )

    assert summary["official_failed"] == 1
    assert summary["official_skipped"] == 0
    assert summary["official_dart_errors"] == 1
    outcomes = captured_run["source_outcomes"]
    assert isinstance(outcomes, dict)
    assert outcomes["dart"]["status"] == "failed"
    assert outcomes["dart"]["failure_kinds"]["configuration"] == 1


def test_default_incremental_window_uses_kst_date_before_utc_midnight_rollover(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[tuple[date, date]] = []

    class EmptyDartConnector:
        list_requests = 0
        pages_fetched = 0
        rows_fetched = 0

        def __init__(self, _api_key: str) -> None:
            pass

        def iter_disclosure_rows(self, start: date, end: date, **_kwargs: object):  # type: ignore[no-untyped-def]
            seen.append((start, end))
            return iter(())

        def fetch_company_master(self) -> list[dict[str, object]]:
            return []

    monkeypatch.setenv("DART_API_KEY", "x" * 40)
    monkeypatch.delenv("OFFICIAL_INGEST_START", raising=False)
    monkeypatch.delenv("OFFICIAL_INGEST_END", raising=False)
    monkeypatch.setattr(official_ingest, "DartConnector", EmptyDartConnector)

    summary = official_ingest.run(
        now=datetime(2026, 7, 15, 15, 5, tzinfo=timezone.utc),  # 2026-07-16 00:05 KST
        settings_overrides={"lookback_days": 2, "kind_enabled": False},
        dry_run=True,
    )

    assert summary["official_failed"] == 0
    assert seen == [(date(2026, 7, 14), date(2026, 7, 16))]
