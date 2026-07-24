from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from curator import dart_canary_sample
from curator.dart_canary_sample import (
    DartCanarySampleOptions,
    run_dart_canary_sample,
)
from curator.official_sources import DartQuotaExceededError, DartRequestBudget


def dart_row(
    *,
    receipt_no: str,
    received_date: str,
    title: str,
    remarks: str = "",
) -> dict[str, object]:
    return {
        "rcept_no": receipt_no,
        "corp_code": "00123456",
        "corp_name": "Canary Corp",
        "stock_code": "123456",
        "corp_cls": "Y",
        "report_nm": title,
        "rcept_dt": received_date,
        "flr_nm": "Canary Corp",
        "rm": remarks,
    }


class FakeConnector:
    def __init__(
        self,
        budget: DartRequestBudget,
        rows: list[dict[str, object]],
        *,
        quota_failure: bool = False,
    ) -> None:
        self.budget = budget
        self.rows = rows
        self.quota_failure = quota_failure
        self.calls: list[tuple[date, date, int, int]] = []
        self.requests_made = 0
        self.pages_fetched = 0
        self.rows_fetched = 0

    def iter_disclosure_rows(
        self,
        start: date,
        end: date,
        *,
        page_count: int = 100,
        max_pages: int = 100,
    ):  # type: ignore[no-untyped-def]
        self.calls.append((start, end, page_count, max_pages))
        self.budget.consume()
        self.requests_made += 1
        if self.quota_failure:
            raise DartQuotaExceededError("OpenDART status 020")
        self.pages_fetched += 1
        for row in self.rows:
            received = date.fromisoformat(
                f"{str(row['rcept_dt'])[:4]}-{str(row['rcept_dt'])[4:6]}-{str(row['rcept_dt'])[6:8]}"
            )
            if start <= received <= end:
                self.rows_fetched += 1
                yield row


def test_canary_scans_last_complete_day_and_exact_365_day_history() -> None:
    rows = [
        dart_row(
            receipt_no="20260715000001",
            received_date="20260715",
            title="Shareholder Proposal",
        ),
        dart_row(
            receipt_no="20260714000002",
            received_date="20260714",
            title="Correction Tender Offer",
        ),
        dart_row(
            receipt_no="20250716000003",
            received_date="20250716",
            title="Withdrawal Tender Offer",
        ),
    ]
    holder: dict[str, FakeConnector] = {}

    def factory(_api_key: str, budget: DartRequestBudget) -> FakeConnector:
        connector = FakeConnector(budget, rows)
        holder["connector"] = connector
        return connector

    report = run_dart_canary_sample(
        "test-key",
        now=datetime(2026, 7, 16, 0, 0, tzinfo=timezone.utc),
        connector_factory=factory,
    )

    connector = holder["connector"]
    assert report["status"] == "succeeded"
    assert report["dry_run"] is True
    assert report["recent_day"] == {
        "date": "2026-07-15",
        "fetched_count": 1,
        "governance_document_count": 1,
        "governance_event_count": 1,
    }
    history = report["history"]
    assert isinstance(history, dict)
    assert history["from_date"] == "2025-07-16"
    assert history["to_date"] == "2026-07-15"
    assert history["days"] == 365
    assert history["windows_scanned"] == 53

    # The completed day is fetched once. The 52 older windows cover every
    # preceding date exactly once, including both history boundaries.
    assert connector.calls[0][:2] == (date(2026, 7, 15), date(2026, 7, 15))
    older_days: list[date] = []
    for start, end, page_count, max_pages in connector.calls[1:]:
        assert page_count == 100
        assert max_pages == 100
        cursor = start
        while cursor <= end:
            older_days.append(cursor)
            cursor = cursor.fromordinal(cursor.toordinal() + 1)
    assert older_days[0] == date(2025, 7, 16)
    assert older_days[-1] == date(2026, 7, 14)
    assert len(older_days) == len(set(older_days)) == 364
    assert report["requests_used"] == len(connector.calls) == 53


def test_canary_selects_real_correction_and_withdrawal_without_changing_titles() -> None:
    correction_title = "Correction Tender Offer"
    withdrawal_title = "Withdrawal Tender Offer"
    rows = [
        dart_row(
            receipt_no="20260714000001",
            received_date="20260714",
            title=correction_title,
        ),
        dart_row(
            receipt_no="20260713000002",
            received_date="20260713",
            title=withdrawal_title,
        ),
    ]

    report = run_dart_canary_sample(
        "test-key",
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        options=DartCanarySampleOptions(lookback_days=10, scan_chunk_days=3),
        connector_factory=lambda _key, budget: FakeConnector(budget, rows),
    )

    samples = report["samples"]
    assert isinstance(samples, dict)
    assert samples["correction_count"] == 1
    assert samples["withdrawal_count"] == 1
    sample_rows = samples["rows"]
    assert isinstance(sample_rows, list)
    assert {row["title"] for row in sample_rows} == {correction_title, withdrawal_title}
    assert all(row["title_preserved"] is True for row in sample_rows)
    by_title = {row["title"]: row for row in sample_rows}
    assert by_title[correction_title]["is_correction"] is True
    assert by_title[withdrawal_title]["is_withdrawn"] is True
    assert by_title[withdrawal_title]["publication_status"] == "withdrawn"
    assert report["missing_sample_kinds"] == []


def test_canary_fails_closed_when_a_required_sample_kind_is_absent() -> None:
    rows = [
        dart_row(
            receipt_no="20260714000001",
            received_date="20260714",
            title="Correction Tender Offer",
        )
    ]
    report = run_dart_canary_sample(
        "test-key",
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        options=DartCanarySampleOptions(lookback_days=10),
        connector_factory=lambda _key, budget: FakeConnector(budget, rows),
    )

    assert report["status"] == "failed"
    assert report["missing_sample_kinds"] == ["withdrawal"]


def test_canary_propagates_status_020_and_uses_the_shared_bounded_budget() -> None:
    budget = DartRequestBudget(17)
    with pytest.raises(DartQuotaExceededError, match="020"):
        run_dart_canary_sample(
            "test-key",
            now=datetime(2026, 7, 16, tzinfo=timezone.utc),
            options=DartCanarySampleOptions(lookback_days=10),
            request_budget=budget,
            connector_factory=lambda _key, shared: FakeConnector(
                shared,
                [],
                quota_failure=True,
            ),
        )
    assert budget.used == 1


def test_canary_rejects_a_budget_over_the_daily_cap() -> None:
    with pytest.raises(ValueError, match="cannot exceed 10000"):
        run_dart_canary_sample(
            "test-key",
            request_budget=DartRequestBudget(10_001),
            connector_factory=lambda _key, budget: FakeConnector(budget, []),
        )


def test_cli_preserves_status_020_as_fail_closed_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    written: list[dict[str, object]] = []
    monkeypatch.setenv("DART_API_KEY", "test-key")

    def quota_failure(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise DartQuotaExceededError("OpenDART request quota exhausted")

    monkeypatch.setattr(dart_canary_sample, "run_dart_canary_sample", quota_failure)
    monkeypatch.setattr(
        dart_canary_sample,
        "_write_report",
        lambda _path, report: written.append(report),
    )
    with pytest.raises(SystemExit) as exc_info:
        dart_canary_sample.main(["--report", "dart-canary.json"])

    assert exc_info.value.code == 2
    assert len(written) == 1
    report = written[0]
    assert report["status"] == "failed"
    assert report["dry_run"] is True
    assert report["error_kind"] == "dart_quota_exhausted"
    assert report["requests_used"] == 0
