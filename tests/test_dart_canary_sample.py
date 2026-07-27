from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from curator import dart_canary_sample
from curator.dart_canary_sample import (
    DartCanarySampleOptions,
    MAX_DART_REQUEST_BUDGET,
    run_dart_canary_sample,
)
from curator.dart_quota import DART_DAILY_LIMIT
from curator.official_sources import (
    DartQuotaExceededError,
    DartRequestBudget,
    DartRequestBudgetError,
    DartResultTruncatedError,
    OfficialSourceError,
)
from curator.opendart_credentials import load_opendart_credentials


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
        self.close_calls = 0

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

    def close(self) -> None:
        self.close_calls += 1


class AdaptiveConnector:
    def __init__(
        self,
        budget: DartRequestBudget,
        *,
        truncated: dict[tuple[date, date], str] | None = None,
        failures: dict[tuple[date, date], OfficialSourceError] | None = None,
    ) -> None:
        self.budget = budget
        self.truncated = truncated or {}
        self.failures = failures or {}
        self.calls: list[tuple[date, date, int, int]] = []
        self.successful_windows: list[tuple[date, date]] = []
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
        self.budget.consume()
        self.calls.append((start, end, page_count, max_pages))
        self.requests_made += 1
        window = (start, end)
        if window in self.failures:
            raise self.failures[window]
        if window in self.truncated:
            raise DartResultTruncatedError(
                start=start,
                end=end,
                detected_at_page=1,
                total_pages=max_pages + 6,
                page_limit=max_pages,
                detail_code=self.truncated[window],
            )
        self.successful_windows.append(window)
        self.pages_fetched += 1
        cursor = start
        while cursor <= end:
            self.rows_fetched += 1
            yield dart_row(
                receipt_no=f"{cursor:%Y%m%d}000001",
                received_date=cursor.strftime("%Y%m%d"),
                title="Shareholder Proposal",
            )
            cursor += timedelta(days=1)


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
    assert connector.close_calls == 1


def test_canary_adaptively_splits_a_truncated_seven_day_window() -> None:
    whole = (date(2026, 7, 1), date(2026, 7, 7))
    budget = DartRequestBudget(10)
    connector = AdaptiveConnector(budget, truncated={whole: ""})
    metrics = dart_canary_sample._WindowScanMetrics()

    fetched, disclosures = dart_canary_sample._collect_window(
        connector,
        *whole,
        page_count=100,
        max_pages=100,
        metrics=metrics,
    )

    assert connector.calls == [
        (*whole, 100, 100),
        (date(2026, 7, 1), date(2026, 7, 4), 100, 100),
        (date(2026, 7, 5), date(2026, 7, 7), 100, 100),
    ]
    expected_days = [f"202607{day:02d}" for day in range(1, 8)]
    assert fetched == 7
    assert [row.receipt_no[:8] for row in disclosures] == expected_days
    assert len({row.receipt_no for row in disclosures}) == 7
    assert budget.used == connector.requests_made == 3
    assert metrics.attempted_windows == 3
    assert metrics.completed_leaf_windows == 2
    assert metrics.split_count == 1


def test_canary_truncation_split_recurses_without_leaf_gaps_or_overlaps() -> None:
    whole = (date(2026, 7, 1), date(2026, 7, 7))
    left = (date(2026, 7, 1), date(2026, 7, 4))
    budget = DartRequestBudget(10)
    connector = AdaptiveConnector(
        budget,
        truncated={
            whole: "",
            left: "",
        },
    )
    metrics = dart_canary_sample._WindowScanMetrics()

    fetched, disclosures = dart_canary_sample._collect_window(
        connector,
        *whole,
        page_count=100,
        max_pages=100,
        metrics=metrics,
    )

    assert connector.calls == [
        (*whole, 100, 100),
        (*left, 100, 100),
        (date(2026, 7, 1), date(2026, 7, 2), 100, 100),
        (date(2026, 7, 3), date(2026, 7, 4), 100, 100),
        (date(2026, 7, 5), date(2026, 7, 7), 100, 100),
    ]
    leaf_days: list[date] = []
    for start, end in connector.successful_windows:
        cursor = start
        while cursor <= end:
            leaf_days.append(cursor)
            cursor += timedelta(days=1)
    expected_days = [date(2026, 7, day) for day in range(1, 8)]
    assert leaf_days == expected_days
    assert len(leaf_days) == len(set(leaf_days))
    assert fetched == 7
    assert [row.receipt_no[:8] for row in disclosures] == [
        day.strftime("%Y%m%d") for day in expected_days
    ]
    assert len({row.receipt_no for row in disclosures}) == 7
    assert budget.used == connector.requests_made == 5
    assert metrics.attempted_windows == 5
    assert metrics.completed_leaf_windows == 3
    assert metrics.split_count == 2


def test_canary_adaptively_splits_detail_scope_truncation() -> None:
    whole = (date(2026, 7, 1), date(2026, 7, 2))
    budget = DartRequestBudget(10)
    connector = AdaptiveConnector(budget, truncated={whole: "D001"})

    fetched, disclosures = dart_canary_sample._collect_window(
        connector,
        *whole,
        page_count=100,
        max_pages=100,
    )

    assert connector.calls == [
        (*whole, 100, 100),
        (date(2026, 7, 1), date(2026, 7, 1), 100, 100),
        (date(2026, 7, 2), date(2026, 7, 2), 100, 100),
    ]
    assert fetched == 2
    assert [row.receipt_no[:8] for row in disclosures] == [
        "20260701",
        "20260702",
    ]
    assert budget.used == connector.requests_made == 3


def test_canary_one_day_truncation_is_a_typed_terminal_failure() -> None:
    target = date(2026, 7, 7)
    budget = DartRequestBudget(10)
    connector = AdaptiveConnector(
        budget,
        truncated={(target, target): "D001"},
    )

    with pytest.raises(
        DartResultTruncatedError,
        match=r"page 100 of 106; one-day window 2026-07-07",
    ) as captured:
        dart_canary_sample._collect_window(
            connector,
            target,
            target,
            page_count=100,
            max_pages=100,
        )

    error = captured.value
    assert error.scope == "detail"
    assert error.detail_code == "D001"
    assert error.window_start == error.window_end == target
    assert error.detected_at_page == error.current_page == error.page == 1
    assert error.page_limit == 100
    assert error.total_pages == 106
    assert error.terminal_one_day is True
    assert budget.used == connector.requests_made == 1


def test_canary_adaptive_split_never_bypasses_shared_request_budget() -> None:
    whole = (date(2026, 7, 1), date(2026, 7, 7))
    budget = DartRequestBudget(2)
    connector = AdaptiveConnector(budget, truncated={whole: ""})

    with pytest.raises(DartRequestBudgetError, match=r"2/2"):
        dart_canary_sample._collect_window(
            connector,
            *whole,
            page_count=100,
            max_pages=100,
        )

    assert connector.calls == [
        (*whole, 100, 100),
        (date(2026, 7, 1), date(2026, 7, 4), 100, 100),
    ]
    assert budget.used == connector.requests_made == 2


def test_canary_does_not_split_other_official_source_failures() -> None:
    whole = (date(2026, 7, 1), date(2026, 7, 7))
    budget = DartRequestBudget(10)
    connector = AdaptiveConnector(
        budget,
        failures={whole: OfficialSourceError("provider contract failed")},
    )

    with pytest.raises(OfficialSourceError, match="provider contract failed"):
        dart_canary_sample._collect_window(
            connector,
            *whole,
            page_count=100,
            max_pages=100,
        )

    assert connector.calls == [(*whole, 100, 100)]
    assert budget.used == connector.requests_made == 1


def test_canary_report_exposes_actual_adaptive_window_counts() -> None:
    recent_day = date(2026, 7, 15)
    older_window = (date(2026, 7, 8), date(2026, 7, 14))
    holder: dict[str, AdaptiveConnector] = {}

    def factory(_key: object, budget: DartRequestBudget) -> AdaptiveConnector:
        connector = AdaptiveConnector(budget, truncated={older_window: ""})
        holder["connector"] = connector
        return connector

    report = run_dart_canary_sample(
        "test-key",
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        options=DartCanarySampleOptions(lookback_days=8, scan_chunk_days=7),
        connector_factory=factory,
    )

    history = report["history"]
    assert isinstance(history, dict)
    assert history["planned_windows"] == 2
    assert history["attempted_windows"] == 4
    assert history["completed_leaf_windows"] == 3
    assert history["windows_scanned"] == 3
    assert history["split_count"] == 1
    assert report["requests_used"] == 4
    assert holder["connector"].calls[0][:2] == (recent_day, recent_day)


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
    holder: dict[str, FakeConnector] = {}

    def factory(_key: object, shared: DartRequestBudget) -> FakeConnector:
        connector = FakeConnector(
            shared,
            [],
            quota_failure=True,
        )
        holder["connector"] = connector
        return connector

    with pytest.raises(DartQuotaExceededError, match="020"):
        run_dart_canary_sample(
            "test-key",
            now=datetime(2026, 7, 16, tzinfo=timezone.utc),
            options=DartCanarySampleOptions(lookback_days=10),
            request_budget=budget,
            connector_factory=factory,
        )
    assert budget.used == 1
    assert holder["connector"].close_calls == 1


def test_canary_rejects_a_budget_over_the_single_run_safety_cap() -> None:
    with pytest.raises(ValueError, match="cannot exceed 10000"):
        run_dart_canary_sample(
            "test-key",
            request_budget=DartRequestBudget(10_001),
            connector_factory=lambda _key, budget: FakeConnector(budget, []),
        )


def test_single_run_safety_budget_is_distinct_from_durable_daily_pool() -> None:
    assert MAX_DART_REQUEST_BUDGET == 10_000
    assert DART_DAILY_LIMIT == 40_000


def test_canary_layers_10k_invocation_cap_over_40k_durable_budget() -> None:
    observed_limits: list[int] = []

    class DurableBudget:
        limit = DART_DAILY_LIMIT
        used = 0

        def consume(self, *, operation: str, credential_id: str) -> object:
            self.used += 1
            return (operation, credential_id)

        def block_020(self, permit: object) -> None:
            del permit

        def disable_901(self, permit: object) -> None:
            del permit

    class EmptyConnector:
        requests_made = 0
        pages_fetched = 0
        rows_fetched = 0

        def iter_disclosure_rows(
            self,
            *_args: object,
            **_kwargs: object,
        ):  # type: ignore[no-untyped-def]
            return iter(())

    def factory(_key: object, budget: object) -> EmptyConnector:
        observed_limits.append(budget.limit)  # type: ignore[attr-defined]
        return EmptyConnector()

    report = run_dart_canary_sample(
        "a" * 40,
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        options=DartCanarySampleOptions(lookback_days=2),
        request_budget=DurableBudget(),
        connector_factory=factory,  # type: ignore[arg-type]
    )

    assert report["status"] == "failed"
    assert observed_limits == [MAX_DART_REQUEST_BUDGET]


def test_canary_accepts_validated_pool_as_one_shared_connector_input() -> None:
    credentials = load_opendart_credentials(
        {"OPENDART_API_KEYS": f"{'a' * 40}\r\n{'b' * 40},{'c' * 40}"}
    )
    observed_ids: list[str] = []

    def factory(pool: object, budget: DartRequestBudget) -> FakeConnector:
        observed_ids.extend(
            credential.credential_id for credential in tuple(pool)  # type: ignore[arg-type]
        )
        return FakeConnector(budget, [])

    report = run_dart_canary_sample(
        credentials,
        now=datetime(2026, 7, 16, tzinfo=timezone.utc),
        options=DartCanarySampleOptions(lookback_days=2),
        connector_factory=factory,
    )

    assert report["status"] == "failed"
    assert len(observed_ids) == 3


def test_cli_preserves_status_020_as_fail_closed_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    written: list[dict[str, object]] = []
    monkeypatch.setenv("DART_API_KEY", "a" * 40)

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


def test_cli_preserves_terminal_truncation_metadata_in_failure_artifact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    written: list[dict[str, object]] = []
    monkeypatch.setenv("DART_API_KEY", "a" * 40)

    def truncation(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise DartResultTruncatedError(
            start=date(2026, 7, 7),
            end=date(2026, 7, 7),
            detected_at_page=1,
            total_pages=106,
            page_limit=100,
            detail_code="D001",
            terminal_one_day=True,
        )

    monkeypatch.setattr(dart_canary_sample, "run_dart_canary_sample", truncation)
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
    assert report["error_kind"] == "DartResultTruncatedError"
    assert report["truncation"] == {
        "scope": "detail",
        "detail_code": "D001",
        "window_start": "2026-07-07",
        "window_end": "2026-07-07",
        "detected_at_page": 1,
        "page_limit": 100,
        "total_pages": 106,
        "terminal_one_day": True,
    }
