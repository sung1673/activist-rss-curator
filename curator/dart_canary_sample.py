from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator, Protocol
from zoneinfo import ZoneInfo

from .dart_quota import (
    DartQuotaLedgerError,
    durable_dart_quota_client,
    durable_dart_quota_configured,
    durable_dart_quota_required,
)
from .official_sources import (
    DartConnector,
    DartInvocationQuota,
    DartQuotaExceededError,
    DartRequestBudget,
    DartRequestBudgetError,
    DartRequestQuota,
    OfficialDisclosure,
    OfficialSourceError,
    disclosure_payloads,
    parse_dart_disclosure,
)
from .opendart_credentials import (
    OpenDartCredential,
    OpenDartCredentialConfigurationError,
    load_opendart_credentials,
)


KST = ZoneInfo("Asia/Seoul")
# This is a single-run blast-radius limit. The authoritative credential-pool
# ledger separately allows up to 40,000 physical requests per KST day.
MAX_DART_REQUEST_BUDGET = 10_000


class DartCanarySampleError(RuntimeError):
    """The DART canary could not produce the required real-data samples."""


class CanaryConnector(Protocol):
    requests_made: int
    pages_fetched: int
    rows_fetched: int

    def iter_disclosure_rows(
        self,
        start: date,
        end: date,
        *,
        page_count: int = 100,
        max_pages: int = 100,
    ) -> Iterator[dict[str, object]]: ...


DartCredentialInput = str | tuple[OpenDartCredential, ...]
ConnectorFactory = Callable[[DartCredentialInput, DartRequestQuota], CanaryConnector]


def _close_connector(connector: object) -> None:
    close = getattr(connector, "close", None)
    if callable(close):
        close()


@dataclass(frozen=True)
class DartCanarySampleOptions:
    lookback_days: int = 365
    scan_chunk_days: int = 7
    sample_limit_per_kind: int = 5
    page_count: int = 100
    max_pages: int = 100

    def validate(self) -> None:
        if self.lookback_days < 2:
            raise ValueError("lookback_days must be at least 2")
        if self.scan_chunk_days < 1:
            raise ValueError("scan_chunk_days must be at least 1")
        if self.sample_limit_per_kind < 1:
            raise ValueError("sample_limit_per_kind must be at least 1")
        if not 1 <= self.page_count <= 100:
            raise ValueError("page_count must be between 1 and 100")
        if self.max_pages < 1:
            raise ValueError("max_pages must be at least 1")


def _default_connector_factory(
    api_key: DartCredentialInput,
    request_budget: DartRequestQuota,
) -> CanaryConnector:
    return DartConnector(api_key, request_budget=request_budget)


def _completed_kst_day(now: datetime) -> date:
    current = now if now.tzinfo is not None else now.replace(tzinfo=timezone.utc)
    return current.astimezone(KST).date() - timedelta(days=1)


def _inclusive_windows(start: date, end: date, chunk_days: int) -> list[tuple[date, date]]:
    if end < start:
        return []
    windows: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        window_end = min(cursor + timedelta(days=chunk_days - 1), end)
        windows.append((cursor, window_end))
        cursor = window_end + timedelta(days=1)
    return windows


def _collect_window(
    connector: CanaryConnector,
    start: date,
    end: date,
    *,
    page_count: int,
    max_pages: int,
) -> tuple[int, list[OfficialDisclosure]]:
    fetched = 0
    disclosures: list[OfficialDisclosure] = []
    for row in connector.iter_disclosure_rows(
        start,
        end,
        page_count=page_count,
        max_pages=max_pages,
    ):
        fetched += 1
        try:
            disclosure = parse_dart_disclosure(row)
        except (TypeError, ValueError) as exc:
            receipt_no = str(row.get("rcept_no") or "").strip()
            raise DartCanarySampleError(
                f"DART canary could not normalize receipt {receipt_no or '<missing>'}"
            ) from exc
        if disclosure is not None:
            disclosures.append(disclosure)
    return fetched, disclosures


def _dedupe(disclosures: Iterable[OfficialDisclosure]) -> list[OfficialDisclosure]:
    by_document_id: dict[str, OfficialDisclosure] = {}
    for disclosure in disclosures:
        previous = by_document_id.get(disclosure.document_id)
        if previous is not None and previous != disclosure:
            raise DartCanarySampleError(
                f"conflicting DART canary rows share {disclosure.document_id}"
            )
        by_document_id[disclosure.document_id] = disclosure
    return sorted(
        by_document_id.values(),
        key=lambda item: (item.received_at, item.receipt_no),
    )


def _most_recent(
    disclosures: Iterable[OfficialDisclosure],
    *,
    predicate: Callable[[OfficialDisclosure], bool],
    limit: int,
) -> list[OfficialDisclosure]:
    matching = [row for row in disclosures if predicate(row)]
    matching.sort(key=lambda row: (row.received_at, row.receipt_no), reverse=True)
    return matching[:limit]


def _sample_row(
    disclosure: OfficialDisclosure,
    *,
    document: dict[str, object],
) -> dict[str, object]:
    stored_title = str(document.get("title") or "")
    if stored_title != disclosure.title:
        raise DartCanarySampleError(
            f"DART canary title changed while normalizing {disclosure.receipt_no}"
        )
    return {
        "receipt_no": disclosure.receipt_no,
        "corp_code": disclosure.corp_code,
        "corp_name": disclosure.corp_name,
        "received_at": disclosure.received_at,
        "title": disclosure.title,
        "title_sha256": hashlib.sha256(disclosure.title.encode("utf-8")).hexdigest(),
        "original_url": disclosure.original_url,
        "event_type": disclosure.event_type.value,
        "is_correction": disclosure.is_correction,
        "is_withdrawn": disclosure.is_cancelled,
        "publication_status": str(document.get("publication_status") or ""),
        "original_language": str(document.get("original_language") or ""),
        "title_preserved": True,
    }


def _run_dart_canary_sample(
    api_key: DartCredentialInput,
    *,
    now: datetime | None = None,
    options: DartCanarySampleOptions | None = None,
    request_budget: DartRequestQuota | None = None,
    connector_factory: ConnectorFactory = _default_connector_factory,
) -> dict[str, object]:
    """Dry-run the last complete day and select real revision samples.

    The history scan covers exactly ``lookback_days`` complete KST dates,
    including the recent-day canary. It never writes to MySQL or any remote API.
    All OpenDART calls share one bounded request budget.
    """

    if isinstance(api_key, str):
        selected_credentials: DartCredentialInput = api_key.strip()
        if not selected_credentials:
            raise ValueError("OpenDART credentials are required")
    else:
        selected_credentials = tuple(api_key)
        if not selected_credentials:
            raise ValueError("OpenDART credentials are required")
    selected_options = options or DartCanarySampleOptions()
    selected_options.validate()
    supplied_budget = request_budget or DartRequestBudget(MAX_DART_REQUEST_BUDGET)
    if (
        isinstance(supplied_budget, DartRequestBudget)
        and supplied_budget.limit > MAX_DART_REQUEST_BUDGET
    ):
        raise ValueError("DART canary request budget cannot exceed 10000")
    budget: DartRequestQuota = (
        DartInvocationQuota(
            supplied_budget,
            limit=MAX_DART_REQUEST_BUDGET,
        )
        if supplied_budget.limit > MAX_DART_REQUEST_BUDGET
        else supplied_budget
    )

    current = now or datetime.now(timezone.utc)
    recent_day = _completed_kst_day(current)
    history_start = recent_day - timedelta(days=selected_options.lookback_days - 1)
    connector = connector_factory(selected_credentials, budget)
    requests_before = budget.used

    recent_fetched, recent_disclosures = _collect_window(
        connector,
        recent_day,
        recent_day,
        page_count=selected_options.page_count,
        max_pages=selected_options.max_pages,
    )
    history_disclosures = list(recent_disclosures)
    history_fetched = recent_fetched
    older_windows = _inclusive_windows(
        history_start,
        recent_day - timedelta(days=1),
        selected_options.scan_chunk_days,
    )
    for window_start, window_end in older_windows:
        fetched, rows = _collect_window(
            connector,
            window_start,
            window_end,
            page_count=selected_options.page_count,
            max_pages=selected_options.max_pages,
        )
        history_fetched += fetched
        history_disclosures.extend(rows)

    recent_normalized = _dedupe(recent_disclosures)
    history_normalized = _dedupe(history_disclosures)
    corrections = _most_recent(
        history_normalized,
        predicate=lambda row: row.is_correction,
        limit=selected_options.sample_limit_per_kind,
    )
    withdrawals = _most_recent(
        history_normalized,
        predicate=lambda row: row.is_cancelled,
        limit=selected_options.sample_limit_per_kind,
    )
    selected = _dedupe([*corrections, *withdrawals])

    # Build the exact production payload shape to validate revision status,
    # identity extraction and original-language/title preservation without a
    # remote write.
    recent_payload = disclosure_payloads(recent_normalized, retrieved_at=current)
    sample_payload = disclosure_payloads(selected, retrieved_at=current)
    documents_by_receipt = {
        str(row.get("external_id") or ""): row
        for row in sample_payload["documents"]
    }
    sample_rows = [
        _sample_row(row, document=documents_by_receipt[row.receipt_no])
        for row in selected
    ]

    missing: list[str] = []
    if not corrections:
        missing.append("correction")
    if not withdrawals:
        missing.append("withdrawal")
    report: dict[str, object] = {
        "schema_version": 1,
        "status": "failed" if missing else "succeeded",
        "dry_run": True,
        "generated_at": current.astimezone(timezone.utc).isoformat(),
        "request_budget": budget.limit,
        "requests_used": budget.used - requests_before,
        "requests_remaining": budget.limit - budget.used,
        "recent_day": {
            "date": recent_day.isoformat(),
            "fetched_count": recent_fetched,
            "governance_document_count": len(recent_payload["documents"]),
            "governance_event_count": len(recent_payload["events"]),
        },
        "history": {
            "from_date": history_start.isoformat(),
            "to_date": recent_day.isoformat(),
            "days": selected_options.lookback_days,
            "windows_scanned": len(older_windows) + 1,
            "fetched_count": history_fetched,
            "governance_document_count": len(history_normalized),
        },
        "samples": {
            "limit_per_kind": selected_options.sample_limit_per_kind,
            "correction_count": len(corrections),
            "withdrawal_count": len(withdrawals),
            "selected_document_count": len(selected),
            "rows": sample_rows,
        },
        "missing_sample_kinds": missing,
        "connector_metrics": {
            "requests_made": int(getattr(connector, "requests_made", 0)),
            "pages_fetched": int(getattr(connector, "pages_fetched", 0)),
            "rows_fetched": int(getattr(connector, "rows_fetched", 0)),
        },
    }
    return report


def run_dart_canary_sample(
    api_key: DartCredentialInput,
    *,
    now: datetime | None = None,
    options: DartCanarySampleOptions | None = None,
    request_budget: DartRequestQuota | None = None,
    connector_factory: ConnectorFactory = _default_connector_factory,
) -> dict[str, object]:
    """Run the bounded dry-run sample and always close its connector."""

    connector: CanaryConnector | None = None

    def managed_factory(
        selected_api_key: DartCredentialInput,
        selected_budget: DartRequestQuota,
    ) -> CanaryConnector:
        nonlocal connector
        if connector is not None:
            raise DartCanarySampleError("DART canary created more than one connector")
        connector = connector_factory(selected_api_key, selected_budget)
        return connector

    try:
        return _run_dart_canary_sample(
            api_key,
            now=now,
            options=options,
            request_budget=request_budget,
            connector_factory=managed_factory,
        )
    finally:
        if connector is not None:
            _close_connector(connector)


def _write_report(path: Path | None, report: dict[str, object]) -> None:
    rendered = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Dry-run the last complete DART day and automatically select "
            "correction/withdrawal samples from the last 365 complete KST days."
        )
    )
    parser.add_argument("--lookback-days", type=int, default=365)
    parser.add_argument("--scan-chunk-days", type=int, default=7)
    parser.add_argument("--sample-limit-per-kind", type=int, default=5)
    parser.add_argument("--page-count", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument("--request-budget", type=int, default=MAX_DART_REQUEST_BUDGET)
    parser.add_argument("--report", type=Path)
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    budget: DartRequestQuota | None = None
    try:
        if not 1 <= args.request_budget <= MAX_DART_REQUEST_BUDGET:
            raise ValueError("request_budget must be between 1 and 10000")
        budget = (
            durable_dart_quota_client(
                phase=os.environ.get("CURATOR_DART_QUOTA_PHASE", "dart-canary")
            )
            if durable_dart_quota_required() or durable_dart_quota_configured()
            else DartRequestBudget(args.request_budget)
        )
        report = run_dart_canary_sample(
            load_opendart_credentials(),
            options=DartCanarySampleOptions(
                lookback_days=args.lookback_days,
                scan_chunk_days=args.scan_chunk_days,
                sample_limit_per_kind=args.sample_limit_per_kind,
                page_count=args.page_count,
                max_pages=args.max_pages,
            ),
            request_budget=budget,
        )
    except (
        DartCanarySampleError,
        OpenDartCredentialConfigurationError,
        DartQuotaLedgerError,
        DartQuotaExceededError,
        DartRequestBudgetError,
        OfficialSourceError,
        OSError,
        ValueError,
    ) as exc:
        error_kind = "dart_quota_exhausted" if isinstance(exc, DartQuotaExceededError) else type(exc).__name__
        report = {
            "schema_version": 1,
            "status": "failed",
            "dry_run": True,
            "error_kind": error_kind,
            "error": str(exc),
            "request_budget": args.request_budget,
            "requests_used": budget.used if budget is not None else 0,
        }
        _write_report(args.report, report)
        raise SystemExit(2) from exc
    finally:
        if budget is not None:
            close = getattr(budget, "close", None)
            if callable(close):
                close()
    _write_report(args.report, report)
    if report["status"] != "succeeded":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
