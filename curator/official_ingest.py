from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable
from zoneinfo import ZoneInfo

from .config import load_config
from .governance import stable_id
from .official_sources import (
    DartConnector,
    KindConnector,
    OfficialDisclosure,
    disclosure_payloads,
    parse_dart_disclosure,
    parse_kind_disclosure,
)
from .remote_api import post_remote_action, remote_api_configured


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, "").strip().casefold() in {"1", "true", "yes", "on"}


def _enabled_env(name: str, *, default: bool = True) -> bool:
    raw = os.environ.get(name)
    return default if raw is None else raw.strip().casefold() in {"1", "true", "yes", "on"}


def _date_env(name: str, default: date) -> date:
    value = os.environ.get(name, "").strip()
    return date.fromisoformat(value) if value else default


def source_right_payloads(config: dict[str, object], *, include_kind: bool) -> list[dict[str, object]]:
    records: list[dict[str, object]] = [
        {
            "source_right_id": "official:dart",
            "source_type": "official_disclosure",
            "source_key": "dart",
            "source_name": "OpenDART",
            "permission_scope": "public-disclosure metadata and source links",
            "evidence_uri": "https://opendart.fss.or.kr/guide/main.do?apiGrpCd=DS001",
            "valid_from": "2015-01-01T00:00:00+00:00",
            "ai_allowed": True,
            "redistribution_allowed": True,
            "status": "active",
        }
    ]
    if include_kind:
        records.append(
            {
                "source_right_id": "official:kind",
                "source_type": "official_disclosure",
                "source_key": "kind",
                "source_name": "KRX KIND",
                "permission_scope": "public-disclosure metadata and source links",
                "evidence_uri": "https://kind.krx.co.kr/",
                "valid_from": "2021-01-01T00:00:00+00:00",
                "ai_allowed": True,
                "redistribution_allowed": True,
                "status": "active",
            }
        )
    # Telegram and other licensed-source rights are operational records managed
    # through the authenticated SourceRight API.  An official-disclosure run
    # must never overwrite an editor-approved right with repository defaults.
    return records


def _dedupe_disclosures(disclosures: Iterable[OfficialDisclosure]) -> list[OfficialDisclosure]:
    by_id: dict[str, OfficialDisclosure] = {}
    for disclosure in disclosures:
        previous = by_id.get(disclosure.document_id)
        if previous is not None and previous != disclosure:
            raise ValueError(
                f"conflicting official rows share document ID {disclosure.document_id}"
            )
        by_id[disclosure.document_id] = disclosure
    return sorted(by_id.values(), key=lambda item: (item.received_at, item.document_id))


def _chunks(records: list[dict[str, object]], size: int = 1800) -> Iterable[list[dict[str, object]]]:
    for index in range(0, len(records), size):
        yield records[index : index + size]


def _payload_records(payload: dict[str, object], key: str) -> list[dict[str, object]]:
    value = payload.get(key)
    if not isinstance(value, list):
        return []
    return [row for row in value if isinstance(row, dict)]


def _int_value(value: object, default: int = 0) -> int:
    try:
        return int(str(value)) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default


def _event_document_ids(event: dict[str, object]) -> set[str]:
    values = event.get("document_ids")
    if not isinstance(values, list):
        return set()
    return {str(value) for value in values if value}


def sync_governance_payload(payload: dict[str, object], *, run: dict[str, object]) -> dict[str, int]:
    companies = _payload_records(payload, "companies")
    documents = _payload_records(payload, "documents")
    events = _payload_records(payload, "events")
    rights = _payload_records(payload, "source_rights")
    if not remote_api_configured():
        return {
            "official_remote_synced": 0,
            "official_remote_failed": 0,
            "official_remote_skipped": 1,
            "official_remote_batches_attempted": 0,
            "official_remote_run_persisted": 0,
        }

    # Event/document chunks stay aligned by document_id. Company master-only
    # chunks are sent first so foreign keys are available for later batches.
    company_by_id = {str(row.get("company_id") or ""): row for row in companies}
    document_chunks = list(_chunks(documents)) or [[]]
    synced = failed = attempted = 0
    covered_companies: set[str] = set()
    for index, document_chunk in enumerate(document_chunks):
        document_ids = {str(row.get("document_id") or "") for row in document_chunk}
        event_chunk = [
            row
            for row in events
            if document_ids & _event_document_ids(row)
        ]
        company_ids = {
            str(row.get("company_id") or "")
            for row in [*document_chunk, *event_chunk]
            if row.get("company_id")
        }
        covered_companies.update(company_ids)
        attempted += 1
        try:
            response = post_remote_action(
                "upsert_governance_snapshot",
                {
                    "companies": [
                        company_by_id[company_id]
                        for company_id in sorted(company_ids)
                        if company_id in company_by_id
                    ],
                    "documents": document_chunk,
                    "events": event_chunk,
                    "source_rights": rights if index == 0 else [],
                    # The collection run is written only after every data chunk
                    # has returned, so an early partial failure cannot be hidden
                    # by a successful final data chunk.
                    "run": {},
                },
                timeout=45.0,
            )
        except Exception:  # noqa: BLE001 - continue so the final failed run can be persisted.
            response = {"ok": False}
        if response.get("ok"):
            synced += 1
        else:
            failed += 1

    remaining = [row for company_id, row in company_by_id.items() if company_id not in covered_companies]
    for company_chunk in _chunks(remaining):
        attempted += 1
        try:
            response = post_remote_action(
                "upsert_governance_snapshot",
                {"companies": company_chunk, "documents": [], "events": [], "source_rights": [], "run": {}},
                timeout=45.0,
            )
        except Exception:  # noqa: BLE001 - continue so the final failed run can be persisted.
            response = {"ok": False}
        if response.get("ok"):
            synced += 1
        else:
            failed += 1

    final_run = dict(run)
    initial_error_count = _int_value(final_run.get("error_count"))
    initial_status = str(final_run.get("status") or "succeeded").strip().casefold()
    final_run["status"] = "failed" if failed or initial_status not in {"success", "succeeded"} else "succeeded"
    final_run["error_count"] = initial_error_count + failed
    final_run["remote_data_batches_attempted"] = attempted
    final_run["remote_data_batches_succeeded"] = synced
    final_run["remote_data_batches_failed"] = failed
    run_persisted = 0
    try:
        run_response = post_remote_action(
            "upsert_governance_snapshot",
            {"companies": [], "documents": [], "events": [], "source_rights": [], "run": final_run},
            timeout=45.0,
        )
    except Exception:  # noqa: BLE001 - the caller must fail when final status cannot be persisted.
        run_response = {"ok": False}
    if run_response.get("ok"):
        run_persisted = 1
    else:
        failed += 1
    return {
        "official_remote_synced": synced,
        "official_remote_failed": failed,
        "official_remote_skipped": 0,
        "official_remote_batches_attempted": attempted,
        "official_remote_run_persisted": run_persisted,
    }


def run(
    root: Path | None = None,
    *,
    now: datetime | None = None,
    start: date | None = None,
    end: date | None = None,
    settings_overrides: dict[str, object] | None = None,
    dry_run: bool = False,
    idempotency_key: str | None = None,
) -> dict[str, int]:
    """Collect and normalize one inclusive official-disclosure date window.

    ``start``/``end`` and ``settings_overrides`` are intentionally optional so
    scheduled ingestion keeps its existing environment/config contract.  The
    official backfill runner supplies them explicitly to make every chunk
    reproducible.  ``dry_run`` performs source reads and normalization but never
    calls the remote MySQL API.
    """
    project_root = root or PROJECT_ROOT
    config = load_config(project_root / "config.yaml")
    configured_settings = config.get("official_ingest", {})
    settings = dict(configured_settings) if isinstance(configured_settings, dict) else {}
    if settings_overrides:
        settings.update(settings_overrides)
    collection_started_at = datetime.now(timezone.utc)
    current = now or collection_started_at
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    else:
        current = current.astimezone(timezone.utc)
    current_kst_date = current.astimezone(ZoneInfo("Asia/Seoul")).date()
    window_end = end or _date_env("OFFICIAL_INGEST_END", current_kst_date)
    lookback = max(0, int(settings.get("lookback_days", 2)))
    window_start = start or _date_env("OFFICIAL_INGEST_START", window_end - timedelta(days=lookback))
    if window_start > window_end:
        raise ValueError("OFFICIAL_INGEST_START must not be after OFFICIAL_INGEST_END")

    api_key = os.environ.get("DART_API_KEY", "").strip()
    dart_enabled = bool(settings.get("dart_enabled", True))
    page_count = min(100, max(1, int(settings.get("page_count", 100))))
    max_pages = max(1, int(settings.get("max_pages", 100)))
    kind_endpoint = os.environ.get("KIND_DISCLOSURE_ENDPOINT", "").strip()
    kind_requested = bool(settings.get("kind_enabled", True))
    kind_selected = kind_requested and _enabled_env("CURATOR_ENABLE_KIND")
    require_kind = _truthy_env("CURATOR_REQUIRE_KIND")
    kind_enabled = kind_selected and bool(kind_endpoint)
    source_fetched = {"dart": 0, "kind": 0}
    source_rejected = {"dart": 0, "kind": 0}
    source_duplicates = {"dart": 0, "kind": 0}
    source_discarded = {"dart": 0, "kind": 0}
    source_errors = {
        "dart": int(dart_enabled and not api_key),
        "kind": int(require_kind and not kind_enabled),
    }
    source_failure_kinds: dict[str, dict[str, int]] = {
        "dart": {"configuration": source_errors["dart"], "connector": 0, "parse": 0, "conflict": 0},
        "kind": {"configuration": source_errors["kind"], "connector": 0, "parse": 0, "conflict": 0},
    }
    source_metrics: dict[str, dict[str, int]] = {
        "dart": {"list_requests": 0, "pages_fetched": 0, "rows_fetched": 0, "elapsed_ms": 0},
        "kind": {"list_requests": 0, "pages_fetched": 0, "rows_fetched": 0, "elapsed_ms": 0},
    }
    raw_fetched = 0
    disclosures: list[OfficialDisclosure] = []
    company_master: list[dict[str, object]] = []
    if api_key and dart_enabled:
        dart_connector = DartConnector(api_key)
        source_started = time.perf_counter()
        source_buffer: list[OfficialDisclosure] = []
        try:
            for row in dart_connector.iter_disclosure_rows(
                window_start,
                window_end,
                page_count=page_count,
                max_pages=max_pages,
            ):
                raw_fetched += 1
                source_fetched["dart"] += 1
                try:
                    disclosure = parse_dart_disclosure(row)
                except (TypeError, ValueError):
                    source_errors["dart"] += 1
                    source_failure_kinds["dart"]["parse"] += 1
                    continue
                if disclosure is not None:
                    source_buffer.append(disclosure)
                else:
                    source_rejected["dart"] += 1
            if source_errors["dart"] == 0 and (
                _truthy_env("DART_SYNC_COMPANY_MASTER")
                or bool(settings.get("sync_company_master", False))
            ):
                company_master = list(dart_connector.fetch_company_master())
        except Exception:  # noqa: BLE001 - the source outcome records the failed contract.
            source_errors["dart"] += 1
            source_failure_kinds["dart"]["connector"] += 1
        finally:
            source_metrics["dart"] = {
                "list_requests": dart_connector.list_requests,
                "pages_fetched": dart_connector.pages_fetched,
                "rows_fetched": dart_connector.rows_fetched,
                "elapsed_ms": max(0, round((time.perf_counter() - source_started) * 1000)),
            }
        if source_errors["dart"]:
            source_discarded["dart"] = len(source_buffer)
            company_master = []
        else:
            try:
                normalized_source = _dedupe_disclosures(source_buffer)
            except ValueError:
                source_errors["dart"] += 1
                source_failure_kinds["dart"]["conflict"] += 1
                source_discarded["dart"] = len(source_buffer)
            else:
                source_duplicates["dart"] = len(source_buffer) - len(normalized_source)
                disclosures.extend(normalized_source)

    if kind_enabled:
        kind_connector = KindConnector(kind_endpoint, api_key=os.environ.get("KIND_API_KEY", ""))
        source_started = time.perf_counter()
        source_buffer = []
        try:
            for row in kind_connector.iter_disclosure_rows(
                window_start,
                window_end,
                page_count=page_count,
                max_pages=max_pages,
            ):
                raw_fetched += 1
                source_fetched["kind"] += 1
                try:
                    disclosure = parse_kind_disclosure(row)
                except (TypeError, ValueError):
                    source_errors["kind"] += 1
                    source_failure_kinds["kind"]["parse"] += 1
                    continue
                if disclosure is not None:
                    source_buffer.append(disclosure)
                else:
                    source_rejected["kind"] += 1
        except Exception:  # noqa: BLE001 - the source outcome records the failed contract.
            source_errors["kind"] += 1
            source_failure_kinds["kind"]["connector"] += 1
        finally:
            source_metrics["kind"] = {
                "list_requests": kind_connector.list_requests,
                "pages_fetched": kind_connector.pages_fetched,
                "rows_fetched": kind_connector.rows_fetched,
                "elapsed_ms": max(0, round((time.perf_counter() - source_started) * 1000)),
            }
        if source_errors["kind"]:
            source_discarded["kind"] = len(source_buffer)
        else:
            try:
                normalized_source = _dedupe_disclosures(source_buffer)
            except ValueError:
                source_errors["kind"] += 1
                source_failure_kinds["kind"]["conflict"] += 1
                source_discarded["kind"] = len(source_buffer)
            else:
                source_duplicates["kind"] = len(source_buffer) - len(normalized_source)
                disclosures.extend(normalized_source)

    normalized = _dedupe_disclosures(disclosures)
    errors = sum(source_errors.values())
    source_accepted = {
        source: sum(1 for disclosure in normalized if disclosure.source.casefold() == source)
        for source in ("dart", "kind")
    }
    source_outcomes = {
        "dart": {
            "enabled": dart_enabled,
            "configured": bool(api_key),
            "fetched": source_fetched["dart"],
            "accepted": source_accepted["dart"],
            "rejected_non_governance": source_rejected["dart"],
            "duplicate_count": source_duplicates["dart"],
            "discarded_valid_count": source_discarded["dart"],
            "error_count": source_errors["dart"],
            "failure_kinds": source_failure_kinds["dart"],
            **source_metrics["dart"],
            "status": "failed" if source_errors["dart"] else ("succeeded" if dart_enabled else "disabled"),
        },
        "kind": {
            "requested": kind_requested,
            "enabled": kind_selected,
            "required": require_kind,
            "configured": bool(kind_endpoint),
            "fetched": source_fetched["kind"],
            "accepted": source_accepted["kind"],
            "rejected_non_governance": source_rejected["kind"],
            "duplicate_count": source_duplicates["kind"],
            "discarded_valid_count": source_discarded["kind"],
            "error_count": source_errors["kind"],
            "failure_kinds": source_failure_kinds["kind"],
            **source_metrics["kind"],
            "status": (
                "failed"
                if source_errors["kind"]
                else ("succeeded" if kind_enabled else "disabled")
            ),
        },
    }
    generated_payload = disclosure_payloads(normalized, retrieved_at=current)
    payload: dict[str, object] = {key: value for key, value in generated_payload.items()}
    if company_master:
        by_id = {
            str(row.get("company_id") or ""): row
            for row in _payload_records(payload, "companies")
        }
        for company in company_master:
            by_id[str(company.get("company_id") or "")] = company
        payload["companies"] = list(by_id.values())
    rights = source_right_payloads(config, include_kind=kind_enabled)
    payload["source_rights"] = rights
    run_id = stable_id(
        "run",
        "ingest-official",
        window_start.isoformat(),
        window_end.isoformat(),
        idempotency_key or current.isoformat(),
        length=32,
    )
    run_record: dict[str, object] = {
        "run_id": run_id,
        "pipeline": "ingest-official",
        "source_key": "+".join(
            source
            for source, enabled in (("dart", dart_enabled), ("kind", kind_selected or require_kind))
            if enabled
        ) or "none",
        "status": "succeeded" if errors == 0 else "failed",
        "started_at": collection_started_at.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "fetched_count": raw_fetched,
        "resolved_count": len(normalized),
        "accepted_count": len(payload["events"]),  # type: ignore[arg-type]
        "error_count": errors,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "idempotency_key": idempotency_key,
        # list.json exposes only a receipt date, not a stable receipt time.
        # Do not publish a fabricated p95 collection lag from midnight.
        "lag_seconds_p95": None,
        "source_outcomes": source_outcomes,
        "metrics": {
            "source_outcomes": source_outcomes,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "retrieved_at": current.isoformat(),
        },
    }
    remote_summary = (
        {
            "official_remote_synced": 0,
            "official_remote_failed": 0,
            "official_remote_skipped": 1,
            "official_remote_batches_attempted": 0,
            "official_remote_run_persisted": 0,
        }
        if dry_run
        else sync_governance_payload(payload, run=run_record)
    )
    require_remote = _truthy_env("CURATOR_REQUIRE_REMOTE_API")
    remote_failed = int(remote_summary.get("official_remote_failed") or 0)
    remote_skipped = int(remote_summary.get("official_remote_skipped") or 0)
    failed = errors + remote_failed + (1 if require_remote and remote_skipped and not dry_run else 0)
    return {
        "official_fetched": raw_fetched,
        "official_documents": len(payload["documents"]),  # type: ignore[arg-type]
        "official_events": len(payload["events"]),  # type: ignore[arg-type]
        "official_companies": len(payload["companies"]),  # type: ignore[arg-type]
        "official_source_rights": len(rights),
        "official_failed": failed,
        "official_skipped": 0,
        "official_dry_run": int(dry_run),
        "official_dart_fetched": source_fetched["dart"],
        "official_dart_accepted": source_accepted["dart"],
        "official_dart_rejected": source_rejected["dart"],
        "official_dart_duplicates": source_duplicates["dart"],
        "official_dart_discarded": source_discarded["dart"],
        "official_dart_pages": source_metrics["dart"]["pages_fetched"],
        "official_dart_errors": source_errors["dart"],
        "official_kind_required": int(require_kind),
        "official_kind_enabled": int(kind_selected),
        "official_kind_configured": int(bool(kind_endpoint)),
        "official_kind_fetched": source_fetched["kind"],
        "official_kind_accepted": source_accepted["kind"],
        "official_kind_rejected": source_rejected["kind"],
        "official_kind_duplicates": source_duplicates["kind"],
        "official_kind_discarded": source_discarded["kind"],
        "official_kind_pages": source_metrics["kind"]["pages_fetched"],
        "official_kind_errors": source_errors["kind"],
        **remote_summary,
    }


def main() -> None:
    summary = run()
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
    if int(summary.get("official_failed") or 0):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
