from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping
from zoneinfo import ZoneInfo

from .config import load_config
from .official_ingest import run as run_official_ingest
from .remote_api import remote_api_configured


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MIN_BACKFILL_DATE = date(2021, 1, 1)
DEFAULT_CHECKPOINT_PATH = Path("data/backfill_official_checkpoint.json")
CHECKPOINT_SCHEMA_VERSION = 1


class BackfillConfigurationError(ValueError):
    pass


class CheckpointError(RuntimeError):
    pass


@dataclass(frozen=True)
class DateWindow:
    """A half-open backfill window: ``start <= date < end_exclusive``."""

    start: date
    end_exclusive: date

    @property
    def key(self) -> str:
        return f"{self.start.isoformat()}:{self.end_exclusive.isoformat()}"

    @property
    def source_end_inclusive(self) -> date:
        return self.end_exclusive - timedelta(days=1)


@dataclass(frozen=True)
class BackfillOptions:
    start: date
    end_exclusive: date
    checkpoint_path: Path
    chunk_days: int = 14
    sources: tuple[str, ...] = ("dart", "kind")
    page_count: int = 100
    max_pages: int = 100
    max_chunks: int = 0
    dry_run: bool = False
    restart: bool = False
    continue_on_error: bool = False
    sync_company_master: bool = False

    def validate(self) -> None:
        if self.start < MIN_BACKFILL_DATE:
            raise BackfillConfigurationError(
                f"official governance backfill starts at {MIN_BACKFILL_DATE.isoformat()} or later"
            )
        if self.end_exclusive <= self.start:
            raise BackfillConfigurationError("end_exclusive must be after start")
        if self.chunk_days < 1:
            raise BackfillConfigurationError("chunk_days must be at least 1")
        if not 1 <= self.page_count <= 100:
            raise BackfillConfigurationError("page_count must be between 1 and 100")
        if self.max_pages < 1:
            raise BackfillConfigurationError("max_pages must be at least 1")
        if self.max_chunks < 0:
            raise BackfillConfigurationError("max_chunks cannot be negative")
        if not self.sources or set(self.sources) - {"dart", "kind"}:
            raise BackfillConfigurationError("sources must contain dart and/or kind")
        if self.sync_company_master and "dart" not in self.sources:
            raise BackfillConfigurationError("sync_company_master requires the dart source")


IngestRunner = Callable[..., dict[str, int]]


def build_date_windows(start: date, end_exclusive: date, chunk_days: int) -> list[DateWindow]:
    if chunk_days < 1:
        raise BackfillConfigurationError("chunk_days must be at least 1")
    windows: list[DateWindow] = []
    cursor = start
    while cursor < end_exclusive:
        next_cursor = min(cursor + timedelta(days=chunk_days), end_exclusive)
        windows.append(DateWindow(cursor, next_cursor))
        cursor = next_cursor
    return windows


def load_env_files(
    project_root: Path,
    names: tuple[str, ...] = (".env", ".env.local", ".env.api"),
) -> list[Path]:
    loaded: list[Path] = []
    for name in names:
        path = project_root / name
        if not path.exists():
            continue
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
        loaded.append(path)
    return loaded


def job_contract(options: BackfillOptions) -> dict[str, object]:
    return {
        "range_start": options.start.isoformat(),
        "range_end_exclusive": options.end_exclusive.isoformat(),
        "chunk_days": options.chunk_days,
        "sources": sorted(options.sources),
        "page_count": options.page_count,
        "max_pages": options.max_pages,
        "sync_company_master": options.sync_company_master,
    }


def job_fingerprint(job: Mapping[str, object]) -> str:
    serialized = json.dumps(dict(job), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def window_idempotency_key(fingerprint: str, window: DateWindow) -> str:
    digest = hashlib.sha256(f"{fingerprint}|{window.key}".encode("utf-8")).hexdigest()[:32]
    return f"official-backfill-v1:{digest}"


def _timestamp(now_provider: Callable[[], datetime]) -> str:
    value = now_provider()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def new_checkpoint(
    job: dict[str, object],
    fingerprint: str,
    *,
    now_provider: Callable[[], datetime],
) -> dict[str, object]:
    created_at = _timestamp(now_provider)
    return {
        "schema_version": CHECKPOINT_SCHEMA_VERSION,
        "job": {**job, "fingerprint": fingerprint},
        "created_at": created_at,
        "updated_at": created_at,
        "company_master_synced": False,
        "completed_windows": {},
        "failed_windows": {},
    }


def load_checkpoint(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CheckpointError(f"cannot read checkpoint {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CheckpointError(f"checkpoint {path} must contain a JSON object")
    if value.get("schema_version") != CHECKPOINT_SCHEMA_VERSION:
        raise CheckpointError(
            f"checkpoint {path} schema_version must be {CHECKPOINT_SCHEMA_VERSION}; use --restart"
        )
    if not isinstance(value.get("job"), dict):
        raise CheckpointError(f"checkpoint {path} is missing job metadata")
    if not isinstance(value.get("company_master_synced"), bool):
        raise CheckpointError(f"checkpoint {path} company_master_synced must be boolean")
    if not isinstance(value.get("completed_windows"), dict) or not isinstance(value.get("failed_windows"), dict):
        raise CheckpointError(f"checkpoint {path} window maps are invalid")
    completed = value["completed_windows"]
    failed = value["failed_windows"]
    assert isinstance(completed, dict) and isinstance(failed, dict)
    overlap = set(completed) & set(failed)
    if overlap:
        raise CheckpointError(
            f"checkpoint {path} contains windows in both completed and failed maps"
        )
    for map_name, records, expected_status in (
        ("completed_windows", completed, "succeeded"),
        ("failed_windows", failed, "failed"),
    ):
        for window_key, result in records.items():
            if not isinstance(result, dict):
                raise CheckpointError(
                    f"checkpoint {path} {map_name}.{window_key} must be an object"
                )
            reconstructed_key = (
                f"{result.get('window_start')}:{result.get('window_end_exclusive')}"
            )
            try:
                attempt = int(result.get("attempt") or 0)
            except (TypeError, ValueError) as exc:
                raise CheckpointError(
                    f"checkpoint {path} {map_name}.{window_key} has invalid attempt"
                ) from exc
            if (
                reconstructed_key != window_key
                or result.get("status") != expected_status
                or attempt < 1
                or not str(result.get("idempotency_key") or "").startswith(
                    "official-backfill-v1:"
                )
            ):
                raise CheckpointError(
                    f"checkpoint {path} {map_name}.{window_key} is inconsistent"
                )
    return value


def save_checkpoint(path: Path, checkpoint: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(checkpoint, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def validate_runtime(options: BackfillOptions) -> None:
    missing: list[str] = []
    if "dart" in options.sources and not os.environ.get("DART_API_KEY", "").strip():
        missing.append("DART_API_KEY")
    if "kind" in options.sources and not os.environ.get("KIND_DISCLOSURE_ENDPOINT", "").strip():
        missing.append("KIND_DISCLOSURE_ENDPOINT")
    if not options.dry_run and not remote_api_configured():
        missing.append("ACTIVIST_API_URL/ACTIVIST_API_SECRET")
    if missing:
        raise BackfillConfigurationError("missing required runtime configuration: " + ", ".join(missing))


def _summary_int(summary: Mapping[str, object], key: str) -> int:
    value = summary.get(key)
    try:
        return int(str(value)) if value not in (None, "") else 0
    except (TypeError, ValueError):
        return 0


def _summary_succeeded(summary: Mapping[str, object], *, dry_run: bool) -> bool:
    if _summary_int(summary, "official_failed") or _summary_int(summary, "official_skipped"):
        return False
    if dry_run:
        return True
    return not (
        _summary_int(summary, "official_remote_failed")
        or _summary_int(summary, "official_remote_skipped")
        or _summary_int(summary, "official_remote_synced") < 1
    )


def _summary_totals(results: list[dict[str, object]]) -> dict[str, int]:
    keys = (
        "official_fetched",
        "official_documents",
        "official_events",
        "official_companies",
        "official_source_rights",
        "official_remote_synced",
        "official_remote_failed",
        "official_dart_fetched",
        "official_dart_accepted",
        "official_dart_rejected",
        "official_dart_duplicates",
        "official_dart_discarded",
        "official_dart_pages",
        "official_dart_errors",
        "official_kind_fetched",
        "official_kind_accepted",
        "official_kind_rejected",
        "official_kind_duplicates",
        "official_kind_discarded",
        "official_kind_pages",
        "official_kind_errors",
    )
    totals = {key: 0 for key in keys}
    for result in results:
        summary = result.get("summary")
        if not isinstance(summary, dict):
            continue
        for key in keys:
            totals[key] += _summary_int(summary, key)
    return totals


def run_backfill(
    project_root: Path,
    options: BackfillOptions,
    *,
    ingest_runner: IngestRunner = run_official_ingest,
    now_provider: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
) -> dict[str, object]:
    """Run pending chunks and persist a checkpoint after each attempted window.

    A normal run resumes only when the checkpoint fingerprint exactly matches
    the requested range and connector limits.  A dry run deliberately ignores
    and never mutates the checkpoint, while still fetching and normalizing the
    selected windows.
    """

    options.validate()
    job = job_contract(options)
    fingerprint = job_fingerprint(job)
    checkpoint = None if options.dry_run or options.restart else load_checkpoint(options.checkpoint_path)
    if checkpoint is not None:
        checkpoint_job = checkpoint.get("job")
        checkpoint_fingerprint = (
            str(checkpoint_job.get("fingerprint") or "") if isinstance(checkpoint_job, dict) else ""
        )
        if checkpoint_fingerprint != fingerprint:
            raise CheckpointError(
                "checkpoint job fingerprint does not match the requested range/options; use --restart"
            )
    else:
        checkpoint = new_checkpoint(job, fingerprint, now_provider=now_provider)

    completed_windows = checkpoint["completed_windows"]
    failed_windows = checkpoint["failed_windows"]
    assert isinstance(completed_windows, dict) and isinstance(failed_windows, dict)

    all_windows = build_date_windows(options.start, options.end_exclusive, options.chunk_days)
    expected_window_keys = {window.key for window in all_windows}
    unknown_window_keys = (set(completed_windows) | set(failed_windows)) - expected_window_keys
    if unknown_window_keys:
        raise CheckpointError(
            "checkpoint contains windows outside the requested job: "
            + ", ".join(sorted(unknown_window_keys))
        )
    if options.dry_run:
        pending = all_windows
    else:
        pending = [window for window in all_windows if window.key not in completed_windows]
    pending_before_limit = len(pending)
    selected = pending[: options.max_chunks] if options.max_chunks else pending
    results: list[dict[str, object]] = []
    invocation_failures = 0

    if options.restart and not options.dry_run:
        save_checkpoint(options.checkpoint_path, checkpoint)

    for window in selected:
        previous_failure = failed_windows.get(window.key)
        previous_attempts = (
            int(previous_failure.get("attempt") or 0) if isinstance(previous_failure, dict) else 0
        )
        master_sync_needed = options.sync_company_master and not bool(checkpoint.get("company_master_synced"))
        overrides: dict[str, object] = {
            "dart_enabled": "dart" in options.sources,
            "kind_enabled": "kind" in options.sources,
            "page_count": options.page_count,
            "max_pages": options.max_pages,
            "sync_company_master": master_sync_needed,
        }
        result: dict[str, object] = {
            "window_start": window.start.isoformat(),
            "window_end_exclusive": window.end_exclusive.isoformat(),
            "source_end_inclusive": window.source_end_inclusive.isoformat(),
            "idempotency_key": window_idempotency_key(fingerprint, window),
            "attempt": previous_attempts + 1,
        }
        try:
            summary = ingest_runner(
                project_root,
                # Retrieval metadata must describe the real attempt time. The
                # stable window idempotency key, not a fabricated historical
                # timestamp, makes retries update the same collection run.
                now=now_provider(),
                start=window.start,
                end=window.source_end_inclusive,
                settings_overrides=overrides,
                dry_run=options.dry_run,
                idempotency_key=result["idempotency_key"],
            )
            result["summary"] = summary
            succeeded = _summary_succeeded(summary, dry_run=options.dry_run)
            if not succeeded:
                result["error"] = "official ingest or required remote sync did not succeed"
        except Exception as exc:  # the checkpoint must record connector/API failures
            succeeded = False
            result["error"] = f"{type(exc).__name__}: {exc}"

        result["status"] = "dry-run-succeeded" if succeeded and options.dry_run else (
            "succeeded" if succeeded else "failed"
        )
        result["finished_at"] = _timestamp(now_provider)
        results.append(result)

        if options.dry_run:
            if not succeeded:
                invocation_failures += 1
                if not options.continue_on_error:
                    break
            continue

        checkpoint["updated_at"] = result["finished_at"]
        if succeeded:
            completed_windows[window.key] = result
            failed_windows.pop(window.key, None)
            if master_sync_needed:
                checkpoint["company_master_synced"] = True
        else:
            invocation_failures += 1
            failed_windows[window.key] = result
        save_checkpoint(options.checkpoint_path, checkpoint)
        if not succeeded and not options.continue_on_error:
            break

    remaining = 0 if options.dry_run else len(
        [window for window in all_windows if window.key not in completed_windows]
    )
    return {
        "schema_version": 1,
        "status": "failed" if invocation_failures else "succeeded",
        "dry_run": options.dry_run,
        "job_fingerprint": fingerprint,
        "range_start": options.start.isoformat(),
        "range_end_exclusive": options.end_exclusive.isoformat(),
        "windows_total": len(all_windows),
        "windows_already_completed": 0 if options.dry_run else len(all_windows) - pending_before_limit,
        "windows_pending_before_limit": pending_before_limit,
        "windows_selected": len(selected),
        "windows_attempted": len(results),
        "windows_succeeded": sum(1 for row in results if str(row.get("status")).endswith("succeeded")),
        "windows_failed": invocation_failures,
        "windows_remaining": remaining,
        "checkpoint_path": None if options.dry_run else str(options.checkpoint_path),
        "totals": _summary_totals(results),
        "window_results": results,
    }


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def _sources(value: str) -> tuple[str, ...]:
    return ("dart", "kind") if value == "both" else (value,)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Restartable, idempotent DART/KIND governance-disclosure backfill (2021+)."
    )
    parser.add_argument("--root", type=Path, default=PROJECT_ROOT, help="project root containing config.yaml")
    parser.add_argument("--from-date", type=_parse_date, help="inclusive start date (default: config backfill_start)")
    parser.add_argument("--to-date", type=_parse_date, help="exclusive end date (default: tomorrow in KST)")
    parser.add_argument("--chunk-days", type=int, default=14)
    parser.add_argument("--source", choices=("dart", "kind", "both"), default="both")
    parser.add_argument("--page-count", type=int, default=100)
    parser.add_argument("--max-pages", type=int, default=100, help="maximum connector pages per source and chunk")
    parser.add_argument("--max-chunks", type=int, default=0, help="process at most N pending chunks (0 = unlimited)")
    parser.add_argument("--checkpoint", type=Path, default=DEFAULT_CHECKPOINT_PATH)
    parser.add_argument("--dry-run", action="store_true", help="fetch/normalize but do not sync or mutate checkpoint")
    parser.add_argument("--restart", action="store_true", help="replace the checkpoint for this requested job")
    parser.add_argument("--continue-on-error", action="store_true")
    parser.add_argument("--sync-company-master", action="store_true")
    return parser


def options_from_args(args: argparse.Namespace) -> tuple[Path, BackfillOptions]:
    project_root = args.root.resolve()
    config = load_config(project_root / "config.yaml")
    settings = config.get("official_ingest", {})
    configured_start = str(settings.get("backfill_start") or MIN_BACKFILL_DATE.isoformat()) if isinstance(settings, dict) else MIN_BACKFILL_DATE.isoformat()
    start = args.from_date or _parse_date(configured_start)
    tomorrow_kst = datetime.now(ZoneInfo("Asia/Seoul")).date() + timedelta(days=1)
    end_exclusive = args.to_date or tomorrow_kst
    checkpoint = args.checkpoint if args.checkpoint.is_absolute() else project_root / args.checkpoint
    return project_root, BackfillOptions(
        start=start,
        end_exclusive=end_exclusive,
        checkpoint_path=checkpoint,
        chunk_days=args.chunk_days,
        sources=_sources(args.source),
        page_count=args.page_count,
        max_pages=args.max_pages,
        max_chunks=args.max_chunks,
        dry_run=args.dry_run,
        restart=args.restart,
        continue_on_error=args.continue_on_error,
        sync_company_master=args.sync_company_master,
    )


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        project_root, options = options_from_args(args)
        load_env_files(project_root)
        options.validate()
        validate_runtime(options)
        report = run_backfill(project_root, options)
    except (BackfillConfigurationError, CheckpointError, OSError, ValueError) as exc:
        print(json.dumps({"status": "failed", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(2) from exc
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    if report["status"] != "succeeded":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
