"""Bounded one-day orchestration for global official-source backfills."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Sequence

from .global_ingest import (
    GlobalIngestConfigurationError,
    main as global_ingest_main,
    sec_completed_day_limit,
    write_evidence,
)


SUPPORTED_BACKFILL_COUNTRIES = ("US", "JP", "GB")
SUPPORTED_BACKFILL_MODES = ("apply", "replay")
MAX_BACKFILL_WINDOWS = 31
_CODE_REVISION = re.compile(r"^[a-f0-9]{7,64}$")


class GlobalBackfillError(RuntimeError):
    """A safe, stable failure code for workflow evidence."""


@dataclass(frozen=True)
class GlobalBackfillWindow:
    start: date
    end_exclusive: date


@dataclass(frozen=True)
class GlobalBackfillPlan:
    country_code: str
    mode: str
    requested_start: date
    requested_end_exclusive: date
    max_windows: int
    windows: tuple[GlobalBackfillWindow, ...]


def _parse_iso_date(value: str, field_name: str) -> date:
    text = str(value or "").strip()
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise GlobalBackfillError(f"invalid_{field_name}") from exc
    if parsed.isoformat() != text:
        raise GlobalBackfillError(f"invalid_{field_name}")
    return parsed


def plan_global_backfill(
    *,
    country_code: str,
    mode: str,
    from_date: str,
    to_date: str,
    max_windows: int,
    now: datetime | None = None,
) -> GlobalBackfillPlan:
    country = str(country_code or "").strip().upper()
    normalized_mode = str(mode or "").strip().casefold()
    if country not in SUPPORTED_BACKFILL_COUNTRIES:
        raise GlobalBackfillError("unsupported_global_backfill_country")
    if normalized_mode not in SUPPORTED_BACKFILL_MODES:
        raise GlobalBackfillError("invalid_global_backfill_mode")
    if (
        isinstance(max_windows, bool)
        or not isinstance(max_windows, int)
        or not 1 <= max_windows <= MAX_BACKFILL_WINDOWS
    ):
        raise GlobalBackfillError("invalid_global_backfill_max_windows")

    start = _parse_iso_date(from_date, "from_date")
    end = _parse_iso_date(to_date, "to_date")
    if end <= start:
        raise GlobalBackfillError("invalid_global_backfill_window")
    window_count = (end - start).days
    if window_count > MAX_BACKFILL_WINDOWS:
        raise GlobalBackfillError("global_backfill_exceeds_31_windows")
    if window_count > max_windows:
        raise GlobalBackfillError("global_backfill_exceeds_max_windows")

    observed = now or datetime.now(timezone.utc)
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise GlobalBackfillError("global_backfill_now_requires_timezone")
    completed_end_exclusive = (
        sec_completed_day_limit(now=observed)
        if country == "US"
        else observed.astimezone(timezone.utc).date()
    )
    if end > completed_end_exclusive:
        raise GlobalBackfillError("global_backfill_requires_completed_days")

    windows = tuple(
        GlobalBackfillWindow(
            start=start + timedelta(days=index),
            end_exclusive=start + timedelta(days=index + 1),
        )
        for index in range(window_count)
    )
    return GlobalBackfillPlan(
        country_code=country,
        mode=normalized_mode,
        requested_start=start,
        requested_end_exclusive=end,
        max_windows=max_windows,
        windows=windows,
    )


def _receipt_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_receipt(
    path: Path,
    *,
    plan: GlobalBackfillPlan,
    window: GlobalBackfillWindow,
    code_revision: str,
) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GlobalBackfillError("invalid_global_backfill_receipt") from exc
    expected_window = {
        "start": window.start.isoformat(),
        "end_exclusive": window.end_exclusive.isoformat(),
    }
    if (
        not isinstance(payload, dict)
        or payload.get("status") != "succeeded"
        or payload.get("country_code") != plan.country_code
        or payload.get("code_revision") != code_revision
        or payload.get("collection_mode") != "completed-day"
        or payload.get("window") != expected_window
    ):
        raise GlobalBackfillError("invalid_global_backfill_receipt")
    raw_count = payload.get("raw_count")
    acknowledged_count = payload.get("acknowledged_count")
    if (
        not isinstance(raw_count, int)
        or isinstance(raw_count, bool)
        or not isinstance(acknowledged_count, int)
        or isinstance(acknowledged_count, bool)
        or raw_count < acknowledged_count
        or acknowledged_count < 0
    ):
        raise GlobalBackfillError("invalid_global_backfill_receipt_counts")
    if plan.mode == "replay":
        replay = payload.get("replay_verification")
        if (
            payload.get("idempotent") is not True
            or not isinstance(replay, dict)
            or replay.get("attempted") is not True
            or replay.get("same_payload") is not True
            or replay.get("idempotent") is not True
            or replay.get("read_only") is not True
            or replay.get("idempotency_keys_match") is not True
            or replay.get("ingest_ids_match") is not True
            or replay.get("chunk_count") != replay.get(
                "idempotent_chunk_count"
            )
            or replay.get("raw_count") != raw_count
            or replay.get("acknowledged_count") != acknowledged_count
        ):
            raise GlobalBackfillError(
                "global_backfill_replay_verification_failed"
            )
    elif "replay_verification" in payload:
        raise GlobalBackfillError("unexpected_global_backfill_replay")
    return payload


def run_global_backfill(
    *,
    plan: GlobalBackfillPlan,
    code_revision: str,
    evidence_dir: Path,
    summary_path: Path,
    max_pages: int,
    ingest_entrypoint: Callable[[Sequence[str] | None], int] = global_ingest_main,
) -> dict[str, object]:
    revision = str(code_revision or "").strip().casefold()
    if _CODE_REVISION.fullmatch(revision) is None:
        raise GlobalBackfillError("invalid_code_revision")
    if (
        isinstance(max_pages, bool)
        or not isinstance(max_pages, int)
        or not 1 <= max_pages <= 1000
    ):
        raise GlobalBackfillError("invalid_global_backfill_max_pages")

    evidence_dir.mkdir(parents=True, exist_ok=True)
    existing = tuple(evidence_dir.iterdir())
    if existing:
        raise GlobalBackfillError("global_backfill_evidence_dir_not_empty")
    started_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    receipts: list[dict[str, object]] = []
    total_raw = 0
    total_acknowledged = 0
    failed_window: str | None = None
    try:
        for window in plan.windows:
            failed_window = window.start.isoformat()
            receipt_path = (
                evidence_dir
                / f"{plan.country_code}-{window.start.isoformat()}.json"
            )
            arguments = [
                "--country",
                plan.country_code,
                "--from-date",
                window.start.isoformat(),
                "--to-date",
                window.end_exclusive.isoformat(),
                "--max-pages",
                str(max_pages),
                "--code-revision",
                revision,
                "--evidence",
                str(receipt_path),
                "--completed-day-only",
            ]
            if plan.mode == "replay":
                arguments.extend(("--verify-replay", "--replay-only"))
            exit_code = ingest_entrypoint(arguments)
            if exit_code != 0:
                raise GlobalBackfillError("global_backfill_window_failed")
            payload = _read_receipt(
                receipt_path,
                plan=plan,
                window=window,
                code_revision=revision,
            )
            raw_value = payload["raw_count"]
            acknowledged_value = payload["acknowledged_count"]
            if not isinstance(raw_value, int) or not isinstance(
                acknowledged_value,
                int,
            ):
                raise GlobalBackfillError(
                    "invalid_global_backfill_receipt_counts"
                )
            raw_count = raw_value
            acknowledged_count = acknowledged_value
            total_raw += raw_count
            total_acknowledged += acknowledged_count
            receipts.append(
                {
                    "date": window.start.isoformat(),
                    "window_end_exclusive": window.end_exclusive.isoformat(),
                    "path": receipt_path.name,
                    "receipt_sha256": _receipt_sha256(receipt_path),
                    "raw_count": raw_count,
                    "acknowledged_count": acknowledged_count,
                    "initial_idempotent": payload.get("idempotent"),
                    "replay_verified": plan.mode == "replay",
                }
            )
            failed_window = None
        summary: dict[str, object] = {
            "schema_version": 1,
            "artifact_type": "global-official-backfill",
            "status": "succeeded",
            "country_code": plan.country_code,
            "mode": plan.mode,
            "code_revision": revision,
            "requested_window": {
                "start": plan.requested_start.isoformat(),
                "end_exclusive": plan.requested_end_exclusive.isoformat(),
            },
            "max_windows": plan.max_windows,
            "processed_windows": len(receipts),
            "total_raw_count": total_raw,
            "total_acknowledged_count": total_acknowledged,
            "receipts": receipts,
            "started_at": started_at,
            "completed_at": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
        }
        write_evidence(summary_path, summary)
        return summary
    except Exception as error:
        code = (
            str(error)
            if isinstance(error, GlobalBackfillError)
            else "global_backfill_failed"
        )
        failure: dict[str, object] = {
            "schema_version": 1,
            "artifact_type": "global-official-backfill",
            "status": "failed",
            "country_code": plan.country_code,
            "mode": plan.mode,
            "code_revision": revision,
            "requested_window": {
                "start": plan.requested_start.isoformat(),
                "end_exclusive": plan.requested_end_exclusive.isoformat(),
            },
            "max_windows": plan.max_windows,
            "processed_windows": len(receipts),
            "failed_window": failed_window,
            "receipts": receipts,
            "error": {"code": code, "class": type(error).__name__},
            "started_at": started_at,
            "completed_at": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat(),
        }
        write_evidence(summary_path, failure)
        raise


def _argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run bounded, sequential one-day global official-source windows."
        )
    )
    parser.add_argument(
        "--country",
        required=True,
        choices=SUPPORTED_BACKFILL_COUNTRIES,
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=SUPPORTED_BACKFILL_MODES,
    )
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--max-windows", type=int, required=True)
    parser.add_argument("--max-pages", type=int, default=100)
    parser.add_argument("--code-revision", default="")
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _argument_parser().parse_args(argv)
    try:
        plan = plan_global_backfill(
            country_code=args.country,
            mode=args.mode,
            from_date=args.from_date,
            to_date=args.to_date,
            max_windows=args.max_windows,
        )
        summary = run_global_backfill(
            plan=plan,
            code_revision=(
                args.code_revision or os.environ.get("GITHUB_SHA", "")
            ),
            evidence_dir=args.evidence_dir,
            summary_path=args.summary,
            max_pages=args.max_pages,
        )
        print(
            json.dumps(
                {
                    "ok": True,
                    "country_code": summary["country_code"],
                    "mode": summary["mode"],
                    "processed_windows": summary["processed_windows"],
                    "total_raw_count": summary["total_raw_count"],
                    "total_acknowledged_count": summary[
                        "total_acknowledged_count"
                    ],
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        return 0
    except Exception as error:
        code = (
            str(error)
            if isinstance(
                error,
                (GlobalBackfillError, GlobalIngestConfigurationError),
            )
            else "global_backfill_failed"
        )
        if not args.summary.exists():
            try:
                write_evidence(
                    args.summary,
                    {
                        "schema_version": 1,
                        "artifact_type": "global-official-backfill",
                        "status": "failed",
                        "country_code": str(args.country),
                        "mode": str(args.mode),
                        "code_revision": (
                            str(
                                args.code_revision
                                or os.environ.get("GITHUB_SHA", "")
                            )
                            .strip()
                            .casefold()
                        ),
                        "requested_window": {
                            "start": str(args.from_date),
                            "end_exclusive": str(args.to_date),
                        },
                        "max_windows": args.max_windows,
                        "processed_windows": 0,
                        "receipts": [],
                        "error": {
                            "code": code,
                            "class": type(error).__name__,
                        },
                    },
                )
            except OSError:
                pass
        print(
            json.dumps(
                {"ok": False, "error": code},
                sort_keys=True,
                separators=(",", ":"),
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
