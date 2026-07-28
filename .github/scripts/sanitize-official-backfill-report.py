#!/usr/bin/env python3
"""Validate and sanitize official-backfill JSON before artifact upload.

The backfill command writes its complete report to a private temporary file.
This boundary copies only a structurally valid, credential-free report into the
artifact path. Invalid or unsafe input is replaced with a small fail-closed
record that is safe to retain.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Mapping, Sequence


MAX_REPORT_BYTES = 2 * 1024 * 1024
MAX_JSON_DEPTH = 32
MAX_JSON_ITEMS = 100_000
MAX_STRING_LENGTH = 256_000
SHA_PATTERN = re.compile(r"^[a-f0-9]{40}$")
SAFE_MODES = frozenset({"dry-run", "apply", "replay"})
SUCCESS_STATUSES = frozenset({"succeeded", "dry-run-succeeded"})
KNOWN_SECRET_ENV_NAMES = (
    "OPENDART_API_KEYS",
    "DART_API_KEY",
    "KIND_API_KEY",
    "ACTIVIST_API_SECRET",
    "ACTIVIST_API_URL",
    "BSIDE_OPS_TOKEN",
    "BSIDE_API_BASE_URL",
)
FORBIDDEN_KEY_NAMES = frozenset(
    {
        "authorization",
        "proxy_authorization",
        "cookie",
        "cookies",
        "set_cookie",
        "headers",
        "raw_headers",
        "request_headers",
        "response_headers",
        "body",
        "raw_body",
        "request_body",
        "response_body",
        "api_key",
        "apikey",
        "access_key",
        "access_token",
        "refresh_token",
        "token",
        "password",
        "passwd",
        "credential",
        "credentials",
        "client_secret",
        "private_key",
        "secret",
    }
)
FORBIDDEN_KEY_SUFFIXES = (
    "_authorization",
    "_cookie",
    "_cookies",
    "_headers",
    "_body",
    "_api_key",
    "_access_key",
    "_access_token",
    "_refresh_token",
    "_token",
    "_password",
    "_passwd",
    "_credential",
    "_credentials",
    "_client_secret",
    "_private_key",
    "_secret",
)
CREDENTIAL_PATTERNS = (
    re.compile(r"(?i)\bauthorization\s*:\s*(?:bearer|basic)\s+\S+"),
    re.compile(r"(?i)\b(?:bearer|basic)\s+[A-Za-z0-9+/._~=-]{12,}"),
    re.compile(r"(?i)\b(?:cookie|set-cookie)\s*:\s*\S+"),
    re.compile(
        r"(?i)(?:[?&]|\b)(?:api[_-]?key|access[_-]?token|refresh[_-]?token|"
        r"password|passwd|client[_-]?secret|token|secret)="
        r"[^&\s\"']{8,}"
    ),
    re.compile(
        r"(?i)[\"']?(?:api[_-]?key|access[_-]?token|refresh[_-]?token|"
        r"password|passwd|client[_-]?secret|token|secret)[\"']?"
        r"\s*[:=]\s*[\"']?[^\s,\"'}]{8,}"
    ),
    re.compile(r"(?i)https?://[^/\s:@]+:[^/\s@]+@"),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
)
REDACTED_VALUE = "[REDACTED_CREDENTIAL_BEARING_VALUE]"


class ReportBoundaryError(ValueError):
    """A stable, non-sensitive report validation failure."""


@dataclass
class SanitizationStats:
    removed_fields: int = 0
    redacted_values: int = 0
    visited_items: int = 0


@dataclass(frozen=True)
class ExpectedReport:
    mode: str
    from_date: date
    to_date: date
    code_revision: str
    command_exit_code: int

    @property
    def window_count(self) -> int:
        return (self.to_date - self.from_date).days


def _fail(code: str) -> ReportBoundaryError:
    return ReportBoundaryError(code)


def _parse_date(value: str, field_name: str) -> date:
    text = str(value or "").strip()
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise _fail(f"invalid_{field_name}") from exc
    if parsed.isoformat() != text:
        raise _fail(f"invalid_{field_name}")
    return parsed


def _expected_from_args(args: argparse.Namespace) -> ExpectedReport:
    mode = str(args.mode or "").strip()
    if mode not in SAFE_MODES:
        raise _fail("invalid_expected_mode")
    from_date = _parse_date(args.from_date, "expected_from_date")
    to_date = _parse_date(args.to_date, "expected_to_date")
    if to_date <= from_date or (to_date - from_date).days > 31:
        raise _fail("invalid_expected_window")
    revision = str(args.code_revision or "").strip()
    if SHA_PATTERN.fullmatch(revision) is None:
        raise _fail("invalid_expected_code_revision")
    exit_code = args.command_exit_code
    if isinstance(exit_code, bool) or not isinstance(exit_code, int):
        raise _fail("invalid_command_exit_code")
    if not 0 <= exit_code <= 255:
        raise _fail("invalid_command_exit_code")
    return ExpectedReport(
        mode=mode,
        from_date=from_date,
        to_date=to_date,
        code_revision=revision,
        command_exit_code=exit_code,
    )


def _reject_duplicate_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _fail("duplicate_json_key")
        result[key] = value
    return result


def _reject_json_constant(_value: str) -> None:
    raise _fail("non_finite_json_number")


def _read_json(path: Path) -> dict[str, object]:
    if path.is_symlink() or not path.is_file():
        raise _fail("report_input_not_regular_file")
    with path.open("rb") as stream:
        raw = stream.read(MAX_REPORT_BYTES + 1)
    if not raw or len(raw) > MAX_REPORT_BYTES:
        raise _fail("report_size_out_of_bounds")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _fail("report_not_utf8") from exc
    try:
        payload = json.loads(
            text,
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=_reject_json_constant,
        )
    except json.JSONDecodeError as exc:
        raise _fail("invalid_json") from exc
    if not isinstance(payload, dict):
        raise _fail("report_must_be_object")
    return payload


def _secret_fragments(environment: Mapping[str, str]) -> tuple[str, ...]:
    fragments: set[str] = set()
    for name in KNOWN_SECRET_ENV_NAMES:
        value = str(environment.get(name, "") or "").strip()
        if not value:
            continue
        if len(value) >= 8:
            fragments.add(value)
        for part in re.split(r"[\s,;]+", value):
            candidate = part.strip()
            if len(candidate) >= 8:
                fragments.add(candidate)
    return tuple(sorted(fragments, key=len, reverse=True))


def _forbidden_key(key: str) -> bool:
    normalized = key.strip().casefold().replace("-", "_")
    return normalized in FORBIDDEN_KEY_NAMES or normalized.endswith(
        FORBIDDEN_KEY_SUFFIXES
    )


def _credential_bearing(value: str, secret_fragments: Sequence[str]) -> bool:
    if any(fragment in value for fragment in secret_fragments):
        return True
    return any(pattern.search(value) is not None for pattern in CREDENTIAL_PATTERNS)


def _sanitize_value(
    value: object,
    *,
    secret_fragments: Sequence[str],
    stats: SanitizationStats,
    depth: int = 0,
) -> object:
    if depth > MAX_JSON_DEPTH:
        raise _fail("report_json_too_deep")
    stats.visited_items += 1
    if stats.visited_items > MAX_JSON_ITEMS:
        raise _fail("report_json_too_many_items")
    if isinstance(value, dict):
        safe: dict[str, object] = {}
        for raw_key, child in value.items():
            if not isinstance(raw_key, str):
                raise _fail("report_key_must_be_string")
            if _forbidden_key(raw_key):
                stats.removed_fields += 1
                continue
            safe[raw_key] = _sanitize_value(
                child,
                secret_fragments=secret_fragments,
                stats=stats,
                depth=depth + 1,
            )
        return safe
    if isinstance(value, list):
        return [
            _sanitize_value(
                child,
                secret_fragments=secret_fragments,
                stats=stats,
                depth=depth + 1,
            )
            for child in value
        ]
    if isinstance(value, str):
        if len(value) > MAX_STRING_LENGTH:
            raise _fail("report_string_too_long")
        if _credential_bearing(value, secret_fragments):
            stats.redacted_values += 1
            return REDACTED_VALUE
        return value
    if value is None or isinstance(value, (bool, int, float)):
        return value
    raise _fail("unsupported_json_value")


def _integer(
    report: Mapping[str, object],
    key: str,
    *,
    minimum: int = 0,
) -> int:
    value = report.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise _fail(f"invalid_{key}")
    return value


def _validate_window_result(
    value: object,
    *,
    expected: ExpectedReport,
) -> tuple[str, date]:
    if not isinstance(value, dict):
        raise _fail("invalid_window_result")
    start = _parse_date(str(value.get("window_start") or ""), "window_start")
    end = _parse_date(
        str(value.get("window_end_exclusive") or ""),
        "window_end_exclusive",
    )
    if (
        end != start + timedelta(days=1)
        or start < expected.from_date
        or end > expected.to_date
    ):
        raise _fail("window_result_out_of_range")
    status = value.get("status")
    allowed = {"failed", "succeeded"}
    if expected.mode == "dry-run":
        allowed.add("dry-run-succeeded")
    if status not in allowed:
        raise _fail("invalid_window_result_status")
    return str(status), start


def _validate_report(
    report: dict[str, object],
    *,
    expected: ExpectedReport,
) -> list[str]:
    if report.get("schema_version") != 1:
        raise _fail("invalid_schema_version")
    status = report.get("status")
    if status not in {"succeeded", "failed"}:
        raise _fail("invalid_report_status")
    if report.get("mode") != expected.mode:
        raise _fail("report_mode_mismatch")
    if report.get("code_revision") != expected.code_revision:
        raise _fail("report_code_revision_mismatch")
    if report.get("range_start") != expected.from_date.isoformat():
        raise _fail("report_from_date_mismatch")
    if report.get("range_end_exclusive") != expected.to_date.isoformat():
        raise _fail("report_to_date_mismatch")

    windows_total = _integer(report, "windows_total")
    windows_attempted = _integer(report, "windows_attempted")
    windows_succeeded = _integer(report, "windows_succeeded")
    windows_failed = _integer(report, "windows_failed")
    windows_remaining = _integer(report, "windows_remaining")
    if windows_total != expected.window_count:
        raise _fail("windows_total_mismatch")
    if (
        windows_attempted > windows_total
        or windows_succeeded + windows_failed != windows_attempted
        or windows_remaining > windows_total
    ):
        raise _fail("invalid_window_counts")

    results = report.get("window_results")
    if not isinstance(results, list) or len(results) != windows_attempted:
        raise _fail("window_results_count_mismatch")
    result_statuses: list[str] = []
    starts: list[date] = []
    for result in results:
        result_status, result_start = _validate_window_result(
            result,
            expected=expected,
        )
        result_statuses.append(result_status)
        starts.append(result_start)
    if len(starts) != len(set(starts)):
        raise _fail("duplicate_window_result")
    observed_failed = sum(item == "failed" for item in result_statuses)
    observed_succeeded = sum(item in SUCCESS_STATUSES for item in result_statuses)
    if observed_failed != windows_failed or observed_succeeded != windows_succeeded:
        raise _fail("window_status_count_mismatch")

    if status == "succeeded":
        if expected.command_exit_code != 0:
            raise _fail("success_report_with_failed_command")
        if windows_failed != 0:
            raise _fail("successful_report_has_failed_window")
    else:
        if expected.command_exit_code == 0:
            raise _fail("failure_report_with_successful_command")
        if windows_failed < 1:
            raise _fail("failed_report_missing_failed_window")

    failed_windows = [
        start.isoformat()
        for start, result_status in zip(starts, result_statuses)
        if result_status == "failed"
    ]
    if failed_windows:
        report["failed_window"] = failed_windows[0]
        report["failed_windows"] = failed_windows
    return failed_windows


def _sanitization_metadata(
    *,
    status: str,
    stats: SanitizationStats,
    source_size_bytes: int,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "status": status,
        "source_size_bytes": source_size_bytes,
        "removed_field_count": stats.removed_fields,
        "redacted_value_count": stats.redacted_values,
    }


def _fallback(expected: ExpectedReport, error_code: str) -> dict[str, object]:
    return {
        "schema_version": 1,
        "status": "failed",
        "mode": expected.mode,
        "dry_run": expected.mode == "dry-run",
        "code_revision": expected.code_revision,
        "range_start": expected.from_date.isoformat(),
        "range_end_exclusive": expected.to_date.isoformat(),
        "windows_total": expected.window_count,
        "windows_attempted": 0,
        "windows_succeeded": 0,
        "windows_failed": 0,
        "windows_remaining": expected.window_count,
        "command_exit_code": expected.command_exit_code,
        "failure": {
            "code": "unsafe_or_invalid_official_backfill_report",
            "validation_code": error_code,
        },
        "artifact_sanitization": {
            "schema_version": 1,
            "status": "fallback",
            "source_size_bytes": 0,
            "removed_field_count": 0,
            "redacted_value_count": 0,
        },
    }


def _encode(payload: Mapping[str, object]) -> bytes:
    encoded = (
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")
    if len(encoded) > MAX_REPORT_BYTES:
        raise _fail("sanitized_report_too_large")
    return encoded


def _write_atomic(path: Path, payload: Mapping[str, object]) -> None:
    parent = path.parent.resolve()
    if not parent.is_dir() or path.is_symlink():
        raise _fail("unsafe_report_output")
    encoded = _encode(payload)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=parent,
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        temporary = None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def sanitize_report(
    input_path: Path,
    output_path: Path,
    *,
    expected: ExpectedReport,
    environment: Mapping[str, str],
) -> dict[str, object]:
    if input_path.resolve() == output_path.resolve():
        raise _fail("input_and_output_must_differ")
    source_size = input_path.stat().st_size if input_path.exists() else 0
    report = _read_json(input_path)
    stats = SanitizationStats()
    sanitized = _sanitize_value(
        report,
        secret_fragments=_secret_fragments(environment),
        stats=stats,
    )
    if not isinstance(sanitized, dict):
        raise _fail("report_must_be_object")
    _validate_report(sanitized, expected=expected)
    sanitized["command_exit_code"] = expected.command_exit_code
    sanitized["artifact_sanitization"] = _sanitization_metadata(
        status="verified",
        stats=stats,
        source_size_bytes=source_size,
    )
    _write_atomic(output_path, sanitized)
    return sanitized


def _validate_fallback(
    report: Mapping[str, object],
    *,
    expected: ExpectedReport,
) -> None:
    failure = report.get("failure")
    sanitization = report.get("artifact_sanitization")
    if (
        report.get("schema_version") != 1
        or report.get("status") != "failed"
        or report.get("mode") != expected.mode
        or report.get("code_revision") != expected.code_revision
        or report.get("range_start") != expected.from_date.isoformat()
        or report.get("range_end_exclusive") != expected.to_date.isoformat()
        or report.get("command_exit_code") != expected.command_exit_code
        or not isinstance(failure, dict)
        or failure.get("code") != "unsafe_or_invalid_official_backfill_report"
        or not isinstance(sanitization, dict)
        or sanitization.get("status") != "fallback"
    ):
        raise _fail("invalid_fallback_report")


def verify_existing_report(
    path: Path,
    *,
    expected: ExpectedReport,
    environment: Mapping[str, str],
) -> dict[str, object]:
    report = _read_json(path)
    stats = SanitizationStats()
    sanitized = _sanitize_value(
        report,
        secret_fragments=_secret_fragments(environment),
        stats=stats,
    )
    if not isinstance(sanitized, dict) or sanitized != report:
        raise _fail("saved_report_requires_sanitization")
    sanitization = report.get("artifact_sanitization")
    if not isinstance(sanitization, dict):
        raise _fail("saved_report_missing_sanitization")
    if sanitization.get("status") == "fallback":
        _validate_fallback(report, expected=expected)
    elif sanitization.get("status") == "verified":
        if report.get("command_exit_code") != expected.command_exit_code:
            raise _fail("saved_report_exit_code_mismatch")
        _validate_report(report, expected=expected)
    else:
        raise _fail("saved_report_invalid_sanitization_status")
    _write_atomic(path, report)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Sanitize official-backfill JSON for an Actions artifact",
    )
    parser.add_argument("--input", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=sorted(SAFE_MODES), required=True)
    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--code-revision", required=True)
    parser.add_argument("--command-exit-code", type=int, required=True)
    parser.add_argument("--verify-existing", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        expected = _expected_from_args(args)
    except ReportBoundaryError as exc:
        print(f"::error::Official backfill artifact boundary rejected its contract ({exc}).")
        return 3

    try:
        if args.verify_existing:
            report = verify_existing_report(
                args.output,
                expected=expected,
                environment=os.environ,
            )
        else:
            if args.input is None:
                raise _fail("missing_report_input")
            report = sanitize_report(
                args.input,
                args.output,
                expected=expected,
                environment=os.environ,
            )
    except (OSError, ReportBoundaryError) as exc:
        code = str(exc) if isinstance(exc, ReportBoundaryError) else "report_io_failed"
        fallback = _fallback(expected, code)
        try:
            _write_atomic(args.output, fallback)
        except (OSError, ReportBoundaryError):
            print("::error::Official backfill artifact boundary could not write a safe report.")
            return 3
        print(
            "::warning::Official backfill report was replaced by a safe fallback "
            f"({code})."
        )
        return 2

    print(
        "Official backfill artifact boundary passed "
        f"(status={report['status']}, mode={report['mode']})."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
