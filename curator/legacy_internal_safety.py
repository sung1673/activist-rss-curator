from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any


INTERNAL_PRIORITY_MARKER = b"story.priority_score"
DATED_REPORT = re.compile(r"^feed/\d{4}-\d{2}-\d{2}\.html$")
INTERNAL_PRIORITY_DISPLAY = re.compile(
    rb"(?m)^[ \t]*const priority = Number\(story\.priority_score \|\| 0\);\r?\n"
    rb"^[ \t]*if \(priority\) \{\r?\n"
    rb"^[ \t]*const score = document\.createElement\((?:'|\")span(?:'|\")\);\r?\n"
    rb"^[ \t]*score\.textContent = `[^`\r\n]*\$\{priority\}`;\r?\n"
    rb"^[ \t]*meta\.appendChild\(score\);\r?\n"
    rb"^[ \t]*\}\r?\n"
)


class LegacyInternalSafetyError(RuntimeError):
    """A safe-to-print public compatibility sanitization failure."""


def validate_no_internal_score(payload: bytes, *, path: str) -> None:
    if INTERNAL_PRIORITY_MARKER in payload:
        raise LegacyInternalSafetyError(
            f"legacy public artifact contains an internal priority score: {path}"
        )


def redact_internal_score_display(payload: bytes, *, path: str) -> bytes:
    if not path.casefold().endswith(".html"):
        validate_no_internal_score(payload, path=path)
        return payload
    redacted = INTERNAL_PRIORITY_DISPLAY.sub(b"", payload)
    validate_no_internal_score(redacted, path=path)
    return redacted


def verify_no_internal_score_site(
    root: Path,
    *,
    minimum_dated_reports: int = 0,
) -> dict[str, Any]:
    if minimum_dated_reports < 0:
        raise LegacyInternalSafetyError(
            "minimum dated report count cannot be negative"
        )
    if root.is_symlink() or not root.is_dir():
        raise LegacyInternalSafetyError(
            "legacy public site must be a regular directory"
        )
    resolved_root = root.resolve()
    files = 0
    dated_reports = 0
    for candidate in resolved_root.rglob("*"):
        if candidate.is_symlink():
            raise LegacyInternalSafetyError(
                "legacy public site contains a symbolic link"
            )
        if candidate.is_dir():
            continue
        if not candidate.is_file():
            raise LegacyInternalSafetyError(
                "legacy public site contains a non-regular file"
            )
        try:
            relative = candidate.resolve().relative_to(resolved_root).as_posix()
        except ValueError as exc:
            raise LegacyInternalSafetyError(
                "legacy public site path escaped its root"
            ) from exc
        validate_no_internal_score(candidate.read_bytes(), path=relative)
        files += 1
        if DATED_REPORT.fullmatch(relative):
            dated_reports += 1
    if dated_reports < minimum_dated_reports:
        raise LegacyInternalSafetyError(
            "legacy public site does not contain the required dated reports"
        )
    return {
        "schema_version": 1,
        "kind": "bside-legacy-internal-score-exposure-check",
        "file_count": files,
        "dated_report_count": dated_reports,
        "internal_priority_score_count": 0,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify that a public compatibility site exposes no internal score"
    )
    parser.add_argument("command", choices=("verify-site",))
    parser.add_argument("--site", type=Path, required=True)
    parser.add_argument("--minimum-dated-reports", type=int, default=0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        result = verify_no_internal_score_site(
            args.site,
            minimum_dated_reports=args.minimum_dated_reports,
        )
    except LegacyInternalSafetyError as exc:
        print(f"legacy_internal_safety_error={exc}", file=sys.stderr)
        return 1
    print(
        "legacy_internal_safety="
        + json.dumps(result, separators=(",", ":"), sort_keys=True)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
