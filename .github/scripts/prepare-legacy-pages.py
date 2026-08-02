from __future__ import annotations

import argparse
import re
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from curator.legacy_telegram_safety import (  # noqa: E402
    LegacyTelegramSafetyError,
    redact_telegram_mentions,
    verify_public_site,
)
from curator.legacy_internal_safety import (  # noqa: E402
    LegacyInternalSafetyError,
    redact_internal_score_display,
    verify_no_internal_score_site,
)


REQUIRED_ROOT_FILES = ("CNAME", "404.html", "feed.xml", "index.html")
REQUIRED_DIRECTORIES = ("feed",)
GOVERNANCE_PREVIEW_FILES = ("app.js", "config.js", "index.html", "styles.css")
PRIVATE_FILENAMES = frozenset({"story-review.html", "story-review-meta.json"})
REQUIRED_FEED_FILES = (
    "index.html",
    "latest.html",
    "search.html",
)
DROPPED_FEED_FILES = frozenset(
    {
        "telegram-admin.html",
        "telegram.html",
    }
)
DATED_REPORT_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}\.html\Z")
ARCHIVE_START_DATE = date(2026, 5, 1)
REQUIRED_ARCHIVE_END_DATE = date(2026, 7, 20)
MAX_REPORT_COUNT = 1_000


class PreparationError(RuntimeError):
    """A safe-to-print legacy Pages staging failure."""


def _assert_separate_paths(source: Path, destination: Path) -> None:
    if (
        source == destination
        or destination.is_relative_to(source)
        or source.is_relative_to(destination)
    ):
        raise PreparationError("source and destination must be separate directory trees")


def _assert_no_symlinks(path: Path) -> None:
    if path.is_symlink():
        raise PreparationError(f"symlinks are not allowed in the legacy Pages artifact: {path.name}")
    for candidate in path.rglob("*"):
        if candidate.is_symlink():
            relative = candidate.relative_to(path)
            raise PreparationError(
                f"symlinks are not allowed in the legacy Pages artifact: {relative.as_posix()}"
            )


def _dated_report_date(path: Path) -> date | None:
    if not DATED_REPORT_PATTERN.fullmatch(path.name):
        return None
    try:
        return date.fromisoformat(path.stem)
    except ValueError:
        return None


def _validate_dated_report_continuity(files: list[Path]) -> None:
    report_dates = {
        report_date
        for candidate in files
        if (report_date := _dated_report_date(candidate)) is not None
    }
    if not report_dates:
        raise PreparationError("legacy feed output must contain dated reports")
    first_date = min(report_dates)
    last_date = max(report_dates)
    if first_date != ARCHIVE_START_DATE or last_date < REQUIRED_ARCHIVE_END_DATE:
        raise PreparationError(
            "legacy feed output does not cover the required archive compatibility boundary"
        )
    expected_count = (last_date - first_date).days + 1
    if expected_count > MAX_REPORT_COUNT:
        raise PreparationError("legacy feed output has an unsafe dated-report range")
    if len(report_dates) != expected_count:
        cursor = first_date
        missing_dates: list[str] = []
        while cursor <= last_date:
            if cursor not in report_dates:
                missing_dates.append(cursor.isoformat())
            cursor += timedelta(days=1)
        raise PreparationError(
            "legacy feed output has missing report dates: " + ", ".join(missing_dates[:10])
        )


def _collect_feed_files(feed_dir: Path) -> list[Path]:
    for filename in REQUIRED_FEED_FILES:
        candidate = feed_dir / filename
        if not candidate.is_file() or candidate.is_symlink():
            raise PreparationError(f"required legacy feed file is missing or unsafe: {filename}")

    private_files = sorted(
        candidate.name
        for candidate in feed_dir.iterdir()
        if candidate.name in PRIVATE_FILENAMES
    )
    if private_files:
        raise PreparationError(
            "private review files must be removed before legacy Pages staging: "
            + ", ".join(private_files)
        )

    allowed_files: list[Path] = []
    unexpected_paths: list[str] = []
    for candidate in feed_dir.iterdir():
        if candidate.is_file() and (
            candidate.name in REQUIRED_FEED_FILES or _dated_report_date(candidate) is not None
        ):
            allowed_files.append(candidate)
        elif candidate.is_file() and candidate.name in DROPPED_FEED_FILES:
            continue
        else:
            unexpected_paths.append(candidate.name)

    if unexpected_paths:
        raise PreparationError(
            "unexpected path in legacy feed output: " + ", ".join(sorted(unexpected_paths))
        )
    _validate_dated_report_continuity(allowed_files)
    return sorted(allowed_files, key=lambda candidate: candidate.name)


def _copy_public_file(source: Path, destination: Path, *, relative: str) -> None:
    try:
        payload = redact_telegram_mentions(source.read_bytes(), path=relative)
        payload = redact_internal_score_display(payload, path=relative)
    except (LegacyTelegramSafetyError, LegacyInternalSafetyError) as exc:
        raise PreparationError(str(exc)) from exc
    destination.write_bytes(payload)


def _copy_governance_preview(source: Path, destination: Path) -> None:
    if source.is_symlink() or not source.is_dir():
        raise PreparationError("governance preview source must be a regular directory")
    _assert_no_symlinks(source)
    actual = sorted(path.name for path in source.iterdir())
    if actual != sorted(GOVERNANCE_PREVIEW_FILES):
        raise PreparationError(
            "governance preview must contain only its four public assets"
        )
    destination.mkdir()
    for filename in GOVERNANCE_PREVIEW_FILES:
        candidate = source / filename
        if not candidate.is_file() or candidate.is_symlink():
            raise PreparationError(
                f"required governance preview file is missing or unsafe: {filename}"
            )
        _copy_public_file(
            candidate,
            destination / filename,
            relative=f"governance/{filename}",
        )


def prepare_legacy_pages(
    source: Path,
    destination: Path,
    governance_preview: Path | None = None,
) -> dict[str, object]:
    if source.is_symlink():
        raise PreparationError("legacy Pages source directory must not be a symlink")
    if destination.is_symlink():
        raise PreparationError("legacy Pages destination must not be a symlink")
    source = source.resolve()
    destination = destination.resolve()
    if not source.is_dir():
        raise PreparationError(f"legacy Pages source directory is missing: {source}")
    _assert_separate_paths(source, destination)
    if destination.exists():
        raise PreparationError(f"legacy Pages destination must not already exist: {destination}")

    for filename in REQUIRED_ROOT_FILES:
        candidate = source / filename
        if not candidate.is_file() or candidate.is_symlink():
            raise PreparationError(f"required legacy Pages file is missing or unsafe: {filename}")

    for dirname in REQUIRED_DIRECTORIES:
        candidate = source / dirname
        if not candidate.is_dir() or candidate.is_symlink():
            raise PreparationError(f"required legacy Pages directory is missing or unsafe: {dirname}")
        _assert_no_symlinks(candidate)

    feed_files = _collect_feed_files(source / "feed")

    destination.mkdir(parents=True)
    for filename in REQUIRED_ROOT_FILES:
        _copy_public_file(
            source / filename,
            destination / filename,
            relative=filename,
        )
    destination_feed = destination / "feed"
    destination_feed.mkdir()
    for source_file in feed_files:
        _copy_public_file(
            source_file,
            destination_feed / source_file.name,
            relative=f"feed/{source_file.name}",
        )
    if governance_preview is not None:
        _copy_governance_preview(
            governance_preview.resolve(),
            destination / "governance",
        )

    staged_names = sorted(path.name for path in destination.iterdir())
    expected_names = sorted(
        (*REQUIRED_ROOT_FILES, *REQUIRED_DIRECTORIES)
        + (("governance",) if governance_preview is not None else ())
    )
    if staged_names != expected_names:
        raise PreparationError("legacy Pages staging produced an unexpected root path")
    if governance_preview is None and (destination / "governance").exists():
        raise PreparationError("governance UI must not be present in the legacy Pages artifact")
    _assert_no_symlinks(destination)
    try:
        verify_public_site(destination)
        verify_no_internal_score_site(destination)
    except (LegacyTelegramSafetyError, LegacyInternalSafetyError) as exc:
        raise PreparationError(str(exc)) from exc

    files = [path for path in destination.rglob("*") if path.is_file()]
    return {
        "destination": str(destination),
        "file_count": len(files),
        "root_paths": staged_names,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare the allowlisted legacy GitHub Pages artifact")
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument(
        "--governance-preview-source",
        type=Path,
        help="Optional four-file public UI mounted only at /governance/ during shadow",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = prepare_legacy_pages(
            args.source,
            args.destination,
            governance_preview=args.governance_preview_source,
        )
    except PreparationError as exc:
        print(f"legacy_pages_artifact_error={exc}", file=sys.stderr)
        return 1
    print(f"legacy_pages_artifact={result['destination']}")
    print(f"legacy_pages_files={result['file_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
