from __future__ import annotations

import argparse
import re
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path


REQUIRED_ROOT_FILES = ("CNAME", "404.html", "feed.xml", "index.html")
REQUIRED_DIRECTORIES = ("feed",)
PRIVATE_FILENAMES = frozenset({"story-review.html", "story-review-meta.json"})
REQUIRED_FEED_FILES = (
    "index.html",
    "latest.html",
    "search.html",
    "telegram-admin.html",
    "telegram.html",
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
        else:
            unexpected_paths.append(candidate.name)

    if unexpected_paths:
        raise PreparationError(
            "unexpected path in legacy feed output: " + ", ".join(sorted(unexpected_paths))
        )
    _validate_dated_report_continuity(allowed_files)
    return sorted(allowed_files, key=lambda candidate: candidate.name)


def prepare_legacy_pages(source: Path, destination: Path) -> dict[str, object]:
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
        shutil.copy2(source / filename, destination / filename)
    destination_feed = destination / "feed"
    destination_feed.mkdir()
    for source_file in feed_files:
        shutil.copy2(source_file, destination_feed / source_file.name)

    staged_names = sorted(path.name for path in destination.iterdir())
    expected_names = sorted((*REQUIRED_ROOT_FILES, *REQUIRED_DIRECTORIES))
    if staged_names != expected_names:
        raise PreparationError("legacy Pages staging produced an unexpected root path")
    if (destination / "governance").exists():
        raise PreparationError("governance UI must not be present in the legacy Pages artifact")
    _assert_no_symlinks(destination)

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
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = prepare_legacy_pages(args.source, args.destination)
    except PreparationError as exc:
        print(f"legacy_pages_artifact_error={exc}", file=sys.stderr)
        return 1
    print(f"legacy_pages_artifact={result['destination']}")
    print(f"legacy_pages_files={result['file_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
