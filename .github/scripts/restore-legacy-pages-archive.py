from __future__ import annotations

import argparse
import sys
import tarfile
from datetime import date, timedelta
from pathlib import Path, PurePosixPath


ARCHIVE_START_DATE = date(2026, 5, 1)
REQUIRED_ARCHIVE_END_DATE = date(2026, 7, 20)
MAX_REPORT_BYTES = 2_000_000
MAX_ARCHIVE_BYTES = 250_000_000
MAX_REPORT_COUNT = 1_000


class ArchiveContinuityError(RuntimeError):
    """A safe-to-print legacy archive continuity failure."""


def _report_date_from_name(filename: str) -> date | None:
    if len(filename) != 15 or not filename.endswith(".html"):
        return None
    try:
        parsed = date.fromisoformat(filename[:-5])
    except ValueError:
        return None
    if parsed.isoformat() + ".html" != filename:
        return None
    return parsed


def _report_date_from_tar_member(name: str) -> date | None:
    path = PurePosixPath(name)
    parts = path.parts
    if parts and parts[0] == ".":
        parts = parts[1:]
    if len(parts) == 3 and parts[0] == "public":
        parts = parts[1:]
    if len(parts) != 2 or parts[0] != "feed":
        return None
    return _report_date_from_name(parts[1])


def _validate_html(payload: bytes, filename: str) -> None:
    if not payload or len(payload) > MAX_REPORT_BYTES:
        raise ArchiveContinuityError(f"legacy dated report has an unsafe size: {filename}")
    lowered_head = payload[:4096].lower()
    lowered_tail = payload[-4096:].lower()
    if b"<html" not in lowered_head or b"</html>" not in lowered_tail:
        raise ArchiveContinuityError(f"legacy dated report is not a complete HTML document: {filename}")


def _load_reports_from_tar(archive_path: Path) -> dict[date, bytes]:
    reports: dict[date, bytes] = {}
    total_bytes = 0
    try:
        archive = tarfile.open(archive_path, mode="r:*")
    except (OSError, tarfile.TarError) as exc:
        raise ArchiveContinuityError(f"cannot open previous Pages archive: {archive_path}") from exc

    with archive:
        for member in archive:
            report_date = _report_date_from_tar_member(member.name)
            if report_date is None:
                continue
            if not member.isfile():
                raise ArchiveContinuityError(
                    f"legacy dated report must be a regular file: {report_date.isoformat()}.html"
                )
            if report_date in reports:
                raise ArchiveContinuityError(
                    f"duplicate legacy dated report in previous artifact: {report_date.isoformat()}"
                )
            if member.size <= 0 or member.size > MAX_REPORT_BYTES:
                raise ArchiveContinuityError(
                    f"legacy dated report has an unsafe size: {report_date.isoformat()}.html"
                )
            extracted = archive.extractfile(member)
            if extracted is None:
                raise ArchiveContinuityError(
                    f"cannot read legacy dated report: {report_date.isoformat()}.html"
                )
            payload = extracted.read(MAX_REPORT_BYTES + 1)
            _validate_html(payload, f"{report_date.isoformat()}.html")
            total_bytes += len(payload)
            if total_bytes > MAX_ARCHIVE_BYTES:
                raise ArchiveContinuityError("previous Pages archive exceeds the safe size budget")
            reports[report_date] = payload
            if len(reports) > MAX_REPORT_COUNT:
                raise ArchiveContinuityError("previous Pages archive has too many dated reports")
    return reports


def _load_reports_from_directory(archive_path: Path) -> dict[date, bytes]:
    feed_dir = archive_path / "feed"
    if not feed_dir.is_dir() or feed_dir.is_symlink():
        raise ArchiveContinuityError("downloaded legacy archive does not contain a safe feed directory")

    reports: dict[date, bytes] = {}
    total_bytes = 0
    for candidate in feed_dir.iterdir():
        report_date = _report_date_from_name(candidate.name)
        if report_date is None:
            continue
        if not candidate.is_file() or candidate.is_symlink():
            raise ArchiveContinuityError(
                f"legacy dated report must be a regular file: {candidate.name}"
            )
        payload = candidate.read_bytes()
        _validate_html(payload, candidate.name)
        total_bytes += len(payload)
        if total_bytes > MAX_ARCHIVE_BYTES:
            raise ArchiveContinuityError("previous Pages archive exceeds the safe size budget")
        reports[report_date] = payload
        if len(reports) > MAX_REPORT_COUNT:
            raise ArchiveContinuityError("previous Pages archive has too many dated reports")
    return reports


def _load_reports(archive_path: Path) -> dict[date, bytes]:
    if archive_path.is_symlink():
        raise ArchiveContinuityError("previous Pages archive path must not be a symlink")
    archive_path = archive_path.resolve()
    if archive_path.is_file():
        return _load_reports_from_tar(archive_path)
    if not archive_path.is_dir():
        raise ArchiveContinuityError(f"previous Pages archive is missing: {archive_path}")
    artifact_tar = archive_path / "artifact.tar"
    if artifact_tar.is_file() and not artifact_tar.is_symlink():
        return _load_reports_from_tar(artifact_tar)
    return _load_reports_from_directory(archive_path)


def _validate_continuity(reports: dict[date, bytes]) -> None:
    if not reports:
        raise ArchiveContinuityError("previous Pages artifact has no dated legacy reports")
    first_date = min(reports)
    last_date = max(reports)
    if first_date != ARCHIVE_START_DATE:
        raise ArchiveContinuityError(
            f"legacy archive must start at {ARCHIVE_START_DATE.isoformat()}, found {first_date.isoformat()}"
        )
    if last_date < REQUIRED_ARCHIVE_END_DATE:
        raise ArchiveContinuityError(
            "legacy archive ends before the required compatibility boundary: "
            f"{last_date.isoformat()}"
        )
    if (last_date - first_date).days + 1 > MAX_REPORT_COUNT:
        raise ArchiveContinuityError("legacy archive has an unsafe dated-report range")

    missing_dates: list[str] = []
    cursor = ARCHIVE_START_DATE
    while cursor <= last_date:
        if cursor not in reports:
            missing_dates.append(cursor.isoformat())
        cursor += timedelta(days=1)
    if missing_dates:
        preview = ", ".join(missing_dates[:10])
        raise ArchiveContinuityError(f"legacy archive has missing report dates: {preview}")


def restore_legacy_archive(archive_path: Path, destination_feed: Path) -> dict[str, object]:
    if destination_feed.is_symlink():
        raise ArchiveContinuityError("legacy feed destination must not be a symlink")
    destination_parent = destination_feed.parent
    if destination_parent.is_symlink() or not destination_parent.is_dir():
        raise ArchiveContinuityError(
            f"legacy feed destination parent is missing or unsafe: {destination_parent}"
        )
    destination_feed = destination_feed.resolve()
    if destination_feed.exists() and not destination_feed.is_dir():
        raise ArchiveContinuityError(f"legacy feed destination is unsafe: {destination_feed}")
    destination_feed.mkdir(exist_ok=True)

    reports = _load_reports(archive_path)
    _validate_continuity(reports)

    restored_count = 0
    preserved_count = 0
    for report_date, payload in sorted(reports.items()):
        destination = destination_feed / f"{report_date.isoformat()}.html"
        if destination.is_symlink():
            raise ArchiveContinuityError(f"legacy feed destination is unsafe: {destination.name}")
        if destination.exists():
            if not destination.is_file():
                raise ArchiveContinuityError(f"legacy feed destination is unsafe: {destination.name}")
            preserved_count += 1
            continue
        destination.write_bytes(payload)
        restored_count += 1

    return {
        "archive_count": len(reports),
        "archive_first_date": min(reports).isoformat(),
        "archive_last_date": max(reports).isoformat(),
        "restored_count": restored_count,
        "preserved_count": preserved_count,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Restore only validated dated reports from a previous legacy Pages artifact"
    )
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--destination-feed", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = restore_legacy_archive(args.archive, args.destination_feed)
    except ArchiveContinuityError as exc:
        print(f"legacy_archive_restore_error={exc}", file=sys.stderr)
        return 1
    for key, value in result.items():
        print(f"legacy_archive_{key}={value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
