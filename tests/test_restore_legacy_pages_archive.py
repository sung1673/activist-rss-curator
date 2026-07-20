from __future__ import annotations

import importlib.util
import io
import tarfile
from datetime import date, timedelta
from pathlib import Path
from types import ModuleType

import pytest


ROOT = Path(__file__).resolve().parents[1]
START_DATE = date(2026, 5, 1)
END_DATE = date(2026, 7, 21)


def load_restorer() -> ModuleType:
    path = ROOT / ".github" / "scripts" / "restore-legacy-pages-archive.py"
    spec = importlib.util.spec_from_file_location("bside_restore_legacy_pages_archive", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def html_payload(report_date: date, marker: str = "archive") -> bytes:
    return (
        f"<!doctype html><html><body>{marker}:{report_date.isoformat()}</body></html>"
    ).encode()


def add_tar_file(archive: tarfile.TarFile, name: str, payload: bytes) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(payload)
    info.mode = 0o644
    archive.addfile(info, io.BytesIO(payload))


def build_pages_tar(
    path: Path,
    *,
    missing: set[date] | None = None,
    unsafe_date_link: tuple[date, bytes] | None = None,
) -> Path:
    missing = missing or set()
    with tarfile.open(path, mode="w") as archive:
        cursor = START_DATE
        while cursor <= END_DATE:
            if cursor not in missing:
                name = f"./feed/{cursor.isoformat()}.html"
                if unsafe_date_link is not None and cursor == unsafe_date_link[0]:
                    info = tarfile.TarInfo(name)
                    info.type = unsafe_date_link[1]
                    info.linkname = "../story-review.html"
                    archive.addfile(info)
                else:
                    add_tar_file(archive, name, html_payload(cursor))
            cursor += timedelta(days=1)
        add_tar_file(archive, "./feed/story-review.html", b"private")
        add_tar_file(archive, "./governance/index.html", b"not legacy")
        add_tar_file(archive, "./feed/debug.json", b"private debug")
    return path


def test_restore_keeps_current_page_and_restores_only_dated_reports(tmp_path: Path) -> None:
    restorer = load_restorer()
    archive_path = build_pages_tar(tmp_path / "artifact.tar")
    destination = tmp_path / "public" / "feed"
    destination.mkdir(parents=True)
    current_page = destination / f"{END_DATE.isoformat()}.html"
    current_page.write_bytes(html_payload(END_DATE, marker="current"))

    result = restorer.restore_legacy_archive(archive_path, destination)

    assert result["archive_count"] == 82
    assert result["restored_count"] == 81
    assert result["preserved_count"] == 1
    assert b"current" in current_page.read_bytes()
    assert (destination / f"{START_DATE.isoformat()}.html").exists()
    assert not (destination / "story-review.html").exists()
    assert not (destination / "debug.json").exists()
    assert not (destination.parent / "governance").exists()


def test_restore_accepts_sanitized_seed_directory(tmp_path: Path) -> None:
    restorer = load_restorer()
    archive_root = tmp_path / "seed"
    archive_feed = archive_root / "feed"
    archive_feed.mkdir(parents=True)
    cursor = START_DATE
    while cursor <= END_DATE:
        (archive_feed / f"{cursor.isoformat()}.html").write_bytes(html_payload(cursor))
        cursor += timedelta(days=1)
    (archive_feed / "story-review.html").write_text("ignored", encoding="utf-8")
    destination = tmp_path / "public" / "feed"
    destination.parent.mkdir()

    result = restorer.restore_legacy_archive(archive_root, destination)

    assert result["archive_count"] == 82
    assert result["restored_count"] == 82
    assert destination.is_dir()
    assert not (destination / "story-review.html").exists()


def test_restore_fails_closed_on_missing_archive_date(tmp_path: Path) -> None:
    restorer = load_restorer()
    missing_date = date(2026, 6, 1)
    archive_path = build_pages_tar(tmp_path / "artifact.tar", missing={missing_date})
    destination = tmp_path / "public" / "feed"
    destination.mkdir(parents=True)

    with pytest.raises(restorer.ArchiveContinuityError, match="missing report dates"):
        restorer.restore_legacy_archive(archive_path, destination)


@pytest.mark.parametrize("link_type", [tarfile.SYMTYPE, tarfile.LNKTYPE])
def test_restore_rejects_dated_report_links(tmp_path: Path, link_type: bytes) -> None:
    restorer = load_restorer()
    archive_path = build_pages_tar(
        tmp_path / "artifact.tar", unsafe_date_link=(START_DATE, link_type)
    )
    destination = tmp_path / "public" / "feed"
    destination.mkdir(parents=True)

    with pytest.raises(restorer.ArchiveContinuityError, match="regular file"):
        restorer.restore_legacy_archive(archive_path, destination)


def test_restore_rejects_duplicate_dated_tar_member(tmp_path: Path) -> None:
    restorer = load_restorer()
    archive_path = build_pages_tar(tmp_path / "artifact.tar")
    with tarfile.open(archive_path, mode="a") as archive:
        add_tar_file(
            archive,
            f"./feed/{START_DATE.isoformat()}.html",
            html_payload(START_DATE, marker="duplicate"),
        )
    destination = tmp_path / "public" / "feed"
    destination.mkdir(parents=True)

    with pytest.raises(restorer.ArchiveContinuityError, match="duplicate legacy dated report"):
        restorer.restore_legacy_archive(archive_path, destination)


def test_restore_rejects_missing_destination_parent(tmp_path: Path) -> None:
    restorer = load_restorer()
    archive_path = build_pages_tar(tmp_path / "artifact.tar")

    with pytest.raises(restorer.ArchiveContinuityError, match="parent is missing or unsafe"):
        restorer.restore_legacy_archive(
            archive_path, tmp_path / "missing-public" / "feed"
        )


def test_restore_rejects_truncated_html(tmp_path: Path) -> None:
    restorer = load_restorer()
    archive_path = tmp_path / "artifact.tar"
    with tarfile.open(archive_path, mode="w") as archive:
        cursor = START_DATE
        while cursor <= END_DATE:
            payload = b"truncated" if cursor == START_DATE else html_payload(cursor)
            add_tar_file(archive, f"./feed/{cursor.isoformat()}.html", payload)
            cursor += timedelta(days=1)
    destination = tmp_path / "public" / "feed"
    destination.mkdir(parents=True)

    with pytest.raises(restorer.ArchiveContinuityError, match="complete HTML"):
        restorer.restore_legacy_archive(archive_path, destination)
