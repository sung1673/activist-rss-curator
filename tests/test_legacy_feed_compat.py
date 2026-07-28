from __future__ import annotations

import hashlib
import json
import stat
import warnings
import zipfile
from datetime import date, timedelta
from pathlib import Path

import pytest

from curator.legacy_feed_compat import (
    MANIFEST_NAME,
    LegacyArtifactIdentity,
    LegacyFeedCompatibilityError,
    prepare_legacy_feed_compatibility,
    verify_legacy_feed_compatibility,
)


WINDOW_END = date(2026, 7, 22)


def html_payload(report_date: date) -> bytes:
    return f"<!doctype html><html><body>{report_date.isoformat()}</body></html>".encode()


def build_archive(
    path: Path,
    *,
    report_days: int = 90,
    missing: set[date] | None = None,
    duplicate: bool = False,
    unsafe_path: bool = False,
    symlink: bool = False,
    embedded_telegram: bool = False,
) -> Path:
    missing = missing or set()
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("feed.xml", "<rss><channel/></rss>")
        archive.writestr("CNAME", "news.bside.ai\n")
        archive.writestr("404.html", "<!doctype html><html><body>404</body></html>")
        for offset in range(report_days):
            report_date = WINDOW_END - timedelta(days=report_days - offset - 1)
            if report_date in missing:
                continue
            payload = html_payload(report_date)
            if embedded_telegram and report_date == WINDOW_END:
                payload = (
                    b"<!doctype html><html><body>"
                    b"<script data-story-telegram-mentions>"
                    b'[{"message_url":"https://t.me/private/42"}]'
                    b"</script></body></html>"
                )
            archive.writestr(
                f"feed/{report_date.isoformat()}.html",
                payload,
            )
        if duplicate:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                archive.writestr(
                    f"feed/{WINDOW_END.isoformat()}.html",
                    html_payload(WINDOW_END),
                )
        if unsafe_path:
            archive.writestr("../feed/2026-01-01.html", html_payload(date(2026, 1, 1)))
        if symlink:
            link = zipfile.ZipInfo("feed/2026-01-01.html")
            link.create_system = 3
            link.external_attr = (stat.S_IFLNK | 0o777) << 16
            archive.writestr(link, "../404.html")
    return path


def identity_for(path: Path) -> LegacyArtifactIdentity:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return LegacyArtifactIdentity(
        run_id="12345",
        artifact_id="67890",
        artifact_name="legacy-pages-archive-seed",
        code_revision="a" * 40,
        artifact_digest=f"sha256:{digest}",
    )


def test_prepare_copies_exact_latest_90_days_and_writes_provenance(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "legacy.zip", report_days=95)
    output = tmp_path / "compat"
    identity = identity_for(archive)

    manifest = prepare_legacy_feed_compatibility(archive, output, identity=identity)

    reports = sorted((output / "feed").glob("*.html"))
    assert len(reports) == 90
    assert reports[0].name == (WINDOW_END - timedelta(days=89)).isoformat() + ".html"
    assert reports[-1].name == WINDOW_END.isoformat() + ".html"
    assert manifest["window_days"] == 90
    assert manifest["dated_report_count"] == 90
    assert manifest["source"]["workflow"] == ".github/workflows/build-feed.yml"
    assert manifest["source"]["artifact_digest"] == identity.artifact_digest
    assert (output / "feed.xml").read_text(encoding="utf-8") == "<rss><channel/></rss>"
    assert verify_legacy_feed_compatibility(output, expected_identity=identity) == manifest


def test_prepare_rejects_artifact_digest_mismatch(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "legacy.zip")
    identity = identity_for(archive)
    wrong = LegacyArtifactIdentity(
        run_id=identity.run_id,
        artifact_id=identity.artifact_id,
        artifact_name=identity.artifact_name,
        code_revision=identity.code_revision,
        artifact_digest="sha256:" + "0" * 64,
    )

    with pytest.raises(LegacyFeedCompatibilityError, match="digest does not match"):
        prepare_legacy_feed_compatibility(archive, tmp_path / "compat", identity=wrong)


def test_prepare_redacts_embedded_telegram_exposure(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "legacy.zip", embedded_telegram=True)
    output = tmp_path / "compat"
    identity = identity_for(archive)

    prepare_legacy_feed_compatibility(archive, output, identity=identity)

    report = (output / "feed" / f"{WINDOW_END.isoformat()}.html").read_bytes()
    assert b"data-story-telegram-mentions>[]</script>" in report
    assert b"https://t.me/" not in report
    assert verify_legacy_feed_compatibility(output, expected_identity=identity)


def test_prepare_fails_closed_when_90_continuous_days_do_not_exist(tmp_path: Path) -> None:
    missing = {WINDOW_END - timedelta(days=10)}
    archive = build_archive(tmp_path / "legacy.zip", report_days=91, missing=missing)

    with pytest.raises(LegacyFeedCompatibilityError, match="continuous 90-day window"):
        prepare_legacy_feed_compatibility(
            archive,
            tmp_path / "compat",
            identity=identity_for(archive),
        )


@pytest.mark.parametrize(
    ("options", "message"),
    [
        ({"duplicate": True}, "duplicate path"),
        ({"unsafe_path": True}, "unsafe path"),
        ({"symlink": True}, "symbolic link"),
    ],
)
def test_prepare_rejects_duplicate_traversal_and_symlink_members(
    tmp_path: Path,
    options: dict[str, bool],
    message: str,
) -> None:
    archive = build_archive(tmp_path / "legacy.zip", **options)

    with pytest.raises(LegacyFeedCompatibilityError, match=message):
        prepare_legacy_feed_compatibility(
            archive,
            tmp_path / "compat",
            identity=identity_for(archive),
        )


def test_verify_rejects_tampered_report_and_manifest_count(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "legacy.zip")
    output = tmp_path / "compat"
    identity = identity_for(archive)
    prepare_legacy_feed_compatibility(archive, output, identity=identity)
    manifest_path = output / MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["dated_report_count"] = 89
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(LegacyFeedCompatibilityError, match="count must be exactly 90"):
        verify_legacy_feed_compatibility(output, expected_identity=identity)


def test_verify_rejects_reintroduced_telegram_exposure(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "legacy.zip")
    output = tmp_path / "compat"
    identity = identity_for(archive)
    prepare_legacy_feed_compatibility(archive, output, identity=identity)
    report = output / "feed" / f"{WINDOW_END.isoformat()}.html"
    report.write_bytes(
        b"<!doctype html><html><body>"
        b"<script data-story-telegram-mentions>"
        b'[{"message_url":"https://t.me/private/42"}]'
        b"</script></body></html>"
    )

    with pytest.raises(LegacyFeedCompatibilityError, match="Telegram"):
        verify_legacy_feed_compatibility(output, expected_identity=identity)


def test_verify_rejects_source_pin_change(tmp_path: Path) -> None:
    archive = build_archive(tmp_path / "legacy.zip")
    output = tmp_path / "compat"
    identity = identity_for(archive)
    prepare_legacy_feed_compatibility(archive, output, identity=identity)
    other = LegacyArtifactIdentity(
        run_id="99999",
        artifact_id=identity.artifact_id,
        artifact_name=identity.artifact_name,
        code_revision=identity.code_revision,
        artifact_digest=identity.artifact_digest,
    )

    with pytest.raises(LegacyFeedCompatibilityError, match="current pin"):
        verify_legacy_feed_compatibility(output, expected_identity=other)
