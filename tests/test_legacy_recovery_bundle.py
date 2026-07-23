from __future__ import annotations

import hashlib
import json
import stat
import warnings
import zipfile
from datetime import date, timedelta
from pathlib import Path

import pytest

from curator.legacy_feed_compat import LegacyArtifactIdentity
from curator.legacy_recovery_bundle import (
    COMPATIBILITY_DIR,
    FULL_SITE_DIR,
    MANIFEST_NAME,
    LegacyRecoveryBundleError,
    prepare_legacy_recovery_bundle,
    verify_legacy_recovery_bundle,
)


WINDOW_END = date(2026, 7, 22)


def html(value: str) -> str:
    return f"<!doctype html><html><body>{value}</body></html>"


def build_full_archive(
    path: Path,
    *,
    unexpected: bool = False,
    duplicate: bool = False,
    symlink: bool = False,
) -> Path:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("CNAME", "news.bside.ai\n")
        archive.writestr("404.html", html("404"))
        archive.writestr("feed.xml", "<rss><channel/></rss>")
        archive.writestr("index.html", html("root"))
        for name in ("index", "latest", "search", "telegram-admin", "telegram"):
            archive.writestr(f"feed/{name}.html", html(name))
        for offset in range(95):
            report_date = WINDOW_END - timedelta(days=94 - offset)
            archive.writestr(f"feed/{report_date.isoformat()}.html", html(str(report_date)))
        if unexpected:
            archive.writestr("state.json", "{}")
        if duplicate:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", UserWarning)
                archive.writestr("index.html", html("duplicate"))
        if symlink:
            link = zipfile.ZipInfo("feed/link.html")
            link.create_system = 3
            link.external_attr = (stat.S_IFLNK | 0o777) << 16
            archive.writestr(link, "../index.html")
    return path


def identity_for(path: Path) -> LegacyArtifactIdentity:
    return LegacyArtifactIdentity(
        run_id="12345",
        artifact_id="67890",
        artifact_name="legacy-pages-archive-seed",
        code_revision="a" * 40,
        artifact_digest="sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
    )


def test_prepare_and_verify_preserve_full_site_and_compatibility(tmp_path: Path) -> None:
    archive = build_full_archive(tmp_path / "legacy.zip")
    identity = identity_for(archive)
    bundle = tmp_path / "bundle"

    manifest = prepare_legacy_recovery_bundle(archive, bundle, identity=identity)

    assert (bundle / FULL_SITE_DIR / "index.html").is_file()
    assert len(list((bundle / FULL_SITE_DIR / "feed").glob("20*.html"))) == 95
    assert len(list((bundle / COMPATIBILITY_DIR / "feed").glob("*.html"))) == 90
    assert manifest["source"]["artifact_digest"] == identity.artifact_digest
    assert manifest["full_site"]["file_count"] == 104
    assert verify_legacy_recovery_bundle(bundle, expected_identity=identity) == manifest


def test_verify_rejects_changed_full_site_file(tmp_path: Path) -> None:
    archive = build_full_archive(tmp_path / "legacy.zip")
    identity = identity_for(archive)
    bundle = tmp_path / "bundle"
    prepare_legacy_recovery_bundle(archive, bundle, identity=identity)
    (bundle / FULL_SITE_DIR / "index.html").write_text(html("changed"), encoding="utf-8")

    with pytest.raises(LegacyRecoveryBundleError, match="does not match"):
        verify_legacy_recovery_bundle(bundle, expected_identity=identity)


def test_verify_rejects_manifest_inventory_tampering(tmp_path: Path) -> None:
    archive = build_full_archive(tmp_path / "legacy.zip")
    identity = identity_for(archive)
    bundle = tmp_path / "bundle"
    prepare_legacy_recovery_bundle(archive, bundle, identity=identity)
    path = bundle / MANIFEST_NAME
    manifest = json.loads(path.read_text(encoding="utf-8"))
    manifest["full_site"]["file_count"] -= 1
    path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(LegacyRecoveryBundleError, match="does not match"):
        verify_legacy_recovery_bundle(bundle, expected_identity=identity)


@pytest.mark.parametrize(
    ("option", "message"),
    [
        ("unexpected", "unexpected file"),
        ("duplicate", "duplicate path"),
        ("symlink", "symbolic link"),
    ],
)
def test_prepare_rejects_unsafe_or_unsanitized_archives(
    tmp_path: Path,
    option: str,
    message: str,
) -> None:
    archive = build_full_archive(tmp_path / "legacy.zip", **{option: True})

    with pytest.raises(LegacyRecoveryBundleError, match=message):
        prepare_legacy_recovery_bundle(
            archive,
            tmp_path / "bundle",
            identity=identity_for(archive),
        )


def test_verify_rejects_a_different_original_pin(tmp_path: Path) -> None:
    archive = build_full_archive(tmp_path / "legacy.zip")
    identity = identity_for(archive)
    bundle = tmp_path / "bundle"
    prepare_legacy_recovery_bundle(archive, bundle, identity=identity)
    changed = LegacyArtifactIdentity(
        run_id="99999",
        artifact_id=identity.artifact_id,
        artifact_name=identity.artifact_name,
        code_revision=identity.code_revision,
        artifact_digest=identity.artifact_digest,
    )

    with pytest.raises(LegacyRecoveryBundleError, match="current pin"):
        verify_legacy_recovery_bundle(bundle, expected_identity=changed)
