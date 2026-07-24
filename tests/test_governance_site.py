from __future__ import annotations

import hashlib
import zipfile
from datetime import date, timedelta
from pathlib import Path

import pytest

from curator.governance_site import build_governance_site
from curator.legacy_feed_compat import LegacyArtifactIdentity, prepare_legacy_feed_compatibility


ROOT = Path(__file__).resolve().parents[1]


def prepared_legacy(tmp_path: Path) -> Path:
    archive = tmp_path / "legacy.zip"
    end = date(2026, 7, 22)
    with zipfile.ZipFile(archive, "w") as bundle:
        bundle.writestr("CNAME", "news.example.test\n")
        bundle.writestr("404.html", "<!doctype html><html><body>not found</body></html>")
        bundle.writestr("feed.xml", "<rss/>")
        for offset in range(90):
            report_date = end - timedelta(days=89 - offset)
            bundle.writestr(
                f"feed/{report_date.isoformat()}.html",
                f"<!doctype html><html><body>{report_date.isoformat()}</body></html>",
            )
    identity = LegacyArtifactIdentity(
        run_id="123",
        artifact_id="456",
        artifact_name="legacy-pages-archive-seed",
        code_revision="b" * 40,
        artifact_digest="sha256:" + hashlib.sha256(archive.read_bytes()).hexdigest(),
    )
    legacy = tmp_path / "legacy"
    prepare_legacy_feed_compatibility(archive, legacy, identity=identity)
    return legacy


def test_governance_site_stages_root_alias_and_safe_legacy_assets(tmp_path: Path) -> None:
    legacy = prepared_legacy(tmp_path)
    (legacy / "feed" / "latest.html").write_text("legacy latest", encoding="utf-8")
    (legacy / "feed" / "index.html").write_text("legacy index", encoding="utf-8")
    (legacy / "feed" / "search.html").write_text("legacy search", encoding="utf-8")
    (legacy / "feed" / "state.json").write_text("{}", encoding="utf-8")

    output = tmp_path / "site"
    result = build_governance_site(
        ROOT,
        output=output,
        api_base="https://api.example.test/activist/api.php/api/v1",
        legacy_root=legacy,
    )

    assert (output / "index.html").read_bytes() == (output / "governance" / "index.html").read_bytes()
    assert '"apiBase":"https://api.example.test/activist/api.php/api/v1"' in (
        output / "config.js"
    ).read_text(encoding="utf-8")
    assert (output / "feed" / "2026-07-22.html").is_file()
    assert not (output / "feed" / "latest.html").exists()
    assert not (output / "feed" / "index.html").exists()
    assert not (output / "feed" / "search.html").exists()
    assert not (output / "feed" / "state.json").exists()
    assert result["compatibility_file_count"] == 94
    assert result["compatibility_report_count"] == 90
    assert result["compatibility_window_end"] == "2026-07-22"


def test_governance_site_removes_stale_output_and_never_copies_telegram_admin(tmp_path: Path) -> None:
    output = tmp_path / "site"
    output.mkdir()
    (output / "stale.txt").write_text("stale", encoding="utf-8")
    legacy = prepared_legacy(tmp_path)
    (legacy / "telegram-admin.html").write_text("secret shell", encoding="utf-8")

    build_governance_site(ROOT, output=output, legacy_root=legacy)

    assert not (output / "stale.txt").exists()
    assert not (output / "telegram-admin.html").exists()
    assert not any("telegram" in path.name.casefold() for path in output.rglob("*"))


def test_governance_site_rejects_denied_feed_asset(tmp_path: Path) -> None:
    legacy = prepared_legacy(tmp_path)
    feed = legacy / "feed"
    (feed / "story-review.html").write_text("private", encoding="utf-8")

    with pytest.raises(RuntimeError, match="denied legacy asset"):
        build_governance_site(ROOT, output=tmp_path / "site", legacy_root=legacy)


def test_governance_site_rejects_project_public_as_output() -> None:
    with pytest.raises(RuntimeError, match="dedicated staging"):
        build_governance_site(ROOT, output=ROOT / "public")


def test_governance_site_rejects_symlinked_compatibility_asset(tmp_path: Path) -> None:
    legacy = prepared_legacy(tmp_path)
    target = tmp_path / "outside.xml"
    target.write_text("<rss/>", encoding="utf-8")
    link = legacy / "feed.xml"
    link.unlink()
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation unavailable")

    with pytest.raises(RuntimeError, match="symbolic link"):
        build_governance_site(ROOT, output=tmp_path / "site", legacy_root=legacy)
