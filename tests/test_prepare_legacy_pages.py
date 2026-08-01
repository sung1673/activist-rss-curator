from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path
from types import ModuleType

import pytest


ROOT = Path(__file__).resolve().parents[1]


def load_preparer() -> ModuleType:
    path = ROOT / ".github" / "scripts" / "prepare-legacy-pages.py"
    spec = importlib.util.spec_from_file_location("bside_prepare_legacy_pages", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_preparer_cli_can_import_curator_from_the_repository_root() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / ".github" / "scripts" / "prepare-legacy-pages.py"),
            "--help",
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    assert "Prepare the allowlisted legacy GitHub Pages artifact" in completed.stdout


def build_source(root: Path) -> Path:
    source = root / "public"
    feed = source / "feed"
    governance = source / "governance"
    feed.mkdir(parents=True)
    governance.mkdir()
    for filename in ("CNAME", "404.html", "feed.xml", "index.html"):
        (source / filename).write_text(filename, encoding="utf-8")
    for filename in (
        "index.html",
        "latest.html",
        "search.html",
        "telegram-admin.html",
        "telegram.html",
    ):
        (feed / filename).write_text(filename, encoding="utf-8")
    (feed / "search.html").write_text(
        '<!doctype html><html><body><a href="telegram.html">signal</a>'
        '<script>fetch("https://t.me/private/42")</script></body></html>',
        encoding="utf-8",
    )
    cursor = date(2026, 5, 1)
    while cursor <= date(2026, 7, 21):
        filename = f"{cursor.isoformat()}.html"
        (feed / filename).write_text(filename, encoding="utf-8")
        cursor += timedelta(days=1)
    (feed / "2026-06-01.html").write_text(
        "<!doctype html><html><body>"
        '<a href="article.html">원문 기사</a>'
        '<a href="telegram.html">Telegram 데일리 보기</a>'
        "<p>기사·이슈·Telegram 신호를 함께 보려면 별도 검색 화면에서 확인하세요.</p>"
        "<style>.story{color:black}.story-context__telegram{display:grid}</style>"
        '<script type="application/json" data-story-telegram-mentions>'
        '[{"message_url":"https://t.me/private/42","text":"signal"}]'
        "</script>"
        "<script>"
        "function telegramChannelUrl(handle) {"
        "return handle ? `https://t.me/${handle}` : '';"
        "}"
        "</script></body></html>",
        encoding="utf-8",
    )
    for filename in ("app.js", "config.js", "index.html", "styles.css"):
        (governance / filename).write_text(f"preview {filename}", encoding="utf-8")
    (source / "unexpected.txt").write_text("must not ship", encoding="utf-8")
    return source


def test_preparer_copies_only_the_legacy_allowlist(tmp_path: Path) -> None:
    preparer = load_preparer()
    source = build_source(tmp_path)
    destination = tmp_path / "artifact"

    result = preparer.prepare_legacy_pages(source, destination)

    assert result["root_paths"] == ["404.html", "CNAME", "feed", "feed.xml", "index.html"]
    assert result["file_count"] == 89
    assert not (destination / "feed" / "telegram-admin.html").exists()
    assert not (destination / "feed" / "telegram.html").exists()
    assert (destination / "feed.xml").is_file()
    assert len(list((destination / "feed").glob("20*.html"))) == 82
    redacted = (destination / "feed" / "2026-06-01.html").read_text(
        encoding="utf-8"
    )
    assert "telegram" not in redacted.casefold()
    assert '<a href="article.html">원문 기사</a>' in redacted
    assert ".story{color:black}" in redacted
    safe_search = (destination / "feed" / "search.html").read_text(
        encoding="utf-8"
    )
    assert "telegram" not in safe_search.casefold()
    assert 'href="latest.html"' in safe_search
    assert 'href="index.html"' in safe_search
    assert not (destination / "governance").exists()
    assert not (destination / "unexpected.txt").exists()


def test_preparer_can_mount_only_the_public_governance_preview_subpath(tmp_path: Path) -> None:
    preparer = load_preparer()
    source = build_source(tmp_path)
    destination = tmp_path / "artifact"

    result = preparer.prepare_legacy_pages(
        source,
        destination,
        governance_preview=source / "governance",
    )

    assert result["root_paths"] == [
        "404.html",
        "CNAME",
        "feed",
        "feed.xml",
        "governance",
        "index.html",
    ]
    assert result["file_count"] == 93
    assert sorted(path.name for path in (destination / "governance").iterdir()) == [
        "app.js",
        "config.js",
        "index.html",
        "styles.css",
    ]


def test_preparer_accepts_sources_without_the_known_dropped_telegram_pages(
    tmp_path: Path,
) -> None:
    preparer = load_preparer()
    source = build_source(tmp_path)
    (source / "feed" / "telegram-admin.html").unlink()
    (source / "feed" / "telegram.html").unlink()

    result = preparer.prepare_legacy_pages(source, tmp_path / "artifact")

    assert result["file_count"] == 89


def test_preparer_rejects_unexpected_governance_preview_assets(tmp_path: Path) -> None:
    preparer = load_preparer()
    source = build_source(tmp_path)
    (source / "governance" / "admin.html").write_text("private", encoding="utf-8")

    with pytest.raises(preparer.PreparationError, match="only its four public assets"):
        preparer.prepare_legacy_pages(
            source,
            tmp_path / "artifact",
            governance_preview=source / "governance",
        )


@pytest.mark.parametrize("filename", ["story-review.html", "story-review-meta.json"])
def test_preparer_rejects_private_review_files(tmp_path: Path, filename: str) -> None:
    preparer = load_preparer()
    source = build_source(tmp_path)
    (source / "feed" / filename).write_text("private", encoding="utf-8")

    with pytest.raises(preparer.PreparationError, match="private review files"):
        preparer.prepare_legacy_pages(source, tmp_path / "artifact")


def test_preparer_fails_closed_when_required_output_is_missing(tmp_path: Path) -> None:
    preparer = load_preparer()
    source = build_source(tmp_path)
    (source / "feed.xml").unlink()

    with pytest.raises(preparer.PreparationError, match="feed.xml"):
        preparer.prepare_legacy_pages(source, tmp_path / "artifact")


def test_preparer_rejects_a_telegram_url_outside_the_redactable_payload(
    tmp_path: Path,
) -> None:
    preparer = load_preparer()
    source = build_source(tmp_path)
    (source / "feed" / "2026-06-01.html").write_text(
        '<!doctype html><html><body><a href="https://t.me/private/42">signal</a>'
        "</body></html>",
        encoding="utf-8",
    )

    with pytest.raises(preparer.PreparationError, match="Telegram URL"):
        preparer.prepare_legacy_pages(source, tmp_path / "artifact")


@pytest.mark.parametrize("relative_path", ["debug.json", "variants/debug.html"])
def test_preparer_rejects_unexpected_feed_output(
    tmp_path: Path, relative_path: str
) -> None:
    preparer = load_preparer()
    source = build_source(tmp_path)
    unexpected = source / "feed" / relative_path
    unexpected.parent.mkdir(parents=True, exist_ok=True)
    unexpected.write_text("private debug output", encoding="utf-8")

    with pytest.raises(preparer.PreparationError, match="unexpected path"):
        preparer.prepare_legacy_pages(source, tmp_path / "artifact")


def test_preparer_rejects_invalid_or_missing_dated_report(tmp_path: Path) -> None:
    preparer = load_preparer()
    source = build_source(tmp_path)
    (source / "feed" / "2026-07-21.html").rename(
        source / "feed" / "2026-99-99.html"
    )

    with pytest.raises(preparer.PreparationError, match="unexpected path"):
        preparer.prepare_legacy_pages(source, tmp_path / "artifact")


def test_preparer_rejects_gap_in_dated_reports(tmp_path: Path) -> None:
    preparer = load_preparer()
    source = build_source(tmp_path)
    (source / "feed" / "2026-06-01.html").unlink()

    with pytest.raises(preparer.PreparationError, match="missing report dates"):
        preparer.prepare_legacy_pages(source, tmp_path / "artifact")


def test_preparer_rejects_overlapping_or_existing_destinations(tmp_path: Path) -> None:
    preparer = load_preparer()
    source = build_source(tmp_path)

    with pytest.raises(preparer.PreparationError, match="separate directory trees"):
        preparer.prepare_legacy_pages(source, source / "artifact")

    destination = tmp_path / "artifact"
    destination.mkdir()
    with pytest.raises(preparer.PreparationError, match="must not already exist"):
        preparer.prepare_legacy_pages(source, destination)


def test_preparer_rejects_symlinks_when_supported(tmp_path: Path) -> None:
    preparer = load_preparer()
    source = build_source(tmp_path)
    link = source / "feed" / "linked.html"
    try:
        os.symlink(source / "feed" / "index.html", link)
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation is not available")

    with pytest.raises(preparer.PreparationError, match="symlinks are not allowed"):
        preparer.prepare_legacy_pages(source, tmp_path / "artifact")


def test_preparer_rejects_source_directory_symlink_when_supported(tmp_path: Path) -> None:
    preparer = load_preparer()
    source = build_source(tmp_path)
    linked_source = tmp_path / "linked-public"
    try:
        os.symlink(source, linked_source, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("directory symlink creation is not available")

    with pytest.raises(preparer.PreparationError, match="source directory must not be a symlink"):
        preparer.prepare_legacy_pages(linked_source, tmp_path / "artifact")
