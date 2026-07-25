from __future__ import annotations

import json
from pathlib import Path

import pytest

from curator.global_alpha_pages_identity import (
    PagesArtifactIdentityError,
    build_pages_artifact_binding,
    build_pages_content_identity,
    main,
    validate_pages_artifact_binding,
)


REVISION = "a" * 40
DIGEST = "sha256:" + ("b" * 64)


def write_site(root: Path) -> None:
    (root / "governance").mkdir(parents=True)
    assets = {
        "index.html": b'<main id="app"></main>\n',
        "config.js": b"window.__CONFIG__={};\n",
        "app.js": b"window.__APP__=true;\n",
        "styles.css": b":root{color:#111827}\n",
    }
    for prefix in (Path(), Path("governance")):
        for name, content in assets.items():
            (root / prefix / name).write_bytes(content)
    (root / "feed.xml").write_bytes(b"<feed/>\n")


def binding(site: Path) -> dict[str, object]:
    return build_pages_artifact_binding(
        site,
        code_revision=REVISION,
        producer_run_id=123,
        producer_run_attempt=2,
        artifact_id=456,
        artifact_name="pages-123-2",
        artifact_digest=DIGEST,
    )


def test_content_identity_covers_full_site_and_exact_terminal_bytes(
    tmp_path: Path,
) -> None:
    write_site(tmp_path)
    first = build_pages_content_identity(tmp_path)
    second = build_pages_content_identity(tmp_path)
    assert first == second
    assert first["site"]["file_count"] == 9  # type: ignore[index]
    assert first["terminal"]["file_count"] == 4  # type: ignore[index]

    original_terminal = first["terminal"]["sha256"]  # type: ignore[index]
    original_site = first["site"]["sha256"]  # type: ignore[index]
    (tmp_path / "feed.xml").write_bytes(b"<feed>changed</feed>\n")
    feed_changed = build_pages_content_identity(tmp_path)
    assert feed_changed["terminal"]["sha256"] == original_terminal  # type: ignore[index]
    assert feed_changed["site"]["sha256"] != original_site  # type: ignore[index]

    (tmp_path / "governance" / "app.js").write_bytes(b"tampered\n")
    with pytest.raises(PagesArtifactIdentityError, match="terminal assets differ"):
        build_pages_content_identity(tmp_path)


def test_binding_is_strictly_run_artifact_and_digest_bound(tmp_path: Path) -> None:
    write_site(tmp_path)
    value = binding(tmp_path)
    assert validate_pages_artifact_binding(
        value,
        expected_revision=REVISION,
    ) == value

    wrong_name = dict(value)
    wrong_name["artifact_name"] = "pages-999-1"
    with pytest.raises(PagesArtifactIdentityError, match="not run-bound"):
        validate_pages_artifact_binding(wrong_name)

    wrong_revision = dict(value)
    wrong_revision["code_revision"] = "c" * 40
    with pytest.raises(PagesArtifactIdentityError, match="release candidate"):
        validate_pages_artifact_binding(
            wrong_revision,
            expected_revision=REVISION,
        )


def test_cli_verify_rejects_any_full_site_content_change(tmp_path: Path) -> None:
    site = tmp_path / "site"
    write_site(site)
    output = tmp_path / "binding.json"
    assert (
        main(
            [
                "create",
                "--site",
                str(site),
                "--code-revision",
                REVISION,
                "--producer-run-id",
                "123",
                "--producer-run-attempt",
                "2",
                "--artifact-id",
                "456",
                "--artifact-name",
                "pages-123-2",
                "--artifact-digest",
                DIGEST,
                "--output",
                str(output),
            ]
        )
        == 0
    )
    persisted = json.loads(output.read_text(encoding="utf-8"))
    assert persisted["artifact_id"] == 456
    assert (
        main(
            [
                "verify",
                "--site",
                str(site),
                "--binding",
                str(output),
                "--expected-revision",
                REVISION,
            ]
        )
        == 0
    )

    (site / "feed.xml").write_bytes(b"<feed>changed</feed>\n")
    with pytest.raises(
        PagesArtifactIdentityError,
        match="do not match the evidence-bound artifact",
    ):
        main(
            [
                "verify",
                "--site",
                str(site),
                "--binding",
                str(output),
                "--expected-revision",
                REVISION,
            ]
        )
