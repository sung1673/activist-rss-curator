#!/usr/bin/env python3
"""Remove a preview credential from Playwright artifacts before upload."""

from __future__ import annotations

import argparse
import os
import re
import tempfile
import zipfile
from pathlib import Path
from typing import Iterable, Sequence


TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9._~-]{32,512}$")
REPLACEMENT = b"[REDACTED_PREVIEW_TOKEN]"


class ArtifactSanitizationError(ValueError):
    """An artifact set is missing, unsafe, or could not be sanitized."""


def _files(root: Path) -> tuple[Path, ...]:
    return tuple(
        path
        for path in sorted(root.rglob("*"))
        if path.is_file() and not path.is_symlink()
    )


def _redact_plain(path: Path, token: bytes) -> bool:
    value = path.read_bytes()
    if token not in value:
        return False
    path.write_bytes(value.replace(token, REPLACEMENT))
    return True


def _redact_zip(path: Path, token: bytes) -> bool:
    changed = False
    with zipfile.ZipFile(path, "r") as source:
        entries = tuple((item, source.read(item.filename)) for item in source.infolist())
    for item, value in entries:
        if token in item.filename.encode("utf-8", errors="surrogateescape") or token in value:
            changed = True
            break
    if not changed:
        return False

    with tempfile.NamedTemporaryFile(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        delete=False,
    ) as stream:
        temporary = Path(stream.name)
    try:
        with zipfile.ZipFile(temporary, "w") as target:
            for item, value in entries:
                safe_name = item.filename.replace(
                    token.decode("ascii"),
                    REPLACEMENT.decode("ascii"),
                )
                item.filename = safe_name
                target.writestr(item, value.replace(token, REPLACEMENT))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return True


def _assert_clean(paths: Iterable[Path], token: bytes) -> None:
    for path in paths:
        if token in str(path).encode("utf-8", errors="surrogateescape"):
            raise ArtifactSanitizationError("preview token remains in an artifact path")
        if zipfile.is_zipfile(path):
            with zipfile.ZipFile(path, "r") as archive:
                for item in archive.infolist():
                    if (
                        token in item.filename.encode("utf-8", errors="surrogateescape")
                        or token in archive.read(item.filename)
                    ):
                        raise ArtifactSanitizationError(
                            "preview token remains in a compressed artifact"
                        )
        elif token in path.read_bytes():
            raise ArtifactSanitizationError("preview token remains in an artifact")


def sanitize_artifacts(root: Path, token_value: str) -> int:
    token = token_value.strip()
    if TOKEN_PATTERN.fullmatch(token) is None:
        raise ArtifactSanitizationError("invalid preview token")
    resolved = root.resolve()
    if not resolved.is_dir():
        raise ArtifactSanitizationError("artifact directory does not exist")
    paths = _files(resolved)
    if not paths:
        raise ArtifactSanitizationError("artifact directory is empty")
    token_bytes = token.encode("ascii")
    changed = 0
    for path in paths:
        sanitized = (
            _redact_zip(path, token_bytes)
            if zipfile.is_zipfile(path)
            else _redact_plain(path, token_bytes)
        )
        changed += int(sanitized)
    _assert_clean(_files(resolved), token_bytes)
    return changed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Redact GOVERNANCE_PREVIEW_TOKEN from Playwright artifacts",
    )
    parser.add_argument("artifact_root", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        changed = sanitize_artifacts(
            args.artifact_root,
            os.environ.get("GOVERNANCE_PREVIEW_TOKEN", ""),
        )
    except ArtifactSanitizationError as exc:
        print(f"::error::{exc}")
        return 1
    print(f"Preview artifacts verified; sanitized file count: {changed}.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
