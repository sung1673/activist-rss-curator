#!/usr/bin/env python3
"""Build a test-only API copy with unbuffered native PDO MySQL queries."""

from __future__ import annotations

import argparse
import hashlib
import shutil
from pathlib import Path


class FixtureError(RuntimeError):
    """Raised when the isolated PHP fixture cannot be built exactly."""


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def prepare_fixture(source: Path, output: Path) -> None:
    source = source.resolve()
    output = output.resolve()
    if source.is_symlink() or not source.is_dir():
        raise FixtureError("source must be a regular deployment directory")
    if output.exists() or output.is_symlink():
        raise FixtureError("output must not already exist")

    source_api = source / "api.php"
    if not source_api.is_file() or source_api.is_symlink():
        raise FixtureError("source api.php is not a regular file")
    source_api_hash = sha256(source_api)
    shutil.copytree(source, output)
    api_path = output / "api.php"
    governance_path = output / "governance_v1.php"
    source_governance = source / "governance_v1.php"
    if not api_path.is_file() or api_path.is_symlink():
        raise FixtureError("copied api.php is not a regular file")
    if not governance_path.is_file() or governance_path.is_symlink():
        raise FixtureError("copied governance_v1.php is not a regular file")

    payload = api_path.read_bytes()
    if b"PDO::MYSQL_ATTR_USE_BUFFERED_QUERY" in payload:
        raise FixtureError("source API already configures buffered-query behavior")

    newline = b"\r\n" if b"\r\n" in payload else b"\n"
    native_prepare = (
        b"        PDO::ATTR_EMULATE_PREPARES => false," + newline
    )
    unbuffered = (
        native_prepare
        + b"        PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => false,"
        + newline
    )
    if payload.count(native_prepare) != 1:
        raise FixtureError(
            "expected exactly one native PDO prepare configuration"
        )
    api_path.write_bytes(payload.replace(native_prepare, unbuffered, 1))

    changed = api_path.read_bytes()
    if changed.count(b"PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => false") != 1:
        raise FixtureError("unbuffered PDO fixture injection was not exact")
    if sha256(source_governance) != sha256(governance_path):
        raise FixtureError("governance_v1.php changed while preparing fixture")
    if sha256(source_api) != source_api_hash:
        raise FixtureError("source api.php changed while preparing fixture")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    prepare_fixture(args.source, args.output)
    print(
        "PHP 7.3 unbuffered fixture prepared without changing governance_v1.php.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
