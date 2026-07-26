from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Mapping


MANIFEST_SCHEMA_VERSION = 1
MANIFEST_FILENAME = "deployment-manifest.json"
CORE_API_FILES = (
    ".htaccess",
    "api.php",
    "governance_v1.php",
    "governance_v2.php",
    "governance_v2_write.php",
    "openapi.yaml",
    "openapi-v2.yaml",
    "migrations/011_global_terminal_v2.sql",
    "migrations/012_dart_credential_pool.sql",
)
SHA_PATTERN = re.compile(r"^[0-9a-f]{40}$")


class DeploymentManifestError(ValueError):
    """Raised when an API deployment identity cannot be built safely."""


def _regular_file(path: Path, *, label: str) -> Path:
    if path.is_symlink() or not path.is_file():
        raise DeploymentManifestError(f"{label} must be a regular file: {path}")
    return path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _contained_regular_file(root: Path, relative_name: str) -> Path:
    relative = Path(relative_name)
    if (
        relative.is_absolute()
        or relative_name != relative.as_posix()
        or not relative.parts
        or any(part in {"", ".", ".."} for part in relative.parts)
    ):
        raise DeploymentManifestError(
            f"core API file path must be canonical and relative: {relative_name}"
        )
    candidate = root
    for index, part in enumerate(relative.parts):
        candidate = candidate / part
        if candidate.is_symlink():
            raise DeploymentManifestError(
                f"core API file path must not contain symlinks: {relative_name}"
            )
        if index < len(relative.parts) - 1 and not candidate.is_dir():
            raise DeploymentManifestError(
                f"core API file parent must be a directory: {relative_name}"
            )
    candidate = _regular_file(
        candidate,
        label=f"core API file {relative_name}",
    )
    try:
        resolved_relative = candidate.resolve().relative_to(root)
    except ValueError as error:
        raise DeploymentManifestError(
            f"core API file escapes deployment root: {relative_name}"
        ) from error
    if resolved_relative.as_posix() != relative_name:
        raise DeploymentManifestError(
            f"core API file resolves to an unexpected path: {relative_name}"
        )
    return candidate


def build_deployment_manifest(
    root: Path,
    *,
    code_revision: str,
) -> dict[str, object]:
    if SHA_PATTERN.fullmatch(code_revision) is None:
        raise DeploymentManifestError(
            "code_revision must be an exact lowercase 40-character Git SHA"
        )
    if root.is_symlink() or not root.is_dir():
        raise DeploymentManifestError(f"deployment root must be a directory: {root}")
    root = root.resolve()
    hashes: dict[str, str] = {}
    for relative_name in CORE_API_FILES:
        candidate = _contained_regular_file(root, relative_name)
        hashes[relative_name] = _sha256(candidate)
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "code_revision": code_revision,
        "files": hashes,
    }


def encode_deployment_manifest(manifest: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            manifest,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def write_deployment_manifest(
    root: Path,
    *,
    code_revision: str,
    output: Path | None = None,
) -> dict[str, object]:
    if root.is_symlink() or not root.is_dir():
        raise DeploymentManifestError(f"deployment root must be a directory: {root}")
    root = root.resolve()
    requested_destination = output or root / MANIFEST_FILENAME
    if requested_destination.is_symlink():
        raise DeploymentManifestError("deployment manifest must not be a symlink")
    destination = requested_destination.resolve()
    if destination != root / MANIFEST_FILENAME:
        raise DeploymentManifestError(
            f"output must be {root / MANIFEST_FILENAME}"
        )
    manifest = build_deployment_manifest(root, code_revision=code_revision)
    encoded = encode_deployment_manifest(manifest)
    temporary_name: str | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            prefix=".deployment-manifest.",
            suffix=".tmp",
            dir=str(root),
            delete=False,
        ) as handle:
            temporary_name = handle.name
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_name, destination)
        temporary_name = None
    finally:
        if temporary_name is not None:
            Path(temporary_name).unlink(missing_ok=True)
    return manifest


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the fail-closed BSIDE API deployment identity manifest"
    )
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--code-revision", required=True)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    manifest = write_deployment_manifest(
        args.root,
        code_revision=args.code_revision,
        output=args.output,
    )
    print(
        "API deployment manifest built: "
        f"revision={manifest['code_revision']} files={len(CORE_API_FILES)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
