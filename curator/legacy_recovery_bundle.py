from __future__ import annotations

import argparse
import hashlib
import json
import re
import stat
import sys
import zipfile
from pathlib import Path, PurePosixPath
from typing import Any

from .legacy_feed_compat import (
    LegacyArtifactIdentity,
    LegacyFeedCompatibilityError,
    prepare_legacy_feed_compatibility,
    verify_legacy_feed_compatibility,
)
from .legacy_telegram_safety import (
    LegacyTelegramSafetyError,
    redact_telegram_mentions,
    validate_public_payload,
)


MANIFEST_NAME = "legacy-recovery-bundle.json"
MANIFEST_SCHEMA_VERSION = 1
FULL_SITE_DIR = "full-site"
COMPATIBILITY_DIR = "compatibility"
MAX_ARCHIVE_BYTES = 250_000_000
MAX_ARCHIVE_FILES = 10_000
MAX_MEMBER_BYTES = 25_000_000
MAX_UNCOMPRESSED_BYTES = 250_000_000
DATED_REPORT = re.compile(r"^feed/(\d{4}-\d{2}-\d{2})\.html$")
REQUIRED_ROOT_FILES = frozenset({"CNAME", "404.html", "feed.xml", "index.html"})
REQUIRED_FEED_FILES = frozenset(
    {
        "feed/index.html",
        "feed/latest.html",
        "feed/search.html",
    }
)
DROPPED_SOURCE_FILES = frozenset(
    {
        "feed/telegram-admin.html",
        "feed/telegram.html",
    }
)
GOVERNANCE_FILES = frozenset(
    {
        "governance/app.js",
        "governance/config.js",
        "governance/index.html",
        "governance/styles.css",
    }
)


class LegacyRecoveryBundleError(RuntimeError):
    """A safe-to-print legacy recovery bundle validation failure."""


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _safe_path(raw_name: str) -> str:
    if not raw_name or "\\" in raw_name or "\x00" in raw_name:
        raise LegacyRecoveryBundleError("legacy recovery archive contains an unsafe path")
    trimmed = raw_name.rstrip("/")
    parts = trimmed.split("/")
    if not trimmed or any(part in {"", ".", ".."} for part in parts):
        raise LegacyRecoveryBundleError(
            f"legacy recovery archive contains an unsafe path: {raw_name}"
        )
    path = PurePosixPath(trimmed)
    if path.is_absolute() or (path.parts and re.fullmatch(r"[A-Za-z]:", path.parts[0])):
        raise LegacyRecoveryBundleError(
            f"legacy recovery archive contains an unsafe path: {raw_name}"
        )
    return path.as_posix()


def _allowed_file(path: str) -> bool:
    return (
        path in REQUIRED_ROOT_FILES
        or path in REQUIRED_FEED_FILES
        or DATED_REPORT.fullmatch(path) is not None
        or path in GOVERNANCE_FILES
    )


def _validate_layout(paths: set[str]) -> None:
    missing = sorted((REQUIRED_ROOT_FILES | REQUIRED_FEED_FILES) - paths)
    if missing:
        raise LegacyRecoveryBundleError(
            "legacy recovery artifact is missing required files: " + ", ".join(missing)
        )
    unexpected = sorted(path for path in paths if not _allowed_file(path))
    if unexpected:
        raise LegacyRecoveryBundleError(
            "legacy recovery artifact contains an unexpected file: " + unexpected[0]
        )
    governance = paths & GOVERNANCE_FILES
    if governance and governance != GOVERNANCE_FILES:
        raise LegacyRecoveryBundleError(
            "legacy recovery artifact contains an incomplete governance preview"
        )
    dated = sorted(path for path in paths if DATED_REPORT.fullmatch(path))
    if len(dated) < 90:
        raise LegacyRecoveryBundleError(
            "legacy recovery artifact must contain at least 90 dated reports"
        )


def _validate_payload(path: str, payload: bytes) -> None:
    if not payload:
        raise LegacyRecoveryBundleError(f"legacy recovery file is empty: {path}")
    if path.endswith(".html"):
        lowered_start = payload[:4096].lower()
        lowered_end = payload[-4096:].lower()
        if b"<html" not in lowered_start or b"</html>" not in lowered_end:
            raise LegacyRecoveryBundleError(f"legacy recovery HTML is incomplete: {path}")
    try:
        validate_public_payload(payload, path=path)
    except LegacyTelegramSafetyError as exc:
        raise LegacyRecoveryBundleError(str(exc)) from exc


def _inventory(root: Path) -> list[dict[str, Any]]:
    if root.is_symlink() or not root.is_dir():
        raise LegacyRecoveryBundleError("legacy recovery full site must be a regular directory")
    resolved_root = root.resolve()
    entries: list[dict[str, Any]] = []
    seen: set[str] = set()
    total = 0
    for candidate in resolved_root.rglob("*"):
        if candidate.is_symlink():
            raise LegacyRecoveryBundleError("legacy recovery full site contains a symbolic link")
        resolved = candidate.resolve()
        try:
            relative = resolved.relative_to(resolved_root).as_posix()
        except ValueError as exc:
            raise LegacyRecoveryBundleError("legacy recovery path escaped its root") from exc
        if candidate.is_dir():
            continue
        if not candidate.is_file():
            raise LegacyRecoveryBundleError(
                f"legacy recovery full site contains a non-regular file: {relative}"
            )
        key = relative.casefold()
        if key in seen:
            raise LegacyRecoveryBundleError(
                f"legacy recovery full site contains a duplicate path: {relative}"
            )
        seen.add(key)
        size = candidate.stat().st_size
        total += size
        if len(entries) >= MAX_ARCHIVE_FILES or size > MAX_MEMBER_BYTES:
            raise LegacyRecoveryBundleError("legacy recovery full site exceeds the safe size budget")
        if total > MAX_UNCOMPRESSED_BYTES:
            raise LegacyRecoveryBundleError("legacy recovery full site exceeds the safe size budget")
        payload = candidate.read_bytes()
        _validate_payload(relative, payload)
        entries.append({"path": relative, "bytes": size, "sha256": _sha256(payload)})
    _validate_layout({str(entry["path"]) for entry in entries})
    return sorted(entries, key=lambda item: str(item["path"]))


def _content_digest(files: list[dict[str, Any]]) -> str:
    lines = [f"{item['path']}\0{item['bytes']}\0{item['sha256']}" for item in files]
    return _sha256("\n".join(lines).encode("utf-8"))


def _write_full_site(archive: Path, output: Path) -> None:
    seen: set[str] = set()
    total = 0
    count = 0
    try:
        opened = zipfile.ZipFile(archive)
    except (OSError, zipfile.BadZipFile) as exc:
        raise LegacyRecoveryBundleError(
            "legacy recovery source is not a readable ZIP archive"
        ) from exc
    output.mkdir(parents=True)
    with opened:
        for member in opened.infolist():
            relative = _safe_path(member.filename)
            key = relative.casefold()
            if key in seen:
                raise LegacyRecoveryBundleError(
                    f"legacy recovery archive contains a duplicate path: {relative}"
                )
            seen.add(key)
            unix_mode = member.external_attr >> 16
            file_type = stat.S_IFMT(unix_mode)
            if file_type == stat.S_IFLNK:
                raise LegacyRecoveryBundleError(
                    f"legacy recovery archive contains a symbolic link: {relative}"
                )
            if member.is_dir():
                if file_type not in {0, stat.S_IFDIR}:
                    raise LegacyRecoveryBundleError(
                        f"legacy recovery archive contains a special entry: {relative}"
                    )
                continue
            if file_type not in {0, stat.S_IFREG} or member.flag_bits & 0x1:
                raise LegacyRecoveryBundleError(
                    f"legacy recovery archive contains an unsafe entry: {relative}"
                )
            count += 1
            total += member.file_size
            if (
                count > MAX_ARCHIVE_FILES
                or member.file_size > MAX_MEMBER_BYTES
                or total > MAX_UNCOMPRESSED_BYTES
            ):
                raise LegacyRecoveryBundleError(
                    "legacy recovery archive exceeds the safe size budget"
                )
            if relative in DROPPED_SOURCE_FILES:
                continue
            if not _allowed_file(relative):
                raise LegacyRecoveryBundleError(
                    f"legacy recovery archive contains an unexpected file: {relative}"
                )
            with opened.open(member, "r") as handle:
                payload = handle.read(MAX_MEMBER_BYTES + 1)
            if len(payload) != member.file_size or len(payload) > MAX_MEMBER_BYTES:
                raise LegacyRecoveryBundleError(
                    f"legacy recovery archive member size is invalid: {relative}"
                )
            try:
                payload = redact_telegram_mentions(payload, path=relative)
            except LegacyTelegramSafetyError as exc:
                raise LegacyRecoveryBundleError(str(exc)) from exc
            _validate_payload(relative, payload)
            destination = output.joinpath(*PurePosixPath(relative).parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(payload)
    _validate_layout({str(item["path"]) for item in _inventory(output)})


def _manifest(identity: LegacyArtifactIdentity, files: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "source": identity.as_dict(),
        "full_site": {
            "directory": FULL_SITE_DIR,
            "file_count": len(files),
            "files": files,
            "content_sha256": _content_digest(files),
        },
        "compatibility": {
            "directory": COMPATIBILITY_DIR,
            "manifest": "legacy-feed-compatibility.json",
        },
    }


def prepare_legacy_recovery_bundle(
    archive: Path,
    output: Path,
    *,
    identity: LegacyArtifactIdentity,
) -> dict[str, Any]:
    identity = identity.validated()
    if archive.is_symlink() or not archive.is_file():
        raise LegacyRecoveryBundleError("legacy recovery ZIP must be a regular file")
    size = archive.stat().st_size
    if size <= 0 or size > MAX_ARCHIVE_BYTES:
        raise LegacyRecoveryBundleError("legacy recovery ZIP has an unsafe size")
    if _file_sha256(archive) != identity.artifact_digest:
        raise LegacyRecoveryBundleError("legacy recovery ZIP digest does not match the pin")
    if output.exists() and (
        output.is_symlink() or not output.is_dir() or any(output.iterdir())
    ):
        raise LegacyRecoveryBundleError("legacy recovery output must be absent or empty")
    output.mkdir(parents=True, exist_ok=True)
    full_site = output / FULL_SITE_DIR
    compatibility = output / COMPATIBILITY_DIR
    try:
        _write_full_site(archive, full_site)
        prepare_legacy_feed_compatibility(archive, compatibility, identity=identity)
    except LegacyFeedCompatibilityError as exc:
        raise LegacyRecoveryBundleError(str(exc)) from exc
    files = _inventory(full_site)
    manifest = _manifest(identity, files)
    (output / MANIFEST_NAME).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    verify_legacy_recovery_bundle(output, expected_identity=identity)
    return manifest


def _load_manifest(bundle: Path) -> dict[str, Any]:
    path = bundle / MANIFEST_NAME
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 5_000_000:
        raise LegacyRecoveryBundleError("legacy recovery manifest is missing or unsafe")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LegacyRecoveryBundleError("legacy recovery manifest is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise LegacyRecoveryBundleError("legacy recovery manifest must be an object")
    return payload


def _identity_from_manifest(manifest: dict[str, Any]) -> LegacyArtifactIdentity:
    source = manifest.get("source")
    if not isinstance(source, dict):
        raise LegacyRecoveryBundleError("legacy recovery source metadata is missing")
    try:
        identity = LegacyArtifactIdentity(
            run_id=str(source["run_id"]),
            artifact_id=str(source["artifact_id"]),
            artifact_name=str(source["artifact_name"]),
            code_revision=str(source["code_revision"]),
            artifact_digest=str(source["artifact_digest"]),
        ).validated()
    except (KeyError, TypeError, LegacyFeedCompatibilityError) as exc:
        raise LegacyRecoveryBundleError("legacy recovery source metadata is invalid") from exc
    if source.get("workflow") != ".github/workflows/build-feed.yml":
        raise LegacyRecoveryBundleError("legacy recovery source workflow is invalid")
    return identity


def verify_legacy_recovery_bundle(
    bundle: Path,
    *,
    expected_identity: LegacyArtifactIdentity | None = None,
) -> dict[str, Any]:
    if bundle.is_symlink() or not bundle.is_dir():
        raise LegacyRecoveryBundleError("legacy recovery bundle must be a regular directory")
    actual_root_names = sorted(path.name for path in bundle.iterdir())
    expected_root_names = sorted({MANIFEST_NAME, FULL_SITE_DIR, COMPATIBILITY_DIR})
    if actual_root_names != expected_root_names:
        raise LegacyRecoveryBundleError("legacy recovery bundle contains an unexpected root path")
    manifest = _load_manifest(bundle)
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise LegacyRecoveryBundleError("legacy recovery manifest schema is unsupported")
    identity = _identity_from_manifest(manifest)
    if expected_identity is not None and identity != expected_identity.validated():
        raise LegacyRecoveryBundleError("legacy recovery source does not match the current pin")
    files = _inventory(bundle / FULL_SITE_DIR)
    expected_manifest = _manifest(identity, files)
    if manifest != expected_manifest:
        raise LegacyRecoveryBundleError(
            "legacy recovery manifest does not match the preserved files"
        )
    try:
        verify_legacy_feed_compatibility(
            bundle / COMPATIBILITY_DIR,
            expected_identity=identity,
        )
    except LegacyFeedCompatibilityError as exc:
        raise LegacyRecoveryBundleError(str(exc)) from exc
    return manifest


def _identity_from_args(args: argparse.Namespace, prefix: str) -> LegacyArtifactIdentity:
    return LegacyArtifactIdentity(
        run_id=getattr(args, f"{prefix}_run_id"),
        artifact_id=getattr(args, f"{prefix}_artifact_id"),
        artifact_name=getattr(args, f"{prefix}_artifact_name"),
        code_revision=getattr(args, f"{prefix}_code_revision"),
        artifact_digest=getattr(args, f"{prefix}_artifact_digest"),
    )


def _add_identity(parser: argparse.ArgumentParser, prefix: str) -> None:
    option = prefix.replace("_", "-")
    parser.add_argument(f"--{option}-run-id", required=True)
    parser.add_argument(f"--{option}-artifact-id", required=True)
    parser.add_argument(f"--{option}-artifact-name", required=True)
    parser.add_argument(f"--{option}-code-revision", required=True)
    parser.add_argument(f"--{option}-artifact-digest", required=True)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare or verify a rolling immutable legacy recovery bundle"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--archive", type=Path, required=True)
    prepare.add_argument("--output", type=Path, required=True)
    _add_identity(prepare, "source")
    verify = commands.add_parser("verify")
    verify.add_argument("--bundle", type=Path, required=True)
    _add_identity(verify, "expected_source")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        if args.command == "prepare":
            result = prepare_legacy_recovery_bundle(
                args.archive,
                args.output,
                identity=_identity_from_args(args, "source"),
            )
        else:
            result = verify_legacy_recovery_bundle(
                args.bundle,
                expected_identity=_identity_from_args(args, "expected_source"),
            )
    except (LegacyRecoveryBundleError, LegacyFeedCompatibilityError) as exc:
        print(f"legacy_recovery_bundle_error={exc}", file=sys.stderr)
        return 1
    print(
        "legacy_recovery_bundle="
        + json.dumps(
            {
                "source_run_id": result["source"]["run_id"],
                "source_artifact_id": result["source"]["artifact_id"],
                "file_count": result["full_site"]["file_count"],
                "content_sha256": result["full_site"]["content_sha256"],
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
