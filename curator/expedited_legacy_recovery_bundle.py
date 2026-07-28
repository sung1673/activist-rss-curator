from __future__ import annotations

import argparse
import json
import stat
import sys
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from .expedited_legacy_compat import (
    EXPEDITED_WINDOW_DAYS,
    EXPEDITED_WINDOW_START,
    MANIFEST_NAME as COMPATIBILITY_MANIFEST_NAME,
    _load_waiver,
    _observed_at,
    prepare_expedited_legacy_compatibility,
    verify_expedited_legacy_compatibility,
)
from .legacy_feed_compat import LegacyArtifactIdentity, LegacyFeedCompatibilityError
from .legacy_recovery_bundle import (
    DATED_REPORT,
    GOVERNANCE_FILES,
    MAX_ARCHIVE_BYTES,
    MAX_ARCHIVE_FILES,
    MAX_MEMBER_BYTES,
    MAX_UNCOMPRESSED_BYTES,
    REQUIRED_FEED_FILES,
    REQUIRED_ROOT_FILES,
    LegacyRecoveryBundleError,
    _allowed_file,
    _content_digest,
    _file_sha256,
    _safe_path,
    _sha256,
    _validate_payload,
)


MANIFEST_NAME = "expedited-legacy-recovery-bundle.json"
MANIFEST_SCHEMA_VERSION = 1
MANIFEST_KIND = "bside-expedited-legacy-recovery-bundle"
FULL_SITE_DIR = "full-site"
COMPATIBILITY_DIR = "compatibility"


def _validate_layout(paths: set[str], *, mode: str) -> None:
    missing = sorted((REQUIRED_ROOT_FILES | REQUIRED_FEED_FILES) - paths)
    if missing:
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery artifact is missing required files: "
            + ", ".join(missing)
        )
    unexpected = sorted(path for path in paths if not _allowed_file(path))
    if unexpected:
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery artifact contains an unexpected file: "
            + unexpected[0]
        )
    governance = paths & GOVERNANCE_FILES
    if governance and governance != GOVERNANCE_FILES:
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery artifact contains an incomplete governance preview"
        )
    dated: set[date] = set()
    for path in paths:
        match = DATED_REPORT.fullmatch(path)
        if match is None:
            continue
        try:
            value = date.fromisoformat(match.group(1))
        except ValueError as exc:
            raise LegacyRecoveryBundleError(
                "expedited legacy recovery artifact contains an invalid report date"
            ) from exc
        dated.add(value)
    if mode == "89_day_human_waiver":
        expected = {
            date.fromordinal(EXPEDITED_WINDOW_START.toordinal() + offset)
            for offset in range(EXPEDITED_WINDOW_DAYS)
        }
        if dated != expected:
            raise LegacyRecoveryBundleError(
                "expedited legacy full site must contain exactly 2026-05-01 through "
                "2026-07-28"
            )
    elif mode == "standard_90_day":
        if len(dated) < 90:
            raise LegacyRecoveryBundleError(
                "standard expedited recovery requires at least 90 dated reports"
            )
    else:
        raise LegacyRecoveryBundleError("expedited legacy compatibility mode is invalid")


def _inventory(root: Path, *, mode: str) -> list[dict[str, Any]]:
    if root.is_symlink() or not root.is_dir():
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery full site must be a regular directory"
        )
    resolved_root = root.resolve()
    entries: list[dict[str, Any]] = []
    seen: set[str] = set()
    total = 0
    dated_digests: set[str] = set()
    for candidate in resolved_root.rglob("*"):
        if candidate.is_symlink():
            raise LegacyRecoveryBundleError(
                "expedited legacy recovery full site contains a symbolic link"
            )
        resolved = candidate.resolve()
        try:
            relative = resolved.relative_to(resolved_root).as_posix()
        except ValueError as exc:
            raise LegacyRecoveryBundleError(
                "expedited legacy recovery path escaped its root"
            ) from exc
        if candidate.is_dir():
            continue
        if not candidate.is_file():
            raise LegacyRecoveryBundleError(
                "expedited legacy recovery full site contains a non-regular file: "
                + relative
            )
        key = relative.casefold()
        if key in seen:
            raise LegacyRecoveryBundleError(
                "expedited legacy recovery full site contains a duplicate path: "
                + relative
            )
        seen.add(key)
        size = candidate.stat().st_size
        total += size
        if (
            len(entries) >= MAX_ARCHIVE_FILES
            or size > MAX_MEMBER_BYTES
            or total > MAX_UNCOMPRESSED_BYTES
        ):
            raise LegacyRecoveryBundleError(
                "expedited legacy recovery full site exceeds the safe size budget"
            )
        payload = candidate.read_bytes()
        _validate_payload(relative, payload)
        digest = _sha256(payload)
        if DATED_REPORT.fullmatch(relative):
            if digest in dated_digests:
                raise LegacyRecoveryBundleError(
                    "expedited legacy recovery contains duplicated dated report content"
                )
            dated_digests.add(digest)
        entries.append({"path": relative, "bytes": size, "sha256": digest})
    _validate_layout({str(entry["path"]) for entry in entries}, mode=mode)
    return sorted(entries, key=lambda item: str(item["path"]))


def _write_full_site(archive: Path, output: Path, *, mode: str) -> None:
    seen: set[str] = set()
    total = 0
    count = 0
    try:
        opened = zipfile.ZipFile(archive)
    except (OSError, zipfile.BadZipFile) as exc:
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery source is not a readable ZIP archive"
        ) from exc
    output.mkdir(parents=True)
    with opened:
        for member in opened.infolist():
            relative = _safe_path(member.filename)
            key = relative.casefold()
            if key in seen:
                raise LegacyRecoveryBundleError(
                    "expedited legacy recovery archive contains a duplicate path: "
                    + relative
                )
            seen.add(key)
            unix_mode = member.external_attr >> 16
            file_type = stat.S_IFMT(unix_mode)
            if file_type == stat.S_IFLNK:
                raise LegacyRecoveryBundleError(
                    "expedited legacy recovery archive contains a symbolic link: "
                    + relative
                )
            if member.is_dir():
                if file_type not in {0, stat.S_IFDIR}:
                    raise LegacyRecoveryBundleError(
                        "expedited legacy recovery archive contains a special entry: "
                        + relative
                    )
                continue
            if file_type not in {0, stat.S_IFREG} or member.flag_bits & 0x1:
                raise LegacyRecoveryBundleError(
                    "expedited legacy recovery archive contains an unsafe entry: "
                    + relative
                )
            count += 1
            total += member.file_size
            if (
                count > MAX_ARCHIVE_FILES
                or member.file_size > MAX_MEMBER_BYTES
                or total > MAX_UNCOMPRESSED_BYTES
            ):
                raise LegacyRecoveryBundleError(
                    "expedited legacy recovery archive exceeds the safe size budget"
                )
            if not _allowed_file(relative):
                raise LegacyRecoveryBundleError(
                    "expedited legacy recovery archive contains an unexpected file: "
                    + relative
                )
            with opened.open(member, "r") as handle:
                payload = handle.read(MAX_MEMBER_BYTES + 1)
            if len(payload) != member.file_size or len(payload) > MAX_MEMBER_BYTES:
                raise LegacyRecoveryBundleError(
                    "expedited legacy recovery archive member size is invalid: "
                    + relative
                )
            _validate_payload(relative, payload)
            destination = output.joinpath(*PurePosixPath(relative).parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(payload)
    _inventory(output, mode=mode)


def _manifest(
    identity: LegacyArtifactIdentity,
    files: list[dict[str, Any]],
    compatibility: dict[str, Any],
    *,
    prepared_at: datetime,
) -> dict[str, Any]:
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "kind": MANIFEST_KIND,
        "prepared_at": prepared_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "source": identity.as_dict(),
        "mode": compatibility["mode"],
        "window_days": compatibility["window_days"],
        "window_start": compatibility["window_start"],
        "window_end": compatibility["window_end"],
        "full_site": {
            "directory": FULL_SITE_DIR,
            "file_count": len(files),
            "files": files,
            "content_sha256": _content_digest(files),
        },
        "compatibility": {
            "directory": COMPATIBILITY_DIR,
            "manifest": COMPATIBILITY_MANIFEST_NAME,
            "content_sha256": compatibility["content_sha256"],
        },
    }


def _identity_from_manifest(manifest: dict[str, Any]) -> LegacyArtifactIdentity:
    source = manifest.get("source")
    if not isinstance(source, dict):
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery source metadata is missing"
        )
    try:
        identity = LegacyArtifactIdentity(
            run_id=str(source["run_id"]),
            artifact_id=str(source["artifact_id"]),
            artifact_name=str(source["artifact_name"]),
            code_revision=str(source["code_revision"]),
            artifact_digest=str(source["artifact_digest"]),
        ).validated()
    except (KeyError, TypeError, LegacyFeedCompatibilityError) as exc:
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery source metadata is invalid"
        ) from exc
    if source.get("workflow") != ".github/workflows/build-feed.yml":
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery source workflow is invalid"
        )
    return identity


def _load_manifest(bundle: Path) -> dict[str, Any]:
    path = bundle / MANIFEST_NAME
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 5_000_000:
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery manifest is missing or unsafe"
        )
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery manifest is invalid JSON"
        ) from exc
    if not isinstance(value, dict):
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery manifest must be an object"
        )
    return value


def prepare_expedited_legacy_recovery_bundle(
    archive: Path,
    output: Path,
    *,
    identity: LegacyArtifactIdentity,
    observed_at: datetime,
    waiver: dict[str, Any] | None = None,
) -> dict[str, Any]:
    identity = identity.validated()
    observed_at = _observed_at(observed_at)
    if archive.is_symlink() or not archive.is_file():
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery ZIP must be a regular file"
        )
    size = archive.stat().st_size
    if size <= 0 or size > MAX_ARCHIVE_BYTES:
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery ZIP has an unsafe size"
        )
    if _file_sha256(archive) != identity.artifact_digest:
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery ZIP digest does not match the pin"
        )
    if output.exists() and (
        output.is_symlink() or not output.is_dir() or any(output.iterdir())
    ):
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery output must be absent or empty"
        )
    output.mkdir(parents=True, exist_ok=True)
    compatibility_root = output / COMPATIBILITY_DIR
    try:
        compatibility = prepare_expedited_legacy_compatibility(
            archive,
            compatibility_root,
            identity=identity,
            observed_at=observed_at,
            waiver=waiver,
        )
    except LegacyFeedCompatibilityError as exc:
        raise LegacyRecoveryBundleError(str(exc)) from exc
    full_site = output / FULL_SITE_DIR
    _write_full_site(archive, full_site, mode=str(compatibility["mode"]))
    files = _inventory(full_site, mode=str(compatibility["mode"]))
    manifest = _manifest(
        identity,
        files,
        compatibility,
        prepared_at=observed_at,
    )
    (output / MANIFEST_NAME).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    return verify_expedited_legacy_recovery_bundle(
        output,
        expected_identity=identity,
        observed_at=observed_at,
    )


def prepare_expedited_legacy_drill_site(
    archive: Path,
    output: Path,
    *,
    identity: LegacyArtifactIdentity,
) -> dict[str, Any]:
    """Extract an immutable legacy Pages artifact for a rollback drill.

    This deliberately does not create release evidence or accept an 89-day
    waiver. The protected final-approval producer later creates the eligible
    recovery bundle with either the actual human waiver or the standard
    90-day contract.
    """

    identity = identity.validated()
    if archive.is_symlink() or not archive.is_file():
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery ZIP must be a regular file"
        )
    size = archive.stat().st_size
    if size <= 0 or size > MAX_ARCHIVE_BYTES:
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery ZIP has an unsafe size"
        )
    if _file_sha256(archive) != identity.artifact_digest:
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery ZIP digest does not match the pin"
        )
    if output.exists() and (
        output.is_symlink() or not output.is_dir() or any(output.iterdir())
    ):
        raise LegacyRecoveryBundleError(
            "expedited legacy drill output must be absent or empty"
        )
    dated: set[date] = set()
    try:
        opened = zipfile.ZipFile(archive)
    except (OSError, zipfile.BadZipFile) as exc:
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery source is not a readable ZIP archive"
        ) from exc
    with opened:
        for member in opened.infolist():
            relative = _safe_path(member.filename)
            match = DATED_REPORT.fullmatch(relative)
            if match is not None:
                dated.add(date.fromisoformat(match.group(1)))
    exact_89 = {
        date.fromordinal(EXPEDITED_WINDOW_START.toordinal() + offset)
        for offset in range(EXPEDITED_WINDOW_DAYS)
    }
    if dated == exact_89:
        mode = "89_day_human_waiver"
    elif len(dated) >= 90:
        mode = "standard_90_day"
    else:
        raise LegacyRecoveryBundleError(
            "expedited rollback drill requires the exact 89-day window or at least 90 days"
        )
    _write_full_site(archive, output, mode=mode)
    files = _inventory(output, mode=mode)
    return {
        "schema_version": 1,
        "kind": "bside-expedited-legacy-drill-site",
        "mode": mode,
        "window_days": len(dated),
        "file_count": len(files),
        "content_sha256": _content_digest(files),
        "source": identity.as_dict(),
    }


def verify_expedited_legacy_recovery_bundle(
    bundle: Path,
    *,
    expected_identity: LegacyArtifactIdentity | None,
    observed_at: datetime,
) -> dict[str, Any]:
    observed_at = _observed_at(observed_at)
    if bundle.is_symlink() or not bundle.is_dir():
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery bundle must be a regular directory"
        )
    actual_names = sorted(path.name for path in bundle.iterdir())
    expected_names = sorted({MANIFEST_NAME, FULL_SITE_DIR, COMPATIBILITY_DIR})
    if actual_names != expected_names:
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery bundle contains an unexpected root path"
        )
    manifest = _load_manifest(bundle)
    if (
        manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION
        or manifest.get("kind") != MANIFEST_KIND
    ):
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery manifest schema or kind is invalid"
        )
    identity = _identity_from_manifest(manifest)
    if expected_identity is not None and identity != expected_identity.validated():
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery source does not match the current pin"
        )
    try:
        compatibility = verify_expedited_legacy_compatibility(
            bundle / COMPATIBILITY_DIR,
            expected_identity=identity,
            observed_at=observed_at,
        )
    except LegacyFeedCompatibilityError as exc:
        raise LegacyRecoveryBundleError(str(exc)) from exc
    files = _inventory(bundle / FULL_SITE_DIR, mode=str(compatibility["mode"]))
    prepared_at_value = manifest.get("prepared_at")
    if not isinstance(prepared_at_value, str):
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery prepared_at is missing"
        )
    try:
        prepared_at = datetime.fromisoformat(
            prepared_at_value.replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery prepared_at is invalid"
        ) from exc
    if (
        prepared_at.tzinfo is None
        or prepared_at.utcoffset() is None
        or prepared_at.astimezone(UTC) > observed_at
    ):
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery prepared_at is invalid"
        )
    expected = _manifest(
        identity,
        files,
        compatibility,
        prepared_at=prepared_at,
    )
    if manifest != expected:
        raise LegacyRecoveryBundleError(
            "expedited legacy recovery manifest does not match the preserved files"
        )
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
        description="Prepare or verify the immutable expedited legacy recovery bundle"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--archive", type=Path, required=True)
    prepare.add_argument("--output", type=Path, required=True)
    prepare.add_argument("--observed-at", required=True)
    prepare.add_argument("--waiver-json", type=Path)
    _add_identity(prepare, "source")
    drill = commands.add_parser("prepare-drill-site")
    drill.add_argument("--archive", type=Path, required=True)
    drill.add_argument("--output", type=Path, required=True)
    drill.add_argument("--receipt", type=Path, required=True)
    _add_identity(drill, "source")
    verify = commands.add_parser("verify")
    verify.add_argument("--bundle", type=Path, required=True)
    verify.add_argument("--observed-at", required=True)
    _add_identity(verify, "expected_source")
    return parser


def _parse_cli_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LegacyRecoveryBundleError("observed-at must be an ISO timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise LegacyRecoveryBundleError("observed-at must include a timezone")
    return parsed.astimezone(UTC)


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        if args.command == "prepare":
            observed_at = _parse_cli_timestamp(args.observed_at)
            waiver = _load_waiver(args.waiver_json) if args.waiver_json else None
            result = prepare_expedited_legacy_recovery_bundle(
                args.archive,
                args.output,
                identity=_identity_from_args(args, "source"),
                observed_at=observed_at,
                waiver=waiver,
            )
        elif args.command == "prepare-drill-site":
            result = prepare_expedited_legacy_drill_site(
                args.archive,
                args.output,
                identity=_identity_from_args(args, "source"),
            )
            args.receipt.write_text(
                json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
                + "\n",
                encoding="utf-8",
                newline="\n",
            )
        else:
            observed_at = _parse_cli_timestamp(args.observed_at)
            result = verify_expedited_legacy_recovery_bundle(
                args.bundle,
                expected_identity=_identity_from_args(args, "expected_source"),
                observed_at=observed_at,
            )
    except (LegacyRecoveryBundleError, LegacyFeedCompatibilityError) as exc:
        print(f"expedited_legacy_recovery_bundle_error={exc}", file=sys.stderr)
        return 1
    print(
        "expedited_legacy_recovery_bundle="
        + json.dumps(
            {
                "mode": result["mode"],
                "window_days": result["window_days"],
                **(
                    {
                        "window_start": result["window_start"],
                        "window_end": result["window_end"],
                        "full_site_content_sha256": result["full_site"][
                            "content_sha256"
                        ],
                    }
                    if "full_site" in result
                    else {
                        "drill_site_content_sha256": result["content_sha256"],
                    }
                ),
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
