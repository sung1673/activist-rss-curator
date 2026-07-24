from __future__ import annotations

import argparse
import hashlib
import json
import re
import stat
import sys
import zipfile
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path, PurePosixPath
from typing import Any


MANIFEST_NAME = "legacy-feed-compatibility.json"
MANIFEST_SCHEMA_VERSION = 1
REQUIRED_WINDOW_DAYS = 90
MAX_ARCHIVE_BYTES = 250_000_000
MAX_ARCHIVE_FILES = 10_000
MAX_FEED_XML_BYTES = 5_000_000
MAX_REPORT_BYTES = 2_000_000
SHA256_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
POSITIVE_INTEGER = re.compile(r"^[1-9][0-9]*$")
DATED_REPORT = re.compile(r"^feed/(\d{4}-\d{2}-\d{2})\.html$")
OPTIONAL_ROOT_ASSETS = ("CNAME", "404.html")


class LegacyFeedCompatibilityError(RuntimeError):
    """A safe-to-print immutable legacy compatibility failure."""


@dataclass(frozen=True)
class LegacyArtifactIdentity:
    run_id: str
    artifact_id: str
    artifact_name: str
    code_revision: str
    artifact_digest: str

    def validated(self) -> LegacyArtifactIdentity:
        digest = self.artifact_digest.casefold()
        revision = self.code_revision.casefold()
        if not POSITIVE_INTEGER.fullmatch(self.run_id):
            raise LegacyFeedCompatibilityError("legacy source run id must be a positive integer")
        if not POSITIVE_INTEGER.fullmatch(self.artifact_id):
            raise LegacyFeedCompatibilityError("legacy source artifact id must be a positive integer")
        if not self.artifact_name or len(self.artifact_name) > 255:
            raise LegacyFeedCompatibilityError("legacy source artifact name is invalid")
        if any(ord(character) < 32 for character in self.artifact_name):
            raise LegacyFeedCompatibilityError("legacy source artifact name is invalid")
        if not GIT_SHA.fullmatch(revision):
            raise LegacyFeedCompatibilityError("legacy source revision must be a full Git SHA")
        if not SHA256_DIGEST.fullmatch(digest):
            raise LegacyFeedCompatibilityError("legacy source digest must pin SHA-256")
        return LegacyArtifactIdentity(
            run_id=self.run_id,
            artifact_id=self.artifact_id,
            artifact_name=self.artifact_name,
            code_revision=revision,
            artifact_digest=digest,
        )

    def as_dict(self) -> dict[str, str]:
        identity = self.validated()
        return {
            "run_id": identity.run_id,
            "artifact_id": identity.artifact_id,
            "artifact_name": identity.artifact_name,
            "code_revision": identity.code_revision,
            "artifact_digest": identity.artifact_digest,
            "workflow": ".github/workflows/build-feed.yml",
        }


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _safe_member_name(raw_name: str) -> str:
    if not raw_name or "\\" in raw_name or "\x00" in raw_name:
        raise LegacyFeedCompatibilityError("legacy artifact contains an unsafe path")
    raw_parts = raw_name.rstrip("/").split("/")
    if any(part in {"", ".", ".."} for part in raw_parts):
        raise LegacyFeedCompatibilityError(f"legacy artifact contains an unsafe path: {raw_name}")
    path = PurePosixPath(raw_name)
    if (
        path.is_absolute()
        or (path.parts and re.fullmatch(r"[A-Za-z]:", path.parts[0]))
    ):
        raise LegacyFeedCompatibilityError(f"legacy artifact contains an unsafe path: {raw_name}")
    return path.as_posix().rstrip("/")


def _validate_html(payload: bytes, path: str) -> None:
    if not payload or len(payload) > MAX_REPORT_BYTES:
        raise LegacyFeedCompatibilityError(f"legacy dated report has an unsafe size: {path}")
    if b"<html" not in payload[:4096].lower() or b"</html>" not in payload[-4096:].lower():
        raise LegacyFeedCompatibilityError(f"legacy dated report is incomplete: {path}")


def _register_payload(
    relative: str,
    payload: bytes,
    *,
    feed: dict[str, bytes],
    reports: dict[date, bytes],
    optional: dict[str, bytes],
) -> None:
    if relative == "feed.xml":
        if not payload or len(payload) > MAX_FEED_XML_BYTES:
            raise LegacyFeedCompatibilityError("legacy feed.xml has an unsafe size")
        feed[relative] = payload
        return
    match = DATED_REPORT.fullmatch(relative)
    if match:
        try:
            report_date = date.fromisoformat(match.group(1))
        except ValueError as exc:
            raise LegacyFeedCompatibilityError(
                f"legacy artifact contains an invalid report date: {relative}"
            ) from exc
        if report_date.isoformat() != match.group(1):
            raise LegacyFeedCompatibilityError(
                f"legacy artifact contains a non-canonical report date: {relative}"
            )
        if report_date in reports:
            raise LegacyFeedCompatibilityError(
                f"legacy artifact contains a duplicate dated report: {relative}"
            )
        _validate_html(payload, relative)
        reports[report_date] = payload
        return
    if relative in OPTIONAL_ROOT_ASSETS:
        if not payload or len(payload) > MAX_REPORT_BYTES:
            raise LegacyFeedCompatibilityError(f"legacy root asset has an unsafe size: {relative}")
        optional[relative] = payload


def _load_zip_archive(archive: Path) -> tuple[bytes, dict[date, bytes], dict[str, bytes]]:
    feed: dict[str, bytes] = {}
    reports: dict[date, bytes] = {}
    optional: dict[str, bytes] = {}
    seen_paths: set[str] = set()
    total_bytes = 0
    file_count = 0
    try:
        opened = zipfile.ZipFile(archive)
    except (OSError, zipfile.BadZipFile) as exc:
        raise LegacyFeedCompatibilityError("legacy artifact is not a readable ZIP archive") from exc

    with opened:
        for member in opened.infolist():
            relative = _safe_member_name(member.filename)
            path_key = relative.casefold()
            if path_key in seen_paths:
                raise LegacyFeedCompatibilityError(
                    f"legacy artifact contains a duplicate path: {relative}"
                )
            seen_paths.add(path_key)
            unix_mode = member.external_attr >> 16
            file_type = stat.S_IFMT(unix_mode)
            if file_type == stat.S_IFLNK:
                raise LegacyFeedCompatibilityError(
                    f"legacy artifact contains a symbolic link: {relative}"
                )
            if member.is_dir():
                if file_type not in {0, stat.S_IFDIR}:
                    raise LegacyFeedCompatibilityError(
                        f"legacy artifact contains a special entry: {relative}"
                    )
                continue
            if file_type not in {0, stat.S_IFREG}:
                raise LegacyFeedCompatibilityError(
                    f"legacy artifact contains a non-regular file: {relative}"
                )
            if member.flag_bits & 0x1:
                raise LegacyFeedCompatibilityError(
                    f"legacy artifact contains an encrypted entry: {relative}"
                )
            file_count += 1
            total_bytes += member.file_size
            if file_count > MAX_ARCHIVE_FILES or total_bytes > MAX_ARCHIVE_BYTES:
                raise LegacyFeedCompatibilityError("legacy artifact exceeds the safe size budget")
            if not (
                relative == "feed.xml"
                or DATED_REPORT.fullmatch(relative)
                or relative in OPTIONAL_ROOT_ASSETS
            ):
                continue
            with opened.open(member, "r") as handle:
                payload = handle.read(max(MAX_FEED_XML_BYTES, MAX_REPORT_BYTES) + 1)
            _register_payload(
                relative,
                payload,
                feed=feed,
                reports=reports,
                optional=optional,
            )
    if set(feed) != {"feed.xml"}:
        raise LegacyFeedCompatibilityError("legacy artifact must contain exactly one root feed.xml")
    return feed["feed.xml"], reports, optional


def _load_site_directory(site: Path) -> tuple[bytes, dict[date, bytes], dict[str, bytes]]:
    if site.is_symlink() or not site.is_dir():
        raise LegacyFeedCompatibilityError("legacy compatibility site must be a regular directory")
    root = site.resolve()
    seen_paths: set[str] = set()
    feed: dict[str, bytes] = {}
    reports: dict[date, bytes] = {}
    optional: dict[str, bytes] = {}
    file_count = 0
    total_bytes = 0
    for candidate in root.rglob("*"):
        if candidate.is_symlink():
            raise LegacyFeedCompatibilityError(
                f"legacy compatibility site contains a symbolic link: {candidate}"
            )
        resolved = candidate.resolve()
        try:
            relative = resolved.relative_to(root).as_posix()
        except ValueError as exc:
            raise LegacyFeedCompatibilityError("legacy compatibility path escaped its root") from exc
        path_key = relative.casefold()
        if path_key in seen_paths:
            raise LegacyFeedCompatibilityError(
                f"legacy compatibility site contains a duplicate path: {relative}"
            )
        seen_paths.add(path_key)
        if candidate.is_dir():
            continue
        if not candidate.is_file():
            raise LegacyFeedCompatibilityError(
                f"legacy compatibility site contains a non-regular file: {relative}"
            )
        file_count += 1
        size = candidate.stat().st_size
        total_bytes += size
        if file_count > MAX_ARCHIVE_FILES or total_bytes > MAX_ARCHIVE_BYTES:
            raise LegacyFeedCompatibilityError("legacy compatibility site exceeds the safe size budget")
        if not (
            relative == "feed.xml"
            or DATED_REPORT.fullmatch(relative)
            or relative in OPTIONAL_ROOT_ASSETS
        ):
            continue
        payload = candidate.read_bytes()
        _register_payload(
            relative,
            payload,
            feed=feed,
            reports=reports,
            optional=optional,
        )
    if set(feed) != {"feed.xml"}:
        raise LegacyFeedCompatibilityError("legacy compatibility site must contain root feed.xml")
    return feed["feed.xml"], reports, optional


def _required_window(reports: dict[date, bytes]) -> tuple[date, date, list[date]]:
    if len(reports) < REQUIRED_WINDOW_DAYS:
        raise LegacyFeedCompatibilityError(
            f"legacy artifact has {len(reports)} dated reports; {REQUIRED_WINDOW_DAYS} are required"
        )
    window_end = max(reports)
    window_start = window_end - timedelta(days=REQUIRED_WINDOW_DAYS - 1)
    required = [window_start + timedelta(days=offset) for offset in range(REQUIRED_WINDOW_DAYS)]
    missing = [value.isoformat() for value in required if value not in reports]
    if missing:
        raise LegacyFeedCompatibilityError(
            "legacy artifact does not contain a continuous 90-day window: " + ", ".join(missing[:10])
        )
    return window_start, window_end, required


def _manifest(
    identity: LegacyArtifactIdentity,
    feed_xml: bytes,
    reports: dict[date, bytes],
    required: list[date],
    optional: dict[str, bytes],
) -> dict[str, Any]:
    files = [
        {
            "path": f"feed/{report_date.isoformat()}.html",
            "bytes": len(reports[report_date]),
            "sha256": _sha256(reports[report_date]),
        }
        for report_date in required
    ]
    content_lines = [f"feed.xml\0{_sha256(feed_xml)}"]
    content_lines.extend(f"{item['path']}\0{item['sha256']}" for item in files)
    root_assets = [
        {"path": name, "bytes": len(payload), "sha256": _sha256(payload)}
        for name, payload in sorted(optional.items())
    ]
    content_lines.extend(f"{item['path']}\0{item['sha256']}" for item in root_assets)
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "source": identity.as_dict(),
        "window_days": REQUIRED_WINDOW_DAYS,
        "window_start": required[0].isoformat(),
        "window_end": required[-1].isoformat(),
        "dated_report_count": len(files),
        "feed_xml": {
            "path": "feed.xml",
            "bytes": len(feed_xml),
            "sha256": _sha256(feed_xml),
        },
        "dated_reports": files,
        "root_assets": root_assets,
        "content_sha256": _sha256("\n".join(content_lines).encode("utf-8")),
    }


def prepare_legacy_feed_compatibility(
    archive: Path,
    output: Path,
    *,
    identity: LegacyArtifactIdentity,
) -> dict[str, Any]:
    identity = identity.validated()
    if archive.is_symlink() or not archive.is_file():
        raise LegacyFeedCompatibilityError("legacy artifact ZIP must be a regular file")
    if archive.stat().st_size <= 0 or archive.stat().st_size > MAX_ARCHIVE_BYTES:
        raise LegacyFeedCompatibilityError("legacy artifact ZIP has an unsafe size")
    actual_digest = _file_sha256(archive)
    if actual_digest != identity.artifact_digest:
        raise LegacyFeedCompatibilityError("legacy artifact ZIP digest does not match the pin")
    if output.exists():
        if output.is_symlink() or not output.is_dir() or any(output.iterdir()):
            raise LegacyFeedCompatibilityError("legacy compatibility output must be absent or empty")

    feed_xml, reports, optional = _load_zip_archive(archive)
    _window_start, _window_end, required = _required_window(reports)
    output.mkdir(parents=True, exist_ok=True)
    (output / "feed").mkdir()
    (output / "feed.xml").write_bytes(feed_xml)
    for report_date in required:
        (output / "feed" / f"{report_date.isoformat()}.html").write_bytes(reports[report_date])
    for name, payload in optional.items():
        (output / name).write_bytes(payload)
    manifest = _manifest(identity, feed_xml, reports, required, optional)
    (output / MANIFEST_NAME).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    verify_legacy_feed_compatibility(output, expected_identity=identity)
    return manifest


def _load_manifest(site: Path) -> dict[str, Any]:
    path = site / MANIFEST_NAME
    if path.is_symlink() or not path.is_file():
        raise LegacyFeedCompatibilityError("legacy compatibility manifest is missing or unsafe")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LegacyFeedCompatibilityError("legacy compatibility manifest is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise LegacyFeedCompatibilityError("legacy compatibility manifest must be an object")
    return payload


def verify_legacy_feed_compatibility(
    site: Path,
    *,
    expected_identity: LegacyArtifactIdentity | None = None,
) -> dict[str, Any]:
    manifest = _load_manifest(site)
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise LegacyFeedCompatibilityError("legacy compatibility manifest schema is unsupported")
    if manifest.get("window_days") != REQUIRED_WINDOW_DAYS:
        raise LegacyFeedCompatibilityError("legacy compatibility window must be exactly 90 days")
    source = manifest.get("source")
    if not isinstance(source, dict):
        raise LegacyFeedCompatibilityError("legacy compatibility source metadata is missing")
    try:
        identity = LegacyArtifactIdentity(
            run_id=str(source["run_id"]),
            artifact_id=str(source["artifact_id"]),
            artifact_name=str(source["artifact_name"]),
            code_revision=str(source["code_revision"]),
            artifact_digest=str(source["artifact_digest"]),
        ).validated()
    except (KeyError, TypeError) as exc:
        raise LegacyFeedCompatibilityError("legacy compatibility source metadata is incomplete") from exc
    if source.get("workflow") != ".github/workflows/build-feed.yml":
        raise LegacyFeedCompatibilityError("legacy compatibility source workflow is invalid")
    if expected_identity is not None and identity != expected_identity.validated():
        raise LegacyFeedCompatibilityError("legacy compatibility source does not match the current pin")

    feed_xml, reports, optional = _load_site_directory(site)
    window_start, window_end, required = _required_window(reports)
    if set(reports) != set(required):
        raise LegacyFeedCompatibilityError("legacy compatibility site must contain exactly 90 reports")
    if manifest.get("window_start") != window_start.isoformat():
        raise LegacyFeedCompatibilityError("legacy compatibility window_start does not match files")
    if manifest.get("window_end") != window_end.isoformat():
        raise LegacyFeedCompatibilityError("legacy compatibility window_end does not match files")
    if manifest.get("dated_report_count") != REQUIRED_WINDOW_DAYS:
        raise LegacyFeedCompatibilityError("legacy compatibility report count must be exactly 90")

    expected_manifest = _manifest(identity, feed_xml, reports, required, optional)
    if manifest != expected_manifest:
        raise LegacyFeedCompatibilityError("legacy compatibility manifest does not match file contents")
    return manifest


def _identity_from_args(args: argparse.Namespace, *, prefix: str) -> LegacyArtifactIdentity:
    return LegacyArtifactIdentity(
        run_id=getattr(args, f"{prefix}_run_id"),
        artifact_id=getattr(args, f"{prefix}_artifact_id"),
        artifact_name=getattr(args, f"{prefix}_artifact_name"),
        code_revision=getattr(args, f"{prefix}_code_revision"),
        artifact_digest=getattr(args, f"{prefix}_artifact_digest"),
    )


def _add_identity_arguments(parser: argparse.ArgumentParser, *, prefix: str) -> None:
    option_prefix = prefix.replace("_", "-")
    parser.add_argument(f"--{option_prefix}-run-id", required=True)
    parser.add_argument(f"--{option_prefix}-artifact-id", required=True)
    parser.add_argument(f"--{option_prefix}-artifact-name", required=True)
    parser.add_argument(f"--{option_prefix}-code-revision", required=True)
    parser.add_argument(f"--{option_prefix}-artifact-digest", required=True)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Prepare or verify an immutable 90-day legacy feed compatibility window"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--archive", type=Path, required=True)
    prepare.add_argument("--output", type=Path, required=True)
    _add_identity_arguments(prepare, prefix="source")
    verify = commands.add_parser("verify")
    verify.add_argument("--site", type=Path, required=True)
    _add_identity_arguments(verify, prefix="expected_source")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        if args.command == "prepare":
            result = prepare_legacy_feed_compatibility(
                args.archive,
                args.output,
                identity=_identity_from_args(args, prefix="source"),
            )
        else:
            result = verify_legacy_feed_compatibility(
                args.site,
                expected_identity=_identity_from_args(args, prefix="expected_source"),
            )
    except LegacyFeedCompatibilityError as exc:
        print(f"legacy_feed_compatibility_error={exc}", file=sys.stderr)
        return 1
    print(
        "legacy_feed_compatibility="
        + json.dumps(
            {
                "window_start": result["window_start"],
                "window_end": result["window_end"],
                "dated_report_count": result["dated_report_count"],
                "content_sha256": result["content_sha256"],
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
