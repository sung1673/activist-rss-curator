from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from .legacy_feed_compat import (
    LegacyArtifactIdentity,
    LegacyFeedCompatibilityError,
    _file_sha256,
    _load_site_directory,
    _load_zip_archive,
    _sha256,
    prepare_legacy_feed_compatibility,
    verify_legacy_feed_compatibility,
)
from .legacy_telegram_safety import (
    LegacyTelegramSafetyError,
    verify_public_site,
)


MANIFEST_NAME = "legacy-feed-expedited-compatibility.json"
MANIFEST_SCHEMA_VERSION = 1
MANIFEST_KIND = "bside-expedited-legacy-feed-compatibility"
EXPEDITED_WINDOW_DAYS = 89
EXPEDITED_WINDOW_START = date(2026, 5, 1)
EXPEDITED_WINDOW_END = date(2026, 7, 28)
WAIVER_EXPIRES_AT = datetime(2026, 7, 28, 20, 45, tzinfo=UTC)
WAIVER_EXCEPTION_ID = "production-alpha-early-access-89-day-2026-07-28"
RELEASE_CHANNEL = "production_alpha_early_access"


def _parse_timestamp(value: object, location: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise LegacyFeedCompatibilityError(f"{location} must be a timezone-aware timestamp")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise LegacyFeedCompatibilityError(
            f"{location} must be a timezone-aware timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise LegacyFeedCompatibilityError(f"{location} must include a timezone")
    return parsed.astimezone(UTC)


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise LegacyFeedCompatibilityError("observed_at must include a timezone")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _observed_at(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise LegacyFeedCompatibilityError("observed_at must include a timezone")
    return value.astimezone(UTC)


def _safe_text(
    value: object,
    location: str,
    *,
    minimum: int = 1,
    maximum: int,
) -> str:
    if not isinstance(value, str):
        raise LegacyFeedCompatibilityError(f"{location} must be text")
    normalized = value.strip()
    if (
        len(normalized) < minimum
        or len(normalized) > maximum
        or any(ord(character) < 32 for character in normalized)
    ):
        raise LegacyFeedCompatibilityError(f"{location} is invalid")
    return normalized


def _load_waiver(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise LegacyFeedCompatibilityError("expedited waiver JSON is missing or unsafe")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LegacyFeedCompatibilityError("expedited waiver JSON is invalid") from exc
    if not isinstance(value, dict):
        raise LegacyFeedCompatibilityError("expedited waiver must be an object")
    return value


def _validated_waiver(value: dict[str, Any], *, observed_at: datetime) -> dict[str, Any]:
    if value.get("exception_id") != WAIVER_EXCEPTION_ID:
        raise LegacyFeedCompatibilityError("expedited waiver exception_id is invalid")
    if value.get("release_channel") != RELEASE_CHANNEL:
        raise LegacyFeedCompatibilityError("expedited waiver release_channel is invalid")
    if value.get("approved") is not True:
        raise LegacyFeedCompatibilityError("expedited waiver must be explicitly approved")
    if value.get("reviewer_type") != "human":
        raise LegacyFeedCompatibilityError("expedited waiver requires a human reviewer")
    if value.get("ai_generated_ground_truth") is not False:
        raise LegacyFeedCompatibilityError("AI cannot approve the expedited waiver")
    if value.get("is_synthetic") is not False:
        raise LegacyFeedCompatibilityError("synthetic legacy evidence is forbidden")
    reviewer_id = _safe_text(value.get("reviewer_id"), "reviewer_id", maximum=128)
    reason = _safe_text(value.get("reason"), "reason", minimum=20, maximum=2_000)
    approved_at = _parse_timestamp(value.get("approved_at"), "approved_at")
    if approved_at > observed_at:
        raise LegacyFeedCompatibilityError("expedited waiver approval is in the future")
    if approved_at >= WAIVER_EXPIRES_AT:
        raise LegacyFeedCompatibilityError("expedited waiver was approved after its deadline")
    return {
        "exception_id": WAIVER_EXCEPTION_ID,
        "release_channel": RELEASE_CHANNEL,
        "status": "active",
        "approved": True,
        "reviewer_type": "human",
        "reviewer_id": reviewer_id,
        "approved_at": _timestamp(approved_at),
        "reason": reason,
        "ai_generated_ground_truth": False,
        "is_synthetic": False,
        "expires_at": _timestamp(WAIVER_EXPIRES_AT),
    }


def _identity_from_source(source: object) -> LegacyArtifactIdentity:
    if not isinstance(source, dict):
        raise LegacyFeedCompatibilityError("expedited legacy source metadata is missing")
    try:
        identity = LegacyArtifactIdentity(
            run_id=str(source["run_id"]),
            artifact_id=str(source["artifact_id"]),
            artifact_name=str(source["artifact_name"]),
            code_revision=str(source["code_revision"]),
            artifact_digest=str(source["artifact_digest"]),
        ).validated()
    except (KeyError, TypeError) as exc:
        raise LegacyFeedCompatibilityError(
            "expedited legacy source metadata is incomplete"
        ) from exc
    if source.get("workflow") != ".github/workflows/build-feed.yml":
        raise LegacyFeedCompatibilityError("expedited legacy source workflow is invalid")
    return identity


def _content_records(
    feed_xml: bytes,
    reports: dict[date, bytes],
    required: list[date],
    optional: dict[str, bytes],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], str]:
    feed_record = {
        "path": "feed.xml",
        "bytes": len(feed_xml),
        "sha256": _sha256(feed_xml),
    }
    report_records = [
        {
            "path": f"feed/{report_date.isoformat()}.html",
            "bytes": len(reports[report_date]),
            "sha256": _sha256(reports[report_date]),
        }
        for report_date in required
    ]
    if len({record["sha256"] for record in report_records}) != len(report_records):
        raise LegacyFeedCompatibilityError("duplicated dated report content is forbidden")
    root_records = [
        {"path": name, "bytes": len(payload), "sha256": _sha256(payload)}
        for name, payload in sorted(optional.items())
    ]
    content_lines = [f"feed.xml\0{feed_record['sha256']}"]
    content_lines.extend(
        f"{record['path']}\0{record['sha256']}" for record in report_records
    )
    content_lines.extend(f"{record['path']}\0{record['sha256']}" for record in root_records)
    return (
        feed_record,
        report_records,
        root_records,
        _sha256("\n".join(content_lines).encode("utf-8")),
    )


def _expedited_required_window(reports: dict[date, bytes]) -> list[date]:
    required = [
        EXPEDITED_WINDOW_START + timedelta(days=offset)
        for offset in range(EXPEDITED_WINDOW_DAYS)
    ]
    if required[-1] != EXPEDITED_WINDOW_END:
        raise LegacyFeedCompatibilityError("expedited legacy policy window is inconsistent")
    if set(reports) != set(required):
        missing = [item.isoformat() for item in required if item not in reports]
        extra = sorted(item.isoformat() for item in set(reports) - set(required))
        detail = ", ".join((missing + extra)[:10])
        raise LegacyFeedCompatibilityError(
            "expedited legacy artifact must contain exactly the real 89-day window"
            + (f": {detail}" if detail else "")
        )
    return required


def _expedited_manifest(
    identity: LegacyArtifactIdentity,
    feed_xml: bytes,
    reports: dict[date, bytes],
    required: list[date],
    optional: dict[str, bytes],
    *,
    prepared_at: datetime,
    waiver: dict[str, Any],
) -> dict[str, Any]:
    feed_record, report_records, root_records, content_digest = _content_records(
        feed_xml,
        reports,
        required,
        optional,
    )
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "kind": MANIFEST_KIND,
        "mode": "89_day_human_waiver",
        "release_channel": RELEASE_CHANNEL,
        "prepared_at": _timestamp(prepared_at),
        "source": identity.as_dict(),
        "window_days": EXPEDITED_WINDOW_DAYS,
        "window_start": EXPEDITED_WINDOW_START.isoformat(),
        "window_end": EXPEDITED_WINDOW_END.isoformat(),
        "dated_report_count": len(report_records),
        "complete_legacy_feed_window": True,
        "feed_xml": feed_record,
        "dated_reports": report_records,
        "root_assets": root_records,
        "content_sha256": content_digest,
        "waiver": waiver,
    }


def _standard_wrapper(
    identity: LegacyArtifactIdentity,
    standard_manifest: dict[str, Any],
    *,
    prepared_at: datetime,
) -> dict[str, Any]:
    canonical = json.dumps(
        standard_manifest,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "kind": MANIFEST_KIND,
        "mode": "standard_90_day",
        "release_channel": RELEASE_CHANNEL,
        "prepared_at": _timestamp(prepared_at),
        "source": identity.as_dict(),
        "window_days": standard_manifest["window_days"],
        "window_start": standard_manifest["window_start"],
        "window_end": standard_manifest["window_end"],
        "dated_report_count": standard_manifest["dated_report_count"],
        "content_sha256": standard_manifest["content_sha256"],
        "standard_manifest_sha256": _sha256(canonical),
        "waiver": {
            "exception_id": WAIVER_EXCEPTION_ID,
            "status": "not_required",
            "reason": "standard_90_day_window_available",
            "expires_at": _timestamp(WAIVER_EXPIRES_AT),
        },
    }


def _write_manifest(output: Path, manifest: dict[str, Any]) -> None:
    (output / MANIFEST_NAME).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _load_manifest(site: Path) -> dict[str, Any]:
    path = site / MANIFEST_NAME
    if path.is_symlink() or not path.is_file():
        raise LegacyFeedCompatibilityError(
            "expedited legacy compatibility manifest is missing or unsafe"
        )
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise LegacyFeedCompatibilityError(
            "expedited legacy compatibility manifest is invalid"
        ) from exc
    if not isinstance(value, dict):
        raise LegacyFeedCompatibilityError(
            "expedited legacy compatibility manifest must be an object"
        )
    return value


def prepare_expedited_legacy_compatibility(
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
        raise LegacyFeedCompatibilityError("legacy artifact ZIP must be a regular file")
    if _file_sha256(archive) != identity.artifact_digest:
        raise LegacyFeedCompatibilityError("legacy artifact ZIP digest does not match the pin")
    if output.exists() and (
        output.is_symlink() or not output.is_dir() or any(output.iterdir())
    ):
        raise LegacyFeedCompatibilityError("legacy compatibility output must be absent or empty")

    feed_xml, reports, optional = _load_zip_archive(archive)
    if len(reports) >= 90:
        standard = prepare_legacy_feed_compatibility(
            archive,
            output,
            identity=identity,
        )
        manifest = _standard_wrapper(identity, standard, prepared_at=observed_at)
        _write_manifest(output, manifest)
        return verify_expedited_legacy_compatibility(
            output,
            expected_identity=identity,
            observed_at=observed_at,
        )

    if observed_at >= WAIVER_EXPIRES_AT:
        raise LegacyFeedCompatibilityError(
            "the 89-day expedited exception expired; a standard 90-day artifact is required"
        )
    required = _expedited_required_window(reports)
    if waiver is None:
        raise LegacyFeedCompatibilityError("the 89-day expedited artifact requires a waiver")
    canonical_waiver = _validated_waiver(waiver, observed_at=observed_at)

    output.mkdir(parents=True, exist_ok=True)
    (output / "feed").mkdir()
    (output / "feed.xml").write_bytes(feed_xml)
    for report_date in required:
        (output / "feed" / f"{report_date.isoformat()}.html").write_bytes(
            reports[report_date]
        )
    for name, payload in optional.items():
        (output / name).write_bytes(payload)
    manifest = _expedited_manifest(
        identity,
        feed_xml,
        reports,
        required,
        optional,
        prepared_at=observed_at,
        waiver=canonical_waiver,
    )
    _write_manifest(output, manifest)
    return verify_expedited_legacy_compatibility(
        output,
        expected_identity=identity,
        observed_at=observed_at,
    )


def verify_expedited_legacy_compatibility(
    site: Path,
    *,
    expected_identity: LegacyArtifactIdentity | None,
    observed_at: datetime,
) -> dict[str, Any]:
    observed_at = _observed_at(observed_at)
    try:
        verify_public_site(site, minimum_dated_reports=EXPEDITED_WINDOW_DAYS)
    except LegacyTelegramSafetyError as exc:
        raise LegacyFeedCompatibilityError(str(exc)) from exc
    manifest = _load_manifest(site)
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise LegacyFeedCompatibilityError(
            "expedited legacy compatibility manifest schema is unsupported"
        )
    if manifest.get("kind") != MANIFEST_KIND:
        raise LegacyFeedCompatibilityError("expedited legacy compatibility kind is invalid")
    if manifest.get("release_channel") != RELEASE_CHANNEL:
        raise LegacyFeedCompatibilityError("expedited legacy release channel is invalid")
    prepared_at = _parse_timestamp(manifest.get("prepared_at"), "prepared_at")
    if prepared_at > observed_at:
        raise LegacyFeedCompatibilityError("expedited legacy manifest is from the future")
    identity = _identity_from_source(manifest.get("source"))
    if expected_identity is not None and identity != expected_identity.validated():
        raise LegacyFeedCompatibilityError(
            "expedited legacy source does not match the current pin"
        )

    mode = manifest.get("mode")
    if mode == "standard_90_day":
        standard = verify_legacy_feed_compatibility(
            site,
            expected_identity=identity,
        )
        expected = _standard_wrapper(identity, standard, prepared_at=prepared_at)
    elif mode == "89_day_human_waiver":
        if observed_at >= WAIVER_EXPIRES_AT:
            raise LegacyFeedCompatibilityError(
                "the 89-day expedited exception expired; a standard 90-day artifact is required"
            )
        if prepared_at >= WAIVER_EXPIRES_AT:
            raise LegacyFeedCompatibilityError(
                "the 89-day expedited manifest was prepared after the deadline"
            )
        waiver_value = manifest.get("waiver")
        if not isinstance(waiver_value, dict):
            raise LegacyFeedCompatibilityError("expedited legacy waiver is missing")
        canonical_waiver = _validated_waiver(waiver_value, observed_at=prepared_at)
        feed_xml, reports, optional = _load_site_directory(site)
        required = _expedited_required_window(reports)
        expected = _expedited_manifest(
            identity,
            feed_xml,
            reports,
            required,
            optional,
            prepared_at=prepared_at,
            waiver=canonical_waiver,
        )
    else:
        raise LegacyFeedCompatibilityError("expedited legacy mode is invalid")
    if manifest != expected:
        raise LegacyFeedCompatibilityError(
            "expedited legacy manifest does not match file contents"
        )
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
        description="Prepare or verify the one-time expedited legacy rollback artifact"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--archive", type=Path, required=True)
    prepare.add_argument("--output", type=Path, required=True)
    prepare.add_argument("--observed-at", required=True)
    prepare.add_argument("--waiver-json", type=Path)
    _add_identity_arguments(prepare, prefix="source")
    verify = commands.add_parser("verify")
    verify.add_argument("--site", type=Path, required=True)
    verify.add_argument("--observed-at", required=True)
    _add_identity_arguments(verify, prefix="expected_source")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        observed_at = _parse_timestamp(args.observed_at, "observed_at")
        if args.command == "prepare":
            waiver = _load_waiver(args.waiver_json) if args.waiver_json else None
            result = prepare_expedited_legacy_compatibility(
                args.archive,
                args.output,
                identity=_identity_from_args(args, prefix="source"),
                observed_at=observed_at,
                waiver=waiver,
            )
        else:
            result = verify_expedited_legacy_compatibility(
                args.site,
                expected_identity=_identity_from_args(args, prefix="expected_source"),
                observed_at=observed_at,
            )
    except LegacyFeedCompatibilityError as exc:
        print(f"expedited_legacy_compatibility_error={exc}", file=sys.stderr)
        return 1
    print(
        "expedited_legacy_compatibility="
        + json.dumps(
            {
                "mode": result["mode"],
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
