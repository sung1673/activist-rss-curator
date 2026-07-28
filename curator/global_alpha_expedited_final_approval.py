from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from .expedited_legacy_compat import WAIVER_EXPIRES_AT
from .expedited_legacy_recovery_bundle import (
    COMPATIBILITY_DIR,
    prepare_expedited_legacy_recovery_bundle,
)
from .global_alpha_expedited_gate import (
    APPROVAL_KIND,
    APPROVAL_SECTION_FIELDS,
    validate_expedited_approval,
)
from .legacy_feed_compat import LegacyArtifactIdentity
from .legacy_recovery_bundle import LegacyRecoveryBundleError


PREPARATION_KIND = "bside-global-alpha-expedited-preparation-input"
BINDING_MATERIALS_KIND = (
    "bside-global-alpha-expedited-approval-binding-materials"
)
FINAL_BINDING_KIND = (
    "bside-global-alpha-expedited-final-approval-binding-materials"
)
LEGACY_ARCHIVE_KIND = "bside-global-alpha-expedited-legacy-archive"
RELEASE_CHANNEL = "production_alpha_early_access"
BASE_BINDING_FIELDS = (
    "human_review_section_sha256",
    "pages_terminal_content_sha256",
    "content_integrity_sha256",
    "experience_sha256",
    "rollback_drill_sha256",
    "observations_sha256",
    "legacy_source_artifact_sha256",
)


class FinalApprovalMaterialError(ValueError):
    pass


def _canonical_sha256(value: object) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _timestamp(value: object, location: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise FinalApprovalMaterialError(
            f"{location} must be a timezone-aware timestamp"
        )
    try:
        parsed = datetime.fromisoformat(
            value.strip().replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise FinalApprovalMaterialError(
            f"{location} must be a timezone-aware timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise FinalApprovalMaterialError(
            f"{location} must include a timezone"
        )
    return parsed.astimezone(UTC)


def _timestamp_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise FinalApprovalMaterialError(
            "current_time must include a timezone"
        )
    return (
        value.astimezone(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _load_object(path: Path, location: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file():
        raise FinalApprovalMaterialError(f"{location} is missing or unsafe")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise FinalApprovalMaterialError(
            f"{location} is not valid JSON"
        ) from exc
    if not isinstance(value, dict):
        raise FinalApprovalMaterialError(f"{location} must be an object")
    return value


def _write_object(path: Path, value: object) -> None:
    path.write_text(
        json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _require_digest(value: object, location: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise FinalApprovalMaterialError(
            f"{location} must be a lowercase SHA-256 digest"
        )
    return value


def _legacy_identity(preparation: Path) -> LegacyArtifactIdentity:
    rollback = _load_object(
        preparation / "rollback-preparation.json",
        "rollback-preparation.json",
    )
    raw = rollback.get("legacy_archive")
    if not isinstance(raw, dict):
        raise FinalApprovalMaterialError(
            "rollback-preparation.json omitted legacy_archive"
        )
    try:
        return LegacyArtifactIdentity(
            run_id=str(raw["run_id"]),
            artifact_id=str(raw["artifact_id"]),
            artifact_name=str(raw["artifact_name"]),
            code_revision=str(raw["code_revision"]),
            artifact_digest=str(raw["artifact_digest"]),
        ).validated()
    except (KeyError, TypeError, ValueError) as exc:
        raise FinalApprovalMaterialError(
            "rollback-preparation legacy identity is invalid"
        ) from exc


def _validate_preparation(
    preparation: Path,
    *,
    expected_revision: str,
) -> tuple[dict[str, Any], dict[str, Any], LegacyArtifactIdentity]:
    if (
        preparation.is_symlink()
        or not preparation.is_dir()
        or len(expected_revision) != 40
        or any(
            character not in "0123456789abcdef"
            for character in expected_revision
        )
    ):
        raise FinalApprovalMaterialError(
            "preparation directory or expected revision is invalid"
        )
    record = _load_object(
        preparation / "expedited-preparation.json",
        "expedited-preparation.json",
    )
    if (
        record.get("kind") != PREPARATION_KIND
        or record.get("code_revision") != expected_revision
        or record.get("release_channel") != RELEASE_CHANNEL
        or not isinstance(record.get("human_review"), dict)
        or "approval" in record
    ):
        raise FinalApprovalMaterialError(
            "expedited preparation contract is invalid"
        )
    materials = _load_object(
        preparation / "approval-binding-materials.json",
        "approval-binding-materials.json",
    )
    if (
        materials.get("kind") != BINDING_MATERIALS_KIND
        or materials.get("code_revision") != expected_revision
    ):
        raise FinalApprovalMaterialError(
            "preparation binding materials contract is invalid"
        )
    base = {
        field: _require_digest(materials.get(field), field)
        for field in BASE_BINDING_FIELDS
    }
    if _canonical_sha256(base) != materials.get("binding_base_sha256"):
        raise FinalApprovalMaterialError(
            "preparation binding base digest mismatch"
        )
    return record, materials, _legacy_identity(preparation)


def derive_final_approval_materials(
    preparation: Path,
    output: Path,
    *,
    expected_revision: str,
    current_time: datetime,
    waiver: Mapping[str, object] | None = None,
) -> dict[str, Any]:
    if current_time.tzinfo is None or current_time.utcoffset() is None:
        raise FinalApprovalMaterialError(
            "current_time must include a timezone"
        )
    current_time = current_time.astimezone(UTC)
    record, materials, identity = _validate_preparation(
        preparation,
        expected_revision=expected_revision,
    )
    if (
        materials["legacy_source_artifact_sha256"]
        != identity.artifact_digest.removeprefix("sha256:")
    ):
        raise FinalApprovalMaterialError(
            "preparation legacy source digest does not match the pinned archive"
        )
    if output.exists() and (
        output.is_symlink()
        or not output.is_dir()
        or any(output.iterdir())
    ):
        raise FinalApprovalMaterialError(
            "final approval output must be absent or empty"
        )
    output.mkdir(parents=True, exist_ok=True)
    observed_at = _timestamp(
        record.get("evidence_as_of"),
        "preparation.evidence_as_of",
    )
    waiver_object = dict(waiver) if waiver is not None else None
    bundle = output / "finalized-legacy-recovery-bundle"
    try:
        recovery = prepare_expedited_legacy_recovery_bundle(
            preparation / "pinned-legacy.zip",
            bundle,
            identity=identity,
            observed_at=observed_at,
            waiver=waiver_object,
        )
    except (LegacyRecoveryBundleError, ValueError) as exc:
        raise FinalApprovalMaterialError(str(exc)) from exc
    compatibility = _load_object(
        bundle
        / COMPATIBILITY_DIR
        / "legacy-feed-expedited-compatibility.json",
        "legacy compatibility manifest",
    )
    mode = compatibility.get("mode")
    if mode == "89_day_human_waiver":
        if waiver_object is None:
            raise FinalApprovalMaterialError(
                "the exact 89-day archive requires a human waiver"
            )
        if current_time >= WAIVER_EXPIRES_AT:
            raise FinalApprovalMaterialError(
                "the 89-day exception expired; an actual 90-day archive is required"
            )
    elif mode == "standard_90_day":
        if waiver_object is not None:
            raise FinalApprovalMaterialError(
                "a legacy waiver is forbidden when a real 90-day archive is available"
            )
    else:
        raise FinalApprovalMaterialError(
            "the finalized legacy compatibility mode is invalid"
        )
    collected_at = _timestamp_text(current_time)
    legacy_archive = {
        "schema_version": 1,
        "kind": LEGACY_ARCHIVE_KIND,
        "environment": "production",
        "evidence_source": "immutable_full_site_recovery_bundle",
        "is_synthetic": False,
        "code_revision": expected_revision,
        "collected_at": collected_at,
        "evidence_as_of": collected_at,
        "archive_sha256": identity.artifact_digest.removeprefix("sha256:"),
        "artifact_id": int(identity.artifact_id),
        "artifact_name": identity.artifact_name,
        "consecutive_day_count": int(compatibility["window_days"]),
        "first_date": compatibility["window_start"],
        "last_date": compatibility["window_end"],
        "generated_at": compatibility["prepared_at"],
        "contains_placeholder": False,
        "duplicate_content_count": 0,
        "compatibility_manifest": compatibility,
        "compatibility_manifest_sha256": _canonical_sha256(compatibility),
        "waiver": compatibility["waiver"],
        "waiver_sha256": _canonical_sha256(compatibility["waiver"]),
    }
    base = {field: materials[field] for field in BASE_BINDING_FIELDS}
    sections = {
        **base,
        "legacy_manifest_sha256": legacy_archive[
            "compatibility_manifest_sha256"
        ],
    }
    binding = {
        **sections,
        "binding_sha256": _canonical_sha256(sections),
    }
    final_materials = {
        "schema_version": 1,
        "kind": FINAL_BINDING_KIND,
        "code_revision": expected_revision,
        "preparation_evidence_as_of": record["evidence_as_of"],
        **binding,
    }
    _write_object(output / "legacy-archive.json", legacy_archive)
    _write_object(
        output / "final-approval-binding-materials.json",
        final_materials,
    )
    return {
        "recovery": recovery,
        "legacy_archive": legacy_archive,
        "binding": binding,
        "final_binding_materials": final_materials,
    }


def build_final_approval_template(
    *,
    code_revision: str,
    binding: Mapping[str, object],
    waiver: Mapping[str, object] | None,
) -> dict[str, Any]:
    approval = {
        "schema_version": 1,
        "kind": APPROVAL_KIND,
        "environment": "production",
        "evidence_source": "protected_human_approval",
        "is_synthetic": False,
        "code_revision": code_revision,
        "collected_at": "REPLACE_WITH_UTC_TIMESTAMP",
        "release_tier_acknowledged": "production-alpha-early-access",
        "ga_certification_claimed": False,
        "expedited_waiver_acknowledged": True,
        "evidence_binding": dict(binding),
        "approvals": [
            {
                "role": role,
                "decision": "REPLACE_WITH_APPROVED_OR_REJECTED",
                "approver_type": "human",
                "approver_reference": "REPLACE_WITH_HUMAN_REFERENCE",
                "decided_at": "REPLACE_WITH_UTC_TIMESTAMP",
                "evidence_sha256": binding["binding_sha256"],
            }
            for role in ("oversight", "source-rights", "expedited-risk")
        ],
        "section_sha256": "RECOMPUTED_BY_SEAL_COMMAND",
    }
    payload: dict[str, Any] = {
        "_instructions": {
            "command": (
                "Fill only the human reference, decision, and UTC timestamp "
                "fields, then run the seal command."
            ),
            "immutable_evidence_binding_sha256": binding[
                "binding_sha256"
            ],
        },
        "approval": approval,
    }
    if waiver is not None:
        payload["legacy_waiver"] = dict(waiver)
    return payload


def seal_final_approval(
    template: Mapping[str, object],
    *,
    expected_revision: str,
    binding: Mapping[str, object],
    current_time: datetime,
) -> dict[str, Any]:
    approval_value = template.get("approval")
    if not isinstance(approval_value, dict):
        raise FinalApprovalMaterialError(
            "final approval template omitted approval"
        )
    approval = dict(approval_value)
    if approval.get("evidence_binding") != dict(binding):
        raise FinalApprovalMaterialError(
            "final approval template binding changed"
        )
    for field in ("artifact_id", "artifact_name", "artifact_sha256"):
        if field in approval:
            raise FinalApprovalMaterialError(
                "final approval cannot self-assert post-upload identity"
            )
    section = {field: approval.get(field) for field in APPROVAL_SECTION_FIELDS}
    approval["section_sha256"] = _canonical_sha256(section)
    validation = {
        **approval,
        "artifact_id": 1,
        "artifact_name": "pending-expedited-final-approval",
        "artifact_sha256": "0" * 64,
    }
    try:
        _summary, gates = validate_expedited_approval(
            validation,
            expected_revision=expected_revision,
            evidence_as_of=current_time.astimezone(UTC),
            required_evidence_binding=binding,
        )
    except ValueError as exc:
        raise FinalApprovalMaterialError(str(exc)) from exc
    if any(gate.get("passed") is not True for gate in gates):
        raise FinalApprovalMaterialError(
            "final human approval gates did not pass"
        )
    result: dict[str, Any] = {"approval": approval}
    waiver = template.get("legacy_waiver")
    if waiver is not None:
        if not isinstance(waiver, dict):
            raise FinalApprovalMaterialError(
                "legacy_waiver must be an object"
            )
        result["legacy_waiver"] = dict(waiver)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Derive or seal deterministic expedited final approval materials"
        )
    )
    commands = parser.add_subparsers(dest="command", required=True)
    template = commands.add_parser("template")
    template.add_argument("--preparation", type=Path, required=True)
    template.add_argument("--output", type=Path, required=True)
    template.add_argument("--expected-revision", required=True)
    template.add_argument("--current-time")
    template.add_argument("--waiver-json", type=Path)
    seal = commands.add_parser("seal")
    seal.add_argument("--preparation", type=Path, required=True)
    seal.add_argument("--workspace", type=Path, required=True)
    seal.add_argument("--template", type=Path, required=True)
    seal.add_argument("--output", type=Path, required=True)
    seal.add_argument("--expected-revision", required=True)
    seal.add_argument("--current-time")
    seal.add_argument("--waiver-json", type=Path)
    return parser


def _current_time(value: str | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    return _timestamp(value, "current_time")


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        waiver = (
            _load_object(args.waiver_json, "legacy waiver")
            if args.waiver_json
            else None
        )
        now = _current_time(args.current_time)
        if args.command == "template":
            derived = derive_final_approval_materials(
                args.preparation,
                args.output,
                expected_revision=args.expected_revision.lower(),
                current_time=now,
                waiver=waiver,
            )
            template = build_final_approval_template(
                code_revision=args.expected_revision.lower(),
                binding=derived["binding"],
                waiver=waiver,
            )
            _write_object(
                args.output / "final-approval-template.json",
                template,
            )
            result_path = args.output / "final-approval-template.json"
        else:
            derived = derive_final_approval_materials(
                args.preparation,
                args.workspace,
                expected_revision=args.expected_revision.lower(),
                current_time=now,
                waiver=waiver,
            )
            template = _load_object(
                args.template,
                "final approval template",
            )
            if template.get("legacy_waiver") != waiver:
                raise FinalApprovalMaterialError(
                    "final approval template waiver differs from the exact waiver input"
                )
            sealed = seal_final_approval(
                template,
                expected_revision=args.expected_revision.lower(),
                binding=derived["binding"],
                current_time=now,
            )
            _write_object(args.output, sealed)
            result_path = args.output
    except (FinalApprovalMaterialError, LegacyRecoveryBundleError) as exc:
        print(f"expedited_final_approval_error={exc}", file=sys.stderr)
        return 1
    print(f"expedited_final_approval={result_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
