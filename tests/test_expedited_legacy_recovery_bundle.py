from __future__ import annotations

import hashlib
import json
import shutil
import zipfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

from curator.expedited_legacy_compat import (
    RELEASE_CHANNEL,
    WAIVER_EXCEPTION_ID,
    WAIVER_EXPIRES_AT,
)
from curator.expedited_legacy_recovery_bundle import (
    COMPATIBILITY_DIR,
    FULL_SITE_DIR,
    MANIFEST_NAME,
    LegacyRecoveryBundleError,
    main,
    prepare_expedited_legacy_drill_site,
    prepare_expedited_legacy_recovery_bundle,
    verify_expedited_legacy_recovery_bundle,
)
from curator.global_alpha_expedited_final_approval import (
    FinalApprovalMaterialError,
    build_final_approval_template,
    derive_final_approval_materials,
    seal_final_approval,
)
from curator.legacy_feed_compat import LegacyArtifactIdentity


BEFORE_DEADLINE = datetime(2026, 7, 28, 20, 30, tzinfo=UTC)


def _html(value: str) -> str:
    return f"<!doctype html><html><body>{value}</body></html>"


def _archive(
    path: Path,
    *,
    end: date = date(2026, 7, 28),
    unexpected: bool = False,
    embedded_telegram: bool = False,
) -> Path:
    start = date(2026, 5, 1)
    with zipfile.ZipFile(path, "w") as opened:
        opened.writestr("CNAME", "news.bside.ai\n")
        opened.writestr("404.html", _html("404"))
        opened.writestr("feed.xml", "<rss><channel/></rss>")
        opened.writestr("index.html", _html("legacy root"))
        for name in ("index", "latest", "search", "telegram-admin", "telegram"):
            opened.writestr(f"feed/{name}.html", _html(name))
        current = start
        while current <= end:
            body = current.isoformat()
            if embedded_telegram and current == end:
                body = (
                    "<script data-story-telegram-mentions>"
                    '[{"message_url":"https://t.me/private/42"}]'
                    "</script>"
                )
            opened.writestr(
                f"feed/{current.isoformat()}.html",
                _html(body),
            )
            current += timedelta(days=1)
        if unexpected:
            opened.writestr("state.json", "{}")
    return path


def _identity(path: Path) -> LegacyArtifactIdentity:
    return LegacyArtifactIdentity(
        run_id="12345",
        artifact_id="67890",
        artifact_name="legacy-pages-archive-seed",
        code_revision="a" * 40,
        artifact_digest="sha256:" + hashlib.sha256(path.read_bytes()).hexdigest(),
    )


def _waiver() -> dict[str, object]:
    return {
        "exception_id": WAIVER_EXCEPTION_ID,
        "release_channel": RELEASE_CHANNEL,
        "approved": True,
        "reviewer_type": "human",
        "reviewer_id": "production-owner",
        "approved_at": "2026-07-28T20:25:00Z",
        "reason": "Approve the exact full legacy site for the 89-day Early Access drill.",
        "ai_generated_ground_truth": False,
        "is_synthetic": False,
    }


def _preparation(
    root: Path,
    archive: Path,
    *,
    evidence_as_of: datetime = BEFORE_DEADLINE,
) -> Path:
    root.mkdir()
    shutil.copyfile(archive, root / "pinned-legacy.zip")
    identity = _identity(archive)
    revision = "a" * 40
    human_approval_chain_sha256 = hashlib.sha256(
        b"human-approval-chain"
    ).hexdigest()
    base = {
        "human_review_section_sha256": "1" * 64,
        "human_approval_chain_sha256": human_approval_chain_sha256,
        "pages_terminal_content_sha256": "2" * 64,
        "content_integrity_sha256": "3" * 64,
        "experience_sha256": "4" * 64,
        "rollback_drill_sha256": "5" * 64,
        "observations_sha256": "6" * 64,
        "legacy_source_artifact_sha256": (
            identity.artifact_digest.removeprefix("sha256:")
        ),
    }
    base_digest = hashlib.sha256(
        json.dumps(
            base,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    (root / "expedited-preparation.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "kind": "bside-global-alpha-expedited-preparation-input",
                "code_revision": revision,
                "release_channel": "production_alpha_early_access",
                "evidence_as_of": evidence_as_of.isoformat(),
                "human_review": {
                    "carry_forward": {
                        "human_approval_chain_sha256": (
                            human_approval_chain_sha256
                        )
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    (root / "approval-binding-materials.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "kind": (
                    "bside-global-alpha-expedited-approval-binding-materials"
                ),
                "code_revision": revision,
                **base,
                "binding_base_sha256": base_digest,
            }
        ),
        encoding="utf-8",
    )
    (root / "rollback-preparation.json").write_text(
        json.dumps(
            {
                "legacy_archive": {
                    "run_id": identity.run_id,
                    "artifact_id": identity.artifact_id,
                    "artifact_name": identity.artifact_name,
                    "code_revision": identity.code_revision,
                    "artifact_digest": identity.artifact_digest,
                }
            }
        ),
        encoding="utf-8",
    )
    return root


def test_89_day_bundle_sanitizes_and_verifies_the_legacy_site(
    tmp_path: Path,
) -> None:
    archive = _archive(tmp_path / "legacy.zip")
    identity = _identity(archive)
    bundle = tmp_path / "bundle"

    manifest = prepare_expedited_legacy_recovery_bundle(
        archive,
        bundle,
        identity=identity,
        observed_at=BEFORE_DEADLINE,
        waiver=_waiver(),
    )

    assert manifest["mode"] == "89_day_human_waiver"
    assert manifest["window_days"] == 89
    assert (bundle / FULL_SITE_DIR / "index.html").is_file()
    assert (bundle / FULL_SITE_DIR / "feed.xml").is_file()
    assert not (bundle / FULL_SITE_DIR / "feed" / "telegram-admin.html").exists()
    assert not (bundle / FULL_SITE_DIR / "feed" / "telegram.html").exists()
    assert len(list((bundle / FULL_SITE_DIR / "feed").glob("20*.html"))) == 89
    assert len(list((bundle / COMPATIBILITY_DIR / "feed").glob("*.html"))) == 89
    assert manifest["full_site"]["file_count"] == 96
    assert verify_expedited_legacy_recovery_bundle(
        bundle,
        expected_identity=identity,
        observed_at=BEFORE_DEADLINE,
    ) == manifest


def test_drill_site_uses_real_immutable_archive_without_creating_waiver_evidence(
    tmp_path: Path,
) -> None:
    archive = _archive(tmp_path / "legacy.zip")
    identity = _identity(archive)
    drill_site = tmp_path / "drill-site"

    receipt = prepare_expedited_legacy_drill_site(
        archive,
        drill_site,
        identity=identity,
    )

    assert receipt["kind"] == "bside-expedited-legacy-drill-site"
    assert receipt["mode"] == "89_day_human_waiver"
    assert receipt["window_days"] == 89
    assert receipt["source"] == identity.validated().as_dict()
    assert (drill_site / "index.html").is_file()
    assert (drill_site / "feed.xml").is_file()
    assert not (drill_site / "feed" / "telegram-admin.html").exists()
    assert not (drill_site / "feed" / "telegram.html").exists()
    assert len(list((drill_site / "feed").glob("20*.html"))) == 89
    assert not (drill_site / COMPATIBILITY_DIR).exists()
    assert not (drill_site / MANIFEST_NAME).exists()
    assert "waiver" not in receipt


def test_prepare_drill_site_cli_does_not_require_an_observed_at(
    tmp_path: Path,
) -> None:
    archive = _archive(tmp_path / "legacy.zip")
    identity = _identity(archive)
    receipt_path = tmp_path / "drill-receipt.json"

    assert (
        main(
            [
                "prepare-drill-site",
                "--archive",
                str(archive),
                "--output",
                str(tmp_path / "drill-site"),
                "--receipt",
                str(receipt_path),
                "--source-run-id",
                identity.run_id,
                "--source-artifact-id",
                identity.artifact_id,
                "--source-artifact-name",
                identity.artifact_name,
                "--source-code-revision",
                identity.code_revision,
                "--source-artifact-digest",
                identity.artifact_digest,
            ]
        )
        == 0
    )
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["kind"] == "bside-expedited-legacy-drill-site"
    assert receipt["window_days"] == 89


def test_final_approval_helper_reproduces_exact_binding_and_seals_human_input(
    tmp_path: Path,
) -> None:
    archive = _archive(tmp_path / "legacy.zip")
    preparation = _preparation(tmp_path / "preparation", archive)
    first = derive_final_approval_materials(
        preparation,
        tmp_path / "first",
        expected_revision="a" * 40,
        current_time=BEFORE_DEADLINE + timedelta(minutes=1),
        waiver=_waiver(),
    )
    second = derive_final_approval_materials(
        preparation,
        tmp_path / "second",
        expected_revision="a" * 40,
        current_time=BEFORE_DEADLINE + timedelta(minutes=2),
        waiver=_waiver(),
    )

    assert first["binding"] == second["binding"]
    assert first["binding"]["human_approval_chain_sha256"] == hashlib.sha256(
        b"human-approval-chain"
    ).hexdigest()
    assert (
        first["legacy_archive"]["compatibility_manifest_sha256"]
        == second["legacy_archive"]["compatibility_manifest_sha256"]
    )
    template = build_final_approval_template(
        code_revision="a" * 40,
        binding=first["binding"],
        waiver=_waiver(),
    )
    decided_at = (BEFORE_DEADLINE + timedelta(minutes=3)).isoformat()
    approval = template["approval"]
    approval["collected_at"] = decided_at
    for record in approval["approvals"]:
        record["decision"] = "approved"
        record["approver_reference"] = "oversight-1"
        record["decided_at"] = decided_at
    sealed = seal_final_approval(
        template,
        expected_revision="a" * 40,
        binding=first["binding"],
        current_time=BEFORE_DEADLINE + timedelta(minutes=4),
    )
    assert sealed["approval"]["evidence_binding"] == first["binding"]
    assert len(sealed["approval"]["section_sha256"]) == 64
    assert sealed["legacy_waiver"] == _waiver()


@pytest.mark.parametrize("mutation", ("omitted", "mismatched"))
def test_final_approval_rejects_unbound_human_approval_chain(
    tmp_path: Path,
    mutation: str,
) -> None:
    archive = _archive(tmp_path / "legacy.zip")
    preparation = _preparation(tmp_path / "preparation", archive)
    record_path = preparation / "expedited-preparation.json"
    record = json.loads(record_path.read_text(encoding="utf-8"))
    carry_forward = record["human_review"]["carry_forward"]
    if mutation == "omitted":
        carry_forward.pop("human_approval_chain_sha256")
    else:
        carry_forward["human_approval_chain_sha256"] = "f" * 64
    record_path.write_text(json.dumps(record), encoding="utf-8")

    with pytest.raises(
        FinalApprovalMaterialError,
        match="human approval chain|human_approval_chain_sha256",
    ):
        derive_final_approval_materials(
            preparation,
            tmp_path / "final",
            expected_revision="a" * 40,
            current_time=BEFORE_DEADLINE + timedelta(minutes=1),
            waiver=_waiver(),
        )


def test_90_day_bundle_uses_standard_mode_after_deadline(tmp_path: Path) -> None:
    archive = _archive(tmp_path / "legacy.zip", end=date(2026, 7, 29))
    identity = _identity(archive)
    observed_at = WAIVER_EXPIRES_AT + timedelta(minutes=1)

    manifest = prepare_expedited_legacy_recovery_bundle(
        archive,
        tmp_path / "bundle",
        identity=identity,
        observed_at=observed_at,
    )

    assert manifest["mode"] == "standard_90_day"
    assert manifest["window_days"] == 90
    assert manifest["window_end"] == "2026-07-29"


def test_bundle_rejects_89_days_after_cutoff_and_unexpected_files(
    tmp_path: Path,
) -> None:
    archive = _archive(tmp_path / "legacy.zip")
    with pytest.raises(LegacyRecoveryBundleError, match="expired"):
        prepare_expedited_legacy_recovery_bundle(
            archive,
            tmp_path / "late",
            identity=_identity(archive),
            observed_at=WAIVER_EXPIRES_AT,
            waiver=_waiver(),
        )

    unexpected = _archive(tmp_path / "unexpected.zip", unexpected=True)
    with pytest.raises(LegacyRecoveryBundleError, match="unexpected file"):
        prepare_expedited_legacy_recovery_bundle(
            unexpected,
            tmp_path / "unexpected",
            identity=_identity(unexpected),
            observed_at=BEFORE_DEADLINE,
            waiver=_waiver(),
        )


def test_bundle_redacts_embedded_telegram_exposure(tmp_path: Path) -> None:
    archive = _archive(tmp_path / "legacy.zip", embedded_telegram=True)
    bundle = tmp_path / "bundle"
    identity = _identity(archive)

    prepare_expedited_legacy_recovery_bundle(
        archive,
        bundle,
        identity=identity,
        observed_at=BEFORE_DEADLINE,
        waiver=_waiver(),
    )

    for root in (FULL_SITE_DIR, COMPATIBILITY_DIR):
        report = (
            bundle / root / "feed" / "2026-07-28.html"
        ).read_bytes()
        assert b"data-story-telegram-mentions>[]</script>" in report
        assert b"https://t.me/" not in report
    assert verify_expedited_legacy_recovery_bundle(
        bundle,
        expected_identity=identity,
        observed_at=BEFORE_DEADLINE,
    )


def test_bundle_verify_rejects_reintroduced_telegram_exposure(
    tmp_path: Path,
) -> None:
    archive = _archive(tmp_path / "legacy.zip")
    identity = _identity(archive)
    bundle = tmp_path / "bundle"
    prepare_expedited_legacy_recovery_bundle(
        archive,
        bundle,
        identity=identity,
        observed_at=BEFORE_DEADLINE,
        waiver=_waiver(),
    )
    report = bundle / FULL_SITE_DIR / "feed" / "2026-07-28.html"
    report.write_text(
        _html(
            "<script data-story-telegram-mentions>"
            '[{"message_url":"https://t.me/private/42"}]'
            "</script>"
        ),
        encoding="utf-8",
    )

    with pytest.raises(LegacyRecoveryBundleError, match="Telegram"):
        verify_expedited_legacy_recovery_bundle(
            bundle,
            expected_identity=identity,
            observed_at=BEFORE_DEADLINE,
        )


def test_full_site_and_manifest_tampering_are_rejected(tmp_path: Path) -> None:
    archive = _archive(tmp_path / "legacy.zip")
    identity = _identity(archive)
    bundle = tmp_path / "bundle"
    prepare_expedited_legacy_recovery_bundle(
        archive,
        bundle,
        identity=identity,
        observed_at=BEFORE_DEADLINE,
        waiver=_waiver(),
    )
    (bundle / FULL_SITE_DIR / "index.html").write_text(
        _html("tampered"),
        encoding="utf-8",
    )
    with pytest.raises(LegacyRecoveryBundleError, match="does not match"):
        verify_expedited_legacy_recovery_bundle(
            bundle,
            expected_identity=identity,
            observed_at=BEFORE_DEADLINE,
        )

    (bundle / FULL_SITE_DIR / "index.html").write_text(
        _html("legacy root"),
        encoding="utf-8",
    )
    manifest_path = bundle / MANIFEST_NAME
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["window_days"] = 88
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(LegacyRecoveryBundleError, match="does not match"):
        verify_expedited_legacy_recovery_bundle(
            bundle,
            expected_identity=identity,
            observed_at=BEFORE_DEADLINE,
        )
