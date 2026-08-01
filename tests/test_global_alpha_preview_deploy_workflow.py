from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "global-alpha-preview-deploy.yml"
PREPARATION = (
    ROOT / ".github" / "workflows" / "global-alpha-expedited-preparation.yml"
)
CUTOVER = ROOT / ".github" / "workflows" / "governance-expedited-cutover.yml"
HANDOFF = ROOT / ".github" / "workflows" / "governance-expedited-handoff.yml"


def _payload() -> dict[str, object]:
    return yaml.load(WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def test_collection_free_preview_is_manual_protected_and_exact_sha() -> None:
    payload = _payload()
    dispatch = payload["on"]["workflow_dispatch"]  # type: ignore[index]
    job = payload["jobs"]["deploy-preview"]  # type: ignore[index]
    text = WORKFLOW.read_text(encoding="utf-8")

    assert set(dispatch["inputs"]) == {
        "confirmation",
        "legacy_pages_run_id",
        "governance_pages_run_id",
    }
    assert "github.ref_name == github.event.repository.default_branch" in job["if"]
    assert job["environment"]["name"] == "github-pages"
    assert payload["concurrency"]["group"] == (
        "bside-pages-deployment-${{ github.repository }}"
    )
    assert "DEPLOY_COLLECTION_FREE_GOVERNANCE_PREVIEW" in text
    assert '"$(git rev-parse HEAD)" == "$GITHUB_SHA"' in text
    for marker in (
        '"$PAGES_OWNER_SNAPSHOT" == "legacy"',
        '"$PIPELINE_MODE_SNAPSHOT" == "shadow"',
        '"${STANDARD_OBSERVATION_SNAPSHOT,,}" == "false"',
        '"${EXPEDITED_OBSERVATION_SNAPSHOT,,}" == "false"',
        '"${TELEGRAM_DELIVERY_SNAPSHOT,,}" == "false"',
        '"${GOVERNANCE_DELIVERY_SNAPSHOT,,}" == "false"',
        '"${KIND_MODE_SNAPSHOT,,}" == "off"',
    ):
        assert marker in text


def test_collection_free_preview_uses_only_immutable_existing_sources() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    assert 'daily.path !== ".github/workflows/daily.yml"' in text
    assert "daily.head_sha" in text
    assert 'currentLegacy.path !== ".github/workflows/build-feed.yml"' in text
    assert 'item.name === "github-pages"' in text
    assert "Current legacy Pages run must have one immutable github-pages artifact" in text
    assert "Prove the immutable legacy source matches the public root" in text
    assert "LEGACY_ROLLBACK_RUN_ID" in text
    assert "LEGACY_ROLLBACK_ARTIFACT_DIGEST" in text
    assert "Pinned legacy artifact digest changed" in text
    assert ".github/scripts/prepare-legacy-pages.py" in text
    assert "--governance-preview-source daily-pages/governance" in text
    assert "curator.governance_site_config" in text
    assert '"daily-pages/governance/$asset"' in text
    assert '"preview-site/governance/$asset"' in text
    assert "python -m curator.governance_site_config \\\n            --site preview-site" not in text
    assert "curator.legacy_telegram_safety verify-site" in text


def test_collection_free_preview_redacts_only_after_proving_the_live_legacy_source() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    materialize = text[
        text.index("Materialize and structurally verify the currently-served legacy source") :
        text.index("Prove the immutable legacy source matches the public root")
    ]
    assemble = text[
        text.index("Assemble legacy root plus exact governance Preview") :
        text.index("Record collection-free source provenance")
    ]

    assert "legacy_telegram_safety verify-site" not in materialize
    assert '[[ "${#symbolic_links[@]}" -ne 0 ]]' in materialize
    assert '[[ "${#dated_reports[@]}" -ge 90 ]]' in materialize
    assert "--source current-legacy-site" in assemble
    assert "--destination preview-site" in assemble
    assert "curator.legacy_telegram_safety verify-site" in assemble
    assert "curator.legacy_internal_safety verify-site" in assemble
    assert "--minimum-dated-reports 90" in assemble


def test_collection_free_preview_never_runs_collection_or_delivery() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    for prohibited in (
        "python -m curator.main",
        "collect_telegram_sources",
        "TELEGRAM_API_ID",
        "TELEGRAM_API_HASH",
        "TELEGRAM_SESSION",
        "TELEGRAM_SESSION_STRING",
        "CURATOR_FEEDS",
    ):
        assert prohibited not in text
    assert "telegram_collection_executed: false" in text
    assert "telegram_delivery_executed: false" in text


def test_collection_free_preview_deploys_one_exact_pages_artifact_and_smokes_bytes() -> None:
    payload = _payload()
    steps = payload["jobs"]["deploy-preview"]["steps"]  # type: ignore[index]
    text = WORKFLOW.read_text(encoding="utf-8")

    upload = next(
        step
        for step in steps
        if step.get("name") == "Upload exact Preview Pages artifact"
    )
    deploy = next(
        step
        for step in steps
        if step.get("uses")
        == "actions/deploy-pages@cd2ce8fcbc39b97be8ca5fce6e763baed58fa128"
    )
    assert upload["with"] == {"name": "github-pages", "path": "preview-site"}
    assert deploy["with"]["artifact_name"] == "github-pages"
    assert "cmp --silent preview-site/index.html remote-preview/index.html" in text
    assert "cmp --silent preview-site/governance/app.js" in text
    assert "production_alpha_early_access" in text
    assert "A prohibited internal marker is present" in text


def test_collection_free_preview_has_exact_automatic_legacy_recovery() -> None:
    payload = _payload()
    steps = payload["jobs"]["deploy-preview"]["steps"]  # type: ignore[index]
    text = WORKFLOW.read_text(encoding="utf-8")

    recovery_upload = next(
        step
        for step in steps
        if step.get("name") == "Upload exact pre-deployment legacy recovery artifact"
    )
    recovery_deploy = next(
        step
        for step in steps
        if step.get("name")
        == "Restore exact pre-deployment legacy Pages on Preview failure"
    )
    assert recovery_upload["with"] == {
        "name": "legacy-restore-pages",
        "path": "current-legacy-site",
    }
    assert recovery_deploy["with"]["artifact_name"] == "legacy-restore-pages"
    assert recovery_deploy["continue-on-error"] == "true"
    assert "steps.legacy_recovery_pages_artifact.outcome == 'success'" in recovery_deploy["if"]
    assert "steps.preview_smoke.outcome != 'success'" in recovery_deploy["if"]
    assert "Verify automatic legacy recovery bytes" in text
    assert "Preview deployment failed and the exact legacy site was restored" in text


def test_expedited_preparation_accepts_only_existing_or_collection_free_preview() -> None:
    text = PREPARATION.read_text(encoding="utf-8")
    resolver = text[
        text.index("const allowedPreviewWorkflows") : text.index(
            "const previewArtifacts", text.index("const allowedPreviewWorkflows")
        )
    ]

    assert '".github/workflows/build-feed.yml"' in resolver
    assert '".github/workflows/global-alpha-preview-deploy.yml"' in resolver
    assert "allowedPreviewWorkflows.has(preview.path)" in resolver
    assert "preview.status !== \"completed\"" in resolver
    assert "preview.conclusion !== \"success\"" in resolver
    assert "preview.event !== \"workflow_dispatch\"" in resolver
    assert "preview.head_sha" in resolver


def test_expedited_preparation_verifies_nested_preview_config_without_root_config() -> None:
    text = PREPARATION.read_text(encoding="utf-8")
    segment = text[
        text.index("Extract and verify exact legacy plus governance Preview restore") :
        text.index("Record immutable rollback preparation provenance")
    ]

    assert "test ! -e rollback-prepared/preview-site/config.js" in segment
    assert "rollback-prepared/preview-config-check/config.js" in segment
    assert "rollback-prepared/preview-config-check/governance/config.js" in segment
    assert "--site rollback-prepared/preview-config-check" in segment
    assert "--site rollback-prepared/preview-site" not in segment


def test_expedited_fences_include_collection_free_preview_writer() -> None:
    workflow_path = '".github/workflows/global-alpha-preview-deploy.yml"'
    preparation = PREPARATION.read_text(encoding="utf-8")
    preparation_drain = preparation[
        preparation.index("const workflowIds") : preparation.index(
            "const activeStatuses", preparation.index("const workflowIds")
        )
    ]

    assert workflow_path in preparation_drain
    assert workflow_path in CUTOVER.read_text(encoding="utf-8")
    assert workflow_path in HANDOFF.read_text(encoding="utf-8")
