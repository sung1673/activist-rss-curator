from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "build-feed.yml"


def _workflow() -> dict[str, object]:
    return yaml.load(WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def test_legacy_pages_deploy_is_suppressed_during_expedited_observation() -> None:
    payload = _workflow()
    concurrency = payload["jobs"]["build-feed"]["concurrency"]["group"]  # type: ignore[index]
    assert "vars.PAGES_OWNER == 'legacy'" in concurrency
    assert (
        "vars.GLOBAL_ALPHA_EXPEDITED_OBSERVATION_ENABLED == 'false'"
        in concurrency
    )
    assert "bside-pages-nondeploy-legacy" in concurrency
    steps = payload["jobs"]["build-feed"]["steps"]  # type: ignore[index]
    boundary = next(
        step
        for step in steps
        if step["name"]
        == "Enforce job-start deployment snapshots at the Pages boundary"
    )
    assert boundary["id"] == "deployment_boundary"
    assert (
        payload["jobs"]["build-feed"]["env"]["EXPEDITED_OBSERVATION_SNAPSHOT"]
        == "${{ vars.GLOBAL_ALPHA_EXPEDITED_OBSERVATION_ENABLED }}"
    )
    script = boundary["run"]
    assert 'PAGES_OWNER_SNAPSHOT" == "legacy"' in script
    assert '"${EXPEDITED_OBSERVATION_SNAPSHOT,,}"' in script
    assert "deploy_allowed=true" in script
    assert "deploy_allowed=false" in script
    assert 'echo "deploy_allowed=$deploy_allowed" >> "$GITHUB_OUTPUT"' in script
    assert "github.rest.actions.getRepoVariable" not in script

    protected_steps = {
        "Configure Pages",
        "Upload Pages artifact",
        "Capture shadow preview Pages distribution start",
        "Deploy to GitHub Pages",
        "Retry GitHub Pages deployment (2/3)",
        "Retry GitHub Pages deployment (3/3)",
        "Verify GitHub Pages deployment",
        "Record final shadow preview Pages distribution outcome",
    }
    for step in steps:
        if step["name"] in protected_steps:
            assert (
                "steps.deployment_boundary.outputs.deploy_allowed == 'true'"
                in step["if"]
            )


def test_legacy_collection_and_archive_seed_remain_available_when_deploy_is_suppressed() -> None:
    payload = _workflow()
    steps = payload["jobs"]["build-feed"]["steps"]  # type: ignore[index]
    prepare = next(
        step for step in steps if step["name"] == "Prepare allowlisted legacy Pages artifact"
    )
    seed = next(
        step for step in steps if step["name"] == "Preserve sanitized legacy archive seed"
    )
    assert "deployment_boundary" not in prepare["if"]
    assert "deployment_boundary" not in seed["if"]
    assert "steps.run_mode.outputs.deploy_pages == 'true'" in seed["if"]
