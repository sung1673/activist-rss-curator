from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "build-feed.yml"


def _workflow() -> dict[str, object]:
    return yaml.load(WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)


def test_legacy_pages_deploy_is_suppressed_during_expedited_observation() -> None:
    payload = _workflow()
    steps = payload["jobs"]["build-feed"]["steps"]  # type: ignore[index]
    boundary = next(
        step
        for step in steps
        if step["name"] == "Revalidate legacy ownership at the deployment boundary"
    )
    assert boundary["id"] == "deployment_boundary"
    assert boundary["uses"] == (
        "actions/github-script@"
        "3a2844b7e9c422d3c10d287c895573f7108da1b3"
    )
    script = boundary["with"]["script"]
    assert 'readVariable("PAGES_OWNER")' in script
    assert '"GLOBAL_ALPHA_EXPEDITED_OBSERVATION_ENABLED"' in script
    assert 'const deployAllowed = expeditedObservation === "false"' in script
    assert 'core.setOutput("deploy_allowed", String(deployAllowed))' in script

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
