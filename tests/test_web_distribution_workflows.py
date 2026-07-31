from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"


def workflow(name: str) -> tuple[str, dict[str, object]]:
    text = (WORKFLOWS / name).read_text(encoding="utf-8")
    payload = yaml.load(text, Loader=yaml.BaseLoader)
    assert isinstance(payload, dict)
    return text, payload


def named_step(steps: list[dict[str, object]], name: str) -> dict[str, object]:
    return next(step for step in steps if step.get("name") == name)


def assert_actual_deployment_recorder(
    *,
    steps: list[dict[str, object]],
    capture_name: str,
    deployment_name: str,
    recorder_name: str,
    operation: str,
) -> dict[str, object]:
    capture = named_step(steps, capture_name)
    deployment = named_step(steps, deployment_name)
    recorder = named_step(steps, recorder_name)
    assert steps.index(capture) < steps.index(deployment) < steps.index(recorder)
    assert capture["id"] == "pages_distribution_start"
    assert "date -u" in str(capture["run"])

    condition = str(recorder["if"])
    assert "always()" in condition
    assert "!cancelled()" in condition
    assert "steps.pages_distribution_start.outcome == 'success'" in condition
    assert "steps.deployment.outcome == 'success'" in condition
    assert "steps.deployment.outcome == 'failure'" in condition

    environment = recorder["env"]
    assert isinstance(environment, dict)
    assert environment["BSIDE_API_BASE_URL"] == (
        "${{ secrets.BSIDE_API_BASE_URL || vars.GOVERNANCE_API_BASE_URL }}"
    )
    assert environment["BSIDE_OPS_TOKEN"] == "${{ secrets.BSIDE_OPS_TOKEN }}"
    assert environment["DISTRIBUTION_OBSERVED_AT"] == (
        "${{ steps.pages_distribution_start.outputs.observed_at }}"
    )

    command = str(recorder["run"])
    assert "python .github/scripts/record-web-distribution.py" in command
    assert "--target pages" in command
    assert f"--operation {operation}" in command
    assert '--succeeded "$succeeded"' in command
    assert '--observed-at "$DISTRIBUTION_OBSERVED_AT"' in command
    return recorder


def test_legacy_shadow_preview_records_each_actual_pages_outcome() -> None:
    _text, payload = workflow("build-feed.yml")
    job = payload["jobs"]["build-feed"]  # type: ignore[index]
    steps = job["steps"]  # type: ignore[index]
    assert isinstance(steps, list)

    capture = named_step(steps, "Capture shadow preview Pages distribution start")
    capture_condition = str(capture["if"])
    assert "steps.run_mode.outputs.deploy_pages == 'true'" in capture_condition
    assert "steps.run_mode.outputs.governance_preview == 'true'" in capture_condition

    recorder = assert_actual_deployment_recorder(
        steps=steps,
        capture_name="Capture shadow preview Pages distribution start",
        deployment_name="Deploy to GitHub Pages",
        recorder_name="Record final shadow preview Pages distribution outcome",
        operation="legacy-shadow-preview",
    )
    recorder_condition = str(recorder["if"])
    assert "steps.run_mode.outputs.deploy_pages == 'true'" in recorder_condition
    assert "steps.run_mode.outputs.governance_preview == 'true'" in recorder_condition
    assert steps.index(named_step(steps, "Verify GitHub Pages deployment")) < steps.index(
        recorder
    )
    assert recorder["env"]["VERIFICATION_OUTCOME"] == (  # type: ignore[index]
        "${{ steps.pages_deployment_result.outcome }}"
    )


def test_daily_records_only_an_actual_final_pages_attempt() -> None:
    _text, payload = workflow("daily.yml")
    job = payload["jobs"]["generate"]  # type: ignore[index]
    steps = job["steps"]  # type: ignore[index]
    assert isinstance(steps, list)

    capture = named_step(steps, "Capture Pages distribution start")
    assert capture["if"] == "steps.pages_mode.outputs.deploy_pages == 'true'"
    recorder = assert_actual_deployment_recorder(
        steps=steps,
        capture_name="Capture Pages distribution start",
        deployment_name="Deploy to GitHub Pages",
        recorder_name="Record final Pages distribution outcome",
        operation="daily-governance",
    )
    condition = str(recorder["if"])
    assert "steps.pages_mode.outputs.deploy_pages == 'true'" in condition
    assert steps.index(named_step(steps, "Verify GitHub Pages deployment")) < steps.index(
        recorder
    )
    assert recorder["env"]["VERIFICATION_OUTCOME"] == (  # type: ignore[index]
        "${{ steps.pages_deployment_result.outcome }}"
    )


def test_cutover_records_primary_governance_deploy_before_recovery_decision() -> None:
    text, payload = workflow("governance-cutover.yml")
    deploy_job = payload["jobs"]["deploy_pages"]  # type: ignore[index]
    steps = deploy_job["steps"]  # type: ignore[index]
    assert isinstance(steps, list)
    checkout = named_step(steps, "Checkout observation producer")
    assert checkout["uses"] == "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"

    recorder = assert_actual_deployment_recorder(
        steps=steps,
        capture_name="Capture governance Pages distribution start",
        deployment_name="Deploy validated governance artifact",
        recorder_name="Record governance Pages distribution outcome",
        operation="cutover-governance",
    )
    assert recorder["env"]["DEPLOYMENT_OUTCOME"] == "${{ steps.deployment.outcome }}"  # type: ignore[index]

    recovery_steps = payload["jobs"]["recover_pages"]["steps"]  # type: ignore[index]
    assert isinstance(recovery_steps, list)
    assert not any("record-web-distribution.py" in str(step.get("run", "")) for step in recovery_steps)
    assert "UNIQUE(workflow_run_id, workflow_run_attempt, distribution_target)" in (
        ROOT / "docs" / "web-distribution-observations.md"
    ).read_text(encoding="utf-8")
    assert "--operation cutover-legacy-recovery" not in text


def test_rollback_records_the_pinned_legacy_build_outcome() -> None:
    _text, payload = workflow("governance-rollback.yml")
    deploy_job = payload["jobs"]["deploy_legacy"]  # type: ignore[index]
    steps = deploy_job["steps"]  # type: ignore[index]
    assert isinstance(steps, list)
    assert named_step(steps, "Checkout observation producer")["uses"] == "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"

    recorder = assert_actual_deployment_recorder(
        steps=steps,
        capture_name="Capture legacy Pages distribution start",
        deployment_name="Deploy pinned legacy artifact",
        recorder_name="Record legacy Pages distribution outcome",
        operation="rollback-legacy",
    )
    assert recorder["env"]["DISTRIBUTION_BUILD_SHA"] == (  # type: ignore[index]
        "${{ needs.close.outputs.legacy_pin_code_revision }}"
    )
    assert '--build-sha "$DISTRIBUTION_BUILD_SHA"' in str(recorder["run"])


def test_no_api_observation_is_fabricated_without_an_api_deployment_workflow() -> None:
    workflow_text = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(WORKFLOWS.glob("*.yml"))
    )
    assert "--target api" not in workflow_text


def test_distribution_workflows_keep_outbound_delivery_disabled() -> None:
    for name in (
        "build-feed.yml",
        "daily.yml",
        "governance-cutover.yml",
        "governance-rollback.yml",
    ):
        text, _payload = workflow(name)
        assert "TELEGRAM_BOT_TOKEN" not in text
        assert "TELEGRAM_CHAT_ID" not in text
        assert "ENABLE_TELEGRAM_DELIVERY" in text
        assert "ENABLE_GOVERNANCE_DELIVERY" in text
