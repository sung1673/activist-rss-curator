from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "governance-expedited-handoff.yml"


def _workflow() -> tuple[str, dict[str, object]]:
    text = WORKFLOW.read_text(encoding="utf-8")
    return text, yaml.load(text, Loader=yaml.BaseLoader)


def test_handoff_is_manual_protected_and_has_scoped_read_only_source() -> None:
    text, payload = _workflow()
    assert "schedule:" not in text
    assert "pull_request:" not in text
    assert payload["permissions"] == {}
    inputs = payload["on"]["workflow_dispatch"]["inputs"]
    assert set(inputs) == {"confirmation", "cutover_run_id", "reason"}
    job = payload["jobs"]["source"]
    assert job["environment"]["name"] == "governance-release"
    assert job["permissions"] == {"actions": "read", "contents": "read"}
    assert "github.rest.actions.updateRepoVariable" not in text
    assert "github.rest.actions.getRepoVariable" not in text
    assert "actions: write" not in text


def test_handoff_binds_exact_successful_cutover_and_recovery_artifact() -> None:
    text, payload = _workflow()
    source = payload["jobs"]["source"]["steps"][1]
    assert source["name"] == "Verify exact successful expedited cutover"
    script = source["with"]["script"]
    assert "VERIFY_EXPEDITED_ALPHA_HANDOFF" in script
    assert ".github/workflows/governance-expedited-cutover.yml" in script
    for contract in (
        'run.status !== "completed"',
        'run.conclusion !== "success"',
        "run.head_sha !== process.env.EXPECTED_SHA",
        "run.head_branch !== process.env.DEFAULT_BRANCH",
        "run.head_repository?.full_name",
        'core.setOutput("cutover_run_id"',
        'core.setOutput("cutover_run_attempt"',
    ):
        assert contract in script
    resolver = payload["jobs"]["resolve_recovery"]
    assert resolver["needs"] == "source"
    resolve_step = resolver["steps"][0]
    assert resolve_step["name"] == "Resolve exact cutover-carried recovery artifact"
    resolve_script = resolve_step["with"]["script"]
    assert "global-alpha-expedited-cutover-recovery-${runId}-${runAttempt}" in (
        resolve_script
    )
    assert "github.rest.actions.listWorkflowRunArtifacts" in resolve_script
    assert "recovery.length !== 1" in resolve_script
    assert "item.name === recoveryName && !item.expired" in resolve_script
    assert "sha256:[0-9a-f]{64}" in resolve_script


def test_handoff_requires_final_mode_before_smoke_and_receipt() -> None:
    text, payload = _workflow()
    steps = payload["jobs"]["verify"]["steps"]
    idle = next(
        step for step in steps if step["name"] == "Verify final Pages producers are idle"
    )
    idle_script = idle["with"]["script"]
    for contract in (
        ".github/workflows/build-feed.yml",
        ".github/workflows/daily.yml",
        "for (const status of activeStatuses)",
        "active.data.total_count > 0",
        "per_page: 1",
    ):
        assert contract in idle_script
    mode = next(step for step in steps if step["name"] == "Verify fresh final repository mode")
    for contract in (
        '[[ "$PAGES_OWNER" == "governance" ]]',
        '[[ "$PIPELINE_MODE" == "live" ]]',
        '[[ "${STANDARD_OBSERVATION,,}" == "false" ]]',
        '[[ "${EXPEDITED_OBSERVATION,,}" == "false" ]]',
        '[[ "${TELEGRAM_DELIVERY,,}" == "false" ]]',
        '[[ "${GOVERNANCE_DELIVERY,,}" == "false" ]]',
        '[[ "${KIND_MODE,,}" == "off" ]]',
    ):
        assert contract in mode["run"]
    smoke = next(
        step for step in steps if step["name"] == "Verify live APIs and exact Early Access Pages"
    )["run"]
    assert "smoke-global-v2.py" in smoke
    assert "--release-state live" in smoke
    assert "--expected-sha \"$GITHUB_SHA\"" in smoke
    assert "/admin/release-state" in smoke
    assert "production_alpha_early_access" in smoke
    assert "telegram|internal_score|queue_status|admin[_-]?token" in smoke
    smoke_step = next(
        step
        for step in steps
        if step["name"] == "Verify live APIs and exact Early Access Pages"
    )
    assert "EXPECTED_ROOT_SHA256" not in smoke_step["env"]
    assert "needs.recover_pages" not in str(smoke_step)
    upload = next(
        step
        for step in steps
        if step["name"] == "Upload immutable operator handoff receipt"
    )
    assert upload["with"]["name"] == (
        "global-alpha-expedited-handoff-${{ github.sha }}"
    )
    assert upload["with"]["retention-days"] == "90"


def test_handoff_failure_closes_apis_and_restores_exact_legacy_bytes() -> None:
    text, payload = _workflow()
    assert {
        "source",
        "resolve_recovery",
        "verify",
        "recover_close",
        "recover_pages",
        "recover_verify",
    } == set(payload["jobs"])
    close = payload["jobs"]["recover_close"]
    assert close["needs"] == ["source", "resolve_recovery", "verify"]
    assert "needs.source.result == 'success'" in close["if"]
    assert "needs.resolve_recovery.result != 'success'" in close["if"]
    assert "needs.verify.result != 'success'" in close["if"]
    close_run = close["steps"][0]["run"]
    assert close_run.index("recover-v2.json") < close_run.index("recover-v1.json")
    assert 'release_state:"closed"' in close_run
    assert ".data.release_state == \"closed\"" in close_run
    assert ".release_state == \"closed\"" in close_run

    recover = payload["jobs"]["recover_pages"]
    assert recover["environment"]["name"] == "github-pages"
    assert recover["permissions"] == {
        "actions": "read",
        "contents": "read",
        "pages": "write",
        "id-token": "write",
    }
    assert "bside-pages-deployment-${{ github.repository }}" in (
        recover["concurrency"]["group"]
    )
    reresolve = next(
        step
        for step in recover["steps"]
        if step["name"] == "Re-resolve exact cutover recovery after API close"
    )
    assert "for (let attempt = 0; attempt < 4; attempt += 1)" in (
        reresolve["with"]["script"]
    )
    assert "setTimeout(resolve, 10000)" in reresolve["with"]["script"]
    download = next(
        step
        for step in recover["steps"]
        if step["name"] == "Download exact cutover-carried recovery bundle"
    )
    assert download["with"]["artifact-ids"] == (
        "${{ steps.recovery.outputs.artifact_id }}"
    )
    assert download["with"]["run-id"] == (
        "${{ needs.source.outputs.cutover_run_id }}"
    )
    assert download["with"]["digest-mismatch"] == "error"
    binding = next(
        step
        for step in recover["steps"]
        if step["name"] == "Verify exact handoff recovery bytes"
    )["run"]
    for contract in (
        "cutover-recovery-binding.json",
        "bside-global-alpha-expedited-cutover-recovery",
        "bundle_manifest_sha256",
        "release_report_sha256",
        "legacy_root_sha256",
        "legacy_feed_sha256",
        "python -m curator.expedited_legacy_recovery_bundle verify",
    ):
        assert contract in binding
    upload = next(
        step
        for step in recover["steps"]
        if step["name"] == "Upload pinned legacy handoff recovery"
    )
    assert upload["with"]["path"] == (
        "recovery-evidence/expedited-legacy-recovery-bundle/full-site"
    )
    verify = payload["jobs"]["recover_verify"]
    verify_step = verify["steps"][0]
    assert verify_step["env"]["EXPECTED_ROOT_SHA256"] == (
        "${{ needs.recover_pages.outputs.legacy_root_sha256 }}"
    )
    assert verify_step["env"]["EXPECTED_FEED_SHA256"] == (
        "${{ needs.recover_pages.outputs.legacy_feed_sha256 }}"
    )
    verify_run = verify_step["run"]
    assert "restored-root.html" in verify_run
    assert "restored-feed.xml" in verify_run
    assert "EXPECTED_ROOT_SHA256" in verify_run
    assert "EXPECTED_FEED_SHA256" in verify_run
    assert "operator must restore owner=legacy" in verify_run
    assert "repo variables were restored" not in text
