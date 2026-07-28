from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = (
    ROOT
    / ".github"
    / "workflows"
    / "global-alpha-observation-chain-preflight.yml"
)
WORKFLOW_TEXT = WORKFLOW_PATH.read_text(encoding="utf-8")
WORKFLOW = yaml.load(WORKFLOW_TEXT, Loader=yaml.BaseLoader)


def test_preflight_is_a_manual_protected_parent_child_workflow() -> None:
    assert isinstance(WORKFLOW, dict)
    assert set(WORKFLOW["on"]) == {"workflow_dispatch"}
    assert WORKFLOW["permissions"] == {
        "actions": "write",
        "contents": "read",
    }

    reject = WORKFLOW["jobs"]["reject-invalid-phase"]
    parent = WORKFLOW["jobs"]["parent"]
    child = WORKFLOW["jobs"]["child"]
    assert reject["if"] == (
        "inputs.phase != 'parent' && inputs.phase != 'child'"
    )
    assert reject["timeout-minutes"] == "1"
    assert parent["if"] == "inputs.phase == 'parent'"
    assert child["if"] == "inputs.phase == 'child'"
    assert parent["timeout-minutes"] == "4"
    assert child["timeout-minutes"] == "2"
    assert parent["environment"]["name"] == "governance-runtime"
    assert child["environment"]["name"] == "governance-runtime"


def test_parent_dispatches_and_waits_for_one_exact_child_success() -> None:
    step = WORKFLOW["jobs"]["parent"]["steps"][0]
    assert (
        step["uses"]
        == "actions/github-script@"
        "3a2844b7e9c422d3c10d287c895573f7108da1b3"
    )
    script = step["with"]["script"]
    for contract in (
        'crypto.randomBytes(32).toString("hex")',
        "core.setSecret(nonce)",
        "createWorkflowDispatch",
        "listWorkflowRuns",
        "getWorkflowRun",
        "dispatchStartedAt + 110_000",
        'childRun.status !== "completed"',
        'childRun.conclusion !== "success"',
        "childRun.run_attempt !== 1",
        'childRun.event !== "workflow_dispatch"',
        "childRun.path !== requiredPath",
        "childRun.head_branch !== defaultBranch",
        "childRun.head_sha ||",
        "childRun.display_title !== childTitle",
    ):
        assert contract in script
    assert "candidates.length > 1" in script
    assert "await github.rest.actions.createWorkflowDispatch" in script
    assert script.index("createWorkflowDispatch") < script.index(
        "listWorkflowRuns"
    )


def test_child_validates_phase_nonce_sha_attempt_event_ref_and_path() -> None:
    child_run = WORKFLOW["jobs"]["child"]["steps"][0]["run"]
    for contract in (
        'os.environ["INPUT_PHASE"] == "child"',
        'os.environ["EVENT_NAME"] == "workflow_dispatch"',
        'os.environ["RUN_ATTEMPT"] == "1"',
        'os.environ["REF_TYPE"] == "branch"',
        'os.environ["REF_NAME"] == default_branch',
        'os.environ["WORKFLOW_REF"] == expected_workflow_ref',
        're.fullmatch(r"[0-9a-f]{40}", expected_revision)',
        'os.environ["ACTUAL_REVISION"] == expected_revision',
        're.fullmatch(r"[0-9a-f]{64}", nonce)',
        're.fullmatch(r"[0-9a-f]{64}", nonce_digest)',
        "hmac.compare_digest(actual_digest, nonce_digest)",
    ):
        assert contract in child_run


def test_preflight_has_no_operational_mutation_or_persisted_output() -> None:
    run_name = WORKFLOW["run-name"]
    assert "nonce" not in run_name.lower()
    for forbidden in (
        "actions/upload-artifact",
        "actions/download-artifact",
        "BSIDE_ADMIN_TOKEN",
        "BSIDE_OPS_TOKEN",
        "BSIDE_RELEASE_AUTHORIZER_TOKEN",
        "GOVERNANCE_PREVIEW_TOKEN",
        "BSIDE_API_BASE_URL",
        "GOVERNANCE_API_BASE_URL",
        "BSIDE_PUBLIC_WEB_URL",
        "curl ",
        "Invoke-WebRequest",
    ):
        assert forbidden not in WORKFLOW_TEXT
