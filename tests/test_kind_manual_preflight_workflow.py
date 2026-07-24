from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from curator.operation_mode import resolve_operation_mode


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_PATH = ROOT / ".github" / "workflows" / "kind-adapter-preflight.yml"
WORKFLOW_TEXT = WORKFLOW_PATH.read_text(encoding="utf-8")
WORKFLOW = yaml.load(WORKFLOW_TEXT, Loader=yaml.BaseLoader)


def _step(name: str) -> dict[str, object]:
    steps = WORKFLOW["jobs"]["preflight"]["steps"]
    return next(step for step in steps if step.get("name") == name)


def test_kind_preflight_is_manual_only_and_default_branch_exact_sha() -> None:
    assert set(WORKFLOW["on"]) == {"workflow_dispatch"}
    assert WORKFLOW["on"]["workflow_dispatch"] == {}
    assert "schedule" not in WORKFLOW["on"]

    job = WORKFLOW["jobs"]["preflight"]
    assert job["if"] == "github.ref_name == github.event.repository.default_branch"
    assert job["environment"]["name"] == "governance-runtime"

    checkout = _step("Checkout exact default-branch revision")
    assert checkout["with"]["ref"] == "${{ github.sha }}"
    assert checkout["with"]["persist-credentials"] == "false"


def test_kind_preflight_uses_only_centrally_pinned_actions() -> None:
    action_refs = [
        step["uses"]
        for step in WORKFLOW["jobs"]["preflight"]["steps"]
        if "uses" in step
    ]
    assert action_refs == [
        "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
        "actions/setup-python@5fda3b95a4ea91299a34e894583c3862153e4b97",
    ]
    assert all(re.fullmatch(r"[^@]+@[0-9a-f]{40}", ref) for ref in action_refs)


def test_kind_preflight_forces_web_only_mode_for_every_rollout_state() -> None:
    job = WORKFLOW["jobs"]["preflight"]
    assert job["env"] == {
        "ENABLE_TELEGRAM_DELIVERY": "false",
        "ENABLE_GOVERNANCE_DELIVERY": "false",
        "CURATOR_DISABLE_TELEGRAM_SEND": "1",
        "CURATOR_DELIVERY_MODE": "disabled",
    }
    rollout = _step("Validate rollout mode with outbound forced off")
    assert rollout["env"]["GOVERNANCE_PIPELINE_MODE"] == (
        "${{ vars.GOVERNANCE_PIPELINE_MODE }}"
    )
    assert rollout["run"] == "python -m curator.operation_mode"
    assert "vars.GOVERNANCE_PIPELINE_MODE" not in str(job.get("if", ""))


@pytest.mark.parametrize("pipeline_mode", ("off", "dart_canary", "shadow", "live"))
def test_kind_preflight_allows_every_valid_pipeline_mode_without_outbound(
    pipeline_mode: str,
) -> None:
    mode = resolve_operation_mode(
        {
            "GOVERNANCE_PIPELINE_MODE": pipeline_mode,
            "ENABLE_TELEGRAM_DELIVERY": "false",
            "ENABLE_GOVERNANCE_DELIVERY": "false",
        }
    )
    assert mode.governance_pipeline_mode == pipeline_mode
    assert mode.telegram_delivery_enabled is False
    assert mode.distribution_mode == "web_only"


def test_kind_preflight_checks_rights_before_its_single_adapter_request() -> None:
    steps = WORKFLOW["jobs"]["preflight"]["steps"]
    names = [step["name"] for step in steps]
    require_index = names.index("Require KIND preflight configuration")
    rights_index = names.index("Verify KIND SourceRight before adapter network access")
    adapter_index = names.index("Validate KIND adapter contract exactly once")
    assert require_index < rights_index < adapter_index

    required = steps[require_index]
    assert required["env"] == {
        "KIND_DISCLOSURE_ENDPOINT": "${{ vars.KIND_DISCLOSURE_ENDPOINT }}",
        "KIND_API_KEY": "${{ secrets.KIND_API_KEY }}",
        "BSIDE_API_BASE_URL": (
            "${{ secrets.BSIDE_API_BASE_URL || vars.GOVERNANCE_API_BASE_URL }}"
        ),
        "BSIDE_OPS_TOKEN": "${{ secrets.BSIDE_OPS_TOKEN }}",
    }
    for name in (
        "KIND_DISCLOSURE_ENDPOINT",
        "KIND_API_KEY",
        "BSIDE_API_BASE_URL",
        "BSIDE_OPS_TOKEN",
    ):
        assert name in required["run"]

    rights = steps[rights_index]
    assert "OfficialSourceRightClient().check_kind_ingest()" in rights["run"]
    assert "KIND_DISCLOSURE_ENDPOINT" not in rights.get("env", {})
    assert "KIND_API_KEY" not in rights.get("env", {})
    assert set(rights["env"]) == {"BSIDE_API_BASE_URL", "BSIDE_OPS_TOKEN"}

    adapter = steps[adapter_index]
    assert adapter["run"] == "python .github/scripts/validate-kind-adapter.py"
    assert set(adapter["env"]) == {"KIND_DISCLOSURE_ENDPOINT", "KIND_API_KEY"}
    assert WORKFLOW_TEXT.count("validate-kind-adapter.py") == 1
    assert WORKFLOW_TEXT.count("OfficialSourceRightClient().check_kind_ingest()") == 1
    assert not any(step.get("continue-on-error") == "true" for step in steps)


def test_kind_preflight_never_interpolates_inputs_or_secrets_in_shell() -> None:
    assert "inputs:" not in WORKFLOW_TEXT
    assert "${{ inputs." not in WORKFLOW_TEXT
    for step in WORKFLOW["jobs"]["preflight"]["steps"]:
        script = step.get("run")
        if not isinstance(script, str):
            continue
        assert "${{ secrets." not in script
        assert "${{ vars." not in script
