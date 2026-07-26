from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "source-right-bootstrap.yml"
BOOTSTRAP = ROOT / "curator" / "source_right_bootstrap.py"


def test_source_right_bootstrap_is_manual_and_environment_protected() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    parsed = yaml.safe_load(text)
    trigger_block = text.split("permissions:", 1)[0]

    assert "workflow_dispatch:" in trigger_block
    assert "schedule:" not in trigger_block
    assert "pull_request:" not in trigger_block
    assert "\n  push:" not in trigger_block
    assert parsed["permissions"] == {"contents": "read"}
    job = parsed["jobs"]["bootstrap"]
    assert job["environment"]["name"] == "governance-release"
    assert (
        parsed["concurrency"]["group"]
        == "governance-production-transition-${{ github.repository }}"
    )
    assert parsed["concurrency"]["cancel-in-progress"] is False


def test_workflow_binds_exact_sha_and_closed_operating_mode() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    assert '[[ "$EXPECTED_RELEASE_SHA" == "$GITHUB_SHA" ]]' in text
    assert "^[a-f0-9]{40}$" in text
    assert (
        "BOOTSTRAP_DART_SEC_METADATA_RIGHTS_AT_EXACT_RELEASE_SHA" in text
    )
    assert '[[ "$PAGES_OWNER" == "legacy" ]]' in text
    assert '[[ "$GOVERNANCE_PIPELINE_MODE" == "off" ]]' in text
    assert '[[ "$GLOBAL_ALPHA_OBSERVATION_ENABLED" == "false" ]]' in text
    assert '[[ "$KIND_CONNECTOR_MODE" == "off" ]]' in text
    assert '[[ "$ENABLE_TELEGRAM_DELIVERY" == "false" ]]' in text
    assert '[[ "$ENABLE_GOVERNANCE_DELIVERY" == "false" ]]' in text
    assert (
        '[[ "$BSIDE_API_BASE_URL" == '
        '"https://alignpe.gabia.io/activist/api.php/api/v1" ]]'
    ) in text


def test_admin_token_is_scoped_to_the_mutating_step_and_never_echoed() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    parsed = yaml.safe_load(text)
    steps = parsed["jobs"]["bootstrap"]["steps"]

    token_steps = [
        step
        for step in steps
        if "BSIDE_ADMIN_TOKEN" in (step.get("env") or {})
    ]
    assert len(token_steps) == 1
    assert token_steps[0]["name"] == (
        "Register and verify approved metadata-only rights"
    )
    assert text.count("${{ secrets.BSIDE_ADMIN_TOKEN }}") == 1
    assert text.count("${{ secrets.BSIDE_API_BASE_URL }}") == 1
    assert "vars.GOVERNANCE_API_BASE_URL" not in text
    assert "secrets.BSIDE_API_BASE_URL ||" not in text
    assert "validate-api-base-urls.py" not in text
    assert "GOVERNANCE_API_BASE_URL" not in BOOTSTRAP.read_text(
        encoding="utf-8"
    )
    assert "BSIDE_OPS_TOKEN" not in text
    assert "BSIDE_RELEASE_AUTHORIZER_TOKEN" not in text
    assert "set -x" not in text
    assert "echo \"$BSIDE_ADMIN_TOKEN\"" not in text


def test_workflow_exposes_only_explicit_optional_selected_markets() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    assert "include_ca:" in text
    assert "include_au:" in text
    assert "CA_OFFICIAL_LINKS_JSON" in text
    assert "AU_OFFICIAL_LINKS_JSON" in text
    assert "--include-ca" in text
    assert "--include-au" in text
    assert "EDINET_API_KEY" not in text
    assert "COMPANIES_HOUSE_API_KEY" not in text
    assert "--include-jp" not in text
    assert "--include-gb" not in text
