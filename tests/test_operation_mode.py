from pathlib import Path

import pytest

from curator.operation_mode import OperationModeError, resolve_operation_mode, write_github_outputs


def test_current_legacy_variables_resolve_without_enabling_delivery() -> None:
    mode = resolve_operation_mode(
        {
            "ENABLE_PAGES": "true",
            "ENABLE_GOVERNANCE_PAGES": "false",
            "ENABLE_GOVERNANCE_SHADOW": "false",
            "ENABLE_TELEGRAM_DELIVERY": "false",
            "ENABLE_GOVERNANCE_DELIVERY": "false",
        }
    )
    assert mode.pages_owner == "legacy"
    assert mode.governance_pipeline_mode == "off"
    assert mode.legacy_pages_enabled is True
    assert mode.scheduled_governance_enabled is False
    assert mode.telegram_delivery_enabled is False
    assert mode.distribution_mode == "web_only"


def test_new_variables_take_over_when_stale_false_booleans_remain() -> None:
    mode = resolve_operation_mode(
        {
            "PAGES_OWNER": "governance",
            "GOVERNANCE_PIPELINE_MODE": "shadow",
            "ENABLE_PAGES": "false",
            "ENABLE_GOVERNANCE_PAGES": "false",
            "ENABLE_GOVERNANCE_SHADOW": "false",
        }
    )
    assert mode.governance_pages_enabled is True
    assert mode.scheduled_governance_enabled is True
    assert mode.dart_canary_allowed is True


@pytest.mark.parametrize(
    "values",
    [
        {"ENABLE_PAGES": "true", "ENABLE_GOVERNANCE_PAGES": "true"},
        {"PAGES_OWNER": "governance", "ENABLE_PAGES": "true"},
        {"PAGES_OWNER": "legacy", "ENABLE_GOVERNANCE_PAGES": "true"},
        {"GOVERNANCE_PIPELINE_MODE": "off", "ENABLE_GOVERNANCE_SHADOW": "true"},
        {"GOVERNANCE_PIPELINE_MODE": "invalid"},
        {"ENABLE_TELEGRAM_DELIVERY": "true"},
        {"ENABLE_GOVERNANCE_DELIVERY": "true"},
    ],
)
def test_conflicts_fail_closed(values: dict[str, str]) -> None:
    with pytest.raises(OperationModeError):
        resolve_operation_mode(values)


def test_github_outputs_are_lowercase_and_explicit(tmp_path: Path) -> None:
    output = tmp_path / "github-output.txt"
    write_github_outputs(
        output,
        resolve_operation_mode(
            {"PAGES_OWNER": "legacy", "GOVERNANCE_PIPELINE_MODE": "dart_canary"}
        ),
    )
    text = output.read_text(encoding="utf-8")
    assert "pages_owner=legacy\n" in text
    assert "dart_canary_allowed=true\n" in text
    assert "telegram_delivery_enabled=false\n" in text
    assert "distribution_mode=web_only\n" in text
