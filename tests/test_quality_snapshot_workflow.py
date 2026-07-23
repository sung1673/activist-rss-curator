from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "quality-snapshot.yml"


def test_quality_snapshot_runs_before_evidence_and_never_enables_outbound() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    parsed = yaml.safe_load(text)
    assert isinstance(parsed, dict)
    triggers = parsed.get(True) or parsed.get("on")
    assert triggers["schedule"] == [{"cron": "25 15 * * *"}]
    assert "python -m curator.quality_snapshot" in text
    assert "BSIDE_OPS_TOKEN: ${{ secrets.BSIDE_OPS_TOKEN }}" in text
    assert '[[ "${ENABLE_TELEGRAM_DELIVERY,,}" != "true" ]]' in text
    assert '[[ "${ENABLE_GOVERNANCE_DELIVERY,,}" != "true" ]]' in text
    assert "retention-days: 90" in text
    assert "send" not in "\n".join(
        line.casefold() for line in text.splitlines() if line.lstrip().startswith("- name:")
    )


def test_quality_snapshot_is_fail_closed_to_shadow_or_live() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")
    assert "vars.GOVERNANCE_PIPELINE_MODE == 'shadow'" in text
    assert "vars.GOVERNANCE_PIPELINE_MODE == 'live'" in text
    assert "cancel-in-progress: false" in text
    assert "governance-runtime" in text
