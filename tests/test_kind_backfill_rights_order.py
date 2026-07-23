from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_kind_backfill_checks_source_right_before_adapter_network_access() -> None:
    workflow = (ROOT / ".github" / "workflows" / "official-backfill.yml").read_text(
        encoding="utf-8"
    )
    rights = workflow.index("Verify KIND SourceRight before adapter network access")
    adapter = workflow.index("Validate KIND adapter contract")
    runner = workflow.index("Run one-day official backfill windows")
    assert rights < adapter < runner
    rights_step = workflow[rights:adapter]
    assert "OfficialSourceRightClient().check_kind_ingest()" in rights_step
    assert "BSIDE_OPS_TOKEN" in rights_step
