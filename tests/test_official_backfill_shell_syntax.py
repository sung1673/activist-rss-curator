from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "official-backfill.yml"


def test_official_backfill_runner_is_valid_bash() -> None:
    if os.name == "nt":
        pytest.skip("Git Bash cannot be used reliably by this Windows test runner")
    bash = shutil.which("bash")
    if bash is None:
        pytest.skip("bash is unavailable on this platform")

    payload = yaml.load(WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    steps = payload["jobs"]["backfill"]["steps"]
    runner = next(
        step
        for step in steps
        if step.get("name") == "Run one-day official backfill windows"
    )
    result = subprocess.run(
        [bash, "-n"],
        input=runner["run"],
        capture_output=True,
        check=False,
        text=True,
    )
    assert result.returncode == 0, result.stderr
