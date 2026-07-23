from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]


def test_untrusted_dispatch_inputs_are_never_interpolated_into_shell_scripts() -> None:
    violations: list[str] = []
    for path in sorted((ROOT / ".github" / "workflows").glob("*.yml")):
        workflow = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for job_name, job in (workflow.get("jobs") or {}).items():
            for step in job.get("steps") or []:
                script = step.get("run")
                if isinstance(script, str) and "${{ inputs." in script:
                    violations.append(
                        f"{path.name}:{job_name}:{step.get('name', '<unnamed>')}"
                    )
    assert violations == [], (
        "workflow_dispatch inputs must be passed through a step env mapping and "
        f"quoted by the shell; direct interpolation found in {violations}"
    )
