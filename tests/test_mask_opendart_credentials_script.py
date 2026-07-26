from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / ".github" / "scripts" / "mask-opendart-credentials.py"


def _run(
    tmp_path: Path,
    *,
    pool: str = "",
    legacy: str = "",
) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["OPENDART_API_KEYS"] = pool
    environment["DART_API_KEY"] = legacy
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--github-output",
            str(tmp_path / "github-output.txt"),
        ],
        cwd=ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=20,
    )


def test_masks_each_pool_key_and_emits_only_nonsecret_metadata(
    tmp_path: Path,
) -> None:
    first = "a" * 40
    second = "b" * 40
    result = _run(tmp_path, pool=f"{first}\r\n{second}\n")

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.splitlines() == [
        f"::add-mask::{first}",
        f"::add-mask::{second}",
        "OpenDART credential configuration validated (mode=pool, count=2).",
    ]
    output = (tmp_path / "github-output.txt").read_text(encoding="utf-8")
    assert output == "credential_mode=pool\ncredential_count=2\n"
    assert first not in output
    assert second not in output


def test_accepts_single_legacy_fallback(tmp_path: Path) -> None:
    key = "c" * 40
    result = _run(tmp_path, legacy=key)

    assert result.returncode == 0
    assert f"::add-mask::{key}" in result.stdout
    assert (tmp_path / "github-output.txt").read_text(encoding="utf-8") == (
        "credential_mode=legacy\ncredential_count=1\n"
    )


def test_missing_or_conflicting_configuration_fails_without_rendering_keys(
    tmp_path: Path,
) -> None:
    missing = _run(tmp_path)
    assert missing.returncode == 1
    assert "missing" in missing.stderr

    pool_key = "d" * 40
    legacy_key = "e" * 40
    conflict = _run(
        tmp_path,
        pool=pool_key,
        legacy=legacy_key,
    )
    assert conflict.returncode == 1
    assert "invalid" in conflict.stderr
    assert pool_key not in conflict.stdout + conflict.stderr
    assert legacy_key not in conflict.stdout + conflict.stderr
