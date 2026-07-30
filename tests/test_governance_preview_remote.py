from __future__ import annotations

import os
import subprocess
import sys
import zipfile
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "global-alpha-preview-smoke.yml"
SPEC = ROOT / "tests" / "e2e" / "governance-preview-remote.spec.mjs"
SANITIZER = ROOT / ".github" / "scripts" / "sanitize-preview-artifacts.py"


def test_remote_preview_spec_is_separate_and_contains_no_mock_routing() -> None:
    default_config = (ROOT / "playwright.config.mjs").read_text(encoding="utf-8")
    preview_config = (ROOT / "playwright.preview.config.mjs").read_text(
        encoding="utf-8"
    )
    spec = SPEC.read_text(encoding="utf-8")
    assert 'testIgnore: "**/governance-preview-remote.spec.mjs"' in default_config
    assert 'testMatch: "governance-preview-remote.spec.mjs"' in preview_config
    assert 'outputDir: "test-results/preview-remote"' in preview_config
    assert spec.count("used_mock_routes: false") == 1
    assert "page.route(" not in spec
    assert ".fulfill(" not in spec
    assert "context.addInitScript" in spec
    assert "context.tracing.start" in spec
    assert "context.tracing.stop" in spec
    assert 'sessionStorage.removeItem(key)' in spec
    assert "code_revision" in spec
    assert "x-bside-api-version" in spec
    assert spec.count('response.headers.get("x-response-bytes") || "0"') == 2
    assert "expect(result.xResponseBytes).toBe(result.responseBytes)" in spec
    assert (
        "expect(receipt.response_bytes_header).toBe(receipt.size_bytes)" in spec
    )
    assert 'redirect: "error"' in spec
    assert "const body = await response.arrayBuffer();" in spec
    assert 'xBsideApiVersion: String(response.headers.get("x-bside-api-version")' in spec
    assert "responseBytes: body.byteLength" in spec
    assert 'const pending = waitForV2(page, apiV2, "/health");' not in spec
    assert 'await assertV2Response(response, "/health")' not in spec
    assert "await response.json()" not in spec
    assert "#preview=" not in spec
    assert "preview_token=" not in spec.casefold()
    assert "fullPage: true" not in spec


def test_remote_preview_workflow_is_manual_default_branch_runtime_only() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    payload = yaml.load(workflow, Loader=yaml.BaseLoader)
    assert set(payload["on"]) == {"workflow_dispatch"}
    job = payload["jobs"]["preview-smoke"]
    assert job["if"] == "github.ref_name == github.event.repository.default_branch"
    assert job["environment"] == {"name": "governance-runtime"}
    assert "GOVERNANCE_PREVIEW_TOKEN" not in job["env"]
    secret_steps = {
        step["name"]
        for step in job["steps"]
        if "GOVERNANCE_PREVIEW_TOKEN" in step.get("env", {})
    }
    assert secret_steps == {
        "Verify exact preview API v2 deployment",
        "Run three-viewport remote preview journeys",
        "Remove preview credentials from browser artifacts",
    }
    upload = next(
        step
        for step in job["steps"]
        if step["name"]
        == "Preserve same-revision screenshots, traces, and receipts"
    )
    assert "env" not in upload
    assert "GOVERNANCE_PIPELINE_MODE=shadow" in workflow
    assert "playwright.preview.config.mjs" in workflow
    assert "sanitize-preview-artifacts.py test-results/preview-remote" in workflow
    assert "global-alpha-preview-smoke-${{ github.sha }}" in workflow
    assert "governance-release" not in workflow
    assert "deploy-pages" not in workflow


def test_preview_artifact_sanitizer_redacts_plain_and_zip_members(
    tmp_path: Path,
) -> None:
    token = "preview-token-" + "x" * 32
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    plain = artifacts / "receipt.txt"
    plain.write_text(f"Authorization: Bearer {token}\n", encoding="utf-8")
    trace = artifacts / "trace.zip"
    with zipfile.ZipFile(trace, "w") as archive:
        archive.writestr("trace.network", f'{{"authorization":"Bearer {token}"}}')
        archive.writestr("safe.txt", "safe")

    result = subprocess.run(
        [sys.executable, str(SANITIZER), str(artifacts)],
        cwd=ROOT,
        env={**os.environ, "GOVERNANCE_PREVIEW_TOKEN": token},
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert token not in result.stdout
    assert token not in result.stderr
    assert token not in plain.read_text(encoding="utf-8")
    with zipfile.ZipFile(trace, "r") as archive:
        assert all(token.encode() not in archive.read(name) for name in archive.namelist())
