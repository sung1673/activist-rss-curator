from __future__ import annotations

import hashlib
import json
import runpy
from pathlib import Path

import pytest
import yaml

from curator.deployment_manifest import (
    CORE_API_FILES,
    MANIFEST_FILENAME,
    DeploymentManifestError,
    write_deployment_manifest,
)


ROOT = Path(__file__).resolve().parents[1]
DEPLOYMENT_ROOT = ROOT / "deploy" / "activist"
REVISION = "a" * 40


def copy_core_files(destination: Path) -> None:
    destination.mkdir()
    for relative_name in CORE_API_FILES:
        target = destination / relative_name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(
            (DEPLOYMENT_ROOT / relative_name).read_bytes()
        )


def test_builder_writes_exact_revision_and_core_file_hashes(tmp_path: Path) -> None:
    deployment = tmp_path / "activist"
    copy_core_files(deployment)

    manifest = write_deployment_manifest(
        deployment,
        code_revision=REVISION,
        output=deployment / MANIFEST_FILENAME,
    )
    persisted = json.loads(
        (deployment / MANIFEST_FILENAME).read_text(encoding="utf-8")
    )

    assert persisted == manifest
    assert persisted["schema_version"] == 1
    assert persisted["code_revision"] == REVISION
    assert set(persisted["files"]) == set(CORE_API_FILES)
    for relative_name in CORE_API_FILES:
        assert persisted["files"][relative_name] == hashlib.sha256(
            (deployment / relative_name).read_bytes()
        ).hexdigest()


@pytest.mark.parametrize(
    "revision",
    (
        "a" * 39,
        "A" * 40,
        "g" * 40,
        " " + "a" * 40,
        "a" * 40 + "\n",
    ),
)
def test_builder_rejects_noncanonical_git_revisions(
    tmp_path: Path,
    revision: str,
) -> None:
    deployment = tmp_path / "activist"
    copy_core_files(deployment)

    with pytest.raises(DeploymentManifestError, match="exact lowercase"):
        write_deployment_manifest(deployment, code_revision=revision)


def test_builder_fails_when_a_core_file_is_missing(tmp_path: Path) -> None:
    deployment = tmp_path / "activist"
    copy_core_files(deployment)
    (deployment / "governance_v2_write.php").unlink()

    with pytest.raises(DeploymentManifestError, match="must be a regular file"):
        write_deployment_manifest(deployment, code_revision=REVISION)


def test_builder_rejects_a_symlink_in_a_nested_core_path(tmp_path: Path) -> None:
    deployment = tmp_path / "activist"
    copy_core_files(deployment)
    migration_dir = deployment / "migrations"
    (migration_dir / "011_global_terminal_v2.sql").unlink()
    migration_dir.rmdir()
    outside = tmp_path / "outside-migrations"
    outside.mkdir()
    (outside / "011_global_terminal_v2.sql").write_bytes(
        (DEPLOYMENT_ROOT / "migrations" / "011_global_terminal_v2.sql").read_bytes()
    )
    try:
        migration_dir.symlink_to(outside, target_is_directory=True)
    except OSError as error:
        pytest.skip(f"directory symlinks are unavailable: {error}")

    with pytest.raises(DeploymentManifestError, match="must not contain symlinks"):
        write_deployment_manifest(deployment, code_revision=REVISION)


def test_builder_refuses_to_write_a_manifest_outside_the_deployment_root(
    tmp_path: Path,
) -> None:
    deployment = tmp_path / "activist"
    copy_core_files(deployment)

    with pytest.raises(DeploymentManifestError, match="output must be"):
        write_deployment_manifest(
            deployment,
            code_revision=REVISION,
            output=tmp_path / MANIFEST_FILENAME,
        )


def test_php_health_is_fail_closed_on_manifest_or_hash_mismatch() -> None:
    php = (DEPLOYMENT_ROOT / "governance_v2.php").read_text(encoding="utf-8")

    for relative_name in CORE_API_FILES:
        assert f"'{relative_name}'" in php
    assert "deployment-manifest.json" in php
    assert "function v2_deployment_core_file_path" in php
    assert "'migrations/011_global_terminal_v2.sql'" in php
    assert "hash_file('sha256', $resolved)" in php
    assert "'error' => 'deployment_identity_unavailable'" in php
    assert "'reason' => $identity['error']" in php
    assert "'code_revision' => $identity['code_revision']" in php
    assert "v2_respond(503" in php


def test_openapi_exposes_exact_revision_and_identity_failure() -> None:
    spec = yaml.safe_load(
        (DEPLOYMENT_ROOT / "openapi-v2.yaml").read_text(encoding="utf-8")
    )
    health = spec["paths"]["/health"]["get"]
    schema = spec["components"]["schemas"]["HealthEnvelope"]

    assert set(health["responses"]) == {"200", "503"}
    assert "code_revision" in schema["required"]
    assert schema["properties"]["code_revision"] == {
        "type": "string",
        "pattern": "^[0-9a-f]{40}$",
    }


def test_ci_builds_and_tamper_tests_the_checked_out_revision() -> None:
    workflow = (ROOT / ".github" / "workflows" / "ci.yml").read_text(
        encoding="utf-8"
    )

    assert "Build exact API deployment identity" in workflow
    assert 'test "$(git rev-parse HEAD)" = "$GITHUB_SHA"' in workflow
    assert "python3 -m curator.deployment_manifest" in workflow
    assert '--code-revision "$GITHUB_SHA"' in workflow
    assert "--output deploy/activist/deployment-manifest.json" in workflow
    assert "deployment_manifest_missing" in workflow
    assert "deployment_core_hash_mismatch" in workflow
    assert ".code_revision == $revision" in workflow
    assert "(.files | length) == 6" in workflow
    assert 'sha256sum "$migration_011_source"' in workflow
    assert "SET @bside_migration_011_sha256" in workflow
    assert "one-byte source change" in workflow
    assert "overwrote a conflicting byte checksum" in workflow
    assert 'ln -s "$RUNNER_TEMP/migrations-real" deploy/activist/migrations' in workflow


def test_php_smoke_binds_release_data_to_the_built_manifest(
    tmp_path: Path,
) -> None:
    smoke_path = ROOT / "tests" / "php73_global_v2_smoke.py"
    namespace = runpy.run_path(str(smoke_path))
    revision_loader = namespace["_deployed_code_revision"]
    revision_loader.__globals__["REPOSITORY_ROOT"] = tmp_path
    manifest_path = (
        tmp_path / "deploy" / "activist" / "deployment-manifest.json"
    )
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(
        json.dumps({"schema_version": 1, "code_revision": REVISION}),
        encoding="utf-8",
    )

    assert revision_loader() == REVISION
    smoke = smoke_path.read_text(encoding="utf-8")
    assert "CODE_REVISION = _deployed_code_revision()" in smoke
    assert '"candidate_sha": CODE_REVISION' in smoke


def test_cutover_preflight_requires_the_current_api_revision() -> None:
    workflow = (ROOT / ".github" / "workflows" / "governance-cutover.yml").read_text(
        encoding="utf-8"
    )
    payload = yaml.load(workflow, Loader=yaml.BaseLoader)
    preflight_steps = payload["jobs"]["preflight"]["steps"]
    checkout = next(
        step
        for step in preflight_steps
        if step["name"] == "Checkout exact release candidate"
    )
    assert checkout["with"]["ref"] == "${{ github.sha }}"
    setup_python = next(
        step
        for step in preflight_steps
        if step["name"] == "Set up Python for exact deployment smoke"
    )
    smoke_step = next(
        step
        for step in preflight_steps
        if step["name"]
        == "Require v1 and v2 preview states before Pages deployment"
    )
    assert preflight_steps.index(checkout) < preflight_steps.index(setup_python)
    assert preflight_steps.index(setup_python) < preflight_steps.index(smoke_step)
    health_probe = workflow.index(
        "python .github/scripts/smoke-global-v2.py"
    )
    state_probe = workflow.index(
        '"$v2_api/admin/release-state" > preflight-v2-state.json'
    )

    assert health_probe < state_probe
    assert '--expected-sha "$GITHUB_SHA"' in workflow
    assert "--release-state preview" in workflow
    assert "--preview-token-env GOVERNANCE_PREVIEW_TOKEN" in workflow
