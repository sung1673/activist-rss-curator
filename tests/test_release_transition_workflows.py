from __future__ import annotations

import re
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"


def workflow(name: str) -> tuple[str, dict[str, object]]:
    text = (WORKFLOWS / name).read_text(encoding="utf-8")
    return text, yaml.load(text, Loader=yaml.BaseLoader)


def step_names(job: dict[str, object]) -> list[str]:
    steps = job["steps"]
    assert isinstance(steps, list)
    return [str(step.get("name")) for step in steps]


def test_transition_gate_requires_and_evaluates_all_six_evidence_files() -> None:
    text, payload = workflow("release-gate.yml")
    for filename in (
        "shadow.jsonl",
        "operations.jsonl",
        "performance.jsonl",
        "benchmark.json",
        "usability.json",
        "release-approval.json",
    ):
        assert filename in text
    assert "--usability evidence/usability.json" in text
    assert "--approval evidence/release-approval.json" in text
    evaluate = payload["jobs"]["evaluate"]
    assert evaluate["permissions"] if "permissions" in evaluate else payload["permissions"]


def test_cutover_uses_same_sha_protected_evidence_and_pages_artifacts() -> None:
    text, payload = workflow("governance-cutover.yml")
    assert "schedule:" not in text
    assert payload["concurrency"] == {
        "group": "governance-production-transition-${{ github.repository }}",
        "cancel-in-progress": "false",
        "queue": "max",
    }
    validate = payload["jobs"]["validate"]
    assert validate["permissions"] == {"actions": "write", "contents": "read"}
    assert validate["environment"]["name"] == "governance-release"
    assert validate["outputs"]["evidence_artifact_digest"] == (
        "${{ steps.resolve.outputs.evidence_artifact_digest }}"
    )
    assert "run.data.conclusion !== \"success\"" in text
    assert "run.data.head_branch !== defaultBranch" in text
    assert "source run SHA does not match the cutover SHA" in text
    assert "global-alpha-release-evidence.yml" in text
    assert (
        "Production Alpha evidence artifact name must be "
        "global-alpha-release-evidence"
    ) in text
    assert "daily.yml" in text
    assert '(run.data.path || "") !== `.github/workflows/${workflowFile}`' in text
    assert "ageHours > 48" in text
    assert "artifact.digest" in text
    inputs = payload["on"]["workflow_dispatch"]["inputs"]
    assert "governance_pages_run_id" not in inputs
    assert "governance_pages_artifact_name" not in inputs
    assert "pages-artifact-identity.json" in text
    assert "Resolve evidence-bound daily Pages artifact" in step_names(validate)
    assert "Only the exact evidence-bound daily Pages artifact may be cut over" in text
    assert "stable(report.pages_artifact) !== stable(binding)" in text
    assert "stable(provenance.pages_artifact) !== stable(binding)" in text
    assert (
        validate["outputs"]["pages_artifact_id"]
        == "${{ steps.pages_binding.outputs.pages_artifact_id }}"
    )
    downloads = [
        step
        for step in validate["steps"]
        if step.get("uses") == "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
    ]
    assert len(downloads) == 3
    assert all(step["with"]["digest-mismatch"] == "error" for step in downloads)
    direct_downloads = {
        step["name"]: step
        for step in downloads
        if step["name"]
        in {
            "Download protected production evidence",
            "Download immutable governance Pages source",
        }
    }
    assert set(direct_downloads) == {
        "Download protected production evidence",
        "Download immutable governance Pages source",
    }
    assert all(
        step["with"]["merge-multiple"] == "true"
        for step in direct_downloads.values()
    )
    gate = next(
        step
        for step in validate["steps"]
        if step["name"] == "Evaluate immutable Production Alpha evidence"
    )
    for argument in (
        "--observations",
        "--pages-artifact-identity",
        "--connector-idempotency",
        "--human-review",
        "--content-integrity",
        "--experience",
        "--approval",
    ):
        assert argument in gate["run"]
    assert "python -m curator.global_alpha_release_gate evaluate" in gate["run"]
    assert '--expected-revision "$GITHUB_SHA"' in gate["run"]
    assert "production-alpha-release-report.json" in gate["run"]
    assert gate["env"]["EVIDENCE_RUN_CREATED_AT"] == (
        "${{ steps.resolve.outputs.evidence_created_at }}"
    )
    assert ".evidence_as_of" in gate["run"]
    assert ".observation.ended_at" in gate["run"]
    assert '"$EVIDENCE_RUN_CREATED_AT"' in gate["run"]
    assert '"$evidence_as_of"' in gate["run"]
    assert '"$observation_ended_at"' in gate["run"]
    assert '"$evidence_age" -le 3600' in gate["run"]
    assert "cmp --silent" in gate["run"]
    assert "ga_certification_claimed" in gate["run"]
    assert "ENABLE_TELEGRAM_DELIVERY" in text
    assert "ENABLE_GOVERNANCE_DELIVERY" in text
    assert "${value,,}" in text
    assert "Cancel stale Pages producer runs before protected deployment" in text
    assert "cancelWorkflowRun" in text
    assert "legacy-feed-compatibility.json" in text
    assert "cmp --silent candidate-pages/index.html candidate-pages/governance/index.html" in text
    compatibility = next(
        step
        for step in validate["steps"]
        if step["name"] == "Validate pinned 90-day legacy feed compatibility window"
    )
    assert "python -m curator.legacy_feed_compat verify" in compatibility["run"]
    assert compatibility["env"] == {
        "LEGACY_RUN_ID": "${{ vars.LEGACY_ROLLBACK_RUN_ID }}",
        "LEGACY_ARTIFACT_ID": "${{ steps.recovery_bundle.outputs.source_artifact_id }}",
        "LEGACY_ARTIFACT_NAME": "${{ vars.LEGACY_ROLLBACK_ARTIFACT_NAME }}",
        "LEGACY_CODE_REVISION": "${{ vars.LEGACY_ROLLBACK_CODE_REVISION }}",
        "LEGACY_ARTIFACT_DIGEST": "${{ vars.LEGACY_ROLLBACK_ARTIFACT_DIGEST }}",
    }
    carry = next(
        step
        for step in validate["steps"]
        if step["name"] == "Preserve verified legacy recovery for cutover and rollback"
    )
    assert carry["with"]["name"] == "legacy-recovery-carry-forward"
    assert carry["with"]["retention-days"] == "90"
    config = next(
        step
        for step in validate["steps"]
        if step["name"] == "Verify immutable governance UI release config"
    )
    assert "python -m curator.governance_site_config" in config["run"]
    assert "--expected-build-sha \"$GITHUB_SHA\"" in config["run"]
    identity = next(
        step
        for step in validate["steps"]
        if step["name"]
        == "Verify evidence-bound full-site and terminal content identity"
    )
    assert "python -m curator.global_alpha_pages_identity verify" in identity["run"]


def test_cutover_switches_owner_deploys_smokes_activates_and_can_recover() -> None:
    text, payload = workflow("governance-cutover.yml")
    preflight = payload["jobs"]["preflight"]
    assert preflight["needs"] == "validate"
    assert preflight["environment"]["name"] == "governance-runtime"
    assert preflight["outputs"] == {
        "v1_state_version": "${{ steps.release_states.outputs.v1_state_version }}",
        "v2_state_version": "${{ steps.release_states.outputs.v2_state_version }}",
    }
    preflight_names = step_names(preflight)
    assert "Require v1 and v2 preview states before Pages deployment" in preflight_names
    assert 'EXPECTED_V2_SCHEMA_VERSION: "12"' in text
    assert "jq -r '.data.release_state' preflight-v2-state.json" in text
    assert 'X-BSIDE-API-Version:[[:space:]]*v2' in text
    preview_source_gate = next(
        step
        for step in preflight["steps"]
        if step["name"] == "Require v1 and v2 preview states before Pages deployment"
    )
    assert preview_source_gate["env"]["GOVERNANCE_PREVIEW_TOKEN"] == (
        "${{ secrets.GOVERNANCE_PREVIEW_TOKEN }}"
    )
    for contract in (
        ".data.all_required_ready == true",
        ".data.required_source_ready",
        "all(.data.required_source_ready[]; . == true)",
        ".public_ready == true",
        ".public_status == \"active\"",
        ".freshness_limit_minutes <= 45",
        "connector:ca:issuer-ir",
        "connector:au:asic-register",
        "connector:jp:edinet",
        "connector:gb:companies-house",
        ".public_status == \"coverage_unavailable\"",
        ".raw_count == 0",
        ".acknowledged_count == 0",
    ):
        assert contract in preview_source_gate["run"]

    deploy = payload["jobs"]["deploy_pages"]
    assert deploy["needs"] == ["validate", "preflight"]
    assert deploy["environment"]["name"] == "github-pages"
    assert any(step.get("uses") == "actions/upload-pages-artifact@fc324d3547104276b827a68afc52ff2a11cc49c9" for step in deploy["steps"])
    assert any(step.get("uses") == "actions/deploy-pages@cd2ce8fcbc39b97be8ca5fce6e763baed58fa128" for step in deploy["steps"])

    activate = payload["jobs"]["activate"]
    assert activate["needs"] == ["validate", "preflight", "deploy_pages"]
    assert activate["environment"]["name"] == "governance-release"
    checkout = next(
        step
        for step in activate["steps"]
        if step["name"] == "Checkout deployed-config verifier"
    )
    assert checkout["uses"] == "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"
    assert checkout["with"]["ref"] == "${{ github.sha }}"
    names = step_names(activate)
    revalidate_index = names.index("Revalidate both protected preview versions")
    smoke_index = names.index("Smoke deployed root and both preview API boundaries")
    atomic_live_index = names.index(
        "Authorize and atomically activate both API versions"
    )
    public_smoke_index = names.index(
        "Smoke public Production Alpha without preview credentials"
    )
    owner_index = names.index("Record verified governance ownership")
    assert (
        revalidate_index
        < smoke_index
        < atomic_live_index
        < public_smoke_index
        < owner_index
    )
    assert "/governance/" in text
    assert "/feed.xml" in text
    assert "/briefs/latest?edition=global" in text
    assert "/live?limit=1" in text
    assert "/sources/status" in text
    assert "/exports/events.json?limit=1" in text
    assert "/exports/events.csv?limit=1" in text
    assert "/feeds/events.atom?limit=1" in text
    assert '"${v1_api%/api/v1}/api/v2"' in text
    public_source_gate = next(
        step
        for step in activate["steps"]
        if step["name"] == "Smoke public Production Alpha without preview credentials"
    )
    for contract in (
        ".data.all_required_ready == true",
        ".data.required_source_ready",
        "all(.data.required_source_ready[]; . == true)",
        ".public_ready == true",
        ".public_status == \"active\"",
        ".freshness_limit_minutes <= 45",
        "connector:ca:issuer-ir",
        "connector:au:asic-register",
        "connector:jp:edinet",
        "connector:gb:companies-house",
        ".public_status == \"coverage_unavailable\"",
        ".raw_count == 0",
        ".acknowledged_count == 0",
    ):
        assert contract in public_source_gate["run"]
    assert "${{ needs.preflight.outputs.v1_state_version }}" in text
    assert "${{ needs.preflight.outputs.v2_state_version }}" in text
    assert 'release_state:"live"' not in text
    atomic_step = activate["steps"][atomic_live_index]
    assert atomic_step["env"]["BSIDE_RELEASE_AUTHORIZER_TOKEN"] == (
        "${{ secrets.BSIDE_RELEASE_AUTHORIZER_TOKEN }}"
    )
    assert atomic_step["env"]["BSIDE_ADMIN_TOKEN"] == "${{ secrets.BSIDE_ADMIN_TOKEN }}"
    assert atomic_step["env"]["EVIDENCE_ARTIFACT_DIGEST"] == (
        "${{ needs.validate.outputs.evidence_artifact_digest }}"
    )
    assert "/admin/release-authorizations" in atomic_step["run"]
    assert "/admin/cutover" in atomic_step["run"]
    assert "openssl rand -hex 32" in atomic_step["run"]
    assert "::add-mask::$release_nonce" in atomic_step["run"]
    assert "expected_v1_state_version" in atomic_step["run"]
    assert "expected_v2_state_version" in atomic_step["run"]
    assert "set PAGES_OWNER=governance immediately before dispatch" in text
    assert "GOVERNANCE_PIPELINE_MODE must remain shadow until live activation" in text
    assert (
        "echo 'gh variable set GOVERNANCE_PIPELINE_MODE --body live --repo "
        "sung1673/activist-rss-curator'"
    ) in text
    assert (
        "echo 'test \"$(gh variable get GOVERNANCE_PIPELINE_MODE --repo "
        "sung1673/activist-rss-curator)\" = \"live\"'"
    ) in text
    assert (
        "echo 'test \"$(gh variable get PAGES_OWNER --repo "
        "sung1673/activist-rss-curator)\" = \"governance\"'"
    ) in text
    assert deploy["permissions"]["actions"] == "read"
    assert activate["permissions"] == {"contents": "read"}
    assert 'release_state:"closed"' in text
    assert "automatic legacy recovery started" in text
    assert "recover_close" in payload["jobs"]
    assert "recover_pages" in payload["jobs"]
    assert "recover_owner" in payload["jobs"]
    assert "needs.recover_close.result == 'success'" in text
    assert "Restore pinned legacy Pages artifact" in text
    assert "Automatic cutover recovery complete" in text
    assert (
        "echo 'gh variable set PAGES_OWNER --body legacy --repo "
        "sung1673/activist-rss-curator'"
    ) in text
    assert "legacy and governance scheduled deployers both remain fail-closed" in text
    recover_close = payload["jobs"]["recover_close"]
    assert recover_close["permissions"]["actions"] == "write"
    assert "Cancel stale Pages producer runs before automatic recovery" in step_names(recover_close)
    assert "Close v2 then v1 before automatic legacy recovery" in step_names(
        recover_close
    )
    close_run = next(
        step["run"]
        for step in recover_close["steps"]
        if step["name"] == "Close v2 then v1 before automatic legacy recovery"
    )
    assert close_run.index("recovery-v2-before.json") < close_run.index(
        "recovery-v1-before.json"
    )
    assert "not an atomic DB transition" in text
    for job_name, environment_name in (
        ("preflight", "governance-runtime"),
        ("recover_close", "governance-runtime"),
        ("recover_pages", "github-pages"),
        ("recover_owner", "governance-runtime"),
    ):
        recovery_job = payload["jobs"][job_name]
        needs = recovery_job["needs"]
        dependency_names = [needs] if isinstance(needs, str) else needs
        assert "validate" in dependency_names
        assert recovery_job["environment"]["name"] == environment_name


def test_rollback_closes_inside_lock_before_deploying_digest_pinned_legacy_artifact() -> None:
    text, payload = workflow("governance-rollback.yml")
    assert "schedule:" not in text
    assert payload["concurrency"] == {
        "group": "governance-production-transition-${{ github.repository }}",
        "cancel-in-progress": "false",
        "queue": "max",
    }
    close = payload["jobs"]["close"]
    assert close["environment"]["name"] == "governance-release"
    assert close["permissions"] == {"actions": "write", "contents": "read"}
    assert "Cancel stale Pages producer runs before rollback deployment" in text
    assert "LEGACY_ROLLBACK_RUN_ID" in text
    assert "LEGACY_ROLLBACK_ARTIFACT_NAME" in text
    assert "LEGACY_ROLLBACK_CODE_REVISION" in text
    assert "LEGACY_ROLLBACK_ARTIFACT_DIGEST" in text
    assert "build-feed.yml" in text
    resolver = (ROOT / ".github" / "scripts" / "resolve-legacy-recovery.cjs").read_text(
        encoding="utf-8"
    )
    assert "pinned legacy artifact digest has changed" in resolver
    close_names = step_names(close)
    assert "Prepare or verify rollback recovery bundle before deployment lock" in close_names
    assert "Close governance release state before Pages rollback" not in close_names
    assert 'release_state:"closed"' in text

    deploy = payload["jobs"]["deploy_legacy"]
    assert deploy["needs"] == "close"
    assert deploy["environment"]["name"] == "github-pages"
    download = next(
        step for step in deploy["steps"] if step.get("uses") == "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
    )
    assert download["with"]["digest-mismatch"] == "error"
    assert download["with"]["name"].startswith("legacy-recovery-rollback-")
    upload = next(
        step for step in deploy["steps"] if step.get("uses") == "actions/upload-pages-artifact@fc324d3547104276b827a68afc52ff2a11cc49c9"
    )
    assert upload["with"]["path"] == "legacy-recovery-bundle/full-site"
    assert any(step.get("uses") == "actions/upload-pages-artifact@fc324d3547104276b827a68afc52ff2a11cc49c9" for step in deploy["steps"])
    assert any(step.get("uses") == "actions/deploy-pages@cd2ce8fcbc39b97be8ca5fce6e763baed58fa128" for step in deploy["steps"])
    deploy_names = step_names(deploy)
    validate_index = deploy_names.index("Validate legacy artifact without executing its contents")
    close_index = deploy_names.index(
        "Close v2 then v1 release states after acquiring Pages deployment lock"
    )
    verify_index = deploy_names.index(
        "Verify both release states closed before legacy deployment"
    )
    configure_index = deploy_names.index("Configure Pages")
    assert validate_index < close_index < verify_index < configure_index
    close_run = deploy["steps"][close_index]["run"]
    assert close_run.index("rollback-v2-before.json") < close_run.index(
        "rollback-v1-before.json"
    )
    assert ".data.release_state" in close_run
    assert "${v1_api%/api/v1}/api/v2" in close_run


def test_rollback_smokes_closed_boundary_then_requires_postdeploy_owner_change() -> None:
    text, payload = workflow("governance-rollback.yml")
    finalize = payload["jobs"]["finalize"]
    assert finalize["needs"] == ["close", "deploy_legacy"]
    assert "environment" not in finalize
    names = step_names(finalize)
    smoke = names.index("Smoke restored legacy site and both closed API boundaries")
    owner = names.index("Verify protected ownership boundary and record rollback")
    assert smoke < owner
    assert "governance_release_closed" in text
    assert "global_terminal_release_closed" in text
    assert "rollback-v2-closed-response.json" in text
    assert "rollback-v1-closed-response.json" in text
    assert "?action=reports" in text
    assert finalize["permissions"] == {"contents": "read"}
    assert (
        "echo 'gh variable set GOVERNANCE_PIPELINE_MODE --body shadow --repo "
        "sung1673/activist-rss-curator'"
    ) in text
    assert (
        "echo 'gh variable set PAGES_OWNER --body legacy --repo "
        "sung1673/activist-rss-curator'"
    ) in text
    assert (
        "echo 'test \"$(gh variable get PAGES_OWNER --repo "
        "sung1673/activist-rss-curator)\" = \"legacy\"'"
    ) in text
    assert "PAGES_OWNER must remain governance" in text
    assert "legacy and governance scheduled deployers both remain fail-closed" in text
    assert "database rows, SourceRight withdrawal history, and new governance data were preserved" in text
    assert not re.search(r"\b(?:DROP|TRUNCATE|DELETE\s+FROM)\b", text, re.IGNORECASE)


def test_cutover_and_rollback_share_non_cancelling_transition_lock() -> None:
    _cutover_text, cutover = workflow("governance-cutover.yml")
    _rollback_text, rollback = workflow("governance-rollback.yml")
    assert cutover["concurrency"] == rollback["concurrency"]
    assert cutover["concurrency"]["cancel-in-progress"] == "false"


def test_variable_handoff_is_summary_only_and_never_expands_workflow_authority() -> None:
    cutover_text, cutover = workflow("governance-cutover.yml")
    rollback_text, rollback = workflow("governance-rollback.yml")
    for text in (cutover_text, rollback_text):
        variable_lines = [line.strip() for line in text.splitlines() if "gh variable" in line]
        assert variable_lines
        assert all(line.startswith("echo '") for line in variable_lines)
        assert "actions/variables" not in text
        assert "gh issue create" not in text
        assert not re.search(r"(?:VARIABLES?_WRITE|GH_PAT|GITHUB_PAT|PERSONAL_ACCESS_TOKEN)", text)
    assert cutover["permissions"] == {"contents": "read"}
    assert rollback["permissions"] == {"contents": "read"}

    docs = (ROOT / "docs" / "governance-cutover-rollback.md").read_text(encoding="utf-8")
    for command in (
        "gh auth status",
        "gh variable set GOVERNANCE_PIPELINE_MODE --body live --repo sung1673/activist-rss-curator",
        "gh variable set GOVERNANCE_PIPELINE_MODE --body shadow --repo sung1673/activist-rss-curator",
        "gh variable set PAGES_OWNER --body legacy --repo sung1673/activist-rss-curator",
        'test "$(gh variable get GOVERNANCE_PIPELINE_MODE --repo sung1673/activist-rss-curator)" = "live"',
        'test "$(gh variable get PAGES_OWNER --repo sung1673/activist-rss-curator)" = "legacy"',
    ):
        assert command in docs
    assert "두 예약 배포가 모두 fail-closed" in docs
    assert "별도 GitHub issue를 자동 생성하지 않는다" in docs


def test_transition_deployers_share_one_non_cancelling_fifo_job_lock() -> None:
    expected = {
        "group": "bside-pages-deployment-${{ github.repository }}",
        "cancel-in-progress": "false",
        "queue": "max",
    }
    deploy_jobs = (
        ("governance-cutover.yml", "deploy_pages"),
        ("governance-cutover.yml", "recover_pages"),
        ("governance-rollback.yml", "deploy_legacy"),
    )
    for workflow_name, job_name in deploy_jobs:
        _text, payload = workflow(workflow_name)
        assert payload["jobs"][job_name]["concurrency"] == expected


def test_pages_producers_never_hold_transition_lock_in_shadow_owner_handoff() -> None:
    _legacy_text, legacy = workflow("build-feed.yml")
    _daily_text, daily = workflow("daily.yml")

    legacy_lock = legacy["jobs"]["build-feed"]["concurrency"]
    daily_lock = daily["jobs"]["generate"]["concurrency"]
    for lock in (legacy_lock, daily_lock):
        assert lock["cancel-in-progress"] == "false"
        assert lock["queue"] == "max"
        assert "bside-pages-deployment-{0}" in lock["group"]

    assert "vars.PAGES_OWNER == 'legacy'" in legacy_lock["group"]
    assert "bside-pages-nondeploy-legacy" in legacy_lock["group"]
    assert "vars.PAGES_OWNER == 'governance'" in daily_lock["group"]
    assert "vars.GOVERNANCE_PIPELINE_MODE == 'live'" in daily_lock["group"]
    assert "bside-pages-nondeploy-governance" in daily_lock["group"]


def test_legacy_and_governance_daily_deployers_use_fail_closed_owner_snapshot() -> None:
    legacy_text, legacy = workflow("build-feed.yml")
    daily_text, daily = workflow("daily.yml")
    assert "gh variable get PAGES_OWNER" not in legacy_text
    assert 'PAGES_OWNER_SNAPSHOT" == "legacy"' in legacy_text
    assert "gh variable get PAGES_OWNER" not in daily_text
    assert 'PAGES_OWNER_SNAPSHOT" == "governance"' in daily_text
    assert "/ops/release-state" in daily_text
    assert "BSIDE_OPS_TOKEN" in daily_text
    assert "BSIDE_ADMIN_TOKEN" not in daily_text
    assert '"$state" == "live"' in daily_text
    assert daily["jobs"]["generate"]["permissions"]["actions"] == "read"
    assert legacy["jobs"]["build-feed"]["permissions"] if "permissions" in legacy["jobs"]["build-feed"] else legacy["permissions"]
