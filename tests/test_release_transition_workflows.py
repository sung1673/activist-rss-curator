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
    assert "run.data.conclusion !== \"success\"" in text
    assert "run.data.head_branch !== defaultBranch" in text
    assert "source run SHA does not match the cutover SHA" in text
    assert "release-evidence.yml" in text
    assert "daily.yml" in text
    assert '(run.data.path || "") !== `.github/workflows/${workflowFile}`' in text
    assert "ageHours > 72" in text
    assert "artifact.digest" in text
    assert "pages-${pages.run.data.id}-${pages.run.data.run_attempt}" in text
    assert "governance Pages artifact name must be exactly" in text
    downloads = [
        step
        for step in validate["steps"]
        if step.get("uses") == "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
    ]
    assert len(downloads) == 3
    assert all(step["with"]["digest-mismatch"] == "error" for step in downloads)
    gate = next(step for step in validate["steps"] if step["name"] == "Evaluate all six release evidence files")
    for argument in ("--shadow", "--operations", "--performance", "--benchmark", "--usability", "--approval"):
        assert argument in gate["run"]
    assert "--expected-revision ${{ github.sha }}" in gate["run"]
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


def test_cutover_switches_owner_deploys_smokes_activates_and_can_recover() -> None:
    text, payload = workflow("governance-cutover.yml")
    deploy = payload["jobs"]["deploy_pages"]
    assert deploy["needs"] == "validate"
    assert deploy["environment"]["name"] == "github-pages"
    assert any(step.get("uses") == "actions/upload-pages-artifact@fc324d3547104276b827a68afc52ff2a11cc49c9" for step in deploy["steps"])
    assert any(step.get("uses") == "actions/deploy-pages@cd2ce8fcbc39b97be8ca5fce6e763baed58fa128" for step in deploy["steps"])

    activate = payload["jobs"]["activate"]
    assert activate["needs"] == ["validate", "deploy_pages"]
    assert activate["environment"]["name"] == "governance-runtime"
    checkout = next(
        step
        for step in activate["steps"]
        if step["name"] == "Checkout deployed-config verifier"
    )
    assert checkout["uses"] == "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"
    assert checkout["with"]["ref"] == "${{ github.sha }}"
    names = step_names(activate)
    smoke_index = names.index("Smoke deployed root governance feed API and export")
    live_index = names.index("Promote reviewed preview state to live")
    public_smoke_index = names.index("Smoke public live API without preview credentials")
    owner_index = names.index("Record verified governance ownership")
    assert smoke_index < live_index < public_smoke_index < owner_index
    assert "/governance/" in text
    assert "/feed.xml" in text
    assert "/exports/events.json?limit=1" in text
    assert "/exports/events.csv?limit=1" in text
    assert "/feeds/events.atom?limit=1" in text
    assert 'release_state:"live"' in text
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
    for job_name, environment_name in (
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
        "Close governance release state after acquiring Pages deployment lock"
    )
    configure_index = deploy_names.index("Configure Pages")
    assert validate_index < close_index < configure_index


def test_rollback_smokes_closed_boundary_then_requires_postdeploy_owner_change() -> None:
    text, payload = workflow("governance-rollback.yml")
    finalize = payload["jobs"]["finalize"]
    assert finalize["needs"] == ["close", "deploy_legacy"]
    assert "environment" not in finalize
    names = step_names(finalize)
    smoke = names.index("Smoke restored legacy site and closed governance API")
    owner = names.index("Verify protected ownership boundary and record rollback")
    assert smoke < owner
    assert "governance_release_closed" in text
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
    assert "/admin/release-state" in daily_text
    assert '"$state" == "live"' in daily_text
    assert daily["jobs"]["generate"]["permissions"]["actions"] == "read"
    assert legacy["jobs"]["build-feed"]["permissions"] if "permissions" in legacy["jobs"]["build-feed"] else legacy["permissions"]
