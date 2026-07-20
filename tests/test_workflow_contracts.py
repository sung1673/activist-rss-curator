from __future__ import annotations

from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"


def workflow_text(name: str) -> str:
    return (WORKFLOWS / name).read_text(encoding="utf-8")


def test_all_workflows_are_valid_yaml() -> None:
    paths = sorted(WORKFLOWS.glob("*.yml"))
    assert paths
    for path in paths:
        payload = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
        assert isinstance(payload, dict), path
        assert "name" in payload, path
        assert "on" in payload, path
        assert "jobs" in payload, path


def test_legacy_shadow_baseline_does_not_commit_generated_files() -> None:
    legacy = workflow_text("build-feed.yml")
    assert "ENABLE_LEGACY_PIPELINE != 'false'" in legacy
    assert "CURATOR_DATA_SOURCE: mysql" in legacy
    assert "CURATOR_DELIVERY_MODE: ${{ vars.ENABLE_GOVERNANCE_DELIVERY == 'true' && 'disabled' || 'legacy-direct' }}" in legacy
    assert "steps.run_mode.outputs.full == 'true' || steps.run_mode.outputs.page == 'true'" in legacy
    assert "deploy_pages=$deploy_pages" in legacy
    assert "ENABLE_PAGES and ENABLE_GOVERNANCE_PAGES are mutually exclusive" in legacy
    for step_name in (
        "Send Telegram smoke test",
        "Resend last Telegram briefing",
        "Resend recent Telegram articles",
    ):
        block = legacy[legacy.index(f"- name: {step_name}") :]
        block = block[: block.index("\n      - name:")]
        assert "ENABLE_GOVERNANCE_DELIVERY != 'true'" in block
    all_workflows = "\n".join(path.read_text(encoding="utf-8") for path in WORKFLOWS.glob("*.yml"))
    assert "commit-generated-changes" not in all_workflows
    assert "git push" not in all_workflows
    assert "contents: write" not in all_workflows


def test_runtime_and_release_artifacts_are_excluded_from_git() -> None:
    ignore = (ROOT / ".gitignore").read_text(encoding="utf-8")
    for generated in (
        "data/state.json",
        "data/archive/*",
        "public/feed.xml",
        "public/index.html",
        "public/feed/",
        ".watchdog-report.md",
        "release-report/",
        "evidence/",
    ):
        assert generated in ignore


def test_official_ingest_has_day_and_night_kst_schedules() -> None:
    workflow = workflow_text("ingest-official.yml")
    assert 'cron: "0,15,30,45 22-23 * * *"' in workflow
    assert 'cron: "0,15,30,45 0-14 * * *"' in workflow
    assert 'cron: "0 15-21 * * *"' in workflow
    assert "CURATOR_INGEST_SCOPE: official" in workflow
    assert "inputs.include_kind" in workflow
    assert "leave off for DART-only smoke/shadow" in workflow
    assert "CURATOR_ENABLE_KIND: ${{ github.event_name == 'schedule' && '1' || inputs.include_kind && '1' || '0' }}" in workflow
    assert "KIND_DISCLOSURE_ENDPOINT is required" in workflow
    assert "validate-kind-adapter.py" in workflow
    assert "ENABLE_GOVERNANCE_SHADOW" in workflow
    assert "CURATOR_DISABLE_TELEGRAM_SEND" in workflow
    assert "CURATOR_DELIVERY_MODE: disabled" in workflow
    assert "curator.publish_outbox" not in workflow


def test_media_resolver_and_publisher_are_independent() -> None:
    media = workflow_text("ingest-media.yml")
    resolver = workflow_text("resolve-links.yml")
    publisher = workflow_text("publish.yml")
    assert 'cron: "7,37 * * * *"' in media
    assert "CURATOR_INGEST_SCOPE: media" in media
    assert "ingest-media-${{ github.ref_name }}" in media
    assert "validate-media-feeds.py" in media
    assert "TELEGRAM_SESSION_STRING" in media
    assert "Either TELEGRAM_SESSION" not in media
    assert "curator.publish_outbox" not in media
    assert "CURATOR_DELIVERY_MODE: disabled" in media
    assert 'cron: "22 * * * *"' in resolver
    assert "curator.resolve_links" in resolver
    assert "claim_link_discoveries" not in resolver  # encapsulated by the resolver CLI
    assert "curator.main" not in resolver
    assert "workflow_run:" in publisher
    assert "ENABLE_GOVERNANCE_DELIVERY" in publisher
    assert "curator.publish_outbox" in publisher
    assert 'DELIVERY_LEASE_SECONDS: "900"' in publisher
    assert "curator.publish_outbox --root . --limit 5" in publisher
    assert "!cancelled()" in publisher
    assert "steps.validate.outcome == 'success'" in publisher
    assert "curator.main" not in publisher
    assert "Daily pages and briefing" in publisher
    outbox_consumers = [
        path.name
        for path in WORKFLOWS.glob("*.yml")
        if "python -m curator.publish_outbox" in path.read_text(encoding="utf-8")
    ]
    assert outbox_consumers == ["publish.yml"]


def test_daily_generation_and_delivery_use_requested_kst_boundaries() -> None:
    workflow = workflow_text("daily.yml")
    assert 'cron: "45 20 * * *"' in workflow
    assert 'cron: "5 21 * * *"' in workflow
    assert 'CURATOR_DAILY_REPORT_WRITE_ONLY: "1"' in workflow
    assert "daily_report_queued=1" in workflow
    assert "CURATOR_DELIVERY_MODE: outbox-enqueue" in workflow
    assert "curator.story_review send" not in workflow
    assert "TELEGRAM_ADMIN_CHAT_ID" not in workflow
    assert "send_telegram_admin_access" not in workflow
    assert "curator.telegram_dashboard send-access" not in workflow
    assert "Build token-gated Telegram admin shell" in workflow
    assert "TELEGRAM_ADMIN_ACCESS_TOKEN: ${{ secrets.TELEGRAM_ADMIN_ACCESS_TOKEN }}" in workflow
    assert "require-env.sh TELEGRAM_ADMIN_ACCESS_TOKEN ACTIVIST_PUBLIC_API_URL" in workflow
    assert "python -m curator.telegram_dashboard write" in workflow
    assert "actions/upload-pages-artifact@v4" in workflow
    assert "actions/deploy-pages@v4" in workflow
    assert "ENABLE_GOVERNANCE_PAGES" in workflow
    assert "vars.ENABLE_PAGES != 'true'" in workflow
    assert "ENABLE_PAGES and ENABLE_GOVERNANCE_PAGES are mutually exclusive" in workflow
    assert "governance-pages-ready-${{ steps.deployment_marker.outputs.kst_date }}" in workflow
    assert "verify-daily-pages-artifact.py" in workflow


def test_workflows_do_not_send_private_admin_messages_or_token_links() -> None:
    legacy = workflow_text("build-feed.yml")
    daily = workflow_text("daily.yml")
    for workflow in (legacy, daily):
        assert "TELEGRAM_ADMIN_CHAT_ID" not in workflow
        assert "send_telegram_admin_access" not in workflow
        assert "curator.telegram_dashboard send-access" not in workflow
        assert "curator.story_review send" not in workflow
        assert "Build token-gated Telegram admin shell" in workflow
        assert "python -m curator.telegram_dashboard write" in workflow


def test_workflow_permissions_are_scoped_to_the_jobs_that_need_them() -> None:
    daily = yaml.load(workflow_text("daily.yml"), Loader=yaml.BaseLoader)
    official = yaml.load(workflow_text("ingest-official.yml"), Loader=yaml.BaseLoader)
    assert daily["permissions"] == {"contents": "read", "models": "read"}
    assert daily["jobs"]["generate"]["permissions"] == {
        "contents": "read",
        "models": "read",
        "pages": "write",
        "id-token": "write",
    }
    assert daily["jobs"]["send"]["permissions"] == {
        "contents": "read",
        "models": "read",
        "actions": "read",
    }
    assert official["permissions"] == {"contents": "read"}


@pytest.mark.parametrize(
    ("workflow_name", "job_name"),
    [
        ("build-feed.yml", "build-feed"),
        ("daily.yml", "generate"),
    ],
)
def test_pages_deployment_retries_one_immutable_artifact_three_times(
    workflow_name: str,
    job_name: str,
) -> None:
    payload = yaml.load(workflow_text(workflow_name), Loader=yaml.BaseLoader)
    job = payload["jobs"][job_name]
    steps = job["steps"]

    uploads = [step for step in steps if step.get("uses") == "actions/upload-pages-artifact@v4"]
    deployments = [step for step in steps if step.get("uses") == "actions/deploy-pages@v4"]
    assert len(uploads) == 1
    assert uploads[0]["with"]["name"] == "github-pages"
    assert len(deployments) == 3
    assert [step["id"] for step in deployments] == [
        "deployment",
        "deployment_retry",
        "deployment_retry_final",
    ]
    for deployment in deployments:
        assert deployment["continue-on-error"] == "true"
        assert deployment["with"]["artifact_name"] == "github-pages"

    waits = {step["name"]: step for step in steps if step["name"].startswith("Wait before")}
    assert waits["Wait before first Pages retry"]["run"] == "sleep 180"
    assert waits["Wait before final Pages retry"]["run"] == "sleep 300"
    assert "steps.deployment.outcome == 'failure'" in deployments[1]["if"]
    assert "steps.deployment.outcome == 'failure'" in deployments[2]["if"]
    assert "steps.deployment_retry.outcome == 'failure'" in deployments[2]["if"]

    verifier = next(step for step in steps if step["name"] == "Verify GitHub Pages deployment")
    assert verifier["id"] == "pages_deployment_result"
    assert "!cancelled()" in verifier["if"]
    assert "DEPLOYMENT_OUTCOME_3" in verifier["env"]
    assert "DEPLOYMENT_URL_3" in verifier["env"]
    assert verifier["env"]["PAGES_ARTIFACT_ID"] == "${{ steps.pages_artifact.outputs.artifact_id }}"
    assert 'echo "page_url=$selected_url" >> "$GITHUB_OUTPUT"' in verifier["run"]
    assert "failed after three attempts" in verifier["run"]
    assert job["environment"]["url"] == "${{ steps.pages_deployment_result.outputs.page_url }}"

    if workflow_name == "daily.yml":
        assert int(job["timeout-minutes"]) >= 60
        marker_index = next(
            index for index, step in enumerate(steps) if step["name"] == "Create daily deployment marker"
        )
        assert marker_index > steps.index(verifier)
    else:
        assert int(job["timeout-minutes"]) == 75
        failure_artifact = next(
            step for step in steps if step["name"] == "Preserve failed Pages artifact"
        )
        assert "steps.pages_deployment_result.outcome == 'failure'" in failure_artifact["if"]
        assert failure_artifact["with"]["retention-days"] == "7"


def test_pages_deployment_is_default_branch_only() -> None:
    legacy = workflow_text("build-feed.yml")
    daily = workflow_text("daily.yml")
    assert 'REF_NAME: ${{ github.ref_name }}' in legacy
    assert 'DEFAULT_BRANCH: ${{ github.event.repository.default_branch }}' in legacy
    assert '"$REF_NAME" == "$DEFAULT_BRANCH"' in legacy
    assert "Determine Pages deployment eligibility" in daily
    assert '"$REF_NAME" == "$DEFAULT_BRANCH"' in daily
    assert "github.ref_name == github.event.repository.default_branch" in daily


def test_pages_incident_listener_is_isolated_and_minimally_privileged() -> None:
    workflow = workflow_text("pages-deployment-incident.yml")
    payload = yaml.load(workflow, Loader=yaml.BaseLoader)
    listener = payload["jobs"]["reconcile"]
    assert payload["permissions"] == {}
    assert listener["permissions"] == {"actions": "read", "issues": "write"}
    assert "github.event.workflow_run.head_repository.full_name == github.repository" in listener["if"]
    assert payload["on"]["workflow_run"]["workflows"] == [
        "Build curated RSS feed",
        "Daily pages and briefing",
    ]
    assert payload["on"]["workflow_run"]["branches"] == ["main"]
    assert "/attempts/{attempt_number}/jobs" in workflow
    assert 'verificationName = "Verify GitHub Pages deployment"' in workflow
    assert "No completed Pages verification step; incident state is unchanged." in workflow
    assert "[ops/incident] GitHub Pages deployment unhealthy" in workflow
    assert "state_reason: \"completed\"" in workflow
    assert "state: \"open\"" in workflow
    assert 'incident.state === "closed"' in workflow
    assert "createComment" in workflow
    assert "actions/checkout" not in workflow
    assert "secrets." not in workflow
    assert "pull_request_target" not in workflow


def test_official_ingest_serializes_overlapping_scheduled_runs() -> None:
    payload = yaml.load(workflow_text("ingest-official.yml"), Loader=yaml.BaseLoader)
    assert payload["concurrency"] == {
        "group": "ingest-official-${{ github.ref_name }}",
        "cancel-in-progress": "false",
    }


def test_ci_audits_python_and_browser_dependencies() -> None:
    workflow = workflow_text("ci.yml")
    assert "pip-audit --requirement requirements.txt" in workflow
    assert "npm audit --audit-level=high" in workflow


def test_watchdog_contract_and_issue_permission() -> None:
    workflow = workflow_text("watchdog.yml")
    script = (ROOT / ".github" / "scripts" / "watchdog.py").read_text(encoding="utf-8")
    assert "issues: write" in workflow
    assert "BSIDE_API_BASE_URL" in workflow
    assert "BSIDE_OPS_TOKEN" in workflow
    assert '"90"' in workflow
    assert 'cron: "1,6,11,16,21,26,31,36,41,46,51,56 * * * *"' in workflow
    assert 'WATCHDOG_MAX_OUTBOX_AGE_MINUTES: "5"' in workflow
    assert "/api/v1/ops/health" in script
    assert "dead_letter_count" in script


def test_release_gate_uses_cross_run_evidence_and_checked_out_revision() -> None:
    workflow = workflow_text("release-gate.yml")
    assert "workflow_dispatch:" in workflow
    assert "actions: read" in workflow
    assert "actions/download-artifact@v4" in workflow
    assert "Validate production evidence run provenance" in workflow
    assert 'MAX_RUN_AGE_HOURS: "72"' in workflow
    assert "Evidence workflow run revision does not match" in workflow
    assert "run-id: ${{ inputs.evidence_run_id }}" in workflow
    assert "python -m curator.release_gate" in workflow
    assert "--expected-revision ${{ github.sha }}" in workflow
    assert "--evidence-as-of ${{ steps.evidence_run.outputs.created_at }}" in workflow
    assert "actions/upload-artifact@v4" in workflow
    assert "Governance release transition gate did not pass" in workflow
