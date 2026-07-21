from __future__ import annotations

import re
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
    assert "ENABLE_LEGACY_PIPELINE == 'true'" in legacy
    assert "CURATOR_DATA_SOURCE: mysql" in legacy
    assert "vars.ENABLE_TELEGRAM_DELIVERY == 'true'" in legacy
    assert "CURATOR_DELIVERY_MODE: disabled" in legacy
    assert 'CURATOR_DISABLE_TELEGRAM_SEND: "1"' in legacy
    assert "allow_pages_deploy:" in legacy
    assert "allow_telegram_delivery:" in legacy
    assert "default: false" in legacy
    assert '"$ALLOW_PAGES_DEPLOY" == "true"' in legacy
    assert (
        "steps.run_mode.outputs.full == 'true' || steps.run_mode.outputs.page == 'true'"
        in legacy
    )
    assert "deploy_pages=$deploy_pages" in legacy
    assert "ENABLE_PAGES and ENABLE_GOVERNANCE_PAGES are mutually exclusive" in legacy
    assert "Prepare allowlisted legacy Pages artifact" in legacy
    assert "python .github/scripts/prepare-legacy-pages.py" in legacy
    assert "python .github/scripts/restore-legacy-pages-archive.py" in legacy
    assert '".github/scripts/prepare-legacy-pages.py"' in legacy
    assert '".github/scripts/restore-legacy-pages-archive.py"' in legacy
    assert "scripts/(prepare-legacy-pages|restore-legacy-pages-archive)\\.py" in legacy
    assert '"public/CNAME"' in legacy
    assert "path: ${{ steps.legacy_pages_artifact.outputs.path }}" in legacy
    assert "path: public" not in legacy
    for step_name in (
        "Send Telegram smoke test",
        "Resend last Telegram briefing",
        "Resend recent Telegram articles",
    ):
        block = legacy[legacy.index(f"- name: {step_name}") :]
        block = block[: block.index("\n      - name:")]
        assert "ENABLE_TELEGRAM_DELIVERY == 'true'" in block
        assert "ENABLE_GOVERNANCE_DELIVERY != 'true'" in block
        assert "inputs.allow_telegram_delivery" in block
    all_workflows = "\n".join(
        path.read_text(encoding="utf-8") for path in WORKFLOWS.glob("*.yml")
    )
    assert "commit-generated-changes" not in all_workflows
    assert "git push" not in all_workflows
    assert "contents: write" not in all_workflows

    build_block = legacy[
        legacy.index("- name: Build feed") : legacy.index(
            "- name: Publish curator run metrics"
        )
    ]
    assert "TELEGRAM_BOT_TOKEN" not in build_block
    assert "TELEGRAM_CHAT_ID" not in build_block


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
    assert (
        "CURATOR_ENABLE_KIND: ${{ github.event_name == 'schedule' && '1' || inputs.include_kind && '1' || '0' }}"
        in workflow
    )
    assert "KIND_DISCLOSURE_ENDPOINT is required" in workflow
    assert "validate-kind-adapter.py" in workflow
    assert "ENABLE_GOVERNANCE_SHADOW" in workflow
    assert "CURATOR_DISABLE_TELEGRAM_SEND" in workflow
    assert "CURATOR_DELIVERY_MODE: disabled" in workflow
    assert "curator.publish_outbox" not in workflow
    payload = yaml.load(workflow, Loader=yaml.BaseLoader)
    job = payload["jobs"]["ingest"]
    assert "github.ref_name == github.event.repository.default_branch" in job["if"]
    checkout = next(step for step in job["steps"] if step["name"] == "Checkout")
    assert checkout["with"]["ref"] == "${{ github.event.repository.default_branch }}"


def test_media_resolver_and_publisher_are_independent() -> None:
    media = workflow_text("ingest-media.yml")
    resolver = workflow_text("resolve-links.yml")
    publisher = workflow_text("publish.yml")
    assert 'cron: "7,37 * * * *"' in media
    assert "CURATOR_INGEST_SCOPE: media" in media
    assert "telegram-collection-${{ github.repository }}" in media
    assert "validate-media-feeds.py" in media
    assert "TELEGRAM_SESSION_STRING" in media
    assert "Either TELEGRAM_SESSION" not in media
    assert "curator.publish_outbox" not in media
    assert "CURATOR_DELIVERY_MODE: disabled" in media
    media_payload = yaml.load(media, Loader=yaml.BaseLoader)
    media_job = media_payload["jobs"]["ingest"]
    assert (
        "github.ref_name == github.event.repository.default_branch" in media_job["if"]
    )
    media_checkout = next(
        step for step in media_job["steps"] if step["name"] == "Checkout"
    )
    assert (
        media_checkout["with"]["ref"] == "${{ github.event.repository.default_branch }}"
    )
    assert 'cron: "22 * * * *"' in resolver
    assert "curator.resolve_links" in resolver
    assert "claim_link_discoveries" not in resolver  # encapsulated by the resolver CLI
    assert "curator.main" not in resolver
    assert "workflow_run:" in publisher
    assert "ENABLE_GOVERNANCE_DELIVERY" in publisher
    assert "ENABLE_TELEGRAM_DELIVERY" in publisher
    assert (
        "github.event.workflow_run.head_repository.full_name == github.repository"
        in publisher
    )
    assert "curator.publish_outbox" in publisher
    assert 'DELIVERY_LEASE_SECONDS: "900"' in publisher
    assert "curator.publish_outbox --root . --limit 5" in publisher
    assert "!cancelled()" in publisher
    assert "steps.validate.outcome == 'success'" in publisher
    assert "curator.main" not in publisher
    assert "Daily pages and briefing" in publisher
    publisher_payload = yaml.load(publisher, Loader=yaml.BaseLoader)
    assert publisher_payload["on"]["workflow_run"]["branches"] == ["main"]
    publisher_job = publisher_payload["jobs"]["publish"]
    assert (
        "github.ref_name == github.event.repository.default_branch"
        in publisher_job["if"]
    )
    publisher_checkout = next(
        step for step in publisher_job["steps"] if step["name"] == "Checkout"
    )
    assert (
        publisher_checkout["with"]["ref"]
        == "${{ github.event.repository.default_branch }}"
    )
    outbox_consumers = [
        path.name
        for path in WORKFLOWS.glob("*.yml")
        if "python -m curator.publish_outbox" in path.read_text(encoding="utf-8")
    ]
    assert outbox_consumers == ["publish.yml"]


def test_outbound_telegram_is_fail_closed_but_read_collection_remains_enabled() -> None:
    config = yaml.safe_load((ROOT / "config.yaml").read_text(encoding="utf-8"))
    assert config["telegram"]["enabled"] is False
    assert config["telegram"]["chat_id"] == ""
    assert config["telegram_sources"]["enabled"] is True
    assert config["telegram_sources"]["incremental_max_pages"] == 1

    repair = workflow_text("repair-telegram-history.yml")
    payload = yaml.load(repair, Loader=yaml.BaseLoader)
    assert set(payload["on"]) == {"workflow_dispatch"}
    repair_inputs = payload["on"]["workflow_dispatch"]["inputs"]
    assert repair_inputs["days"]["description"] == "Historical window in days (1-365)"
    assert repair_inputs["limit_per_channel"]["description"] == (
        "Telegram history page size per channel (1-3000)"
    )
    assert repair_inputs["channel_limit"]["description"] == (
        "Maximum channels (0 means all; maximum 500)"
    )
    assert (
        repair_inputs["max_messages"]["description"] == "Global message cap (1-300000)"
    )
    assert repair_inputs["max_messages"]["default"] == "300000"
    assert repair_inputs["before_message_id"]["default"] == "0"
    assert (
        "one only_handles channel" in repair_inputs["before_message_id"]["description"]
    )
    assert "curator.telegram_repair" in repair
    assert "CURATOR_DATA_SOURCE: mysql" in repair
    assert "TELEGRAM_API_ID" in repair
    assert "TELEGRAM_SESSION_STRING" in repair
    assert "TELEGRAM_BOT_TOKEN" not in repair
    assert "TELEGRAM_CHAT_ID" not in repair
    assert '--before-message-id "$REPAIR_BEFORE_MESSAGE_ID"' in repair

    repair_job = payload["jobs"]["repair"]
    assert (
        repair_job["if"] == "github.ref_name == github.event.repository.default_branch"
    )
    assert repair_job["environment"] == "telegram-history-repair"
    checkout = next(step for step in repair_job["steps"] if step["name"] == "Checkout")
    assert checkout["with"]["ref"] == "${{ github.event.repository.default_branch }}"
    initialize = next(
        step
        for step in repair_job["steps"]
        if step["name"] == "Initialize repair metrics"
    )
    publish = next(
        step for step in repair_job["steps"] if step["name"] == "Publish repair metrics"
    )
    assert initialize["env"]["CURATOR_RUN_METRICS_PATH"] == (
        "${{ runner.temp }}/telegram-repair-metrics.json"
    )
    assert '{"ok":false,"status":"started"}' in initialize["run"]
    assert publish["if"] == "always()"
    assert publish["with"]["if-no-files-found"] == "error"


def test_telegram_collectors_share_one_non_cancelling_concurrency_group() -> None:
    expected = {
        "group": "telegram-collection-${{ github.repository }}",
        "cancel-in-progress": "false",
        "queue": "max",
    }
    for workflow_name in (
        "build-feed.yml",
        "ingest-media.yml",
        "repair-telegram-history.yml",
    ):
        payload = yaml.load(workflow_text(workflow_name), Loader=yaml.BaseLoader)
        assert payload["concurrency"] == expected, workflow_name


@pytest.mark.parametrize(
    ("workflow_name", "job_name"),
    (
        ("ingest-official.yml", "ingest"),
        ("ingest-media.yml", "ingest"),
        ("publish.yml", "publish"),
        ("repair-telegram-history.yml", "repair"),
        ("resolve-links.yml", "resolve"),
        ("watchdog.yml", "health"),
    ),
)
def test_secret_bearing_manual_jobs_run_default_branch_code_only(
    workflow_name: str, job_name: str
) -> None:
    payload = yaml.load(workflow_text(workflow_name), Loader=yaml.BaseLoader)
    job = payload["jobs"][job_name]

    assert "github.ref_name == github.event.repository.default_branch" in job["if"]
    checkout = next(step for step in job["steps"] if step["name"] == "Checkout")
    assert checkout["with"]["ref"] == "${{ github.event.repository.default_branch }}"


def test_every_workflow_step_exposing_telegram_delivery_secrets_is_opted_in() -> None:
    delivery_secrets = (
        "${{ secrets.TELEGRAM_BOT_TOKEN }}",
        "${{ secrets.TELEGRAM_CHAT_ID }}",
    )
    exposed_steps: list[str] = []

    for path in sorted(WORKFLOWS.glob("*.yml")):
        payload = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
        for job_name, job in payload["jobs"].items():
            job_gate = str(job.get("if", ""))
            for step in job.get("steps", []):
                rendered_step = yaml.safe_dump(step, allow_unicode=True)
                if not any(secret in rendered_step for secret in delivery_secrets):
                    continue

                exposed_steps.append(
                    f"{path.name}:{job_name}:{step.get('name', '<unnamed>')}"
                )
                combined_gate = f"{job_gate}\n{step.get('if', '')}"
                assert "vars.ENABLE_TELEGRAM_DELIVERY == 'true'" in combined_gate, (
                    f"{exposed_steps[-1]} exposes Telegram delivery credentials without "
                    "the repository delivery opt-in"
                )

    assert exposed_steps


def test_daily_generation_and_delivery_use_requested_kst_boundaries() -> None:
    workflow = workflow_text("daily.yml")
    assert 'cron: "45 20 * * *"' in workflow
    assert 'cron: "5 21 * * *"' in workflow
    assert 'CURATOR_DAILY_REPORT_WRITE_ONLY: "1"' in workflow
    assert "daily_report_queued=1" in workflow
    assert "CURATOR_DELIVERY_MODE: outbox-enqueue" in workflow
    assert "ENABLE_TELEGRAM_DELIVERY" in workflow
    assert "curator.story_review send" not in workflow
    assert "TELEGRAM_ADMIN_CHAT_ID" not in workflow
    assert "send_telegram_admin_access" not in workflow
    assert "curator.telegram_dashboard send-access" not in workflow
    assert "Build token-gated Telegram admin shell" in workflow
    assert (
        "TELEGRAM_ADMIN_ACCESS_TOKEN: ${{ secrets.TELEGRAM_ADMIN_ACCESS_TOKEN }}"
        in workflow
    )
    assert (
        "require-env.sh TELEGRAM_ADMIN_ACCESS_TOKEN ACTIVIST_PUBLIC_API_URL" in workflow
    )
    assert "python -m curator.telegram_dashboard write" in workflow
    assert "actions/upload-pages-artifact@v5" in workflow
    assert "actions/deploy-pages@v5" in workflow
    assert "ENABLE_GOVERNANCE_PAGES" in workflow
    assert "vars.ENABLE_PAGES != 'true'" in workflow
    assert "ENABLE_PAGES and ENABLE_GOVERNANCE_PAGES are mutually exclusive" in workflow
    assert (
        "governance-pages-ready-${{ steps.deployment_marker.outputs.kst_date }}"
        in workflow
    )
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

    uploads = [
        step for step in steps if step.get("uses") == "actions/upload-pages-artifact@v5"
    ]
    deployments = [
        step for step in steps if step.get("uses") == "actions/deploy-pages@v5"
    ]
    configuration = next(
        step for step in steps if step.get("uses") == "actions/configure-pages@v6"
    )
    assert "enablement" not in configuration.get("with", {})
    assert "token" not in configuration.get("with", {})
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

    waits = {
        step["name"]: step for step in steps if step["name"].startswith("Wait before")
    }
    assert waits["Wait before first Pages retry"]["run"] == "sleep 180"
    assert waits["Wait before final Pages retry"]["run"] == "sleep 300"
    assert "steps.deployment.outcome == 'failure'" in deployments[1]["if"]
    assert "steps.deployment.outcome == 'failure'" in deployments[2]["if"]
    assert "steps.deployment_retry.outcome == 'failure'" in deployments[2]["if"]

    verifier = next(
        step for step in steps if step["name"] == "Verify GitHub Pages deployment"
    )
    assert verifier["id"] == "pages_deployment_result"
    assert "!cancelled()" in verifier["if"]
    assert "DEPLOYMENT_OUTCOME_3" in verifier["env"]
    assert "DEPLOYMENT_URL_3" in verifier["env"]
    assert (
        verifier["env"]["PAGES_ARTIFACT_ID"]
        == "${{ steps.pages_artifact.outputs.artifact_id }}"
    )
    assert 'echo "page_url=$selected_url" >> "$GITHUB_OUTPUT"' in verifier["run"]
    assert "failed after three attempts" in verifier["run"]
    assert (
        job["environment"]["url"]
        == "${{ steps.pages_deployment_result.outputs.page_url }}"
    )

    if workflow_name == "daily.yml":
        assert int(job["timeout-minutes"]) >= 60
        marker_index = next(
            index
            for index, step in enumerate(steps)
            if step["name"] == "Create daily deployment marker"
        )
        assert marker_index > steps.index(verifier)
    else:
        assert int(job["timeout-minutes"]) == 75
        build_feed = next(step for step in steps if step["name"] == "Build feed")
        assert int(build_feed["timeout-minutes"]) == 45
        assert "id" not in build_feed
        assert (
            build_feed["env"]["CURATOR_RUN_METRICS_PATH"]
            == "${{ runner.temp }}/curator-run-metrics.json"
        )
        initialize_metrics = next(
            step for step in steps if step["name"] == "Initialize curator run metrics"
        )
        assert (
            initialize_metrics["if"] == "${{ steps.run_mode.outputs.full == 'true' }}"
        )
        assert initialize_metrics["env"]["CURATOR_RUN_METRICS_PATH"] == (
            "${{ runner.temp }}/curator-run-metrics.json"
        )
        assert '{"ok":false,"status":"started"}' in initialize_metrics["run"]
        assert steps.index(initialize_metrics) < steps.index(build_feed)
        verify_metrics = next(
            step for step in steps if step["name"] == "Verify curator run metrics"
        )
        assert "success()" in verify_metrics["if"]
        assert verify_metrics["env"]["CURATOR_RUN_METRICS_PATH"] == (
            "${{ runner.temp }}/curator-run-metrics.json"
        )
        assert "payload.get('ok') is True" in verify_metrics["run"]
        assert "payload.get('status') == 'complete'" in verify_metrics["run"]
        metrics = next(
            step for step in steps if step["name"] == "Publish curator run metrics"
        )
        assert "always()" in metrics["if"]
        assert metrics["uses"] == "actions/upload-artifact@v7"
        assert metrics["with"]["if-no-files-found"] == "error"
        assert (
            steps.index(build_feed) < steps.index(verify_metrics) < steps.index(metrics)
        )
        assert payload["permissions"]["actions"] == "read"
        restore = next(
            step
            for step in steps
            if step["name"] == "Restore validated legacy dated reports"
        )
        prepare = next(
            step
            for step in steps
            if step["name"] == "Prepare allowlisted legacy Pages artifact"
        )
        daily_report = next(
            step for step in steps if step["name"] == "Build daily report page"
        )
        pages_upload = uploads[0]
        assert (
            steps.index(restore)
            < steps.index(daily_report)
            < steps.index(prepare)
            < steps.index(pages_upload)
        )
        assert "restore-legacy-pages-archive.py" in restore["run"]
        assert "previous-legacy-pages" in restore["run"]
        assert (
            pages_upload["with"]["path"]
            == "${{ steps.legacy_pages_artifact.outputs.path }}"
        )
        failure_artifact = next(
            step for step in steps if step["name"] == "Preserve failed Pages artifact"
        )
        assert (
            "steps.pages_deployment_result.outcome == 'failure'"
            in failure_artifact["if"]
        )
        assert (
            "steps.legacy_pages_artifact.outcome == 'success'" in failure_artifact["if"]
        )
        assert (
            failure_artifact["with"]["path"]
            == "${{ steps.legacy_pages_artifact.outputs.path }}"
        )
        assert failure_artifact["with"]["retention-days"] == "7"


def test_legacy_pages_archive_download_and_seed_are_fail_closed() -> None:
    payload = yaml.load(workflow_text("build-feed.yml"), Loader=yaml.BaseLoader)
    job = payload["jobs"]["build-feed"]
    steps = job["steps"]
    resolver = next(
        step
        for step in steps
        if step["name"] == "Resolve previous legacy Pages artifact"
    )
    download = next(
        step
        for step in steps
        if step["name"] == "Download previous legacy Pages artifact"
    )
    seed = next(
        step
        for step in steps
        if step["name"] == "Preserve sanitized legacy archive seed"
    )

    assert resolver["uses"] == "actions/github-script@v9"
    assert 'sourceRun.conclusion !== "success"' in resolver["with"]["script"]
    assert "artifact.expired" in resolver["with"]["script"]
    assert "sourceRunId === Number(context.runId)" in resolver["with"]["script"]
    assert "sourceRun.path !== expectedWorkflowPath" in resolver["with"]["script"]
    assert "sourceRun.head_repository?.full_name" in resolver["with"]["script"]
    assert "core.setFailed" in resolver["with"]["script"]
    assert download["uses"] == "actions/download-artifact@v8"
    assert download["with"] == {
        "artifact-ids": "${{ steps.previous_legacy_pages.outputs.artifact_id }}",
        "path": "${{ runner.temp }}/previous-legacy-pages",
        "github-token": "${{ github.token }}",
        "repository": "${{ github.repository }}",
        "run-id": "${{ steps.previous_legacy_pages.outputs.run_id }}",
        "merge-multiple": "true",
        "digest-mismatch": "error",
    }
    assert seed["uses"] == "actions/upload-artifact@v7"
    assert seed["with"]["name"] == "legacy-pages-archive-seed"
    assert seed["with"]["path"] == "${{ steps.legacy_pages_artifact.outputs.path }}"
    assert seed["with"]["retention-days"] == "30"
    assert "preserve_legacy_archive_seed" in seed["if"]


def test_pages_deployment_is_default_branch_only() -> None:
    legacy = workflow_text("build-feed.yml")
    daily = workflow_text("daily.yml")
    assert "REF_NAME: ${{ github.ref_name }}" in legacy
    assert "DEFAULT_BRANCH: ${{ github.event.repository.default_branch }}" in legacy
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
    assert (
        "github.event.workflow_run.head_repository.full_name == github.repository"
        in listener["if"]
    )
    assert payload["on"]["workflow_run"]["workflows"] == [
        "Build curated RSS feed",
        "Daily pages and briefing",
    ]
    assert payload["on"]["workflow_run"]["branches"] == ["main"]
    assert "/attempts/{attempt_number}/jobs" in workflow
    assert 'verificationName = "Verify GitHub Pages deployment"' in workflow
    assert (
        "No completed Pages verification step; incident state is unchanged." in workflow
    )
    assert "[ops/incident] GitHub Pages deployment unhealthy" in workflow
    assert 'state_reason: "completed"' in workflow
    assert 'state: "open"' in workflow
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
    assert ".github/scripts/prepare-legacy-pages.py" in workflow
    assert ".github/scripts/restore-legacy-pages-archive.py" in workflow


def test_workflows_use_current_node24_official_action_majors() -> None:
    workflows = "\n".join(
        path.read_text(encoding="utf-8") for path in WORKFLOWS.glob("*.yml")
    )
    expected_actions = {
        "actions/checkout@v7",
        "actions/setup-python@v7",
        "actions/setup-node@v7",
        "actions/github-script@v9",
        "actions/configure-pages@v6",
        "actions/deploy-pages@v5",
        "actions/upload-pages-artifact@v5",
        "actions/upload-artifact@v7",
        "actions/download-artifact@v8",
    }
    assert set(re.findall(r"uses:\s+(actions/[^\s]+)", workflows)) == expected_actions
    assert "pip-install" not in workflows
    assert "always-auth" not in workflows
    assert "require('@actions/github')" not in workflows
    assert 'require("@actions/github")' not in workflows
    assert not re.search(r"\b(?:const|let)\s+getOctokit\b", workflows)
    assert "ACTIONS_ALLOW_USE_UNSECURE_NODE_VERSION" not in workflows

    ci = yaml.load(workflow_text("ci.yml"), Loader=yaml.BaseLoader)
    setup_node = next(
        step
        for step in ci["jobs"]["ui-e2e"]["steps"]
        if step.get("uses") == "actions/setup-node@v7"
    )
    assert setup_node["with"] == {"node-version": "22", "cache": "npm"}
    assert not [path for path in (ROOT / "public").rglob(".*") if path.is_file()]


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
    payload = yaml.load(workflow, Loader=yaml.BaseLoader)
    assert "workflow_dispatch:" in workflow
    assert "actions: read" in workflow
    assert "actions/download-artifact@v8" in workflow
    assert "digest-mismatch: error" in workflow
    download = next(
        step
        for step in payload["jobs"]["evaluate"]["steps"]
        if step.get("uses") == "actions/download-artifact@v8"
    )
    assert download["with"] == {
        "name": "${{ inputs.evidence_artifact_name }}",
        "path": "evidence",
        "github-token": "${{ github.token }}",
        "run-id": "${{ inputs.evidence_run_id }}",
        "digest-mismatch": "error",
    }
    assert "Validate production evidence run provenance" in workflow
    assert 'MAX_RUN_AGE_HOURS: "72"' in workflow
    assert "Evidence workflow run revision does not match" in workflow
    assert "run-id: ${{ inputs.evidence_run_id }}" in workflow
    assert "python -m curator.release_gate" in workflow
    assert "--expected-revision ${{ github.sha }}" in workflow
    assert "--evidence-as-of ${{ steps.evidence_run.outputs.created_at }}" in workflow
    assert "actions/upload-artifact@v7" in workflow
    assert "Governance release transition gate did not pass" in workflow
