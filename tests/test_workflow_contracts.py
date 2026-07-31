from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
PRODUCTION_OFFICIAL_WRITE_CONCURRENCY = {
    "group": (
        "governance-production-official-write-"
        "${{ github.repository }}-${{ github.ref }}"
    ),
    "queue": "max",
    "cancel-in-progress": "false",
}


def workflow_text(name: str) -> str:
    return (WORKFLOWS / name).read_text(encoding="utf-8")


def _contains_secret_expression(value: object) -> bool:
    if isinstance(value, str):
        return "${{ secrets." in value
    if isinstance(value, dict):
        return any(_contains_secret_expression(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_secret_expression(item) for item in value)
    return False


def _environment_name(job: dict[str, object]) -> str | None:
    environment = job.get("environment")
    if isinstance(environment, str):
        return environment
    if isinstance(environment, dict):
        name = environment.get("name")
        return name if isinstance(name, str) else None
    return None


def test_all_workflows_are_valid_yaml() -> None:
    paths = sorted(WORKFLOWS.glob("*.yml"))
    assert paths
    for path in paths:
        payload = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
        assert isinstance(payload, dict), path
        assert "name" in payload, path
        assert "on" in payload, path
        assert "jobs" in payload, path


def test_ci_parses_the_critical_rollback_shell_script() -> None:
    ci = workflow_text("ci.yml")
    assert "Validate critical rollback shell syntax" in ci
    assert "bash -n .github/scripts/close-governance-release-state.sh" in ci


@pytest.mark.parametrize(
    ("workflow_name", "job_name", "producer_steps", "artifact_step", "path"),
    (
        (
            "ingest-global.yml",
            "ingest",
            (
                "Initialize fail-safe evidence",
                "Collect and ingest official source",
            ),
            "Preserve global ingest evidence",
            "${{ runner.temp }}/global-ingest-${{ matrix.country }}.json",
        ),
        (
            "ingest-selected-markets.yml",
            "ingest",
            (
                "Initialize fail-safe evidence",
                "Validate and ingest approved manual metadata links",
            ),
            "Preserve manual official-link ingest evidence",
            (
                "${{ runner.temp }}/manual-official-link-ingest-"
                "${{ matrix.country }}.json"
            ),
        ),
        (
            "global-alpha-watchdog.yml",
            "observe",
            (
                "Initialize fail-closed observation evidence",
                "Observe API, release, sources, and public build",
            ),
            "Preserve immutable observation evidence",
            "${{ runner.temp }}/global-alpha-observation.json",
        ),
    ),
)
def test_runner_temp_evidence_paths_are_scoped_to_steps(
    workflow_name: str,
    job_name: str,
    producer_steps: tuple[str, ...],
    artifact_step: str,
    path: str,
) -> None:
    workflow = yaml.load(
        workflow_text(workflow_name),
        Loader=yaml.BaseLoader,
    )
    job = workflow["jobs"][job_name]
    assert "EVIDENCE_PATH" not in job.get("env", {})

    steps = {
        step["name"]: step
        for step in job["steps"]
        if isinstance(step, dict) and "name" in step
    }
    for step_name in producer_steps:
        assert steps[step_name]["env"]["EVIDENCE_PATH"] == path
    assert steps[artifact_step]["with"]["path"] == path


def test_upload_artifact_v7_uses_the_declared_digest_output_name() -> None:
    upload_action = (
        "actions/upload-artifact@"
        "043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
    )
    for path in sorted(WORKFLOWS.glob("*.yml")):
        text = path.read_text(encoding="utf-8")
        workflow = yaml.load(text, Loader=yaml.BaseLoader)
        for job in workflow["jobs"].values():
            if not isinstance(job, dict):
                continue
            for step in job.get("steps", []):
                if (
                    not isinstance(step, dict)
                    or step.get("uses") != upload_action
                    or not isinstance(step.get("id"), str)
                ):
                    continue
                step_id = step["id"]
                assert f"steps.{step_id}.outputs.digest" not in text, (
                    path.name,
                    step_id,
                )


def test_dispatch_jobs_with_repository_secrets_use_main_only_environments() -> None:
    runtime_jobs = {
        ("ingest-media.yml", "ingest"),
        ("ingest-official.yml", "ingest"),
        ("kind-adapter-preflight.yml", "preflight"),
        ("official-backfill.yml", "backfill"),
        ("release-evidence-inputs.yml", "collect"),
        ("resolve-links.yml", "resolve"),
        ("shadow-compare.yml", "compare"),
        ("watchdog.yml", "health"),
        ("web-vitals.yml", "mobile-routes"),
    }
    observed_runtime_jobs: set[tuple[str, str]] = set()
    allowed_protected_environments = {
        "github-pages",
        "governance-release",
        "governance-runtime",
        "telegram-history-repair",
    }

    for path in sorted(WORKFLOWS.glob("*.yml")):
        workflow = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
        triggers = workflow.get("on")
        if not isinstance(triggers, dict) or "workflow_dispatch" not in triggers:
            continue
        for job_name, job in workflow["jobs"].items():
            if not isinstance(job, dict) or not _contains_secret_expression(job):
                continue
            environment_name = _environment_name(job)
            assert environment_name in allowed_protected_environments, (
                path.name,
                job_name,
                environment_name,
            )
            identity = (path.name, str(job_name))
            if identity in runtime_jobs:
                assert environment_name == "governance-runtime"
                observed_runtime_jobs.add(identity)

    assert observed_runtime_jobs == runtime_jobs


def test_official_writes_and_release_transitions_use_one_non_dropping_queue() -> None:
    for name in (
        "ingest-official.yml",
        "official-backfill.yml",
        "ingest-global.yml",
        "global-backfill.yml",
        "ingest-official-sites.yml",
        "ingest-selected-markets.yml",
        "official-slot-epoch-reset.yml",
        "source-right-bootstrap.yml",
        "governance-cutover.yml",
    ):
        workflow = yaml.load(workflow_text(name), Loader=yaml.BaseLoader)
        assert workflow["concurrency"] == PRODUCTION_OFFICIAL_WRITE_CONCURRENCY, name


def test_emergency_rollback_does_not_wait_behind_official_backfill() -> None:
    workflow = yaml.load(
        workflow_text("governance-rollback.yml"),
        Loader=yaml.BaseLoader,
    )
    assert workflow["concurrency"] == {
        "group": (
            "governance-emergency-rollback-"
            "${{ github.repository }}-${{ github.ref }}"
        ),
        "cancel-in-progress": "false",
        "queue": "max",
    }
    assert (
        workflow["concurrency"]["group"]
        != PRODUCTION_OFFICIAL_WRITE_CONCURRENCY["group"]
    )


def test_human_brief_publish_joins_official_write_transition_boundary() -> None:
    workflow = yaml.load(
        workflow_text("global-brief.yml"),
        Loader=yaml.BaseLoader,
    )
    assert workflow["jobs"]["publish"]["concurrency"] == (
        PRODUCTION_OFFICIAL_WRITE_CONCURRENCY
    )
    assert workflow["concurrency"]["group"] == (
        "global-brief-${{ github.repository }}-${{ github.ref }}"
    )


def test_protected_writers_require_the_exact_default_branch_ref() -> None:
    job_contracts = (
        ("ingest-official.yml", "ingest"),
        ("official-backfill.yml", "backfill"),
        ("official-slot-epoch-reset.yml", "reset"),
        ("ingest-global.yml", "ingest"),
        ("global-backfill.yml", "backfill"),
        ("ingest-official-sites.yml", "ingest"),
        ("ingest-selected-markets.yml", "ingest"),
        ("global-brief.yml", "candidates"),
        ("global-brief.yml", "publish"),
    )
    for workflow_name, job_name in job_contracts:
        workflow = yaml.load(
            workflow_text(workflow_name),
            Loader=yaml.BaseLoader,
        )
        job_if = str(workflow["jobs"][job_name]["if"])
        assert "github.ref_type == 'branch'" in job_if, workflow_name
        assert (
            "github.ref == format('refs/heads/{0}', "
            "github.event.repository.default_branch)"
        ) in job_if, workflow_name

    for workflow_name, job_name, step_name in (
        (
            "source-right-bootstrap.yml",
            "bootstrap",
            "Enforce protected closed-state bootstrap",
        ),
        (
            "governance-cutover.yml",
            "validate",
            "Enforce default branch and safe rollout variables",
        ),
        (
            "governance-rollback.yml",
            "close",
            "Enforce protected rollback inputs and default branch",
        ),
    ):
        workflow = yaml.load(
            workflow_text(workflow_name),
            Loader=yaml.BaseLoader,
        )
        step = next(
            item
            for item in workflow["jobs"][job_name]["steps"]
            if item["name"] == step_name
        )
        assert step["env"]["REF"] == "${{ github.ref }}"
        assert step["env"]["REF_TYPE"] == "${{ github.ref_type }}"
        assert (
            '[[ "$REF_TYPE" == "branch" && '
            '"$REF" == "refs/heads/$DEFAULT_BRANCH" ]]'
        ) in step["run"]


def test_backfill_shell_never_interpolates_dispatch_text_directly() -> None:
    workflow = workflow_text("official-backfill.yml")
    payload = yaml.load(workflow, Loader=yaml.BaseLoader)
    steps = payload["jobs"]["backfill"]["steps"]
    for input_name in (
        "mode",
        "frozen_apply_run_id",
        "source",
        "from_date",
        "to_date",
        "max_windows",
        "sync_company_master",
        "defer_review_sample",
        "canary_lookback_days",
        "canary_request_budget",
    ):
        for step in steps:
            assert f"${{{{ inputs.{input_name} }}}}" not in str(step.get("run", ""))
    run_step = next(
        step for step in steps if step["name"] == "Run one-day official backfill windows"
    )
    for env_name in (
        "BACKFILL_MODE",
        "BACKFILL_SOURCE",
        "BACKFILL_FROM_DATE",
        "BACKFILL_TO_DATE",
        "BACKFILL_MAX_WINDOWS",
        "BACKFILL_SYNC_COMPANY_MASTER",
    ):
        assert env_name in run_step["env"]


def test_kind_preflight_reuses_https_only_endpoint_validation() -> None:
    validator = (ROOT / ".github" / "scripts" / "validate-kind-adapter.py").read_text(
        encoding="utf-8"
    )
    assert "validate_kind_endpoint(endpoint)" in validator
    assert 'parsed.scheme not in {"http", "https"}' not in validator


def test_legacy_shadow_baseline_does_not_commit_generated_files() -> None:
    legacy = workflow_text("build-feed.yml")
    assert "ENABLE_LEGACY_PIPELINE == 'true'" in legacy
    assert "CURATOR_DATA_SOURCE: mysql" in legacy
    assert "Permanent web-only distribution policy" in legacy
    assert "TELEGRAM_BOT_TOKEN" not in legacy
    assert "TELEGRAM_CHAT_ID" not in legacy
    assert "CURATOR_DELIVERY_MODE: disabled" in legacy
    assert 'CURATOR_DISABLE_TELEGRAM_SEND: "1"' in legacy
    assert "allow_pages_deploy:" in legacy
    assert "allow_telegram_delivery:" not in legacy
    assert "send_telegram_test:" not in legacy
    assert "resend_last_briefing:" not in legacy
    assert "resend_recent_articles:" not in legacy
    assert "resend_cluster_guid:" not in legacy
    assert "send_daily_report:" not in legacy
    assert "default: false" in legacy
    assert '"$ALLOW_PAGES_DEPLOY" == "true"' in legacy
    assert "Promoting a deployable page_only run to a verified full run" in legacy
    baseline = yaml.load(legacy, Loader=yaml.BaseLoader)
    baseline_steps = baseline["jobs"]["build-feed"]["steps"]
    for step_name in (
        "Build daily report page",
        "Build token-gated Telegram admin shell",
    ):
        publication_step = next(
            step for step in baseline_steps if step["name"] == step_name
        )
        assert publication_step["if"] == (
            "${{ steps.run_mode.outputs.full == 'true' }}"
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
        assert "if: ${{ false }}" in block
        assert "permanently disabled" in block
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
    assert 'cron: "0,30 15-21 * * *"' in workflow
    assert 'cron: "40 21 * * 0"' in workflow
    assert "CURATOR_INGEST_SCOPE: official" in workflow
    assert "inputs.include_kind" in workflow
    assert "leave off for DART-only smoke/shadow" in workflow
    assert "vars.GOVERNANCE_PIPELINE_MODE == 'dart_canary'" in workflow
    assert "steps.rollout.outputs.governance_pipeline_mode == 'shadow'" in workflow
    assert "steps.rollout.outputs.governance_pipeline_mode == 'live'" in workflow
    assert "KIND_CONNECTOR_MODE: ${{ vars.KIND_CONNECTOR_MODE }}" in workflow
    assert "steps.rollout.outputs.kind_connector_enabled == 'true'" in workflow
    assert "python -m curator.operation_mode --github-output \"$GITHUB_OUTPUT\"" in workflow
    assert "KIND_DISCLOSURE_ENDPOINT BSIDE_API_BASE_URL BSIDE_OPS_TOKEN" in workflow
    assert "mask-opendart-credentials.py" in workflow
    assert "OPENDART_API_KEYS: ${{ secrets.OPENDART_API_KEYS }}" in workflow
    assert (
        "DART_API_KEY: ${{ secrets.OPENDART_API_KEYS == '' "
        "&& secrets.DART_API_KEY || '' }}"
    ) in workflow
    assert "DART_API_KEY ACTIVIST_API_URL ACTIVIST_API_SECRET" not in workflow
    assert "CURATOR_REQUIRE_DURABLE_DART_QUOTA" in workflow
    assert "CURATOR_GITHUB_RUN_CREATED_AT" in workflow
    assert "github.rest.actions.getWorkflowRun" in workflow
    assert "validate-kind-adapter.py" not in workflow
    assert "ENABLE_GOVERNANCE_SHADOW" in workflow
    assert "CURATOR_DISABLE_TELEGRAM_SEND" in workflow
    assert "CURATOR_DELIVERY_MODE: disabled" in workflow
    assert "curator.publish_outbox" not in workflow
    payload = yaml.load(workflow, Loader=yaml.BaseLoader)
    job = payload["jobs"]["ingest"]
    condition = job["if"]
    assert condition.strip().startswith(
        "vars.DART_OFFICIAL_INGEST_ENABLED == 'true' &&"
    )
    assert condition.count("vars.DART_OFFICIAL_INGEST_ENABLED") == 1
    assert "github.ref_type == 'branch'" in condition
    assert (
        "github.ref == format('refs/heads/{0}', "
        "github.event.repository.default_branch)"
    ) in condition
    assert "github.event_name == 'workflow_dispatch'" in condition
    checkout = next(step for step in job["steps"] if step["name"] == "Checkout")
    # Pin the exact workflow revision so the candidate artifact and its
    # declared full SHA cannot drift if main advances during the run. The job
    # condition above still restricts dispatch to the default-branch ref.
    assert checkout["with"]["ref"] == "${{ github.sha }}"
    validation = next(
        step for step in job["steps"] if step["name"] == "Validate operational configuration"
    )
    assert validation["env"]["BSIDE_API_BASE_URL"] == (
        "${{ secrets.BSIDE_API_BASE_URL || vars.GOVERNANCE_API_BASE_URL }}"
    )
    assert validation["env"]["GOVERNANCE_API_BASE_URL"] == (
        "${{ vars.GOVERNANCE_API_BASE_URL }}"
    )
    assert validation["env"]["BSIDE_OPS_TOKEN"] == "${{ secrets.BSIDE_OPS_TOKEN }}"
    assert validation["env"]["BSIDE_BACKEND_BINDING_ID"] == (
        "${{ vars.BSIDE_BACKEND_BINDING_ID }}"
    )
    assert validation["env"]["OPENDART_API_KEYS"] == "${{ secrets.OPENDART_API_KEYS }}"
    assert validation["env"]["DART_API_KEY"] == (
        "${{ secrets.OPENDART_API_KEYS == '' && secrets.DART_API_KEY || '' }}"
    )
    assert "BSIDE_BACKEND_BINDING_ID" in validation["run"]
    assert "python .github/scripts/validate-api-base-urls.py" in validation["run"]
    ingest = next(
        step for step in job["steps"] if step["name"] == "Ingest selected official sources"
    )
    expected_kind_selection = (
        "${{ (((github.event_name == 'schedule' || "
        "inputs.repair_expected_slot_at != '') && "
        "(steps.rollout.outputs.governance_pipeline_mode == 'shadow' || "
        "steps.rollout.outputs.governance_pipeline_mode == 'live') && "
        "steps.rollout.outputs.kind_connector_enabled == 'true') || "
        "inputs.include_kind) && '1' || '0' }}"
    )
    assert validation["env"]["CURATOR_ENABLE_KIND"] == expected_kind_selection
    assert validation["env"]["CURATOR_REQUIRE_KIND"] == expected_kind_selection
    assert ingest["env"]["CURATOR_ENABLE_KIND"] == expected_kind_selection
    assert ingest["env"]["CURATOR_REQUIRE_KIND"] == expected_kind_selection
    claim = next(
        step
        for step in job["steps"]
        if step["name"] == "Claim oldest due official-ingest slot"
    )
    dart_preflight = next(
        step
        for step in job["steps"]
        if step["name"]
        == "Verify DART SourceRight and exact deployed release before claim"
    )
    assert job["steps"].index(validation) < job["steps"].index(dart_preflight)
    assert job["steps"].index(dart_preflight) < job["steps"].index(claim)
    assert job["steps"].index(claim) < job["steps"].index(ingest)
    assert dart_preflight["env"]["BSIDE_OPS_TOKEN"] == (
        "${{ secrets.BSIDE_OPS_TOKEN }}"
    )
    assert "--preflight-dart" in dart_preflight["run"]
    assert '--expected-release-sha "$GITHUB_SHA"' in dart_preflight["run"]
    assert '--pipeline-mode "$GOVERNANCE_PIPELINE_MODE"' in dart_preflight["run"]
    assert dart_preflight["env"]["GOVERNANCE_PIPELINE_MODE"] == (
        "${{ steps.rollout.outputs.governance_pipeline_mode }}"
    )
    ingest_script = str(ingest["run"])
    assert ingest_script.index("CURATOR_OFFICIAL_SLOT_TERMINAL_NOOP") < ingest_script.index(
        "python -m curator.main"
    )
    assert "exit 0" in ingest_script
    assert ingest["env"]["BSIDE_API_BASE_URL"] == validation["env"]["BSIDE_API_BASE_URL"]
    assert ingest["env"]["BSIDE_OPS_TOKEN"] == validation["env"]["BSIDE_OPS_TOKEN"]
    assert ingest["env"]["BSIDE_BACKEND_BINDING_ID"] == (
        validation["env"]["BSIDE_BACKEND_BINDING_ID"]
    )
    assert ingest["env"]["OPENDART_API_KEYS"] == validation["env"]["OPENDART_API_KEYS"]
    assert ingest["env"]["DART_API_KEY"] == validation["env"]["DART_API_KEY"]
    assert ingest["env"]["GOVERNANCE_PIPELINE_MODE"] == (
        "${{ steps.rollout.outputs.governance_pipeline_mode }}"
    )


def test_official_backfill_preflights_dart_apply_and_replay_before_checkpoint_access() -> None:
    payload = yaml.load(
        workflow_text("official-backfill.yml"),
        Loader=yaml.BaseLoader,
    )
    steps = payload["jobs"]["backfill"]["steps"]
    preflight = next(
        step
        for step in steps
        if step["name"] == "Verify DART SourceRight before any durable DART write"
    )
    run = next(
        step
        for step in steps
        if step["name"] == "Run one-day official backfill windows"
    )
    previous = next(
        step
        for step in steps
        if step["name"] == "Resolve previous matching checkpoint"
    )
    assert preflight["if"] == "inputs.mode != 'dry-run' && inputs.source != 'kind'"
    assert steps.index(preflight) < steps.index(previous) < steps.index(run)
    assert "--preflight-dart" in preflight["run"]
    assert '--expected-release-sha "$GITHUB_SHA"' in preflight["run"]
    assert '--pipeline-mode "$GOVERNANCE_PIPELINE_MODE"' in preflight["run"]
    assert preflight["env"]["BSIDE_OPS_TOKEN"] == "${{ secrets.BSIDE_OPS_TOKEN }}"
    assert preflight["env"]["GOVERNANCE_PIPELINE_MODE"] == (
        "${{ steps.rollout.outputs.governance_pipeline_mode }}"
    )
    assert run["env"]["GOVERNANCE_PIPELINE_MODE"] == (
        "${{ steps.rollout.outputs.governance_pipeline_mode }}"
    )


def test_ci_type_checks_the_fixed_official_source_contract() -> None:
    ci = workflow_text("ci.yml")
    assert "curator/dart_frozen_replay_bundle.py" in ci
    assert (
        ci.index("curator/official_source_contracts.py")
        < ci.index("curator/official_source_rights.py")
    )


def test_official_slot_repair_and_epoch_reset_are_operator_gated() -> None:
    ingest_workflow = workflow_text("ingest-official.yml")
    ingest_payload = yaml.load(ingest_workflow, Loader=yaml.BaseLoader)
    dispatch_inputs = ingest_payload["on"]["workflow_dispatch"]["inputs"]
    assert "repair_event_schedule" in dispatch_inputs
    assert "repair_expected_slot_at" in dispatch_inputs
    assert ingest_payload["concurrency"] == PRODUCTION_OFFICIAL_WRITE_CONCURRENCY
    assert "CURATOR_OFFICIAL_SLOT_REPAIR_EXPECTED_AT" in ingest_workflow
    assert "inputs.repair_expected_slot_at != ''" in ingest_workflow

    reset_workflow = workflow_text("official-slot-epoch-reset.yml")
    reset_payload = yaml.load(reset_workflow, Loader=yaml.BaseLoader)
    reset_job = reset_payload["jobs"]["reset"]
    assert reset_payload["concurrency"] == ingest_payload["concurrency"]
    assert reset_job["environment"] == {"name": "governance-release"}
    assert "vars.GOVERNANCE_PIPELINE_MODE == 'off'" in reset_job["if"]
    reset_step = next(
        step
        for step in reset_job["steps"]
        if step["name"] == "Advance append-only epoch at next KST day"
    )
    assert reset_step["env"]["BSIDE_ADMIN_TOKEN"] == "${{ secrets.BSIDE_ADMIN_TOKEN }}"
    assert reset_step["env"]["RESET_REASON"] == "${{ inputs.reason }}"
    assert reset_step["env"]["RESET_CONFIRMATION"] == "${{ inputs.confirmation }}"
    reset_script = str(reset_step["run"])
    assert "${{ inputs." not in reset_script
    assert '"$RESET_REASON"' in reset_script
    assert '"$RESET_CONFIRMATION"' in reset_script


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
    assert media_checkout["with"]["ref"] == "${{ github.sha }}"
    assert any(
        step["name"] == "Verify immutable workflow revision"
        for step in media_job["steps"]
    )
    assert 'cron: "7,22,37,52 * * * *"' in resolver
    assert "inputs.limit || 200" in resolver
    assert "--max-runtime 1200" in resolver
    assert "curator.resolve_links" in resolver
    assert "vars.PAGES_OWNER == 'legacy'" in resolver
    assert "vars.ENABLE_LEGACY_PIPELINE == 'true'" in resolver
    assert "claim_link_discoveries" not in resolver  # encapsulated by the resolver CLI
    assert "curator.main" not in resolver
    assert "workflow_run:" not in publisher
    assert "schedule:" not in publisher
    assert "workflow_dispatch:" in publisher
    assert "if: ${{ false }}" in publisher
    assert "permanently disabled" in publisher
    assert "TELEGRAM_BOT_TOKEN" not in publisher
    assert "TELEGRAM_CHAT_ID" not in publisher
    assert "curator.publish_outbox" not in publisher
    assert "curator.main" not in publisher
    publisher_payload = yaml.load(publisher, Loader=yaml.BaseLoader)
    assert set(publisher_payload["on"]) == {"workflow_dispatch"}
    publisher_job = publisher_payload["jobs"]["publish"]
    assert publisher_job["if"] == "${{ false }}"
    outbox_consumers = [
        path.name
        for path in WORKFLOWS.glob("*.yml")
        if "python -m curator.publish_outbox" in path.read_text(encoding="utf-8")
    ]
    assert outbox_consumers == []


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
    assert repair_inputs["expected_selection_fingerprint"]["default"] == ""
    assert (
        "Required for start_after/before>0"
        in repair_inputs["expected_selection_fingerprint"]["description"]
    )
    assert "optional assertion for before=0" in (
        repair_inputs["expected_selection_fingerprint"]["description"]
    )
    assert repair_inputs["finalize_signal_rebuild"]["type"] == "boolean"
    assert repair_inputs["finalize_signal_rebuild"]["default"] == "false"
    assert "curator.telegram_repair" in repair
    assert "CURATOR_DATA_SOURCE: mysql" in repair
    assert "TELEGRAM_API_ID" in repair
    assert "TELEGRAM_SESSION_STRING" in repair
    assert "TELEGRAM_BOT_TOKEN" not in repair
    assert "TELEGRAM_CHAT_ID" not in repair
    assert '--before-message-id "$REPAIR_BEFORE_MESSAGE_ID"' in repair
    assert (
        '--expected-selection-fingerprint '
        '"$REPAIR_EXPECTED_SELECTION_FINGERPRINT"' in repair
    )

    repair_job = payload["jobs"]["repair"]
    assert (
        repair_job["if"] == "github.ref_name == github.event.repository.default_branch"
    )
    assert repair_job["environment"] == "telegram-history-repair"
    checkout = next(step for step in repair_job["steps"] if step["name"] == "Checkout")
    assert checkout["with"]["ref"] == "${{ github.sha }}"
    assert any(
        step["name"] == "Verify immutable workflow revision"
        for step in repair_job["steps"]
    )
    initialize = next(
        step
        for step in repair_job["steps"]
        if step["name"] == "Initialize repair metrics"
    )
    publish = next(
        step for step in repair_job["steps"] if step["name"] == "Publish repair metrics"
    )
    validate_telegram = next(
        step
        for step in repair_job["steps"]
        if step["name"] == "Validate Telegram collection configuration"
    )
    collect = next(
        step
        for step in repair_job["steps"]
        if step["name"] == "Backfill and reconcile Telegram history"
    )
    finalize = next(
        step
        for step in repair_job["steps"]
        if step["name"] == "Finalize Telegram signal rebuild"
    )
    assert initialize["env"]["CURATOR_RUN_METRICS_PATH"] == (
        "${{ runner.temp }}/telegram-repair-metrics.json"
    )
    assert '{"ok":false,"status":"started"}' in initialize["run"]
    assert publish["if"] == "always()"
    assert publish["with"]["if-no-files-found"] == "error"
    assert validate_telegram["if"] == "${{ !inputs.finalize_signal_rebuild }}"
    assert collect["if"] == "${{ !inputs.finalize_signal_rebuild }}"
    assert finalize["if"] == "${{ inputs.finalize_signal_rebuild }}"
    assert "TELEGRAM_API_ID" in collect["env"]
    assert "TELEGRAM_API_HASH" in collect["env"]
    assert "TELEGRAM_SESSION_STRING" in collect["env"]
    assert "TELEGRAM_API_ID" not in finalize["env"]
    assert "TELEGRAM_API_HASH" not in finalize["env"]
    assert "TELEGRAM_SESSION_STRING" not in finalize["env"]
    assert "--finalize-signal-rebuild" in finalize["run"]


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

    if workflow_name == "ingest-official.yml":
        assert "github.ref_type == 'branch'" in job["if"]
        assert (
            "github.ref == format('refs/heads/{0}', "
            "github.event.repository.default_branch)"
        ) in job["if"]
    else:
        assert "github.ref_name == github.event.repository.default_branch" in job["if"]
    checkout = next(step for step in job["steps"] if step["name"] == "Checkout")
    assert checkout["with"]["ref"] == "${{ github.sha }}"
    if workflow_name != "ingest-official.yml":
        guard = next(
            step for step in job["steps"] if step["name"] == "Verify immutable workflow revision"
        )
        assert "git rev-parse HEAD" in guard["run"]
        assert '"$actual" == "$GITHUB_SHA"' in guard["run"]


def test_every_workflow_step_exposing_telegram_delivery_secrets_is_opted_in() -> None:
    delivery_secrets = (
        "${{ secrets.TELEGRAM_BOT_TOKEN }}",
        "${{ secrets.TELEGRAM_CHAT_ID }}",
    )
    exposed_steps: list[str] = []

    for path in sorted(WORKFLOWS.glob("*.yml")):
        payload = yaml.load(path.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
        for job_name, job in payload["jobs"].items():
            for step in job.get("steps", []):
                rendered_step = yaml.safe_dump(step, allow_unicode=True)
                if not any(secret in rendered_step for secret in delivery_secrets):
                    continue

                exposed_steps.append(
                    f"{path.name}:{job_name}:{step.get('name', '<unnamed>')}"
                )
    assert exposed_steps == []


def test_daily_generation_uses_requested_kst_boundary_and_has_no_delivery_job() -> None:
    workflow = workflow_text("daily.yml")
    assert 'cron: "45 20 * * *"' in workflow
    assert 'cron: "5 21 * * *"' not in workflow
    assert 'CURATOR_DAILY_REPORT_WRITE_ONLY: "1"' in workflow
    assert "CURATOR_REQUIRE_NONEMPTY_DAILY_REPORT" not in workflow
    assert "daily_report_queued=1" not in workflow
    assert "CURATOR_DELIVERY_MODE: outbox-enqueue" not in workflow
    assert "TELEGRAM_BOT_TOKEN" not in workflow
    assert "TELEGRAM_CHAT_ID" not in workflow
    assert "curator.story_review send" not in workflow
    assert "TELEGRAM_ADMIN_CHAT_ID" not in workflow
    assert "send_telegram_admin_access" not in workflow
    assert "curator.telegram_dashboard send-access" not in workflow
    assert "Build token-gated Telegram admin shell" not in workflow
    assert "TELEGRAM_ADMIN_ACCESS_TOKEN" not in workflow
    assert "python -m curator.telegram_dashboard write" not in workflow
    assert "python -m curator.governance_site" in workflow
    assert (
        "BSIDE_PUBLIC_WEB_URL: ${{ vars.BSIDE_PUBLIC_WEB_URL || "
        "'https://news.bside.ai' }}"
    ) in workflow
    assert "--output governance-pages-artifact" in workflow
    assert '--legacy-root "$LEGACY_COMPATIBILITY_ROOT"' in workflow
    assert "python -m curator.legacy_recovery_bundle prepare" in workflow
    assert "python -m curator.legacy_recovery_bundle verify" in workflow
    assert "resolve-legacy-recovery.cjs" in workflow
    assert "LEGACY_ROLLBACK_RUN_ID" in workflow
    assert "LEGACY_ROLLBACK_ARTIFACT_NAME" in workflow
    assert "LEGACY_ROLLBACK_CODE_REVISION" in workflow
    assert "LEGACY_ROLLBACK_ARTIFACT_DIGEST" in workflow
    assert "/actions/artifacts/$LEGACY_ARTIFACT_ID/zip" in workflow
    assert "--proto '=https'" in workflow
    assert "path: governance-pages-artifact" in workflow
    assert ".governance-pages" not in workflow
    assert ".deployment-marker" not in workflow
    assert "actions/upload-pages-artifact@fc324d3547104276b827a68afc52ff2a11cc49c9" in workflow
    assert "actions/deploy-pages@cd2ce8fcbc39b97be8ca5fce6e763baed58fa128" in workflow
    assert "PAGES_OWNER" in workflow
    assert "python -m curator.operation_mode" in workflow
    assert (
        "governance-pages-ready-${{ steps.deployment_marker.outputs.kst_date }}"
        in workflow
    )
    payload = yaml.load(workflow, Loader=yaml.BaseLoader)
    assert set(payload["jobs"]) == {"generate"}


def test_daily_legacy_recovery_is_digest_pinned_and_rolled_only_from_trusted_runs() -> None:
    payload = yaml.load(workflow_text("daily.yml"), Loader=yaml.BaseLoader)
    steps = payload["jobs"]["generate"]["steps"]
    resolver = next(
        step
        for step in steps
        if step["name"] == "Resolve rolling legacy recovery source"
    )
    assert resolver["uses"] == "actions/github-script@3a2844b7e9c422d3c10d287c895573f7108da1b3"
    assert resolver["env"] == {
        "LEGACY_RUN_ID": "${{ vars.LEGACY_ROLLBACK_RUN_ID }}",
        "LEGACY_ARTIFACT_NAME": "${{ vars.LEGACY_ROLLBACK_ARTIFACT_NAME }}",
        "LEGACY_CODE_REVISION": "${{ vars.LEGACY_ROLLBACK_CODE_REVISION }}",
        "LEGACY_ARTIFACT_DIGEST": "${{ vars.LEGACY_ROLLBACK_ARTIFACT_DIGEST }}",
        "DEFAULT_BRANCH": "${{ github.event.repository.default_branch }}",
    }
    resolver_script = resolver["with"]["script"]
    assert "resolve-legacy-recovery.cjs" in resolver_script
    module = (ROOT / ".github" / "scripts" / "resolve-legacy-recovery.cjs").read_text(
        encoding="utf-8"
    )
    assert 'run.conclusion !== "success"' in module
    assert "run.head_branch === defaultBranch" in module
    assert '".github/workflows/build-feed.yml"' in module
    assert "run.head_sha || \"\"" in module
    assert "pin.artifactDigest" in module
    assert "matches.length !== 1" in module
    assert "!item.expired" in module
    assert '".github/workflows/daily.yml"' in module
    assert '".github/workflows/governance-cutover.yml"' in module

    download = next(
        step
        for step in steps
        if step["name"] == "Download immutable legacy seed archive"
    )
    assert "actions/artifacts/$LEGACY_ARTIFACT_ID/zip" in download["run"]
    assert "--proto '=https'" in download["run"]
    assert download["env"]["GH_TOKEN"] == "${{ github.token }}"

    prepare = next(
        step
        for step in steps
        if step["name"] == "Prepare or verify rolling legacy recovery bundle"
    )
    assert "python -m curator.legacy_recovery_bundle prepare" in prepare["run"]
    assert "python -m curator.legacy_recovery_bundle verify" in prepare["run"]
    assert "--source-artifact-digest \"$LEGACY_ARTIFACT_DIGEST\"" in prepare["run"]
    assert prepare["env"]["LEGACY_RUN_ID"] == (
        "${{ steps.legacy_recovery.outputs.pin_run_id }}"
    )
    assert prepare["env"]["LEGACY_ARTIFACT_DIGEST"] == (
        "${{ steps.legacy_recovery.outputs.pin_artifact_digest }}"
    )
    carry_download = next(
        step for step in steps if step["name"] == "Download rolling legacy recovery bundle"
    )
    assert carry_download["with"]["digest-mismatch"] == "error"
    carry_upload = next(
        step
        for step in steps
        if step["name"] == "Refresh verified legacy recovery bundle retention"
    )
    assert carry_upload["with"]["name"] == (
        "${{ steps.legacy_recovery.outputs.carry_artifact_name }}"
    )
    assert carry_upload["with"]["retention-days"] == "90"


def test_workflows_do_not_send_private_admin_messages_or_token_links() -> None:
    legacy = workflow_text("build-feed.yml")
    daily = workflow_text("daily.yml")
    for workflow in (legacy, daily):
        assert "TELEGRAM_ADMIN_CHAT_ID" not in workflow
        assert "send_telegram_admin_access" not in workflow
        assert "curator.telegram_dashboard send-access" not in workflow
        assert "curator.story_review send" not in workflow
    assert "Build token-gated Telegram admin shell" in legacy
    assert "python -m curator.telegram_dashboard write" in legacy
    assert "Build token-gated Telegram admin shell" not in daily
    assert "python -m curator.telegram_dashboard write" not in daily


def test_workflow_permissions_are_scoped_to_the_jobs_that_need_them() -> None:
    daily = yaml.load(workflow_text("daily.yml"), Loader=yaml.BaseLoader)
    official = yaml.load(workflow_text("ingest-official.yml"), Loader=yaml.BaseLoader)
    build_feed = yaml.load(workflow_text("build-feed.yml"), Loader=yaml.BaseLoader)
    assert daily["permissions"] == {"contents": "read", "models": "read"}
    assert daily["jobs"]["generate"]["permissions"] == {
        "actions": "read",
        "contents": "read",
        "models": "read",
        "pages": "write",
        "id-token": "write",
    }
    assert "send" not in daily["jobs"]
    assert official["permissions"] == {"contents": "read", "actions": "read"}
    assert build_feed["permissions"] == {
        "actions": "read",
        "contents": "read",
        "models": "read",
        "pages": "write",
        "id-token": "write",
    }
    boundary = next(
        step
        for step in build_feed["jobs"]["build-feed"]["steps"]
        if step["name"]
        == "Enforce job-start deployment snapshots at the Pages boundary"
    )
    assert "github.rest.actions.getRepoVariable" not in boundary["run"]
    assert "EXPEDITED_OBSERVATION_SNAPSHOT" in boundary["run"]
    assert "deploy_allowed=true" in boundary["run"]
    assert "deploy_allowed=false" in boundary["run"]
    assert "BSIDE_ADMIN_TOKEN" not in boundary.get("env", {})


def test_daily_governance_pages_deploy_only_after_authenticated_live_state() -> None:
    payload = yaml.load(workflow_text("daily.yml"), Loader=yaml.BaseLoader)
    steps = payload["jobs"]["generate"]["steps"]
    eligibility = next(
        step for step in steps if step["name"] == "Determine Pages deployment eligibility"
    )
    assert eligibility["env"]["BSIDE_OPS_TOKEN"] == "${{ secrets.BSIDE_OPS_TOKEN }}"
    assert "BSIDE_ADMIN_TOKEN" not in eligibility["env"]
    assert eligibility["env"]["GOVERNANCE_PIPELINE_MODE"] == (
        "${{ steps.rollout.outputs.governance_pipeline_mode }}"
    )
    assert "/ops/release-state" in eligibility["run"]
    assert ".data.release_state" in eligibility["run"]
    assert '[[ "$release_state" == "live" ]]' in eligibility["run"]
    assert '"$GOVERNANCE_PIPELINE_MODE" == "live"' in eligibility["run"]
    assert "deploy_pages=false" in eligibility["run"]
    boundary = next(
        step
        for step in steps
        if step["name"] == "Revalidate live governance ownership at the deployment boundary"
    )
    assert boundary["env"]["GOVERNANCE_PIPELINE_MODE_SNAPSHOT"] == (
        "${{ steps.rollout.outputs.governance_pipeline_mode }}"
    )
    assert boundary["env"]["BSIDE_OPS_TOKEN"] == "${{ secrets.BSIDE_OPS_TOKEN }}"
    assert "BSIDE_ADMIN_TOKEN" not in boundary["env"]
    assert "/ops/release-state" in boundary["run"]
    assert '"$GOVERNANCE_PIPELINE_MODE_SNAPSHOT" == "live"' in boundary["run"]
    rollback_artifact = next(step for step in steps if step["name"] == "Preserve rollback artifact")
    assert rollback_artifact["with"]["retention-days"] == "90"


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
        step for step in steps if step.get("uses") == "actions/upload-pages-artifact@fc324d3547104276b827a68afc52ff2a11cc49c9"
    ]
    deployments = [
        step for step in steps if step.get("uses") == "actions/deploy-pages@cd2ce8fcbc39b97be8ca5fce6e763baed58fa128"
    ]
    configuration = next(
        step for step in steps if step.get("uses") == "actions/configure-pages@45bfe0192ca1faeb007ade9deae92b16b8254a0d"
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
    assert "deployment_attempted=false" in verifier["run"]
    assert (
        "deployment was not attempted because an upstream step failed"
        in verifier["run"]
    )
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
        assert metrics["uses"] == "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
        assert metrics["with"]["if-no-files-found"] == "error"
        assert (
            steps.index(build_feed) < steps.index(verify_metrics) < steps.index(metrics)
        )
        daily_report = next(
            step for step in steps if step["name"] == "Build daily report page"
        )
        assert daily_report["env"]["CURATOR_RUN_METRICS_PATH"] == (
            "${{ runner.temp }}/curator-run-metrics.json"
        )
        assert steps.index(verify_metrics) < steps.index(daily_report)
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


def test_build_feed_test_only_paths_do_not_trigger_page_run() -> None:
    workflow = workflow_text("build-feed.yml")
    page_regex_match = re.search(
        r"page_regex='(?P<regex>[^']+)'",
        workflow,
    )

    assert page_regex_match is not None
    page_regex = page_regex_match.group("regex")
    assert "tests/" not in page_regex
    assert "curator/(daily_report|governance_ui|story_review)\\.py" in page_regex


def test_build_feed_page_only_work_is_promoted_before_publication() -> None:
    workflow = workflow_text("build-feed.yml")
    promotion = (
        'if [[ "$page_run" == "true" && "$full_run" != "true" '
        '&& "$EVENT_NAME" == "push" '
        '&& "$COMMIT_MESSAGE" != *"[page-only]"* ]]; then'
    )
    promotion_index = workflow.index(promotion)
    deployment_index = workflow.index("          deploy_pages=false")

    assert promotion_index < deployment_index
    promoted_block = workflow[promotion_index:deployment_index]
    assert "full_run=true" in promoted_block
    assert "automatic page-code push" in promoted_block
    assert "publication metrics are produced in the same job" in promoted_block


def test_build_feed_deployable_page_only_is_promoted_to_verified_full_run() -> None:
    workflow = workflow_text("build-feed.yml")
    dispatch_block = workflow[
        workflow.index('elif [[ "$EVENT_NAME" == "workflow_dispatch" ]]'):
        workflow.index('elif [[ "$EVENT_NAME" == "push" ]]')
    ]
    promotion = (
        'if [[ "$page_run" == "true" && "$full_run" != "true" '
        '&& "$REF_NAME" == "$DEFAULT_BRANCH" '
        '&& "$LEGACY_PAGES_ENABLED" == "true" '
        '&& "$GOVERNANCE_PAGES_ENABLED" != "true" '
        '&& "$ALLOW_PAGES_DEPLOY" == "true" ]]; then'
    )
    promotion_index = workflow.index(promotion)
    deployment_index = workflow.index("          deploy_pages=false")

    assert "page_only)" in dispatch_block
    assert "page_run=true" in dispatch_block
    assert promotion_index < deployment_index
    promoted_block = workflow[promotion_index:deployment_index]
    assert "full_run=true" in promoted_block
    assert "deployable page_only run" in promoted_block
    assert "metrics verification" in promoted_block


def test_build_feed_nondeploy_page_only_remains_collection_free() -> None:
    workflow = workflow_text("build-feed.yml")
    payload = yaml.load(workflow, Loader=yaml.BaseLoader)
    steps = payload["jobs"]["build-feed"]["steps"]
    promotion_index = workflow.index(
        'if [[ "$page_run" == "true" && "$full_run" != "true" '
        '&& "$REF_NAME" == "$DEFAULT_BRANCH" '
        '&& "$LEGACY_PAGES_ENABLED" == "true" '
        '&& "$GOVERNANCE_PAGES_ENABLED" != "true" '
        '&& "$ALLOW_PAGES_DEPLOY" == "true" ]]; then'
    )
    deployment_index = workflow.index("          deploy_pages=false")
    promoted_block = workflow[promotion_index:deployment_index]
    deploy_block = workflow[deployment_index:workflow.index(
        '          echo "full=$full_run" >> "$GITHUB_OUTPUT"'
    )]

    assert '"$ALLOW_PAGES_DEPLOY" == "true"' in promoted_block
    assert '"$REF_NAME" == "$DEFAULT_BRANCH"' in promoted_block
    assert '"$ALLOW_PAGES_DEPLOY" == "true"' in deploy_block
    assert '"$full_run" == "true" || "$page_run" == "true"' in deploy_block
    daily_report = next(
        step for step in steps if step["name"] == "Build daily report page"
    )
    admin_shell = next(
        step
        for step in steps
        if step["name"] == "Build token-gated Telegram admin shell"
    )
    assert daily_report["if"] == (
        "${{ steps.run_mode.outputs.full == 'true' }}"
    )
    assert admin_shell["if"] == (
        "${{ steps.run_mode.outputs.full == 'true' }}"
    )


def test_build_feed_metrics_are_verified_before_any_page_publication() -> None:
    payload = yaml.load(workflow_text("build-feed.yml"), Loader=yaml.BaseLoader)
    steps = payload["jobs"]["build-feed"]["steps"]
    initialize = next(
        step for step in steps if step["name"] == "Initialize curator run metrics"
    )
    collect = next(step for step in steps if step["name"] == "Build feed")
    verify = next(
        step for step in steps if step["name"] == "Verify curator run metrics"
    )
    daily_report = next(
        step for step in steps if step["name"] == "Build daily report page"
    )
    pages_upload = next(
        step
        for step in steps
        if step.get("uses")
        == "actions/upload-pages-artifact@fc324d3547104276b827a68afc52ff2a11cc49c9"
    )

    assert (
        steps.index(initialize)
        < steps.index(collect)
        < steps.index(verify)
        < steps.index(daily_report)
        < steps.index(pages_upload)
    )
    assert verify["if"] == "${{ success() && steps.run_mode.outputs.full == 'true' }}"


def test_build_feed_allow_pages_deploy_false_cannot_deploy() -> None:
    workflow = workflow_text("build-feed.yml")
    deploy_block = workflow[
        workflow.index("          deploy_pages=false"):
        workflow.index('          echo "full=$full_run" >> "$GITHUB_OUTPUT"')
    ]

    assert "deploy_pages=false" in deploy_block
    assert '"$ALLOW_PAGES_DEPLOY" == "true"' in deploy_block
    assert deploy_block.index('"$ALLOW_PAGES_DEPLOY" == "true"') < deploy_block.index(
        "deploy_pages=true"
    )


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

    assert resolver["uses"] == "actions/github-script@3a2844b7e9c422d3c10d287c895573f7108da1b3"
    assert 'sourceRun.conclusion !== "success"' in resolver["with"]["script"]
    assert "artifact.expired" in resolver["with"]["script"]
    assert "sourceRunId === Number(context.runId)" in resolver["with"]["script"]
    assert "sourceRun.path !== expectedWorkflowPath" in resolver["with"]["script"]
    assert "sourceRun.head_repository?.full_name" in resolver["with"]["script"]
    assert "core.setFailed" in resolver["with"]["script"]
    assert download["uses"] == "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
    assert download["with"] == {
        "artifact-ids": "${{ steps.previous_legacy_pages.outputs.artifact_id }}",
        "path": "${{ runner.temp }}/previous-legacy-pages",
        "github-token": "${{ github.token }}",
        "repository": "${{ github.repository }}",
        "run-id": "${{ steps.previous_legacy_pages.outputs.run_id }}",
        "merge-multiple": "true",
        "digest-mismatch": "error",
    }
    assert seed["uses"] == "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
    assert seed["with"]["name"] == "legacy-pages-archive-seed"
    assert seed["with"]["path"] == "${{ steps.legacy_pages_artifact.outputs.path }}"
    assert seed["with"]["retention-days"] == "90"
    assert "preserve_legacy_archive_seed" in seed["if"]


def test_pages_deployment_is_default_branch_only() -> None:
    legacy = workflow_text("build-feed.yml")
    daily = workflow_text("daily.yml")
    assert "REF_NAME: ${{ github.ref_name }}" in legacy
    assert "DEFAULT_BRANCH: ${{ github.event.repository.default_branch }}" in legacy
    assert '"$REF_NAME" == "$DEFAULT_BRANCH"' in legacy
    assert "Determine Pages deployment eligibility" in daily
    assert '"$REF_NAME" == "$DEFAULT_BRANCH"' in daily
    assert "REF_NAME: ${{ github.ref_name }}" in daily
    assert "DEFAULT_BRANCH: ${{ github.event.repository.default_branch }}" in daily


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
        "Daily governance pages",
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
    assert payload["concurrency"] == PRODUCTION_OFFICIAL_WRITE_CONCURRENCY


def test_official_backfill_is_bounded_serialized_and_preserves_evidence() -> None:
    workflow = workflow_text("official-backfill.yml")
    payload = yaml.load(workflow, Loader=yaml.BaseLoader)
    dispatch = payload["on"]["workflow_dispatch"]["inputs"]
    assert set(dispatch) == {
        "mode",
        "frozen_apply_run_id",
        "source",
        "from_date",
        "to_date",
        "max_windows",
        "sync_company_master",
        "defer_review_sample",
        "canary_lookback_days",
        "canary_request_budget",
    }
    assert dispatch["mode"]["options"] == ["dry-run", "apply", "replay"]
    assert dispatch["frozen_apply_run_id"]["default"] == ""
    assert dispatch["source"]["options"] == ["dart", "kind", "both"]
    assert dispatch["defer_review_sample"]["default"] == "false"
    assert dispatch["defer_review_sample"]["required"] == "true"
    assert dispatch["defer_review_sample"]["type"] == "boolean"
    assert dispatch["canary_lookback_days"]["default"] == "365"
    assert dispatch["canary_request_budget"]["default"] == "10000"
    input_validation = next(
        step
        for step in payload["jobs"]["backfill"]["steps"]
        if step["name"] == "Validate bounded backfill inputs"
    )
    assert "completed_kst_end_exclusive" in input_validation["run"]
    assert "end > completed_kst_end_exclusive" in input_validation["run"]
    assert "tomorrow_kst" not in input_validation["run"]
    assert "timedelta(days=1)" not in input_validation["run"]
    assert "canary_lookback_days must be between 2 and 365" in input_validation["run"]
    assert "canary_request_budget must be between 1 and 10000" in input_validation["run"]
    assert "replay requires source=dart" in input_validation["run"]
    assert "replay requires one exact 30-day range" in input_validation["run"]
    assert "replay requires max_windows=30" in input_validation["run"]
    assert "replay cannot sync the DART company master" in input_validation["run"]
    assert "replay requires a positive frozen_apply_run_id" in input_validation["run"]
    assert "frozen_apply_run_id is forbidden outside replay mode" in input_validation["run"]
    assert input_validation["env"]["DEFER_REVIEW_SAMPLE"] == (
        "${{ inputs.defer_review_sample }}"
    )
    assert "defer_review_sample must be true or false" in input_validation["run"]
    assert 'os.environ["MODE"] != "apply"' in input_validation["run"]
    assert 'os.environ["SOURCE"] != "dart"' in input_validation["run"]
    assert "(end - start).days != 30" in input_validation["run"]
    assert "max_windows != 30" in input_validation["run"]
    assert 'os.environ["SYNC_COMPANY_MASTER"].lower() != "false"' in (
        input_validation["run"]
    )
    assert (
        "defer_review_sample requires mode=apply, source=dart, one exact"
        in input_validation["run"]
    )
    assert (
        "30-day range, max_windows=30, and sync_company_master=false"
        in input_validation["run"]
    )
    assert payload["concurrency"] == PRODUCTION_OFFICIAL_WRITE_CONCURRENCY
    assert payload["permissions"] == {"contents": "read", "actions": "read"}

    job = payload["jobs"]["backfill"]
    assert job["if"] == (
        "vars.DART_OFFICIAL_INGEST_ENABLED == 'true' && "
        "github.ref_type == 'branch' && "
        "github.ref == format('refs/heads/{0}', "
        "github.event.repository.default_branch)"
    )
    assert int(job["timeout-minutes"]) == 360
    for runtime_path in (
        "BACKFILL_REPORT",
        "BACKFILL_LOG",
        "DART_CANARY_REPORT",
    ):
        assert runtime_path not in job["env"]
    steps = job["steps"]
    initialize = next(step for step in steps if step["name"] == "Initialize backfill evidence")
    assert initialize["env"] == {
        "BACKFILL_REPORT": "${{ runner.temp }}/official-backfill-report.json",
        "DART_CANARY_REPORT": "${{ runner.temp }}/dart-canary-sample-report.json",
        "DART_REVIEW_SAMPLE_JSONL": "${{ runner.temp }}/dart-review-sample.jsonl",
        "DART_REVIEW_SAMPLE_CSV": "${{ runner.temp }}/dart-review-sample.csv",
        "DART_REVIEW_SAMPLE_MANIFEST": (
            "${{ runner.temp }}/dart-review-sample-manifest.json"
        ),
        "DART_REPLAY_STATE_BEFORE": (
            "${{ runner.temp }}/dart-replay-state-before.json"
        ),
        "DART_REPLAY_STATE_AFTER": (
            "${{ runner.temp }}/dart-replay-state-after.json"
        ),
        "DART_REPLAY_STATE_BINDING": (
            "${{ runner.temp }}/dart-replay-state-binding.json"
        ),
        "DART_FROZEN_APPLY_ARTIFACT_BINDING": (
            "${{ runner.temp }}/dart-frozen-apply-artifact-binding.json"
        ),
        "DART_FROZEN_REPLAY_ARTIFACT_BINDING": (
            "${{ runner.temp }}/dart-frozen-replay-artifact-binding.json"
        ),
        "DART_DRIFT_PROBE_REPORT": (
            "${{ runner.temp }}/dart-replay-drift-probe.json"
        ),
    }
    checkout = next(step for step in steps if step["name"] == "Checkout immutable dispatch revision")
    assert checkout["with"]["ref"] == "${{ github.sha }}"
    revision_guard = next(step for step in steps if step["name"] == "Verify immutable dispatch revision")
    assert "git rev-parse HEAD" in revision_guard["run"]
    assert '"$actual" == "$GITHUB_SHA"' in revision_guard["run"]
    run_step = next(step for step in steps if step["name"] == "Run one-day official backfill windows")
    assert "--chunk-days 1" in run_step["run"]
    assert '--max-chunks "$BACKFILL_MAX_WINDOWS"' in run_step["run"]
    assert '${{ inputs.max_windows }}' not in run_step["run"]
    assert run_step["env"]["BACKFILL_MAX_WINDOWS"] == "${{ inputs.max_windows }}"
    assert '--request-budget "$DART_REQUEST_BUDGET"' in run_step["run"]
    assert run_step["env"]["DART_REQUEST_BUDGET"] == (
        "${{ steps.dart_budget.outputs.remaining_request_budget }}"
    )
    assert '[[ "$DART_REQUEST_BUDGET" =~ ^[0-9]+$ ]]' in run_step["run"]
    assert "DART_REQUEST_BUDGET < 1 || DART_REQUEST_BUDGET > 10000" in run_step["run"]
    assert run_step["env"]["CURATOR_DISABLE_TELEGRAM_SEND"] == "1"
    assert run_step["env"]["CURATOR_DELIVERY_MODE"] == "disabled"
    assert run_step["env"]["ENABLE_TELEGRAM_DELIVERY"] == "false"
    assert run_step["env"]["ENABLE_GOVERNANCE_DELIVERY"] == "false"
    assert run_step["env"]["BSIDE_OPS_TOKEN"] == "${{ secrets.BSIDE_OPS_TOKEN }}"
    assert run_step["env"]["BSIDE_BACKEND_BINDING_ID"] == (
        "${{ vars.BSIDE_BACKEND_BINDING_ID }}"
    )
    assert run_step["env"]["BSIDE_API_BASE_URL"] == (
        "${{ secrets.BSIDE_API_BASE_URL || vars.GOVERNANCE_API_BASE_URL }}"
    )
    assert run_step["env"]["CURATOR_REQUIRE_DURABLE_DART_QUOTA"] == (
        "${{ inputs.mode != 'replay' && '1' || '0' }}"
    )
    assert run_step["env"]["CURATOR_DART_QUOTA_PHASE"] == (
        "${{ inputs.mode == 'replay' && "
        "'official-backfill-replay' || 'official-backfill' }}"
    )
    assert run_step["env"]["CURATOR_REQUIRE_REMOTE_API"] == (
        "${{ inputs.mode != 'dry-run' && '1' || '0' }}"
    )
    assert "--replay" in run_step["run"]
    assert "--frozen-bundle-dir" in run_step["run"]
    assert "--frozen-artifact-binding" in run_step["run"]
    assert run_step["env"]["OPENDART_API_KEYS"] == (
        "${{ inputs.mode != 'replay' && secrets.OPENDART_API_KEYS || '' }}"
    )
    assert run_step["env"]["DART_API_KEY"] == (
        "${{ inputs.mode != 'replay' && secrets.OPENDART_API_KEYS == '' && "
        "secrets.DART_API_KEY || '' }}"
    )
    validation = next(step for step in steps if step["name"] == "Validate operational configuration")
    assert "BSIDE_OPS_TOKEN" in validation["run"]
    assert "BSIDE_BACKEND_BINDING_ID" in validation["run"]
    assert "KIND_DISCLOSURE_ENDPOINT BSIDE_API_BASE_URL BSIDE_OPS_TOKEN" in validation["run"]
    assert "python .github/scripts/validate-api-base-urls.py" in validation["run"]
    assert "mask-opendart-credentials.py" in validation["run"]
    assert "required+=(DART_API_KEY" not in validation["run"]
    assert validation["env"]["OPENDART_API_KEYS"] == (
        "${{ inputs.mode != 'replay' && secrets.OPENDART_API_KEYS || '' }}"
    )
    assert validation["env"]["GOVERNANCE_API_BASE_URL"] == (
        "${{ vars.GOVERNANCE_API_BASE_URL }}"
    )
    canary = next(
        step
        for step in steps
        if step["name"] == "Dry-run completed-day and revision DART canary"
    )
    assert canary["if"] == "inputs.mode == 'dry-run' && inputs.source != 'kind'"
    assert "python -m curator.dart_canary_sample" in canary["run"]
    assert '--lookback-days "$CANARY_LOOKBACK_DAYS"' in canary["run"]
    assert '--request-budget "$CANARY_REQUEST_BUDGET"' in canary["run"]
    assert canary["env"]["CANARY_LOOKBACK_DAYS"] == (
        "${{ inputs.canary_lookback_days }}"
    )
    assert canary["env"]["CANARY_REQUEST_BUDGET"] == (
        "${{ inputs.canary_request_budget }}"
    )
    assert 'report.get("request_budget") != requested_budget' in canary["run"]
    assert 'history.get("eligible_days") != requested_lookback' in canary["run"]
    assert "type(used) is not int" in canary["run"]
    assert 'output.write(f"requests_used={used}\\n")' in canary["run"]
    assert canary["env"]["OPENDART_API_KEYS"] == "${{ secrets.OPENDART_API_KEYS }}"
    assert canary["env"]["DART_API_KEY"] == (
        "${{ secrets.OPENDART_API_KEYS == '' && secrets.DART_API_KEY || '' }}"
    )
    assert canary["env"]["BSIDE_OPS_TOKEN"] == "${{ secrets.BSIDE_OPS_TOKEN }}"
    assert canary["env"]["BSIDE_BACKEND_BINDING_ID"] == (
        "${{ vars.BSIDE_BACKEND_BINDING_ID }}"
    )
    assert canary["env"]["DART_CANARY_REPORT"] == (
        "${{ runner.temp }}/dart-canary-sample-report.json"
    )
    assert canary["env"]["CURATOR_REQUIRE_DURABLE_DART_QUOTA"] == "1"


    assert canary["env"]["CURATOR_DART_QUOTA_PHASE"] == "dart-canary"
    dart_budget = next(
        step
        for step in steps
        if step["name"] == "Resolve shared DART invocation budget"
    )
    assert dart_budget["id"] == "dart_budget"
    assert dart_budget["env"] == {
        "BACKFILL_MODE": "${{ inputs.mode }}",
        "BACKFILL_SOURCE": "${{ inputs.source }}",
        "CANARY_REQUESTS_USED": "${{ steps.dart_canary.outputs.requests_used }}",
        "CANARY_REQUEST_BUDGET": "${{ inputs.canary_request_budget }}",
    }
    assert 'raw_used.isascii()' in dart_budget["run"]
    assert 'raw_used.isdecimal()' in dart_budget["run"]
    assert "remaining = max(0, 10_000 - used)" in dart_budget["run"]
    assert "used > canary_budget" in dart_budget["run"]
    assert 'output.write(f"canary_requests_used={used}\\n")' in dart_budget["run"]
    assert 'output.write(f"remaining_request_budget={remaining}\\n")' in dart_budget["run"]
    assert "remaining == 0" in dart_budget["run"]
    replay_before = next(
        step
        for step in steps
        if step["name"] == "Capture production DART state before replay"
    )
    replay_after = next(
        step
        for step in steps
        if step["name"] == "Capture production DART state after replay"
    )
    replay_binding = next(
        step
        for step in steps
        if step["name"] == "Bind replay receipts to unchanged production state"
    )
    receipt_contract = next(
        step
        for step in steps
        if step["name"] == "Validate exact 30-window DART receipt contract"
    )
    assert replay_before["if"] == "inputs.mode == 'replay'"
    assert replay_after["if"] == "always() && inputs.mode == 'replay'"
    assert replay_binding["if"] == "inputs.mode == 'replay'"
    assert (
        receipt_contract["if"]
        == "inputs.mode != 'dry-run' && inputs.source == 'dart' && inputs.max_windows == 30"
    )
    assert "/ops/alpha-replay-state?code_revision=${GITHUB_SHA}" in replay_before["run"]
    assert "/ops/alpha-replay-state?code_revision=${GITHUB_SHA}" in replay_after["run"]
    assert "before_contract != after_contract" in replay_binding["run"]
    assert 'report.get("windows_attempted") != 30' in replay_binding["run"]
    assert 'report.get("replay_verified") is not True' in replay_binding["run"]
    assert "apply_summary_counts_sha256" in replay_binding["run"]
    assert "receipt_contract_sha256" in replay_binding["run"]
    assert "len(windows) != 30" in receipt_contract["run"]
    assert steps.index(replay_before) < steps.index(run_step) < steps.index(replay_after)
    assert steps.index(replay_after) < steps.index(replay_binding)
    frozen_resolver = next(
        step for step in steps if step["name"] == "Resolve exact immutable DART apply bundle"
    )
    assert frozen_resolver["if"] == "inputs.mode == 'replay'"
    assert "getWorkflowRun" in frozen_resolver["with"]["script"]
    assert "FROZEN_APPLY_RUN_ID" in frozen_resolver["env"]
    assert "matches.length !== 1" in frozen_resolver["with"]["script"]
    assert "72 * 60 * 60 * 1000" in frozen_resolver["with"]["script"]
    assert "run.head_sha !== context.sha" in frozen_resolver["with"]["script"]
    drift_probe = next(
        step for step in steps if step["name"] == "Run fresh read-only DART drift probe"
    )
    assert drift_probe["env"]["OPENDART_API_KEYS"] == "${{ secrets.OPENDART_API_KEYS }}"
    assert drift_probe["env"]["CURATOR_DART_QUOTA_PHASE"] == (
        "official-backfill-drift-probe"
    )
    assert "--drift-probe-only" in drift_probe["run"]
    assert "dart-drift-probe.stderr.raw" in drift_probe["run"]
    assert "raw connector output was discarded" in drift_probe["run"]
    review_sample = next(
        step
        for step in steps
        if step["name"] == "Build deterministic 30-day DART review sample"
    )
    deferred_review_sample = next(
        step
        for step in steps
        if step["name"] == "Defer deterministic 30-day DART review sample"
    )
    assert deferred_review_sample["if"] == "inputs.defer_review_sample"
    assert deferred_review_sample["env"] == {
        "DART_REVIEW_SAMPLE_JSONL": (
            "${{ runner.temp }}/dart-review-sample.jsonl"
        ),
        "DART_REVIEW_SAMPLE_CSV": "${{ runner.temp }}/dart-review-sample.csv",
        "DART_REVIEW_SAMPLE_MANIFEST": (
            "${{ runner.temp }}/dart-review-sample-manifest.json"
        ),
        "EXPECTED_FROM_DATE": "${{ inputs.from_date }}",
        "EXPECTED_TO_DATE": "${{ inputs.to_date }}",
    }
    assert ': > "$DART_REVIEW_SAMPLE_JSONL"' in deferred_review_sample["run"]
    assert ': > "$DART_REVIEW_SAMPLE_CSV"' in deferred_review_sample["run"]
    assert '"status": "deferred"' in deferred_review_sample["run"]
    assert '"release_eligible": False' in deferred_review_sample["run"]
    assert '"reason": "operator_requested_review_sample_deferral"' in (
        deferred_review_sample["run"]
    )
    assert '"code_revision": os.environ["GITHUB_SHA"]' in (
        deferred_review_sample["run"]
    )
    assert '"run_id": int(os.environ["GITHUB_RUN_ID"])' in (
        deferred_review_sample["run"]
    )
    assert '"run_attempt": int(os.environ["GITHUB_RUN_ATTEMPT"])' in (
        deferred_review_sample["run"]
    )
    assert '"from": os.environ["EXPECTED_FROM_DATE"]' in (
        deferred_review_sample["run"]
    )
    assert '"to": os.environ["EXPECTED_TO_DATE"]' in (
        deferred_review_sample["run"]
    )
    assert "python -m curator.dart_review_sample" not in deferred_review_sample["run"]
    assert review_sample["if"] == (
        "inputs.mode == 'apply' && "
        "inputs.source != 'kind' && "
        "!inputs.defer_review_sample"
    )
    assert "python -m curator.dart_review_sample" in review_sample["run"]
    assert "report.get(\"windows_total\") == 30" in review_sample["run"]
    assert "report.get(\"windows_remaining\") == 0" in review_sample["run"]
    assert "--sample-size 100" in review_sample["run"]
    assert "--backfill-report \"$BACKFILL_REPORT\"" in review_sample["run"]
    assert "--checkpoint \"$BACKFILL_CHECKPOINT\"" in review_sample["run"]
    assert "--code-revision \"$GITHUB_SHA\"" in review_sample["run"]
    assert "python .github/scripts/validate-api-base-urls.py" in review_sample["run"]
    assert review_sample["env"]["BSIDE_API_BASE_URL"] == (
        "${{ secrets.BSIDE_API_BASE_URL || vars.GOVERNANCE_API_BASE_URL }}"
    )
    assert review_sample["env"]["GOVERNANCE_API_BASE_URL"] == (
        "${{ vars.GOVERNANCE_API_BASE_URL }}"
    )
    assert review_sample["env"]["BSIDE_OPS_TOKEN"] == "${{ secrets.BSIDE_OPS_TOKEN }}"
    assert review_sample["env"]["BSIDE_BACKEND_BINDING_ID"] == (
        "${{ vars.BSIDE_BACKEND_BINDING_ID }}"
    )
    assert "TELEGRAM_BOT_TOKEN" not in workflow
    assert "TELEGRAM_CHAT_ID" not in workflow

    uploads = [step for step in steps if step.get("uses") == "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"]
    assert {step["name"] for step in uploads} == {
        "Preserve immutable private frozen DART apply bundle",
        "Preserve backfill report",
        "Preserve resumable checkpoint",
        "Preserve checkpoint diagnostic",
    }
    report_upload = next(step for step in uploads if step["name"] == "Preserve backfill report")
    checkpoint_upload = next(
        step for step in uploads if step["name"] == "Preserve resumable checkpoint"
    )
    assert report_upload["if"] == (
        "always() && "
        "steps.backfill_artifact_boundary.outputs.evidence_safe == 'true'"
    )
    assert checkpoint_upload["if"] == (
        "always() && "
        "steps.checkpoint_evidence.outputs.upload_checkpoint == 'true'"
    )
    assert all(step["with"]["if-no-files-found"] == "error" for step in uploads)
    assert report_upload["with"]["path"].splitlines() == [
        "${{ runner.temp }}/official-backfill-report.json",
        "${{ runner.temp }}/dart-canary-sample-report.json",
        "${{ runner.temp }}/dart-review-sample.jsonl",
        "${{ runner.temp }}/dart-review-sample.csv",
        "${{ runner.temp }}/dart-review-sample-manifest.json",
        "${{ runner.temp }}/dart-replay-state-before.json",
        "${{ runner.temp }}/dart-replay-state-after.json",
        "${{ runner.temp }}/dart-replay-state-binding.json",
        "${{ runner.temp }}/dart-frozen-apply-artifact-binding.json",
        "${{ runner.temp }}/dart-frozen-replay-artifact-binding.json",
        "${{ runner.temp }}/dart-replay-drift-probe.json",
    ]
    frozen_upload = next(
        step
        for step in uploads
        if step["name"] == "Preserve immutable private frozen DART apply bundle"
    )
    assert frozen_upload["with"]["retention-days"] == "90"
    assert frozen_upload["with"]["name"] == (
        "${{ env.DART_FROZEN_APPLY_ARTIFACT_NAME }}"
    )
    checkpoint = next(step for step in uploads if step["name"] == "Preserve resumable checkpoint")
    assert checkpoint["with"]["name"] == "${{ env.CHECKPOINT_ARTIFACT_NAME }}"
    diagnostic = next(
        step
        for step in uploads
        if step["name"] == "Preserve checkpoint diagnostic"
    )
    assert diagnostic["if"] == "always()"
    assert diagnostic["with"]["name"] == (
        "official-backfill-checkpoint-diagnostic-"
        "${{ github.run_id }}-${{ github.run_attempt }}"
    )
    assert diagnostic["with"]["retention-days"] == "30"
    prepare_checkpoint = next(
        step for step in steps if step["name"] == "Prepare checkpoint evidence"
    )
    assert prepare_checkpoint["id"] == "checkpoint_evidence"
    assert 'if [[ "$MODE" == "apply"' in prepare_checkpoint["run"]
    assert "if (( completed_count > 0 ))" in prepare_checkpoint["run"]
    assert "steps.frozen_resume_boundary.outputs.evidence_safe" in (
        prepare_checkpoint["run"]
    )
    assert 'echo "resumable=$resumable"' in prepare_checkpoint["run"]
    assert 'echo "upload_checkpoint=$upload_checkpoint"' in (
        prepare_checkpoint["run"]
    )
    assert (
        'elif [[ "$MODE" == "replay" && '
        '"$checkpoint_present" == "true" ]]'
        in prepare_checkpoint["run"]
    )
    assert prepare_checkpoint["run"].count('cp "$CHECKPOINT_PATH"') >= 2
    assert "$evidence_dir/backfill_official_checkpoint.json" in (
        prepare_checkpoint["run"]
    )
    assert "official-backfill-checkpoint-diagnostic" in prepare_checkpoint["run"]
    resolver = next(step for step in steps if step["name"] == "Resolve previous matching checkpoint")
    assert resolver["uses"] == "actions/github-script@3a2844b7e9c422d3c10d287c895573f7108da1b3"
    assert "official-backfill.yml" in resolver["with"]["script"]
    assert "!item.expired" in resolver["with"]["script"]
    restore = next(
        step for step in steps if step["name"] == "Restore previous matching checkpoint"
    )
    assert restore["env"] == {
        "EXPECTED_MODE": "${{ inputs.mode }}",
        "EXPECTED_SOURCE": "${{ inputs.source }}",
        "EXPECTED_FROM_DATE": "${{ inputs.from_date }}",
        "EXPECTED_TO_DATE": "${{ inputs.to_date }}",
    }
    assert "mapfile -d '' -t restored_files" in restore["run"]
    assert "mapfile -d '' -t metadata_files" in restore["run"]
    assert (
        "find \"$restore_root\" -type f "
        "-name 'backfill_official_checkpoint.json' -print0"
        in restore["run"]
    )
    assert (
        "find \"$restore_root\" -type f -name 'checkpoint-metadata.json' -print0"
        in restore["run"]
    )
    assert "if (( ${#restored_files[@]} > 1 ))" in restore["run"]
    assert "if (( ${#restored_files[@]} == 0 ))" in restore["run"]
    assert "if (( ${#metadata_files[@]} != 1 ))" in restore["run"]
    assert "validate-backfill-checkpoint-metadata.py" in restore["run"]
    assert '--mode "$EXPECTED_MODE"' in restore["run"]
    assert '--source "$EXPECTED_SOURCE"' in restore["run"]
    assert '--from-date "$EXPECTED_FROM_DATE"' in restore["run"]
    assert '--to-date "$EXPECTED_TO_DATE"' in restore["run"]


def test_global_backfill_is_bounded_serialized_and_preserves_daily_receipts() -> None:
    workflow = workflow_text("global-backfill.yml")
    payload = yaml.load(workflow, Loader=yaml.BaseLoader)
    dispatch = payload["on"]["workflow_dispatch"]["inputs"]
    assert set(dispatch) == {
        "source",
        "from_date",
        "to_date",
        "mode",
        "release_state",
        "max_windows",
    }
    assert dispatch["source"]["options"] == ["all", "US"]
    assert dispatch["mode"]["options"] == ["apply", "replay"]
    assert dispatch["release_state"]["options"] == ["closed", "preview"]
    assert dispatch["release_state"]["default"] == "closed"
    assert payload["permissions"] == {"contents": "read"}
    assert payload["concurrency"] == PRODUCTION_OFFICIAL_WRITE_CONCURRENCY

    job = payload["jobs"]["backfill"]
    assert job["if"] == (
        "github.ref_type == 'branch' && "
        "github.ref == format('refs/heads/{0}', "
        "github.event.repository.default_branch)"
    )
    assert int(job["timeout-minutes"]) == 360
    assert job["environment"]["name"] == "governance-runtime"
    for secret_name in (
        "BSIDE_API_BASE_URL",
        "BSIDE_OPS_TOKEN",
        "EDINET_API_KEY",
        "COMPANIES_HOUSE_API_KEY",
    ):
        assert secret_name not in job["env"]
    steps = job["steps"]
    checkout = next(
        step
        for step in steps
        if step["name"] == "Checkout immutable dispatch revision"
    )
    assert checkout["with"]["ref"] == "${{ github.sha }}"
    revision = next(
        step
        for step in steps
        if step["name"] == "Verify immutable dispatch revision"
    )
    assert "git rev-parse HEAD" in revision["run"]
    assert '"$actual" == "$GITHUB_SHA"' in revision["run"]

    validation = next(
        step
        for step in steps
        if step["name"] == "Validate source configuration"
    )
    expected_sec_identity = (
        "BSIDE-Governance-Intelligence/1.0 support@bside.ai"
    )
    assert job["env"]["EXPECTED_SEC_EDGAR_USER_AGENT"] == expected_sec_identity
    assert (
        '"$SEC_EDGAR_USER_AGENT" == "$EXPECTED_SEC_EDGAR_USER_AGENT"'
        in validation["run"]
    )
    assert "BSIDE_OPS_TOKEN" in validation["env"]
    assert "EDINET_API_KEY" not in workflow
    assert "COMPANIES_HOUSE_API_KEY" not in workflow

    deployment_smoke = next(
        step
        for step in steps
        if step["name"] == "Verify exact private API v2 deployment"
    )
    assert ".github/scripts/smoke-global-v2.py" in deployment_smoke["run"]
    assert '--expected-sha "$GITHUB_SHA"' in deployment_smoke["run"]
    assert '--release-state "$REQUIRED_RELEASE_STATE"' in deployment_smoke["run"]
    assert "--privileged-token-env BSIDE_OPS_TOKEN" in deployment_smoke["run"]
    assert "--preview-token-env GOVERNANCE_PREVIEW_TOKEN" in (
        deployment_smoke["run"]
    )
    assert '"$GOVERNANCE_PIPELINE_MODE" == "shadow"' in (
        deployment_smoke["run"]
    )
    assert deployment_smoke["env"]["BSIDE_OPS_TOKEN"] == (
        "${{ secrets.BSIDE_OPS_TOKEN }}"
    )
    assert deployment_smoke["env"]["GOVERNANCE_PREVIEW_TOKEN"] == (
        "${{ secrets.GOVERNANCE_PREVIEW_TOKEN }}"
    )
    assert deployment_smoke["env"]["REQUIRED_RELEASE_STATE"] == (
        "${{ inputs.release_state }}"
    )

    run_step = next(
        step
        for step in steps
        if step["name"] == "Run sequential one-day global backfill"
    )
    assert steps.index(deployment_smoke) < steps.index(run_step)
    assert "python -m curator.global_backfill" in run_step["run"]
    assert "--max-windows \"$BACKFILL_MAX_WINDOWS\"" in run_step["run"]
    assert "--code-revision \"$GITHUB_SHA\"" in run_step["run"]
    assert '${{ inputs.' not in run_step["run"]
    assert run_step["env"]["BACKFILL_MODE"] == "${{ inputs.mode }}"
    assert run_step["env"]["BACKFILL_FROM_DATE"] == "${{ inputs.from_date }}"
    assert run_step["env"]["BACKFILL_TO_DATE"] == "${{ inputs.to_date }}"
    assert run_step["env"]["BACKFILL_MAX_WINDOWS"] == (
        "${{ inputs.max_windows }}"
    )
    assert run_step["env"]["GLOBAL_INGEST_EXPECTED_RELEASE_STATE"] == (
        "${{ inputs.release_state }}"
    )
    assert run_step["env"]["GOVERNANCE_PREVIEW_TOKEN"] == (
        "${{ secrets.GOVERNANCE_PREVIEW_TOKEN }}"
    )
    assert "BSIDE_OPS_TOKEN" in run_step["env"]
    assert "EDINET_API_KEY" not in run_step["env"]
    assert "COMPANIES_HOUSE_API_KEY" not in run_step["env"]

    preserve = next(
        step
        for step in steps
        if step["name"] == "Preserve per-day receipts and summary"
    )
    assert preserve["if"] == "always()"
    assert preserve["with"]["if-no-files-found"] == "error"
    assert int(preserve["with"]["retention-days"]) == 30
    assert "global-backfill-${{ matrix.country }}" in preserve["with"]["path"]


def test_global_refresh_is_exact_sha_ops_authenticated_and_preview_bound() -> None:
    payload = yaml.load(
        workflow_text("ingest-global.yml"),
        Loader=yaml.BaseLoader,
    )
    steps = payload["jobs"]["ingest"]["steps"]
    boundary = next(
        step
        for step in steps
        if step["name"] == "Verify exact active API v2 release boundary"
    )
    run = boundary["run"]
    assert '--expected-sha "$GITHUB_SHA"' in run
    assert "--privileged-token-env BSIDE_OPS_TOKEN" in run
    assert "--release-state preview" in run
    assert "--preview-token-env GOVERNANCE_PREVIEW_TOKEN" in run
    assert "--release-state live" in run
    assert "GOVERNANCE_PIPELINE_MODE" in run
    assert boundary["env"]["BSIDE_OPS_TOKEN"] == (
        "${{ secrets.BSIDE_OPS_TOKEN }}"
    )
    assert boundary["env"]["GOVERNANCE_PREVIEW_TOKEN"] == (
        "${{ secrets.GOVERNANCE_PREVIEW_TOKEN }}"
    )
    collect = next(
        step
        for step in steps
        if step["name"] == "Collect and ingest official source"
    )
    assert steps.index(boundary) < steps.index(collect)
    assert collect["env"]["BSIDE_OPS_TOKEN"] == (
        "${{ secrets.BSIDE_OPS_TOKEN }}"
    )
    assert collect["env"]["GOVERNANCE_PREVIEW_TOKEN"] == (
        "${{ secrets.GOVERNANCE_PREVIEW_TOKEN }}"
    )
    assert "GLOBAL_INGEST_EXPECTED_RELEASE_STATE=preview" in collect["run"]
    assert "GLOBAL_INGEST_EXPECTED_RELEASE_STATE=live" in collect["run"]


def test_ci_audits_python_and_browser_dependencies() -> None:
    workflow = workflow_text("ci.yml")
    assert "pip-audit --requirement requirements.txt" in workflow
    assert "npm audit --audit-level=high" in workflow
    assert ".github/scripts/prepare-legacy-pages.py" in workflow
    assert ".github/scripts/restore-legacy-pages-archive.py" in workflow
    assert ".github/scripts/sanitize-official-backfill-report.py" in workflow


def test_ci_type_checks_every_release_critical_governance_module() -> None:
    ci = yaml.load(workflow_text("ci.yml"), Loader=yaml.BaseLoader)
    type_step = next(
        step
        for step in ci["jobs"]["quality"]["steps"]
        if step.get("name") == "Type check governance core"
    )
    command = str(type_step["run"])
    required_modules = {
        "curator/backfill_checkpoint_api.py",
        "curator/benchmark_candidates.py",
        "curator/dart_canary_sample.py",
        "curator/dart_quota.py",
        "curator/event_identity.py",
        "curator/global_alpha_pages_identity.py",
        "curator/global_backfill.py",
        "curator/governance_site.py",
        "curator/governance_site_config.py",
        "curator/label_agreement.py",
        "curator/mysql_backup.py",
        "curator/legacy_feed_compat.py",
        "curator/legacy_recovery_bundle.py",
        "curator/official_schedule.py",
        "curator/official_slot_claim.py",
        "curator/official_slot_epoch.py",
        "curator/operation_mode.py",
        "curator/quality_benchmark.py",
        "curator/quality_snapshot.py",
        "curator/release_evidence.py",
        "curator/release_evidence_inputs.py",
        "curator/shadow_compare.py",
        "curator/shadow_engine.py",
        ".github/scripts/sanitize-official-backfill-report.py",
    }
    assert all(module in command for module in required_modules)
    assert "--disallow-untyped-defs" in command
    assert "--no-implicit-optional" in command


def test_ci_checks_production_php_73() -> None:
    ci = yaml.load(workflow_text("ci.yml"), Loader=yaml.BaseLoader)
    job = ci["jobs"]["php73"]
    assert job["name"] == "Production PHP 7.3 compatibility"
    assert job["runs-on"] == "ubuntu-24.04"
    setup = next(
        step
        for step in job["steps"]
        if str(step.get("uses", "")).startswith("shivammathur/setup-php@")
    )
    assert setup["uses"] == (
        "shivammathur/setup-php@f3e473d116dcccaddc5834248c87452386958240"
    )
    assert setup["with"] == {
        "php-version": "7.3",
        "extensions": "mbstring, pdo_mysql",
        "tools": "none",
        "coverage": "none",
    }
    assert setup["env"] == {"fail-fast": "true"}
    commands = "\n".join(str(step.get("run", "")) for step in job["steps"])
    assert "PHP_MAJOR_VERSION !== 7" in commands
    assert "PHP_MINOR_VERSION !== 3" in commands
    assert "extension_loaded($extension)" in commands
    assert "find deploy -type f -name '*.php'" in commands
    assert "php tests/php_contracts.php" in commands
    assert "tests/php73_dart_quota_unbuffered_smoke.py" in commands
    assert "tests/php73_governance_snapshot_unbuffered_smoke.py" in commands
    assert "tests/php73_dart_identity_lifecycle_smoke.py" in commands


def test_workflows_pin_verified_node24_action_commits() -> None:
    workflows = "\n".join(
        path.read_text(encoding="utf-8") for path in WORKFLOWS.glob("*.yml")
    )
    pinned_actions = {
        "actions/checkout": ("3d3c42e5aac5ba805825da76410c181273ba90b1", "v7.0.1"),
        "actions/setup-python": ("5fda3b95a4ea91299a34e894583c3862153e4b97", "v7.0.0"),
        "actions/setup-node": ("820762786026740c76f36085b0efc47a31fe5020", "v7.0.0"),
        "actions/github-script": ("3a2844b7e9c422d3c10d287c895573f7108da1b3", "v9.0.0"),
        "actions/configure-pages": ("45bfe0192ca1faeb007ade9deae92b16b8254a0d", "v6.0.0"),
        "actions/deploy-pages": ("cd2ce8fcbc39b97be8ca5fce6e763baed58fa128", "v5.0.0"),
        "actions/upload-pages-artifact": ("fc324d3547104276b827a68afc52ff2a11cc49c9", "v5.0.0"),
        "actions/upload-artifact": ("043fb46d1a93c77aae656e7c1c64a875d1fc6a0a", "v7.0.1"),
        "actions/download-artifact": ("3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c", "v8.0.1"),
        "shivammathur/setup-php": ("f3e473d116dcccaddc5834248c87452386958240", "v2.37.2"),
    }
    expected_refs = {f"{action}@{sha}" for action, (sha, _version) in pinned_actions.items()}
    expected_lines = {
        f"uses: {action}@{sha} # {version}"
        for action, (sha, version) in pinned_actions.items()
    }
    action_lines = [
        line.strip()
        for line in workflows.splitlines()
        if line.lstrip().startswith("uses:")
    ]
    assert action_lines
    assert set(action_lines) == expected_lines
    assert set(re.findall(r"uses:\s+([^\s]+)", workflows)) == expected_refs
    assert not re.search(r"uses:\s+[^@\s]+@v\d+", workflows)
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
        if step.get("uses") == "actions/setup-node@820762786026740c76f36085b0efc47a31fe5020"
    )
    assert setup_node["with"] == {"node-version": "22", "cache": "npm"}
    assert not [path for path in (ROOT / "public").rglob(".*") if path.is_file()]


def test_watchdog_contract_and_issue_permission() -> None:
    workflow = workflow_text("watchdog.yml")
    payload = yaml.load(workflow, Loader=yaml.BaseLoader)
    script = (ROOT / ".github" / "scripts" / "watchdog.py").read_text(encoding="utf-8")
    assert "issues: write" in workflow
    assert "BSIDE_API_BASE_URL" in workflow
    assert "BSIDE_OPS_TOKEN" in workflow
    assert '"90"' in workflow
    assert 'cron: "1,6,11,16,21,26,31,36,41,46,51,56 * * * *"' in workflow
    assert "BSIDE_PUBLIC_WEB_URL" in workflow
    assert "WATCHDOG_GOVERNANCE_PAGES" in workflow
    assert "vars.GOVERNANCE_PIPELINE_MODE == 'dart_canary'" in payload["jobs"]["health"]["if"]
    incident_evidence = next(
        step
        for step in payload["jobs"]["health"]["steps"]
        if step["name"] == "Initialize generic incident evidence"
    )
    assert incident_evidence["id"] == "incident_evidence"
    assert "no configured URL or credential" in incident_evidence["run"]
    checkout = next(
        step
        for step in payload["jobs"]["health"]["steps"]
        if step["name"] == "Checkout"
    )
    revision_guard = next(
        step
        for step in payload["jobs"]["health"]["steps"]
        if step["name"] == "Verify immutable workflow revision"
    )
    assert checkout["id"] == "checkout"
    assert revision_guard["id"] == "revision_guard"
    routing = next(
        step
        for step in payload["jobs"]["health"]["steps"]
        if step["name"] == "Validate operational API routing"
    )
    assert routing["env"]["BSIDE_API_BASE_URL"] == (
        "${{ secrets.BSIDE_API_BASE_URL || vars.GOVERNANCE_API_BASE_URL }}"
    )
    assert routing["env"]["GOVERNANCE_API_BASE_URL"] == (
        "${{ vars.GOVERNANCE_API_BASE_URL }}"
    )
    assert (
        "validate-api-base-urls.py --github-env \"$GITHUB_ENV\"" in routing["run"]
    )
    assert "report_path=$report" in routing["run"]
    health = next(
        step
        for step in payload["jobs"]["health"]["steps"]
        if step["name"] == "Inspect ingest freshness and web availability"
    )
    assert health["run"] == "python .github/scripts/watchdog.py"
    assert health["env"]["WATCHDOG_EXPECTED_OFFICIAL_SOURCES"] == (
        "${{ (steps.rollout.outputs.governance_pipeline_mode == 'dart_canary' || "
        "steps.rollout.outputs.kind_connector_enabled != 'true') && "
        "'dart' || 'dart,kind' }}"
    )
    rollout = next(
        step
        for step in payload["jobs"]["health"]["steps"]
        if step["name"] == "Resolve fail-closed rollout mode"
    )
    assert rollout["env"]["KIND_CONNECTOR_MODE"] == "${{ vars.KIND_CONNECTOR_MODE }}"
    incident = next(
        step
        for step in payload["jobs"]["health"]["steps"]
        if step["name"] == "Create, update, or resolve the incident issue"
    )
    assert "always()" in incident["if"]
    assert "steps.api_routing.outcome == 'failure'" in incident["env"]["INCIDENT"]
    assert "steps.checkout.outcome == 'failure'" in incident["env"]["INCIDENT"]
    assert "steps.revision_guard.outcome == 'failure'" in incident["env"]["INCIDENT"]
    assert "steps.rollout.outcome == 'failure'" in incident["env"]["INCIDENT"]
    assert incident["env"]["REPORT_PATH"] == (
        "${{ steps.health.outputs.report_path || steps.api_routing.outputs.report_path || steps.incident_evidence.outputs.report_path }}"
    )
    assert 'api_endpoint(base_url, "/ops/health")' in script
    assert "/ops/availability-observations" in script
    assert "active_deployment_sha(payload)" in script
    assert 'source_state.get("last_scheduled_success_at")' in script
    assert "parse_expected_official_sources" in script
    assert 'os.environ.get("GITHUB_SHA"' not in script
    assert "dead_letter_count" not in script


def test_release_gate_uses_cross_run_evidence_and_checked_out_revision() -> None:
    workflow = workflow_text("release-gate.yml")
    payload = yaml.load(workflow, Loader=yaml.BaseLoader)
    assert "workflow_dispatch:" in workflow
    assert "actions: read" in workflow
    assert "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c" in workflow
    assert "digest-mismatch: error" in workflow
    download = next(
        step
        for step in payload["jobs"]["evaluate"]["steps"]
        if step.get("uses") == "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
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
    assert "EXPECTED_WORKFLOW_PATH: .github/workflows/release-evidence.yml" in workflow
    assert "Evidence artifact was not produced by the protected release-evidence workflow" in workflow
    assert '"$head_branch" != "$DEFAULT_BRANCH"' in workflow
    assert "run-id: ${{ inputs.evidence_run_id }}" in workflow
    assert "python -m curator.release_gate" in workflow
    assert "--expected-revision ${{ github.sha }}" in workflow
    assert "--evidence-as-of ${{ steps.evidence_run.outputs.created_at }}" in workflow
    assert "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a" in workflow
    assert "Governance release transition gate did not pass" in workflow
