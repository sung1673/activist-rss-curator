from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
ACTION_SHAS = {
    "actions/checkout": "3d3c42e5aac5ba805825da76410c181273ba90b1",
    "actions/setup-python": "5fda3b95a4ea91299a34e894583c3862153e4b97",
    "actions/github-script": "3a2844b7e9c422d3c10d287c895573f7108da1b3",
    "actions/download-artifact": "3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c",
    "actions/upload-artifact": "043fb46d1a93c77aae656e7c1c64a875d1fc6a0a",
    "actions/configure-pages": "45bfe0192ca1faeb007ade9deae92b16b8254a0d",
    "actions/upload-pages-artifact": "fc324d3547104276b827a68afc52ff2a11cc49c9",
    "actions/deploy-pages": "cd2ce8fcbc39b97be8ca5fce6e763baed58fa128",
}


def workflow(name: str) -> tuple[str, dict[str, object]]:
    text = (WORKFLOWS / name).read_text(encoding="utf-8")
    return text, yaml.load(text, Loader=yaml.BaseLoader)


def step_names(job: dict[str, object]) -> list[str]:
    steps = job["steps"]
    assert isinstance(steps, list)
    return [str(step.get("name")) for step in steps]


def test_candidate_pages_are_rechecked_for_internal_scores_at_expedited_boundaries() -> None:
    _preparation_text, preparation = workflow(
        "global-alpha-expedited-preparation.yml"
    )
    _cutover_text, cutover = workflow("governance-expedited-cutover.yml")
    boundaries = (
        (
            preparation["jobs"]["evaluate"],
            "Bind exact candidate Pages bytes",
        ),
        (
            cutover["jobs"]["validate"],
            "Verify exact Pages bytes and Early Access channel",
        ),
    )
    for job, step_name in boundaries:
        step = next(step for step in job["steps"] if step["name"] == step_name)
        run = step["run"]
        assert "python -m curator.legacy_internal_safety verify-site" in run
        assert "--site candidate-pages" in run
        assert "--minimum-dated-reports 90" in run


def test_expedited_evidence_is_separate_protected_same_sha_workflow() -> None:
    text, payload = workflow("global-alpha-expedited-preparation.yml")
    assert set(payload["jobs"]) == {
        "drain_pages_producers",
        "prepare_rollback",
        "rollback_drill",
        "observe",
        "collect_automated",
        "evaluate",
        "recover_preview_on_failure",
        "cleanup_variables",
    }
    assert "schedule:" not in text
    assert payload["concurrency"]["cancel-in-progress"] == "false"
    inputs = payload["on"]["workflow_dispatch"]["inputs"]
    assert set(inputs) == {
        "confirmation",
        "dart_apply_run_id",
        "dart_replay_run_id",
        "sec_apply_run_id",
        "sec_replay_run_id",
        "selected_markets_run_id",
        "governance_pages_run_id",
        "preview_smoke_run_id",
        "web_vitals_run_id",
        "web_vitals_artifact_digest",
        "legacy_preview_pages_run_id",
        "editorial_publication_run_id",
        "editorial_publication_artifact_digest",
    }
    assert inputs["confirmation"]["description"] == (
        "Type CREATE_EXPEDITED_ALPHA_PREPARATION"
    )
    assert "CREATE_EXPEDITED_ALPHA_PREPARATION" in text
    assert "github.event.repository.default_branch" in text
    assert '[[ "$(git rev-parse HEAD)" == "$GITHUB_SHA" ]]' in text
    drain = payload["jobs"]["drain_pages_producers"]
    assert drain["environment"]["name"] == "governance-release"
    assert drain["permissions"] == {"actions": "write", "contents": "read"}
    assert payload["jobs"]["prepare_rollback"]["needs"] == "drain_pages_producers"
    drain_step = next(
        step
        for step in drain["steps"]
        if step["name"] == "Cancel and drain pre-fence Pages producers"
    )
    drain_script = drain_step["with"]["script"]
    for contract in (
        ".github/workflows/build-feed.yml",
        ".github/workflows/daily.yml",
        "github.rest.actions.cancelWorkflowRun",
        "github.rest.actions.forceCancelWorkflowRun",
        "await listActive()",
        "remaining.length === 0",
        "consecutiveEmptyScans >= 2",
        "Date.now() + 9 * 60 * 1000",
        "Date.now() + 60 * 1000",
        "setTimeout(resolve, 10000)",
    ):
        assert contract in drain_script
    assert payload["jobs"]["observe"]["environment"]["name"] == "governance-runtime"
    assert payload["jobs"]["prepare_rollback"]["environment"]["name"] == (
        "governance-release"
    )
    assert payload["jobs"]["rollback_drill"]["environment"]["name"] == "github-pages"
    assert payload["jobs"]["observe"]["needs"] == "rollback_drill"
    assert payload["jobs"]["collect_automated"]["needs"] == "observe"
    assert payload["jobs"]["collect_automated"]["environment"]["name"] == (
        "governance-runtime"
    )
    assert payload["jobs"]["evaluate"]["needs"] == [
        "prepare_rollback",
        "rollback_drill",
        "observe",
        "collect_automated",
    ]
    final_text, final = workflow("global-alpha-expedited-evidence.yml")
    assert set(final["jobs"]) == {"evaluate"}
    assert "global-alpha-expedited-preparation.yml" in final_text
    assert "global-alpha-expedited-evidence-inputs.yml" in final_text
    assert "BSIDE_OPS_TOKEN" not in final_text


def test_expedited_evidence_collects_seven_real_observations() -> None:
    text, payload = workflow("global-alpha-expedited-preparation.yml")
    observe = payload["jobs"]["observe"]
    collect = next(
        step
        for step in observe["steps"]
        if step["name"] == "Collect seven real five-minute Preview observations"
    )
    run = collect["run"]
    assert "for slot in 0 1 2 3 4 5 6" in run
    assert "sleep 300" in run
    assert "python -m curator.global_alpha_monitor" in run
    assert "--require-active-pipeline" in run
    assert 'length == 7' in run
    assert ">= 1790" in run
    assert "<= 480" in run
    assert collect["env"]["GOVERNANCE_PIPELINE_MODE"] == "shadow"
    for key in (
        "BSIDE_OPS_TOKEN",
        "GOVERNANCE_PREVIEW_TOKEN",
        "BSIDE_ALPHA_PREVIEW_WEB_URL",
    ):
        assert key in collect["env"]
    boundary = next(
        step
        for step in observe["steps"]
        if step["name"] == "Enforce the explicit 30-minute observation boundary"
    )
    for contract in (
        '[[ "$PAGES_OWNER" == "legacy" && "$PIPELINE_MODE" == "shadow" ]]',
        '[[ "${STANDARD_OBSERVATION,,}" == "false" ]]',
        '[[ "${EXPEDITED_OBSERVATION,,}" == "true" ]]',
        '[[ "${TELEGRAM_DELIVERY,,}" == "false" ]]',
        '[[ "${GOVERNANCE_DELIVERY,,}" == "false" ]]',
        '[[ "${KIND_MODE,,}" == "off" ]]',
    ):
        assert contract in boundary["run"]
    assert "GLOBAL_ALPHA_EXPEDITED_OBSERVATION_ENABLED" in text


def test_expedited_preparation_rejects_telegram_recovery_payloads_and_smokes_404() -> None:
    _text, payload = workflow("global-alpha-expedited-preparation.yml")
    prepared = payload["jobs"]["prepare_rollback"]["steps"]
    safety = next(
        step
        for step in prepared
        if step["name"] == "Fail closed on Telegram data in prepared legacy recovery"
    )["run"]
    for contract in (
        'test ! -e "$site/feed/telegram.html"',
        'test ! -e "$site/feed/telegram-admin.html"',
        "python -m curator.legacy_telegram_safety verify-site",
        '--site "$site"',
        "--minimum-dated-reports 89",
    ):
        assert contract in safety
    smoke = next(
        step
        for step in payload["jobs"]["rollback_drill"]["steps"]
        if step["name"] == "Smoke the deployed legacy root inside ten minutes"
    )["run"]
    assert "for forbidden_path in feed/telegram.html feed/telegram-admin.html" in smoke
    assert '[[ "$status" != "404" ]]' in smoke
    assert "--write-out '%{http_code}'" in smoke


def test_expedited_evidence_binds_all_fixed_producers_and_gate() -> None:
    text, payload = workflow("global-alpha-expedited-preparation.yml")
    dependency_pins = {
        "feedparser==6.0.11",
        "httpx==0.27.2",
        "beautifulsoup4==4.12.3",
        "python-dateutil==2.9.0.post0",
        "PyYAML==6.0.2",
    }
    requirements = {
        line.strip()
        for line in (ROOT / "requirements.txt")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    assert dependency_pins <= requirements

    drain = payload["jobs"]["drain_pages_producers"]
    drain_names = step_names(drain)
    preflight_install_index = drain_names.index(
        "Install pinned evaluator preflight dependencies"
    )
    preflight_import_index = drain_names.index(
        "Verify evaluator import closure before producer drain"
    )
    producer_drain_index = drain_names.index(
        "Cancel and drain pre-fence Pages producers"
    )
    assert preflight_install_index < preflight_import_index < producer_drain_index
    preflight_install = drain["steps"][preflight_install_index]["run"]
    for pin in dependency_pins:
        assert f'"{pin}"' in preflight_install
    assert preflight_install.count("==") == len(dependency_pins)
    assert "-r requirements.txt" not in preflight_install
    preflight_import = drain["steps"][preflight_import_index]["run"]
    assert (
        "from curator.official_backfill import validate_checkpoint" in preflight_import
    )

    evaluate = payload["jobs"]["evaluate"]
    names = step_names(evaluate)
    setup_index = names.index("Set up gate Python")
    assert names[setup_index + 1] == "Install pinned evaluator dependencies"
    evaluator_install = evaluate["steps"][setup_index + 1]["run"]
    assert "python -m pip install --disable-pip-version-check" in evaluator_install
    for pin in dependency_pins:
        assert f'"{pin}"' in evaluator_install
    assert evaluator_install.count("==") == len(dependency_pins)
    assert "-r requirements.txt" not in evaluator_install

    _ci_text, ci = workflow("ci.yml")
    ci_steps = ci["jobs"]["test"]["steps"]
    ci_names = step_names(ci["jobs"]["test"])
    ci_install_index = ci_names.index(
        "Install expedited evaluator dependency closure"
    )
    ci_import_index = ci_names.index(
        "Verify expedited evaluator imports before full requirements"
    )
    ci_full_install_index = ci_names.index("Install runtime dependencies")
    assert ci_install_index < ci_import_index < ci_full_install_index
    ci_install = ci_steps[ci_install_index]["run"]
    for pin in dependency_pins:
        assert f'"{pin}"' in ci_install
    assert ci_install.count("==") == len(dependency_pins)
    assert "-r requirements.txt" not in ci_install
    ci_import = ci_steps[ci_import_index]["run"]
    assert "from curator.official_backfill import validate_checkpoint" in ci_import

    resolver = next(
        step
        for step in evaluate["steps"]
        if step["name"] == "Resolve exact named protected producer runs"
    )
    script = resolver["with"]["script"]
    for workflow_name in (
        "official-backfill.yml",
        "global-backfill.yml",
        "ingest-selected-markets.yml",
        "daily.yml",
        "global-alpha-preview-smoke.yml",
    ):
        assert workflow_name in script
    for artifact_contract in (
        "official-backfill-report-",
        "global-backfill-US-",
        "manual-official-link-ingest-CA-",
        "manual-official-link-ingest-AU-",
        "global-alpha-preview-smoke-",
    ):
        assert artifact_contract in script
    assert 'run.conclusion !== "success"' in script
    assert 'run.event !== "workflow_dispatch"' in script
    assert "run.head_branch !== defaultBranch" in script
    assert "(run.head_sha || \"\").toLowerCase() !== expectedSha" in script
    assert "!/^sha256:[0-9a-f]{64}$/i.test" in script

    final_text, final_payload = workflow("global-alpha-expedited-evidence.yml")
    gate = next(
        step
        for step in final_payload["jobs"]["evaluate"]["steps"]
        if step["name"] == "Assemble and evaluate the exact expedited bundle"
    )
    assert "python -m curator.global_alpha_expedited_gate evaluate" in gate["run"]
    assert '--input "$PWD/expedited-inputs.json"' in gate["run"]
    assert '--expected-revision "$GITHUB_SHA"' in gate["run"]
    assert "global-alpha-expedited-release-report.json" in gate["run"]
    assert '.release_gate_passed == true' in gate["run"]
    assert '"production_alpha_early_access"' in gate["run"]
    approval_text, _approval_payload = workflow(
        "global-alpha-expedited-evidence-inputs.yml"
    )
    assert "GLOBAL_ALPHA_EXPEDITED_RELEASE_INPUTS_GZIP_B64" not in text
    assert "GLOBAL_ALPHA_EXPEDITED_RELEASE_INPUTS_GZIP_B64" in approval_text
    assert "base64.b64decode(encoded, validate=True)" in approval_text
    assert "gzip.GzipFile" in approval_text
    assert "handle.read(500_001)" in approval_text
    assert 'echo "$GLOBAL_ALPHA_EXPEDITED_RELEASE_INPUTS_GZIP_B64"' not in (
        approval_text
    )
    assert (
        "python -m curator.global_alpha_expedited_final_approval template"
        in approval_text
    )
    assert "legacy-waiver.json" in approval_text
    assert "bside-global-production-alpha-expedited-inputs" in final_text


def test_expedited_evidence_upload_is_exact_and_immutable() -> None:
    _text, payload = workflow("global-alpha-expedited-evidence.yml")
    evaluate = payload["jobs"]["evaluate"]
    upload = next(
        step
        for step in evaluate["steps"]
        if step["name"] == "Upload immutable expedited release evidence"
    )
    assert upload["uses"] == (
        "actions/upload-artifact@"
        "043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
    )
    assert upload["with"] == {
        "name": "global-alpha-expedited-evidence",
        "path": "global-alpha-expedited-evidence",
        "if-no-files-found": "error",
        "retention-days": "90",
        "compression-level": "9",
    }
    assemble = next(
        step
        for step in evaluate["steps"]
        if step["name"] == "Assemble immutable expedited evidence"
    )
    for filename in (
        "global-alpha-expedited-release-report.json",
        "expedited-inputs.json",
        "expedited-producer-manifest.json",
        "pages-artifact-identity.json",
        "artifact-provenance.json",
        "expedited-legacy-recovery-bundle",
        "expedited-rollback-drill.json",
    ):
        assert filename in assemble["run"]


def test_expedited_cutover_accepts_only_fresh_exact_evidence() -> None:
    text, payload = workflow("governance-expedited-cutover.yml")
    assert "schedule:" not in text
    assert payload["concurrency"] == {
        "group": (
            "governance-production-official-write-"
            "${{ github.repository }}-${{ github.ref }}"
        ),
        "queue": "max",
        "cancel-in-progress": "false",
    }
    inputs = payload["on"]["workflow_dispatch"]["inputs"]
    assert set(inputs) == {
        "confirmation",
        "evidence_run_id",
        "evidence_artifact_name",
        "evidence_artifact_digest",
        "reason",
    }
    assert inputs["confirmation"]["description"] == (
        "Type CUTOVER_EXPEDITED_ALPHA_EARLY_ACCESS"
    )
    assert inputs["evidence_artifact_name"]["default"] == (
        "global-alpha-expedited-evidence"
    )
    assert "CUTOVER_EXPEDITED_ALPHA_EARLY_ACCESS" in text
    validate = payload["jobs"]["validate"]
    assert validate["environment"]["name"] == "governance-release"
    resolver = next(
        step
        for step in validate["steps"]
        if step["name"] == "Resolve exact expedited evidence workflow and artifact"
    )
    script = resolver["with"]["script"]
    assert ".github/workflows/global-alpha-expedited-evidence.yml" in script
    assert "global-alpha-expedited-evidence" in script
    assert "artifact_created_at" in script
    assert "run_updated_at" in script
    assert "Date.parse(run.created_at)" not in script
    assert "requestedDigest" in script
    assert "(run.head_sha || \"\").toLowerCase()" in script
    assert "run.head_branch !== process.env.DEFAULT_BRANCH" in script
    assert validate["outputs"]["evidence_artifact_digest"] == (
        "${{ steps.evidence.outputs.artifact_digest }}"
    )


def test_expedited_cutover_rechecks_gate_pages_and_preview() -> None:
    text, payload = workflow("governance-expedited-cutover.yml")
    validate = payload["jobs"]["validate"]
    recheck = next(
        step
        for step in validate["steps"]
        if step["name"] == "Re-evaluate immutable expedited evidence and freshness"
    )
    assert "python -m curator.global_alpha_expedited_gate evaluate" in recheck["run"]
    assert '"$age" -ge -60 && "$age" -le 3600' in recheck["run"]
    assert "EVIDENCE_ARTIFACT_CREATED_AT" in recheck["env"]
    assert "EVIDENCE_RUN_UPDATED_AT" in recheck["env"]
    assert '"$EVIDENCE_RUN_UPDATED_AT"' in recheck["run"]
    assert '"$EVIDENCE_ARTIFACT_CREATED_AT"' in recheck["run"]
    assert '"$EVIDENCE_RUN_CREATED_AT" "$evidence_as_of"' not in recheck["run"]
    assert "cmp --silent" in recheck["run"]
    assert "pages-artifact-identity.json" in text
    assert "protected-input-provenance.json" in recheck["run"]
    assert "source-right-evidence/KR.json" in recheck["run"]
    assert "source_right_evidence_sha256" in recheck["run"]
    assert "human_review_bytes_sha256" in recheck["run"]
    assert "Only the exact evidence-bound Pages artifact may be cut over" in text
    pages = next(
        step
        for step in validate["steps"]
        if step["name"] == "Verify exact Pages bytes and Early Access channel"
    )
    assert "python -m curator.global_alpha_pages_identity verify" in pages["run"]
    assert "python -m curator.governance_site_config" in pages["run"]
    assert "production_alpha_early_access" in pages["run"]
    assert "Production Alpha · Early Access" in pages["run"]
    legacy_deadline = next(
        step
        for step in validate["steps"]
        if step["name"]
        == "Verify evidence-carried full-site expedited recovery at cutover time"
    )
    assert "python -m curator.expedited_legacy_recovery_bundle verify" in (
        legacy_deadline["run"]
    )
    for contract in (
        'test ! -e "$site/feed/telegram.html"',
        'test ! -e "$site/feed/telegram-admin.html"',
        "python -m curator.legacy_telegram_safety verify-site",
        "--minimum-dated-reports 89",
    ):
        assert contract in legacy_deadline["run"]
    binding = next(
        step
        for step in validate["steps"]
        if step["name"] == "Bind exact recovery bytes for protected handoff"
    )
    for contract in (
        "cutover-recovery-binding.json",
        "bside-global-alpha-expedited-cutover-recovery",
        "cutover_run_id",
        "cutover_run_attempt",
        "evidence_artifact_digest",
        "legacy_artifact_digest",
        "bundle_manifest_sha256",
        "legacy_root_sha256",
        "legacy_feed_sha256",
    ):
        assert contract in binding["run"]
    relay = next(
        step
        for step in validate["steps"]
        if step["name"] == "Preserve exact recovery bundle for protected handoff"
    )
    assert relay["with"] == {
        "name": (
            "global-alpha-expedited-cutover-recovery-"
            "${{ github.run_id }}-${{ github.run_attempt }}"
        ),
        "path": "cutover-handoff-recovery",
        "if-no-files-found": "error",
        "retention-days": "90",
        "compression-level": "9",
    }

    preflight = payload["jobs"]["preflight"]
    assert preflight["environment"]["name"] == "governance-runtime"
    assert preflight["permissions"]["actions"] == "write"
    assert "Cancel, drain, and audit stale Pages producers" in step_names(
        preflight
    )
    drain = next(
        step
        for step in preflight["steps"]
        if step["name"] == "Cancel, drain, and audit stale Pages producers"
    )
    for contract in (
        "./.github/scripts/orphaned-pages-run.cjs",
        "handleCancelServerError",
        "revalidateOrphanedUnstarted",
        "cutover-pages-producer-drain-audit.json",
        "orphaned_unstarted: auditEntries",
        "left - right",
    ):
        assert contract in drain["with"]["script"]
    assert "30535379482" not in drain["with"]["script"]
    drain_upload = next(
        step
        for step in preflight["steps"]
        if step["name"] == "Upload exact Pages producer drain audit"
    )
    assert drain_upload["with"] == {
        "name": (
            "global-alpha-expedited-cutover-pages-drain-"
            "${{ github.run_id }}-${{ github.run_attempt }}"
        ),
        "path": "cutover-pages-producer-drain-audit.json",
        "if-no-files-found": "error",
        "retention-days": "90",
        "compression-level": "9",
    }
    source_gate = next(
        step
        for step in preflight["steps"]
        if step["name"] == "Require exact Preview states and expedited source policy"
    )
    for contract in (
        "connector:kr:dart",
        "connector:us:sec-edgar",
        "connector:ca:issuer-ir",
        "connector:au:asic-register",
        "connector:jp:edinet",
        "connector:gb:companies-house",
        '"coverage_unavailable"',
        ".raw_count == 0",
        ".acknowledged_count == 0",
    ):
        assert contract in source_gate["run"]


def test_expedited_cutover_deploys_smokes_then_atomically_activates() -> None:
    text, payload = workflow("governance-expedited-cutover.yml")
    assert list(payload["jobs"])[:5] == [
        "validate",
        "preflight",
        "deploy_pages",
        "preview_smoke",
        "activate",
    ]
    deploy = payload["jobs"]["deploy_pages"]
    assert deploy["environment"]["name"] == "github-pages"
    names = step_names(deploy)
    assert names.index("Revalidate protected Preview versions inside the Pages lock") < (
        names.index("Upload exact Early Access Pages")
    ) < names.index("Deploy exact Early Access Pages")
    assert any(
        step.get("uses")
        == "actions/upload-pages-artifact@fc324d3547104276b827a68afc52ff2a11cc49c9"
        for step in deploy["steps"]
    )
    assert any(
        step.get("uses")
        == "actions/deploy-pages@cd2ce8fcbc39b97be8ca5fce6e763baed58fa128"
        for step in deploy["steps"]
    )
    preview = payload["jobs"]["preview_smoke"]
    assert preview["needs"] == ["validate", "preflight", "deploy_pages"]
    assert preview["environment"]["name"] == "governance-runtime"
    assert "Smoke deployed Pages while both APIs remain Preview" in step_names(preview)
    activate = payload["jobs"]["activate"]
    assert activate["needs"] == [
        "validate",
        "preflight",
        "deploy_pages",
        "preview_smoke",
    ]
    assert activate["environment"]["name"] == "governance-release"
    atomic = next(
        step
        for step in activate["steps"]
        if step["name"] == "Authorize and atomically activate v1 and v2"
    )
    assert "/admin/release-authorizations" in atomic["run"]
    assert "/admin/cutover" in atomic["run"]
    assert (
        '[[ "$BSIDE_RELEASE_AUTHORIZER_TOKEN" != "$BSIDE_ADMIN_TOKEN" ]]'
        in atomic["run"]
    )
    assert "Protected release roles must use distinct credentials." in atomic["run"]
    assert "openssl rand -hex 32" in atomic["run"]
    assert "::add-mask::$release_nonce" in atomic["run"]
    assert "expected_v1_state_version" in atomic["run"]
    assert "expected_v2_state_version" in atomic["run"]
    assert atomic["env"]["BSIDE_RELEASE_AUTHORIZER_TOKEN"] == (
        "${{ secrets.BSIDE_RELEASE_AUTHORIZER_TOKEN }}"
    )
    assert atomic["env"]["BSIDE_ADMIN_TOKEN"] == "${{ secrets.BSIDE_ADMIN_TOKEN }}"
    assert "Final public Early Access smoke" in step_names(activate)
    assert (
        "PAGES_OWNER must be committed to governance by the authenticated operator"
        in text
    )
    assert (
        "Verify protected ownership and deployment fence"
        in step_names(deploy)
    )
    assert "GOVERNANCE_PIPELINE_MODE must remain shadow until live activation" in text


def test_expedited_cutover_has_fail_safe_close_and_legacy_recovery() -> None:
    text, payload = workflow("governance-expedited-cutover.yml")
    assert {
        "recover_close",
        "recover_pages",
        "recover_verify",
    }.issubset(payload["jobs"])
    close = payload["jobs"]["recover_close"]
    assert close["environment"]["name"] == "governance-runtime"
    close_run = next(
        step["run"]
        for step in close["steps"]
        if step["name"] == "Fail safe by closing v2 then v1"
    )
    assert close_run.index("recover-v2.json") < close_run.index("recover-v1.json")
    assert 'release_state:"closed"' in close_run
    recover_pages = payload["jobs"]["recover_pages"]
    assert recover_pages["environment"]["name"] == "github-pages"
    assert "Restore pinned legacy artifact" in step_names(recover_pages)
    assert "evidence/expedited-legacy-recovery-bundle/full-site" in text
    recovery_safety = next(
        step
        for step in recover_pages["steps"]
        if step["name"] == "Verify pinned legacy recovery before deployment"
    )["run"]
    for contract in (
        'test ! -e "$site/feed/telegram.html"',
        'test ! -e "$site/feed/telegram-admin.html"',
        "python -m curator.legacy_telegram_safety verify-site",
        "--minimum-dated-reports 89",
    ):
        assert contract in recovery_safety
    verify = payload["jobs"]["recover_verify"]
    assert verify["environment"]["name"] == "governance-runtime"
    assert "Verify failed cutover remains fenced" in step_names(verify)
    assert "Verify closed APIs and restored legacy root" in step_names(verify)
    recovery_smoke = next(
        step
        for step in verify["steps"]
        if step["name"] == "Verify closed APIs and restored legacy root"
    )["run"]
    assert (
        "for forbidden_path in feed/telegram.html feed/telegram-admin.html"
        in recovery_smoke
    )
    assert '[[ "$status" != "404" ]]' in recovery_smoke
    assert '[[ "$PAGES_OWNER" == "governance" ]]' in text
    assert "must set PAGES_OWNER=legacy before disabling" in text


def test_expedited_workflow_actions_are_immutable_pins() -> None:
    for workflow_name in (
        "global-alpha-expedited-preparation.yml",
        "global-alpha-expedited-evidence-inputs.yml",
        "global-alpha-expedited-evidence.yml",
        "governance-expedited-cutover.yml",
        "governance-expedited-handoff.yml",
    ):
        _text, payload = workflow(workflow_name)
        for job in payload["jobs"].values():
            for step in job.get("steps", []):
                uses = step.get("uses")
                if not uses or uses.startswith("./"):
                    continue
                action, separator, revision = uses.partition("@")
                assert separator
                assert action in ACTION_SHAS
                assert revision == ACTION_SHAS[action]


def test_standard_release_workflows_remain_separate() -> None:
    standard_evidence = WORKFLOWS / "global-alpha-release-evidence.yml"
    standard_cutover = WORKFLOWS / "governance-cutover.yml"
    assert standard_evidence.is_file()
    assert standard_cutover.is_file()
    assert "global-alpha-observation-chain.yml" in standard_evidence.read_text(
        encoding="utf-8"
    )
    assert "global-alpha-release-evidence" in standard_cutover.read_text(
        encoding="utf-8"
    )


def test_expedited_evidence_uses_actual_receipts_human_bytes_and_rights() -> None:
    text, payload = workflow("global-alpha-expedited-preparation.yml")
    evaluate = payload["jobs"]["evaluate"]
    resolver = next(
        step
        for step in evaluate["steps"]
        if step["name"] == "Resolve exact named protected producer runs"
    )["with"]["script"]
    assert "new Set(requestedRunIds).size !== requestedRunIds.length" in resolver
    assert "official-backfill-checkpoint-apply-dart-" in resolver
    assert "official-backfill-checkpoint-replay-dart-" in resolver
    producer_downloads = {
        step["name"]: step["with"]
        for step in evaluate["steps"]
        if step.get("name", "").startswith("Download immutable ")
        and step.get("name", "").endswith(" producer evidence")
    }
    for key, label in (
        ("dart_apply", "DART apply"),
        ("dart_replay", "DART replay"),
        ("sec_apply", "SEC apply"),
        ("sec_replay", "SEC replay"),
    ):
        download = producer_downloads[
            f"Download immutable {label} producer evidence"
        ]
        assert download["run-id"] == (
            "${{ steps.producers.outputs." + key + "_run_id }}"
        )
        assert download["repository"] == "${{ github.repository }}"
        assert download["path"] == "connector-artifacts"
        assert download["merge-multiple"] == "false"
    derive = next(
        step
        for step in evaluate["steps"]
        if step["name"]
        == "Derive DART and SEC receipts only from immutable producer artifacts"
    )["run"]
    for contract in (
        "official-backfill-report.json",
        "backfill_official_checkpoint.json",
        "dart_apply_bytes != dart_replay_bytes",
        "attempted + previously_completed != 30",
        '"evidenced_window_count": len(windows)',
        'report.get("windows_already_completed") != 30',
        "dart-replay-state-binding.json",
        "apply_summary_counts_sha256",
        "replay_summary_counts_sha256",
        "DART replay is not bound to the exact apply receipt contract",
        "global-backfill-US-summary.json",
        "US-????-??-??.json",
        "not 1 <= request_count <= 6",
        'replay.get("read_only") is not True',
    ):
        assert contract in derive
    for contract in (
        "global-alpha-expedited-editorial.yml",
        "global-alpha-expedited-editorial-publication-",
        "human-review.json",
        "publication-receipt.json",
        "publication-replay-receipt.json",
        "raw human review cannot self-assert publication artifact identity",
    ):
        assert contract in text
    final_text, _final = workflow("global-alpha-expedited-evidence.yml")
    assert "cannot self-assert post-upload identity" in final_text
    assert "APPROVAL_ARTIFACT_ID" in final_text
    assert "editorial_human_review" in final_text
    assert "final_approval" in final_text
    assert "ops/source-right-eligibility" in text
    assert '"source_right_valid": right_contract_valid' in text
    assert '"source_right_valid": True' not in text


def test_corrected_human_approval_chain_is_bound_end_to_end() -> None:
    editorial_text, editorial = workflow(
        "global-alpha-expedited-editorial.yml"
    )
    prepare = next(
        step
        for step in editorial["jobs"]["carry_forward_prepare"]["steps"]
        if step["name"]
        == "Freeze unchanged current-SHA basis without publishing"
    )["run"]
    publish = next(
        step
        for step in editorial["jobs"]["carry_forward_publish"]["steps"]
        if step["name"] == "Publish and replay only the frozen intent"
    )["run"]
    recover = next(
        step
        for step in editorial["jobs"]["carry_forward_recover"]["steps"]
        if step["name"]
        == "Recover publication from only the frozen intent"
    )["run"]
    assert "human_approval_chain_sha256" in prepare
    assert "human approval chain digest changed during publication" in publish
    assert "human approval chain digest changed during recovery" in recover
    assert editorial_text.count("human_approval_chain_sha256") >= 9

    preparation_text, preparation = workflow(
        "global-alpha-expedited-preparation.yml"
    )
    preparation_steps = preparation["jobs"]["evaluate"]["steps"]
    verify_publication = next(
        step
        for step in preparation_steps
        if step["name"]
        == "Verify actual editorial publication and inject its immutable identity"
    )["run"]
    assemble_preparation = next(
        step
        for step in preparation_steps
        if step["name"] == "Assemble immutable expedited preparation"
    )["run"]
    assert "editorial human approval chain digest mismatch" in verify_publication
    assert '"human_approval_chain_sha256": human[' in assemble_preparation
    assert "human_approval_chain_sha256" in preparation_text

    approval_text, approval_workflow = workflow(
        "global-alpha-expedited-evidence-inputs.yml"
    )
    approval_steps = approval_workflow["jobs"]["materialize"]["steps"]
    verify_preparation = next(
        step
        for step in approval_steps
        if step["name"]
        == "Verify preparation provenance before accepting final approval"
    )["run"]
    bind_approval = next(
        step
        for step in approval_steps
        if step["name"] == "Bind final human approval to the exact preparation"
    )["run"]
    assert "preparation human approval chain digest mismatch" in verify_preparation
    assert '"human_approval_chain_sha256",' in bind_approval
    assert approval_text.count("human_approval_chain_sha256") >= 6

    evidence_text, evidence = workflow(
        "global-alpha-expedited-evidence.yml"
    )
    evidence_steps = evidence["jobs"]["evaluate"]["steps"]
    verify_final = next(
        step
        for step in evidence_steps
        if step["name"] == "Verify final approval binds the exact preparation"
    )["run"]
    assemble_evidence = next(
        step
        for step in evidence_steps
        if step["name"] == "Assemble immutable expedited evidence"
    )["run"]
    assert "human approval chain digest changed after publication" in verify_final
    assert '"human_approval_chain_sha256",' in verify_final
    assert "human_approval_chain_sha256" in assemble_evidence
    assert evidence_text.count("human_approval_chain_sha256") >= 6

    final_helper = (
        ROOT / "curator" / "global_alpha_expedited_final_approval.py"
    ).read_text(encoding="utf-8")
    gate_helper = (
        ROOT / "curator" / "global_alpha_expedited_gate.py"
    ).read_text(encoding="utf-8")
    assert '"human_approval_chain_sha256",' in final_helper
    assert "preparation human approval chain digest mismatch" in final_helper
    assert "expedited_human_review.approval_chain_bound" in gate_helper
    assert '"human_approval_chain_sha256": human_summary[' in gate_helper


def test_expedited_preparation_requires_exact_approved_identity_targets() -> None:
    _text, preparation = workflow(
        "global-alpha-expedited-preparation.yml"
    )
    verify_publication = next(
        step
        for step in preparation["jobs"]["evaluate"]["steps"]
        if step["name"]
        == "Verify actual editorial publication and inject its immutable identity"
    )["run"]

    for contract in (
        'human_carry = human.get("carry_forward")',
        'human_carry.get("approved_canonical_basis")',
        'approved_basis.get("events")',
        "not isinstance(approved_events, list) or len(approved_events) != 20",
        'expected_target = issuer_name + " — " + title',
        'event.get("identity_target") != expected_target',
    ):
        assert contract in verify_publication
    assert "issuer_name.strip" not in verify_publication
    assert "title.strip" not in verify_publication
    assert "issuer_name.casefold" not in verify_publication
    assert "title.casefold" not in verify_publication


def test_expedited_cutover_leaves_repository_variables_for_authenticated_handoff() -> None:
    text, payload = workflow("governance-expedited-cutover.yml")
    deploy = payload["jobs"]["deploy_pages"]
    assert deploy["permissions"]["actions"] == "read"
    activate = payload["jobs"]["activate"]
    assert activate["permissions"]["actions"] == "read"
    live = next(
        step
        for step in activate["steps"]
        if step["name"] == "Verify protected handoff remains fenced"
    )
    assert live["env"]["PAGES_OWNER"] == "${{ vars.PAGES_OWNER }}"
    assert '[[ "$PAGES_OWNER" == "governance" ]]' in live["run"]
    assert '[[ "$PIPELINE_MODE" == "shadow" ]]' in live["run"]
    assert "authenticated operator must now set GOVERNANCE_PIPELINE_MODE=live" in (
        live["run"]
    )
    recovery = payload["jobs"]["recover_verify"]
    assert recovery["permissions"]["actions"] == "read"
    fail_closed = next(
        step
        for step in recovery["steps"]
        if step["name"] == "Verify failed cutover remains fenced"
    )["run"]
    assert '[[ "$PAGES_OWNER" == "governance" ]]' in fail_closed
    assert '[[ "$PIPELINE_MODE" == "shadow" ]]' in fail_closed
    assert "must set PAGES_OWNER=legacy before disabling" in fail_closed
    assert "github.rest.actions.updateRepoVariable" not in text
    assert "github.rest.actions.getRepoVariable" not in text
    assert "(.data.items | length) == 6" in text
    assert "remote-preview-config.js" in text
    assert "final-config.js" in text


def test_expedited_final_approval_producer_is_protected_and_pinned() -> None:
    text, payload = workflow("global-alpha-expedited-evidence-inputs.yml")
    assert set(payload["jobs"]) == {"materialize"}
    job = payload["jobs"]["materialize"]
    assert job["environment"]["name"] == "governance-release"
    assert "UPLOAD_EXPEDITED_ALPHA_FINAL_APPROVAL" in text
    assert "GLOBAL_ALPHA_EXPEDITED_RELEASE_INPUTS_GZIP_B64" in text
    assert "base64.b64decode(encoded, validate=True)" in text
    assert "gzip.GzipFile" in text
    assert "handle.read(500_001)" in text
    assert "protected input must contain approval and optional legacy_waiver only" in (
        text
    )
    assert "approval.json" in text
    upload = next(
        step
        for step in job["steps"]
        if step["name"]
        == "Upload immutable expedited final approval"
    )
    assert upload["with"]["name"] == (
        "global-alpha-expedited-final-approval-${{ github.sha }}"
    )


def test_final_approval_binding_has_no_post_approval_clock_dependency() -> None:
    text, payload = workflow("global-alpha-expedited-evidence-inputs.yml")
    finalize = next(
        step
        for step in payload["jobs"]["materialize"]["steps"]
        if step["name"] == "Finalize actual 89-or-90-day recovery evidence"
    )["run"]
    assert "global_alpha_expedited_final_approval template" in finalize
    assert 'observed_at="$(date -u' not in finalize
    binding = next(
        step
        for step in payload["jobs"]["materialize"]["steps"]
        if step["name"] == "Bind final human approval to the exact preparation"
    )["run"]
    assert 'approval.get("evidence_binding") != binding' in binding
    helper = (
        ROOT / "curator" / "global_alpha_expedited_final_approval.py"
    ).read_text(encoding="utf-8")
    assert 'record.get("evidence_as_of")' in helper
    assert "current_time >= WAIVER_EXPIRES_AT" in helper
    assert "the exact 89-day archive requires a human waiver" in helper
    assert (
        "a legacy waiver is forbidden when a real 90-day archive is available"
        in helper
    )
    assert "final-approval-template.json" in helper
    assert "seal_final_approval" in helper


def test_preparation_runtime_secrets_are_isolated_from_pure_evaluation() -> None:
    _text, payload = workflow("global-alpha-expedited-preparation.yml")
    collector = payload["jobs"]["collect_automated"]
    assert collector["environment"]["name"] == "governance-runtime"
    collector_text = "\n".join(
        str(step) for step in collector["steps"]
    )
    assert "BSIDE_OPS_TOKEN" in collector_text
    assert "ops/alpha-release-evidence" in collector_text
    assert "ops/source-right-eligibility" in collector_text
    evaluator = payload["jobs"]["evaluate"]
    assert "environment" not in evaluator
    evaluator_text = "\n".join(str(step) for step in evaluator["steps"])
    assert "secrets.BSIDE_OPS_TOKEN" not in evaluator_text
    assert "Download same-run protected automated evidence" in step_names(evaluator)


def test_preparation_always_verifies_observation_handoff_and_recovers_preview() -> None:
    text, payload = workflow("global-alpha-expedited-preparation.yml")
    recovery = payload["jobs"]["recover_preview_on_failure"]
    assert "always()" in recovery["if"]
    assert "needs.rollback_drill.result != 'success'" in recovery["if"]
    assert "Restore Preview after interrupted rollback drill" in step_names(recovery)
    exact = next(
        step
        for step in recovery["steps"]
        if step["name"] == "Verify restored Preview bytes"
    )["run"]
    assert "cmp --silent" in exact
    cleanup = payload["jobs"]["cleanup_variables"]
    assert cleanup["if"] == "${{ always() }}"
    assert cleanup["permissions"]["actions"] == "read"
    step = cleanup["steps"][0]
    assert step["name"] == "Verify immutable deployment fence remains active"
    script = step["run"]
    assert '[[ "${EXPEDITED_OBSERVATION,,}" == "true" ]]' in script
    assert '[[ "$PAGES_OWNER" == "legacy" ]]' in script
    assert '[[ "$PIPELINE_MODE" == "shadow" ]]' in script
    assert '[[ "${TELEGRAM_DELIVERY,,}" == "false" ]]' in script
    assert '[[ "${GOVERNANCE_DELIVERY,,}" == "false" ]]' in script
    assert '[[ "${KIND_MODE,,}" == "off" ]]' in script
    assert "Keep GLOBAL_ALPHA_EXPEDITED_OBSERVATION_ENABLED=true" in script
    assert "github.rest.actions.updateRepoVariable" not in script
    assert "github.rest.actions.getRepoVariable" not in script
    assert "preview_terminal_content_sha256" in text


def test_protected_human_artifact_does_not_self_assert_post_upload_identity() -> None:
    input_text, input_payload = workflow(
        "global-alpha-expedited-evidence-inputs.yml"
    )
    materialize = input_payload["jobs"]["materialize"]
    assert materialize["environment"]["name"] == "governance-release"
    assert "preparation_run_id" in input_payload["on"]["workflow_dispatch"]["inputs"]
    assert "preparation_artifact_digest" in (
        input_payload["on"]["workflow_dispatch"]["inputs"]
    )
    assert "global-alpha-expedited-preparation.yml" in input_text
    final_text, final_payload = workflow("global-alpha-expedited-evidence.yml")
    assembly = next(
        step
        for step in final_payload["jobs"]["evaluate"]["steps"]
        if step["name"] == "Assemble and evaluate the exact expedited bundle"
    )
    assert "APPROVAL_ARTIFACT_ID" in assembly["env"]
    assert "cannot self-assert post-upload identity" in assembly["run"]
    assert "approval.update(artifact_identity)" in assembly["run"]
    final_assembly = next(
        step
        for step in final_payload["jobs"]["evaluate"]["steps"]
        if step["name"] == "Assemble immutable expedited evidence"
    )["run"]
    assert "human_review_bytes_sha256" in final_assembly
    assert "approval_bytes_sha256" in final_assembly
    assert "editorial_human_review" in final_assembly
    assert "final_approval" in final_assembly
    assert "post_upload_identity_injected_only_in_evaluator_wrapper" in (
        final_assembly
    )
    assert "source_right_evidence_sha256" in final_text


def test_expedited_workflows_have_no_disabled_steps_or_corrupt_badge_text() -> None:
    for workflow_name in (
        "global-alpha-expedited-preparation.yml",
        "global-alpha-expedited-evidence-inputs.yml",
        "global-alpha-expedited-evidence.yml",
        "governance-expedited-cutover.yml",
    ):
        text, _payload = workflow(workflow_name)
        assert "if: ${{ false }}" not in text
        assert "if: false" not in text
        assert "бд" not in text
        assert "쨌" not in text
        assert "\ufffd" not in text
    cutover_text, _payload = workflow("governance-expedited-cutover.yml")
    assert "Production Alpha · Early Access" in cutover_text


def test_cutover_keeps_owner_fail_closed_and_compares_exact_bytes() -> None:
    text, payload = workflow("governance-expedited-cutover.yml")
    boundary = next(
        step
        for step in payload["jobs"]["validate"]["steps"]
        if step["name"] == "Enforce protected expedited cutover inputs"
    )["run"]
    assert '[[ "$PAGES_OWNER" == "governance" ]]' in boundary
    deploy = payload["jobs"]["deploy_pages"]
    assert deploy["permissions"]["actions"] == "read"
    assert step_names(deploy)[0] == (
        "Verify protected ownership and deployment fence"
    )
    preview = payload["jobs"]["preview_smoke"]
    assert "Download evidence-bound candidate Pages for byte comparison" in (
        step_names(preview)
    )
    smoke = next(
        step
        for step in preview["steps"]
        if step["name"] == "Smoke deployed Pages while both APIs remain Preview"
    )["run"]
    for path in (
        "candidate-pages/index.html",
        "candidate-pages/governance/index.html",
        "candidate-pages/feed.xml",
        "candidate-pages/governance/config.js",
        "candidate-pages/governance/app.js",
    ):
        assert path in smoke
    assert smoke.count("cmp --silent") >= 5
    assert "GLOBAL_ALPHA_EXPEDITED_OBSERVATION_ENABLED" in text
