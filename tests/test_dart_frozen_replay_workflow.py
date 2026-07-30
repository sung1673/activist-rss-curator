from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = ROOT / ".github" / "workflows"
UPLOAD_ARTIFACT = (
    "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a"
)


def workflow(name: str) -> tuple[str, dict[str, object]]:
    text = (WORKFLOWS / name).read_text(encoding="utf-8")
    return text, yaml.load(text, Loader=yaml.BaseLoader)


def step(job: dict[str, object], name: str) -> dict[str, object]:
    return next(item for item in job["steps"] if item.get("name") == name)  # type: ignore[index,union-attr]


def test_replay_requires_one_explicit_exact_apply_artifact() -> None:
    text, payload = workflow("official-backfill.yml")
    dispatch = payload["on"]["workflow_dispatch"]["inputs"]  # type: ignore[index]
    assert dispatch["frozen_apply_run_id"] == {
        "description": "Exact successful apply workflow run id required only for replay",
        "required": "false",
        "type": "string",
        "default": "",
    }
    job = payload["jobs"]["backfill"]  # type: ignore[index]
    validation = step(job, "Validate bounded backfill inputs")
    assert "replay requires a positive frozen_apply_run_id" in validation["run"]
    assert "frozen_apply_run_id is forbidden outside replay mode" in validation["run"]

    resolver = step(job, "Resolve exact immutable DART apply bundle")
    script = resolver["with"]["script"]
    for contract in (
        "getWorkflowRun",
        "FROZEN_APPLY_RUN_ID",
        'run.event !== "workflow_dispatch"',
        'run.conclusion !== "success"',
        "run.head_sha !== context.sha",
        "run.head_branch !== defaultBranch",
        'workflowPath !== ".github/workflows/official-backfill.yml"',
        "startedAt < cutoff",
        "updatedAt < cutoff",
        "matches.length !== 1",
        "artifact.expired",
        "digestPattern",
    ):
        assert contract in script
    assert "listWorkflowRuns" not in script
    assert "matches.sort" not in script
    assert "latest" not in script.casefold()
    assert "retention-days: 90" in text


def test_frozen_replay_process_has_no_dart_source_credentials() -> None:
    _text, payload = workflow("official-backfill.yml")
    job = payload["jobs"]["backfill"]  # type: ignore[index]
    replay = step(job, "Run one-day official backfill windows")
    assert replay["env"]["OPENDART_API_KEYS"] == (
        "${{ inputs.mode != 'replay' && secrets.OPENDART_API_KEYS || '' }}"
    )
    assert replay["env"]["DART_API_KEY"] == (
        "${{ inputs.mode != 'replay' && secrets.OPENDART_API_KEYS == '' && "
        "secrets.DART_API_KEY || '' }}"
    )
    assert replay["env"]["CURATOR_REQUIRE_DURABLE_DART_QUOTA"] == (
        "${{ inputs.mode != 'replay' && '1' || '0' }}"
    )
    assert "--frozen-bundle-dir" in replay["run"]
    assert "--frozen-artifact-binding" in replay["run"]
    assert "--drift-probe-only" not in replay["run"]

    probe = step(job, "Run fresh read-only DART drift probe")
    assert probe["env"]["OPENDART_API_KEYS"] == "${{ secrets.OPENDART_API_KEYS }}"
    assert probe["env"]["CURATOR_REQUIRE_DURABLE_DART_QUOTA"] == "1"
    assert probe["env"]["CURATOR_DART_QUOTA_PHASE"] == (
        "official-backfill-drift-probe"
    )
    assert "ACTIVIST_API_SECRET" not in probe["env"]
    assert "--drift-probe-only" in probe["run"]
    assert "--drift-probe-output" in probe["run"]
    assert '2> "$raw_stderr"' in probe["run"]
    assert 'rm -f "$raw_stdout" "$raw_stderr"' in probe["run"]


def test_private_bundle_and_diagnostics_cross_the_sanitized_artifact_boundary() -> None:
    _text, payload = workflow("official-backfill.yml")
    job = payload["jobs"]["backfill"]  # type: ignore[index]
    uploads = [
        item
        for item in job["steps"]  # type: ignore[index]
        if item.get("uses") == UPLOAD_ARTIFACT
    ]
    frozen = next(
        item
        for item in uploads
        if item["name"] == "Preserve immutable private frozen DART apply bundle"
    )
    assert frozen["with"]["retention-days"] == "90"
    assert frozen["with"]["compression-level"] == "9"
    assert frozen["with"]["include-hidden-files"] == "false"
    report = next(item for item in uploads if item["name"] == "Preserve backfill report")
    paths = report["with"]["path"].splitlines()
    assert "${{ runner.temp }}/dart-frozen-apply-artifact-binding.json" in paths
    assert "${{ runner.temp }}/dart-frozen-replay-artifact-binding.json" in paths
    assert "${{ runner.temp }}/dart-replay-drift-probe.json" in paths
    boundary = step(job, "Enforce sanitized backfill artifact boundary")
    assert "official-backfill-report.json.raw" in boundary["run"]
    assert "official-backfill-report.json.stderr.raw" in boundary["run"]
    assert "dart-drift-probe.stdout.raw" not in paths
    assert "dart-drift-probe.stderr.raw" not in paths


def test_replay_report_preserves_raw_drift_and_binds_release_gate_derivation() -> None:
    _text, payload = workflow("official-backfill.yml")
    job = payload["jobs"]["backfill"]  # type: ignore[index]
    binding = step(job, "Bind frozen replay and drift status into report")
    script = binding["run"]
    for contract in (
        'probe.get("status") not in {"matched", "drift_detected"}',
        '"stable-public-payload-source-count-diagnostic-v1"',
        'probe.get("release_gate_matched") is not True',
        'probe.get("blocking_drift_window_count") != 0',
        '"status": probe["status"]',
        '"diagnostic_only_window_count": diagnostic_only_window_count',
        '"blocking_drift_window_count": 0',
    ):
        assert contract in script
    assert '"status": "matched"' not in script
    assert 'probe["status"] == "matched"' not in script


def test_partial_apply_resume_is_checkpoint_bound_and_never_refetched_silently() -> None:
    _text, payload = workflow("official-backfill.yml")
    job = payload["jobs"]["backfill"]  # type: ignore[index]
    restore = step(job, "Restore previous matching checkpoint")
    script = restore["run"]
    assert "frozen-replay-bundle/manifest.json" in script
    assert "completed_count > 0" in script
    assert "cannot resume without its exact frozen bundle" in script
    assert "Refusing to overwrite an existing frozen apply workspace" in script
    assert "checkpoint_payload_hash(canonical_checkpoint(checkpoint))" in script
    assert '--expected-checkpoint-sha256 "$expected_checkpoint_sha256"' in script
    assert '--expected-from-date "$EXPECTED_FROM_DATE"' in script
    assert '--expected-to-date "$EXPECTED_TO_DATE"' in script
    prepare = step(job, "Prepare checkpoint evidence")
    assert prepare["if"] == "always()"
    assert "frozen-replay-bundle" in prepare["run"]
    assert '"frozen_bundle_present":%s' in prepare["run"]
    assert "if (( completed_count > 0 ))" in prepare["run"]
    assert 'echo "resumable=$resumable"' in prepare["run"]
    assert 'echo "upload_checkpoint=$upload_checkpoint"' in prepare["run"]
    assert (
        'elif [[ "$MODE" == "replay" && '
        '"$checkpoint_present" == "true" ]]'
        in prepare["run"]
    )
    resumable = step(job, "Preserve resumable checkpoint")
    assert resumable["if"] == (
        "always() && "
        "steps.checkpoint_evidence.outputs.upload_checkpoint == 'true'"
    )
    diagnostic = step(job, "Preserve checkpoint diagnostic")
    assert diagnostic["with"]["name"].startswith(
        "official-backfill-checkpoint-diagnostic-"
    )


def test_partial_checkpoint_resolver_is_exact_sha_and_default_branch_bound() -> None:
    _text, payload = workflow("official-backfill.yml")
    job = payload["jobs"]["backfill"]  # type: ignore[index]
    resolver = step(job, "Resolve previous matching checkpoint")
    script = resolver["with"]["script"]
    for contract in (
        'run.event !== "workflow_dispatch"',
        "run.head_sha !== context.sha",
        "run.head_branch !== defaultBranch",
        "run.head_repository?.full_name",
        'workflowPath !== ".github/workflows/official-backfill.yml"',
        "item.name === process.env.EXPECTED_ARTIFACT_NAME",
        "matches.length > 1",
        "core.setOutput(\"found\", \"false\")",
    ):
        assert contract in script
    assert "run.conclusion" not in script


def test_expedited_preparation_requires_frozen_bytes_and_matched_probe() -> None:
    text, payload = workflow("global-alpha-expedited-preparation.yml")
    evaluate = payload["jobs"]["evaluate"]  # type: ignore[index]
    resolver = step(evaluate, "Resolve exact named protected producer runs")
    assert "official-dart-frozen-replay-apply-${run.id}-${run.run_attempt}" in (
        resolver["with"]["script"]
    )
    assert "official-backfill-checkpoint-replay-dart-" in (
        resolver["with"]["script"]
    )
    download = step(evaluate, "Download immutable DART and SEC producer evidence")
    assert "dart_apply_artifact_2_id" in download["with"]["artifact-ids"]
    assert "dart_replay_artifact_1_id" in download["with"]["artifact-ids"]
    derive = step(
        evaluate,
        "Derive DART and SEC receipts only from immutable producer artifacts",
    )
    for contract in (
        "dart-frozen-apply-artifact-binding.json",
        "dart-frozen-replay-artifact-binding.json",
        "dart-replay-drift-probe.json",
        "validate_probe_contract",
        'probe_range_start != report.get("range_start")',
        'probe_range_end != report.get("range_end_exclusive")',
        'probe_fingerprint != report.get("job_fingerprint")',
        "expected_range_start=probe_range_start",
        "expected_range_end_exclusive=probe_range_end",
        "expected_job_fingerprint=probe_fingerprint",
        'drift_validation.get("release_gate_policy")',
        'drift_validation.get("release_gate_matched") is not True',
        'drift_validation.get("blocking_drift_window_count") != 0',
        'drift_validation.get("diagnostic_only_window_count")',
        'drift_validation.get("window_count") != 30',
        'drift.get("status") not in {"matched", "drift_detected"}',
        'drift.get("release_gate_policy")',
        'drift_report.get("release_gate_matched") is not True',
        'drift_report.get("blocking_drift_window_count") != 0',
        'replay_receipt.get("source_network_accessed") is not False',
        "DART replay did not consume the exact immutable apply artifact",
        "bside-global-alpha-expedited-connector-receipts-v2",
    ):
        assert contract in text
    derive_script = derive["run"]
    for resume_contract in (
        "attempted + previously_completed != 30",
        '"execution_window_count": report.get("windows_attempted")',
        '"preexisting_window_count": report.get(',
        '"evidenced_window_count": len(windows)',
        "resume evidence are inconsistent",
    ):
        assert resume_contract in derive_script


def test_explicit_validators_receive_no_secret_environment() -> None:
    _text, payload = workflow("official-backfill.yml")
    job = payload["jobs"]["backfill"]  # type: ignore[index]
    for name in (
        "Verify and bind immutable DART apply bundle",
        "Validate read-only DART drift probe diagnostic",
        "Validate private frozen DART apply bundle",
    ):
        validator = step(job, name)
        environment = validator.get("env", {})
        assert all("secrets." not in str(value) for value in environment.values())
        assert "OPENDART_API_KEYS" not in environment
        assert "DART_API_KEY" not in environment
        assert "ACTIVIST_API_SECRET" not in environment
        assert "BSIDE_OPS_TOKEN" not in environment
