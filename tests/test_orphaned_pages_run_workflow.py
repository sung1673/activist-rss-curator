from __future__ import annotations

import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "global-alpha-expedited-preparation.yml"
CLASSIFIER = ROOT / ".github" / "scripts" / "orphaned-pages-run.cjs"
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
PACKAGE = ROOT / "package.json"


def _workflow() -> tuple[str, dict[str, object]]:
    text = WORKFLOW.read_text(encoding="utf-8")
    payload = yaml.load(text, Loader=yaml.BaseLoader)
    assert isinstance(payload, dict)
    return text, payload


def _step(job: dict[str, object], name: str) -> dict[str, object]:
    steps = job.get("steps")
    assert isinstance(steps, list)
    for candidate in steps:
        if isinstance(candidate, dict) and candidate.get("name") == name:
            return candidate
    raise AssertionError(f"workflow step not found: {name}")


def test_orphan_drain_is_least_privilege_and_exact_sha_bound() -> None:
    text, payload = _workflow()
    assert "30535379482" not in text
    jobs = payload["jobs"]
    assert isinstance(jobs, dict)
    drain = jobs["drain_pages_producers"]
    assert isinstance(drain, dict)
    assert drain["permissions"] == {
        "actions": "write",
        "contents": "read",
    }
    assert drain["outputs"] == {
        "orphaned_unstarted_json": "${{ steps.drain.outputs.orphaned_unstarted_json }}"
    }

    checkout = _step(drain, "Checkout the exact expedited candidate")
    assert checkout["with"]["ref"] == "${{ github.sha }}"
    drain_step = _step(drain, "Cancel and drain pre-fence Pages producers")
    assert drain_step["env"]["EXPECTED_SHA"] == "${{ github.sha }}"


def test_orphan_drain_only_excludes_qualified_ids_on_later_scans() -> None:
    _text, payload = _workflow()
    jobs = payload["jobs"]
    assert isinstance(jobs, dict)
    drain = jobs["drain_pages_producers"]
    assert isinstance(drain, dict)
    script = str(
        _step(drain, "Cancel and drain pre-fence Pages producers")["with"]["script"]
    )

    assert "./.github/scripts/orphaned-pages-run.cjs" in script
    assert "const orphanedUnstarted = new Map();" in script
    assert "const orphanSnapshots = new Map();" in script
    assert "orphanedUnstarted.set(run.id, result.orphanedUnstarted);" in script
    assert "orphanSnapshots.set(run.id, {" in script
    assert "left.run_id - right.run_id" in script
    assert "[409, 422]" in script
    assert script.count("confirmTerminalAfterCancelConflict") == 2

    scan_index = script.index("const scanned = await listActive();")
    classify_index = script.index("await cancelOrClassify(entry);", scan_index)
    filter_index = script.index("const remaining = scanned.filter(", scan_index)
    assert scan_index < classify_index < filter_index
    assert "!orphanedUnstarted.has(run.id)" in script[filter_index:]
    final_gate_index = script.index("if (consecutiveEmptyScans >= 2)")
    revalidate_index = script.index(
        "await orphanClassifier.revalidateOrphanedUnstarted",
        final_gate_index,
    )
    audit_index = script.index(
        "const orphanAudit = [...orphanedUnstarted.values()]",
        revalidate_index,
    )
    assert final_gate_index < revalidate_index < audit_index


def test_classifier_proves_the_run_never_started_without_sha_deployments() -> None:
    classifier = CLASSIFIER.read_text(encoding="utf-8")
    for contract in (
        'liveRun.path !== BUILD_FEED_WORKFLOW',
        'liveRun.status !== "queued"',
        "liveRun.run_attempt !== 1",
        "liveRun.run_started_at !== createdAt",
        "liveRun.updated_at !== createdAt",
        "nowMs - createdAtMs < MINIMUM_AGE_MS",
        'workflow.data.state !== "disabled_manually"',
        '!countIsExactlyZero(jobs, "jobs")',
        '!countIsExactlyZero(artifacts, "artifacts")',
        "github.rest.actions.getWorkflowRun",
        "github.rest.actions.forceCancelWorkflowRun",
        "!isServerError(cancelError) || !isServerError(forceError)",
        "const beforeForce = await readCandidateEvidence",
        "const afterForce = await readCandidateEvidence",
        "async function revalidateOrphanedUnstarted",
        "Orphaned Pages run ${listedRun.id} changed after quarantine",
    ):
        assert contract in classifier

    assert "listDeployments" not in classifier
    assert "deployments" not in classifier
    assert "30535379482" not in classifier


def test_orphan_audit_is_preserved_in_rollback_preparation() -> None:
    _text, payload = _workflow()
    jobs = payload["jobs"]
    assert isinstance(jobs, dict)
    prepare = jobs["prepare_rollback"]
    assert isinstance(prepare, dict)
    assert prepare["needs"] == "drain_pages_producers"

    provenance = _step(prepare, "Record immutable rollback preparation provenance")
    assert provenance["env"]["ORPHANED_UNSTARTED_JSON"] == (
        "${{ needs.drain_pages_producers.outputs.orphaned_unstarted_json }}"
    )
    run = str(provenance["run"])
    assert "--argjson orphaned_unstarted" in run
    assert "pages_producer_drain" in run
    assert "orphaned_unstarted: $orphaned_unstarted" in run


def test_orphan_classifier_runs_in_the_standard_ci_job() -> None:
    package = json.loads(PACKAGE.read_text(encoding="utf-8"))
    assert package["scripts"]["test:orphaned-pages-run"] == (
        "node --test tests/node/orphaned-pages-run.test.cjs"
    )

    ci = yaml.load(CI_WORKFLOW.read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    matching_steps = [
        step
        for job in ci["jobs"].values()
        for step in job.get("steps", [])
        if step.get("name") == "Run orphaned Pages run classifier contracts"
    ]
    assert matching_steps == [
        {
            "name": "Run orphaned Pages run classifier contracts",
            "run": "npm run test:orphaned-pages-run",
        }
    ]
