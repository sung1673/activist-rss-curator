from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = ROOT / ".github" / "workflows" / "ci.yml"
INPUTS_WORKFLOW = (
    ROOT
    / ".github"
    / "workflows"
    / "global-alpha-evidence-inputs.yml"
)
WORKFLOW = (
    ROOT
    / ".github"
    / "workflows"
    / "global-alpha-review-candidates.yml"
)


def test_review_export_is_manual_default_branch_and_protected() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    assert "workflow_dispatch:" in text
    assert "\n  schedule:" not in text
    assert "\n  push:" not in text
    assert "\n  pull_request:" not in text
    assert "github.ref_name" in text
    assert "github.event.repository.default_branch" in text
    assert "EXPORT_GLOBAL_ALPHA_REVIEW_CANDIDATES" in text
    assert "cancel-in-progress: false" in text
    assert "name: governance-runtime" in text


def test_review_export_uses_preview_api_and_exact_workflow_sha() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    assert "curator.global_alpha_review_export" in text
    assert "GOVERNANCE_PREVIEW_TOKEN" in text
    assert "BSIDE_API_BASE_URL" in text
    assert "GITHUB_SHA" in text
    assert "--preview-token" not in text
    assert "--api-base-url" not in text
    assert "--expected-revision" not in text
    assert "--event-count 60" in text
    assert "--pair-count 120" in text
    assert "--max-events 500" in text
    assert "GLOBAL_ALPHA_OBSERVATION_ENABLED" not in text
    assert "BSIDE_ADMIN_TOKEN" not in text
    assert "BSIDE_OPS_TOKEN" not in text


def test_review_export_is_prepare_compatible_and_immutable() -> None:
    text = WORKFLOW.read_text(encoding="utf-8")

    assert "_parse_review_export" in text
    assert "_select_review_candidates" in text
    assert "(60,120,5)" in text
    assert "global-alpha-review-candidate-export.json" in text
    assert "global-alpha-review-candidates-${{ github.sha }}" in text
    assert "if-no-files-found: error" in text
    assert "canonical JSON SHA-256 used by evidence bundle" in text
    assert "uploaded file bytes SHA-256" in text
    assert "human decisions and ground truth are intentionally absent" in text


def test_review_export_is_in_the_protected_ci_type_check() -> None:
    ci_text = CI_WORKFLOW.read_text(encoding="utf-8")

    assert "mypy" in ci_text
    assert "curator/global_alpha_review_export.py" in ci_text


def test_protected_inputs_bind_exact_review_candidate_producer() -> None:
    text = INPUTS_WORKFLOW.read_text(encoding="utf-8")
    workflow = yaml.load(text, Loader=yaml.BaseLoader)
    inputs = workflow["on"]["workflow_dispatch"]["inputs"]

    assert inputs["review_candidate_run_id"]["required"] == "true"
    assert workflow["permissions"] == {"actions": "read", "contents": "read"}
    assert (
        ".github/workflows/global-alpha-review-candidates.yml"
        in text
    )
    assert "global-alpha-review-candidates-${{ github.sha }}" in text
    assert 'run.conclusion !== "success"' in text
    assert 'run.event !== "workflow_dispatch"' in text
    assert "run.head_branch !== process.env.DEFAULT_BRANCH" in text
    assert "ageHours > 72" in text
    assert "matches.length !== 1" in text
    assert "/^sha256:[0-9a-f]{64}$/i" in text

    downloads = [
        step
        for step in workflow["jobs"]["materialize"]["steps"]
        if step.get("uses")
        == "actions/download-artifact@3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c"
    ]
    assert len(downloads) == 1
    assert downloads[0]["with"]["artifact-ids"] == (
        "${{ steps.review_candidates.outputs.artifact_id }}"
    )
    assert downloads[0]["with"]["run-id"] == (
        "${{ steps.review_candidates.outputs.run_id }}"
    )
    assert downloads[0]["with"]["digest-mismatch"] == "error"
    assert downloads[0]["with"]["merge-multiple"] == "true"

    assert "verify-materialized-review" in text
    assert "--producer-run-created-at" in text
    assert "--artifact-digest" in text
    assert "review-candidate-provenance.json" in text
    assert "The review-candidate artifact must contain exactly one file" in text
