from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
V2 = (ROOT / "deploy" / "activist" / "governance_v2.php").read_text(
    encoding="utf-8"
)
WRITE = (ROOT / "deploy" / "activist" / "governance_v2_write.php").read_text(
    encoding="utf-8"
)
SPEC = yaml.safe_load(
    (ROOT / "deploy" / "activist" / "openapi-v2.yaml").read_text(
        encoding="utf-8"
    )
)
WORKFLOW_PATH = (
    ROOT / ".github" / "workflows" / "global-alpha-expedited-editorial.yml"
)
WORKFLOW_TEXT = WORKFLOW_PATH.read_text(encoding="utf-8")
WORKFLOW = yaml.safe_load(WORKFLOW_TEXT)


def test_editorial_candidate_routes_are_editor_only_and_never_public() -> None:
    assert "'/admin/expedited-review-candidates'" in V2
    assert "#^/admin/expedited-review-candidates/" in V2
    editor_auth = V2[
        V2.index("$path === '/admin/review-queue'") : V2.index(
            "} elseif (strpos($path, '/admin/')"
        )
    ]
    assert "$path === '/admin/expedited-review-candidates'" in editor_auth
    assert "v2_require_role($config, array('editor'))" in V2
    for route in (
        "/admin/expedited-review-candidates",
        "/admin/expedited-review-candidates/{event_id}",
    ):
        assert SPEC["paths"][route]["get"]["security"] == [{"EditorBearer": []}]


def test_candidate_export_is_kr_us_official_rights_and_connector_bound() -> None:
    section = WRITE[
        WRITE.index("function v2_expedited_review_documents") : WRITE.index(
            "function v2_admin_review_event"
        )
    ]
    assert "e.country_code IN (\\'KR\\',\\'US\\')" in section
    assert "d.verification_status=\\'official\\'" in section
    assert (
        "d.source_class IN (\\'official_disclosure\\',\\'official_register\\',"
        in section
    )
    assert "source_right_redistribution_sql('sr')" in section
    assert "sc.source_right_id=d.source_right_id" in section
    assert "sc.source_key=COALESCE(NULLIF(d.source_key,\\'\\'),sr.source_key)" in section
    assert "sc.connector_status=\\'active\\'" in section
    assert "d.body_text" not in section
    document_schema = SPEC["components"]["schemas"]["ExpeditedOfficialDocument"]
    assert document_schema["additionalProperties"] is False
    assert set(document_schema["required"]) == {
        "document_id",
        "issuer_id",
        "country_code",
        "source_right_id",
        "source_class",
        "source_key",
        "document_type",
        "original_language",
        "title",
        "original_url",
        "content_hash",
        "filed_at",
        "published_at",
        "retrieved_at",
        "updated_at",
        "relation_type",
        "position_no",
        "connector_id",
        "connector_base_url",
        "coverage_mode",
        "connector_status",
    }
    assert "body_text" not in document_schema["properties"]
    assert document_schema["properties"]["source_class"]["enum"] == [
        "official_disclosure",
        "official_register",
        "company_statement",
        "official_issuer",
    ]


def test_event_mutation_locks_evidence_and_approval_requires_digest() -> None:
    review = WRITE[
        WRITE.index("function v2_admin_review_event") : WRITE.index(
            "function v2_admin_brief_candidates"
        )
    ]
    assert "'expected_evidence_sha256'" in review
    assert "$decision === 'approve' && $expectedEvidenceSha256 === null" in review
    assert "v2_expedited_review_documents(" in review
    assert "$eventId,\n                true" in review
    assert "'stale_event_evidence'" in review
    approval = SPEC["components"]["schemas"]["EventApprovalRequest"]
    assert "expected_evidence_sha256" in approval["required"]
    assert approval["properties"]["expected_evidence_sha256"]["pattern"] == (
        "^[a-f0-9]{64}$"
    )


def test_brief_publish_is_exactly_bound_to_deployed_full_sha() -> None:
    brief = WRITE[
        WRITE.index("function v2_admin_publish_brief") :
    ]
    assert "'/^[a-f0-9]{40}$/'" in brief
    assert "$deploymentIdentity = v2_deployment_identity_status();" in brief
    assert "hash_equals((string)$deploymentIdentity['code_revision'], $buildSha)" in brief
    assert "'brief_build_sha_mismatch'" in brief
    schema = SPEC["components"]["schemas"]["BriefPublicationRequest"]["properties"][
        "build_sha"
    ]
    assert schema["minLength"] == schema["maxLength"] == 40
    assert schema["pattern"] == "^[a-f0-9]{40}$"


def test_protected_workflow_exports_and_replays_exact_publication() -> None:
    assert WORKFLOW["permissions"] == {"contents": "read", "actions": "read"}
    assert WORKFLOW["concurrency"]["cancel-in-progress"] is False
    assert WORKFLOW["jobs"]["export"]["environment"]["name"] == "governance-runtime"
    assert WORKFLOW["jobs"]["apply"]["environment"]["name"] == "governance-release"
    assert "global-alpha-expedited-editorial-candidates-${{ github.sha }}" in WORKFLOW_TEXT
    assert (
        "global-alpha-expedited-editorial-publication-${{ github.sha }}-"
        "${{ github.run_id }}-${{ github.run_attempt }}"
    ) in WORKFLOW_TEXT
    assert "GLOBAL_ALPHA_EXPEDITED_EDITORIAL_DECISIONS_GZIP_B64" in WORKFLOW_TEXT
    assert WORKFLOW_TEXT.count(
        "https://alignpe.gabia.io/activist/api.php/api/v1"
    ) == 2
    assert WORKFLOW_TEXT.count(
        "https://alignpe.gabia.io/activist/api.php/api/v2"
    ) == 2
    assert (
        "The editor token may only be sent to the protected production API."
        in WORKFLOW_TEXT
    )
    assert "/actions/runs/$CANDIDATE_RUN_ID/artifacts?per_page=100" in WORKFLOW_TEXT
    assert ".github/workflows/global-alpha-expedited-editorial.yml" in WORKFLOW_TEXT
    assert '"workflow_dispatch"' in WORKFLOW_TEXT
    assert '"completed"' in WORKFLOW_TEXT
    assert '"success"' in WORKFLOW_TEXT
    assert "listing.get(\"total_count\") != 1" in WORKFLOW_TEXT
    assert "dt.timedelta(hours=72)" in WORKFLOW_TEXT
    assert "digest-mismatch: error" in WORKFLOW_TEXT
    assert "decode-decisions" in WORKFLOW_TEXT
    assert "base64 --decode" not in WORKFLOW_TEXT
    assert "gzip --decompress" not in WORKFLOW_TEXT
    assert "editorial-candidates.json" in WORKFLOW_TEXT
    assert "human-decisions-template.json" in WORKFLOW_TEXT
    assert "review-pack.md" in WORKFLOW_TEXT
    assert "human-review.json" in WORKFLOW_TEXT
    assert "publication-receipt.json" in WORKFLOW_TEXT
    assert "publication-replay-receipt.json" in WORKFLOW_TEXT
    assert "mutations_applied" in WORKFLOW_TEXT
    assert "idempotent_replay" in WORKFLOW_TEXT
    assert "semantic_receipt_sha256" in WORKFLOW_TEXT
    assert (
        "delete GLOBAL_ALPHA_EXPEDITED_EDITORIAL_DECISIONS_GZIP_B64 now"
        in WORKFLOW_TEXT
    )
    assert "release state=live" not in WORKFLOW_TEXT.casefold()
    assert "pages_owner=governance" not in WORKFLOW_TEXT.casefold()
