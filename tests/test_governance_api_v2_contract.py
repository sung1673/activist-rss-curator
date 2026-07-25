import runpy
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
V2_PATH = ROOT / "deploy" / "activist" / "governance_v2.php"
V2_WRITE_PATH = ROOT / "deploy" / "activist" / "governance_v2_write.php"
OPENAPI_PATH = ROOT / "deploy" / "activist" / "openapi-v2.yaml"
MIGRATION_PATH = (
    ROOT / "deploy" / "activist" / "migrations" / "011_global_terminal_v2.sql"
)
DOCS_PATH = ROOT / "docs" / "governance-api-v2.md"
GLOBAL_INGEST_PATH = ROOT / "curator" / "global_ingest.py"
GLOBAL_INGEST_WORKFLOW_PATH = ROOT / ".github" / "workflows" / "ingest-global.yml"

V2 = V2_PATH.read_text(encoding="utf-8")
V2_WRITE = V2_WRITE_PATH.read_text(encoding="utf-8")
MIGRATION = MIGRATION_PATH.read_text(encoding="utf-8")
DOCS = DOCS_PATH.read_text(encoding="utf-8")
GLOBAL_INGEST = GLOBAL_INGEST_PATH.read_text(encoding="utf-8")
GLOBAL_INGEST_WORKFLOW = GLOBAL_INGEST_WORKFLOW_PATH.read_text(encoding="utf-8")
SPEC = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))

PRODUCTION_SERVER = "https://alignpe.gabia.io/activist/api.php/api/v2"
PUBLIC_EVENT_FAMILIES = {
    "large_ownership",
    "meeting_and_vote",
    "tender_offer_and_mna",
    "capital_issuance",
    "capital_return",
    "board_and_compensation",
    "listing_status",
    "correction_and_withdrawal",
}
PUBLIC_VERIFICATION_STATUSES = {
    "official",
    "confirmed",
    "corroborated",
    "corrected",
    "withdrawn",
}
PUBLIC_IMPORTANCE_LEVELS = {
    "low",
    "medium",
    "high",
    "critical",
    "market_sensitive",
}
COMMON_EVENT_FILTERS = {
    "country",
    "market",
    "issuer_id",
    "event_family",
    "verification_status",
    "change_type",
    "from",
    "to",
}


def _parameter_names(operation: dict) -> set[str]:
    names: set[str] = set()
    for parameter in operation.get("parameters", []):
        if "$ref" in parameter:
            name = parameter["$ref"].rsplit("/", 1)[-1]
            names.add(SPEC["components"]["parameters"][name]["name"])
        else:
            names.add(parameter["name"])
    return names


def _local_refs(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "$ref" and isinstance(child, str) and child.startswith("#/"):
                yield child
            else:
                yield from _local_refs(child)
    elif isinstance(value, list):
        for child in value:
            yield from _local_refs(child)


def _resolve_local_ref(reference: str):
    current = SPEC
    for segment in reference.removeprefix("#/").split("/"):
        current = current[segment.replace("~1", "/").replace("~0", "~")]
    return current


def test_v2_spec_identifies_the_production_alpha_and_exact_server():
    assert SPEC["openapi"] == "3.1.0"
    assert SPEC["info"]["version"] == "2.0.0-alpha.2"
    assert SPEC["x-production-stage"] == "Production Alpha"
    assert SPEC["x-schema-version"] == 11
    assert SPEC["servers"] == [
        {"url": PRODUCTION_SERVER, "description": "Production API origin"}
    ]
    assert PRODUCTION_SERVER in DOCS
    assert "Production Alpha" in DOCS


def test_all_local_openapi_references_resolve():
    references = set(_local_refs(SPEC))
    assert references
    for reference in references:
        assert _resolve_local_ref(reference) is not None


def test_v2_schema_and_manifest_version_are_consistent():
    assert "const GOV_V2_SCHEMA_VERSION = 11;" in V2
    assert "const GOV_V2_RELEASE_STATE_KEY = 'global_terminal_v2';" in V2
    assert "'011_global_terminal_v2'" in V2
    assert "11,\n      '011_global_terminal_v2'" in MIGRATION
    assert "@bside_migration_011_sha256" in MIGRATION
    assert "011 source byte checksum missing or invalid" in MIGRATION
    assert (
        "'f16556d6fdf1c63a74352d9c671953b70b5f45a0d219fb01798f3571649786cd'"
        not in MIGRATION
    )
    assert "v2_expected_migration_manifest(string $migration011Checksum)" in V2
    assert "$identity['files'][$migrationPath]" in V2
    assert "migration_deployment_identity_unavailable" in V2
    assert SPEC["x-release-gate"]["state-key"] == "global_terminal_v2"


def test_global_ingest_receipt_schema_persists_batch_integrity_fields():
    receipt_ddl = MIGRATION.split(
        "CREATE TABLE IF NOT EXISTS activist_global_ingest_receipts (",
        1,
    )[1].split(
        ") ENGINE=InnoDB",
        1,
    )[0]
    for column in (
        "batch_id VARCHAR(77) NOT NULL",
        "chunk_index SMALLINT UNSIGNED NOT NULL",
        "chunk_count SMALLINT UNSIGNED NOT NULL",
        "window_start DATE NOT NULL",
        "window_end_exclusive DATE NOT NULL",
        "request_count INT UNSIGNED NOT NULL",
        "batch_raw_count INT UNSIGNED NOT NULL",
        "batch_acknowledged_count INT UNSIGNED NOT NULL",
        "batch_request_count INT UNSIGNED NOT NULL",
    ):
        assert column in receipt_ddl
    assert (
        "UNIQUE KEY uq_global_ingest_batch_chunk (\n"
        "    connector_id, batch_id, chunk_index\n"
        "  )"
    ) in receipt_ddl
    assert "KEY idx_global_ingest_batch (connector_id, batch_id)" in receipt_ddl
    assert "global ingest receipt column shape incomplete" in MIGRATION
    assert "0:3:connector_id,batch_id,chunk_index" in MIGRATION
    assert "1:2:connector_id,batch_id" in MIGRATION
    for field in (
        "`batch_id`",
        "`chunk_index`",
        "`chunk_count`",
        "`request_count`",
        "`batch_raw_count`",
        "`batch_acknowledged_count`",
        "`batch_request_count`",
    ):
        assert field in DOCS
    assert "`code_revision`도 포함한 결정적 해시" in DOCS
    assert "final 선행, 순서 역전" in DOCS
    assert "HTTP 409로 거절" in DOCS


def test_connector_activation_audit_and_brief_event_uniqueness_are_structural():
    audit_ddl = MIGRATION.split(
        "CREATE TABLE IF NOT EXISTS activist_global_connector_audit (",
        1,
    )[1].split(
        ") ENGINE=InnoDB",
        1,
    )[0]
    for column in (
        "audit_id VARCHAR(96) NOT NULL PRIMARY KEY",
        "connector_id VARCHAR(96) NOT NULL",
        "previous_status VARCHAR(24) NOT NULL",
        "new_status VARCHAR(24) NOT NULL",
        "reason VARCHAR(1000) NOT NULL",
        "changed_by VARCHAR(191) NOT NULL",
        "created_at DATETIME NOT NULL",
    ):
        assert column in audit_ddl
    assert (
        "KEY idx_global_connector_audit_connector (connector_id, created_at)"
    ) in audit_ddl
    assert "FOREIGN KEY (connector_id)" in audit_ddl
    assert "REFERENCES activist_source_connectors (connector_id)" in audit_ddl
    assert "ON UPDATE CASCADE" in audit_ddl
    assert "ON DELETE RESTRICT" in audit_ddl
    brief_ddl = MIGRATION.split(
        "CREATE TABLE IF NOT EXISTS activist_brief_items (",
        1,
    )[1].split(
        ") ENGINE=InnoDB",
        1,
    )[0]
    assert "UNIQUE KEY uq_brief_item_event (brief_id, event_id)" in brief_ddl
    assert "uq_brief_item_event_lane" not in brief_ddl
    assert "global connector audit column shape incomplete" in MIGRATION
    assert "AND ENGINE='InnoDB') <> 13 THEN" in MIGRATION
    assert "0:2:brief_id,event_id" in MIGRATION
    assert "1:2:connector_id,created_at" in MIGRATION
    assert "fk_global_connector_audit_connector" in MIGRATION


def test_spec_documents_only_dispatch_paths_that_exist_in_v2():
    expected_paths = {
        "/",
        "/health",
        "/openapi.yaml",
        "/openapi.json",
        "/briefs/latest",
        "/live",
        "/events",
        "/events/{event_id}",
        "/issuers",
        "/issuers/{issuer_id}",
        "/calendar",
        "/search",
        "/sources/status",
        "/exports/events.json",
        "/exports/events.csv",
        "/feeds/events.atom",
        "/ops/connectors/{connector_id}/checkpoint",
        "/ops/source-right-eligibility",
        "/ops/alpha-release-evidence",
        "/ops/release-state",
        "/ops/ingest",
        "/admin/connectors",
        "/admin/connectors/{connector_id}",
        "/admin/review-queue",
        "/admin/events/{event_id}/review",
        "/admin/brief-candidates",
        "/admin/briefs",
        "/admin/release-state",
        "/admin/release-authorizations",
        "/admin/cutover",
    }
    assert set(SPEC["paths"]) == expected_paths
    for literal_path in expected_paths - {
        "/",
        "/events/{event_id}",
        "/issuers/{issuer_id}",
        "/ops/connectors/{connector_id}/checkpoint",
        "/admin/connectors/{connector_id}",
        "/admin/events/{event_id}/review",
    }:
        assert f"'{literal_path}'" in V2
    assert "#^/events/" in V2
    assert "#^/issuers/" in V2
    assert "#^/ops/connectors/" in V2
    assert "#^/admin/connectors/" in V2
    assert "#^/admin/events/" in V2
    assert "function handle_v2_request" in V2


def test_release_gate_and_role_security_match_the_php_dispatch():
    gate = SPEC["x-release-gate"]
    assert gate["states"] == ["closed", "preview", "live"]
    assert gate["direct-state-endpoint-transitions"] == {
        "closed": ["preview"],
        "preview": ["closed"],
        "live": ["closed"],
    }
    assert gate["protected-atomic-transitions"] == {"preview": ["live"]}
    assert gate["always-available"] == ["/health", "/openapi.yaml", "/openapi.json"]
    assert gate["privileged-bypass"] == [
        "/ops/connectors/{connector_id}/checkpoint",
        "/ops/source-right-eligibility",
        "/ops/alpha-release-evidence",
        "/ops/ingest",
        "/ops/release-state",
        "/admin/release-state",
        "/admin/release-authorizations",
        "/admin/cutover",
        "/admin/connectors",
        "/admin/connectors/{connector_id}",
        "/admin/review-queue",
        "/admin/events/{event_id}/review",
        "/admin/brief-candidates",
        "/admin/briefs",
    ]
    assert "if ($state === 'closed')" in V2
    assert "if ($state === 'preview')" in V2
    assert "v2_require_preview_token($config);" in V2
    assert "global_terminal_release_closed" in V2
    assert "$current === 'preview' && $target === 'live'" in V2
    assert "v2_current_public_document_rights_guard" in V2
    assert "current_source_rights_invalid" in V2
    assert "closed → preview" in DOCS
    assert "preview → live" in DOCS
    assert "protected_atomic_cutover_required" in DOCS
    assert SPEC["paths"]["/ops/source-right-eligibility"]["get"]["security"] == [
        {"OpsBearer": []}
    ]
    assert SPEC["paths"]["/ops/ingest"]["post"]["security"] == [{"OpsBearer": []}]
    for route, method in (
        ("/admin/review-queue", "get"),
        ("/admin/events/{event_id}/review", "post"),
        ("/admin/brief-candidates", "get"),
        ("/admin/briefs", "post"),
    ):
        assert SPEC["paths"][route][method]["security"] == [{"EditorBearer": []}]
    assert SPEC["paths"]["/admin/release-state"]["get"]["security"] == [
        {"AdminBearer": []}
    ]
    assert SPEC["paths"]["/admin/release-state"]["post"]["security"] == [
        {"AdminBearer": []}
    ]
    assert SPEC["paths"]["/admin/connectors"]["get"]["security"] == [
        {"AdminBearer": []}
    ]
    assert SPEC["paths"]["/admin/connectors/{connector_id}"]["get"]["security"] == [
        {"AdminBearer": []}
    ]
    assert SPEC["paths"]["/admin/connectors/{connector_id}"]["post"]["security"] == [
        {"AdminBearer": []}
    ]
    assert "v2_require_role($config, array('ops'))" in V2
    assert "v2_require_role($config, array('editor'))" in V2
    assert "v2_require_role($config, array('admin'))" in V2


def test_admin_connector_contract_is_closed_audited_and_rights_gated():
    connector_parameter = SPEC["components"]["parameters"]["ConnectorId"]
    assert connector_parameter["name"] == "connector_id"
    assert connector_parameter["in"] == "path"
    assert connector_parameter["required"] is True
    assert connector_parameter["schema"]["pattern"] == (
        r"^connector:[a-z]{2}:[a-z0-9_.:\-]{1,64}$"
    )

    detail = SPEC["paths"]["/admin/connectors/{connector_id}"]
    assert detail["parameters"] == [{"$ref": "#/components/parameters/ConnectorId"}]
    assert detail["get"]["responses"]["200"]["content"]["application/json"][
        "schema"
    ] == {"$ref": "#/components/schemas/ConnectorAdminDetailEnvelope"}
    update = detail["post"]
    assert update["requestBody"]["required"] is True
    assert update["requestBody"]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/ConnectorUpdateRequest"
    }
    assert set(update["responses"]) == {
        "200",
        "400",
        "401",
        "403",
        "404",
        "409",
        "415",
        "503",
    }

    request = SPEC["components"]["schemas"]["ConnectorUpdateRequest"]
    assert request["additionalProperties"] is False
    assert set(request["required"]) == {
        "target_status",
        "expected_updated_at",
        "reason",
    }
    assert request["properties"]["target_status"]["enum"] == [
        "configured",
        "inactive",
    ]
    assert request["properties"]["expected_updated_at"]["format"] == "date-time"
    assert request["properties"]["reason"]["minLength"] == 8
    assert request["properties"]["reason"]["maxLength"] == 1000

    state = SPEC["components"]["schemas"]["ConnectorAdminState"]
    assert set(state["required"]) == {
        "connector_id",
        "country_code",
        "source_key",
        "source_name",
        "source_type",
        "base_url",
        "source_right_id",
        "coverage_mode",
        "connector_status",
        "schedule_minutes",
        "last_checked_at",
        "last_success_at",
        "last_error_class",
        "code_revision",
        "updated_at",
        "collect_eligibility",
    }
    eligibility = SPEC["components"]["schemas"]["ConnectorCollectEligibility"]
    assert eligibility["additionalProperties"] is False
    assert {
        "eligible",
        "identity_match",
        "ineligible_reasons",
        "rights_revision",
        "right_status",
        "valid_from",
        "valid_until",
        "revoked_at",
        "redistribution_allowed",
        "ai_allowed",
    } == set(eligibility["required"])
    assert (
        SPEC["components"]["schemas"]["ConnectorAdminDetailEnvelope"]["properties"][
            "data"
        ]["properties"]["audit_log"]["maxItems"]
        == 50
    )
    result = SPEC["components"]["schemas"]["ConnectorUpdateResult"]
    assert result["allOf"][1]["required"] == [
        "previous_status",
        "changed",
        "audit_id",
    ]

    assert "function v2_admin_connectors" in V2
    assert "function v2_admin_update_connector" in V2_WRITE
    assert "connector_update_validation_failed" in V2_WRITE
    assert "stale_connector_update" in V2_WRITE
    assert "connector_source_right_ineligible" in V2_WRITE
    assert "global_connector_audit" in V2
    assert "global_connector_audit" in V2_WRITE


def test_automated_ingest_is_idempotent_and_never_publishes_events():
    request = SPEC["components"]["schemas"]["GlobalIngestRequest"]
    assert set(request["required"]) == {
        "idempotency_key",
        "code_revision",
        "envelope",
    }
    assert request["properties"]["ingest_mode"]["enum"] == ["apply", "replay"]
    assert request["properties"]["ingest_mode"]["default"] == "apply"
    envelope = SPEC["components"]["schemas"]["GlobalIngestPayload"]
    assert envelope["properties"]["records"]["maxItems"] == 500
    assert envelope["properties"]["lifecycle_observations"]["maxItems"] == 500
    assert "chunk" in envelope["required"]
    assert envelope["properties"]["chunk"] == {
        "$ref": "#/components/schemas/GlobalIngestChunk"
    }
    chunk = SPEC["components"]["schemas"]["GlobalIngestChunk"]
    assert chunk["additionalProperties"] is False
    assert set(chunk["required"]) == {
        "index",
        "count",
        "batch_raw_count",
        "batch_acknowledged_count",
        "batch_request_count",
        "batch_id",
        "window_start",
        "window_end_exclusive",
    }
    assert "envelope.chunk: single chunk totals mismatch" in V2_WRITE
    assert "$chunk['batch_raw_count']" in V2_WRITE
    assert "$chunk['batch_acknowledged_count']" in V2_WRITE
    assert "$chunk['window_end_exclusive']" in V2_WRITE
    assert "$acknowledged = count($records) + count($observations)" in V2_WRITE
    assert "$envelope['raw_count'] < $acknowledged" in V2_WRITE
    assert (
        "$chunk['batch_raw_count'] < $chunk['batch_acknowledged_count']"
        in V2_WRITE
    )
    assert "smaller than accepted entity count" in V2_WRITE
    assert "global_ingest_idempotency_conflict" in V2_WRITE
    assert "global_ingest_code_revision_mismatch" in V2_WRITE
    assert "global_ingest_replay_missing" in V2_WRITE
    assert "unset($semantic['ingest_mode'])" in V2_WRITE
    record = SPEC["components"]["schemas"]["GlobalIngestRecord"]
    assert "metadata" in record["required"]
    assert (
        "server-normalized semantic record contract"
        in record["properties"]["content_hash"]["description"]
    )
    assert record["properties"]["metadata"]["additionalProperties"] == {
        "$ref": "#/components/schemas/CanonicalMetadataValue"
    }
    assert "function v2_global_document_content_hash" in V2_WRITE
    assert "v2_global_document_content_hash(" in V2_WRITE
    assert "content_hash: semantic contract mismatch" in V2_WRITE
    assert "global_document_hash_contract_conflict" in V2_WRITE
    assert "'source_type' => (string)$right['source_type']" in V2_WRITE
    assert "'public_allowed' => $record['public_allowed']" in V2_WRITE
    assert "'ai_allowed' => $record['ai_allowed']" in V2_WRITE
    assert "floats are not cross-runtime canonical" in V2_WRITE
    assert "v2_source_right_row(" in V2_WRITE
    assert "\\'pending\\',\\'draft\\'" in V2_WRITE
    assert "\\'needs_review\\',NULL" in V2_WRITE
    assert "'public_events_created' => 0" in V2_WRITE
    assert "자동 수집은 사건을 직접 공개하지 않는다" in DOCS
    operation = SPEC["paths"]["/ops/ingest"]["post"]
    assert operation["x-rejected-connector-ids"] == ["connector:kr:dart"]
    assert "connector:kr:dart" in V2_WRITE
    assert "OpenDART uses the established official-ingest pipeline" in V2_WRITE
    assert "한국 공시는 기존 `official-ingest` 파이프라인" in DOCS


def test_editor_review_is_optimistic_and_official_evidence_gated():
    approval = SPEC["components"]["schemas"]["EventApprovalRequest"]
    assert {
        "expected_updated_at",
        "identity_action",
        "identity_target",
        "identity_effective_at",
        "importance",
        "summary",
        "current_status",
        "actor",
    } <= set(approval["required"])
    assert "event_family" in approval["properties"]
    assert "merge_into_event_id" in approval["properties"]
    assert "WHERE event_id=? LIMIT 1 FOR UPDATE" in V2_WRITE
    assert "stale_event_review" in V2_WRITE
    assert "event_official_evidence_required" in V2_WRITE
    assert "editor classification required" in V2_WRITE
    assert "merge_requires_explicit_target" in V2_WRITE
    assert "'decision' => 'merged'" in V2_WRITE
    assert "'canonical_event_id' => $mergeIntoEventId" in V2_WRITE
    assert "서버는 자동 병합하지 않는다" in DOCS
    assert "`decision=merged`" in DOCS
    assert "editor 전용" in DOCS


def test_review_queue_updated_at_is_rfc3339_and_round_trips_unchanged():
    updated_at = SPEC["components"]["schemas"]["ReviewQueueItem"]["properties"][
        "updated_at"
    ]
    assert updated_at["type"] == "string"
    assert updated_at["format"] == "date-time"
    assert updated_at["pattern"] == (r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
    assert "exact value as expected_updated_at" in updated_at["description"]

    review_queue = V2_WRITE[
        V2_WRITE.index("function v2_admin_review_queue") : V2_WRITE.index(
            "function v2_admin_review_event"
        )
    ]
    assert "$row['updated_at'] = v1_release_iso_time(" in review_queue

    smoke = (ROOT / "tests" / "php73_global_v2_smoke.py").read_text(encoding="utf-8")
    assert "def require_rfc3339_utc(value: object, field: str) -> str:" in smoke
    assert "expected_updated_at = require_rfc3339_utc(" in smoke
    assert "mysql_timestamp_as_utc" not in smoke
    helper = runpy.run_path(str(ROOT / "tests" / "php73_global_v2_smoke.py"))[
        "require_rfc3339_utc"
    ]
    timestamp = "2026-07-25T01:02:03Z"
    assert helper(timestamp, "updated_at") == timestamp
    for invalid in (
        "2026-07-25 01:02:03",
        "2026-07-25T01:02:03.123Z",
        "2026-07-25T01:02:03+00:00",
    ):
        with pytest.raises(AssertionError):
            helper(invalid, "updated_at")


def test_event_actor_review_and_merge_populate_required_updated_at():
    merge = V2_WRITE[
        V2_WRITE.index("function v2_merge_reviewed_event") : V2_WRITE.index(
            "function v2_admin_review_queue"
        )
    ]
    assert (
        "' (event_id,actor_id,actor_role,review_status,created_at,updated_at)'"
        in merge
    )
    assert "' SELECT ?,actor_id,actor_role,review_status,created_at,? FROM '" in merge
    assert "$canonicalEventId,\n        $now," in merge

    review = V2_WRITE[
        V2_WRITE.index("function v2_admin_review_event") : V2_WRITE.index(
            "function v2_admin_brief_candidates"
        )
    ]
    assert (
        "' (event_id,actor_id,actor_role,review_status,created_at,updated_at)'"
        in review
    )
    assert "' VALUES (?,?,?,\\'approved\\',?,?)'" in review
    assert ". 'review_status=\\'approved\\',updated_at=VALUES(updated_at)'" in review


def test_public_event_and_release_state_timestamps_are_canonical_utc():
    utc_pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$"
    event = SPEC["components"]["schemas"]["Event"]
    event_time_fields = {
        "occurred_at",
        "filed_at",
        "first_observed_at",
        "updated_at",
        "deadline_at",
    }
    assert event_time_fields <= set(event["required"])
    for field in event_time_fields:
        schema = event["properties"][field]
        assert schema["format"] == "date-time"
        assert schema["pattern"] == utc_pattern

    document = SPEC["components"]["schemas"]["Document"]["properties"]
    for field in ("filed_at", "published_at"):
        assert document[field]["pattern"] == utc_pattern
    observation = SPEC["components"]["schemas"]["Observation"]["properties"]
    for field in ("first_observed_at", "observed_at"):
        assert observation[field]["pattern"] == utc_pattern

    release = SPEC["components"]["schemas"]["ReleaseStateEnvelope"]["properties"][
        "data"
    ]["properties"]
    for field in ("cutover_at", "sunset_at", "updated_at"):
        assert release[field]["format"] == "date-time"
        assert release[field]["pattern"] == utc_pattern
    audit = release["history"]["items"]
    assert audit["additionalProperties"] is False
    for field in ("cutover_at", "sunset_at", "created_at"):
        assert audit["properties"][field]["format"] == "date-time"
        assert audit["properties"][field]["pattern"] == utc_pattern

    normalizer = V2[
        V2.index("function v2_public_iso_time") : V2.index("function v2_query_events")
    ]
    assert "v1_release_iso_time($value)" in normalizer
    assert "invalid_public_timestamp_" in normalizer
    for field in event_time_fields:
        assert f"'{field}'" in normalizer

    release_handler = V2[
        V2.index("function v2_admin_release_state") : V2.index(
            "function v2_current_public_document_rights_guard"
        )
    ]
    assert "v2_normalize_public_time_fields(" in release_handler
    for field in ("cutover_at", "sunset_at", "updated_at", "created_at"):
        assert f"'{field}'" in release_handler

    ui = (ROOT / "public" / "governance" / "app.js").read_text(encoding="utf-8")
    assert "const UTC_DATE_TIME_PATTERN =" in ui
    assert "function isUtcDateTime(value)" in ui
    assert "function isNullableUtcDateTime(value)" in ui
    for expression in (
        "isNullableUtcDateTime(value.occurred_at)",
        "isNullableUtcDateTime(value.filed_at)",
        "isNullableUtcDateTime(value.first_observed_at)",
        "isUtcDateTime(value.updated_at)",
        "isNullableUtcDateTime(value.deadline_at)",
    ):
        assert expression in ui


def test_brief_publication_freezes_top_five_or_an_explicit_empty_reason():
    brief = SPEC["components"]["schemas"]["BriefPublicationRequest"]
    assert brief["properties"]["items"]["maxItems"] == 105
    assert (
        SPEC["components"]["schemas"]["BriefPublicationEnvelope"]["properties"]["data"][
            "properties"
        ]["top_count"]["maximum"]
        == 5
    )
    assert "$laneCounts['top'] > 5" in V2_WRITE
    assert "$eventKey = $eventId;" in V2_WRITE
    assert (
        "same event in more than one"
        in SPEC["paths"]["/admin/briefs"]["post"]["description"]
    )
    assert "brief_top_official_evidence_required" in V2_WRITE
    assert "brief_empty_reason_mismatch" in V2_WRITE
    assert "no_confirmed_material_events" in V2_WRITE
    assert "coverage_unavailable" in V2_WRITE
    assert "event_snapshot_json" in V2_WRITE
    assert "brief_edition_conflict" in V2_WRITE
    assert "array('KR', 'US', 'JP', 'GB', 'CA', 'AU')" in V2_WRITE
    assert "global edition에서는 6개국 각각" in DOCS
    assert "Top은 최대 5건" in DOCS


def test_latest_brief_uses_immutable_snapshot_and_never_hides_latest_outage():
    latest = V2[
        V2.index("function v2_brief_event_rows") : V2.index("function v2_calendar")
    ]
    assert "event_snapshot_json" in latest
    assert "LIMIT 1" in latest
    assert "LIMIT 20" not in latest
    assert "$brief = $statement->fetch();" in latest
    assert "'stale'" in latest
    assert "$emptyReason === 'coverage_unavailable'" in latest
    assert "'partial_coverage'" in latest
    assert "'scope' => $blockingCoverage ? 'blocking' : 'warning'" in latest
    assert "$emptyReason = 'coverage_unavailable';" in latest
    assert "'coverage_notice' => $coverageNotice" in latest
    assert "current_source_url" in latest
    assert "unset($snapshot['source_url'])" in latest
    assert "!isset($snapshot['title_provenance'])" in latest
    assert "array('source', 'generated_metadata', 'operator_metadata')" in latest
    assert "v2_document_visibility_sql('current_url_d', 'current_url_sr')" in latest
    assert "unset($normalizedRow['source_url'])" in V2_WRITE
    brief_schema = SPEC["components"]["schemas"]["BriefEnvelope"]["properties"]["data"]
    assert "coverage_notice" in brief_schema["required"]
    notice = brief_schema["properties"]["coverage_notice"]
    assert set(notice["properties"]["reason"]["enum"]) == {
        "coverage_unavailable",
        "partial_coverage",
    }
    assert set(notice["properties"]["scope"]["enum"]) == {"blocking", "warning"}
    assert {
        "unavailable_countries",
        "unavailable_sources",
    }.issubset(set(notice["required"]))
    assert "항상 canonical latest" in DOCS
    assert "이전 발행본으로 되돌아가지 않고" in DOCS


def test_response_budget_and_pagination_limits_match_php():
    assert SPEC["x-response-budget"]["maximum-bytes"] == 250000
    assert SPEC["x-pagination"] == {
        "default-items": 25,
        "maximum-items": 100,
        "maximum-page": 100,
        "maximum-offset": 10000,
        "continuation": "next_offset",
        "rule": "page and offset are mutually exclusive",
    }
    assert "const V2_RESPONSE_BUDGET_BYTES = 250000;" in V2
    assert "const V2_DEFAULT_PAGE_SIZE = 25;" in V2
    assert "const V2_MAX_PAGE_SIZE = 100;" in V2
    assert "const V2_MAX_PAGE_NUMBER = 100;" in V2
    assert "const V2_MAX_OFFSET = 10000;" in V2
    limit = SPEC["components"]["parameters"]["Limit"]["schema"]
    assert limit["default"] == 25
    assert limit["maximum"] == 100
    assert SPEC["components"]["parameters"]["Page"]["schema"]["maximum"] == 100
    assert SPEC["components"]["parameters"]["Offset"]["schema"] == {
        "type": "integer",
        "minimum": 0,
        "maximum": 10000,
    }
    page_meta = SPEC["components"]["schemas"]["PageMeta"]
    assert {
        "offset",
        "next_offset",
        "continuation_limited",
    } <= set(page_meta["required"])
    assert "ambiguous_pagination" in V2
    assert "v2_fit_json_list_page($rows, $page, $hasMore)" in V2
    for route in ("/live", "/events", "/issuers", "/calendar", "/search"):
        assert "offset" in _parameter_names(SPEC["paths"][route]["get"])


def test_event_detail_returns_only_approved_active_actors_with_roles():
    detail = V2[
        V2.index("function v2_get_event") : V2.index("function v2_list_issuers")
    ]
    assert "ea.review_status=\\'approved\\'" in detail
    assert "a.review_status=\\'approved\\'" in detail
    assert "a.record_status=\\'active\\'" in detail
    assert "ea.actor_role" in detail
    assert "'actors' => $actors->fetchAll()" in detail
    schema = SPEC["components"]["schemas"]["EventDetailEnvelope"]["properties"]["data"]
    assert "actors" in schema["required"]
    assert schema["properties"]["actors"]["items"] == {
        "$ref": "#/components/schemas/PublicEventActor"
    }
    actor = SPEC["components"]["schemas"]["PublicEventActor"]
    assert {"actor_id", "display_name", "actor_type", "actor_role"} == set(
        actor["required"]
    )


def test_csv_export_neutralizes_spreadsheet_formula_cells():
    helper = V2[
        V2.index("function v2_csv_safe_cell") : V2.index(
            "function v2_export_events_csv"
        )
    ]
    assert "[=+\\-@]" in helper
    assert "return '\\'' . $text;" in helper
    export = V2[
        V2.index("function v2_export_events_csv") : V2.index("function v2_atom_escape")
    ]
    assert "v2_csv_safe_cell(" in export


def test_all_event_collection_routes_document_the_common_filters():
    for route in (
        "/live",
        "/events",
        "/calendar",
        "/search",
        "/exports/events.json",
        "/exports/events.csv",
        "/feeds/events.atom",
    ):
        names = _parameter_names(SPEC["paths"][route]["get"])
        assert COMMON_EVENT_FILTERS <= names, route
    assert "q" in _parameter_names(SPEC["paths"]["/search"]["get"])
    for key in COMMON_EVENT_FILTERS:
        assert f"'{key}'" in V2


def test_search_covers_public_terminal_fields_and_rights_gated_evidence():
    search = V2[
        V2.index("if ($includeQuery)") : V2.index("return array($where, $params);")
    ]
    for expression in (
        "e.current_status",
        "e.global_event_family",
        "i.legal_name",
        "primary_listing.ticker",
        "primary_listing.market",
        "search_ii.identifier_value",
        "search_a.display_name",
        "search_d.title",
        "search_d.document_type",
    ):
        assert expression in search
    assert "v2_document_visibility_sql('search_d', 'search_sr')" in search
    assert "search_a.record_status=\\'active\\'" in search
    assert "$like = '%' . v2_like_literal($query) . '%';" in search
    assert "LIKE ? ESCAPE \\'!\\'" in search
    like_helper = V2[
        V2.index("function v2_like_literal") : V2.index("function v2_require_role")
    ]
    for literal in ("'!'", "'%'", "'_'", "'\\\\'"):
        assert literal in like_helper
    assert "licensed_telegram" not in SPEC["paths"]["/search"]["get"]["description"]
    assert "not a Telegram source" in SPEC["paths"]["/search"]["get"]["description"]


def test_json_csv_and_atom_exports_are_byte_bounded_and_continuable():
    for route in (
        "/exports/events.json",
        "/exports/events.csv",
        "/feeds/events.atom",
    ):
        names = _parameter_names(SPEC["paths"][route]["get"])
        assert {"page", "limit", "offset"} <= names
    export = V2[
        V2.index("function v2_export_event_rows") : V2.index(
            "function v2_admin_release_state"
        )
    ]
    assert "v2_list_params()" in export
    assert "v2_fit_json_list_page(" in export
    assert "ftruncate($stream" in export
    assert "X-BSIDE-Next-Offset" in export
    assert 'rel="next"' in export
    assert "strlen($prefix . $candidateEntries . $nextLink . '</feed>')" in export
    json_meta = SPEC["components"]["schemas"]["EventExportEnvelope"]["properties"][
        "meta"
    ]
    assert {"next_offset", "has_more", "continuation_limited"} <= set(
        json_meta["required"]
    )
    for route in ("/exports/events.csv", "/feeds/events.atom"):
        headers = SPEC["paths"][route]["get"]["responses"]["200"]["headers"]
        assert {"X-BSIDE-Next-Offset", "X-BSIDE-Has-More", "Link"} <= set(headers)


def test_public_country_family_and_status_enums_are_fail_closed():
    assert set(SPEC["components"]["schemas"]["CountryCode"]["enum"]) == {
        "KR",
        "US",
        "JP",
        "GB",
        "CA",
        "AU",
    }
    assert (
        set(SPEC["components"]["schemas"]["EventFamily"]["enum"])
        == PUBLIC_EVENT_FAMILIES
    )
    statuses = set(SPEC["components"]["schemas"]["PublicVerificationStatus"]["enum"])
    assert statuses == PUBLIC_VERIFICATION_STATUSES
    assert "signal" not in statuses
    importance = SPEC["components"]["schemas"]["EventImportance"]
    assert set(importance["enum"]) == PUBLIC_IMPORTANCE_LEVELS
    visibility = V2[
        V2.index("function v2_event_visibility_sql") : V2.index(
            "function v2_official_evidence_sql"
        )
    ]
    for family in PUBLIC_EVENT_FAMILIES:
        assert f"\\'{family}\\'" in visibility
    for status in PUBLIC_VERIFICATION_STATUSES:
        assert f"\\'{status}\\'" in visibility
    for value in PUBLIC_IMPORTANCE_LEVELS:
        assert f"\\'{value}\\'" in visibility
    assert "\\'signal\\'" not in visibility
    ingest_family = SPEC["components"]["schemas"]["IngestEventFamily"]
    assert any(
        branch.get("const") == "unclassified" for branch in ingest_family["oneOf"]
    )
    assert SPEC["components"]["schemas"]["Event"]["properties"]["event_family"] == {
        "$ref": "#/components/schemas/EventFamily"
    }
    event_schema = SPEC["components"]["schemas"]["Event"]
    assert "importance" in event_schema["required"]
    assert event_schema["properties"]["importance"] == {
        "$ref": "#/components/schemas/EventImportance"
    }
    event_select = V2[
        V2.index("function v2_event_select") : V2.index("function v2_event_filters")
    ]
    assert "e.importance" in event_select
    assert "AS actor_name" in event_select
    assert "AS actor_role" in event_select
    assert event_select.count(
        "ORDER BY event_actor.actor_role,event_actor.actor_id LIMIT 1"
    ) == 2
    assert event_schema["properties"]["actor_role"] == {
        "type": ["string", "null"],
        "description": (
            "Role of the representative approved actor returned in actor_name."
        ),
    }
    assert SPEC["components"]["schemas"]["ReviewQueueItem"]["properties"][
        "event_family"
    ] == {"$ref": "#/components/schemas/IngestEventFamily"}
    assert "unclassified" not in PUBLIC_EVENT_FAMILIES


def test_source_status_exposes_explicit_cadence_lag_and_freshness():
    status = SPEC["components"]["schemas"]["SourceStatus"]
    assert {
        "lag_minutes",
        "expected_cadence_minutes",
        "freshness_limit_minutes",
        "fresh",
        "collect_status",
        "collect_fresh",
        "public_status",
        "public_ready",
        "live_ready",
        "live_cursor_age_minutes",
    } <= set(status["properties"])
    assert {
        "expected_cadence_minutes",
        "freshness_limit_minutes",
        "fresh",
        "collect_status",
        "collect_fresh",
        "public_status",
        "public_ready",
        "live_ready",
    } <= set(status["required"])
    assert "$row['lag_minutes']" in V2
    assert "$row['expected_cadence_minutes']" in V2
    assert "$row['freshness_limit_minutes']" in V2
    assert "$row['fresh']" in V2
    assert "$row['collect_fresh']" in V2
    assert "$row['public_ready']" in V2
    assert "blocked_identity" in V2
    assert "blocked_identity" in status["properties"]["public_status"]["enum"]
    assert "redistribution_blocked" in V2
    assert "excluded_source" in V2
    assert "return min(45, max(15, $cadence * 3));" in V2
    assert "intraday_cursor_missing_or_stale" in V2
    assert "sec-current-v1:" in V2
    assert "v2_sec_current_cursor_age_minutes" in V2
    assert "rtrim(strtr(base64_encode($decoded), '+/', '-_'), '=')" in V2
    assert "$payload['schema_version'] !== 1" in V2
    assert "$liveCursorAge <= $limit" in V2
    assert "? 'delayed' : 'stale'" in V2
    assert "$cursorPayload['source_cursor'] = $normalized['next_cursor'];" in V2_WRITE
    assert "$sourceRow['public_ready'] === true" in V2_WRITE
    assert "`public_ready=true`" in DOCS
    envelope = SPEC["components"]["schemas"]["SourceStatusEnvelope"]["properties"][
        "data"
    ]
    assert {"required_source_ready", "all_required_ready"} <= set(
        envelope["required"]
    )
    assert "$requiredSourceReady" in V2
    assert "$allRequiredReady" in V2


def test_v2_source_right_identity_is_exactly_bound_on_every_public_gate():
    document_identity = V2[
        V2.index("function v2_document_source_right_identity_sql") : V2.index(
            "function v2_connector_source_right_identity_sql"
        )
    ]
    connector_identity = V2[
        V2.index("function v2_connector_source_right_identity_sql") : V2.index(
            "function v2_non_telegram_document_sql"
        )
    ]
    document_visibility = V2[
        V2.index("function v2_document_visibility_sql") : V2.index(
            "function v2_document_source_right_identity_sql"
        )
    ]
    source_status = V2[
        V2.index("function v2_source_status_data") : V2.index(
            "function v2_sources_status"
        )
    ]
    live_guard = V2[
        V2.index("function v2_current_public_document_rights_guard") : V2.index(
            "function v2_json_body"
        )
    ]

    assert ".source_right_id=" in document_identity
    assert "BINARY " in document_identity
    assert ".source_class=BINARY " in document_identity
    assert ".source_key=BINARY " in document_identity
    assert ".source_right_id=" in connector_identity
    assert ".source_type=BINARY " in connector_identity
    assert ".source_key=BINARY " in connector_identity
    assert "v2_document_source_right_identity_sql(" in document_visibility
    assert "v2_connector_source_right_identity_sql('sc', 'sr')" in source_status
    assert "THEN \\'blocked_identity\\'" in source_status
    assert "v2_document_source_right_identity_sql('d', 'sr')" in live_guard
    assert "ORDER BY BINARY sr.source_right_id FOR UPDATE" in live_guard
    atomic_cutover = V2[
        V2.index("function v2_admin_atomic_cutover") : V2.index(
            "function v2_admin_update_release_state"
        )
    ]
    assert atomic_cutover.index(
        "v2_lock_current_public_source_rights($pdo, $config)"
    ) < atomic_cutover.index("v2_current_public_document_rights_guard($pdo, $config)")
    assert atomic_cutover.index(
        "v1_current_public_document_rights_guard($pdo, $config)"
    ) < atomic_cutover.index("v2_current_public_document_rights_guard($pdo, $config)")
    live_transition_guard = SPEC["x-release-gate"]["live-transition-guard"]
    assert "source identity" in live_transition_guard
    assert "one-time authorization" in live_transition_guard


def test_atomic_cutover_locks_and_revalidates_all_six_required_sources():
    registry = V2[
        V2.index("function v2_required_alpha_source_identities") : V2.index(
            "function v2_required_alpha_source_rights_guard"
        )
    ]
    guard = V2[
        V2.index("function v2_required_alpha_source_rights_guard") : V2.index(
            "function v2_json_body"
        )
    ]
    expected = {
        (
            "connector:kr:dart",
            "KR",
            "dart",
            "official_disclosure",
            "official:dart",
            "market-wide",
        ),
        (
            "connector:us:sec-edgar",
            "US",
            "sec-edgar",
            "official_disclosure",
            "official:sec-edgar",
            "market-wide",
        ),
        (
            "connector:jp:edinet",
            "JP",
            "edinet",
            "official_disclosure",
            "official:edinet",
            "market-wide",
        ),
        (
            "connector:gb:companies-house",
            "GB",
            "companies-house",
            "official_register",
            "official:companies-house",
            "official-register",
        ),
        (
            "connector:ca:issuer-ir",
            "CA",
            "issuer-ir",
            "official_issuer",
            "official:ca-issuer-ir",
            "link-only",
        ),
        (
            "connector:au:asic-register",
            "AU",
            "asic-register",
            "official_register",
            "official:asic-register",
            "link-only",
        ),
    }
    for identity in expected:
        for value in identity:
            assert f"'{value}'" in registry
    assert registry.count("'connector_id' =>") == 6
    assert "ORDER BY BINARY connector_id FOR UPDATE" in guard
    assert "ORDER BY BINARY sr.source_right_id FOR UPDATE" in guard
    for field in (
        "country_code",
        "source_key",
        "source_type",
        "source_right_id",
        "coverage_mode",
    ):
        assert f"'{field}'" in guard
    for column in (
        "schedule_minutes",
        "last_checked_at",
        "last_success_at",
        "last_observed_at",
        "last_raw_count",
        "last_acknowledged_count",
        "last_error_class",
        "cursor_json",
    ):
        assert column in guard
    assert "v2_source_connector_readiness($connector)" in guard
    assert "v2_current_source_right_sql('sr')" in guard
    assert "source_right_redistribution_sql('sr')" in guard
    assert "AS collect_eligible" in guard
    assert "AS public_eligible" in guard
    assert "collect_not_allowed" in guard
    assert "public_redistribution_not_allowed" in guard
    release_guard = SPEC["x-release-gate"]["live-transition-guard"]
    assert "all six required Production Alpha connector" in release_guard
    assert "zero published documents" in release_guard
    conflict_schema = SPEC["paths"]["/admin/cutover"]["post"]["responses"]["409"][
        "content"
    ]["application/json"]["schema"]
    assert {
        item["$ref"] for item in conflict_schema["anyOf"]
    } == {
        "#/components/schemas/RequiredAlphaSourcesInvalidError",
        "#/components/schemas/ErrorEnvelope",
    }
    invalid_error = SPEC["components"]["schemas"]["RequiredAlphaSourcesInvalidError"]
    assert invalid_error["properties"]["required_connector_count"]["const"] == 6
    assert set(
        invalid_error["properties"]["invalid_sources"]["items"]["properties"][
            "reasons"
        ]["items"]["enum"]
    ) == {
        "connector_missing",
        "connector_identity_mismatch",
        "connector_not_active",
        "connector_has_error",
        "last_success_missing_or_stale",
        "last_checked_missing_or_stale",
        "intraday_cursor_missing_or_stale",
        "link_observation_missing_or_stale",
        "link_observation_not_acknowledged",
        "source_right_missing",
        "source_right_identity_mismatch",
        "collect_not_allowed",
        "public_redistribution_not_allowed",
    }

    atomic = V2[
        V2.index("function v2_admin_atomic_cutover") : V2.index(
            "function v2_admin_update_release_state"
        )
    ]
    required_guard = atomic.index(
        "v2_required_alpha_source_rights_guard($pdo, $config)"
    )
    public_guard = atomic.index(
        "v2_lock_current_public_source_rights($pdo, $config)"
    )
    consume = atomic.index(
        "UPDATE ' . table_name($config, 'release_authorizations')"
    )
    assert atomic.index("$pdo->beginTransaction()") < required_guard
    assert atomic.index(
        "v2_release_state_rows_for_update($pdo, $config)"
    ) < required_guard
    assert required_guard < public_guard < consume
    assert "required_alpha_sources_invalid" in atomic
    assert "$pdo->rollBack();" in atomic[required_guard:public_guard]


def test_release_state_rejects_unknown_fields_before_mutation():
    release_update = V2[
        V2.index("function v2_admin_update_release_state") : V2.index(
            "function v2_alpha_date_epoch"
        )
    ]
    assert "v2_write_assert_keys(" in release_update
    assert "'unknown_release_state_field'" in release_update
    assert release_update.index("v2_write_assert_keys(") < release_update.index(
        "$pdo->beginTransaction()"
    )
    assert (
        SPEC["components"]["schemas"]["ReleaseStateUpdate"]["additionalProperties"]
        is False
    )


def test_protected_cutover_requires_exact_authorizer_and_consumes_once_atomically():
    authorization_ddl = MIGRATION.split(
        "CREATE TABLE IF NOT EXISTS activist_release_authorizations (",
        1,
    )[1].split(
        ") ENGINE=InnoDB",
        1,
    )[0]
    for column in (
        "candidate_sha CHAR(40) NOT NULL",
        "evidence_artifact_digest CHAR(71) NOT NULL",
        "evidence_run_id BIGINT UNSIGNED NOT NULL",
        "evidence_artifact_id BIGINT UNSIGNED NOT NULL",
        "nonce_sha256 CHAR(64) NOT NULL",
        "expected_v1_state_version BIGINT UNSIGNED NOT NULL",
        "expected_v2_state_version BIGINT UNSIGNED NOT NULL",
        "expires_at DATETIME NOT NULL",
        "fully_consumed_at DATETIME NULL",
    ):
        assert column in authorization_ddl
    assert "release_nonce" not in authorization_ddl
    assert "UNIQUE KEY uq_release_authorization_nonce (nonce_sha256)" in authorization_ddl
    assert "release_authorization_id" in MIGRATION

    exact_role = V2[
        V2.index("function v2_require_exact_role") : V2.index(
            "function v2_require_preview_token"
        )
    ]
    assert "v1_role_hashes($config, $requiredRole)" in exact_role
    assert "array_merge" not in exact_role
    dispatch = V2[V2.index("function handle_v2_request") :]
    assert (
        "$role = v2_require_exact_role($config, 'release_authorizer');"
        in dispatch
    )
    assert dispatch.index("'/admin/release-authorizations'") < dispatch.index(
        "$path === '/admin/release-state'"
    )

    issue = V2[
        V2.index("function v2_admin_issue_release_authorization") : V2.index(
            "function v2_atomic_cutover_fields"
        )
    ]
    for binding in (
        "candidate_sha",
        "evidence_artifact_digest",
        "evidence_run_id",
        "evidence_artifact_id",
        "nonce_sha256",
        "expected_v1_state_version",
        "expected_v2_state_version",
        "expires_at",
    ):
        assert binding in issue
    assert "v2_assert_deployed_candidate($fields)" in issue
    assert "V2_RELEASE_AUTHORIZATION_MIN_TTL_SECONDS" in issue
    assert "V2_RELEASE_AUTHORIZATION_MAX_TTL_SECONDS" in issue
    assert "release_nonce" not in issue.split("v2_respond(201", 1)[1]

    atomic = V2[
        V2.index("function v2_admin_atomic_cutover") : V2.index(
            "function v2_admin_update_release_state"
        )
    ]
    assert "$pdo->beginTransaction()" in atomic
    assert "v2_release_state_rows_for_update($pdo, $config)" in atomic
    assert "LIMIT 1 FOR UPDATE" in atomic
    assert "release_authorization_replayed" in atomic
    assert "release_authorization_expired" in atomic
    assert "release_authorization_binding_mismatch" in atomic
    assert atomic.count("new_state") >= 1
    assert "release_authorization_id" in atomic
    assert "fully_consumed_at" in atomic
    assert atomic.index("fully_consumed_at") < atomic.index("$pdo->commit()")
    assert "WHERE authorization_id=? AND fully_consumed_at IS NULL" in atomic

    direct = V2[
        V2.index("function v2_admin_update_release_state") : V2.index(
            "function v2_alpha_date_epoch"
        )
    ]
    assert "'protected_atomic_cutover_required'" in direct
    assert "v2_admin_atomic_cutover($pdo, $config, (string)$role)" in dispatch

    assert SPEC["paths"]["/admin/release-authorizations"]["post"]["security"] == [
        {"ReleaseAuthorizerBearer": []}
    ]
    assert SPEC["paths"]["/admin/cutover"]["post"]["security"] == [
        {"AdminBearer": []}
    ]
    assert SPEC["paths"]["/ops/release-state"]["get"]["security"] == [
        {"OpsBearer": []}
    ]
    smoke = (ROOT / "tests" / "php73_global_v2_smoke.py").read_text(
        encoding="utf-8"
    )
    direct_v2 = smoke[
        smoke.index("direct_v2_live, _ = request_json") : smoke.index(
            "direct_v1_live, _ = request_json"
        )
    ]
    direct_v1 = smoke[
        smoke.index("direct_v1_live, _ = request_json") : smoke.index(
            "admin_cannot_authorize, _ = request_json"
        )
    ]
    assert '"expected_version": 1' in direct_v2
    assert '"expected_version": v1_preview_version' in direct_v1


def test_alpha_release_evidence_is_ops_only_and_database_derived():
    operation = SPEC["paths"]["/ops/alpha-release-evidence"]["get"]
    assert operation["security"] == [{"OpsBearer": []}]
    revision = operation["parameters"][0]
    assert revision["name"] == "code_revision"
    assert revision["required"] is True
    assert revision["schema"]["pattern"] == "^[a-f0-9]{40}$"
    assert (
        operation["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
        == "#/components/schemas/AlphaAutomatedEvidenceEnvelope"
    )
    exporter = V2[
        V2.index("function v2_alpha_global_connector_windows") : V2.index(
            "function handle_v2_request"
        )
    ]
    assert "global_ingest_receipts" in exporter
    assert "official_backfill_checkpoints" in exporter
    assert "hash_equals($payloadHash, hash('sha256', $raw))" in exporter
    assert "alpha_evidence_duplicate_window" in exporter
    assert "v2_alpha_latest_contiguous_windows" in exporter
    assert "'filtered_out_count' =>" in exporter
    assert "'accepted_count' =>" in exporter
    assert "$raw < $acknowledged" in exporter
    assert "$raw !== $acknowledged" not in exporter
    assert "(string)$event['title'] === (string)$document['title']" in exporter
    assert "production_database_export" in exporter
    assert "LIMIT 10001" in exporter

    window_schema = SPEC["components"]["schemas"]["AlphaCompletedWindow"]
    assert set(window_schema["required"]) == {
        "window_start",
        "window_end_exclusive",
        "raw_count",
        "filtered_out_count",
        "accepted_count",
        "acknowledged_count",
        "status",
        "code_revision",
        "receipt_sha256",
    }
    assert window_schema["additionalProperties"] is False


def test_v2_public_documents_and_urls_always_exclude_telegram():
    exclusion = V2[
        V2.index("function v2_non_telegram_document_sql") : V2.index(
            "function v2_current_source_right_sql"
        )
    ]
    assert "licensed_telegram" in exclusion
    assert "authorized_telegram" in exclusion
    visibility = V2[
        V2.index("function v2_document_visibility_sql") : V2.index(
            "function v2_event_visibility_sql"
        )
    ]
    assert "v2_non_telegram_document_sql($documentAlias)" in visibility
    assert "v2_non_telegram_document_sql('d')" in V2
    assert "비-Telegram" in DOCS


def test_ingest_idempotency_ignores_attempt_and_transport_telemetry():
    helper = V2_WRITE[
        V2_WRITE.index("function v2_write_ingest_idempotency_hash") : V2_WRITE.index(
            "function v2_source_right_row"
        )
    ]
    assert "unset($semantic['envelope']['retrieved_at'])" in helper
    assert "unset($semantic['envelope']['request_count'])" in helper
    assert (
        "unset($semantic['envelope']['chunk']['batch_request_count'])"
        in helper
    )
    assert "unset($record['first_observed_at'])" in helper
    for substantive_field in (
        "rights_revision",
        "raw_count",
        "lifecycle_observations",
    ):
        assert substantive_field not in helper
    assert "v2_write_ingest_idempotency_hash($payload)" in V2_WRITE


def test_partial_batch_allows_request_telemetry_to_change_on_retry():
    metadata = V2_WRITE[
        V2_WRITE.index("function v2_ingest_assert_batch_metadata") : V2_WRITE.index(
            "function v2_ingest_assert_batch_prefix"
        )
    ]
    complete = V2_WRITE[
        V2_WRITE.index("function v2_ingest_assert_batch_complete") : V2_WRITE.index(
            "function v2_ingest_locked_checkpoint"
        )
    ]
    assert "batch_request_count" not in metadata
    assert (
        "$requestTotal !== (int)$chunk['batch_request_count']"
        in complete
    )
    exporter = V2[
        V2.index("function v2_alpha_global_connector_windows") : V2.index(
            "function v2_alpha_dart_windows"
        )
    ]
    assert "$final = $batchRows[$chunkCount - 1]" in exporter
    assert "$requests !== (int)$final['batch_request_count']" in exporter
    assert (
        "$requests !== (int)$first['batch_request_count']"
        not in exporter
    )


def test_unchanged_overlap_observation_does_not_advance_live_timestamp():
    upsert = V2_WRITE[
        V2_WRITE.index("function v2_ingest_upsert_record") : V2_WRITE.index(
            "function v2_ingest_lifecycle_observation"
        )
    ]
    touch_start = upsert.index("$touchEvent = $pdo->prepare(")
    touch_end = upsert.index("$touchEvent->execute(", touch_start)
    unchanged_touch = upsert[touch_start:touch_end]
    assert "first_observed_at=LEAST" in unchanged_touch
    assert "updated_at" not in unchanged_touch

    new_version = upsert[
        upsert.index("if ($newVersion)") : touch_start
    ]
    assert "payload_json=?,updated_at=? WHERE event_id=?" in new_version

    lifecycle = V2_WRITE[
        V2_WRITE.index("function v2_ingest_lifecycle_observation") : V2_WRITE.index(
            "function v2_ops_ingest"
        )
    ]
    assert "function v2_lifecycle_semantic_hash" in V2_WRITE
    assert "WHERE observation_id=? LIMIT 1 FOR UPDATE" in lifecycle
    assert "Use the editor's event-then-association lock order" in lifecycle
    event_lock = lifecycle[
        lifecycle.index("$eventLock = $pdo->prepare(") :
        lifecycle.index("$eventLock->execute")
    ]
    association_lock = lifecycle[
        lifecycle.index("$associationLock = $pdo->prepare(") :
        lifecycle.index("$associationLock->execute")
    ]
    assert "governance_events" in event_lock
    assert "identity_status<>\\'merged\\'" in event_lock
    assert "review_status<>\\'merged\\'" in event_lock
    assert "LIMIT 1 FOR UPDATE" in event_lock
    assert "event_documents" in association_lock
    assert "event_id=? AND document_id=?" in association_lock
    assert "LIMIT 1 FOR UPDATE" in association_lock
    assert "global_lifecycle_observation_conflict" in lifecycle
    assert "global_lifecycle_resolution_conflict" in lifecycle
    assert "An exact replay is a complete no-op" in lifecycle
    assert "ON DUPLICATE KEY UPDATE" not in lifecycle
    assert "identity_status=\\'needs_review\\',comparison_key=NULL,updated_at=?" in lifecycle

    smoke = (ROOT / "tests" / "php73_global_v2_smoke.py").read_text(
        encoding="utf-8"
    )
    assert "php73-v2-sec-unchanged-overlap" in smoke
    assert "a semantic document version must advance event.updated_at" in smoke
    assert "an editorial approval must advance event.updated_at" in smoke
    assert "php73-v2-monotonic-lifecycle" in smoke
    assert "php73-v2-monotonic-lifecycle-replay" in smoke
    assert "an exact lifecycle replay must be a canonical no-op" in smoke
    assert "php73-v2-monotonic-lifecycle-withdrawal" in smoke


def test_document_versions_and_lifecycle_are_bounded_by_source_right():
    upsert = V2_WRITE[
        V2_WRITE.index("function v2_ingest_upsert_record") : V2_WRITE.index(
            "function v2_ingest_lifecycle_observation"
        )
    ]
    assert "' WHERE source_right_id=? AND external_id=?'" in upsert
    assert "' WHERE source_class=? AND external_id=?'" not in upsert
    assert '$record[\'record_id\'] . "\\x1f" . $versionNo . "\\x1f"' in upsert
    assert "Only the latest version can make a retry idempotent" in upsert
    lifecycle = V2_WRITE[
        V2_WRITE.index("function v2_ingest_lifecycle_observation") : V2_WRITE.index(
            "function v2_ops_ingest"
        )
    ]
    assert (
        "' WHERE source_right_id=? AND source_key=? AND external_id IN (?,?)'"
        in lifecycle
    )
    assert "(string)$connector['source_right_id']" in lifecycle


def test_ingest_url_validator_rejects_encoded_credentials_and_ambiguity():
    validator = V2_WRITE[
        V2_WRITE.index("function v2_write_https_url") : V2_WRITE.index(
            "function v2_write_is_list"
        )
    ]
    for forbidden in (
        "token",
        "secret",
        "key",
        "signature",
        "credential",
        "x-amz-",
        "x-goog-",
    ):
        assert forbidden in validator
    assert "rawurldecode" in validator
    assert "malformed URL query key" in validator
    assert "ambiguous URL query key" in validator
    assert "$seenKeys" in validator


def test_connector_checkpoint_is_ops_only_and_contains_no_source_evidence():
    operation = SPEC["paths"]["/ops/connectors/{connector_id}/checkpoint"]["get"]
    assert operation["security"] == [{"OpsBearer": []}]
    envelope = SPEC["components"]["schemas"]["ConnectorCheckpointEnvelope"]
    data = envelope["properties"]["data"]
    assert data["additionalProperties"] is False
    assert set(data["required"]) == {
        "connector_id",
        "cursor_json",
        "last_success_at",
        "last_checked_at",
        "code_revision",
    }
    assert not {
        "source_right_id",
        "evidence_uri",
        "evidence_sha256",
        "rights_revision",
    } & set(data["properties"])
    assert "function v2_ops_connector_checkpoint" in V2
    assert "v2_ops_connector_checkpoint($pdo, $config, $matches[1])" in V2
    assert "invalid_connector_checkpoint" in V2
    assert "'schema_version' => 1" in V2_WRITE
    assert "'window_end_exclusive' => $chunk['window_end_exclusive']" in V2_WRITE
    assert "$chunk['batch_raw_count']" in V2_WRITE
    assert "$chunk['batch_acknowledged_count']" in V2_WRITE


def test_final_chunk_requires_a_complete_consistent_receipt_batch():
    for column in (
        "batch_id VARCHAR(77) NOT NULL",
        "chunk_index SMALLINT UNSIGNED NOT NULL",
        "chunk_count SMALLINT UNSIGNED NOT NULL",
        "window_start DATE NOT NULL",
        "window_end_exclusive DATE NOT NULL",
        "request_count INT UNSIGNED NOT NULL",
        "batch_raw_count INT UNSIGNED NOT NULL",
        "batch_acknowledged_count INT UNSIGNED NOT NULL",
        "batch_request_count INT UNSIGNED NOT NULL",
    ):
        assert column in MIGRATION
    assert "UNIQUE KEY uq_global_ingest_batch_chunk" in MIGRATION
    assert "connector_id, batch_id, chunk_index" in MIGRATION
    assert "function v2_ingest_assert_batch_prefix" in V2_WRITE
    assert "function v2_ingest_assert_batch_complete" in V2_WRITE
    assert "global_ingest_chunk_out_of_order" in V2_WRITE
    assert "global_ingest_batch_metadata_conflict" in V2_WRITE
    assert "global_ingest_batch_totals_mismatch" in V2_WRITE
    insert_position = V2_WRITE.index(
        "'INSERT INTO ' . table_name($config, 'global_ingest_receipts')"
    )
    completeness_position = V2_WRITE.index(
        "v2_ingest_assert_batch_complete",
        insert_position,
    )
    checkpoint_position = V2_WRITE.index(
        "'schema_version' => 1",
        completeness_position,
    )
    assert insert_position < completeness_position < checkpoint_position
    assert "$normalized['request_count']" in V2_WRITE
    assert "function v2_ingest_checkpoint_should_advance" in V2_WRITE
    checkpoint_helper = V2_WRITE[
        V2_WRITE.index("function v2_ingest_checkpoint_should_advance") :
        V2_WRITE.index("function v2_ops_ingest")
    ]
    assert "strcmp($incomingEnd, $existingEnd)" in checkpoint_helper
    assert "$normalized['next_cursor'] !== null" in checkpoint_helper
    assert "global_connector_checkpoint_corrupt" in V2_WRITE


def test_unclassified_sec_and_edinet_candidates_preserve_source_fields():
    description = SPEC["components"]["schemas"]["IngestEventFamily"]["description"]
    assert "8-K/8-K/A" in description
    assert "EDINET extraordinary reports" in description
    record = SPEC["components"]["schemas"]["GlobalIngestRecord"]["properties"]
    assert "source language" in record["title"]["description"]
    assert record["original_url"]["pattern"] == "^https://"
    assert "SEC 일일 인덱스의 `8-K`/`8-K/A`" in DOCS
    assert "EDINET 임시보고서" in DOCS
    assert "원문 제목" in DOCS


def test_title_provenance_is_required_across_ingest_public_events_and_exports():
    provenance = SPEC["components"]["schemas"]["TitleProvenance"]
    assert provenance["enum"] == [
        "source",
        "generated_metadata",
        "operator_metadata",
    ]
    event = SPEC["components"]["schemas"]["Event"]
    assert "title_provenance" in event["required"]
    assert event["properties"]["title_provenance"] == {
        "$ref": "#/components/schemas/TitleProvenance"
    }
    record = SPEC["components"]["schemas"]["GlobalIngestRecord"]
    assert "title_provenance" not in record["required"]
    metadata = record["properties"]["metadata"]
    assert "title_provenance" in metadata["required"]
    assert metadata["properties"]["title_provenance"] == {
        "$ref": "#/components/schemas/TitleProvenance"
    }
    visibility = V2[
        V2.index("function v2_event_visibility_sql") : V2.index(
            "function v2_official_evidence_sql"
        )
    ]
    assert "$.metadata.title_provenance" in visibility
    assert "generated_metadata" in visibility
    event_select = V2[
        V2.index("function v2_event_select") : V2.index("function v2_event_filters")
    ]
    assert "AS title_provenance" in event_select
    csv = V2[
        V2.index("function v2_export_events_csv") : V2.index("function v2_atom_escape")
    ]
    assert "'title_provenance'," in csv
    assert ".metadata.title_provenance: invalid value" in V2_WRITE
    review = SPEC["components"]["schemas"]["ReviewQueueItem"]
    assert "title_provenance" in review["required"]
    assert review["properties"]["title_provenance"] == {
        "$ref": "#/components/schemas/TitleProvenance"
    }


def test_global_ingest_runner_contract_and_configuration_are_documented():
    assert "OpenDART deliberately does not use this module" in GLOBAL_INGEST
    assert "SecHybridConnector" in GLOBAL_INGEST
    assert "EdinetDocumentsConnector" in GLOBAL_INGEST
    assert "CompaniesHouseFilingHistoryConnector" in GLOBAL_INGEST
    assert "default_completed_window" in GLOBAL_INGEST
    assert "source_right_changed_before_ingest" in GLOBAL_INGEST
    for country in ("US", "JP", "GB"):
        assert f"- {country}" in GLOBAL_INGEST_WORKFLOW
    for setting in (
        "BSIDE_API_BASE_URL",
        "BSIDE_OPS_TOKEN",
        "SEC_EDGAR_USER_AGENT",
        "EDINET_API_KEY",
        "COMPANIES_HOUSE_API_KEY",
        "COMPANIES_HOUSE_ISSUERS_JSON",
    ):
        assert setting in GLOBAL_INGEST_WORKFLOW
        assert setting in DOCS
    assert "17,47 * * * *" in GLOBAL_INGEST_WORKFLOW
    assert "ACK가 실제 record와 lifecycle observation 합계" in DOCS


def test_coverage_modes_and_initial_scope_are_not_presented_as_equal_recall():
    assert set(SPEC["components"]["schemas"]["CoverageMode"]["enum"]) == {
        "market-wide",
        "official-register",
        "selected-issuers",
        "link-only",
        "unavailable",
    }
    assert "동일한 시장 재현율을 보장하지 않는다" in DOCS
    assert "SEDAR+ 전문 수집·재배포 제외" in DOCS
    assert "ASX 공시 전문 수집·재배포 제외" in DOCS
    assert "link-only / manual-metadata" in DOCS
    for mode in ("market-wide", "official-register", "link-only"):
        assert f"'{mode}'" in MIGRATION


def test_machine_readable_alpha_scope_marks_ca_and_au_as_manual_links():
    scope = SPEC["x-production-alpha-source-scope"]
    assert scope["CA"] == {
        "coverage-mode": "link-only",
        "ingest-mode": "manual-metadata",
        "host-policy": "issuer-bound-provenance",
    }
    assert scope["AU"] == {
        "coverage-mode": "link-only",
        "ingest-mode": "manual-metadata",
        "host-policy": "official-asic-hosts-only",
    }
    assert scope["GB"]["coverage-mode"] == "official-register"
    for country in ("KR", "US", "JP"):
        assert scope[country]["coverage-mode"] == "market-wide"


def test_docs_do_not_claim_unimplemented_v2_mutation_or_distribution_routes():
    forbidden_claims = (
        "POST /feedback",
        "POST /admin/ingest",
        "POST /metrics/web-vitals",
        "Telegram 발송",
    )
    for claim in forbidden_claims:
        assert claim not in DOCS
    assert "투자 추천" in DOCS
    assert "자동 번역하지 않는다" in DOCS
