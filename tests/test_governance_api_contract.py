from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
API = (ROOT / "deploy" / "activist" / "api.php").read_text(encoding="utf-8")
V1 = (ROOT / "deploy" / "activist" / "governance_v1.php").read_text(encoding="utf-8")
MIGRATION = (
    ROOT / "deploy" / "activist" / "migrations" / "001_governance_v1.sql"
).read_text(encoding="utf-8")
LINEAGE_MIGRATION = (
    ROOT / "deploy" / "activist" / "migrations" / "002_legacy_source_right_lineage.sql"
).read_text(encoding="utf-8")
EDITORIAL_MIGRATION = (
    ROOT / "deploy" / "activist" / "migrations" / "003_editorial_governance.sql"
).read_text(encoding="utf-8")
SIGNAL_REBUILD_MIGRATION = (
    ROOT
    / "deploy"
    / "activist"
    / "migrations"
    / "004_telegram_signal_rebuild_staging.sql"
).read_text(encoding="utf-8")
TELEGRAM_IDENTITY_INDEX_MIGRATION = (
    ROOT
    / "deploy"
    / "activist"
    / "migrations"
    / "005_telegram_channel_identity_index.sql"
).read_text(encoding="utf-8")
OPENAPI_PATH = ROOT / "deploy" / "activist" / "openapi.yaml"
HTACCESS = (ROOT / "deploy" / "activist" / ".htaccess").read_text(encoding="utf-8")


def test_governance_schema_contains_all_required_entities():
    required_tables = {
        "companies",
        "actors",
        "source_rights",
        "documents",
        "governance_events",
        "campaigns",
        "claim_evidence",
        "proposal_votes",
        "commitment_outcomes",
        "timeline_entries",
        "editorial_revisions",
        "campaign_documents",
        "editorial_ingest_chunks",
        "delivery_outbox",
    }
    for table in required_tables:
        assert f"'{table}'" in API
        assert f"activist_{table}" in MIGRATION


def test_operational_state_tables_and_leases_are_migrated():
    for table in ("collection_runs", "feedback", "link_discoveries"):
        assert f"'{table}'" in API
        assert f"activist_{table}" in MIGRATION
    for column in ("lease_token", "lease_expires_at", "external_message_id"):
        assert column in API
        assert column in MIGRATION
    assert "last_message_id=GREATEST(last_message_id,VALUES(last_message_id))" in API


def test_signal_rebuild_runtime_resources_use_message_posted_at():
    assert "'telegram_signal_messages' => array(" in V1
    assert "'telegram_signal_matches' => array(" in V1
    assert "runtime_message.posted_at" in V1
    assert ") runtime_data', 'posted_at')" in V1


def test_legacy_lineage_migration_guards_optional_tables_on_fresh_database():
    for table in ("activist_articles", "activist_stories", "activist_documents"):
        guard = (
            "IF EXISTS (\n"
            "    SELECT 1 FROM information_schema.TABLES\n"
            f"    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table}'\n"
            "  ) THEN"
        )
        assert guard in LINEAGE_MIGRATION


def test_v1_public_routes_and_exports_are_documented():
    specification = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    paths = specification["paths"]
    expected = {
        "/companies",
        "/companies/{company_id}",
        "/events",
        "/events/{event_id}",
        "/campaigns/{campaign_id}",
        "/documents/{document_id}",
        "/calendar",
        "/search",
        "/exports/events.csv",
        "/exports/events.json",
        "/feeds/events.atom",
        "/feedback",
    }
    assert expected <= paths.keys()
    assert "V1_MAX_PAGE_SIZE = 100" in V1
    assert "V1_RESPONSE_BUDGET_BYTES = 256000" in V1


def test_unified_search_includes_governance_actors():
    specification = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    kinds = specification["components"]["schemas"]["SearchResult"]["properties"][
        "kind"
    ]["enum"]
    assert "actor" in kinds
    assert "SELECT \\'actor\\', a.actor_id, a.display_name" in V1
    assert "a.aliases_json LIKE ?" in V1


def test_openapi_schema_references_resolve():
    specification = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    schemas = specification["components"]["schemas"]
    references: list[str] = []

    def collect(value):  # type: ignore[no-untyped-def]
        if isinstance(value, dict):
            if "$ref" in value:
                references.append(value["$ref"])
            for child in value.values():
                collect(child)
        elif isinstance(value, list):
            for child in value:
                collect(child)

    collect(specification)
    schema_prefix = "#/components/schemas/"
    missing = {
        reference.removeprefix(schema_prefix)
        for reference in references
        if reference.startswith(schema_prefix)
        and reference.removeprefix(schema_prefix) not in schemas
    }
    assert not missing


def test_feedback_is_never_automatically_public():
    assert "VALUES (?,?,?,?,?,?,?,?,\\'pending\\',0" in V1
    assert "'is_public' => false" in V1
    assert "feedback_rate_limited" in V1


def test_source_rights_gate_telegram_and_public_documents():
    assert (
        "source_class NOT IN (\\'licensed_telegram\\',\\'authorized_telegram\\')" in V1
    )
    assert "redistribution_allowed = 1" in V1
    assert "revoked_at IS NULL" in V1
    assert "valid_until IS NULL" in V1


def test_legacy_public_surfaces_fail_closed_on_telegram_source_rights():
    assert "function source_right_redistribution_sql" in API
    assert "function legacy_article_visibility_sql" in API
    assert "function legacy_story_visibility_sql" in API
    assert "function telegram_message_visibility_sql" in API
    assert "function telegram_signal_visibility_sql" in API
    assert "NULLIF(TRIM(" in API
    assert ".evidence_uri), \\'\\') IS NOT NULL" in API
    assert ".evidence_hash), \\'\\') IS NOT NULL" in API
    assert "JSON_CONTAINS(" in API
    assert "$.source_right_ids" in API
    assert "COUNT(DISTINCT signal_sr.source_right_id)" in API
    assert "source_right_id=VALUES(source_right_id)" in API
    assert "idx_article_source_right" in API
    assert "idx_story_source_right" in API
    # All three public legacy read paths invoke the fail-closed predicates.
    assert API.count("legacy_article_visibility_sql('a', 'article_sr')") >= 3
    assert API.count("telegram_message_visibility_sql($config, 'm')") >= 3
    assert "legacy_story_visibility_sql($config, 's')" in API
    story_visibility = API[
        API.index("function legacy_story_visibility_sql") : API.index(
            "function telegram_signal_visibility_sql"
        )
    ]
    assert "rights_sa.position_no = 0" in story_visibility
    assert (
        "rights_a.canonical_url = ' . $storyAlias . '.representative_url"
        in story_visibility
    )
    assert (
        "rights_a.source_right_id <=> ' . $storyAlias . '.source_right_id"
        in story_visibility
    )


def test_authoritative_signal_rebuild_removes_stale_rows_transactionally():
    snapshot = API[
        API.index("function upsert_telegram_snapshot") : API.index(
            "function upsert_report"
        )
    ]
    assert "replace_issue_signals" in snapshot
    assert "deprecated_replacement_signal_ids" in snapshot
    assert "signal_rebuild_token" in snapshot
    assert "signal_rebuild_begin" in snapshot
    assert "signal_rebuild_finalize" in snapshot
    assert "issue_signals_staged" in snapshot
    assert "signal_rebuild_finalized" in snapshot
    assert "signal_rebuild_idempotent" in snapshot
    assert "stale_signal_rebuild_token" in snapshot
    assert "signal_rebuild_stage_requires_signals_only" in snapshot
    assert "signal_rebuild_in_progress" in snapshot
    assert "telegram_signal_rebuild_lease_seconds" in snapshot
    assert "$signalRebuildLeaseExpired" in snapshot
    assert "count($channels) > 0 || count($messages) > 0" in snapshot
    assert "invalid_issue_signals_replace_since" in snapshot
    assert "telegram_signal_rebuild_state" in snapshot
    assert "telegram_signal_rebuild_staging" in snapshot
    assert "FOR UPDATE" in snapshot
    assert "DELETE FROM ' . table_name($config, 'telegram_issue_signals')" in snapshot
    assert "latest_seen_at >= ?" in snapshot
    assert "article_id NOT IN (" in snapshot
    assert snapshot.index("$pdo->beginTransaction()") < snapshot.index(
        "$deleteSignals->execute($deleteParams)"
    ) < snapshot.index("$pdo->commit()")


def test_signal_rebuild_staging_schema_is_migrated_and_bootstrapped():
    for table in (
        "telegram_signal_rebuild_state",
        "telegram_signal_rebuild_staging",
    ):
        assert f"'{table}'" in API
        assert f"activist_{table}" in SIGNAL_REBUILD_MIGRATION
    for column in (
        "active_token CHAR(64)",
        "finalized_token CHAR(64)",
        "rebuild_token CHAR(64)",
        "payload_json MEDIUMTEXT",
    ):
        assert column in API
        assert column in SIGNAL_REBUILD_MIGRATION
    assert "INSERT IGNORE INTO activist_telegram_signal_rebuild_state" in (
        SIGNAL_REBUILD_MIGRATION
    )


def test_telegram_snapshot_capability_preflight_is_authenticated():
    handler = API[API.index("function handle_write") : API.index("function upsert_snapshot")]
    assert "'telegram_snapshot_capabilities'" in handler
    assert handler.index("$nonce = require_signature") < handler.index(
        "if ($action === 'telegram_snapshot_capabilities')"
    )
    assert "'signal_rebuild_protocol' => 'staging-v1'" in handler
    assert "'max_payload_bytes'" in handler


def test_timestamped_deployment_backups_are_denied_by_apache():
    assert r".*\.bak(?:\..*)?" in HTACCESS


def test_telegram_snapshot_rows_fail_closed_and_ack_actual_writes():
    snapshot = API[
        API.index("function upsert_telegram_snapshot") : API.index(
            "function upsert_report"
        )
    ]
    for error in (
        "invalid_telegram_channel",
        "invalid_telegram_channel_cursor",
        "invalid_telegram_message",
        "invalid_telegram_message_identity",
        "invalid_telegram_article_match",
        "invalid_telegram_article_match_identity",
        "invalid_issue_signal",
        "invalid_issue_signal_article_id",
    ):
        assert error in snapshot
    for counter in (
        "$channelsProcessed",
        "$messagesProcessed",
        "$matchesProcessed",
        "$signalsProcessed",
        "$signalsStaged",
    ):
        assert counter in snapshot
    assert "'channels' => $channelsProcessed" in snapshot
    assert "'messages' => $messagesProcessed" in snapshot
    assert "'article_matches' => $matchesProcessed" in snapshot
    assert "'issue_signals' => $signalsProcessed" in snapshot
    assert snapshot.count("!is_string($channelId) && !is_int($channelId)") == 2


def test_runtime_export_supports_newest_first_opaque_cursor():
    assert "v1_runtime_cursor_encode" in V1
    assert "v1_runtime_cursor_decode" in V1
    assert "'updated_desc'" in V1
    assert "ORDER BY `' . $timeColumn . '` DESC, `' . $primary . '` DESC" in V1
    assert "invalid_cursor" in V1


def test_cross_window_corrections_use_collection_key():
    assert "collection_key VARCHAR(96) NULL" in API
    assert "collection_key VARCHAR(96) NULL" in MIGRATION
    assert "idx_document_collection" in API
    assert "previousDocumentStmt" in V1
    assert "previousEventStmt" in V1
    assert (
        "correction_of_document_id=COALESCE(correction_of_document_id,VALUES(correction_of_document_id))"
        in V1
    )
    assert "version_no=GREATEST(version_no,VALUES(version_no))" in V1
    assert "correction_of_document_id=VALUES(correction_of_document_id)" not in V1
    assert "$existingDocumentLineageStmt" in V1
    assert "document_lineage_conflict:" in V1


def test_outbox_claim_can_target_one_delivery_and_blocks_invalid_rights():
    claim = V1[
        V1.index("function claim_delivery_outbox") : V1.index(
            "function ack_delivery_outbox"
        )
    ]
    assert "requestedDeliveryId" in claim
    assert "requested_status" in claim
    assert "external_message_id" in claim
    assert "delivery_id = ?" in claim
    assert "$limit = 1;" in claim
    assert "max(300, min(1800" in claim
    assert "'max_claim_items' => 1" in claim
    assert "delivery_source_rights_valid" in V1
    assert "source_right_inactive_or_missing" in claim
    assert "delivery_lease_expired_outcome_unknown" in claim
    assert "outcome_unknown_count" in claim
    assert (
        "OR (status = \\'processing\\' AND lease_expires_at < UTC_TIMESTAMP())"
        not in claim
    )
    enqueue = V1[
        V1.index("function enqueue_delivery_outbox") : V1.index(
            "function delivery_payload_source_right_ids"
        )
    ]
    lineage = V1[
        V1.index("function delivery_payload_source_right_ids") : V1.index(
            "function delivery_source_rights_valid"
        )
    ]
    assert "delivery_source_rights_valid($pdo, $config, $contentJson)" in enqueue
    assert "rights_lineage_complete" in lineage
    assert "$decoded['rights_lineage_complete'] !== true" in lineage
    assert "array_key_exists('source_right_ids', $decoded)" in lineage
    assert "ON DUPLICATE KEY UPDATE updated_at=VALUES(updated_at)" in enqueue
    assert "status=VALUES(status)" not in enqueue


def test_admin_and_ops_routes_use_role_bearer_tokens():
    assert "v1_require_role($config, array('ops'))" in V1
    assert "v1_require_role($config, array('editor'))" in V1
    assert "v1_require_role($config, array('rights'))" in V1
    assert "last_success_at" in V1
    assert "pending_outbox" in V1
    assert "dead_letter_count" in V1


def test_telegram_admin_token_is_header_only():
    assert "HTTP_X_TELEGRAM_ADMIN_TOKEN" in API
    assert "$_GET['admin_token']" not in API


def test_shared_host_preserves_standard_authorization_header():
    assert 'SetEnvIf Authorization "(.*)" HTTP_AUTHORIZATION=$1' in HTACCESS
    assert "Options -Indexes" in HTACCESS


def test_server_to_server_hmac_actions_are_registered():
    actions = {
        "upsert_governance_snapshot",
        "upsert_editorial_snapshot",
        "enqueue_delivery_outbox",
        "claim_delivery_outbox",
        "ack_delivery_outbox",
        "fail_delivery_outbox",
        "export_runtime_state",
        "enqueue_link_discoveries",
        "claim_link_discoveries",
        "resolve_link_discovery",
    }
    for action in actions:
        assert f"'{action}'" in API
        assert f"function {action}" in V1
    # handle_write authenticates once before dispatching every registered action.
    auth_position = API.index("$nonce = require_signature($body, $config)")
    dispatch_position = API.index("if ($action === 'upsert_governance_snapshot')")
    assert auth_position < dispatch_position


def test_delivery_confirmation_requires_external_message_id():
    assert "delivery_id_lease_and_external_message_id_required" in V1
    assert "status=\\'delivered\\', external_message_id=?" in V1
    assert "'items' => $rows" in V1
    assert "'outbox_id'" in V1


def test_outbox_openapi_documents_singleton_lease_and_unknown_outcome_policy():
    specification = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    contract = specification["x-hmac-actions"]["delivery-outbox-contract"]
    assert contract["claim"] == {
        "max_items": 1,
        "default_lease_seconds": 900,
        "min_lease_seconds": 300,
        "max_lease_seconds": 1800,
        "semantics": contract["claim"]["semantics"],
    }
    assert contract["acknowledgement"]["external_message_id_required"] is True
    assert (
        contract["acknowledgement"]["idempotent_when_external_message_id_matches"]
        is True
    )
    assert contract["acknowledgement"]["ambiguous_send_outcome"] == "dead_letter"


def test_runtime_state_can_rehydrate_operational_relations_incrementally():
    for resource in (
        "articles",
        "stories",
        "telegram_channels",
        "telegram_messages",
        "telegram_article_matches",
        "telegram_issue_signals",
        "delivery_outbox",
        "companies",
        "source_rights",
        "collection_runs",
        "governance_events",
        "documents",
    ):
        assert f"'{resource}' => array(" in V1
    assert "invalid_since" in V1
    assert "runtime_record_exceeds_response_budget" in V1
    channel_resource = V1[
        V1.index("'telegram_channels' => array(") : V1.index(
            "'telegram_messages' => array("
        )
    ]
    assert "payload_json" in channel_resource


def test_link_discovery_state_machine_is_separate_from_articles():
    for status in ("discovered", "resolving", "resolved", "expired"):
        assert status in V1
    enqueue_section = V1[V1.index("function enqueue_link_discoveries") :]
    assert "table_name($config, 'articles')" not in enqueue_section


def test_legacy_read_actions_remain_available():
    for action in (
        "reports",
        "report",
        "latest_snapshot",
        "articles",
        "telegram_reactions",
        "telegram_dashboard",
        "search",
    ):
        assert f"'{action}'" in API


def test_original_language_is_persisted_without_translation_fields():
    assert "original_language" in API
    assert "original_language" in MIGRATION
    assert "translated_title" not in API
    assert "translated_body" not in API


def test_telegram_identity_migration_uses_channel_id_message_keys():
    assert "function migrate_telegram_channel_identity" in V1
    assert "CONCAT(\\'id:\\', ?, \\':\\', m.telegram_message_id)" in V1
    assert (
        "migrate_telegram_channel_identity($pdo, $config, $handle, $telegramChannelId)"
        in API
    )
    assert "telegram_channel_id <> ?" in V1
    assert "channel_' . substr(hash('sha256', $previousId)" in V1
    assert "idx_telegram_channel_message_id (telegram_channel_id, telegram_message_id)" in API
    assert (
        "idx_telegram_channel_message_id (telegram_channel_id, telegram_message_id)"
        in TELEGRAM_IDENTITY_INDEX_MIGRATION
    )
    assert "identity_migration_version TINYINT UNSIGNED NOT NULL DEFAULT 0" in API
    assert "identity_migration_version TINYINT UNSIGNED NOT NULL DEFAULT 0" in (
        TELEGRAM_IDENTITY_INDEX_MIGRATION
    )
    assert "MAX(COLUMN_TYPE) = 'tinyint unsigned'" in (
        TELEGRAM_IDENTITY_INDEX_MIGRATION
    )
    assert (
        "GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) = "
        "'telegram_channel_id,telegram_message_id'"
        in TELEGRAM_IDENTITY_INDEX_MIGRATION
    )
    assert "MIN(NON_UNIQUE) = 1" in TELEGRAM_IDENTITY_INDEX_MIGRATION
    assert "SET SESSION lock_wait_timeout = 30" in TELEGRAM_IDENTITY_INDEX_MIGRATION
    assert "ALGORITHM=INPLACE, LOCK=NONE" in TELEGRAM_IDENTITY_INDEX_MIGRATION
    assert (
        "if ($hasStableMapping && (int)$identityMigrationVersion >= 1 "
        "&& count($aliases) === 1) { return; }"
        in V1
    )
    assert "SET identity_migration_version=0 WHERE handle=?" in API
    assert "OR (telegram_channel_id=? AND handle<>?)" in API
    assert "$identityInvalidations" in API
    assert "identity_migration_version=1" in API
    assert "tmp_bside_canonical_messages.posted_at" in V1
    assert "tmp_bside_canonical_matches.score" in V1


def test_cancelled_disclosure_requires_human_review_with_lifecycle_history():
    assert "$isCancelled = !empty($event['is_cancelled'])" in V1
    assert "$verification = 'withdrawn'" in V1
    assert "$publication = $isEventFollowup ? 'draft'" in V1
    assert (
        "review_status=IF(payload_json<=>VALUES(payload_json),review_status,VALUES(review_status))"
        in V1
    )
    assert (
        "publication_status=IF(payload_json<=>VALUES(payload_json),publication_status,VALUES(publication_status))"
        in V1
    )
    assert (
        "review_status=IF(VALUES(verification_status)=\\'withdrawn\\',\\'pending\\'"
        not in V1
    )
    assert (
        "publication_status=IF(VALUES(verification_status)=\\'withdrawn\\',\\'draft\\'"
        not in V1
    )
    assert "entry_type, title, description" in V1
    assert "\\'cancellation\\'" in V1
    assert "\\'lifecycle_status\\'" in V1
    assert "Official cancellation disclosure:" in V1
    assert "$followupTimelineUnchanged" in V1
    assert (
        "review_status=IF(' . $followupTimelineUnchanged . ',review_status,\\'pending\\')"
        in V1
    )


def test_cancelled_document_remains_public_as_verifiable_lifecycle_evidence():
    start = V1.index("function upsert_governance_snapshot")
    ingest = V1[start : V1.index("$eventStmt =", start)]
    cancellation = ingest[ingest.index("if (!empty($document['is_cancelled']))") :]
    assert "$verification = 'withdrawn'" in cancellation
    assert "$publication = 'published'" in cancellation


def test_company_master_does_not_erase_non_empty_fields_with_partial_updates():
    ingest = V1[
        V1.index("$companyStmt =") : V1.index("foreach ($companies as $company)")
    ]
    for field in (
        "stock_code",
        "market",
        "legal_name",
        "legal_name_en",
        "short_name",
        "homepage_url",
    ):
        assert f"{field}=COALESCE(NULLIF(VALUES({field}),\\'\\'),{field})" in ingest
    assert "VALUES(aliases_json)=\\'[]\\'" in ingest


def test_source_right_boolean_flags_are_parsed_strictly():
    assert "function v1_bool_int" in V1
    assert (
        "in_array(strtolower(trim($value)), array('1', 'true', 'yes', 'on'), true)"
        in V1
    )
    source_right_section = V1[V1.index("function v1_admin_upsert_source_right") :]
    assert "!empty($payload['redistribution_allowed'])" not in source_right_section


def test_cors_and_hmac_fail_closed_before_mutating_dispatch():
    assert "function valid_cors_origin" in API
    assert "preg_match('/[\\r\\n]/', $origin)" in API
    assert "Access-Control-Allow-Credentials" not in API
    assert "strpos($v1Path, '/ops/') !== 0" in API
    assert "strpos($v1Path, '/admin/') !== 0" in API
    assert "strlen($secret) < 32" in API
    assert "abs(time() - (int)$timestamp) > 300" in API
    assert "hash_equals($expected, strtolower($signature))" in API
    assert "(string)$e->getCode() === '23000'" in API
    auth_position = API.index("$nonce = require_signature($body, $config)")
    nonce_position = API.index("remember_nonce($pdo, $config, $nonce)")
    mutation_position = API.index("if ($action === 'upsert_snapshot')")
    assert auth_position < nonce_position < mutation_position


def test_sql_identifiers_and_dynamic_pagination_are_bounded():
    assert "preg_match('/^[A-Za-z0-9_]+$/', $prefix)" in API
    assert "preg_match('/^[A-Za-z0-9_]{1,32}$/', $charset)" in API
    assert "PDO::ATTR_EMULATE_PREPARES => false" in API
    assert "V1_MAX_PAGE_SIZE = 100" in V1
    assert "$page = max(1, min(100000, $page))" in V1
    assert "LIMIT ' . ((int)$page['limit'] + 1)" in V1
    assert "OFFSET ' . (int)$page['offset']" in V1
    assert "foreach (array('event_type' => $eventType" in V1


def test_all_api_response_shapes_enforce_250kb_budget():
    respond_section = API[
        API.index("function respond") : API.index("function table_name")
    ]
    assert "V1_RESPONSE_BUDGET_BYTES" in respond_section
    assert "response_budget_exceeded" in respond_section
    assert "X-Response-Bytes" in respond_section
    assert "X-Response-Bytes" in V1[V1.index("function v1_export_events_csv") :]


def test_legacy_query_adapters_publish_90_day_migration_headers():
    assert "function legacy_adapter_headers" in API
    assert "2026-10-14T00:00:00Z" in API
    for header in (
        "Deprecation: true",
        "Sunset: ",
        "successor-version",
        "X-BSIDE-Legacy-Adapter: true",
    ):
        assert header in API
    handle_read = API[API.index("function handle_read") :]
    assert "legacy_adapter_headers($config, $action)" in handle_read


def test_feedback_rate_limit_uses_private_salt_and_retry_contract():
    feedback = V1[
        V1.index("function v1_submit_feedback") : V1.index("function v1_ops_health")
    ]
    assert "strlen($salt) < 32" in feedback
    assert "feedback_rate_limit_not_configured" in feedback
    assert "Retry-After: 3600" in feedback
    assert "feedback_rate_limited" in feedback
    assert "is_public' => false" in feedback


def test_public_events_and_linked_records_follow_source_right_revocation():
    assert "function v1_event_visibility_sql" in V1
    assert "function v1_optional_document_visibility_sql" in V1
    visibility = V1[
        V1.index("function v1_document_visibility_sql") : V1.index(
            "function v1_event_visibility_sql"
        )
    ]
    assert "NULLIF(TRIM(" in visibility
    assert ".evidence_uri), \\'\\') IS NOT NULL" in visibility
    assert ".evidence_hash), \\'\\') IS NOT NULL" in visibility
    event_visibility = V1[
        V1.index("function v1_event_visibility_sql") : V1.index(
            "function v1_optional_document_visibility_sql"
        )
    ]
    assert "EXISTS (SELECT 1" in event_visibility
    assert "NOT EXISTS" not in event_visibility
    assert V1.count("v1_event_visibility_sql($config, 'e')") >= 6
    for expression in (
        "tl.document_id",
        "v.evidence_document_id",
        "co.evidence_document_id",
    ):
        assert f"v1_required_document_visibility_sql($config, '{expression}')" in V1


def test_ingest_cannot_grant_telegram_rights_or_publish_telegram_only_events():
    ingest = V1[
        V1.index("function upsert_governance_snapshot") : V1.index(
            "function enqueue_delivery_outbox"
        )
    ]
    assert "$sourceType !== 'official_disclosure'" in ingest
    assert "strpos($id, 'official:') !== 0" in ingest
    assert "array('dart', 'kind')" in ingest
    assert "$telegramOnly = $hasTelegramEvidence && !$hasIndependentEvidence" in ingest
    assert (
        "$evidenceMissing = !$hasTelegramEvidence && !$hasIndependentEvidence" in ingest
    )
    assert "if ($telegramOnly) { $verification = 'signal'; }" in ingest
    assert "$requiresReview = $telegramOnly || $evidenceMissing ||" in ingest


def test_hmac_ingest_cannot_reactivate_or_overwrite_administered_source_rights():
    ingest = V1[
        V1.index("function upsert_governance_snapshot") : V1.index(
            "function enqueue_delivery_outbox"
        )
    ]
    right_upsert = ingest[
        ingest.index("$rightStmt =") : ingest.index("foreach ($rights as $right)")
    ]
    assert "ON DUPLICATE KEY UPDATE source_right_id=source_right_id" in right_upsert
    for administered_field in (
        "evidence_uri",
        "evidence_hash",
        "valid_until",
        "revoked_at",
        "ai_allowed",
        "redistribution_allowed",
        "status",
    ):
        assert f"{administered_field}=VALUES({administered_field})" not in right_upsert

    admin = V1[
        V1.index("function v1_admin_upsert_source_right") : V1.index(
            "function v1_admin_create_revision"
        )
    ]
    assert "status=VALUES(status)" in admin
    assert "revoked_at=VALUES(revoked_at)" in admin


def test_runtime_governance_events_export_complete_source_right_lineage():
    resources = V1[
        V1.index("function v1_runtime_resource") : V1.index(
            "function v1_runtime_cursor_encode"
        )
    ]
    runtime_page = V1[V1.index("function runtime_state_page") :]
    assert "'governance_events' => array(" in resources
    assert "SELECT DISTINCT runtime_ed.event_id" in runtime_page
    assert "event_documents') . ' runtime_ed" in runtime_page
    assert "documents') . ' runtime_d" in runtime_page
    assert "$row['source_right_ids'] = array_keys($lineage['rights'])" in runtime_page
    assert "invalid_governance_event_source_right_lineage" in runtime_page
    assert "$row['evidence_revision'] = hash('sha256'" in runtime_page
    assert (
        "$row['publishable_evidence_count'] = count($lineage['publishable'])"
        in runtime_page
    )
    assert "v1_document_visibility_sql('runtime_d', 'runtime_sr')" in runtime_page


def test_mysql_datetimes_are_normalized_to_utc_before_storage():
    helper = V1[
        V1.index("function v1_mysql_datetime_utc") : V1.index("function v1_bool_int")
    ]
    mysql_helper = API[
        API.index("function mysql_dt") : API.index("function first_mysql_dt")
    ]
    assert "new DateTimeZone('UTC')" in helper
    assert "new DateTimeImmutable(trim($value), $utc)" in helper
    assert "setTimezone($utc)->format('Y-m-d H:i:s')" in helper
    assert "return v1_mysql_datetime_utc($value)" in mysql_helper


def test_editor_review_requires_verified_or_withdrawn_evidence_and_fresh_token():
    review = V1[
        V1.index("function v1_admin_review_event") : V1.index(
            "function v1_admin_review_feedback"
        )
    ]
    assert (
        "array('official', 'confirmed', 'corroborated', 'corrected', 'withdrawn')"
        in review
    )
    assert "verified_evidence_required_before_publication" in review
    assert "publishable_evidence_required_before_publication" in review
    assert "v1_document_visibility_sql('review_d', 'review_sr')" in review
    assert "expected_updated_at" in review
    assert "stale_review" in review


def test_editorial_snapshot_contract_is_atomic_idempotent_and_fail_closed():
    ingest = V1[
        V1.index("function upsert_editorial_snapshot") : V1.index(
            "function delivery_event_is_publishable"
        )
    ]
    for entity in (
        "actors",
        "event_actors",
        "campaigns",
        "claim_evidence",
        "proposal_votes",
        "commitment_outcomes",
        "timeline_entries",
    ):
        assert f"'{entity}'" in ingest
    assert "count($records) > 500" in ingest
    assert "payload_sha256" in ingest
    assert "$pdo->beginTransaction()" in ingest
    assert "editorial_chunk_conflict" in ingest
    assert "'rejected' => 0" in ingest
    assert "v1_editorial_datetime_utc" in V1
    assert "review_status=IF(" in V1
    assert "publication_status=IF(" in V1


def test_editorial_schema_upgrade_and_campaign_evidence_relation_exist():
    for table in ("activist_campaign_documents", "activist_editorial_ingest_chunks"):
        assert table in EDITORIAL_MIGRATION
    for column in ("review_status", "payload_sha256", "updated_at"):
        assert column in EDITORIAL_MIGRATION
    assert "activist_add_editorial_column" in EDITORIAL_MIGRATION


def test_all_editorial_review_routes_and_queue_are_documented_and_implemented():
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    expected = {
        "/admin/actors/{actor_id}/review",
        "/admin/event-actors/{event_id}/{actor_id}/{actor_role}/review",
        "/admin/campaigns/{campaign_id}/review",
        "/admin/claims/{claim_id}/review",
        "/admin/proposal-votes/{proposal_vote_id}/review",
        "/admin/commitments/{commitment_id}/review",
        "/admin/timeline-entries/{timeline_entry_id}/review",
    }
    assert expected <= spec["paths"].keys()
    for entity in (
        "actor",
        "event_actor",
        "campaign",
        "claim",
        "proposal_vote",
        "commitment",
        "timeline",
    ):
        assert f"SELECT \\'{entity}\\'" in V1
    assert "function v1_admin_review_event_actor" in V1
    assert "event_actor_not_found" in V1


def test_public_and_delivery_paths_fail_closed_after_approval_or_rights_change():
    assert "review_status IN (\\'approved\\',\\'not_required\\')" in V1
    assert "verification_status <> \\'withdrawn\\' OR " in V1
    assert "function v1_campaign_visibility_sql" in V1
    assert "event_not_publishable_or_unapproved" in V1
    assert "$editorialBlock->execute" in V1


def test_official_followups_are_bounded_unambiguous_and_preserve_canonical_event():
    ingest = V1[
        V1.index("function upsert_governance_snapshot") : V1.index(
            "function upsert_editorial_snapshot"
        )
    ]
    assert "V1_CORRECTION_LOOKBACK_DAYS" in ingest
    assert "LIMIT 2 FOR UPDATE" in ingest
    assert "count($candidates) !== 1" in ingest
    assert "count($eventCandidates) === 1" in ingest
    assert "$eventByIdStmt" in ingest
    assert (
        "$versionNo = max($versionNo, ((int)$previousDocument['version_no']) + 1)"
        in ingest
    )
    assert "$title = (string)$canonicalEvent['title']" in ingest
    assert "$occurred = (string)$canonicalEvent['occurred_at']" in ingest
    assert "ambiguous_independent" in ingest
    assert "Official correction disclosure:" in ingest


def test_editorial_enums_metrics_and_parent_companies_are_revalidated_server_side():
    normalize = V1[
        V1.index("function v1_editorial_normalize_record") : V1.index(
            "function upsert_governance_snapshot"
        )
    ]
    for value in (
        "activist_shareholder",
        "shareholder_coalition",
        "settled",
        "partially_met",
        "cancelled",
    ):
        assert value in normalize
    assert "object required" in normalize
    assert "!is_int($record[$field]) && !is_float($record[$field])" in V1
    references = V1[
        V1.index("function v1_editorial_validate_references") : V1.index(
            "function v1_editorial_apply_record"
        )
    ]
    assert "event_id/campaign_id: company mismatch" in references
    assert "company_id: parent company mismatch" in references
