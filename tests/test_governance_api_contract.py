import re
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
API = (ROOT / "deploy" / "activist" / "api.php").read_text(encoding="utf-8")
V1 = (ROOT / "deploy" / "activist" / "governance_v1.php").read_text(encoding="utf-8")
V2 = (ROOT / "deploy" / "activist" / "governance_v2.php").read_text(encoding="utf-8")
V2_WRITE = (ROOT / "deploy" / "activist" / "governance_v2_write.php").read_text(
    encoding="utf-8"
)
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
RELEASE_GUARD_MIGRATION = (
    ROOT
    / "deploy"
    / "activist"
    / "migrations"
    / "006_governance_release_guard.sql"
).read_text(encoding="utf-8")
IDENTITY_EVIDENCE_MIGRATION = (
    ROOT
    / "deploy"
    / "activist"
    / "migrations"
    / "007_governance_identity_and_evidence.sql"
).read_text(encoding="utf-8")
OFFICIAL_SITE_MIGRATION = (
    ROOT
    / "deploy"
    / "activist"
    / "migrations"
    / "008_official_site_snapshot_receipts.sql"
).read_text(encoding="utf-8")
DART_QUOTA_MIGRATION = (
    ROOT
    / "deploy"
    / "activist"
    / "migrations"
    / "009_dart_global_quota_ledger.sql"
).read_text(encoding="utf-8")
SLOT_CLAIM_MIGRATION = (
    ROOT
    / "deploy"
    / "activist"
    / "migrations"
    / "010_official_slot_claim_ledger.sql"
).read_text(encoding="utf-8")
DART_CREDENTIAL_POOL_MIGRATION = (
    ROOT
    / "deploy"
    / "activist"
    / "migrations"
    / "012_dart_credential_pool.sql"
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
        "/actors",
        "/actors/{actor_id}",
        "/events",
        "/events/{event_id}",
        "/today",
        "/campaigns/{campaign_id}",
        "/documents/{document_id}",
        "/calendar",
        "/search",
        "/exports/events.csv",
        "/exports/events.json",
        "/feeds/events.atom",
        "/feedback",
        "/revisions",
        "/metrics/web-vitals",
        "/ops/availability-observations",
        "/ops/web-distribution-observations",
        "/ops/quality-observations",
        "/ops/release-evidence",
        "/ops/official-run-ledger",
        "/ops/official-slot-claims",
        "/ops/dart-quota",
        "/ops/official-site-candidates",
        "/ops/official-site-rights",
        "/ops/backfill-checkpoints/{job_fingerprint}",
        "/admin/shadow-discrepancies",
        "/admin/official-slot-epoch",
    }
    assert expected <= paths.keys()
    assert "V1_MAX_PAGE_SIZE = 100" in V1
    assert "V1_RESPONSE_BUDGET_BYTES = 250000" in V1
    assert "V1_RESPONSE_BUDGET_BYTES : 250000" in API


def test_release_guard_migration_is_explicit_fail_closed_and_idempotent():
    for table in (
        "activist_schema_migrations",
        "activist_governance_release_state",
        "activist_governance_release_audit",
    ):
        assert table in RELEASE_GUARD_MIGRATION
    assert "'governance_v1', 'closed', 0" in RELEASE_GUARD_MIGRATION
    assert "ON DUPLICATE KEY UPDATE state_key=VALUES(state_key)" in RELEASE_GUARD_MIGRATION
    assert "UNIQUE KEY uq_governance_release_version" in RELEASE_GUARD_MIGRATION
    assert "6, '006_governance_release_guard'" in RELEASE_GUARD_MIGRATION


def test_identity_evidence_migration_covers_canonical_events_and_production_observations():
    for column in (
        "listing_status",
        "master_modified_at",
        "identity_action",
        "identity_target",
        "identity_actor_id",
        "identity_effective_at",
        "identity_deadline_at",
        "identity_status",
        "comparison_key",
        "code_revision",
        "first_observed_at",
        "raw_count",
        "acknowledged_count",
    ):
        assert column in IDENTITY_EVIDENCE_MIGRATION
    for table in (
        "activist_event_observations",
        "activist_shadow_discrepancies",
        "activist_shadow_run_observations",
        "activist_availability_observations",
        "activist_web_distribution_observations",
        "activist_governance_quality_observations",
        "activist_web_vital_observations",
        "activist_official_backfill_checkpoints",
        "activist_human_release_evidence_bundles",
    ):
        assert table in IDENTITY_EVIDENCE_MIGRATION
    assert "UNIQUE KEY `uq_event_comparison_key`" in IDENTITY_EVIDENCE_MIGRATION
    assert "workflow_run_attempt" in IDENTITY_EVIDENCE_MIGRATION
    assert "uq_web_distribution_run_attempt_target" in IDENTITY_EVIDENCE_MIGRATION
    assert "7, '007_governance_identity_and_evidence'" in IDENTITY_EVIDENCE_MIGRATION


def test_identity_ingest_is_fail_closed_and_creates_event_observations():
    ingest = V1[V1.index("function upsert_governance_snapshot") :]
    assert "^eventcmp:v1:[a-f0-9]{64}$" in ingest
    assert "invalid_complete_event_identity:" in ingest
    assert "incomplete_event_identity_has_comparison_key:" in ingest
    assert "event_identity_field_conflict:" in ingest
    assert "$identityStatus !== 'complete'" in ingest
    assert "event_observations" in ingest
    assert "event_observation_document_missing:" in ingest
    assert "acknowledged_count" in ingest
    assert "code_revision" in ingest
    assert "expected_backend_binding_id" in ingest
    assert "backend_binding_required" in ingest
    assert "backend_binding_mismatch" in ingest
    assert ingest.index("hash_equals($backendBindingId,$expectedBackendBindingId)") < ingest.index(
        "$pdo->beginTransaction()"
    )
    assert "'backend_binding_id' => $backendBindingId" in ingest


def test_ops_observation_checkpoint_and_evidence_contracts_are_private_and_bounded():
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    for route in (
        "/ops/availability-observations",
        "/ops/release-evidence",
        "/ops/backfill-checkpoints/{job_fingerprint}",
    ):
        assert spec["paths"][route]
    dispatch = V1[V1.index("function handle_v1_request") : V1.index("function v1_serve_openapi")]
    assert "v1_require_role($config, array('ops'))" in dispatch
    assert "count($observations) > 10" in V1
    assert "observation_id_conflict" in V1
    assert "backfill_checkpoint_version_conflict" in V1
    assert "expected_version" in V1[V1.index("function v1_ops_put_backfill_checkpoint") :]
    assert "'evidence_source'=>'production_db_export'" in V1
    assert "'is_synthetic'=>false" in V1
    assert "'distribution_mode'=>'web_only'" in V1


def test_release_evidence_is_kst_daily_build_scoped_and_never_synthesizes_missing_quality():
    section = V1[V1.index("function v1_kst_observation_date") : V1.index("function v1_release_request_id")]
    assert "Asia/Seoul" in section
    assert "v1_evidence_utc_bounds" in section
    assert "'observation_date'=>$day" in section
    assert "'build_sha'=>$sha" in section
    assert "'operations_days'=>$operationsDays" in section
    assert "'shadow_days'=>$shadowDays" in section
    assert "'web_distribution_days'=>$distributionDays" in section
    assert "'quality_observations'=>$qualityRows" in section
    assert "kind_observation_lag_p95_minutes" in section
    assert "dart_success_poll_interval_p95_minutes" in section
    assert "v1_kind_observation_stats_by_day($pdo,$config,$from,$to)" in section
    assert "kind_observation_count" in section
    assert "kind_lag_sample_count" in section
    assert "content_snapshot_at" in section
    assert "governance_corpus_2021_plus_kst_day_end_v2" in section
    assert "same_story_evaluated_pair_count'=>null" not in section


def test_content_corpus_v2_keeps_every_public_object_document_reference_in_scope():
    corpus = V1[
        V1.index("function v1_content_corpus_document_refs_sql")
        : V1.index("function v1_content_corpus_document_refs_params")
    ]
    snapshot = V1[
        V1.index("function v1_content_corpus_snapshot")
        : V1.index("function v1_current_public_document_rights_guard")
    ]
    for table in (
        "event_documents",
        "campaign_documents",
        "claim_evidence",
        "proposal_votes",
        "commitment_outcomes",
        "timeline_entries",
    ):
        assert f"table_name($config,'{table}')" in corpus
    for reference in (
        "ed.document_id",
        "cd.document_id",
        "ce.document_id",
        "v.evidence_document_id",
        "co.evidence_document_id",
        "tl.document_id",
    ):
        assert reference in corpus
    assert "UNION SELECT" in corpus
    assert "public_document_refs.document_id=d.document_id" in snapshot
    assert "d.source_class=\\'official_disclosure\\'" not in corpus
    assert "v1_document_visibility_sql" not in corpus
    assert corpus.count("publication_status=\\'published\\'") >= 5
    assert corpus.count("review_status=\\'approved\\'") >= 5
    assert "ce.editorial_status=\\'approved\\'" in corpus
    assert "timeline_e.identity_status=\\'complete\\'" in corpus
    assert "timeline_e.verification_status<>\\'signal\\'" in corpus
    assert "timeline_cp.publication_status=\\'published\\'" in corpus
    assert "timeline_cp.review_status=\\'approved\\'" in corpus
    assert "tl.event_id IS NOT NULL" in corpus
    assert "tl.campaign_id IS NOT NULL" in corpus
    assert "governance_corpus_2021_plus_kst_day_end_v2" in snapshot

    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    evidence = spec["components"]["schemas"]["QualityObservationEvidence"]
    assert (
        evidence["properties"]["content_scope"]["const"]
        == "governance_corpus_2021_plus_kst_day_end_v2"
    )


def test_preview_to_live_rechecks_current_v2_rights_under_one_lock_order():
    identity = V1[
        V1.index("function v1_document_source_right_identity_sql")
        : V1.index("function v1_document_visibility_sql")
    ]
    visibility = V1[
        V1.index("function v1_document_visibility_sql")
        : V1.index("function v1_event_visibility_sql")
    ]
    guard = V1[
        V1.index("function v1_current_public_document_rights_guard")
        : V1.index("function v1_quality_observation_payload_hash")
    ]
    transition = V1[
        V1.index("function v1_admin_update_release_state")
        : V1.index("function v1_admin_upsert_source_right")
    ]
    right_writer = V1[
        V1.index("function v1_admin_upsert_source_right")
        : V1.index("function v1_admin_create_revision")
    ]
    ingest_writer = V1[
        V1.index("function upsert_governance_snapshot")
        : V1.index("function v1_editorial_reference_exists")
    ]

    assert "BINARY ' . $documentAlias . '.source_class=BINARY " in identity
    assert "BINARY ' . $documentAlias . '.source_key=BINARY " in identity
    assert "v1_document_source_right_identity_sql(" in visibility
    assert "v1_content_corpus_document_refs_sql($config)" in guard
    assert "v1_content_corpus_document_refs_params($checkedAt,$scopeStart)" in guard
    assert "v1_content_document_right_valid_at($document,$checkedAt)" in guard
    for field in (
        "document_source_class",
        "document_source_key",
        "right_source_type",
        "right_source_key",
    ):
        assert field in guard
    right_validator = V1[
        V1.index("function v1_content_document_right_valid_at")
        : V1.index("function v1_content_corpus_snapshot")
    ]
    assert right_validator.count("hash_equals(") == 2
    assert "$before = v1_release_state($pdo, $config, true)" in transition
    assert "'protected_atomic_cutover_required'" in transition
    atomic_cutover = V2[
        V2.index("function v2_admin_atomic_cutover") : V2.index(
            "function v2_admin_update_release_state"
        )
    ]
    assert atomic_cutover.index(
        "v2_lock_current_public_source_rights($pdo, $config)"
    ) < atomic_cutover.index(
        "v1_current_public_document_rights_guard($pdo, $config)"
    )
    assert atomic_cutover.index(
        "v1_current_public_document_rights_guard($pdo, $config)"
    ) < atomic_cutover.index("release_state=\\'live\\'")
    assert "current_source_rights_invalid" in atomic_cutover
    assert "v1_invalid_source_right_document_count" in atomic_cutover
    assert right_writer.index("v1_release_state($pdo,$config,true)") < right_writer.index(
        "INSERT INTO "
    )
    assert ingest_writer.index("v1_release_state($pdo,$config,true)") < ingest_writer.index(
        "$rightStmt = $pdo->prepare"
    )


def test_protected_role_hashes_are_pairwise_disjoint_without_secret_diagnostics():
    overlap = V1[
        V1.index("function v1_protected_role_hash_overlap_status")
        : V1.index("function v1_require_disjoint_protected_role_hashes")
    ]
    for role in (
        "admin",
        "editor",
        "ops",
        "preview",
        "release_authorizer",
        "rights",
    ):
        assert f"'{role}'" in overlap
    assert "v1_preview_token_hashes($config)" in overlap
    assert "conflicting_roles" in overlap
    assert "'hash'" not in overlap
    returned_status = overlap[overlap.index("return array(") :]
    assert "$owners" not in returned_status
    assert "$roleHashes" not in returned_status

    failure = V1[
        V1.index("function v1_require_disjoint_protected_role_hashes")
        : V1.index("function v1_preview_auth_configured")
    ]
    assert "protected_role_token_hash_overlap" in failure
    assert "conflicting_roles" in failure
    assert "$owners" not in failure
    assert "$roleHashes" not in failure

    role_auth = V1[
        V1.index("function v1_require_role")
        : V1.index("function v1_normalize_identity_text")
    ]
    preview_auth = V1[
        V1.index("function v1_require_preview_token")
        : V1.index("function v1_release_state")
    ]
    for auth in (role_auth, preview_auth):
        assert "v1_require_disjoint_protected_role_hashes($config);" in auth

    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    policy = spec["x-release-gate"]["protected-live-transition"]
    assert "pairwise disjoint" in policy
    assert "without returning credential hashes" in policy


def test_canonical_json_encode_calls_supply_explicit_failure_context():
    compact = "".join(V1.split())
    expected_calls = (
        (
            "$revisionPayload,'kind_source_right_revision_encode_failed'",
            "v1_kind_source_right_eligibility",
        ),
        (
            "$revisionPayload,'official_site_right_revision_encode_failed'",
            "v1_ops_official_site_rights",
        ),
        (
            "$previousIdentity,'event_previous_identity_encode_failed'",
            "v1_admin_complete_event_identity",
        ),
        (
            "$identity,'event_identity_encode_failed'",
            "v1_admin_complete_event_identity",
        ),
    )
    for arguments, function_name in expected_calls:
        assert f"v1_canonical_json_encode({arguments})" in compact, function_name

    assert "v1_canonical_json_encode($revisionPayload)" not in compact
    assert "v1_canonical_json_encode($previousIdentity)" not in compact
    assert "v1_canonical_json_encode($identity)" not in compact


def test_availability_evidence_uses_exact_kst_minute01_slot_coverage():
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    schema = spec["components"]["schemas"]["DailyRouteAvailabilityEvidence"]
    assert schema["properties"]["cadence_id"]["const"] == "watchdog-v1-kst-5m-minute01"
    assert schema["properties"]["expected_slot_count"]["const"] == 288
    assert schema["properties"]["covered_slots_bitmap_hex"]["pattern"] == "^[0-9a-f]{72}$"

    section = V1[V1.index("function v1_availability_cadence_bucket") : V1.index("function v1_release_request_id")]
    assert "GOV_V1_AVAILABILITY_CADENCE_ID" in V1
    assert "GOV_V1_AVAILABILITY_SLOTS_PER_DAY = 288" in V1
    assert "$minuteOfDay === 0" in section
    assert "$local->modify('-1 day')->format('Y-m-d')" in section
    assert "v1_availability_utc_bounds($availabilityFrom,$to)" in section
    assert "LIMIT 50001" in section
    assert "availability_evidence_row_limit_exceeded" in section
    for field in (
        "cadence_id",
        "expected_slot_count",
        "covered_slot_count",
        "missing_slot_count",
        "duplicate_slot_count",
        "off_cadence_count",
        "covered_slots_bitmap_hex",
        "first_observed_at",
        "last_observed_at",
        "actual_interval_seconds_p95",
        "actual_max_gap_seconds",
    ):
        assert f"'{field}'" in section


def test_release_evidence_prefers_each_official_source_submitted_raw_denominator():
    section = V1[V1.index("function v1_ops_release_evidence") : V1.index("function v1_release_request_id")]
    assert "$sourceRaw = $sourceOutcome['raw_count']" in section
    assert "$sourceAck = $sourceOutcome['acknowledged_count']" in section
    assert "$sourceRaw === $sourceAck" in section
    assert "v1_official_run_ledger_row($run)" in section


def test_shadow_run_snapshots_prove_zero_discrepancy_days_and_are_integrity_checked():
    assert "/admin/shadow-runs" in yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))["paths"]
    section = V1[V1.index("function v1_shadow_event_keys") : V1.index("function v1_admin_shadow_discrepancies")]
    assert "legacy_events_sha256" in section
    assert "candidate_events_sha256" in section
    assert "shadow_run_integrity_error" in section
    assert "duplicate_shadow_comparison_key" in section
    assert "legacy_crosswalk" in section
    assert "shadow_run_crosswalk_integrity_error" in section
    for column in (
        "legacy_eligible_record_count",
        "legacy_crosswalked_record_count",
        "legacy_unmatched_record_count",
        "legacy_ambiguous_record_count",
        "legacy_crosswalk_coverage_rate",
        "legacy_crosswalk_sha256",
    ):
        assert column in IDENTITY_EVIDENCE_MIGRATION
    assert "unchanged'=>true" in section


def test_actual_distribution_and_quality_evidence_have_durable_idempotent_writers():
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    assert "/ops/web-distribution-observations" in spec["paths"]
    assert "/ops/quality-observations" in spec["paths"]
    distribution = V1[V1.index("function v1_record_web_distribution_observations") : V1.index("function v1_record_quality_observations")]
    quality = V1[V1.index("function v1_record_quality_observations") : V1.index("function v1_record_web_vitals")]
    assert "workflow_run_id" in distribution
    assert "workflow_run_attempt" in distribution
    assert "failure_detected_at" in distribution
    assert "count($items) > 50" in distribution
    assert "web_distribution_observation_conflict" in distribution
    assert "production_quality_job" in quality
    assert "quality_numerator_exceeds_denominator" in quality
    assert "payload_sha256" in quality
    assert "quality_observation_conflict" in quality
    assert "kind_observation_lag_not_actual" in quality
    quality_schema = spec["components"]["schemas"]["QualityRawCounts"]
    assert not any(name.startswith("same_story_") for name in quality_schema["properties"])


def test_human_release_evidence_bundle_is_same_sha_append_only_and_canonical():
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    assert "/admin/release-evidence-inputs" in spec["paths"]
    validation = V1[
        V1.index("function v1_validate_human_evidence_document") : V1.index(
            "function v1_human_evidence_row_response"
        )
    ]
    section = V1[
        V1.index("function v1_admin_upsert_release_evidence_inputs") : V1.index(
            "function v1_kst_observation_date"
        )
    ]
    assert "benchmark','usability','release_approval" in section
    assert "invalid_human_evidence_provenance" in validation
    assert "human_release_evidence_version_conflict" in section
    assert "INSERT INTO " in section and "human_release_evidence_bundles" in section
    assert "UPDATE " not in section


def test_cutover_metadata_is_atomic_audited_and_drives_legacy_headers():
    assert "cutover_at" in IDENTITY_EVIDENCE_MIGRATION
    assert "sunset_at" in IDENTITY_EVIDENCE_MIGRATION
    transition = V1[V1.index("function v1_admin_update_release_state") : V1.index("function v1_admin_upsert_source_right")]
    assert "$current === 'preview' && $target === 'live'" in transition
    assert "time() + 90 * 86400" in transition
    assert "cutover_at = ?, sunset_at = ?" in transition
    assert "request_id, cutover_at, sunset_at" in transition
    assert "cutover_at" in V1[V1.index("function v1_admin_release_state") : V1.index("function v1_assert_object_keys")]


def test_ops_health_uses_stalest_dart_kind_success_not_media_maximum():
    health = V1[V1.index("function v1_ops_health") : V1.index("function v1_admin_review_queue")]
    assert "'dart'=>array" in health and "'kind'=>array" in health
    assert "min($official['dart']['last_success_at'],$official['kind']['last_success_at'])" in health
    assert "official_sources" in health
    assert "table_name($config, 'runs')" not in health


def test_atom_uses_filtered_api_self_link_and_public_site_alternate_link():
    atom = V1[V1.index("function v1_events_atom") : V1.index("function v1_submit_feedback")]
    assert "governance_api_base_url" in atom
    assert "https://alignpe.gabia.io/activist/api.php/api/v1" in atom
    assert "<link rel=\"self\"" in atom
    assert "v1_event_feed_self_query($page)" in atom
    assert "public_base_url" in atom
    assert "https://news.bside.ai" in atom
    assert "'/#/events/'" in atom


def test_atom_self_query_normalizes_alias_dates_order_and_limit():
    query = V1[
        V1.index("function v1_event_feed_self_query") : V1.index("function v1_events_atom")
    ]
    assert "$verification = $status" in query
    assert "mysql_dt($value)" in query
    assert "Y-m-d\\TH:i:s\\Z" in query
    assert "$query['limit']" in query
    assert "ksort($query, SORT_STRING)" in query
    assert "PHP_QUERY_RFC3986" in query


def test_web_vitals_are_privacy_minimal_rate_limited_and_expire_after_30_days():
    section = V1[V1.index("function v1_record_web_vitals") : V1.index("function v1_ops_get_backfill_checkpoint")]
    assert "count($items) > 50" in section
    assert "web_vitals_rate_limited" in section
    assert "INTERVAL 30 DAY" in section
    assert "stored_identifiers' => false" in section
    for forbidden in ("REMOTE_ADDR", "HTTP_USER_AGENT", "query_string", "session_id", "user_id"):
        assert forbidden not in section


def test_public_revisions_and_large_document_paging_do_not_leak_internal_values():
    revisions = V1[V1.index("function v1_public_revisions") : V1.index("function v1_admin_shadow_discrepancies")]
    assert "revision_status = \\'published\\'" in revisions
    assert "previous_value" not in revisions
    assert "revised_value" not in revisions
    assert "requested_by" not in revisions
    document = V1[V1.index("function v1_get_document") : V1.index("function v1_date_bound")]
    assert "body_limit_bytes" in document
    assert "CAST(d.body_text AS BINARY)" in document
    assert "body_truncated" in document
    assert "body_next_offset" in document


def test_common_public_event_filters_are_shared_by_search_calendar_exports_and_feed():
    filters = V1[V1.index("function v1_event_query_parts") : V1.index("function v1_public_event_select")]
    for field in ("company_id", "actor_id", "event_type", "verification_status", "status", "source_class", "evidence_document_id", "from", "to"):
        assert field in filters
    calendar = V1[V1.index("function v1_calendar_vote_filter_parts") : V1.index("function v1_like")]
    assert "v1_event_query_parts($config, false)" in calendar
    assert "v1_calendar_vote_filter_parts($config)" in calendar
    assert "if (!v1_event_filter_requested(false))" not in calendar
    for field in ("v.company_id = ?", "v.proposer_actor_id = ?", "'event_type'=>$eventType", "'verification_status'=>$verification", "vote_filter_d.source_class"):
        assert field in calendar
    assert "v1_event_query_parts($config)" in V1[V1.index("function v1_search") : V1.index("function v1_export_events_json")]
    assert V1.count("v1_query_public_events($pdo, $config, $page)") >= 4


def test_v1_uses_read_only_schema_version_guard_instead_of_request_time_ddl():
    assert "const GOV_V1_SCHEMA_VERSION = 10" in V1
    assert "function v1_require_schema_version" in V1
    assert "schema_version_mismatch" in V1
    assert "ensure_schema(" not in V1
    assert "CREATE TABLE" not in V1
    dispatch = V1[V1.index("function handle_v1_request") : V1.index("function v1_serve_openapi")]
    assert dispatch.index("v1_require_schema_version($pdo, $config)") < dispatch.index("v1_list_companies($pdo, $config)")
    assert "ensure_schema($pdo, $config)" in API  # signed writers remain compatible
    handle_read = API[API.index("function handle_read") :]
    assert "ensure_schema($pdo, $config)" not in handle_read
    assert "v1_require_schema_version($pdo, $config)" in handle_read


def test_release_state_gates_public_data_but_not_health_openapi_or_privileged_routes():
    dispatch = V1[V1.index("function handle_v1_request") : V1.index("function v1_serve_openapi")]
    assert dispatch.index("$path === '/health'") < dispatch.index("$pdo = pdo_conn($config)")
    assert dispatch.index("$path === '/openapi.yaml'") < dispatch.index("$pdo = pdo_conn($config)")
    assert dispatch.index("$pdo = pdo_conn($config)") < dispatch.index("if ($path === '/')")
    assert dispatch.index("v1_require_public_release_access($pdo, $config)") < dispatch.index("if ($path === '/')")
    assert "strpos($path, '/ops/') === 0 || strpos($path, '/admin/') === 0" in dispatch
    assert "v1_require_public_release_access($pdo, $config)" in dispatch
    assert "governance_release_closed" in V1
    assert "preview_token_required" in V1
    assert "invalid_preview_token" in V1
    assert "Cache-Control: private, no-store" in V1
    assert "Vary: Authorization" in V1


def test_today_is_ranked_server_side_from_the_complete_public_event_set():
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    assert "/today" in spec["paths"]
    section = V1[V1.index("function v1_today_ranked_select") : V1.index("function v1_get_event")]
    assert "verification_status <> \\'signal\\'" in section
    assert "importance" in section and "official_disclosure" in section
    assert "deadline_watch" in section
    assert "LIMIT 5" in section and "LIMIT 10" in section
    assert "v1_query_public_events" not in section
    assert "archive_endpoint' => '/events'" in section


def test_calendar_vote_filters_do_not_drop_the_vote_branch():
    section = V1[V1.index("function v1_calendar_vote_filter_parts") : V1.index("function v1_like")]
    assert "UNION ALL" in section
    assert "v1_required_document_visibility_sql($config, 'v.evidence_document_id')" in section
    assert "vote_filter_ea.actor_id=?" in section
    assert "vote_filter_d.document_id=v.evidence_document_id" in section


def test_official_site_candidates_are_private_bounded_and_deterministic():
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    route = spec["paths"]["/ops/official-site-candidates"]["get"]
    assert route["security"] == [{"bearerAuth": []}]
    section = V1[V1.index("function v1_ops_official_site_candidates") : V1.index("function v1_admin_review_queue")]
    assert "official_disclosure" in section
    assert "LIMIT 20" in section and "LIMIT 10" in section
    assert "ORDER BY raw_score DESC,event_count DESC,e.company_id ASC" in section
    assert "ORDER BY raw_score DESC,event_count DESC,a.actor_id ASC" in section
    for actor_type in ("activist_shareholder", "institution", "shareholder_coalition"):
        assert actor_type in section
    assert "body_text" not in section and "payload_json" not in section


def test_kind_lag_uses_receipt_to_first_observation_without_fabricated_dates():
    section = V1[
        V1.index("function v1_kind_observation_stats_by_day") : V1.index(
            "function v1_public_revisions"
        )
    ]
    assert "eo.first_observed_at" in section
    assert "eo.source_key=\\'kind\\'" in section
    assert "$row['published_at'] === null" in section
    assert "$firstEpoch < $receiptEpoch" in section
    assert "$row['source_type'] !== 'official_disclosure'" in section
    assert "$row['source_right_id'] !== 'official:kind'" in section
    for forbidden in ("COALESCE(d.published_at", "DATE(d.published_at", "00:00:00"):
        assert forbidden not in section


def test_kind_source_right_is_preflighted_and_revalidated_inside_ingest_transaction():
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    route = spec["paths"]["/ops/source-right-eligibility"]["get"]
    assert route["security"] == [{"bearerAuth": []}]
    section = V1[
        V1.index("function v1_kind_source_right_eligibility") : V1.index(
            "function v1_ops_official_site_candidates"
        )
    ]
    assert "source_right_id=\\'official:kind\\'" in section
    assert "FOR UPDATE" in section
    assert "redistribution_not_allowed" in section
    assert "ai_not_allowed" in section
    assert "rights_revision" in section
    ingest = V1[V1.index("function upsert_governance_snapshot") :]
    assert ingest.index("v1_kind_source_right_eligibility($pdo,$config,true)") < ingest.index(
        "$rightStmt = $pdo->prepare"
    )


def test_dart_source_right_is_out_of_band_and_rechecked_under_write_lock():
    helper = V1[
        V1.index("function v1_dart_source_right_expectation") :
        V1.index("function v1_global_event_family_for_legacy_type")
    ]
    assert "expected_source_right_revisions" in helper
    assert "'rights_revision'" in helper
    assert "'contract_revision'" in helper
    ingest = V1[V1.index("function upsert_governance_snapshot") :]
    assert "dart_source_right_managed_out_of_band" in ingest
    assert "dart_global_bridge_unavailable" in ingest
    assert "dart_source_right_precondition_required" in ingest
    assert "dart_deployment_revision_required" in ingest
    assert "dart_release_state_precondition_required" in ingest
    assert "dart_deployment_revision_mismatch" in ingest
    assert "dart_guarded_action_required" in ingest
    assert "v1_lock_existing_dart_lineage(" in ingest
    assert "v2_release_state_rows_for_update($pdo,$config)" in ingest
    assert "dart_release_state_mismatch" in ingest
    assert "v1_dart_release_state_expectation" in helper
    assert "expected_release_state" in helper
    assert "$storedV1ReleaseState !== $storedV2ReleaseState" in ingest
    assert "$storedV1ReleaseState !== $dartReleaseStateExpectation" in ingest
    assert "dart_body_text_forbidden" in V1
    body_guard = V1[
        V1.index("function v1_normalize_governance_snapshot_documents") :
        V1.index("function v1_governance_snapshot_lineage_candidates")
    ]
    assert "'body_text','content'" in body_guard
    assert "$document[$bodyField] !== ''" in body_guard
    assert "$dartReleaseStates[GOV_V1_RELEASE_STATE_KEY]" in ingest
    assert "$dartReleaseStates[GOV_V2_RELEASE_STATE_KEY]" in ingest
    assert "v2_source_right_row(" in ingest
    assert "'official:dart'," in ingest
    assert "true" in ingest[
        ingest.index("v2_source_right_row(") :
        ingest.index("v2_source_right_ineligible_reasons")
    ]
    assert "v2_source_right_revision($dartRight)" in ingest
    assert "v2_source_right_contract_revision($dartRight)" in ingest
    assert "dart_source_right_ineligible_or_changed" in ingest
    assert ingest.index("v2_release_state_rows_for_update($pdo,$config)") < ingest.index(
        "v2_source_right_row("
    )
    assert ingest.index("v2_source_right_row(") < ingest.index(
        "v1_lock_global_dart_connector("
    )
    assert ingest.index("v1_lock_global_dart_connector(") < ingest.index(
        "v1_lock_existing_dart_lineage("
    )
    assert ingest.index("dart_global_bridge_unavailable") < ingest.index(
        "$pdo->beginTransaction()"
    )
    assert ingest.index("v2_deployment_identity_status()") < ingest.index(
        "$companyStmt = $pdo->prepare"
    )
    assert ingest.index("v2_source_right_row(") < ingest.index(
        "$companyStmt = $pdo->prepare"
    )
    assert ingest.index("v2_source_right_row(") < ingest.index(
        "$rightStmt = $pdo->prepare"
    )


def test_editor_identity_completion_recomputes_comparison_key_and_preserves_event_id():
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    route = spec["paths"]["/admin/events/{event_id}/identity"]["post"]
    assert route["security"] == [{"bearerAuth": []}]
    section = V1[
        V1.index("function v1_admin_complete_event_identity") : V1.index(
            "function v1_admin_review_event"
        )
    ]
    assert "v1_build_event_identity" in section
    assert "WHERE event_id=? FOR UPDATE" in section
    assert "event_comparison_key_conflict" in section
    assert "identity_status=\\'complete\\'" in section
    assert "SET event_id" not in section
    assert "hash_equals($comparisonKey,$eventId)" not in V1.replace(" ", "")


def test_hmac_cross_source_identity_reuses_the_locked_canonical_event_owner():
    ingest = V1[V1.index("function upsert_governance_snapshot") : V1.index("function v1_editorial_reference_exists")]
    assert "WHERE comparison_key=? LIMIT 1 FOR UPDATE" in ingest
    assert "$eventId = (string)$comparisonOwner" in ingest
    assert ingest.index(
        "v1_pdo_fetch_column_and_close(\n"
        "                    $eventComparisonOwnerStmt"
    ) < ingest.index("$eventStmt->execute")
    assert "hash_equals((string)$computedIdentity['comparison_key'],$comparisonKey)" in ingest
    assert "array('high', 'market_sensitive', 'critical')" in ingest
    assert "if ($importance === 'market_sensitive') { $importance = 'critical'; }" not in ingest


def test_guarded_dart_snapshot_closes_every_native_pdo_read_cursor():
    helpers = V1[
        V1.index("function v1_pdo_fetch_one_and_close") :
        V1.index("function v1_dart_quota_fetch_one_and_close")
    ]
    assert "function v1_pdo_fetch_column_and_close" in helpers
    assert "function v1_pdo_fetch_all_and_close" in helpers
    assert helpers.count("$statement->closeCursor()") == 6

    guarded = V1[
        V1.index("function v1_lock_official_slot_claim_for_run") :
        V1.index("function v1_editorial_reference_exists")
    ]
    unsafe_reads = (
        "$companyLookup->fetchColumn()",
        "$documentLookup->fetchColumn()",
        "$eventDocumentLookup->fetchColumn()",
        "$eventObservationLookup->fetchColumn()",
        "$eventIdentityLookup->fetch()",
        "$comparisonIdentityLookup->fetch()",
        "$runLookup->fetchColumn()",
        "$providedPredecessorStmt->fetch()",
        "$previousDocumentStmt->fetchAll()",
        "$existingDocumentLineageStmt->fetch()",
        "$eventByIdStmt->fetch()",
        "$eventComparisonOwnerStmt->fetchColumn()",
        "$eventIdentityStmt->fetch()",
        "$officialActorLookupStmt->fetchColumn()",
        "$documentClassStmt->fetch()",
        "$companyPublicationEligibilityStmt->fetch()",
        "$approvedIdentityActorRelationStmt->fetchColumn()",
        "$eventLifecycleStmt->fetchColumn()",
        "$documentObservationStmt->fetch()",
    )
    assert all(read not in guarded for read in unsafe_reads)
    assert guarded.count("v1_pdo_fetch_one_and_close(") >= 9
    assert guarded.count("v1_pdo_fetch_column_and_close(") >= 10
    assert "v1_pdo_fetch_all_and_close(" in guarded

    v2_right = V2_WRITE[
        V2_WRITE.index("function v2_source_right_row") :
        V2_WRITE.index("function v2_source_right_revision")
    ]
    assert "v1_pdo_fetch_one_and_close($statement,array($sourceRightId))" in v2_right
    release_rows = V2[
        V2.index("function v2_release_state_rows_for_update") :
        V2.index("function v2_release_authorization_fields")
    ]
    assert "v1_pdo_fetch_all_and_close(" in release_rows
    assert "while ($row = $statement->fetch())" not in release_rows


def test_partial_company_disclosures_do_not_erase_company_master_listing_status():
    section = V1[V1.index("$companyStmt = $pdo->prepare") : V1.index("$rightStmt = $pdo->prepare")]
    assert "listing_status=listing_status" in section
    assert "master_modified_at=master_modified_at" in section
    assert "array_key_exists('listing_status',$company)" in section
    assert "$companyMasterStmt->execute" in section


def test_hmac_outbound_enqueue_and_claim_are_permanently_disabled_before_db_mutation():
    enqueue = V1[
        V1.index("function enqueue_delivery_outbox") : V1.index(
            "function delivery_payload_source_right_ids"
        )
    ]
    claim = V1[V1.index("function claim_delivery_outbox") : V1.index("function ack_delivery_outbox")]
    for section, count_field in ((enqueue, "accepted"), (claim, "claimed")):
        assert "respond(410" in section
        assert "outbound_delivery_disabled" in section
        assert "distribution_mode'=>'web_only'" in section
        assert f"'{count_field}'=>0" in section
    assert enqueue.index("respond(410") < enqueue.index("$pdo->prepare")
    assert claim.index("respond(410") < claim.index("$pdo->beginTransaction")


def test_release_state_admin_api_is_audited_and_optimistically_concurrent():
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    route = spec["paths"]["/admin/release-state"]
    assert {"get", "post"} <= route.keys()
    assert "v1_admin_release_state($pdo, $config)" in V1
    assert "v1_admin_update_release_state" in V1
    assert "expected_version_required" in V1
    assert "stale_release_state" in V1
    assert "invalid_release_transition" in V1
    assert "governance_release_audit" in V1
    assert "preview_auth_configured" in V1
    assert "preview_token_hash" not in str(route)


def test_openapi_uses_the_deployed_api_origin_and_documents_release_gate():
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    assert spec["servers"][0]["url"] == "https://alignpe.gabia.io/activist/api.php/api/v1"
    assert spec["x-public-site"] == "https://news.bside.ai"
    assert spec["x-release-gate"]["states"] == ["closed", "preview", "live"]
    assert spec["x-release-gate"]["schema-version"] == 10


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


def test_source_rights_gate_every_public_document_without_null_exceptions():
    visibility = V1[V1.index("function v1_document_visibility_sql") : V1.index("function v1_event_visibility_sql")]
    assert ".source_right_id IS NOT NULL" in visibility
    assert ".source_right_id IS NULL" not in visibility
    assert "source_class NOT IN" not in visibility
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
    assert ".evidence_hash" in API
    assert "REGEXP \\'^[A-Fa-f0-9]{64}$\\'" in API
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
    assert "previousEventStmt" not in V1
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


def test_telegram_reactions_requires_admin_access_before_reading_inputs():
    reactions = API[
        API.index("if ($action === 'telegram_reactions') {") :
    ]
    assert reactions.index("require_telegram_admin_access($config);") < reactions.index(
        "$limit = isset($_GET['limit'])"
    )


def test_public_legacy_search_never_queries_or_returns_telegram_signal_data():
    search = API[
        API.index("function handle_search(PDO $pdo, array $config): void {") :
        API.index("function handle_read(string $action, array $config): void {")
    ]
    assert "$telegram = array();" in search
    assert "telegram_issue_signals" not in search
    assert "search_public_telegram_row" not in search
    assert "active_telegram_source_keys" not in search


def test_legacy_public_rows_strip_internal_scores_and_workflow_fields():
    helper = API[
        API.index("function legacy_public_row(array $row): array {") :
        API.index("function telegram_event_token(string $token): bool {")
    ]
    for field in (
        "status",
        "reason",
        "priority_score",
        "priority_level",
        "source_right_id",
        "search_score",
        "score_breakdown",
        "signal_score",
        "analysis_bucket",
        "queue_status",
        "review_status",
        "payload_json",
        "idempotency_key",
    ):
        assert f"'{field}'" in helper
    assert "array_map('legacy_public_row', $stmt->fetchAll())" in API
    search = API[
        API.index("function handle_search(PDO $pdo, array $config): void {") :
        API.index("function handle_read(string $action, array $config): void {")
    ]
    assert search.count("'legacy_public_row'") >= 2


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
    assert "'upsert_governance_snapshot_dart_guarded'" in API
    assert (
        "upsert_governance_snapshot($pdo, $config, $payload, true)"
        in API
    )
    assert (
        "upsert_governance_snapshot($pdo, $config, $payload, false)"
        in API
    )
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
    extension = specification["x-hmac-actions"]
    contract = extension["delivery-outbox-contract"]
    disabled = extension["permanently-disabled-actions"]
    assert disabled == {
        "distribution_mode": "web_only",
        "http_status": 410,
        "error": "outbound_delivery_disabled",
        "actions": ["enqueue_delivery_outbox", "claim_delivery_outbox"],
    }
    assert contract["deprecated"] is True
    assert contract["historical_schema_only"] is True
    assert contract["new_enqueue_enabled"] is False
    assert contract["new_claim_enabled"] is False
    assert "enqueue_delivery_outbox" not in extension["actions"]
    assert "claim_delivery_outbox" not in extension["actions"]
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
        "link_discoveries",
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
    assert "lineage_version" in enqueue_section
    assert "source_right_id" in enqueue_section
    assert "VALUES(lineage_version)>=lineage_version" in enqueue_section
    assert "COALESCE(VALUES(title),title)" in enqueue_section
    assert (
        "CASE WHEN source_right_id IS NOT NULL THEN source_right_id "
        "ELSE VALUES(source_right_id) END"
    ) in enqueue_section
    assert (
        "ORDER BY lineage_version DESC, discovered_at DESC, discovery_id ASC"
    ) in enqueue_section


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
    upsert = V1[V1.index("function upsert_governance_snapshot") :]
    ingest = upsert[
        upsert.index("$companyStmt =")
        : upsert.index("foreach ($companies as $company)")
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


def test_source_right_id_keeps_an_immutable_source_identity():
    source_right_section = V1[
        V1.index("function v1_admin_upsert_source_right")
        : V1.index("function v1_admin_create_revision")
    ]
    assert (
        "SELECT source_type,source_key,status,updated_at,revoked_at FROM "
        in source_right_section
    )
    assert "WHERE source_right_id=? LIMIT 1 FOR UPDATE" in source_right_section
    assert "(string)$existingIdentity['source_type'] !== $sourceType" in (
        source_right_section
    )
    assert "(string)$existingIdentity['source_key'] !== $sourceKey" in (
        source_right_section
    )
    assert "source_right_identity_immutable" in source_right_section
    assert "stale_source_right" in source_right_section
    assert "expected_status" in source_right_section
    assert "expected_updated_at" in source_right_section
    assert "hash_equals(" in source_right_section
    assert "source_type=VALUES(source_type)" not in source_right_section
    assert "source_key=VALUES(source_key)" not in source_right_section

    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    operation = spec["paths"]["/admin/source-rights"]["post"]
    assert "409" in operation["responses"]
    source_right = spec["components"]["schemas"]["SourceRight"]
    assert "immutable" in source_right["description"].lower()
    assert "Immutable" in source_right["properties"]["source_type"]["description"]
    assert "Immutable" in source_right["properties"]["source_key"]["description"]
    assert source_right["properties"]["expected_status"]["enum"] == [
        "missing",
        "pending",
        "active",
        "expired",
        "revoked",
    ]
    assert source_right["properties"]["expected_updated_at"]["format"] == "date-time"


def test_cors_and_hmac_fail_closed_before_mutating_dispatch():
    assert "function valid_cors_origin" in API
    assert "preg_match('/[\\r\\n]/', $origin)" in API
    assert "Access-Control-Allow-Credentials" not in API
    cors = API[
        API.index("if ($corsOrigin !== '')")
        : API.index("if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS')")
    ]
    exposed_match = re.search(
        r"header\('Access-Control-Expose-Headers:\s*([^']+)'\);",
        cors,
    )
    assert exposed_match is not None
    exposed_headers = {
        header.strip() for header in exposed_match.group(1).split(",")
    }
    assert exposed_headers == {
        "X-BSIDE-API-Version",
        "X-Response-Bytes",
        "X-BSIDE-Offset",
        "X-BSIDE-Returned",
        "X-BSIDE-Has-More",
        "X-BSIDE-Next-Offset",
        "X-Has-More",
        "X-Next-Page",
        "Content-Disposition",
        "Retry-After",
        "Deprecation",
        "Sunset",
        "Link",
        "Warning",
        "X-BSIDE-Legacy-Adapter",
    }
    for private_header in ("WWW-Authenticate", "Set-Cookie", "Server"):
        assert private_header not in exposed_headers
    assert "strpos($versionedPath, '/ops/') !== 0" in API
    assert "strpos($versionedPath, '/admin/') !== 0" in API
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


def test_legacy_query_adapters_publish_headers_only_after_recorded_cutover():
    assert "function legacy_adapter_headers" in API
    assert "governance_release_state" in API
    assert "empty($release['cutover_at'])" in API
    assert "time() < $cutoverTimestamp" in API
    assert "legacy_api_sunset_at" not in API
    for header in (
        "Deprecation: true",
        "Sunset: ",
        "successor-version",
        "X-BSIDE-Legacy-Adapter: true",
    ):
        assert header in API
    handle_read = API[API.index("function handle_read") :]
    assert handle_read.index("v1_require_schema_version($pdo, $config)") < handle_read.index(
        "legacy_adapter_headers($pdo, $config, $action)"
    )


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
    assert ".evidence_hash" in visibility
    assert "REGEXP \\'^[A-Fa-f0-9]{64}$\\'" in visibility
    event_visibility = V1[
        V1.index("function v1_event_visibility_sql") : V1.index(
            "function v1_optional_document_visibility_sql"
        )
    ]
    assert "EXISTS (SELECT 1" in event_visibility
    assert "NOT EXISTS" not in event_visibility
    assert "visibility_identity_ea.actor_id = " in event_visibility
    assert ".identity_actor_id" in event_visibility
    assert r"visibility_identity_ea.review_status = \'approved\'" in event_visibility
    assert r"visibility_identity_a.review_status = \'approved\'" in event_visibility
    assert r"visibility_identity_a.record_status = \'active\'" in event_visibility
    assert r"NULLIF(TRIM(visibility_identity_a.display_name), \'\') IS NOT NULL" in event_visibility
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
    assert "$requiresReview = $identityStatus !== 'complete' || $telegramOnly || $evidenceMissing" in ingest


def test_official_ingest_never_auto_publishes_non_public_or_unidentified_company_events():
    ingest = V1[
        V1.index("function upsert_governance_snapshot") : V1.index(
            "function enqueue_delivery_outbox"
        )
    ]
    assert "SELECT stock_code,listing_status,record_status" in ingest
    assert "array('listed','suspended')" in ingest
    assert "$companyAutoPublishEligible" in ingest
    assert "|| !$companyAutoPublishEligible || !$approvedIdentityActorRelation" in ingest
    # This eligibility check is deliberately absent from manual review: an
    # editor can publish a justified unlisted-company event after review.
    review = V1[
        V1.index("function v1_admin_review_event") : V1.index(
            "function v1_admin_review_event_actor"
        )
    ]
    assert "companyAutoPublishEligible" not in review


def test_official_ingest_creates_only_pending_identifiable_filer_relations():
    ingest = V1[
        V1.index("function upsert_governance_snapshot") : V1.index(
            "function enqueue_delivery_outbox"
        )
    ]
    assert "isset($event['actor']) && is_array($event['actor'])" in ingest
    assert "isset($event['event_actor']) && is_array($event['event_actor'])" in ingest
    assert "$candidateDisplayName !== ''" in ingest
    assert "$candidateRole === 'filer'" in ingest
    assert "$candidateReviewStatus === 'pending'" in ingest
    assert "$candidateRecordStatus === 'inactive'" in ingest
    assert "$candidateRelationReview === 'pending'" in ingest
    assert "VALUES (?,?,?,NULL,?,NULL,\\'[]\\',NULL,\\'pending\\',\\'inactive\\',?,?)" in ingest
    assert "VALUES (?,?,\\'filer\\',\\'pending\\',?,?)" in ingest


def test_hmac_ingest_cannot_reactivate_or_overwrite_administered_source_rights():
    ingest = V1[
        V1.index("function upsert_governance_snapshot") : V1.index(
            "function enqueue_delivery_outbox"
        )
    ]
    right_statement = ingest.index("$rightStmt =")
    right_upsert = ingest[
        right_statement :
        ingest.index("foreach ($rights as $right)", right_statement)
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
    assert "approved_event_actor_required_before_publication" in review
    assert "review_identity_ea.actor_id = ?" in review
    assert r"review_identity_ea.review_status = \'approved\'" in review
    assert r"review_identity_a.review_status = \'approved\'" in review
    assert r"review_identity_a.record_status = \'active\'" in review
    assert r"NULLIF(TRIM(review_identity_a.display_name), \'\') IS NOT NULL" in review
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
    assert "$eventCandidates" not in ingest
    assert "previousEventStmt" not in ingest
    assert "$eventByIdStmt" in ingest
    assert (
        "$versionNo = max($versionNo, ((int)$previousDocument['version_no']) + 1)"
        in ingest
    )
    assert "$title = (string)$canonicalEvent['title']" in ingest
    assert "$occurred = (string)$canonicalEvent['occurred_at']" in ingest
    assert "ambiguous_independent" in ingest
    assert "Official correction disclosure:" in ingest


def test_official_followup_adopts_a_canonical_event_only_after_exact_raw_identity_match():
    ingest = V1[
        V1.index("function upsert_governance_snapshot") : V1.index(
            "function upsert_editorial_snapshot"
        )
    ]
    followup = ingest[
        ingest.index(
            "$identityStatus = trim((string)v1_first($event, array('identity_status')"
        ) : ingest.index("if ($isEventFollowup && !$canonicalEvent)")
    ]
    assert "$submittedIdentity = $identityStatus === 'complete'" in followup
    assert (
        "v1_build_event_identity($companyId,$eventType,$identityAction,"
        "$identityTarget,$identityActorId,"
        in followup
    )
    assert "$identityEffectiveInput,$identityDeadlineInput,false)" in followup
    assert "(string)$canonicalEvent['identity_status'] === 'complete'" in followup
    assert "$canonicalStoredIdentity = " in followup
    assert "v1_resolve_stored_event_identity(" in followup
    assert "preg_match('/^eventcmp:v1:[a-f0-9]{64}$/',$comparisonKey)" in followup
    assert "hash_equals((string)$submittedIdentity['comparison_key'],$comparisonKey)" in followup
    for field in (
        "company_id",
        "event_type",
        "identity_action",
        "identity_target",
        "identity_actor_id",
        "identity_effective_at",
        "identity_deadline_at",
        "comparison_key",
    ):
        assert f"'{field}'" in followup
    assert "(string)$submittedIdentity[$identityField]" in followup
    assert "(string)$canonicalStoredIdentity[$identityField]" in followup
    conflict = "throw new RuntimeException('followup_event_identity_conflict:'"
    assert conflict in followup
    assert ingest.index(conflict) < ingest.index(
        "$eventType = (string)$canonicalEvent['event_type']"
    )

    classifier = V1[
        V1.index("function v1_governance_snapshot_identity_conflict_code")
        : V1.index("function upsert_governance_snapshot")
    ]
    for code in (
        "followup_event_identity_conflict",
        "invalid_complete_event_identity",
        "incomplete_event_identity_has_comparison_key",
        "event_identity_scope_conflict",
        "event_identity_field_conflict",
    ):
        assert f"'{code}'" in classifier
    assert "stored_event_identity_integrity_error" not in classifier
    assert "$failure = v1_governance_snapshot_failure_response($failureError)" in ingest
    assert "respond($failure['status'],$failure['payload'])" in ingest
    assert "'error'=>$e->getMessage()" not in ingest
    transaction_catch = ingest[
        ingest.index("} catch (Throwable $e) {") : ingest.index(
            "if ($terminalCompletionFailure)"
        )
    ]
    assert "throw $e" not in transaction_catch
    assert "getMessage()" not in transaction_catch
    assert "catch (Throwable $rollbackError)" in transaction_catch
    assert "$failureError = $rollbackError" in transaction_catch


def test_reviewed_dart_event_replay_is_an_exact_read_only_ack():
    helpers = V1[
        V1.index("function v1_dart_reviewed_event_is_protected")
        : V1.index("function v1_governance_snapshot_validation_reason")
    ]
    for required_state in (
        "identity_status",
        "complete",
        "review_status",
        "approved",
        "publication_status",
        "published",
        "is_correction",
        "is_cancelled",
        "official",
        "corrected",
        "ambiguous_independent",
    ):
        assert required_state in helpers
    assert "$issuerId !== 'issuer:kr:dart:' . $companyId" in helpers
    assert "$countryCode !== 'KR'" in helpers
    assert "$eventType !== $eventFamily" in helpers
    assert "$eventDeadlineAt !== $deadlineAt" in helpers
    assert "'global:' . substr(" in helpers
    assert "!hash_equals($expectedComparisonKey,$comparisonKey)" in helpers
    assert "$companyId !== $submittedCompanyId" in helpers
    assert "!hash_equals($canonicalActorId,$submittedActorId)" in helpers
    assert "$submittedDeadlineAt !== $canonicalDeadlineAt" in helpers
    assert "$submittedIdentityDeadlineAt !== $canonicalDeadlineAt" in helpers
    assert "array_key_exists('event_link_status',$storedPayload)" in helpers
    assert "($isCorrection ? 'corrected' : 'official')" in helpers
    document_guard = helpers[
        helpers.index("function v1_dart_identity_change_document_matches("):
        helpers.index(
            "function v1_dart_reviewed_company_replay_matches(",
            helpers.index("function v1_dart_identity_change_document_matches("),
        )
    ]
    assert "if (!empty($submitted['is_cancelled']))" in document_guard
    assert "array_key_exists('is_correction',$submitted)" in document_guard
    assert "!is_bool($submitted['is_correction'])" in document_guard
    assert (
        "$submitted['is_correction'] !== $allowCorrectionReplay"
        in document_guard
    )
    assert "foreach (array(&$storedPayload,&$submittedPayload)" in document_guard
    assert (
        "$documentPayload['is_correction'] = $allowCorrectionReplay"
        in document_guard
    )
    assert "!is_bool($documentPayload['is_correction'])" in document_guard
    assert (
        "$documentPayload['is_correction']\n"
        "                    !== $allowCorrectionReplay"
        in document_guard
    )
    assert "? empty($submitted['is_correction'])" not in document_guard
    assert ": !empty($submitted['is_correction'])" not in document_guard
    assert "$canonicalSubmittedEvent['event_link_status']" in helpers
    assert "v1_followup_event_replay_payload_matches(" in helpers
    assert "$storedPayload," in helpers
    assert "$canonicalSubmittedEvent," in helpers
    assert "false," in helpers
    assert "function v1_dart_reviewed_company_replay_matches(" in helpers
    company_guard = helpers[
        helpers.index("function v1_dart_reviewed_company_replay_matches(")
        : helpers.index(
            "/**\n * Return only a stable, caller-actionable validation reason.",
            helpers.index("function v1_dart_reviewed_company_replay_matches("),
        )
    ]
    for table in (
        "companies",
        "issuers",
        "issuer_identifiers",
        "issuer_listings",
    ):
        assert f"table_name($config,'{table}')" in company_guard
    assert company_guard.count("FOR UPDATE") == 4
    assert "$submittedStockCode !== null" in company_guard
    assert "$submittedMarket !== null" in company_guard
    assert "$submittedLegalNameEn !== null" in company_guard
    assert "$submittedShortName !== null" in company_guard
    assert "$submittedHomepage !== null" in company_guard
    assert "count($submittedAliases) > 0" in company_guard
    assert "$hasListingStatus" in company_guard
    assert "$hasMasterModifiedAt" in company_guard
    assert "'DART_CORP_CODE',$companyId,'KRX'" in company_guard
    assert "$projectionStockCode !== ''" in company_guard
    assert "$projectionStockCode !== $storedStockCode" in company_guard
    assert "$projectionMarket !== $storedMarket" in company_guard
    assert "'TICKER',$storedStockCode,$storedMarket" in company_guard
    assert "'listing:kr:' . $companyId" in company_guard

    ingest = V1[
        V1.index("function upsert_governance_snapshot") : V1.index(
            "function upsert_editorial_snapshot"
        )
    ]
    preflight = ingest[
        ingest.index("$isolatedReplayEventStmt = $pdo->prepare")
        : ingest.index("$companyStmt = $pdo->prepare")
    ]
    assert "v1_dart_reviewed_event_is_protected(" in preflight
    assert "v1_dart_reviewed_event_replay(" in preflight
    assert "$globalDartProjectionEnabled" in preflight
    assert "$dartGuardedAction" in preflight
    assert "count($storedDocumentRows) !== 1" in preflight
    assert "count($submittedDocumentIdSet) !== 1" in preflight
    assert "(int)($storedDocumentRow['position_no'] ?? -1) !== 0" in preflight
    assert "isset($duplicateSubmittedDocumentIds[$storedDocumentId])" in preflight
    assert "v1_dart_identity_change_document_matches(" in preflight
    assert "$submittedDocumentsById[$storedDocumentId]," in preflight
    assert "!empty($reviewedEventReplay['is_correction'])" in preflight
    assert "count($storedDocumentOwners) !== 1" in preflight
    assert "count($submittedReferenceOwners) !== 1" in preflight
    assert "count($approvedActorRows) !== 1" in preflight
    assert "count($storedObservationRows) !== 1" in preflight
    assert "$storedObservationRows[0]['document_id']" in preflight
    assert "$isolatedReplayDocumentSnapshots[$storedDocumentId]" in preflight
    assert "$readOnlyDartIdentityMutationEventIds[$submittedEventId]" in preflight
    assert "$readOnlyDartReviewedEventCompanyIds[$submittedEventId]" in preflight
    assert "$readOnlyDartReviewedDocumentIds[$storedDocumentId]" in preflight
    assert "$readOnlyDartReviewedCompanyIds = array_fill_keys(" in preflight
    assert "$submittedReadOnlyCompanies = array();" in preflight
    assert "v1_dart_reviewed_company_replay_matches(" in preflight
    pending_identity_branch = preflight[
        preflight.index(
            "if (!$storedFollowupFlag && $storedIsolationMarker === ''"
        ) : preflight.index(
            "if (!$storedFollowupFlag || $storedIsolationMarker"
        )
    ]
    assert (
        "'event_documents'=>count($storedDocumentRows)"
        in pending_identity_branch
    )
    assert (
        "'event_observations'=>count($storedDocumentRows)"
        in pending_identity_branch
    )
    assert (
        "!isset(\n"
        "                    $readOnlyDartReviewedEventCompanyIds"
        "[$submittedEventId]"
    ) in preflight
    assert (
        "!isset(\n"
        "                    $readOnlyDartReviewedDocumentIds"
        "[$submittedDocumentId]"
    ) in preflight
    assert preflight.count(
        "unset($readOnlyDartReviewedCompanyIds[$submittedCompanyId]);"
    ) == 2
    assert ingest.index(
        "v1_dart_reviewed_company_replay_matches("
    ) < ingest.index("$companyStmt = $pdo->prepare")
    assert "v1_dart_reviewed_event_is_protected($storedOwnerRow)" in preflight
    assert "count($submittedReferenceOwners) !== 1" in preflight
    assert "$readOnlyDartReviewedEventCompanyIds[" in preflight
    assert preflight.index(
        "v1_dart_reviewed_event_is_protected("
    ) < preflight.index("$storedFollowupFlag")
    reviewed_branch = preflight[
        preflight.index("v1_dart_reviewed_event_is_protected(")
        : preflight.index(
            "if ((string)$isolatedReplayEvent['identity_status']"
        )
    ]
    assert "$pendingDartLifecycleObservations[]" not in reviewed_branch
    assert "$dartLifecycleObservationInsertStmt" not in reviewed_branch
    company_loop = ingest[
        ingest.index("foreach ($companies as $company)") : ingest.index(
            "$rightStmt = $pdo->prepare"
        )
    ]
    assert "isset($readOnlyDartReviewedCompanyIds[$companyId])" in company_loop
    assert company_loop.index(
        "isset($readOnlyDartReviewedCompanyIds[$companyId])"
    ) < company_loop.index("$companyStmt->execute")

    write_loop = ingest[
        ingest.index("foreach ($events as $event)") : ingest.index(
            "if ($run) {", ingest.index("foreach ($events as $event)")
        )
    ]
    skip = write_loop[
        write_loop.index(
            "isset($readOnlyDartIdentityMutationEventIds[$submittedEventId])"
        ) : write_loop.index(
            "if (isset($isolatedReplayCanonicalEventPayloads[$submittedEventId]))"
        )
    ]
    assert "$counts['events']++" in skip
    assert "(int)$readOnlyAckCounts['event_documents']" in skip
    assert "(int)$readOnlyAckCounts['event_observations']" in skip
    assert "!is_array($readOnlyAckCounts)" in skip
    assert "!is_int($readOnlyAckCounts['event_documents'])" in skip
    assert "!is_int($readOnlyAckCounts['event_observations'])" in skip
    assert "continue;" in skip
    assert "eventStmt->execute" not in skip
    assert "timelineStmt->execute" not in skip


def test_governance_snapshot_failures_expose_only_allowlisted_safe_diagnostics():
    classifier = V1[
        V1.index("function v1_governance_snapshot_validation_reason")
        : V1.index("function v1_global_dart_bridge_enabled")
    ]
    assert "$separator = strpos($message,':')" in classifier
    assert "$separator === false ? $message : substr($message,0,$separator)" in classifier
    assert "preg_match('/^[a-z][a-z0-9_]{0,63}$/D',$reason)" in classifier
    assert "in_array($reason,$allowed,true)" in classifier
    assert "stored_event_identity_integrity_error" not in classifier
    assert "preg_match('/^[A-Z0-9]{5}$/D',$sqlState)" in classifier
    assert "'sqlstate_class'=>$sqlState" in classifier
    assert "'driver_code'=>$driverCode" in classifier
    assert "'error'=>'governance_snapshot_persistence_failed'" in classifier
    assert "'validation_reason'=>$validationReason" in classifier
    assert "array('ok'=>false,'error'=>'internal_error')" in classifier
    for unsafe in (
        "errorInfo[2]",
        "getTrace",
        "getFile",
        "getLine",
        "error_log(",
    ):
        assert unsafe not in classifier
    expected_validation_reasons = {
        "dart_document_title_provenance_conflict",
        "dart_event_metadata_invalid",
        "dart_title_provenance_conflict",
        "document_lineage_conflict",
        "event_identity_field_conflict",
        "event_identity_scope_conflict",
        "event_observation_document_missing",
        "event_observation_hash_invalid",
        "followup_event_identity_conflict",
        "global_dart_connector_not_writable",
        "global_release_state_guard_unavailable",
        "incomplete_event_identity_has_comparison_key",
        "invalid_collection_run_code_revision",
        "invalid_collection_run_counts",
        "invalid_complete_event_identity",
        "invalid_scheduled_slot_claim_provenance",
        "non_scheduled_run_has_slot_claim",
        "release_state_unavailable",
        "scheduled_slot_claim_completion_conflict",
        "scheduled_slot_claim_conflict",
    }
    php_allowlist = classifier[
        classifier.index("$allowed = array(") : classifier.index(
            ");", classifier.index("$allowed = array(")
        )
    ]
    assert set(re.findall(r"'([a-z][a-z0-9_]{0,63})'", php_allowlist)) == (
        expected_validation_reasons
    )
    python_ingest = (ROOT / "curator" / "official_ingest.py").read_text(
        encoding="utf-8"
    )
    python_allowlist = python_ingest[
        python_ingest.index("_REMOTE_VALIDATION_REASON_ALLOWLIST = {") :
        python_ingest.index(
            "}",
            python_ingest.index("_REMOTE_VALIDATION_REASON_ALLOWLIST = {"),
        )
    ]
    assert set(re.findall(r'"([a-z][a-z0-9_]{0,63})"', python_allowlist)) == (
        expected_validation_reasons
    )


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


def test_schema_manifest_is_exact_contiguous_and_covers_slot_claims():
    manifest = V1[V1.index("function v1_expected_migration_manifest") : V1.index("function v1_require_schema_version")]
    assert "009_dart_global_quota_ledger" in manifest
    assert "010_official_slot_claim_ledger" in manifest
    assert "migration_manifest_cardinality_mismatch" in manifest
    assert "migration_manifest_entry_mismatch" in manifest
    assert "migration_checksum" in manifest
    assert "SELECT MAX(" not in manifest
    assert "009_dart_global_quota_ledger" in DART_QUOTA_MIGRATION
    assert "activist_official_site_snapshots" in OFFICIAL_SITE_MIGRATION
    assert "activist_official_slot_claims" in SLOT_CLAIM_MIGRATION


def test_schema_manifest_migrations_never_bless_preexisting_conflicts():
    assert "activist_008_record_migration" in OFFICIAL_SITE_MIGRATION
    assert "008 migration name conflict" in OFFICIAL_SITE_MIGRATION
    assert "008 migration checksum conflict" in OFFICIAL_SITE_MIGRATION
    assert "ON DUPLICATE KEY UPDATE migration_name=VALUES(migration_name)" not in OFFICIAL_SITE_MIGRATION
    assert "ON DUPLICATE KEY UPDATE migration_checksum=VALUES(migration_checksum)" not in OFFICIAL_SITE_MIGRATION
    assert "009 prerequisite migration manifest incomplete" in DART_QUOTA_MIGRATION
    assert "009 migration manifest conflict" in DART_QUOTA_MIGRATION
    assert "ON DUPLICATE KEY UPDATE" not in DART_QUOTA_MIGRATION
    assert "010 prerequisite migration manifest incomplete" in SLOT_CLAIM_MIGRATION
    assert "010 migration manifest conflict" in SLOT_CLAIM_MIGRATION
    assert "information_schema.columns" in SLOT_CLAIM_MIGRATION
    assert "information_schema.statistics" in SLOT_CLAIM_MIGRATION
    assert "ON DUPLICATE KEY UPDATE" not in SLOT_CLAIM_MIGRATION


def test_official_site_receipt_is_atomic_exact_and_never_downgrades_existing_content():
    section = V1[V1.index("function v1_official_site_contract_error") : V1.index("function upsert_governance_snapshot")]
    assert "v1_strict_canonical_json_encode($payloadCore" in section
    assert "connector_total_count_mismatch" in section
    assert "expected_ack_count" in section
    assert "official_site_source_right_ineligible" in section
    assert "v1_official_site_stable_id('site-doc',array($connectorId,$externalId,$contentHash),32)" in section
    assert "source_right_id,source_class,source_key,external_id" in section
    assert "$documentSourceIdentityEnabled = v1_global_dart_bridge_enabled" in section
    assert "if ($documentSourceIdentityEnabled) { $documentValues[] = $sourceKey; }" in section
    assert "($existing['source_key'] ?? '') !== $sourceKey" in section
    assert "SET retrieved_at=GREATEST(retrieved_at,?),updated_at=?" in section
    assert "correction_of_document_id" in section and "$versionNo = $latest ? (int)$latest['version_no'] + 1 : 1" in section
    assert "publication_status=VALUES(publication_status)" not in section
    assert "official_site_event_identity_conflict" in section
    assert "official_site_review_idempotency_conflict" in section
    assert "official_site_tombstone_idempotency_conflict" in section
    assert "connector_id, receipt_sha256" in OFFICIAL_SITE_MIGRATION
    assert "idx_official_site_review_snapshot_entity" in OFFICIAL_SITE_MIGRATION
    assert "uq_official_site_review_snapshot_entity" not in OFFICIAL_SITE_MIGRATION


def test_official_site_source_text_and_identity_dates_are_not_silently_transformed():
    section = V1[V1.index("function upsert_official_site_snapshot") : V1.index("function upsert_governance_snapshot")]
    assert "$title = (string)($document['title'] ?? '')" in section
    assert "$title = (string)($event['title'] ?? '')" in section
    assert "mb_substr($document['title']" not in section
    assert "mb_substr($event['title']" not in section
    assert "v1_normalize_identity_datetime($event['occurred_at'] ?? null,false)" in section
    assert "v1_normalize_identity_datetime($event['deadline_at'] ?? null,false)" in section
    assert "identity_effective_at'],$occurredAt" in section
    assert "identity_deadline_at'],$deadlineAt" in section


def test_stored_event_identity_is_disambiguated_by_its_canonical_key():
    helper = V1[
        V1.index("function v1_resolve_stored_event_identity")
        : V1.index("function v1_expected_migration_manifest")
    ]
    assert "preg_match('/^eventcmp:v1:[a-f0-9]{64}$/'" in helper
    assert "!is_string($effectiveAt) || !is_string($deadlineAt)" in helper
    assert helper.count(
        "preg_match('/^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$/'"
    ) == 2
    assert "preg_match('/^(\\d{4}-\\d{2}-\\d{2}) 00:00:00$/'" in helper
    assert "array_unshift($candidates,$parts[1])" in helper
    assert "foreach ($candidateSets[0] as $effectiveCandidate)" in helper
    assert "foreach ($candidateSets[1] as $deadlineCandidate)" in helper
    assert "hash_equals($storedKey,(string)$identity['comparison_key'])" in helper
    assert "$normalizedTuple === $storedTuple" in helper
    assert "return count($matches) === 1 ? $matches[0] : null" in helper
    assert V1.count("v1_resolve_stored_event_identity(") == 4

    event_loop = V1[
        V1.index("foreach ($normalizedEvents as $event)")
        : V1.index("$priorEventLink->execute", V1.index("foreach ($normalizedEvents as $event)"))
    ]
    assert "$storedIdentity = v1_resolve_stored_event_identity" in event_loop
    assert "(string)$existing['identity_status'] === 'complete'" in event_loop
    assert "hash_equals((string)$existing['comparison_key'],$event['comparison_key'])" in event_loop

    ingest = V1[V1.index("function upsert_governance_snapshot") :]
    assert "$canonicalStoredIdentity = " in ingest
    assert "? v1_resolve_stored_event_identity(" in ingest
    assert "$verifiedStoredIdentity = v1_resolve_stored_event_identity" in ingest
    assert "stored_event_identity_integrity_error" in ingest

    datetime_normalizer = V1[
        V1.index("function v1_normalize_identity_datetime")
        : V1.index("function v1_build_event_identity")
    ]
    assert "return array('canonical'=>$text, 'mysql'=>$text . ' 00:00:00')" in datetime_normalizer
    assert "'canonical'=>str_replace(' ', 'T', $mysql) . '+00:00'" in datetime_normalizer


def test_official_run_ledger_is_slot_attributed_and_python_digest_compatible():
    section = V1[V1.index("function v1_official_run_metric") : V1.index("function v1_ops_release_evidence")]
    assert "v1_strict_canonical_json_encode($row" in section
    assert "trigger_created_at" in section
    assert "v1_official_scheduled_run_matches" in section
    assert "v1_kst_observation_date($sortAt)" in section
    assert "modify('-1 day')" in section and "modify('+1 day')" in section
    assert "scheduled_slot_at" in section and "_sort_at" in section
    assert "$triggerTime < $nextSlot" not in section
    assert "slot_claim_id" in section
    assert "v1_official_next_cadence_slot" in section
    assert "next_cadence_slot_at" in section
    assert "claim_lag_seconds" in section
    assert "in_array($source,$selectedSources,true)" in section


def test_dart_review_corpus_is_private_exact_bounded_and_digest_compatible():
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    assert spec["info"]["version"] == "1.12.1"
    assert spec["x-changelog"][0]["version"] == "1.12.1"
    route = spec["paths"]["/ops/dart-review-corpus"]["get"]
    assert route["security"] == [{"bearerAuth": []}]
    response = spec["components"]["schemas"]["DartReviewCorpusPage"]
    assert response["additionalProperties"] is False
    assert set(response["required"]) == {
        "ok",
        "contract_version",
        "range",
        "population_count",
        "corpus_sha256",
        "backend_binding_id",
        "items",
        "next_cursor",
        "api_version",
    }
    assert set(response["properties"]) == set(response["required"])
    item = spec["components"]["schemas"]["DartReviewCorpusItem"]
    expected_item_keys = {
        "document_id",
        "event_id",
        "company_id",
        "company_name",
        "event_type",
        "revision_status",
        "external_id",
        "title",
        "original_language",
        "original_url",
        "published_at",
        "source_right_id",
        "correction_of_document_id",
        "version_no",
        "has_later_correction",
        "has_successor",
        "is_correction",
        "is_cancelled",
        "event_verification_status",
        "document_verification_status",
        "document_publication_status",
        "identity_status",
        "review_status",
        "importance",
    }
    assert item["additionalProperties"] is False
    assert set(item["required"]) == expected_item_keys
    assert set(item["properties"]) == expected_item_keys
    assert item["properties"]["source_right_id"]["const"] == "official:dart"
    assert item["properties"]["company_name"]["pattern"] == r".*\S.*"
    assert item["properties"]["title"]["pattern"] == r".*\S.*"
    assert (
        item["properties"]["original_language"]["pattern"]
        == r"^[a-z]{2,3}(?:-[A-Z]{2})?$"
    )
    assert item["properties"]["original_url"]["pattern"].endswith("://")
    for field in (
        "event_type",
        "event_verification_status",
        "document_verification_status",
        "document_publication_status",
        "identity_status",
        "review_status",
        "importance",
    ):
        assert item["properties"][field]["pattern"] == r"^[a-z][a-z0-9_.:\-]{0,63}$"

    dispatch = V1[
        V1.index("function handle_v1_request") : V1.index("function v1_serve_openapi")
    ]
    assert "'/ops/dart-review-corpus'" in dispatch
    assert dispatch.index("$preauthorizedRole = v1_require_role") < dispatch.index(
        "$privileged = strpos($path, '/ops/')"
    )
    section = V1[
        V1.index("function v1_dart_review_corpus_cursor_encode")
        : V1.index("function v1_ops_release_evidence")
    ]
    assert "source_right_id=?" in section
    assert "array('official:dart','official_disclosure',$startUtc,$endUtc)" in section
    assert "d.published_at>=? AND d.published_at<?" in section
    assert "ORDER BY d.published_at ASC,BINARY d.document_id ASC,BINARY e.event_id ASC" in section
    assert "COUNT(ed.document_id) AS link_count" in section
    assert "company_match_count<>1" in section
    assert "dart_review_corpus_integrity_error" in section
    assert "WITH RECURSIVE target_chain" in section
    assert "dart_review_corpus_lineage_error" in section
    assert "predecessor.version_no+1<>chain.version_no" in section
    assert "predecessor.collection_key<=>chain.collection_key" in section
    assert "successor.correction_of_document_id=predecessor.document_id" in section
    assert "chain.cycle_detected=1" in section
    assert "v1_strict_canonical_json_encode($item" in section
    assert "hash_update($hash" in section and ' . "\\n"' in section
    assert "V1_RESPONSE_BUDGET_BYTES" in section
    assert "dart-review-corpus-v1" in section
    assert "Asia/Seoul" in section
    assert "$to > $serverDate" in section
    assert "$rangeDays > 31" in section
    assert "JSON_VALID(d.payload_json)" in section
    assert "$.has_later_correction" in section
    assert "JSON_VALID(e.payload_json)" in section
    assert "$.is_correction" in section
    assert "$.is_cancelled" in section
    assert "'payload_json'=>" not in section
    assert "'has_later_correction'=>$hasLaterCorrection" in section
    assert "'has_successor'=>$hasSuccessor" in section
    assert "$isCorrection = (int)$row['event_is_correction'] === 1" in section
    assert "$isCancelled = (int)$row['event_is_cancelled'] === 1" in section
    assert "$correctionOf === $documentId" in section
    assert "successor.document_id<>d.document_id" in section
    assert "createFromFormat('!Ymd',substr($externalId,0,8)" in section
    assert "str_replace('-','',substr($publishedAt,0,10))" not in section
    assert "v1_dart_review_corpus_required_text($companyName,255)" in section
    assert "v1_dart_review_corpus_required_text($title,700)" in section
    assert "/^[a-z]{2,3}(?:-[A-Z]{2})?$/" in section
    assert "v1_dart_review_corpus_https_url($originalUrl)" in section
    assert "v1_dart_review_corpus_token($token)" in section
    assert "dart_review_corpus_metadata_error" in section
    assert "Cache-Control: private, no-store" in section
    assert "'backend_binding_id'=>$backendBindingId" in section
    assert response["properties"]["backend_binding_id"]["pattern"] == "^[a-f0-9]{64}$"
    for status in (
        "withdrawal_linked",
        "withdrawal_unlinked",
        "correction_linked",
        "correction_unlinked",
        "original_superseded",
        "current",
    ):
        assert status in section


def test_health_accepts_only_complete_acknowledged_scheduled_runs_and_reports_deployment():
    section = V1[V1.index("function v1_ops_health") : V1.index("function v1_kind_source_right_eligibility")]
    assert "v1_official_scheduled_run_matches($ledger)" in section
    assert "(int)$ledger['raw_count'] === (int)$ledger['acknowledged_count']" in section
    assert "$outcome['raw_count'] === $outcome['acknowledged_count']" in section
    assert "last_scheduled_success_at" in section
    assert "active_deployment_status" in section


def test_dart_quota_pool_is_global_kst_day_atomic_and_idempotent():
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    assert {"get", "post"} <= spec["paths"]["/ops/dart-quota"].keys()
    post = spec["paths"]["/ops/dart-quota"]["post"]
    durable_ack = post["x-durable-ack-contract"]
    assert durable_ack["commit-confirmed-before-200"] is True
    assert durable_ack["independent-fresh-connection-readback-before-200"] is True
    assert durable_ack["exact-post-replay-required"] is True
    assert durable_ack["explicit-replay-duplicate-required"] is True
    persistence = durable_ack["persistence-failure"]
    assert persistence["http-status"] == 503
    assert persistence["code"] == "dart_quota_persistence_failed"
    safe_persistence_details = {
        "transaction_commit_failed",
        "transaction_state_invalid",
        "transaction_readback_connection_failed",
        "transaction_readback_binding_failed",
        "transaction_readback_attempt_failed",
        "transaction_readback_day_failed",
        "transaction_readback_credential_failed",
    }
    assert set(persistence["detail-enum"]) == safe_persistence_details
    persistence_schema = spec["components"]["schemas"]["DartQuotaPersistenceFailure"]
    assert persistence_schema["additionalProperties"] is False
    assert set(persistence_schema["required"]) == {
        "ok",
        "error",
        "server_kst_date",
        "api_version",
    }
    persistence_error = persistence_schema["properties"]["error"]
    assert persistence_error["additionalProperties"] is False
    assert persistence_error["properties"]["code"]["const"] == (
        "dart_quota_persistence_failed"
    )
    assert (
        set(persistence_error["properties"]["detail"]["enum"])
        == safe_persistence_details
    )
    assert post["responses"]["503"]["content"]["application/json"]["schema"][
        "oneOf"
    ][0] == {"$ref": "#/components/schemas/DartQuotaPersistenceFailure"}
    assert (
        "expected_backend_binding_id"
        in spec["components"]["schemas"]["DartQuotaConsumeRequest"]["required"]
    )
    assert (
        "expected_backend_binding_id"
        in spec["components"]["schemas"]["DartQuotaBlockRequest"]["required"]
    )
    assert (
        "credential_id"
        in spec["components"]["schemas"]["DartQuotaConsumeRequest"]["required"]
    )
    assert (
        "expected_backend_binding_id"
        in spec["components"]["schemas"]["DartQuotaDisableRequest"]["required"]
    )
    assert "backend_binding_id" in spec["components"]["schemas"]["DartQuotaStatus"]["required"]
    assert (
        "backend_binding_id"
        in spec["components"]["schemas"]["DartQuotaAcknowledgement"]["required"]
    )
    assert "activist_dart_quota_days" in DART_QUOTA_MIGRATION
    assert "activist_dart_quota_attempts" in DART_QUOTA_MIGRATION
    assert "activist_dart_quota_credentials" in DART_CREDENTIAL_POOL_MIGRATION
    assert "activist_dart_quota_credential_days" in DART_CREDENTIAL_POOL_MIGRATION
    assert "@bside_migration_012_sha256" in DART_CREDENTIAL_POOL_MIGRATION
    assert "legacy-single" in DART_CREDENTIAL_POOL_MIGRATION
    section = V1[V1.index("function v1_dart_quota_server_day") : V1.index("function v1_ops_official_site_candidates")]
    assert "Asia/Seoul" in section
    assert "used_count=used_count+1" in section
    assert "used_count<limit_count" in section
    assert "dart_quota_idempotency_conflict" in section
    assert "opendart_status_020" in section
    assert "opendart_status_901" in section
    assert "dart_credential_blocked" in section
    assert "dart_credential_disabled" in section
    assert "GOV_V1_DART_GLOBAL_DAILY_LIMIT = 40000" in V1
    assert "COALESCE(blocked_until,?)" in section
    assert "v1_dart_quota_block_until_utc($quotaDay)" in section
    assert "$allowPreviousDay && $day === $today->modify('-1 day')->format('Y-m-d')" in section
    assert "$action === 'block_020' || $action === 'disable_901'" in section
    assert (
        "if (!$attempt || (string)$attempt['quota_day'] !== $quotaDay"
        in section
    )
    assert "|| (string)$attempt['credential_id'] !== $credentialId" in section
    assert "|| (int)$attempt['consumed_units'] !== 1" in section
    assert "'accepted'=>$action === 'status' ? 0 : 1" in section
    assert "SELECT @@server_uuid AS server_uuid,DATABASE() AS database_name" in section
    assert '"mysql8\\n"' in section
    binding_check = section.index(
        "if (!hash_equals($backendBindingId,$expectedBackendBindingId))"
    )
    transaction_start = section.index("$pdo->beginTransaction()")
    assert binding_check < transaction_start
    assert "backend_binding_required" in section
    assert "backend_binding_mismatch" in section
    assert "'backend_binding_id'=>$backendBindingId" in section
    assert "'server_uuid'=>" not in section
    assert "'database_name'=>" not in section
    commit_readback = section[
        section.index("function v1_dart_quota_commit_and_readback") :
        section.index("function v1_dart_quota_validate_credential_id")
    ]
    assert "if ($requestHashColumn === '' || !$pdo->inTransaction())" in commit_readback
    assert "$committed = $pdo->commit();" in commit_readback
    commit_call = commit_readback[
        commit_readback.index("try {\n        $committed = $pdo->commit();") :
        commit_readback.index("try {\n        $readbackPdo = pdo_conn($config);")
    ]
    assert "catch (Throwable $commitError)" in commit_call
    assert (
        "throw new RuntimeException('dart_quota_commit_threw',0,$commitError);"
        in commit_call
    )
    assert "$committed !== true" in commit_call
    assert "throw new RuntimeException('dart_quota_commit_returned_false');" in commit_call
    assert "$pdo->inTransaction()" in commit_call
    assert (
        "throw new RuntimeException('dart_quota_transaction_state_after_commit');"
        in commit_call
    )
    assert commit_readback.index("$committed = $pdo->commit();") < commit_readback.index(
        "$readbackPdo = pdo_conn($config);"
    )
    assert "$readbackPdo === $pdo" in commit_readback
    assert "v1_backend_binding_id_value($readbackPdo,$config)" in commit_readback
    assert "hash_equals($expectedBackendBindingId,$readbackBindingId)" in commit_readback
    assert "$attemptReadback = $readbackPdo->prepare(" in commit_readback
    assert "$dayReadback = $readbackPdo->prepare(" in commit_readback
    assert "$credentialReadback = $readbackPdo->prepare(" in commit_readback
    assert (
        "$durableAttempt = v1_dart_quota_fetch_one_and_close("
        in commit_readback
    )
    assert "$durableDay = v1_dart_quota_fetch_one_and_close(" in commit_readback
    assert (
        "$durableCredentialDay = v1_dart_quota_fetch_one_and_close("
        in commit_readback
    )
    assert (
        "WHERE attempt_id=? AND quota_day=? AND credential_id=? AND code_revision=? "
        in commit_readback
    )
    assert "AND consumed_units=1 LIMIT 1" in commit_readback
    assert "in_array((string)$durableAttempt['status'],$allowedStatuses,true)" in commit_readback
    assert "hash_equals((string)$durableAttempt[$requestHashColumn],$requestHash)" in commit_readback
    assert (
        "hash_equals($attemptId,(string)$durableCredentialDay['blocked_by_attempt_id'])"
        in commit_readback
    )
    assert (
        "hash_equals($attemptId,(string)$durableCredentialDay['disabled_by_attempt_id'])"
        in commit_readback
    )
    assert "dart_quota_attempt_readback_failed" in commit_readback
    assert "dart_quota_day_readback_failed" in commit_readback
    assert "dart_quota_credential_day_readback_failed" in commit_readback
    expected_persistence_details = {
        "transaction_commit_failed",
        "transaction_state_invalid",
        "transaction_readback_connection_failed",
        "transaction_readback_binding_failed",
        "transaction_readback_attempt_failed",
        "transaction_readback_day_failed",
        "transaction_readback_credential_failed",
    }
    assert {
        value
        for value in expected_persistence_details
        if f"return '{value}';" in commit_readback
    } == expected_persistence_details
    persistence_log = commit_readback[
        commit_readback.index("function v1_dart_quota_log_persistence_failure") :
    ]
    logged_phase_literals = {
        value
        for value in expected_persistence_details
        if f"'{value}'" in persistence_log
    }
    assert logged_phase_literals == expected_persistence_details
    assert "$safePhase = in_array($phase,$allowedPhases,true)" in persistence_log
    assert "? $phase : 'transaction_state_invalid';" in persistence_log
    expected_internal_outcomes = {
        "commit_threw",
        "commit_returned_false",
        "transaction_state_after_commit",
        "cursor_close_threw",
        "cursor_close_returned_false",
        "persistence_failure",
    }
    logged_outcome_literals = {
        value
        for value in expected_internal_outcomes
        if f"'{value}'" in persistence_log
    }
    assert logged_outcome_literals == expected_internal_outcomes
    assert "$safeOutcome = in_array($outcome,$safeOutcomes,true)" in persistence_log
    assert "? $outcome : 'persistence_failure';" in persistence_log
    assert "v1_dart_quota_sql_diagnostic($error)" in persistence_log
    assert persistence_log.count("error_log(") == 1
    assert "phase=' . $safePhase" in persistence_log
    assert "' outcome=' . $safeOutcome" in persistence_log
    assert "' sqlstate_class=' . $sqlStateClass" in persistence_log
    assert "' driver_code=' . $driverCode" in persistence_log
    assert "get_class(" not in persistence_log
    assert " exception=" not in persistence_log
    assert "getMessage(" not in persistence_log
    assert "getPrevious(" not in persistence_log
    assert " message=" not in persistence_log
    assert "preg_replace(" not in persistence_log
    assert "substr(" not in persistence_log
    assert (
        "v1_dart_quota_error(503,'dart_quota_persistence_failed',$phase);"
        in section
    )
    assert "array('blocked_020','disabled_901')" in section
    assert section.count("v1_dart_quota_commit_and_readback(") == 7
    assert "$pdo->commit(); v1_respond(200,v1_dart_quota_payload(" not in section


def test_official_slot_claims_use_durable_global_oldest_identity_and_epoch_guard():
    spec = yaml.safe_load(OPENAPI_PATH.read_text(encoding="utf-8"))
    assert {"get", "post"} <= spec["paths"]["/ops/official-slot-claims"].keys()
    assert {"get", "post"} <= spec["paths"]["/admin/official-slot-epoch"].keys()
    section = V1[
        V1.index("function v1_official_slot_claim_error") : V1.index(
            "function v1_official_run_ledger_row"
        )
    ]
    assert "official_slot_claim_activated" in section
    assert "modify('+1 day')->setTime(0,0,0)" in section
    assert "foreach (array('0,15,30,45 22-23 * * *','0,15,30,45 0-14 * * *','0,30 15-21 * * *') as $family)" in section
    assert "sort($dueSlots,SORT_STRING)" in section
    assert "official_slot_repair_not_oldest" in section
    assert "rerun_after_next_cadence" in section
    assert "official_slot_epoch_reset_requires_closed_release" in section
    assert "official_slot_epoch_version_conflict" in section
    assert "claims_preserved'=>true" in section
    evidence = V1[
        V1.index("function v1_ops_release_evidence") : V1.index(
            "function v1_release_request_id"
        )
    ]
    assert "official_slot_epoch_boundary_in_evidence_range" in evidence


def test_slot_claim_migration_has_canonical_checksum_and_exact_shape_guards():
    assert "2b8be6264c8a4f3be038729fbf6bbe22e720457874f02c89c82d33db9dc78f51" in SLOT_CLAIM_MIGRATION
    assert "slot claim ledger column shape mismatch" in SLOT_CLAIM_MIGRATION
    assert "slot claim ledger index shape mismatch" in SLOT_CLAIM_MIGRATION
    assert "slot claim epoch index shape mismatch" in SLOT_CLAIM_MIGRATION


def test_canonical_route_and_csv_formula_guards_apply_before_dispatch():
    request_path = V1[V1.index("function v1_canonical_route_path") : V1.index("function v1_respond")]
    assert request_path.count("rawurldecode") == 1
    assert "v1_request_path(); // canonicalized once for both CORS and dispatch" in API
    csv = V1[V1.index("function v1_csv_export_cell") : V1.index("function v1_export_events_csv")]
    assert "=+\\-@" in csv
    assert "v1_csv_export_cell" in V1[V1.index("function v1_export_events_csv") : V1.index("function v1_events_atom")]
