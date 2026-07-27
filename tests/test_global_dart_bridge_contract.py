from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
V1 = (ROOT / "deploy" / "activist" / "governance_v1.php").read_text(
    encoding="utf-8"
)
V2 = (ROOT / "deploy" / "activist" / "governance_v2.php").read_text(
    encoding="utf-8"
)
V2_WRITE = (
    ROOT / "deploy" / "activist" / "governance_v2_write.php"
).read_text(encoding="utf-8")
API = (ROOT / "deploy" / "activist" / "api.php").read_text(encoding="utf-8")
PHP_SMOKE = (
    ROOT / "tests" / "php73_release_state_smoke.py"
).read_text(encoding="utf-8")


def _section(start: str, end: str) -> str:
    return V1[V1.index(start) : V1.index(end)]


def test_bridge_is_additive_and_requires_the_exact_schema_12_manifest():
    gate = _section(
        "function v1_global_dart_bridge_enabled",
        "function v1_global_event_family_for_legacy_type",
    )
    assert "function_exists('v2_schema_manifest_status')" in gate
    assert "$manifest['valid'] === true" in gate
    assert "(int)$manifest['highest_version'] >= 12" in gate
    assert "migration_version=11" not in gate
    assert "migration_checksum" not in gate
    assert "return false;" in gate

    ingest = _section(
        "function upsert_governance_snapshot",
        "function v1_editorial_reference_exists",
    )
    assert "$globalDartBridgeEnabled = v1_global_dart_bridge_enabled" in ingest
    assert "$globalDartProjectionEnabled = false" in ingest
    assert "$globalDartProjectionEnabled = true" in ingest
    assert "if ($globalDartProjectionEnabled)" in ingest
    assert "upsert_governance_snapshot_dart_guarded" in API
    assert "dart_guarded_action_required" in ingest
    assert "dart_deployment_revision_mismatch" in ingest
    assert "v2_deployment_identity_status()" in ingest
    projection = ingest[ingest.index("$globalIssuerStmt = null") :]
    assert "$globalDartBridgeEnabled" not in projection


def test_guarded_lineage_covers_corrections_observations_and_partial_projection_rows():
    candidate_section = _section(
        "function v1_governance_snapshot_lineage_candidates",
        "function v1_lock_existing_dart_lineage",
    )
    lineage_section = _section(
        "function v1_lock_existing_dart_lineage",
        "function v1_global_event_family_for_legacy_type",
    )
    assert "correction_of_document_id" in candidate_section
    assert "$documentIds[$correctionOf] = true" in candidate_section
    assert "table_name($config,'event_observations')" in lineage_section
    assert "eo.source_key=\\'dart\\'" in lineage_section
    assert "predecessor.source_right_id=\\'official:dart\\'" in lineage_section
    assert "$issuerId === 'issuer:kr:dart:' . $companyId" in lineage_section
    assert "$countryCode === 'KR'" in lineage_section
    ingest = _section(
        "function upsert_governance_snapshot",
        "function v1_editorial_reference_exists",
    )
    assert ingest.count("predecessor.source_right_id=?") >= 2


def test_lineage_uses_derived_document_ids_and_covers_company_and_run_owners():
    identity = _section(
        "function v1_governance_snapshot_document_id",
        "function v1_governance_snapshot_lineage_candidates",
    )
    candidates = _section(
        "function v1_governance_snapshot_lineage_candidates",
        "function v1_lock_existing_dart_lineage",
    )
    lineage = _section(
        "function v1_lock_existing_dart_lineage",
        "function v1_global_event_family_for_legacy_type",
    )
    ingest = _section(
        "function upsert_governance_snapshot",
        "function v1_editorial_reference_exists",
    )

    assert "function v1_normalize_governance_snapshot_documents" in identity
    assert "'dart' : 'doc'" in identity
    assert "$documents = v1_normalize_governance_snapshot_documents" in ingest
    assert ingest.index(
        "$documents = v1_normalize_governance_snapshot_documents"
    ) < ingest.index("v1_governance_snapshot_lineage_candidates(")
    assert "array $companies" in candidates
    assert "array $run" in candidates
    assert "'company_ids'=>array_keys($companyIds)" in candidates
    assert "'run_ids'=>array_keys($runIds)" in candidates
    assert "table_name($config,'companies')" in lineage
    assert "table_name($config,'collection_runs')" in lineage
    assert "in_array('dart',$sourceTokens,true)" in lineage


def test_dart_write_lock_order_and_connector_kill_switch_are_fail_closed():
    ingest = _section(
        "function upsert_governance_snapshot",
        "function v1_editorial_reference_exists",
    )
    release_lock = ingest.index(
        "$dartReleaseStates = v2_release_state_rows_for_update"
    )
    right_lock = ingest.index("$dartRight = v2_source_right_row(")
    connector_lock = ingest.index(
        "$dartConnector = v1_lock_global_dart_connector"
    )
    lineage_lock = ingest.index("v1_lock_existing_dart_lineage(")
    first_mutation = ingest.index("$companyStmt = $pdo->prepare")
    assert release_lock < right_lock < connector_lock < lineage_lock < first_mutation
    assert "array('configured','active')" in ingest
    assert "dart_connector_inactive" in ingest
    assert "dart_connector_not_ready" in ingest

    admin = V2_WRITE[
        V2_WRITE.index("function v2_admin_update_connector") :
        V2_WRITE.index("function v2_global_issuer_id")
    ]
    assert admin.index("v2_release_state_rows_for_update") < admin.index(
        "v2_source_right_row("
    ) < admin.index("LIMIT 1 FOR UPDATE")

    global_ingest = V2_WRITE[V2_WRITE.index("function v2_ops_ingest") :]
    transaction = global_ingest[global_ingest.index("$pdo->beginTransaction()") :]
    assert transaction.index("$lockedRight = v2_source_right_row(") < transaction.index(
        "$connectorLock = $pdo->prepare("
    )

    eligibility = V2[
        V2.index("function v2_ops_source_right_eligibility") :
        V2.index("function v2_brief_event_rows")
    ]
    assert "'connector:kr:dart'" in eligibility
    assert "'connector_ready'" in eligibility
    assert "'connector_status'" not in eligibility[eligibility.index("$response = array(") :]


def test_signed_dart_precondition_marks_company_master_only_payload_as_guarded():
    ingest = _section(
        "function upsert_governance_snapshot",
        "function v1_editorial_reference_exists",
    )
    expectation = ingest.index(
        "$dartExpectation = v1_dart_source_right_expectation($payload);"
    )
    marks_dart = ingest.index(
        "if ($dartExpectation !== null) { $containsDartWrite = true; }"
    )
    generic_rejection = ingest.index(
        "if ($containsDartWrite && !$dartGuardedAction)"
    )
    guarded_payload_rejection = ingest.index(
        "if ($dartGuardedAction && !$containsDartWrite)"
    )

    assert expectation < marks_dart < generic_rejection
    assert marks_dart < guarded_payload_rejection


def test_php_mysql_smoke_proves_all_dart_guards_are_no_mutation():
    assert 'company_master_only_id = "00999979"' in PHP_SMOKE
    assert '"upsert_governance_snapshot_dart_guarded",' in PHP_SMOKE
    assert (
        "exact guarded DART company-master-only chunk was not projected"
        in PHP_SMOKE
    )

    assert 'derived_identity_document.pop("document_id", None)' in PHP_SMOKE
    assert 'derived_identity_document.pop("source_right_id", None)' in PHP_SMOKE
    assert 'derived_identity_document.pop("source", None)' in PHP_SMOKE
    assert '"MAX(original_url),MAX(content_hash) FROM ci_documents "' in PHP_SMOKE
    assert "missing document_id bypass partially changed" in PHP_SMOKE

    assert "generic_company_rewrite" in PHP_SMOKE
    assert "company-only generic action changed" in PHP_SMOKE
    assert "generic_run_rewrite" in PHP_SMOKE
    assert "generic run-only action changed" in PHP_SMOKE

    assert "connector_ready\") is False" in PHP_SMOKE
    assert "inactive_write.get(\"error\") == \"dart_connector_inactive\"" in PHP_SMOKE
    assert '== "0\\t0\\t0\\t0\\tinactive"' in PHP_SMOKE
    assert "inactive DART connector allowed a data mutation" in PHP_SMOKE


def test_dart_companies_are_projected_to_stable_issuer_identity_and_listing_rows():
    ingest = _section(
        "function upsert_governance_snapshot",
        "function v1_editorial_reference_exists",
    )
    assert "'issuer:kr:dart:' . $companyId" in ingest
    assert "table_name($config,'issuers')" in ingest
    assert "table_name($config,'issuer_identifiers')" in ingest
    assert "table_name($config,'issuer_listings')" in ingest
    assert "'DART_CORP_CODE',$companyId,'KRX',1" in ingest
    assert "'TICKER',$stockCode,$market,0" in ingest
    assert "'listing:kr:' . $companyId" in ingest
    assert "'identity_namespace'=>'DART_CORP_CODE'" in ingest
    assert "'bridge'=>'v1_official_ingest'" in ingest


def test_global_issuer_insert_select_qualifies_existing_target_columns():
    ingest = _section(
        "function upsert_governance_snapshot",
        "function v1_editorial_reference_exists",
    )
    issuer_upsert = ingest[
        ingest.index("$globalIssuerTable =")
        : ingest.index("$globalIdentifierStmt = $pdo->prepare")
    ]

    assert "$globalIssuerTable = table_name($config,'issuers');" in issuer_upsert
    for field in (
        "legal_name_en",
        "short_name",
        "homepage_url",
        "master_modified_at",
    ):
        assert "$globalIssuerTable . '." + field in issuer_upsert


def test_only_official_dart_documents_receive_the_global_dart_projection():
    ingest = _section(
        "function upsert_governance_snapshot",
        "function v1_editorial_reference_exists",
    ).replace("\\'", "'")
    assert (
        "SET issuer_id=?,country_code='KR',source_key='dart',"
        "filed_at=COALESCE(filed_at,?)"
        in ingest
    )
    assert "source_right_id='official:dart'" in ingest
    assert (
        "strtolower(trim((string)v1_first($document,"
        "array('source_right_id'),''))) === 'official:dart'"
        in ingest
    )


def test_dart_events_receive_a_global_family_only_with_dart_evidence():
    family = _section(
        "function v1_global_event_family_for_legacy_type",
        "function v1_global_dart_metric_count",
    )
    expected = {
        "five_percent_holding": "large_ownership",
        "shareholder_proposal": "meeting_and_vote",
        "general_meeting": "meeting_and_vote",
        "tender_offer": "tender_offer_and_mna",
        "merger": "tender_offer_and_mna",
        "split": "tender_offer_and_mna",
        "rights_issue": "capital_issuance",
        "convertible_bond": "capital_issuance",
        "bond_with_warrant": "capital_issuance",
        "exchangeable_bond": "capital_issuance",
        "dividend": "capital_return",
        "treasury_shares": "capital_return",
        "value_up": "capital_return",
        "board": "board_and_compensation",
        "executive_compensation": "board_and_compensation",
        "trading_suspension": "listing_status",
        "delisting": "listing_status",
        "duplicate_listing": "listing_status",
    }
    for legacy_type, global_family in expected.items():
        assert f"'{legacy_type}'=>'{global_family}'" in family

    ingest = _section(
        "function upsert_governance_snapshot",
        "function v1_editorial_reference_exists",
    ).replace("\\'", "'")
    assert "bridge_event.issuer_id=?" in ingest
    assert "bridge_event.country_code='KR'" in ingest
    assert "bridge_event.global_event_family=?" in ingest
    assert "bridge_event.first_observed_at=COALESCE" in ingest
    assert "bridge_d.source_right_id='official:dart'" in ingest


def test_dart_event_title_provenance_is_derived_only_from_linked_official_evidence():
    ingest = _section(
        "function upsert_governance_snapshot",
        "function v1_editorial_reference_exists",
    )
    evidence = ingest[
        ingest.index("$hasOfficialDartEvidence = false;")
        : ingest.index("$eventStmt->execute(array(")
    ]
    assert "$documentSourceRightIds" in ingest
    assert "SELECT source_class,source_right_id" in ingest
    assert "$evidenceClass === 'official_disclosure'" in evidence
    assert "$evidenceSourceRightId === 'official:dart'" in evidence
    assert "if ($hasOfficialDartEvidence)" in evidence
    assert "dart_event_metadata_invalid" in evidence
    assert "dart_title_provenance_conflict" in evidence
    assert "$event['metadata']['title_provenance'] = 'source';" in evidence
    assert evidence.index("$hasOfficialDartEvidence = true;") < evidence.index(
        "$event['metadata']['title_provenance'] = 'source';"
    )
    assert "dart_document_title_provenance_conflict:" in ingest
    assert "$document['metadata']['title_provenance'] = 'source';" in ingest


def test_connector_freshness_uses_source_scoped_success_and_exact_ack_counts():
    outcome = _section(
        "function v1_global_dart_run_outcome",
        "function v1_bridge_dart_connector_run",
    )
    assert "in_array('dart',$sourceTokens,true)" in outcome
    assert "source_outcomes" in outcome
    assert "source_ack_counts" in outcome
    assert "$raw === $ack" in outcome
    assert "$errors === 0" in outcome
    assert "in_array($status,array('success','succeeded'),true)" in outcome

    bridge = _section(
        "function v1_bridge_dart_connector_run",
        "function upsert_governance_snapshot",
    ).replace("\\'", "'")
    assert "if ($outcome['selected'] !== true) { return; }" in bridge
    assert "connector_id=? AND country_code='KR' AND source_key='dart'" in bridge
    assert "'connector:kr:dart'" in bridge
    assert "LIMIT 1 FOR UPDATE" not in bridge
    assert "array('configured','active')" in bridge
    assert "connector_status IN ('configured','active')" in bridge
    assert "&& $runCompletionValid" in bridge
    assert "$finishedAt !== null && $codeRevision !== null && $hasDurableWindow" in bridge
    assert "window_end_inclusive" in bridge
    assert "$storedWindowEnd > $windowEnd" in bridge
    assert "$cursorJson = (string)$connector['cursor_json']" in bridge
    assert "last_success_at=?" in bridge
    assert "last_raw_count=?" in bridge
    assert "last_acknowledged_count=?" in bridge
    assert "code_revision=?" in bridge


def test_failed_partial_and_kind_only_runs_cannot_advance_dart_last_success():
    bridge = _section(
        "function v1_bridge_dart_connector_run",
        "function upsert_governance_snapshot",
    )
    success_statement = bridge[
        bridge.index("$update = $pdo->prepare")
        : bridge.index("$update->execute")
    ]
    failure_statement = bridge[
        bridge.index("$failure = $pdo->prepare")
        : bridge.index("$failure->execute")
    ]
    assert "last_success_at=?" in success_statement
    assert "cursor_json=?" in success_statement
    assert "last_success_at" not in failure_statement
    assert "cursor_json" not in failure_statement
    assert "last_checked_at=?" in failure_statement
    assert "last_error_class=?" in failure_statement
    assert "kind" not in bridge.casefold()
