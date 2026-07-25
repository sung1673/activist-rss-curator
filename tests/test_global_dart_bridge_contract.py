from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
V1 = (ROOT / "deploy" / "activist" / "governance_v1.php").read_text(
    encoding="utf-8"
)


def _section(start: str, end: str) -> str:
    return V1[V1.index(start) : V1.index(end)]


def test_bridge_is_additive_and_requires_the_exact_schema_11_manifest():
    gate = _section(
        "function v1_global_dart_bridge_enabled",
        "function v1_global_event_family_for_legacy_type",
    )
    assert "function_exists('v2_schema_manifest_status')" in gate
    assert "$manifest['valid'] === true" in gate
    assert "(int)$manifest['highest_version'] >= 11" in gate
    assert "migration_version=11" not in gate
    assert "migration_checksum" not in gate
    assert "return false;" in gate

    ingest = _section(
        "function upsert_governance_snapshot",
        "function v1_editorial_reference_exists",
    )
    assert "$globalDartBridgeEnabled = v1_global_dart_bridge_enabled" in ingest
    assert "if ($globalDartBridgeEnabled)" in ingest


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
