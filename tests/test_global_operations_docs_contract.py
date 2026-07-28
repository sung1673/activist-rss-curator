from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
README = (ROOT / "README.md").read_text(encoding="utf-8")
OPERATIONS = (ROOT / "docs" / "operations-automation.md").read_text(
    encoding="utf-8"
)
API_DOCS = (ROOT / "docs" / "governance-api-v2.md").read_text(
    encoding="utf-8"
)
ALPHA_EVIDENCE_DOCS = (
    ROOT / "docs" / "global-alpha-release-evidence.md"
).read_text(encoding="utf-8")
GLOBAL_BACKFILL_DOCS = (
    ROOT / "docs" / "global-official-backfill.md"
).read_text(encoding="utf-8")
CUTOVER_DOCS = (
    ROOT / "docs" / "governance-cutover-rollback.md"
).read_text(encoding="utf-8")
MIGRATION = (
    ROOT
    / "deploy"
    / "activist"
    / "migrations"
    / "011_global_terminal_v2.sql"
).read_text(encoding="utf-8")
SELECTED_RUNNER = (
    ROOT / "curator" / "selected_market_ingest.py"
).read_text(encoding="utf-8")

WORKFLOWS = {
    name: (ROOT / ".github" / "workflows" / name).read_text(encoding="utf-8")
    for name in (
        "ingest-global.yml",
        "ingest-selected-markets.yml",
        "global-brief.yml",
        "global-alpha-observation-chain.yml",
        "global-alpha-watchdog.yml",
        "governance-cutover.yml",
        "governance-rollback.yml",
    )
}


def test_new_production_alpha_workflows_are_named_and_documented():
    for name in WORKFLOWS:
        assert f"`{name}`" in README
        assert f"`{name}`" in OPERATIONS
    assert "Production Alpha" in README
    assert "Production Alpha" in OPERATIONS


def test_migration_011_deployment_is_bound_to_the_exact_sql_bytes():
    for document in (OPERATIONS, API_DOCS):
        assert "`migrations/011_global_terminal_v2.sql`" in document
        assert "@bside_migration_011_sha256" in document
        assert "byte-preserving" in document
    assert "같은 MySQL 연결의 같은 입력 stream" in OPERATIONS
    assert "schema_migrations" in API_DOCS


def test_global_source_secrets_and_variables_match_workflows():
    global_workflow = WORKFLOWS["ingest-global.yml"]
    secret_names = {
        "BSIDE_API_BASE_URL",
        "BSIDE_OPS_TOKEN",
    }
    variable_names = {
        "GOVERNANCE_PIPELINE_MODE",
        "GOVERNANCE_API_BASE_URL",
        "SEC_EDGAR_USER_AGENT",
    }
    for name in secret_names | variable_names:
        assert name in global_workflow
        assert name in README
        assert name in OPERATIONS
    for obsolete_name in (
        "EDINET_API_KEY",
        "COMPANIES_HOUSE_API_KEY",
        "EDINET_CONNECTOR_MODE",
        "COMPANIES_HOUSE_CONNECTOR_MODE",
        "COMPANIES_HOUSE_ISSUERS_JSON",
    ):
        assert obsolete_name not in global_workflow
    assert "SEC_API_KEY" not in global_workflow
    assert "SEC_EDGAR_API_KEY" not in global_workflow
    assert "SEC 공개 EDGAR 수집은 별도 API key를 사용하지 않는다" in OPERATIONS
    assert "검색 결과 HTML이나 Full-Text Search 화면을 파싱하지 않는다" in API_DOCS


def test_global_checkpoint_catch_up_and_company_limit_are_documented():
    for document in (README, OPERATIONS, API_DOCS):
        assert "최대 31일" in document
        assert "Companies House" in document
        assert "최대 50개" in document
    assert "`GET /ops/connectors/{connector_id}/checkpoint`" in API_DOCS
    assert "`partial_explicit_window`" in API_DOCS
    assert "수동 실행도 SEC source cursor 확인을 위해 checkpoint를 읽으며" in OPERATIONS
    assert "그 외 source는 지정 범위만 처리" in OPERATIONS
    assert "(connector_id, batch_id, chunk_index)" in OPERATIONS
    assert "`chunk_index`" in API_DOCS
    assert "배포 SHA별로 분리" in OPERATIONS
    assert "final 선행·순서 역전·metadata/합계 불일치는 HTTP 409" in OPERATIONS


def test_selected_market_variables_and_metadata_only_scope_are_documented():
    workflow = WORKFLOWS["ingest-selected-markets.yml"]
    for name in ("CA_OFFICIAL_LINKS_JSON", "AU_OFFICIAL_LINKS_JSON"):
        assert name in workflow
        assert name in README
        assert name in OPERATIONS
        assert name in API_DOCS
    assert "source_urls_requested=0" not in SELECTED_RUNNER
    assert '"source_urls_requested": 0' in SELECTED_RUNNER
    assert '"body_storage": False' in SELECTED_RUNNER
    assert "URL에 네트워크 요청을 보내거나 본문을 저장하지 않는다" in README
    assert "설정된 `original_url`을 요청하지 않고 본문도 저장하지 않는다" in OPERATIONS
    assert "최대 50개 issuer" in OPERATIONS
    for document in (README, OPERATIONS, API_DOCS):
        assert "link-only" in document
        assert "manual-metadata" in document
    assert "최상위 필드가 정확히 `schema_version`, `approved_hosts`, `records`" in OPERATIONS
    assert "`official_host`와 issuer 식별자는 승인 호스트 항목과 정확히 일치" in OPERATIONS
    assert "`asic.gov.au`와 그 하위 공식 호스트만 허용" in OPERATIONS
    assert "모든 URL query를 기본 거절" in OPERATIONS
    assert "JSON 배열" not in OPERATIONS[
        OPERATIONS.index("`CA_OFFICIAL_LINKS_JSON`과")::
    ].split("`global-brief.yml`", 1)[0]
    assert "`coverage_unavailable`" in README
    assert "`coverage_unavailable`" in OPERATIONS


def test_all_six_source_right_ids_match_migration_runner_and_docs():
    source_rights = {
        "official:dart",
        "official:sec-edgar",
        "official:edinet",
        "official:companies-house",
        "official:ca-issuer-ir",
        "official:asic-register",
    }
    for source_right_id in source_rights:
        assert source_right_id in MIGRATION
        assert source_right_id in README
        assert source_right_id in OPERATIONS
        assert source_right_id in API_DOCS
    assert '"CA": "official:ca-issuer-ir"' in SELECTED_RUNNER
    assert '"AU": "official:asic-register"' in SELECTED_RUNNER
    assert "Migration 011이 만드는 비한국 SourceRight는 모두 `pending`" in OPERATIONS


def test_admin_connector_activation_is_audited_and_fail_closed():
    for route in (
        "GET /admin/connectors",
        "GET /admin/connectors/{connector_id}",
        "POST /admin/connectors/{connector_id}",
    ):
        assert route in API_DOCS
    assert "collect_eligibility.eligible=true" in OPERATIONS
    assert "identity_match=true" in OPERATIONS
    assert "409 connector_source_right_ineligible" in OPERATIONS
    assert "409 stale_connector_update" in OPERATIONS
    assert "activist_global_connector_audit" in OPERATIONS
    assert "같은 상태 재요청도 포함해 모든 POST" in OPERATIONS


def test_cutover_revalidates_required_sources_and_optional_identities():
    for document in (OPERATIONS, API_DOCS):
        assert "`FOR UPDATE`" in document
        assert "`required_alpha_sources_invalid`" in document
        assert "필수 4개 connector" in document
        assert "JP·GB" in document
        assert "identity" in document
        assert "공개 문서가 0건" in document
        assert "승인" in document
        assert "소비" in document
    for capability in ("수집", "공개 재배포"):
        assert capability in OPERATIONS
        assert capability in API_DOCS
    assert "기존 v1·v2 공개 문서" in OPERATIONS
    assert "기존 v1·v2 공개 문서" in API_DOCS


def test_alpha_evidence_backfill_and_cutover_docs_match_keyless_scope():
    normalized_evidence = " ".join(ALPHA_EVIDENCE_DOCS.split())
    normalized_backfill = " ".join(GLOBAL_BACKFILL_DOCS.split())
    for document in (ALPHA_EVIDENCE_DOCS, CUTOVER_DOCS):
        assert "필수 4개" in document
        assert "JP·GB" in document
        assert "`coverage_unavailable`" in document
        assert "`public_ready=false`" in document
    assert "정확히 DART와 SEC EDGAR 두 항목만" in normalized_evidence
    assert "한국 DART와 미국 SEC EDGAR 두 소스뿐" in normalized_backfill
    assert "`dart/KR`, `sec-edgar/US` 두 항목" in GLOBAL_BACKFILL_DOCS
    for unsupported in (
        "`source`: `US|JP|GB|all`",
        "`EDINET_CONNECTOR_MODE=active`",
        "`COMPANIES_HOUSE_CONNECTOR_MODE=active`",
    ):
        assert unsupported not in GLOBAL_BACKFILL_DOCS
    assert "DART·SEC 정확히 2개 실제 connector" in CUTOVER_DOCS


def test_human_brief_and_permanently_disabled_telegram_are_explicit():
    brief = WORKFLOWS["global-brief.yml"]
    assert "pending explicit human approval" in brief
    assert "no public brief was written by this scheduled job" in brief
    assert "inputs.operation == 'publish'" in brief
    assert "사람 1명" in OPERATIONS
    for document in (README, OPERATIONS):
        assert "ENABLE_TELEGRAM_DELIVERY=false" in document
        assert "ENABLE_GOVERNANCE_DELIVERY=false" in document
        assert "Telegram" in document
    assert "사람 1명" in API_DOCS
    assert "ENABLE_TELEGRAM_DELIVERY=false" in API_DOCS
    assert "ENABLE_GOVERNANCE_DELIVERY=false" in API_DOCS
