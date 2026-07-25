-- BSIDE Global Market Terminal v2 schema.
--
-- Additive migration for six-jurisdiction issuer identity, source coverage,
-- frozen brief editions, document sections and idempotent global ingestion.
-- Apply after 010_official_slot_claim_ledger.sql and before deploying the PHP
-- API version that expects GOV_V2_SCHEMA_VERSION=11.
--
-- The caller must prepend this exact file, in the same MySQL input stream, with:
--   SET @bside_migration_011_sha256 = '<sha256-of-these-file-bytes>';
-- The migration rejects a missing/non-canonical digest and records that injected
-- byte identity instead of a mutable descriptor checksum.

DROP PROCEDURE IF EXISTS activist_011_preflight;
DELIMITER $$
CREATE PROCEDURE activist_011_preflight()
BEGIN
  DECLARE existing_v11_count INT DEFAULT 0;

  IF @bside_migration_011_sha256 IS NULL
     OR CAST(@bside_migration_011_sha256 AS BINARY)
        NOT REGEXP BINARY '^[0-9a-f]{64}$' THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '011 source byte checksum missing or invalid';
  END IF;

  SELECT COUNT(*) INTO existing_v11_count
  FROM activist_schema_migrations
  WHERE migration_version=11;

  IF (SELECT COUNT(*) FROM activist_schema_migrations
      WHERE migration_version BETWEEN 1 AND 10) <> 10
     OR EXISTS (
       SELECT 1 FROM activist_schema_migrations
       WHERE migration_version BETWEEN 1 AND 10 AND (
         BINARY migration_name <> BINARY CASE migration_version
           WHEN 1 THEN '001_governance_v1'
           WHEN 2 THEN '002_legacy_source_right_lineage'
           WHEN 3 THEN '003_editorial_governance'
           WHEN 4 THEN '004_telegram_signal_rebuild_staging'
           WHEN 5 THEN '005_telegram_channel_identity_index'
           WHEN 6 THEN '006_governance_release_guard'
           WHEN 7 THEN '007_governance_identity_and_evidence'
           WHEN 8 THEN '008_official_site_snapshot_receipts'
           WHEN 9 THEN '009_dart_global_quota_ledger'
           WHEN 10 THEN '010_official_slot_claim_ledger'
         END
         OR migration_checksum IS NULL
         OR BINARY migration_checksum <> BINARY CASE migration_version
           WHEN 1 THEN '2f1f03aa62d733339b79b5bca50e1c480b4f706a5823fd3490bd799421e93afd'
           WHEN 2 THEN 'fdcb2d634a787c7bbe534bd3892470a13aef11254dd75cec1afb54a9f2b61051'
           WHEN 3 THEN '906a0071bc11b595eae388a17074bd955f1ebb25f8a7453e3e89534e42ba4f25'
           WHEN 4 THEN 'de64071e117fae70d6849f8191be7267a885e75bf3d498ab7488fa616348fb7f'
           WHEN 5 THEN 'cf1245fe562e583707d821f126562a6f10aa9c8db5e0c9b20afa8ff267d1d903'
           WHEN 6 THEN 'f7f7a46f86118316dc21a67bb5b547668d64978b9fe4054b4c86104b85d7ced7'
           WHEN 7 THEN '074bbb5f066d5f3a20e3b894762ae356fa0a102c61546634fc16be05400f2ebe'
           WHEN 8 THEN 'b12e5e5290a5901192ddb4c8ec999719aa3dc25596c6c46d16ac383f3be74376'
           WHEN 9 THEN '9e60867847b7cc2b7d9166c73e395ae872d12a4e91aa62457049468017e5f94d'
           WHEN 10 THEN '2b8be6264c8a4f3be038729fbf6bbe22e720457874f02c89c82d33db9dc78f51'
         END
       )
     ) THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '011 prerequisite migration manifest incomplete';
  END IF;

  IF EXISTS (
    SELECT 1 FROM activist_schema_migrations
    WHERE migration_version=11
      AND (
        BINARY migration_name <> BINARY '011_global_terminal_v2'
        OR migration_checksum IS NULL
        OR BINARY migration_checksum <>
          BINARY @bside_migration_011_sha256
      )
  ) THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '011 migration manifest conflict';
  END IF;

  IF existing_v11_count=0
     AND EXISTS (
       SELECT 1 FROM activist_governance_release_state
       WHERE state_key='global_terminal_v2'
         AND (
           release_state<>'closed'
           OR state_version<>0
           OR updated_by<>'migration:011'
         )
     ) THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '011 initial global terminal state is not fail-closed';
  END IF;
END$$
DELIMITER ;
CALL activist_011_preflight();
DROP PROCEDURE activist_011_preflight;

DROP PROCEDURE IF EXISTS activist_011_add_column;
DROP PROCEDURE IF EXISTS activist_011_add_index;
DROP PROCEDURE IF EXISTS activist_011_modify_column;
DELIMITER $$
CREATE PROCEDURE activist_011_add_column(
  IN table_name_value VARCHAR(64),
  IN column_name_value VARCHAR(64),
  IN definition_value VARCHAR(500)
)
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = table_name_value
      AND COLUMN_NAME = column_name_value
  ) THEN
    SET @ddl = CONCAT(
      'ALTER TABLE `', table_name_value, '` ADD COLUMN `',
      column_name_value, '` ', definition_value
    );
    PREPARE statement_value FROM @ddl;
    EXECUTE statement_value;
    DEALLOCATE PREPARE statement_value;
  END IF;
END$$

CREATE PROCEDURE activist_011_add_index(
  IN table_name_value VARCHAR(64),
  IN index_name_value VARCHAR(64),
  IN definition_value VARCHAR(500)
)
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = table_name_value
      AND INDEX_NAME = index_name_value
  ) THEN
    SET @ddl = CONCAT(
      'ALTER TABLE `', table_name_value, '` ADD ', definition_value
    );
    PREPARE statement_value FROM @ddl;
    EXECUTE statement_value;
    DEALLOCATE PREPARE statement_value;
  END IF;
END$$

CREATE PROCEDURE activist_011_modify_column(
  IN table_name_value VARCHAR(64),
  IN column_name_value VARCHAR(64),
  IN definition_value VARCHAR(500)
)
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = table_name_value
      AND COLUMN_NAME = column_name_value
      AND (
        (
          LOWER(definition_value) LIKE 'char(8)%'
          AND LOWER(COLUMN_TYPE) <> 'char(8)'
        )
        OR (
          LOWER(definition_value) LIKE 'varchar(96)%'
          AND LOWER(COLUMN_TYPE) <> 'varchar(96)'
        )
        OR (
          LOWER(definition_value) NOT LIKE '%not null%'
          AND IS_NULLABLE <> 'YES'
        )
        OR (
          LOWER(definition_value) LIKE '%not null%'
          AND IS_NULLABLE <> 'NO'
        )
      )
  ) THEN
    SET @ddl = CONCAT(
      'ALTER TABLE `', table_name_value, '` MODIFY COLUMN `',
      column_name_value, '` ', definition_value
    );
    PREPARE statement_value FROM @ddl;
    EXECUTE statement_value;
    DEALLOCATE PREPARE statement_value;
  END IF;
END$$
DELIMITER ;

CREATE TABLE IF NOT EXISTS activist_jurisdictions (
  country_code CHAR(2) NOT NULL PRIMARY KEY,
  display_name VARCHAR(80) NOT NULL,
  display_name_en VARCHAR(80) NOT NULL,
  default_market VARCHAR(40) NULL,
  timezone_name VARCHAR(64) NOT NULL,
  launch_order TINYINT UNSIGNED NOT NULL,
  record_status VARCHAR(24) NOT NULL DEFAULT 'active',
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_jurisdiction_launch_order (launch_order),
  KEY idx_jurisdiction_status (record_status, launch_order)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_issuers (
  issuer_id VARCHAR(96) NOT NULL PRIMARY KEY,
  country_code CHAR(2) NOT NULL,
  legal_name VARCHAR(255) NOT NULL,
  legal_name_en VARCHAR(255) NULL,
  short_name VARCHAR(255) NULL,
  original_language VARCHAR(16) NOT NULL,
  homepage_url TEXT NULL,
  listing_status VARCHAR(24) NOT NULL DEFAULT 'unknown',
  record_status VARCHAR(24) NOT NULL DEFAULT 'active',
  master_modified_at DATETIME NULL,
  payload_json MEDIUMTEXT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_issuer_country_name (country_code, legal_name),
  KEY idx_issuer_country_status (country_code, listing_status, record_status),
  KEY idx_issuer_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_issuer_identifiers (
  issuer_id VARCHAR(96) NOT NULL,
  identifier_type VARCHAR(40) NOT NULL,
  identifier_value VARCHAR(191) NOT NULL,
  market VARCHAR(40) NOT NULL DEFAULT '',
  is_primary TINYINT(1) NOT NULL DEFAULT 0,
  valid_from DATE NULL,
  valid_until DATE NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (issuer_id, identifier_type, identifier_value, market),
  UNIQUE KEY uq_issuer_identifier_global (identifier_type, identifier_value, market),
  KEY idx_issuer_identifier_lookup (issuer_id, is_primary, identifier_type),
  KEY idx_issuer_identifier_validity (valid_from, valid_until)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_issuer_listings (
  listing_id VARCHAR(96) NOT NULL PRIMARY KEY,
  issuer_id VARCHAR(96) NOT NULL,
  country_code CHAR(2) NOT NULL,
  market VARCHAR(40) NOT NULL,
  ticker VARCHAR(24) NULL,
  isin VARCHAR(24) NULL,
  currency_code CHAR(3) NULL,
  listing_status VARCHAR(24) NOT NULL DEFAULT 'unknown',
  is_primary TINYINT(1) NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_issuer_listing_market_ticker (country_code, market, ticker),
  KEY idx_issuer_listing_issuer (issuer_id, is_primary),
  KEY idx_issuer_listing_status (country_code, market, listing_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_source_connectors (
  connector_id VARCHAR(96) NOT NULL PRIMARY KEY,
  country_code CHAR(2) NOT NULL,
  source_key VARCHAR(191) NOT NULL,
  source_name VARCHAR(255) NOT NULL,
  source_type VARCHAR(40) NOT NULL,
  base_url TEXT NOT NULL,
  source_right_id VARCHAR(64) NULL,
  coverage_mode VARCHAR(24) NOT NULL,
  connector_status VARCHAR(24) NOT NULL DEFAULT 'inactive',
  schedule_minutes SMALLINT UNSIGNED NULL,
  cursor_json MEDIUMTEXT NULL,
  last_checked_at DATETIME NULL,
  last_success_at DATETIME NULL,
  last_observed_at DATETIME NULL,
  last_raw_count INT UNSIGNED NOT NULL DEFAULT 0,
  last_acknowledged_count INT UNSIGNED NOT NULL DEFAULT 0,
  last_error_class VARCHAR(80) NULL,
  code_revision VARCHAR(64) NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_source_connector_key (country_code, source_key),
  KEY idx_source_connector_status (connector_status, country_code),
  KEY idx_source_connector_freshness (last_success_at, connector_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_global_connector_audit (
  audit_id VARCHAR(96) NOT NULL PRIMARY KEY,
  connector_id VARCHAR(96) NOT NULL,
  previous_status VARCHAR(24) NOT NULL,
  new_status VARCHAR(24) NOT NULL,
  reason VARCHAR(1000) NOT NULL,
  changed_by VARCHAR(191) NOT NULL,
  created_at DATETIME NOT NULL,
  KEY idx_global_connector_audit_connector (connector_id, created_at),
  CONSTRAINT fk_global_connector_audit_connector
    FOREIGN KEY (connector_id)
    REFERENCES activist_source_connectors (connector_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_release_authorizations (
  authorization_id VARCHAR(64) NOT NULL PRIMARY KEY,
  candidate_sha CHAR(40) NOT NULL,
  evidence_artifact_digest CHAR(71) NOT NULL,
  evidence_run_id BIGINT UNSIGNED NOT NULL,
  evidence_artifact_id BIGINT UNSIGNED NOT NULL,
  nonce_sha256 CHAR(64) NOT NULL,
  expected_v1_state_version BIGINT UNSIGNED NOT NULL,
  expected_v2_state_version BIGINT UNSIGNED NOT NULL,
  expires_at DATETIME NOT NULL,
  v1_consumed_at DATETIME NULL,
  v1_consumed_state_version BIGINT UNSIGNED NULL,
  v2_consumed_at DATETIME NULL,
  v2_consumed_state_version BIGINT UNSIGNED NULL,
  fully_consumed_at DATETIME NULL,
  revoked_at DATETIME NULL,
  revoke_reason VARCHAR(255) NULL,
  created_by VARCHAR(191) NOT NULL,
  create_reason VARCHAR(1000) NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_release_authorization_nonce (nonce_sha256),
  KEY idx_release_authorization_candidate (
    candidate_sha, evidence_artifact_digest
  ),
  KEY idx_release_authorization_expiry (
    expires_at, revoked_at, fully_consumed_at
  )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_source_coverage (
  coverage_id VARCHAR(96) NOT NULL PRIMARY KEY,
  connector_id VARCHAR(96) NOT NULL,
  country_code CHAR(2) NOT NULL,
  market VARCHAR(40) NOT NULL DEFAULT '',
  event_family VARCHAR(64) NOT NULL DEFAULT 'all',
  coverage_mode VARCHAR(24) NOT NULL,
  issuer_scope_json MEDIUMTEXT NULL,
  public_note VARCHAR(500) NOT NULL,
  effective_from DATETIME NOT NULL,
  effective_until DATETIME NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_source_coverage_scope (
    connector_id, country_code, market, event_family, effective_from
  ),
  KEY idx_source_coverage_public (
    country_code, coverage_mode, event_family, effective_until
  )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_brief_editions (
  brief_id VARCHAR(96) NOT NULL PRIMARY KEY,
  edition VARCHAR(16) NOT NULL,
  cutoff_at DATETIME NOT NULL,
  published_at DATETIME NULL,
  publication_status VARCHAR(24) NOT NULL DEFAULT 'draft',
  approved_by VARCHAR(191) NULL,
  approved_at DATETIME NULL,
  build_sha VARCHAR(64) NOT NULL,
  payload_json MEDIUMTEXT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_brief_edition_cutoff (edition, cutoff_at),
  KEY idx_brief_publication (edition, publication_status, published_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_brief_items (
  brief_id VARCHAR(96) NOT NULL,
  event_id VARCHAR(96) NOT NULL,
  lane VARCHAR(16) NOT NULL,
  position_no TINYINT UNSIGNED NOT NULL,
  event_updated_at DATETIME NOT NULL,
  event_snapshot_json MEDIUMTEXT NOT NULL,
  selection_reason VARCHAR(500) NOT NULL,
  review_status VARCHAR(24) NOT NULL DEFAULT 'pending',
  approved_by VARCHAR(191) NULL,
  approved_at DATETIME NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (brief_id, lane, position_no),
  UNIQUE KEY uq_brief_item_event (brief_id, event_id),
  KEY idx_brief_item_event (event_id, brief_id),
  KEY idx_brief_item_review (review_status, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_document_sections (
  section_id VARCHAR(96) NOT NULL PRIMARY KEY,
  document_id VARCHAR(96) NOT NULL,
  section_key VARCHAR(191) NOT NULL,
  position_no INT UNSIGNED NOT NULL,
  heading VARCHAR(700) NULL,
  body_text MEDIUMTEXT NULL,
  evidence_locator VARCHAR(500) NULL,
  original_language VARCHAR(16) NOT NULL,
  content_hash CHAR(64) NOT NULL,
  publication_status VARCHAR(24) NOT NULL DEFAULT 'draft',
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_document_section_key (document_id, section_key),
  UNIQUE KEY uq_document_section_position (document_id, position_no),
  KEY idx_document_section_public (publication_status, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_global_lifecycle_observations (
  observation_id VARCHAR(96) NOT NULL PRIMARY KEY,
  connector_id VARCHAR(96) NOT NULL,
  country_code CHAR(2) NOT NULL,
  source_key VARCHAR(191) NOT NULL,
  external_id VARCHAR(191) NOT NULL,
  parent_external_id VARCHAR(191) NULL,
  change_type VARCHAR(24) NOT NULL,
  observed_at DATETIME NOT NULL,
  payload_json MEDIUMTEXT NULL,
  resolution_status VARCHAR(24) NOT NULL DEFAULT 'pending',
  resolved_document_id VARCHAR(96) NULL,
  resolved_event_id VARCHAR(96) NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_global_lifecycle_source (
    connector_id, external_id, change_type, observed_at
  ),
  KEY idx_global_lifecycle_pending (
    resolution_status, connector_id, observed_at
  ),
  KEY idx_global_lifecycle_parent (connector_id, parent_external_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_global_ingest_receipts (
  ingest_id VARCHAR(96) NOT NULL PRIMARY KEY,
  connector_id VARCHAR(96) NOT NULL,
  idempotency_key VARCHAR(191) NOT NULL,
  payload_sha256 CHAR(64) NOT NULL,
  batch_id VARCHAR(77) NOT NULL,
  chunk_index SMALLINT UNSIGNED NOT NULL,
  chunk_count SMALLINT UNSIGNED NOT NULL,
  window_start DATE NOT NULL,
  window_end_exclusive DATE NOT NULL,
  request_count INT UNSIGNED NOT NULL,
  raw_count INT UNSIGNED NOT NULL,
  acknowledged_count INT UNSIGNED NOT NULL,
  batch_raw_count INT UNSIGNED NOT NULL,
  batch_acknowledged_count INT UNSIGNED NOT NULL,
  batch_request_count INT UNSIGNED NOT NULL,
  code_revision VARCHAR(64) NOT NULL,
  started_at DATETIME NOT NULL,
  completed_at DATETIME NOT NULL,
  created_at DATETIME NOT NULL,
  UNIQUE KEY uq_global_ingest_connector_key (connector_id, idempotency_key),
  UNIQUE KEY uq_global_ingest_batch_chunk (
    connector_id, batch_id, chunk_index
  ),
  KEY idx_global_ingest_completed (connector_id, completed_at),
  KEY idx_global_ingest_batch (connector_id, batch_id),
  KEY idx_global_ingest_window (
    connector_id, window_end_exclusive, completed_at
  ),
  KEY idx_global_ingest_revision (code_revision, completed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CALL activist_011_modify_column(
  'activist_governance_events',
  'company_id',
  'CHAR(8) NULL'
);
CALL activist_011_add_column(
  'activist_documents',
  'issuer_id',
  'VARCHAR(96) NULL AFTER company_id'
);
CALL activist_011_add_column(
  'activist_documents',
  'country_code',
  'CHAR(2) NULL AFTER issuer_id'
);
CALL activist_011_add_column(
  'activist_documents',
  'source_key',
  'VARCHAR(191) NULL AFTER source_class'
);
CALL activist_011_add_column(
  'activist_documents',
  'filed_at',
  'DATETIME NULL AFTER published_at'
);
CALL activist_011_add_column(
  'activist_governance_events',
  'issuer_id',
  'VARCHAR(96) NULL AFTER company_id'
);
CALL activist_011_add_column(
  'activist_governance_events',
  'country_code',
  'CHAR(2) NULL AFTER issuer_id'
);
CALL activist_011_add_column(
  'activist_governance_events',
  'global_event_family',
  'VARCHAR(64) NULL AFTER country_code'
);
CALL activist_011_add_column(
  'activist_governance_events',
  'change_type',
  'VARCHAR(24) NOT NULL DEFAULT ''new'' AFTER verification_status'
);
CALL activist_011_add_column(
  'activist_governance_events',
  'current_status',
  'VARCHAR(64) NULL AFTER change_type'
);
CALL activist_011_add_column(
  'activist_governance_events',
  'first_observed_at',
  'DATETIME NULL AFTER current_status'
);
CALL activist_011_add_column(
  'activist_governance_release_audit',
  'release_authorization_id',
  'VARCHAR(64) NULL AFTER request_id'
);
CALL activist_011_modify_column(
  'activist_governance_events',
  'identity_actor_id',
  'VARCHAR(96) NULL'
);

CALL activist_011_add_index(
  'activist_documents',
  'idx_document_issuer_published',
  'KEY `idx_document_issuer_published` (`issuer_id`,`published_at`)'
);
CALL activist_011_add_index(
  'activist_documents',
  'idx_document_country_source',
  'KEY `idx_document_country_source` (`country_code`,`source_key`,`filed_at`)'
);
CALL activist_011_add_index(
  'activist_governance_events',
  'idx_event_issuer_occurred',
  'KEY `idx_event_issuer_occurred` (`issuer_id`,`occurred_at`)'
);
CALL activist_011_add_index(
  'activist_governance_events',
  'idx_event_country_change',
  'KEY `idx_event_country_change` (`country_code`,`change_type`,`updated_at`)'
);
CALL activist_011_add_index(
  'activist_governance_events',
  'idx_event_country_family',
  'KEY `idx_event_country_family` (`country_code`,`global_event_family`,`occurred_at`)'
);
CALL activist_011_add_index(
  'activist_brief_items',
  'uq_brief_item_event',
  'UNIQUE KEY `uq_brief_item_event` (`brief_id`,`event_id`)'
);
CALL activist_011_add_index(
  'activist_governance_release_audit',
  'idx_release_audit_authorization',
  'KEY `idx_release_audit_authorization` (`release_authorization_id`,`state_key`)'
);

INSERT INTO activist_jurisdictions (
  country_code, display_name, display_name_en, default_market,
  timezone_name, launch_order, record_status, created_at, updated_at
) VALUES
  ('KR', '한국', 'South Korea', 'KRX', 'Asia/Seoul', 1, 'active', UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('US', '미국', 'United States', 'US', 'America/New_York', 2, 'active', UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('JP', '일본', 'Japan', 'JPX', 'Asia/Tokyo', 3, 'active', UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('GB', '영국', 'United Kingdom', 'LSE', 'Europe/London', 4, 'active', UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('CA', '캐나다', 'Canada', 'TSX', 'America/Toronto', 5, 'active', UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('AU', '호주', 'Australia', 'ASX', 'Australia/Sydney', 6, 'active', UTC_TIMESTAMP(), UTC_TIMESTAMP())
ON DUPLICATE KEY UPDATE country_code=country_code;

INSERT INTO activist_issuers (
  issuer_id, country_code, legal_name, legal_name_en, short_name,
  original_language, homepage_url, listing_status, record_status,
  master_modified_at, payload_json, created_at, updated_at
)
SELECT
  CONCAT('issuer:kr:dart:', company_id),
  'KR',
  legal_name,
  legal_name_en,
  short_name,
  'ko',
  homepage_url,
  listing_status,
  record_status,
  master_modified_at,
  JSON_OBJECT('legacy_company_id', company_id),
  created_at,
  updated_at
FROM activist_companies
ON DUPLICATE KEY UPDATE
  legal_name=VALUES(legal_name),
  legal_name_en=VALUES(legal_name_en),
  short_name=VALUES(short_name),
  homepage_url=VALUES(homepage_url),
  listing_status=VALUES(listing_status),
  record_status=VALUES(record_status),
  master_modified_at=VALUES(master_modified_at),
  updated_at=VALUES(updated_at);

INSERT INTO activist_issuer_identifiers (
  issuer_id, identifier_type, identifier_value, market, is_primary,
  valid_from, valid_until, created_at, updated_at
)
SELECT
  CONCAT('issuer:kr:dart:', company_id),
  'DART_CORP_CODE',
  company_id,
  'KRX',
  1,
  NULL,
  NULL,
  created_at,
  updated_at
FROM activist_companies
ON DUPLICATE KEY UPDATE
  identifier_value=identifier_value;

INSERT INTO activist_issuer_identifiers (
  issuer_id, identifier_type, identifier_value, market, is_primary,
  valid_from, valid_until, created_at, updated_at
)
SELECT
  CONCAT('issuer:kr:dart:', company_id),
  'TICKER',
  stock_code,
  COALESCE(NULLIF(market,''), 'KRX'),
  0,
  NULL,
  NULL,
  created_at,
  updated_at
FROM activist_companies
WHERE stock_code IS NOT NULL AND stock_code <> ''
ON DUPLICATE KEY UPDATE
  identifier_value=identifier_value;

INSERT INTO activist_issuer_listings (
  listing_id, issuer_id, country_code, market, ticker, isin,
  currency_code, listing_status, is_primary, created_at, updated_at
)
SELECT
  CONCAT('listing:kr:', company_id),
  CONCAT('issuer:kr:dart:', company_id),
  'KR',
  COALESCE(NULLIF(market,''), 'KRX'),
  stock_code,
  NULL,
  'KRW',
  listing_status,
  1,
  created_at,
  updated_at
FROM activist_companies
WHERE stock_code IS NOT NULL AND stock_code <> ''
ON DUPLICATE KEY UPDATE
  listing_id=listing_id;

UPDATE activist_documents d
LEFT JOIN activist_source_rights sr
  ON sr.source_right_id=d.source_right_id
SET d.issuer_id=CONCAT('issuer:kr:dart:', d.company_id),
    d.country_code='KR',
    d.source_key=COALESCE(NULLIF(d.source_key,''), NULLIF(sr.source_key,''))
WHERE d.company_id IS NOT NULL
  AND (d.issuer_id IS NULL OR d.country_code IS NULL OR d.source_key IS NULL);

UPDATE activist_governance_events
SET issuer_id=CONCAT('issuer:kr:dart:', company_id),
    country_code='KR',
    global_event_family=COALESCE(
      global_event_family,
      CASE event_type
        WHEN 'five_percent_holding' THEN 'large_ownership'
        WHEN 'shareholder_proposal' THEN 'meeting_and_vote'
        WHEN 'general_meeting' THEN 'meeting_and_vote'
        WHEN 'tender_offer' THEN 'tender_offer_and_mna'
        WHEN 'merger' THEN 'tender_offer_and_mna'
        WHEN 'split' THEN 'tender_offer_and_mna'
        WHEN 'rights_issue' THEN 'capital_issuance'
        WHEN 'convertible_bond' THEN 'capital_issuance'
        WHEN 'bond_with_warrant' THEN 'capital_issuance'
        WHEN 'exchangeable_bond' THEN 'capital_issuance'
        WHEN 'dividend' THEN 'capital_return'
        WHEN 'treasury_shares' THEN 'capital_return'
        WHEN 'board' THEN 'board_and_compensation'
        WHEN 'executive_compensation' THEN 'board_and_compensation'
        WHEN 'trading_suspension' THEN 'listing_status'
        WHEN 'delisting' THEN 'listing_status'
        WHEN 'duplicate_listing' THEN 'listing_status'
        WHEN 'value_up' THEN 'capital_return'
        ELSE NULL
      END
    ),
    first_observed_at=COALESCE(first_observed_at,created_at)
WHERE company_id IS NOT NULL
  AND (
    issuer_id IS NULL
    OR country_code IS NULL
    OR global_event_family IS NULL
    OR first_observed_at IS NULL
  );

INSERT INTO activist_source_rights (
  source_right_id, source_type, source_key, source_name,
  permission_scope, evidence_uri, evidence_hash, valid_from, valid_until,
  revoked_at, ai_allowed, redistribution_allowed, status, notes,
  created_at, updated_at
) VALUES
  (
    'official:sec-edgar', 'official_disclosure', 'sec-edgar', 'SEC EDGAR',
    'Public filing metadata and source links',
    'https://www.sec.gov/search-filings/edgar-application-programming-interfaces',
    NULL, '2009-01-01 00:00:00', NULL, NULL, 0, 0, 'pending',
    'Requires explicit SourceRight approval before collection or publication.',
    UTC_TIMESTAMP(), UTC_TIMESTAMP()
  ),
  (
    'official:edinet', 'official_disclosure', 'edinet', 'EDINET',
    'Public statutory filing metadata and source links',
    'https://disclosure2.edinet-fsa.go.jp/guide/static/disclosure/WZEK0090.html',
    NULL, '2013-09-17 00:00:00', NULL, NULL, 0, 0, 'pending',
    'Requires explicit approval; TDnet content is not included.',
    UTC_TIMESTAMP(), UTC_TIMESTAMP()
  ),
  (
    'official:companies-house', 'official_register', 'companies-house', 'Companies House',
    'Public company register metadata and source links',
    'https://developer-specs.company-information.service.gov.uk/',
    NULL, '2015-06-01 00:00:00', NULL, NULL, 0, 0, 'pending',
    'Requires explicit approval; RNS content is not included.',
    UTC_TIMESTAMP(), UTC_TIMESTAMP()
  ),
  (
    'official:ca-issuer-ir', 'official_issuer', 'issuer-ir', 'Canadian issuer IR manual links',
    'Manually approved issuer-controlled IR link metadata; maximum 50 issuer-host mappings',
    NULL,
    NULL, '2015-01-01 00:00:00', NULL, NULL, 0, 0, 'pending',
    'Requires issuer-bound hostname evidence; SEDAR+ and third-party links are excluded.',
    UTC_TIMESTAMP(), UTC_TIMESTAMP()
  ),
  (
    'official:asic-register', 'official_register', 'asic-register', 'ASIC manual register links',
    'Manual link metadata restricted to official asic.gov.au hosts; maximum 50 issuer-host mappings',
    'https://www.asic.gov.au/online-services/search-asic-registers/data-gov-au/',
    NULL, '2015-01-01 00:00:00', NULL, NULL, 0, 0, 'pending',
    'Requires explicit approval; ASX, data.gov.au and third-party links are excluded.',
    UTC_TIMESTAMP(), UTC_TIMESTAMP()
  )
ON DUPLICATE KEY UPDATE source_right_id=source_right_id;

INSERT INTO activist_source_connectors (
  connector_id, country_code, source_key, source_name, source_type,
  base_url, source_right_id, coverage_mode, connector_status,
  schedule_minutes, cursor_json, last_checked_at, last_success_at,
  last_observed_at, last_raw_count, last_acknowledged_count,
  last_error_class, code_revision, created_at, updated_at
) VALUES
  ('connector:kr:dart', 'KR', 'dart', 'OpenDART', 'official_disclosure',
   'https://opendart.fss.or.kr', 'official:dart', 'market-wide', 'active',
   15, NULL, NULL, NULL, NULL, 0, 0, NULL, NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('connector:us:sec-edgar', 'US', 'sec-edgar', 'SEC EDGAR current filings + daily index', 'official_disclosure',
   'https://www.sec.gov', 'official:sec-edgar', 'market-wide', 'pending_rights',
   30, NULL, NULL, NULL, NULL, 0, 0, 'source_right_required', NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('connector:jp:edinet', 'JP', 'edinet', 'EDINET', 'official_disclosure',
   'https://api.edinet-fsa.go.jp', 'official:edinet', 'market-wide', 'pending_rights',
   15, NULL, NULL, NULL, NULL, 0, 0, 'source_right_required', NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('connector:gb:companies-house', 'GB', 'companies-house', 'Companies House', 'official_register',
   'https://api.company-information.service.gov.uk', 'official:companies-house',
   'official-register', 'pending_rights', 30, NULL, NULL, NULL, NULL, 0, 0, 'source_right_required', NULL,
   UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('connector:ca:issuer-ir', 'CA', 'issuer-ir', 'Canadian issuer IR manual links', 'official_issuer',
   'https://www.canada.ca', 'official:ca-issuer-ir', 'link-only', 'pending_rights',
   30, NULL, NULL, NULL, NULL, 0, 0, 'source_right_required', NULL,
   UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('connector:au:asic-register', 'AU', 'asic-register', 'ASIC manual register links', 'official_register',
   'https://www.asic.gov.au', 'official:asic-register', 'link-only', 'pending_rights',
   30, NULL, NULL, NULL, NULL, 0, 0, 'source_right_required', NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP())
ON DUPLICATE KEY UPDATE connector_id=connector_id;

INSERT INTO activist_source_coverage (
  coverage_id, connector_id, country_code, market, event_family,
  coverage_mode, issuer_scope_json, public_note, effective_from,
  effective_until, created_at, updated_at
) VALUES
  ('coverage:kr:dart:ownership', 'connector:kr:dart', 'KR', 'KRX', 'large_ownership',
   'market-wide', NULL, 'OpenDART governance filing allowlist', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:kr:dart:meeting', 'connector:kr:dart', 'KR', 'KRX', 'meeting_and_vote',
   'market-wide', NULL, 'OpenDART governance filing allowlist', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:kr:dart:mna', 'connector:kr:dart', 'KR', 'KRX', 'tender_offer_and_mna',
   'market-wide', NULL, 'OpenDART governance filing allowlist', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:kr:dart:issuance', 'connector:kr:dart', 'KR', 'KRX', 'capital_issuance',
   'market-wide', NULL, 'OpenDART governance filing allowlist', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:kr:dart:return', 'connector:kr:dart', 'KR', 'KRX', 'capital_return',
   'market-wide', NULL, 'OpenDART governance filing allowlist', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:kr:dart:board', 'connector:kr:dart', 'KR', 'KRX', 'board_and_compensation',
   'market-wide', NULL, 'OpenDART governance filing allowlist', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:kr:dart:listing', 'connector:kr:dart', 'KR', 'KRX', 'listing_status',
   'market-wide', NULL, 'OpenDART governance filing allowlist', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:kr:dart:revision', 'connector:kr:dart', 'KR', 'KRX', 'correction_and_withdrawal',
   'market-wide', NULL, 'OpenDART correction and withdrawal lifecycle', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:us:sec:ownership', 'connector:us:sec-edgar', 'US', 'US', 'large_ownership',
   'market-wide', NULL, 'SEC Latest Filings Atom intraday discovery plus completed-day index reconciliation; allowlisted governance forms only', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:us:sec:meeting', 'connector:us:sec-edgar', 'US', 'US', 'meeting_and_vote',
   'market-wide', NULL, 'SEC Latest Filings Atom intraday discovery plus completed-day index reconciliation; allowlisted governance forms only', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:us:sec:mna', 'connector:us:sec-edgar', 'US', 'US', 'tender_offer_and_mna',
   'market-wide', NULL, 'SEC Latest Filings Atom intraday discovery plus completed-day index reconciliation; allowlisted governance forms only', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:jp:edinet:ownership', 'connector:jp:edinet', 'JP', 'JPX', 'large_ownership',
   'market-wide', NULL, 'EDINET document-type allowlist; TDnet excluded', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:jp:edinet:meeting', 'connector:jp:edinet', 'JP', 'JPX', 'meeting_and_vote',
   'market-wide', NULL, 'EDINET document-type allowlist; TDnet excluded', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:jp:edinet:mna', 'connector:jp:edinet', 'JP', 'JPX', 'tender_offer_and_mna',
   'market-wide', NULL, 'EDINET document-type allowlist; TDnet excluded', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:jp:edinet:return', 'connector:jp:edinet', 'JP', 'JPX', 'capital_return',
   'market-wide', NULL, 'EDINET document-type allowlist; TDnet excluded', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:jp:edinet:revision', 'connector:jp:edinet', 'JP', 'JPX', 'correction_and_withdrawal',
   'market-wide', NULL, 'EDINET correction and withdrawal lifecycle', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:gb:ch:meeting', 'connector:gb:companies-house', 'GB', '', 'meeting_and_vote',
   'official-register', JSON_OBJECT('selection','configured company numbers'),
   'Companies House configured company-number scope; RNS excluded', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:gb:ch:issuance', 'connector:gb:companies-house', 'GB', '', 'capital_issuance',
   'official-register', JSON_OBJECT('selection','configured company numbers'),
   'Companies House configured company-number scope; RNS excluded', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:gb:ch:board', 'connector:gb:companies-house', 'GB', '', 'board_and_compensation',
   'official-register', JSON_OBJECT('selection','configured company numbers'),
   'Companies House configured company-number scope; RNS excluded', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:gb:ch:listing', 'connector:gb:companies-house', 'GB', '', 'listing_status',
   'official-register', JSON_OBJECT('selection','configured company numbers'),
   'Companies House configured company-number scope; RNS excluded', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:ca:ir:meeting', 'connector:ca:issuer-ir', 'CA', '', 'meeting_and_vote',
   'link-only', JSON_OBJECT('selection','manual issuer-host evidence mapping'),
   'Manual issuer-controlled IR link metadata only; SEDAR+ excluded', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:ca:ir:mna', 'connector:ca:issuer-ir', 'CA', '', 'tender_offer_and_mna',
   'link-only', JSON_OBJECT('selection','manual issuer-host evidence mapping'),
   'Manual issuer-controlled IR link metadata only; SEDAR+ excluded', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:ca:ir:return', 'connector:ca:issuer-ir', 'CA', '', 'capital_return',
   'link-only', JSON_OBJECT('selection','manual issuer-host evidence mapping'),
   'Manual issuer-controlled IR link metadata only; SEDAR+ excluded', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:ca:ir:board', 'connector:ca:issuer-ir', 'CA', '', 'board_and_compensation',
   'link-only', JSON_OBJECT('selection','manual issuer-host evidence mapping'),
   'Manual issuer-controlled IR link metadata only; SEDAR+ excluded', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:au:asic:board', 'connector:au:asic-register', 'AU', '', 'board_and_compensation',
   'link-only', JSON_OBJECT('selection','manual ASIC official-host metadata'),
   'Manual asic.gov.au link metadata only; ASX excluded', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP()),
  ('coverage:au:asic:listing', 'connector:au:asic-register', 'AU', '', 'listing_status',
   'link-only', JSON_OBJECT('selection','manual ASIC official-host metadata'),
   'Manual asic.gov.au link metadata only; ASX excluded', UTC_TIMESTAMP(), NULL, UTC_TIMESTAMP(), UTC_TIMESTAMP())
ON DUPLICATE KEY UPDATE coverage_id=coverage_id;

INSERT INTO activist_governance_release_state (
  state_key, release_state, state_version, updated_by, update_reason,
  cutover_at, sunset_at, updated_at
) VALUES (
  'global_terminal_v2', 'closed', 0, 'migration:011',
  'Initial fail-closed global terminal state', NULL, NULL, UTC_TIMESTAMP()
) ON DUPLICATE KEY UPDATE state_key=state_key;

INSERT IGNORE INTO activist_governance_release_audit (
  audit_id, state_key, state_version, previous_state, new_state,
  changed_by, change_reason, request_id, cutover_at, sunset_at, created_at
) VALUES (
  'release:global-terminal:initial:011', 'global_terminal_v2', 0, NULL, 'closed',
  'migration:011', 'Initial fail-closed global terminal state', NULL,
  NULL, NULL, UTC_TIMESTAMP()
);

DROP PROCEDURE activist_011_add_column;
DROP PROCEDURE activist_011_add_index;
DROP PROCEDURE activist_011_modify_column;

DROP PROCEDURE IF EXISTS activist_011_record_migration;
DELIMITER $$
CREATE PROCEDURE activist_011_record_migration()
BEGIN
  DECLARE existing_count INT DEFAULT 0;
  DECLARE existing_name VARCHAR(191) DEFAULT NULL;
  DECLARE existing_checksum CHAR(64) DEFAULT NULL;

  IF (SELECT COUNT(*) FROM activist_schema_migrations
      WHERE migration_version BETWEEN 1 AND 10) <> 10
     OR EXISTS (
       SELECT 1 FROM activist_schema_migrations
       WHERE migration_version BETWEEN 1 AND 10 AND (
         migration_name <> CASE migration_version
           WHEN 1 THEN '001_governance_v1'
           WHEN 2 THEN '002_legacy_source_right_lineage'
           WHEN 3 THEN '003_editorial_governance'
           WHEN 4 THEN '004_telegram_signal_rebuild_staging'
           WHEN 5 THEN '005_telegram_channel_identity_index'
           WHEN 6 THEN '006_governance_release_guard'
           WHEN 7 THEN '007_governance_identity_and_evidence'
           WHEN 8 THEN '008_official_site_snapshot_receipts'
           WHEN 9 THEN '009_dart_global_quota_ledger'
           WHEN 10 THEN '010_official_slot_claim_ledger'
         END
         OR migration_checksum IS NULL
         OR migration_checksum <> CASE migration_version
           WHEN 1 THEN '2f1f03aa62d733339b79b5bca50e1c480b4f706a5823fd3490bd799421e93afd'
           WHEN 2 THEN 'fdcb2d634a787c7bbe534bd3892470a13aef11254dd75cec1afb54a9f2b61051'
           WHEN 3 THEN '906a0071bc11b595eae388a17074bd955f1ebb25f8a7453e3e89534e42ba4f25'
           WHEN 4 THEN 'de64071e117fae70d6849f8191be7267a885e75bf3d498ab7488fa616348fb7f'
           WHEN 5 THEN 'cf1245fe562e583707d821f126562a6f10aa9c8db5e0c9b20afa8ff267d1d903'
           WHEN 6 THEN 'f7f7a46f86118316dc21a67bb5b547668d64978b9fe4054b4c86104b85d7ced7'
           WHEN 7 THEN '074bbb5f066d5f3a20e3b894762ae356fa0a102c61546634fc16be05400f2ebe'
           WHEN 8 THEN 'b12e5e5290a5901192ddb4c8ec999719aa3dc25596c6c46d16ac383f3be74376'
           WHEN 9 THEN '9e60867847b7cc2b7d9166c73e395ae872d12a4e91aa62457049468017e5f94d'
           WHEN 10 THEN '2b8be6264c8a4f3be038729fbf6bbe22e720457874f02c89c82d33db9dc78f51'
         END
       )
     ) THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '011 prerequisite migration manifest incomplete';
  END IF;

  IF (SELECT COUNT(*) FROM information_schema.TABLES
      WHERE TABLE_SCHEMA=DATABASE()
        AND TABLE_NAME IN (
          'activist_jurisdictions',
          'activist_issuers',
          'activist_issuer_identifiers',
          'activist_issuer_listings',
          'activist_source_connectors',
          'activist_global_connector_audit',
          'activist_release_authorizations',
          'activist_source_coverage',
          'activist_brief_editions',
          'activist_brief_items',
          'activist_document_sections',
          'activist_global_lifecycle_observations',
          'activist_global_ingest_receipts'
        )
        AND ENGINE='InnoDB') <> 13 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '011 global terminal table shape incomplete';
  END IF;

  IF (SELECT COUNT(*) FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA=DATABASE()
        AND (
          (TABLE_NAME='activist_documents'
            AND COLUMN_NAME IN ('issuer_id','country_code','source_key','filed_at'))
          OR
          (TABLE_NAME='activist_governance_events'
            AND COLUMN_NAME IN (
              'issuer_id','country_code','global_event_family','change_type',
              'current_status','first_observed_at'
            ))
        )) <> 10 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '011 global event column shape incomplete';
  END IF;

  IF (SELECT COUNT(*) FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA=DATABASE()
        AND TABLE_NAME='activist_global_ingest_receipts') <> 19
     OR (SELECT COUNT(*) FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA=DATABASE()
           AND TABLE_NAME='activist_global_ingest_receipts'
           AND IS_NULLABLE='NO'
           AND (
             (COLUMN_NAME='batch_id'
               AND LOWER(COLUMN_TYPE)='varchar(77)')
             OR (COLUMN_NAME IN ('chunk_index','chunk_count')
               AND LOWER(COLUMN_TYPE)='smallint unsigned')
             OR (COLUMN_NAME IN ('window_start','window_end_exclusive')
               AND DATA_TYPE='date')
             OR (COLUMN_NAME IN (
                   'request_count','batch_raw_count',
                   'batch_acknowledged_count','batch_request_count'
                 )
               AND LOWER(COLUMN_TYPE)='int unsigned')
           )) <> 9 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '011 global ingest receipt column shape incomplete';
  END IF;

  IF (SELECT COUNT(*) FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA=DATABASE()
        AND TABLE_NAME='activist_global_connector_audit') <> 7
     OR (SELECT COUNT(*) FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA=DATABASE()
           AND TABLE_NAME='activist_global_connector_audit'
           AND IS_NULLABLE='NO'
           AND (
             (COLUMN_NAME IN ('audit_id','connector_id')
               AND LOWER(COLUMN_TYPE)='varchar(96)')
             OR (COLUMN_NAME IN ('previous_status','new_status')
               AND LOWER(COLUMN_TYPE)='varchar(24)')
             OR (COLUMN_NAME='reason'
               AND LOWER(COLUMN_TYPE)='varchar(1000)')
             OR (COLUMN_NAME='changed_by'
               AND LOWER(COLUMN_TYPE)='varchar(191)')
             OR (COLUMN_NAME='created_at'
               AND DATA_TYPE='datetime')
           )) <> 7 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '011 global connector audit column shape incomplete';
  END IF;

  IF (SELECT COUNT(*) FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA=DATABASE()
        AND TABLE_NAME='activist_release_authorizations') <> 20
     OR (SELECT COUNT(*) FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA=DATABASE()
           AND TABLE_NAME='activist_release_authorizations'
           AND IS_NULLABLE='NO'
           AND (
             (COLUMN_NAME='authorization_id'
               AND LOWER(COLUMN_TYPE)='varchar(64)')
             OR (COLUMN_NAME='candidate_sha'
               AND LOWER(COLUMN_TYPE)='char(40)')
             OR (COLUMN_NAME='evidence_artifact_digest'
               AND LOWER(COLUMN_TYPE)='char(71)')
             OR (COLUMN_NAME='nonce_sha256'
               AND LOWER(COLUMN_TYPE)='char(64)')
             OR (COLUMN_NAME IN (
                   'evidence_run_id','evidence_artifact_id',
                   'expected_v1_state_version','expected_v2_state_version'
                 )
               AND LOWER(COLUMN_TYPE)='bigint unsigned')
             OR (COLUMN_NAME IN ('expires_at','created_at','updated_at')
               AND DATA_TYPE='datetime')
             OR (COLUMN_NAME='created_by'
               AND LOWER(COLUMN_TYPE)='varchar(191)')
             OR (COLUMN_NAME='create_reason'
               AND LOWER(COLUMN_TYPE)='varchar(1000)')
           )) <> 13 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '011 release authorization column shape incomplete';
  END IF;

  IF NOT EXISTS (
       SELECT 1 FROM information_schema.COLUMNS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_governance_events'
         AND COLUMN_NAME='company_id'
         AND LOWER(COLUMN_TYPE)='char(8)'
         AND IS_NULLABLE='YES'
     )
     OR NOT EXISTS (
       SELECT 1 FROM information_schema.COLUMNS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_brief_editions'
         AND COLUMN_NAME='payload_json'
         AND DATA_TYPE='mediumtext'
         AND IS_NULLABLE='YES'
     )
     OR NOT EXISTS (
       SELECT 1 FROM information_schema.COLUMNS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_brief_items'
         AND COLUMN_NAME='event_snapshot_json'
         AND DATA_TYPE='mediumtext'
         AND IS_NULLABLE='NO'
     )
     OR NOT EXISTS (
       SELECT 1 FROM information_schema.COLUMNS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_governance_events'
         AND COLUMN_NAME='identity_actor_id'
         AND LOWER(COLUMN_TYPE)='varchar(96)'
         AND IS_NULLABLE='YES'
     )
     OR NOT EXISTS (
       SELECT 1 FROM information_schema.COLUMNS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_governance_release_audit'
         AND COLUMN_NAME='release_authorization_id'
         AND LOWER(COLUMN_TYPE)='varchar(64)'
         AND IS_NULLABLE='YES'
     )
     OR NOT EXISTS (
       SELECT 1 FROM information_schema.STATISTICS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_document_sections'
         AND INDEX_NAME='uq_document_section_position'
         AND NON_UNIQUE=0
     )
     OR NOT EXISTS (
       SELECT 1 FROM information_schema.STATISTICS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_issuer_identifiers'
         AND INDEX_NAME='uq_issuer_identifier_global'
         AND NON_UNIQUE=0
     )
     OR COALESCE((
       SELECT CONCAT(
         MIN(NON_UNIQUE), ':', COUNT(*), ':',
         GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')
       )
       FROM information_schema.STATISTICS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_brief_items'
         AND INDEX_NAME='uq_brief_item_event'
     ), '') <> '0:2:brief_id,event_id'
     OR COALESCE((
       SELECT CONCAT(
         MIN(NON_UNIQUE), ':', COUNT(*), ':',
         GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')
       )
       FROM information_schema.STATISTICS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_global_connector_audit'
         AND INDEX_NAME='PRIMARY'
     ), '') <> '0:1:audit_id'
     OR COALESCE((
       SELECT CONCAT(
         MIN(NON_UNIQUE), ':', COUNT(*), ':',
         GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')
       )
       FROM information_schema.STATISTICS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_global_connector_audit'
         AND INDEX_NAME='idx_global_connector_audit_connector'
     ), '') <> '1:2:connector_id,created_at'
     OR NOT EXISTS (
       SELECT 1
       FROM information_schema.KEY_COLUMN_USAGE k
       JOIN information_schema.REFERENTIAL_CONSTRAINTS r
         ON r.CONSTRAINT_SCHEMA=k.CONSTRAINT_SCHEMA
        AND r.CONSTRAINT_NAME=k.CONSTRAINT_NAME
        AND r.TABLE_NAME=k.TABLE_NAME
       WHERE k.CONSTRAINT_SCHEMA=DATABASE()
         AND k.TABLE_NAME='activist_global_connector_audit'
         AND k.CONSTRAINT_NAME='fk_global_connector_audit_connector'
         AND k.COLUMN_NAME='connector_id'
         AND k.REFERENCED_TABLE_NAME='activist_source_connectors'
         AND k.REFERENCED_COLUMN_NAME='connector_id'
         AND r.UPDATE_RULE='CASCADE'
         AND r.DELETE_RULE='RESTRICT'
     )
     OR NOT EXISTS (
       SELECT 1 FROM information_schema.STATISTICS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_global_ingest_receipts'
         AND INDEX_NAME='uq_global_ingest_connector_key'
         AND NON_UNIQUE=0
     )
     OR COALESCE((
       SELECT CONCAT(
         MIN(NON_UNIQUE), ':', COUNT(*), ':',
         GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')
       )
       FROM information_schema.STATISTICS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_global_ingest_receipts'
         AND INDEX_NAME='uq_global_ingest_connector_key'
     ), '') <> '0:2:connector_id,idempotency_key'
     OR COALESCE((
       SELECT CONCAT(
         MIN(NON_UNIQUE), ':', COUNT(*), ':',
         GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')
       )
       FROM information_schema.STATISTICS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_global_ingest_receipts'
         AND INDEX_NAME='uq_global_ingest_batch_chunk'
     ), '') <> '0:3:connector_id,batch_id,chunk_index'
     OR COALESCE((
       SELECT CONCAT(
         MIN(NON_UNIQUE), ':', COUNT(*), ':',
         GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')
       )
       FROM information_schema.STATISTICS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_global_ingest_receipts'
         AND INDEX_NAME='idx_global_ingest_batch'
     ), '') <> '1:2:connector_id,batch_id'
     OR COALESCE((
       SELECT CONCAT(
         MIN(NON_UNIQUE), ':', COUNT(*), ':',
         GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')
       )
       FROM information_schema.STATISTICS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_global_ingest_receipts'
         AND INDEX_NAME='idx_global_ingest_window'
     ), '') <> '1:3:connector_id,window_end_exclusive,completed_at'
     OR COALESCE((
       SELECT CONCAT(
         MIN(NON_UNIQUE), ':', COUNT(*), ':',
         GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')
       )
       FROM information_schema.STATISTICS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_release_authorizations'
         AND INDEX_NAME='uq_release_authorization_nonce'
     ), '') <> '0:1:nonce_sha256'
     OR COALESCE((
       SELECT CONCAT(
         MIN(NON_UNIQUE), ':', COUNT(*), ':',
         GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')
       )
       FROM information_schema.STATISTICS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_release_authorizations'
         AND INDEX_NAME='idx_release_authorization_candidate'
     ), '') <> '1:2:candidate_sha,evidence_artifact_digest'
     OR COALESCE((
       SELECT CONCAT(
         MIN(NON_UNIQUE), ':', COUNT(*), ':',
         GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')
       )
       FROM information_schema.STATISTICS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_release_authorizations'
         AND INDEX_NAME='idx_release_authorization_expiry'
     ), '') <> '1:3:expires_at,revoked_at,fully_consumed_at'
     OR COALESCE((
       SELECT CONCAT(
         MIN(NON_UNIQUE), ':', COUNT(*), ':',
         GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')
       )
       FROM information_schema.STATISTICS
       WHERE TABLE_SCHEMA=DATABASE()
         AND TABLE_NAME='activist_governance_release_audit'
         AND INDEX_NAME='idx_release_audit_authorization'
     ), '') <> '1:2:release_authorization_id,state_key' THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '011 global terminal constraint shape incomplete';
  END IF;

  IF (SELECT COUNT(*) FROM activist_jurisdictions
      WHERE record_status='active'
        AND (
          (country_code='KR' AND timezone_name='Asia/Seoul' AND launch_order=1)
          OR (country_code='US' AND timezone_name='America/New_York' AND launch_order=2)
          OR (country_code='JP' AND timezone_name='Asia/Tokyo' AND launch_order=3)
          OR (country_code='GB' AND timezone_name='Europe/London' AND launch_order=4)
          OR (country_code='CA' AND timezone_name='America/Toronto' AND launch_order=5)
          OR (country_code='AU' AND timezone_name='Australia/Sydney' AND launch_order=6)
        )) <> 6
     OR (SELECT COUNT(*) FROM activist_source_connectors
         WHERE
           (connector_id='connector:kr:dart' AND country_code='KR'
             AND source_key='dart' AND source_right_id='official:dart'
             AND coverage_mode='market-wide')
           OR (connector_id='connector:us:sec-edgar' AND country_code='US'
             AND source_key='sec-edgar' AND source_right_id='official:sec-edgar'
             AND coverage_mode='market-wide')
           OR (connector_id='connector:jp:edinet' AND country_code='JP'
             AND source_key='edinet' AND source_right_id='official:edinet'
             AND coverage_mode='market-wide')
           OR (connector_id='connector:gb:companies-house' AND country_code='GB'
             AND source_key='companies-house'
             AND source_right_id='official:companies-house'
             AND coverage_mode='official-register')
           OR (connector_id='connector:ca:issuer-ir' AND country_code='CA'
             AND source_key='issuer-ir' AND source_right_id='official:ca-issuer-ir'
             AND coverage_mode='link-only')
           OR (connector_id='connector:au:asic-register' AND country_code='AU'
             AND source_key='asic-register'
             AND source_right_id='official:asic-register'
             AND coverage_mode='link-only')) <> 6
     OR (SELECT COUNT(*) FROM activist_source_rights
         WHERE
           (source_right_id='official:sec-edgar'
             AND source_type='official_disclosure' AND source_key='sec-edgar')
           OR (source_right_id='official:edinet'
             AND source_type='official_disclosure' AND source_key='edinet')
           OR (source_right_id='official:companies-house'
             AND source_type='official_register' AND source_key='companies-house')
           OR (source_right_id='official:ca-issuer-ir'
             AND source_type='official_issuer' AND source_key='issuer-ir')
           OR (source_right_id='official:asic-register'
             AND source_type='official_register' AND source_key='asic-register')) <> 5
     OR (SELECT COUNT(DISTINCT event_family) FROM activist_source_coverage
         WHERE connector_id='connector:kr:dart' AND country_code='KR'
           AND coverage_mode='market-wide' AND effective_until IS NULL) <> 8
     OR (SELECT COUNT(DISTINCT event_family) FROM activist_source_coverage
         WHERE connector_id='connector:us:sec-edgar' AND country_code='US'
           AND coverage_mode='market-wide' AND effective_until IS NULL) <> 3
     OR (SELECT COUNT(DISTINCT event_family) FROM activist_source_coverage
         WHERE connector_id='connector:jp:edinet' AND country_code='JP'
           AND coverage_mode='market-wide' AND effective_until IS NULL) <> 5
     OR (SELECT COUNT(DISTINCT event_family) FROM activist_source_coverage
         WHERE connector_id='connector:gb:companies-house' AND country_code='GB'
           AND coverage_mode='official-register' AND effective_until IS NULL) <> 4
     OR (SELECT COUNT(DISTINCT event_family) FROM activist_source_coverage
         WHERE connector_id='connector:ca:issuer-ir' AND country_code='CA'
           AND coverage_mode='link-only' AND effective_until IS NULL) <> 4
     OR (SELECT COUNT(DISTINCT event_family) FROM activist_source_coverage
         WHERE connector_id='connector:au:asic-register' AND country_code='AU'
           AND coverage_mode='link-only' AND effective_until IS NULL) <> 2 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '011 global terminal registry seed conflict';
  END IF;

  IF EXISTS (
       SELECT 1
       FROM activist_companies c
       LEFT JOIN activist_issuer_identifiers ii
         ON ii.issuer_id=CONCAT('issuer:kr:dart:', c.company_id)
        AND ii.identifier_type='DART_CORP_CODE'
        AND ii.identifier_value=c.company_id
        AND ii.market='KRX'
       WHERE ii.issuer_id IS NULL
     )
     OR EXISTS (
       SELECT 1
       FROM activist_companies c
       JOIN activist_issuer_identifiers ii
         ON ii.identifier_type='DART_CORP_CODE'
        AND ii.identifier_value=c.company_id
        AND ii.market='KRX'
       WHERE ii.issuer_id<>CONCAT('issuer:kr:dart:', c.company_id)
     )
     OR EXISTS (
       SELECT 1
       FROM activist_companies c
       LEFT JOIN activist_issuer_listings il
         ON il.listing_id=CONCAT('listing:kr:', c.company_id)
        AND il.issuer_id=CONCAT('issuer:kr:dart:', c.company_id)
        AND il.country_code='KR'
       WHERE c.stock_code IS NOT NULL
         AND c.stock_code<>''
         AND il.listing_id IS NULL
     )
     OR EXISTS (
       SELECT 1
       FROM activist_governance_events e
       WHERE e.company_id IS NOT NULL
         AND e.event_type IN (
           'five_percent_holding',
           'shareholder_proposal',
           'general_meeting',
           'tender_offer',
           'merger',
           'split',
           'rights_issue',
           'convertible_bond',
           'bond_with_warrant',
           'exchangeable_bond',
           'dividend',
           'treasury_shares',
           'board',
           'executive_compensation',
           'trading_suspension',
           'delisting',
           'duplicate_listing',
           'value_up'
         )
         AND e.global_event_family IS NULL
     ) THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '011 legacy issuer identity backfill conflict';
  END IF;

  SELECT COUNT(*) INTO existing_count
  FROM activist_schema_migrations
  WHERE migration_version=11;

  IF existing_count=0 THEN
    IF (SELECT COUNT(*) FROM activist_governance_release_state
        WHERE state_key='global_terminal_v2'
          AND release_state='closed'
          AND state_version=0
          AND updated_by='migration:011') <> 1
       OR (SELECT COUNT(*) FROM activist_governance_release_audit
           WHERE audit_id='release:global-terminal:initial:011'
             AND state_key='global_terminal_v2'
             AND state_version=0
             AND previous_state IS NULL
             AND new_state='closed'
             AND changed_by='migration:011') <> 1 THEN
      SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = '011 initial fail-closed release evidence missing';
    END IF;
    INSERT INTO activist_schema_migrations (
      migration_version, migration_name, migration_checksum, applied_at
    ) VALUES (
      11,
      '011_global_terminal_v2',
      @bside_migration_011_sha256,
      UTC_TIMESTAMP()
    );
  ELSE
    SELECT migration_name, migration_checksum
      INTO existing_name, existing_checksum
    FROM activist_schema_migrations
    WHERE migration_version=11
    LIMIT 1;
    IF BINARY existing_name <> BINARY '011_global_terminal_v2'
       OR existing_checksum IS NULL
       OR BINARY existing_checksum <>
          BINARY @bside_migration_011_sha256 THEN
      SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = '011 migration manifest conflict';
    END IF;
  END IF;
END$$
DELIMITER ;

CALL activist_011_record_migration();
DROP PROCEDURE activist_011_record_migration;
