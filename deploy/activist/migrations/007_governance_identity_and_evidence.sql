-- BSIDE governance canonical identity, observations and release evidence data.
-- Apply after 006_governance_release_guard.sql with a schema-owner account.

DROP PROCEDURE IF EXISTS activist_007_add_column;
DROP PROCEDURE IF EXISTS activist_007_add_index;
DROP PROCEDURE IF EXISTS activist_007_drop_index;
DROP PROCEDURE IF EXISTS activist_007_modify_column;
DELIMITER $$
CREATE PROCEDURE activist_007_add_column(IN table_name_value VARCHAR(64), IN column_name_value VARCHAR(64), IN definition_value VARCHAR(500))
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = table_name_value AND COLUMN_NAME = column_name_value
  ) THEN
    SET @ddl = CONCAT('ALTER TABLE `', table_name_value, '` ADD COLUMN `', column_name_value, '` ', definition_value);
    PREPARE statement_value FROM @ddl;
    EXECUTE statement_value;
    DEALLOCATE PREPARE statement_value;
  END IF;
END$$

CREATE PROCEDURE activist_007_add_index(IN table_name_value VARCHAR(64), IN index_name_value VARCHAR(64), IN definition_value VARCHAR(500))
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = table_name_value AND INDEX_NAME = index_name_value
  ) THEN
    SET @ddl = CONCAT('ALTER TABLE `', table_name_value, '` ADD ', definition_value);
    PREPARE statement_value FROM @ddl;
    EXECUTE statement_value;
    DEALLOCATE PREPARE statement_value;
  END IF;
END$$

CREATE PROCEDURE activist_007_drop_index(IN table_name_value VARCHAR(64), IN index_name_value VARCHAR(64))
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = table_name_value AND INDEX_NAME = index_name_value
  ) THEN
    SET @ddl = CONCAT('ALTER TABLE `', table_name_value, '` DROP INDEX `', index_name_value, '`');
    PREPARE statement_value FROM @ddl;
    EXECUTE statement_value;
    DEALLOCATE PREPARE statement_value;
  END IF;
END$$

CREATE PROCEDURE activist_007_modify_column(IN table_name_value VARCHAR(64), IN column_name_value VARCHAR(64), IN definition_value VARCHAR(500))
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = table_name_value AND COLUMN_NAME = column_name_value
  ) THEN
    SET @ddl = CONCAT('ALTER TABLE `', table_name_value, '` MODIFY COLUMN `', column_name_value, '` ', definition_value);
    PREPARE statement_value FROM @ddl;
    EXECUTE statement_value;
    DEALLOCATE PREPARE statement_value;
  END IF;
END$$
DELIMITER ;

CALL activist_007_add_column('activist_companies', 'listing_status', 'VARCHAR(24) NOT NULL DEFAULT ''unknown'' AFTER record_status');
CALL activist_007_add_column('activist_companies', 'master_modified_at', 'DATETIME NULL AFTER listing_status');

CALL activist_007_add_column('activist_governance_events', 'identity_action', 'VARCHAR(255) NULL AFTER collection_key');
CALL activist_007_add_column('activist_governance_events', 'identity_target', 'VARCHAR(700) NULL AFTER identity_action');
CALL activist_007_add_column('activist_governance_events', 'identity_actor_id', 'VARCHAR(64) NULL AFTER identity_target');
CALL activist_007_add_column('activist_governance_events', 'identity_effective_at', 'DATETIME NULL AFTER identity_actor_id');
CALL activist_007_add_column('activist_governance_events', 'identity_deadline_at', 'DATETIME NULL AFTER identity_effective_at');
CALL activist_007_add_column('activist_governance_events', 'identity_status', 'VARCHAR(24) NOT NULL DEFAULT ''needs_review'' AFTER identity_deadline_at');
CALL activist_007_add_column('activist_governance_events', 'comparison_key', 'VARCHAR(96) NULL AFTER identity_status');

CALL activist_007_add_column('activist_collection_runs', 'code_revision', 'VARCHAR(64) NULL AFTER source_key');
CALL activist_007_add_column('activist_collection_runs', 'first_observed_at', 'DATETIME NULL AFTER finished_at');
CALL activist_007_add_column('activist_collection_runs', 'raw_count', 'INT UNSIGNED NOT NULL DEFAULT 0 AFTER first_observed_at');
CALL activist_007_add_column('activist_collection_runs', 'acknowledged_count', 'INT UNSIGNED NOT NULL DEFAULT 0 AFTER raw_count');

CALL activist_007_add_column('activist_governance_release_state', 'cutover_at', 'DATETIME NULL AFTER update_reason');
CALL activist_007_add_column('activist_governance_release_state', 'sunset_at', 'DATETIME NULL AFTER cutover_at');
CALL activist_007_add_column('activist_governance_release_audit', 'cutover_at', 'DATETIME NULL AFTER request_id');
CALL activist_007_add_column('activist_governance_release_audit', 'sunset_at', 'DATETIME NULL AFTER cutover_at');

CALL activist_007_add_index('activist_companies', 'idx_company_listing_status', 'KEY `idx_company_listing_status` (`listing_status`,`record_status`)');
CALL activist_007_add_index('activist_governance_events', 'uq_event_comparison_key', 'UNIQUE KEY `uq_event_comparison_key` (`comparison_key`)');
CALL activist_007_add_index('activist_governance_events', 'idx_event_identity_review', 'KEY `idx_event_identity_review` (`identity_status`,`updated_at`)');
CALL activist_007_add_index('activist_governance_events', 'idx_event_identity_actor', 'KEY `idx_event_identity_actor` (`identity_actor_id`,`occurred_at`)');
CALL activist_007_add_index('activist_collection_runs', 'idx_collection_revision_finished', 'KEY `idx_collection_revision_finished` (`code_revision`,`finished_at`)');

CREATE TABLE IF NOT EXISTS activist_event_observations (
  observation_id VARCHAR(96) NOT NULL PRIMARY KEY,
  event_id VARCHAR(96) NOT NULL,
  document_id VARCHAR(96) NOT NULL,
  source_class VARCHAR(40) NOT NULL,
  source_key VARCHAR(191) NOT NULL,
  first_observed_at DATETIME NOT NULL,
  observed_at DATETIME NOT NULL,
  payload_hash CHAR(64) NOT NULL,
  payload_json MEDIUMTEXT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_event_observation_source (event_id, document_id, source_key),
  KEY idx_event_observation_event_time (event_id, observed_at),
  KEY idx_event_observation_document (document_id),
  KEY idx_event_observation_source_time (source_class, observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_shadow_discrepancies (
  discrepancy_id VARCHAR(96) NOT NULL PRIMARY KEY,
  observation_date DATE NOT NULL,
  code_revision VARCHAR(64) NOT NULL,
  comparison_key VARCHAR(191) NOT NULL,
  discrepancy_type VARCHAR(40) NOT NULL,
  legacy_event_json MEDIUMTEXT NULL,
  candidate_event_json MEDIUMTEXT NULL,
  review_status VARCHAR(24) NOT NULL DEFAULT 'pending',
  review_note TEXT NULL,
  reviewed_by VARCHAR(191) NULL,
  reviewed_at DATETIME NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_shadow_discrepancy (observation_date, code_revision, comparison_key, discrepancy_type),
  KEY idx_shadow_review_date (review_status, observation_date),
  KEY idx_shadow_revision_date (code_revision, observation_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- A discrepancy row cannot prove that both engines completed on a day where
-- their outputs were identical.  Persist one immutable comparison snapshot per
-- release-candidate SHA and KST observation date so the 14-day gate has a real
-- denominator even when the discrepancy count is zero.
CREATE TABLE IF NOT EXISTS activist_shadow_run_observations (
  observation_date DATE NOT NULL,
  code_revision VARCHAR(64) NOT NULL,
  legacy_status VARCHAR(16) NOT NULL,
  candidate_status VARCHAR(16) NOT NULL,
  legacy_comparison_keys_json MEDIUMTEXT NOT NULL,
  candidate_comparison_keys_json MEDIUMTEXT NOT NULL,
  legacy_event_count INT UNSIGNED NOT NULL,
  candidate_event_count INT UNSIGNED NOT NULL,
  legacy_events_sha256 CHAR(64) NOT NULL,
  candidate_events_sha256 CHAR(64) NOT NULL,
  legacy_crosswalk_schema_version TINYINT UNSIGNED NULL,
  legacy_eligible_record_count INT UNSIGNED NULL,
  legacy_crosswalked_record_count INT UNSIGNED NULL,
  legacy_unmatched_record_count INT UNSIGNED NULL,
  legacy_ambiguous_record_count INT UNSIGNED NULL,
  legacy_crosswalk_coverage_rate DECIMAL(10,6) NULL,
  legacy_crosswalk_sha256 CHAR(64) NULL,
  created_by VARCHAR(191) NOT NULL,
  updated_by VARCHAR(191) NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (observation_date, code_revision),
  KEY idx_shadow_run_revision_date (code_revision, observation_date),
  KEY idx_shadow_run_status_date (legacy_status, candidate_status, observation_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_availability_observations (
  observation_id VARCHAR(96) NOT NULL PRIMARY KEY,
  observed_at DATETIME NOT NULL,
  route_template VARCHAR(191) NOT NULL,
  http_status SMALLINT UNSIGNED NOT NULL,
  duration_ms INT UNSIGNED NOT NULL,
  succeeded TINYINT(1) NOT NULL,
  build_sha VARCHAR(64) NOT NULL,
  source VARCHAR(40) NOT NULL,
  created_at DATETIME NOT NULL,
  KEY idx_availability_time_success (observed_at, succeeded),
  KEY idx_availability_build_time (build_sha, observed_at),
  KEY idx_availability_route_time (route_template, observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_web_distribution_observations (
  observation_id VARCHAR(96) NOT NULL PRIMARY KEY,
  observed_at DATETIME NOT NULL,
  distribution_target VARCHAR(16) NOT NULL,
  duration_ms INT UNSIGNED NOT NULL,
  succeeded TINYINT(1) NOT NULL,
  build_sha VARCHAR(64) NOT NULL,
  workflow_run_id BIGINT UNSIGNED NOT NULL,
  workflow_run_attempt INT UNSIGNED NOT NULL,
  failure_detected_at DATETIME NULL,
  source VARCHAR(40) NOT NULL,
  created_at DATETIME NOT NULL,
  UNIQUE KEY uq_web_distribution_run_attempt_target (workflow_run_id, workflow_run_attempt, distribution_target),
  KEY idx_web_distribution_time_success (observed_at, succeeded),
  KEY idx_web_distribution_build_time (build_sha, observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Keep reruns of a GitHub workflow distinct while preserving idempotency for
-- one concrete run attempt.  These calls also upgrade databases that applied
-- an earlier draft of migration 007.
CALL activist_007_add_column('activist_web_distribution_observations', 'workflow_run_attempt', 'INT UNSIGNED NOT NULL DEFAULT 1 AFTER workflow_run_id');
CALL activist_007_modify_column('activist_web_distribution_observations', 'workflow_run_attempt', 'INT UNSIGNED NOT NULL');
CALL activist_007_drop_index('activist_web_distribution_observations', 'uq_web_distribution_run_target');
CALL activist_007_add_index('activist_web_distribution_observations', 'uq_web_distribution_run_attempt_target', 'UNIQUE KEY `uq_web_distribution_run_attempt_target` (`workflow_run_id`,`workflow_run_attempt`,`distribution_target`)');

CREATE TABLE IF NOT EXISTS activist_governance_quality_observations (
  observation_id VARCHAR(96) NOT NULL PRIMARY KEY,
  observation_date DATE NOT NULL,
  code_revision CHAR(40) NOT NULL,
  dart_success_poll_interval_p95_minutes DECIMAL(12,4) NOT NULL,
  kind_observation_lag_p95_minutes DECIMAL(12,4) NULL,
  kind_observation_count INT UNSIGNED NOT NULL DEFAULT 0,
  kind_lag_sample_count INT UNSIGNED NOT NULL DEFAULT 0,
  content_snapshot_at DATETIME NULL,
  content_scope VARCHAR(64) NULL,
  official_evidence_total_count INT UNSIGNED NOT NULL,
  official_evidence_linked_count INT UNSIGNED NOT NULL,
  same_story_evaluated_pair_count INT UNSIGNED NOT NULL,
  same_story_predicted_same_count INT UNSIGNED NOT NULL,
  same_story_true_positive_count INT UNSIGNED NOT NULL,
  top_sensitive_total_count INT UNSIGNED NOT NULL,
  top_sensitive_reviewed_count INT UNSIGNED NOT NULL,
  original_language_total_count INT UNSIGNED NOT NULL,
  original_language_preserved_count INT UNSIGNED NOT NULL,
  source_right_total_count INT UNSIGNED NOT NULL,
  valid_source_right_count INT UNSIGNED NOT NULL,
  source VARCHAR(40) NOT NULL,
  payload_sha256 CHAR(64) NOT NULL,
  created_by VARCHAR(191) NOT NULL,
  created_at DATETIME NOT NULL,
  UNIQUE KEY uq_governance_quality_day_revision (observation_date, code_revision),
  KEY idx_governance_quality_revision_date (code_revision, observation_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CALL activist_007_modify_column('activist_governance_quality_observations', 'kind_observation_lag_p95_minutes', 'DECIMAL(12,4) NULL');
CALL activist_007_add_column('activist_governance_quality_observations', 'kind_observation_count', 'INT UNSIGNED NOT NULL DEFAULT 0 AFTER kind_observation_lag_p95_minutes');
CALL activist_007_add_column('activist_governance_quality_observations', 'kind_lag_sample_count', 'INT UNSIGNED NOT NULL DEFAULT 0 AFTER kind_observation_count');
CALL activist_007_add_column('activist_governance_quality_observations', 'content_snapshot_at', 'DATETIME NULL AFTER kind_lag_sample_count');
CALL activist_007_add_column('activist_governance_quality_observations', 'content_scope', 'VARCHAR(64) NULL AFTER content_snapshot_at');

-- A non-empty, complete legacy-to-canonical crosswalk is the denominator for
-- claiming that a zero-discrepancy shadow day is meaningful.  Draft migration
-- upgrades leave prior rows NULL so release export fails closed until the same
-- SHA/day is re-observed with the protected crosswalk contract.
CALL activist_007_add_column('activist_shadow_run_observations', 'legacy_crosswalk_schema_version', 'TINYINT UNSIGNED NULL AFTER candidate_events_sha256');
CALL activist_007_add_column('activist_shadow_run_observations', 'legacy_eligible_record_count', 'INT UNSIGNED NULL AFTER legacy_crosswalk_schema_version');
CALL activist_007_add_column('activist_shadow_run_observations', 'legacy_crosswalked_record_count', 'INT UNSIGNED NULL AFTER legacy_eligible_record_count');
CALL activist_007_add_column('activist_shadow_run_observations', 'legacy_unmatched_record_count', 'INT UNSIGNED NULL AFTER legacy_crosswalked_record_count');
CALL activist_007_add_column('activist_shadow_run_observations', 'legacy_ambiguous_record_count', 'INT UNSIGNED NULL AFTER legacy_unmatched_record_count');
CALL activist_007_add_column('activist_shadow_run_observations', 'legacy_crosswalk_coverage_rate', 'DECIMAL(10,6) NULL AFTER legacy_ambiguous_record_count');
CALL activist_007_add_column('activist_shadow_run_observations', 'legacy_crosswalk_sha256', 'CHAR(64) NULL AFTER legacy_crosswalk_coverage_rate');

DROP PROCEDURE activist_007_add_column;
DROP PROCEDURE activist_007_add_index;
DROP PROCEDURE activist_007_drop_index;
DROP PROCEDURE activist_007_modify_column;

CREATE TABLE IF NOT EXISTS activist_web_vital_observations (
  metric_id VARCHAR(96) NOT NULL PRIMARY KEY,
  measured_at DATETIME NOT NULL,
  route_template VARCHAR(191) NOT NULL,
  metric_name VARCHAR(8) NOT NULL,
  metric_value DECIMAL(14,4) NOT NULL,
  device_class VARCHAR(16) NOT NULL,
  build_sha VARCHAR(64) NOT NULL,
  source VARCHAR(40) NOT NULL DEFAULT 'first_party',
  expires_at DATETIME NOT NULL,
  created_at DATETIME NOT NULL,
  KEY idx_web_vital_metric_time (metric_name, measured_at),
  KEY idx_web_vital_build_time (build_sha, measured_at),
  KEY idx_web_vital_expiry (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_official_backfill_checkpoints (
  job_fingerprint CHAR(64) NOT NULL PRIMARY KEY,
  checkpoint_version BIGINT UNSIGNED NOT NULL,
  checkpoint_json MEDIUMTEXT NOT NULL,
  payload_hash CHAR(64) NOT NULL,
  updated_by VARCHAR(191) NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_backfill_checkpoint_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Human-labelled benchmark, usability and approval evidence is uploaded as one
-- immutable, same-revision bundle.  Corrections append a version; prior
-- versions remain available for audit and are never overwritten.
CREATE TABLE IF NOT EXISTS activist_human_release_evidence_bundles (
  code_revision CHAR(40) NOT NULL,
  bundle_version INT UNSIGNED NOT NULL,
  bundle_sha256 CHAR(64) NOT NULL,
  benchmark_json MEDIUMTEXT NOT NULL,
  benchmark_sha256 CHAR(64) NOT NULL,
  usability_json MEDIUMTEXT NOT NULL,
  usability_sha256 CHAR(64) NOT NULL,
  release_approval_json MEDIUMTEXT NOT NULL,
  release_approval_sha256 CHAR(64) NOT NULL,
  created_by VARCHAR(191) NOT NULL,
  created_at DATETIME NOT NULL,
  PRIMARY KEY (code_revision, bundle_version),
  UNIQUE KEY uq_human_evidence_bundle_sha (bundle_sha256),
  KEY idx_human_evidence_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO activist_schema_migrations (
  migration_version, migration_name, applied_at
) VALUES (
  7, '007_governance_identity_and_evidence', UTC_TIMESTAMP()
) ON DUPLICATE KEY UPDATE migration_version=VALUES(migration_version);
