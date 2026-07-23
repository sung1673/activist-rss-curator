-- BSIDE Governance API release guard and explicit schema version marker.
--
-- Apply with a schema-owner account before deploying the PHP version that
-- expects GOV_V1_SCHEMA_VERSION=6. The default table prefix is activist_.
-- Custom installations must replace that prefix consistently before running.

CREATE TABLE IF NOT EXISTS activist_schema_migrations (
  migration_version INT UNSIGNED NOT NULL PRIMARY KEY,
  migration_name VARCHAR(191) NOT NULL,
  applied_at DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_governance_release_state (
  state_key VARCHAR(40) NOT NULL PRIMARY KEY,
  release_state VARCHAR(16) NOT NULL,
  state_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  updated_by VARCHAR(191) NOT NULL,
  update_reason TEXT NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_governance_release_updated (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_governance_release_audit (
  audit_id VARCHAR(64) NOT NULL PRIMARY KEY,
  state_key VARCHAR(40) NOT NULL,
  state_version BIGINT UNSIGNED NOT NULL,
  previous_state VARCHAR(16) NULL,
  new_state VARCHAR(16) NOT NULL,
  changed_by VARCHAR(191) NOT NULL,
  change_reason TEXT NOT NULL,
  request_id VARCHAR(96) NULL,
  created_at DATETIME NOT NULL,
  UNIQUE KEY uq_governance_release_version (state_key, state_version),
  KEY idx_governance_release_audit_time (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO activist_governance_release_state (
  state_key, release_state, state_version, updated_by, update_reason, updated_at
) VALUES (
  'governance_v1', 'closed', 0, 'migration:006',
  'Initial fail-closed governance API state', UTC_TIMESTAMP()
) ON DUPLICATE KEY UPDATE state_key=VALUES(state_key);

INSERT IGNORE INTO activist_governance_release_audit (
  audit_id, state_key, state_version, previous_state, new_state,
  changed_by, change_reason, request_id, created_at
) VALUES (
  'release:initial:006', 'governance_v1', 0, NULL, 'closed',
  'migration:006', 'Initial fail-closed governance API state', NULL, UTC_TIMESTAMP()
);

INSERT INTO activist_schema_migrations (
  migration_version, migration_name, applied_at
) VALUES (
  6, '006_governance_release_guard', UTC_TIMESTAMP()
) ON DUPLICATE KEY UPDATE migration_version=VALUES(migration_version);
