-- Atomic receipts, identity-link history and review-only tombstones for
-- allowlisted company/activist official-site snapshots.

DROP PROCEDURE IF EXISTS activist_008_add_column;
DROP PROCEDURE IF EXISTS activist_008_modify_column;
DROP PROCEDURE IF EXISTS activist_008_drop_index;
DROP PROCEDURE IF EXISTS activist_008_add_unique_index;
DROP PROCEDURE IF EXISTS activist_008_record_migration;
DELIMITER $$
CREATE PROCEDURE activist_008_add_column(IN table_name_value VARCHAR(64), IN column_name_value VARCHAR(64), IN definition_value VARCHAR(500))
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

CREATE PROCEDURE activist_008_modify_column(IN table_name_value VARCHAR(64), IN column_name_value VARCHAR(64), IN definition_value VARCHAR(500))
BEGIN
  SET @ddl = CONCAT('ALTER TABLE `', table_name_value, '` MODIFY COLUMN `', column_name_value, '` ', definition_value);
  PREPARE statement_value FROM @ddl;
  EXECUTE statement_value;
  DEALLOCATE PREPARE statement_value;
END$$

CREATE PROCEDURE activist_008_drop_index(IN table_name_value VARCHAR(64), IN index_name_value VARCHAR(64))
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

CREATE PROCEDURE activist_008_add_unique_index(IN table_name_value VARCHAR(64), IN index_name_value VARCHAR(64), IN columns_value VARCHAR(500))
BEGIN
  DECLARE index_column_count INT DEFAULT 0;
  DECLARE index_non_unique INT DEFAULT 1;
  DECLARE index_columns VARCHAR(500) DEFAULT '';
  IF EXISTS (
    SELECT 1 FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = table_name_value AND INDEX_NAME = index_name_value
  ) THEN
    SELECT COUNT(*), MIN(NON_UNIQUE), GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX SEPARATOR ',')
      INTO index_column_count, index_non_unique, index_columns
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = table_name_value AND INDEX_NAME = index_name_value;
    IF index_non_unique <> 0 OR index_column_count <> 3 OR index_columns <> REPLACE(columns_value, ' ', '') THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '008 document version index has an incompatible shape';
    END IF;
  ELSE
    SET @ddl = CONCAT('ALTER TABLE `', table_name_value, '` ADD UNIQUE INDEX `', index_name_value, '` (', columns_value, ')');
    PREPARE statement_value FROM @ddl;
    EXECUTE statement_value;
    DEALLOCATE PREPARE statement_value;
  END IF;
END$$

-- Versions 1-7 predate the checksum column.  An existing row may receive its
-- checksum exactly once, but its name must already be the expected historical
-- name.  Once populated, both fields are immutable.  Version 8 is append-only
-- from its first write and an existing NULL checksum is treated as corruption.
CREATE PROCEDURE activist_008_record_migration(
  IN version_value INT UNSIGNED,
  IN name_value VARCHAR(191),
  IN checksum_value CHAR(64),
  IN allow_legacy_checksum_baseline TINYINT UNSIGNED
)
BEGIN
  DECLARE existing_count INT DEFAULT 0;
  DECLARE existing_name VARCHAR(191) DEFAULT NULL;
  DECLARE existing_checksum CHAR(64) DEFAULT NULL;

  SELECT COUNT(*) INTO existing_count
  FROM activist_schema_migrations
  WHERE migration_version = version_value;

  IF existing_count = 0 THEN
    INSERT INTO activist_schema_migrations (
      migration_version, migration_name, migration_checksum, applied_at
    ) VALUES (version_value, name_value, checksum_value, UTC_TIMESTAMP());
  ELSE
    SELECT migration_name, migration_checksum
      INTO existing_name, existing_checksum
    FROM activist_schema_migrations
    WHERE migration_version = version_value
    LIMIT 1;

    IF BINARY existing_name <> BINARY name_value THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '008 migration name conflict';
    END IF;
    IF existing_checksum IS NULL THEN
      IF allow_legacy_checksum_baseline <> 1 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '008 migration checksum missing';
      END IF;
      UPDATE activist_schema_migrations
      SET migration_checksum = checksum_value
      WHERE migration_version = version_value AND migration_checksum IS NULL;
    ELSEIF BINARY existing_checksum <> BINARY checksum_value THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '008 migration checksum conflict';
    END IF;
  END IF;
END$$
DELIMITER ;

CALL activist_008_add_column('activist_schema_migrations', 'migration_checksum', 'CHAR(64) NULL AFTER migration_name');
CALL activist_008_drop_index('activist_documents', 'uq_document_external_version');
CALL activist_008_add_unique_index('activist_documents', 'uq_document_right_external_version', 'source_right_id, external_id, version_no');

CREATE TABLE IF NOT EXISTS activist_official_site_snapshots (
  snapshot_id VARCHAR(96) NOT NULL PRIMARY KEY,
  receipt_sha256 CHAR(64) NOT NULL,
  request_sha256 CHAR(64) NOT NULL,
  manifest_sha256 CHAR(64) NOT NULL,
  connector_id VARCHAR(96) NOT NULL,
  source_right_id VARCHAR(64) NOT NULL,
  source_type VARCHAR(40) NOT NULL,
  source_key VARCHAR(191) NOT NULL,
  connector_receipt_json MEDIUMTEXT NOT NULL,
  code_revision CHAR(40) NOT NULL,
  collected_at DATETIME NOT NULL,
  accepted_json TEXT NOT NULL,
  status VARCHAR(24) NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_official_site_connector_receipt (connector_id, receipt_sha256),
  UNIQUE KEY uq_official_site_request_hash (request_sha256),
  KEY idx_official_site_snapshot_collected (connector_id, collected_at),
  KEY idx_official_site_snapshot_revision (code_revision, collected_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_official_site_identity_links (
  link_id VARCHAR(96) NOT NULL PRIMARY KEY,
  connector_id VARCHAR(96) NOT NULL,
  source_right_id VARCHAR(64) NOT NULL,
  entity_type VARCHAR(24) NOT NULL,
  external_id VARCHAR(191) NOT NULL,
  entity_id VARCHAR(96) NOT NULL,
  snapshot_id VARCHAR(96) NOT NULL,
  active TINYINT(1) NOT NULL DEFAULT 1,
  active_identity_key CHAR(64) GENERATED ALWAYS AS (
    IF(active = 1, SHA2(CONCAT_WS(CHAR(31), connector_id, entity_type, external_id), 256), NULL)
  ) STORED,
  retired_at DATETIME NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_official_site_active_identity (active_identity_key),
  UNIQUE KEY uq_official_site_snapshot_identity (snapshot_id, connector_id, entity_type, external_id),
  KEY idx_official_site_identity_lookup (connector_id, entity_type, external_id, active),
  KEY idx_official_site_identity_entity (entity_type, entity_id, active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_official_site_review_items (
  review_item_id VARCHAR(96) NOT NULL PRIMARY KEY,
  snapshot_id VARCHAR(96) NOT NULL,
  connector_id VARCHAR(96) NOT NULL,
  entity_type VARCHAR(40) NOT NULL,
  entity_id VARCHAR(96) NOT NULL,
  reason TEXT NOT NULL,
  payload_json MEDIUMTEXT NULL,
  review_status VARCHAR(24) NOT NULL DEFAULT 'pending',
  reviewed_by VARCHAR(191) NULL,
  reviewed_at DATETIME NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_official_site_review_snapshot_entity (snapshot_id, entity_type, entity_id),
  KEY idx_official_site_review_pending (review_status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_official_site_tombstones (
  tombstone_id VARCHAR(96) NOT NULL PRIMARY KEY,
  snapshot_id VARCHAR(96) NOT NULL,
  connector_id VARCHAR(96) NOT NULL,
  entity_type VARCHAR(24) NOT NULL,
  external_id VARCHAR(191) NOT NULL,
  entity_id VARCHAR(96) NULL,
  reason TEXT NOT NULL,
  observed_at DATETIME NOT NULL,
  payload_json MEDIUMTEXT NULL,
  review_status VARCHAR(24) NOT NULL DEFAULT 'pending',
  reviewed_by VARCHAR(191) NULL,
  reviewed_at DATETIME NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_official_site_tombstone_snapshot (snapshot_id, connector_id, entity_type, external_id),
  KEY idx_official_site_tombstone_pending (review_status, observed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Checksums are SHA-256 of
-- `bside-governance-migration-v1:<version>:<migration_name>`.  Versions 1-7
-- receive the one-time baseline because their original schema had no checksum
-- column.  No pre-existing non-NULL value is ever overwritten.
CALL activist_008_record_migration(1, '001_governance_v1', '2f1f03aa62d733339b79b5bca50e1c480b4f706a5823fd3490bd799421e93afd', 1);
CALL activist_008_record_migration(2, '002_legacy_source_right_lineage', 'fdcb2d634a787c7bbe534bd3892470a13aef11254dd75cec1afb54a9f2b61051', 1);
CALL activist_008_record_migration(3, '003_editorial_governance', '906a0071bc11b595eae388a17074bd955f1ebb25f8a7453e3e89534e42ba4f25', 1);
CALL activist_008_record_migration(4, '004_telegram_signal_rebuild_staging', 'de64071e117fae70d6849f8191be7267a885e75bf3d498ab7488fa616348fb7f', 1);
CALL activist_008_record_migration(5, '005_telegram_channel_identity_index', 'cf1245fe562e583707d821f126562a6f10aa9c8db5e0c9b20afa8ff267d1d903', 1);
CALL activist_008_record_migration(6, '006_governance_release_guard', 'f7f7a46f86118316dc21a67bb5b547668d64978b9fe4054b4c86104b85d7ced7', 1);
CALL activist_008_record_migration(7, '007_governance_identity_and_evidence', '074bbb5f066d5f3a20e3b894762ae356fa0a102c61546634fc16be05400f2ebe', 1);
CALL activist_008_record_migration(8, '008_official_site_snapshot_receipts', 'b12e5e5290a5901192ddb4c8ec999719aa3dc25596c6c46d16ac383f3be74376', 0);

CALL activist_008_modify_column('activist_schema_migrations', 'migration_checksum', 'CHAR(64) NOT NULL');

DROP PROCEDURE activist_008_add_column;
DROP PROCEDURE activist_008_modify_column;
DROP PROCEDURE activist_008_drop_index;
DROP PROCEDURE activist_008_add_unique_index;
DROP PROCEDURE activist_008_record_migration;
