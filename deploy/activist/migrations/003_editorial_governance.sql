-- Existing-install upgrade for governed editorial entities.
-- Fresh installs receive the same definitions from 001_governance_v1.sql.

CREATE TABLE IF NOT EXISTS activist_campaign_documents (
  campaign_id VARCHAR(96) NOT NULL,
  document_id VARCHAR(96) NOT NULL,
  relation_type VARCHAR(40) NOT NULL DEFAULT 'evidence',
  position_no INT NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL,
  PRIMARY KEY (campaign_id, document_id, relation_type),
  KEY idx_campaign_document_document (document_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_editorial_ingest_chunks (
  chunk_id VARCHAR(96) NOT NULL PRIMARY KEY,
  bundle_sha256 CHAR(64) NOT NULL,
  chunk_index INT NOT NULL,
  chunk_count INT NOT NULL,
  entity_type VARCHAR(40) NOT NULL,
  payload_sha256 CHAR(64) NOT NULL,
  accepted_json TEXT NOT NULL,
  created_at DATETIME NOT NULL,
  UNIQUE KEY uq_editorial_bundle_chunk (bundle_sha256, chunk_index),
  KEY idx_editorial_chunk_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP PROCEDURE IF EXISTS activist_add_editorial_column;
DROP PROCEDURE IF EXISTS activist_add_editorial_index;
DELIMITER $$
CREATE PROCEDURE activist_add_editorial_column(IN table_name_value VARCHAR(64), IN column_name_value VARCHAR(64), IN definition_value VARCHAR(255))
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

CREATE PROCEDURE activist_add_editorial_index(IN table_name_value VARCHAR(64), IN index_name_value VARCHAR(64), IN columns_value VARCHAR(255))
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = table_name_value AND INDEX_NAME = index_name_value
  ) THEN
    SET @ddl = CONCAT('CREATE INDEX `', index_name_value, '` ON `', table_name_value, '` (', columns_value, ')');
    PREPARE statement_value FROM @ddl;
    EXECUTE statement_value;
    DEALLOCATE PREPARE statement_value;
  END IF;
END$$
DELIMITER ;

CALL activist_add_editorial_column('activist_actors', 'review_status', 'VARCHAR(24) NOT NULL DEFAULT ''pending''');
CALL activist_add_editorial_column('activist_event_actors', 'review_status', 'VARCHAR(24) NOT NULL DEFAULT ''pending''');
CALL activist_add_editorial_column('activist_event_actors', 'updated_at', 'DATETIME NULL');
CALL activist_add_editorial_column('activist_proposal_votes', 'review_status', 'VARCHAR(24) NOT NULL DEFAULT ''pending''');
CALL activist_add_editorial_column('activist_commitment_outcomes', 'review_status', 'VARCHAR(24) NOT NULL DEFAULT ''pending''');
CALL activist_add_editorial_column('activist_timeline_entries', 'review_status', 'VARCHAR(24) NOT NULL DEFAULT ''pending''');
CALL activist_add_editorial_column('activist_editorial_ingest_chunks', 'payload_sha256', 'CHAR(64) NULL');

CALL activist_add_editorial_index('activist_actors', 'idx_actor_review', 'review_status, updated_at');
CALL activist_add_editorial_index('activist_event_actors', 'idx_event_actor_review', 'review_status, updated_at');
CALL activist_add_editorial_index('activist_proposal_votes', 'idx_vote_review', 'review_status, meeting_at');
CALL activist_add_editorial_index('activist_commitment_outcomes', 'idx_commitment_review', 'review_status, target_at');
CALL activist_add_editorial_index('activist_timeline_entries', 'idx_timeline_review', 'review_status, occurred_at');

ALTER TABLE activist_actors MODIFY record_status VARCHAR(24) NOT NULL DEFAULT 'inactive';
UPDATE activist_event_actors SET updated_at = created_at WHERE updated_at IS NULL;
ALTER TABLE activist_event_actors MODIFY updated_at DATETIME NOT NULL;
DELETE FROM activist_editorial_ingest_chunks WHERE payload_sha256 IS NULL;
ALTER TABLE activist_editorial_ingest_chunks MODIFY payload_sha256 CHAR(64) NOT NULL;

DROP PROCEDURE activist_add_editorial_column;
DROP PROCEDURE activist_add_editorial_index;
