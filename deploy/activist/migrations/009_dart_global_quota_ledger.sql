-- Global, server-authoritative OpenDART request quota ledger.
-- Every physical HTTP attempt is consumed before the outbound request; an
-- OpenDART 020 response blocks the same KST day for every workflow.

CREATE TABLE IF NOT EXISTS activist_dart_quota_days (
  quota_day DATE NOT NULL PRIMARY KEY,
  limit_count INT UNSIGNED NOT NULL DEFAULT 10000,
  used_count INT UNSIGNED NOT NULL DEFAULT 0,
  blocked TINYINT(1) NOT NULL DEFAULT 0,
  block_reason VARCHAR(64) NULL,
  blocked_until DATETIME NULL,
  blocked_by_attempt_id VARCHAR(96) NULL,
  blocked_at DATETIME NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_dart_quota_blocked (blocked, blocked_until)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_dart_quota_attempts (
  attempt_id VARCHAR(96) NOT NULL PRIMARY KEY,
  quota_day DATE NOT NULL,
  operation VARCHAR(24) NOT NULL,
  code_revision VARCHAR(40) NOT NULL,
  consume_request_sha256 CHAR(64) NOT NULL,
  block_request_sha256 CHAR(64) NULL,
  status VARCHAR(24) NOT NULL DEFAULT 'consumed',
  consumed_units TINYINT UNSIGNED NOT NULL DEFAULT 1,
  consumed_at DATETIME NOT NULL,
  blocked_at DATETIME NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_dart_quota_attempt_day (quota_day, attempt_id),
  KEY idx_dart_quota_attempt_day_status (quota_day, status, consumed_at),
  KEY idx_dart_quota_attempt_revision (code_revision, consumed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP PROCEDURE IF EXISTS activist_009_record_migration;
DELIMITER $$
CREATE PROCEDURE activist_009_record_migration()
BEGIN
  DECLARE existing_count INT DEFAULT 0;
  DECLARE existing_name VARCHAR(191) DEFAULT NULL;
  DECLARE existing_checksum CHAR(64) DEFAULT NULL;

  IF (SELECT COUNT(*) FROM activist_schema_migrations WHERE migration_version BETWEEN 1 AND 8) <> 8
     OR EXISTS (
       SELECT 1 FROM activist_schema_migrations
       WHERE migration_version BETWEEN 1 AND 8 AND (
         migration_name <> CASE migration_version
           WHEN 1 THEN '001_governance_v1'
           WHEN 2 THEN '002_legacy_source_right_lineage'
           WHEN 3 THEN '003_editorial_governance'
           WHEN 4 THEN '004_telegram_signal_rebuild_staging'
           WHEN 5 THEN '005_telegram_channel_identity_index'
           WHEN 6 THEN '006_governance_release_guard'
           WHEN 7 THEN '007_governance_identity_and_evidence'
           WHEN 8 THEN '008_official_site_snapshot_receipts'
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
         END
       )
     ) THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '009 prerequisite migration manifest incomplete';
  END IF;

  SELECT COUNT(*) INTO existing_count
  FROM activist_schema_migrations
  WHERE migration_version = 9;

  IF existing_count = 0 THEN
    INSERT INTO activist_schema_migrations (
      migration_version, migration_name, migration_checksum, applied_at
    ) VALUES (
      9,
      '009_dart_global_quota_ledger',
      '9e60867847b7cc2b7d9166c73e395ae872d12a4e91aa62457049468017e5f94d',
      UTC_TIMESTAMP()
    );
  ELSE
    SELECT migration_name, migration_checksum
      INTO existing_name, existing_checksum
    FROM activist_schema_migrations
    WHERE migration_version = 9
    LIMIT 1;
    IF BINARY existing_name <> BINARY '009_dart_global_quota_ledger'
       OR existing_checksum IS NULL
       OR BINARY existing_checksum <> BINARY '9e60867847b7cc2b7d9166c73e395ae872d12a4e91aa62457049468017e5f94d' THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '009 migration manifest conflict';
    END IF;
  END IF;
END$$
DELIMITER ;

CALL activist_009_record_migration();
DROP PROCEDURE activist_009_record_migration;
