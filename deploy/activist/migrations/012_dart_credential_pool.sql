-- OpenDART credential-pool quota ledger.
--
-- Migration 009 counted physical attempts globally and treated status 020 as a
-- global day block. This additive migration retains that authoritative global
-- count, raises its KST-day hard ceiling to 40,000, and adds a credential
-- dimension. Existing attempts are mapped to the explicit legacy-single
-- sentinel. New calls must use the full lowercase SHA-256 of the API-key bytes
-- as their non-secret credential_id.
--
-- The caller must prepend this exact file, in the same MySQL input stream, with:
--   SET @bside_migration_012_sha256 = '<sha256-of-these-file-bytes>';

DROP PROCEDURE IF EXISTS activist_012_preflight;
DELIMITER $$
CREATE PROCEDURE activist_012_preflight()
BEGIN
  DECLARE existing_v12_count INT DEFAULT 0;

  IF @bside_migration_012_sha256 IS NULL
     OR CAST(@bside_migration_012_sha256 AS BINARY)
       NOT REGEXP BINARY '^[0-9a-f]{64}$' THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '012 source byte checksum missing or invalid';
  END IF;

  IF (SELECT COUNT(*) FROM activist_schema_migrations
      WHERE migration_version BETWEEN 1 AND 11) <> 11
     OR EXISTS (
       SELECT 1 FROM activist_schema_migrations
       WHERE migration_version BETWEEN 1 AND 11 AND (
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
           WHEN 11 THEN '011_global_terminal_v2'
         END
         OR migration_checksum IS NULL
         OR BINARY migration_checksum NOT REGEXP BINARY '^[0-9a-f]{64}$'
         OR (
           migration_version <= 10
           AND BINARY migration_checksum <> BINARY CASE migration_version
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
       )
     ) THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '012 prerequisite migration manifest incomplete';
  END IF;

  SELECT COUNT(*) INTO existing_v12_count
  FROM activist_schema_migrations
  WHERE migration_version=12;
  IF existing_v12_count > 1
     OR EXISTS (
       SELECT 1 FROM activist_schema_migrations
       WHERE migration_version=12
         AND (
           BINARY migration_name <> BINARY '012_dart_credential_pool'
           OR BINARY migration_checksum <>
             BINARY @bside_migration_012_sha256
         )
     ) THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '012 migration manifest conflict';
  END IF;

  SET @bside_012_first_install = IF(existing_v12_count=0,1,0);

  IF @bside_012_first_install=1 THEN
    IF EXISTS (
      SELECT 1 FROM activist_dart_quota_days
      WHERE limit_count NOT IN (10000,40000)
         OR used_count > 40000
    ) OR EXISTS (
      SELECT 1
      FROM activist_dart_quota_attempts a
      LEFT JOIN activist_dart_quota_days d ON d.quota_day=a.quota_day
      WHERE d.quota_day IS NULL
         OR a.consumed_units<>1
    ) OR EXISTS (
      SELECT 1
      FROM activist_dart_quota_days d
      LEFT JOIN (
        SELECT quota_day,SUM(consumed_units) AS attempt_units
        FROM activist_dart_quota_attempts
        GROUP BY quota_day
      ) a ON a.quota_day=d.quota_day
      WHERE d.used_count<>COALESCE(a.attempt_units,0)
    ) THEN
      SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = '012 legacy DART quota ledger integrity failure';
    END IF;
  END IF;
END$$
DELIMITER ;
CALL activist_012_preflight();
DROP PROCEDURE activist_012_preflight;

DROP PROCEDURE IF EXISTS activist_012_add_column;
DROP PROCEDURE IF EXISTS activist_012_add_index;
DELIMITER $$
CREATE PROCEDURE activist_012_add_column(
  IN table_name_value VARCHAR(64),
  IN column_name_value VARCHAR(64),
  IN definition_value VARCHAR(500)
)
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema=DATABASE()
      AND table_name=table_name_value
      AND column_name=column_name_value
  ) THEN
    SET @bside_012_ddl = CONCAT(
      'ALTER TABLE `', table_name_value, '` ADD COLUMN `',
      column_name_value, '` ', definition_value
    );
    PREPARE bside_012_statement FROM @bside_012_ddl;
    EXECUTE bside_012_statement;
    DEALLOCATE PREPARE bside_012_statement;
  END IF;
END$$

CREATE PROCEDURE activist_012_add_index(
  IN table_name_value VARCHAR(64),
  IN index_name_value VARCHAR(64),
  IN definition_value VARCHAR(500)
)
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.statistics
    WHERE table_schema=DATABASE()
      AND table_name=table_name_value
      AND index_name=index_name_value
  ) THEN
    SET @bside_012_ddl = CONCAT(
      'ALTER TABLE `', table_name_value, '` ADD ', definition_value
    );
    PREPARE bside_012_statement FROM @bside_012_ddl;
    EXECUTE bside_012_statement;
    DEALLOCATE PREPARE bside_012_statement;
  END IF;
END$$
DELIMITER ;

CALL activist_012_add_column(
  'activist_dart_quota_attempts',
  'credential_id',
  'VARCHAR(64) NOT NULL DEFAULT ''legacy-single'' AFTER `quota_day`'
);
CALL activist_012_add_column(
  'activist_dart_quota_attempts',
  'disable_request_sha256',
  'CHAR(64) NULL AFTER `block_request_sha256`'
);
CALL activist_012_add_column(
  'activist_dart_quota_attempts',
  'disabled_at',
  'DATETIME NULL AFTER `blocked_at`'
);
CALL activist_012_add_index(
  'activist_dart_quota_attempts',
  'idx_dart_quota_attempt_credential_day',
  'KEY `idx_dart_quota_attempt_credential_day` (`credential_id`,`quota_day`,`consumed_at`)'
);
DROP PROCEDURE activist_012_add_index;
DROP PROCEDURE activist_012_add_column;

CREATE TABLE IF NOT EXISTS activist_dart_quota_credentials (
  credential_id VARCHAR(64) NOT NULL PRIMARY KEY,
  status VARCHAR(24) NOT NULL DEFAULT 'active',
  disable_reason VARCHAR(64) NULL,
  disabled_by_attempt_id VARCHAR(96) NULL,
  disabled_at DATETIME NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_dart_quota_credential_status (status, updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_dart_quota_credential_days (
  quota_day DATE NOT NULL,
  credential_id VARCHAR(64) NOT NULL,
  limit_count INT UNSIGNED NOT NULL DEFAULT 40000,
  used_count INT UNSIGNED NOT NULL DEFAULT 0,
  blocked TINYINT(1) NOT NULL DEFAULT 0,
  block_reason VARCHAR(64) NULL,
  blocked_until DATETIME NULL,
  blocked_by_attempt_id VARCHAR(96) NULL,
  blocked_at DATETIME NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (quota_day, credential_id),
  KEY idx_dart_quota_credential_blocked
    (credential_id, blocked, blocked_until),
  KEY idx_dart_quota_credential_day_usage
    (quota_day, used_count)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

INSERT INTO activist_dart_quota_credentials (
  credential_id,status,disable_reason,disabled_by_attempt_id,disabled_at,
  created_at,updated_at
)
SELECT 'legacy-single','legacy',NULL,NULL,NULL,
       COALESCE(MIN(consumed_at),UTC_TIMESTAMP()),UTC_TIMESTAMP()
FROM activist_dart_quota_attempts
HAVING COUNT(*)>0
ON DUPLICATE KEY UPDATE
  status=IF(status='legacy','legacy',status),
  updated_at=updated_at;

INSERT INTO activist_dart_quota_credential_days (
  quota_day,credential_id,limit_count,used_count,blocked,block_reason,
  blocked_until,blocked_by_attempt_id,blocked_at,created_at,updated_at
)
SELECT quota_day,'legacy-single',40000,used_count,blocked,block_reason,
       blocked_until,blocked_by_attempt_id,blocked_at,created_at,updated_at
FROM activist_dart_quota_days
WHERE @bside_012_first_install=1
  AND (used_count>0 OR blocked=1)
ON DUPLICATE KEY UPDATE
  limit_count=40000,
  used_count=GREATEST(
    activist_dart_quota_credential_days.used_count,VALUES(used_count)
  ),
  blocked=GREATEST(
    activist_dart_quota_credential_days.blocked,VALUES(blocked)
  ),
  block_reason=COALESCE(
    activist_dart_quota_credential_days.block_reason,VALUES(block_reason)
  ),
  blocked_until=COALESCE(
    activist_dart_quota_credential_days.blocked_until,VALUES(blocked_until)
  ),
  blocked_by_attempt_id=COALESCE(
    activist_dart_quota_credential_days.blocked_by_attempt_id,
    VALUES(blocked_by_attempt_id)
  ),
  blocked_at=COALESCE(
    activist_dart_quota_credential_days.blocked_at,VALUES(blocked_at)
  ),
  updated_at=GREATEST(
    activist_dart_quota_credential_days.updated_at,VALUES(updated_at)
  );

ALTER TABLE activist_dart_quota_days
  MODIFY COLUMN limit_count INT UNSIGNED NOT NULL DEFAULT 40000;

UPDATE activist_dart_quota_days
SET limit_count=40000,
    blocked=0,
    block_reason=NULL,
    blocked_until=NULL,
    blocked_by_attempt_id=NULL,
    blocked_at=NULL,
    updated_at=UTC_TIMESTAMP()
WHERE @bside_012_first_install=1;

DROP PROCEDURE IF EXISTS activist_012_validate_and_record;
DELIMITER $$
CREATE PROCEDURE activist_012_validate_and_record()
BEGIN
  DECLARE existing_count INT DEFAULT 0;
  DECLARE existing_name VARCHAR(191) DEFAULT NULL;
  DECLARE existing_checksum CHAR(64) DEFAULT NULL;
  DECLARE shape_count INT DEFAULT 0;
  DECLARE shape_valid INT DEFAULT 0;

  SELECT COUNT(*),COALESCE(SUM(CASE
    WHEN column_name='credential_id'
      AND column_type='varchar(64)' AND is_nullable='NO'
      AND column_default='legacy-single' THEN 1
    WHEN column_name='disable_request_sha256'
      AND column_type='char(64)' AND is_nullable='YES' THEN 1
    WHEN column_name='disabled_at'
      AND column_type='datetime' AND is_nullable='YES' THEN 1
    ELSE 0 END),0)
    INTO shape_count,shape_valid
  FROM information_schema.columns
  WHERE table_schema=DATABASE()
    AND table_name='activist_dart_quota_attempts'
    AND column_name IN (
      'credential_id','disable_request_sha256','disabled_at'
    );
  IF shape_count<>3 OR shape_valid<>3 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '012 DART attempt credential shape mismatch';
  END IF;

  IF (SELECT COUNT(*) FROM information_schema.columns
      WHERE table_schema=DATABASE()
        AND table_name='activist_dart_quota_credentials')<>7
     OR (SELECT COUNT(*) FROM information_schema.columns
      WHERE table_schema=DATABASE()
        AND table_name='activist_dart_quota_credential_days')<>11
     OR EXISTS (
       SELECT 1 FROM activist_dart_quota_days
       WHERE limit_count<>40000 OR used_count>40000 OR blocked<>0
          OR block_reason IS NOT NULL OR blocked_until IS NOT NULL
          OR blocked_by_attempt_id IS NOT NULL OR blocked_at IS NOT NULL
     )
     OR EXISTS (
       SELECT 1 FROM activist_dart_quota_attempts
       WHERE credential_id=''
          OR (
            credential_id<>'legacy-single'
            AND BINARY credential_id NOT REGEXP BINARY '^[0-9a-f]{64}$'
          )
     )
     OR EXISTS (
       SELECT 1
       FROM activist_dart_quota_days d
       LEFT JOIN (
         SELECT quota_day,SUM(consumed_units) AS attempt_units
         FROM activist_dart_quota_attempts
         GROUP BY quota_day
       ) a ON a.quota_day=d.quota_day
       WHERE d.used_count<>COALESCE(a.attempt_units,0)
     )
     OR EXISTS (
       SELECT 1
       FROM activist_dart_quota_credential_days cd
       LEFT JOIN (
         SELECT quota_day,credential_id,SUM(consumed_units) AS attempt_units
         FROM activist_dart_quota_attempts
         GROUP BY quota_day,credential_id
       ) a ON a.quota_day=cd.quota_day
          AND a.credential_id=cd.credential_id
       WHERE a.credential_id IS NULL
          OR cd.used_count<>a.attempt_units
     )
     OR EXISTS (
       SELECT 1
       FROM (
         SELECT quota_day,credential_id,SUM(consumed_units) AS attempt_units
         FROM activist_dart_quota_attempts
         GROUP BY quota_day,credential_id
       ) a
       LEFT JOIN activist_dart_quota_credential_days cd
         ON cd.quota_day=a.quota_day
        AND cd.credential_id=a.credential_id
       WHERE cd.credential_id IS NULL
     ) THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = '012 DART credential pool integrity failure';
  END IF;

  SELECT COUNT(*) INTO existing_count
  FROM activist_schema_migrations
  WHERE migration_version=12;
  IF existing_count=0 THEN
    INSERT INTO activist_schema_migrations (
      migration_version,migration_name,migration_checksum,applied_at
    ) VALUES (
      12,
      '012_dart_credential_pool',
      @bside_migration_012_sha256,
      UTC_TIMESTAMP()
    );
  ELSE
    SELECT migration_name,migration_checksum
      INTO existing_name,existing_checksum
    FROM activist_schema_migrations
    WHERE migration_version=12
    LIMIT 1;
    IF BINARY existing_name<>BINARY '012_dart_credential_pool'
       OR BINARY existing_checksum<>
         BINARY @bside_migration_012_sha256 THEN
      SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = '012 migration manifest conflict';
    END IF;
  END IF;
END$$
DELIMITER ;
CALL activist_012_validate_and_record();
DROP PROCEDURE activist_012_validate_and_record;
SET @bside_012_first_install = NULL;
