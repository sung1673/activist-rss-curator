-- Durable, server-authoritative identity for scheduled official-ingest slots.
-- A workflow run claims the oldest due slot in its cron family. Completion is
-- recorded only after the collection run and its exact raw/ACK counts persist.

CREATE TABLE IF NOT EXISTS activist_official_slot_claim_state (
  pipeline VARCHAR(64) NOT NULL PRIMARY KEY,
  active_from DATETIME NOT NULL,
  epoch_version INT UNSIGNED NOT NULL,
  activated_at DATETIME NOT NULL,
  activation_revision VARCHAR(40) NOT NULL,
  change_reason VARCHAR(500) NOT NULL,
  changed_by VARCHAR(64) NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_official_slot_claim_epochs (
  epoch_id VARCHAR(96) NOT NULL PRIMARY KEY,
  pipeline VARCHAR(64) NOT NULL,
  epoch_version INT UNSIGNED NOT NULL,
  change_type VARCHAR(24) NOT NULL,
  previous_active_from DATETIME NULL,
  active_from DATETIME NOT NULL,
  change_reason VARCHAR(500) NOT NULL,
  code_revision VARCHAR(40) NOT NULL,
  changed_by VARCHAR(64) NOT NULL,
  created_at DATETIME NOT NULL,
  UNIQUE KEY uq_official_slot_epoch_version (pipeline, epoch_version),
  KEY idx_official_slot_epoch_active (pipeline, active_from)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_official_slot_claims (
  claim_id VARCHAR(96) NOT NULL PRIMARY KEY,
  pipeline VARCHAR(64) NOT NULL,
  epoch_version INT UNSIGNED NOT NULL,
  scheduled_slot_at DATETIME NOT NULL,
  event_schedule VARCHAR(64) NOT NULL,
  github_run_id VARCHAR(64) NOT NULL,
  github_run_attempt INT UNSIGNED NOT NULL,
  trigger_created_at DATETIME NOT NULL,
  claimed_at DATETIME NOT NULL,
  next_cadence_slot_at DATETIME NOT NULL,
  trigger_lag_seconds INT UNSIGNED NOT NULL,
  claim_lag_seconds INT UNSIGNED NOT NULL,
  late TINYINT(1) NOT NULL,
  code_revision VARCHAR(40) NOT NULL,
  identity_sha256 CHAR(64) NOT NULL,
  status VARCHAR(24) NOT NULL DEFAULT 'claimed',
  terminal_reason VARCHAR(64) NULL,
  failed_at DATETIME NULL,
  completed_run_id VARCHAR(96) NULL,
  completed_run_attempt INT UNSIGNED NULL,
  completion_raw_count INT UNSIGNED NULL,
  completion_ack_count INT UNSIGNED NULL,
  completion_sha256 CHAR(64) NULL,
  completed_at DATETIME NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_official_slot_pipeline_slot (pipeline, scheduled_slot_at),
  UNIQUE KEY uq_official_slot_pipeline_run (pipeline, github_run_id),
  KEY idx_official_slot_schedule (pipeline, event_schedule, scheduled_slot_at),
  KEY idx_official_slot_status (pipeline, status, scheduled_slot_at),
  KEY idx_official_slot_revision (code_revision, scheduled_slot_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

DROP PROCEDURE IF EXISTS activist_010_record_migration;
DELIMITER $$
CREATE PROCEDURE activist_010_record_migration()
BEGIN
  DECLARE existing_count INT DEFAULT 0;
  DECLARE existing_name VARCHAR(191) DEFAULT NULL;
  DECLARE existing_checksum CHAR(64) DEFAULT NULL;
  DECLARE shape_count INT DEFAULT 0;
  DECLARE shape_valid INT DEFAULT 0;
  DECLARE index_valid INT DEFAULT 0;

  IF (SELECT COUNT(*) FROM activist_schema_migrations WHERE migration_version BETWEEN 1 AND 9) <> 9
     OR EXISTS (
       SELECT 1 FROM activist_schema_migrations
       WHERE migration_version BETWEEN 1 AND 9 AND (
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
         END
       )
     ) THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '010 prerequisite migration manifest incomplete';
  END IF;

  IF (SELECT COUNT(*) FROM information_schema.tables
      WHERE table_schema = DATABASE()
        AND table_name IN ('activist_official_slot_claim_state','activist_official_slot_claim_epochs','activist_official_slot_claims')
        AND engine = 'InnoDB') <> 3 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '010 slot claim table engine mismatch';
  END IF;

  SELECT COUNT(*), COALESCE(SUM(CASE
    WHEN column_name='pipeline' AND column_type='varchar(64)' AND is_nullable='NO' THEN 1
    WHEN column_name='active_from' AND column_type='datetime' AND is_nullable='NO' THEN 1
    WHEN column_name='epoch_version' AND column_type='int unsigned' AND is_nullable='NO' THEN 1
    WHEN column_name='activated_at' AND column_type='datetime' AND is_nullable='NO' THEN 1
    WHEN column_name='activation_revision' AND column_type='varchar(40)' AND is_nullable='NO' THEN 1
    WHEN column_name='change_reason' AND column_type='varchar(500)' AND is_nullable='NO' THEN 1
    WHEN column_name='changed_by' AND column_type='varchar(64)' AND is_nullable='NO' THEN 1
    WHEN column_name='created_at' AND column_type='datetime' AND is_nullable='NO' THEN 1
    WHEN column_name='updated_at' AND column_type='datetime' AND is_nullable='NO' THEN 1
    ELSE 0 END),0) INTO shape_count,shape_valid
  FROM information_schema.columns
  WHERE table_schema=DATABASE() AND table_name='activist_official_slot_claim_state';
  IF shape_count <> 9 OR shape_valid <> 9 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '010 slot claim state column shape mismatch';
  END IF;

  SELECT COUNT(*), COALESCE(SUM(CASE
    WHEN column_name='epoch_id' AND column_type='varchar(96)' AND is_nullable='NO' THEN 1
    WHEN column_name='pipeline' AND column_type='varchar(64)' AND is_nullable='NO' THEN 1
    WHEN column_name='epoch_version' AND column_type='int unsigned' AND is_nullable='NO' THEN 1
    WHEN column_name='change_type' AND column_type='varchar(24)' AND is_nullable='NO' THEN 1
    WHEN column_name='previous_active_from' AND column_type='datetime' AND is_nullable='YES' THEN 1
    WHEN column_name='active_from' AND column_type='datetime' AND is_nullable='NO' THEN 1
    WHEN column_name='change_reason' AND column_type='varchar(500)' AND is_nullable='NO' THEN 1
    WHEN column_name='code_revision' AND column_type='varchar(40)' AND is_nullable='NO' THEN 1
    WHEN column_name='changed_by' AND column_type='varchar(64)' AND is_nullable='NO' THEN 1
    WHEN column_name='created_at' AND column_type='datetime' AND is_nullable='NO' THEN 1
    ELSE 0 END),0) INTO shape_count,shape_valid
  FROM information_schema.columns
  WHERE table_schema=DATABASE() AND table_name='activist_official_slot_claim_epochs';
  IF shape_count <> 10 OR shape_valid <> 10 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '010 slot claim epoch column shape mismatch';
  END IF;

  SELECT COUNT(*), COALESCE(SUM(CASE
    WHEN column_name='claim_id' AND column_type='varchar(96)' AND is_nullable='NO' THEN 1
    WHEN column_name='pipeline' AND column_type='varchar(64)' AND is_nullable='NO' THEN 1
    WHEN column_name='epoch_version' AND column_type='int unsigned' AND is_nullable='NO' THEN 1
    WHEN column_name='scheduled_slot_at' AND column_type='datetime' AND is_nullable='NO' THEN 1
    WHEN column_name='event_schedule' AND column_type='varchar(64)' AND is_nullable='NO' THEN 1
    WHEN column_name='github_run_id' AND column_type='varchar(64)' AND is_nullable='NO' THEN 1
    WHEN column_name='github_run_attempt' AND column_type='int unsigned' AND is_nullable='NO' THEN 1
    WHEN column_name IN ('trigger_created_at','claimed_at','next_cadence_slot_at') AND column_type='datetime' AND is_nullable='NO' THEN 1
    WHEN column_name IN ('trigger_lag_seconds','claim_lag_seconds') AND column_type='int unsigned' AND is_nullable='NO' THEN 1
    WHEN column_name='late' AND column_type='tinyint(1)' AND is_nullable='NO' THEN 1
    WHEN column_name='code_revision' AND column_type='varchar(40)' AND is_nullable='NO' THEN 1
    WHEN column_name='identity_sha256' AND column_type='char(64)' AND is_nullable='NO' THEN 1
    WHEN column_name='status' AND column_type='varchar(24)' AND is_nullable='NO' AND column_default='claimed' THEN 1
    WHEN column_name='terminal_reason' AND column_type='varchar(64)' AND is_nullable='YES' THEN 1
    WHEN column_name='failed_at' AND column_type='datetime' AND is_nullable='YES' THEN 1
    WHEN column_name='completed_run_id' AND column_type='varchar(96)' AND is_nullable='YES' THEN 1
    WHEN column_name='completed_run_attempt' AND column_type='int unsigned' AND is_nullable='YES' THEN 1
    WHEN column_name IN ('completion_raw_count','completion_ack_count') AND column_type='int unsigned' AND is_nullable='YES' THEN 1
    WHEN column_name='completion_sha256' AND column_type='char(64)' AND is_nullable='YES' THEN 1
    WHEN column_name='completed_at' AND column_type='datetime' AND is_nullable='YES' THEN 1
    WHEN column_name IN ('created_at','updated_at') AND column_type='datetime' AND is_nullable='NO' THEN 1
    ELSE 0 END),0) INTO shape_count,shape_valid
  FROM information_schema.columns
  WHERE table_schema=DATABASE() AND table_name='activist_official_slot_claims';
  IF shape_count <> 26 OR shape_valid <> 26 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '010 slot claim ledger column shape mismatch';
  END IF;

  SELECT COUNT(*) INTO index_valid FROM information_schema.statistics
  WHERE table_schema=DATABASE() AND table_name='activist_official_slot_claim_state'
    AND index_name='PRIMARY' AND non_unique=0 AND seq_in_index=1 AND column_name='pipeline';
  IF index_valid <> 1 OR (SELECT COUNT(*) FROM information_schema.statistics
      WHERE table_schema=DATABASE() AND table_name='activist_official_slot_claim_state') <> 1 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '010 slot claim state primary key mismatch';
  END IF;

  SELECT COUNT(*) INTO index_valid FROM information_schema.statistics
  WHERE table_schema=DATABASE() AND table_name='activist_official_slot_claims' AND (
    (index_name='PRIMARY' AND non_unique=0 AND seq_in_index=1 AND column_name='claim_id') OR
    (index_name='uq_official_slot_pipeline_slot' AND non_unique=0 AND seq_in_index=1 AND column_name='pipeline') OR
    (index_name='uq_official_slot_pipeline_slot' AND non_unique=0 AND seq_in_index=2 AND column_name='scheduled_slot_at') OR
    (index_name='uq_official_slot_pipeline_run' AND non_unique=0 AND seq_in_index=1 AND column_name='pipeline') OR
    (index_name='uq_official_slot_pipeline_run' AND non_unique=0 AND seq_in_index=2 AND column_name='github_run_id') OR
    (index_name='idx_official_slot_schedule' AND non_unique=1 AND seq_in_index=1 AND column_name='pipeline') OR
    (index_name='idx_official_slot_schedule' AND non_unique=1 AND seq_in_index=2 AND column_name='event_schedule') OR
    (index_name='idx_official_slot_schedule' AND non_unique=1 AND seq_in_index=3 AND column_name='scheduled_slot_at') OR
    (index_name='idx_official_slot_status' AND non_unique=1 AND seq_in_index=1 AND column_name='pipeline') OR
    (index_name='idx_official_slot_status' AND non_unique=1 AND seq_in_index=2 AND column_name='status') OR
    (index_name='idx_official_slot_status' AND non_unique=1 AND seq_in_index=3 AND column_name='scheduled_slot_at') OR
    (index_name='idx_official_slot_revision' AND non_unique=1 AND seq_in_index=1 AND column_name='code_revision') OR
    (index_name='idx_official_slot_revision' AND non_unique=1 AND seq_in_index=2 AND column_name='scheduled_slot_at'));
  IF index_valid <> 13 OR (SELECT COUNT(*) FROM information_schema.statistics
      WHERE table_schema=DATABASE() AND table_name='activist_official_slot_claims') <> 13 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '010 slot claim ledger index shape mismatch';
  END IF;

  SELECT COUNT(*) INTO index_valid FROM information_schema.statistics
  WHERE table_schema=DATABASE() AND table_name='activist_official_slot_claim_epochs' AND (
    (index_name='PRIMARY' AND non_unique=0 AND seq_in_index=1 AND column_name='epoch_id') OR
    (index_name='uq_official_slot_epoch_version' AND non_unique=0 AND seq_in_index=1 AND column_name='pipeline') OR
    (index_name='uq_official_slot_epoch_version' AND non_unique=0 AND seq_in_index=2 AND column_name='epoch_version') OR
    (index_name='idx_official_slot_epoch_active' AND non_unique=1 AND seq_in_index=1 AND column_name='pipeline') OR
    (index_name='idx_official_slot_epoch_active' AND non_unique=1 AND seq_in_index=2 AND column_name='active_from'));
  IF index_valid <> 5 OR (SELECT COUNT(*) FROM information_schema.statistics
      WHERE table_schema=DATABASE() AND table_name='activist_official_slot_claim_epochs') <> 5 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '010 slot claim epoch index shape mismatch';
  END IF;

  SELECT COUNT(*) INTO existing_count
  FROM activist_schema_migrations
  WHERE migration_version = 10;

  IF existing_count = 0 THEN
    INSERT INTO activist_schema_migrations (
      migration_version, migration_name, migration_checksum, applied_at
    ) VALUES (
      10,
      '010_official_slot_claim_ledger',
      '2b8be6264c8a4f3be038729fbf6bbe22e720457874f02c89c82d33db9dc78f51',
      UTC_TIMESTAMP()
    );
  ELSE
    SELECT migration_name, migration_checksum
      INTO existing_name, existing_checksum
    FROM activist_schema_migrations
    WHERE migration_version = 10
    LIMIT 1;
    IF BINARY existing_name <> BINARY '010_official_slot_claim_ledger'
       OR existing_checksum IS NULL
       OR BINARY existing_checksum <> BINARY '2b8be6264c8a4f3be038729fbf6bbe22e720457874f02c89c82d33db9dc78f51' THEN
      SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '010 migration manifest conflict';
    END IF;
  END IF;
END$$
DELIMITER ;

CALL activist_010_record_migration();
DROP PROCEDURE activist_010_record_migration;
