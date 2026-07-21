-- Atomic, resumable Telegram issue-signal snapshot rebuilds.
-- The table prefix matches the production default used by prior migrations.

CREATE TABLE IF NOT EXISTS activist_telegram_signal_rebuild_state (
  state_key VARCHAR(16) NOT NULL PRIMARY KEY,
  active_token CHAR(64) NULL,
  started_at DATETIME NULL,
  finalized_token CHAR(64) NULL,
  finalized_at DATETIME NULL,
  live_revision BIGINT UNSIGNED NOT NULL DEFAULT 0,
  updated_at DATETIME NOT NULL,
  KEY idx_signal_rebuild_active (active_token),
  KEY idx_signal_rebuild_finalized (finalized_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_telegram_signal_rebuild_staging (
  rebuild_token CHAR(64) NOT NULL,
  article_id VARCHAR(96) NOT NULL,
  payload_json MEDIUMTEXT NOT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (rebuild_token, article_id),
  KEY idx_signal_rebuild_staging_created (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SET @activist_signal_rebuild_ddl = (
  SELECT IF(
    COUNT(*) = 0,
    'ALTER TABLE activist_telegram_signal_rebuild_state ADD COLUMN live_revision BIGINT UNSIGNED NOT NULL DEFAULT 0',
    'SELECT 1'
  )
  FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'activist_telegram_signal_rebuild_state'
    AND COLUMN_NAME = 'live_revision'
);
PREPARE activist_signal_rebuild_statement FROM @activist_signal_rebuild_ddl;
EXECUTE activist_signal_rebuild_statement;
DEALLOCATE PREPARE activist_signal_rebuild_statement;
SET @activist_signal_rebuild_ddl = NULL;

INSERT IGNORE INTO activist_telegram_signal_rebuild_state (
  state_key, active_token, started_at, finalized_token, finalized_at, live_revision, updated_at
) VALUES ('global', NULL, NULL, NULL, NULL, 0, UTC_TIMESTAMP());
