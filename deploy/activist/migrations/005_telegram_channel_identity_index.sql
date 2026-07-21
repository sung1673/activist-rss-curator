-- Bound canonical Telegram identity lookups after the 365-day history repair.
-- Apply explicitly before deploying the matching PHP code; do not build this
-- large-table index lazily inside an API request.

SET @activist_previous_lock_wait_timeout = @@SESSION.lock_wait_timeout;
SET SESSION lock_wait_timeout = 30;

SET @activist_telegram_identity_version_ddl = (
  SELECT CASE
    WHEN COUNT(*) = 0 THEN
      'ALTER TABLE activist_telegram_channels ADD COLUMN identity_migration_version TINYINT UNSIGNED NOT NULL DEFAULT 0 AFTER telegram_channel_id'
    WHEN COUNT(*) = 1
      AND MAX(DATA_TYPE) = 'tinyint'
      AND MAX(COLUMN_TYPE) = 'tinyint unsigned'
      AND MAX(IS_NULLABLE) = 'NO'
      AND MAX(CAST(COLUMN_DEFAULT AS CHAR)) = '0'
    THEN 'SELECT 1'
    ELSE
      -- Deliberately retry the named ADD so an incompatible existing column
      -- raises duplicate-column instead of being accepted as migrated.
      'ALTER TABLE activist_telegram_channels ADD COLUMN identity_migration_version TINYINT UNSIGNED NOT NULL DEFAULT 0 AFTER telegram_channel_id'
  END
  FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'activist_telegram_channels'
    AND COLUMN_NAME = 'identity_migration_version'
);
PREPARE activist_telegram_identity_version_statement FROM @activist_telegram_identity_version_ddl;
EXECUTE activist_telegram_identity_version_statement;
DEALLOCATE PREPARE activist_telegram_identity_version_statement;
SET @activist_telegram_identity_version_ddl = NULL;

SET @activist_telegram_identity_index_ddl = (
  SELECT CASE
    WHEN COUNT(*) = 0 THEN
      'ALTER TABLE activist_telegram_messages ADD INDEX idx_telegram_channel_message_id (telegram_channel_id, telegram_message_id), ALGORITHM=INPLACE, LOCK=NONE'
    WHEN COUNT(*) = 2
      AND GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) = 'telegram_channel_id,telegram_message_id'
      AND MIN(NON_UNIQUE) = 1
      AND SUM(CASE WHEN SUB_PART IS NULL THEN 1 ELSE 0 END) = 2
    THEN 'SELECT 1'
    ELSE
      -- Deliberately retry the named ADD so a wrong same-name index fails.
      'ALTER TABLE activist_telegram_messages ADD INDEX idx_telegram_channel_message_id (telegram_channel_id, telegram_message_id), ALGORITHM=INPLACE, LOCK=NONE'
  END
  FROM information_schema.STATISTICS
  WHERE TABLE_SCHEMA = DATABASE()
    AND TABLE_NAME = 'activist_telegram_messages'
    AND INDEX_NAME = 'idx_telegram_channel_message_id'
);
PREPARE activist_telegram_identity_index_statement FROM @activist_telegram_identity_index_ddl;
EXECUTE activist_telegram_identity_index_statement;
DEALLOCATE PREPARE activist_telegram_identity_index_statement;
SET @activist_telegram_identity_index_ddl = NULL;
SET SESSION lock_wait_timeout = @activist_previous_lock_wait_timeout;
SET @activist_previous_lock_wait_timeout = NULL;
