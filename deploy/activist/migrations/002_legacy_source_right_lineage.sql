-- Legacy article/story lineage required for fail-closed Telegram redistribution.
-- Safe for both fresh databases and databases that already ran migration 001.

DROP PROCEDURE IF EXISTS activist_add_source_right_lineage;
DELIMITER //
CREATE PROCEDURE activist_add_source_right_lineage()
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'activist_articles'
  ) THEN
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'activist_articles' AND COLUMN_NAME = 'source_right_id'
    ) THEN
      ALTER TABLE activist_articles ADD COLUMN source_right_id VARCHAR(64) NULL;
    END IF;
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'activist_articles' AND INDEX_NAME = 'idx_article_source_right'
    ) THEN
      CREATE INDEX idx_article_source_right ON activist_articles (source_right_id);
    END IF;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'activist_stories'
  ) THEN
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'activist_stories' AND COLUMN_NAME = 'source_right_id'
    ) THEN
      ALTER TABLE activist_stories ADD COLUMN source_right_id VARCHAR(64) NULL;
    END IF;
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'activist_stories' AND INDEX_NAME = 'idx_story_source_right'
    ) THEN
      CREATE INDEX idx_story_source_right ON activist_stories (source_right_id);
    END IF;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'activist_documents'
  ) THEN
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'activist_documents' AND COLUMN_NAME = 'collection_key'
    ) THEN
      ALTER TABLE activist_documents ADD COLUMN collection_key VARCHAR(96) NULL;
    END IF;
    IF NOT EXISTS (
      SELECT 1 FROM information_schema.STATISTICS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'activist_documents' AND INDEX_NAME = 'idx_document_collection'
    ) THEN
      CREATE INDEX idx_document_collection ON activist_documents (company_id, source_class, collection_key, version_no);
    END IF;
  END IF;
END//
DELIMITER ;

CALL activist_add_source_right_lineage();
DROP PROCEDURE activist_add_source_right_lineage;
