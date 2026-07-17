-- BSIDE Governance Intelligence schema v1
-- MySQL 5.7+/MariaDB 10.3+, utf8mb4. The default API table prefix is activist_.
-- Deploy with a schema owner account before switching readers to /api/v1.

CREATE TABLE IF NOT EXISTS activist_companies (
  company_id CHAR(8) NOT NULL PRIMARY KEY,
  stock_code VARCHAR(12) NULL,
  market VARCHAR(40) NULL,
  legal_name VARCHAR(255) NOT NULL,
  legal_name_en VARCHAR(255) NULL,
  short_name VARCHAR(255) NULL,
  aliases_json TEXT NULL,
  homepage_url TEXT NULL,
  record_status VARCHAR(24) NOT NULL DEFAULT 'active',
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_company_stock_code (stock_code),
  KEY idx_company_name (legal_name),
  KEY idx_company_market_status (market, record_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_actors (
  actor_id VARCHAR(64) NOT NULL PRIMARY KEY,
  actor_type VARCHAR(40) NOT NULL,
  display_name VARCHAR(255) NOT NULL,
  display_name_en VARCHAR(255) NULL,
  company_id CHAR(8) NULL,
  country_code CHAR(2) NULL,
  aliases_json TEXT NULL,
  homepage_url TEXT NULL,
  review_status VARCHAR(24) NOT NULL DEFAULT 'pending',
  record_status VARCHAR(24) NOT NULL DEFAULT 'inactive',
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_actor_name (display_name),
  KEY idx_actor_review (review_status, updated_at),
  KEY idx_actor_type_status (actor_type, record_status),
  KEY idx_actor_company (company_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_source_rights (
  source_right_id VARCHAR(64) NOT NULL PRIMARY KEY,
  source_type VARCHAR(40) NOT NULL,
  source_key VARCHAR(191) NOT NULL,
  source_name VARCHAR(255) NOT NULL,
  permission_scope TEXT NOT NULL,
  evidence_uri TEXT NULL,
  evidence_hash CHAR(64) NULL,
  valid_from DATETIME NOT NULL,
  valid_until DATETIME NULL,
  revoked_at DATETIME NULL,
  ai_allowed TINYINT(1) NOT NULL DEFAULT 0,
  redistribution_allowed TINYINT(1) NOT NULL DEFAULT 0,
  status VARCHAR(24) NOT NULL DEFAULT 'pending',
  notes TEXT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_source_right_key (source_type, source_key),
  KEY idx_source_right_validity (status, valid_from, valid_until, revoked_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_documents (
  document_id VARCHAR(96) NOT NULL PRIMARY KEY,
  company_id CHAR(8) NULL,
  source_right_id VARCHAR(64) NULL,
  source_class VARCHAR(40) NOT NULL,
  external_id VARCHAR(191) NOT NULL,
  document_type VARCHAR(80) NULL,
  original_language VARCHAR(16) NOT NULL,
  title VARCHAR(700) NOT NULL,
  body_text MEDIUMTEXT NULL,
  original_url TEXT NOT NULL,
  content_hash CHAR(64) NOT NULL,
  collection_key VARCHAR(96) NULL,
  correction_of_document_id VARCHAR(96) NULL,
  version_no INT NOT NULL DEFAULT 1,
  published_at DATETIME NULL,
  retrieved_at DATETIME NOT NULL,
  verification_status VARCHAR(24) NOT NULL DEFAULT 'unverified',
  publication_status VARCHAR(24) NOT NULL DEFAULT 'draft',
  payload_json MEDIUMTEXT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_document_external_version (source_class, external_id, version_no),
  KEY idx_document_company_published (company_id, published_at),
  KEY idx_document_source_right (source_right_id),
  KEY idx_document_collection (company_id, source_class, collection_key, version_no),
  KEY idx_document_correction (correction_of_document_id),
  KEY idx_document_publication (publication_status, published_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_governance_events (
  event_id VARCHAR(96) NOT NULL PRIMARY KEY,
  company_id CHAR(8) NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  title VARCHAR(700) NOT NULL,
  original_language VARCHAR(16) NOT NULL DEFAULT 'ko',
  summary MEDIUMTEXT NULL,
  occurred_at DATETIME NOT NULL,
  deadline_at DATETIME NULL,
  importance VARCHAR(24) NOT NULL DEFAULT 'medium',
  verification_status VARCHAR(24) NOT NULL DEFAULT 'signal',
  review_status VARCHAR(24) NOT NULL DEFAULT 'pending',
  publication_status VARCHAR(24) NOT NULL DEFAULT 'draft',
  collection_key VARCHAR(96) NULL,
  payload_json MEDIUMTEXT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_event_company_occurred (company_id, occurred_at),
  KEY idx_event_type_occurred (event_type, occurred_at),
  KEY idx_event_deadline (deadline_at),
  KEY idx_event_public (publication_status, occurred_at),
  KEY idx_event_review (review_status, importance, occurred_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_campaigns (
  campaign_id VARCHAR(96) NOT NULL PRIMARY KEY,
  company_id CHAR(8) NOT NULL,
  lead_actor_id VARCHAR(64) NULL,
  title VARCHAR(700) NOT NULL,
  original_language VARCHAR(16) NOT NULL DEFAULT 'ko',
  demand_text MEDIUMTEXT NULL,
  stage VARCHAR(40) NOT NULL,
  outcome VARCHAR(40) NULL,
  started_at DATETIME NOT NULL,
  ended_at DATETIME NULL,
  review_status VARCHAR(24) NOT NULL DEFAULT 'pending',
  publication_status VARCHAR(24) NOT NULL DEFAULT 'draft',
  payload_json MEDIUMTEXT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_campaign_company_stage (company_id, stage),
  KEY idx_campaign_actor (lead_actor_id),
  KEY idx_campaign_public (publication_status, started_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_event_documents (
  event_id VARCHAR(96) NOT NULL,
  document_id VARCHAR(96) NOT NULL,
  relation_type VARCHAR(40) NOT NULL DEFAULT 'evidence',
  position_no INT NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL,
  PRIMARY KEY (event_id, document_id, relation_type),
  KEY idx_event_document_document (document_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_campaign_documents (
  campaign_id VARCHAR(96) NOT NULL,
  document_id VARCHAR(96) NOT NULL,
  relation_type VARCHAR(40) NOT NULL DEFAULT 'evidence',
  position_no INT NOT NULL DEFAULT 0,
  created_at DATETIME NOT NULL,
  PRIMARY KEY (campaign_id, document_id, relation_type),
  KEY idx_campaign_document_document (document_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_event_actors (
  event_id VARCHAR(96) NOT NULL,
  actor_id VARCHAR(64) NOT NULL,
  actor_role VARCHAR(40) NOT NULL,
  review_status VARCHAR(24) NOT NULL DEFAULT 'pending',
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  PRIMARY KEY (event_id, actor_id, actor_role),
  KEY idx_event_actor_review (review_status, updated_at),
  KEY idx_event_actor_actor (actor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_claim_evidence (
  claim_id VARCHAR(96) NOT NULL PRIMARY KEY,
  event_id VARCHAR(96) NOT NULL,
  campaign_id VARCHAR(96) NULL,
  actor_id VARCHAR(64) NULL,
  document_id VARCHAR(96) NOT NULL,
  claim_type VARCHAR(40) NOT NULL,
  claim_text MEDIUMTEXT NOT NULL,
  original_language VARCHAR(16) NOT NULL,
  evidence_locator VARCHAR(500) NULL,
  editorial_status VARCHAR(24) NOT NULL DEFAULT 'pending',
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_claim_event_type (event_id, claim_type),
  KEY idx_claim_campaign (campaign_id),
  KEY idx_claim_document (document_id),
  KEY idx_claim_editorial (editorial_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_proposal_votes (
  proposal_vote_id VARCHAR(96) NOT NULL PRIMARY KEY,
  event_id VARCHAR(96) NULL,
  campaign_id VARCHAR(96) NULL,
  company_id CHAR(8) NOT NULL,
  proposer_actor_id VARCHAR(64) NULL,
  agenda_no VARCHAR(40) NULL,
  agenda_title VARCHAR(700) NOT NULL,
  original_language VARCHAR(16) NOT NULL DEFAULT 'ko',
  meeting_at DATETIME NOT NULL,
  recommendation VARCHAR(40) NULL,
  recommendation_source VARCHAR(255) NULL,
  result VARCHAR(24) NOT NULL DEFAULT 'pending',
  votes_for DECIMAL(7,4) NULL,
  votes_against DECIMAL(7,4) NULL,
  votes_abstain DECIMAL(7,4) NULL,
  evidence_document_id VARCHAR(96) NULL,
  review_status VARCHAR(24) NOT NULL DEFAULT 'pending',
  publication_status VARCHAR(24) NOT NULL DEFAULT 'draft',
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_vote_company_meeting (company_id, meeting_at),
  KEY idx_vote_event (event_id),
  KEY idx_vote_campaign (campaign_id),
  KEY idx_vote_review (review_status, meeting_at),
  KEY idx_vote_public (publication_status, meeting_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_commitment_outcomes (
  commitment_id VARCHAR(96) NOT NULL PRIMARY KEY,
  event_id VARCHAR(96) NULL,
  campaign_id VARCHAR(96) NULL,
  company_id CHAR(8) NOT NULL,
  commitment_text MEDIUMTEXT NOT NULL,
  original_language VARCHAR(16) NOT NULL DEFAULT 'ko',
  target_at DATETIME NULL,
  actual_action MEDIUMTEXT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'announced',
  target_metrics_json TEXT NULL,
  actual_metrics_json TEXT NULL,
  evidence_document_id VARCHAR(96) NULL,
  review_status VARCHAR(24) NOT NULL DEFAULT 'pending',
  publication_status VARCHAR(24) NOT NULL DEFAULT 'draft',
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_commitment_company_status (company_id, status),
  KEY idx_commitment_target (target_at),
  KEY idx_commitment_review (review_status, target_at),
  KEY idx_commitment_public (publication_status, target_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_timeline_entries (
  timeline_entry_id VARCHAR(96) NOT NULL PRIMARY KEY,
  event_id VARCHAR(96) NULL,
  campaign_id VARCHAR(96) NULL,
  document_id VARCHAR(96) NULL,
  occurred_at DATETIME NOT NULL,
  entry_type VARCHAR(40) NOT NULL,
  title VARCHAR(700) NOT NULL,
  description MEDIUMTEXT NULL,
  original_language VARCHAR(16) NOT NULL DEFAULT 'ko',
  review_status VARCHAR(24) NOT NULL DEFAULT 'pending',
  publication_status VARCHAR(24) NOT NULL DEFAULT 'draft',
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_timeline_event_time (event_id, occurred_at),
  KEY idx_timeline_campaign_time (campaign_id, occurred_at),
  KEY idx_timeline_review (review_status, occurred_at),
  KEY idx_timeline_public_time (publication_status, occurred_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_editorial_revisions (
  revision_id VARCHAR(96) NOT NULL PRIMARY KEY,
  entity_type VARCHAR(40) NOT NULL,
  entity_id VARCHAR(96) NOT NULL,
  field_name VARCHAR(80) NULL,
  previous_value MEDIUMTEXT NULL,
  revised_value MEDIUMTEXT NULL,
  reason TEXT NOT NULL,
  revision_status VARCHAR(24) NOT NULL DEFAULT 'pending',
  requested_by VARCHAR(191) NULL,
  reviewed_by VARCHAR(191) NULL,
  reviewed_at DATETIME NULL,
  published_at DATETIME NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_revision_entity (entity_type, entity_id),
  KEY idx_revision_status_created (revision_status, created_at)
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

CREATE TABLE IF NOT EXISTS activist_delivery_outbox (
  delivery_id VARCHAR(96) NOT NULL PRIMARY KEY,
  event_id VARCHAR(96) NULL,
  delivery_channel VARCHAR(40) NOT NULL,
  destination VARCHAR(191) NOT NULL,
  idempotency_key VARCHAR(191) NOT NULL,
  payload_json MEDIUMTEXT NOT NULL,
  status VARCHAR(24) NOT NULL DEFAULT 'pending',
  attempt_count INT NOT NULL DEFAULT 0,
  next_attempt_at DATETIME NULL,
  lease_token VARCHAR(64) NULL,
  locked_by VARCHAR(96) NULL,
  locked_at DATETIME NULL,
  lease_expires_at DATETIME NULL,
  external_message_id VARCHAR(191) NULL,
  last_error TEXT NULL,
  delivered_at DATETIME NULL,
  dead_lettered_at DATETIME NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_delivery_idempotency (delivery_channel, destination, idempotency_key),
  KEY idx_delivery_ready (status, next_attempt_at),
  KEY idx_delivery_lease (status, lease_expires_at),
  KEY idx_delivery_event (event_id),
  KEY idx_delivery_dead_letter (dead_lettered_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_feedback (
  feedback_id VARCHAR(64) NOT NULL PRIMARY KEY,
  feedback_type VARCHAR(32) NOT NULL,
  entity_type VARCHAR(40) NULL,
  entity_id VARCHAR(96) NULL,
  submitter_name VARCHAR(191) NULL,
  submitter_contact VARCHAR(320) NULL,
  message TEXT NOT NULL,
  evidence_urls_json TEXT NULL,
  status VARCHAR(24) NOT NULL DEFAULT 'pending',
  is_public TINYINT(1) NOT NULL DEFAULT 0,
  review_note TEXT NULL,
  reviewed_by VARCHAR(191) NULL,
  reviewed_at DATETIME NULL,
  ip_hash CHAR(64) NOT NULL,
  user_agent_hash CHAR(64) NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_feedback_status_created (status, created_at),
  KEY idx_feedback_rate (ip_hash, created_at),
  KEY idx_feedback_entity (entity_type, entity_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_collection_runs (
  run_id VARCHAR(96) NOT NULL PRIMARY KEY,
  pipeline VARCHAR(64) NOT NULL,
  source_key VARCHAR(191) NULL,
  status VARCHAR(24) NOT NULL,
  started_at DATETIME NOT NULL,
  finished_at DATETIME NULL,
  fetched_count INT NOT NULL DEFAULT 0,
  resolved_count INT NOT NULL DEFAULT 0,
  accepted_count INT NOT NULL DEFAULT 0,
  error_count INT NOT NULL DEFAULT 0,
  lag_seconds_p95 INT NULL,
  metrics_json MEDIUMTEXT NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_collection_pipeline_finished (pipeline, finished_at),
  KEY idx_collection_status_finished (status, finished_at),
  KEY idx_collection_source_finished (source_key, finished_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS activist_link_discoveries (
  discovery_id VARCHAR(96) NOT NULL PRIMARY KEY,
  discovered_url TEXT NOT NULL,
  discovered_url_hash CHAR(64) NOT NULL,
  source VARCHAR(191) NULL,
  title VARCHAR(700) NULL,
  status VARCHAR(24) NOT NULL DEFAULT 'discovered',
  resolved_url TEXT NULL,
  attempt_count INT NOT NULL DEFAULT 0,
  next_attempt_at DATETIME NULL,
  lease_token VARCHAR(64) NULL,
  locked_by VARCHAR(96) NULL,
  locked_at DATETIME NULL,
  lease_expires_at DATETIME NULL,
  last_error TEXT NULL,
  discovered_at DATETIME NOT NULL,
  resolved_at DATETIME NULL,
  expired_at DATETIME NULL,
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  UNIQUE KEY uq_discovered_url_hash (discovered_url_hash),
  KEY idx_link_discovery_ready (status, next_attempt_at),
  KEY idx_link_discovery_lease (status, lease_expires_at),
  KEY idx_link_discovery_resolved (resolved_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
