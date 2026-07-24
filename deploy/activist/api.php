<?php
declare(strict_types=1);

ini_set('display_errors', '0');
error_reporting(E_ALL);

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-store, no-cache, must-revalidate, max-age=0');
header('X-Content-Type-Options: nosniff');
header('Referrer-Policy: no-referrer');
header('X-Frame-Options: DENY');

$configPath = __DIR__ . '/_private/config.php';
if (!is_file($configPath)) {
    respond(500, array('ok' => false, 'error' => 'config_missing'));
}
$config = require $configPath;
require_once __DIR__ . '/governance_v1.php';
$v1Path = v1_request_path(); // canonicalized once for both CORS and dispatch

$origin = isset($_SERVER['HTTP_ORIGIN']) ? trim((string)$_SERVER['HTTP_ORIGIN']) : '';
$allowedOrigin = isset($config['allowed_origin']) ? trim((string)$config['allowed_origin']) : '';
$corsOrigin = '';
if (valid_cors_origin($origin) && valid_cors_origin($allowedOrigin) && hash_equals($allowedOrigin, $origin)) {
    $corsOrigin = $allowedOrigin;
} elseif (valid_cors_origin($origin) && $v1Path !== null && strpos($v1Path, '/ops/') !== 0 && strpos($v1Path, '/admin/') !== 0) {
    $publicOrigins = isset($config['public_api_cors_origins']) && is_array($config['public_api_cors_origins'])
        ? $config['public_api_cors_origins'] : array();
    if (in_array('*', $publicOrigins, true)) {
        $corsOrigin = '*';
    } elseif (in_array($origin, array_filter($publicOrigins, 'valid_cors_origin'), true)) {
        $corsOrigin = $origin;
    }
}
if ($corsOrigin !== '') {
    header('Access-Control-Allow-Origin: ' . $corsOrigin);
    header('Vary: Origin');
    header('Access-Control-Allow-Methods: GET, POST, PUT, OPTIONS');
    header('Access-Control-Allow-Headers: Content-Type, Authorization, X-Request-ID, X-Activist-Timestamp, X-Activist-Nonce, X-Activist-Signature, X-Telegram-Admin-Token');
    header('Access-Control-Max-Age: 600');
}
if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') {
    http_response_code(204);
    exit;
}

$method = $_SERVER['REQUEST_METHOD'];

try {
    if ($v1Path !== null) {
        handle_v1_request($method, $v1Path, $config);
    } elseif ($method === 'GET') {
        $action = isset($_GET['action']) ? (string)$_GET['action'] : 'health';
        handle_read($action, $config);
    } elseif ($method === 'POST') {
        $action = isset($_GET['action']) ? (string)$_GET['action'] : 'health';
        handle_write($action, $config);
    } else {
        respond(405, array('ok' => false, 'error' => 'method_not_allowed'));
    }
} catch (Throwable $e) {
    error_log('[activist-api] ' . $e->getMessage());
    respond(500, array('ok' => false, 'error' => 'internal_error'));
}

function respond(int $status, array $payload): void {
    $encoded = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    if ($encoded === false) {
        $status = 500;
        $encoded = '{"ok":false,"error":"json_encoding_failed"}';
    }
    $budget = defined('V1_RESPONSE_BUDGET_BYTES') ? V1_RESPONSE_BUDGET_BYTES : 250000;
    if (strlen($encoded) > $budget) {
        $status = 500;
        $encoded = json_encode(array('ok' => false, 'error' => 'response_budget_exceeded', 'max_bytes' => $budget));
    }
    header('X-Response-Bytes: ' . strlen((string)$encoded));
    http_response_code($status);
    echo $encoded;
    exit;
}

function valid_cors_origin($origin): bool {
    if (!is_string($origin) || $origin === '' || strlen($origin) > 2048 || preg_match('/[\r\n]/', $origin)) { return false; }
    $parts = parse_url($origin);
    if (!is_array($parts) || !isset($parts['scheme'], $parts['host'])) { return false; }
    if (!in_array(strtolower((string)$parts['scheme']), array('http', 'https'), true)) { return false; }
    foreach (array('user', 'pass', 'query', 'fragment') as $key) {
        if (isset($parts[$key])) { return false; }
    }
    return !isset($parts['path']) || $parts['path'] === '';
}

function table_name(array $config, string $name): string {
    return '`' . table_plain_name($config, $name) . '`';
}

function table_plain_name(array $config, string $name): string {
    $prefix = isset($config['table_prefix']) ? (string)$config['table_prefix'] : 'activist_';
    if (!preg_match('/^[A-Za-z0-9_]+$/', $prefix)) {
        respond(500, array('ok' => false, 'error' => 'invalid_table_prefix'));
    }
    return $prefix . $name;
}

function pdo_conn(array $config): PDO {
    $host = (string)$config['db_host'];
    $port = (int)$config['db_port'];
    $name = (string)$config['db_name'];
    $charset = isset($config['db_charset']) ? (string)$config['db_charset'] : 'utf8mb4';
    if (!preg_match('/^[A-Za-z0-9_]{1,32}$/', $charset)) {
        respond(500, array('ok' => false, 'error' => 'invalid_db_charset'));
    }
    $dsn = 'mysql:host=' . $host . ';port=' . $port . ';dbname=' . $name . ';charset=' . $charset;
    $pdo = new PDO($dsn, (string)$config['db_user'], (string)$config['db_password'], array(
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_EMULATE_PREPARES => false,
    ));
    return $pdo;
}

function ensure_schema(PDO $pdo, array $config): void {
    $charset = ' DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci';
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'api_nonces') . ' (
        nonce VARCHAR(96) NOT NULL PRIMARY KEY,
        seen_at DATETIME NOT NULL,
        INDEX idx_seen_at (seen_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'runs') . ' (
        run_id VARCHAR(96) NOT NULL PRIMARY KEY,
        started_at DATETIME NULL,
        finished_at DATETIME NULL,
        mode VARCHAR(40) NULL,
        fetched INT NOT NULL DEFAULT 0,
        accepted INT NOT NULL DEFAULT 0,
        duplicates INT NOT NULL DEFAULT 0,
        rejected INT NOT NULL DEFAULT 0,
        published_now INT NOT NULL DEFAULT 0,
        pending INT NOT NULL DEFAULT 0,
        published_total INT NOT NULL DEFAULT 0,
        payload_json MEDIUMTEXT NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_finished_at (finished_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'articles') . ' (
        record_id VARCHAR(96) NOT NULL PRIMARY KEY,
        canonical_url_hash VARCHAR(96) NULL,
        title_hash VARCHAR(96) NULL,
        canonical_url TEXT NULL,
        title VARCHAR(700) NULL,
        normalized_title VARCHAR(700) NULL,
        summary MEDIUMTEXT NULL,
        source VARCHAR(255) NULL,
        feed_name VARCHAR(255) NULL,
        feed_category VARCHAR(80) NULL,
        image_url TEXT NULL,
        published_at DATETIME NULL,
        seen_at DATETIME NULL,
        status VARCHAR(40) NULL,
        reason VARCHAR(120) NULL,
        relevance_level VARCHAR(40) NULL,
        priority_score INT NOT NULL DEFAULT 0,
        priority_level VARCHAR(40) NULL,
        story_key VARCHAR(120) NULL,
        source_right_id VARCHAR(64) NULL,
        payload_json MEDIUMTEXT NULL,
        sort_at DATETIME NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_url_hash (canonical_url_hash),
        INDEX idx_title_hash (title_hash),
        INDEX idx_seen_at (seen_at),
        INDEX idx_published_at (published_at),
        INDEX idx_story_key (story_key),
        INDEX idx_article_source_right (source_right_id),
        INDEX idx_priority (priority_score)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'stories') . ' (
        story_key VARCHAR(120) NOT NULL PRIMARY KEY,
        guid VARCHAR(191) NULL,
        representative_title VARCHAR(700) NULL,
        representative_url TEXT NULL,
        relevance_level VARCHAR(40) NULL,
        theme_group VARCHAR(120) NULL,
        status VARCHAR(40) NULL,
        article_count INT NOT NULL DEFAULT 0,
        priority_score INT NOT NULL DEFAULT 0,
        source_right_id VARCHAR(64) NULL,
        published_at DATETIME NULL,
        last_article_seen_at DATETIME NULL,
        payload_json MEDIUMTEXT NULL,
        sort_at DATETIME NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_guid (guid),
        INDEX idx_published_at (published_at),
        INDEX idx_priority (priority_score),
        INDEX idx_story_source_right (source_right_id),
        INDEX idx_theme_group (theme_group)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'article_raw') . ' (
        raw_id VARCHAR(96) NOT NULL PRIMARY KEY,
        record_id VARCHAR(96) NULL,
        raw_kind VARCHAR(40) NOT NULL,
        payload_hash VARCHAR(64) NOT NULL,
        compression VARCHAR(20) NOT NULL DEFAULT \'gzip\',
        payload_compressed MEDIUMBLOB NOT NULL,
        schema_version INT NOT NULL DEFAULT 1,
        retained_until DATETIME NOT NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        UNIQUE KEY uq_raw_kind_hash (raw_kind, payload_hash),
        INDEX idx_record_id (record_id),
        INDEX idx_retained_until (retained_until)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'story_articles') . ' (
        story_key VARCHAR(120) NOT NULL,
        article_id VARCHAR(96) NOT NULL,
        position_no INT NOT NULL DEFAULT 0,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (story_key, article_id),
        INDEX idx_article_id (article_id)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'reports') . ' (
        date_id VARCHAR(20) NOT NULL PRIMARY KEY,
        title VARCHAR(255) NULL,
        start_at DATETIME NULL,
        end_at DATETIME NULL,
        public_url TEXT NULL,
        story_count INT NOT NULL DEFAULT 0,
        article_count INT NOT NULL DEFAULT 0,
        payload_json MEDIUMTEXT NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_end_at (end_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'telegram_channels') . ' (
        handle VARCHAR(191) NOT NULL PRIMARY KEY,
        telegram_channel_id VARCHAR(64) NULL,
        identity_migration_version TINYINT UNSIGNED NOT NULL DEFAULT 0,
        title VARCHAR(255) NULL,
        description TEXT NULL,
        joined TINYINT(1) NOT NULL DEFAULT 0,
        enabled TINYINT(1) NOT NULL DEFAULT 1,
        source VARCHAR(40) NULL,
        source_type VARCHAR(60) NULL,
        is_public_channel TINYINT(1) NOT NULL DEFAULT 1,
        quality_score INT NOT NULL DEFAULT 0,
        last_message_id BIGINT NOT NULL DEFAULT 0,
        last_collected_at DATETIME NULL,
        last_recommendation_checked_at DATETIME NULL,
        last_error VARCHAR(191) NULL,
        payload_json MEDIUMTEXT NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_channel_id (telegram_channel_id),
        INDEX idx_enabled_quality (enabled, quality_score)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'telegram_messages') . ' (
        message_key VARCHAR(180) NOT NULL PRIMARY KEY,
        channel_handle VARCHAR(191) NOT NULL,
        telegram_channel_id VARCHAR(64) NULL,
        telegram_message_id BIGINT NOT NULL,
        posted_at DATETIME NULL,
        edited_at DATETIME NULL,
        deleted_at DATETIME NULL,
        collected_at DATETIME NULL,
        text MEDIUMTEXT NULL,
        normalized_text MEDIUMTEXT NULL,
        views INT NOT NULL DEFAULT 0,
        forwards INT NOT NULL DEFAULT 0,
        replies_count INT NOT NULL DEFAULT 0,
        message_url TEXT NULL,
        urls_json MEDIUMTEXT NULL,
        risk_flags_json TEXT NULL,
        raw_json MEDIUMTEXT NULL,
        updated_at DATETIME NOT NULL,
        UNIQUE KEY uq_channel_message (channel_handle, telegram_message_id),
        INDEX idx_telegram_channel_message_id (telegram_channel_id, telegram_message_id),
        INDEX idx_posted_at (posted_at),
        INDEX idx_channel_posted (channel_handle, posted_at),
        INDEX idx_deleted_at (deleted_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'telegram_article_matches') . ' (
        article_id VARCHAR(96) NOT NULL,
        message_key VARCHAR(180) NOT NULL,
        match_type VARCHAR(40) NOT NULL,
        score DECIMAL(6,4) NOT NULL DEFAULT 0,
        reason VARCHAR(500) NULL,
        channel_handle VARCHAR(191) NULL,
        telegram_message_id BIGINT NULL,
        message_url TEXT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (article_id, message_key, match_type),
        INDEX idx_message_key (message_key),
        INDEX idx_article_score (article_id, score),
        INDEX idx_match_type (match_type, score)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'telegram_issue_signals') . ' (
        article_id VARCHAR(96) NOT NULL PRIMARY KEY,
        related_telegram_count INT NOT NULL DEFAULT 0,
        related_telegram_channels_count INT NOT NULL DEFAULT 0,
        first_seen_at DATETIME NULL,
        latest_seen_at DATETIME NULL,
        confidence_score DECIMAL(6,4) NOT NULL DEFAULT 0,
        payload_json MEDIUMTEXT NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_signal_strength (related_telegram_channels_count, related_telegram_count),
        INDEX idx_latest_seen (latest_seen_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'telegram_signal_rebuild_state') . ' (
        state_key VARCHAR(16) NOT NULL PRIMARY KEY,
        active_token CHAR(64) NULL,
        started_at DATETIME NULL,
        finalized_token CHAR(64) NULL,
        finalized_at DATETIME NULL,
        live_revision BIGINT UNSIGNED NOT NULL DEFAULT 0,
        updated_at DATETIME NOT NULL,
        INDEX idx_signal_rebuild_active (active_token),
        INDEX idx_signal_rebuild_finalized (finalized_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'telegram_signal_rebuild_staging') . ' (
        rebuild_token CHAR(64) NOT NULL,
        article_id VARCHAR(96) NOT NULL,
        payload_json MEDIUMTEXT NOT NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (rebuild_token, article_id),
        INDEX idx_signal_rebuild_staging_created (created_at)
    ) ENGINE=InnoDB' . $charset);
    ensure_column($pdo, $config, 'telegram_signal_rebuild_state', 'live_revision', 'BIGINT UNSIGNED NOT NULL DEFAULT 0');
    $signalRebuildState = $pdo->prepare('INSERT IGNORE INTO ' . table_name($config, 'telegram_signal_rebuild_state') . ' (
        state_key, active_token, started_at, finalized_token, finalized_at, live_revision, updated_at
    ) VALUES (?,?,?,?,?,?,?)');
    $signalRebuildState->execute(array('global', null, null, null, null, 0, gmdate('Y-m-d H:i:s')));
    $pdo->exec('DELETE FROM ' . table_name($config, 'api_nonces') . ' WHERE seen_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 DAY)');
    ensure_column($pdo, $config, 'articles', 'sort_at', 'DATETIME NULL');
    ensure_column($pdo, $config, 'articles', 'source_right_id', 'VARCHAR(64) NULL');
    ensure_column($pdo, $config, 'stories', 'sort_at', 'DATETIME NULL');
    ensure_column($pdo, $config, 'stories', 'source_right_id', 'VARCHAR(64) NULL');
    ensure_index($pdo, $config, 'articles', 'idx_sort_at', 'sort_at');
    ensure_index($pdo, $config, 'articles', 'idx_status_sort', 'status, sort_at');
    ensure_index($pdo, $config, 'articles', 'idx_article_source_right', 'source_right_id');
    ensure_index($pdo, $config, 'stories', 'idx_sort_at', 'sort_at');
    ensure_index($pdo, $config, 'stories', 'idx_story_source_right', 'source_right_id');
    $pdo->exec('UPDATE ' . table_name($config, 'articles') . ' SET sort_at = COALESCE(published_at, seen_at, updated_at) WHERE sort_at IS NULL');
    $pdo->exec('UPDATE ' . table_name($config, 'stories') . ' SET sort_at = COALESCE(published_at, last_article_seen_at, updated_at) WHERE sort_at IS NULL');
    $pdo->exec('DELETE FROM ' . table_name($config, 'article_raw') . ' WHERE retained_until < UTC_TIMESTAMP()');
    ensure_governance_schema($pdo, $config);
}

/**
 * Governance intelligence schema v1.
 *
 * Runtime creation is intentionally additive so an API deployment can be
 * rolled out before a separate migration job. The matching reviewed SQL
 * migration lives in deploy/activist/migrations/001_governance_v1.sql.
 */
function ensure_governance_schema(PDO $pdo, array $config): void {
    $charset = ' DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci';
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'companies') . ' (
        company_id CHAR(8) NOT NULL PRIMARY KEY,
        stock_code VARCHAR(12) NULL,
        market VARCHAR(40) NULL,
        legal_name VARCHAR(255) NOT NULL,
        legal_name_en VARCHAR(255) NULL,
        short_name VARCHAR(255) NULL,
        aliases_json TEXT NULL,
        homepage_url TEXT NULL,
        record_status VARCHAR(24) NOT NULL DEFAULT \'active\',
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        UNIQUE KEY uq_company_stock_code (stock_code),
        INDEX idx_company_name (legal_name),
        INDEX idx_company_market_status (market, record_status)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'actors') . ' (
        actor_id VARCHAR(64) NOT NULL PRIMARY KEY,
        actor_type VARCHAR(40) NOT NULL,
        display_name VARCHAR(255) NOT NULL,
        display_name_en VARCHAR(255) NULL,
        company_id CHAR(8) NULL,
        country_code CHAR(2) NULL,
        aliases_json TEXT NULL,
        homepage_url TEXT NULL,
        review_status VARCHAR(24) NOT NULL DEFAULT \'pending\',
        record_status VARCHAR(24) NOT NULL DEFAULT \'inactive\',
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_actor_name (display_name),
        INDEX idx_actor_review (review_status, updated_at),
        INDEX idx_actor_type_status (actor_type, record_status),
        INDEX idx_actor_company (company_id)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'source_rights') . ' (
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
        status VARCHAR(24) NOT NULL DEFAULT \'pending\',
        notes TEXT NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        UNIQUE KEY uq_source_right_key (source_type, source_key),
        INDEX idx_source_right_validity (status, valid_from, valid_until, revoked_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'documents') . ' (
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
        verification_status VARCHAR(24) NOT NULL DEFAULT \'unverified\',
        publication_status VARCHAR(24) NOT NULL DEFAULT \'draft\',
        payload_json MEDIUMTEXT NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        UNIQUE KEY uq_document_right_external_version (source_right_id, external_id, version_no),
        INDEX idx_document_company_published (company_id, published_at),
        INDEX idx_document_source_right (source_right_id),
        INDEX idx_document_collection (company_id, source_class, collection_key, version_no),
        INDEX idx_document_correction (correction_of_document_id),
        INDEX idx_document_publication (publication_status, published_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'governance_events') . ' (
        event_id VARCHAR(96) NOT NULL PRIMARY KEY,
        company_id CHAR(8) NOT NULL,
        event_type VARCHAR(64) NOT NULL,
        title VARCHAR(700) NOT NULL,
        original_language VARCHAR(16) NOT NULL DEFAULT \'ko\',
        summary MEDIUMTEXT NULL,
        occurred_at DATETIME NOT NULL,
        deadline_at DATETIME NULL,
        importance VARCHAR(24) NOT NULL DEFAULT \'medium\',
        verification_status VARCHAR(24) NOT NULL DEFAULT \'signal\',
        review_status VARCHAR(24) NOT NULL DEFAULT \'pending\',
        publication_status VARCHAR(24) NOT NULL DEFAULT \'draft\',
        collection_key VARCHAR(96) NULL,
        payload_json MEDIUMTEXT NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_event_company_occurred (company_id, occurred_at),
        INDEX idx_event_type_occurred (event_type, occurred_at),
        INDEX idx_event_deadline (deadline_at),
        INDEX idx_event_public (publication_status, occurred_at),
        INDEX idx_event_review (review_status, importance, occurred_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'campaigns') . ' (
        campaign_id VARCHAR(96) NOT NULL PRIMARY KEY,
        company_id CHAR(8) NOT NULL,
        lead_actor_id VARCHAR(64) NULL,
        title VARCHAR(700) NOT NULL,
        original_language VARCHAR(16) NOT NULL DEFAULT \'ko\',
        demand_text MEDIUMTEXT NULL,
        stage VARCHAR(40) NOT NULL,
        outcome VARCHAR(40) NULL,
        started_at DATETIME NOT NULL,
        ended_at DATETIME NULL,
        review_status VARCHAR(24) NOT NULL DEFAULT \'pending\',
        publication_status VARCHAR(24) NOT NULL DEFAULT \'draft\',
        payload_json MEDIUMTEXT NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_campaign_company_stage (company_id, stage),
        INDEX idx_campaign_actor (lead_actor_id),
        INDEX idx_campaign_public (publication_status, started_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'event_documents') . ' (
        event_id VARCHAR(96) NOT NULL,
        document_id VARCHAR(96) NOT NULL,
        relation_type VARCHAR(40) NOT NULL DEFAULT \'evidence\',
        position_no INT NOT NULL DEFAULT 0,
        created_at DATETIME NOT NULL,
        PRIMARY KEY (event_id, document_id, relation_type),
        INDEX idx_event_document_document (document_id)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'campaign_documents') . ' (
        campaign_id VARCHAR(96) NOT NULL,
        document_id VARCHAR(96) NOT NULL,
        relation_type VARCHAR(40) NOT NULL DEFAULT \'evidence\',
        position_no INT NOT NULL DEFAULT 0,
        created_at DATETIME NOT NULL,
        PRIMARY KEY (campaign_id, document_id, relation_type),
        INDEX idx_campaign_document_document (document_id)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'event_actors') . ' (
        event_id VARCHAR(96) NOT NULL,
        actor_id VARCHAR(64) NOT NULL,
        actor_role VARCHAR(40) NOT NULL,
        review_status VARCHAR(24) NOT NULL DEFAULT \'pending\',
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        PRIMARY KEY (event_id, actor_id, actor_role),
        INDEX idx_event_actor_review (review_status, updated_at),
        INDEX idx_event_actor_actor (actor_id)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'claim_evidence') . ' (
        claim_id VARCHAR(96) NOT NULL PRIMARY KEY,
        event_id VARCHAR(96) NOT NULL,
        campaign_id VARCHAR(96) NULL,
        actor_id VARCHAR(64) NULL,
        document_id VARCHAR(96) NOT NULL,
        claim_type VARCHAR(40) NOT NULL,
        claim_text MEDIUMTEXT NOT NULL,
        original_language VARCHAR(16) NOT NULL,
        evidence_locator VARCHAR(500) NULL,
        editorial_status VARCHAR(24) NOT NULL DEFAULT \'pending\',
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_claim_event_type (event_id, claim_type),
        INDEX idx_claim_campaign (campaign_id),
        INDEX idx_claim_document (document_id),
        INDEX idx_claim_editorial (editorial_status)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'proposal_votes') . ' (
        proposal_vote_id VARCHAR(96) NOT NULL PRIMARY KEY,
        event_id VARCHAR(96) NULL,
        campaign_id VARCHAR(96) NULL,
        company_id CHAR(8) NOT NULL,
        proposer_actor_id VARCHAR(64) NULL,
        agenda_no VARCHAR(40) NULL,
        agenda_title VARCHAR(700) NOT NULL,
        original_language VARCHAR(16) NOT NULL DEFAULT \'ko\',
        meeting_at DATETIME NOT NULL,
        recommendation VARCHAR(40) NULL,
        recommendation_source VARCHAR(255) NULL,
        result VARCHAR(24) NOT NULL DEFAULT \'pending\',
        votes_for DECIMAL(7,4) NULL,
        votes_against DECIMAL(7,4) NULL,
        votes_abstain DECIMAL(7,4) NULL,
        evidence_document_id VARCHAR(96) NULL,
        review_status VARCHAR(24) NOT NULL DEFAULT \'pending\',
        publication_status VARCHAR(24) NOT NULL DEFAULT \'draft\',
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_vote_company_meeting (company_id, meeting_at),
        INDEX idx_vote_event (event_id),
        INDEX idx_vote_campaign (campaign_id),
        INDEX idx_vote_review (review_status, meeting_at),
        INDEX idx_vote_public (publication_status, meeting_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'commitment_outcomes') . ' (
        commitment_id VARCHAR(96) NOT NULL PRIMARY KEY,
        event_id VARCHAR(96) NULL,
        campaign_id VARCHAR(96) NULL,
        company_id CHAR(8) NOT NULL,
        commitment_text MEDIUMTEXT NOT NULL,
        original_language VARCHAR(16) NOT NULL DEFAULT \'ko\',
        target_at DATETIME NULL,
        actual_action MEDIUMTEXT NULL,
        status VARCHAR(32) NOT NULL DEFAULT \'announced\',
        target_metrics_json TEXT NULL,
        actual_metrics_json TEXT NULL,
        evidence_document_id VARCHAR(96) NULL,
        review_status VARCHAR(24) NOT NULL DEFAULT \'pending\',
        publication_status VARCHAR(24) NOT NULL DEFAULT \'draft\',
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_commitment_company_status (company_id, status),
        INDEX idx_commitment_target (target_at),
        INDEX idx_commitment_review (review_status, target_at),
        INDEX idx_commitment_public (publication_status, target_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'timeline_entries') . ' (
        timeline_entry_id VARCHAR(96) NOT NULL PRIMARY KEY,
        event_id VARCHAR(96) NULL,
        campaign_id VARCHAR(96) NULL,
        document_id VARCHAR(96) NULL,
        occurred_at DATETIME NOT NULL,
        entry_type VARCHAR(40) NOT NULL,
        title VARCHAR(700) NOT NULL,
        description MEDIUMTEXT NULL,
        original_language VARCHAR(16) NOT NULL DEFAULT \'ko\',
        review_status VARCHAR(24) NOT NULL DEFAULT \'pending\',
        publication_status VARCHAR(24) NOT NULL DEFAULT \'draft\',
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_timeline_event_time (event_id, occurred_at),
        INDEX idx_timeline_campaign_time (campaign_id, occurred_at),
        INDEX idx_timeline_review (review_status, occurred_at),
        INDEX idx_timeline_public_time (publication_status, occurred_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'editorial_revisions') . ' (
        revision_id VARCHAR(96) NOT NULL PRIMARY KEY,
        entity_type VARCHAR(40) NOT NULL,
        entity_id VARCHAR(96) NOT NULL,
        field_name VARCHAR(80) NULL,
        previous_value MEDIUMTEXT NULL,
        revised_value MEDIUMTEXT NULL,
        reason TEXT NOT NULL,
        revision_status VARCHAR(24) NOT NULL DEFAULT \'pending\',
        requested_by VARCHAR(191) NULL,
        reviewed_by VARCHAR(191) NULL,
        reviewed_at DATETIME NULL,
        published_at DATETIME NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_revision_entity (entity_type, entity_id),
        INDEX idx_revision_status_created (revision_status, created_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'editorial_ingest_chunks') . ' (
        chunk_id VARCHAR(96) NOT NULL PRIMARY KEY,
        bundle_sha256 CHAR(64) NOT NULL,
        chunk_index INT NOT NULL,
        chunk_count INT NOT NULL,
        entity_type VARCHAR(40) NOT NULL,
        payload_sha256 CHAR(64) NOT NULL,
        accepted_json TEXT NOT NULL,
        created_at DATETIME NOT NULL,
        UNIQUE KEY uq_editorial_bundle_chunk (bundle_sha256, chunk_index),
        INDEX idx_editorial_chunk_created (created_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'delivery_outbox') . ' (
        delivery_id VARCHAR(96) NOT NULL PRIMARY KEY,
        event_id VARCHAR(96) NULL,
        delivery_channel VARCHAR(40) NOT NULL,
        destination VARCHAR(191) NOT NULL,
        idempotency_key VARCHAR(191) NOT NULL,
        payload_json MEDIUMTEXT NOT NULL,
        status VARCHAR(24) NOT NULL DEFAULT \'pending\',
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
        INDEX idx_delivery_ready (status, next_attempt_at),
        INDEX idx_delivery_lease (status, lease_expires_at),
        INDEX idx_delivery_event (event_id),
        INDEX idx_delivery_dead_letter (dead_lettered_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'feedback') . ' (
        feedback_id VARCHAR(64) NOT NULL PRIMARY KEY,
        feedback_type VARCHAR(32) NOT NULL,
        entity_type VARCHAR(40) NULL,
        entity_id VARCHAR(96) NULL,
        submitter_name VARCHAR(191) NULL,
        submitter_contact VARCHAR(320) NULL,
        message TEXT NOT NULL,
        evidence_urls_json TEXT NULL,
        status VARCHAR(24) NOT NULL DEFAULT \'pending\',
        is_public TINYINT(1) NOT NULL DEFAULT 0,
        review_note TEXT NULL,
        reviewed_by VARCHAR(191) NULL,
        reviewed_at DATETIME NULL,
        ip_hash CHAR(64) NOT NULL,
        user_agent_hash CHAR(64) NULL,
        created_at DATETIME NOT NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_feedback_status_created (status, created_at),
        INDEX idx_feedback_rate (ip_hash, created_at),
        INDEX idx_feedback_entity (entity_type, entity_id)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'collection_runs') . ' (
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
        INDEX idx_collection_pipeline_finished (pipeline, finished_at),
        INDEX idx_collection_status_finished (status, finished_at),
        INDEX idx_collection_source_finished (source_key, finished_at)
    ) ENGINE=InnoDB' . $charset);
    $pdo->exec('CREATE TABLE IF NOT EXISTS ' . table_name($config, 'link_discoveries') . ' (
        discovery_id VARCHAR(96) NOT NULL PRIMARY KEY,
        discovered_url TEXT NOT NULL,
        discovered_url_hash CHAR(64) NOT NULL,
        source VARCHAR(191) NULL,
        title VARCHAR(700) NULL,
        summary TEXT NULL,
        feed_name VARCHAR(191) NULL,
        feed_category VARCHAR(64) NULL,
        source_kind VARCHAR(40) NULL,
        source_right_id VARCHAR(64) NULL,
        lineage_version SMALLINT UNSIGNED NOT NULL DEFAULT 0,
        published_at DATETIME NULL,
        status VARCHAR(24) NOT NULL DEFAULT \'discovered\',
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
        INDEX idx_link_discovery_ready (status, next_attempt_at),
        INDEX idx_link_discovery_lease (status, lease_expires_at),
        INDEX idx_link_discovery_resolved (resolved_at)
    ) ENGINE=InnoDB' . $charset);
    ensure_column($pdo, $config, 'link_discoveries', 'summary', 'TEXT NULL');
    ensure_column($pdo, $config, 'link_discoveries', 'feed_name', 'VARCHAR(191) NULL');
    ensure_column($pdo, $config, 'link_discoveries', 'feed_category', 'VARCHAR(64) NULL');
    ensure_column($pdo, $config, 'link_discoveries', 'source_kind', 'VARCHAR(40) NULL');
    ensure_column($pdo, $config, 'link_discoveries', 'source_right_id', 'VARCHAR(64) NULL');
    ensure_column($pdo, $config, 'link_discoveries', 'lineage_version', 'SMALLINT UNSIGNED NOT NULL DEFAULT 0');
    ensure_column($pdo, $config, 'link_discoveries', 'published_at', 'DATETIME NULL');
    ensure_index($pdo, $config, 'link_discoveries', 'idx_link_discovery_lineage', 'lineage_version, status, resolved_at');
    ensure_index($pdo, $config, 'link_discoveries', 'idx_link_discovery_claim', 'lineage_version, status, discovered_at');
    ensure_column($pdo, $config, 'delivery_outbox', 'lease_token', 'VARCHAR(64) NULL');
    ensure_column($pdo, $config, 'delivery_outbox', 'locked_by', 'VARCHAR(96) NULL');
    ensure_column($pdo, $config, 'delivery_outbox', 'lease_expires_at', 'DATETIME NULL');
    ensure_index($pdo, $config, 'delivery_outbox', 'idx_delivery_lease', 'status, lease_expires_at');
    ensure_column($pdo, $config, 'feedback', 'review_note', 'TEXT NULL');
    ensure_column($pdo, $config, 'feedback', 'reviewed_by', 'VARCHAR(191) NULL');
    ensure_column($pdo, $config, 'feedback', 'reviewed_at', 'DATETIME NULL');
    ensure_column($pdo, $config, 'documents', 'collection_key', 'VARCHAR(96) NULL');
    ensure_index($pdo, $config, 'documents', 'idx_document_collection', 'company_id, source_class, collection_key, version_no');
    ensure_column($pdo, $config, 'actors', 'review_status', 'VARCHAR(24) NOT NULL DEFAULT \'pending\'');
    ensure_index($pdo, $config, 'actors', 'idx_actor_review', 'review_status, updated_at');
    ensure_column($pdo, $config, 'event_actors', 'review_status', 'VARCHAR(24) NOT NULL DEFAULT \'pending\'');
    ensure_column($pdo, $config, 'event_actors', 'updated_at', 'DATETIME NULL');
    $pdo->exec('UPDATE ' . table_name($config, 'event_actors') . ' SET updated_at=created_at WHERE updated_at IS NULL');
    ensure_index($pdo, $config, 'event_actors', 'idx_event_actor_review', 'review_status, updated_at');
    ensure_column($pdo, $config, 'proposal_votes', 'review_status', 'VARCHAR(24) NOT NULL DEFAULT \'pending\'');
    ensure_index($pdo, $config, 'proposal_votes', 'idx_vote_review', 'review_status, meeting_at');
    ensure_column($pdo, $config, 'commitment_outcomes', 'review_status', 'VARCHAR(24) NOT NULL DEFAULT \'pending\'');
    ensure_index($pdo, $config, 'commitment_outcomes', 'idx_commitment_review', 'review_status, target_at');
    ensure_column($pdo, $config, 'timeline_entries', 'review_status', 'VARCHAR(24) NOT NULL DEFAULT \'pending\'');
    ensure_index($pdo, $config, 'timeline_entries', 'idx_timeline_review', 'review_status, occurred_at');
    ensure_column($pdo, $config, 'editorial_ingest_chunks', 'payload_sha256', 'CHAR(64) NOT NULL');
}

function column_exists(PDO $pdo, array $config, string $table, string $column): bool {
    $stmt = $pdo->prepare('SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND COLUMN_NAME = ?');
    $stmt->execute(array(table_plain_name($config, $table), $column));
    return (int)$stmt->fetchColumn() > 0;
}

function ensure_column(PDO $pdo, array $config, string $table, string $column, string $definition): void {
    if (!column_exists($pdo, $config, $table, $column)) {
        $pdo->exec('ALTER TABLE ' . table_name($config, $table) . ' ADD COLUMN `' . $column . '` ' . $definition);
    }
}

function index_exists(PDO $pdo, array $config, string $table, string $index): bool {
    $stmt = $pdo->prepare('SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND INDEX_NAME = ?');
    $stmt->execute(array(table_plain_name($config, $table), $index));
    return (int)$stmt->fetchColumn() > 0;
}

function ensure_index(PDO $pdo, array $config, string $table, string $index, string $columns): void {
    if (!preg_match('/^[A-Za-z0-9_]+$/', $index) || !preg_match('/^[A-Za-z0-9_, ]+$/', $columns)) {
        respond(500, array('ok' => false, 'error' => 'invalid_index_definition'));
    }
    if (!index_exists($pdo, $config, $table, $index)) {
        $pdo->exec('CREATE INDEX `' . $index . '` ON ' . table_name($config, $table) . ' (' . $columns . ')');
    }
}

/**
 * A SourceRight is publishable only while its evidence-backed grant is active.
 * Keep this predicate in one place so legacy endpoints cannot accidentally
 * apply a weaker interpretation than /api/v1 documents.
 */
function source_right_redistribution_sql(string $rightsAlias): string {
    return '(' . $rightsAlias . '.source_right_id IS NOT NULL'
        . ' AND ' . $rightsAlias . '.status = \'active\''
        . ' AND ' . $rightsAlias . '.redistribution_allowed = 1'
        . ' AND ' . $rightsAlias . '.valid_from <= UTC_TIMESTAMP()'
        . ' AND (' . $rightsAlias . '.valid_until IS NULL OR ' . $rightsAlias . '.valid_until > UTC_TIMESTAMP())'
        . ' AND ' . $rightsAlias . '.revoked_at IS NULL'
        . ' AND (NULLIF(TRIM(' . $rightsAlias . '.evidence_uri), \'\') IS NOT NULL'
        . ' OR NULLIF(TRIM(' . $rightsAlias . '.evidence_hash), \'\') IS NOT NULL))';
}

function legacy_article_visibility_sql(string $articleAlias, string $rightsAlias): string {
    $isTelegram = '(LOWER(COALESCE(' . $articleAlias . '.feed_category,\'\')) LIKE \'telegram%\''
        . ' OR LOWER(COALESCE(' . $articleAlias . '.source,\'\')) LIKE \'telegram%\''
        . ' OR LOWER(COALESCE(' . $articleAlias . '.feed_name,\'\')) LIKE \'telegram:%\')';
    return '((' . $articleAlias . '.source_right_id IS NULL AND NOT ' . $isTelegram . ')'
        . ' OR (' . $articleAlias . '.source_right_id IS NOT NULL AND ' . source_right_redistribution_sql($rightsAlias) . '))';
}

function legacy_story_visibility_sql(array $config, string $storyAlias): string {
    return 'EXISTS (SELECT 1 FROM ' . table_name($config, 'story_articles') . ' rights_sa'
        . ' JOIN ' . table_name($config, 'articles') . ' rights_a ON rights_a.record_id = rights_sa.article_id'
        . ' LEFT JOIN ' . table_name($config, 'source_rights') . ' rights_sr ON rights_sr.source_right_id = rights_a.source_right_id'
        . ' WHERE rights_sa.story_key = ' . $storyAlias . '.story_key'
        . ' AND rights_sa.position_no = 0'
        . ' AND rights_a.canonical_url = ' . $storyAlias . '.representative_url'
        . ' AND rights_a.source_right_id <=> ' . $storyAlias . '.source_right_id AND '
        . legacy_article_visibility_sql('rights_a', 'rights_sr') . ')';
}

function telegram_signal_visibility_sql(array $config, string $signalAlias): string {
    return '(JSON_VALID(' . $signalAlias . '.payload_json)'
        . ' AND JSON_LENGTH(' . $signalAlias . '.payload_json, \'$.source_right_ids\') > 0'
        . ' AND (SELECT COUNT(DISTINCT signal_sr.source_right_id) FROM ' . table_name($config, 'source_rights') . ' signal_sr'
        . ' WHERE LOWER(signal_sr.source_type) LIKE \'%telegram%\''
        . ' AND ' . source_right_redistribution_sql('signal_sr')
        . ' AND JSON_CONTAINS(' . $signalAlias . '.payload_json, JSON_QUOTE(signal_sr.source_right_id), \'$.source_right_ids\'))'
        . ' = JSON_LENGTH(' . $signalAlias . '.payload_json, \'$.source_right_ids\'))';
}

function telegram_message_visibility_sql(array $config, string $messageAlias): string {
    return 'EXISTS (SELECT 1 FROM ' . table_name($config, 'source_rights') . ' message_sr'
        . ' WHERE message_sr.source_key = ' . $messageAlias . '.channel_handle'
        . ' AND LOWER(message_sr.source_type) LIKE \'%telegram%\''
        . ' AND ' . source_right_redistribution_sql('message_sr') . ')';
}

function active_telegram_source_keys(PDO $pdo, array $config): array {
    $sql = 'SELECT source_key FROM ' . table_name($config, 'source_rights') . ' sr'
        . ' WHERE LOWER(sr.source_type) LIKE \'%telegram%\' AND ' . source_right_redistribution_sql('sr');
    $stmt = $pdo->prepare($sql); $stmt->execute();
    $keys = array();
    foreach ($stmt->fetchAll() as $row) {
        $key = mb_strtolower(normalize_handle_value((string)$row['source_key']), 'UTF-8');
        if ($key !== '') { $keys[$key] = true; }
    }
    return $keys;
}

function read_body(array $config): string {
    $max = isset($config['max_body_bytes']) ? (int)$config['max_body_bytes'] : 2097152;
    $len = isset($_SERVER['CONTENT_LENGTH']) ? (int)$_SERVER['CONTENT_LENGTH'] : 0;
    if ($len > $max) {
        respond(413, array('ok' => false, 'error' => 'payload_too_large'));
    }
    $body = file_get_contents('php://input');
    if ($body === false) {
        respond(400, array('ok' => false, 'error' => 'body_unreadable'));
    }
    if (strlen($body) > $max) {
        respond(413, array('ok' => false, 'error' => 'payload_too_large'));
    }
    return $body;
}

function require_signature(string $body, array $config): string {
    $secret = isset($config['api_secret']) ? (string)$config['api_secret'] : '';
    if (strlen($secret) < 32) {
        respond(500, array('ok' => false, 'error' => 'secret_missing_or_too_short'));
    }
    $timestamp = isset($_SERVER['HTTP_X_ACTIVIST_TIMESTAMP']) ? (string)$_SERVER['HTTP_X_ACTIVIST_TIMESTAMP'] : '';
    $nonce = isset($_SERVER['HTTP_X_ACTIVIST_NONCE']) ? (string)$_SERVER['HTTP_X_ACTIVIST_NONCE'] : '';
    $signature = isset($_SERVER['HTTP_X_ACTIVIST_SIGNATURE']) ? (string)$_SERVER['HTTP_X_ACTIVIST_SIGNATURE'] : '';
    if (!preg_match('/^\d{10}$/', $timestamp) || !preg_match('/^[A-Za-z0-9_.:-]{16,96}$/', $nonce)) {
        respond(401, array('ok' => false, 'error' => 'auth_required'));
    }
    if (abs(time() - (int)$timestamp) > 300) {
        respond(401, array('ok' => false, 'error' => 'timestamp_expired'));
    }
    if (strpos($signature, 'sha256=') === 0) {
        $signature = substr($signature, 7);
    }
    if (!preg_match('/^[a-f0-9]{64}$/i', $signature)) {
        respond(401, array('ok' => false, 'error' => 'invalid_signature'));
    }
    $base = $timestamp . "\n" . $nonce . "\n" . $body;
    $expected = hash_hmac('sha256', $base, $secret);
    if (!hash_equals($expected, strtolower($signature))) {
        respond(401, array('ok' => false, 'error' => 'invalid_signature'));
    }
    return $nonce;
}

function remember_nonce(PDO $pdo, array $config, string $nonce): void {
    try {
        $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'api_nonces') . ' (nonce, seen_at) VALUES (?, UTC_TIMESTAMP())');
        $stmt->execute(array($nonce));
    } catch (PDOException $e) {
        if ((string)$e->getCode() === '23000') {
            respond(409, array('ok' => false, 'error' => 'nonce_reused'));
        }
        throw $e;
    }
}

function legacy_adapter_headers(PDO $pdo, array $config, string $action): void {
    $legacyActions = array('search', 'articles', 'reports', 'report', 'latest_snapshot', 'telegram_reactions', 'telegram_dashboard');
    if (!in_array($action, $legacyActions, true)) { return; }
    $stmt = $pdo->prepare('SELECT cutover_at,sunset_at FROM ' . table_name($config, 'governance_release_state') . ' WHERE state_key=? LIMIT 1');
    $stmt->execute(array(GOV_V1_RELEASE_STATE_KEY)); $release = $stmt->fetch();
    if (!is_array($release) || empty($release['cutover_at']) || empty($release['sunset_at'])) { return; }
    $cutoverTimestamp = strtotime((string)$release['cutover_at'] . ' UTC');
    $sunsetTimestamp = strtotime((string)$release['sunset_at'] . ' UTC');
    if ($cutoverTimestamp === false || $sunsetTimestamp === false || time() < $cutoverTimestamp) { return; }
    header('Deprecation: true');
    header('Sunset: ' . gmdate('D, d M Y H:i:s', (int)$sunsetTimestamp) . ' GMT');
    header('Link: </api/v1/openapi.yaml>; rel="successor-version"; type="application/yaml"');
    header('Warning: 299 BSIDE "Legacy API adapter; migrate to /api/v1"');
    header('X-BSIDE-Legacy-Adapter: true');
}

function decode_json_body(string $body): array {
    $payload = json_decode($body, true);
    if (!is_array($payload)) {
        respond(400, array('ok' => false, 'error' => 'invalid_json'));
    }
    return $payload;
}

function mysql_dt($value): ?string {
    return v1_mysql_datetime_utc($value);
}

function first_mysql_dt(array $row, array $keys, string $fallback): string {
    foreach ($keys as $key) {
        $value = array_key_exists($key, $row) ? mysql_dt($row[$key]) : null;
        if ($value !== null) {
            return $value;
        }
    }
    return $fallback;
}

function str_value(array $row, string $key, int $max = 65535): ?string {
    if (!array_key_exists($key, $row) || $row[$key] === null) {
        return null;
    }
    $value = (string)$row[$key];
    if ($max > 0 && mb_strlen($value, 'UTF-8') > $max) {
        return mb_substr($value, 0, $max, 'UTF-8');
    }
    return $value;
}

function int_value(array $row, string $key): int {
    return isset($row[$key]) ? (int)$row[$key] : 0;
}

function nonnegative_decimal_string($value): ?string {
    if (is_int($value)) {
        return $value >= 0 ? (string)$value : null;
    }
    if (!is_string($value) || !preg_match('/^[0-9]+$/', $value)) {
        return null;
    }
    $normalized = ltrim($value, '0');
    return $normalized === '' ? '0' : $normalized;
}

function raw_bytes(array $row): ?string {
    $encoded = str_value($row, 'payload_base64', 0);
    if ($encoded === null || $encoded === '') {
        return null;
    }
    $decoded = base64_decode($encoded, true);
    return $decoded === false ? null : $decoded;
}

function json_value($value): string {
    return json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
}


function bool_int(array $row, string $key, bool $default = false): int {
    if (!array_key_exists($key, $row)) {
        return $default ? 1 : 0;
    }
    return !empty($row[$key]) ? 1 : 0;
}

function normalize_handle_value($value): string {
    $handle = trim((string)$value);
    $handle = preg_replace('/^https?:\/\/t\.me\/s?\//i', '', $handle);
    $handle = ltrim((string)$handle, '@');
    $handle = trim((string)$handle, '/');
    return mb_substr($handle, 0, 191, 'UTF-8');
}

function telegram_message_key_from_row(array $row): string {
    $messageId = int_value($row, 'telegram_message_id');
    if ($messageId <= 0) {
        $messageId = int_value($row, 'id');
    }
    $channelId = str_value($row, 'telegram_channel_id', 64);
    if ($channelId !== null && $channelId !== '') {
        return 'id:' . $channelId . ':' . $messageId;
    }
    return 'handle:' . normalize_handle_value(isset($row['handle']) ? $row['handle'] : '') . ':' . $messageId;
}

function telegram_risk_flags(string $text): array {
    $lower = mb_strtolower($text, 'UTF-8');
    $flags = array();
    foreach (array('찌라시', '루머', '카더라', '확인안됨', '미확인') as $keyword) {
        if (mb_strpos($lower, $keyword) !== false) { $flags['rumor'] = true; }
    }
    foreach (array('매수', '급등', '추천', '수익', '목표가', '리딩') as $keyword) {
        if (mb_strpos($lower, $keyword) !== false) { $flags['promotional'] = true; }
    }
    foreach (array('상장폐지', '거래정지', '불성실공시', '감사의견', '공개매수', '유상증자') as $keyword) {
        if (mb_strpos($lower, $keyword) !== false) { $flags['market_sensitive'] = true; }
    }
    if (mb_strpos($lower, '?') !== false && (mb_strpos($lower, '확인') !== false || mb_strpos($lower, '사실') !== false || mb_strpos($lower, '진위') !== false)) {
        $flags['unverified'] = true;
    }
    return array_keys($flags);
}

function text_excerpt(?string $text, int $max = 180): string {
    $text = trim((string)$text);
    $text = preg_replace('/\s+/u', ' ', $text);
    if (mb_strlen($text, 'UTF-8') <= $max) {
        return $text;
    }
    return mb_substr($text, 0, $max - 1, 'UTF-8') . '…';
}

function query_context_excerpt(?string $text, string $query, int $max = 180): string {
    $clean = trim(preg_replace('/\s+/u', ' ', (string)$text));
    if ($clean === '') {
        return '';
    }
    $tokens = search_tokens($query);
    if (!$tokens) {
        return text_excerpt($clean, $max);
    }
    $lower = mb_strtolower($clean, 'UTF-8');
    $hitIndex = null;
    foreach ($tokens as $token) {
        $needle = mb_strtolower((string)$token, 'UTF-8');
        if ($needle === '') {
            continue;
        }
        $pos = mb_strpos($lower, $needle, 0, 'UTF-8');
        if ($pos !== false) {
            $hitIndex = (int)$pos;
            break;
        }
    }
    if ($hitIndex === null) {
        return text_excerpt($clean, $max);
    }
    $start = max(0, $hitIndex - 42);
    $snippet = mb_substr($clean, $start, $max, 'UTF-8');
    $prefix = $start > 0 ? '... ' : '';
    $suffix = ($start + $max) < mb_strlen($clean, 'UTF-8') ? ' ...' : '';
    return text_excerpt('관련 문맥: ' . $prefix . $snippet . $suffix, $max + 18);
}

function decode_json_array(?string $value): array {
    if ($value === null || $value === '') {
        return array();
    }
    $decoded = json_decode($value, true);
    return is_array($decoded) ? $decoded : array();
}

function public_telegram_message(array $row, string $query = ''): array {
    $riskFlags = decode_json_array(isset($row['risk_flags_json']) ? (string)$row['risk_flags_json'] : null);
    $text = isset($row['text']) ? (string)$row['text'] : '';
    $contextExcerpt = query_context_excerpt($text, $query, 180);
    return array(
        'channel_handle' => isset($row['channel_handle']) ? (string)$row['channel_handle'] : '',
        'channel_title' => isset($row['channel_title']) && $row['channel_title'] !== null ? (string)$row['channel_title'] : (isset($row['channel_handle']) ? (string)$row['channel_handle'] : ''),
        'telegram_message_id' => isset($row['telegram_message_id']) ? (int)$row['telegram_message_id'] : 0,
        'posted_at' => isset($row['posted_at']) ? (string)$row['posted_at'] : '',
        'message_url' => isset($row['message_url']) ? (string)$row['message_url'] : '',
        'match_type' => isset($row['match_type']) ? (string)$row['match_type'] : 'keyword',
        'score' => isset($row['score']) ? (float)$row['score'] : 0,
        'reason' => isset($row['reason']) ? (string)$row['reason'] : '',
        'risk_flags' => $riskFlags,
        'excerpt' => $contextExcerpt,
        'context_excerpt' => $contextExcerpt,
    );
}

function scalar_int(PDO $pdo, string $sql, array $params = array()): int {
    $stmt = $pdo->prepare($sql);
    $stmt->execute($params);
    return (int)$stmt->fetchColumn();
}

function telegram_dashboard_message_type(string $text): string {
    $lower = mb_strtolower($text, 'UTF-8');
    if (preg_match('/공시|불성실공시|거래정지|상장폐지|정정신고서/u', $lower) === 1) {
        return '공시·규제';
    }
    if (preg_match('/실적|매출|영업이익|컨센서스|가이던스/u', $lower) === 1) {
        return '실적';
    }
    if (preg_match('/주주|행동주의|경영권|위임장|공개매수|이사회/u', $lower) === 1) {
        return '주주·지배구조';
    }
    if (preg_match('/밸류업|벨류업|배당|자사주|주주환원/u', $lower) === 1) {
        return '밸류업·환원';
    }
    if (preg_match('/환율|채권|금리|fed|미국|중국|일본/u', $lower) === 1) {
        return '매크로·해외';
    }
    return '기타';
}

function telegram_dashboard_tokens(string $text): array {
    $stopwords = array(
        '그리고' => true, '관련' => true, '기사' => true, '뉴스' => true, '시장' => true,
        '오늘' => true, '이번' => true, '지난' => true, '있는' => true, '없는' => true,
        '으로' => true, '에서' => true, '한다' => true, '했다' => true, '합니다' => true,
        '보도' => true, '공유' => true, 'https' => true, 'http' => true, 'www' => true,
    );
    preg_match_all('/[0-9A-Za-z가-힣·.\-]{2,}/u', mb_strtolower($text, 'UTF-8'), $matches);
    $tokens = array();
    foreach ($matches[0] as $token) {
        $token = trim((string)$token, ".-_·");
        if ($token === '' || isset($stopwords[$token]) || mb_strlen($token, 'UTF-8') < 2) {
            continue;
        }
        $tokens[] = mb_substr($token, 0, 40, 'UTF-8');
    }
    return array_slice(array_values(array_unique($tokens)), 0, 24);
}

function telegram_listed_company_candidates(string $text): array {
    $known = array(
        '삼성전자', '고려아연', '영풍', '풍산', '한국앤컴퍼니', 'KT&G', 'KT', 'SK스퀘어',
        'SK이노베이션', 'LG화학', 'LG전자', '한화', '한화솔루션', '현대차', '현대모비스',
        'HD현대', 'HD현대일렉트릭', 'HD현대로보틱스', '한화오션', '두산밥캣', 'DB하이텍',
        'SM엔터', '카카오', '네이버', '셀트리온', '포스코홀딩스', '우리금융', '우리금융지주',
        '일진홀딩스', '슈프리마에이치큐', '보령', '코웨이', '쿠팡', '아이로보틱스',
        '인크레더블버즈', 'Shinhan Financial Group', 'KB Financial Group', 'Samsung C&T',
        'Korea Zinc', 'LG Chem', 'Hyundai Motor', 'Hyundai Mobis'
    );
    $excluded = array_change_key_case(array(
        'NPS' => true, '국민연금' => true, 'Elliott Management' => true, 'Starboard Value' => true,
        'Third Point' => true, 'Trian Partners' => true, 'D.E. Shaw' => true, 'ValueAct' => true,
        'Sachem Head' => true, 'Saba Capital' => true, 'Browning West' => true
    ), CASE_LOWER);
    $candidates = array();
    $lower = mb_strtolower($text, 'UTF-8');
    foreach ($known as $company) {
        if (mb_strpos($text, $company) !== false || mb_strpos($lower, mb_strtolower($company, 'UTF-8')) !== false) {
            $candidates[] = $company;
        }
    }
    if (preg_match_all('/([가-힣A-Za-z0-9&]{2,}(?:금융|지주|전자|물산|제약|화학|바이오|엔터|건설|증권|은행|보험|투자|홀딩스|그룹|산업|상사|에너지|중공업|해운|통신))/u', $text, $matches)) {
        foreach ($matches[1] as $match) {
            $candidates[] = trim((string)$match);
        }
    }
    $unique = array();
    foreach ($candidates as $company) {
        $company = trim(preg_replace('/\s+/u', ' ', (string)$company), " -·,");
        if ($company === '' || isset($excluded[mb_strtolower($company, 'UTF-8')])) {
            continue;
        }
        $unique[$company] = true;
    }
    $companies = array_keys($unique);
    usort($companies, function ($left, $right) {
        return mb_strlen($right, 'UTF-8') <=> mb_strlen($left, 'UTF-8');
    });
    $filtered = array();
    foreach ($companies as $company) {
        $contained = false;
        foreach ($filtered as $existing) {
            if ($company !== $existing && mb_strpos($existing, $company) !== false) {
                $contained = true;
                break;
            }
        }
        if (!$contained) {
            $filtered[] = $company;
        }
    }
    return array_slice($filtered, 0, 4);
}

function count_rows_to_label_counts(array $counts, int $limit = 6): array {
    arsort($counts);
    $rows = array();
    foreach (array_slice($counts, 0, $limit, true) as $label => $count) {
        if ((string)$label === '') {
            continue;
        }
        $rows[] = array('label' => (string)$label, 'count' => (int)$count);
    }
    return $rows;
}

function telegram_company_signal_score(array $row): int {
    $mentions = (int)($row['mentions_14d'] ?? 0);
    $recent = (int)($row['mentions_24h'] ?? 0);
    $channels = (int)($row['channels_count'] ?? 0);
    $events = isset($row['event_types']) && is_array($row['event_types']) ? count($row['event_types']) : 0;
    $velocity = (float)($row['velocity_ratio'] ?? 0);
    $flags = isset($row['risk_flags']) && is_array($row['risk_flags']) ? $row['risk_flags'] : array();
    $score = min(30, $mentions * 3) + min(24, $channels * 8) + min(16, $recent * 5) + min(12, $events * 4);
    if ($velocity >= 2 && $recent >= 2) {
        $score += 12;
    } elseif ($velocity >= 1.3 && $recent >= 2) {
        $score += 6;
    }
    if (in_array('promotional', $flags, true)) {
        $score -= 12;
    }
    if (in_array('rumor', $flags, true) || in_array('unverified', $flags, true)) {
        $score -= 8;
    }
    if ($channels <= 1 && $mentions >= 5) {
        $score -= 10;
    }
    return max(0, min(100, (int)$score));
}

function telegram_company_lifecycle(array $row): string {
    $recent = (int)($row['mentions_24h'] ?? 0);
    $previous = (int)($row['mentions_prev_24h'] ?? 0);
    $velocity = (float)($row['velocity_ratio'] ?? 0);
    $channels = (int)($row['channels_count'] ?? 0);
    if ($recent >= 2 && $previous === 0) {
        return 'new';
    }
    if ($recent >= 2 && $velocity >= 1.4 && $channels >= 2) {
        return 'rising';
    }
    if ($recent > 0) {
        return 'active';
    }
    return 'fading';
}

function telegram_company_bucket(array $row): string {
    $flags = isset($row['risk_flags']) && is_array($row['risk_flags']) ? $row['risk_flags'] : array();
    if (array_intersect($flags, array('rumor', 'promotional', 'unverified'))) {
        return 'risk_watch';
    }
    return in_array((string)($row['lifecycle'] ?? ''), array('new', 'rising'), true) ? 'new_rising' : 'tracked_company';
}

function telegram_company_signal_rows(PDO $pdo, string $messagesTable, string $channelsTable, string $referenceSql): array {
    $stmt = $pdo->prepare(
        'SELECT m.channel_handle, COALESCE(c.title, m.channel_handle) AS channel_title, m.posted_at, m.message_url, m.text, m.normalized_text, '
        . 'CASE WHEN m.posted_at >= DATE_SUB(' . $referenceSql . ', INTERVAL 24 HOUR) THEN 1 ELSE 0 END AS is_recent, '
        . 'CASE WHEN m.posted_at >= DATE_SUB(' . $referenceSql . ', INTERVAL 48 HOUR) AND m.posted_at < DATE_SUB(' . $referenceSql . ', INTERVAL 24 HOUR) THEN 1 ELSE 0 END AS is_previous '
        . 'FROM ' . $messagesTable . ' m LEFT JOIN ' . $channelsTable . ' c ON c.handle = m.channel_handle '
        . 'WHERE m.deleted_at IS NULL AND m.posted_at >= DATE_SUB(' . $referenceSql . ', INTERVAL 14 DAY) '
        . 'ORDER BY m.posted_at DESC LIMIT 20000'
    );
    $stmt->execute();
    $grouped = array();
    foreach ($stmt->fetchAll() as $row) {
        $text = (string)($row['normalized_text'] ?: $row['text'] ?: '');
        $companies = telegram_listed_company_candidates($text);
        if (!$companies) {
            continue;
        }
        $handle = (string)($row['channel_handle'] ?: $row['channel_title'] ?: 'unknown');
        $eventType = telegram_dashboard_message_type($text);
        $flags = telegram_risk_flags($text);
        foreach ($companies as $company) {
            if (!isset($grouped[$company])) {
                $grouped[$company] = array(
                    'company' => $company,
                    'mentions_14d' => 0,
                    'mentions_24h' => 0,
                    'mentions_prev_24h' => 0,
                    'channels' => array(),
                    'event_types' => array(),
                    'risk_flags' => array(),
                    'top_messages' => array(),
                    'latest_at' => '',
                );
            }
            $grouped[$company]['mentions_14d']++;
            if ((int)$row['is_recent'] === 1) {
                $grouped[$company]['mentions_24h']++;
            } elseif ((int)$row['is_previous'] === 1) {
                $grouped[$company]['mentions_prev_24h']++;
            }
            $grouped[$company]['channels'][$handle] = ($grouped[$company]['channels'][$handle] ?? 0) + 1;
            $grouped[$company]['event_types'][$eventType] = ($grouped[$company]['event_types'][$eventType] ?? 0) + 1;
            foreach ($flags as $flag) {
                $grouped[$company]['risk_flags'][$flag] = ($grouped[$company]['risk_flags'][$flag] ?? 0) + 1;
            }
            $postedAt = (string)($row['posted_at'] ?: '');
            if ($postedAt > $grouped[$company]['latest_at']) {
                $grouped[$company]['latest_at'] = $postedAt;
            }
            if (count($grouped[$company]['top_messages']) < 5) {
                $grouped[$company]['top_messages'][] = array(
                    'channel_title' => (string)($row['channel_title'] ?: $handle),
                    'channel_handle' => $handle,
                    'posted_at' => $postedAt,
                    'message_url' => (string)($row['message_url'] ?: ''),
                    'excerpt' => text_excerpt($text, 120),
                    'event_type' => $eventType,
                    'risk_flags' => $flags,
                );
            }
        }
    }
    $rows = array();
    foreach ($grouped as $company => $row) {
        $previous = (int)$row['mentions_prev_24h'];
        $recent = (int)$row['mentions_24h'];
        $public = array(
            'company' => $company,
            'mentions_14d' => (int)$row['mentions_14d'],
            'mentions_24h' => $recent,
            'mentions_prev_24h' => $previous,
            'channels_count' => count($row['channels']),
            'top_channels' => count_rows_to_label_counts($row['channels'], 6),
            'event_types' => count_rows_to_label_counts($row['event_types'], 5),
            'risk_flags' => array_map(function ($item) { return (string)$item['label']; }, count_rows_to_label_counts($row['risk_flags'], 6)),
            'velocity_ratio' => $previous > 0 ? round($recent / $previous, 2) : ($recent > 0 ? (float)$recent : 0.0),
            'top_messages' => $row['top_messages'],
            'latest_at' => (string)$row['latest_at'],
        );
        $public['signal_score'] = telegram_company_signal_score($public);
        $public['lifecycle'] = telegram_company_lifecycle($public);
        $public['analysis_bucket'] = telegram_company_bucket($public);
        $rows[] = $public;
    }
    usort($rows, function ($left, $right) {
        foreach (array('signal_score', 'mentions_24h', 'channels_count', 'mentions_14d') as $key) {
            $diff = (int)($right[$key] ?? 0) <=> (int)($left[$key] ?? 0);
            if ($diff !== 0) {
                return $diff;
            }
        }
        return strcmp((string)($right['latest_at'] ?? ''), (string)($left['latest_at'] ?? ''));
    });
    return $rows;
}

function telegram_company_signal_overview(array $rows): array {
    $buckets = array('new_rising' => 0, 'risk_watch' => 0, 'tracked_company' => 0);
    $topScore = 0;
    foreach ($rows as $row) {
        $bucket = (string)($row['analysis_bucket'] ?? 'tracked_company');
        if (!isset($buckets[$bucket])) {
            $buckets[$bucket] = 0;
        }
        $buckets[$bucket]++;
        $topScore = max($topScore, (int)($row['signal_score'] ?? 0));
    }
    return array(
        'companies_total' => count($rows),
        'top_score' => $topScore,
        'new_rising' => $buckets['new_rising'],
        'risk_watch' => $buckets['risk_watch'],
        'tracked' => $buckets['tracked_company'],
    );
}

function public_telegram_signal(array $row, ?array $allowedHandles = null): array {
    $payload = decode_json_array(isset($row['payload_json']) ? (string)$row['payload_json'] : null);
    $messages = array();
    foreach ((isset($payload['top_related_messages']) && is_array($payload['top_related_messages'])) ? $payload['top_related_messages'] : array() as $message) {
        if (!is_array($message)) {
            continue;
        }
        $messageHandle = mb_strtolower(normalize_handle_value(isset($message['channel_handle']) ? $message['channel_handle'] : ''), 'UTF-8');
        if ($allowedHandles !== null && ($messageHandle === '' || !isset($allowedHandles[$messageHandle]))) {
            continue;
        }
        $messages[] = array(
            'channel_title' => isset($message['channel_title']) ? (string)$message['channel_title'] : '',
            'channel_handle' => isset($message['channel_handle']) ? (string)$message['channel_handle'] : '',
            'posted_at' => isset($message['posted_at']) ? (string)$message['posted_at'] : '',
            'message_url' => isset($message['message_url']) ? (string)$message['message_url'] : '',
            'excerpt' => text_excerpt(isset($message['excerpt']) ? (string)$message['excerpt'] : '', 140),
            'views' => isset($message['views']) ? (int)$message['views'] : 0,
            'forwards' => isset($message['forwards']) ? (int)$message['forwards'] : 0,
            'match_type' => isset($message['match_type']) ? (string)$message['match_type'] : '',
            'score' => isset($message['score']) ? (float)$message['score'] : 0,
            'reason' => isset($message['reason']) ? (string)$message['reason'] : '',
        );
    }
    $topChannels = (isset($payload['top_channels']) && is_array($payload['top_channels'])) ? $payload['top_channels'] : array();
    if ($allowedHandles !== null) {
        $topChannels = array_values(array_filter($topChannels, function ($handle) use ($allowedHandles) {
            $key = mb_strtolower(normalize_handle_value($handle), 'UTF-8');
            return $key !== '' && isset($allowedHandles[$key]);
        }));
    }
    $channelKeys = array();
    foreach ($messages as $message) {
        $key = mb_strtolower(normalize_handle_value(isset($message['channel_handle']) ? $message['channel_handle'] : ''), 'UTF-8');
        if ($key !== '') { $channelKeys[$key] = true; }
    }
    foreach ($topChannels as $handle) {
        $key = mb_strtolower(normalize_handle_value($handle), 'UTF-8');
        if ($key !== '') { $channelKeys[$key] = true; }
    }
    $publicMessageCount = $allowedHandles === null
        ? (isset($row['related_telegram_count']) ? (int)$row['related_telegram_count'] : 0)
        : count($messages);
    $publicChannelCount = $allowedHandles === null
        ? (isset($row['related_telegram_channels_count']) ? (int)$row['related_telegram_channels_count'] : 0)
        : count($channelKeys);
    return array(
        'article_id' => isset($row['article_id']) ? (string)$row['article_id'] : '',
        'signal_type' => isset($payload['signal_type']) ? (string)$payload['signal_type'] : 'article_match',
        'signal_title' => isset($payload['signal_title']) ? text_excerpt((string)$payload['signal_title'], 120) : (isset($row['article_id']) ? (string)$row['article_id'] : ''),
        'related_telegram_count' => $publicMessageCount,
        'related_telegram_channels_count' => $publicChannelCount,
        'direct_url_count' => isset($payload['direct_url_count']) ? (int)$payload['direct_url_count'] : 0,
        'keyword_match_count' => isset($payload['keyword_match_count']) ? (int)$payload['keyword_match_count'] : 0,
        'first_seen_at' => isset($row['first_seen_at']) ? (string)$row['first_seen_at'] : '',
        'latest_seen_at' => isset($row['latest_seen_at']) ? (string)$row['latest_seen_at'] : '',
        'confidence_score' => isset($row['confidence_score']) ? (float)$row['confidence_score'] : 0,
        'signal_summary' => isset($payload['signal_summary']) ? text_excerpt((string)$payload['signal_summary'], 120) : '',
        'top_channels' => array_slice($topChannels, 0, 8),
        'top_channel_counts' => $allowedHandles === null && isset($payload['top_channel_counts']) && is_array($payload['top_channel_counts']) ? array_slice($payload['top_channel_counts'], 0, 8) : array(),
        'top_keywords' => (isset($payload['top_keywords']) && is_array($payload['top_keywords'])) ? array_slice($payload['top_keywords'], 0, 8) : array(),
        'risk_flags' => (isset($payload['risk_flags']) && is_array($payload['risk_flags'])) ? array_slice($payload['risk_flags'], 0, 8) : array(),
        'top_related_messages' => array_slice($messages, 0, 5),
    );
}

function telegram_signal_risk_flags(array $signal): array {
    $flags = array();
    foreach ((isset($signal['risk_flags']) && is_array($signal['risk_flags'])) ? $signal['risk_flags'] : array() as $flag) {
        $flag = (string)$flag;
        if ($flag !== '') {
            $flags[$flag] = true;
        }
    }
    if ((int)($signal['related_telegram_channels_count'] ?? 0) <= 1 && (int)($signal['related_telegram_count'] ?? 0) >= 5) {
        $flags['single_channel_spike'] = true;
    }
    $labels = array_keys($flags);
    sort($labels);
    return $labels;
}

function telegram_signal_age_hours(?string $value): float {
    $time = $value ? strtotime($value) : false;
    if ($time === false) {
        return 0.0;
    }
    return max(0.0, (time() - $time) / 3600.0);
}

function telegram_signal_lifecycle(array $signal): string {
    $firstAge = telegram_signal_age_hours(isset($signal['first_seen_at']) ? (string)$signal['first_seen_at'] : null);
    $latestAge = telegram_signal_age_hours(isset($signal['latest_seen_at']) ? (string)$signal['latest_seen_at'] : null);
    $count = (int)($signal['related_telegram_count'] ?? 0);
    $channels = (int)($signal['related_telegram_channels_count'] ?? 0);
    if ($firstAge <= 24 && $latestAge <= 8) {
        return 'new';
    }
    if ($latestAge <= 12 && ($channels >= 2 || $count >= 5)) {
        return 'rising';
    }
    if ($latestAge <= 36) {
        return 'active';
    }
    if ($latestAge <= 96) {
        return 'fading';
    }
    return 'stale';
}

function telegram_signal_score(array $signal): int {
    $count = (int)($signal['related_telegram_count'] ?? 0);
    $channels = (int)($signal['related_telegram_channels_count'] ?? 0);
    $confidence = (float)($signal['confidence_score'] ?? 0);
    $latestAge = telegram_signal_age_hours(isset($signal['latest_seen_at']) ? (string)$signal['latest_seen_at'] : null);
    $freshness = $latestAge <= 6 ? 14 : ($latestAge <= 24 ? 10 : ($latestAge <= 72 ? 5 : 0));
    $score = min(26, $count * 4) + min(30, $channels * 10) + min(22, (int)round($confidence * 22)) + $freshness;
    $flags = telegram_signal_risk_flags($signal);
    if (in_array('promotional', $flags, true)) {
        $score -= 14;
    }
    if (in_array('rumor', $flags, true) || in_array('unverified', $flags, true)) {
        $score -= 8;
    }
    if (in_array('single_channel_spike', $flags, true)) {
        $score -= 10;
    }
    return max(0, min(100, (int)$score));
}

function telegram_signal_bucket(array $signal): string {
    $flags = telegram_signal_risk_flags($signal);
    if (array_intersect($flags, array('rumor', 'promotional', 'unverified', 'single_channel_spike'))) {
        return 'risk_watch';
    }
    if (in_array((string)($signal['signal_type'] ?? ''), array('topic_burst', 'url_burst'), true)) {
        return 'watchlist_candidate';
    }
    return in_array((string)($signal['lifecycle'] ?? telegram_signal_lifecycle($signal)), array('new', 'rising'), true)
        ? 'new_rising'
        : 'confirmed_reaction';
}

function telegram_enrich_signal(array $signal): array {
    $signal['risk_flags'] = telegram_signal_risk_flags($signal);
    $signal['lifecycle'] = telegram_signal_lifecycle($signal);
    $signal['signal_score'] = telegram_signal_score($signal);
    $signal['analysis_bucket'] = telegram_signal_bucket($signal);
    return $signal;
}

function telegram_signal_overview(array $signals, array $counts): array {
    $buckets = array('new_rising' => 0, 'watchlist_candidate' => 0, 'risk_watch' => 0, 'confirmed_reaction' => 0);
    $topScore = 0;
    foreach ($signals as $signal) {
        $bucket = (string)($signal['analysis_bucket'] ?? 'confirmed_reaction');
        if (!isset($buckets[$bucket])) {
            $buckets[$bucket] = 0;
        }
        $buckets[$bucket]++;
        $topScore = max($topScore, (int)($signal['signal_score'] ?? 0));
    }
    $recent = (int)($counts['messages_24h'] ?? 0);
    $previous = (int)($counts['messages_prev_24h'] ?? 0);
    $ratio = $previous > 0 ? round($recent / $previous, 2) : ($recent > 0 ? (float)$recent : 0.0);
    $label = ($ratio >= 1.4 && $recent >= 5) ? 'rising' : (($previous > 0 && $ratio <= 0.7) ? 'cooling' : 'steady');
    return array(
        'top_score' => $topScore,
        'new_rising' => $buckets['new_rising'],
        'watchlist_candidates' => $buckets['watchlist_candidate'],
        'risk_watch' => $buckets['risk_watch'],
        'confirmed_reactions' => $buckets['confirmed_reaction'],
        'recent_24h' => $recent,
        'previous_24h' => $previous,
        'velocity_ratio' => $ratio,
        'velocity_label' => $label,
    );
}

function telegram_risk_flag_counts(array $signals): array {
    $counts = array();
    foreach ($signals as $signal) {
        foreach (telegram_signal_risk_flags($signal) as $flag) {
            $counts[$flag] = isset($counts[$flag]) ? $counts[$flag] + 1 : 1;
        }
    }
    arsort($counts);
    return array_map(
        function ($label, $count) { return array('label' => (string)$label, 'count' => (int)$count); },
        array_keys($counts),
        array_values($counts)
    );
}

function telegram_admin_access_hash(array $config): string {
    if (isset($config['telegram_admin_access_token_hash'])) {
        $hash = strtolower(trim((string)$config['telegram_admin_access_token_hash']));
        if (preg_match('/^[a-f0-9]{64}$/', $hash)) {
            return $hash;
        }
    }
    if (isset($config['telegram_admin_access_token'])) {
        $token = trim((string)$config['telegram_admin_access_token']);
        if ($token !== '') {
            return hash('sha256', $token);
        }
    }
    return '';
}

function require_telegram_admin_access(array $config): void {
    $expected = telegram_admin_access_hash($config);
    if ($expected === '') {
        respond(403, array('ok' => false, 'error' => 'admin_token_not_configured'));
    }
    $token = '';
    if (isset($_SERVER['HTTP_X_TELEGRAM_ADMIN_TOKEN'])) {
        $token = trim((string)$_SERVER['HTTP_X_TELEGRAM_ADMIN_TOKEN']);
    }
    if ($token === '' || !hash_equals($expected, hash('sha256', $token))) {
        respond(403, array('ok' => false, 'error' => 'admin_token_required'));
    }
}

function handle_telegram_dashboard(PDO $pdo, array $config): void {
    require_telegram_admin_access($config);
    $channels = table_name($config, 'telegram_channels');
    $messages = table_name($config, 'telegram_messages');
    $matches = table_name($config, 'telegram_article_matches');
    $signals = table_name($config, 'telegram_issue_signals');
    $referenceSql = '(SELECT COALESCE(MAX(posted_at), NOW()) FROM ' . $messages . ' WHERE deleted_at IS NULL)';

    $counts = array(
        'channels_total' => scalar_int($pdo, 'SELECT COUNT(*) FROM ' . $channels),
        'channels_collectable' => scalar_int($pdo, 'SELECT COUNT(*) FROM ' . $channels . ' WHERE is_public_channel = 1'),
        'channels_enabled' => scalar_int($pdo, 'SELECT COUNT(*) FROM ' . $channels . ' WHERE is_public_channel = 1 AND enabled = 1'),
        'channels_failed' => scalar_int($pdo, 'SELECT COUNT(*) FROM ' . $channels . ' WHERE is_public_channel = 1 AND enabled = 1 AND last_error IS NOT NULL AND last_error <> ""'),
        'messages_total' => scalar_int($pdo, 'SELECT COUNT(*) FROM ' . $messages . ' WHERE deleted_at IS NULL'),
        'messages_24h' => scalar_int($pdo, 'SELECT COUNT(*) FROM ' . $messages . ' WHERE deleted_at IS NULL AND posted_at >= DATE_SUB(' . $referenceSql . ', INTERVAL 24 HOUR)'),
        'messages_prev_24h' => scalar_int($pdo, 'SELECT COUNT(*) FROM ' . $messages . ' WHERE deleted_at IS NULL AND posted_at >= DATE_SUB(' . $referenceSql . ', INTERVAL 48 HOUR) AND posted_at < DATE_SUB(' . $referenceSql . ', INTERVAL 24 HOUR)'),
        'messages_14d' => scalar_int($pdo, 'SELECT COUNT(*) FROM ' . $messages . ' WHERE deleted_at IS NULL AND posted_at >= DATE_SUB(' . $referenceSql . ', INTERVAL 14 DAY)'),
        'matches_total' => scalar_int($pdo, 'SELECT COUNT(*) FROM ' . $matches),
        'signals_total' => scalar_int($pdo, 'SELECT COUNT(*) FROM ' . $signals),
    );

    $matchByChannel = array();
    $stmt = $pdo->prepare(
        'SELECT channel_handle, COUNT(*) AS matches, '
        . 'SUM(CASE WHEN match_type IN ("exact_url","canonical_url") THEN 1 ELSE 0 END) AS direct_matches, '
        . 'SUM(CASE WHEN match_type NOT IN ("exact_url","canonical_url") THEN 1 ELSE 0 END) AS weak_matches, '
        . 'AVG(score) AS avg_score '
        . 'FROM ' . $matches . ' GROUP BY channel_handle'
    );
    $stmt->execute();
    foreach ($stmt->fetchAll() as $row) {
        $matchByChannel[(string)$row['channel_handle']] = array(
            'matches' => (int)$row['matches'],
            'direct_matches' => (int)$row['direct_matches'],
            'weak_matches' => (int)$row['weak_matches'],
            'avg_score' => isset($row['avg_score']) ? round((float)$row['avg_score'], 4) : 0,
        );
    }

    $riskByChannel = array();
    $stmt = $pdo->prepare(
        'SELECT channel_handle, COUNT(*) AS risk_messages FROM ' . $messages
        . ' WHERE deleted_at IS NULL AND risk_flags_json IS NOT NULL AND risk_flags_json <> "" AND risk_flags_json <> "[]" '
        . 'GROUP BY channel_handle'
    );
    $stmt->execute();
    foreach ($stmt->fetchAll() as $row) {
        $riskByChannel[(string)$row['channel_handle']] = (int)$row['risk_messages'];
    }

    $stmt = $pdo->prepare(
        'SELECT c.handle, c.title, c.quality_score, c.enabled, c.last_error, c.last_collected_at, c.last_message_id, '
        . 'COUNT(m.message_key) AS messages, MAX(m.posted_at) AS latest_at '
        . 'FROM ' . $channels . ' c '
        . 'LEFT JOIN ' . $messages . ' m ON m.channel_handle = c.handle AND m.deleted_at IS NULL '
        . 'WHERE c.is_public_channel = 1 '
        . 'GROUP BY c.handle, c.title, c.quality_score, c.enabled, c.last_error, c.last_collected_at, c.last_message_id '
        . 'ORDER BY messages DESC, latest_at DESC, c.quality_score DESC LIMIT 40'
    );
    $stmt->execute();
    $topChannels = array();
    foreach ($stmt->fetchAll() as $row) {
        $handle = (string)$row['handle'];
        $channelMatches = isset($matchByChannel[$handle]) ? $matchByChannel[$handle] : array('matches' => 0, 'direct_matches' => 0, 'weak_matches' => 0, 'avg_score' => 0);
        $messageCount = (int)$row['messages'];
        $riskMessages = isset($riskByChannel[$handle]) ? (int)$riskByChannel[$handle] : 0;
        $baseQuality = (int)$row['quality_score'];
        $matchRate = $messageCount > 0 ? $channelMatches['matches'] / $messageCount : 0;
        $riskRate = $messageCount > 0 ? $riskMessages / $messageCount : 0;
        $signalQuality = max(0, min(100, $baseQuality + min(10, (int)floor($messageCount / 250)) + min(18, $channelMatches['direct_matches'] * 3) + min(8, $channelMatches['weak_matches']) + min(14, (int)floor($matchRate * 42)) - min(24, (int)floor($riskRate * 48))));
        $topChannels[] = array(
            'handle' => $handle,
            'title' => (string)($row['title'] ?: $row['handle']),
            'quality_score' => $baseQuality,
            'signal_quality_score' => $signalQuality,
            'enabled' => (int)$row['enabled'],
            'messages' => $messageCount,
            'matches' => $channelMatches['matches'],
            'direct_matches' => $channelMatches['direct_matches'],
            'weak_matches' => $channelMatches['weak_matches'],
            'match_rate' => round($matchRate, 4),
            'avg_match_score' => $channelMatches['avg_score'],
            'risk_messages' => $riskMessages,
            'risk_rate' => round($riskRate, 4),
            'latest_at' => (string)($row['latest_at'] ?: ''),
            'last_collected_at' => (string)($row['last_collected_at'] ?: ''),
            'last_message_id' => (int)$row['last_message_id'],
            'last_error' => (string)($row['last_error'] ?: ''),
        );
    }
    usort($topChannels, function ($left, $right) {
        foreach (array('signal_quality_score', 'matches', 'messages') as $key) {
            $diff = (int)$right[$key] <=> (int)$left[$key];
            if ($diff !== 0) {
                return $diff;
            }
        }
        return strcmp((string)$right['latest_at'], (string)$left['latest_at']);
    });

    $stmt = $pdo->prepare(
        'SELECT DATE(posted_at) AS day_key, COUNT(*) AS count_value FROM ' . $messages
        . ' WHERE deleted_at IS NULL AND posted_at >= DATE_SUB(' . $referenceSql . ', INTERVAL 21 DAY) '
        . 'GROUP BY DATE(posted_at) ORDER BY day_key ASC'
    );
    $stmt->execute();
    $dayCounts = array();
    foreach ($stmt->fetchAll() as $row) {
        $dayCounts[] = array((string)$row['day_key'], (int)$row['count_value']);
    }

    $stmt = $pdo->prepare(
        'SELECT text, normalized_text FROM ' . $messages
        . ' WHERE deleted_at IS NULL AND posted_at >= DATE_SUB(' . $referenceSql . ', INTERVAL 14 DAY) '
        . 'ORDER BY posted_at DESC LIMIT 10000'
    );
    $stmt->execute();
    $typeCounts = array();
    $keywordCounts = array();
    $bytes = 0;
    $sampleCount = 0;
    foreach ($stmt->fetchAll() as $row) {
        $text = (string)($row['normalized_text'] ?: $row['text'] ?: '');
        $type = telegram_dashboard_message_type($text);
        $typeCounts[$type] = isset($typeCounts[$type]) ? $typeCounts[$type] + 1 : 1;
        foreach (telegram_dashboard_tokens($text) as $token) {
            $keywordCounts[$token] = isset($keywordCounts[$token]) ? $keywordCounts[$token] + 1 : 1;
        }
        if ($sampleCount < 500) {
            $bytes += strlen($text);
            $sampleCount++;
        }
    }
    arsort($typeCounts);
    arsort($keywordCounts);

    $stmt = $pdo->prepare(
        'SELECT article_id, related_telegram_count, related_telegram_channels_count, first_seen_at, latest_seen_at, confidence_score, payload_json '
        . 'FROM ' . $signals . ' ORDER BY confidence_score DESC, related_telegram_channels_count DESC, related_telegram_count DESC LIMIT 20'
    );
    $stmt->execute();
    $signalRows = array();
    foreach ($stmt->fetchAll() as $row) {
        $signalRows[] = telegram_enrich_signal(public_telegram_signal($row));
    }
    usort($signalRows, function ($left, $right) {
        $score = (int)($right['signal_score'] ?? 0) <=> (int)($left['signal_score'] ?? 0);
        if ($score !== 0) {
            return $score;
        }
        return (float)($right['confidence_score'] ?? 0) <=> (float)($left['confidence_score'] ?? 0);
    });
    $newRisingSignals = array_values(array_filter($signalRows, function ($signal) { return ($signal['analysis_bucket'] ?? '') === 'new_rising'; }));
    $watchlistSignals = array_values(array_filter($signalRows, function ($signal) { return ($signal['analysis_bucket'] ?? '') === 'watchlist_candidate'; }));
    $riskWatchSignals = array_values(array_filter($signalRows, function ($signal) { return ($signal['analysis_bucket'] ?? '') === 'risk_watch'; }));
    $companyRows = telegram_company_signal_rows($pdo, $messages, $channels, $referenceSql);
    $newCompanyRows = array_values(array_filter($companyRows, function ($row) { return ($row['analysis_bucket'] ?? '') === 'new_rising'; }));
    $companyRiskRows = array_values(array_filter($companyRows, function ($row) { return ($row['analysis_bucket'] ?? '') === 'risk_watch'; }));

    $matchTypeRows = array();
    $stmt = $pdo->prepare('SELECT match_type AS label, COUNT(*) AS count FROM ' . $matches . ' GROUP BY match_type ORDER BY count DESC');
    $stmt->execute();
    foreach ($stmt->fetchAll() as $row) {
        $matchTypeRows[] = array('label' => (string)$row['label'], 'count' => (int)$row['count']);
    }

    $qualityBands = array('80+' => 0, '60-79' => 0, '40-59' => 0, '0-39' => 0);
    foreach ($topChannels as $row) {
        $score = isset($row['signal_quality_score']) ? (int)$row['signal_quality_score'] : 0;
        if ($score >= 80) {
            $qualityBands['80+']++;
        } elseif ($score >= 60) {
            $qualityBands['60-79']++;
        } elseif ($score >= 40) {
            $qualityBands['40-59']++;
        } else {
            $qualityBands['0-39']++;
        }
    }

    $dailyMessages = $counts['messages_14d'] > 0 ? $counts['messages_14d'] / 14 : 0;
    $avgBytes = $sampleCount > 0 ? max(1, (int)round($bytes / $sampleCount)) : 0;
    respond(200, array(
        'ok' => true,
        'generated_at' => gmdate('c'),
        'source' => 'db',
        'counts' => $counts,
        'top_channels' => $topChannels,
        'type_counts' => array_map(
            function ($label, $count) { return array('label' => (string)$label, 'count' => (int)$count); },
            array_keys($typeCounts),
            array_values($typeCounts)
        ),
        'day_counts' => $dayCounts,
        'top_keywords' => array_map(
            function ($label, $count) { return array('label' => (string)$label, 'count' => (int)$count); },
            array_slice(array_keys($keywordCounts), 0, 30),
            array_slice(array_values($keywordCounts), 0, 30)
        ),
        'signals' => array_slice($signalRows, 0, 12),
        'signal_overview' => telegram_signal_overview($signalRows, $counts),
        'new_rising_signals' => array_slice($newRisingSignals, 0, 8),
        'watchlist_candidates' => array_slice($watchlistSignals, 0, 8),
        'risk_watch_signals' => array_slice($riskWatchSignals, 0, 8),
        'risk_flag_counts' => telegram_risk_flag_counts($signalRows),
        'company_signal_overview' => telegram_company_signal_overview($companyRows),
        'top_company_signals' => array_slice($companyRows, 0, 16),
        'new_rising_companies' => array_slice($newCompanyRows, 0, 8),
        'company_risk_watch' => array_slice($companyRiskRows, 0, 8),
        'match_type_counts' => $matchTypeRows,
        'quality_bands' => array_map(
            function ($label, $count) { return array('label' => (string)$label, 'count' => (int)$count); },
            array_keys($qualityBands),
            array_values($qualityBands)
        ),
        'growth' => array(
            'avg_message_bytes' => $avgBytes,
            'daily_messages' => round($dailyMessages, 1),
            'monthly_messages' => (int)round($dailyMessages * 30),
            'yearly_messages' => (int)round($dailyMessages * 365),
            'monthly_mb' => $avgBytes ? round($dailyMessages * 30 * $avgBytes / 1024 / 1024, 2) : 0,
            'yearly_mb' => $avgBytes ? round($dailyMessages * 365 * $avgBytes / 1024 / 1024, 2) : 0,
        ),
    ));
}

function handle_write(string $action, array $config): void {
    $allowed = array(
        'upsert_snapshot',
        'upsert_report',
        'upsert_telegram_snapshot',
        'upsert_governance_snapshot',
        'upsert_official_site_snapshot',
        'upsert_editorial_snapshot',
        'enqueue_delivery_outbox',
        'claim_delivery_outbox',
        'ack_delivery_outbox',
        'fail_delivery_outbox',
        'export_runtime_state',
        'enqueue_link_discoveries',
        'claim_link_discoveries',
        'resolve_link_discovery',
        'telegram_snapshot_capabilities',
        'schema',
    );
    if (!in_array($action, $allowed, true)) {
        respond(404, array('ok' => false, 'error' => 'unknown_action'));
    }
    $body = read_body($config);
    $nonce = require_signature($body, $config);
    $payload = $body === '' ? array() : decode_json_body($body);
    $pdo = pdo_conn($config);
    ensure_schema($pdo, $config);
    remember_nonce($pdo, $config, $nonce);

    if ($action === 'schema') {
        respond(200, array('ok' => true, 'schema' => 'ready'));
    }
    if ($action === 'telegram_snapshot_capabilities') {
        $signalRebuildState = $pdo->prepare('SELECT live_revision FROM ' . table_name($config, 'telegram_signal_rebuild_state') . ' WHERE state_key = ?');
        $signalRebuildState->execute(array('global'));
        $liveRevision = $signalRebuildState->fetchColumn();
        if ($liveRevision === false) {
            throw new RuntimeException('signal_rebuild_state_missing');
        }
        respond(200, array(
            'ok' => true,
            'signal_rebuild_protocol' => 'staging-v1',
            'live_revision' => (int)$liveRevision,
            'max_payload_bytes' => isset($config['max_body_bytes']) ? (int)$config['max_body_bytes'] : 2097152,
        ));
    }
    if ($action === 'upsert_snapshot') {
        upsert_snapshot($pdo, $config, $payload);
    }
    if ($action === 'upsert_report') {
        upsert_report($pdo, $config, isset($payload['report']) && is_array($payload['report']) ? $payload['report'] : $payload);
    }
    if ($action === 'upsert_telegram_snapshot') {
        upsert_telegram_snapshot($pdo, $config, $payload);
    }
    if ($action === 'upsert_governance_snapshot') {
        v1_require_schema_version($pdo, $config);
        upsert_governance_snapshot($pdo, $config, $payload);
    }
    if ($action === 'upsert_official_site_snapshot') {
        v1_require_schema_version($pdo, $config);
        $payloadObject = $body === '' ? null : json_decode($body);
        if (!is_object($payloadObject)) {
            respond(400, array('ok' => false, 'error' => 'invalid_json_object'));
        }
        upsert_official_site_snapshot($pdo, $config, $payload, $payloadObject);
    }
    if ($action === 'upsert_editorial_snapshot') {
        upsert_editorial_snapshot($pdo, $config, $payload);
    }
    if ($action === 'enqueue_delivery_outbox') {
        enqueue_delivery_outbox($pdo, $config, $payload);
    }
    if ($action === 'claim_delivery_outbox') {
        claim_delivery_outbox($pdo, $config, $payload);
    }
    if ($action === 'ack_delivery_outbox') {
        ack_delivery_outbox($pdo, $config, $payload);
    }
    if ($action === 'fail_delivery_outbox') {
        fail_delivery_outbox($pdo, $config, $payload);
    }
    if ($action === 'export_runtime_state') {
        export_runtime_state($pdo, $config, $payload);
    }
    if ($action === 'enqueue_link_discoveries') {
        enqueue_link_discoveries($pdo, $config, $payload);
    }
    if ($action === 'claim_link_discoveries') {
        claim_link_discoveries($pdo, $config, $payload);
    }
    if ($action === 'resolve_link_discovery') {
        resolve_link_discovery($pdo, $config, $payload);
    }
    respond(404, array('ok' => false, 'error' => 'unknown_action'));
}

function upsert_snapshot(PDO $pdo, array $config, array $payload): void {
    $articles = isset($payload['articles']) && is_array($payload['articles']) ? $payload['articles'] : array();
    $rawRecords = isset($payload['raw_records']) && is_array($payload['raw_records']) ? $payload['raw_records'] : array();
    $stories = isset($payload['stories']) && is_array($payload['stories']) ? $payload['stories'] : array();
    $run = isset($payload['run']) && is_array($payload['run']) ? $payload['run'] : array();
    if (count($articles) > 2000 || count($rawRecords) > 2000 || count($stories) > 500) {
        respond(413, array('ok' => false, 'error' => 'too_many_records'));
    }
    $now = gmdate('Y-m-d H:i:s');
    $pdo->beginTransaction();
    try {
        $articleStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'articles') . ' (
            record_id, canonical_url_hash, title_hash, canonical_url, title, normalized_title, summary, source, feed_name,
            feed_category, image_url, published_at, seen_at, status, reason, relevance_level, priority_score,
            priority_level, story_key, source_right_id, sort_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE canonical_url_hash=VALUES(canonical_url_hash), title_hash=VALUES(title_hash), canonical_url=VALUES(canonical_url),
            title=VALUES(title), normalized_title=VALUES(normalized_title), summary=VALUES(summary), source=VALUES(source), feed_name=VALUES(feed_name),
            feed_category=VALUES(feed_category), image_url=VALUES(image_url), published_at=VALUES(published_at), seen_at=VALUES(seen_at),
            status=VALUES(status), reason=VALUES(reason), relevance_level=VALUES(relevance_level), priority_score=VALUES(priority_score),
            priority_level=VALUES(priority_level), story_key=VALUES(story_key), source_right_id=VALUES(source_right_id), sort_at=VALUES(sort_at), updated_at=VALUES(updated_at)');
        foreach ($articles as $article) {
            if (!is_array($article)) { continue; }
            $recordId = str_value($article, 'record_id', 96);
            if ($recordId === null || $recordId === '') { continue; }
            $publishedAt = mysql_dt(isset($article['published_at']) ? $article['published_at'] : null);
            $seenAt = mysql_dt(isset($article['seen_at']) ? $article['seen_at'] : null);
            $articleStmt->execute(array(
                $recordId,
                str_value($article, 'canonical_url_hash', 96),
                str_value($article, 'title_hash', 96),
                str_value($article, 'canonical_url', 65535),
                str_value($article, 'title', 700),
                str_value($article, 'normalized_title', 700),
                str_value($article, 'summary', 1048576),
                str_value($article, 'source', 255),
                str_value($article, 'feed_name', 255),
                str_value($article, 'feed_category', 80),
                str_value($article, 'image_url', 65535),
                $publishedAt,
                $seenAt,
                str_value($article, 'status', 40),
                str_value($article, 'reason', 120),
                str_value($article, 'relevance_level', 40),
                int_value($article, 'priority_score'),
                str_value($article, 'priority_level', 40),
                str_value($article, 'story_key', 120),
                str_value($article, 'source_right_id', 64),
                $publishedAt !== null ? $publishedAt : ($seenAt !== null ? $seenAt : $now),
                $now,
            ));
        }

        $rawStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'article_raw') . ' (
            raw_id, record_id, raw_kind, payload_hash, compression, payload_compressed, schema_version, retained_until, created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE record_id=VALUES(record_id), compression=VALUES(compression), payload_compressed=VALUES(payload_compressed),
            schema_version=VALUES(schema_version), retained_until=VALUES(retained_until), updated_at=VALUES(updated_at)');
        foreach ($rawRecords as $raw) {
            if (!is_array($raw)) { continue; }
            $rawId = str_value($raw, 'raw_id', 96);
            $recordId = str_value($raw, 'record_id', 96);
            $rawKind = str_value($raw, 'raw_kind', 40);
            $payloadHash = str_value($raw, 'payload_hash', 64);
            $bytes = raw_bytes($raw);
            $retainedUntil = mysql_dt(isset($raw['retained_until']) ? $raw['retained_until'] : null);
            if ($rawId === null || $recordId === null || $rawKind === null || $payloadHash === null || $bytes === null || $retainedUntil === null) {
                continue;
            }
            if (!preg_match('/^[a-f0-9]{64}$/i', $payloadHash)) {
                continue;
            }
            $rawStmt->bindValue(1, $rawId, PDO::PARAM_STR);
            $rawStmt->bindValue(2, $recordId, PDO::PARAM_STR);
            $rawStmt->bindValue(3, $rawKind, PDO::PARAM_STR);
            $rawStmt->bindValue(4, strtolower($payloadHash), PDO::PARAM_STR);
            $rawStmt->bindValue(5, str_value($raw, 'compression', 20) ?: 'gzip', PDO::PARAM_STR);
            $rawStmt->bindValue(6, $bytes, PDO::PARAM_LOB);
            $rawStmt->bindValue(7, int_value($raw, 'schema_version'), PDO::PARAM_INT);
            $rawStmt->bindValue(8, $retainedUntil, PDO::PARAM_STR);
            $rawStmt->bindValue(9, $now, PDO::PARAM_STR);
            $rawStmt->bindValue(10, $now, PDO::PARAM_STR);
            $rawStmt->execute();
        }

        $storyStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'stories') . ' (
            story_key, guid, representative_title, representative_url, relevance_level, theme_group, status,
            article_count, priority_score, source_right_id, published_at, last_article_seen_at, sort_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE guid=VALUES(guid), representative_title=VALUES(representative_title), representative_url=VALUES(representative_url),
            relevance_level=VALUES(relevance_level), theme_group=VALUES(theme_group), status=VALUES(status), article_count=VALUES(article_count),
            priority_score=VALUES(priority_score), source_right_id=VALUES(source_right_id), published_at=VALUES(published_at), last_article_seen_at=VALUES(last_article_seen_at),
            sort_at=VALUES(sort_at), updated_at=VALUES(updated_at)');
        $linkStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'story_articles') . ' (story_key, article_id, position_no, updated_at) VALUES (?,?,?,?)
            ON DUPLICATE KEY UPDATE position_no=VALUES(position_no), updated_at=VALUES(updated_at)');
        foreach ($stories as $story) {
            if (!is_array($story)) { continue; }
            $storyKey = str_value($story, 'story_key', 120);
            if ($storyKey === null || $storyKey === '') { continue; }
            $publishedAt = mysql_dt(isset($story['published_at']) ? $story['published_at'] : null);
            $lastSeenAt = mysql_dt(isset($story['last_article_seen_at']) ? $story['last_article_seen_at'] : null);
            $storyStmt->execute(array(
                $storyKey,
                str_value($story, 'guid', 191),
                str_value($story, 'representative_title', 700),
                str_value($story, 'representative_url', 65535),
                str_value($story, 'relevance_level', 40),
                str_value($story, 'theme_group', 120),
                str_value($story, 'status', 40),
                int_value($story, 'article_count'),
                int_value($story, 'priority_score'),
                str_value($story, 'source_right_id', 64),
                $publishedAt,
                $lastSeenAt,
                $publishedAt !== null ? $publishedAt : ($lastSeenAt !== null ? $lastSeenAt : $now),
                $now,
            ));
            $articleIds = isset($story['article_ids']) && is_array($story['article_ids']) ? $story['article_ids'] : array();
            $pos = 0;
            foreach ($articleIds as $articleId) {
                $articleId = (string)$articleId;
                if ($articleId === '') { continue; }
                $linkStmt->execute(array($storyKey, mb_substr($articleId, 0, 96, 'UTF-8'), $pos, $now));
                $pos++;
            }
        }

        if (!empty($run)) {
            $runId = str_value($run, 'run_id', 96);
            if ($runId !== null && $runId !== '') {
                $runStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'runs') . ' (
                    run_id, started_at, finished_at, mode, fetched, accepted, duplicates, rejected, published_now, pending,
                    published_total, payload_json, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON DUPLICATE KEY UPDATE started_at=VALUES(started_at), finished_at=VALUES(finished_at), mode=VALUES(mode), fetched=VALUES(fetched),
                    accepted=VALUES(accepted), duplicates=VALUES(duplicates), rejected=VALUES(rejected), published_now=VALUES(published_now),
                    pending=VALUES(pending), published_total=VALUES(published_total), payload_json=VALUES(payload_json), updated_at=VALUES(updated_at)');
                $runStmt->execute(array(
                    $runId,
                    mysql_dt(isset($run['started_at']) ? $run['started_at'] : null),
                    mysql_dt(isset($run['finished_at']) ? $run['finished_at'] : null),
                    str_value($run, 'mode', 40),
                    int_value($run, 'fetched'),
                    int_value($run, 'accepted'),
                    int_value($run, 'duplicates'),
                    int_value($run, 'rejected'),
                    int_value($run, 'published_now'),
                    int_value($run, 'pending'),
                    int_value($run, 'published_total'),
                    json_value($run),
                    $now,
                    $now,
                ));
            }
        }
        $pdo->commit();
    } catch (Throwable $e) {
        $pdo->rollBack();
        throw $e;
    }
    respond(200, array('ok' => true, 'articles' => count($articles), 'raw_records' => count($rawRecords), 'stories' => count($stories)));
}


function upsert_telegram_snapshot(PDO $pdo, array $config, array $payload): void {
    foreach (array('channels', 'messages', 'article_matches', 'issue_signals') as $arrayField) {
        if (array_key_exists($arrayField, $payload) && !is_array($payload[$arrayField])) {
            respond(400, array('ok' => false, 'error' => 'invalid_' . $arrayField));
        }
    }
    $channels = isset($payload['channels']) && is_array($payload['channels']) ? $payload['channels'] : array();
    $messages = isset($payload['messages']) && is_array($payload['messages']) ? $payload['messages'] : array();
    $matches = isset($payload['article_matches']) && is_array($payload['article_matches']) ? $payload['article_matches'] : array();
    $signals = isset($payload['issue_signals']) && is_array($payload['issue_signals']) ? $payload['issue_signals'] : array();

    if (array_key_exists('replacement_signal_ids', $payload)) {
        respond(400, array('ok' => false, 'error' => 'deprecated_replacement_signal_ids'));
    }
    foreach (array('signal_rebuild_begin', 'signal_rebuild_finalize') as $booleanField) {
        if (array_key_exists($booleanField, $payload) && !is_bool($payload[$booleanField])) {
            respond(400, array('ok' => false, 'error' => 'invalid_' . $booleanField));
        }
    }
    $hasSignalRebuildToken = array_key_exists('signal_rebuild_token', $payload);
    $signalRebuildToken = null;
    if ($hasSignalRebuildToken) {
        if (!is_string($payload['signal_rebuild_token']) || !preg_match('/^[a-f0-9]{64}$/', $payload['signal_rebuild_token'])) {
            respond(400, array('ok' => false, 'error' => 'invalid_signal_rebuild_token'));
        }
        $signalRebuildToken = $payload['signal_rebuild_token'];
    }
    $signalRebuildBegin = array_key_exists('signal_rebuild_begin', $payload) && $payload['signal_rebuild_begin'] === true;
    $signalRebuildFinalize = array_key_exists('signal_rebuild_finalize', $payload) && $payload['signal_rebuild_finalize'] === true;
    if (($signalRebuildBegin || $signalRebuildFinalize) && $signalRebuildToken === null) {
        respond(400, array('ok' => false, 'error' => 'signal_rebuild_token_required'));
    }
    if ($signalRebuildBegin && $signalRebuildFinalize) {
        respond(400, array('ok' => false, 'error' => 'conflicting_signal_rebuild_phase'));
    }
    $signalRebuildBaseRevision = null;
    if ($signalRebuildBegin) {
        if (!array_key_exists('signal_rebuild_base_revision', $payload)) {
            respond(400, array('ok' => false, 'error' => 'signal_rebuild_base_revision_required'));
        }
        $signalRebuildBaseRevision = nonnegative_decimal_string($payload['signal_rebuild_base_revision']);
        if ($signalRebuildBaseRevision === null) {
            respond(400, array('ok' => false, 'error' => 'invalid_signal_rebuild_base_revision'));
        }
    } elseif (array_key_exists('signal_rebuild_base_revision', $payload)) {
        respond(400, array('ok' => false, 'error' => 'signal_rebuild_base_revision_requires_begin'));
    }

    $replaceSignals = bool_int($payload, 'replace_issue_signals') === 1;
    $replaceSignalsSince = $replaceSignals ? mysql_dt(isset($payload['issue_signals_replace_since']) ? $payload['issue_signals_replace_since'] : null) : null;
    if (count($channels) > 1000 || count($messages) > 2500 || count($matches) > 10000 || count($signals) > 1000) {
        respond(413, array('ok' => false, 'error' => 'too_many_records'));
    }
    if ($replaceSignals && !$signalRebuildFinalize) {
        respond(400, array('ok' => false, 'error' => 'signal_rebuild_finalize_required'));
    }
    if ($signalRebuildFinalize) {
        if (!$replaceSignals) {
            respond(400, array('ok' => false, 'error' => 'signal_rebuild_finalize_requires_replace'));
        }
        if ($replaceSignalsSince === null) {
            respond(400, array('ok' => false, 'error' => 'invalid_issue_signals_replace_since'));
        }
        if (count($signals) !== 0) {
            respond(400, array('ok' => false, 'error' => 'signal_rebuild_finalize_requires_empty_signals'));
        }
        if (count($channels) !== 0 || count($messages) !== 0 || count($matches) !== 0) {
            respond(400, array('ok' => false, 'error' => 'signal_rebuild_finalize_requires_metadata_only'));
        }
    }
    if ($signalRebuildToken !== null && !$signalRebuildFinalize
        && (count($channels) !== 0 || count($messages) !== 0 || count($matches) !== 0)) {
        respond(400, array('ok' => false, 'error' => 'signal_rebuild_stage_requires_signals_only'));
    }

    foreach ($channels as $channel) {
        if (!is_array($channel)) {
            respond(400, array('ok' => false, 'error' => 'invalid_telegram_channel'));
        }
        $rawHandle = isset($channel['handle']) ? $channel['handle'] : (isset($channel['username']) ? $channel['username'] : null);
        if (!is_string($rawHandle) || normalize_handle_value($rawHandle) === '') {
            respond(400, array('ok' => false, 'error' => 'invalid_telegram_channel_identity'));
        }
        if (isset($channel['telegram_channel_id'])) {
            $channelId = $channel['telegram_channel_id'];
            if ((!is_string($channelId) && !is_int($channelId))
                || trim((string)$channelId) === '' || mb_strlen((string)$channelId, 'UTF-8') > 64) {
                respond(400, array('ok' => false, 'error' => 'invalid_telegram_channel_identity'));
            }
        }
        if (array_key_exists('last_message_id', $channel)) {
            $lastMessageId = $channel['last_message_id'];
            $validLastMessageId = (is_int($lastMessageId) && $lastMessageId >= 0)
                || (is_string($lastMessageId) && preg_match('/^[0-9]+$/', $lastMessageId));
            if (!$validLastMessageId || (int)$lastMessageId < 0) {
                respond(400, array('ok' => false, 'error' => 'invalid_telegram_channel_cursor'));
            }
        }
    }
    foreach ($messages as $message) {
        if (!is_array($message)) {
            respond(400, array('ok' => false, 'error' => 'invalid_telegram_message'));
        }
        if (!isset($message['handle']) || !is_string($message['handle'])) {
            respond(400, array('ok' => false, 'error' => 'invalid_telegram_message_identity'));
        }
        if (isset($message['telegram_channel_id'])) {
            $channelId = $message['telegram_channel_id'];
            if ((!is_string($channelId) && !is_int($channelId))
                || trim((string)$channelId) === '' || mb_strlen((string)$channelId, 'UTF-8') > 64) {
                respond(400, array('ok' => false, 'error' => 'invalid_telegram_message_identity'));
            }
        }
        $rawMessageId = isset($message['telegram_message_id']) ? $message['telegram_message_id'] : null;
        $validMessageId = is_int($rawMessageId)
            || (is_string($rawMessageId) && preg_match('/^[1-9][0-9]*$/', $rawMessageId));
        if (!$validMessageId || (int)$rawMessageId <= 0) {
            $rawMessageId = isset($message['id']) ? $message['id'] : null;
            $validMessageId = is_int($rawMessageId)
                || (is_string($rawMessageId) && preg_match('/^[1-9][0-9]*$/', $rawMessageId));
        }
        $handle = normalize_handle_value($message['handle']);
        $messageId = $validMessageId ? (int)$rawMessageId : 0;
        $messageKey = $messageId > 0 ? telegram_message_key_from_row($message) : '';
        if ($handle === '' || $messageId <= 0 || mb_strlen($messageKey, 'UTF-8') > 180) {
            respond(400, array('ok' => false, 'error' => 'invalid_telegram_message_identity'));
        }
    }
    foreach ($matches as $match) {
        if (!is_array($match)) {
            respond(400, array('ok' => false, 'error' => 'invalid_telegram_article_match'));
        }
        $articleId = isset($match['article_id']) && is_string($match['article_id']) ? $match['article_id'] : null;
        $messageKey = isset($match['telegram_message_key']) && is_string($match['telegram_message_key']) ? $match['telegram_message_key'] : null;
        $matchType = isset($match['match_type']) && is_string($match['match_type']) ? $match['match_type'] : null;
        if ($articleId === null || trim($articleId) === '' || mb_strlen($articleId, 'UTF-8') > 96
            || $messageKey === null || trim($messageKey) === '' || mb_strlen($messageKey, 'UTF-8') > 180
            || $matchType === null || trim($matchType) === '' || mb_strlen($matchType, 'UTF-8') > 40) {
            respond(400, array('ok' => false, 'error' => 'invalid_telegram_article_match_identity'));
        }
    }
    $authoritativeSignalIds = array();
    foreach ($signals as $signal) {
        if (!is_array($signal)) {
            respond(400, array('ok' => false, 'error' => 'invalid_issue_signal'));
        }
        $articleId = isset($signal['article_id']) && is_string($signal['article_id']) ? $signal['article_id'] : null;
        if ($articleId === null || trim($articleId) === '' || mb_strlen($articleId, 'UTF-8') > 96) {
            respond(400, array('ok' => false, 'error' => 'invalid_issue_signal_article_id'));
        }
    }

    $now = gmdate('Y-m-d H:i:s');
    $channelsProcessed = 0;
    $messagesProcessed = 0;
    $matchesProcessed = 0;
    $signalsProcessed = 0;
    $signalsStaged = 0;
    $signalsDeleted = 0;
    $isSignalRebuildStage = $signalRebuildToken !== null && !$signalRebuildFinalize;
    $hasFencedLiveInputs = $signalRebuildToken === null
        && (count($channels) > 0 || count($messages) > 0 || count($matches) > 0 || count($signals) > 0);
    $responseLiveRevision = null;
    $signalRebuildStateTable = table_name($config, 'telegram_signal_rebuild_state');
    $signalRebuildStagingTable = table_name($config, 'telegram_signal_rebuild_staging');
    $signalRebuildLeaseSeconds = isset($config['telegram_signal_rebuild_lease_seconds'])
        ? max(1, min(86400, (int)$config['telegram_signal_rebuild_lease_seconds'])) : 600;
    $pdo->beginTransaction();
    try {
        if ($signalRebuildToken !== null || $hasFencedLiveInputs) {
            $lockSignalRebuild = $pdo->prepare('SELECT active_token, finalized_token, live_revision, updated_at FROM ' . $signalRebuildStateTable . ' WHERE state_key = ? FOR UPDATE');
            $lockSignalRebuild->execute(array('global'));
            $signalRebuildState = $lockSignalRebuild->fetch();
            if (!is_array($signalRebuildState)) {
                throw new RuntimeException('signal_rebuild_state_missing');
            }
            $currentLiveRevision = nonnegative_decimal_string((string)$signalRebuildState['live_revision']);
            if ($currentLiveRevision === null) {
                throw new RuntimeException('invalid_signal_rebuild_live_revision');
            }
            $responseLiveRevision = (int)$currentLiveRevision;
            $activeSignalRebuildToken = isset($signalRebuildState['active_token'])
                ? (string)$signalRebuildState['active_token'] : '';
            $signalRebuildLeaseTimestamp = isset($signalRebuildState['updated_at'])
                ? strtotime((string)$signalRebuildState['updated_at'] . ' UTC') : false;
            if ($activeSignalRebuildToken !== '' && $signalRebuildLeaseTimestamp === false) {
                throw new RuntimeException('invalid_signal_rebuild_lease_timestamp');
            }
            $signalRebuildLeaseExpired = $activeSignalRebuildToken !== ''
                && $signalRebuildLeaseTimestamp <= time() - $signalRebuildLeaseSeconds;
            if ($signalRebuildBegin) {
                if (!hash_equals($currentLiveRevision, (string)$signalRebuildBaseRevision)) {
                    $pdo->rollBack();
                    respond(409, array(
                        'ok' => false,
                        'error' => 'signal_rebuild_revision_conflict',
                        'live_revision' => (int)$currentLiveRevision,
                    ));
                }
                if ($activeSignalRebuildToken !== ''
                    && !hash_equals($activeSignalRebuildToken, $signalRebuildToken)
                    && !$signalRebuildLeaseExpired) {
                    $pdo->rollBack();
                    respond(409, array(
                        'ok' => false,
                        'error' => 'signal_rebuild_in_progress',
                        'live_revision' => (int)$currentLiveRevision,
                    ));
                }
                if ($activeSignalRebuildToken === ''
                    || (!hash_equals($activeSignalRebuildToken, $signalRebuildToken)
                        && $signalRebuildLeaseExpired)) {
                    $pdo->exec('DELETE FROM ' . $signalRebuildStagingTable);
                    $beginSignalRebuild = $pdo->prepare('UPDATE ' . $signalRebuildStateTable . '
                        SET active_token = ?, started_at = ?, updated_at = ? WHERE state_key = ?');
                    $beginSignalRebuild->execute(array($signalRebuildToken, $now, $now, 'global'));
                }
            } elseif ($signalRebuildFinalize
                && $activeSignalRebuildToken === ''
                && isset($signalRebuildState['finalized_token'])
                && hash_equals((string)$signalRebuildState['finalized_token'], $signalRebuildToken)) {
                $pdo->rollBack();
                respond(200, array(
                    'ok' => true,
                    'channels' => 0,
                    'messages' => 0,
                    'article_matches' => 0,
                    'issue_signals' => 0,
                    'issue_signals_deleted' => 0,
                    'signal_rebuild_finalized' => $signalRebuildToken,
                    'signal_rebuild_idempotent' => true,
                    'live_revision' => (int)$currentLiveRevision,
                ));
            } elseif ($signalRebuildToken !== null
                && (!isset($signalRebuildState['active_token']) || !hash_equals((string)$signalRebuildState['active_token'], $signalRebuildToken))) {
                $pdo->rollBack();
                respond(409, array('ok' => false, 'error' => 'stale_signal_rebuild_token'));
            } elseif ($hasFencedLiveInputs && $activeSignalRebuildToken !== '') {
                if (!$signalRebuildLeaseExpired) {
                    $pdo->rollBack();
                    respond(409, array(
                        'ok' => false,
                        'error' => 'signal_rebuild_in_progress',
                        'live_revision' => (int)$currentLiveRevision,
                    ));
                }
                $pdo->exec('DELETE FROM ' . $signalRebuildStagingTable);
                $clearExpiredSignalRebuild = $pdo->prepare('UPDATE ' . $signalRebuildStateTable . '
                    SET active_token = NULL, started_at = NULL, updated_at = ? WHERE state_key = ?');
                $clearExpiredSignalRebuild->execute(array($now, 'global'));
            }
        }

        $channelStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'telegram_channels') . ' (
            handle, telegram_channel_id, title, description, joined, enabled, source, source_type, is_public_channel,
            quality_score, last_message_id, last_collected_at, last_recommendation_checked_at, last_error, payload_json,
            identity_migration_version, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?)
        ON DUPLICATE KEY UPDATE telegram_channel_id=VALUES(telegram_channel_id), title=VALUES(title), description=VALUES(description),
            joined=VALUES(joined), enabled=VALUES(enabled), source=VALUES(source), source_type=VALUES(source_type),
            is_public_channel=VALUES(is_public_channel), quality_score=VALUES(quality_score), last_message_id=GREATEST(last_message_id,VALUES(last_message_id)),
            last_collected_at=VALUES(last_collected_at), last_recommendation_checked_at=VALUES(last_recommendation_checked_at),
            last_error=VALUES(last_error), payload_json=VALUES(payload_json), identity_migration_version=1, updated_at=VALUES(updated_at)');
        foreach ($channels as $channel) {
            $handle = normalize_handle_value(isset($channel['handle']) ? $channel['handle'] : (isset($channel['username']) ? $channel['username'] : ''));
            $telegramChannelId = str_value($channel, 'telegram_channel_id', 64);
            if ($telegramChannelId !== null && $telegramChannelId !== '') {
                migrate_telegram_channel_identity($pdo, $config, $handle, $telegramChannelId);
            }
            $channelStmt->execute(array(
                $handle,
                $telegramChannelId,
                str_value($channel, 'title', 255),
                str_value($channel, 'description', 65535),
                bool_int($channel, 'joined'),
                bool_int($channel, 'enabled', true),
                str_value($channel, 'source', 40),
                str_value($channel, 'source_type', 60),
                bool_int($channel, 'is_public_channel', true),
                int_value($channel, 'quality_score'),
                int_value($channel, 'last_message_id'),
                mysql_dt(isset($channel['last_collected_at']) ? $channel['last_collected_at'] : null),
                mysql_dt(isset($channel['last_recommendation_checked_at']) ? $channel['last_recommendation_checked_at'] : null),
                str_value($channel, 'last_error', 191),
                json_value($channel),
                $now,
            ));
            $channelsProcessed += 1;
        }

        $messageStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'telegram_messages') . ' (
            message_key, channel_handle, telegram_channel_id, telegram_message_id, posted_at, edited_at, deleted_at, collected_at,
            text, normalized_text, views, forwards, replies_count, message_url, urls_json, risk_flags_json, raw_json, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE channel_handle=VALUES(channel_handle), telegram_channel_id=VALUES(telegram_channel_id),
            posted_at=VALUES(posted_at), edited_at=VALUES(edited_at), deleted_at=VALUES(deleted_at),
            collected_at=VALUES(collected_at), text=VALUES(text), normalized_text=VALUES(normalized_text), views=VALUES(views),
            forwards=VALUES(forwards), replies_count=VALUES(replies_count), message_url=VALUES(message_url), urls_json=VALUES(urls_json),
            risk_flags_json=VALUES(risk_flags_json), raw_json=VALUES(raw_json), updated_at=VALUES(updated_at)');
        $invalidateHandleOnlyIdentity = $pdo->prepare('UPDATE ' . table_name($config, 'telegram_channels') . '
            SET identity_migration_version=0 WHERE handle=? AND identity_migration_version<>0');
        $invalidateMismatchedIdentity = $pdo->prepare('UPDATE ' . table_name($config, 'telegram_channels') . '
            SET identity_migration_version=0 WHERE identity_migration_version<>0 AND (
                (handle=? AND (telegram_channel_id IS NULL OR telegram_channel_id = \'\' OR telegram_channel_id <> ?))
                OR (telegram_channel_id=? AND handle<>?)
            )');
        $identityInvalidations = array();
        foreach ($messages as $message) {
            $handle = normalize_handle_value(isset($message['handle']) ? $message['handle'] : '');
            $messageId = int_value($message, 'telegram_message_id');
            if ($messageId <= 0) { $messageId = int_value($message, 'id'); }
            $messageChannelId = str_value($message, 'telegram_channel_id', 64);
            $text = str_value($message, 'text', 1048576) ?: '';
            $riskFlags = telegram_risk_flags($text);
            $rawJson = isset($message['raw_json']) && is_array($message['raw_json']) ? json_value($message['raw_json']) : null;
            $messageStmt->execute(array(
                telegram_message_key_from_row($message),
                $handle,
                $messageChannelId,
                $messageId,
                mysql_dt(isset($message['posted_at']) ? $message['posted_at'] : null),
                mysql_dt(isset($message['edited_at']) ? $message['edited_at'] : null),
                mysql_dt(isset($message['deleted_at']) ? $message['deleted_at'] : null),
                mysql_dt(isset($message['collected_at']) ? $message['collected_at'] : null),
                $text,
                str_value($message, 'normalized_text', 1048576),
                int_value($message, 'views'),
                int_value($message, 'forwards'),
                int_value($message, 'replies_count'),
                str_value($message, 'message_url', 65535),
                json_value(isset($message['urls']) && is_array($message['urls']) ? $message['urls'] : array()),
                json_value($riskFlags),
                $rawJson,
                $now,
            ));
            $identityInvalidations[$handle . "\n" . ($messageChannelId ?: '')] = array($handle, $messageChannelId);
            $messagesProcessed += 1;
        }
        // A later handle-only, conflicting, or renamed message invalidates the
        // bounded fast path. Dedupe by channel identity so a large message
        // payload adds at most one marker update per represented channel.
        foreach ($identityInvalidations as $identityInvalidation) {
            $handle = (string)$identityInvalidation[0];
            $messageChannelId = $identityInvalidation[1];
            if ($messageChannelId === null || $messageChannelId === '') {
                $invalidateHandleOnlyIdentity->execute(array($handle));
            } else {
                $invalidateMismatchedIdentity->execute(array(
                    $handle, $messageChannelId, $messageChannelId, $handle,
                ));
            }
        }

        $matchStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'telegram_article_matches') . ' (
            article_id, message_key, match_type, score, reason, channel_handle, telegram_message_id, message_url, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE score=VALUES(score), reason=VALUES(reason), channel_handle=VALUES(channel_handle),
            telegram_message_id=VALUES(telegram_message_id), message_url=VALUES(message_url), updated_at=VALUES(updated_at)');
        foreach ($matches as $match) {
            $articleId = str_value($match, 'article_id', 96);
            $messageKey = str_value($match, 'telegram_message_key', 180);
            $matchType = str_value($match, 'match_type', 40);
            $matchStmt->execute(array(
                $articleId,
                $messageKey,
                $matchType,
                isset($match['score']) ? (float)$match['score'] : 0,
                str_value($match, 'reason', 500),
                normalize_handle_value(isset($match['channel_handle']) ? $match['channel_handle'] : ''),
                int_value($match, 'telegram_message_id') ?: null,
                str_value($match, 'message_url', 65535),
                $now,
            ));
            $matchesProcessed += 1;
        }

        $signalStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'telegram_issue_signals') . ' (
            article_id, related_telegram_count, related_telegram_channels_count, first_seen_at, latest_seen_at,
            confidence_score, payload_json, updated_at
        ) VALUES (?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE related_telegram_count=VALUES(related_telegram_count), related_telegram_channels_count=VALUES(related_telegram_channels_count),
            first_seen_at=VALUES(first_seen_at), latest_seen_at=VALUES(latest_seen_at), confidence_score=VALUES(confidence_score),
            payload_json=VALUES(payload_json), updated_at=VALUES(updated_at)');

        if ($isSignalRebuildStage) {
            $stageSignal = $pdo->prepare('INSERT INTO ' . $signalRebuildStagingTable . ' (
                rebuild_token, article_id, payload_json, created_at, updated_at
            ) VALUES (?,?,?,?,?)
            ON DUPLICATE KEY UPDATE payload_json=VALUES(payload_json), updated_at=VALUES(updated_at)');
            foreach ($signals as $signal) {
                $stageSignal->execute(array(
                    $signalRebuildToken,
                    str_value($signal, 'article_id', 96),
                    json_value($signal),
                    $now,
                    $now,
                ));
                $signalsStaged += 1;
            }
        } elseif ($signalRebuildFinalize) {
            $countStagedSignals = $pdo->prepare('SELECT COUNT(*) FROM ' . $signalRebuildStagingTable . ' WHERE rebuild_token = ?');
            $countStagedSignals->execute(array($signalRebuildToken));
            $stagedSignalCount = (int)$countStagedSignals->fetchColumn();
            if ($stagedSignalCount > 10000) {
                $pdo->rollBack();
                respond(413, array('ok' => false, 'error' => 'too_many_staged_issue_signals'));
            }
            $loadStagedSignals = $pdo->prepare('SELECT article_id, payload_json FROM ' . $signalRebuildStagingTable . ' WHERE rebuild_token = ? ORDER BY article_id');
            $loadStagedSignals->execute(array($signalRebuildToken));
            while ($stagedSignalRow = $loadStagedSignals->fetch()) {
                $signal = json_decode((string)$stagedSignalRow['payload_json'], true);
                if (!is_array($signal)) {
                    throw new RuntimeException('invalid_staged_issue_signal_json');
                }
                $articleId = str_value($signal, 'article_id', 0);
                if ($articleId === null || !hash_equals((string)$stagedSignalRow['article_id'], $articleId)) {
                    throw new RuntimeException('staged_issue_signal_identity_mismatch');
                }
                $signalStmt->execute(array(
                    $articleId,
                    int_value($signal, 'related_telegram_count'),
                    int_value($signal, 'related_telegram_channels_count'),
                    mysql_dt(isset($signal['first_seen_at']) ? $signal['first_seen_at'] : null),
                    mysql_dt(isset($signal['latest_seen_at']) ? $signal['latest_seen_at'] : null),
                    isset($signal['confidence_score']) ? (float)$signal['confidence_score'] : 0,
                    json_value($signal),
                    $now,
                ));
                $authoritativeSignalIds[$articleId] = true;
                $signalsProcessed += 1;
            }
        } else {
            foreach ($signals as $signal) {
                $signalStmt->execute(array(
                    str_value($signal, 'article_id', 96),
                    int_value($signal, 'related_telegram_count'),
                    int_value($signal, 'related_telegram_channels_count'),
                    mysql_dt(isset($signal['first_seen_at']) ? $signal['first_seen_at'] : null),
                    mysql_dt(isset($signal['latest_seen_at']) ? $signal['latest_seen_at'] : null),
                    isset($signal['confidence_score']) ? (float)$signal['confidence_score'] : 0,
                    json_value($signal),
                    $now,
                ));
                $signalsProcessed += 1;
            }
        }

        if ($isSignalRebuildStage) {
            $heartbeatSignalRebuild = $pdo->prepare('UPDATE ' . $signalRebuildStateTable . '
                SET updated_at = UTC_TIMESTAMP() WHERE state_key = ? AND active_token = ?');
            $heartbeatSignalRebuild->execute(array('global', $signalRebuildToken));
        }
        if ($signalRebuildFinalize) {
            $deleteSql = 'DELETE FROM ' . table_name($config, 'telegram_issue_signals') . ' WHERE latest_seen_at >= ?';
            $deleteParams = array($replaceSignalsSince);
            if ($authoritativeSignalIds) {
                $deleteSql .= ' AND article_id NOT IN (' . implode(',', array_fill(0, count($authoritativeSignalIds), '?')) . ')';
                $deleteParams = array_merge($deleteParams, array_keys($authoritativeSignalIds));
            }
            $deleteSignals = $pdo->prepare($deleteSql);
            $deleteSignals->execute($deleteParams);
            $signalsDeleted = $deleteSignals->rowCount();
        }
        if ($signalRebuildFinalize) {
            $deleteStagedSignals = $pdo->prepare('DELETE FROM ' . $signalRebuildStagingTable . ' WHERE rebuild_token = ?');
            $deleteStagedSignals->execute(array($signalRebuildToken));
            $finalizeSignalRebuild = $pdo->prepare('UPDATE ' . $signalRebuildStateTable . '
                SET active_token = NULL, started_at = NULL, finalized_token = ?, finalized_at = ?,
                    live_revision = live_revision + 1, updated_at = ?
                WHERE state_key = ? AND active_token = ?');
            $finalizeSignalRebuild->execute(array($signalRebuildToken, $now, $now, 'global', $signalRebuildToken));
            if ($finalizeSignalRebuild->rowCount() !== 1) {
                throw new RuntimeException('signal_rebuild_finalize_state_conflict');
            }
        } elseif ($hasFencedLiveInputs) {
            $incrementLiveRevision = $pdo->prepare('UPDATE ' . $signalRebuildStateTable . '
                SET live_revision = live_revision + 1, updated_at = ? WHERE state_key = ?');
            $incrementLiveRevision->execute(array($now, 'global'));
            if ($incrementLiveRevision->rowCount() !== 1) {
                throw new RuntimeException('signal_rebuild_revision_increment_failed');
            }
        }
        if ($signalRebuildFinalize || $hasFencedLiveInputs) {
            $readLiveRevision = $pdo->prepare('SELECT live_revision FROM ' . $signalRebuildStateTable . ' WHERE state_key = ?');
            $readLiveRevision->execute(array('global'));
            $updatedLiveRevision = $readLiveRevision->fetchColumn();
            if ($updatedLiveRevision === false) {
                throw new RuntimeException('signal_rebuild_state_missing');
            }
            $responseLiveRevision = (int)$updatedLiveRevision;
        }
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    $response = array(
        'ok' => true,
        'channels' => $channelsProcessed,
        'messages' => $messagesProcessed,
        'article_matches' => $matchesProcessed,
        'issue_signals' => $signalsProcessed,
        'issue_signals_deleted' => $signalsDeleted,
    );
    if ($isSignalRebuildStage) {
        $response['issue_signals_staged'] = $signalsStaged;
        $response['signal_rebuild_token'] = $signalRebuildToken;
    }
    if ($signalRebuildFinalize) {
        $response['signal_rebuild_finalized'] = $signalRebuildToken;
    }
    if ($responseLiveRevision !== null) {
        $response['live_revision'] = $responseLiveRevision;
    }
    respond(200, $response);
}

function upsert_report(PDO $pdo, array $config, array $report): void {
    $dateId = str_value($report, 'date_id', 20);
    if ($dateId === null || !preg_match('/^\d{4}-\d{2}-\d{2}$/', $dateId)) {
        respond(400, array('ok' => false, 'error' => 'invalid_date_id'));
    }
    $now = gmdate('Y-m-d H:i:s');
    $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'reports') . ' (
        date_id, title, start_at, end_at, public_url, story_count, article_count, payload_json, updated_at
    ) VALUES (?,?,?,?,?,?,?,?,?)
    ON DUPLICATE KEY UPDATE title=VALUES(title), start_at=VALUES(start_at), end_at=VALUES(end_at), public_url=VALUES(public_url),
        story_count=VALUES(story_count), article_count=VALUES(article_count), payload_json=VALUES(payload_json), updated_at=VALUES(updated_at)');
    $stmt->execute(array(
        $dateId,
        str_value($report, 'title', 255),
        mysql_dt(isset($report['start_at']) ? $report['start_at'] : null),
        mysql_dt(isset($report['end_at']) ? $report['end_at'] : null),
        str_value($report, 'public_url', 65535),
        int_value($report, 'story_count'),
        int_value($report, 'article_count'),
        json_value($report),
        $now,
    ));
    respond(200, array('ok' => true, 'date_id' => $dateId));
}

function search_tokens(string $query): array {
    $query = trim(preg_replace('/\s+/u', ' ', $query));
    if ($query === '') {
        return array();
    }
    $rawTokens = preg_split('/[^\p{L}\p{N}]+/u', $query, -1, PREG_SPLIT_NO_EMPTY);
    if (!$rawTokens) {
        $rawTokens = array($query);
    }
    $stopwords = array(
        '관련' => true,
        '기사' => true,
        '보도' => true,
        '뉴스' => true,
        '시장' => true,
        '자본시장' => true,
        '주주' => true,
        '기업' => true,
        'google' => true,
        'news' => true,
    );
    $tokens = array();
    foreach ($rawTokens as $token) {
        $token = trim((string)$token);
        if ($token === '' || mb_strlen($token, 'UTF-8') < 2) {
            continue;
        }
        $lower = mb_strtolower($token, 'UTF-8');
        if (isset($stopwords[$lower]) || isset($stopwords[$token])) {
            continue;
        }
        if (!in_array($token, $tokens, true)) {
            $tokens[] = $token;
        }
        if (count($tokens) >= 5) {
            break;
        }
    }
    if (!$tokens && mb_strlen($query, 'UTF-8') >= 2) {
        $tokens[] = $query;
    }
    return $tokens;
}

function article_search_snippet(array $row, string $query, int $max = 150): string {
    $text = trim(preg_replace('/\s+/u', ' ', (string)($row['summary'] ?: $row['title'] ?: '')));
    if ($text === '') {
        return '';
    }
    $tokens = search_tokens($query);
    if (!$tokens) {
        return text_excerpt($text, $max);
    }
    $lower = mb_strtolower($text, 'UTF-8');
    foreach ($tokens as $token) {
        $pos = mb_strpos($lower, mb_strtolower((string)$token, 'UTF-8'), 0, 'UTF-8');
        if ($pos !== false) {
            $start = max(0, (int)$pos - 38);
            $snippet = mb_substr($text, $start, $max, 'UTF-8');
            return text_excerpt(($start > 0 ? '... ' : '') . $snippet, $max + 4);
        }
    }
    return text_excerpt($text, $max);
}

function article_search_reasons(array $row, string $query): array {
    $tokens = search_tokens($query);
    $textFields = array(
        '제목' => (string)($row['title'] ?: ''),
        '요약' => (string)($row['summary'] ?: ''),
        '매체' => (string)(($row['source'] ?: '') . ' ' . ($row['feed_name'] ?: '')),
        '분류' => (string)($row['feed_category'] ?: ''),
    );
    $reasons = array();
    foreach ($tokens as $token) {
        $needle = mb_strtolower((string)$token, 'UTF-8');
        foreach ($textFields as $label => $value) {
            if ($value !== '' && mb_strpos(mb_strtolower($value, 'UTF-8'), $needle, 0, 'UTF-8') !== false) {
                $reasons[] = $label . ' ' . $token;
                break;
            }
        }
        if (count($reasons) >= 4) {
            break;
        }
    }
    return $reasons;
}

function public_article_row(array $row, string $query): array {
    if ($query !== '') {
        $row['search_snippet'] = article_search_snippet($row, $query);
        $row['match_reasons'] = article_search_reasons($row, $query);
    } else {
        $row['search_snippet'] = text_excerpt((string)($row['summary'] ?: ''), 150);
        $row['match_reasons'] = array();
    }
    return $row;
}

function telegram_event_token(string $token): bool {
    $lower = mb_strtolower($token, 'UTF-8');
    $terms = array(
        'activist', 'activism', 'board', 'buyback', 'campaign', 'contest', 'delisting', 'director',
        'dividend', 'governance', 'letter', 'proxy', 'settlement', 'shareholder', 'stake',
        'stewardship', 'tender', '감리', '감사', '감사의견', '감자', '거래정지', '검찰', '경영권',
        '고발', '공개매수', '공개서한', '교체', '금감원', '노조', '리스크', '물적분할', '배당',
        '밸류업', '불성실공시', '분쟁', '분할', '상장폐지', '선임', '소각', '소송', '소액주주',
        '스튜어드십', '실적', '위임장', '유상증자', '의결권', '이사회', '자사주', '정정',
        '제재', '주주제안', '주주총회', '주주환원', '지배구조', '합병', '해임'
    );
    foreach ($terms as $term) {
        if ($lower === $term || mb_strpos($lower, $term, 0, 'UTF-8') !== false) {
            return true;
        }
    }
    return false;
}

function telegram_strong_token(string $token): bool {
    return mb_strlen($token, 'UTF-8') >= 3 || preg_match('/\d/u', $token) === 1;
}

function telegram_query_fallback_allowed(array $tokens): bool {
    if (count($tokens) < 3) {
        return false;
    }
    $eventCount = 0;
    $entityCount = 0;
    foreach ($tokens as $token) {
        if (telegram_event_token($token)) {
            $eventCount++;
        } elseif (telegram_strong_token($token)) {
            $entityCount++;
        }
    }
    return $eventCount >= 1 && $entityCount >= 1;
}

function search_event_rules(): array {
    return array(
        array('id' => 'management_dispute', 'label' => '경영권·주주행동', 'keywords' => array('경영권', '공개매수', '주주제안', '주주총회', '주총', '의결권', '이사회', '가처분', '소송', '행동주의', '스튜어드십', '주주행동', 'proxy', 'board', 'shareholder', 'activist')),
        array('id' => 'delisting', 'label' => '상장폐지·거래정지', 'keywords' => array('상장폐지', '상폐', '거래정지', '관리종목', '실질심사', '감사의견', '자본잠식', '정리매매', '불성실공시', 'delisting')),
        array('id' => 'valueup', 'label' => '밸류업·자본정책', 'keywords' => array('밸류업', '벨류업', '기업가치', '자사주', '소각', '배당', '주주환원', 'roe', 'pbr', '유상증자', '감자', 'buyback', 'dividend')),
        array('id' => 'tender_offer', 'label' => '공개매수·M&A', 'keywords' => array('공개매수', 'tender offer', '매수가', '응모', '최대주주 변경', '인수', '합병', 'm&a')),
        array('id' => 'shareholder_action', 'label' => '주주제안·의결권', 'keywords' => array('주주제안', '의결권대리행사', '위임장', '주주서한', '공개서한', '행동주의 펀드', 'shareholder proposal', 'proxy solicitation')),
        array('id' => 'disclosure_violation', 'label' => '불성실공시·제재', 'keywords' => array('불성실공시', '정정공시', '지연공시', '제재', '벌점', '공시위반', 'disclosure violation')),
        array('id' => 'capital_policy', 'label' => '증자·CB·자본정책', 'keywords' => array('유상증자', '전환사채', 'cb', 'bw', 'eb', '리픽싱', '감자', '배당', '자사주', '소각')),
        array('id' => 'disclosure', 'label' => '공시·제도', 'keywords' => array('공시', '주요사항보고서', 'dart', 'kind', '거래소', '금융위', '금감원', '정정공시', '제도', '감독', 'disclosure')),
        array('id' => 'global', 'label' => '해외·영문', 'keywords' => array('activist', 'activism', 'proxy', 'settlement', 'tender offer', 'governance', 'stewardship', 'sec', 'bloomberg', 'cnbc')),
    );
}

function search_text_blob(array $row): string {
    $parts = array();
    foreach (array('title', 'representative_title', 'signal_title', 'summary', 'signal_summary', 'source', 'feed_name', 'feed_category', 'theme_group', 'relevance_level') as $key) {
        if (isset($row[$key]) && $row[$key] !== null) {
            $parts[] = (string)$row[$key];
        }
    }
    if (isset($row['top_keywords']) && is_array($row['top_keywords'])) {
        $parts[] = implode(' ', $row['top_keywords']);
    }
    if (isset($row['top_channels']) && is_array($row['top_channels'])) {
        $parts[] = implode(' ', $row['top_channels']);
    }
    if (isset($row['top_related_messages']) && is_array($row['top_related_messages'])) {
        foreach ($row['top_related_messages'] as $message) {
            if (is_array($message)) {
                foreach (array('excerpt', 'text', 'channel_title', 'channel_handle') as $key) {
                    if (isset($message[$key]) && $message[$key] !== null) {
                        $parts[] = (string)$message[$key];
                    }
                }
            }
        }
    }
    return implode(' ', $parts);
}

function search_classify_event(array $row): array {
    $haystack = mb_strtolower(search_text_blob($row), 'UTF-8');
    $best = array('id' => 'general', 'label' => '일반 이슈', 'hits' => array());
    foreach (search_event_rules() as $rule) {
        $hits = array();
        foreach ($rule['keywords'] as $keyword) {
            if ($keyword !== '' && mb_strpos($haystack, mb_strtolower((string)$keyword, 'UTF-8'), 0, 'UTF-8') !== false) {
                $hits[] = (string)$keyword;
            }
        }
        if (count($hits) > count($best['hits'])) {
            $best = array('id' => $rule['id'], 'label' => $rule['label'], 'hits' => array_slice(array_values(array_unique($hits)), 0, 8));
        }
    }
    return $best;
}

function search_row_risk_flags(array $row): array {
    $flags = array();
    if (isset($row['risk_flags']) && is_array($row['risk_flags'])) {
        foreach ($row['risk_flags'] as $flag) {
            $flags[(string)$flag] = true;
        }
    }
    if (isset($row['risk_flags_json'])) {
        foreach (decode_json_array((string)$row['risk_flags_json']) as $flag) {
            $flags[(string)$flag] = true;
        }
    }
    foreach (telegram_risk_flags(search_text_blob($row)) as $flag) {
        $flags[(string)$flag] = true;
    }
    return array_values(array_keys($flags));
}

function search_spread_score(array $row): float {
    $articleCount = isset($row['article_count']) ? (int)$row['article_count'] : 0;
    $telegramCount = isset($row['related_telegram_count']) ? (int)$row['related_telegram_count'] : 0;
    $channelCount = isset($row['related_telegram_channels_count']) ? (int)$row['related_telegram_channels_count'] : 0;
    $publisherCount = isset($row['publisher_count']) ? (int)$row['publisher_count'] : 0;
    $engagement = 0.0;
    if (isset($row['top_related_messages']) && is_array($row['top_related_messages'])) {
        foreach ($row['top_related_messages'] as $message) {
            if (is_array($message)) {
                $engagement += ((int)($message['views'] ?? 0)) / 5000.0 + ((int)($message['forwards'] ?? 0)) / 50.0;
            }
        }
    }
    return min(1.0, log(1 + $articleCount + $publisherCount * 2 + $telegramCount + $channelCount * 2 + $engagement) / 4.0);
}

function search_recency_score(array $row): float {
    $raw = '';
    foreach (array('published_at', 'sort_at', 'last_article_seen_at', 'latest_seen_at', 'first_seen_at', 'posted_at', 'seen_at', 'updated_at') as $key) {
        if (isset($row[$key]) && (string)$row[$key] !== '') {
            $raw = (string)$row[$key];
            break;
        }
    }
    if ($raw === '') {
        return 0.25;
    }
    $ts = strtotime($raw);
    if ($ts === false) {
        return 0.25;
    }
    $ageHours = max(0.0, (time() - $ts) / 3600.0);
    return max(0.0, min(1.0, 1.0 - $ageHours / (24.0 * 14.0)));
}

function search_materiality_score(array $row): float {
    $event = search_classify_event($row);
    if ($event['id'] === 'management_dispute' || $event['id'] === 'delisting') {
        return 1.0;
    }
    if ($event['id'] === 'valueup' || $event['id'] === 'disclosure') {
        return 0.72;
    }
    if ($event['id'] === 'global') {
        return 0.55;
    }
    return 0.35;
}

function search_risk_penalty(array $row): float {
    $flags = search_row_risk_flags($row);
    $penalty = 0.0;
    if (in_array('promotional', $flags, true)) { $penalty += 0.35; }
    if (in_array('rumor', $flags, true)) { $penalty += 0.22; }
    if (in_array('unverified', $flags, true)) { $penalty += 0.12; }
    return $penalty;
}

function search_score_row(array $row, string $kind, string $query, string $sort): array {
    $tokens = search_tokens($query);
    $text = mb_strtolower(search_text_blob($row), 'UTF-8');
    $hits = 0;
    foreach ($tokens as $token) {
        if (mb_strpos($text, mb_strtolower((string)$token, 'UTF-8'), 0, 'UTF-8') !== false) {
            $hits++;
        }
    }
    $queryRelevance = count($tokens) > 0 ? $hits / count($tokens) : 0.5;
    $officialAnchor = preg_match('/공시|dart|kind|거래소|금융위|금감원|법원|주요사항보고서/iu', $text) === 1 ? 1.0 : 0.0;
    $confidence = isset($row['confidence_score']) ? (float)$row['confidence_score'] : ($kind === 'telegram' ? 0.5 : 0.65);
    $base = $kind === 'story' ? 0.08 : ($kind === 'article' ? 0.02 : -0.02);
    $breakdown = array(
        'relevance' => round($queryRelevance, 4),
        'official_anchor' => round($officialAnchor, 4),
        'source_diversity' => round(search_spread_score($row), 4),
        'recency' => round(search_recency_score($row), 4),
        'materiality' => round(search_materiality_score($row), 4),
        'momentum' => round($kind === 'telegram' ? search_spread_score($row) : ((isset($row['related_telegram_count']) ? min(1.0, (float)$row['related_telegram_count'] / 10.0) : 0.0)), 4),
        'risk_penalty' => round(search_risk_penalty($row), 4),
    );
    $smart = $base
        + 0.28 * $breakdown['relevance']
        + 0.14 * $breakdown['official_anchor']
        + 0.16 * $breakdown['source_diversity']
        + 0.16 * $breakdown['recency']
        + 0.16 * $breakdown['materiality']
        + 0.10 * $confidence
        - $breakdown['risk_penalty'];
    if ($sort === 'latest') {
        $final = $breakdown['recency'];
    } elseif ($sort === 'spread') {
        $final = $breakdown['source_diversity'];
    } elseif ($sort === 'telegram_momentum' || $sort === 'telegram') {
        $final = $kind === 'telegram' ? $breakdown['momentum'] + $confidence * 0.2 : $breakdown['momentum'];
    } elseif ($sort === 'low_noise') {
        $final = $smart - $breakdown['risk_penalty'] * 1.5;
    } else {
        $final = $smart;
    }
    if ($kind === 'telegram') {
        $hasArticleAnchor = isset($row['article_id']) && (string)$row['article_id'] !== '' && strpos((string)$row['article_id'], 'telegram-topic:') !== 0;
        if (!$hasArticleAnchor) {
            $channelCount = isset($row['related_telegram_channels_count']) ? (int)$row['related_telegram_channels_count'] : 0;
            $cap = $channelCount > 1 ? 0.62 : 0.45;
            $flags = search_row_risk_flags($row);
            if (in_array('promotional', $flags, true) || in_array('rumor', $flags, true) || in_array('unverified', $flags, true)) {
                $cap = min($cap, 0.35);
            }
            $final = min($final, $cap);
        }
    }
    $breakdown['final_score'] = round(max(0.0, $final), 4);
    return $breakdown;
}

function search_sort_rows(array $rows, string $kind, string $query, string $sort): array {
    foreach ($rows as $idx => $row) {
        $score = search_score_row($row, $kind, $query, $sort);
        $rows[$idx]['search_score'] = $score['final_score'];
        $rows[$idx]['score_breakdown'] = $score;
    }
    usort($rows, function ($a, $b) {
        $score = ((float)($b['search_score'] ?? 0)) <=> ((float)($a['search_score'] ?? 0));
        if ($score !== 0) {
            return $score;
        }
        return strcmp((string)($b['sort_at'] ?? $b['latest_seen_at'] ?? ''), (string)($a['sort_at'] ?? $a['latest_seen_at'] ?? ''));
    });
    return $rows;
}

function search_why_matters(array $row, string $kind): array {
    $event = search_classify_event($row);
    $flags = search_row_risk_flags($row);
    $lines = array();
    if ($event['id'] === 'management_dispute') {
        $lines[] = '주주권·의결권·이사회 책임 쟁점과 연결되는 이슈입니다.';
    } elseif ($event['id'] === 'delisting') {
        $lines[] = '거래 가능성과 투자자 보호 절차에 직접 연결되는 시장 민감 이벤트입니다.';
    } elseif ($event['id'] === 'valueup') {
        $lines[] = '자사주·배당·기업가치 제고 등 실제 자본정책 여부를 함께 봐야 합니다.';
    } elseif ($event['id'] === 'disclosure') {
        $lines[] = '공시·제도 변화와 후속 기사 확산 여부를 확인할 필요가 있습니다.';
    } elseif ($event['id'] === 'global') {
        $lines[] = '해외 행동주의·거버넌스 흐름을 국내 관점에서 비교해 볼 수 있습니다.';
    }
    if ((int)($row['article_count'] ?? 0) > 1) {
        $lines[] = '복수 기사로 묶인 이슈라 단발 보도보다 확산도가 높습니다.';
    }
    if ((int)($row['related_telegram_channels_count'] ?? 0) > 1) {
        $lines[] = 'Telegram 여러 채널에서 반복 언급됐습니다.';
    }
    if (in_array('promotional', $flags, true) || in_array('rumor', $flags, true) || in_array('unverified', $flags, true)) {
        $lines[] = '미확인·홍보성 가능성이 있어 원문 확인이 필요합니다.';
    }
    return array_slice($lines, 0, 3);
}

function search_public_story_row(array $row, string $query, string $sort): array {
    $row['title'] = isset($row['representative_title']) ? (string)$row['representative_title'] : '';
    $row['event_type'] = search_classify_event($row);
    $row['risk_flags'] = search_row_risk_flags($row);
    $row['why_matters'] = search_why_matters($row, 'story');
    $row['verification_status'] = preg_match('/공시|dart|kind|거래소|금융위|금감원/iu', search_text_blob($row)) === 1 ? 'official_hint' : 'media_confirmed';
    $score = search_score_row($row, 'story', $query, $sort);
    $row['search_score'] = $score['final_score'];
    $row['score_breakdown'] = $score;
    return $row;
}

function search_public_article_row(array $row, string $query, string $sort): array {
    $row = public_article_row($row, $query);
    $row['event_type'] = search_classify_event($row);
    $row['risk_flags'] = search_row_risk_flags($row);
    $row['why_matters'] = search_why_matters($row, 'article');
    $score = search_score_row($row, 'article', $query, $sort);
    $row['search_score'] = $score['final_score'];
    $row['score_breakdown'] = $score;
    return $row;
}

function search_public_telegram_row(array $row, string $query, string $sort, array $allowedHandles): array {
    $signal = public_telegram_signal($row, $allowedHandles);
    $signal['event_type'] = search_classify_event($signal);
    $signal['risk_flags'] = search_row_risk_flags($signal);
    $signal['why_matters'] = search_why_matters($signal, 'telegram');
    $score = search_score_row($signal, 'telegram', $query, $sort);
    $signal['search_score'] = $score['final_score'];
    $signal['score_breakdown'] = $score;
    return $signal;
}

function search_query_interpretation(string $query, array $rows): array {
    $tokens = search_tokens($query);
    $events = array();
    foreach ($rows as $row) {
        $event = search_classify_event($row);
        if ($event['id'] !== 'general') {
            if (!isset($events[$event['id']])) {
                $events[$event['id']] = array('id' => $event['id'], 'label' => $event['label'], 'count' => 0);
            }
            $events[$event['id']]['count']++;
        }
    }
    usort($events, function ($a, $b) { return ((int)$b['count']) <=> ((int)$a['count']); });
    return array(
        'keywords' => $tokens,
        'event_types' => array_slice(array_values($events), 0, 5),
        'query_intent' => count($events) ? 'event_keyword' : 'general_news',
        'confidence' => min(0.95, 0.45 + count($tokens) * 0.12 + count($events) * 0.08),
    );
}

function search_briefing(string $query, array $articles, array $stories, array $telegram): array {
    $rows = array_merge($stories, $articles, $telegram);
    $events = array();
    $riskCounts = array();
    $sources = array();
    $channels = array();
    foreach ($rows as $row) {
        $event = search_classify_event($row);
        $events[$event['label']] = ($events[$event['label']] ?? 0) + 1;
        foreach (search_row_risk_flags($row) as $flag) {
            $riskCounts[$flag] = ($riskCounts[$flag] ?? 0) + 1;
        }
        if (isset($row['source']) && $row['source'] !== '') {
            $sources[(string)$row['source']] = true;
        }
        if (isset($row['feed_name']) && $row['feed_name'] !== '') {
            $sources[(string)$row['feed_name']] = true;
        }
        if (isset($row['top_channels']) && is_array($row['top_channels'])) {
            foreach ($row['top_channels'] as $channel) {
                $channels[(string)$channel] = true;
            }
        }
    }
    arsort($events);
    arsort($riskCounts);
    $eventLabel = $events ? (string)array_key_first($events) : '일반 이슈';
    $riskFlags = array_slice(array_keys($riskCounts), 0, 6);
    $headline = $query . ' 관련 공개 정보가 ' . $eventLabel . ' 관점에서 정리됐습니다.';
    $bullets = array(
        $eventLabel . ' 관련 기사·이슈·Telegram 언급을 함께 확인할 수 있습니다.',
        count($sources) > 0 ? '기사 출처 ' . count($sources) . '곳이 검색어와 연결됩니다.' : '기사 출처 확산은 아직 제한적입니다.',
        count($channels) > 0 ? 'Telegram 공개 채널 ' . count($channels) . '곳에서 관련 언급이 확인됩니다.' : 'Telegram 관련 언급은 제한적입니다.',
    );
    if ($riskFlags) {
        $bullets[] = '주의 플래그: ' . implode(' · ', $riskFlags);
    } else {
        $bullets[] = '주요 루머·홍보성 플래그는 제한적입니다.';
    }
    return array(
        'headline' => $headline,
        'verification_status' => count($articles) > 0 || count($stories) > 0 ? 'media_confirmed' : 'telegram_only',
        'spread_status' => (count($articles) + count($stories) + count($telegram)) >= 10 ? 'expanding' : 'limited',
        'source_counts' => array(
            'articles' => count($articles),
            'stories' => count($stories),
            'publishers' => count($sources),
            'telegram_signals' => count($telegram),
            'telegram_channels' => count($channels),
        ),
        'risk_flags' => $riskFlags,
        'bullets' => $bullets,
        'disclaimer' => '공개 정보 기반 이슈 정리이며 투자 제안·권유·종목 추천이 아닙니다.',
    );
}

function search_timeline(array $articles, array $stories, array $telegram): array {
    $items = array();
    foreach ($stories as $row) {
        $items[] = array('kind' => '이슈', 'time' => (string)($row['last_article_seen_at'] ?? $row['published_at'] ?? ''), 'title' => (string)($row['representative_title'] ?? ''), 'url' => (string)($row['representative_url'] ?? ''), 'event_type' => $row['event_type'] ?? null);
    }
    foreach ($articles as $row) {
        $items[] = array('kind' => '기사', 'time' => (string)($row['published_at'] ?? $row['sort_at'] ?? ''), 'title' => (string)($row['title'] ?? ''), 'url' => (string)($row['canonical_url'] ?? ''), 'event_type' => $row['event_type'] ?? null);
    }
    foreach ($telegram as $signal) {
        foreach (array_slice((array)($signal['top_related_messages'] ?? array()), 0, 4) as $message) {
            if (is_array($message)) {
                $items[] = array('kind' => 'Telegram', 'time' => (string)($message['posted_at'] ?? ''), 'title' => text_excerpt((string)($message['excerpt'] ?? $signal['signal_title'] ?? ''), 120), 'url' => (string)($message['message_url'] ?? ''), 'event_type' => $signal['event_type'] ?? null);
            }
        }
    }
    usort($items, function ($a, $b) {
        return strtotime((string)$b['time']) <=> strtotime((string)$a['time']);
    });
    return array_slice($items, 0, 40);
}

function handle_search(PDO $pdo, array $config): void {
    $query = isset($_GET['q']) ? trim((string)$_GET['q']) : '';
    if (mb_strlen($query, 'UTF-8') < 2) {
        respond(400, array('ok' => false, 'error' => 'query_too_short'));
    }
    $limit = isset($_GET['limit']) ? max(1, min(60, (int)$_GET['limit'])) : 30;
    $days = isset($_GET['days']) ? max(1, min(365, (int)$_GET['days'])) : 365;
    $sort = isset($_GET['sort']) ? (string)$_GET['sort'] : 'smart';
    if (!in_array($sort, array('smart', 'latest', 'spread', 'official', 'telegram_momentum', 'telegram', 'low_noise'), true)) {
        $sort = 'smart';
    }
    $tokens = search_tokens($query);
    if (!$tokens) {
        respond(400, array('ok' => false, 'error' => 'query_too_short'));
    }
    $articleWhere = array(
        'a.canonical_url IS NOT NULL',
        'a.title IS NOT NULL',
        '(a.status IS NULL OR a.status NOT IN ("rejected", "duplicate"))',
        'a.sort_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL ' . $days . ' DAY)',
        legacy_article_visibility_sql('a', 'article_sr'),
    );
    $articleParams = array();
    foreach ($tokens as $token) {
        $like = '%' . $token . '%';
        $articleWhere[] = '(a.title LIKE ? OR a.normalized_title LIKE ? OR a.summary LIKE ? OR a.source LIKE ? OR a.feed_name LIKE ? OR a.feed_category LIKE ?)';
        array_push($articleParams, $like, $like, $like, $like, $like, $like);
    }
    $articleSql = 'SELECT a.record_id, a.canonical_url, a.title, a.summary, a.source, a.feed_name, a.feed_category, a.image_url, a.published_at, a.seen_at, a.status, a.reason, a.relevance_level, a.priority_score, a.priority_level, a.story_key, a.source_right_id, a.sort_at FROM '
        . table_name($config, 'articles') . ' a LEFT JOIN ' . table_name($config, 'source_rights') . ' article_sr ON article_sr.source_right_id = a.source_right_id'
        . ' WHERE ' . implode(' AND ', $articleWhere)
        . ' ORDER BY a.sort_at DESC, a.priority_score DESC LIMIT ' . min(120, $limit * 3);
    $stmt = $pdo->prepare($articleSql);
    $stmt->execute($articleParams);
    $articles = array();
    foreach ($stmt->fetchAll() as $row) {
        $articles[] = search_public_article_row($row, $query, $sort);
    }

    $storyWhere = array(
        's.representative_title IS NOT NULL',
        's.sort_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL ' . $days . ' DAY)',
        legacy_story_visibility_sql($config, 's'),
    );
    $storyParams = array();
    foreach ($tokens as $token) {
        $like = '%' . $token . '%';
        $storyWhere[] = '(s.representative_title LIKE ? OR s.theme_group LIKE ? OR s.relevance_level LIKE ?)';
        array_push($storyParams, $like, $like, $like);
    }
    $storySql = 'SELECT s.story_key, s.guid, s.representative_title, s.representative_url, s.relevance_level, s.theme_group, s.status, s.article_count, s.priority_score, s.source_right_id, s.published_at, s.last_article_seen_at, s.sort_at FROM '
        . table_name($config, 'stories') . ' s'
        . ' WHERE ' . implode(' AND ', $storyWhere)
        . ' ORDER BY s.sort_at DESC, s.priority_score DESC LIMIT ' . min(80, $limit * 2);
    $stmt = $pdo->prepare($storySql);
    $stmt->execute($storyParams);
    $stories = array();
    foreach ($stmt->fetchAll() as $row) {
        $stories[] = search_public_story_row($row, $query, $sort);
    }

    $signalsTable = table_name($config, 'telegram_issue_signals');
    $signalSql = 'SELECT sig.article_id, sig.related_telegram_count, sig.related_telegram_channels_count, sig.first_seen_at, sig.latest_seen_at, sig.confidence_score, sig.payload_json, sig.updated_at FROM '
        . $signalsTable . ' sig'
        . ' WHERE sig.latest_seen_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL ' . $days . ' DAY)'
        . ' AND ' . telegram_signal_visibility_sql($config, 'sig')
        . ' ORDER BY sig.related_telegram_channels_count DESC, sig.related_telegram_count DESC, sig.latest_seen_at DESC LIMIT 160';
    $stmt = $pdo->prepare($signalSql);
    $stmt->execute();
    $telegram = array();
    $allowedTelegramHandles = active_telegram_source_keys($pdo, $config);
    foreach ($stmt->fetchAll() as $row) {
        $signal = search_public_telegram_row($row, $query, $sort, $allowedTelegramHandles);
        $text = mb_strtolower(search_text_blob($signal), 'UTF-8');
        $matches = false;
        foreach ($tokens as $token) {
            if (mb_strpos($text, mb_strtolower((string)$token, 'UTF-8'), 0, 'UTF-8') !== false) {
                $matches = true;
                break;
            }
        }
        if ($matches) {
            $telegram[] = $signal;
        }
    }

    $articles = array_slice(search_sort_rows($articles, 'article', $query, $sort), 0, $limit);
    $stories = array_slice(search_sort_rows($stories, 'story', $query, $sort), 0, $limit);
    $telegram = array_slice(search_sort_rows($telegram, 'telegram', $query, $sort), 0, $limit);
    $allRows = array_merge($stories, $articles, $telegram);
    $interpretation = search_query_interpretation($query, $allRows);
    $briefing = search_briefing($query, $articles, $stories, $telegram);
    respond(200, array(
        'ok' => true,
        'query' => $query,
        'query_interpretation' => $interpretation,
        'briefing' => $briefing,
        'tabs' => array(
            'overview' => count($allRows),
            'stories' => count($stories),
            'articles' => count($articles),
            'telegram' => count($telegram),
            'timeline' => count($allRows),
        ),
        'stories' => $stories,
        'articles' => $articles,
        'telegram' => $telegram,
        'timeline' => search_timeline($articles, $stories, $telegram),
    ));
}

function handle_read(string $action, array $config): void {
    if ($action === 'health') {
        respond(200, array('ok' => true, 'service' => 'activist', 'time' => gmdate('c')));
    }
    if (!in_array($action, array('reports', 'report', 'latest_snapshot', 'articles', 'telegram_reactions', 'telegram_dashboard', 'search'), true)) {
        respond(404, array('ok' => false, 'error' => 'unknown_action'));
    }
    $pdo = pdo_conn($config);
    v1_require_schema_version($pdo, $config);
    legacy_adapter_headers($pdo, $config, $action);
    if ($action === 'search') {
        handle_search($pdo, $config);
    }
    if ($action === 'telegram_dashboard') {
        handle_telegram_dashboard($pdo, $config);
    }
    if ($action === 'reports') {
        $limit = isset($_GET['limit']) ? max(1, min(60, (int)$_GET['limit'])) : 20;
        $stmt = $pdo->prepare('SELECT date_id, title, start_at, end_at, public_url, story_count, article_count, updated_at FROM ' . table_name($config, 'reports') . ' ORDER BY date_id DESC LIMIT ' . $limit);
        $stmt->execute();
        respond(200, array('ok' => true, 'reports' => $stmt->fetchAll()));
    }
    if ($action === 'report') {
        $date = isset($_GET['date']) ? (string)$_GET['date'] : '';
        if (!preg_match('/^\d{4}-\d{2}-\d{2}$/', $date)) {
            respond(400, array('ok' => false, 'error' => 'invalid_date'));
        }
        $stmt = $pdo->prepare('SELECT date_id, title, start_at, end_at, public_url, story_count, article_count, updated_at FROM ' . table_name($config, 'reports') . ' WHERE date_id = ?');
        $stmt->execute(array($date));
        $row = $stmt->fetch();
        if (!$row) {
            respond(404, array('ok' => false, 'error' => 'not_found'));
        }
        respond(200, array('ok' => true, 'report' => $row));
    }
    if ($action === 'latest_snapshot') {
        $limit = isset($_GET['limit']) ? max(1, min(100, (int)$_GET['limit'])) : 50;
        $stmt = $pdo->prepare('SELECT s.story_key, s.guid, s.representative_title, s.representative_url, s.relevance_level, s.theme_group, s.status, s.article_count, s.priority_score, s.source_right_id, s.published_at, s.last_article_seen_at FROM '
            . table_name($config, 'stories') . ' s WHERE ' . legacy_story_visibility_sql($config, 's') . ' ORDER BY s.sort_at DESC LIMIT ' . $limit);
        $stmt->execute();
        respond(200, array('ok' => true, 'stories' => $stmt->fetchAll()));
    }
    if ($action === 'telegram_reactions') {
        $limit = isset($_GET['limit']) ? max(1, min(20, (int)$_GET['limit'])) : 5;
        $days = isset($_GET['days']) ? max(1, min(180, (int)$_GET['days'])) : 180;
        $url = isset($_GET['url']) ? trim((string)$_GET['url']) : '';
        $query = isset($_GET['q']) ? trim((string)$_GET['q']) : '';
        $messagesByKey = array();
        $articleIds = array();
        if ($url !== '' && preg_match('/^https?:\/\//i', $url)) {
            $stmt = $pdo->prepare('SELECT a.record_id FROM ' . table_name($config, 'articles') . ' a LEFT JOIN '
                . table_name($config, 'source_rights') . ' article_sr ON article_sr.source_right_id=a.source_right_id'
                . ' WHERE a.canonical_url = ? AND ' . legacy_article_visibility_sql('a', 'article_sr') . ' LIMIT 10');
            $stmt->execute(array($url));
            foreach ($stmt->fetchAll() as $row) {
                $articleIds[] = (string)$row['record_id'];
            }
        }
        if ($articleIds) {
            $placeholders = implode(',', array_fill(0, count($articleIds), '?'));
            $sql = 'SELECT m.channel_handle, COALESCE(c.title, m.channel_handle) AS channel_title, m.telegram_message_id, m.posted_at, m.message_url, m.text, m.risk_flags_json, tm.match_type, tm.score, tm.reason '
                . 'FROM ' . table_name($config, 'telegram_article_matches') . ' tm '
                . 'JOIN ' . table_name($config, 'telegram_messages') . ' m ON m.message_key = tm.message_key '
                . 'LEFT JOIN ' . table_name($config, 'telegram_channels') . ' c ON c.handle = m.channel_handle '
                . 'WHERE tm.article_id IN (' . $placeholders . ') AND m.deleted_at IS NULL '
                . 'AND ' . telegram_message_visibility_sql($config, 'm') . ' '
                . 'AND (tm.match_type IN ("exact_url","canonical_url") OR tm.score >= 0.5300) '
                . 'ORDER BY tm.score DESC, m.posted_at DESC LIMIT ' . $limit;
            $stmt = $pdo->prepare($sql);
            $stmt->execute($articleIds);
            foreach ($stmt->fetchAll() as $row) {
                $messagesByKey[(string)$row['message_url']] = public_telegram_message($row, $query);
            }
        }
        if ($url !== '' && count($messagesByKey) < $limit) {
            $stmt = $pdo->prepare('SELECT m.channel_handle, COALESCE(c.title, m.channel_handle) AS channel_title, m.telegram_message_id, m.posted_at, m.message_url, m.text, m.risk_flags_json, "exact_url" AS match_type, 0.9000 AS score, "URL 직접 공유" AS reason '
                . 'FROM ' . table_name($config, 'telegram_messages') . ' m '
                . 'LEFT JOIN ' . table_name($config, 'telegram_channels') . ' c ON c.handle = m.channel_handle '
                . 'WHERE m.deleted_at IS NULL AND m.urls_json LIKE ? AND m.posted_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL ' . $days . ' DAY) '
                . 'AND ' . telegram_message_visibility_sql($config, 'm') . ' '
                . 'ORDER BY m.posted_at DESC LIMIT ' . $limit);
            $stmt->execute(array('%' . $url . '%'));
            foreach ($stmt->fetchAll() as $row) {
                $messagesByKey[(string)$row['message_url']] = public_telegram_message($row, $query);
            }
        }
        if ($query !== '' && count($messagesByKey) < $limit) {
            $tokens = array_slice(search_tokens($query), 0, 5);
            if (telegram_query_fallback_allowed($tokens)) {
                $where = array(
                    'm.deleted_at IS NULL',
                    'm.posted_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL ' . $days . ' DAY)',
                    telegram_message_visibility_sql($config, 'm'),
                );
                $params = array();
                foreach ($tokens as $token) {
                    $where[] = 'm.normalized_text LIKE ?';
                    $params[] = '%' . mb_strtolower($token, 'UTF-8') . '%';
                }
                $sql = 'SELECT m.channel_handle, COALESCE(c.title, m.channel_handle) AS channel_title, m.telegram_message_id, m.posted_at, m.message_url, m.text, m.risk_flags_json, "keyword" AS match_type, 0.5600 AS score, "검색어 문맥 일치" AS reason '
                    . 'FROM ' . table_name($config, 'telegram_messages') . ' m '
                    . 'LEFT JOIN ' . table_name($config, 'telegram_channels') . ' c ON c.handle = m.channel_handle '
                    . 'WHERE ' . implode(' AND ', $where) . ' ORDER BY m.posted_at DESC LIMIT ' . $limit;
                $stmt = $pdo->prepare($sql);
                $stmt->execute($params);
                foreach ($stmt->fetchAll() as $row) {
                    $messagesByKey[(string)$row['message_url']] = public_telegram_message($row, $query);
                }
            }
        }
        respond(200, array('ok' => true, 'messages' => array_slice(array_values($messagesByKey), 0, $limit)));
    }
    if ($action === 'articles') {
        $limit = isset($_GET['limit']) ? max(1, min(40, (int)$_GET['limit'])) : 12;
        $days = isset($_GET['days']) ? max(1, min(180, (int)$_GET['days'])) : 60;
        $query = isset($_GET['q']) ? trim((string)$_GET['q']) : '';
        $storyKey = isset($_GET['story_key']) ? trim((string)$_GET['story_key']) : '';
        if ($query !== '' && mb_strlen($query, 'UTF-8') < 2) {
            respond(400, array('ok' => false, 'error' => 'query_too_short'));
        }
        $where = array(
            'a.canonical_url IS NOT NULL',
            'a.title IS NOT NULL',
            '(a.status IS NULL OR a.status NOT IN ("rejected", "duplicate"))',
            'a.sort_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL ' . $days . ' DAY)',
            legacy_article_visibility_sql('a', 'article_sr'),
        );
        $params = array();
        if ($query !== '') {
            $tokens = search_tokens($query);
            if (!$tokens) {
                respond(400, array('ok' => false, 'error' => 'query_too_short'));
            }
            foreach ($tokens as $token) {
                $like = '%' . $token . '%';
                $where[] = '(a.title LIKE ? OR a.normalized_title LIKE ? OR a.summary LIKE ? OR a.source LIKE ? OR a.feed_name LIKE ? OR a.feed_category LIKE ?)';
                array_push($params, $like, $like, $like, $like, $like, $like);
            }
        }
        if ($storyKey !== '') {
            if (!preg_match('/^[A-Za-z0-9_:\\-]{1,120}$/', $storyKey)) {
                respond(400, array('ok' => false, 'error' => 'invalid_story_key'));
            }
            $where[] = 'a.story_key = ?';
            $params[] = $storyKey;
        }
        $sql = 'SELECT a.record_id, a.canonical_url, a.title, a.summary, a.source, a.feed_name, a.feed_category, a.image_url, a.published_at, a.seen_at, a.status, a.reason, a.relevance_level, a.priority_score, a.priority_level, a.story_key, a.source_right_id, a.sort_at FROM '
            . table_name($config, 'articles') . ' a LEFT JOIN ' . table_name($config, 'source_rights') . ' article_sr ON article_sr.source_right_id=a.source_right_id'
            . ' WHERE ' . implode(' AND ', $where)
            . ' ORDER BY a.sort_at DESC, a.priority_score DESC LIMIT ' . $limit;
        $stmt = $pdo->prepare($sql);
        $stmt->execute($params);
        $rows = array();
        foreach ($stmt->fetchAll() as $row) {
            $rows[] = public_article_row($row, $query);
        }
        respond(200, array('ok' => true, 'articles' => $rows));
    }
}
