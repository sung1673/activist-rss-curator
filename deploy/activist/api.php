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

$origin = isset($_SERVER['HTTP_ORIGIN']) ? $_SERVER['HTTP_ORIGIN'] : '';
$allowedOrigin = isset($config['allowed_origin']) ? (string)$config['allowed_origin'] : '';
if ($origin !== '' && $allowedOrigin !== '' && hash_equals($allowedOrigin, $origin)) {
    header('Access-Control-Allow-Origin: ' . $allowedOrigin);
    header('Vary: Origin');
    header('Access-Control-Allow-Methods: GET, POST, OPTIONS');
    header('Access-Control-Allow-Headers: Content-Type, X-Activist-Timestamp, X-Activist-Nonce, X-Activist-Signature');
}
if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') {
    http_response_code(204);
    exit;
}

$action = isset($_GET['action']) ? (string)$_GET['action'] : 'health';
$method = $_SERVER['REQUEST_METHOD'];

try {
    if ($method === 'GET') {
        handle_read($action, $config);
    } elseif ($method === 'POST') {
        handle_write($action, $config);
    } else {
        respond(405, array('ok' => false, 'error' => 'method_not_allowed'));
    }
} catch (Throwable $e) {
    error_log('[activist-api] ' . $e->getMessage());
    respond(500, array('ok' => false, 'error' => 'internal_error'));
}

function respond(int $status, array $payload): void {
    http_response_code($status);
    echo json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    exit;
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
        payload_json MEDIUMTEXT NULL,
        sort_at DATETIME NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_url_hash (canonical_url_hash),
        INDEX idx_title_hash (title_hash),
        INDEX idx_seen_at (seen_at),
        INDEX idx_published_at (published_at),
        INDEX idx_story_key (story_key),
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
        published_at DATETIME NULL,
        last_article_seen_at DATETIME NULL,
        payload_json MEDIUMTEXT NULL,
        sort_at DATETIME NULL,
        updated_at DATETIME NOT NULL,
        INDEX idx_guid (guid),
        INDEX idx_published_at (published_at),
        INDEX idx_priority (priority_score),
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
    $pdo->exec('DELETE FROM ' . table_name($config, 'api_nonces') . ' WHERE seen_at < DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 DAY)');
    ensure_column($pdo, $config, 'articles', 'sort_at', 'DATETIME NULL');
    ensure_column($pdo, $config, 'stories', 'sort_at', 'DATETIME NULL');
    ensure_index($pdo, $config, 'articles', 'idx_sort_at', 'sort_at');
    ensure_index($pdo, $config, 'articles', 'idx_status_sort', 'status, sort_at');
    ensure_index($pdo, $config, 'stories', 'idx_sort_at', 'sort_at');
    $pdo->exec('UPDATE ' . table_name($config, 'articles') . ' SET sort_at = COALESCE(published_at, seen_at, updated_at) WHERE sort_at IS NULL');
    $pdo->exec('UPDATE ' . table_name($config, 'stories') . ' SET sort_at = COALESCE(published_at, last_article_seen_at, updated_at) WHERE sort_at IS NULL');
    $pdo->exec('DELETE FROM ' . table_name($config, 'article_raw') . ' WHERE retained_until < UTC_TIMESTAMP()');
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
    if ($secret === '') {
        respond(500, array('ok' => false, 'error' => 'secret_missing'));
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
        if ($e->getCode() === '23000') {
            respond(409, array('ok' => false, 'error' => 'nonce_reused'));
        }
        throw $e;
    }
}

function decode_json_body(string $body): array {
    $payload = json_decode($body, true);
    if (!is_array($payload)) {
        respond(400, array('ok' => false, 'error' => 'invalid_json'));
    }
    return $payload;
}

function mysql_dt($value): ?string {
    if (!is_string($value) || trim($value) === '') {
        return null;
    }
    try {
        $dt = new DateTime($value);
        return $dt->format('Y-m-d H:i:s');
    } catch (Throwable $e) {
        return null;
    }
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

function decode_json_array(?string $value): array {
    if ($value === null || $value === '') {
        return array();
    }
    $decoded = json_decode($value, true);
    return is_array($decoded) ? $decoded : array();
}

function public_telegram_message(array $row): array {
    $riskFlags = decode_json_array(isset($row['risk_flags_json']) ? (string)$row['risk_flags_json'] : null);
    return array(
        'channel_handle' => isset($row['channel_handle']) ? (string)$row['channel_handle'] : '',
        'channel_title' => isset($row['channel_title']) && $row['channel_title'] !== null ? (string)$row['channel_title'] : (isset($row['channel_handle']) ? (string)$row['channel_handle'] : ''),
        'telegram_message_id' => isset($row['telegram_message_id']) ? (int)$row['telegram_message_id'] : 0,
        'posted_at' => isset($row['posted_at']) ? (string)$row['posted_at'] : '',
        'message_url' => isset($row['message_url']) ? (string)$row['message_url'] : '',
        'match_type' => isset($row['match_type']) ? (string)$row['match_type'] : 'keyword',
        'score' => isset($row['score']) ? (float)$row['score'] : 0,
        'risk_flags' => $riskFlags,
        'excerpt' => text_excerpt(isset($row['text']) ? (string)$row['text'] : '', 180),
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

function public_telegram_signal(array $row): array {
    $payload = decode_json_array(isset($row['payload_json']) ? (string)$row['payload_json'] : null);
    $messages = array();
    foreach ((isset($payload['top_related_messages']) && is_array($payload['top_related_messages'])) ? $payload['top_related_messages'] : array() as $message) {
        if (!is_array($message)) {
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
        );
    }
    return array(
        'article_id' => isset($row['article_id']) ? (string)$row['article_id'] : '',
        'signal_type' => isset($payload['signal_type']) ? (string)$payload['signal_type'] : 'article_match',
        'signal_title' => isset($payload['signal_title']) ? text_excerpt((string)$payload['signal_title'], 120) : (isset($row['article_id']) ? (string)$row['article_id'] : ''),
        'related_telegram_count' => isset($row['related_telegram_count']) ? (int)$row['related_telegram_count'] : 0,
        'related_telegram_channels_count' => isset($row['related_telegram_channels_count']) ? (int)$row['related_telegram_channels_count'] : 0,
        'first_seen_at' => isset($row['first_seen_at']) ? (string)$row['first_seen_at'] : '',
        'latest_seen_at' => isset($row['latest_seen_at']) ? (string)$row['latest_seen_at'] : '',
        'confidence_score' => isset($row['confidence_score']) ? (float)$row['confidence_score'] : 0,
        'top_channels' => (isset($payload['top_channels']) && is_array($payload['top_channels'])) ? array_slice($payload['top_channels'], 0, 8) : array(),
        'top_keywords' => (isset($payload['top_keywords']) && is_array($payload['top_keywords'])) ? array_slice($payload['top_keywords'], 0, 8) : array(),
        'risk_flags' => (isset($payload['risk_flags']) && is_array($payload['risk_flags'])) ? array_slice($payload['risk_flags'], 0, 8) : array(),
        'top_related_messages' => array_slice($messages, 0, 5),
    );
}

function handle_telegram_dashboard(PDO $pdo, array $config): void {
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
        'messages_14d' => scalar_int($pdo, 'SELECT COUNT(*) FROM ' . $messages . ' WHERE deleted_at IS NULL AND posted_at >= DATE_SUB(' . $referenceSql . ', INTERVAL 14 DAY)'),
        'matches_total' => scalar_int($pdo, 'SELECT COUNT(*) FROM ' . $matches),
        'signals_total' => scalar_int($pdo, 'SELECT COUNT(*) FROM ' . $signals),
    );

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
        $topChannels[] = array(
            'handle' => (string)$row['handle'],
            'title' => (string)($row['title'] ?: $row['handle']),
            'quality_score' => (int)$row['quality_score'],
            'enabled' => (int)$row['enabled'],
            'messages' => (int)$row['messages'],
            'latest_at' => (string)($row['latest_at'] ?: ''),
            'last_collected_at' => (string)($row['last_collected_at'] ?: ''),
            'last_message_id' => (int)$row['last_message_id'],
            'last_error' => (string)($row['last_error'] ?: ''),
        );
    }

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
        $signalRows[] = public_telegram_signal($row);
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
        'signals' => $signalRows,
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
    $allowed = array('upsert_snapshot', 'upsert_report', 'upsert_telegram_snapshot', 'schema');
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
    if ($action === 'upsert_snapshot') {
        upsert_snapshot($pdo, $config, $payload);
    }
    if ($action === 'upsert_report') {
        upsert_report($pdo, $config, isset($payload['report']) && is_array($payload['report']) ? $payload['report'] : $payload);
    }
    if ($action === 'upsert_telegram_snapshot') {
        upsert_telegram_snapshot($pdo, $config, $payload);
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
            priority_level, story_key, sort_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE canonical_url_hash=VALUES(canonical_url_hash), title_hash=VALUES(title_hash), canonical_url=VALUES(canonical_url),
            title=VALUES(title), normalized_title=VALUES(normalized_title), summary=VALUES(summary), source=VALUES(source), feed_name=VALUES(feed_name),
            feed_category=VALUES(feed_category), image_url=VALUES(image_url), published_at=VALUES(published_at), seen_at=VALUES(seen_at),
            status=VALUES(status), reason=VALUES(reason), relevance_level=VALUES(relevance_level), priority_score=VALUES(priority_score),
            priority_level=VALUES(priority_level), story_key=VALUES(story_key), sort_at=VALUES(sort_at), updated_at=VALUES(updated_at)');
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
            article_count, priority_score, published_at, last_article_seen_at, sort_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE guid=VALUES(guid), representative_title=VALUES(representative_title), representative_url=VALUES(representative_url),
            relevance_level=VALUES(relevance_level), theme_group=VALUES(theme_group), status=VALUES(status), article_count=VALUES(article_count),
            priority_score=VALUES(priority_score), published_at=VALUES(published_at), last_article_seen_at=VALUES(last_article_seen_at),
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
    $channels = isset($payload['channels']) && is_array($payload['channels']) ? $payload['channels'] : array();
    $messages = isset($payload['messages']) && is_array($payload['messages']) ? $payload['messages'] : array();
    $matches = isset($payload['article_matches']) && is_array($payload['article_matches']) ? $payload['article_matches'] : array();
    $signals = isset($payload['issue_signals']) && is_array($payload['issue_signals']) ? $payload['issue_signals'] : array();
    if (count($channels) > 1000 || count($messages) > 2500 || count($matches) > 10000 || count($signals) > 1000) {
        respond(413, array('ok' => false, 'error' => 'too_many_records'));
    }
    $now = gmdate('Y-m-d H:i:s');
    $pdo->beginTransaction();
    try {
        $channelStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'telegram_channels') . ' (
            handle, telegram_channel_id, title, description, joined, enabled, source, source_type, is_public_channel,
            quality_score, last_message_id, last_collected_at, last_recommendation_checked_at, last_error, payload_json, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE telegram_channel_id=VALUES(telegram_channel_id), title=VALUES(title), description=VALUES(description),
            joined=VALUES(joined), enabled=VALUES(enabled), source=VALUES(source), source_type=VALUES(source_type),
            is_public_channel=VALUES(is_public_channel), quality_score=VALUES(quality_score), last_message_id=VALUES(last_message_id),
            last_collected_at=VALUES(last_collected_at), last_recommendation_checked_at=VALUES(last_recommendation_checked_at),
            last_error=VALUES(last_error), payload_json=VALUES(payload_json), updated_at=VALUES(updated_at)');
        foreach ($channels as $channel) {
            if (!is_array($channel)) { continue; }
            $handle = normalize_handle_value(isset($channel['handle']) ? $channel['handle'] : (isset($channel['username']) ? $channel['username'] : ''));
            if ($handle === '') { continue; }
            $channelStmt->execute(array(
                $handle,
                str_value($channel, 'telegram_channel_id', 64),
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
        }

        $messageStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'telegram_messages') . ' (
            message_key, channel_handle, telegram_channel_id, telegram_message_id, posted_at, edited_at, deleted_at, collected_at,
            text, normalized_text, views, forwards, replies_count, message_url, urls_json, risk_flags_json, raw_json, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE posted_at=VALUES(posted_at), edited_at=VALUES(edited_at), deleted_at=VALUES(deleted_at),
            collected_at=VALUES(collected_at), text=VALUES(text), normalized_text=VALUES(normalized_text), views=VALUES(views),
            forwards=VALUES(forwards), replies_count=VALUES(replies_count), message_url=VALUES(message_url), urls_json=VALUES(urls_json),
            risk_flags_json=VALUES(risk_flags_json), raw_json=VALUES(raw_json), updated_at=VALUES(updated_at)');
        foreach ($messages as $message) {
            if (!is_array($message)) { continue; }
            $handle = normalize_handle_value(isset($message['handle']) ? $message['handle'] : '');
            $messageId = int_value($message, 'telegram_message_id');
            if ($messageId <= 0) { $messageId = int_value($message, 'id'); }
            if ($handle === '' || $messageId <= 0) { continue; }
            $text = str_value($message, 'text', 1048576) ?: '';
            $riskFlags = telegram_risk_flags($text);
            $rawJson = isset($message['raw_json']) && is_array($message['raw_json']) ? json_value($message['raw_json']) : null;
            $messageStmt->execute(array(
                telegram_message_key_from_row($message),
                $handle,
                str_value($message, 'telegram_channel_id', 64),
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
        }

        $matchStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'telegram_article_matches') . ' (
            article_id, message_key, match_type, score, reason, channel_handle, telegram_message_id, message_url, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE score=VALUES(score), reason=VALUES(reason), channel_handle=VALUES(channel_handle),
            telegram_message_id=VALUES(telegram_message_id), message_url=VALUES(message_url), updated_at=VALUES(updated_at)');
        foreach ($matches as $match) {
            if (!is_array($match)) { continue; }
            $articleId = str_value($match, 'article_id', 96);
            $messageKey = str_value($match, 'telegram_message_key', 180);
            $matchType = str_value($match, 'match_type', 40);
            if ($articleId === null || $articleId === '' || $messageKey === null || $messageKey === '' || $matchType === null || $matchType === '') { continue; }
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
        }

        $signalStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'telegram_issue_signals') . ' (
            article_id, related_telegram_count, related_telegram_channels_count, first_seen_at, latest_seen_at,
            confidence_score, payload_json, updated_at
        ) VALUES (?,?,?,?,?,?,?,?)
        ON DUPLICATE KEY UPDATE related_telegram_count=VALUES(related_telegram_count), related_telegram_channels_count=VALUES(related_telegram_channels_count),
            first_seen_at=VALUES(first_seen_at), latest_seen_at=VALUES(latest_seen_at), confidence_score=VALUES(confidence_score),
            payload_json=VALUES(payload_json), updated_at=VALUES(updated_at)');
        foreach ($signals as $signal) {
            if (!is_array($signal)) { continue; }
            $articleId = str_value($signal, 'article_id', 96);
            if ($articleId === null || $articleId === '') { continue; }
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
        }
        $pdo->commit();
    } catch (Throwable $e) {
        $pdo->rollBack();
        throw $e;
    }
    respond(200, array('ok' => true, 'channels' => count($channels), 'messages' => count($messages), 'article_matches' => count($matches), 'issue_signals' => count($signals)));
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

function handle_read(string $action, array $config): void {
    if ($action === 'health') {
        respond(200, array('ok' => true, 'service' => 'activist', 'time' => gmdate('c')));
    }
    if (!in_array($action, array('reports', 'report', 'latest_snapshot', 'articles', 'telegram_reactions', 'telegram_dashboard'), true)) {
        respond(404, array('ok' => false, 'error' => 'unknown_action'));
    }
    $pdo = pdo_conn($config);
    ensure_schema($pdo, $config);
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
        $stmt = $pdo->prepare('SELECT story_key, guid, representative_title, representative_url, relevance_level, theme_group, status, article_count, priority_score, published_at, last_article_seen_at FROM ' . table_name($config, 'stories') . ' ORDER BY sort_at DESC LIMIT ' . $limit);
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
            $stmt = $pdo->prepare('SELECT record_id FROM ' . table_name($config, 'articles') . ' WHERE canonical_url = ? LIMIT 10');
            $stmt->execute(array($url));
            foreach ($stmt->fetchAll() as $row) {
                $articleIds[] = (string)$row['record_id'];
            }
        }
        if ($articleIds) {
            $placeholders = implode(',', array_fill(0, count($articleIds), '?'));
            $sql = 'SELECT m.channel_handle, COALESCE(c.title, m.channel_handle) AS channel_title, m.telegram_message_id, m.posted_at, m.message_url, m.text, m.risk_flags_json, tm.match_type, tm.score '
                . 'FROM ' . table_name($config, 'telegram_article_matches') . ' tm '
                . 'JOIN ' . table_name($config, 'telegram_messages') . ' m ON m.message_key = tm.message_key '
                . 'LEFT JOIN ' . table_name($config, 'telegram_channels') . ' c ON c.handle = m.channel_handle '
                . 'WHERE tm.article_id IN (' . $placeholders . ') AND m.deleted_at IS NULL '
                . 'AND (tm.match_type IN ("exact_url","canonical_url") OR tm.score >= 0.4500) '
                . 'ORDER BY tm.score DESC, m.posted_at DESC LIMIT ' . $limit;
            $stmt = $pdo->prepare($sql);
            $stmt->execute($articleIds);
            foreach ($stmt->fetchAll() as $row) {
                $messagesByKey[(string)$row['message_url']] = public_telegram_message($row);
            }
        }
        if ($url !== '' && count($messagesByKey) < $limit) {
            $stmt = $pdo->prepare('SELECT m.channel_handle, COALESCE(c.title, m.channel_handle) AS channel_title, m.telegram_message_id, m.posted_at, m.message_url, m.text, m.risk_flags_json, "exact_url" AS match_type, 0.9000 AS score '
                . 'FROM ' . table_name($config, 'telegram_messages') . ' m '
                . 'LEFT JOIN ' . table_name($config, 'telegram_channels') . ' c ON c.handle = m.channel_handle '
                . 'WHERE m.deleted_at IS NULL AND m.urls_json LIKE ? AND m.posted_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL ' . $days . ' DAY) '
                . 'ORDER BY m.posted_at DESC LIMIT ' . $limit);
            $stmt->execute(array('%' . $url . '%'));
            foreach ($stmt->fetchAll() as $row) {
                $messagesByKey[(string)$row['message_url']] = public_telegram_message($row);
            }
        }
        if ($query !== '' && count($messagesByKey) < $limit) {
            $tokens = array_slice(search_tokens($query), 0, 5);
            if (telegram_query_fallback_allowed($tokens)) {
                $where = array('m.deleted_at IS NULL', 'm.posted_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL ' . $days . ' DAY)');
                $params = array();
                foreach ($tokens as $token) {
                    $where[] = 'm.normalized_text LIKE ?';
                    $params[] = '%' . mb_strtolower($token, 'UTF-8') . '%';
                }
                $sql = 'SELECT m.channel_handle, COALESCE(c.title, m.channel_handle) AS channel_title, m.telegram_message_id, m.posted_at, m.message_url, m.text, m.risk_flags_json, "keyword" AS match_type, 0.4600 AS score '
                    . 'FROM ' . table_name($config, 'telegram_messages') . ' m '
                    . 'LEFT JOIN ' . table_name($config, 'telegram_channels') . ' c ON c.handle = m.channel_handle '
                    . 'WHERE ' . implode(' AND ', $where) . ' ORDER BY m.posted_at DESC LIMIT ' . $limit;
                $stmt = $pdo->prepare($sql);
                $stmt->execute($params);
                foreach ($stmt->fetchAll() as $row) {
                    $messagesByKey[(string)$row['message_url']] = public_telegram_message($row);
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
            'canonical_url IS NOT NULL',
            'title IS NOT NULL',
            '(status IS NULL OR status NOT IN ("rejected", "duplicate"))',
            'sort_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL ' . $days . ' DAY)',
        );
        $params = array();
        if ($query !== '') {
            $tokens = search_tokens($query);
            if (!$tokens) {
                respond(400, array('ok' => false, 'error' => 'query_too_short'));
            }
            foreach ($tokens as $token) {
                $like = '%' . $token . '%';
                $where[] = '(title LIKE ? OR normalized_title LIKE ? OR summary LIKE ? OR source LIKE ? OR feed_name LIKE ? OR feed_category LIKE ?)';
                array_push($params, $like, $like, $like, $like, $like, $like);
            }
        }
        if ($storyKey !== '') {
            if (!preg_match('/^[A-Za-z0-9_:\\-]{1,120}$/', $storyKey)) {
                respond(400, array('ok' => false, 'error' => 'invalid_story_key'));
            }
            $where[] = 'story_key = ?';
            $params[] = $storyKey;
        }
        $sql = 'SELECT record_id, canonical_url, title, summary, source, feed_name, feed_category, image_url, published_at, seen_at, status, reason, relevance_level, priority_score, priority_level, story_key, sort_at FROM '
            . table_name($config, 'articles')
            . ' WHERE ' . implode(' AND ', $where)
            . ' ORDER BY sort_at DESC, priority_score DESC LIMIT ' . $limit;
        $stmt = $pdo->prepare($sql);
        $stmt->execute($params);
        respond(200, array('ok' => true, 'articles' => $stmt->fetchAll()));
    }
}
