<?php
declare(strict_types=1);

// Test-only router for PHP's built-in server. Production API requests are served
// from deploy/activist unchanged; these endpoints expose deterministic DB state
// and pin a test lease to avoid one-second boundary flakes on localhost only.
$path = parse_url(isset($_SERVER['REQUEST_URI']) ? (string)$_SERVER['REQUEST_URI'] : '/', PHP_URL_PATH);
if ($path !== '/__test/state' && $path !== '/__test/pin-lease') {
    return false;
}

header('Content-Type: application/json; charset=utf-8');
header('Cache-Control: no-store');

$expectedToken = getenv('PHP73_CI_INSPECTION_TOKEN');
$providedToken = isset($_SERVER['HTTP_X_CI_INSPECTION_TOKEN'])
    ? (string)$_SERVER['HTTP_X_CI_INSPECTION_TOKEN']
    : '';
if (!is_string($expectedToken) || $expectedToken === '' || !hash_equals($expectedToken, $providedToken)) {
    http_response_code(403);
    echo '{"ok":false,"error":"inspection_forbidden"}';
    return true;
}

try {
    $config = require __DIR__ . '/../deploy/activist/_private/config.php';
    $prefix = isset($config['table_prefix']) ? (string)$config['table_prefix'] : 'activist_';
    if (!preg_match('/^[A-Za-z0-9_]+$/', $prefix)) {
        throw new RuntimeException('invalid_table_prefix');
    }
    $table = static function (string $name) use ($prefix): string {
        return '`' . $prefix . $name . '`';
    };
    $dsn = 'mysql:host=' . (string)$config['db_host']
        . ';port=' . (int)$config['db_port']
        . ';dbname=' . (string)$config['db_name']
        . ';charset=' . (string)$config['db_charset'];
    $pdo = new PDO($dsn, (string)$config['db_user'], (string)$config['db_password'], array(
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
        PDO::ATTR_EMULATE_PREPARES => false,
    ));

    if ($path === '/__test/pin-lease') {
        if (!isset($_SERVER['REQUEST_METHOD']) || $_SERVER['REQUEST_METHOD'] !== 'POST') {
            http_response_code(405);
            echo '{"ok":false,"error":"method_not_allowed"}';
            return true;
        }
        $rebuildToken = isset($_SERVER['HTTP_X_CI_REBUILD_TOKEN'])
            ? (string)$_SERVER['HTTP_X_CI_REBUILD_TOKEN'] : '';
        if (!preg_match('/^[a-f0-9]{64}$/', $rebuildToken)) {
            http_response_code(400);
            echo '{"ok":false,"error":"invalid_rebuild_token"}';
            return true;
        }
        $pinLease = $pdo->prepare(
            'UPDATE ' . $table('telegram_signal_rebuild_state')
            . ' SET updated_at = DATE_ADD(UTC_TIMESTAMP(), INTERVAL 30 SECOND)'
            . " WHERE state_key = 'global' AND active_token = ?"
        );
        $pinLease->execute(array($rebuildToken));
        if ($pinLease->rowCount() !== 1) {
            http_response_code(409);
            echo '{"ok":false,"error":"active_rebuild_not_found"}';
            return true;
        }
        http_response_code(200);
        echo '{"ok":true,"lease_pinned_seconds":30}';
        return true;
    }

    $state = $pdo->query(
        'SELECT active_token, finalized_token, live_revision FROM '
        . $table('telegram_signal_rebuild_state') . " WHERE state_key = 'global'"
    )->fetch();
    $signals = $pdo->query(
        'SELECT article_id, related_telegram_count, latest_seen_at FROM '
        . $table('telegram_issue_signals') . ' ORDER BY article_id'
    )->fetchAll();
    $staging = $pdo->query(
        'SELECT rebuild_token, article_id FROM '
        . $table('telegram_signal_rebuild_staging') . ' ORDER BY rebuild_token, article_id'
    )->fetchAll();
    $channels = $pdo->query(
        'SELECT handle, telegram_channel_id FROM '
        . $table('telegram_channels') . ' ORDER BY handle'
    )->fetchAll();
    $messages = $pdo->query(
        'SELECT message_key, channel_handle, telegram_channel_id, telegram_message_id FROM '
        . $table('telegram_messages') . ' ORDER BY message_key'
    )->fetchAll();
    $matches = $pdo->query(
        'SELECT article_id, message_key, match_type FROM '
        . $table('telegram_article_matches') . ' ORDER BY article_id, message_key, match_type'
    )->fetchAll();

    http_response_code(200);
    echo json_encode(array(
        'ok' => true,
        'state' => $state,
        'signals' => $signals,
        'staging' => $staging,
        'channels' => $channels,
        'messages' => $messages,
        'matches' => $matches,
    ), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
} catch (Throwable $e) {
    error_log('[php73-ci-router] ' . $e->getMessage());
    http_response_code(500);
    echo '{"ok":false,"error":"inspection_failed"}';
}

return true;
