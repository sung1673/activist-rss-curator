<?php
/**
 * BSIDE Governance Intelligence API v1.
 *
 * This file is loaded by api.php after the private configuration. It contains
 * public read routes, role-gated operations routes, ingestion contracts and
 * outbox/link-discovery lease handlers. Existing ?action= APIs remain in
 * api.php and keep their original response shapes.
 */

const V1_RESPONSE_BUDGET_BYTES = 250000;
const V1_DEFAULT_PAGE_SIZE = 25;
const V1_MAX_PAGE_SIZE = 100;
const V1_CORRECTION_LOOKBACK_DAYS = 730;
const GOV_V1_SCHEMA_VERSION = 10;
const GOV_V1_RELEASE_STATE_KEY = 'governance_v1';
const GOV_V1_AVAILABILITY_CADENCE_ID = 'watchdog-v1-kst-5m-minute01';
const GOV_V1_AVAILABILITY_SLOTS_PER_DAY = 288;
const GOV_V1_DART_GLOBAL_DAILY_LIMIT = 40000;

/** Decode and normalize a route exactly once before both CORS and dispatch. */
function v1_canonical_route_path(string $value): string {
    $decoded = rawurldecode($value);
    if (preg_match('/[\x00-\x1f\x7f\\\\]/', $decoded) === 1) {
        return '/__invalid_route__';
    }
    return '/' . trim($decoded, '/');
}

function v1_request_path(): ?string {
    if (isset($_GET['_route'])) {
        $route = v1_canonical_route_path(trim((string)$_GET['_route']));
        if (strpos($route, '/api/v1') === 0) {
            $rest = substr($route, strlen('/api/v1'));
            return $rest === '' ? '/' : '/' . trim($rest, '/');
        }
    }
    $candidates = array();
    if (isset($_SERVER['PATH_INFO'])) {
        $candidates[] = (string)$_SERVER['PATH_INFO'];
    }
    if (isset($_SERVER['REQUEST_URI'])) {
        $uriPath = parse_url((string)$_SERVER['REQUEST_URI'], PHP_URL_PATH);
        if (is_string($uriPath)) {
            $candidates[] = $uriPath;
        }
    }
    foreach ($candidates as $candidate) {
        $candidate = v1_canonical_route_path($candidate);
        $marker = '/api/v1';
        $position = strpos($candidate, $marker);
        if ($position === false) {
            continue;
        }
        $rest = substr($candidate, $position + strlen($marker));
        if ($rest !== '' && substr($rest, 0, 1) !== '/') {
            continue;
        }
        return $rest === '' ? '/' : '/' . trim($rest, '/');
    }
    return null;
}

function v1_respond(int $status, array $payload): void {
    $payload['api_version'] = 'v1';
    $encoded = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    if ($encoded === false) {
        respond(500, array('ok' => false, 'error' => 'json_encoding_failed'));
    }
    if (strlen($encoded) > V1_RESPONSE_BUDGET_BYTES) {
        respond(500, array(
            'ok' => false,
            'error' => 'response_budget_exceeded',
            'max_bytes' => V1_RESPONSE_BUDGET_BYTES,
        ));
    }
    header('Content-Type: application/json; charset=utf-8');
    header('X-BSIDE-API-Version: v1');
    header('X-Response-Bytes: ' . strlen($encoded));
    http_response_code($status);
    echo $encoded;
    exit;
}

function v1_list_params(): array {
    $limit = isset($_GET['limit']) ? (int)$_GET['limit'] : V1_DEFAULT_PAGE_SIZE;
    $page = isset($_GET['page']) ? (int)$_GET['page'] : 1;
    $limit = max(1, min(V1_MAX_PAGE_SIZE, $limit));
    $page = max(1, min(100000, $page));
    return array(
        'limit' => $limit,
        'page' => $page,
        'offset' => ($page - 1) * $limit,
    );
}

function v1_page_meta(array $page, int $returned, bool $hasMore): array {
    return array(
        'page' => (int)$page['page'],
        'limit' => (int)$page['limit'],
        'returned' => $returned,
        'has_more' => $hasMore,
        'next_page' => $hasMore ? ((int)$page['page'] + 1) : null,
    );
}

function v1_fetch_page(PDOStatement $statement, array $page): array {
    $rows = $statement->fetchAll();
    $hasMore = count($rows) > (int)$page['limit'];
    if ($hasMore) {
        $rows = array_slice($rows, 0, (int)$page['limit']);
    }
    return array($rows, $hasMore);
}

function v1_valid_entity_id(string $value, int $max = 96): bool {
    return $value !== ''
        && strlen($value) <= $max
        && preg_match('/^[A-Za-z0-9_.:\-]+$/', $value) === 1;
}

function v1_mysql_datetime_utc($value): ?string {
    if (!is_string($value) || trim($value) === '') { return null; }
    try {
        $utc = new DateTimeZone('UTC');
        $datetime = new DateTimeImmutable(trim($value), $utc);
        return $datetime->setTimezone($utc)->format('Y-m-d H:i:s');
    } catch (Throwable $e) {
        return null;
    }
}

/** Editorial writes must carry an explicit UTC offset; naive local time is rejected. */
function v1_editorial_datetime_utc($value): ?string {
    if (!is_string($value) || preg_match('/(?:Z|[+-][0-9]{2}:[0-9]{2})$/', trim($value)) !== 1) {
        return null;
    }
    return v1_mysql_datetime_utc($value);
}

function v1_editorial_language($value): ?string {
    if (!is_string($value) || preg_match('/^[a-z]{2,3}(?:-[A-Z]{2})?$/', $value) !== 1) {
        return null;
    }
    return $value;
}

function v1_bool_int($value): int {
    if (is_bool($value)) { return $value ? 1 : 0; }
    if (is_int($value) || is_float($value)) { return ((int)$value) === 1 ? 1 : 0; }
    if (is_string($value)) {
        return in_array(strtolower(trim($value)), array('1', 'true', 'yes', 'on'), true) ? 1 : 0;
    }
    return 0;
}

function v1_bearer_token(): string {
    $header = '';
    if (isset($_SERVER['HTTP_AUTHORIZATION'])) {
        $header = (string)$_SERVER['HTTP_AUTHORIZATION'];
    } elseif (isset($_SERVER['REDIRECT_HTTP_AUTHORIZATION'])) {
        $header = (string)$_SERVER['REDIRECT_HTTP_AUTHORIZATION'];
    }
    if (!preg_match('/^Bearer\s+([^\s]{20,512})$/i', trim($header), $matches)) {
        return '';
    }
    return (string)$matches[1];
}

function v1_role_hashes(array $config, string $role): array {
    $hashes = array();
    $key = $role . '_bearer_token_hash';
    if (isset($config[$key]) && preg_match('/^[a-f0-9]{64}$/i', (string)$config[$key])) {
        $hashes[] = strtolower((string)$config[$key]);
    }
    if (isset($config['role_token_hashes']) && is_array($config['role_token_hashes']) && isset($config['role_token_hashes'][$role])) {
        $configured = $config['role_token_hashes'][$role];
        if (!is_array($configured)) {
            $configured = array($configured);
        }
        foreach ($configured as $hash) {
            if (preg_match('/^[a-f0-9]{64}$/i', (string)$hash)) {
                $hashes[] = strtolower((string)$hash);
            }
        }
    }
    return array_values(array_unique($hashes));
}

function v1_require_role(array $config, array $allowedRoles): string {
    v1_require_disjoint_protected_role_hashes($config);
    $token = v1_bearer_token();
    if ($token === '') {
        header('WWW-Authenticate: Bearer realm="BSIDE API", charset="UTF-8"');
        v1_respond(401, array('ok' => false, 'error' => 'bearer_token_required'));
    }
    $candidate = hash('sha256', $token);
    $roles = array_values(array_unique(array_merge($allowedRoles, array('admin'))));
    foreach ($roles as $role) {
        foreach (v1_role_hashes($config, (string)$role) as $expected) {
            if (hash_equals($expected, $candidate)) {
                return (string)$role;
            }
        }
    }
    v1_respond(403, array('ok' => false, 'error' => 'insufficient_role'));
}

/** Normalize identity text using the same conservative profile as the Python engine. */
function v1_normalize_identity_text($value): string {
    $text = is_string($value) || is_numeric($value) ? (string)$value : '';
    if (class_exists('Normalizer')) {
        $normalized = Normalizer::normalize($text, Normalizer::FORM_KC);
        if (is_string($normalized)) { $text = $normalized; }
    }
    $collapsed = preg_replace('/\s+/u', ' ', $text);
    if (is_string($collapsed)) { $text = $collapsed; }
    return mb_strtolower(trim($text), 'UTF-8');
}

/**
 * Return the canonical comparison value and MySQL UTC storage value.
 * Date-only inputs remain date-only in the hash. Naive timestamps are accepted
 * only while validating values already read from a UTC DATETIME column.
 */
function v1_normalize_identity_datetime($value, bool $allowMysqlUtc = false): ?array {
    if (!is_string($value) && !is_numeric($value)) { return null; }
    $text = trim((string)$value);
    if (preg_match('/^(\d{4})(\d{2})(\d{2})$/', $text, $parts) === 1) {
        $text = $parts[1] . '-' . $parts[2] . '-' . $parts[3];
    }
    if (preg_match('/^(\d{4})-(\d{2})-(\d{2})$/', $text, $parts) === 1) {
        if (!checkdate((int)$parts[2], (int)$parts[3], (int)$parts[1])) { return null; }
        return array('canonical'=>$text, 'mysql'=>$text . ' 00:00:00');
    }
    if ($allowMysqlUtc && preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $text) === 1) {
        $mysql = v1_mysql_datetime_utc($text);
    } else {
        if (preg_match('/(?:Z|[+-][0-9]{2}:[0-9]{2})$/i', $text) !== 1) { return null; }
        $mysql = v1_mysql_datetime_utc($text);
    }
    if ($mysql === null) { return null; }
    return array('canonical'=>str_replace(' ', 'T', $mysql) . '+00:00', 'mysql'=>$mysql);
}

/** Build, normalize and hash all seven event identity dimensions server-side. */
function v1_build_event_identity(string $companyId, string $eventType, $action, $target, $actorId,
    $effectiveAt, $deadlineAt, bool $allowMysqlUtc = false): ?array {
    $company = trim($companyId);
    $type = str_replace('-', '_', v1_normalize_identity_text($eventType));
    $normalizedAction = v1_normalize_identity_text($action);
    $normalizedTarget = v1_normalize_identity_text($target);
    $normalizedActor = v1_normalize_identity_text($actorId);
    $effective = v1_normalize_identity_datetime($effectiveAt, $allowMysqlUtc);
    $deadline = v1_normalize_identity_datetime($deadlineAt, $allowMysqlUtc);
    if (preg_match('/^[0-9]{8}$/', $company) !== 1
        || preg_match('/^[a-z][a-z0-9_]{0,63}$/', $type) !== 1
        || $normalizedAction === '' || mb_strlen($normalizedAction, 'UTF-8') > 255
        || $normalizedTarget === '' || mb_strlen($normalizedTarget, 'UTF-8') > 700
        || !v1_valid_entity_id($normalizedActor, 64) || $effective === null || $deadline === null) {
        return null;
    }
    $values = array($company,$type,$normalizedAction,$normalizedTarget,$normalizedActor,
        (string)$effective['canonical'],(string)$deadline['canonical']);
    $comparisonKey = 'eventcmp:v1:' . hash('sha256', implode("\x1f", array_merge(array('governance-event-identity-v1'),$values)));
    return array(
        'company_id'=>$company,'event_type'=>$type,'identity_action'=>$normalizedAction,'identity_target'=>$normalizedTarget,
        'identity_actor_id'=>$normalizedActor,'identity_effective_at'=>(string)$effective['mysql'],
        'identity_deadline_at'=>(string)$deadline['mysql'],'comparison_key'=>$comparisonKey,
    );
}

/**
 * Recover the canonical identity represented by a stored MySQL DATETIME pair.
 *
 * DATETIME loses whether midnight originated as a date-only value or as an
 * explicit UTC timestamp. The stored comparison key disambiguates those two
 * representations without coercing a real midnight timestamp to date-only.
 */
function v1_resolve_stored_event_identity(string $companyId, string $eventType, $action, $target, $actorId,
    $effectiveAt, $deadlineAt, $comparisonKey): ?array {
    $storedKey = is_string($comparisonKey) ? $comparisonKey : '';
    if (preg_match('/^eventcmp:v1:[a-f0-9]{64}$/',$storedKey) !== 1) { return null; }
    if (!is_string($effectiveAt) || !is_string($deadlineAt)
        || preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/',$effectiveAt) !== 1
        || preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/',$deadlineAt) !== 1) {
        return null;
    }
    $storedTuple = array((string)$companyId,(string)$eventType,(string)$action,(string)$target,(string)$actorId,
        $effectiveAt,$deadlineAt);
    $candidateSets = array();
    foreach (array($effectiveAt,$deadlineAt) as $value) {
        $mysql = (string)$value;
        $candidates = array($mysql);
        if (preg_match('/^(\d{4}-\d{2}-\d{2}) 00:00:00$/',$mysql,$parts) === 1) {
            array_unshift($candidates,$parts[1]);
        }
        $candidateSets[] = array_values(array_unique($candidates));
    }
    $matches = array();
    foreach ($candidateSets[0] as $effectiveCandidate) {
        foreach ($candidateSets[1] as $deadlineCandidate) {
            $identity = v1_build_event_identity($companyId,$eventType,$action,$target,$actorId,
                $effectiveCandidate,$deadlineCandidate,true);
            if ($identity !== null && hash_equals($storedKey,(string)$identity['comparison_key'])) {
                $normalizedTuple = array((string)$identity['company_id'],(string)$identity['event_type'],
                    (string)$identity['identity_action'],(string)$identity['identity_target'],
                    (string)$identity['identity_actor_id'],(string)$identity['identity_effective_at'],
                    (string)$identity['identity_deadline_at']);
                if ($normalizedTuple === $storedTuple) { $matches[] = $identity; }
            }
        }
    }
    return count($matches) === 1 ? $matches[0] : null;
}

function v1_expected_migration_manifest(): array {
    return array(
        1=>array('001_governance_v1','2f1f03aa62d733339b79b5bca50e1c480b4f706a5823fd3490bd799421e93afd'),
        2=>array('002_legacy_source_right_lineage','fdcb2d634a787c7bbe534bd3892470a13aef11254dd75cec1afb54a9f2b61051'),
        3=>array('003_editorial_governance','906a0071bc11b595eae388a17074bd955f1ebb25f8a7453e3e89534e42ba4f25'),
        4=>array('004_telegram_signal_rebuild_staging','de64071e117fae70d6849f8191be7267a885e75bf3d498ab7488fa616348fb7f'),
        5=>array('005_telegram_channel_identity_index','cf1245fe562e583707d821f126562a6f10aa9c8db5e0c9b20afa8ff267d1d903'),
        6=>array('006_governance_release_guard','f7f7a46f86118316dc21a67bb5b547668d64978b9fe4054b4c86104b85d7ced7'),
        7=>array('007_governance_identity_and_evidence','074bbb5f066d5f3a20e3b894762ae356fa0a102c61546634fc16be05400f2ebe'),
        8=>array('008_official_site_snapshot_receipts','b12e5e5290a5901192ddb4c8ec999719aa3dc25596c6c46d16ac383f3be74376'),
        9=>array('009_dart_global_quota_ledger','9e60867847b7cc2b7d9166c73e395ae872d12a4e91aa62457049468017e5f94d'),
        10=>array('010_official_slot_claim_ledger','2b8be6264c8a4f3be038729fbf6bbe22e720457874f02c89c82d33db9dc78f51'),
    );
}

function v1_schema_manifest_status(PDO $pdo, array $config): array {
    try {
        $stmt = $pdo->query('SELECT migration_version,migration_name,migration_checksum FROM '
            . table_name($config,'schema_migrations') . ' WHERE migration_version<='
            . GOV_V1_SCHEMA_VERSION . ' ORDER BY migration_version');
        $rows = $stmt->fetchAll();
    } catch (PDOException $e) {
        return array('valid'=>false,'highest_version'=>null,'error'=>'migration_manifest_unavailable');
    }
    $expected = v1_expected_migration_manifest(); $highest = null;
    foreach ($rows as $row) {
        if (isset($row['migration_version']) && is_numeric($row['migration_version'])) {
            $highest = max((int)($highest === null ? 0 : $highest),(int)$row['migration_version']);
        }
    }
    if (count($rows) !== count($expected)) {
        return array('valid'=>false,'highest_version'=>$highest,'error'=>'migration_manifest_cardinality_mismatch');
    }
    foreach ($rows as $index => $row) {
        $version = $index + 1;
        if ((int)$row['migration_version'] !== $version || !isset($expected[$version])
            || !hash_equals($expected[$version][0],(string)$row['migration_name'])
            || !hash_equals($expected[$version][1],strtolower((string)$row['migration_checksum']))) {
            return array('valid'=>false,'highest_version'=>$highest,'error'=>'migration_manifest_entry_mismatch',
                'invalid_version'=>$version);
        }
    }
    return array('valid'=>true,'highest_version'=>GOV_V1_SCHEMA_VERSION,'error'=>null);
}

function v1_current_schema_version(PDO $pdo, array $config): ?int {
    $manifest = v1_schema_manifest_status($pdo,$config);
    return $manifest['valid'] === true ? GOV_V1_SCHEMA_VERSION : null;
}

function v1_require_schema_version(PDO $pdo, array $config): int {
    $manifest = v1_schema_manifest_status($pdo,$config);
    if ($manifest['valid'] !== true) {
        header('Retry-After: 300');
        v1_respond(503, array(
            'ok' => false,
            'error' => 'schema_version_mismatch',
            'expected_schema_version' => GOV_V1_SCHEMA_VERSION,
            'actual_schema_version' => $manifest['highest_version'],
            'schema_manifest_error' => $manifest['error'],
        ));
    }
    return GOV_V1_SCHEMA_VERSION;
}

/** Quota pooling depends on migration 012 even though the wider v1 API remains at schema 10. */
function v1_require_dart_quota_schema(PDO $pdo, array $config): void {
    if (!function_exists('v2_schema_manifest_status')) {
        header('Retry-After: 300');
        v1_respond(503,array(
            'ok'=>false,
            'error'=>'dart_quota_schema_unavailable',
            'expected_schema_version'=>12,
            'actual_schema_version'=>null,
        ));
    }
    $manifest=v2_schema_manifest_status($pdo,$config);
    if (!is_array($manifest) || ($manifest['valid'] ?? false) !== true
        || (int)($manifest['highest_version'] ?? 0) !== 12) {
        header('Retry-After: 300');
        v1_respond(503,array(
            'ok'=>false,
            'error'=>'dart_quota_schema_unavailable',
            'expected_schema_version'=>12,
            'actual_schema_version'=>$manifest['highest_version'] ?? null,
            'schema_manifest_error'=>$manifest['error'] ?? 'migration_manifest_unavailable',
        ));
    }
}

function v1_preview_token_hashes(array $config): array {
    $hashes = v1_role_hashes($config, 'preview');
    if (isset($config['governance_preview_token_hash'])
        && preg_match('/^[a-f0-9]{64}$/i', (string)$config['governance_preview_token_hash'])) {
        $hashes[] = strtolower((string)$config['governance_preview_token_hash']);
    }
    return array_values(array_unique($hashes));
}

/**
 * Fail closed when one bearer credential is assigned to more than one
 * protected role. Administrator fallback is implemented by v1_require_role;
 * copying the administrator hash into another role is therefore unnecessary
 * and would erase the separation required by protected release approval.
 *
 * The returned status intentionally contains role names only. Token hashes
 * must never appear in an API response, log or diagnostic artifact.
 */
function v1_protected_role_hash_overlap_status(array $config): array {
    $roleHashes = array(
        'admin'=>v1_role_hashes($config,'admin'),
        'editor'=>v1_role_hashes($config,'editor'),
        'ops'=>v1_role_hashes($config,'ops'),
        'preview'=>v1_preview_token_hashes($config),
        'release_authorizer'=>v1_role_hashes($config,'release_authorizer'),
        'rights'=>v1_role_hashes($config,'rights'),
    );
    $owners = array();
    foreach ($roleHashes as $role=>$hashes) {
        foreach ($hashes as $hash) {
            if (!isset($owners[$hash])) { $owners[$hash]=array(); }
            $owners[$hash][]=$role;
        }
    }
    $pairs = array();
    foreach ($owners as $roles) {
        $roles=array_values(array_unique($roles));
        sort($roles,SORT_STRING);
        $roleCount=count($roles);
        for ($left=0;$left<$roleCount;$left++) {
            for ($right=$left+1;$right<$roleCount;$right++) {
                $pairKey=$roles[$left]."\x1f".$roles[$right];
                $pairs[$pairKey]=array($roles[$left],$roles[$right]);
            }
        }
    }
    ksort($pairs,SORT_STRING);
    $conflicts=array_values($pairs);
    return array(
        'valid'=>count($conflicts)===0,
        'conflict_count'=>count($conflicts),
        'conflicting_roles'=>$conflicts,
    );
}

function v1_require_disjoint_protected_role_hashes(array $config): void {
    $status=v1_protected_role_hash_overlap_status($config);
    if ($status['valid']!==true) {
        header('Retry-After: 300');
        v1_respond(503,array(
            'ok'=>false,
            'error'=>'protected_role_token_hash_overlap',
            'conflict_count'=>$status['conflict_count'],
            'conflicting_roles'=>$status['conflicting_roles'],
        ));
    }
}

function v1_preview_auth_configured(array $config): bool {
    return count(v1_preview_token_hashes($config)) > 0;
}

function v1_require_preview_token(array $config): void {
    v1_require_disjoint_protected_role_hashes($config);
    $token = v1_bearer_token();
    if ($token === '') {
        header('WWW-Authenticate: Bearer realm="BSIDE governance preview", charset="UTF-8"');
        v1_respond(401, array('ok' => false, 'error' => 'preview_token_required'));
    }
    $candidate = hash('sha256', $token);
    foreach (v1_preview_token_hashes($config) as $expected) {
        if (hash_equals($expected, $candidate)) { return; }
    }
    v1_respond(403, array('ok' => false, 'error' => 'invalid_preview_token'));
}

function v1_release_state(PDO $pdo, array $config, bool $forUpdate = false): ?array {
    $sql = 'SELECT release_state, state_version, updated_by, update_reason, cutover_at, sunset_at, updated_at FROM '
        . table_name($config, 'governance_release_state') . ' WHERE state_key = ?'
        . ($forUpdate ? ' FOR UPDATE' : '');
    $stmt = $pdo->prepare($sql);
    $stmt->execute(array(GOV_V1_RELEASE_STATE_KEY));
    $row = $stmt->fetch();
    return is_array($row) ? $row : null;
}

function v1_require_public_release_access(PDO $pdo, array $config): string {
    $row = v1_release_state($pdo, $config);
    $state = is_array($row) && isset($row['release_state']) ? (string)$row['release_state'] : '';
    if (!in_array($state, array('closed', 'preview', 'live'), true)) {
        header('Retry-After: 300');
        v1_respond(503, array('ok' => false, 'error' => 'release_state_unavailable'));
    }
    if ($state === 'closed') {
        header('Retry-After: 300');
        v1_respond(503, array('ok' => false, 'error' => 'governance_release_closed'));
    }
    if ($state === 'preview') {
        v1_require_preview_token($config);
        header('Cache-Control: private, no-store');
        header('Vary: Authorization');
        return $state;
    }
    header('Cache-Control: public, max-age=60, stale-while-revalidate=300');
    return $state;
}

function handle_v1_request(string $method, string $path, array $config): void {
    // v1_request_path already canonicalized the path used by CORS. Avoid a
    // second decode that could classify an encoded privileged path as public.
    $path = '/' . trim($path, '/');
    if ($method === 'GET' && $path === '/health') {
        v1_respond(200, array('ok' => true, 'service' => 'bside-governance-intelligence', 'time' => gmdate('c')));
    }
    if ($method === 'GET' && ($path === '/openapi.yaml' || $path === '/openapi.json')) {
        v1_serve_openapi($path);
    }

    $preauthorizedRole = null;
    $opsReadPaths = array('/ops/health', '/ops/runtime-state', '/ops/release-evidence', '/ops/official-run-ledger', '/ops/dart-review-corpus', '/ops/official-slot-claims', '/ops/dart-quota',
        '/ops/official-site-candidates', '/ops/official-site-rights', '/ops/source-right-eligibility');
    if ($method === 'GET' && in_array($path, $opsReadPaths, true)) {
        $preauthorizedRole = v1_require_role($config, array('ops'));
    } elseif (preg_match('#^/ops/backfill-checkpoints/[a-f0-9]{64}$#', $path) === 1 && in_array($method, array('GET','PUT'), true)) {
        $preauthorizedRole = v1_require_role($config, array('ops'));
    } elseif ($method === 'POST' && in_array($path,array('/ops/availability-observations','/ops/web-distribution-observations','/ops/official-slot-claims','/ops/dart-quota'),true)) {
        $preauthorizedRole = v1_require_role($config, array('ops'));
    } elseif ($method === 'POST' && $path === '/ops/quality-observations') {
        $preauthorizedRole = v1_require_role($config, array('ops','editor'));
    } elseif ($path === '/admin/release-state') {
        $preauthorizedRole = v1_require_role($config, array('admin'));
    } elseif ($path === '/admin/official-slot-epoch') {
        $preauthorizedRole = v1_require_role($config, array('admin'));
    } elseif ($path === '/admin/shadow-discrepancies' || $path === '/admin/shadow-runs'
        || $path === '/admin/release-evidence-inputs') {
        $preauthorizedRole = v1_require_role($config, array('editor'));
    } elseif (strpos($path, '/admin/') === 0) {
        if ($method !== 'GET' && $method !== 'POST') {
            header('Allow: GET, POST');
            v1_respond(405, array('ok' => false, 'error' => 'method_not_allowed'));
        }
    } elseif ($method !== 'GET' && !($method === 'POST' && in_array($path, array('/feedback', '/metrics/web-vitals'), true))) {
        header('Allow: GET, POST');
        v1_respond(405, array('ok' => false, 'error' => 'method_not_allowed'));
    }

    $pdo = pdo_conn($config);
    v1_require_schema_version($pdo, $config);
    if ($path === '/ops/dart-quota') {
        v1_require_dart_quota_schema($pdo,$config);
    }
    $privileged = strpos($path, '/ops/') === 0 || strpos($path, '/admin/') === 0;
    if (!$privileged) {
        v1_require_public_release_access($pdo, $config);
    }

    if ($method === 'GET') {
        if ($path === '/') {
            v1_respond(200, array(
                'ok' => true,
                'service' => 'bside-governance-intelligence',
                'documentation' => '/api/v1/openapi.yaml',
            ));
        }
        if ($path === '/companies') { v1_list_companies($pdo, $config); }
        if (preg_match('#^/companies/([0-9]{8})$#', $path, $m)) { v1_get_company($pdo, $config, $m[1]); }
        if ($path === '/actors') { v1_list_actors($pdo, $config); }
        if (preg_match('#^/actors/([A-Za-z0-9_.:\-]{1,64})$#', $path, $m)) { v1_get_actor($pdo, $config, $m[1]); }
        if ($path === '/events') { v1_list_events($pdo, $config); }
        if ($path === '/today') { v1_today($pdo, $config); }
        if (preg_match('#^/events/([A-Za-z0-9_.:\-]{1,96})$#', $path, $m)) { v1_get_event($pdo, $config, $m[1]); }
        if (preg_match('#^/campaigns/([A-Za-z0-9_.:\-]{1,96})$#', $path, $m)) { v1_get_campaign($pdo, $config, $m[1]); }
        if (preg_match('#^/documents/([A-Za-z0-9_.:\-]{1,96})$#', $path, $m)) { v1_get_document($pdo, $config, $m[1]); }
        if ($path === '/calendar') { v1_calendar($pdo, $config); }
        if ($path === '/search') { v1_search($pdo, $config); }
        if ($path === '/revisions') { v1_public_revisions($pdo, $config); }
        if ($path === '/exports/events.json') { v1_export_events_json($pdo, $config); }
        if ($path === '/exports/events.csv') { v1_export_events_csv($pdo, $config); }
        if ($path === '/feeds/events.atom') { v1_events_atom($pdo, $config); }
        if ($path === '/ops/health') { v1_ops_health($pdo, $config); }
        if ($path === '/ops/runtime-state') { v1_runtime_state_route($pdo, $config); }
        if ($path === '/ops/release-evidence') { v1_ops_release_evidence($pdo, $config); }
        if ($path === '/ops/official-run-ledger') { v1_ops_official_run_ledger($pdo, $config); }
        if ($path === '/ops/dart-review-corpus') { v1_ops_dart_review_corpus($pdo, $config); }
        if ($path === '/ops/official-slot-claims') { v1_ops_official_slot_claims($pdo, $config); }
        if ($path === '/ops/dart-quota') { v1_ops_dart_quota_status($pdo, $config); }
        if ($path === '/ops/official-site-candidates') { v1_ops_official_site_candidates($pdo, $config); }
        if ($path === '/ops/official-site-rights') { v1_ops_official_site_rights($pdo, $config); }
        if ($path === '/ops/source-right-eligibility') { v1_ops_source_right_eligibility($pdo, $config); }
        if (preg_match('#^/ops/backfill-checkpoints/([a-f0-9]{64})$#', $path, $m)) {
            v1_ops_get_backfill_checkpoint($pdo, $config, $m[1]);
        }
        if ($path === '/admin/release-state') {
            v1_admin_release_state($pdo, $config);
        }
        if ($path === '/admin/official-slot-epoch') {
            v1_admin_official_slot_epoch($pdo, $config);
        }
        if ($path === '/admin/shadow-discrepancies') {
            v1_admin_shadow_discrepancies($pdo, $config);
        }
        if ($path === '/admin/shadow-runs') {
            v1_admin_shadow_runs($pdo, $config);
        }
        if ($path === '/admin/release-evidence-inputs') {
            v1_admin_release_evidence_inputs($pdo, $config);
        }
        if ($path === '/admin/review-queue') {
            v1_require_role($config, array('editor'));
            v1_admin_review_queue($pdo, $config);
        }
        if ($path === '/admin/source-rights') {
            v1_require_role($config, array('rights'));
            v1_admin_source_rights($pdo, $config);
        }
        if ($path === '/admin/editorial-revisions') {
            v1_require_role($config, array('editor'));
            v1_admin_editorial_revisions($pdo, $config);
        }
        if ($path === '/admin/feedback') {
            v1_require_role($config, array('editor'));
            v1_admin_feedback($pdo, $config);
        }
    }
    if ($method === 'POST') {
        if ($path === '/feedback') { v1_submit_feedback($pdo, $config); }
        if ($path === '/metrics/web-vitals') { v1_record_web_vitals($pdo, $config); }
        if ($path === '/ops/availability-observations') {
            v1_record_availability_observations($pdo, $config, (string)$preauthorizedRole);
        }
        if ($path === '/ops/web-distribution-observations') {
            v1_record_web_distribution_observations($pdo, $config, (string)$preauthorizedRole);
        }
        if ($path === '/ops/quality-observations') {
            v1_record_quality_observations($pdo, $config, (string)$preauthorizedRole);
        }
        if ($path === '/ops/dart-quota') { v1_ops_dart_quota_write($pdo, $config); }
        if ($path === '/ops/official-slot-claims') { v1_ops_official_slot_claim_write($pdo, $config); }
        if ($path === '/admin/release-state') {
            v1_admin_update_release_state($pdo, $config, (string)$preauthorizedRole);
        }
        if ($path === '/admin/official-slot-epoch') {
            v1_admin_reset_official_slot_epoch($pdo, $config, (string)$preauthorizedRole);
        }
        if ($path === '/admin/shadow-discrepancies') {
            v1_admin_upsert_shadow_discrepancy($pdo, $config, (string)$preauthorizedRole);
        }
        if ($path === '/admin/shadow-runs') {
            v1_admin_upsert_shadow_run($pdo, $config, (string)$preauthorizedRole);
        }
        if ($path === '/admin/release-evidence-inputs') {
            v1_admin_upsert_release_evidence_inputs($pdo, $config, (string)$preauthorizedRole);
        }
        if ($path === '/admin/source-rights') {
            $role = v1_require_role($config, array('rights'));
            v1_admin_upsert_source_right($pdo, $config, $role);
        }
        if ($path === '/admin/editorial-revisions') {
            $role = v1_require_role($config, array('editor'));
            v1_admin_create_revision($pdo, $config, $role);
        }
        if (preg_match('#^/admin/events/([A-Za-z0-9_.:\-]{1,96})/review$#', $path, $m)) {
            $role = v1_require_role($config, array('editor'));
            v1_admin_review_event($pdo, $config, $m[1], $role);
        }
        if (preg_match('#^/admin/events/([A-Za-z0-9_.:\-]{1,96})/identity$#', $path, $m)) {
            $role = v1_require_role($config, array('editor'));
            v1_admin_complete_event_identity($pdo, $config, $m[1], $role);
        }
        if (preg_match('#^/admin/event-actors/([A-Za-z0-9_.:\-]{1,96})/([A-Za-z0-9_.:\-]{1,64})/([A-Za-z0-9_.:\-]{1,40})/review$#', $path, $m)) {
            $role = v1_require_role($config, array('editor'));
            v1_admin_review_event_actor($pdo, $config, $m[1], $m[2], $m[3], $role);
        }
        $editorialReviewRoutes = array(
            'actors' => 'actor',
            'campaigns' => 'campaign',
            'claims' => 'claim',
            'proposal-votes' => 'proposal_vote',
            'commitments' => 'commitment',
            'timeline-entries' => 'timeline',
        );
        foreach ($editorialReviewRoutes as $routeName => $entityType) {
            if (preg_match('#^/admin/' . preg_quote($routeName, '#') . '/([A-Za-z0-9_.:\-]{1,96})/review$#', $path, $m)) {
                $role = v1_require_role($config, array('editor'));
                v1_admin_review_editorial_entity($pdo, $config, $entityType, $m[1], $role);
            }
        }
        if (preg_match('#^/admin/feedback/([A-Za-z0-9_.:\-]{1,64})/review$#', $path, $m)) {
            $role = v1_require_role($config, array('editor'));
            v1_admin_review_feedback($pdo, $config, $m[1], $role);
        }
    }
    if ($method === 'PUT' && preg_match('#^/ops/backfill-checkpoints/([a-f0-9]{64})$#', $path, $m)) {
        v1_ops_put_backfill_checkpoint($pdo, $config, $m[1], (string)$preauthorizedRole);
    }
    v1_respond(404, array('ok' => false, 'error' => 'not_found'));
}

function v1_serve_openapi(string $path): void {
    $file = __DIR__ . '/openapi.yaml';
    if (!is_file($file)) {
        v1_respond(404, array('ok' => false, 'error' => 'openapi_not_deployed'));
    }
    if ($path === '/openapi.json') {
        v1_respond(406, array('ok' => false, 'error' => 'json_spec_not_available', 'yaml' => '/api/v1/openapi.yaml'));
    }
    header('Content-Type: application/yaml; charset=utf-8');
    header('Cache-Control: public, max-age=300');
    readfile($file);
    exit;
}

/**
 * A document may use only the grant registered for its exact source identity.
 * BINARY comparisons keep this fail-closed under the database's
 * case-insensitive default collation.
 */
function v1_document_source_right_identity_sql(
    string $documentAlias = 'd',
    string $rightsAlias = 'sr'
): string {
    return '(' . $documentAlias . '.source_right_id IS NOT NULL'
        . ' AND ' . $rightsAlias . '.source_right_id='
        . $documentAlias . '.source_right_id'
        . ' AND BINARY ' . $documentAlias . '.source_class=BINARY '
        . $rightsAlias . '.source_type'
        . ' AND BINARY ' . $documentAlias . '.source_key=BINARY '
        . $rightsAlias . '.source_key)';
}

function v1_document_visibility_sql(
    string $documentAlias = 'd',
    string $rightsAlias = 'sr',
    bool $requireLegacyCompany = true
): string {
    return '(' . $documentAlias . '.publication_status = \'published\''
        . ($requireLegacyCompany
            ? ' AND ' . $documentAlias . '.company_id IS NOT NULL'
            : ' AND ' . $documentAlias . '.issuer_id IS NOT NULL')
        . ' AND ('
        . $documentAlias . '.source_right_id IS NOT NULL'
        . ' AND ' . $rightsAlias . '.source_right_id IS NOT NULL'
        . ' AND ' . $rightsAlias . '.status = \'active\''
        . ' AND NULLIF(TRIM(' . $rightsAlias . '.permission_scope), \'\') IS NOT NULL'
        . ' AND ' . $rightsAlias . '.redistribution_allowed = 1'
        . ' AND ' . $rightsAlias . '.valid_from <= UTC_TIMESTAMP()'
        . ' AND (' . $rightsAlias . '.valid_until IS NULL OR ' . $rightsAlias . '.valid_until > UTC_TIMESTAMP())'
        . ' AND ' . $rightsAlias . '.revoked_at IS NULL'
        . ' AND (NULLIF(TRIM(' . $rightsAlias . '.evidence_uri), \'\') IS NOT NULL'
        . ' OR ' . $rightsAlias . '.evidence_hash'
        . ' REGEXP \'^[A-Fa-f0-9]{64}$\')'
        . ' AND ' . v1_document_source_right_identity_sql(
            $documentAlias,
            $rightsAlias
        )
        . '))';
}

/** Published events require at least one currently publishable evidence document. */
function v1_event_visibility_sql(array $config, string $eventAlias = 'e'): string {
    $links = table_name($config, 'event_documents');
    $documents = table_name($config, 'documents');
    $rights = table_name($config, 'source_rights');
    $eventActors = table_name($config, 'event_actors');
    $actors = table_name($config, 'actors');
    return '(' . $eventAlias . '.publication_status = \'published\''
        . ' AND ' . $eventAlias . '.identity_status = \'complete\''
        . ' AND ' . $eventAlias . '.review_status IN (\'approved\',\'not_required\')'
        . ' AND (' . $eventAlias . '.importance NOT IN (\'high\',\'critical\',\'market_sensitive\') OR ' . $eventAlias . '.review_status = \'approved\')'
        . ' AND (' . $eventAlias . '.verification_status <> \'withdrawn\' OR ' . $eventAlias . '.review_status = \'approved\')'
        . ' AND (NULLIF(TRIM(' . $eventAlias . '.identity_actor_id), \'\') IS NULL OR EXISTS ('
        . 'SELECT 1 FROM ' . $eventActors . ' visibility_identity_ea'
        . ' JOIN ' . $actors . ' visibility_identity_a ON visibility_identity_a.actor_id = visibility_identity_ea.actor_id'
        . ' WHERE visibility_identity_ea.event_id = ' . $eventAlias . '.event_id'
        . ' AND visibility_identity_ea.actor_id = ' . $eventAlias . '.identity_actor_id'
        . ' AND visibility_identity_ea.review_status = \'approved\''
        . ' AND visibility_identity_a.review_status = \'approved\''
        . ' AND visibility_identity_a.record_status = \'active\''
        . ' AND NULLIF(TRIM(visibility_identity_a.display_name), \'\') IS NOT NULL))'
        . ' AND EXISTS (SELECT 1 FROM ' . $links . ' visibility_ed'
        . ' JOIN ' . $documents . ' visibility_d ON visibility_d.document_id = visibility_ed.document_id'
        . ' LEFT JOIN ' . $rights . ' visibility_sr ON visibility_sr.source_right_id = visibility_d.source_right_id'
        . ' WHERE visibility_ed.event_id = ' . $eventAlias . '.event_id AND '
        . v1_document_visibility_sql('visibility_d', 'visibility_sr') . '))';
}

/** A campaign is public only while at least one reviewed evidence document remains publishable. */
function v1_campaign_visibility_sql(array $config, string $campaignAlias = 'cp'): string {
    return '(' . $campaignAlias . '.publication_status = \'published\''
        . ' AND ' . $campaignAlias . '.review_status = \'approved\''
        . ' AND EXISTS (SELECT 1 FROM ' . table_name($config, 'actors') . ' visibility_campaign_actor'
        . ' WHERE visibility_campaign_actor.actor_id = ' . $campaignAlias . '.lead_actor_id'
        . ' AND visibility_campaign_actor.review_status = \'approved\' AND visibility_campaign_actor.record_status = \'active\')'
        . ' AND EXISTS (SELECT 1 FROM ' . table_name($config, 'campaign_documents') . ' visibility_cd'
        . ' JOIN ' . table_name($config, 'documents') . ' visibility_campaign_d ON visibility_campaign_d.document_id = visibility_cd.document_id'
        . ' LEFT JOIN ' . table_name($config, 'source_rights') . ' visibility_campaign_sr ON visibility_campaign_sr.source_right_id = visibility_campaign_d.source_right_id'
        . ' WHERE visibility_cd.campaign_id = ' . $campaignAlias . '.campaign_id AND '
        . v1_document_visibility_sql('visibility_campaign_d', 'visibility_campaign_sr') . '))';
}

function v1_optional_document_visibility_sql(array $config, string $documentIdExpression): string {
    return '(' . $documentIdExpression . ' IS NULL OR EXISTS (SELECT 1 FROM ' . table_name($config, 'documents') . ' optional_d'
        . ' LEFT JOIN ' . table_name($config, 'source_rights') . ' optional_sr ON optional_sr.source_right_id = optional_d.source_right_id'
        . ' WHERE optional_d.document_id = ' . $documentIdExpression . ' AND '
        . v1_document_visibility_sql('optional_d', 'optional_sr') . '))';
}

function v1_required_document_visibility_sql(array $config, string $documentIdExpression): string {
    return '(' . $documentIdExpression . ' IS NOT NULL AND EXISTS (SELECT 1 FROM ' . table_name($config, 'documents') . ' required_d'
        . ' LEFT JOIN ' . table_name($config, 'source_rights') . ' required_sr ON required_sr.source_right_id = required_d.source_right_id'
        . ' WHERE required_d.document_id = ' . $documentIdExpression . ' AND '
        . v1_document_visibility_sql('required_d', 'required_sr') . '))';
}

function v1_actor_visibility_sql(array $config, string $actorAlias = 'a'): string {
    return '(' . $actorAlias . '.review_status = \'approved\' AND ' . $actorAlias . '.record_status = \'active\' AND ('
        . 'EXISTS (SELECT 1 FROM ' . table_name($config, 'event_actors') . ' visible_ea'
        . ' JOIN ' . table_name($config, 'governance_events') . ' visible_actor_event ON visible_actor_event.event_id = visible_ea.event_id'
        . ' WHERE visible_ea.actor_id = ' . $actorAlias . '.actor_id AND visible_ea.review_status = \'approved\' AND '
        . v1_event_visibility_sql($config, 'visible_actor_event') . ')'
        . ' OR EXISTS (SELECT 1 FROM ' . table_name($config, 'campaigns') . ' visible_actor_campaign'
        . ' WHERE visible_actor_campaign.lead_actor_id = ' . $actorAlias . '.actor_id AND '
        . v1_campaign_visibility_sql($config, 'visible_actor_campaign') . ')))';
}

function v1_company_has_public_event_sql(array $config, string $companyAlias = 'c'): string {
    return 'EXISTS (SELECT 1 FROM ' . table_name($config, 'governance_events') . ' visible_company_event'
        . ' WHERE visible_company_event.company_id = ' . $companyAlias . '.company_id AND '
        . v1_event_visibility_sql($config, 'visible_company_event') . ')';
}

function v1_list_companies(PDO $pdo, array $config): void {
    $page = v1_list_params();
    $query = isset($_GET['q']) ? trim((string)$_GET['q']) : '';
    $market = isset($_GET['market']) ? trim((string)$_GET['market']) : '';
    if ($query !== '' && mb_strlen($query, 'UTF-8') < 2) {
        v1_respond(400, array('ok' => false, 'error' => 'query_too_short'));
    }
    $where = array('c.record_status = \'active\'', v1_company_has_public_event_sql($config, 'c'));
    $params = array();
    if ($query !== '') {
        $like = '%' . mb_substr($query, 0, 100, 'UTF-8') . '%';
        $where[] = '(c.legal_name LIKE ? OR c.legal_name_en LIKE ? OR c.short_name LIKE ? OR c.stock_code = ?)';
        array_push($params, $like, $like, $like, $query);
    }
    if ($market !== '') {
        if (!preg_match('/^[A-Za-z0-9_.\-]{1,40}$/', $market)) {
            v1_respond(400, array('ok' => false, 'error' => 'invalid_market'));
        }
        $where[] = 'c.market = ?';
        $params[] = $market;
    }
    $sql = 'SELECT c.company_id, c.stock_code, c.market, c.legal_name, c.legal_name_en, c.short_name, c.aliases_json, c.homepage_url, c.listing_status, c.master_modified_at, '
        . '(SELECT COUNT(*) FROM ' . table_name($config, 'governance_events') . ' e WHERE e.company_id = c.company_id AND ' . v1_event_visibility_sql($config, 'e') . ') AS event_count, '
        . '(SELECT COUNT(*) FROM ' . table_name($config, 'campaigns') . ' cp WHERE cp.company_id = c.company_id AND ' . v1_campaign_visibility_sql($config, 'cp') . ' AND cp.ended_at IS NULL) AS active_campaign_count '
        . 'FROM ' . table_name($config, 'companies') . ' c WHERE ' . implode(' AND ', $where)
        . ' ORDER BY c.legal_name ASC, c.company_id ASC LIMIT ' . ((int)$page['limit'] + 1) . ' OFFSET ' . (int)$page['offset'];
    $stmt = $pdo->prepare($sql);
    $stmt->execute($params);
    list($rows, $hasMore) = v1_fetch_page($stmt, $page);
    foreach ($rows as &$row) {
        $row['aliases'] = decode_json_array(isset($row['aliases_json']) ? $row['aliases_json'] : null);
        unset($row['aliases_json']);
        $row['event_count'] = (int)$row['event_count'];
        $row['active_campaign_count'] = (int)$row['active_campaign_count'];
    }
    unset($row);
    v1_respond(200, array('ok' => true, 'data' => $rows, 'pagination' => v1_page_meta($page, count($rows), $hasMore)));
}

function v1_get_company(PDO $pdo, array $config, string $companyId): void {
    $stmt = $pdo->prepare('SELECT c.company_id, c.stock_code, c.market, c.legal_name, c.legal_name_en, c.short_name, c.aliases_json, c.homepage_url, '
        . 'c.listing_status, c.master_modified_at, c.updated_at FROM ' . table_name($config, 'companies') . ' c '
        . 'WHERE c.company_id = ? AND c.record_status = \'active\' AND ' . v1_company_has_public_event_sql($config, 'c') . ' LIMIT 1');
    $stmt->execute(array($companyId));
    $company = $stmt->fetch();
    if (!$company) {
        v1_respond(404, array('ok' => false, 'error' => 'company_not_found'));
    }
    $company['aliases'] = decode_json_array(isset($company['aliases_json']) ? $company['aliases_json'] : null);
    unset($company['aliases_json']);
    $eventStmt = $pdo->prepare('SELECT event_id, event_type, title, original_language, occurred_at, deadline_at, importance, verification_status '
        . 'FROM ' . table_name($config, 'governance_events') . ' e WHERE e.company_id = ? AND ' . v1_event_visibility_sql($config, 'e')
        . ' ORDER BY e.occurred_at DESC LIMIT 25');
    $eventStmt->execute(array($companyId));
    $campaignStmt = $pdo->prepare('SELECT campaign_id, lead_actor_id, title, original_language, stage, outcome, started_at, ended_at '
        . 'FROM ' . table_name($config, 'campaigns') . ' cp WHERE cp.company_id = ? AND ' . v1_campaign_visibility_sql($config, 'cp') . ' '
        . 'ORDER BY (ended_at IS NULL) DESC, started_at DESC LIMIT 20');
    $campaignStmt->execute(array($companyId));
    $commitmentStmt = $pdo->prepare('SELECT co.commitment_id, co.event_id, co.campaign_id, LEFT(co.commitment_text,2000) AS commitment_text, co.original_language, co.target_at, LEFT(co.actual_action,2000) AS actual_action, co.status '
        . 'FROM ' . table_name($config, 'commitment_outcomes') . ' co WHERE co.company_id = ? AND co.review_status = \'approved\' AND co.publication_status = \'published\' AND '
        . v1_required_document_visibility_sql($config, 'co.evidence_document_id')
        . ' ORDER BY COALESCE(co.target_at, co.updated_at) DESC LIMIT 20');
    $commitmentStmt->execute(array($companyId));
    v1_respond(200, array(
        'ok' => true,
        'data' => array(
            'company' => $company,
            'events' => $eventStmt->fetchAll(),
            'campaigns' => $campaignStmt->fetchAll(),
            'commitments' => $commitmentStmt->fetchAll(),
        ),
    ));
}

function v1_list_actors(PDO $pdo, array $config): void {
    $page = v1_list_params();
    $query = isset($_GET['q']) ? trim((string)$_GET['q']) : '';
    $actorType = isset($_GET['actor_type']) ? trim((string)$_GET['actor_type']) : '';
    $companyId = isset($_GET['company_id']) ? trim((string)$_GET['company_id']) : '';
    $where = array(v1_actor_visibility_sql($config, 'a'));
    $params = array();
    if ($query !== '') {
        if (mb_strlen($query, 'UTF-8') < 2) { v1_respond(400, array('ok' => false, 'error' => 'query_too_short')); }
        $like = v1_like(mb_substr($query, 0, 100, 'UTF-8'));
        $where[] = '(a.display_name LIKE ? OR a.display_name_en LIKE ? OR a.aliases_json LIKE ?)';
        array_push($params, $like, $like, $like);
    }
    if ($actorType !== '') {
        if (!preg_match('/^[A-Za-z0-9_.:\-]{1,40}$/', $actorType)) { v1_respond(400, array('ok' => false, 'error' => 'invalid_actor_type')); }
        $where[] = 'a.actor_type = ?'; $params[] = $actorType;
    }
    if ($companyId !== '') {
        if (!preg_match('/^[0-9]{8}$/', $companyId)) { v1_respond(400, array('ok' => false, 'error' => 'invalid_company_id')); }
        $where[] = 'a.company_id = ?'; $params[] = $companyId;
    }
    $sql = 'SELECT a.actor_id, a.actor_type, a.display_name, a.display_name_en, a.company_id, a.country_code, '
        . 'a.aliases_json, a.homepage_url, a.updated_at FROM ' . table_name($config, 'actors') . ' a WHERE '
        . implode(' AND ', $where) . ' ORDER BY a.display_name ASC, a.actor_id ASC LIMIT '
        . ((int)$page['limit'] + 1) . ' OFFSET ' . (int)$page['offset'];
    $stmt = $pdo->prepare($sql); $stmt->execute($params);
    list($rows, $hasMore) = v1_fetch_page($stmt, $page);
    foreach ($rows as &$row) {
        $row['aliases'] = decode_json_array(isset($row['aliases_json']) ? $row['aliases_json'] : null);
        unset($row['aliases_json']);
    }
    unset($row);
    v1_respond(200, array('ok' => true, 'data' => $rows, 'pagination' => v1_page_meta($page, count($rows), $hasMore)));
}

function v1_get_actor(PDO $pdo, array $config, string $actorId): void {
    $stmt = $pdo->prepare('SELECT a.actor_id, a.actor_type, a.display_name, a.display_name_en, a.company_id, a.country_code, '
        . 'a.aliases_json, a.homepage_url, a.updated_at FROM ' . table_name($config, 'actors') . ' a WHERE a.actor_id = ? AND '
        . v1_actor_visibility_sql($config, 'a') . ' LIMIT 1');
    $stmt->execute(array($actorId));
    $actor = $stmt->fetch();
    if (!$actor) { v1_respond(404, array('ok' => false, 'error' => 'actor_not_found')); }
    $actor['aliases'] = decode_json_array(isset($actor['aliases_json']) ? $actor['aliases_json'] : null);
    unset($actor['aliases_json']);
    $events = $pdo->prepare(v1_public_event_select($config)
        . 'JOIN ' . table_name($config, 'event_actors') . ' actor_detail_ea ON actor_detail_ea.event_id = e.event_id '
        . 'WHERE actor_detail_ea.actor_id = ? AND actor_detail_ea.review_status = \'approved\' AND '
        . v1_event_visibility_sql($config, 'e') . ' ORDER BY e.occurred_at DESC LIMIT 50');
    $events->execute(array($actorId));
    $campaigns = $pdo->prepare('SELECT cp.campaign_id, cp.company_id, c.legal_name AS company_name, cp.title, cp.original_language, '
        . 'cp.stage, cp.outcome, cp.started_at, cp.ended_at FROM ' . table_name($config, 'campaigns') . ' cp '
        . 'JOIN ' . table_name($config, 'companies') . ' c ON c.company_id = cp.company_id '
        . 'WHERE cp.lead_actor_id = ? AND ' . v1_campaign_visibility_sql($config, 'cp') . ' ORDER BY cp.started_at DESC LIMIT 50');
    $campaigns->execute(array($actorId));
    v1_respond(200, array('ok' => true, 'data' => array(
        'actor' => $actor,
        'events' => $events->fetchAll(),
        'campaigns' => $campaigns->fetchAll(),
    )));
}

function v1_event_query_parts(array $config, bool $includeDateFilters = true): array {
    $where = array(v1_event_visibility_sql($config, 'e'));
    $params = array();
    $companyId = isset($_GET['company_id']) ? trim((string)$_GET['company_id']) : '';
    $actorId = isset($_GET['actor_id']) ? trim((string)$_GET['actor_id']) : '';
    $eventType = isset($_GET['event_type']) ? trim((string)$_GET['event_type']) : '';
    $verification = isset($_GET['verification_status']) ? trim((string)$_GET['verification_status']) : '';
    $status = isset($_GET['status']) ? trim((string)$_GET['status']) : '';
    $identityStatus = isset($_GET['identity_status']) ? trim((string)$_GET['identity_status']) : '';
    $importance = isset($_GET['importance']) ? trim((string)$_GET['importance']) : '';
    $sourceClass = isset($_GET['source_class']) ? trim((string)$_GET['source_class']) : '';
    $evidenceDocumentId = isset($_GET['evidence_document_id']) ? trim((string)$_GET['evidence_document_id']) : '';
    $from = isset($_GET['from']) ? trim((string)$_GET['from']) : '';
    $to = isset($_GET['to']) ? trim((string)$_GET['to']) : '';
    if ($companyId !== '') {
        if (!preg_match('/^[0-9]{8}$/', $companyId)) { v1_respond(400, array('ok' => false, 'error' => 'invalid_company_id')); }
        $where[] = 'e.company_id = ?'; $params[] = $companyId;
    }
    if ($actorId !== '') {
        if (!v1_valid_entity_id($actorId, 64)) { v1_respond(400, array('ok' => false, 'error' => 'invalid_actor_id')); }
        $where[] = '(e.identity_actor_id = ? OR EXISTS (SELECT 1 FROM ' . table_name($config, 'event_actors')
            . ' filter_ea WHERE filter_ea.event_id = e.event_id AND filter_ea.actor_id = ? AND filter_ea.review_status = \'approved\'))';
        $params[] = $actorId; $params[] = $actorId;
    }
    if ($status !== '') {
        if ($verification !== '' && $verification !== $status) {
            v1_respond(400, array('ok' => false, 'error' => 'conflicting_status_filters'));
        }
        $verification = $status;
    }
    foreach (array('event_type' => $eventType, 'verification_status' => $verification, 'identity_status' => $identityStatus, 'importance' => $importance) as $field => $value) {
        if ($value === '') { continue; }
        if (!preg_match('/^[A-Za-z0-9_.:\-]{1,64}$/', $value)) { v1_respond(400, array('ok' => false, 'error' => 'invalid_' . $field)); }
        $where[] = 'e.' . $field . ' = ?'; $params[] = $value;
    }
    if ($sourceClass !== '') {
        if (!preg_match('/^[A-Za-z0-9_.:\-]{1,40}$/', $sourceClass)) { v1_respond(400, array('ok' => false, 'error' => 'invalid_source_class')); }
        $where[] = 'EXISTS (SELECT 1 FROM ' . table_name($config, 'event_documents') . ' filter_ed '
            . 'JOIN ' . table_name($config, 'documents') . ' filter_d ON filter_d.document_id = filter_ed.document_id '
            . 'LEFT JOIN ' . table_name($config, 'source_rights') . ' filter_sr ON filter_sr.source_right_id = filter_d.source_right_id '
            . 'WHERE filter_ed.event_id = e.event_id AND filter_d.source_class = ? AND '
            . v1_document_visibility_sql('filter_d', 'filter_sr') . ')';
        $params[] = $sourceClass;
    }
    if ($evidenceDocumentId !== '') {
        if (!v1_valid_entity_id($evidenceDocumentId)) { v1_respond(400, array('ok' => false, 'error' => 'invalid_evidence_document_id')); }
        $where[] = 'EXISTS (SELECT 1 FROM ' . table_name($config, 'event_documents') . ' evidence_ed '
            . 'JOIN ' . table_name($config, 'documents') . ' evidence_d ON evidence_d.document_id = evidence_ed.document_id '
            . 'LEFT JOIN ' . table_name($config, 'source_rights') . ' evidence_sr ON evidence_sr.source_right_id = evidence_d.source_right_id '
            . 'WHERE evidence_ed.event_id = e.event_id AND evidence_ed.document_id = ? AND '
            . v1_document_visibility_sql('evidence_d', 'evidence_sr') . ')';
        $params[] = $evidenceDocumentId;
    }
    if ($includeDateFilters && $from !== '') {
        $dt = mysql_dt($from);
        if ($dt === null) { v1_respond(400, array('ok' => false, 'error' => 'invalid_from')); }
        $where[] = 'e.occurred_at >= ?'; $params[] = $dt;
    }
    if ($includeDateFilters && $to !== '') {
        $dt = mysql_dt($to);
        if ($dt === null) { v1_respond(400, array('ok' => false, 'error' => 'invalid_to')); }
        $where[] = 'e.occurred_at <= ?'; $params[] = $dt;
    }
    return array($where, $params);
}

function v1_event_filter_requested(bool $includeDates = true): bool {
    $keys = array('company_id','actor_id','event_type','verification_status','status','identity_status','importance','source_class','evidence_document_id');
    if ($includeDates) { $keys = array_merge($keys, array('from','to')); }
    foreach ($keys as $key) {
        if (isset($_GET[$key]) && trim((string)$_GET[$key]) !== '') { return true; }
    }
    return false;
}

function v1_public_event_select(array $config): string {
    return 'SELECT e.event_id, e.company_id, c.stock_code, c.market, c.legal_name AS company_name, e.event_type, '
        . 'e.title, e.original_language, e.occurred_at, e.deadline_at, e.importance, e.verification_status, '
        . 'e.identity_action, e.identity_target, e.identity_actor_id, e.identity_effective_at, e.identity_deadline_at, '
        . 'e.identity_status, e.comparison_key, e.updated_at '
        . 'FROM ' . table_name($config, 'governance_events') . ' e '
        . 'JOIN ' . table_name($config, 'companies') . ' c ON c.company_id = e.company_id ';
}

function v1_query_public_events(PDO $pdo, array $config, array $page): array {
    list($where, $params) = v1_event_query_parts($config);
    $sql = v1_public_event_select($config) . 'WHERE ' . implode(' AND ', $where)
        . ' ORDER BY e.occurred_at DESC, e.event_id DESC LIMIT ' . ((int)$page['limit'] + 1)
        . ' OFFSET ' . (int)$page['offset'];
    $stmt = $pdo->prepare($sql);
    $stmt->execute($params);
    return v1_fetch_page($stmt, $page);
}

function v1_list_events(PDO $pdo, array $config): void {
    $page = v1_list_params();
    list($rows, $hasMore) = v1_query_public_events($pdo, $config, $page);
    v1_respond(200, array('ok' => true, 'data' => $rows, 'pagination' => v1_page_meta($page, count($rows), $hasMore)));
}

/**
 * Rank the complete public event set for the Today page.  This must remain a
 * server-side query: ranking a recent client page silently drops older high
 * importance events and near-term deadlines.
 */
function v1_today_ranked_select(array $config): string {
    $officialEvidence = 'EXISTS (SELECT 1 FROM ' . table_name($config, 'event_documents') . ' today_ed '
        . 'JOIN ' . table_name($config, 'documents') . ' today_d ON today_d.document_id=today_ed.document_id '
        . 'LEFT JOIN ' . table_name($config, 'source_rights') . ' today_sr ON today_sr.source_right_id=today_d.source_right_id '
        . 'WHERE today_ed.event_id=e.event_id AND today_d.source_class=\'official_disclosure\' AND '
        . v1_document_visibility_sql('today_d', 'today_sr') . ')';
    $deadlineWatch = '(e.deadline_at IS NOT NULL AND e.deadline_at BETWEEN DATE_SUB(UTC_TIMESTAMP(), INTERVAL 2 DAY) '
        . 'AND DATE_ADD(UTC_TIMESTAMP(), INTERVAL 45 DAY))';
    $importanceScore = '(CASE e.importance WHEN \'critical\' THEN 500 WHEN \'market_sensitive\' THEN 450 '
        . 'WHEN \'high\' THEN 400 WHEN \'medium\' THEN 200 WHEN \'low\' THEN 100 ELSE 0 END)';
    $verificationScore = '(CASE e.verification_status WHEN \'official\' THEN 60 WHEN \'confirmed\' THEN 55 '
        . 'WHEN \'corroborated\' THEN 45 WHEN \'corrected\' THEN 30 WHEN \'disputed\' THEN 20 '
        . 'WHEN \'withdrawn\' THEN 15 ELSE 0 END)';
    $deadlineScore = '(CASE WHEN e.deadline_at BETWEEN UTC_TIMESTAMP() AND DATE_ADD(UTC_TIMESTAMP(), INTERVAL 7 DAY) THEN 120 '
        . 'WHEN e.deadline_at BETWEEN DATE_SUB(UTC_TIMESTAMP(), INTERVAL 2 DAY) AND UTC_TIMESTAMP() THEN 100 '
        . 'WHEN e.deadline_at BETWEEN UTC_TIMESTAMP() AND DATE_ADD(UTC_TIMESTAMP(), INTERVAL 45 DAY) THEN 70 ELSE 0 END)';
    return 'SELECT e.event_id,e.company_id,c.stock_code,c.market,c.legal_name AS company_name,e.event_type,e.title,e.original_language,'
        . 'e.occurred_at,e.deadline_at,e.importance,e.verification_status,e.identity_action,e.identity_target,e.identity_actor_id,'
        . 'e.identity_effective_at,e.identity_deadline_at,e.identity_status,e.comparison_key,e.updated_at,'
        . $officialEvidence . ' AS has_official_evidence,' . $deadlineWatch . ' AS deadline_watch,'
        . '(' . $importanceScore . '+' . $verificationScore . '+(CASE WHEN ' . $officialEvidence . ' THEN 80 ELSE 0 END)+'
        . $deadlineScore . ') AS ranking_score '
        . 'FROM ' . table_name($config, 'governance_events') . ' e '
        . 'JOIN ' . table_name($config, 'companies') . ' c ON c.company_id=e.company_id '
        . 'WHERE ' . v1_event_visibility_sql($config, 'e') . ' AND e.verification_status <> \'signal\'';
}

function v1_today(PDO $pdo, array $config): void {
    $ranked = v1_today_ranked_select($config);
    $topStmt = $pdo->prepare('SELECT * FROM (' . $ranked . ') today_ranked '
        . 'ORDER BY ranking_score DESC, occurred_at DESC, event_id ASC LIMIT 5');
    $topStmt->execute(); $top = $topStmt->fetchAll();
    $topIds = array(); foreach ($top as $row) { $topIds[] = (string)$row['event_id']; }

    $watchWhere = '(verification_status IN (\'unverified\',\'disputed\',\'corrected\') OR deadline_watch=1)';
    $params = array();
    if (count($topIds) > 0) {
        $watchWhere .= ' AND event_id NOT IN (' . implode(',', array_fill(0, count($topIds), '?')) . ')';
        $params = $topIds;
    }
    $watchStmt = $pdo->prepare('SELECT * FROM (' . $ranked . ') today_watch WHERE ' . $watchWhere
        . ' ORDER BY CASE WHEN deadline_watch=1 THEN 0 ELSE 1 END ASC, '
        . 'CASE WHEN deadline_at >= UTC_TIMESTAMP() THEN deadline_at ELSE DATE_ADD(UTC_TIMESTAMP(), INTERVAL 100 YEAR) END ASC, '
        . 'ranking_score DESC, occurred_at DESC, event_id ASC LIMIT 10');
    $watchStmt->execute($params); $watch = $watchStmt->fetchAll();
    foreach ($top as &$row) {
        $row['has_official_evidence'] = (int)$row['has_official_evidence'] === 1;
        $row['deadline_watch'] = (int)$row['deadline_watch'] === 1;
        $row['ranking_score'] = (int)$row['ranking_score'];
    }
    unset($row);
    foreach ($watch as &$row) {
        $row['has_official_evidence'] = (int)$row['has_official_evidence'] === 1;
        $row['deadline_watch'] = (int)$row['deadline_watch'] === 1;
        $row['ranking_score'] = (int)$row['ranking_score'];
    }
    unset($row);
    v1_respond(200, array(
        'ok' => true,
        'generated_at' => gmdate('c'),
        'ranking_policy' => array(
            'version' => 'today-v1',
            'signal_excluded' => true,
            'top_limit' => 5,
            'watch_limit' => 10,
            'watch_deadline_window_days' => array('past' => 2, 'future' => 45),
            'archive_endpoint' => '/events',
        ),
        'top' => $top,
        'watch' => $watch,
    ));
}

function v1_get_event(PDO $pdo, array $config, string $eventId): void {
    $stmt = $pdo->prepare(v1_public_event_select($config) . 'WHERE e.event_id = ? AND ' . v1_event_visibility_sql($config, 'e') . ' LIMIT 1');
    $stmt->execute(array($eventId));
    $event = $stmt->fetch();
    if (!$event) { v1_respond(404, array('ok' => false, 'error' => 'event_not_found')); }
    $claimSql = 'SELECT ce.claim_id, ce.campaign_id, CASE WHEN a.actor_id IS NULL THEN NULL ELSE ce.actor_id END AS actor_id, a.display_name AS actor_name, ce.claim_type, LEFT(ce.claim_text,1500) AS claim_text, '
        . 'ce.original_language, ce.evidence_locator, ce.document_id, d.title AS document_title, d.original_url '
        . 'FROM ' . table_name($config, 'claim_evidence') . ' ce '
        . 'LEFT JOIN ' . table_name($config, 'actors') . ' a ON a.actor_id = ce.actor_id AND a.review_status = \'approved\' AND a.record_status = \'active\' '
        . 'JOIN ' . table_name($config, 'documents') . ' d ON d.document_id = ce.document_id '
        . 'LEFT JOIN ' . table_name($config, 'source_rights') . ' sr ON sr.source_right_id = d.source_right_id '
        . 'WHERE ce.event_id = ? AND ce.editorial_status = \'approved\' AND ' . v1_document_visibility_sql('d', 'sr')
        . ' ORDER BY ce.created_at ASC LIMIT 40';
    $claimStmt = $pdo->prepare($claimSql); $claimStmt->execute(array($eventId));
    $documentSql = 'SELECT d.document_id, ed.relation_type, d.source_class, d.document_type, d.original_language, d.title, '
        . 'd.original_url, d.published_at, d.verification_status, d.version_no '
        . 'FROM ' . table_name($config, 'event_documents') . ' ed '
        . 'JOIN ' . table_name($config, 'documents') . ' d ON d.document_id = ed.document_id '
        . 'LEFT JOIN ' . table_name($config, 'source_rights') . ' sr ON sr.source_right_id = d.source_right_id '
        . 'WHERE ed.event_id = ? AND ' . v1_document_visibility_sql('d', 'sr')
        . ' ORDER BY ed.position_no ASC, d.published_at ASC LIMIT 50';
    $documentStmt = $pdo->prepare($documentSql); $documentStmt->execute(array($eventId));
    $actorStmt = $pdo->prepare('SELECT ea.actor_role, a.actor_id, a.actor_type, a.display_name, a.display_name_en '
        . 'FROM ' . table_name($config, 'event_actors') . ' ea JOIN ' . table_name($config, 'actors') . ' a ON a.actor_id = ea.actor_id '
        . 'WHERE ea.event_id = ? AND ea.review_status = \'approved\' AND a.review_status = \'approved\' AND a.record_status = \'active\' ORDER BY ea.actor_role, a.display_name LIMIT 100');
    $actorStmt->execute(array($eventId));
    $timelineStmt = $pdo->prepare('SELECT tl.timeline_entry_id, tl.campaign_id, tl.document_id, tl.occurred_at, tl.entry_type, tl.title, LEFT(tl.description,1500) AS description, tl.original_language '
        . 'FROM ' . table_name($config, 'timeline_entries') . ' tl WHERE tl.event_id = ? AND tl.review_status = \'approved\' AND tl.publication_status = \'published\' AND '
        . v1_required_document_visibility_sql($config, 'tl.document_id')
        . ' ORDER BY tl.occurred_at ASC LIMIT 40');
    $timelineStmt->execute(array($eventId));
    $revisionStmt = $pdo->prepare('SELECT revision_id, field_name, reason, published_at FROM ' . table_name($config, 'editorial_revisions')
        . ' WHERE entity_type = \'event\' AND entity_id = ? AND revision_status = \'published\' ORDER BY published_at DESC LIMIT 25');
    $revisionStmt->execute(array($eventId));
    v1_respond(200, array('ok' => true, 'data' => array(
        'event' => $event,
        'actors' => $actorStmt->fetchAll(),
        'claims' => $claimStmt->fetchAll(),
        'documents' => $documentStmt->fetchAll(),
        'timeline' => $timelineStmt->fetchAll(),
        'revisions' => $revisionStmt->fetchAll(),
    )));
}

function v1_get_campaign(PDO $pdo, array $config, string $campaignId): void {
    $stmt = $pdo->prepare('SELECT cp.campaign_id, cp.company_id, c.legal_name AS company_name, cp.lead_actor_id, a.display_name AS lead_actor_name, '
        . 'cp.title, cp.original_language, LEFT(cp.demand_text,10000) AS demand_text, cp.stage, cp.outcome, cp.started_at, cp.ended_at, cp.updated_at '
        . 'FROM ' . table_name($config, 'campaigns') . ' cp JOIN ' . table_name($config, 'companies') . ' c ON c.company_id = cp.company_id '
        . 'JOIN ' . table_name($config, 'actors') . ' a ON a.actor_id = cp.lead_actor_id AND a.review_status = \'approved\' AND a.record_status = \'active\' '
        . 'WHERE cp.campaign_id = ? AND ' . v1_campaign_visibility_sql($config, 'cp') . ' LIMIT 1');
    $stmt->execute(array($campaignId));
    $campaign = $stmt->fetch();
    if (!$campaign) { v1_respond(404, array('ok' => false, 'error' => 'campaign_not_found')); }
    $timelineStmt = $pdo->prepare('SELECT tl.timeline_entry_id, tl.event_id, tl.document_id, tl.occurred_at, tl.entry_type, tl.title, LEFT(tl.description,1500) AS description, tl.original_language '
        . 'FROM ' . table_name($config, 'timeline_entries') . ' tl WHERE tl.campaign_id = ? AND tl.review_status = \'approved\' AND tl.publication_status = \'published\' AND '
        . v1_required_document_visibility_sql($config, 'tl.document_id')
        . ' ORDER BY tl.occurred_at ASC LIMIT 30');
    $timelineStmt->execute(array($campaignId));
    $voteStmt = $pdo->prepare('SELECT proposal_vote_id, event_id, agenda_no, agenda_title, original_language, meeting_at, recommendation, '
        . 'recommendation_source, result, votes_for, votes_against, votes_abstain, evidence_document_id '
        . 'FROM ' . table_name($config, 'proposal_votes') . ' v WHERE v.campaign_id = ? AND v.review_status = \'approved\' AND v.publication_status = \'published\' AND '
        . v1_required_document_visibility_sql($config, 'v.evidence_document_id')
        . ' ORDER BY v.meeting_at ASC LIMIT 50');
    $voteStmt->execute(array($campaignId));
    $commitmentStmt = $pdo->prepare('SELECT commitment_id, event_id, LEFT(commitment_text,2000) AS commitment_text, original_language, target_at, LEFT(actual_action,2000) AS actual_action, status, evidence_document_id '
        . 'FROM ' . table_name($config, 'commitment_outcomes') . ' co WHERE co.campaign_id = ? AND co.review_status = \'approved\' AND co.publication_status = \'published\' AND '
        . v1_required_document_visibility_sql($config, 'co.evidence_document_id')
        . ' ORDER BY COALESCE(co.target_at, co.updated_at) ASC LIMIT 25');
    $commitmentStmt->execute(array($campaignId));
    v1_respond(200, array('ok' => true, 'data' => array(
        'campaign' => $campaign,
        'timeline' => $timelineStmt->fetchAll(),
        'votes' => $voteStmt->fetchAll(),
        'commitments' => $commitmentStmt->fetchAll(),
    )));
}

function v1_utf8_byte_prefix(string $bytes, int $maxBytes): string {
    $candidate = substr($bytes, 0, $maxBytes);
    while ($candidate !== '' && !mb_check_encoding($candidate, 'UTF-8')) {
        $candidate = substr($candidate, 0, -1);
    }
    return $candidate;
}

function v1_get_document(PDO $pdo, array $config, string $documentId): void {
    $includeBody = isset($_GET['include']) && (string)$_GET['include'] === 'body';
    $bodyOffset = isset($_GET['body_offset']) ? (int)$_GET['body_offset'] : 0;
    $bodyLimit = isset($_GET['body_limit_bytes']) ? (int)$_GET['body_limit_bytes']
        : (isset($_GET['body_limit']) ? (int)$_GET['body_limit'] : 60000);
    if (isset($_GET['body_limit_bytes']) && isset($_GET['body_limit']) && (int)$_GET['body_limit_bytes'] !== (int)$_GET['body_limit']) {
        v1_respond(400, array('ok' => false, 'error' => 'conflicting_body_limit'));
    }
    if ($bodyOffset < 0 || $bodyOffset > 1000000000 || $bodyLimit < 1 || $bodyLimit > 60000) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_body_page'));
    }
    if (!$includeBody && (isset($_GET['body_offset']) || isset($_GET['body_limit']) || isset($_GET['body_limit_bytes']))) {
        v1_respond(400, array('ok' => false, 'error' => 'include_body_required_for_paging'));
    }
    $bodyField = $includeBody
        ? 'SUBSTRING(CAST(d.body_text AS BINARY), ' . ($bodyOffset + 1) . ', ' . ($bodyLimit + 4) . ') AS body_page_bytes, OCTET_LENGTH(d.body_text) AS body_total_bytes,'
        : 'LEFT(d.body_text, 4000) AS body_excerpt, OCTET_LENGTH(d.body_text) AS body_total_bytes,';
    $sql = 'SELECT d.document_id, d.company_id, c.legal_name AS company_name, d.source_class, d.external_id, d.document_type, '
        . 'd.original_language, d.title, ' . $bodyField . ' d.original_url, d.content_hash, d.correction_of_document_id, '
        . 'd.version_no, d.published_at, d.retrieved_at, d.verification_status '
        . 'FROM ' . table_name($config, 'documents') . ' d '
        . 'LEFT JOIN ' . table_name($config, 'companies') . ' c ON c.company_id = d.company_id '
        . 'LEFT JOIN ' . table_name($config, 'source_rights') . ' sr ON sr.source_right_id = d.source_right_id '
        . 'WHERE d.document_id = ? AND ' . v1_document_visibility_sql('d', 'sr') . ' LIMIT 1';
    $stmt = $pdo->prepare($sql); $stmt->execute(array($documentId));
    $document = $stmt->fetch();
    if (!$document) { v1_respond(404, array('ok' => false, 'error' => 'document_not_found')); }
    $totalBytes = isset($document['body_total_bytes']) && $document['body_total_bytes'] !== null ? (int)$document['body_total_bytes'] : 0;
    $document['body_total_bytes'] = $totalBytes;
    if ($includeBody) {
        $raw = isset($document['body_page_bytes']) && is_string($document['body_page_bytes']) ? $document['body_page_bytes'] : '';
        unset($document['body_page_bytes']);
        if ($raw !== '' && $bodyOffset > 0 && (ord($raw[0]) & 0xC0) === 0x80) {
            v1_respond(400, array('ok' => false, 'error' => 'body_offset_not_utf8_boundary'));
        }
        $pageBody = v1_utf8_byte_prefix($raw, $bodyLimit);
        $returnedBytes = strlen($pageBody);
        $nextOffset = $bodyOffset + $returnedBytes;
        $document['body_text'] = $pageBody;
        $document['body_offset'] = $bodyOffset;
        $document['body_bytes_returned'] = $returnedBytes;
        $document['body_truncated'] = $nextOffset < $totalBytes;
        $document['body_next_offset'] = $nextOffset < $totalBytes ? $nextOffset : null;
    } else {
        $document['body_truncated'] = $totalBytes > strlen(isset($document['body_excerpt']) ? (string)$document['body_excerpt'] : '');
    }
    v1_respond(200, array('ok' => true, 'data' => $document));
}

function v1_date_bound(string $key, string $fallback): string {
    $value = isset($_GET[$key]) ? trim((string)$_GET[$key]) : $fallback;
    if (!preg_match('/^\d{4}-\d{2}-\d{2}$/', $value)) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_' . $key));
    }
    $parsed = DateTimeImmutable::createFromFormat('!Y-m-d', $value, new DateTimeZone('UTC'));
    if (!$parsed || $parsed->format('Y-m-d') !== $value) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_' . $key));
    }
    return $value;
}

/** Apply the public event filter vocabulary to proposal-vote calendar rows. */
function v1_calendar_vote_filter_parts(array $config): array {
    $where = array(
        'v.review_status = \'approved\'',
        'v.publication_status = \'published\'',
        v1_required_document_visibility_sql($config, 'v.evidence_document_id'),
    );
    $params = array();
    $companyId = isset($_GET['company_id']) ? trim((string)$_GET['company_id']) : '';
    $actorId = isset($_GET['actor_id']) ? trim((string)$_GET['actor_id']) : '';
    $eventType = isset($_GET['event_type']) ? trim((string)$_GET['event_type']) : '';
    $verification = isset($_GET['verification_status']) ? trim((string)$_GET['verification_status']) : '';
    $status = isset($_GET['status']) ? trim((string)$_GET['status']) : '';
    $identityStatus = isset($_GET['identity_status']) ? trim((string)$_GET['identity_status']) : '';
    $importance = isset($_GET['importance']) ? trim((string)$_GET['importance']) : '';
    $sourceClass = isset($_GET['source_class']) ? trim((string)$_GET['source_class']) : '';
    $evidenceDocumentId = isset($_GET['evidence_document_id']) ? trim((string)$_GET['evidence_document_id']) : '';
    if ($companyId !== '') {
        if (preg_match('/^[0-9]{8}$/', $companyId) !== 1) { v1_respond(400, array('ok'=>false,'error'=>'invalid_company_id')); }
        $where[] = 'v.company_id = ?'; $params[] = $companyId;
    }
    if ($actorId !== '') {
        if (!v1_valid_entity_id($actorId, 64)) { v1_respond(400, array('ok'=>false,'error'=>'invalid_actor_id')); }
        $where[] = '(v.proposer_actor_id = ? OR EXISTS (SELECT 1 FROM ' . table_name($config,'event_actors')
            . ' vote_filter_ea WHERE vote_filter_ea.event_id=v.event_id AND vote_filter_ea.actor_id=? '
            . 'AND vote_filter_ea.review_status=\'approved\'))';
        $params[] = $actorId; $params[] = $actorId;
    }
    if ($status !== '') {
        if ($verification !== '' && $verification !== $status) {
            v1_respond(400,array('ok'=>false,'error'=>'conflicting_status_filters'));
        }
        $verification = $status;
    }
    $eventFields = array('event_type'=>$eventType,'verification_status'=>$verification,'identity_status'=>$identityStatus,'importance'=>$importance);
    foreach ($eventFields as $field => $value) {
        if ($value === '') { continue; }
        if (preg_match('/^[A-Za-z0-9_.:\-]{1,64}$/',$value) !== 1) {
            v1_respond(400,array('ok'=>false,'error'=>'invalid_'.$field));
        }
        $where[] = 'EXISTS (SELECT 1 FROM ' . table_name($config,'governance_events') . ' vote_filter_e '
            . 'WHERE vote_filter_e.event_id=v.event_id AND vote_filter_e.' . $field . '=? AND '
            . v1_event_visibility_sql($config,'vote_filter_e') . ')';
        $params[] = $value;
    }
    if ($sourceClass !== '') {
        if (preg_match('/^[A-Za-z0-9_.:\-]{1,40}$/',$sourceClass) !== 1) {
            v1_respond(400,array('ok'=>false,'error'=>'invalid_source_class'));
        }
        $where[] = 'EXISTS (SELECT 1 FROM ' . table_name($config,'documents') . ' vote_filter_d '
            . 'LEFT JOIN ' . table_name($config,'source_rights') . ' vote_filter_sr '
            . 'ON vote_filter_sr.source_right_id=vote_filter_d.source_right_id '
            . 'WHERE vote_filter_d.document_id=v.evidence_document_id AND vote_filter_d.source_class=? AND '
            . v1_document_visibility_sql('vote_filter_d','vote_filter_sr') . ')';
        $params[] = $sourceClass;
    }
    if ($evidenceDocumentId !== '') {
        if (!v1_valid_entity_id($evidenceDocumentId)) {
            v1_respond(400,array('ok'=>false,'error'=>'invalid_evidence_document_id'));
        }
        $where[] = 'v.evidence_document_id = ?'; $params[] = $evidenceDocumentId;
    }
    return array($where,$params);
}

function v1_calendar(PDO $pdo, array $config): void {
    $defaultFrom = gmdate('Y-m-d');
    $defaultTo = gmdate('Y-m-d', time() + 90 * 86400);
    $from = v1_date_bound('from', $defaultFrom);
    $to = v1_date_bound('to', $defaultTo);
    $fromDate = new DateTimeImmutable($from, new DateTimeZone('UTC'));
    $toDate = new DateTimeImmutable($to, new DateTimeZone('UTC'));
    if ($toDate < $fromDate || $toDate->diff($fromDate)->days > 366) {
        v1_respond(400, array('ok' => false, 'error' => 'calendar_range_exceeds_366_days'));
    }
    $page = v1_list_params();
    $start = $from . ' 00:00:00';
    $end = $to . ' 23:59:59';
    list($eventWhere, $eventParams) = v1_event_query_parts($config, false);
    $eventWhere[] = 'COALESCE(e.deadline_at, e.occurred_at) BETWEEN ? AND ?';
    $eventParams[] = $start; $eventParams[] = $end;
    list($voteWhere, $voteParams) = v1_calendar_vote_filter_parts($config);
    $voteWhere[] = 'v.meeting_at BETWEEN ? AND ?';
    $voteParams[] = $start; $voteParams[] = $end;
    $sql = 'SELECT * FROM ('
        . 'SELECT CONCAT(\'event:\', e.event_id) AS calendar_id, \'event\' AS item_type, e.event_id AS entity_id, e.company_id, '
        . 'c.legal_name AS company_name, COALESCE(e.deadline_at, e.occurred_at) AS scheduled_at, e.title, e.original_language, e.event_type AS category '
        . 'FROM ' . table_name($config, 'governance_events') . ' e JOIN ' . table_name($config, 'companies') . ' c ON c.company_id = e.company_id '
        . 'WHERE ' . implode(' AND ', $eventWhere) . ' ';
    $params = $eventParams;
    $sql .= 'UNION ALL '
        . 'SELECT CONCAT(\'vote:\', v.proposal_vote_id), \'proposal_vote\', v.proposal_vote_id, v.company_id, c.legal_name, '
        . 'v.meeting_at, v.agenda_title, v.original_language, \'proposal_vote\' '
        . 'FROM ' . table_name($config, 'proposal_votes') . ' v JOIN ' . table_name($config, 'companies') . ' c ON c.company_id = v.company_id '
        . 'WHERE ' . implode(' AND ', $voteWhere);
    $params = array_merge($params,$voteParams);
    $sql .= ') calendar_items ORDER BY scheduled_at ASC, calendar_id ASC LIMIT ' . ((int)$page['limit'] + 1)
        . ' OFFSET ' . (int)$page['offset'];
    $stmt = $pdo->prepare($sql); $stmt->execute($params);
    list($rows, $hasMore) = v1_fetch_page($stmt, $page);
    v1_respond(200, array(
        'ok' => true,
        'range' => array('from' => $from, 'to' => $to),
        'data' => $rows,
        'pagination' => v1_page_meta($page, count($rows), $hasMore),
    ));
}

function v1_like(string $query): string {
    $escaped = str_replace(array('\\', '%', '_'), array('\\\\', '\\%', '\\_'), $query);
    return '%' . $escaped . '%';
}

function v1_search(PDO $pdo, array $config): void {
    $query = isset($_GET['q']) ? trim((string)$_GET['q']) : '';
    if (mb_strlen($query, 'UTF-8') < 2) {
        v1_respond(400, array('ok' => false, 'error' => 'query_too_short'));
    }
    $query = mb_substr($query, 0, 100, 'UTF-8');
    $page = v1_list_params();
    $like = v1_like($query);
    if (v1_event_filter_requested(true)) {
        list($eventWhere, $eventParams) = v1_event_query_parts($config);
        $eventWhere[] = '(e.title LIKE ? OR e.summary LIKE ?)';
        $eventParams[] = $like; $eventParams[] = $like;
        $stmt = $pdo->prepare('SELECT \'event\' AS kind, e.event_id AS entity_id, e.title, e.event_type AS subtitle, '
            . 'e.company_id, e.occurred_at, e.occurred_at AS sort_at FROM ' . table_name($config, 'governance_events')
            . ' e WHERE ' . implode(' AND ', $eventWhere) . ' ORDER BY sort_at DESC, entity_id ASC LIMIT '
            . ((int)$page['limit'] + 1) . ' OFFSET ' . (int)$page['offset']);
        $stmt->execute($eventParams); list($rows, $hasMore) = v1_fetch_page($stmt, $page);
        v1_respond(200, array('ok'=>true,'query'=>$query,'filters_scope'=>'events','data'=>$rows,
            'pagination'=>v1_page_meta($page,count($rows),$hasMore)));
    }
    $sql = 'SELECT * FROM ('
        . 'SELECT \'company\' AS kind, c.company_id AS entity_id, c.legal_name AS title, c.legal_name_en AS subtitle, '
        . 'NULL AS company_id, NULL AS occurred_at, c.updated_at AS sort_at '
        . 'FROM ' . table_name($config, 'companies') . ' c WHERE c.record_status = \'active\' AND ' . v1_company_has_public_event_sql($config, 'c') . ' '
        . 'AND (c.legal_name LIKE ? OR c.legal_name_en LIKE ? OR c.short_name LIKE ? OR c.stock_code = ?) '
        . 'UNION ALL '
        . 'SELECT \'actor\', a.actor_id, a.display_name, a.actor_type, a.company_id, NULL, a.updated_at '
        . 'FROM ' . table_name($config, 'actors') . ' a WHERE ' . v1_actor_visibility_sql($config, 'a') . ' '
        . 'AND (a.display_name LIKE ? OR a.display_name_en LIKE ? OR a.aliases_json LIKE ?) '
        . 'UNION ALL '
        . 'SELECT \'event\', e.event_id, e.title, e.event_type, e.company_id, e.occurred_at, e.occurred_at '
        . 'FROM ' . table_name($config, 'governance_events') . ' e WHERE ' . v1_event_visibility_sql($config, 'e') . ' AND (e.title LIKE ? OR e.summary LIKE ?) '
        . 'UNION ALL '
        . 'SELECT \'campaign\', cp.campaign_id, cp.title, cp.stage, cp.company_id, cp.started_at, cp.started_at '
        . 'FROM ' . table_name($config, 'campaigns') . ' cp WHERE ' . v1_campaign_visibility_sql($config, 'cp') . ' AND (cp.title LIKE ? OR cp.demand_text LIKE ?) '
        . 'UNION ALL '
        . 'SELECT \'document\', d.document_id, d.title, d.source_class, d.company_id, d.published_at, COALESCE(d.published_at, d.retrieved_at) '
        . 'FROM ' . table_name($config, 'documents') . ' d LEFT JOIN ' . table_name($config, 'source_rights') . ' sr ON sr.source_right_id = d.source_right_id '
        . 'WHERE ' . v1_document_visibility_sql('d', 'sr') . ' AND d.title LIKE ?'
        . ') search_results ORDER BY sort_at DESC, kind ASC, entity_id ASC LIMIT ' . ((int)$page['limit'] + 1)
        . ' OFFSET ' . (int)$page['offset'];
    $params = array($like, $like, $like, $query, $like, $like, $like, $like, $like, $like, $like, $like);
    $stmt = $pdo->prepare($sql); $stmt->execute($params);
    list($rows, $hasMore) = v1_fetch_page($stmt, $page);
    v1_respond(200, array(
        'ok' => true,
        'query' => $query,
        'data' => $rows,
        'pagination' => v1_page_meta($page, count($rows), $hasMore),
    ));
}

function v1_export_events_json(PDO $pdo, array $config): void {
    $page = v1_list_params();
    list($rows, $hasMore) = v1_query_public_events($pdo, $config, $page);
    header('Content-Disposition: attachment; filename="bside-governance-events.json"');
    v1_respond(200, array(
        'ok' => true,
        'generated_at' => gmdate('c'),
        'data' => $rows,
        'pagination' => v1_page_meta($page, count($rows), $hasMore),
    ));
}

/** Neutralize spreadsheet formula execution in CSV without changing stored data. */
function v1_csv_export_cell($value): string {
    $text = $value === null ? '' : (string)$value;
    if (preg_match('/^[\x00-\x20]*[=+\-@]/', $text) === 1) {
        return "'" . $text;
    }
    return $text;
}

function v1_export_events_csv(PDO $pdo, array $config): void {
    $page = v1_list_params();
    list($rows, $hasMore) = v1_query_public_events($pdo, $config, $page);
    $stream = fopen('php://temp', 'w+');
    if ($stream === false) { v1_respond(500, array('ok' => false, 'error' => 'export_failed')); }
    fputcsv($stream, array('event_id', 'company_id', 'stock_code', 'market', 'company_name', 'event_type', 'title', 'original_language', 'occurred_at', 'deadline_at', 'importance', 'verification_status', 'updated_at'));
    foreach ($rows as $row) {
        fputcsv($stream, array_map('v1_csv_export_cell', array(
            $row['event_id'], $row['company_id'], $row['stock_code'], $row['market'], $row['company_name'],
            $row['event_type'], $row['title'], $row['original_language'], $row['occurred_at'], $row['deadline_at'],
            $row['importance'], $row['verification_status'], $row['updated_at'],
        )));
    }
    rewind($stream);
    $csv = stream_get_contents($stream);
    fclose($stream);
    if ($csv === false || strlen($csv) + 3 > V1_RESPONSE_BUDGET_BYTES) {
        v1_respond(500, array('ok' => false, 'error' => 'response_budget_exceeded'));
    }
    header('Content-Type: text/csv; charset=utf-8');
    header('Content-Disposition: attachment; filename="bside-governance-events.csv"');
    header('X-BSIDE-API-Version: v1');
    header('X-Has-More: ' . ($hasMore ? 'true' : 'false'));
    header('X-Next-Page: ' . ($hasMore ? ((int)$page['page'] + 1) : ''));
    header('X-Response-Bytes: ' . (strlen($csv) + 3));
    echo "\xEF\xBB\xBF" . $csv;
    exit;
}

function v1_xml(string $value): string {
    return htmlspecialchars($value, ENT_QUOTES | ENT_XML1, 'UTF-8');
}

function v1_event_feed_self_query(array $page): string {
    $query = array();
    $verification = isset($_GET['verification_status']) ? trim((string)$_GET['verification_status']) : '';
    $status = isset($_GET['status']) ? trim((string)$_GET['status']) : '';
    if ($verification === '') { $verification = $status; }
    foreach (array(
        'company_id', 'actor_id', 'event_type', 'identity_status', 'importance',
        'source_class', 'evidence_document_id'
    ) as $key) {
        $value = isset($_GET[$key]) ? trim((string)$_GET[$key]) : '';
        if ($value !== '') { $query[$key] = $value; }
    }
    if ($verification !== '') { $query['verification_status'] = $verification; }
    foreach (array('from', 'to') as $key) {
        $value = isset($_GET[$key]) ? trim((string)$_GET[$key]) : '';
        if ($value === '') { continue; }
        $normalized = mysql_dt($value);
        if ($normalized !== null) {
            $query[$key] = gmdate('Y-m-d\TH:i:s\Z', strtotime($normalized . ' UTC'));
        }
    }
    $query['limit'] = (int)$page['limit'];
    ksort($query, SORT_STRING);
    return http_build_query($query, '', '&', PHP_QUERY_RFC3986);
}

function v1_events_atom(PDO $pdo, array $config): void {
    $_GET['limit'] = isset($_GET['limit']) ? min(100, (int)$_GET['limit']) : 50;
    $_GET['page'] = 1;
    $page = v1_list_params();
    list($rows) = v1_query_public_events($pdo, $config, $page);
    $apiBase = isset($config['governance_api_base_url'])
        ? rtrim((string)$config['governance_api_base_url'],'/')
        : 'https://alignpe.gabia.io/activist/api.php/api/v1';
    if (filter_var($apiBase,FILTER_VALIDATE_URL) === false || stripos($apiBase,'https://') !== 0
        || parse_url($apiBase,PHP_URL_QUERY) !== null || parse_url($apiBase,PHP_URL_FRAGMENT) !== null) {
        v1_respond(500,array('ok'=>false,'error'=>'invalid_governance_api_base_url'));
    }
    $selfQuery = v1_event_feed_self_query($page);
    $selfUrl = $apiBase . '/feeds/events.atom' . ($selfQuery === '' ? '' : '?' . $selfQuery);
    $publicBase = isset($config['public_base_url'])
        ? rtrim((string)$config['public_base_url'],'/')
        : 'https://news.bside.ai';
    if (filter_var($publicBase,FILTER_VALIDATE_URL) === false || stripos($publicBase,'https://') !== 0
        || parse_url($publicBase,PHP_URL_QUERY) !== null || parse_url($publicBase,PHP_URL_FRAGMENT) !== null) {
        v1_respond(500,array('ok'=>false,'error'=>'invalid_public_base_url'));
    }
    $updated = $rows ? (string)$rows[0]['updated_at'] : gmdate('Y-m-d H:i:s');
    $updatedIso = gmdate('c', strtotime($updated . ' UTC'));
    $xml = '<?xml version="1.0" encoding="UTF-8"?>' . "\n";
    $xml .= '<feed xmlns="http://www.w3.org/2005/Atom">' . "\n";
    $xml .= '<id>' . v1_xml($selfUrl) . '</id>' . "\n";
    $xml .= '<title>BSIDE Governance Events</title>' . "\n";
    $xml .= '<updated>' . v1_xml($updatedIso) . '</updated>' . "\n";
    $xml .= '<link rel="self" href="' . v1_xml($selfUrl) . '" type="application/atom+xml" />' . "\n";
    foreach ($rows as $row) {
        $eventUrl = $publicBase . '/#/events/' . rawurlencode((string)$row['event_id']);
        $publishedIso = gmdate('c', strtotime((string)$row['occurred_at'] . ' UTC'));
        $entryUpdated = gmdate('c', strtotime((string)$row['updated_at'] . ' UTC'));
        $xml .= '<entry>' . "\n";
        $xml .= '<id>' . v1_xml('urn:bside:event:' . (string)$row['event_id']) . '</id>' . "\n";
        $xml .= '<title>' . v1_xml((string)$row['title']) . '</title>' . "\n";
        $xml .= '<link rel="alternate" href="' . v1_xml($eventUrl) . '" />' . "\n";
        $xml .= '<published>' . v1_xml($publishedIso) . '</published>' . "\n";
        $xml .= '<updated>' . v1_xml($entryUpdated) . '</updated>' . "\n";
        $xml .= '<category term="' . v1_xml((string)$row['event_type']) . '" />' . "\n";
        $xml .= '<summary>' . v1_xml((string)$row['company_name'] . ' · ' . (string)$row['verification_status']) . '</summary>' . "\n";
        $xml .= '</entry>' . "\n";
    }
    $xml .= '</feed>';
    if (strlen($xml) > V1_RESPONSE_BUDGET_BYTES) { v1_respond(500, array('ok' => false, 'error' => 'response_budget_exceeded')); }
    header('Content-Type: application/atom+xml; charset=utf-8');
    header('Cache-Control: public, max-age=300');
    header('X-BSIDE-API-Version: v1');
    header('X-Response-Bytes: ' . strlen($xml));
    echo $xml;
    exit;
}

function v1_submit_feedback(PDO $pdo, array $config): void {
    $contentType = isset($_SERVER['CONTENT_TYPE']) ? strtolower((string)$_SERVER['CONTENT_TYPE']) : '';
    if (strpos($contentType, 'application/json') !== 0) {
        v1_respond(415, array('ok' => false, 'error' => 'application_json_required'));
    }
    $body = read_body($config);
    $payload = decode_json_body($body);
    if (!empty($payload['website'])) {
        v1_respond(202, array('ok' => true, 'feedback_id' => 'accepted', 'status' => 'pending'));
    }
    $type = isset($payload['feedback_type']) ? (string)$payload['feedback_type'] : '';
    if (!in_array($type, array('correction', 'right_of_reply', 'source_rights', 'general'), true)) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_feedback_type'));
    }
    $message = isset($payload['message']) ? trim((string)$payload['message']) : '';
    if (mb_strlen($message, 'UTF-8') < 10 || mb_strlen($message, 'UTF-8') > 10000) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_message_length'));
    }
    $entityType = isset($payload['entity_type']) ? trim((string)$payload['entity_type']) : '';
    $entityId = isset($payload['entity_id']) ? trim((string)$payload['entity_id']) : '';
    if (($entityType === '') !== ($entityId === '')) {
        v1_respond(400, array('ok' => false, 'error' => 'entity_type_and_id_required_together'));
    }
    if ($entityType !== '' && (!in_array($entityType, array('company', 'event', 'campaign', 'document', 'actor'), true) || !v1_valid_entity_id($entityId))) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_entity_reference'));
    }
    $evidence = isset($payload['evidence_urls']) && is_array($payload['evidence_urls']) ? array_slice($payload['evidence_urls'], 0, 10) : array();
    $safeEvidence = array();
    foreach ($evidence as $url) {
        $url = trim((string)$url);
        if (strlen($url) <= 2048 && filter_var($url, FILTER_VALIDATE_URL) && preg_match('#^https?://#i', $url)) { $safeEvidence[] = $url; }
    }
    $ip = isset($_SERVER['REMOTE_ADDR']) ? (string)$_SERVER['REMOTE_ADDR'] : 'unknown';
    $salt = isset($config['feedback_ip_salt']) ? (string)$config['feedback_ip_salt'] : (isset($config['api_secret']) ? (string)$config['api_secret'] : 'bside');
    if (strlen($salt) < 32) { v1_respond(503, array('ok' => false, 'error' => 'feedback_rate_limit_not_configured')); }
    $ipHash = hash_hmac('sha256', $ip, $salt);
    $rateStmt = $pdo->prepare('SELECT COUNT(*) FROM ' . table_name($config, 'feedback') . ' WHERE ip_hash = ? AND created_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 HOUR)');
    $rateStmt->execute(array($ipHash));
    if ((int)$rateStmt->fetchColumn() >= 5) {
        header('Retry-After: 3600');
        v1_respond(429, array('ok' => false, 'error' => 'feedback_rate_limited'));
    }
    $id = 'fb_' . bin2hex(random_bytes(16));
    $userAgent = isset($_SERVER['HTTP_USER_AGENT']) ? (string)$_SERVER['HTTP_USER_AGENT'] : '';
    $now = gmdate('Y-m-d H:i:s');
    $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'feedback') . ' (feedback_id, feedback_type, entity_type, entity_id, '
        . 'submitter_name, submitter_contact, message, evidence_urls_json, status, is_public, ip_hash, user_agent_hash, created_at, updated_at) '
        . 'VALUES (?,?,?,?,?,?,?,?,\'pending\',0,?,?,?,?)');
    $stmt->execute(array(
        $id, $type, $entityType !== '' ? $entityType : null, $entityId !== '' ? $entityId : null,
        isset($payload['submitter_name']) ? mb_substr(trim((string)$payload['submitter_name']), 0, 191, 'UTF-8') : null,
        isset($payload['submitter_contact']) ? mb_substr(trim((string)$payload['submitter_contact']), 0, 320, 'UTF-8') : null,
        $message, json_value($safeEvidence), $ipHash, $userAgent !== '' ? hash('sha256', $userAgent) : null, $now, $now,
    ));
    v1_respond(202, array('ok' => true, 'feedback_id' => $id, 'status' => 'pending', 'is_public' => false));
}

function v1_ops_health(PDO $pdo, array $config): void {
    $officialStmt = $pdo->query('SELECT run_id,pipeline,source_key,code_revision,status,started_at,finished_at,first_observed_at,'
        . 'raw_count,acknowledged_count,metrics_json FROM ' . table_name($config, 'collection_runs')
        . ' WHERE source_key IN (\'dart\',\'kind\',\'dart+kind\',\'kind+dart\') ORDER BY COALESCE(finished_at,started_at) DESC LIMIT 5000');
    $official = array(
        'dart'=>array('last_attempt_at'=>null,'last_success_at'=>null,'last_scheduled_slot_at'=>null,'last_scheduled_success_at'=>null),
        'kind'=>array('last_attempt_at'=>null,'last_success_at'=>null,'last_scheduled_slot_at'=>null,'last_scheduled_success_at'=>null),
    );
    $healthClaimStmt = $pdo->query('SELECT * FROM ' . table_name($config,'official_slot_claims')
        . ' ORDER BY scheduled_slot_at DESC LIMIT 5000');
    $healthClaims = array();
    foreach ($healthClaimStmt->fetchAll() as $claim) { $healthClaims[(string)$claim['claim_id']] = $claim; }
    foreach ($officialStmt->fetchAll() as $run) {
        $ledger = v1_official_run_ledger_row($run);
        $claimId = isset($ledger['slot_claim_id']) && is_string($ledger['slot_claim_id']) ? $ledger['slot_claim_id'] : '';
        if ($claimId !== '' && isset($healthClaims[$claimId])
            && v1_official_claim_matches_ledger_run($healthClaims[$claimId],$ledger,$run)) {
            $ledger['slot_claim_status'] = (string)$healthClaims[$claimId]['status'];
            $ledger['slot_claim_terminal_reason'] = $healthClaims[$claimId]['terminal_reason'] === null
                ? null : (string)$healthClaims[$claimId]['terminal_reason'];
        }
        if (!v1_official_scheduled_run_matches($ledger)) { continue; }
        $scheduledSlot = v1_mysql_datetime_utc($ledger['scheduled_slot_at']);
        if ($scheduledSlot === null) { continue; }
        foreach ($ledger['source_outcomes'] as $source => $outcome) {
            if (!isset($official[$source])) { continue; }
            if ($official[$source]['last_scheduled_slot_at'] === null || $scheduledSlot > $official[$source]['last_scheduled_slot_at']) {
                $official[$source]['last_scheduled_slot_at'] = $scheduledSlot;
                $official[$source]['last_attempt_at'] = $scheduledSlot;
            }
            $sourceStatus = strtolower((string)$outcome['status']);
            $topStatus = strtolower((string)$ledger['status']);
            $completeAck = in_array($topStatus,array('success','succeeded'),true)
                && (int)$ledger['raw_count'] === (int)$ledger['acknowledged_count']
                && $ledger['slot_claim_status'] === 'completed'
                && $ledger['slot_claim_late'] === false
                && (!isset($ledger['slot_claim_terminal_reason']) || $ledger['slot_claim_terminal_reason'] === null)
                && is_int($outcome['raw_count']) && is_int($outcome['acknowledged_count'])
                && $outcome['raw_count'] === $outcome['acknowledged_count'];
            if ($completeAck && in_array($sourceStatus,array('success','succeeded'),true)
                && ($official[$source]['last_scheduled_success_at'] === null || $scheduledSlot > $official[$source]['last_scheduled_success_at'])) {
                $official[$source]['last_scheduled_success_at'] = $scheduledSlot;
                $official[$source]['last_success_at'] = $scheduledSlot;
            }
        }
    }
    $lastSuccess = $official['dart']['last_success_at'] !== null && $official['kind']['last_success_at'] !== null
        ? min($official['dart']['last_success_at'],$official['kind']['last_success_at']) : null;
    $nowEpoch = time();
    foreach ($official as &$sourceState) {
        $lastEpoch = $sourceState['last_success_at'] !== null ? strtotime($sourceState['last_success_at'] . ' UTC') : false;
        $sourceState['freshness_seconds'] = $lastEpoch !== false ? max(0,$nowEpoch-$lastEpoch) : null;
        $sourceState['last_attempt_at'] = v1_release_iso_time($sourceState['last_attempt_at']);
        $sourceState['last_success_at'] = v1_release_iso_time($sourceState['last_success_at']);
        $sourceState['last_scheduled_slot_at'] = v1_release_iso_time($sourceState['last_scheduled_slot_at']);
        $sourceState['last_scheduled_success_at'] = v1_release_iso_time($sourceState['last_scheduled_success_at']);
        $sourceState['status'] = $lastEpoch === false ? 'missing' : 'observed';
    }
    unset($sourceState);
    $pending = scalar_int($pdo, 'SELECT COUNT(*) FROM ' . table_name($config, 'delivery_outbox') . ' WHERE status IN (\'pending\',\'retry\',\'remote_queued\',\'processing\')');
    $oldestStmt = $pdo->query('SELECT MIN(created_at) FROM ' . table_name($config, 'delivery_outbox') . ' WHERE status IN (\'pending\',\'retry\',\'remote_queued\',\'processing\')');
    $oldestPending = $oldestStmt->fetchColumn();
    $dead = scalar_int($pdo, 'SELECT COUNT(*) FROM ' . table_name($config, 'delivery_outbox') . ' WHERE status = \'dead_letter\'');
    $deploymentStmt = $pdo->query('SELECT observation_id,observed_at,distribution_target,build_sha,workflow_run_id,'
        . 'workflow_run_attempt,source FROM ' . table_name($config,'web_distribution_observations')
        . ' WHERE succeeded=1 ORDER BY observed_at DESC,created_at DESC,observation_id DESC LIMIT 2');
    $deploymentRows = $deploymentStmt->fetchAll();
    $activeDeployment = null; $activeDeploymentStatus = 'missing';
    if (count($deploymentRows) > 0) {
        $latest = $deploymentRows[0]; $activeDeploymentStatus = 'observed';
        if (isset($deploymentRows[1]) && (string)$deploymentRows[1]['observed_at'] === (string)$latest['observed_at']
            && (string)$deploymentRows[1]['build_sha'] !== (string)$latest['build_sha']) {
            $activeDeploymentStatus = 'ambiguous';
        } elseif (v1_valid_build_sha($latest['build_sha']) === null) {
            $activeDeploymentStatus = 'invalid';
        } else {
            $activeDeployment = array(
                'build_sha'=>strtolower((string)$latest['build_sha']),
                'observed_at'=>v1_release_iso_time($latest['observed_at']),
                'distribution_target'=>(string)$latest['distribution_target'],
                'workflow_run_id'=>(int)$latest['workflow_run_id'],
                'workflow_run_attempt'=>(int)$latest['workflow_run_attempt'],
                'source'=>(string)$latest['source'],
            );
        }
    }
    $status = $lastSuccess !== null && $activeDeployment !== null ? 'ok' : 'degraded';
    v1_respond(200, array(
        'ok' => true,
        'status' => $status,
        'last_success_at' => v1_release_iso_time($lastSuccess),
        'official_sources' => $official,
        'pending_outbox' => $pending,
        'oldest_pending_at' => $oldestPending ?: null,
        'dead_letter_count' => $dead,
        'active_deployment' => $activeDeployment,
        'active_deployment_status' => $activeDeploymentStatus,
        'checked_at' => gmdate('c'),
    ));
}

/** Evaluate the separately approved KIND right without exposing its evidence reference. */
function v1_kind_source_right_eligibility(PDO $pdo, array $config, bool $forUpdate = false): array {
    $sql = 'SELECT source_right_id,source_type,source_key,permission_scope,evidence_uri,evidence_hash,valid_from,valid_until,'
        . 'revoked_at,ai_allowed,redistribution_allowed,status,updated_at FROM ' . table_name($config,'source_rights')
        . ' WHERE source_right_id=\'official:kind\'' . ($forUpdate ? ' FOR UPDATE' : '');
    $row = $pdo->query($sql)->fetch();
    if (!is_array($row)) {
        return array('eligible'=>false,'rights_revision'=>null,'reasons'=>array('source_right_missing'));
    }
    $reasons = array();
    if ((string)$row['source_type'] !== 'official_disclosure' || (string)$row['source_key'] !== 'kind') {
        $reasons[] = 'source_identity_mismatch';
    }
    if ((string)$row['status'] !== 'active') { $reasons[] = 'source_right_inactive'; }
    $now = gmdate('Y-m-d H:i:s');
    if ((string)$row['valid_from'] > $now) { $reasons[] = 'source_right_not_yet_valid'; }
    if ($row['valid_until'] !== null && (string)$row['valid_until'] <= $now) { $reasons[] = 'source_right_expired'; }
    if ($row['revoked_at'] !== null) { $reasons[] = 'source_right_revoked'; }
    if (trim((string)$row['permission_scope']) === '') { $reasons[] = 'permission_scope_missing'; }
    if (trim((string)$row['evidence_uri']) === '' && preg_match('/^[a-f0-9]{64}$/',(string)$row['evidence_hash']) !== 1) {
        $reasons[] = 'evidence_missing';
    }
    if ((int)$row['ai_allowed'] !== 1) { $reasons[] = 'ai_not_allowed'; }
    if ((int)$row['redistribution_allowed'] !== 1) { $reasons[] = 'redistribution_not_allowed'; }
    $revisionPayload = array(
        'source_right_id'=>(string)$row['source_right_id'],'source_type'=>(string)$row['source_type'],
        'source_key'=>(string)$row['source_key'],'permission_scope_sha256'=>hash('sha256',(string)$row['permission_scope']),
        'evidence_present'=>trim((string)$row['evidence_uri']) !== '' || trim((string)$row['evidence_hash']) !== '',
        'valid_from'=>(string)$row['valid_from'],'valid_until'=>$row['valid_until'],'revoked_at'=>$row['revoked_at'],
        'ai_allowed'=>(int)$row['ai_allowed'],'redistribution_allowed'=>(int)$row['redistribution_allowed'],
        'status'=>(string)$row['status'],'updated_at'=>(string)$row['updated_at'],
    );
    return array('eligible'=>count($reasons) === 0,
        'rights_revision'=>hash('sha256',v1_canonical_json_encode(
            $revisionPayload,'kind_source_right_revision_encode_failed'
        )),'reasons'=>$reasons);
}

function v1_ops_source_right_eligibility(PDO $pdo, array $config): void {
    $sourceRightId = isset($_GET['source_right_id']) ? trim((string)$_GET['source_right_id']) : '';
    $use = isset($_GET['use']) ? trim((string)$_GET['use']) : '';
    if ($sourceRightId !== 'official:kind' || $use !== 'ingest') {
        v1_respond(400,array('ok'=>false,'error'=>'unsupported_source_right_eligibility_query'));
    }
    $result = v1_kind_source_right_eligibility($pdo,$config,false);
    if (!$result['eligible']) {
        v1_respond(409,array('ok'=>false,'error'=>'source_right_ineligible','source_right_id'=>$sourceRightId,'use'=>$use,
            'eligible'=>false,'rights_revision'=>$result['rights_revision'],'reasons'=>$result['reasons'],'checked_at'=>gmdate('c')));
    }
    v1_respond(200,array('ok'=>true,'source_right_id'=>$sourceRightId,'use'=>$use,'eligible'=>true,
        'rights_revision'=>$result['rights_revision'],'checked_at'=>gmdate('c')));
}

/** Minimal ops view for allowlisted company/activist official-site connectors. */
function v1_ops_official_site_rights(PDO $pdo, array $config): void {
    $page = v1_list_params(); $now = gmdate('Y-m-d H:i:s');
    $stmt = $pdo->prepare('SELECT source_right_id,source_type,source_key,source_name,permission_scope,evidence_uri,evidence_hash,'
        . 'valid_from,valid_until,ai_allowed,redistribution_allowed,status,updated_at FROM '
        . table_name($config,'source_rights') . ' WHERE source_type IN (\'company_statement\',\'activist_statement\')'
        . ' AND status=\'active\' AND valid_from<=? AND (valid_until IS NULL OR valid_until>?) AND revoked_at IS NULL'
        . ' AND redistribution_allowed=1 AND permission_scope<>\'\' AND (evidence_uri IS NOT NULL OR evidence_hash IS NOT NULL)'
        . ' ORDER BY source_right_id');
    $stmt->execute(array($now,$now)); $eligible = array();
    foreach ($stmt->fetchAll() as $row) {
        $type = (string)$row['source_type']; $key = (string)$row['source_key'];
        $identityValid = ($type === 'company_statement' && preg_match('/^company-site:[0-9]{8}$/',$key) === 1)
            || ($type === 'activist_statement' && strlen($key) <= 64
                && preg_match('/^activist-site:[A-Za-z0-9_.:\-]+$/',$key) === 1);
        $evidencePresent = trim((string)$row['evidence_uri']) !== ''
            || preg_match('/^[a-f0-9]{64}$/i',(string)$row['evidence_hash']) === 1;
        if (!$identityValid || !$evidencePresent) { continue; }
        $revisionPayload = array(
            'source_right_id'=>(string)$row['source_right_id'],'source_type'=>$type,'source_key'=>$key,
            'permission_scope_sha256'=>hash('sha256',(string)$row['permission_scope']),'evidence_present'=>true,
            'valid_from'=>(string)$row['valid_from'],'valid_until'=>$row['valid_until'],
            'ai_allowed'=>(int)$row['ai_allowed'],'redistribution_allowed'=>(int)$row['redistribution_allowed'],
            'status'=>(string)$row['status'],'updated_at'=>(string)$row['updated_at'],
        );
        $eligible[] = array(
            'source_right_id'=>(string)$row['source_right_id'],'source_type'=>$type,'source_key'=>$key,
            'source_name'=>(string)$row['source_name'],
            'permission_scope'=>mb_substr((string)$row['permission_scope'],0,2000,'UTF-8'),
            'valid_from'=>v1_release_iso_time($row['valid_from']),'valid_until'=>v1_release_iso_time($row['valid_until']),
            'ai_allowed'=>(int)$row['ai_allowed'] === 1,'redistribution_allowed'=>true,'status'=>'active',
            'evidence_present'=>true,'rights_revision'=>hash('sha256',v1_canonical_json_encode(
                $revisionPayload,'official_site_right_revision_encode_failed'
            )),
        );
    }
    $offset = (int)$page['offset']; $slice = array_slice($eligible,$offset,(int)$page['limit']+1);
    $hasMore = count($slice) > (int)$page['limit']; if ($hasMore) { $slice = array_slice($slice,0,(int)$page['limit']); }
    v1_respond(200,array('ok'=>true,'checked_at'=>gmdate('c'),'data'=>$slice,
        'pagination'=>v1_page_meta($page,count($slice),$hasMore)));
}

function v1_dart_quota_server_day(): string {
    return (new DateTimeImmutable('now',new DateTimeZone('Asia/Seoul')))->format('Y-m-d');
}

function v1_dart_quota_block_until_utc(string $quotaDay): string {
    $kst = new DateTimeZone('Asia/Seoul');
    return (new DateTimeImmutable($quotaDay . ' 00:00:00',$kst))->modify('+1 day')
        ->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d H:i:s');
}

function v1_dart_quota_error(
    int $status,
    string $code,
    ?string $detail = null,
    array $safeContext = array()
): void {
    $error = array('code'=>$code);
    if ($detail !== null) { $error['detail']=$detail; }
    foreach ($safeContext as $key=>$value) {
        if (!in_array($key,array('credential_id','credential_reason'),true)) {
            continue;
        }
        $error[$key]=$value;
    }
    v1_respond($status,array(
        'ok'=>false,
        'error'=>$error,
        'server_kst_date'=>v1_dart_quota_server_day(),
    ));
}

/**
 * Execute a native-PDO lookup, fetch at most one row, and release its cursor.
 *
 * This helper is deliberately neutral rather than DART-quota specific because
 * guarded governance writes also run on hosts where PDO MySQL is unbuffered.
 * Leaving even a one-row result pending makes the next statement (and
 * sometimes rollback itself) fail with CR_COMMANDS_OUT_OF_SYNC / driver 2014.
 *
 * @return array|false
 */
function v1_pdo_fetch_one_and_close(PDOStatement $statement, array $params) {
    try {
        if ($statement->execute($params) !== true) {
            throw new RuntimeException('pdo_query_execute_failed');
        }
        $row = $statement->fetch();
    } catch (Throwable $queryError) {
        try {
            $closed = $statement->closeCursor();
        } catch (Throwable $cursorError) {
            throw new RuntimeException('pdo_cursor_close_threw',0,$cursorError);
        }
        if ($closed !== true) {
            throw new RuntimeException('pdo_cursor_close_returned_false');
        }
        throw $queryError;
    }
    try {
        $closed = $statement->closeCursor();
    } catch (Throwable $cursorError) {
        throw new RuntimeException('pdo_cursor_close_threw',0,$cursorError);
    }
    if ($closed !== true) {
        throw new RuntimeException('pdo_cursor_close_returned_false');
    }
    return $row;
}

/** @return mixed */
function v1_pdo_fetch_column_and_close(PDOStatement $statement, array $params) {
    try {
        if ($statement->execute($params) !== true) {
            throw new RuntimeException('pdo_query_execute_failed');
        }
        $value = $statement->fetchColumn();
    } catch (Throwable $queryError) {
        try {
            $closed = $statement->closeCursor();
        } catch (Throwable $cursorError) {
            throw new RuntimeException('pdo_cursor_close_threw',0,$cursorError);
        }
        if ($closed !== true) {
            throw new RuntimeException('pdo_cursor_close_returned_false');
        }
        throw $queryError;
    }
    try {
        $closed = $statement->closeCursor();
    } catch (Throwable $cursorError) {
        throw new RuntimeException('pdo_cursor_close_threw',0,$cursorError);
    }
    if ($closed !== true) {
        throw new RuntimeException('pdo_cursor_close_returned_false');
    }
    return $value;
}

/** @return array<int,array<string,mixed>> */
function v1_pdo_fetch_all_and_close(PDOStatement $statement, array $params): array {
    try {
        if ($statement->execute($params) !== true) {
            throw new RuntimeException('pdo_query_execute_failed');
        }
        $rows = $statement->fetchAll();
    } catch (Throwable $queryError) {
        try {
            $closed = $statement->closeCursor();
        } catch (Throwable $cursorError) {
            throw new RuntimeException('pdo_cursor_close_threw',0,$cursorError);
        }
        if ($closed !== true) {
            throw new RuntimeException('pdo_cursor_close_returned_false');
        }
        throw $queryError;
    }
    try {
        $closed = $statement->closeCursor();
    } catch (Throwable $cursorError) {
        throw new RuntimeException('pdo_cursor_close_threw',0,$cursorError);
    }
    if ($closed !== true) {
        throw new RuntimeException('pdo_cursor_close_returned_false');
    }
    return $rows;
}

/**
 * Execute a quota lookup, fetch at most one row, and release its server cursor.
 *
 * Native, unbuffered PDO drivers may otherwise keep a result pending until the
 * statement is destroyed.  A quota mutation must never ACK (or even continue
 * to another statement) when that result cannot be released deterministically.
 *
 * @return array|false
 */
function v1_dart_quota_fetch_one_and_close(PDOStatement $statement, array $params) {
    try {
        if ($statement->execute($params) !== true) {
            throw new RuntimeException('dart_quota_query_execute_failed');
        }
        $row = $statement->fetch();
    } catch (Throwable $queryError) {
        try {
            $closed = $statement->closeCursor();
        } catch (Throwable $cursorError) {
            throw new RuntimeException('dart_quota_cursor_close_threw',0,$cursorError);
        }
        if ($closed !== true) {
            throw new RuntimeException('dart_quota_cursor_close_returned_false');
        }
        throw $queryError;
    }
    try {
        $closed = $statement->closeCursor();
    } catch (Throwable $cursorError) {
        throw new RuntimeException('dart_quota_cursor_close_threw',0,$cursorError);
    }
    if ($closed !== true) {
        throw new RuntimeException('dart_quota_cursor_close_returned_false');
    }
    return $row;
}

/** @return array<int,array<string,mixed>> */
function v1_dart_quota_fetch_all_and_close(PDOStatement $statement, array $params): array {
    try {
        if ($statement->execute($params) !== true) {
            throw new RuntimeException('dart_quota_query_execute_failed');
        }
        $rows = $statement->fetchAll();
    } catch (Throwable $queryError) {
        try {
            $closed = $statement->closeCursor();
        } catch (Throwable $cursorError) {
            throw new RuntimeException('dart_quota_cursor_close_threw',0,$cursorError);
        }
        if ($closed !== true) {
            throw new RuntimeException('dart_quota_cursor_close_returned_false');
        }
        throw $queryError;
    }
    try {
        $closed = $statement->closeCursor();
    } catch (Throwable $cursorError) {
        throw new RuntimeException('dart_quota_cursor_close_threw',0,$cursorError);
    }
    if ($closed !== true) {
        throw new RuntimeException('dart_quota_cursor_close_returned_false');
    }
    return $rows;
}

function v1_backend_binding_id_value(PDO $pdo, array $config): string {
    $bindingLookup = $pdo->prepare(
        'SELECT @@server_uuid AS server_uuid,DATABASE() AS database_name'
    );
    $row = v1_dart_quota_fetch_one_and_close($bindingLookup,array());
    $prefix = isset($config['table_prefix']) ? (string)$config['table_prefix'] : 'activist_';
    if (!is_array($row)
        || preg_match('/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/',
            strtolower((string)($row['server_uuid'] ?? ''))) !== 1
        || trim((string)($row['database_name'] ?? '')) === '' || preg_match('/^[A-Za-z0-9_]+$/',$prefix) !== 1) {
        throw new RuntimeException('backend_binding_unavailable');
    }
    return hash('sha256',"mysql8\n" . strtolower((string)$row['server_uuid']) . "\n"
        . (string)$row['database_name'] . "\n" . $prefix);
}

function v1_backend_binding_id(PDO $pdo, array $config): string {
    try {
        return v1_backend_binding_id_value($pdo,$config);
    } catch (Throwable $e) {
        v1_respond(503,array('ok'=>false,'error'=>'backend_binding_unavailable'));
    }
}

function v1_dart_quota_payload(
    array $row,
    string $action,
    ?string $attemptId,
    bool $duplicate,
    string $backendBindingId,
    ?array $credentialRow = null
): array {
    $limit = (int)$row['limit_count']; $used = (int)$row['used_count'];
    $payload = array(
        'ok'=>true,'action'=>$action,'attempt_id'=>$attemptId,'quota_day'=>(string)$row['quota_day'],
        'accepted'=>$action === 'status' ? 0 : 1,'limit_count'=>$limit,'used_count'=>$used,
        'remaining_count'=>max(0,$limit-$used),'duplicate'=>$duplicate,
        'blocked_until'=>$credentialRow !== null && (int)$credentialRow['blocked'] === 1
            ? v1_release_iso_time($credentialRow['blocked_until']) : null,
        'backend_binding_id'=>$backendBindingId,
    );
    if ($credentialRow !== null) {
        $credentialLimit = (int)$credentialRow['limit_count'];
        $credentialUsed = (int)$credentialRow['used_count'];
        $payload['credential_id']=(string)$credentialRow['credential_id'];
        $payload['credential_status']=(string)$credentialRow['credential_status'];
        $payload['credential_limit_count']=$credentialLimit;
        $payload['credential_used_count']=$credentialUsed;
        $payload['credential_remaining_count']=max(0,$credentialLimit-$credentialUsed);
        $payload['credential_blocked_until']=(int)$credentialRow['blocked'] === 1
            ? v1_release_iso_time($credentialRow['blocked_until']) : null;
    }
    return $payload;
}

/**
 * Commit a DART quota mutation and prove the exact durable state before ACK.
 *
 * A successful PDO call alone is not sufficient for this ledger: the response
 * must be based on a fresh, post-commit read from the same backend binding.
 */
function v1_dart_quota_commit_and_readback(
    PDO $pdo,
    array $config,
    string $action,
    string $attemptId,
    string $quotaDay,
    string $credentialId,
    string $revision,
    string $requestHash,
    array $allowedStatuses,
    string $expectedBackendBindingId
): array {
    $requestHashColumn = $action === 'consume'
        ? 'consume_request_sha256'
        : ($action === 'block_020'
            ? 'block_request_sha256'
            : ($action === 'disable_901' ? 'disable_request_sha256' : ''));
    if ($requestHashColumn === '' || !$pdo->inTransaction()) {
        throw new RuntimeException('dart_quota_commit_precondition_failed');
    }
    try {
        $committed = $pdo->commit();
    } catch (Throwable $commitError) {
        throw new RuntimeException('dart_quota_commit_threw',0,$commitError);
    }
    if ($committed !== true) {
        throw new RuntimeException('dart_quota_commit_returned_false');
    }
    if ($pdo->inTransaction()) {
        throw new RuntimeException('dart_quota_transaction_state_after_commit');
    }

    try {
        $readbackPdo = pdo_conn($config);
    } catch (Throwable $connectionError) {
        throw new RuntimeException(
            'dart_quota_readback_connection_failed',
            0,
            $connectionError
        );
    }
    if ($readbackPdo === $pdo) {
        throw new RuntimeException('dart_quota_readback_connection_not_independent');
    }
    try {
        $readbackBindingId = v1_backend_binding_id_value($readbackPdo,$config);
    } catch (Throwable $bindingError) {
        throw new RuntimeException(
            'dart_quota_readback_backend_failed',
            0,
            $bindingError
        );
    }
    if (!hash_equals($expectedBackendBindingId,$readbackBindingId)) {
        throw new RuntimeException('dart_quota_readback_backend_mismatch');
    }

    try {
        $attemptReadback = $readbackPdo->prepare(
            'SELECT attempt_id,quota_day,credential_id,operation,code_revision,consume_request_sha256,'
            . 'block_request_sha256,disable_request_sha256,status,consumed_units '
            . 'FROM ' . table_name($config,'dart_quota_attempts')
            . ' WHERE attempt_id=? AND quota_day=? AND credential_id=? AND code_revision=? '
            . 'AND consumed_units=1 LIMIT 1'
        );
        $durableAttempt = v1_dart_quota_fetch_one_and_close(
            $attemptReadback,
            array($attemptId,$quotaDay,$credentialId,$revision)
        );
    } catch (Throwable $attemptReadbackError) {
        throw new RuntimeException(
            'dart_quota_attempt_readback_query_failed',
            0,
            $attemptReadbackError
        );
    }
    if (!$durableAttempt
        || !in_array((string)$durableAttempt['status'],$allowedStatuses,true)
        || !is_string($durableAttempt[$requestHashColumn] ?? null)
        || !hash_equals((string)$durableAttempt[$requestHashColumn],$requestHash)) {
        throw new RuntimeException('dart_quota_attempt_readback_failed');
    }

    try {
        $dayReadback = $readbackPdo->prepare(
            'SELECT quota_day,limit_count,used_count,blocked,block_reason,blocked_until,'
            . 'blocked_by_attempt_id,blocked_at,updated_at FROM '
            . table_name($config,'dart_quota_days') . ' WHERE quota_day=? LIMIT 1'
        );
        $durableDay = v1_dart_quota_fetch_one_and_close($dayReadback,array($quotaDay));
    } catch (Throwable $dayReadbackError) {
        throw new RuntimeException(
            'dart_quota_day_readback_query_failed',
            0,
            $dayReadbackError
        );
    }
    if (!$durableDay
        || (int)$durableDay['limit_count'] !== GOV_V1_DART_GLOBAL_DAILY_LIMIT
        || (int)$durableDay['used_count'] < 1
        || (int)$durableDay['used_count'] > GOV_V1_DART_GLOBAL_DAILY_LIMIT) {
        throw new RuntimeException('dart_quota_day_readback_failed');
    }

    try {
        $credentialReadback = $readbackPdo->prepare(
            'SELECT c.credential_id,c.status AS credential_status,c.disable_reason,'
            . 'c.disabled_by_attempt_id,c.disabled_at,cd.quota_day,cd.limit_count,cd.used_count,'
            . 'cd.blocked,cd.block_reason,cd.blocked_until,cd.blocked_by_attempt_id,'
            . 'cd.blocked_at,cd.updated_at FROM '
            . table_name($config,'dart_quota_credentials') . ' c JOIN '
            . table_name($config,'dart_quota_credential_days')
            . ' cd ON cd.credential_id=c.credential_id '
            . 'WHERE c.credential_id=? AND cd.quota_day=? LIMIT 1'
        );
        $durableCredentialDay = v1_dart_quota_fetch_one_and_close(
            $credentialReadback,
            array($credentialId,$quotaDay)
        );
    } catch (Throwable $credentialReadbackError) {
        throw new RuntimeException(
            'dart_quota_credential_readback_query_failed',
            0,
            $credentialReadbackError
        );
    }
    if (!$durableCredentialDay
        || (string)$durableCredentialDay['credential_id'] !== $credentialId
        || (string)$durableCredentialDay['quota_day'] !== $quotaDay
        || (int)$durableCredentialDay['limit_count'] !== GOV_V1_DART_GLOBAL_DAILY_LIMIT
        || (int)$durableCredentialDay['used_count'] < 1
        || (int)$durableCredentialDay['used_count'] > GOV_V1_DART_GLOBAL_DAILY_LIMIT) {
        throw new RuntimeException('dart_quota_credential_day_readback_failed');
    }
    if ($action === 'block_020'
        && ((int)$durableCredentialDay['blocked'] !== 1
            || (string)$durableCredentialDay['block_reason'] !== 'opendart_status_020'
            || !is_string($durableCredentialDay['blocked_by_attempt_id'])
            || !hash_equals($attemptId,(string)$durableCredentialDay['blocked_by_attempt_id'])
            || $durableCredentialDay['blocked_until'] === null)) {
        throw new RuntimeException('dart_quota_credential_block_readback_failed');
    }
    if ($action === 'disable_901'
        && ((string)$durableCredentialDay['credential_status'] !== 'disabled_901'
            || (string)$durableCredentialDay['disable_reason'] !== 'opendart_status_901'
            || !is_string($durableCredentialDay['disabled_by_attempt_id'])
            || !hash_equals($attemptId,(string)$durableCredentialDay['disabled_by_attempt_id'])
            || $durableCredentialDay['disabled_at'] === null)) {
        throw new RuntimeException('dart_quota_credential_disable_readback_failed');
    }
    return array($durableDay,$durableCredentialDay,$durableAttempt);
}

function v1_dart_quota_persistence_phase(Throwable $error): string {
    $message = $error->getMessage();
    if (in_array($message,array(
        'dart_quota_commit_threw',
        'dart_quota_commit_returned_false',
        'dart_quota_transaction_state_after_commit',
    ),true)) {
        return 'transaction_commit_failed';
    }
    if (strpos($message,'dart_quota_readback_connection_') === 0) {
        return 'transaction_readback_connection_failed';
    }
    if ($message === 'backend_binding_unavailable'
        || strpos($message,'dart_quota_readback_backend_') === 0) {
        return 'transaction_readback_binding_failed';
    }
    if (strpos($message,'dart_quota_attempt_readback_') === 0) {
        return 'transaction_readback_attempt_failed';
    }
    if (strpos($message,'dart_quota_day_readback_') === 0) {
        return 'transaction_readback_day_failed';
    }
    if (strpos($message,'dart_quota_credential_') === 0
        && strpos($message,'_readback_') !== false) {
        return 'transaction_readback_credential_failed';
    }
    return 'transaction_state_invalid';
}

function v1_dart_quota_persistence_outcome(Throwable $error): string {
    $candidate = $error;
    for ($depth=0; $depth<5; $depth++) {
        $message = $candidate->getMessage();
        if ($message === 'dart_quota_commit_threw') {
            return 'commit_threw';
        }
        if ($message === 'dart_quota_commit_returned_false') {
            return 'commit_returned_false';
        }
        if ($message === 'dart_quota_transaction_state_after_commit') {
            return 'transaction_state_after_commit';
        }
        if ($message === 'dart_quota_cursor_close_threw') {
            return 'cursor_close_threw';
        }
        if ($message === 'dart_quota_cursor_close_returned_false') {
            return 'cursor_close_returned_false';
        }
        $previous = $candidate->getPrevious();
        if (!$previous instanceof Throwable) {
            break;
        }
        $candidate = $previous;
    }
    return 'persistence_failure';
}

/** @return array{0:string,1:int} */
function v1_dart_quota_sql_diagnostic(Throwable $error): array {
    $candidate = $error;
    for ($depth=0; $depth<5; $depth++) {
        if ($candidate instanceof PDOException
            && isset($candidate->errorInfo)
            && is_array($candidate->errorInfo)) {
            $sqlState = strtoupper((string)($candidate->errorInfo[0] ?? ''));
            $sqlStateClass = preg_match('/^[A-Z0-9]{5}$/D',$sqlState) === 1
                ? substr($sqlState,0,2) : 'NA';
            $rawDriverCode = $candidate->errorInfo[1] ?? 0;
            $driverCode = 0;
            if (is_int($rawDriverCode) && $rawDriverCode >= 0) {
                $driverCode = $rawDriverCode;
            } elseif (is_string($rawDriverCode)
                && preg_match('/^\d{1,10}$/D',$rawDriverCode) === 1) {
                $driverCode = (int)$rawDriverCode;
            }
            return array($sqlStateClass,$driverCode);
        }
        $previous = $candidate->getPrevious();
        if (!$previous instanceof Throwable) {
            break;
        }
        $candidate = $previous;
    }
    return array('NA',0);
}

function v1_dart_quota_log_persistence_failure(Throwable $error, string $phase): void {
    $allowedPhases = array(
        'transaction_commit_failed',
        'transaction_state_invalid',
        'transaction_readback_connection_failed',
        'transaction_readback_binding_failed',
        'transaction_readback_attempt_failed',
        'transaction_readback_day_failed',
        'transaction_readback_credential_failed',
    );
    $safePhase = in_array($phase,$allowedPhases,true)
        ? $phase : 'transaction_state_invalid';
    $safeOutcomes = array(
        'commit_threw',
        'commit_returned_false',
        'transaction_state_after_commit',
        'cursor_close_threw',
        'cursor_close_returned_false',
        'persistence_failure',
    );
    $outcome = v1_dart_quota_persistence_outcome($error);
    $safeOutcome = in_array($outcome,$safeOutcomes,true)
        ? $outcome : 'persistence_failure';
    list($sqlStateClass,$driverCode) = v1_dart_quota_sql_diagnostic($error);
    error_log('[activist-api] dart_quota_persistence_failed phase=' . $safePhase
        . ' outcome=' . $safeOutcome
        . ' sqlstate_class=' . $sqlStateClass
        . ' driver_code=' . $driverCode);
}

function v1_dart_quota_validate_credential_id($value): string {
    $raw = is_string($value) ? $value : '';
    $credentialId = trim($raw);
    if ($raw !== $credentialId
        || preg_match('/^[a-f0-9]{64}$/D',$credentialId) !== 1) {
        v1_dart_quota_error(400,'invalid_request','credential_id');
    }
    return $credentialId;
}

function v1_dart_quota_validate_day($value, bool $allowPreviousDay = false): string {
    $day = is_string($value) ? trim($value) : '';
    $kst = new DateTimeZone('Asia/Seoul');
    $today = new DateTimeImmutable('today',$kst);
    $isAllowedDay = $day === $today->format('Y-m-d')
        || ($allowPreviousDay && $day === $today->modify('-1 day')->format('Y-m-d'));
    if (preg_match('/^\d{4}-\d{2}-\d{2}$/',$day) !== 1 || !$isAllowedDay) {
        v1_dart_quota_error(400,'quota_date_mismatch');
    }
    return $day;
}

/** Read-only current-KST-day quota status for watchdogs and operators. */
function v1_ops_dart_quota_status(PDO $pdo, array $config): void {
    $day = v1_dart_quota_validate_day($_GET['quota_day'] ?? '');
    $backendBindingId = v1_backend_binding_id($pdo,$config);
    $stmt = $pdo->prepare('SELECT quota_day,limit_count,used_count,blocked,block_reason,blocked_until,blocked_by_attempt_id,'
        . 'blocked_at,updated_at FROM ' . table_name($config,'dart_quota_days') . ' WHERE quota_day=? LIMIT 1');
    $row = v1_dart_quota_fetch_one_and_close($stmt,array($day));
    if (!$row) {
        $row = array('quota_day'=>$day,'limit_count'=>GOV_V1_DART_GLOBAL_DAILY_LIMIT,'used_count'=>0,'blocked'=>0,'block_reason'=>null,
            'blocked_until'=>null,'blocked_by_attempt_id'=>null,'blocked_at'=>null,'updated_at'=>null);
    }
    $payload = v1_dart_quota_payload($row,'status',null,false,$backendBindingId);
    $payload['server_kst_date'] = v1_dart_quota_server_day();
    $payload['block_reason'] = $row['block_reason'];
    $payload['blocked_by_attempt_id'] = $row['blocked_by_attempt_id'];
    $payload['blocked_at'] = v1_release_iso_time($row['blocked_at']);
    $payload['updated_at'] = v1_release_iso_time($row['updated_at']);
    $credentialStmt = $pdo->prepare(
        'SELECT c.credential_id,c.status AS credential_status,c.disable_reason,'
        . 'c.disabled_by_attempt_id,c.disabled_at,c.updated_at AS credential_updated_at,'
        . 'COALESCE(cd.limit_count,?) AS limit_count,COALESCE(cd.used_count,0) AS used_count,'
        . 'COALESCE(cd.blocked,0) AS blocked,cd.block_reason,cd.blocked_until,'
        . 'cd.blocked_by_attempt_id,cd.blocked_at,cd.updated_at '
        . 'FROM ' . table_name($config,'dart_quota_credentials') . ' c '
        . 'LEFT JOIN ' . table_name($config,'dart_quota_credential_days')
        . ' cd ON cd.credential_id=c.credential_id AND cd.quota_day=? '
        . 'ORDER BY c.credential_id'
    );
    $credentials = array();
    foreach (v1_dart_quota_fetch_all_and_close(
        $credentialStmt,
        array(GOV_V1_DART_GLOBAL_DAILY_LIMIT,$day)
    ) as $credential) {
        $credentialLimit=(int)$credential['limit_count'];
        $credentialUsed=(int)$credential['used_count'];
        $credentials[]=array(
            'credential_id'=>(string)$credential['credential_id'],
            'status'=>(string)$credential['credential_status'],
            'limit_count'=>$credentialLimit,
            'used_count'=>$credentialUsed,
            'remaining_count'=>max(0,$credentialLimit-$credentialUsed),
            'blocked'=>(int)$credential['blocked'] === 1,
            'block_reason'=>$credential['block_reason'],
            'blocked_until'=>v1_release_iso_time($credential['blocked_until']),
            'blocked_by_attempt_id'=>$credential['blocked_by_attempt_id'],
            'blocked_at'=>v1_release_iso_time($credential['blocked_at']),
            'disable_reason'=>$credential['disable_reason'],
            'disabled_by_attempt_id'=>$credential['disabled_by_attempt_id'],
            'disabled_at'=>v1_release_iso_time($credential['disabled_at']),
            'updated_at'=>v1_release_iso_time(
                $credential['updated_at'] ?? $credential['credential_updated_at']
            ),
        );
    }
    $payload['credentials']=$credentials;
    v1_respond(200,$payload);
}

/** Atomically consume one physical DART HTTP attempt or record a 020 block. */
function v1_ops_dart_quota_write(PDO $pdo, array $config): void {
    $payload = v1_admin_json_body($config);
    $action = isset($payload['action']) ? trim((string)$payload['action']) : '';
    $required = $action === 'consume'
        ? array('action','attempt_id','quota_day','credential_id','operation','code_revision','expected_backend_binding_id')
        : ($action === 'block_020'
            ? array('action','attempt_id','quota_day','credential_id','reason','code_revision','expected_backend_binding_id')
            : ($action === 'disable_901'
                ? array('action','attempt_id','quota_day','credential_id','reason','code_revision','expected_backend_binding_id')
                : array()));
    $keys = array_keys($payload); sort($keys,SORT_STRING); $expectedKeys = $required; sort($expectedKeys,SORT_STRING);
    if (!$required || $keys !== $expectedKeys) { v1_dart_quota_error(400,'invalid_request','exact_fields_required'); }
    $attemptId = trim((string)$payload['attempt_id']);
    $quotaDay = v1_dart_quota_validate_day(
        $payload['quota_day'],
        $action === 'block_020' || $action === 'disable_901'
    );
    $credentialId = v1_dart_quota_validate_credential_id($payload['credential_id']);
    $revision = strtolower(trim((string)$payload['code_revision']));
    $expectedBackendBindingId = trim((string)$payload['expected_backend_binding_id']);
    if (preg_match('/^[a-f0-9]{64}$/',$expectedBackendBindingId) !== 1) {
        v1_dart_quota_error(400,'backend_binding_required');
    }
    $backendBindingId = v1_backend_binding_id($pdo,$config);
    if (!hash_equals($backendBindingId,$expectedBackendBindingId)) {
        v1_dart_quota_error(409,'backend_binding_mismatch');
    }
    if (!v1_valid_entity_id($attemptId,96) || preg_match('/^[a-f0-9]{7,40}$/',$revision) !== 1) {
        v1_dart_quota_error(400,'invalid_request','attempt_or_revision');
    }
    if ($action === 'consume' && !in_array($payload['operation'],array('list','corp_code'),true)) {
        v1_dart_quota_error(400,'invalid_request','operation');
    }
    if ($action === 'block_020' && $payload['reason'] !== 'opendart_status_020') {
        v1_dart_quota_error(400,'invalid_request','reason');
    }
    if ($action === 'disable_901' && $payload['reason'] !== 'opendart_status_901') {
        v1_dart_quota_error(400,'invalid_request','reason');
    }
    $requestHash = hash('sha256',v1_strict_canonical_json_encode($payload,'dart_quota_request_encode_failed'));
    $now = gmdate('Y-m-d H:i:s'); $blockUntil = v1_dart_quota_block_until_utc($quotaDay);
    try {
        if ($pdo->beginTransaction() !== true || !$pdo->inTransaction()) {
            throw new RuntimeException('dart_quota_transaction_begin_failed');
        }
        $dayInsert = $pdo->prepare('INSERT IGNORE INTO ' . table_name($config,'dart_quota_days')
            . ' (quota_day,limit_count,used_count,blocked,block_reason,blocked_until,blocked_by_attempt_id,blocked_at,created_at,updated_at)'
            . ' VALUES (?,40000,0,0,NULL,NULL,NULL,NULL,?,?)');
        $dayInsert->execute(array($quotaDay,$now,$now));
        $dayLookup = $pdo->prepare('SELECT quota_day,limit_count,used_count,blocked,block_reason,blocked_until,'
            . 'blocked_by_attempt_id,blocked_at,updated_at FROM ' . table_name($config,'dart_quota_days') . ' WHERE quota_day=? FOR UPDATE');
        $day = v1_dart_quota_fetch_one_and_close($dayLookup,array($quotaDay));
        if (!$day || (int)$day['limit_count'] !== GOV_V1_DART_GLOBAL_DAILY_LIMIT
            || (int)$day['used_count'] > GOV_V1_DART_GLOBAL_DAILY_LIMIT
            || (int)$day['blocked'] !== 0 || $day['block_reason'] !== null
            || $day['blocked_until'] !== null || $day['blocked_by_attempt_id'] !== null
            || $day['blocked_at'] !== null) {
            throw new RuntimeException('dart_quota_day_integrity_error');
        }

        $credentialInsert = $pdo->prepare(
            'INSERT IGNORE INTO ' . table_name($config,'dart_quota_credentials')
            . ' (credential_id,status,disable_reason,disabled_by_attempt_id,disabled_at,created_at,updated_at)'
            . ' VALUES (?,\'active\',NULL,NULL,NULL,?,?)'
        );
        $credentialInsert->execute(array($credentialId,$now,$now));
        $credentialLookup = $pdo->prepare(
            'SELECT credential_id,status,disable_reason,disabled_by_attempt_id,disabled_at,updated_at '
            . 'FROM ' . table_name($config,'dart_quota_credentials')
            . ' WHERE credential_id=? FOR UPDATE'
        );
        $credential = v1_dart_quota_fetch_one_and_close(
            $credentialLookup,
            array($credentialId)
        );
        if (!$credential || !in_array((string)$credential['status'],array('active','disabled_901'),true)) {
            throw new RuntimeException('dart_quota_credential_integrity_error');
        }
        $credentialDayInsert = $pdo->prepare(
            'INSERT IGNORE INTO ' . table_name($config,'dart_quota_credential_days')
            . ' (quota_day,credential_id,limit_count,used_count,blocked,block_reason,blocked_until,'
            . 'blocked_by_attempt_id,blocked_at,created_at,updated_at)'
            . ' VALUES (?,?,40000,0,0,NULL,NULL,NULL,NULL,?,?)'
        );
        $credentialDayInsert->execute(array($quotaDay,$credentialId,$now,$now));
        $credentialDayLookup = $pdo->prepare(
            'SELECT quota_day,credential_id,limit_count,used_count,blocked,block_reason,blocked_until,'
            . 'blocked_by_attempt_id,blocked_at,updated_at FROM '
            . table_name($config,'dart_quota_credential_days')
            . ' WHERE quota_day=? AND credential_id=? FOR UPDATE'
        );
        $credentialDay = v1_dart_quota_fetch_one_and_close(
            $credentialDayLookup,
            array($quotaDay,$credentialId)
        );
        if (!$credentialDay
            || (int)$credentialDay['limit_count'] !== GOV_V1_DART_GLOBAL_DAILY_LIMIT
            || (int)$credentialDay['used_count'] > GOV_V1_DART_GLOBAL_DAILY_LIMIT) {
            throw new RuntimeException('dart_quota_credential_day_integrity_error');
        }
        $credentialDay['credential_status']=(string)$credential['status'];

        $attemptLookup = $pdo->prepare('SELECT attempt_id,quota_day,credential_id,operation,code_revision,consume_request_sha256,'
            . 'block_request_sha256,disable_request_sha256,status,consumed_units FROM ' . table_name($config,'dart_quota_attempts')
            . ' WHERE attempt_id=? FOR UPDATE');
        $attempt = v1_dart_quota_fetch_one_and_close($attemptLookup,array($attemptId));

        if ($action === 'consume') {
            if ($attempt) {
                if ((string)$attempt['quota_day'] !== $quotaDay
                    || (string)$attempt['credential_id'] !== $credentialId
                    || (string)$attempt['operation'] !== (string)$payload['operation']
                    || (string)$attempt['code_revision'] !== $revision || (int)$attempt['consumed_units'] !== 1
                    || !hash_equals((string)$attempt['consume_request_sha256'],$requestHash)) {
                    $pdo->rollBack(); v1_dart_quota_error(409,'dart_quota_idempotency_conflict');
                }
                $attemptStatus = (string)$attempt['status'];
                if (!in_array($attemptStatus,array('consumed','blocked_020','disabled_901'),true)) {
                    throw new RuntimeException('dart_quota_attempt_status_integrity_error');
                }
                list($day,$credentialDay) = v1_dart_quota_commit_and_readback(
                    $pdo,$config,'consume',$attemptId,$quotaDay,$credentialId,$revision,
                    $requestHash,array($attemptStatus),$backendBindingId
                );
                v1_respond(200,v1_dart_quota_payload(
                    $day,'consume',$attemptId,true,$backendBindingId,$credentialDay
                ));
            }
            if ((string)$credential['status'] === 'disabled_901') {
                $pdo->rollBack(); v1_dart_quota_error(
                    409,'dart_credential_disabled',null,
                    array('credential_id'=>$credentialId,'credential_reason'=>'disabled_901')
                );
            }
            if ((int)$credentialDay['blocked'] === 1) {
                $retry = max(1,(int)(strtotime((string)$credentialDay['blocked_until'] . ' UTC')-time()));
                header('Retry-After: ' . $retry); $pdo->rollBack(); v1_dart_quota_error(
                    409,'dart_credential_blocked',null,
                    array('credential_id'=>$credentialId,'credential_reason'=>'blocked_020')
                );
            }
            if ((int)$day['used_count'] >= GOV_V1_DART_GLOBAL_DAILY_LIMIT) {
                $retry = max(1,(int)(strtotime($blockUntil . ' UTC')-time()));
                header('Retry-After: ' . $retry); $pdo->rollBack(); v1_dart_quota_error(429,'dart_quota_exhausted');
            }
            $insert = $pdo->prepare('INSERT INTO ' . table_name($config,'dart_quota_attempts')
                . ' (attempt_id,quota_day,credential_id,operation,code_revision,consume_request_sha256,'
                . 'block_request_sha256,disable_request_sha256,status,consumed_units,consumed_at,blocked_at,disabled_at,updated_at)'
                . ' VALUES (?,?,?,?,?,?,NULL,NULL,\'consumed\',1,?,NULL,NULL,?)');
            $insert->execute(array(
                $attemptId,$quotaDay,$credentialId,(string)$payload['operation'],
                $revision,$requestHash,$now,$now,
            ));
            $update = $pdo->prepare('UPDATE ' . table_name($config,'dart_quota_days')
                . ' SET used_count=used_count+1,updated_at=? WHERE quota_day=? AND used_count<limit_count');
            $update->execute(array($now,$quotaDay));
            if ($update->rowCount() !== 1) { throw new RuntimeException('dart_quota_atomic_consume_failed'); }
            $credentialDayUpdate = $pdo->prepare(
                'UPDATE ' . table_name($config,'dart_quota_credential_days')
                . ' SET used_count=used_count+1,updated_at=? '
                . 'WHERE quota_day=? AND credential_id=? AND blocked=0 AND used_count<limit_count'
            );
            $credentialDayUpdate->execute(array($now,$quotaDay,$credentialId));
            if ($credentialDayUpdate->rowCount() !== 1) {
                throw new RuntimeException('dart_quota_credential_atomic_consume_failed');
            }
            $day = v1_dart_quota_fetch_one_and_close($dayLookup,array($quotaDay));
            $credentialDay = v1_dart_quota_fetch_one_and_close(
                $credentialDayLookup,
                array($quotaDay,$credentialId)
            );
            $credentialDay['credential_status']=(string)$credential['status'];
            list($day,$credentialDay) = v1_dart_quota_commit_and_readback(
                $pdo,$config,'consume',$attemptId,$quotaDay,$credentialId,$revision,
                $requestHash,array('consumed','blocked_020','disabled_901'),$backendBindingId
            );
            v1_respond(200,v1_dart_quota_payload(
                $day,'consume',$attemptId,false,$backendBindingId,$credentialDay
            ));
        }

        if (!$attempt || (string)$attempt['quota_day'] !== $quotaDay
            || (string)$attempt['credential_id'] !== $credentialId
            || (string)$attempt['code_revision'] !== $revision
            || (int)$attempt['consumed_units'] !== 1) {
            $pdo->rollBack(); v1_dart_quota_error(409,'invalid_request','consumed_attempt_required');
        }

        if ($action === 'block_020') {
            if ($attempt['block_request_sha256'] !== null) {
                if (!hash_equals((string)$attempt['block_request_sha256'],$requestHash)) {
                    $pdo->rollBack(); v1_dart_quota_error(409,'dart_quota_idempotency_conflict');
                }
                if ((int)$credentialDay['blocked'] !== 1 || $credentialDay['blocked_until'] === null) {
                    throw new RuntimeException('dart_quota_credential_block_integrity_error');
                }
                list($day,$credentialDay) = v1_dart_quota_commit_and_readback(
                    $pdo,$config,'block_020',$attemptId,$quotaDay,$credentialId,$revision,
                    $requestHash,array('blocked_020','disabled_901'),$backendBindingId
                );
                v1_respond(200,v1_dart_quota_payload(
                    $day,'block_020',$attemptId,true,$backendBindingId,$credentialDay
                ));
            }
            $attemptUpdate = $pdo->prepare('UPDATE ' . table_name($config,'dart_quota_attempts')
                . ' SET block_request_sha256=?,status=\'blocked_020\',blocked_at=?,updated_at=? '
                . 'WHERE attempt_id=? AND block_request_sha256 IS NULL');
            $attemptUpdate->execute(array($requestHash,$now,$now,$attemptId));
            if ($attemptUpdate->rowCount() !== 1) { throw new RuntimeException('dart_quota_atomic_block_failed'); }
            $credentialDayUpdate = $pdo->prepare(
                'UPDATE ' . table_name($config,'dart_quota_credential_days')
                . ' SET blocked=1,block_reason=COALESCE(block_reason,\'opendart_status_020\'),'
                . 'blocked_until=COALESCE(blocked_until,?),'
                . 'blocked_by_attempt_id=COALESCE(blocked_by_attempt_id,?),'
                . 'blocked_at=COALESCE(blocked_at,?),updated_at=? '
                . 'WHERE quota_day=? AND credential_id=?'
            );
            $credentialDayUpdate->execute(array(
                $blockUntil,$attemptId,$now,$now,$quotaDay,$credentialId,
            ));
            if ($credentialDayUpdate->rowCount() > 1) {
                throw new RuntimeException('dart_quota_credential_atomic_block_failed');
            }
            $credentialDay = v1_dart_quota_fetch_one_and_close(
                $credentialDayLookup,
                array($quotaDay,$credentialId)
            );
            $credentialDay['credential_status']=(string)$credential['status'];
            list($day,$credentialDay) = v1_dart_quota_commit_and_readback(
                $pdo,$config,'block_020',$attemptId,$quotaDay,$credentialId,$revision,
                $requestHash,array('blocked_020','disabled_901'),$backendBindingId
            );
            v1_respond(200,v1_dart_quota_payload(
                $day,'block_020',$attemptId,false,$backendBindingId,$credentialDay
            ));
        }

        if ($attempt['disable_request_sha256'] !== null) {
            if (!hash_equals((string)$attempt['disable_request_sha256'],$requestHash)) {
                $pdo->rollBack(); v1_dart_quota_error(409,'dart_quota_idempotency_conflict');
            }
            if ((string)$credential['status'] !== 'disabled_901') {
                throw new RuntimeException('dart_quota_credential_disable_integrity_error');
            }
            $credentialDay['credential_status']='disabled_901';
            list($day,$credentialDay) = v1_dart_quota_commit_and_readback(
                $pdo,$config,'disable_901',$attemptId,$quotaDay,$credentialId,$revision,
                $requestHash,array('disabled_901'),$backendBindingId
            );
            v1_respond(200,v1_dart_quota_payload(
                $day,'disable_901',$attemptId,true,$backendBindingId,$credentialDay
            ));
        }
        $attemptUpdate = $pdo->prepare(
            'UPDATE ' . table_name($config,'dart_quota_attempts')
            . ' SET disable_request_sha256=?,status=\'disabled_901\',disabled_at=?,updated_at=? '
            . 'WHERE attempt_id=? AND disable_request_sha256 IS NULL'
        );
        $attemptUpdate->execute(array($requestHash,$now,$now,$attemptId));
        if ($attemptUpdate->rowCount() !== 1) {
            throw new RuntimeException('dart_quota_atomic_disable_failed');
        }
        $credentialUpdate = $pdo->prepare(
            'UPDATE ' . table_name($config,'dart_quota_credentials')
            . ' SET status=\'disabled_901\','
            . 'disable_reason=COALESCE(disable_reason,\'opendart_status_901\'),'
            . 'disabled_by_attempt_id=COALESCE(disabled_by_attempt_id,?),'
            . 'disabled_at=COALESCE(disabled_at,?),updated_at=? '
            . 'WHERE credential_id=? AND status IN (\'active\',\'disabled_901\')'
        );
        $credentialUpdate->execute(array($attemptId,$now,$now,$credentialId));
        if ($credentialUpdate->rowCount() > 1) {
            throw new RuntimeException('dart_quota_credential_atomic_disable_failed');
        }
        $credential = v1_dart_quota_fetch_one_and_close(
            $credentialLookup,
            array($credentialId)
        );
        if (!$credential || (string)$credential['status'] !== 'disabled_901') {
            throw new RuntimeException('dart_quota_credential_disable_integrity_error');
        }
        $credentialDay['credential_status']='disabled_901';
        list($day,$credentialDay) = v1_dart_quota_commit_and_readback(
            $pdo,$config,'disable_901',$attemptId,$quotaDay,$credentialId,$revision,
            $requestHash,array('disabled_901'),$backendBindingId
        );
        v1_respond(200,v1_dart_quota_payload(
            $day,'disable_901',$attemptId,false,$backendBindingId,$credentialDay
        ));
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) {
            try { $pdo->rollBack(); }
            catch (Throwable $rollbackError) {
                list($rollbackSqlStateClass,$rollbackDriverCode) =
                    v1_dart_quota_sql_diagnostic($rollbackError);
                error_log('[activist-api] dart_quota_rollback_failed'
                    . ' outcome=rollback_failed'
                    . ' sqlstate_class=' . $rollbackSqlStateClass
                    . ' driver_code=' . $rollbackDriverCode);
            }
        }
        $phase = v1_dart_quota_persistence_phase($e);
        v1_dart_quota_log_persistence_failure($e,$phase);
        v1_dart_quota_error(503,'dart_quota_persistence_failed',$phase);
    }
}

/** Candidate metadata for the private official-site connector allowlist job. */
function v1_ops_official_site_candidates(PDO $pdo, array $config): void {
    $officialEvidence = 'EXISTS (SELECT 1 FROM ' . table_name($config,'event_documents') . ' candidate_ed '
        . 'JOIN ' . table_name($config,'documents') . ' candidate_d ON candidate_d.document_id=candidate_ed.document_id '
        . 'LEFT JOIN ' . table_name($config,'source_rights') . ' candidate_sr ON candidate_sr.source_right_id=candidate_d.source_right_id '
        . 'WHERE candidate_ed.event_id=e.event_id AND candidate_d.source_class=\'official_disclosure\' AND '
        . v1_document_visibility_sql('candidate_d','candidate_sr') . ')';
    $eligibleEvent = '(e.review_status IN (\'pending\',\'changes_requested\',\'approved\',\'not_required\') '
        . 'AND e.publication_status IN (\'draft\',\'published\') AND e.verification_status NOT IN (\'signal\',\'withdrawn\') '
        . 'AND ' . $officialEvidence . ')';
    $weight = '(CASE e.importance WHEN \'critical\' THEN 100 WHEN \'market_sensitive\' THEN 90 '
        . 'WHEN \'high\' THEN 80 WHEN \'medium\' THEN 40 WHEN \'low\' THEN 20 ELSE 10 END)';

    $companySql = 'SELECT e.company_id,c.legal_name AS company_name,COUNT(DISTINCT e.event_id) AS event_count,'
        . 'SUM(' . $weight . ') AS raw_score,MAX(e.occurred_at) AS latest_event_at '
        . 'FROM ' . table_name($config,'governance_events') . ' e '
        . 'JOIN ' . table_name($config,'companies') . ' c ON c.company_id=e.company_id '
        . 'WHERE c.record_status=\'active\' AND ' . $eligibleEvent . ' GROUP BY e.company_id,c.legal_name '
        . 'ORDER BY raw_score DESC,event_count DESC,e.company_id ASC LIMIT 20';
    $companyRows = $pdo->query($companySql)->fetchAll();

    $actorPairs = '(SELECT candidate_ea.event_id,candidate_ea.actor_id FROM ' . table_name($config,'event_actors')
        . ' candidate_ea WHERE candidate_ea.review_status=\'approved\' UNION '
        . 'SELECT candidate_identity.event_id,candidate_identity.identity_actor_id FROM ' . table_name($config,'governance_events')
        . ' candidate_identity WHERE candidate_identity.identity_actor_id IS NOT NULL)';
    $actorSql = 'SELECT a.actor_id,a.display_name AS actor_name,a.actor_type,COUNT(DISTINCT e.event_id) AS event_count,'
        . 'SUM(' . $weight . ') AS raw_score,MAX(e.occurred_at) AS latest_event_at FROM ' . $actorPairs . ' candidate_pair '
        . 'JOIN ' . table_name($config,'governance_events') . ' e ON e.event_id=candidate_pair.event_id '
        . 'JOIN ' . table_name($config,'actors') . ' a ON a.actor_id=candidate_pair.actor_id '
        . 'WHERE a.review_status=\'approved\' AND a.record_status=\'active\' '
        . 'AND a.actor_type IN (\'activist_shareholder\',\'institution\',\'shareholder_coalition\') AND '
        . $eligibleEvent . ' GROUP BY a.actor_id,a.display_name,a.actor_type '
        . 'ORDER BY raw_score DESC,event_count DESC,a.actor_id ASC LIMIT 10';
    $actorRows = $pdo->query($actorSql)->fetchAll();

    $rank = 0;
    foreach ($companyRows as &$row) {
        $row['rank'] = ++$rank; $row['event_count'] = (int)$row['event_count']; $row['raw_score'] = (int)$row['raw_score'];
        $row['latest_event_at'] = v1_release_iso_time($row['latest_event_at']);
    }
    unset($row); $rank = 0;
    foreach ($actorRows as &$row) {
        $row['rank'] = ++$rank; $row['event_count'] = (int)$row['event_count']; $row['raw_score'] = (int)$row['raw_score'];
        $row['latest_event_at'] = v1_release_iso_time($row['latest_event_at']);
    }
    unset($row);
    v1_respond(200,array(
        'ok'=>true,
        'generated_at'=>gmdate('c'),
        'score_version'=>'official-site-candidates-v1',
        'selection'=>array('companies_limit'=>20,'actors_limit'=>10,'official_evidence_required'=>true),
        'companies'=>$companyRows,
        'actors'=>$actorRows,
    ));
}

function v1_admin_review_queue(PDO $pdo, array $config): void {
    $page = v1_list_params();
    $sql = 'SELECT review_items.*, c.legal_name AS company_name FROM ('
        . 'SELECT \'event\' AS entity_type, e.event_id AS entity_id, e.company_id, e.title, e.importance, e.review_status, e.created_at, e.updated_at, '
        . 'e.event_id, NULL AS actor_id, NULL AS actor_role FROM ' . table_name($config, 'governance_events') . ' e WHERE e.review_status IN (\'pending\',\'changes_requested\') '
        . 'UNION ALL SELECT \'actor\', a.actor_id, a.company_id, a.display_name, \'medium\', a.review_status, a.created_at, a.updated_at, '
        . 'NULL, a.actor_id, NULL FROM ' . table_name($config, 'actors') . ' a WHERE a.review_status IN (\'pending\',\'changes_requested\') '
        . 'UNION ALL SELECT \'event_actor\', CONCAT(ea.event_id,\':\',ea.actor_id,\':\',ea.actor_role), e.company_id, '
        . 'CONCAT(a.display_name,\' — \',ea.actor_role), e.importance, ea.review_status, ea.created_at, ea.updated_at, ea.event_id, ea.actor_id, ea.actor_role '
        . 'FROM ' . table_name($config, 'event_actors') . ' ea JOIN ' . table_name($config, 'governance_events') . ' e ON e.event_id=ea.event_id '
        . 'JOIN ' . table_name($config, 'actors') . ' a ON a.actor_id=ea.actor_id WHERE ea.review_status IN (\'pending\',\'changes_requested\') '
        . 'UNION ALL SELECT \'campaign\', cp.campaign_id, cp.company_id, cp.title, \'high\', cp.review_status, cp.created_at, cp.updated_at, '
        . 'NULL, cp.lead_actor_id, NULL FROM ' . table_name($config, 'campaigns') . ' cp WHERE cp.review_status IN (\'pending\',\'changes_requested\') '
        . 'UNION ALL SELECT \'claim\', ce.claim_id, e.company_id, LEFT(ce.claim_text,700), \'medium\', ce.editorial_status, ce.created_at, ce.updated_at, '
        . 'ce.event_id, ce.actor_id, NULL FROM ' . table_name($config, 'claim_evidence') . ' ce JOIN ' . table_name($config, 'governance_events') . ' e ON e.event_id=ce.event_id '
        . 'WHERE ce.editorial_status IN (\'pending\',\'changes_requested\') '
        . 'UNION ALL SELECT \'proposal_vote\', v.proposal_vote_id, v.company_id, v.agenda_title, \'high\', v.review_status, v.created_at, v.updated_at, '
        . 'v.event_id, v.proposer_actor_id, NULL FROM ' . table_name($config, 'proposal_votes') . ' v WHERE v.review_status IN (\'pending\',\'changes_requested\') '
        . 'UNION ALL SELECT \'commitment\', co.commitment_id, co.company_id, LEFT(co.commitment_text,700), \'medium\', co.review_status, co.created_at, co.updated_at, '
        . 'co.event_id, NULL, NULL FROM ' . table_name($config, 'commitment_outcomes') . ' co WHERE co.review_status IN (\'pending\',\'changes_requested\') '
        . 'UNION ALL SELECT \'timeline\', tl.timeline_entry_id, COALESCE(e.company_id,cp.company_id), tl.title, \'medium\', tl.review_status, tl.created_at, tl.updated_at, '
        . 'tl.event_id, NULL, NULL FROM ' . table_name($config, 'timeline_entries') . ' tl '
        . 'LEFT JOIN ' . table_name($config, 'governance_events') . ' e ON e.event_id=tl.event_id LEFT JOIN ' . table_name($config, 'campaigns') . ' cp ON cp.campaign_id=tl.campaign_id '
        . 'WHERE tl.review_status IN (\'pending\',\'changes_requested\') '
        . 'UNION ALL SELECT \'official_site_review\', osr.review_item_id, IF(osr.entity_type=\'company\',osr.entity_id,NULL), '
        . 'LEFT(osr.reason,700), \'medium\', osr.review_status, osr.created_at, osr.updated_at, NULL, NULL, NULL FROM '
        . table_name($config,'official_site_review_items') . ' osr WHERE osr.review_status IN (\'pending\',\'changes_requested\') '
        . 'UNION ALL SELECT \'official_site_tombstone\', ost.tombstone_id, IF(ost.entity_type=\'company\',ost.entity_id,NULL), '
        . 'CONCAT(\'Delete signal: \',LEFT(ost.external_id,620)), \'high\', ost.review_status, ost.created_at, ost.updated_at, NULL, NULL, NULL FROM '
        . table_name($config,'official_site_tombstones') . ' ost WHERE ost.review_status IN (\'pending\',\'changes_requested\')'
        . ') review_items LEFT JOIN ' . table_name($config, 'companies') . ' c ON c.company_id=review_items.company_id ORDER BY '
        . 'CASE review_items.importance WHEN \'critical\' THEN 0 WHEN \'market_sensitive\' THEN 0 WHEN \'high\' THEN 1 ELSE 2 END, '
        . 'review_items.created_at ASC, review_items.entity_type ASC, review_items.entity_id ASC LIMIT ' . ((int)$page['limit'] + 1) . ' OFFSET ' . (int)$page['offset'];
    $stmt = $pdo->prepare($sql);
    $stmt->execute();
    list($rows, $hasMore) = v1_fetch_page($stmt, $page);
    foreach ($rows as &$row) {
        foreach (array('created_at','updated_at') as $field) {
            if (isset($row[$field]) && is_string($row[$field]) && preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $row[$field])) {
                $row[$field] = str_replace(' ', 'T', $row[$field]) . 'Z';
            }
        }
    }
    unset($row);
    v1_respond(200, array('ok' => true, 'data' => $rows, 'pagination' => v1_page_meta($page, count($rows), $hasMore)));
}

function v1_admin_source_rights(PDO $pdo, array $config): void {
    $page = v1_list_params();
    $page['limit'] = min(25, (int)$page['limit']);
    $page['offset'] = ((int)$page['page'] - 1) * (int)$page['limit'];
    $stmt = $pdo->prepare('SELECT source_right_id, source_type, source_key, source_name, LEFT(permission_scope,4000) AS permission_scope, evidence_uri, evidence_hash, '
        . 'valid_from, valid_until, revoked_at, ai_allowed, redistribution_allowed, status, LEFT(notes,2000) AS notes, created_at, updated_at '
        . 'FROM ' . table_name($config, 'source_rights') . ' ORDER BY updated_at DESC, source_right_id ASC '
        . 'LIMIT ' . ((int)$page['limit'] + 1) . ' OFFSET ' . (int)$page['offset']);
    $stmt->execute();
    list($rows, $hasMore) = v1_fetch_page($stmt, $page);
    foreach ($rows as &$row) {
        foreach (array(
            'valid_from',
            'valid_until',
            'revoked_at',
            'created_at',
            'updated_at',
        ) as $field) {
            $row[$field] = v1_release_iso_time(
                isset($row[$field]) ? $row[$field] : null
            );
        }
    }
    unset($row);
    v1_respond(200, array('ok' => true, 'data' => $rows, 'pagination' => v1_page_meta($page, count($rows), $hasMore)));
}

function v1_admin_editorial_revisions(PDO $pdo, array $config): void {
    $page = v1_list_params();
    $page['limit'] = min(25, (int)$page['limit']);
    $page['offset'] = ((int)$page['page'] - 1) * (int)$page['limit'];
    $status = isset($_GET['status']) ? trim((string)$_GET['status']) : '';
    $where = '';
    $params = array();
    if ($status !== '') {
        if (!in_array($status, array('pending', 'approved', 'rejected', 'published'), true)) {
            v1_respond(400, array('ok' => false, 'error' => 'invalid_revision_status'));
        }
        $where = ' WHERE revision_status = ?'; $params[] = $status;
    }
    $stmt = $pdo->prepare('SELECT revision_id, entity_type, entity_id, field_name, LEFT(previous_value,2000) AS previous_value, LEFT(revised_value,2000) AS revised_value, LEFT(reason,2000) AS reason, '
        . 'revision_status, requested_by, reviewed_by, reviewed_at, published_at, created_at, updated_at '
        . 'FROM ' . table_name($config, 'editorial_revisions') . $where . ' ORDER BY created_at DESC '
        . 'LIMIT ' . ((int)$page['limit'] + 1) . ' OFFSET ' . (int)$page['offset']);
    $stmt->execute($params);
    list($rows, $hasMore) = v1_fetch_page($stmt, $page);
    v1_respond(200, array('ok' => true, 'data' => $rows, 'pagination' => v1_page_meta($page, count($rows), $hasMore)));
}

function v1_admin_feedback(PDO $pdo, array $config): void {
    $page = v1_list_params();
    $page['limit'] = min(10, (int)$page['limit']);
    $page['offset'] = ((int)$page['page'] - 1) * (int)$page['limit'];
    $status = isset($_GET['status']) ? trim((string)$_GET['status']) : 'pending';
    if (!in_array($status, array('pending', 'reviewing', 'resolved', 'rejected'), true)) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_feedback_status'));
    }
    $stmt = $pdo->prepare('SELECT feedback_id, feedback_type, entity_type, entity_id, submitter_name, submitter_contact, LEFT(message,3000) AS message, '
        . 'evidence_urls_json, status, is_public, LEFT(review_note,4000) AS review_note, reviewed_by, reviewed_at, created_at, updated_at '
        . 'FROM ' . table_name($config, 'feedback') . ' WHERE status = ? ORDER BY created_at ASC '
        . 'LIMIT ' . ((int)$page['limit'] + 1) . ' OFFSET ' . (int)$page['offset']);
    $stmt->execute(array($status));
    list($rows, $hasMore) = v1_fetch_page($stmt, $page);
    foreach ($rows as &$row) {
        $row['evidence_urls'] = array_slice(decode_json_array(isset($row['evidence_urls_json']) ? $row['evidence_urls_json'] : null), 0, 5);
        unset($row['evidence_urls_json']);
        $row['is_public'] = false;
    }
    unset($row);
    v1_respond(200, array('ok' => true, 'data' => $rows, 'pagination' => v1_page_meta($page, count($rows), $hasMore)));
}

function v1_admin_json_body(array $config): array {
    $contentType = isset($_SERVER['CONTENT_TYPE']) ? strtolower((string)$_SERVER['CONTENT_TYPE']) : '';
    if (strpos($contentType, 'application/json') !== 0) {
        v1_respond(415, array('ok' => false, 'error' => 'application_json_required'));
    }
    return decode_json_body(read_body($config));
}

function v1_release_iso_time($value): ?string {
    if (!is_string($value) || preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $value) !== 1) {
        return null;
    }
    return str_replace(' ', 'T', $value) . 'Z';
}

function v1_admin_release_state(PDO $pdo, array $config): void {
    $row = v1_release_state($pdo, $config);
    if ($row === null || !in_array((string)$row['release_state'], array('closed', 'preview', 'live'), true)) {
        v1_respond(503, array('ok' => false, 'error' => 'release_state_unavailable'));
    }
    $historyLimit = isset($_GET['history_limit']) ? (int)$_GET['history_limit'] : 20;
    $historyLimit = max(1, min(50, $historyLimit));
    $historyStmt = $pdo->prepare('SELECT audit_id, state_version, previous_state, new_state, changed_by, change_reason, request_id, cutover_at, sunset_at, created_at FROM '
        . table_name($config, 'governance_release_audit') . ' WHERE state_key = ? ORDER BY state_version DESC LIMIT ' . $historyLimit);
    $historyStmt->execute(array(GOV_V1_RELEASE_STATE_KEY));
    $history = $historyStmt->fetchAll();
    foreach ($history as &$entry) {
        $entry['state_version'] = (int)$entry['state_version'];
        $entry['cutover_at'] = v1_release_iso_time(isset($entry['cutover_at']) ? $entry['cutover_at'] : null);
        $entry['sunset_at'] = v1_release_iso_time(isset($entry['sunset_at']) ? $entry['sunset_at'] : null);
        $entry['created_at'] = v1_release_iso_time(isset($entry['created_at']) ? $entry['created_at'] : null);
    }
    unset($entry);
    v1_respond(200, array(
        'ok' => true,
        'release_state' => (string)$row['release_state'],
        'state_version' => (int)$row['state_version'],
        'updated_by' => (string)$row['updated_by'],
        'update_reason' => (string)$row['update_reason'],
        'cutover_at' => v1_release_iso_time(isset($row['cutover_at']) ? $row['cutover_at'] : null),
        'sunset_at' => v1_release_iso_time(isset($row['sunset_at']) ? $row['sunset_at'] : null),
        'updated_at' => v1_release_iso_time($row['updated_at']),
        'schema_version' => GOV_V1_SCHEMA_VERSION,
        'preview_auth_configured' => v1_preview_auth_configured($config),
        'history' => $history,
    ));
}

function v1_assert_object_keys(array $value, array $allowed, string $location): void {
    $allowedMap = array_fill_keys($allowed, true);
    foreach (array_keys($value) as $key) {
        if (!is_string($key) || !isset($allowedMap[$key])) {
            v1_respond(400, array('ok' => false, 'error' => 'unexpected_field', 'field' => $location . '.' . (string)$key));
        }
    }
}

function v1_valid_route_template($value): ?string {
    if (!is_string($value)) { return null; }
    $value = trim($value);
    if (strlen($value) < 1 || strlen($value) > 191 || substr($value, 0, 1) !== '/'
        || preg_match('/[?#\s]/', $value) === 1
        || preg_match('#^/[A-Za-z0-9_./:{}\-]+$#', $value) !== 1) {
        return null;
    }
    return $value;
}

function v1_valid_build_sha($value): ?string {
    if (!is_string($value)) { return null; }
    $value = strtolower(trim($value));
    return preg_match('/^[a-f0-9]{7,64}$/', $value) === 1 ? $value : null;
}

function v1_percentile(array $values, float $percentile): ?float {
    if (!$values) { return null; }
    sort($values, SORT_NUMERIC);
    $index = (int)ceil($percentile * count($values)) - 1;
    $index = max(0, min(count($values) - 1, $index));
    return (float)$values[$index];
}

/**
 * Compute KIND receipt-to-first-observed lag only from timestamped document
 * observations. Missing publication timestamps are deliberately omitted;
 * collection dates or midnight defaults must never stand in for receipt time.
 */
function v1_kind_observation_stats_by_day(PDO $pdo, array $config, string $from, string $to): array {
    list($start,$end) = v1_evidence_utc_bounds($from,$to);
    $stmt = $pdo->prepare('SELECT eo.observation_id,eo.first_observed_at,d.published_at,d.source_right_id,sr.source_type,sr.source_key FROM '
        . table_name($config,'event_observations') . ' eo JOIN ' . table_name($config,'documents')
        . ' d ON d.document_id=eo.document_id LEFT JOIN ' . table_name($config,'source_rights')
        . ' sr ON sr.source_right_id=d.source_right_id WHERE eo.first_observed_at BETWEEN ? AND ? '
        . 'AND eo.source_key=\'kind\' '
        . 'ORDER BY eo.first_observed_at,eo.observation_id');
    $stmt->execute(array($start,$end)); $stats = array();
    foreach ($stmt->fetchAll() as $row) {
        $day = v1_kst_observation_date((string)$row['first_observed_at']);
        if ($day === null) { continue; }
        if (!isset($stats[$day])) { $stats[$day] = array('observation_count'=>0,'lag_sample_count'=>0,'lag_seconds'=>array()); }
        $stats[$day]['observation_count']++;
        if ((string)$row['source_right_id'] !== 'official:kind' || (string)$row['source_type'] !== 'official_disclosure'
            || (string)$row['source_key'] !== 'kind' || $row['published_at'] === null) { continue; }
        $firstEpoch = strtotime((string)$row['first_observed_at'] . ' UTC');
        $receiptEpoch = strtotime((string)$row['published_at'] . ' UTC');
        if ($firstEpoch === false || $receiptEpoch === false || $firstEpoch < $receiptEpoch) { continue; }
        $stats[$day]['lag_sample_count']++;
        $stats[$day]['lag_seconds'][] = (float)($firstEpoch-$receiptEpoch);
    }
    return $stats;
}

function v1_kind_observation_lags_by_day(PDO $pdo, array $config, string $from, string $to): array {
    $stats = v1_kind_observation_stats_by_day($pdo,$config,$from,$to); $byDay = array();
    foreach ($stats as $day => $values) { $byDay[$day] = $values['lag_seconds']; }
    return $byDay;
}

function v1_kind_observation_lag_p95_minutes(PDO $pdo, array $config, string $date): ?float {
    $stats = v1_kind_observation_stats_by_day($pdo,$config,$date,$date);
    $seconds = isset($stats[$date]) ? v1_percentile($stats[$date]['lag_seconds'],0.95) : null;
    return $seconds === null ? null : $seconds/60.0;
}

function v1_public_revisions(PDO $pdo, array $config): void {
    $page = v1_list_params();
    $entityType = isset($_GET['entity_type']) ? trim((string)$_GET['entity_type']) : '';
    $entityId = isset($_GET['entity_id']) ? trim((string)$_GET['entity_id']) : '';
    if ($entityType !== '' && !in_array($entityType, array('company','event','campaign','document','actor'), true)) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_entity_type'));
    }
    if ($entityId !== '' && !v1_valid_entity_id($entityId)) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_entity_id'));
    }
    $where = array('er.revision_status = \'published\'', 'er.published_at IS NOT NULL');
    $params = array();
    if ($entityType !== '') { $where[] = 'er.entity_type = ?'; $params[] = $entityType; }
    if ($entityId !== '') { $where[] = 'er.entity_id = ?'; $params[] = $entityId; }
    $where[] = '('
        . '(er.entity_type = \'event\' AND EXISTS (SELECT 1 FROM ' . table_name($config, 'governance_events') . ' e '
        . 'WHERE e.event_id = er.entity_id AND ' . v1_event_visibility_sql($config, 'e') . ')) OR '
        . '(er.entity_type = \'campaign\' AND EXISTS (SELECT 1 FROM ' . table_name($config, 'campaigns') . ' cp '
        . 'WHERE cp.campaign_id = er.entity_id AND ' . v1_campaign_visibility_sql($config, 'cp') . ')) OR '
        . '(er.entity_type = \'company\' AND EXISTS (SELECT 1 FROM ' . table_name($config, 'companies') . ' c '
        . 'WHERE c.company_id = er.entity_id AND ' . v1_company_has_public_event_sql($config, 'c') . ')) OR '
        . '(er.entity_type = \'actor\' AND EXISTS (SELECT 1 FROM ' . table_name($config, 'actors') . ' a '
        . 'WHERE a.actor_id = er.entity_id AND ' . v1_actor_visibility_sql($config, 'a') . ')) OR '
        . '(er.entity_type = \'document\' AND EXISTS (SELECT 1 FROM ' . table_name($config, 'documents') . ' d '
        . 'LEFT JOIN ' . table_name($config, 'source_rights') . ' sr ON sr.source_right_id = d.source_right_id '
        . 'WHERE d.document_id = er.entity_id AND ' . v1_document_visibility_sql('d', 'sr') . '))'
        . ')';
    $sql = 'SELECT er.revision_id, er.entity_type, er.entity_id, er.field_name, er.reason, er.published_at, er.updated_at '
        . 'FROM ' . table_name($config, 'editorial_revisions') . ' er WHERE ' . implode(' AND ', $where)
        . ' ORDER BY er.published_at DESC, er.revision_id DESC LIMIT ' . ((int)$page['limit'] + 1)
        . ' OFFSET ' . (int)$page['offset'];
    $stmt = $pdo->prepare($sql); $stmt->execute($params);
    list($rows, $hasMore) = v1_fetch_page($stmt, $page);
    v1_respond(200, array('ok' => true, 'data' => $rows, 'pagination' => v1_page_meta($page, count($rows), $hasMore)));
}

function v1_shadow_event_keys($run, string $field): array {
    if (!is_array($run)) {
        v1_respond(400, array('ok'=>false,'error'=>'invalid_shadow_run','field'=>$field));
    }
    v1_assert_object_keys($run, array('status','events'), $field);
    $status = isset($run['status']) ? trim((string)$run['status']) : '';
    $events = isset($run['events']) && is_array($run['events']) ? $run['events'] : null;
    if (!in_array($status, array('succeeded','failed'), true) || $events === null || count($events) > 10000) {
        v1_respond(400, array('ok'=>false,'error'=>'invalid_shadow_run','field'=>$field));
    }
    $keys = array(); $seen = array();
    foreach ($events as $index => $event) {
        if (!is_array($event)) {
            v1_respond(400, array('ok'=>false,'error'=>'invalid_shadow_event','field'=>$field,'index'=>$index));
        }
        v1_assert_object_keys($event, array('comparison_key'), $field . '.events[' . $index . ']');
        $key = isset($event['comparison_key']) ? strtolower(trim((string)$event['comparison_key'])) : '';
        if (preg_match('/^eventcmp:v1:[a-f0-9]{64}$/', $key) !== 1 || isset($seen[$key])) {
            v1_respond(400, array('ok'=>false,'error'=>isset($seen[$key]) ? 'duplicate_shadow_comparison_key' : 'invalid_shadow_comparison_key',
                'field'=>$field,'index'=>$index));
        }
        $seen[$key] = true; $keys[] = $key;
    }
    sort($keys, SORT_STRING);
    return array($status, $keys);
}

/** Require a non-empty, lossless legacy-to-canonical crosswalk for every shadow day. */
function v1_shadow_legacy_crosswalk($value): array {
    if (!is_array($value)) {
        v1_respond(400,array('ok'=>false,'error'=>'invalid_legacy_crosswalk'));
    }
    $fields = array('schema_version','eligible_legacy_record_count','crosswalked_legacy_record_count',
        'unmatched_legacy_record_count','ambiguous_legacy_record_count','coverage_rate','crosswalk_sha256');
    v1_assert_object_keys($value,$fields,'legacy_crosswalk');
    foreach ($fields as $field) {
        if (!array_key_exists($field,$value)) {
            v1_respond(400,array('ok'=>false,'error'=>'invalid_legacy_crosswalk','field'=>$field));
        }
    }
    $schemaVersion = $value['schema_version'];
    $eligible = $value['eligible_legacy_record_count'];
    $crosswalked = $value['crosswalked_legacy_record_count'];
    $unmatched = $value['unmatched_legacy_record_count'];
    $ambiguous = $value['ambiguous_legacy_record_count'];
    $coverage = $value['coverage_rate'];
    $sha = is_string($value['crosswalk_sha256']) ? strtolower(trim($value['crosswalk_sha256'])) : '';
    if (!is_int($schemaVersion) || $schemaVersion !== 1 || !is_int($eligible) || $eligible < 1
        || !is_int($crosswalked) || $crosswalked !== $eligible || !is_int($unmatched) || $unmatched !== 0
        || !is_int($ambiguous) || $ambiguous !== 0 || (!is_int($coverage) && !is_float($coverage))
        || !is_finite((float)$coverage) || abs((float)$coverage - 1.0) > 0.000001
        || preg_match('/^[a-f0-9]{64}$/',$sha) !== 1) {
        v1_respond(400,array('ok'=>false,'error'=>'invalid_legacy_crosswalk'));
    }
    return array('schema_version'=>1,'eligible_legacy_record_count'=>$eligible,
        'crosswalked_legacy_record_count'=>$crosswalked,'unmatched_legacy_record_count'=>0,
        'ambiguous_legacy_record_count'=>0,'coverage_rate'=>1.0,'crosswalk_sha256'=>$sha);
}

function v1_shadow_crosswalk_response(array $row): array {
    $required = array('legacy_crosswalk_schema_version','legacy_eligible_record_count',
        'legacy_crosswalked_record_count','legacy_unmatched_record_count','legacy_ambiguous_record_count',
        'legacy_crosswalk_coverage_rate','legacy_crosswalk_sha256');
    foreach ($required as $field) {
        if (!array_key_exists($field,$row) || $row[$field] === null) {
            v1_respond(503,array('ok'=>false,'error'=>'shadow_run_crosswalk_integrity_error',
                'observation_date'=>$row['observation_date'] ?? null,'code_revision'=>$row['code_revision'] ?? null));
        }
    }
    $eligible = (int)$row['legacy_eligible_record_count'];
    $crosswalked = (int)$row['legacy_crosswalked_record_count'];
    $unmatched = (int)$row['legacy_unmatched_record_count'];
    $ambiguous = (int)$row['legacy_ambiguous_record_count'];
    $coverage = (float)$row['legacy_crosswalk_coverage_rate'];
    $sha = strtolower((string)$row['legacy_crosswalk_sha256']);
    if ((int)$row['legacy_crosswalk_schema_version'] !== 1 || $eligible < 1 || $crosswalked !== $eligible
        || $unmatched !== 0 || $ambiguous !== 0 || abs($coverage - 1.0) > 0.000001
        || preg_match('/^[a-f0-9]{64}$/',$sha) !== 1) {
        v1_respond(503,array('ok'=>false,'error'=>'shadow_run_crosswalk_integrity_error',
            'observation_date'=>$row['observation_date'] ?? null,'code_revision'=>$row['code_revision'] ?? null));
    }
    return array('schema_version'=>1,'eligible_legacy_record_count'=>$eligible,
        'crosswalked_legacy_record_count'=>$crosswalked,'unmatched_legacy_record_count'=>$unmatched,
        'ambiguous_legacy_record_count'=>$ambiguous,'coverage_rate'=>1.0,'crosswalk_sha256'=>$sha);
}

function v1_shadow_keys_json(array $keys): string {
    $encoded = json_encode(array_values($keys), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    if (!is_string($encoded)) { throw new RuntimeException('shadow_keys_json_encode_failed'); }
    return $encoded;
}

function v1_shadow_events_response(string $encoded): array {
    $keys = json_decode($encoded, true);
    if (!is_array($keys)) { throw new RuntimeException('shadow_keys_json_invalid'); }
    $events = array();
    foreach ($keys as $key) { $events[] = array('comparison_key'=>(string)$key); }
    return $events;
}

function v1_admin_shadow_runs(PDO $pdo, array $config): void {
    $page = v1_list_params(); $where = array('1=1'); $params = array();
    $revision = isset($_GET['code_revision']) ? v1_valid_build_sha($_GET['code_revision']) : null;
    if (isset($_GET['code_revision']) && $revision === null) { v1_respond(400, array('ok'=>false,'error'=>'invalid_code_revision')); }
    if ($revision !== null) { $where[] = 'code_revision=?'; $params[] = $revision; }
    foreach (array('from'=>'>=','to'=>'<=') as $key => $operator) {
        if (!isset($_GET[$key]) || trim((string)$_GET[$key]) === '') { continue; }
        $date = trim((string)$_GET[$key]);
        if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $date) !== 1) { v1_respond(400, array('ok'=>false,'error'=>'invalid_'.$key)); }
        $where[] = 'observation_date ' . $operator . ' ?'; $params[] = $date;
    }
    $stmt = $pdo->prepare('SELECT observation_date,code_revision,legacy_status,candidate_status,legacy_comparison_keys_json,'
        . 'candidate_comparison_keys_json,legacy_event_count,candidate_event_count,legacy_events_sha256,candidate_events_sha256,'
        . 'legacy_crosswalk_schema_version,legacy_eligible_record_count,legacy_crosswalked_record_count,'
        . 'legacy_unmatched_record_count,legacy_ambiguous_record_count,legacy_crosswalk_coverage_rate,legacy_crosswalk_sha256,'
        . 'created_by,updated_by,created_at,updated_at FROM ' . table_name($config, 'shadow_run_observations')
        . ' WHERE ' . implode(' AND ', $where) . ' ORDER BY observation_date DESC,code_revision DESC LIMIT '
        . ((int)$page['limit'] + 1) . ' OFFSET ' . (int)$page['offset']);
    $stmt->execute($params); list($rows,$hasMore) = v1_fetch_page($stmt,$page); $data = array();
    foreach ($rows as $row) {
        $legacyEvents = v1_shadow_events_response((string)$row['legacy_comparison_keys_json']);
        $candidateEvents = v1_shadow_events_response((string)$row['candidate_comparison_keys_json']);
        if (count($legacyEvents) !== (int)$row['legacy_event_count']
            || count($candidateEvents) !== (int)$row['candidate_event_count']
            || hash('sha256',(string)$row['legacy_comparison_keys_json']) !== (string)$row['legacy_events_sha256']
            || hash('sha256',(string)$row['candidate_comparison_keys_json']) !== (string)$row['candidate_events_sha256']) {
            v1_respond(503, array('ok'=>false,'error'=>'shadow_run_integrity_error','observation_date'=>$row['observation_date'],
                'code_revision'=>$row['code_revision']));
        }
        $legacyCrosswalk = v1_shadow_crosswalk_response($row);
        $data[] = array('observation_date'=>(string)$row['observation_date'],'code_revision'=>(string)$row['code_revision'],
            'legacy_run'=>array('status'=>(string)$row['legacy_status'],'events'=>$legacyEvents,'event_count'=>(int)$row['legacy_event_count'],
                'events_sha256'=>(string)$row['legacy_events_sha256']),
            'candidate_run'=>array('status'=>(string)$row['candidate_status'],'events'=>$candidateEvents,'event_count'=>(int)$row['candidate_event_count'],
                'events_sha256'=>(string)$row['candidate_events_sha256']),
            'legacy_crosswalk'=>$legacyCrosswalk,
            'created_by'=>(string)$row['created_by'],'updated_by'=>(string)$row['updated_by'],
            'created_at'=>v1_release_iso_time($row['created_at']),'updated_at'=>v1_release_iso_time($row['updated_at']));
    }
    v1_respond(200,array('ok'=>true,'data'=>$data,'pagination'=>v1_page_meta($page,count($data),$hasMore)));
}

function v1_admin_upsert_shadow_run(PDO $pdo, array $config, string $role): void {
    $payload = v1_admin_json_body($config);
    v1_assert_object_keys($payload,array('observation_date','code_revision','legacy_run','candidate_run','legacy_crosswalk','expected_updated_at'),'body');
    $date = isset($payload['observation_date']) ? trim((string)$payload['observation_date']) : '';
    $revision = isset($payload['code_revision']) ? v1_valid_build_sha($payload['code_revision']) : null;
    if (preg_match('/^\d{4}-\d{2}-\d{2}$/',$date) !== 1 || $revision === null) {
        v1_respond(400,array('ok'=>false,'error'=>'invalid_shadow_run_identity'));
    }
    list($legacyStatus,$legacyKeys) = v1_shadow_event_keys(isset($payload['legacy_run']) ? $payload['legacy_run'] : null,'legacy_run');
    list($candidateStatus,$candidateKeys) = v1_shadow_event_keys(isset($payload['candidate_run']) ? $payload['candidate_run'] : null,'candidate_run');
    $legacyCrosswalk = v1_shadow_legacy_crosswalk($payload['legacy_crosswalk'] ?? null);
    $legacyJson = v1_shadow_keys_json($legacyKeys); $candidateJson = v1_shadow_keys_json($candidateKeys);
    $legacyHash = hash('sha256',$legacyJson); $candidateHash = hash('sha256',$candidateJson);
    $expected = isset($payload['expected_updated_at']) ? v1_editorial_datetime_utc($payload['expected_updated_at']) : null;
    $now = gmdate('Y-m-d H:i:s');
    if (scalar_int($pdo,'SELECT COUNT(*) FROM ' . table_name($config,'shadow_run_observations')
        . ' WHERE updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 MINUTE)') >= 120) {
        header('Retry-After: 60'); v1_respond(429,array('ok'=>false,'error'=>'shadow_run_write_rate_limited'));
    }
    $pdo->beginTransaction();
    try {
        $lookup = $pdo->prepare('SELECT legacy_status,candidate_status,legacy_events_sha256,candidate_events_sha256,'
            . 'legacy_crosswalk_schema_version,legacy_eligible_record_count,legacy_crosswalked_record_count,'
            . 'legacy_unmatched_record_count,legacy_ambiguous_record_count,legacy_crosswalk_coverage_rate,legacy_crosswalk_sha256,updated_at FROM '
            . table_name($config,'shadow_run_observations') . ' WHERE observation_date=? AND code_revision=? LIMIT 1 FOR UPDATE');
        $lookup->execute(array($date,$revision)); $existing = $lookup->fetch();
        if ($existing) {
            $identical = (string)$existing['legacy_status'] === $legacyStatus
                && (string)$existing['candidate_status'] === $candidateStatus
                && hash_equals((string)$existing['legacy_events_sha256'],$legacyHash)
                && hash_equals((string)$existing['candidate_events_sha256'],$candidateHash)
                && (int)$existing['legacy_crosswalk_schema_version'] === 1
                && (int)$existing['legacy_eligible_record_count'] === $legacyCrosswalk['eligible_legacy_record_count']
                && (int)$existing['legacy_crosswalked_record_count'] === $legacyCrosswalk['crosswalked_legacy_record_count']
                && (int)$existing['legacy_unmatched_record_count'] === 0
                && (int)$existing['legacy_ambiguous_record_count'] === 0
                && abs((float)$existing['legacy_crosswalk_coverage_rate'] - 1.0) <= 0.000001
                && hash_equals((string)$existing['legacy_crosswalk_sha256'],$legacyCrosswalk['crosswalk_sha256']);
            if ($identical && ($expected === null || (string)$existing['updated_at'] === $expected)) {
                $pdo->commit();
                v1_respond(200,array('ok'=>true,'unchanged'=>true,'observation_date'=>$date,'code_revision'=>$revision,
                    'legacy_crosswalk'=>$legacyCrosswalk,
                    'updated_at'=>v1_release_iso_time($existing['updated_at'])));
            }
            if ($expected === null || (string)$existing['updated_at'] !== $expected) {
                $pdo->rollBack(); v1_respond(409,array('ok'=>false,'error'=>'shadow_run_version_conflict'));
            }
            $stmt = $pdo->prepare('UPDATE ' . table_name($config,'shadow_run_observations')
                . ' SET legacy_status=?,candidate_status=?,legacy_comparison_keys_json=?,candidate_comparison_keys_json=?,'
                . 'legacy_event_count=?,candidate_event_count=?,legacy_events_sha256=?,candidate_events_sha256=?,'
                . 'legacy_crosswalk_schema_version=?,legacy_eligible_record_count=?,legacy_crosswalked_record_count=?,'
                . 'legacy_unmatched_record_count=?,legacy_ambiguous_record_count=?,legacy_crosswalk_coverage_rate=?,legacy_crosswalk_sha256=?,'
                . 'updated_by=?,updated_at=? '
                . 'WHERE observation_date=? AND code_revision=?');
            $stmt->execute(array($legacyStatus,$candidateStatus,$legacyJson,$candidateJson,count($legacyKeys),count($candidateKeys),
                $legacyHash,$candidateHash,1,$legacyCrosswalk['eligible_legacy_record_count'],$legacyCrosswalk['crosswalked_legacy_record_count'],
                0,0,1.0,$legacyCrosswalk['crosswalk_sha256'],$role,$now,$date,$revision));
        } else {
            if (isset($payload['expected_updated_at'])) {
                $pdo->rollBack(); v1_respond(409,array('ok'=>false,'error'=>'shadow_run_missing'));
            }
            $stmt = $pdo->prepare('INSERT INTO ' . table_name($config,'shadow_run_observations')
                . ' (observation_date,code_revision,legacy_status,candidate_status,legacy_comparison_keys_json,candidate_comparison_keys_json,'
                . 'legacy_event_count,candidate_event_count,legacy_events_sha256,candidate_events_sha256,legacy_crosswalk_schema_version,'
                . 'legacy_eligible_record_count,legacy_crosswalked_record_count,legacy_unmatched_record_count,legacy_ambiguous_record_count,'
                . 'legacy_crosswalk_coverage_rate,legacy_crosswalk_sha256,created_by,updated_by,created_at,updated_at) '
                . 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)');
            $stmt->execute(array($date,$revision,$legacyStatus,$candidateStatus,$legacyJson,$candidateJson,count($legacyKeys),count($candidateKeys),
                $legacyHash,$candidateHash,1,$legacyCrosswalk['eligible_legacy_record_count'],$legacyCrosswalk['crosswalked_legacy_record_count'],
                0,0,1.0,$legacyCrosswalk['crosswalk_sha256'],$role,$role,$now,$now));
        }
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    v1_respond($existing ? 200 : 201,array('ok'=>true,'unchanged'=>false,'observation_date'=>$date,'code_revision'=>$revision,
        'legacy_event_count'=>count($legacyKeys),'candidate_event_count'=>count($candidateKeys),
        'legacy_crosswalk'=>$legacyCrosswalk,'updated_at'=>str_replace(' ','T',$now).'Z'));
}

function v1_admin_shadow_discrepancies(PDO $pdo, array $config): void {
    $page = v1_list_params();
    $where = array('1=1'); $params = array();
    foreach (array('review_status' => 24, 'discrepancy_type' => 40) as $field => $max) {
        $value = isset($_GET[$field]) ? trim((string)$_GET[$field]) : '';
        if ($value === '') { continue; }
        if (preg_match('/^[A-Za-z0-9_.:\-]{1,' . $max . '}$/', $value) !== 1) {
            v1_respond(400, array('ok' => false, 'error' => 'invalid_' . $field));
        }
        $where[] = 'sd.' . $field . ' = ?'; $params[] = $value;
    }
    $revision = isset($_GET['code_revision']) ? v1_valid_build_sha($_GET['code_revision']) : null;
    if (isset($_GET['code_revision']) && $revision === null) { v1_respond(400, array('ok' => false, 'error' => 'invalid_code_revision')); }
    if ($revision !== null) { $where[] = 'sd.code_revision = ?'; $params[] = $revision; }
    foreach (array('from' => '>=', 'to' => '<=') as $key => $operator) {
        if (!isset($_GET[$key]) || trim((string)$_GET[$key]) === '') { continue; }
        $date = trim((string)$_GET[$key]);
        if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $date) !== 1) { v1_respond(400, array('ok' => false, 'error' => 'invalid_' . $key)); }
        $where[] = 'sd.observation_date ' . $operator . ' ?'; $params[] = $date;
    }
    $stmt = $pdo->prepare('SELECT sd.discrepancy_id, sd.observation_date, sd.code_revision, sd.comparison_key, sd.discrepancy_type, '
        . 'sd.legacy_event_json, sd.candidate_event_json, sd.review_status, sd.review_note, sd.reviewed_by, sd.reviewed_at, sd.created_at, sd.updated_at '
        . 'FROM ' . table_name($config, 'shadow_discrepancies') . ' sd WHERE ' . implode(' AND ', $where)
        . ' ORDER BY sd.observation_date DESC, sd.updated_at DESC, sd.discrepancy_id DESC LIMIT ' . ((int)$page['limit'] + 1)
        . ' OFFSET ' . (int)$page['offset']);
    $stmt->execute($params); list($rows, $hasMore) = v1_fetch_page($stmt, $page);
    foreach ($rows as &$row) {
        $row['legacy_event'] = isset($row['legacy_event_json']) && $row['legacy_event_json'] !== null ? json_decode((string)$row['legacy_event_json'], true) : null;
        $row['candidate_event'] = isset($row['candidate_event_json']) && $row['candidate_event_json'] !== null ? json_decode((string)$row['candidate_event_json'], true) : null;
        unset($row['legacy_event_json'], $row['candidate_event_json']);
    }
    unset($row);
    v1_respond(200, array('ok' => true, 'data' => $rows, 'pagination' => v1_page_meta($page, count($rows), $hasMore)));
}

function v1_admin_upsert_shadow_discrepancy(PDO $pdo, array $config, string $role): void {
    $payload = v1_admin_json_body($config);
    v1_assert_object_keys($payload, array('discrepancy_id','observation_date','code_revision','comparison_key','discrepancy_type',
        'legacy_event','candidate_event','review_status','review_note','expected_updated_at'), 'body');
    $date = isset($payload['observation_date']) ? trim((string)$payload['observation_date']) : '';
    $revision = isset($payload['code_revision']) ? v1_valid_build_sha($payload['code_revision']) : null;
    $comparisonKey = isset($payload['comparison_key']) ? trim((string)$payload['comparison_key']) : '';
    $type = isset($payload['discrepancy_type']) ? trim((string)$payload['discrepancy_type']) : '';
    $status = isset($payload['review_status']) ? trim((string)$payload['review_status']) : 'pending';
    $note = isset($payload['review_note']) ? trim((string)$payload['review_note']) : '';
    if (preg_match('/^\d{4}-\d{2}-\d{2}$/', $date) !== 1 || $revision === null
        || $comparisonKey === '' || strlen($comparisonKey) > 191
        || preg_match('/^[A-Za-z0-9_.:\-]{1,40}$/', $type) !== 1
        || !in_array($status, array('pending','reviewed','resolved','dismissed'), true)
        || mb_strlen($note, 'UTF-8') > 5000 || ($status !== 'pending' && $note === '')) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_shadow_discrepancy'));
    }
    foreach (array('legacy_event','candidate_event') as $field) {
        if (isset($payload[$field]) && !is_array($payload[$field])) { v1_respond(400, array('ok' => false, 'error' => 'invalid_' . $field)); }
    }
    $legacyJson = array_key_exists('legacy_event', $payload) ? json_value($payload['legacy_event']) : null;
    $candidateJson = array_key_exists('candidate_event', $payload) ? json_value($payload['candidate_event']) : null;
    if (($legacyJson !== null && strlen($legacyJson) > 1000000) || ($candidateJson !== null && strlen($candidateJson) > 1000000)) {
        v1_respond(413, array('ok' => false, 'error' => 'shadow_payload_too_large'));
    }
    $id = isset($payload['discrepancy_id']) ? trim((string)$payload['discrepancy_id']) : '';
    if ($id === '') { $id = v1_stable_id('shadow', $date . '|' . $revision . '|' . $comparisonKey . '|' . $type); }
    if (!v1_valid_entity_id($id)) { v1_respond(400, array('ok' => false, 'error' => 'invalid_discrepancy_id')); }
    $expected = isset($payload['expected_updated_at']) ? v1_editorial_datetime_utc($payload['expected_updated_at']) : null;
    $now = gmdate('Y-m-d H:i:s');
    if (scalar_int($pdo, 'SELECT COUNT(*) FROM ' . table_name($config, 'shadow_discrepancies') . ' WHERE updated_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 MINUTE)') >= 300) {
        header('Retry-After: 60'); v1_respond(429, array('ok' => false, 'error' => 'shadow_write_rate_limited'));
    }
    $pdo->beginTransaction();
    try {
        $existingStmt = $pdo->prepare('SELECT updated_at FROM ' . table_name($config, 'shadow_discrepancies') . ' WHERE discrepancy_id=? LIMIT 1 FOR UPDATE');
        $existingStmt->execute(array($id)); $existing = $existingStmt->fetch();
        if ($existing) {
            if ($expected === null || (string)$existing['updated_at'] !== $expected) { $pdo->rollBack(); v1_respond(409, array('ok' => false, 'error' => 'shadow_discrepancy_version_conflict')); }
            $stmt = $pdo->prepare('UPDATE ' . table_name($config, 'shadow_discrepancies') . ' SET observation_date=?, code_revision=?, comparison_key=?, discrepancy_type=?, '
                . 'legacy_event_json=?, candidate_event_json=?, review_status=?, review_note=?, reviewed_by=?, reviewed_at=?, updated_at=? WHERE discrepancy_id=?');
            $stmt->execute(array($date,$revision,$comparisonKey,$type,$legacyJson,$candidateJson,$status,$note ?: null,
                $status === 'pending' ? null : $role,$status === 'pending' ? null : $now,$now,$id));
        } else {
            if (isset($payload['expected_updated_at'])) { $pdo->rollBack(); v1_respond(409, array('ok' => false, 'error' => 'shadow_discrepancy_missing')); }
            $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'shadow_discrepancies') . ' (discrepancy_id,observation_date,code_revision,comparison_key,discrepancy_type,'
                . 'legacy_event_json,candidate_event_json,review_status,review_note,reviewed_by,reviewed_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)');
            $stmt->execute(array($id,$date,$revision,$comparisonKey,$type,$legacyJson,$candidateJson,$status,$note ?: null,
                $status === 'pending' ? null : $role,$status === 'pending' ? null : $now,$now,$now));
        }
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    v1_respond($existing ? 200 : 201, array('ok' => true, 'discrepancy_id' => $id, 'review_status' => $status, 'updated_at' => str_replace(' ', 'T', $now) . 'Z'));
}

function v1_record_availability_observations(PDO $pdo, array $config, string $role): void {
    $payload = v1_admin_json_body($config);
    v1_assert_object_keys($payload, array('observations'), 'body');
    $observations = isset($payload['observations']) && is_array($payload['observations']) ? $payload['observations'] : null;
    if ($observations === null || count($observations) < 1 || count($observations) > 10) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_observation_batch'));
    }
    if (scalar_int($pdo, 'SELECT COUNT(*) FROM ' . table_name($config, 'availability_observations') . ' WHERE created_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 MINUTE)') >= 3000) {
        header('Retry-After: 60'); v1_respond(429, array('ok' => false, 'error' => 'availability_rate_limited'));
    }
    $normalized = array(); $seen = array(); $nowEpoch = time();
    foreach ($observations as $index => $item) {
        if (!is_array($item)) { v1_respond(400, array('ok' => false, 'error' => 'invalid_observation', 'index' => $index)); }
        v1_assert_object_keys($item, array('observation_id','route_template','observed_at','http_status','duration_ms','succeeded','build_sha','source','error_class'), 'observations[' . $index . ']');
        $id = isset($item['observation_id']) ? trim((string)$item['observation_id']) : '';
        $route = isset($item['route_template']) ? v1_valid_route_template($item['route_template']) : null;
        $observed = isset($item['observed_at']) ? v1_editorial_datetime_utc($item['observed_at']) : null;
        $sha = isset($item['build_sha']) ? v1_valid_build_sha($item['build_sha']) : null;
        $source = isset($item['source']) ? trim((string)$item['source']) : '';
        $httpStatus = isset($item['http_status']) && is_int($item['http_status']) ? $item['http_status'] : -1;
        $duration = isset($item['duration_ms']) && is_int($item['duration_ms']) ? $item['duration_ms'] : -1;
        $succeeded = isset($item['succeeded']) && is_bool($item['succeeded']) ? ($item['succeeded'] ? 1 : 0) : -1;
        $observedEpoch = $observed !== null ? strtotime($observed . ' UTC') : false;
        $errorClass = array_key_exists('error_class', $item) && $item['error_class'] !== null ? trim((string)$item['error_class']) : '';
        if (!v1_valid_entity_id($id) || isset($seen[$id]) || $route === null || $observed === null || $observedEpoch === false
            || $observedEpoch < $nowEpoch - 172800 || $observedEpoch > $nowEpoch + 300 || !(($httpStatus >= 100 && $httpStatus <= 599) || $httpStatus === 0)
            || $duration < 0 || $duration > 600000 || $succeeded < 0 || $sha === null || $source !== 'github_watchdog'
            || ($errorClass !== '' && preg_match('/^[A-Za-z][A-Za-z0-9_.]{0,63}$/', $errorClass) !== 1)
            || (($httpStatus >= 200 && $httpStatus < 400) ? 1 : 0) !== $succeeded) {
            v1_respond(400, array('ok' => false, 'error' => 'invalid_observation', 'index' => $index));
        }
        $seen[$id] = true;
        $normalized[] = array($id,$observed,$route,$httpStatus,$duration,$succeeded,$sha,$source);
    }
    $now = gmdate('Y-m-d H:i:s'); $inserted = 0;
    $pdo->beginTransaction();
    try {
        $lookup = $pdo->prepare('SELECT observed_at,route_template,http_status,duration_ms,succeeded,build_sha,source FROM '
            . table_name($config, 'availability_observations') . ' WHERE observation_id=? LIMIT 1 FOR UPDATE');
        $insert = $pdo->prepare('INSERT INTO ' . table_name($config, 'availability_observations')
            . ' (observation_id,observed_at,route_template,http_status,duration_ms,succeeded,build_sha,source,created_at) VALUES (?,?,?,?,?,?,?,?,?)');
        foreach ($normalized as $row) {
            $lookup->execute(array($row[0])); $existing = $lookup->fetch();
            if ($existing) {
                $existingValues = array((string)$existing['observed_at'],(string)$existing['route_template'],(int)$existing['http_status'],
                    (int)$existing['duration_ms'],(int)$existing['succeeded'],(string)$existing['build_sha'],(string)$existing['source']);
                if ($existingValues !== array_slice($row, 1)) { throw new RuntimeException('availability_observation_conflict:' . $row[0]); }
                continue;
            }
            $insert->execute(array_merge($row, array($now))); $inserted++;
        }
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        if (strpos($e->getMessage(), 'availability_observation_conflict:') === 0) {
            v1_respond(409, array('ok' => false, 'error' => 'observation_id_conflict'));
        }
        throw $e;
    }
    v1_respond(202, array('ok' => true, 'accepted_count' => count($normalized), 'inserted_count' => $inserted,
        'duplicate_count' => count($normalized) - $inserted, 'role' => $role));
}

function v1_record_web_distribution_observations(PDO $pdo, array $config, string $role): void {
    $payload = v1_admin_json_body($config); v1_assert_object_keys($payload,array('observations'),'body');
    $items = isset($payload['observations']) && is_array($payload['observations']) ? $payload['observations'] : null;
    if ($items === null || count($items) < 1 || count($items) > 50) {
        v1_respond(400,array('ok'=>false,'error'=>'invalid_web_distribution_batch'));
    }
    $normalized = array(); $seen = array(); $nowEpoch = time();
    foreach ($items as $index => $item) {
        if (!is_array($item)) { v1_respond(400,array('ok'=>false,'error'=>'invalid_web_distribution_observation','index'=>$index)); }
        v1_assert_object_keys($item,array('observation_id','observed_at','distribution_target','duration_ms','succeeded','build_sha',
            'workflow_run_id','workflow_run_attempt','failure_detected_at','source'),'observations['.$index.']');
        $id = isset($item['observation_id']) ? trim((string)$item['observation_id']) : '';
        $observed = isset($item['observed_at']) ? v1_editorial_datetime_utc($item['observed_at']) : null;
        $target = isset($item['distribution_target']) ? trim((string)$item['distribution_target']) : '';
        $duration = isset($item['duration_ms']) && is_int($item['duration_ms']) ? $item['duration_ms'] : -1;
        $succeeded = isset($item['succeeded']) && is_bool($item['succeeded']) ? ($item['succeeded'] ? 1 : 0) : -1;
        $sha = isset($item['build_sha']) ? strtolower(trim((string)$item['build_sha'])) : '';
        $runId = isset($item['workflow_run_id']) && is_int($item['workflow_run_id']) ? $item['workflow_run_id'] : 0;
        $runAttempt = isset($item['workflow_run_attempt']) && is_int($item['workflow_run_attempt']) ? $item['workflow_run_attempt'] : 0;
        $failureDetected = array_key_exists('failure_detected_at',$item) && $item['failure_detected_at'] !== null
            ? v1_editorial_datetime_utc($item['failure_detected_at']) : null;
        $source = isset($item['source']) ? trim((string)$item['source']) : '';
        $observedEpoch = $observed !== null ? strtotime($observed . ' UTC') : false;
        $failureEpoch = $failureDetected !== null ? strtotime($failureDetected . ' UTC') : false;
        if (!array_key_exists('failure_detected_at',$item) || !v1_valid_entity_id($id) || isset($seen[$id])
            || $observedEpoch === false || $observedEpoch < $nowEpoch-604800
            || $observedEpoch > $nowEpoch+300 || !in_array($target,array('pages','api'),true) || $duration < 0 || $duration > 3600000
            || $succeeded < 0 || preg_match('/^[a-f0-9]{40}$/',$sha) !== 1 || $runId < 1 || $runAttempt < 1
            || $runAttempt > 10000 || $source !== 'github_actions'
            || ($succeeded === 1 && $failureDetected !== null)
            || ($succeeded === 0 && ($failureEpoch === false || $failureEpoch < $observedEpoch || $failureEpoch > $nowEpoch+300))) {
            v1_respond(400,array('ok'=>false,'error'=>'invalid_web_distribution_observation','index'=>$index));
        }
        $seen[$id] = true;
        $normalized[] = array($id,$observed,$target,$duration,$succeeded,$sha,$runId,$runAttempt,$failureDetected,$source);
    }
    $now = gmdate('Y-m-d H:i:s'); $inserted = 0; $pdo->beginTransaction();
    try {
        $lookup = $pdo->prepare('SELECT observed_at,distribution_target,duration_ms,succeeded,build_sha,workflow_run_id,workflow_run_attempt,failure_detected_at,source FROM '
            . table_name($config,'web_distribution_observations') . ' WHERE observation_id=? LIMIT 1 FOR UPDATE');
        $insert = $pdo->prepare('INSERT INTO ' . table_name($config,'web_distribution_observations')
            . ' (observation_id,observed_at,distribution_target,duration_ms,succeeded,build_sha,workflow_run_id,workflow_run_attempt,failure_detected_at,source,created_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,?,?,?)');
        foreach ($normalized as $row) {
            $lookup->execute(array($row[0])); $existing = $lookup->fetch();
            if ($existing) {
                $existingValues = array((string)$existing['observed_at'],(string)$existing['distribution_target'],(int)$existing['duration_ms'],
                    (int)$existing['succeeded'],(string)$existing['build_sha'],(int)$existing['workflow_run_id'],
                    (int)$existing['workflow_run_attempt'],
                    $existing['failure_detected_at'] !== null ? (string)$existing['failure_detected_at'] : null,(string)$existing['source']);
                if ($existingValues !== array_slice($row,1)) { throw new RuntimeException('web_distribution_observation_conflict'); }
                continue;
            }
            $insert->execute(array_merge($row,array($now))); $inserted++;
        }
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        if (strpos($e->getMessage(),'web_distribution_observation_conflict') === 0 || (string)$e->getCode() === '23000') {
            v1_respond(409,array('ok'=>false,'error'=>'web_distribution_observation_conflict'));
        }
        throw $e;
    }
    v1_respond(202,array('ok'=>true,'accepted_count'=>count($normalized),'inserted_count'=>$inserted,
        'duplicate_count'=>count($normalized)-$inserted,'role'=>$role));
}

/** The shared v2 denominator: distinct document IDs referenced by 2021+ public-approved objects. */
function v1_content_corpus_document_refs_sql(array $config): string {
    return 'SELECT ed.document_id FROM ' . table_name($config,'event_documents') . ' ed '
        . 'JOIN ' . table_name($config,'governance_events') . ' e ON e.event_id=ed.event_id '
        . 'WHERE e.publication_status=\'published\' AND e.review_status=\'approved\' AND e.identity_status=\'complete\' '
        . 'AND e.verification_status<>\'signal\' AND e.created_at<=? AND ed.created_at<=? AND e.occurred_at>=? '
        . 'UNION SELECT cd.document_id FROM ' . table_name($config,'campaign_documents') . ' cd '
        . 'JOIN ' . table_name($config,'campaigns') . ' cp ON cp.campaign_id=cd.campaign_id '
        . 'WHERE cp.publication_status=\'published\' AND cp.review_status=\'approved\' '
        . 'AND cp.created_at<=? AND cd.created_at<=? AND cp.started_at>=? '
        . 'UNION SELECT ce.document_id FROM ' . table_name($config,'claim_evidence') . ' ce '
        . 'JOIN ' . table_name($config,'governance_events') . ' claim_e ON claim_e.event_id=ce.event_id '
        . 'WHERE ce.editorial_status=\'approved\' AND claim_e.publication_status=\'published\' '
        . 'AND claim_e.review_status=\'approved\' AND claim_e.identity_status=\'complete\' '
        . 'AND claim_e.verification_status<>\'signal\' AND ce.created_at<=? AND claim_e.created_at<=? AND claim_e.occurred_at>=? '
        . 'UNION SELECT v.evidence_document_id AS document_id FROM ' . table_name($config,'proposal_votes') . ' v '
        . 'WHERE v.evidence_document_id IS NOT NULL AND v.publication_status=\'published\' AND v.review_status=\'approved\' '
        . 'AND v.created_at<=? AND v.meeting_at>=? '
        . 'UNION SELECT co.evidence_document_id AS document_id FROM ' . table_name($config,'commitment_outcomes') . ' co '
        . 'WHERE co.evidence_document_id IS NOT NULL AND co.publication_status=\'published\' AND co.review_status=\'approved\' '
        . 'AND co.created_at<=? AND COALESCE(co.target_at,co.created_at)>=? '
        . 'UNION SELECT tl.document_id FROM ' . table_name($config,'timeline_entries') . ' tl '
        . 'WHERE tl.document_id IS NOT NULL AND tl.publication_status=\'published\' AND tl.review_status=\'approved\' '
        . 'AND tl.created_at<=? AND tl.occurred_at>=? AND ('
        . '(tl.event_id IS NOT NULL AND EXISTS (SELECT 1 FROM ' . table_name($config,'governance_events') . ' timeline_e '
        . 'WHERE timeline_e.event_id=tl.event_id AND timeline_e.publication_status=\'published\' '
        . 'AND timeline_e.review_status=\'approved\' AND timeline_e.identity_status=\'complete\' '
        . 'AND timeline_e.verification_status<>\'signal\' AND timeline_e.created_at<=?)) OR '
        . '(tl.campaign_id IS NOT NULL AND EXISTS (SELECT 1 FROM ' . table_name($config,'campaigns') . ' timeline_cp '
        . 'WHERE timeline_cp.campaign_id=tl.campaign_id AND timeline_cp.publication_status=\'published\' '
        . 'AND timeline_cp.review_status=\'approved\' AND timeline_cp.created_at<=?)))';
}

function v1_content_corpus_document_refs_params(string $snapshotAt, string $scopeStart): array {
    return array(
        $snapshotAt,$snapshotAt,$scopeStart,
        $snapshotAt,$snapshotAt,$scopeStart,
        $snapshotAt,$snapshotAt,$scopeStart,
        $snapshotAt,$scopeStart,
        $snapshotAt,$scopeStart,
        $snapshotAt,$scopeStart,$snapshotAt,$snapshotAt,
    );
}

function v1_content_document_right_valid_at(array $document, string $at): bool {
    return (string)$document['right_status'] === 'active' && (int)$document['ai_allowed'] === 1
        && (int)$document['redistribution_allowed'] === 1
        && (string)$document['valid_from'] <= $at
        && ($document['valid_until'] === null || (string)$document['valid_until'] > $at)
        && ($document['revoked_at'] === null || (string)$document['revoked_at'] > $at)
        && (trim((string)$document['evidence_uri']) !== ''
            || preg_match('/^[a-f0-9]{64}$/',(string)$document['evidence_hash']) === 1)
        && isset(
            $document['document_source_class'],
            $document['document_source_key'],
            $document['right_source_type'],
            $document['right_source_key']
        )
        && hash_equals(
            (string)$document['right_source_type'],
            (string)$document['document_source_class']
        )
        && hash_equals(
            (string)$document['right_source_key'],
            (string)$document['document_source_key']
        );
}

/** Measure the actual 2021+ in-scope corpus as it stood at one completed KST day end. */
function v1_content_corpus_snapshot(PDO $pdo, array $config, string $date): array {
    list($_dayStart,$snapshotAt) = v1_evidence_utc_bounds($date,$date);
    $scopeStart = '2020-12-31 15:00:00'; // 2021-01-01 00:00:00 KST
    $eventSql = 'SELECT COUNT(*) AS official_evidence_total_count,'
        . 'COALESCE(SUM(EXISTS(SELECT 1 FROM ' . table_name($config,'event_documents') . ' snapshot_ed JOIN '
        . table_name($config,'documents') . ' snapshot_d ON snapshot_d.document_id=snapshot_ed.document_id '
        . 'WHERE snapshot_ed.event_id=e.event_id AND snapshot_d.source_class=\'official_disclosure\' '
        . 'AND snapshot_d.created_at<=?)),0) AS official_evidence_linked_count,'
        . 'COALESCE(SUM(e.importance IN (\'high\',\'critical\',\'market_sensitive\',\'top\')),0) AS top_sensitive_total_count,'
        . 'COALESCE(SUM(e.importance IN (\'high\',\'critical\',\'market_sensitive\',\'top\') '
        . 'AND e.review_status IN (\'approved\',\'reviewed\')),0) AS top_sensitive_reviewed_count '
        . 'FROM ' . table_name($config,'governance_events') . ' e WHERE e.created_at<=? AND e.occurred_at>=? '
        . 'AND e.verification_status<>\'signal\'';
    $eventStmt = $pdo->prepare($eventSql); $eventStmt->execute(array($snapshotAt,$snapshotAt,$scopeStart));
    $eventCounts = $eventStmt->fetch();
    $counts = array(
        'official_evidence_total_count'=>(int)$eventCounts['official_evidence_total_count'],
        'official_evidence_linked_count'=>(int)$eventCounts['official_evidence_linked_count'],
        'top_sensitive_total_count'=>(int)$eventCounts['top_sensitive_total_count'],
        'top_sensitive_reviewed_count'=>(int)$eventCounts['top_sensitive_reviewed_count'],
        'original_language_total_count'=>0,'original_language_preserved_count'=>0,
        'source_right_total_count'=>0,'valid_source_right_count'=>0,
    );
    // The content/rights denominator is the immutable public-object reference
    // corpus, not the subset of documents that still passes today's visibility
    // predicate. A linked document therefore remains in scope after content
    // drift, right expiry or revocation, so those failures cannot shrink the
    // denominator and make the release gate pass.
    $publicDocumentRefs = v1_content_corpus_document_refs_sql($config);
    $documentStmt = $pdo->prepare('SELECT d.retrieved_at,d.original_language,d.title,d.body_text,d.payload_json,'
        . 'd.source_class AS document_source_class,d.source_key AS document_source_key,'
        . 'sr.source_type AS right_source_type,sr.source_key AS right_source_key,'
        . 'sr.status AS right_status,sr.valid_from,sr.valid_until,sr.revoked_at,sr.ai_allowed,sr.redistribution_allowed,'
        . 'sr.evidence_uri,sr.evidence_hash FROM '
        . table_name($config,'documents') . ' d JOIN (' . $publicDocumentRefs . ') public_document_refs '
        . 'ON public_document_refs.document_id=d.document_id LEFT JOIN ' . table_name($config,'source_rights')
        . ' sr ON sr.source_right_id=d.source_right_id WHERE d.created_at<=? ORDER BY d.document_id');
    $documentParams = v1_content_corpus_document_refs_params($snapshotAt,$scopeStart);
    $documentParams[] = $snapshotAt; $documentStmt->execute($documentParams);
    while ($document = $documentStmt->fetch()) {
        $counts['original_language_total_count']++; $counts['source_right_total_count']++;
        $payload = json_decode((string)$document['payload_json'],true); $preserved = is_array($payload);
        if ($preserved) {
            $rawTitlePresent = array_key_exists('title',$payload) || array_key_exists('report_nm',$payload);
            $rawLanguagePresent = array_key_exists('original_language',$payload) || array_key_exists('language',$payload);
            $rawTitle = array_key_exists('title',$payload) ? (string)$payload['title'] : (string)($payload['report_nm'] ?? '');
            $rawLanguage = (string)($payload['original_language'] ?? ($payload['language'] ?? ''));
            $rawBodyPresent = array_key_exists('body_text',$payload) || array_key_exists('content',$payload);
            $rawBody = array_key_exists('body_text',$payload) ? (string)$payload['body_text'] : (string)($payload['content'] ?? '');
            $storedBodyPresent = $document['body_text'] !== null && (string)$document['body_text'] !== '';
            $preserved = $rawTitlePresent && $rawLanguagePresent && $rawTitle === (string)$document['title']
                && $rawLanguage === (string)$document['original_language']
                && ($rawBodyPresent ? $rawBody === (string)($document['body_text'] ?? '') : !$storedBodyPresent);
        }
        if ($preserved) { $counts['original_language_preserved_count']++; }
        if (v1_content_document_right_valid_at($document,$snapshotAt)) { $counts['valid_source_right_count']++; }
    }
    return array('snapshot_at'=>$snapshotAt,'snapshot_at_iso'=>v1_release_iso_time($snapshotAt),
        'content_scope'=>'governance_corpus_2021_plus_kst_day_end_v2','raw_counts'=>$counts);
}

/**
 * Re-check all v2 corpus rights at cutover time.
 *
 * The caller holds the release-state row first. Every in-process SourceRight
 * writer takes that same lock before changing rights, which serializes this
 * current read without introducing a rights-row -> state-row lock inversion.
 */
function v1_current_public_document_rights_guard(PDO $pdo, array $config): array {
    $checkedAt = gmdate('Y-m-d H:i:s');
    $scopeStart = '2020-12-31 15:00:00';
    $refs = v1_content_corpus_document_refs_sql($config);
    $stmt = $pdo->prepare('SELECT d.document_id,'
        . 'd.source_class AS document_source_class,d.source_key AS document_source_key,'
        . 'sr.source_type AS right_source_type,sr.source_key AS right_source_key,'
        . 'sr.status AS right_status,sr.valid_from,sr.valid_until,sr.revoked_at,'
        . 'sr.ai_allowed,sr.redistribution_allowed,sr.evidence_uri,sr.evidence_hash FROM '
        . table_name($config,'documents') . ' d JOIN (' . $refs . ') current_public_document_refs '
        . 'ON current_public_document_refs.document_id=d.document_id LEFT JOIN ' . table_name($config,'source_rights')
        . ' sr ON sr.source_right_id=d.source_right_id WHERE d.created_at<=? ORDER BY d.document_id');
    $params = v1_content_corpus_document_refs_params($checkedAt,$scopeStart);
    $params[] = $checkedAt; $stmt->execute($params);
    $total = 0; $invalid = 0;
    while ($document = $stmt->fetch()) {
        $total++;
        if (!v1_content_document_right_valid_at($document,$checkedAt)) { $invalid++; }
    }
    return array('checked_at'=>$checkedAt,'total_count'=>$total,'invalid_count'=>$invalid);
}

function v1_quality_observation_payload_hash(string $date, string $revision, float $dartPoll, ?float $kindLag,
    int $kindObservationCount, int $kindLagSampleCount, string $contentSnapshotAt, string $contentScope,
    array $counts, string $source): string {
    $canonical = json_encode(array('observation_date'=>$date,'code_revision'=>$revision,
        'dart_success_poll_interval_p95_minutes'=>$dartPoll,'kind_observation_lag_p95_minutes'=>$kindLag,
        'kind_observation_count'=>$kindObservationCount,'kind_lag_sample_count'=>$kindLagSampleCount,
        'content_snapshot_at'=>$contentSnapshotAt,'content_scope'=>$contentScope,'raw_counts'=>$counts,'source'=>$source),
        JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES|JSON_PRESERVE_ZERO_FRACTION);
    if (!is_string($canonical)) { throw new RuntimeException('quality_observation_json_encode_failed'); }
    return hash('sha256',$canonical);
}

function v1_record_quality_observations(PDO $pdo, array $config, string $role): void {
    $payload = v1_admin_json_body($config); v1_assert_object_keys($payload,array('observations'),'body');
    $items = isset($payload['observations']) && is_array($payload['observations']) ? $payload['observations'] : null;
    if ($items === null || count($items) < 1 || count($items) > 10) {
        v1_respond(400,array('ok'=>false,'error'=>'invalid_quality_observation_batch'));
    }
    $countFields = array('official_evidence_total_count','official_evidence_linked_count','top_sensitive_total_count','top_sensitive_reviewed_count',
        'original_language_total_count','original_language_preserved_count','source_right_total_count','valid_source_right_count');
    $databaseCountFields = array('official_evidence_total_count','official_evidence_linked_count','same_story_evaluated_pair_count',
        'same_story_predicted_same_count','same_story_true_positive_count','top_sensitive_total_count','top_sensitive_reviewed_count',
        'original_language_total_count','original_language_preserved_count','source_right_total_count','valid_source_right_count');
    $normalized = array(); $seen = array();
    foreach ($items as $index => $item) {
        if (!is_array($item)) { v1_respond(400,array('ok'=>false,'error'=>'invalid_quality_observation','index'=>$index)); }
        v1_assert_object_keys($item,array('observation_id','observation_date','code_revision','dart_success_poll_interval_p95_minutes',
            'kind_observation_lag_p95_minutes','raw_counts','source'),'observations['.$index.']');
        $id = isset($item['observation_id']) ? trim((string)$item['observation_id']) : '';
        $date = isset($item['observation_date']) ? trim((string)$item['observation_date']) : '';
        $sha = isset($item['code_revision']) ? strtolower(trim((string)$item['code_revision'])) : '';
        $dartPoll = $item['dart_success_poll_interval_p95_minutes'] ?? null;
        $kindLag = $item['kind_observation_lag_p95_minutes'] ?? null;
        $counts = isset($item['raw_counts']) && is_array($item['raw_counts']) ? $item['raw_counts'] : null;
        $source = isset($item['source']) ? trim((string)$item['source']) : '';
        $parsedDate = DateTimeImmutable::createFromFormat('!Y-m-d',$date,new DateTimeZone('UTC'));
        if (!array_key_exists('kind_observation_lag_p95_minutes',$item) || !v1_valid_entity_id($id) || isset($seen[$id])
            || preg_match('/^\d{4}-\d{2}-\d{2}$/',$date) !== 1
            || !$parsedDate || $parsedDate->format('Y-m-d') !== $date
            || preg_match('/^[a-f0-9]{40}$/',$sha) !== 1 || (!is_int($dartPoll) && !is_float($dartPoll))
            || ($kindLag !== null && !is_int($kindLag) && !is_float($kindLag)) || !is_finite((float)$dartPoll)
            || ($kindLag !== null && !is_finite((float)$kindLag))
            || (float)$dartPoll < 0 || ($kindLag !== null && (float)$kindLag < 0) || (float)$dartPoll > 10080
            || ($kindLag !== null && (float)$kindLag > 10080)
            || $counts === null || $source !== 'production_quality_job') {
            v1_respond(400,array('ok'=>false,'error'=>'invalid_quality_observation','index'=>$index));
        }
        $todayKst = (new DateTimeImmutable('now',new DateTimeZone('Asia/Seoul')))->format('Y-m-d');
        if ($date >= $todayKst) {
            v1_respond(400,array('ok'=>false,'error'=>'completed_kst_day_required','index'=>$index));
        }
        v1_assert_object_keys($counts,$countFields,'observations['.$index.'].raw_counts');
        $orderedCounts = array();
        foreach ($countFields as $field) {
            if (!array_key_exists($field,$counts) || !is_int($counts[$field]) || $counts[$field] < 0) {
                v1_respond(400,array('ok'=>false,'error'=>'invalid_quality_count','field'=>$field,'index'=>$index));
            }
            $orderedCounts[$field] = $counts[$field];
        }
        foreach (array(array('official_evidence_linked_count','official_evidence_total_count'),
            array('top_sensitive_reviewed_count','top_sensitive_total_count'),
            array('original_language_preserved_count','original_language_total_count'),
            array('valid_source_right_count','source_right_total_count')) as $pair) {
            if ($orderedCounts[$pair[0]] > $orderedCounts[$pair[1]]) {
                v1_respond(400,array('ok'=>false,'error'=>'quality_numerator_exceeds_denominator','field'=>$pair[0],'index'=>$index));
            }
        }
        $snapshot = v1_content_corpus_snapshot($pdo,$config,$date);
        if ($orderedCounts !== $snapshot['raw_counts']) {
            v1_respond(409,array('ok'=>false,'error'=>'quality_counts_not_actual','index'=>$index,
                'content_snapshot_at'=>$snapshot['snapshot_at_iso'],'actual_raw_counts'=>$snapshot['raw_counts']));
        }
        $kindStatsByDay = v1_kind_observation_stats_by_day($pdo,$config,$date,$date);
        $kindStats = isset($kindStatsByDay[$date]) ? $kindStatsByDay[$date]
            : array('observation_count'=>0,'lag_sample_count'=>0,'lag_seconds'=>array());
        $kindObservationCount = (int)$kindStats['observation_count'];
        $kindLagSampleCount = (int)$kindStats['lag_sample_count'];
        $actualKindSeconds = v1_percentile($kindStats['lag_seconds'],0.95);
        $actualKindLag = $actualKindSeconds === null ? null : $actualKindSeconds/60.0;
        if ($kindObservationCount === 0 && ($kindLag !== null || $kindLagSampleCount !== 0)) {
            v1_respond(409,array('ok'=>false,'error'=>'kind_no_disclosure_day_requires_null_lag','index'=>$index));
        }
        if ($kindObservationCount > 0 && $kindLagSampleCount !== $kindObservationCount) {
            v1_respond(409,array('ok'=>false,'error'=>'kind_observation_timestamp_incomplete','index'=>$index,
                'kind_observation_count'=>$kindObservationCount,'kind_lag_sample_count'=>$kindLagSampleCount));
        }
        if ($kindObservationCount > 0 && ($kindLag === null || $actualKindLag === null
            || abs((float)$kindLag-$actualKindLag) > 0.0001)) {
            v1_respond(409,array('ok'=>false,'error'=>'kind_observation_lag_not_actual','index'=>$index,
                'actual_kind_observation_lag_p95_minutes'=>$actualKindLag));
        }
        // Match the DECIMAL(12,4) storage representation before hashing so a
        // successful write cannot fail its own later integrity check.
        $dartPollValue = round((float)$dartPoll,4);
        $kindLagValue = $kindLag === null ? null : round((float)$kindLag,4);
        $seen[$id] = true; $normalized[] = array($id,$date,$sha,$dartPollValue,$kindLagValue,$kindObservationCount,
            $kindLagSampleCount,(string)$snapshot['snapshot_at'],(string)$snapshot['content_scope'],$orderedCounts,$source,
            v1_quality_observation_payload_hash($date,$sha,$dartPollValue,$kindLagValue,$kindObservationCount,$kindLagSampleCount,
                (string)$snapshot['snapshot_at'],(string)$snapshot['content_scope'],$orderedCounts,$source));
    }
    $now = gmdate('Y-m-d H:i:s'); $inserted = 0; $pdo->beginTransaction();
    try {
        $lookup = $pdo->prepare('SELECT payload_sha256 FROM ' . table_name($config,'governance_quality_observations')
            . ' WHERE observation_id=? LIMIT 1 FOR UPDATE');
        $insert = $pdo->prepare('INSERT INTO ' . table_name($config,'governance_quality_observations')
            . ' (observation_id,observation_date,code_revision,dart_success_poll_interval_p95_minutes,kind_observation_lag_p95_minutes,'
            . 'kind_observation_count,kind_lag_sample_count,content_snapshot_at,content_scope,'
            . implode(',',$databaseCountFields) . ',source,payload_sha256,created_by,created_at) VALUES (' . implode(',',array_fill(0,24,'?')) . ')');
        foreach ($normalized as $row) {
            $lookup->execute(array($row[0])); $existing = $lookup->fetch();
            if ($existing) {
                if (!hash_equals((string)$existing['payload_sha256'],$row[11])) { throw new RuntimeException('quality_observation_conflict'); }
                continue;
            }
            $values = array($row[0],$row[1],$row[2],$row[3],$row[4],$row[5],$row[6],$row[7],$row[8]);
            foreach ($databaseCountFields as $field) {
                $values[] = strpos($field,'same_story_') === 0 ? 0 : $row[9][$field];
            }
            $values[] = $row[10]; $values[] = $row[11]; $values[] = $role; $values[] = $now;
            $insert->execute($values); $inserted++;
        }
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        if (strpos($e->getMessage(),'quality_observation_conflict') === 0 || (string)$e->getCode() === '23000') {
            v1_respond(409,array('ok'=>false,'error'=>'quality_observation_conflict'));
        }
        throw $e;
    }
    v1_respond(202,array('ok'=>true,'accepted_count'=>count($normalized),'inserted_count'=>$inserted,
        'duplicate_count'=>count($normalized)-$inserted,'role'=>$role));
}

function v1_record_web_vitals(PDO $pdo, array $config): void {
    $payload = v1_admin_json_body($config);
    $singleFields = array('route_template','measured_at','metric_name','metric_value','metric','value','device_class','build_sha','source');
    if (isset($payload['observations'])) {
        v1_assert_object_keys($payload, array('observations'), 'body');
        $items = is_array($payload['observations']) ? $payload['observations'] : null;
    } else {
        v1_assert_object_keys($payload, $singleFields, 'body');
        $items = array($payload);
    }
    if ($items === null || count($items) < 1 || count($items) > 50) { v1_respond(400, array('ok' => false, 'error' => 'invalid_web_vital_batch')); }
    if (scalar_int($pdo, 'SELECT COUNT(*) FROM ' . table_name($config, 'web_vital_observations') . ' WHERE created_at >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL 1 MINUTE)') >= 2000) {
        header('Retry-After: 60'); v1_respond(429, array('ok' => false, 'error' => 'web_vitals_rate_limited'));
    }
    $normalized = array(); $nowEpoch = time();
    foreach ($items as $index => $item) {
        if (!is_array($item)) { v1_respond(400, array('ok' => false, 'error' => 'invalid_web_vital', 'index' => $index)); }
        v1_assert_object_keys($item, $singleFields, 'observations[' . $index . ']');
        $route = isset($item['route_template']) ? v1_valid_route_template($item['route_template']) : null;
        $measured = isset($item['measured_at']) ? v1_editorial_datetime_utc($item['measured_at']) : gmdate('Y-m-d H:i:s');
        $metricValue = array_key_exists('metric_name', $item) ? $item['metric_name'] : (isset($item['metric']) ? $item['metric'] : '');
        $metric = strtoupper(trim((string)$metricValue));
        $rawValue = array_key_exists('metric_value', $item) ? $item['metric_value'] : (array_key_exists('value', $item) ? $item['value'] : null);
        $value = is_int($rawValue) || is_float($rawValue) ? (float)$rawValue : -1;
        $device = isset($item['device_class']) ? trim((string)$item['device_class']) : '';
        $sha = isset($item['build_sha']) ? v1_valid_build_sha($item['build_sha']) : null;
        $source = isset($item['source']) ? trim((string)$item['source']) : 'first_party';
        $measuredEpoch = $measured !== null ? strtotime($measured . ' UTC') : false;
        $maxValue = $metric === 'CLS' ? 100 : 600000;
        if ($route === null || $measured === null || $measuredEpoch === false || $measuredEpoch < $nowEpoch - 86400 || $measuredEpoch > $nowEpoch + 300
            || !in_array($metric, array('LCP','INP','CLS'), true) || !is_finite($value) || $value < 0 || $value > $maxValue
            || !in_array($device, array('mobile','desktop','tablet'), true) || $sha === null || $source !== 'first_party') {
            v1_respond(400, array('ok' => false, 'error' => 'invalid_web_vital', 'index' => $index));
        }
        $normalized[] = array('wv_' . bin2hex(random_bytes(16)),$measured,$route,$metric,$value,$device,$sha,$source);
    }
    $now = gmdate('Y-m-d H:i:s');
    $pdo->beginTransaction();
    try {
        $pdo->exec('DELETE FROM ' . table_name($config, 'web_vital_observations') . ' WHERE expires_at <= UTC_TIMESTAMP() LIMIT 5000');
        $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'web_vital_observations')
            . ' (metric_id,measured_at,route_template,metric_name,metric_value,device_class,build_sha,source,expires_at,created_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,DATE_ADD(?, INTERVAL 30 DAY),?)');
        foreach ($normalized as $row) { $stmt->execute(array_merge($row, array($row[1],$now))); }
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    v1_respond(202, array('ok' => true, 'accepted_count' => count($normalized), 'retention_days' => 30, 'stored_identifiers' => false));
}

function v1_ops_get_backfill_checkpoint(PDO $pdo, array $config, string $fingerprint): void {
    $stmt = $pdo->prepare('SELECT job_fingerprint,checkpoint_version,checkpoint_json,payload_hash,updated_at FROM '
        . table_name($config, 'official_backfill_checkpoints') . ' WHERE job_fingerprint=? LIMIT 1');
    $stmt->execute(array($fingerprint)); $row = $stmt->fetch();
    if (!$row) { v1_respond(404, array('ok'=>false,'error'=>'backfill_checkpoint_not_found','job_fingerprint'=>$fingerprint)); }
    $checkpoint = json_decode((string)$row['checkpoint_json']);
    if (!is_object($checkpoint) || hash('sha256', (string)$row['checkpoint_json']) !== (string)$row['payload_hash']) {
        v1_respond(503, array('ok'=>false,'error'=>'backfill_checkpoint_integrity_error','job_fingerprint'=>$fingerprint));
    }
    v1_respond(200, array('ok'=>true,'job_fingerprint'=>$fingerprint,'checkpoint_version'=>(int)$row['checkpoint_version'],
        'payload_hash'=>(string)$row['payload_hash'],'updated_at'=>v1_release_iso_time($row['updated_at']),'checkpoint'=>$checkpoint));
}

function v1_json_object_body(array $config): object {
    $contentType = isset($_SERVER['CONTENT_TYPE']) ? strtolower((string)$_SERVER['CONTENT_TYPE']) : '';
    if (strpos($contentType, 'application/json') !== 0) { v1_respond(415, array('ok'=>false,'error'=>'application_json_required')); }
    $decoded = json_decode(read_body($config));
    if (!is_object($decoded) || json_last_error() !== JSON_ERROR_NONE) { v1_respond(400, array('ok'=>false,'error'=>'invalid_json_object')); }
    return $decoded;
}

function v1_canonical_json_node($value) {
    if (is_object($value)) {
        $properties = get_object_vars($value);
        ksort($properties, SORT_STRING);
        $canonical = new stdClass();
        foreach ($properties as $key => $child) { $canonical->{$key} = v1_canonical_json_node($child); }
        return $canonical;
    }
    if (is_array($value)) {
        $canonical = array();
        foreach ($value as $child) { $canonical[] = v1_canonical_json_node($child); }
        return $canonical;
    }
    return $value;
}

/**
 * Canonical JSON for cross-runtime receipts.
 *
 * The older checkpoint canonicalizer intentionally treats every PHP array as
 * a JSON list.  External Python producers, however, hash JSON objects with
 * sorted keys.  Keep the checkpoint contract stable and use this stricter
 * encoder only for new cross-runtime evidence/receipt contracts.
 */
function v1_strict_canonical_json_node($value) {
    if (is_object($value)) {
        $properties = get_object_vars($value);
        ksort($properties, SORT_STRING);
        $canonical = new stdClass();
        foreach ($properties as $key => $child) { $canonical->{$key} = v1_strict_canonical_json_node($child); }
        return $canonical;
    }
    if (is_array($value)) {
        $keys = array_keys($value);
        $isList = count($keys) === 0 || $keys === range(0, count($keys) - 1);
        if ($isList) {
            $canonical = array();
            foreach ($value as $child) { $canonical[] = v1_strict_canonical_json_node($child); }
            return $canonical;
        }
        ksort($value, SORT_STRING);
        $canonical = new stdClass();
        foreach ($value as $key => $child) { $canonical->{$key} = v1_strict_canonical_json_node($child); }
        return $canonical;
    }
    return $value;
}

function v1_ops_put_backfill_checkpoint(PDO $pdo, array $config, string $fingerprint, string $role): void {
    $payloadObject = v1_json_object_body($config);
    $payload = get_object_vars($payloadObject);
    v1_assert_object_keys($payload, array('expected_version','checkpoint'), 'body');
    if (!isset($payload['expected_version']) || !is_int($payload['expected_version']) || $payload['expected_version'] < 0
        || !isset($payload['checkpoint']) || !is_object($payload['checkpoint'])) {
        v1_respond(400, array('ok'=>false,'error'=>'invalid_backfill_checkpoint_request'));
    }
    $checkpoint = $payload['checkpoint']; $checkpointValues = get_object_vars($checkpoint);
    $job = isset($checkpointValues['job']) && is_object($checkpointValues['job']) ? get_object_vars($checkpointValues['job']) : array();
    if (!isset($checkpointValues['schema_version']) || !is_int($checkpointValues['schema_version']) || $checkpointValues['schema_version'] < 1
        || !$job || !isset($job['fingerprint']) || !hash_equals($fingerprint, (string)$job['fingerprint'])) {
        v1_respond(400, array('ok'=>false,'error'=>'checkpoint_fingerprint_mismatch'));
    }
    foreach (array('completed_windows','failed_windows') as $mapField) {
        if (!isset($checkpointValues[$mapField]) || !is_object($checkpointValues[$mapField])) {
            v1_respond(400, array('ok'=>false,'error'=>'checkpoint_window_map_required','field'=>$mapField));
        }
    }
    $canonicalCheckpoint = v1_canonical_json_node($checkpoint);
    $encoded = json_encode($canonicalCheckpoint, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION);
    if ($encoded === false || strlen($encoded) > 8000000) { v1_respond(413, array('ok'=>false,'error'=>'checkpoint_payload_too_large')); }
    $hash = hash('sha256', $encoded); $expected = (int)$payload['expected_version']; $now = gmdate('Y-m-d H:i:s');
    $pdo->beginTransaction();
    try {
        $lookup = $pdo->prepare('SELECT checkpoint_version,payload_hash FROM ' . table_name($config, 'official_backfill_checkpoints')
            . ' WHERE job_fingerprint=? LIMIT 1 FOR UPDATE');
        $lookup->execute(array($fingerprint)); $existing = $lookup->fetch();
        $actual = $existing ? (int)$existing['checkpoint_version'] : 0;
        if ($actual !== $expected) {
            $pdo->rollBack();
            v1_respond(409, array('ok'=>false,'error'=>'backfill_checkpoint_version_conflict','job_fingerprint'=>$fingerprint,
                'expected_version'=>$expected,'actual_version'=>$actual));
        }
        if ($existing && hash_equals((string)$existing['payload_hash'], $hash)) {
            $pdo->commit();
            v1_respond(200, array('ok'=>true,'job_fingerprint'=>$fingerprint,'checkpoint_version'=>$actual,
                'payload_hash'=>$hash,'unchanged'=>true,'updated_at'=>str_replace(' ','T',$now).'Z'));
        }
        $next = $actual + 1;
        if ($existing) {
            $stmt = $pdo->prepare('UPDATE ' . table_name($config, 'official_backfill_checkpoints')
                . ' SET checkpoint_version=?,checkpoint_json=?,payload_hash=?,updated_by=?,updated_at=? WHERE job_fingerprint=?');
            $stmt->execute(array($next,$encoded,$hash,$role,$now,$fingerprint));
        } else {
            $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'official_backfill_checkpoints')
                . ' (job_fingerprint,checkpoint_version,checkpoint_json,payload_hash,updated_by,created_at,updated_at) VALUES (?,?,?,?,?,?,?)');
            $stmt->execute(array($fingerprint,$next,$encoded,$hash,$role,$now,$now));
        }
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    v1_respond($existing ? 200 : 201, array('ok'=>true,'job_fingerprint'=>$fingerprint,'checkpoint_version'=>$next,
        'payload_hash'=>$hash,'unchanged'=>false,'updated_at'=>str_replace(' ','T',$now).'Z'));
}

function v1_canonical_json_encode($value, string $error): string {
    $encoded = json_encode(v1_canonical_json_node($value),
        JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION);
    if (!is_string($encoded)) { throw new RuntimeException($error); }
    return $encoded;
}

function v1_strict_canonical_json_encode($value, string $error): string {
    $encoded = json_encode(v1_strict_canonical_json_node($value),
        JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES | JSON_PRESERVE_ZERO_FRACTION);
    if (!is_string($encoded)) { throw new RuntimeException($error); }
    return $encoded;
}

function v1_object_property(object $value, string $key) {
    $properties = get_object_vars($value);
    return array_key_exists($key,$properties) ? $properties[$key] : null;
}

function v1_validate_human_evidence_document(object $document, string $kind, string $expectedRevision): void {
    $evidence = $kind === 'benchmark' ? v1_object_property($document,'evidence') : $document;
    if (!is_object($evidence)) { v1_respond(400,array('ok'=>false,'error'=>'human_evidence_provenance_missing','kind'=>$kind)); }
    $revision = strtolower(trim((string)v1_object_property($evidence,'code_revision')));
    $environment = trim((string)v1_object_property($evidence,'environment'));
    $source = strtolower(trim((string)v1_object_property($evidence,'evidence_source')));
    $synthetic = v1_object_property($evidence,'is_synthetic');
    if (!hash_equals($expectedRevision,$revision) || $environment !== 'production' || $synthetic !== false || $source === ''
        || preg_match('/(?:fixture|synthetic|sample|test)/',$source) === 1) {
        v1_respond(400,array('ok'=>false,'error'=>'invalid_human_evidence_provenance','kind'=>$kind));
    }
    if ($kind === 'release_approval') {
        $approvedRevision = strtolower(trim((string)v1_object_property($document,'approved_revision')));
        if (!hash_equals($expectedRevision,$approvedRevision) || !is_bool(v1_object_property($document,'release_approved'))) {
            v1_respond(400,array('ok'=>false,'error'=>'invalid_release_approval_revision'));
        }
    }
}

function v1_human_evidence_row_response(array $row, bool $includeDocuments): array {
    $bundleCanonical = v1_canonical_json_encode((object)array(
        'benchmark_sha256'=>(string)$row['benchmark_sha256'],
        'code_revision'=>(string)$row['code_revision'],
        'release_approval_sha256'=>(string)$row['release_approval_sha256'],
        'usability_sha256'=>(string)$row['usability_sha256'],
    ),'human_evidence_bundle_hash_failed');
    if (!hash_equals((string)$row['bundle_sha256'],hash('sha256',$bundleCanonical))) {
        throw new RuntimeException('human_release_evidence_integrity_error:bundle');
    }
    $response = array('code_revision'=>(string)$row['code_revision'],'bundle_version'=>(int)$row['bundle_version'],
        'bundle_sha256'=>(string)$row['bundle_sha256'],
        'document_sha256'=>array('benchmark'=>(string)$row['benchmark_sha256'],'usability'=>(string)$row['usability_sha256'],
            'release_approval'=>(string)$row['release_approval_sha256']),
        'created_by'=>(string)$row['created_by'],'created_at'=>v1_release_iso_time($row['created_at']));
    if ($includeDocuments) {
        foreach (array('benchmark','usability','release_approval') as $kind) {
            $decoded = json_decode((string)$row[$kind . '_json']);
            if (!is_object($decoded) || hash('sha256',(string)$row[$kind . '_json']) !== (string)$row[$kind . '_sha256']) {
                throw new RuntimeException('human_release_evidence_integrity_error:' . $kind);
            }
            $response[$kind] = $decoded;
        }
    }
    return $response;
}

function v1_load_human_evidence_bundle(PDO $pdo, array $config, string $revision): ?array {
    $stmt = $pdo->prepare('SELECT code_revision,bundle_version,bundle_sha256,benchmark_json,benchmark_sha256,usability_json,usability_sha256,'
        . 'release_approval_json,release_approval_sha256,created_by,created_at FROM '
        . table_name($config,'human_release_evidence_bundles') . ' WHERE code_revision=? ORDER BY bundle_version DESC LIMIT 1');
    $stmt->execute(array($revision)); $row = $stmt->fetch();
    return is_array($row) ? $row : null;
}

function v1_admin_release_evidence_inputs(PDO $pdo, array $config): void {
    $revision = isset($_GET['code_revision']) ? strtolower(trim((string)$_GET['code_revision'])) : '';
    if (preg_match('/^[a-f0-9]{40}$/',$revision) !== 1) {
        v1_respond(400,array('ok'=>false,'error'=>'full_code_revision_required'));
    }
    $row = v1_load_human_evidence_bundle($pdo,$config,$revision);
    if (!$row) { v1_respond(404,array('ok'=>false,'error'=>'human_release_evidence_not_found','code_revision'=>$revision)); }
    try { $bundle = v1_human_evidence_row_response($row,true); }
    catch (Throwable $e) { v1_respond(503,array('ok'=>false,'error'=>'human_release_evidence_integrity_error')); }
    v1_respond(200,array('ok'=>true,'bundle'=>$bundle));
}

function v1_admin_upsert_release_evidence_inputs(PDO $pdo, array $config, string $role): void {
    $payloadObject = v1_json_object_body($config); $payload = get_object_vars($payloadObject);
    v1_assert_object_keys($payload,array('code_revision','expected_version','benchmark','usability','release_approval'),'body');
    $revision = isset($payload['code_revision']) ? strtolower(trim((string)$payload['code_revision'])) : '';
    $expected = isset($payload['expected_version']) && is_int($payload['expected_version']) ? $payload['expected_version'] : -1;
    if (preg_match('/^[a-f0-9]{40}$/',$revision) !== 1 || $expected < 0
        || !isset($payload['benchmark']) || !is_object($payload['benchmark'])
        || !isset($payload['usability']) || !is_object($payload['usability'])
        || !isset($payload['release_approval']) || !is_object($payload['release_approval'])) {
        v1_respond(400,array('ok'=>false,'error'=>'invalid_human_release_evidence_bundle'));
    }
    v1_validate_human_evidence_document($payload['benchmark'],'benchmark',$revision);
    v1_validate_human_evidence_document($payload['usability'],'usability',$revision);
    v1_validate_human_evidence_document($payload['release_approval'],'release_approval',$revision);
    $documents = array(); $hashes = array(); $totalBytes = 0;
    foreach (array('benchmark','usability','release_approval') as $kind) {
        $documents[$kind] = v1_canonical_json_encode($payload[$kind],'human_evidence_json_encode_failed:' . $kind);
        $hashes[$kind] = hash('sha256',$documents[$kind]); $totalBytes += strlen($documents[$kind]);
    }
    if ($totalBytes > 220000) { v1_respond(413,array('ok'=>false,'error'=>'human_release_evidence_bundle_too_large','max_bytes'=>220000)); }
    $bundleCanonical = v1_canonical_json_encode((object)array('benchmark_sha256'=>$hashes['benchmark'],
        'code_revision'=>$revision,'release_approval_sha256'=>$hashes['release_approval'],'usability_sha256'=>$hashes['usability']),
        'human_evidence_bundle_hash_failed');
    $bundleHash = hash('sha256',$bundleCanonical); $now = gmdate('Y-m-d H:i:s');
    $pdo->beginTransaction();
    try {
        $lookup = $pdo->prepare('SELECT bundle_version,bundle_sha256 FROM ' . table_name($config,'human_release_evidence_bundles')
            . ' WHERE code_revision=? ORDER BY bundle_version DESC LIMIT 1 FOR UPDATE');
        $lookup->execute(array($revision)); $existing = $lookup->fetch(); $actual = $existing ? (int)$existing['bundle_version'] : 0;
        if ($actual !== $expected) {
            $pdo->rollBack(); v1_respond(409,array('ok'=>false,'error'=>'human_release_evidence_version_conflict',
                'expected_version'=>$expected,'actual_version'=>$actual));
        }
        if ($existing && hash_equals((string)$existing['bundle_sha256'],$bundleHash)) {
            $pdo->commit(); v1_respond(200,array('ok'=>true,'unchanged'=>true,'code_revision'=>$revision,
                'bundle_version'=>$actual,'bundle_sha256'=>$bundleHash));
        }
        $next = $actual + 1;
        $insert = $pdo->prepare('INSERT INTO ' . table_name($config,'human_release_evidence_bundles')
            . ' (code_revision,bundle_version,bundle_sha256,benchmark_json,benchmark_sha256,usability_json,usability_sha256,'
            . 'release_approval_json,release_approval_sha256,created_by,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)');
        $insert->execute(array($revision,$next,$bundleHash,$documents['benchmark'],$hashes['benchmark'],$documents['usability'],$hashes['usability'],
            $documents['release_approval'],$hashes['release_approval'],$role,$now));
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    v1_respond(201,array('ok'=>true,'unchanged'=>false,'code_revision'=>$revision,'bundle_version'=>$next,
        'bundle_sha256'=>$bundleHash,'document_sha256'=>$hashes,'created_at'=>str_replace(' ','T',$now).'Z'));
}

function v1_kst_observation_date($utcValue): ?string {
    if (!is_string($utcValue) || trim($utcValue) === '') { return null; }
    try {
        $utc = new DateTimeZone('UTC'); $kst = new DateTimeZone('Asia/Seoul');
        return (new DateTimeImmutable($utcValue,$utc))->setTimezone($kst)->format('Y-m-d');
    } catch (Throwable $e) { return null; }
}

function v1_evidence_utc_bounds(string $from, string $to): array {
    $kst = new DateTimeZone('Asia/Seoul'); $utc = new DateTimeZone('UTC');
    $start = (new DateTimeImmutable($from . ' 00:00:00',$kst))->setTimezone($utc);
    $end = (new DateTimeImmutable($to . ' 00:00:00',$kst))->modify('+1 day')->modify('-1 second')->setTimezone($utc);
    return array($start->format('Y-m-d H:i:s'),$end->format('Y-m-d H:i:s'));
}

/**
 * Attribute an actual watchdog timestamp to the most recent KST minute-01
 * five-minute slot.  The 23:56 slot owns 00:00:00..00:00:59 of the next
 * civil day, so the resulting observation_date is the slot's KST date.
 */
function v1_availability_cadence_bucket($utcValue): ?array {
    if (!is_string($utcValue) || trim($utcValue) === '') { return null; }
    try {
        $utc = new DateTimeZone('UTC'); $kst = new DateTimeZone('Asia/Seoul');
        $instant = new DateTimeImmutable($utcValue,$utc);
        $local = $instant->setTimezone($kst);
        $minuteOfDay = ((int)$local->format('H')) * 60 + (int)$local->format('i');
        if ($minuteOfDay === 0) {
            $day = $local->modify('-1 day')->format('Y-m-d');
            $slotIndex = GOV_V1_AVAILABILITY_SLOTS_PER_DAY - 1;
        } else {
            $day = $local->format('Y-m-d');
            $slotIndex = intdiv($minuteOfDay - 1,5);
        }
        if ($slotIndex < 0 || $slotIndex >= GOV_V1_AVAILABILITY_SLOTS_PER_DAY) { return null; }
        return array('observation_date'=>$day,'slot_index'=>$slotIndex,'epoch'=>$instant->getTimestamp());
    } catch (Throwable $e) { return null; }
}

/** Exact raw timestamp window for KST minute-01 cadence days, inclusive. */
function v1_availability_utc_bounds(string $from, string $to): array {
    $kst = new DateTimeZone('Asia/Seoul'); $utc = new DateTimeZone('UTC');
    $start = (new DateTimeImmutable($from . ' 00:01:00',$kst))->setTimezone($utc);
    $end = (new DateTimeImmutable($to . ' 00:01:00',$kst))->modify('+1 day')->modify('-1 second')->setTimezone($utc);
    return array($start->format('Y-m-d H:i:s'),$end->format('Y-m-d H:i:s'));
}

/** Encode chronological slot indexes as a fixed 288-bit (72 hex) bitmap. */
function v1_availability_bitmap_hex(array $slotIndexes): string {
    $bytes = array_fill(0,36,0);
    foreach ($slotIndexes as $slotIndex) {
        $slotIndex = (int)$slotIndex;
        if ($slotIndex < 0 || $slotIndex >= GOV_V1_AVAILABILITY_SLOTS_PER_DAY) { continue; }
        $byteIndex = intdiv($slotIndex,8); $bitIndex = 7 - ($slotIndex % 8);
        $bytes[$byteIndex] = $bytes[$byteIndex] | (1 << $bitIndex);
    }
    $hex = '';
    foreach ($bytes as $byte) { $hex .= str_pad(dechex($byte),2,'0',STR_PAD_LEFT); }
    return $hex;
}

/** Return the UTC epoch edges [KST day 00:01, next day 00:01]. */
function v1_availability_day_edges(string $day): array {
    $kst = new DateTimeZone('Asia/Seoul');
    $start = new DateTimeImmutable($day . ' 00:01:00',$kst);
    return array($start->getTimestamp(),$start->modify('+1 day')->getTimestamp());
}

function v1_release_metric_value(array $metrics, string $key) {
    if (array_key_exists($key,$metrics)) { return $metrics[$key]; }
    if (isset($metrics['metrics']) && is_array($metrics['metrics']) && array_key_exists($key,$metrics['metrics'])) {
        return $metrics['metrics'][$key];
    }
    return null;
}

function v1_release_metric_add(array &$group, string $field, $value): void {
    if (!is_int($value) && !is_float($value)) { return; }
    if (!isset($group[$field]) || $group[$field] === null) { $group[$field] = 0; }
    $group[$field] += (int)$value;
}

function v1_official_run_metric(array $metrics, string $key) {
    if (array_key_exists($key,$metrics)) { return $metrics[$key]; }
    if (isset($metrics['metrics']) && is_array($metrics['metrics']) && array_key_exists($key,$metrics['metrics'])) {
        return $metrics['metrics'][$key];
    }
    return null;
}

function v1_official_schedule_slot_matches($eventSchedule, $slot): bool {
    if (!is_string($eventSchedule) || !is_string($slot) || trim($slot) === '') { return false; }
    try {
        // GitHub Actions cron expressions are UTC.  The slot is later assigned
        // to a KST evidence date, but cron-family validation uses UTC fields.
        // MySQL DATETIME values have no offset, so parse them explicitly as UTC
        // instead of inheriting the PHP host's default timezone.
        $utc = new DateTimeZone('UTC');
        $utcSlot = (new DateTimeImmutable($slot,$utc))->setTimezone($utc);
    } catch (Throwable $e) { return false; }
    if ((int)$utcSlot->format('s') !== 0) { return false; }
    $hour = (int)$utcSlot->format('G'); $minute = (int)$utcSlot->format('i');
    if ($eventSchedule === '0,15,30,45 22-23 * * *') {
        return $hour >= 22 && $hour <= 23 && in_array($minute,array(0,15,30,45),true);
    }
    if ($eventSchedule === '0,15,30,45 0-14 * * *') {
        return $hour >= 0 && $hour <= 14 && in_array($minute,array(0,15,30,45),true);
    }
    if ($eventSchedule === '0,30 15-21 * * *') {
        return $hour >= 15 && $hour <= 21 && in_array($minute,array(0,30),true);
    }
    return false;
}

function v1_official_slot_claim_error(int $status, string $code, array $extra = array()): void {
    v1_respond($status,array_merge(array(
        'ok'=>false,
        'error'=>array('code'=>$code),
    ),$extra));
}

/** The next boundary is in the complete 82-slot KST cadence, not one cron family. */
function v1_official_next_cadence_slot(string $slot): ?string {
    if (!v1_official_schedule_slot_matches('0,15,30,45 22-23 * * *',$slot)
        && !v1_official_schedule_slot_matches('0,15,30,45 0-14 * * *',$slot)
        && !v1_official_schedule_slot_matches('0,30 15-21 * * *',$slot)) { return null; }
    try {
        $kst = new DateTimeZone('Asia/Seoul');
        $value = (new DateTimeImmutable($slot,new DateTimeZone('UTC')))->setTimezone($kst);
    } catch (Throwable $e) { return null; }
    $minutes = (int)$value->format('G') < 7 ? 30 : 15;
    return $value->modify('+' . $minutes . ' minutes')->setTimezone(new DateTimeZone('UTC'))->format('Y-m-d H:i:s');
}

/** Enumerate due slots in exactly one immutable GitHub cron family. */
function v1_official_due_slots(string $eventSchedule, string $activeFrom, string $through): array {
    if (!in_array($eventSchedule,array('0,15,30,45 22-23 * * *','0,15,30,45 0-14 * * *','0,30 15-21 * * *'),true)) {
        return array();
    }
    $utc = new DateTimeZone('UTC');
    try {
        $start = (new DateTimeImmutable($activeFrom,$utc))->setTime(0,0,0);
        $end = (new DateTimeImmutable($through,$utc))->setTime(0,0,0);
    } catch (Throwable $e) { return array(); }
    if ($end < $start) { return array(); }
    if ($eventSchedule === '0,15,30,45 22-23 * * *') {
        $hours = array(22,23); $minutes = array(0,15,30,45);
    } elseif ($eventSchedule === '0,15,30,45 0-14 * * *') {
        $hours = range(0,14); $minutes = array(0,15,30,45);
    } else {
        $hours = range(15,21); $minutes = array(0,30);
    }
    $rows = array();
    for ($day = $start; $day <= $end; $day = $day->modify('+1 day')) {
        foreach ($hours as $hour) {
            foreach ($minutes as $minute) {
                $candidate = $day->setTime((int)$hour,(int)$minute,0)->format('Y-m-d H:i:s');
                if ($candidate >= $activeFrom && $candidate <= $through) { $rows[] = $candidate; }
            }
        }
    }
    return $rows;
}

function v1_official_slot_claim_identity(array $payload, string $trigger, ?string $expected): string {
    return hash('sha256',v1_strict_canonical_json_encode(array(
        'action'=>(string)$payload['action'],
        'pipeline'=>(string)$payload['pipeline'],
        'github_run_id'=>(string)$payload['github_run_id'],
        'event_schedule'=>(string)$payload['event_schedule'],
        'trigger_created_at'=>$trigger,
        'code_revision'=>(string)$payload['code_revision'],
        'expected_slot_at'=>$expected,
    ),'official_slot_claim_identity_encode_failed'));
}

function v1_official_slot_claim_response(array $row, int $attempt, bool $duplicate): array {
    return array(
        'ok'=>true,
        'accepted'=>1,
        'claim_id'=>(string)$row['claim_id'],
        'pipeline'=>(string)$row['pipeline'],
        'github_run_id'=>(string)$row['github_run_id'],
        'github_run_attempt'=>$attempt,
        'event_schedule'=>(string)$row['event_schedule'],
        'scheduled_slot_at'=>v1_release_iso_time($row['scheduled_slot_at']),
        'trigger_created_at'=>v1_release_iso_time($row['trigger_created_at']),
        'claimed_at'=>v1_release_iso_time($row['claimed_at']),
        'next_cadence_slot_at'=>v1_release_iso_time($row['next_cadence_slot_at']),
        'trigger_lag_seconds'=>(int)$row['trigger_lag_seconds'],
        'claim_lag_seconds'=>(int)$row['claim_lag_seconds'],
        'late'=>((int)$row['late'] === 1),
        'status'=>(string)$row['status'],
        'terminal_reason'=>$row['terminal_reason'] === null ? null : (string)$row['terminal_reason'],
        'duplicate'=>$duplicate,
    );
}

/**
 * Atomically claim one oldest due slot. First contact only activates a clean
 * next-KST-day epoch; it never attributes the ambiguous bootstrap invocation.
 */
function v1_ops_official_slot_claim_write(PDO $pdo, array $config): void {
    $payload = v1_admin_json_body($config);
    v1_assert_object_keys($payload,array('action','pipeline','github_run_id','github_run_attempt','event_schedule',
        'trigger_created_at','code_revision','expected_slot_at'),'body');
    $required = array('action','pipeline','github_run_id','github_run_attempt','event_schedule','trigger_created_at','code_revision');
    foreach ($required as $field) {
        if (!array_key_exists($field,$payload)) { v1_official_slot_claim_error(400,'official_slot_claim_invalid',array('field'=>$field)); }
    }
    $action = is_string($payload['action']) ? trim($payload['action']) : '';
    $pipeline = is_string($payload['pipeline']) ? trim($payload['pipeline']) : '';
    $runId = is_string($payload['github_run_id']) ? trim($payload['github_run_id']) : '';
    $attempt = is_int($payload['github_run_attempt']) ? $payload['github_run_attempt'] : 0;
    $schedule = is_string($payload['event_schedule']) ? trim($payload['event_schedule']) : '';
    $trigger = v1_editorial_datetime_utc($payload['trigger_created_at']);
    $revision = v1_valid_build_sha($payload['code_revision']);
    $expected = array_key_exists('expected_slot_at',$payload) ? v1_editorial_datetime_utc($payload['expected_slot_at']) : null;
    if (!in_array($action,array('claim','repair'),true) || $pipeline !== 'ingest-official'
        || preg_match('/^[0-9]{1,64}$/',$runId) !== 1 || $attempt < 1
        || !in_array($schedule,array('0,15,30,45 22-23 * * *','0,15,30,45 0-14 * * *','0,30 15-21 * * *'),true)
        || $trigger === null || $revision === null || strlen($revision) > 40
        || ($action === 'claim' && array_key_exists('expected_slot_at',$payload))
        || ($action === 'repair' && ($expected === null || !v1_official_schedule_slot_matches($schedule,$expected)))) {
        v1_official_slot_claim_error(400,'official_slot_claim_invalid');
    }
    $now = gmdate('Y-m-d H:i:s');
    if ($trigger > $now) { v1_official_slot_claim_error(400,'official_slot_claim_future_trigger'); }
    $identity = v1_official_slot_claim_identity($payload,$trigger,$expected);
    $stateTable = table_name($config,'official_slot_claim_state');
    $claimTable = table_name($config,'official_slot_claims');
    $pdo->beginTransaction();
    try {
        $stateStmt = $pdo->prepare('SELECT active_from,epoch_version,activated_at,activation_revision,change_reason,changed_by FROM ' . $stateTable
            . ' WHERE pipeline=? FOR UPDATE');
        $stateStmt->execute(array($pipeline)); $state = $stateStmt->fetch();
        if (!$state) {
            $kst = new DateTimeZone('Asia/Seoul'); $utc = new DateTimeZone('UTC');
            $activeFrom = (new DateTimeImmutable('now',$kst))->modify('+1 day')->setTime(0,0,0)->setTimezone($utc)->format('Y-m-d H:i:s');
            $activationReason = 'Automatic first-contact activation at the next complete KST day boundary';
            $activationActor = 'ops_claim_bootstrap';
            $insertState = $pdo->prepare('INSERT INTO ' . $stateTable
                . ' (pipeline,active_from,epoch_version,activated_at,activation_revision,change_reason,changed_by,created_at,updated_at) '
                . 'VALUES (?,?,1,?,?,?,?,?,?)');
            $insertState->execute(array($pipeline,$activeFrom,$now,$revision,$activationReason,$activationActor,$now,$now));
            $epochInsert = $pdo->prepare('INSERT INTO ' . table_name($config,'official_slot_claim_epochs')
                . ' (epoch_id,pipeline,epoch_version,change_type,previous_active_from,active_from,change_reason,code_revision,changed_by,created_at) '
                . 'VALUES (?,?,1,\'activation\',NULL,?,?,?,?,?)');
            $epochInsert->execute(array('official-epoch:' . substr(hash('sha256',$pipeline . '|1|' . $activeFrom),0,48),
                $pipeline,$activeFrom,$activationReason,$revision,$activationActor,$now));
            $pdo->commit();
            v1_official_slot_claim_error(409,'official_slot_claim_activated',array('active_from'=>v1_release_iso_time($activeFrom)));
        }
        $activeFrom = (string)$state['active_from'];
        if ($now < $activeFrom) {
            $pdo->commit();
            v1_official_slot_claim_error(409,'official_slot_claim_not_active',array('active_from'=>v1_release_iso_time($activeFrom)));
        }
        $existingStmt = $pdo->prepare('SELECT * FROM ' . $claimTable . ' WHERE pipeline=? AND github_run_id=? FOR UPDATE');
        $existingStmt->execute(array($pipeline,$runId)); $existing = $existingStmt->fetch();
        if ($existing) {
            if (!hash_equals((string)$existing['identity_sha256'],$identity) || $attempt < (int)$existing['github_run_attempt']) {
                $pdo->rollBack(); v1_official_slot_claim_error(409,'official_slot_claim_idempotency_conflict');
            }
            $terminalFailure = is_string($existing['terminal_reason'])
                && (string)$existing['terminal_reason'] !== '';
            $crossedBeforeRerun = (string)$existing['status'] !== 'completed'
                && $attempt > (int)$existing['github_run_attempt']
                && $now >= (string)$existing['next_cadence_slot_at'];
            if ($crossedBeforeRerun) {
                $terminalUpdate = $pdo->prepare('UPDATE ' . $claimTable
                    . ' SET status=\'failed\',terminal_reason=\'rerun_after_next_cadence\',failed_at=?,updated_at=? WHERE claim_id=?');
                $terminalUpdate->execute(array($now,$now,(string)$existing['claim_id']));
                $existing['status'] = 'failed'; $existing['terminal_reason'] = 'rerun_after_next_cadence';
                $existing['failed_at'] = $now; $existing['updated_at'] = $now;
                $terminalFailure = true;
            }
            // A completed claim is immutable. A higher GitHub rerun attempt may
            // receive the same claim and later prove a semantic no-op, but it
            // must not rewrite the original attempt stored in the evidence row.
            if ($attempt > (int)$existing['github_run_attempt'] && (string)$existing['status'] !== 'completed'
                && !$terminalFailure) {
                $updateAttempt = $pdo->prepare('UPDATE ' . $claimTable . ' SET github_run_attempt=?,updated_at=? WHERE claim_id=?');
                $updateAttempt->execute(array($attempt,$now,(string)$existing['claim_id']));
                $existing['github_run_attempt'] = $attempt; $existing['updated_at'] = $now;
            }
            $pdo->commit();
            v1_respond(200,v1_official_slot_claim_response($existing,$attempt,true));
        }
        $claimedSql = 'SELECT scheduled_slot_at FROM ' . $claimTable
            . ' WHERE pipeline=?' . ($action === 'repair' ? '' : ' AND event_schedule=?')
            . ' AND scheduled_slot_at BETWEEN ? AND ? FOR UPDATE';
        $claimedStmt = $pdo->prepare($claimedSql);
        $claimedParams = $action === 'repair'
            ? array($pipeline,$activeFrom,$trigger)
            : array($pipeline,$schedule,$activeFrom,$trigger);
        $claimedStmt->execute($claimedParams); $claimed = array();
        foreach ($claimedStmt->fetchAll() as $row) { $claimed[(string)$row['scheduled_slot_at']] = true; }
        $oldest = null;
        if ($action === 'repair') {
            $dueSlots = array();
            foreach (array('0,15,30,45 22-23 * * *','0,15,30,45 0-14 * * *','0,30 15-21 * * *') as $family) {
                $dueSlots = array_merge($dueSlots,v1_official_due_slots($family,$activeFrom,$trigger));
            }
            sort($dueSlots,SORT_STRING);
        } else { $dueSlots = v1_official_due_slots($schedule,$activeFrom,$trigger); }
        foreach ($dueSlots as $slot) {
            if (!isset($claimed[$slot])) { $oldest = $slot; break; }
        }
        if ($oldest === null) {
            $pdo->rollBack(); v1_official_slot_claim_error(409,'official_slot_claim_not_due');
        }
        if ($action === 'repair' && ($expected !== $oldest
            || !v1_official_schedule_slot_matches($schedule,$oldest))) {
            $pdo->rollBack(); v1_official_slot_claim_error(409,'official_slot_repair_not_oldest',array(
                'oldest_due_slot_at'=>v1_release_iso_time($oldest),
            ));
        }
        $nextSlot = v1_official_next_cadence_slot($oldest);
        if ($nextSlot === null) { throw new RuntimeException('official_slot_claim_next_boundary_invalid'); }
        $slotEpoch = strtotime($oldest . ' UTC'); $triggerEpoch = strtotime($trigger . ' UTC'); $nowEpoch = strtotime($now . ' UTC');
        if ($slotEpoch === false || $triggerEpoch === false || $nowEpoch === false || $triggerEpoch < $slotEpoch) {
            throw new RuntimeException('official_slot_claim_timestamp_invalid');
        }
        $late = $now >= $nextSlot ? 1 : 0;
        if ($action === 'repair' && $late !== 1) {
            $pdo->rollBack(); v1_official_slot_claim_error(409,'official_slot_repair_not_late');
        }
        $claimId = 'official-slot:' . substr(hash('sha256',$pipeline . '|' . $oldest),0,48);
        $insert = $pdo->prepare('INSERT INTO ' . $claimTable
            . ' (claim_id,pipeline,epoch_version,scheduled_slot_at,event_schedule,github_run_id,github_run_attempt,trigger_created_at,claimed_at,'
            . 'next_cadence_slot_at,trigger_lag_seconds,claim_lag_seconds,late,code_revision,identity_sha256,status,created_at,updated_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\'claimed\',?,?)');
        $insert->execute(array($claimId,$pipeline,(int)$state['epoch_version'],$oldest,$schedule,$runId,$attempt,$trigger,$now,$nextSlot,
            $triggerEpoch-$slotEpoch,$nowEpoch-$slotEpoch,$late,$revision,$identity,$now,$now));
        $select = $pdo->prepare('SELECT * FROM ' . $claimTable . ' WHERE claim_id=?');
        $select->execute(array($claimId)); $row = $select->fetch();
        if (!$row) { throw new RuntimeException('official_slot_claim_insert_missing'); }
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        if ((string)$e->getCode() === '23000') { v1_official_slot_claim_error(409,'official_slot_claim_concurrent_conflict'); }
        throw $e;
    }
    v1_respond(200,v1_official_slot_claim_response($row,$attempt,false));
}

/** Ops audit feed includes claimed, failed, completed and permanently late slots. */
function v1_ops_official_slot_claims(PDO $pdo, array $config): void {
    $kstNow = new DateTimeImmutable('now',new DateTimeZone('Asia/Seoul'));
    $to = v1_date_bound('to',$kstNow->format('Y-m-d'));
    $from = v1_date_bound('from',$kstNow->modify('-6 days')->format('Y-m-d'));
    $fromDate = new DateTimeImmutable($from,new DateTimeZone('Asia/Seoul'));
    $toDate = new DateTimeImmutable($to,new DateTimeZone('Asia/Seoul'));
    if ($toDate < $fromDate || $toDate->diff($fromDate)->days > 30) {
        v1_official_slot_claim_error(400,'official_slot_claim_range_exceeds_31_days');
    }
    list($start,$end) = v1_evidence_utc_bounds($from,$to);
    $page = v1_list_params();
    $stmt = $pdo->prepare('SELECT claim_id,pipeline,epoch_version,scheduled_slot_at,event_schedule,github_run_id,github_run_attempt,'
        . 'trigger_created_at,claimed_at,next_cadence_slot_at,trigger_lag_seconds,claim_lag_seconds,late,code_revision,status,'
        . 'terminal_reason,failed_at,completed_run_id,completed_run_attempt,completion_raw_count,completion_ack_count,completion_sha256,completed_at,created_at,updated_at FROM '
        . table_name($config,'official_slot_claims') . ' WHERE scheduled_slot_at BETWEEN ? AND ? ORDER BY scheduled_slot_at,claim_id LIMIT '
        . ((int)$page['limit'] + 1) . ' OFFSET ' . (int)$page['offset']);
    $stmt->execute(array($start,$end)); list($rows,$hasMore) = v1_fetch_page($stmt,$page);
    foreach ($rows as &$row) {
        foreach (array('scheduled_slot_at','trigger_created_at','claimed_at','next_cadence_slot_at','failed_at','completed_at','created_at','updated_at') as $field) {
            $row[$field] = v1_release_iso_time(isset($row[$field]) ? $row[$field] : null);
        }
        foreach (array('epoch_version','github_run_attempt','trigger_lag_seconds','claim_lag_seconds','completed_run_attempt','completion_raw_count','completion_ack_count') as $field) {
            $row[$field] = $row[$field] === null ? null : (int)$row[$field];
        }
        $row['late'] = (int)$row['late'] === 1;
    }
    unset($row);
    $stateStmt = $pdo->prepare('SELECT pipeline,active_from,epoch_version,activated_at,activation_revision,change_reason,changed_by FROM '
        . table_name($config,'official_slot_claim_state') . ' WHERE pipeline=?');
    $stateStmt->execute(array('ingest-official')); $state = $stateStmt->fetch();
    if ($state) {
        $state['active_from'] = v1_release_iso_time($state['active_from']);
        $state['activated_at'] = v1_release_iso_time($state['activated_at']);
        $state['epoch_version'] = (int)$state['epoch_version'];
    }
    v1_respond(200,array('ok'=>true,'state'=>$state ?: null,'range'=>array('from'=>$from,'to'=>$to),'data'=>$rows,
        'pagination'=>v1_page_meta($page,count($rows),$hasMore)));
}

function v1_admin_official_slot_epoch(PDO $pdo, array $config): void {
    $stateStmt = $pdo->prepare('SELECT pipeline,active_from,epoch_version,activated_at,activation_revision,change_reason,changed_by,created_at,updated_at FROM '
        . table_name($config,'official_slot_claim_state') . ' WHERE pipeline=?');
    $stateStmt->execute(array('ingest-official')); $state = $stateStmt->fetch();
    if ($state) {
        foreach (array('active_from','activated_at','created_at','updated_at') as $field) {
            $state[$field] = v1_release_iso_time($state[$field]);
        }
        $state['epoch_version'] = (int)$state['epoch_version'];
    }
    $historyStmt = $pdo->prepare('SELECT epoch_id,pipeline,epoch_version,change_type,previous_active_from,active_from,'
        . 'change_reason,code_revision,changed_by,created_at FROM ' . table_name($config,'official_slot_claim_epochs')
        . ' WHERE pipeline=? ORDER BY epoch_version DESC LIMIT 100');
    $historyStmt->execute(array('ingest-official')); $history = $historyStmt->fetchAll();
    foreach ($history as &$row) {
        $row['epoch_version'] = (int)$row['epoch_version'];
        foreach (array('previous_active_from','active_from','created_at') as $field) {
            $row[$field] = v1_release_iso_time(isset($row[$field]) ? $row[$field] : null);
        }
    }
    unset($row);
    v1_respond(200,array('ok'=>true,'state'=>$state ?: null,'history'=>$history));
}

/** Admin-only, append-only epoch reset. Existing claims and history are untouched. */
function v1_admin_reset_official_slot_epoch(PDO $pdo, array $config, string $role): void {
    $payload = v1_admin_json_body($config);
    v1_assert_object_keys($payload,array('action','pipeline','expected_epoch_version','reason','code_revision','confirmation'),'body');
    $action = isset($payload['action']) && is_string($payload['action']) ? trim($payload['action']) : '';
    $pipeline = isset($payload['pipeline']) && is_string($payload['pipeline']) ? trim($payload['pipeline']) : '';
    $expected = isset($payload['expected_epoch_version']) && is_int($payload['expected_epoch_version'])
        ? $payload['expected_epoch_version'] : 0;
    $reason = isset($payload['reason']) && is_string($payload['reason']) ? trim($payload['reason']) : '';
    $revision = isset($payload['code_revision']) ? v1_valid_build_sha($payload['code_revision']) : null;
    $confirmation = isset($payload['confirmation']) && is_string($payload['confirmation'])
        ? $payload['confirmation'] : '';
    if ($action !== 'reset' || $pipeline !== 'ingest-official' || $expected < 1
        || mb_strlen($reason,'UTF-8') < 20 || mb_strlen($reason,'UTF-8') > 500
        || $revision === null || strlen($revision) > 40
        || $confirmation !== 'RESET_OFFICIAL_SLOT_EPOCH_AT_NEXT_KST_DAY') {
        v1_official_slot_claim_error(400,'official_slot_epoch_reset_invalid');
    }
    $stateTable = table_name($config,'official_slot_claim_state');
    $epochTable = table_name($config,'official_slot_claim_epochs');
    $now = gmdate('Y-m-d H:i:s');
    $pdo->beginTransaction();
    try {
        $release = v1_release_state($pdo,$config,true);
        if (!$release || (string)$release['release_state'] !== 'closed') {
            $pdo->rollBack(); v1_official_slot_claim_error(409,'official_slot_epoch_reset_requires_closed_release');
        }
        $stmt = $pdo->prepare('SELECT * FROM ' . $stateTable . ' WHERE pipeline=? FOR UPDATE');
        $stmt->execute(array($pipeline)); $state = $stmt->fetch();
        if (!$state) {
            $pdo->rollBack(); v1_official_slot_claim_error(409,'official_slot_epoch_not_activated');
        }
        if ((int)$state['epoch_version'] !== $expected) {
            $pdo->rollBack(); v1_official_slot_claim_error(409,'official_slot_epoch_version_conflict',array(
                'actual_epoch_version'=>(int)$state['epoch_version'],
            ));
        }
        $kst = new DateTimeZone('Asia/Seoul'); $utc = new DateTimeZone('UTC');
        $nowKst = new DateTimeImmutable('now',$kst);
        $currentActiveKst = (new DateTimeImmutable((string)$state['active_from'],$utc))->setTimezone($kst);
        $base = $nowKst > $currentActiveKst ? $nowKst : $currentActiveKst;
        $activeFrom = $base->modify('+1 day')->setTime(0,0,0)->setTimezone($utc)->format('Y-m-d H:i:s');
        $nextVersion = $expected + 1;
        $update = $pdo->prepare('UPDATE ' . $stateTable
            . ' SET active_from=?,epoch_version=?,activated_at=?,activation_revision=?,change_reason=?,changed_by=?,updated_at=? '
            . 'WHERE pipeline=? AND epoch_version=?');
        $update->execute(array($activeFrom,$nextVersion,$now,$revision,$reason,$role,$now,$pipeline,$expected));
        if ($update->rowCount() !== 1) { throw new RuntimeException('official_slot_epoch_reset_lost_update'); }
        $epochId = 'official-epoch:' . substr(hash('sha256',$pipeline . '|' . $nextVersion . '|' . $activeFrom),0,48);
        $insert = $pdo->prepare('INSERT INTO ' . $epochTable
            . ' (epoch_id,pipeline,epoch_version,change_type,previous_active_from,active_from,change_reason,code_revision,changed_by,created_at) '
            . 'VALUES (?,?,?,\'reset\',?,?,?,?,?,?)');
        $insert->execute(array($epochId,$pipeline,$nextVersion,(string)$state['active_from'],$activeFrom,$reason,$revision,$role,$now));
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    v1_respond(200,array('ok'=>true,'pipeline'=>$pipeline,'epoch_version'=>$nextVersion,'active_from'=>v1_release_iso_time($activeFrom),
        'epoch_id'=>$epochId,'claims_preserved'=>true));
}

/** Normalize one durable official collection run into the paged ledger contract. */
function v1_official_run_ledger_row(array $run): array {
    $metrics = json_decode((string)$run['metrics_json'],true);
    if (!is_array($metrics)) { $metrics = array(); }
    $runKindValue = v1_official_run_metric($metrics,'run_kind');
    $runKind = is_string($runKindValue) ? trim($runKindValue) : '';
    $eventScheduleValue = v1_official_run_metric($metrics,'event_schedule');
    $eventSchedule = is_string($eventScheduleValue) && trim($eventScheduleValue) !== '' ? trim($eventScheduleValue) : null;
    $companyMaster = v1_bool_int(v1_official_run_metric($metrics,'company_master_sync')) === 1;
    if (!in_array($runKind,array('scheduled_incremental','manual','backfill','company_master'),true)) { $runKind = null; }
    $slotValue = v1_official_run_metric($metrics,'scheduled_slot_at');
    $scheduledSlot = is_string($slotValue) ? v1_mysql_datetime_utc($slotValue) : null;
    $triggerValue = v1_official_run_metric($metrics,'trigger_created_at');
    $triggerCreated = is_string($triggerValue) ? v1_editorial_datetime_utc($triggerValue) : null;
    $claimIdValue = v1_official_run_metric($metrics,'slot_claim_id');
    $claimId = is_string($claimIdValue) && v1_valid_entity_id($claimIdValue) ? $claimIdValue : null;
    $githubRunIdValue = v1_official_run_metric($metrics,'github_run_id');
    $githubRunId = is_string($githubRunIdValue) && preg_match('/^[0-9]{1,64}$/',$githubRunIdValue) === 1
        ? $githubRunIdValue : null;
    $githubAttemptValue = v1_official_run_metric($metrics,'github_run_attempt');
    $githubAttempt = is_int($githubAttemptValue) && $githubAttemptValue >= 1 ? $githubAttemptValue : null;
    $claimedValue = v1_official_run_metric($metrics,'slot_claimed_at');
    $slotClaimedAt = is_string($claimedValue) ? v1_editorial_datetime_utc($claimedValue) : null;
    $nextValue = v1_official_run_metric($metrics,'next_cadence_slot_at');
    $nextCadence = is_string($nextValue) ? v1_editorial_datetime_utc($nextValue) : null;
    $triggerLagValue = v1_official_run_metric($metrics,'trigger_lag_seconds');
    $triggerLag = is_int($triggerLagValue) && $triggerLagValue >= 0 ? $triggerLagValue : null;
    $claimLagValue = v1_official_run_metric($metrics,'claim_lag_seconds');
    $claimLag = is_int($claimLagValue) && $claimLagValue >= 0 ? $claimLagValue : null;
    $lateValue = v1_official_run_metric($metrics,'slot_claim_late');
    $slotLate = is_bool($lateValue) ? $lateValue : null;
    $outcomesValue = v1_official_run_metric($metrics,'source_outcomes');
    $outcomes = is_array($outcomesValue) ? $outcomesValue : array();
    $ackValue = v1_official_run_metric($metrics,'source_ack_counts');
    $ackCounts = is_array($ackValue) ? $ackValue : array();
    $sourceOutcomes = array(); $sourceKey = strtolower((string)$run['source_key']);
    foreach (array('dart','kind') as $source) {
        $isNamed = in_array($source,explode('+',$sourceKey),true);
        if (!$isNamed && !isset($outcomes[$source])) { continue; }
        $outcome = isset($outcomes[$source]) && is_array($outcomes[$source]) ? $outcomes[$source] : array();
        $raw = isset($outcome['raw_count']) && is_numeric($outcome['raw_count']) ? (int)$outcome['raw_count']
            : (isset($outcome['fetched']) && is_numeric($outcome['fetched']) ? (int)$outcome['fetched'] : null);
        $ack = isset($ackCounts[$source]) && is_numeric($ackCounts[$source]) ? (int)$ackCounts[$source]
            : (isset($outcome['acknowledged_count']) && is_numeric($outcome['acknowledged_count'])
                ? (int)$outcome['acknowledged_count'] : null);
        $sourceOutcomes[$source] = array(
            'status'=>isset($outcome['status']) ? strtolower(trim((string)$outcome['status'])) : 'missing',
            'raw_count'=>$raw,
            'acknowledged_count'=>$ack,
        );
    }
    ksort($sourceOutcomes,SORT_STRING);
    return array(
        'run_id'=>(string)$run['run_id'],
        'pipeline'=>(string)$run['pipeline'],
        'source_key'=>$sourceKey,
        'code_revision'=>v1_valid_build_sha($run['code_revision']),
        'status'=>strtolower((string)$run['status']),
        'started_at'=>v1_release_iso_time($run['started_at']),
        'finished_at'=>v1_release_iso_time($run['finished_at']),
        'first_observed_at'=>v1_release_iso_time($run['first_observed_at']),
        'raw_count'=>(int)$run['raw_count'],
        'acknowledged_count'=>(int)$run['acknowledged_count'],
        'run_kind'=>$runKind,
        'event_schedule'=>$eventSchedule,
        'scheduled_slot_at'=>$scheduledSlot === null ? null : v1_release_iso_time($scheduledSlot),
        'trigger_created_at'=>$triggerCreated === null ? null : v1_release_iso_time($triggerCreated),
        'slot_claim_id'=>$claimId,
        'github_run_id'=>$githubRunId,
        'github_run_attempt'=>$githubAttempt,
        'slot_claimed_at'=>$slotClaimedAt === null ? null : v1_release_iso_time($slotClaimedAt),
        'next_cadence_slot_at'=>$nextCadence === null ? null : v1_release_iso_time($nextCadence),
        'trigger_lag_seconds'=>$triggerLag,
        'claim_lag_seconds'=>$claimLag,
        'slot_claim_late'=>$slotLate,
        'slot_claim_status'=>null,
        'slot_claim_terminal_reason'=>null,
        'company_master_sync'=>$companyMaster,
        'source_outcomes'=>$sourceOutcomes,
    );
}

function v1_official_scheduled_run_matches(array $row): bool {
    if ($row['run_kind'] !== 'scheduled_incremental' || $row['scheduled_slot_at'] === null
        || $row['trigger_created_at'] === null
        || $row['slot_claim_id'] === null || $row['github_run_id'] === null || $row['github_run_attempt'] === null
        || $row['slot_claimed_at'] === null || $row['next_cadence_slot_at'] === null
        || $row['trigger_lag_seconds'] === null || $row['claim_lag_seconds'] === null
        || $row['slot_claim_late'] === null || $row['slot_claim_status'] === null
        || !v1_official_schedule_slot_matches($row['event_schedule'],$row['scheduled_slot_at'])) { return false; }
    $slot = v1_mysql_datetime_utc($row['scheduled_slot_at']);
    $trigger = v1_mysql_datetime_utc($row['trigger_created_at']);
    $claimed = v1_mysql_datetime_utc($row['slot_claimed_at']);
    $nextSlot = v1_official_next_cadence_slot((string)$row['scheduled_slot_at']);
    $declaredNext = v1_mysql_datetime_utc($row['next_cadence_slot_at']);
    if ($slot === null || $trigger === null || $claimed === null || $nextSlot === null || $declaredNext !== $nextSlot
        || $trigger < $slot || $claimed < $trigger
        || !in_array($row['slot_claim_status'],array('claimed','failed','completed'),true)) { return false; }
    $slotEpoch = strtotime($slot . ' UTC'); $triggerEpoch = strtotime($trigger . ' UTC'); $claimedEpoch = strtotime($claimed . ' UTC');
    if ($slotEpoch === false || $triggerEpoch === false || $claimedEpoch === false) { return false; }
    return (int)$row['trigger_lag_seconds'] === $triggerEpoch-$slotEpoch
        && (int)$row['claim_lag_seconds'] === $claimedEpoch-$slotEpoch
        && (bool)$row['slot_claim_late'] === ($claimed >= $nextSlot);
}

function v1_official_claim_matches_ledger_run(array $claim, array $row, array $run): bool {
    if ($row['slot_claim_id'] !== (string)$claim['claim_id']
        || $row['pipeline'] !== (string)$claim['pipeline']
        || $row['event_schedule'] !== (string)$claim['event_schedule']
        || v1_mysql_datetime_utc($row['scheduled_slot_at']) !== (string)$claim['scheduled_slot_at']
        || v1_mysql_datetime_utc($row['trigger_created_at']) !== (string)$claim['trigger_created_at']
        || v1_mysql_datetime_utc($row['slot_claimed_at']) !== (string)$claim['claimed_at']
        || v1_mysql_datetime_utc($row['next_cadence_slot_at']) !== (string)$claim['next_cadence_slot_at']
        || $row['github_run_id'] !== (string)$claim['github_run_id']
        || $row['github_run_attempt'] !== (int)$claim['github_run_attempt']
        || $row['trigger_lag_seconds'] !== (int)$claim['trigger_lag_seconds']
        || $row['claim_lag_seconds'] !== (int)$claim['claim_lag_seconds']
        || $row['slot_claim_late'] !== ((int)$claim['late'] === 1)
        || !is_string($row['code_revision']) || !hash_equals((string)$claim['code_revision'],$row['code_revision'])
        || !in_array((string)$claim['status'],array('failed','completed'),true)
        || (string)$claim['completed_run_id'] !== $row['run_id']
        || (int)$claim['completed_run_attempt'] !== $row['github_run_attempt']
        || (int)$claim['completion_raw_count'] !== $row['raw_count']
        || (int)$claim['completion_ack_count'] !== $row['acknowledged_count']
        || !is_string($claim['completion_sha256'])
        || preg_match('/^[a-f0-9]{64}$/',(string)$claim['completion_sha256']) !== 1) { return false; }
    $metrics = json_decode((string)$run['metrics_json'],true);
    if (!is_array($metrics)) { return false; }
    unset($metrics['server_correction_link_ambiguous'],$metrics['server_event_link_ambiguous']);
    try { $digest = v1_official_completion_semantic_sha($metrics); }
    catch (Throwable $e) { return false; }
    if (!hash_equals((string)$claim['completion_sha256'],$digest)) { return false; }
    $topSucceeded = in_array($row['status'],array('success','succeeded'),true)
        && $row['raw_count'] === $row['acknowledged_count'];
    if ((string)$claim['status'] === 'completed') {
        return $topSucceeded && $claim['completed_at'] !== null && $claim['terminal_reason'] === null;
    }
    return $claim['completed_at'] === null;
}

function v1_official_claim_only_ledger_row(array $claim): array {
    $sourceOutcomes = array(
        'dart'=>array('status'=>'missing','raw_count'=>0,'acknowledged_count'=>0),
        'kind'=>array('status'=>'missing','raw_count'=>0,'acknowledged_count'=>0),
    );
    return array(
        'run_id'=>'slot-claim:' . (string)$claim['claim_id'],
        'pipeline'=>(string)$claim['pipeline'],
        'source_key'=>'dart+kind',
        'code_revision'=>v1_valid_build_sha($claim['code_revision']),
        'status'=>'incomplete',
        'started_at'=>v1_release_iso_time($claim['claimed_at']),
        'finished_at'=>v1_release_iso_time($claim['claimed_at']),
        'first_observed_at'=>v1_release_iso_time($claim['claimed_at']),
        'raw_count'=>0,
        'acknowledged_count'=>0,
        'run_kind'=>'scheduled_incremental',
        'event_schedule'=>(string)$claim['event_schedule'],
        'scheduled_slot_at'=>v1_release_iso_time($claim['scheduled_slot_at']),
        'trigger_created_at'=>v1_release_iso_time($claim['trigger_created_at']),
        'slot_claim_id'=>(string)$claim['claim_id'],
        'github_run_id'=>(string)$claim['github_run_id'],
        'github_run_attempt'=>(int)$claim['github_run_attempt'],
        'slot_claimed_at'=>v1_release_iso_time($claim['claimed_at']),
        'next_cadence_slot_at'=>v1_release_iso_time($claim['next_cadence_slot_at']),
        'trigger_lag_seconds'=>(int)$claim['trigger_lag_seconds'],
        'claim_lag_seconds'=>(int)$claim['claim_lag_seconds'],
        'slot_claim_late'=>((int)$claim['late'] === 1),
        'slot_claim_status'=>(string)$claim['status'],
        'slot_claim_terminal_reason'=>$claim['terminal_reason'] === null ? null : (string)$claim['terminal_reason'],
        'company_master_sync'=>false,
        'source_outcomes'=>$sourceOutcomes,
    );
}

function v1_official_run_ledger_rows(PDO $pdo, array $config, string $from, string $to): array {
    list($start,$end) = v1_evidence_utc_bounds($from,$to);
    // Include a one-day buffer because the authoritative date for a valid
    // scheduled run is its KST scheduled slot, not its (possibly next-day)
    // completion timestamp.
    $queryStart = (new DateTimeImmutable($start,new DateTimeZone('UTC')))->modify('-1 day')->format('Y-m-d H:i:s');
    $queryEnd = (new DateTimeImmutable($end,new DateTimeZone('UTC')))->modify('+1 day')->format('Y-m-d H:i:s');
    $claimStmt = $pdo->prepare('SELECT * FROM ' . table_name($config,'official_slot_claims')
        . ' WHERE scheduled_slot_at BETWEEN ? AND ? ORDER BY scheduled_slot_at,claim_id');
    $claimStmt->execute(array($start,$end)); $claims = array();
    foreach ($claimStmt->fetchAll() as $claim) { $claims[(string)$claim['claim_id']] = $claim; }
    $stmt = $pdo->prepare('SELECT run_id,pipeline,source_key,code_revision,status,started_at,finished_at,first_observed_at,'
        . 'raw_count,acknowledged_count,metrics_json FROM ' . table_name($config,'collection_runs')
        . ' WHERE COALESCE(finished_at,started_at) BETWEEN ? AND ? AND source_key IN (\'dart\',\'kind\',\'dart+kind\',\'kind+dart\')'
        . ' ORDER BY COALESCE(finished_at,started_at),run_id LIMIT 50000');
    $stmt->execute(array($queryStart,$queryEnd)); $rows = array(); $seenClaims = array();
    foreach ($stmt->fetchAll() as $run) {
        $row = v1_official_run_ledger_row($run);
        $claimId = isset($row['slot_claim_id']) && is_string($row['slot_claim_id']) ? $row['slot_claim_id'] : '';
        if ($claimId !== '' && isset($claims[$claimId])
            && v1_official_claim_matches_ledger_run($claims[$claimId],$row,$run)) {
            $row['slot_claim_status'] = (string)$claims[$claimId]['status'];
            $row['slot_claim_terminal_reason'] = $claims[$claimId]['terminal_reason'] === null
                ? null : (string)$claims[$claimId]['terminal_reason'];
            $seenClaims[$claimId] = true;
        }
        $scheduled = v1_official_scheduled_run_matches($row);
        $sortAt = $scheduled ? v1_mysql_datetime_utc($row['scheduled_slot_at'])
            : v1_mysql_datetime_utc($row['finished_at'] !== null ? $row['finished_at'] : $row['started_at']);
        $day = $sortAt === null ? null : v1_kst_observation_date($sortAt);
        if ($sortAt === null || $day === null || $day < $from || $day > $to) { continue; }
        $row['_sort_at'] = $sortAt; $rows[] = $row;
    }
    foreach ($claims as $claimId => $claim) {
        if (isset($seenClaims[$claimId])) { continue; }
        $row = v1_official_claim_only_ledger_row($claim);
        if (!v1_official_scheduled_run_matches($row)) { continue; }
        $row['_sort_at'] = (string)$claim['scheduled_slot_at']; $rows[] = $row;
    }
    usort($rows,function (array $left, array $right): int {
        $timeOrder = strcmp((string)$left['_sort_at'],(string)$right['_sort_at']);
        return $timeOrder !== 0 ? $timeOrder : strcmp((string)$left['run_id'],(string)$right['run_id']);
    });
    foreach ($rows as &$row) { unset($row['_sort_at']); } unset($row);
    return $rows;
}

function v1_official_run_ledger_sort_at(array $row): ?string {
    if (v1_official_scheduled_run_matches($row)) {
        return v1_mysql_datetime_utc($row['scheduled_slot_at']);
    }
    return v1_mysql_datetime_utc($row['finished_at'] !== null ? $row['finished_at'] : $row['started_at']);
}

function v1_official_run_ledger_hash(array $rows): string {
    $context = hash_init('sha256');
    foreach ($rows as $row) { hash_update($context,v1_strict_canonical_json_encode($row,'official_run_ledger_encode_failed') . "\n"); }
    return hash_final($context);
}

function v1_official_schedule_summary(array $rows, string $from, string $to): array {
    $fromDate = new DateTimeImmutable($from,new DateTimeZone('Asia/Seoul'));
    $toDate = new DateTimeImmutable($to,new DateTimeZone('Asia/Seoul'));
    $expectedSlots = ((int)$fromDate->diff($toDate)->days + 1) * 82;
    $slotRuns = array(); $sourceObserved = array('dart'=>array(),'kind'=>array());
    $sourceSucceeded = array('dart'=>array(),'kind'=>array()); $sourceFailed = array('dart'=>array(),'kind'=>array());
    $invalidRows = 0; $invalidMetadata = 0; $lateClaims = 0; $incompleteClaims = 0; $terminalFailures = 0;
    foreach ($rows as $row) {
        if ($row['run_kind'] === null) { $invalidMetadata++; continue; }
        if ($row['run_kind'] !== 'scheduled_incremental') { continue; }
        $slot = isset($row['scheduled_slot_at']) && is_string($row['scheduled_slot_at']) ? $row['scheduled_slot_at'] : '';
        if ($slot === '' || !v1_official_scheduled_run_matches($row)) {
            $invalidRows++; continue;
        }
        if (!isset($slotRuns[$slot])) { $slotRuns[$slot] = 0; }
        $slotRuns[$slot]++;
        if ($row['slot_claim_late'] === true) { $lateClaims++; }
        if ($row['slot_claim_status'] !== 'completed') { $incompleteClaims++; }
        if (isset($row['slot_claim_terminal_reason']) && $row['slot_claim_terminal_reason'] !== null) { $terminalFailures++; }
        $topSucceeded = in_array(strtolower((string)$row['status']),array('success','succeeded'),true)
            && (int)$row['raw_count'] === (int)$row['acknowledged_count']
            && $row['slot_claim_status'] === 'completed' && $row['slot_claim_late'] === false;
        $selectedSources = explode('+',strtolower((string)$row['source_key']));
        foreach (array('dart','kind') as $source) {
            // Disabled outcomes are retained in the raw ledger for audit, but
            // only sources selected by this run belong in its denominator.
            if (!in_array($source,$selectedSources,true) || !isset($row['source_outcomes'][$source])) { continue; }
            $sourceObserved[$source][$slot] = true;
            $outcome = $row['source_outcomes'][$source];
            $sourceStatus = strtolower((string)$outcome['status']);
            if ($topSucceeded && in_array($sourceStatus,array('success','succeeded'),true)
                && is_int($outcome['raw_count']) && is_int($outcome['acknowledged_count'])
                && $outcome['raw_count'] === $outcome['acknowledged_count']) {
                $sourceSucceeded[$source][$slot] = true; unset($sourceFailed[$source][$slot]);
            } elseif (!isset($sourceSucceeded[$source][$slot])) { $sourceFailed[$source][$slot] = true; }
        }
    }
    $duplicateSlots = 0;
    foreach ($slotRuns as $count) { if ($count > 1) { $duplicateSlots += $count - 1; } }
    $observedSlots = count($slotRuns);
    return array(
        'contract_version'=>1,
        'timezone'=>'Asia/Seoul',
        'cadence_id'=>'official-v1-82-slots',
        'from'=>$from,
        'to'=>$to,
        'expected_slot_count'=>$expectedSlots,
        'ledger_row_count'=>count($rows),
        'ledger_sha256'=>v1_official_run_ledger_hash($rows),
        'scheduled_run_count'=>array_sum($slotRuns),
        'observed_slot_count'=>$observedSlots,
        'claimed_slot_count'=>$observedSlots,
        'missing_slot_count'=>max(0,$expectedSlots-$observedSlots),
        'late_claim_count'=>$lateClaims,
        'incomplete_claim_count'=>$incompleteClaims,
        'terminal_failure_count'=>$terminalFailures,
        'duplicate_slot_count'=>$duplicateSlots,
        'invalid_scheduled_run_count'=>$invalidRows,
        'invalid_run_metadata_count'=>$invalidMetadata,
        'dart_expected_count'=>$expectedSlots,
        'dart_succeeded_count'=>count($sourceSucceeded['dart']),
        'dart_missing_count'=>max(0,$expectedSlots-count($sourceObserved['dart'])),
        'dart_failed_count'=>count($sourceFailed['dart']),
        'kind_expected_count'=>$expectedSlots,
        'kind_succeeded_count'=>count($sourceSucceeded['kind']),
        'kind_missing_count'=>max(0,$expectedSlots-count($sourceObserved['kind'])),
        'kind_failed_count'=>count($sourceFailed['kind']),
    );
}

function v1_ops_official_run_ledger(PDO $pdo, array $config): void {
    $kstNow = new DateTimeImmutable('now',new DateTimeZone('Asia/Seoul'));
    $to = v1_date_bound('to',$kstNow->format('Y-m-d'));
    $from = v1_date_bound('from',$kstNow->modify('-6 days')->format('Y-m-d'));
    $fromDate = new DateTimeImmutable($from,new DateTimeZone('Asia/Seoul'));
    $toDate = new DateTimeImmutable($to,new DateTimeZone('Asia/Seoul'));
    if ($toDate < $fromDate || $toDate->diff($fromDate)->days > 30) {
        v1_respond(400,array('ok'=>false,'error'=>'ledger_range_exceeds_31_days'));
    }
    $limit = isset($_GET['limit']) ? (int)$_GET['limit'] : 25; $limit = max(1,min(100,$limit));
    $cursor = isset($_GET['cursor']) ? trim((string)$_GET['cursor']) : '';
    $decodedCursor = $cursor === '' ? null : v1_runtime_cursor_decode($cursor);
    if ($cursor !== '' && $decodedCursor === null) { v1_respond(400,array('ok'=>false,'error'=>'invalid_cursor')); }
    $rows = v1_official_run_ledger_rows($pdo,$config,$from,$to); $eligible = array();
    foreach ($rows as $row) {
        $sortMysql = v1_official_run_ledger_sort_at($row);
        if ($sortMysql === null) { v1_respond(503,array('ok'=>false,'error'=>'official_run_ledger_integrity_error')); }
        if ($decodedCursor !== null && !($sortMysql > $decodedCursor[0]
            || ($sortMysql === $decodedCursor[0] && strcmp($row['run_id'],$decodedCursor[1]) > 0))) { continue; }
        $row['_sort_at'] = $sortMysql; $eligible[] = $row;
    }
    $hasMore = count($eligible) > $limit; $pageRows = array_slice($eligible,0,$limit); $nextCursor = null;
    if ($hasMore && count($pageRows) > 0) {
        $last = $pageRows[count($pageRows)-1]; $nextCursor = v1_runtime_cursor_encode($last['_sort_at'],$last['run_id']);
    }
    foreach ($pageRows as &$row) { unset($row['_sort_at']); } unset($row);
    v1_respond(200,array(
        'ok'=>true,
        'range'=>array('from'=>$from,'to'=>$to),
        'ledger_row_count'=>count($rows),
        'ledger_sha256'=>v1_official_run_ledger_hash($rows),
        'data'=>$pageRows,
        'pagination'=>array('limit'=>$limit,'returned'=>count($pageRows),'has_more'=>$hasMore,'next_cursor'=>$nextCursor),
    ));
}

function v1_dart_review_corpus_cursor_encode(string $from, string $to, string $publishedAt,
    string $documentId, string $eventId): string {
    $raw = implode("\x1f",array('dart-review-corpus-v1',$from,$to,$publishedAt,$documentId,$eventId));
    return rtrim(strtr(base64_encode($raw),'+/','-_'),'=');
}

function v1_dart_review_corpus_cursor_decode(string $cursor, string $from, string $to): ?array {
    if ($cursor === '' || strlen($cursor) > 512 || preg_match('/^[A-Za-z0-9_-]+$/',$cursor) !== 1) { return null; }
    $padding = strlen($cursor) % 4;
    if ($padding !== 0) { $cursor .= str_repeat('=',4-$padding); }
    $decoded = base64_decode(strtr($cursor,'-_','+/'),true);
    if (!is_string($decoded)) { return null; }
    $parts = explode("\x1f",$decoded);
    if (count($parts) !== 6 || $parts[0] !== 'dart-review-corpus-v1' || $parts[1] !== $from || $parts[2] !== $to
        || preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/',$parts[3]) !== 1
        || preg_match('/^dart:[0-9]{14}$/',$parts[4]) !== 1 || !v1_valid_entity_id($parts[5],96)) {
        return null;
    }
    return array($parts[3],$parts[4],$parts[5]);
}

function v1_dart_review_corpus_required_text(string $value, int $maxLength): bool {
    return $value !== '' && mb_check_encoding($value,'UTF-8')
        && mb_strlen($value,'UTF-8') <= $maxLength && preg_match('/\S/u',$value) === 1;
}

function v1_dart_review_corpus_token(string $value): bool {
    return preg_match('/^[a-z][a-z0-9_.:\-]{0,63}$/',$value) === 1;
}

function v1_dart_review_corpus_https_url(string $value): bool {
    if ($value === '' || strlen($value) > 65535 || preg_match('/[\x00-\x20\x7f]/',$value) === 1) {
        return false;
    }
    $parts = parse_url($value);
    return is_array($parts) && isset($parts['scheme'],$parts['host'])
        && strtolower((string)$parts['scheme']) === 'https' && trim((string)$parts['host']) !== ''
        && !isset($parts['user']) && !isset($parts['pass']) && !isset($parts['fragment']);
}

function v1_dart_review_corpus_item(array $row): array {
    $externalId = (string)$row['external_id']; $documentId = (string)$row['document_id'];
    $eventId = (string)$row['event_id']; $companyId = (string)$row['company_id'];
    $companyName = (string)$row['company_name']; $eventType = (string)$row['event_type'];
    $title = (string)$row['title']; $originalLanguage = (string)$row['original_language'];
    $originalUrl = (string)$row['original_url'];
    $eventVerification = (string)$row['event_verification_status'];
    $documentVerification = (string)$row['document_verification_status'];
    $documentPublication = (string)$row['document_publication_status'];
    $identityStatus = (string)$row['identity_status']; $reviewStatus = (string)$row['review_status'];
    $importance = (string)$row['importance'];
    $publishedAt = v1_release_iso_time($row['published_at']);
    $receiptDate = DateTimeImmutable::createFromFormat('!Ymd',substr($externalId,0,8),new DateTimeZone('UTC'));
    if (preg_match('/^[0-9]{14}$/',$externalId) !== 1 || $documentId !== 'dart:' . $externalId
        || !v1_valid_entity_id($eventId,96) || preg_match('/^[0-9]{8}$/',$companyId) !== 1 || $publishedAt === null
        || !$receiptDate || $receiptDate->format('Ymd') !== substr($externalId,0,8)
        || (string)$row['source_right_id'] !== 'official:dart') {
        throw new RuntimeException('dart_review_corpus_identity_error');
    }
    foreach (array($eventType,$eventVerification,$documentVerification,$documentPublication,
        $identityStatus,$reviewStatus,$importance) as $token) {
        if (!v1_dart_review_corpus_token($token)) {
            throw new RuntimeException('dart_review_corpus_metadata_error');
        }
    }
    if (!v1_dart_review_corpus_required_text($companyName,255)
        || !v1_dart_review_corpus_required_text($title,700)
        || preg_match('/^[a-z]{2,3}(?:-[A-Z]{2})?$/',$originalLanguage) !== 1
        || !v1_dart_review_corpus_https_url($originalUrl)) {
        throw new RuntimeException('dart_review_corpus_metadata_error');
    }
    $correctionOf = trim((string)($row['correction_of_document_id'] ?? ''));
    if ($correctionOf !== '' && (preg_match('/^dart:[0-9]{14}$/',$correctionOf) !== 1
        || $correctionOf === $documentId)) {
        throw new RuntimeException('dart_review_corpus_lineage_error');
    }
    $versionNo = (int)$row['version_no'];
    $hasLaterCorrection = (int)$row['has_later_correction'] === 1;
    $hasSuccessor = (int)$row['has_successor'] === 1;
    if ($versionNo < 1 || ($correctionOf !== '' && $versionNo < 2)
        || ($correctionOf === '' && $versionNo !== 1)) {
        throw new RuntimeException('dart_review_corpus_lineage_error');
    }
    $isCorrection = (int)$row['event_is_correction'] === 1 || $correctionOf !== '' || $versionNo > 1;
    $isCancelled = (int)$row['event_is_cancelled'] === 1
        || (string)$row['event_verification_status'] === 'withdrawn'
        || (string)$row['document_verification_status'] === 'withdrawn'
        || (string)$row['document_publication_status'] === 'withdrawn';
    if ($isCancelled) {
        $revisionStatus = $correctionOf !== '' ? 'withdrawal_linked' : 'withdrawal_unlinked';
    } elseif ($isCorrection) {
        $revisionStatus = $correctionOf !== '' ? 'correction_linked' : 'correction_unlinked';
    } else {
        $revisionStatus = ($hasLaterCorrection || $hasSuccessor) ? 'original_superseded' : 'current';
    }
    return array(
        'document_id'=>$documentId,
        'event_id'=>$eventId,
        'company_id'=>$companyId,
        'company_name'=>$companyName,
        'event_type'=>$eventType,
        'revision_status'=>$revisionStatus,
        'external_id'=>$externalId,
        'title'=>$title,
        'original_language'=>$originalLanguage,
        'original_url'=>$originalUrl,
        'published_at'=>$publishedAt,
        'source_right_id'=>(string)$row['source_right_id'],
        'correction_of_document_id'=>$correctionOf === '' ? null : $correctionOf,
        'version_no'=>$versionNo,
        'has_later_correction'=>$hasLaterCorrection,
        'has_successor'=>$hasSuccessor,
        'is_correction'=>$isCorrection,
        'is_cancelled'=>$isCancelled,
        'event_verification_status'=>$eventVerification,
        'document_verification_status'=>$documentVerification,
        'document_publication_status'=>$documentPublication,
        'identity_status'=>$identityStatus,
        'review_status'=>$reviewStatus,
        'importance'=>$importance,
    );
}

/**
 * Export the exact OpenDART review population without making it public.
 *
 * The full-range digest is independent of cursor and limit. Every target
 * document must have exactly one event link and a matching company row;
 * otherwise the complete corpus is rejected rather than silently sampled.
 */
function v1_ops_dart_review_corpus(PDO $pdo, array $config): void {
    header('Cache-Control: private, no-store');
    header('Vary: Authorization');
    if (!isset($_GET['from']) || !isset($_GET['to'])) {
        v1_respond(400,array('ok'=>false,'error'=>'dart_review_corpus_range_required'));
    }
    $from = v1_date_bound('from',''); $to = v1_date_bound('to','');
    $kst = new DateTimeZone('Asia/Seoul'); $utc = new DateTimeZone('UTC');
    $fromDate = new DateTimeImmutable($from,$kst); $toDate = new DateTimeImmutable($to,$kst);
    $serverDate = (new DateTimeImmutable('now',$kst))->format('Y-m-d');
    $rangeDays = (int)$fromDate->diff($toDate)->days;
    if ($fromDate >= $toDate || $rangeDays < 1 || $rangeDays > 31) {
        v1_respond(400,array('ok'=>false,'error'=>'dart_review_corpus_range_exceeds_31_days'));
    }
    if ($to > $serverDate) {
        v1_respond(400,array('ok'=>false,'error'=>'dart_review_corpus_future_range'));
    }
    $limit = 100;
    if (isset($_GET['limit'])) {
        $rawLimit = trim((string)$_GET['limit']);
        if (preg_match('/^[1-9][0-9]{0,2}$/',$rawLimit) !== 1 || (int)$rawLimit > 100) {
            v1_respond(400,array('ok'=>false,'error'=>'invalid_limit'));
        }
        $limit = (int)$rawLimit;
    }
    $cursor = isset($_GET['cursor']) ? trim((string)$_GET['cursor']) : '';
    $decodedCursor = $cursor === '' ? null : v1_dart_review_corpus_cursor_decode($cursor,$from,$to);
    if ($cursor !== '' && $decodedCursor === null) {
        v1_respond(400,array('ok'=>false,'error'=>'invalid_cursor'));
    }
    $startUtc = $fromDate->setTime(0,0,0)->setTimezone($utc)->format('Y-m-d H:i:s');
    $endUtc = $toDate->setTime(0,0,0)->setTimezone($utc)->format('Y-m-d H:i:s');
    $documents = table_name($config,'documents'); $eventDocuments = table_name($config,'event_documents');
    $events = table_name($config,'governance_events'); $companies = table_name($config,'companies');
    $params = array('official:dart','official_disclosure',$startUtc,$endUtc);
    $backendBindingId = v1_backend_binding_id($pdo,$config);

    $pdo->exec('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ');
    $pdo->exec('SET TRANSACTION READ ONLY');
    $pdo->beginTransaction();
    try {
        $integrity = $pdo->prepare('SELECT d.document_id,COUNT(ed.document_id) AS link_count,'
            . 'SUM(CASE WHEN e.event_id IS NOT NULL THEN 1 ELSE 0 END) AS event_count,'
            . 'SUM(CASE WHEN c.company_id IS NOT NULL THEN 1 ELSE 0 END) AS company_count,'
            . 'SUM(CASE WHEN d.company_id=e.company_id THEN 1 ELSE 0 END) AS company_match_count '
            . 'FROM ' . $documents . ' d LEFT JOIN ' . $eventDocuments . ' ed ON ed.document_id=d.document_id '
            . 'LEFT JOIN ' . $events . ' e ON e.event_id=ed.event_id '
            . 'LEFT JOIN ' . $companies . ' c ON c.company_id=e.company_id '
            . 'WHERE d.source_right_id=? AND d.source_class=? AND d.published_at>=? AND d.published_at<? '
            . 'GROUP BY d.document_id HAVING link_count<>1 OR event_count<>1 OR company_count<>1 OR company_match_count<>1 LIMIT 1');
        $integrity->execute($params);
        if ($integrity->fetch()) {
            $pdo->rollBack();
            v1_respond(503,array('ok'=>false,'error'=>'dart_review_corpus_integrity_error'));
        }
        $lineageIntegrity = $pdo->prepare('WITH RECURSIVE target_chain AS ('
            . 'SELECT d.document_id,d.correction_of_document_id,d.company_id,d.source_right_id,d.source_class,'
            . 'd.collection_key,d.version_no,d.external_id,d.published_at,d.retrieved_at,0 AS depth,'
            . 'CAST(CONCAT(\',\',d.document_id,\',\') AS CHAR(8192)) AS lineage_path,0 AS cycle_detected '
            . 'FROM ' . $documents . ' d WHERE d.source_right_id=? AND d.source_class=? '
            . 'AND d.published_at>=? AND d.published_at<? UNION ALL '
            . 'SELECT predecessor.document_id,predecessor.correction_of_document_id,predecessor.company_id,'
            . 'predecessor.source_right_id,predecessor.source_class,predecessor.collection_key,'
            . 'predecessor.version_no,predecessor.external_id,predecessor.published_at,predecessor.retrieved_at,'
            . 'chain.depth+1,CONCAT(chain.lineage_path,predecessor.document_id,\',\'),'
            . 'IF(LOCATE(CONCAT(\',\',predecessor.document_id,\',\'),chain.lineage_path)>0,1,0) '
            . 'FROM ' . $documents . ' predecessor JOIN target_chain chain '
            . 'ON predecessor.document_id=chain.correction_of_document_id '
            . 'WHERE chain.correction_of_document_id IS NOT NULL AND chain.correction_of_document_id<>\'\' '
            . 'AND chain.depth<64 AND chain.cycle_detected=0) '
            . 'SELECT chain.document_id FROM target_chain chain '
            . 'LEFT JOIN ' . $documents . ' predecessor ON predecessor.document_id=chain.correction_of_document_id '
            . 'WHERE chain.cycle_detected=1 '
            . 'OR (chain.depth>=64 AND chain.correction_of_document_id IS NOT NULL '
            . 'AND chain.correction_of_document_id<>\'\') '
            . 'OR (SELECT COUNT(*) FROM ' . $eventDocuments . ' current_link '
            . 'WHERE current_link.document_id=chain.document_id)<>1 '
            . 'OR (SELECT COUNT(*) FROM ' . $documents . ' successor '
            . 'WHERE successor.correction_of_document_id=chain.document_id)>1 '
            . 'OR (chain.correction_of_document_id IS NOT NULL AND chain.correction_of_document_id<>\'\' AND ('
            . 'predecessor.document_id IS NULL OR predecessor.source_right_id<>chain.source_right_id '
            . 'OR predecessor.source_class<>chain.source_class '
            . 'OR NOT (predecessor.company_id<=>chain.company_id) '
            . 'OR NOT (predecessor.collection_key<=>chain.collection_key) '
            . 'OR predecessor.version_no+1<>chain.version_no '
            . 'OR predecessor.external_id NOT REGEXP \'^[0-9]{14}$\' '
            . 'OR chain.external_id NOT REGEXP \'^[0-9]{14}$\' '
            . 'OR BINARY predecessor.external_id>=BINARY chain.external_id '
            . 'OR COALESCE(predecessor.published_at,predecessor.retrieved_at)>'
            . 'COALESCE(chain.published_at,chain.retrieved_at) '
            . 'OR (SELECT COUNT(*) FROM ' . $documents . ' successor '
            . 'WHERE successor.correction_of_document_id=predecessor.document_id)<>1 '
            . 'OR NOT EXISTS(SELECT 1 FROM ' . $eventDocuments . ' current_link '
            . 'JOIN ' . $eventDocuments . ' predecessor_link ON predecessor_link.event_id=current_link.event_id '
            . 'WHERE current_link.document_id=chain.document_id '
            . 'AND predecessor_link.document_id=predecessor.document_id))) LIMIT 1');
        $lineageIntegrity->execute($params);
        if ($lineageIntegrity->fetch()) {
            $pdo->rollBack();
            v1_respond(503,array('ok'=>false,'error'=>'dart_review_corpus_lineage_error'));
        }
        $stmt = $pdo->prepare('SELECT d.document_id,d.external_id,d.title,d.original_language,d.original_url,d.published_at,'
            . 'd.source_right_id,d.correction_of_document_id,d.version_no,d.verification_status AS document_verification_status,'
            . 'd.publication_status AS document_publication_status,ed.event_id,e.company_id,c.legal_name AS company_name,'
            . 'e.event_type,e.verification_status AS event_verification_status,e.identity_status,e.review_status,e.importance,'
            . 'CASE WHEN JSON_TYPE(JSON_EXTRACT(IF(JSON_VALID(d.payload_json),d.payload_json,\'{}\'),'
            . '\'$.has_later_correction\'))=\'BOOLEAN\' AND JSON_UNQUOTE(JSON_EXTRACT('
            . 'IF(JSON_VALID(d.payload_json),d.payload_json,\'{}\'),\'$.has_later_correction\'))=\'true\' '
            . 'THEN 1 ELSE 0 END AS has_later_correction,'
            . 'CASE WHEN JSON_TYPE(JSON_EXTRACT(IF(JSON_VALID(e.payload_json),e.payload_json,\'{}\'),'
            . '\'$.is_correction\'))=\'BOOLEAN\' AND JSON_UNQUOTE(JSON_EXTRACT('
            . 'IF(JSON_VALID(e.payload_json),e.payload_json,\'{}\'),\'$.is_correction\'))=\'true\' '
            . 'THEN 1 ELSE 0 END AS event_is_correction,'
            . 'CASE WHEN JSON_TYPE(JSON_EXTRACT(IF(JSON_VALID(e.payload_json),e.payload_json,\'{}\'),'
            . '\'$.is_cancelled\'))=\'BOOLEAN\' AND JSON_UNQUOTE(JSON_EXTRACT('
            . 'IF(JSON_VALID(e.payload_json),e.payload_json,\'{}\'),\'$.is_cancelled\'))=\'true\' '
            . 'THEN 1 ELSE 0 END AS event_is_cancelled,'
            . 'EXISTS(SELECT 1 FROM ' . $documents . ' successor WHERE successor.source_right_id=\'official:dart\' '
            . 'AND successor.source_class=\'official_disclosure\' AND successor.document_id<>d.document_id '
            . 'AND successor.correction_of_document_id=d.document_id) AS has_successor '
            . 'FROM ' . $documents . ' d JOIN ' . $eventDocuments . ' ed ON ed.document_id=d.document_id '
            . 'JOIN ' . $events . ' e ON e.event_id=ed.event_id JOIN ' . $companies . ' c ON c.company_id=e.company_id '
            . 'WHERE d.source_right_id=? AND d.source_class=? AND d.published_at>=? AND d.published_at<? '
            . 'ORDER BY d.published_at ASC,BINARY d.document_id ASC,BINARY e.event_id ASC');
        $stmt->execute($params);
        $hash = hash_init('sha256'); $populationCount = 0; $eligibleCount = 0; $pageItems = array();
        foreach ($stmt as $row) {
            $item = v1_dart_review_corpus_item($row); $populationCount++;
            hash_update($hash,v1_strict_canonical_json_encode($item,'dart_review_corpus_encode_failed') . "\n");
            $afterCursor = $decodedCursor === null || (string)$row['published_at'] > $decodedCursor[0]
                || ((string)$row['published_at'] === $decodedCursor[0] && strcmp((string)$row['document_id'],$decodedCursor[1]) > 0)
                || ((string)$row['published_at'] === $decodedCursor[0] && (string)$row['document_id'] === $decodedCursor[1]
                    && strcmp((string)$row['event_id'],$decodedCursor[2]) > 0);
            if (!$afterCursor) { continue; }
            $eligibleCount++;
            if (count($pageItems) < $limit) {
                $item['_cursor_published_at'] = (string)$row['published_at'];
                $pageItems[] = $item;
            }
        }
        $corpusSha = hash_final($hash); $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        if (strpos($e->getMessage(),'dart_review_corpus_') === 0) {
            v1_respond(503,array('ok'=>false,'error'=>$e->getMessage()));
        }
        throw $e;
    }

    do {
        $hasMore = $eligibleCount > count($pageItems); $nextCursor = null;
        if ($hasMore && count($pageItems) > 0) {
            $last = $pageItems[count($pageItems)-1];
            $nextCursor = v1_dart_review_corpus_cursor_encode($from,$to,$last['_cursor_published_at'],
                (string)$last['document_id'],(string)$last['event_id']);
        }
        $items = array();
        foreach ($pageItems as $item) { unset($item['_cursor_published_at']); $items[] = $item; }
        $payload = array('ok'=>true,'contract_version'=>'dart-review-corpus-v1','range'=>array('from'=>$from,'to'=>$to),
            'population_count'=>$populationCount,'corpus_sha256'=>$corpusSha,'backend_binding_id'=>$backendBindingId,
            'items'=>$items,'next_cursor'=>$nextCursor);
        $sized = $payload; $sized['api_version'] = 'v1';
        $encoded = json_encode($sized,JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if (!is_string($encoded)) {
            v1_respond(503,array('ok'=>false,'error'=>'dart_review_corpus_response_encode_failed'));
        }
        if (strlen($encoded) <= V1_RESPONSE_BUDGET_BYTES) { break; }
        array_pop($pageItems);
    } while (count($pageItems) > 0);
    if ($eligibleCount > 0 && count($pageItems) === 0 && strlen($encoded) > V1_RESPONSE_BUDGET_BYTES) {
        v1_respond(503,array('ok'=>false,'error'=>'dart_review_corpus_item_budget_exceeded'));
    }
    v1_respond(200,$payload);
}

function v1_ops_release_evidence(PDO $pdo, array $config): void {
    $kstNow = new DateTimeImmutable('now',new DateTimeZone('Asia/Seoul'));
    $to = v1_date_bound('to',$kstNow->format('Y-m-d'));
    $from = v1_date_bound('from',$kstNow->modify('-6 days')->format('Y-m-d'));
    $fromDate = new DateTimeImmutable($from,new DateTimeZone('Asia/Seoul'));
    $toDate = new DateTimeImmutable($to,new DateTimeZone('Asia/Seoul'));
    if ($toDate < $fromDate || $toDate->diff($fromDate)->days > 30) {
        v1_respond(400,array('ok'=>false,'error'=>'evidence_range_exceeds_31_days'));
    }
    list($start,$end) = v1_evidence_utc_bounds($from,$to);
    $epochBoundaryStmt = $pdo->prepare('SELECT COUNT(*) FROM ' . table_name($config,'official_slot_claim_epochs')
        . ' WHERE pipeline=? AND active_from > ? AND active_from <= ?');
    $epochBoundaryStmt->execute(array('ingest-official',$start,$end));
    if ((int)$epochBoundaryStmt->fetchColumn() > 0) {
        v1_official_slot_claim_error(409,'official_slot_epoch_boundary_in_evidence_range');
    }
    $runQueryStart = (new DateTimeImmutable($start,new DateTimeZone('UTC')))->modify('-1 day')->format('Y-m-d H:i:s');
    $runQueryEnd = (new DateTimeImmutable($end,new DateTimeZone('UTC')))->modify('+1 day')->format('Y-m-d H:i:s');

    $rawRunStmt = $pdo->prepare('SELECT run_id,pipeline,source_key,code_revision,status,started_at,finished_at,first_observed_at,raw_count,'
        . 'acknowledged_count,lag_seconds_p95,metrics_json FROM ' . table_name($config,'collection_runs')
        . ' WHERE COALESCE(finished_at,started_at) BETWEEN ? AND ? AND source_key IN (\'dart\',\'kind\',\'dart+kind\',\'kind+dart\') '
        . 'ORDER BY COALESCE(finished_at,started_at),run_id LIMIT 50000');
    $rawRunStmt->execute(array($runQueryStart,$runQueryEnd)); $rawRuns = $rawRunStmt->fetchAll();
    $releaseClaimStmt = $pdo->prepare('SELECT * FROM ' . table_name($config,'official_slot_claims')
        . ' WHERE scheduled_slot_at BETWEEN ? AND ?');
    $releaseClaimStmt->execute(array($runQueryStart,$runQueryEnd)); $releaseClaims = array();
    foreach ($releaseClaimStmt->fetchAll() as $claim) { $releaseClaims[(string)$claim['claim_id']] = $claim; }
    $runGroups = array(); $operations = array(); $runShas = array(); $lastOfficialSuccess = array();
    foreach ($rawRuns as $run) {
        $observedAt = (string)($run['finished_at'] ?: $run['started_at']);
        $revision = v1_valid_build_sha($run['code_revision']);
        $sourceKey = strtolower((string)$run['source_key']); $status = strtolower((string)$run['status']);
        $ledgerRun = v1_official_run_ledger_row($run);
        $claimId = isset($ledgerRun['slot_claim_id']) && is_string($ledgerRun['slot_claim_id']) ? $ledgerRun['slot_claim_id'] : '';
        if ($claimId !== '' && isset($releaseClaims[$claimId])
            && v1_official_claim_matches_ledger_run($releaseClaims[$claimId],$ledgerRun,$run)) {
            $ledgerRun['slot_claim_status'] = (string)$releaseClaims[$claimId]['status'];
            $ledgerRun['slot_claim_terminal_reason'] = $releaseClaims[$claimId]['terminal_reason'] === null
                ? null : (string)$releaseClaims[$claimId]['terminal_reason'];
        }
        $scheduledIncremental = v1_official_scheduled_run_matches($ledgerRun);
        $scheduledSlot = $scheduledIncremental ? (string)$ledgerRun['scheduled_slot_at'] : null;
        $dateBasis = $scheduledIncremental ? v1_mysql_datetime_utc($scheduledSlot) : $observedAt;
        $day = $dateBasis === null ? null : v1_kst_observation_date($dateBasis);
        $succeeded = in_array($status,array('success','succeeded'),true)
            && (int)$ledgerRun['raw_count'] === (int)$ledgerRun['acknowledged_count']
            && (!$scheduledIncremental || ($ledgerRun['slot_claim_status'] === 'completed'
                && $ledgerRun['slot_claim_late'] === false));
        if ($day !== null && $day < $from && $scheduledIncremental && $revision !== null && $succeeded) {
            // The one-day query buffer supplies the immediately preceding
            // successful poll so the first in-range DART interval is actual,
            // not silently dropped or synthesized from cron slots.
            $epoch = strtotime($observedAt . ' UTC');
            if ($epoch !== false) {
                foreach ($ledgerRun['source_outcomes'] as $carrySource => $carryOutcome) {
                    $carryStatus = strtolower((string)$carryOutcome['status']);
                    if (in_array($carrySource,array('dart','kind'),true)
                        && in_array($carryStatus,array('success','succeeded'),true)
                        && is_int($carryOutcome['raw_count']) && is_int($carryOutcome['acknowledged_count'])
                        && $carryOutcome['raw_count'] === $carryOutcome['acknowledged_count']) {
                        $lastKey = $carrySource . '|' . $revision;
                        if (!isset($lastOfficialSuccess[$lastKey]) || $epoch > $lastOfficialSuccess[$lastKey]) {
                            $lastOfficialSuccess[$lastKey] = $epoch;
                        }
                    }
                }
            }
        }
        if ($day === null || $day < $from || $day > $to) { continue; }
        $runKey = $day . '|' . $sourceKey . '|' . (string)$revision;
        if (!isset($runGroups[$runKey])) {
            $runGroups[$runKey] = array('observation_date'=>$day,'source_key'=>$sourceKey,'code_revision'=>$revision,
                'attempt_count'=>0,'success_count'=>0,'raw_count'=>0,'acknowledged_count'=>0,'first_observed_at'=>null,'last_finished_at'=>null);
        }
        $runGroups[$runKey]['attempt_count']++;
        if ($succeeded) { $runGroups[$runKey]['success_count']++; }
        $runGroups[$runKey]['raw_count'] += (int)$run['raw_count'];
        $runGroups[$runKey]['acknowledged_count'] += (int)$run['acknowledged_count'];
        if ($run['first_observed_at'] !== null && ($runGroups[$runKey]['first_observed_at'] === null
            || (string)$run['first_observed_at'] < $runGroups[$runKey]['first_observed_at'])) {
            $runGroups[$runKey]['first_observed_at'] = (string)$run['first_observed_at'];
        }
        if ($run['finished_at'] !== null && ($runGroups[$runKey]['last_finished_at'] === null
            || (string)$run['finished_at'] > $runGroups[$runKey]['last_finished_at'])) {
            $runGroups[$runKey]['last_finished_at'] = (string)$run['finished_at'];
        }
        if ($revision === null) { continue; }
        $runShas[$revision] = true; $opKey = $day . '|' . $revision;
        if (!isset($operations[$opKey])) {
            $operations[$opKey] = array('observation_date'=>$day,'code_revision'=>$revision,
                'official_ingest_expected_count'=>0,'official_ingest_succeeded_count'=>0,
                'dart_expected_count'=>0,'dart_succeeded_count'=>0,'dart_raw_count'=>0,'dart_acknowledged_count'=>0,
                'kind_expected_count'=>0,'kind_succeeded_count'=>0,'kind_raw_count'=>0,'kind_acknowledged_count'=>0,
                'scheduled_source_slots'=>array('dart'=>array(),'kind'=>array()),
                'official_lag_values'=>array(),'dart_poll_intervals'=>array(),'kind_lag_values'=>array(),
                'kind_observation_count'=>0,'kind_lag_sample_count'=>0,'content_snapshot_at'=>null,'content_scope'=>null,
                'web_distribution_attempted_count'=>0,'web_distribution_succeeded_count'=>0,
                'official_evidence_total_count'=>null,'official_evidence_linked_count'=>null,
                'top_sensitive_total_count'=>null,'top_sensitive_reviewed_count'=>null,
                'original_language_total_count'=>null,'original_language_preserved_count'=>null,
                'source_right_total_count'=>null,'valid_source_right_count'=>null);
        }
        $metrics = json_decode((string)$run['metrics_json'],true); if (!is_array($metrics)) { $metrics = array(); }
        $outcomes = isset($metrics['source_outcomes']) && is_array($metrics['source_outcomes']) ? $metrics['source_outcomes']
            : (isset($metrics['metrics']['source_outcomes']) && is_array($metrics['metrics']['source_outcomes']) ? $metrics['metrics']['source_outcomes'] : array());
        $sources = array();
        foreach (array('dart','kind') as $officialSource) {
            if (isset($outcomes[$officialSource]) && is_array($outcomes[$officialSource])) {
                $outcomeStatus = strtolower(trim((string)($outcomes[$officialSource]['status'] ?? '')));
                if ($outcomeStatus !== '' && $outcomeStatus !== 'disabled') { $sources[$officialSource] = $outcomeStatus; }
            } elseif (in_array($officialSource,explode('+',$sourceKey),true)) { $sources[$officialSource] = $status; }
        }
        foreach ($sources as $officialSource => $sourceStatus) {
            if (!$scheduledIncremental || isset($operations[$opKey]['scheduled_source_slots'][$officialSource][$scheduledSlot])) {
                continue;
            }
            $operations[$opKey]['scheduled_source_slots'][$officialSource][$scheduledSlot] = true;
            $sourceOutcome = isset($ledgerRun['source_outcomes'][$officialSource])
                ? $ledgerRun['source_outcomes'][$officialSource] : array('raw_count'=>null,'acknowledged_count'=>null);
            $sourceRaw = $sourceOutcome['raw_count']; $sourceAck = $sourceOutcome['acknowledged_count'];
            $sourceSucceeded = $succeeded && in_array($sourceStatus,array('success','succeeded'),true)
                && is_int($sourceRaw) && is_int($sourceAck) && $sourceRaw === $sourceAck;
            $operations[$opKey]['official_ingest_expected_count']++;
            $operations[$opKey][$officialSource . '_expected_count']++;
            if ($sourceSucceeded) {
                $operations[$opKey]['official_ingest_succeeded_count']++;
                $operations[$opKey][$officialSource . '_succeeded_count']++;
                // DART has no authoritative receipt timestamp; measure the
                // interval between actual successful polls, never cron slots.
                $epoch = strtotime($observedAt . ' UTC'); $lastKey = $officialSource . '|' . $revision;
                if ($officialSource === 'dart' && $epoch !== false && isset($lastOfficialSuccess[$lastKey])) {
                    $operations[$opKey]['dart_poll_intervals'][] = (float)max(0,$epoch-$lastOfficialSuccess[$lastKey]);
                }
                if ($epoch !== false) { $lastOfficialSuccess[$lastKey] = $epoch; }
            }
            if ($sourceRaw !== null) { $operations[$opKey][$officialSource . '_raw_count'] += $sourceRaw; }
            if ($sourceAck !== null) { $operations[$opKey][$officialSource . '_acknowledged_count'] += $sourceAck; }
        }
        if ($scheduledIncremental && is_numeric($run['lag_seconds_p95'])) {
            $operations[$opKey]['official_lag_values'][] = (float)$run['lag_seconds_p95'];
        }
        if (preg_match('/(?:^|[-_])(publish|pages-deploy|deploy-pages|web-distribution)(?:$|[-_])/',$run['pipeline']) === 1) {
            $operations[$opKey]['web_distribution_attempted_count']++;
            if ($succeeded) { $operations[$opKey]['web_distribution_succeeded_count']++; }
        }
    }

    // Availability is a strict seven-day, four-route, 288-slot/day contract.
    // Its day begins at KST 00:01 and the final 23:56 slot owns the next
    // civil day's 00:00 minute.  Query those exact raw timestamp edges.
    $availabilityFromDate = $toDate->modify('-6 days');
    if ($availabilityFromDate < $fromDate) { $availabilityFromDate = $fromDate; }
    $availabilityFrom = $availabilityFromDate->format('Y-m-d');
    list($availabilityStart,$availabilityEnd) = v1_availability_utc_bounds($availabilityFrom,$to);
    $availabilityStmt = $pdo->prepare('SELECT observation_id,observed_at,route_template,http_status,duration_ms,succeeded,build_sha,source FROM '
        . table_name($config,'availability_observations') . ' WHERE observed_at BETWEEN ? AND ? ORDER BY observed_at,observation_id LIMIT 50001');
    $availabilityStmt->execute(array($availabilityStart,$availabilityEnd)); $availability = $availabilityStmt->fetchAll();
    if (count($availability) > 50000) { v1_respond(503,array('ok'=>false,'error'=>'availability_evidence_row_limit_exceeded')); }
    $successCount = 0; $durations = array(); $intervals = array(); $availabilityShas = array(); $availabilityGroups = array();
    foreach ($availability as $row) {
        $success = (int)$row['succeeded'] === 1; if ($success) { $successCount++; }
        $duration = (float)$row['duration_ms']; $durations[] = $duration; $sha = (string)$row['build_sha']; $availabilityShas[$sha] = true;
        $bucket = v1_availability_cadence_bucket((string)$row['observed_at']);
        if ($bucket === null) { v1_respond(503,array('ok'=>false,'error'=>'availability_observed_at_integrity_error')); }
        $route = (string)$row['route_template']; $day = (string)$bucket['observation_date'];
        if ($day < $availabilityFrom || $day > $to) {
            v1_respond(503,array('ok'=>false,'error'=>'availability_cadence_window_integrity_error'));
        }
        $groupKey = $day . '|' . $route . '|' . $sha;
        if (!isset($availabilityGroups[$groupKey])) {
            $availabilityGroups[$groupKey] = array('observation_date'=>$day,'route_template'=>$route,'build_sha'=>$sha,
                'raw_attempt_count'=>0,'raw_success_count'=>0,'raw_failure_count'=>0,'durations'=>array(),'intervals'=>array(),
                'failure_intervals'=>array(),'slot_indexes'=>array(),'duplicate_slot_count'=>0,'off_cadence_count'=>0,
                'epochs'=>array(),'first_observed_at'=>null,'last_observed_at'=>null,'last_epoch'=>null);
        }
        $group =& $availabilityGroups[$groupKey];
        $group['raw_attempt_count']++;
        $group[$success ? 'raw_success_count' : 'raw_failure_count']++;
        $group['durations'][] = $duration;
        $slotIndex = (int)$bucket['slot_index']; $epoch = (int)$bucket['epoch'];
        if (isset($group['slot_indexes'][$slotIndex])) { $group['duplicate_slot_count']++; }
        else { $group['slot_indexes'][$slotIndex] = true; }
        if ($group['last_epoch'] !== null) {
            $interval = (float)max(0,$epoch-(int)$group['last_epoch']);
            $group['intervals'][] = $interval;
            if (!$success) { $group['failure_intervals'][] = $interval; }
        }
        $group['last_epoch'] = $epoch; $group['epochs'][] = $epoch;
        if ($group['first_observed_at'] === null) { $group['first_observed_at'] = (string)$row['observed_at']; }
        $group['last_observed_at'] = (string)$row['observed_at'];
        unset($group);
    }
    $availabilitySummary = array();
    foreach ($availabilityGroups as $group) {
        $attempted = (int)$group['raw_attempt_count']; $covered = count($group['slot_indexes']);
        $missing = GOV_V1_AVAILABILITY_SLOTS_PER_DAY - $covered;
        $groupIntervals = $group['intervals'];
        foreach ($groupIntervals as $interval) { $intervals[] = $interval; }
        $epochs = $group['epochs']; sort($epochs,SORT_NUMERIC);
        list($dayStartEpoch,$dayEndEpoch) = v1_availability_day_edges((string)$group['observation_date']);
        $edgeAndActualGaps = array();
        if ($epochs) {
            $edgeAndActualGaps[] = (float)max(0,$epochs[0]-$dayStartEpoch);
            for ($index=1; $index<count($epochs); $index++) {
                $edgeAndActualGaps[] = (float)max(0,$epochs[$index]-$epochs[$index-1]);
            }
            $edgeAndActualGaps[] = (float)max(0,$dayEndEpoch-$epochs[count($epochs)-1]);
        } else { $edgeAndActualGaps[] = (float)($dayEndEpoch-$dayStartEpoch); }
        $availabilitySummary[] = array('observation_date'=>$group['observation_date'],'route_template'=>$group['route_template'],'build_sha'=>$group['build_sha'],
            'raw_attempt_count'=>$attempted,'raw_success_count'=>$group['raw_success_count'],'raw_failure_count'=>$group['raw_failure_count'],
            'success_rate_denominator'=>$attempted,'success_rate'=>$attempted > 0 ? $group['raw_success_count']/$attempted : null,
            'duration_ms_p95'=>v1_percentile($group['durations'],0.95),
            'cadence_id'=>GOV_V1_AVAILABILITY_CADENCE_ID,
            'expected_slot_count'=>GOV_V1_AVAILABILITY_SLOTS_PER_DAY,
            'covered_slot_count'=>$covered,'missing_slot_count'=>$missing,
            'duplicate_slot_count'=>(int)$group['duplicate_slot_count'],'off_cadence_count'=>(int)$group['off_cadence_count'],
            'covered_slots_bitmap_hex'=>v1_availability_bitmap_hex(array_keys($group['slot_indexes'])),
            'first_observed_at'=>v1_release_iso_time($group['first_observed_at']),
            'last_observed_at'=>v1_release_iso_time($group['last_observed_at']),
            'actual_interval_seconds_p95'=>v1_percentile($groupIntervals,0.95),
            'actual_max_gap_seconds'=>max($edgeAndActualGaps),
            // Compatibility alias; the actual_* fields above are authoritative.
            'observation_interval_seconds_p95'=>v1_percentile($groupIntervals,0.95),
            'failure_detection_seconds_p95'=>v1_percentile($group['failure_intervals'],0.95));
    }
    usort($availabilitySummary,function($a,$b) {
        return strcmp($a['observation_date'].'|'.$a['route_template'].'|'.$a['build_sha'],
            $b['observation_date'].'|'.$b['route_template'].'|'.$b['build_sha']);
    });

    $distributionStmt = $pdo->prepare('SELECT observed_at,distribution_target,duration_ms,succeeded,build_sha,workflow_run_id,failure_detected_at,source FROM '
        . table_name($config,'web_distribution_observations') . ' WHERE observed_at BETWEEN ? AND ? ORDER BY observed_at,observation_id LIMIT 50000');
    $distributionStmt->execute(array($start,$end)); $distributionGroups = array(); $distributionShas = array();
    foreach ($distributionStmt->fetchAll() as $row) {
        $day = v1_kst_observation_date((string)$row['observed_at']); if ($day === null) { continue; }
        $sha = (string)$row['build_sha']; $distributionShas[$sha] = true; $key = $day . '|' . $sha;
        if (!isset($distributionGroups[$key])) {
            $distributionGroups[$key] = array('observation_date'=>$day,'code_revision'=>$sha,'raw_attempt_count'=>0,
                'raw_success_count'=>0,'raw_failure_count'=>0,'targets'=>array(),'durations'=>array(),'failure_detection'=>array());
        }
        $succeeded = (int)$row['succeeded'] === 1; $distributionGroups[$key]['raw_attempt_count']++;
        $distributionGroups[$key][$succeeded ? 'raw_success_count' : 'raw_failure_count']++;
        $distributionGroups[$key]['targets'][(string)$row['distribution_target']] = true;
        $distributionGroups[$key]['durations'][] = (float)$row['duration_ms'];
        if (!$succeeded && $row['failure_detected_at'] !== null) {
            $observedEpoch = strtotime((string)$row['observed_at'] . ' UTC');
            $detectedEpoch = strtotime((string)$row['failure_detected_at'] . ' UTC');
            if ($observedEpoch !== false && $detectedEpoch !== false) {
                $distributionGroups[$key]['failure_detection'][] = (float)max(0,$detectedEpoch-$observedEpoch);
            }
        }
    }
    $distributionDays = array();
    foreach ($distributionGroups as $key => $group) {
        $attempted = $group['raw_attempt_count'];
        $distributionDays[] = array('observation_date'=>$group['observation_date'],'code_revision'=>$group['code_revision'],
            'raw_attempt_count'=>$attempted,'raw_success_count'=>$group['raw_success_count'],'raw_failure_count'=>$group['raw_failure_count'],
            'success_rate_denominator'=>$attempted,'success_rate'=>$attempted > 0 ? $group['raw_success_count']/$attempted : null,
            'distribution_targets'=>array_values(array_keys($group['targets'])),'duration_ms_p95'=>v1_percentile($group['durations'],0.95),
            'failure_detection_seconds_p95'=>v1_percentile($group['failure_detection'],0.95));
    }

    $vitalsStmt = $pdo->prepare('SELECT measured_at,route_template,metric_name,metric_value,device_class,build_sha,source FROM '
        . table_name($config,'web_vital_observations') . ' WHERE measured_at BETWEEN ? AND ? AND expires_at > UTC_TIMESTAMP() ORDER BY measured_at LIMIT 50000');
    $vitalsStmt->execute(array($start,$end)); $vitals = $vitalsStmt->fetchAll(); $vitalGroups = array(); $vitalShas = array();
    foreach ($vitals as $row) {
        $day = v1_kst_observation_date((string)$row['measured_at']); if ($day === null) { continue; }
        $sha = (string)$row['build_sha']; $vitalShas[$sha] = true;
        $key = $day . '|' . (string)$row['route_template'] . '|' . (string)$row['metric_name'] . '|'
            . (string)$row['device_class'] . '|' . $sha;
        if (!isset($vitalGroups[$key])) {
            $vitalGroups[$key] = array('observation_date'=>$day,'route_template'=>(string)$row['route_template'],
                'metric_name'=>(string)$row['metric_name'],'device_class'=>(string)$row['device_class'],'build_sha'=>$sha,'values'=>array());
        }
        $vitalGroups[$key]['values'][] = (float)$row['metric_value'];
    }
    $vitalSummary = array();
    foreach ($vitalGroups as $group) {
        $vitalSummary[] = array('observation_date'=>$group['observation_date'],'route_template'=>$group['route_template'],
            'metric_name'=>$group['metric_name'],'device_class'=>$group['device_class'],'build_sha'=>$group['build_sha'],
            'sample_count'=>count($group['values']),'p75'=>v1_percentile($group['values'],0.75));
    }

    // Content quality is not a "rows created today" metric. The immutable
    // quality writer verifies a full 2021+ corpus snapshot at KST day end;
    // exporter fallback below computes the same DB snapshot only before that
    // day's immutable observation exists.
    $revisionsByDay = array();
    foreach ($operations as $group) { $revisionsByDay[$group['observation_date']][$group['code_revision']] = true; }
    $actualKindStatsByDay = v1_kind_observation_stats_by_day($pdo,$config,$from,$to);
    $qualityStmt = $pdo->prepare('SELECT observation_id,observation_date,code_revision,dart_success_poll_interval_p95_minutes,'
        . 'kind_observation_lag_p95_minutes,kind_observation_count,kind_lag_sample_count,content_snapshot_at,content_scope,'
        . 'official_evidence_total_count,official_evidence_linked_count,same_story_evaluated_pair_count,'
        . 'same_story_predicted_same_count,same_story_true_positive_count,top_sensitive_total_count,top_sensitive_reviewed_count,'
        . 'original_language_total_count,original_language_preserved_count,source_right_total_count,valid_source_right_count,source,payload_sha256,created_at FROM '
        . table_name($config,'governance_quality_observations') . ' WHERE observation_date BETWEEN ? AND ? ORDER BY observation_date,code_revision');
    $qualityStmt->execute(array($from,$to)); $qualityRows = $qualityStmt->fetchAll(); $qualityByKey = array(); $qualityShas = array();
    foreach ($qualityRows as $quality) {
        $qualityCounts = array();
        foreach (array('official_evidence_total_count','official_evidence_linked_count','top_sensitive_total_count','top_sensitive_reviewed_count',
            'original_language_total_count','original_language_preserved_count','source_right_total_count','valid_source_right_count') as $field) {
            $qualityCounts[$field] = (int)$quality[$field];
        }
        if ((int)$quality['same_story_evaluated_pair_count'] !== 0 || (int)$quality['same_story_predicted_same_count'] !== 0
            || (int)$quality['same_story_true_positive_count'] !== 0) {
            v1_respond(503,array('ok'=>false,'error'=>'quality_observation_reserved_field_error',
                'observation_id'=>(string)$quality['observation_id']));
        }
        $qualityKindLag = $quality['kind_observation_lag_p95_minutes'] === null
            ? null : (float)$quality['kind_observation_lag_p95_minutes'];
        $qualityKindObservationCount = (int)$quality['kind_observation_count'];
        $qualityKindLagSampleCount = (int)$quality['kind_lag_sample_count'];
        $qualitySnapshotAt = (string)$quality['content_snapshot_at'];
        $qualityScope = (string)$quality['content_scope'];
        if (($qualityKindObservationCount === 0 && ($qualityKindLagSampleCount !== 0 || $qualityKindLag !== null))
            || ($qualityKindObservationCount > 0 && ($qualityKindLagSampleCount !== $qualityKindObservationCount || $qualityKindLag === null))
            || v1_release_iso_time($qualitySnapshotAt) === null
            || $qualityScope !== 'governance_corpus_2021_plus_kst_day_end_v2') {
            v1_respond(503,array('ok'=>false,'error'=>'quality_observation_scope_error',
                'observation_id'=>(string)$quality['observation_id']));
        }
        $qualityHash = v1_quality_observation_payload_hash((string)$quality['observation_date'],(string)$quality['code_revision'],
            (float)$quality['dart_success_poll_interval_p95_minutes'],$qualityKindLag,$qualityKindObservationCount,
            $qualityKindLagSampleCount,$qualitySnapshotAt,$qualityScope,
            $qualityCounts,(string)$quality['source']);
        if (!hash_equals((string)$quality['payload_sha256'],$qualityHash)) {
            v1_respond(503,array('ok'=>false,'error'=>'quality_observation_integrity_error',
                'observation_id'=>(string)$quality['observation_id']));
        }
        $qualityByKey[(string)$quality['observation_date'].'|'.(string)$quality['code_revision']] = $quality;
        $qualityShas[(string)$quality['code_revision']] = true;
    }
    foreach ($operations as &$group) {
        $day = $group['observation_date']; $singleRevision = isset($revisionsByDay[$day]) && count($revisionsByDay[$day]) === 1;
        $kindStats = $singleRevision && isset($actualKindStatsByDay[$day]) ? $actualKindStatsByDay[$day]
            : array('observation_count'=>0,'lag_sample_count'=>0,'lag_seconds'=>array());
        $group['kind_lag_values'] = $kindStats['lag_seconds'];
        $group['kind_observation_count'] = (int)$kindStats['observation_count'];
        $group['kind_lag_sample_count'] = (int)$kindStats['lag_sample_count'];
        if ($singleRevision) {
            $snapshot = v1_content_corpus_snapshot($pdo,$config,$day);
            foreach ($snapshot['raw_counts'] as $field => $value) { $group[$field] = $value; }
            $group['content_snapshot_at'] = $snapshot['snapshot_at_iso'];
            $group['content_scope'] = $snapshot['content_scope'];
            $group['content_metric_assignment'] = 'database_corpus_snapshot';
        } else { $group['content_metric_assignment'] = 'ambiguous_multiple_revisions'; }
        $qualityKey = $day . '|' . $group['code_revision'];
        if (isset($qualityByKey[$qualityKey])) {
            $quality = $qualityByKey[$qualityKey];
            foreach (array('official_evidence_total_count','official_evidence_linked_count','top_sensitive_total_count','top_sensitive_reviewed_count',
                'original_language_total_count','original_language_preserved_count','source_right_total_count','valid_source_right_count') as $field) {
                $group[$field] = (int)$quality[$field];
            }
            $group['quality_observation_id'] = (string)$quality['observation_id'];
            $group['quality_payload_sha256'] = (string)$quality['payload_sha256'];
            $group['kind_observation_count'] = (int)$quality['kind_observation_count'];
            $group['kind_lag_sample_count'] = (int)$quality['kind_lag_sample_count'];
            $group['content_snapshot_at'] = v1_release_iso_time((string)$quality['content_snapshot_at']);
            $group['content_scope'] = (string)$quality['content_scope'];
            $group['content_metric_assignment'] = 'immutable_quality_observation';
        }
        // Only the three incremental cron schedules form the denominator.
        // Manual, backfill and company-master runs remain in the raw ledger.
        $group['dart_expected_count'] = 82;
        $group['kind_expected_count'] = 82;
        $group['official_ingest_expected_count'] = 164;
        $group['dart_succeeded_count'] = min(82,(int)$group['dart_succeeded_count']);
        $group['kind_succeeded_count'] = min(82,(int)$group['kind_succeeded_count']);
        $group['official_ingest_succeeded_count'] = $group['dart_succeeded_count'] + $group['kind_succeeded_count'];
        $group['official_ingest_failed_count'] = max(0,$group['official_ingest_expected_count']-$group['official_ingest_succeeded_count']);
        $group['official_ingest_success_rate'] = $group['official_ingest_expected_count'] > 0
            ? $group['official_ingest_succeeded_count']/$group['official_ingest_expected_count'] : null;
        $group['dart_success_poll_interval_seconds_p95'] = v1_percentile($group['dart_poll_intervals'],0.95);
        $group['kind_first_observed_lag_seconds_p95'] = v1_percentile($group['kind_lag_values'],0.95);
        $group['official_lag_seconds_p95'] = v1_percentile(array_merge($group['official_lag_values'],$group['dart_poll_intervals']),0.95);
        $group['dart_success_poll_interval_p95_minutes'] = $group['dart_success_poll_interval_seconds_p95'] !== null
            ? $group['dart_success_poll_interval_seconds_p95']/60.0 : null;
        $group['kind_observation_lag_p95_minutes'] = $group['kind_first_observed_lag_seconds_p95'] !== null
            ? $group['kind_first_observed_lag_seconds_p95']/60.0 : null;
        if (isset($qualityByKey[$qualityKey])) {
            $group['dart_success_poll_interval_p95_minutes'] = (float)$qualityByKey[$qualityKey]['dart_success_poll_interval_p95_minutes'];
            $group['kind_observation_lag_p95_minutes'] = $qualityByKey[$qualityKey]['kind_observation_lag_p95_minutes'] === null
                ? null : (float)$qualityByKey[$qualityKey]['kind_observation_lag_p95_minutes'];
        }
        if (isset($distributionGroups[$qualityKey])) {
            $distribution = $distributionGroups[$qualityKey];
            $group['web_distribution_attempted_count'] = (int)$distribution['raw_attempt_count'];
            $group['web_distribution_succeeded_count'] = (int)$distribution['raw_success_count'];
        }
        $group['web_distribution_success_rate'] = $group['web_distribution_attempted_count'] > 0
            ? $group['web_distribution_succeeded_count']/$group['web_distribution_attempted_count'] : null;
        $watchdogIntervals = array();
        foreach ($availabilitySummary as $availabilityGroup) {
            if ($availabilityGroup['observation_date'] === $day && $availabilityGroup['build_sha'] === $group['code_revision']
                && $availabilityGroup['observation_interval_seconds_p95'] !== null) {
                $watchdogIntervals[] = $availabilityGroup['observation_interval_seconds_p95'];
            }
        }
        $group['web_failure_detection_observation_interval_seconds_p95'] = v1_percentile($watchdogIntervals,0.95);
        $group['raw_counts'] = array(
            'official_evidence_total_count'=>$group['official_evidence_total_count'],
            'official_evidence_linked_count'=>$group['official_evidence_linked_count'],
            'top_sensitive_total_count'=>$group['top_sensitive_total_count'],
            'top_sensitive_reviewed_count'=>$group['top_sensitive_reviewed_count'],
            'original_language_total_count'=>$group['original_language_total_count'],
            'original_language_preserved_count'=>$group['original_language_preserved_count'],
            'source_right_total_count'=>$group['source_right_total_count'],
            'valid_source_right_count'=>$group['valid_source_right_count'],
        );
        unset($group['scheduled_source_slots'],$group['official_lag_values'],$group['dart_poll_intervals'],$group['kind_lag_values']);
    }
    unset($group);

    $shadowStatusStmt = $pdo->prepare('SELECT observation_date,code_revision,review_status,COUNT(*) AS raw_count '
        . 'FROM ' . table_name($config,'shadow_discrepancies') . ' WHERE observation_date BETWEEN ? AND ? '
        . 'GROUP BY observation_date,code_revision,review_status ORDER BY observation_date,code_revision,review_status');
    $shadowStatusStmt->execute(array($from,$to)); $shadowStatus = array(); $shadowOverall = array(); $shadowRows = array();
    foreach ($shadowStatusStmt->fetchAll() as $row) {
        $key = (string)$row['observation_date'] . '|' . (string)$row['code_revision'];
        if (!isset($shadowStatus[$key])) { $shadowStatus[$key] = array('review_status_counts'=>array()); }
        $count = (int)$row['raw_count']; $status = (string)$row['review_status'];
        $shadowStatus[$key]['review_status_counts'][$status] = $count;
        if (!isset($shadowOverall[$status])) { $shadowOverall[$status] = 0; } $shadowOverall[$status] += $count;
        $shadowRows[] = array('observation_date'=>(string)$row['observation_date'],'code_revision'=>(string)$row['code_revision'],
            'review_status'=>$status,'raw_count'=>$count);
    }
    $shadowRunStmt = $pdo->prepare('SELECT observation_date,code_revision,legacy_status,candidate_status,legacy_comparison_keys_json,'
        . 'candidate_comparison_keys_json,legacy_event_count,candidate_event_count,legacy_events_sha256,candidate_events_sha256,'
        . 'legacy_crosswalk_schema_version,legacy_eligible_record_count,legacy_crosswalked_record_count,'
        . 'legacy_unmatched_record_count,legacy_ambiguous_record_count,legacy_crosswalk_coverage_rate,legacy_crosswalk_sha256,updated_at FROM '
        . table_name($config,'shadow_run_observations') . ' WHERE observation_date BETWEEN ? AND ? ORDER BY observation_date,code_revision');
    $shadowRunStmt->execute(array($from,$to)); $shadowDays = array(); $shadowShas = array();
    foreach ($shadowRunStmt->fetchAll() as $row) {
        $shadowShas[(string)$row['code_revision']] = true;
        $legacyJson = (string)$row['legacy_comparison_keys_json']; $candidateJson = (string)$row['candidate_comparison_keys_json'];
        $legacyKeys = json_decode($legacyJson,true); $candidateKeys = json_decode($candidateJson,true);
        if (!is_array($legacyKeys) || !is_array($candidateKeys)
            || count($legacyKeys) !== (int)$row['legacy_event_count'] || count($candidateKeys) !== (int)$row['candidate_event_count']
            || hash('sha256',$legacyJson) !== (string)$row['legacy_events_sha256']
            || hash('sha256',$candidateJson) !== (string)$row['candidate_events_sha256']) {
            v1_respond(503,array('ok'=>false,'error'=>'shadow_run_integrity_error','observation_date'=>$row['observation_date'],'code_revision'=>$row['code_revision']));
        }
        $legacyCrosswalk = v1_shadow_crosswalk_response($row);
        $key = (string)$row['observation_date'] . '|' . (string)$row['code_revision'];
        $reviewCounts = isset($shadowStatus[$key]) ? $shadowStatus[$key]['review_status_counts'] : array();
        $pending = isset($reviewCounts['pending']) ? (int)$reviewCounts['pending'] : 0; $total = array_sum($reviewCounts);
        $legacyEvents = array(); foreach ($legacyKeys as $comparisonKey) { $legacyEvents[] = array('comparison_key'=>(string)$comparisonKey); }
        $candidateEvents = array(); foreach ($candidateKeys as $comparisonKey) { $candidateEvents[] = array('comparison_key'=>(string)$comparisonKey); }
        $shadowDays[] = array('observation_date'=>(string)$row['observation_date'],'code_revision'=>(string)$row['code_revision'],
            'legacy_status'=>(string)$row['legacy_status'],'candidate_status'=>(string)$row['candidate_status'],
            'legacy_events'=>$legacyEvents,'candidate_events'=>$candidateEvents,
            'legacy_event_count'=>(int)$row['legacy_event_count'],'candidate_event_count'=>(int)$row['candidate_event_count'],
            'legacy_events_sha256'=>(string)$row['legacy_events_sha256'],'candidate_events_sha256'=>(string)$row['candidate_events_sha256'],
            'legacy_crosswalk'=>$legacyCrosswalk,
            'discrepancy_count'=>$total,'reviewed_discrepancy_count'=>$total-$pending,'unreviewed_discrepancy_count'=>$pending,
            'discrepancies_reviewed'=>$pending === 0,'review_status_counts'=>$reviewCounts,
            'updated_at'=>v1_release_iso_time($row['updated_at']));
    }

    $release = v1_release_state($pdo,$config); $allShas = array_values(array_unique(array_merge(array_keys($runShas),array_keys($availabilityShas),
        array_keys($vitalShas),array_keys($distributionShas),array_keys($qualityShas),array_keys($shadowShas))));
    sort($allShas,SORT_STRING); $requestedRevision = isset($_GET['code_revision']) ? strtolower(trim((string)$_GET['code_revision'])) : '';
    if ($requestedRevision !== '' && preg_match('/^[a-f0-9]{40}$/',$requestedRevision) !== 1) {
        v1_respond(400,array('ok'=>false,'error'=>'invalid_code_revision'));
    }
    // Large human-labelled documents are returned only when the caller selects
    // an exact full revision, keeping the default operational response below
    // the 250 KiB API budget.
    $humanRevision = $requestedRevision;
    $humanEvidence = null;
    if ($humanRevision !== '') {
        $humanRow = v1_load_human_evidence_bundle($pdo,$config,$humanRevision);
        if ($humanRow) {
            // The ops export carries immutable hashes and release status only.
            // Full labelled documents stay on the authenticated admin endpoint,
            // keeping a 31-day evidence response inside the API size budget.
            try { $humanEvidence = v1_human_evidence_row_response($humanRow,false); }
            catch (Throwable $e) { v1_respond(503,array('ok'=>false,'error'=>'human_release_evidence_integrity_error')); }
        }
    }
    $collectionRuns = array_values($runGroups); usort($collectionRuns,function($a,$b) {
        return strcmp($a['observation_date'].'|'.$a['source_key'].'|'.(string)$a['code_revision'],$b['observation_date'].'|'.$b['source_key'].'|'.(string)$b['code_revision']);
    });
    $operationsDays = array_values($operations); usort($operationsDays,function($a,$b) {
        return strcmp($a['observation_date'].'|'.$a['code_revision'],$b['observation_date'].'|'.$b['code_revision']);
    });
    $scheduleFrom = $toDate->modify('-6 days')->format('Y-m-d');
    if ($scheduleFrom < $from) { $scheduleFrom = $from; }
    $scheduleRows = v1_official_run_ledger_rows($pdo,$config,$scheduleFrom,$to);
    $officialSchedule = v1_official_schedule_summary($scheduleRows,$scheduleFrom,$to);
    v1_respond(200,array('ok'=>true,'evidence_source'=>'production_db_export','is_synthetic'=>false,'distribution_mode'=>'web_only',
        'timezone'=>'Asia/Seoul','range'=>array('from'=>$from,'to'=>$to),'generated_at'=>gmdate('c'),'schema_version'=>GOV_V1_SCHEMA_VERSION,
        'release_state'=>$release ? (string)$release['release_state'] : null,'code_revisions'=>$allShas,
        'collection_runs'=>$collectionRuns,'official_schedule'=>$officialSchedule,
        'operations_days'=>$operationsDays,'quality_observations'=>$qualityRows,
        'web_distribution_days'=>$distributionDays,
        'availability'=>array('raw_attempt_count'=>count($availability),'raw_success_count'=>$successCount,
            'raw_failure_count'=>count($availability)-$successCount,'success_rate_denominator'=>count($availability),
            'success_rate'=>count($availability)>0 ? $successCount/count($availability) : null,
            'duration_ms_p95'=>v1_percentile($durations,0.95),'observation_interval_seconds_p95'=>v1_percentile($intervals,0.95),
            'daily_route_build_counts'=>$availabilitySummary),
        'web_vitals'=>array('raw_sample_count'=>count($vitals),'groups'=>$vitalSummary),
        'shadow_discrepancies'=>$shadowRows,'shadow_discrepancy_review_status_counts'=>$shadowOverall,'shadow_days'=>$shadowDays,
        'human_release_evidence'=>$humanEvidence,
        'human_release_evidence_status'=>$humanEvidence !== null ? 'available' : ($humanRevision === '' ? 'code_revision_required' : 'missing')));
}

function v1_release_request_id(): ?string {
    $value = isset($_SERVER['HTTP_X_REQUEST_ID']) ? trim((string)$_SERVER['HTTP_X_REQUEST_ID']) : '';
    return preg_match('/^[A-Za-z0-9_.:\-]{1,96}$/', $value) === 1 ? $value : null;
}

function v1_admin_update_release_state(PDO $pdo, array $config, string $role): void {
    $payload = v1_admin_json_body($config);
    $target = isset($payload['release_state']) ? trim((string)$payload['release_state']) : '';
    $reason = isset($payload['reason']) ? trim((string)$payload['reason']) : '';
    if (!in_array($target, array('closed', 'preview', 'live'), true)) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_release_state'));
    }
    if (!array_key_exists('expected_version', $payload) || !is_int($payload['expected_version']) || $payload['expected_version'] < 0) {
        v1_respond(400, array('ok' => false, 'error' => 'expected_version_required'));
    }
    if (mb_strlen($reason, 'UTF-8') < 8 || mb_strlen($reason, 'UTF-8') > 2000) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_release_reason'));
    }
    if ($target === 'preview' && !v1_preview_auth_configured($config)) {
        v1_respond(503, array('ok' => false, 'error' => 'preview_auth_not_configured'));
    }

    $pdo->beginTransaction();
    try {
        $before = v1_release_state($pdo, $config, true);
        if ($before === null) {
            $pdo->rollBack();
            v1_respond(503, array('ok' => false, 'error' => 'release_state_unavailable'));
        }
        $current = (string)$before['release_state'];
        $currentVersion = (int)$before['state_version'];
        if ($currentVersion !== (int)$payload['expected_version']) {
            $pdo->rollBack();
            v1_respond(409, array(
                'ok' => false,
                'error' => 'stale_release_state',
                'current_state' => $current,
                'current_version' => $currentVersion,
            ));
        }
        if ($target === $current) {
            $pdo->commit();
            v1_respond(200, array(
                'ok' => true,
                'changed' => false,
                'release_state' => $current,
                'state_version' => $currentVersion,
                'cutover_at' => v1_release_iso_time(isset($before['cutover_at']) ? $before['cutover_at'] : null),
                'sunset_at' => v1_release_iso_time(isset($before['sunset_at']) ? $before['sunset_at'] : null),
            ));
        }
        $allowedTransitions = array(
            'closed' => array('preview'),
            'preview' => array('closed', 'live'),
            'live' => array('closed'),
        );
        if (!isset($allowedTransitions[$current]) || !in_array($target, $allowedTransitions[$current], true)) {
            $pdo->rollBack();
            v1_respond(409, array(
                'ok' => false,
                'error' => 'invalid_release_transition',
                'current_state' => $current,
                'requested_state' => $target,
            ));
        }
        if ($current === 'preview' && $target === 'live') {
            $pdo->rollBack();
            v1_respond(409,array(
                'ok'=>false,
                'error'=>'protected_atomic_cutover_required',
            ));
        }
        $nextVersion = $currentVersion + 1;
        $changedBy = 'api_role:' . $role;
        $cutoverAt = isset($before['cutover_at']) && $before['cutover_at'] !== null ? (string)$before['cutover_at'] : null;
        $sunsetAt = isset($before['sunset_at']) && $before['sunset_at'] !== null ? (string)$before['sunset_at'] : null;
        // Every protected preview-to-live promotion starts a fresh 90-day
        // compatibility epoch; prior epochs remain immutable in the audit.
        if ($current === 'preview' && $target === 'live') {
            $cutoverAt = gmdate('Y-m-d H:i:s');
            $sunsetAt = gmdate('Y-m-d H:i:s',time() + 90 * 86400);
        }
        $update = $pdo->prepare('UPDATE ' . table_name($config, 'governance_release_state')
            . ' SET release_state = ?, state_version = ?, updated_by = ?, update_reason = ?, cutover_at = ?, sunset_at = ?, updated_at = UTC_TIMESTAMP()'
            . ' WHERE state_key = ? AND state_version = ?');
        $update->execute(array($target, $nextVersion, $changedBy, $reason, $cutoverAt, $sunsetAt, GOV_V1_RELEASE_STATE_KEY, $currentVersion));
        if ($update->rowCount() !== 1) {
            $pdo->rollBack();
            v1_respond(409, array('ok' => false, 'error' => 'stale_release_state'));
        }
        $audit = $pdo->prepare('INSERT INTO ' . table_name($config, 'governance_release_audit')
            . ' (audit_id, state_key, state_version, previous_state, new_state, changed_by, change_reason, request_id, cutover_at, sunset_at, created_at)'
            . ' VALUES (?,?,?,?,?,?,?,?,?,?,UTC_TIMESTAMP())');
        $audit->execute(array(
            'release:' . bin2hex(random_bytes(16)), GOV_V1_RELEASE_STATE_KEY, $nextVersion,
            $current, $target, $changedBy, $reason, v1_release_request_id(), $cutoverAt, $sunsetAt,
        ));
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    v1_respond(200, array(
        'ok' => true,
        'changed' => true,
        'previous_state' => $current,
        'release_state' => $target,
        'state_version' => $nextVersion,
        'cutover_at' => v1_release_iso_time($cutoverAt),
        'sunset_at' => v1_release_iso_time($sunsetAt),
    ));
}

function v1_admin_upsert_source_right(PDO $pdo, array $config, string $role): void {
    $payload = v1_admin_json_body($config);
    $id = isset($payload['source_right_id']) ? trim((string)$payload['source_right_id']) : '';
    if ($id === '') { $id = 'sr_' . bin2hex(random_bytes(16)); }
    if (!v1_valid_entity_id($id, 64)) { v1_respond(400, array('ok' => false, 'error' => 'invalid_source_right_id')); }
    $sourceType = isset($payload['source_type']) ? trim((string)$payload['source_type']) : '';
    $sourceKey = isset($payload['source_key']) ? trim((string)$payload['source_key']) : '';
    $sourceName = isset($payload['source_name']) ? trim((string)$payload['source_name']) : '';
    $scope = isset($payload['permission_scope']) ? trim((string)$payload['permission_scope']) : '';
    if (!preg_match('/^[A-Za-z0-9_.:\-]{1,40}$/', $sourceType) || $sourceKey === '' || $sourceName === '' || $scope === '') {
        v1_respond(400, array('ok' => false, 'error' => 'source_right_required_fields_missing'));
    }
    $sourceKey = mb_substr($sourceKey, 0, 191, 'UTF-8');
    $validFrom = mysql_dt(isset($payload['valid_from']) ? $payload['valid_from'] : null);
    $validUntil = mysql_dt(isset($payload['valid_until']) ? $payload['valid_until'] : null);
    if ($validFrom === null) { v1_respond(400, array('ok' => false, 'error' => 'invalid_valid_from')); }
    if ($validUntil !== null && $validUntil <= $validFrom) { v1_respond(400, array('ok' => false, 'error' => 'invalid_valid_until')); }
    $status = isset($payload['status']) ? (string)$payload['status'] : 'pending';
    if (!in_array($status, array('pending', 'active', 'expired', 'revoked'), true)) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_source_right_status'));
    }
    $revokedAt = $status === 'revoked' ? (mysql_dt(isset($payload['revoked_at']) ? $payload['revoked_at'] : null) ?: gmdate('Y-m-d H:i:s')) : null;
    $evidenceUri = isset($payload['evidence_uri']) ? trim((string)$payload['evidence_uri']) : '';
    $evidenceHash = isset($payload['evidence_hash']) ? strtolower(trim((string)$payload['evidence_hash'])) : null;
    if ($evidenceHash !== null && $evidenceHash !== '' && !preg_match('/^[a-f0-9]{64}$/', $evidenceHash)) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_evidence_hash'));
    }
    if ($status === 'active' && $evidenceUri === '' && ($evidenceHash === null || $evidenceHash === '')) {
        v1_respond(400, array('ok' => false, 'error' => 'active_source_right_requires_evidence'));
    }
    $hasExpectedState = array_key_exists('expected_status', $payload);
    $expectedStatus = $hasExpectedState
        ? strtolower(trim((string)$payload['expected_status'])) : null;
    if (
        $hasExpectedState
        && !in_array(
            $expectedStatus,
            array('missing', 'pending', 'active', 'expired', 'revoked'),
            true
        )
    ) {
        v1_respond(400, array(
            'ok' => false,
            'error' => 'invalid_expected_source_right_status',
        ));
    }
    $expectedUpdatedAtRaw = isset($payload['expected_updated_at'])
        ? trim((string)$payload['expected_updated_at']) : '';
    $expectedUpdatedAt = $expectedUpdatedAtRaw !== ''
        ? mysql_dt($expectedUpdatedAtRaw) : null;
    if (
        ($hasExpectedState && $expectedStatus !== 'missing'
            && $expectedUpdatedAt === null)
        || ($hasExpectedState && $expectedStatus === 'missing'
            && $expectedUpdatedAtRaw !== '')
        || (!$hasExpectedState && $expectedUpdatedAtRaw !== '')
    ) {
        v1_respond(400, array(
            'ok' => false,
            'error' => 'invalid_expected_source_right_version',
        ));
    }
    $now = gmdate('Y-m-d H:i:s');
    $pdo->beginTransaction();
    try {
        // Global order is release-state row -> SourceRight rows. The cutover
        // guard uses the same order, so a concurrent revocation cannot race the
        // preview-to-live read or create a lock cycle.
        if (v1_release_state($pdo,$config,true) === null) {
            throw new RuntimeException('release_state_unavailable');
        }
        $identity = $pdo->prepare(
            'SELECT source_type,source_key,status,updated_at,revoked_at FROM '
            . table_name($config, 'source_rights')
            . ' WHERE source_right_id=? LIMIT 1 FOR UPDATE'
        );
        $identity->execute(array($id));
        $existingIdentity = $identity->fetch();
        if (
            $existingIdentity
            && (
                (string)$existingIdentity['source_type'] !== $sourceType
                || (string)$existingIdentity['source_key'] !== $sourceKey
            )
        ) {
            $pdo->rollBack();
            v1_respond(409, array(
                'ok' => false,
                'error' => 'source_right_identity_immutable',
                'source_right_id' => $id,
                'existing_source_type' => (string)$existingIdentity['source_type'],
                'existing_source_key' => (string)$existingIdentity['source_key'],
            ));
        }
        if ($hasExpectedState) {
            $stale = (
                ($expectedStatus === 'missing' && $existingIdentity)
                || ($expectedStatus !== 'missing' && !$existingIdentity)
                || (
                    $existingIdentity
                    && $expectedStatus !== 'missing'
                    && (
                        !hash_equals(
                            (string)$expectedStatus,
                            (string)$existingIdentity['status']
                        )
                        || !hash_equals(
                            (string)$expectedUpdatedAt,
                            (string)$existingIdentity['updated_at']
                        )
                    )
                )
            );
            if ($stale) {
                $pdo->rollBack();
                v1_respond(409, array(
                    'ok' => false,
                    'error' => 'stale_source_right',
                    'source_right_id' => $id,
                ));
            }
        }
        $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'source_rights') . ' (source_right_id, source_type, source_key, source_name, '
            . 'permission_scope, evidence_uri, evidence_hash, valid_from, valid_until, revoked_at, ai_allowed, redistribution_allowed, status, notes, created_at, updated_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            . (
                $hasExpectedState && $expectedStatus === 'missing'
                ? ''
                : ' ON DUPLICATE KEY UPDATE '
                    . 'source_name=VALUES(source_name), permission_scope=VALUES(permission_scope), evidence_uri=VALUES(evidence_uri), evidence_hash=VALUES(evidence_hash), '
                    . 'valid_from=VALUES(valid_from), valid_until=VALUES(valid_until), revoked_at=VALUES(revoked_at), ai_allowed=VALUES(ai_allowed), '
                    . 'redistribution_allowed=VALUES(redistribution_allowed), status=VALUES(status), notes=VALUES(notes), updated_at=VALUES(updated_at)'
            ));
        $stmt->execute(array(
            $id, $sourceType, $sourceKey, mb_substr($sourceName, 0, 255, 'UTF-8'), $scope,
            $evidenceUri !== '' ? mb_substr($evidenceUri, 0, 65535, 'UTF-8') : null,
            $evidenceHash ?: null, $validFrom, $validUntil, $revokedAt, v1_bool_int(isset($payload['ai_allowed']) ? $payload['ai_allowed'] : false),
            v1_bool_int(isset($payload['redistribution_allowed']) ? $payload['redistribution_allowed'] : false), $status,
            isset($payload['notes']) ? mb_substr((string)$payload['notes'], 0, 65535, 'UTF-8') : 'updated_by:' . $role,
            $now, $now,
        ));
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        if (
            $hasExpectedState
            && $expectedStatus === 'missing'
            && (string)$e->getCode() === '23000'
        ) {
            v1_respond(409, array(
                'ok' => false,
                'error' => 'stale_source_right',
                'source_right_id' => $id,
            ));
        }
        throw $e;
    }
    v1_respond(200, array(
        'ok' => true,
        'source_right_id' => $id,
        'status' => $status,
        'updated_at' => v1_release_iso_time($now),
    ));
}

function v1_admin_create_revision(PDO $pdo, array $config, string $role): void {
    $payload = v1_admin_json_body($config);
    $entityType = isset($payload['entity_type']) ? trim((string)$payload['entity_type']) : '';
    $entityId = isset($payload['entity_id']) ? trim((string)$payload['entity_id']) : '';
    $reason = isset($payload['reason']) ? trim((string)$payload['reason']) : '';
    if (!in_array($entityType, array('company', 'event', 'campaign', 'document', 'claim', 'commitment'), true)
        || !v1_valid_entity_id($entityId) || mb_strlen($reason, 'UTF-8') < 5) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_revision_request'));
    }
    $id = 'rev_' . bin2hex(random_bytes(16));
    $now = gmdate('Y-m-d H:i:s');
    $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'editorial_revisions') . ' (revision_id, entity_type, entity_id, field_name, '
        . 'previous_value, revised_value, reason, revision_status, requested_by, created_at, updated_at) VALUES (?,?,?,?,?,?,?,\'pending\',?,?,?)');
    $stmt->execute(array(
        $id, $entityType, $entityId,
        isset($payload['field_name']) ? mb_substr((string)$payload['field_name'], 0, 80, 'UTF-8') : null,
        isset($payload['previous_value']) ? mb_substr((string)$payload['previous_value'], 0, 1048576, 'UTF-8') : null,
        isset($payload['revised_value']) ? mb_substr((string)$payload['revised_value'], 0, 1048576, 'UTF-8') : null,
        $reason, 'api_role:' . $role, $now, $now,
    ));
    v1_respond(201, array('ok' => true, 'revision_id' => $id, 'status' => 'pending'));
}

function v1_review_expected_updated_at(array $payload): string {
    $value = isset($payload['expected_updated_at']) && is_string($payload['expected_updated_at']) ? trim($payload['expected_updated_at']) : '';
    if (preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/', $value) === 1) { return $value; }
    $normalized = v1_editorial_datetime_utc($value);
    if ($normalized === null) { v1_respond(400, array('ok' => false, 'error' => 'expected_updated_at_required')); }
    return $normalized;
}

/** Complete an incomplete event identity without changing its stable event_id. */
function v1_admin_complete_event_identity(PDO $pdo, array $config, string $eventId, string $role): void {
    $payload = v1_admin_json_body($config);
    v1_assert_object_keys($payload,array('identity_action','identity_target','identity_actor_id','identity_effective_at',
        'identity_deadline_at','expected_updated_at','reason'),'event_identity');
    $reason = isset($payload['reason']) ? trim((string)$payload['reason']) : '';
    $expectedUpdatedAt = v1_review_expected_updated_at($payload);
    if (mb_strlen($reason,'UTF-8') < 5) {
        v1_respond(400,array('ok'=>false,'error'=>'event_identity_reason_required'));
    }
    $pdo->beginTransaction();
    try {
        $select = $pdo->prepare('SELECT event_id,company_id,event_type,identity_action,identity_target,identity_actor_id,'
            . 'identity_effective_at,identity_deadline_at,identity_status,comparison_key,review_status,publication_status,updated_at FROM '
            . table_name($config,'governance_events') . ' WHERE event_id=? FOR UPDATE');
        $select->execute(array($eventId)); $before = $select->fetch();
        if (!$before) { $pdo->rollBack(); v1_respond(404,array('ok'=>false,'error'=>'event_not_found')); }
        if (!hash_equals((string)$before['updated_at'],$expectedUpdatedAt)) {
            $pdo->rollBack(); v1_respond(409,array('ok'=>false,'error'=>'stale_event_identity'));
        }
        $identity = v1_build_event_identity((string)$before['company_id'],(string)$before['event_type'],
            $payload['identity_action'] ?? null,$payload['identity_target'] ?? null,$payload['identity_actor_id'] ?? null,
            $payload['identity_effective_at'] ?? null,$payload['identity_deadline_at'] ?? null,false);
        if ($identity === null) {
            $pdo->rollBack(); v1_respond(400,array('ok'=>false,'error'=>'invalid_complete_event_identity'));
        }
        $actor = $pdo->prepare('SELECT record_status FROM ' . table_name($config,'actors') . ' WHERE actor_id=?');
        $actor->execute(array($identity['identity_actor_id']));
        if ((string)($actor->fetchColumn() ?: '') !== 'active') {
            $pdo->rollBack(); v1_respond(409,array('ok'=>false,'error'=>'active_identity_actor_required'));
        }
        if ((string)$before['identity_status'] === 'complete') {
            if (hash_equals((string)$before['comparison_key'],(string)$identity['comparison_key'])) {
                $pdo->commit();
                v1_respond(200,array('ok'=>true,'event_id'=>$eventId,'identity_status'=>'complete',
                    'comparison_key'=>(string)$identity['comparison_key'],'unchanged'=>true,'updated_at'=>v1_release_iso_time($before['updated_at'])));
            }
            $pdo->rollBack(); v1_respond(409,array('ok'=>false,'error'=>'complete_event_identity_is_immutable'));
        }
        if ((string)$before['publication_status'] === 'published' || (string)$before['review_status'] === 'approved') {
            $pdo->rollBack(); v1_respond(409,array('ok'=>false,'error'=>'published_event_identity_is_immutable'));
        }
        $previousIdentity = array(
            'identity_action'=>$before['identity_action'],'identity_target'=>$before['identity_target'],
            'identity_actor_id'=>$before['identity_actor_id'],'identity_effective_at'=>v1_release_iso_time($before['identity_effective_at']),
            'identity_deadline_at'=>v1_release_iso_time($before['identity_deadline_at']),
            'identity_status'=>(string)$before['identity_status'],'comparison_key'=>$before['comparison_key'],
        );
        $update = $pdo->prepare('UPDATE ' . table_name($config,'governance_events')
            . ' SET identity_action=?,identity_target=?,identity_actor_id=?,identity_effective_at=?,identity_deadline_at=?,'
            . 'identity_status=\'complete\',comparison_key=?,review_status=\'pending\',publication_status=\'draft\','
            . 'updated_at=GREATEST(UTC_TIMESTAMP(),DATE_ADD(updated_at,INTERVAL 1 SECOND)) WHERE event_id=?');
        $update->execute(array($identity['identity_action'],$identity['identity_target'],$identity['identity_actor_id'],
            $identity['identity_effective_at'],$identity['identity_deadline_at'],$identity['comparison_key'],$eventId));
        $updated = $pdo->prepare('SELECT updated_at FROM ' . table_name($config,'governance_events') . ' WHERE event_id=?');
        $updated->execute(array($eventId)); $updatedAt = (string)$updated->fetchColumn();
        $revisionId = 'rev_' . bin2hex(random_bytes(16));
        $revision = $pdo->prepare('INSERT INTO ' . table_name($config,'editorial_revisions')
            . ' (revision_id,entity_type,entity_id,field_name,previous_value,revised_value,reason,revision_status,requested_by,'
            . 'reviewed_by,reviewed_at,published_at,created_at,updated_at) VALUES (?,\'event\',?,\'event_identity\',?,?,?,\'published\','
            . '\'identity_completion_api\',?,UTC_TIMESTAMP(),UTC_TIMESTAMP(),UTC_TIMESTAMP(),UTC_TIMESTAMP())');
        $revision->execute(array($revisionId,$eventId,
            v1_canonical_json_encode($previousIdentity,'event_previous_identity_encode_failed'),
            v1_canonical_json_encode($identity,'event_identity_encode_failed'),$reason,'api_role:' . $role));
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        if ($e instanceof PDOException && (string)$e->getCode() === '23000') {
            v1_respond(409,array('ok'=>false,'error'=>'event_comparison_key_conflict','review_required'=>true));
        }
        throw $e;
    }
    v1_respond(200,array('ok'=>true,'event_id'=>$eventId,'identity_status'=>'complete',
        'comparison_key'=>(string)$identity['comparison_key'],'unchanged'=>false,'updated_at'=>v1_release_iso_time($updatedAt)));
}

function v1_admin_review_event(PDO $pdo, array $config, string $eventId, string $role): void {
    $payload = v1_admin_json_body($config);
    $decision = isset($payload['decision']) ? (string)$payload['decision'] : '';
    $reason = isset($payload['reason']) ? trim((string)$payload['reason']) : '';
    if (!in_array($decision, array('approve', 'reject', 'changes_requested'), true) || mb_strlen($reason, 'UTF-8') < 5) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_review_decision'));
    }
    $expectedUpdatedAt = v1_review_expected_updated_at($payload);
    $reviewStatus = $decision === 'approve' ? 'approved' : ($decision === 'reject' ? 'rejected' : 'changes_requested');
    $publicationStatus = $decision === 'approve' ? 'published' : 'draft';
    $pdo->beginTransaction();
    try {
        $select = $pdo->prepare('SELECT review_status, publication_status, verification_status, identity_actor_id, identity_status, comparison_key, updated_at FROM ' . table_name($config, 'governance_events') . ' WHERE event_id = ? FOR UPDATE');
        $select->execute(array($eventId)); $before = $select->fetch();
        if (!$before) { $pdo->rollBack(); v1_respond(404, array('ok' => false, 'error' => 'event_not_found')); }
        if (!hash_equals((string)$before['updated_at'], $expectedUpdatedAt)) {
            $pdo->rollBack(); v1_respond(409, array('ok' => false, 'error' => 'stale_review'));
        }
        if ($decision === 'approve' && !in_array((string)$before['verification_status'], array('official', 'confirmed', 'corroborated', 'corrected', 'withdrawn'), true)) {
            $pdo->rollBack();
            v1_respond(409, array('ok' => false, 'error' => 'verified_evidence_required_before_publication'));
        }
        if ($decision === 'approve' && ((string)$before['identity_status'] !== 'complete'
            || preg_match('/^eventcmp:v1:[a-f0-9]{64}$/', (string)$before['comparison_key']) !== 1)) {
            $pdo->rollBack();
            v1_respond(409, array('ok' => false, 'error' => 'complete_identity_required_before_publication'));
        }
        if ($decision === 'approve' && trim((string)$before['identity_actor_id']) !== '') {
            $actorRelation = $pdo->prepare('SELECT COUNT(*) FROM ' . table_name($config, 'event_actors') . ' review_identity_ea'
                . ' JOIN ' . table_name($config, 'actors') . ' review_identity_a ON review_identity_a.actor_id = review_identity_ea.actor_id'
                . ' WHERE review_identity_ea.event_id = ? AND review_identity_ea.actor_id = ?'
                . ' AND review_identity_ea.review_status = \'approved\''
                . ' AND review_identity_a.review_status = \'approved\' AND review_identity_a.record_status = \'active\''
                . ' AND NULLIF(TRIM(review_identity_a.display_name), \'\') IS NOT NULL');
            $actorRelation->execute(array($eventId, (string)$before['identity_actor_id']));
            if ((int)$actorRelation->fetchColumn() < 1) {
                $pdo->rollBack();
                v1_respond(409, array('ok' => false, 'error' => 'approved_event_actor_required_before_publication'));
            }
        }
        if ($decision === 'approve') {
            $evidence = $pdo->prepare('SELECT COUNT(*) FROM ' . table_name($config, 'event_documents') . ' review_ed'
                . ' JOIN ' . table_name($config, 'documents') . ' review_d ON review_d.document_id = review_ed.document_id'
                . ' LEFT JOIN ' . table_name($config, 'source_rights') . ' review_sr ON review_sr.source_right_id = review_d.source_right_id'
                . ' WHERE review_ed.event_id = ? AND ' . v1_document_visibility_sql('review_d', 'review_sr'));
            $evidence->execute(array($eventId));
            if ((int)$evidence->fetchColumn() < 1) {
                $pdo->rollBack();
                v1_respond(409, array('ok' => false, 'error' => 'publishable_evidence_required_before_publication'));
            }
        }
        $update = $pdo->prepare('UPDATE ' . table_name($config, 'governance_events') . ' SET review_status = ?, publication_status = ?, '
            . 'updated_at = GREATEST(UTC_TIMESTAMP(), DATE_ADD(updated_at, INTERVAL 1 SECOND)) WHERE event_id = ?');
        $update->execute(array($reviewStatus, $publicationStatus, $eventId));
        $revisionId = 'rev_' . bin2hex(random_bytes(16));
        $revision = $pdo->prepare('INSERT INTO ' . table_name($config, 'editorial_revisions') . ' (revision_id, entity_type, entity_id, field_name, '
            . 'previous_value, revised_value, reason, revision_status, requested_by, reviewed_by, reviewed_at, published_at, created_at, updated_at) '
            . 'VALUES (?,\'event\',?,\'review_status\',?,?,?,\'published\',?,?,UTC_TIMESTAMP(),UTC_TIMESTAMP(),UTC_TIMESTAMP(),UTC_TIMESTAMP())');
        $revision->execute(array($revisionId, $eventId, (string)$before['review_status'], $reviewStatus, $reason, 'review_api', 'api_role:' . $role));
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    v1_respond(200, array('ok' => true, 'event_id' => $eventId, 'review_status' => $reviewStatus, 'publication_status' => $publicationStatus));
}

function v1_admin_review_event_actor(PDO $pdo, array $config, string $eventId, string $actorId, string $actorRole, string $role): void {
    $payload = v1_admin_json_body($config);
    $decision = isset($payload['decision']) ? trim((string)$payload['decision']) : '';
    $reason = isset($payload['reason']) ? trim((string)$payload['reason']) : '';
    if (!in_array($decision, array('approve', 'reject', 'changes_requested'), true) || mb_strlen($reason, 'UTF-8') < 5) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_review_decision'));
    }
    $expectedUpdatedAt = v1_review_expected_updated_at($payload);
    $reviewStatus = $decision === 'approve' ? 'approved' : ($decision === 'reject' ? 'rejected' : 'changes_requested');
    $pdo->beginTransaction();
    try {
        $select = $pdo->prepare('SELECT review_status, updated_at FROM ' . table_name($config, 'event_actors')
            . ' WHERE event_id = ? AND actor_id = ? AND actor_role = ? FOR UPDATE');
        $select->execute(array($eventId, $actorId, $actorRole)); $before = $select->fetch();
        if (!$before) { $pdo->rollBack(); v1_respond(404, array('ok' => false, 'error' => 'event_actor_not_found')); }
        if (!hash_equals((string)$before['updated_at'], $expectedUpdatedAt)) {
            $pdo->rollBack(); v1_respond(409, array('ok' => false, 'error' => 'stale_review'));
        }
        if ($decision === 'approve') {
            $actor = $pdo->prepare('SELECT COUNT(*) FROM ' . table_name($config, 'actors')
                . ' WHERE actor_id = ? AND review_status = \'approved\' AND record_status = \'active\'');
            $actor->execute(array($actorId));
            if ((int)$actor->fetchColumn() !== 1) {
                $pdo->rollBack(); v1_respond(409, array('ok' => false, 'error' => 'approved_actor_required_before_publication'));
            }
            $evidence = $pdo->prepare('SELECT COUNT(*) FROM ' . table_name($config, 'event_documents') . ' relation_ed'
                . ' JOIN ' . table_name($config, 'documents') . ' relation_d ON relation_d.document_id = relation_ed.document_id'
                . ' LEFT JOIN ' . table_name($config, 'source_rights') . ' relation_sr ON relation_sr.source_right_id = relation_d.source_right_id'
                . ' WHERE relation_ed.event_id = ? AND ' . v1_document_visibility_sql('relation_d', 'relation_sr'));
            $evidence->execute(array($eventId));
            if ((int)$evidence->fetchColumn() < 1) {
                $pdo->rollBack(); v1_respond(409, array('ok' => false, 'error' => 'publishable_evidence_required_before_publication'));
            }
        }
        $update = $pdo->prepare('UPDATE ' . table_name($config, 'event_actors') . ' SET review_status = ?, '
            . 'updated_at = GREATEST(UTC_TIMESTAMP(), DATE_ADD(updated_at, INTERVAL 1 SECOND))'
            . ' WHERE event_id = ? AND actor_id = ? AND actor_role = ?');
        $update->execute(array($reviewStatus, $eventId, $actorId, $actorRole));
        $revisionId = 'rev_' . bin2hex(random_bytes(16));
        $relationId = v1_stable_id('event-actor', $eventId . ':' . $actorId . ':' . $actorRole);
        $revision = $pdo->prepare('INSERT INTO ' . table_name($config, 'editorial_revisions') . ' (revision_id, entity_type, entity_id, field_name, '
            . 'previous_value, revised_value, reason, revision_status, requested_by, reviewed_by, reviewed_at, published_at, created_at, updated_at) '
            . 'VALUES (?,\'event_actor\',?,\'review_status\',?,?,?,\'published\',\'review_api\',?,UTC_TIMESTAMP(),UTC_TIMESTAMP(),UTC_TIMESTAMP(),UTC_TIMESTAMP())');
        $revision->execute(array($revisionId, $relationId, (string)$before['review_status'], $reviewStatus, $reason, 'api_role:' . $role));
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    v1_respond(200, array('ok' => true, 'event_id' => $eventId, 'actor_id' => $actorId, 'actor_role' => $actorRole, 'review_status' => $reviewStatus));
}

function v1_editorial_publishable_evidence_count(PDO $pdo, array $config, string $entityType, string $entityId): int {
    $documents = table_name($config, 'documents');
    $rights = table_name($config, 'source_rights');
    $visibility = v1_document_visibility_sql('review_d', 'review_sr');
    if ($entityType === 'campaign') {
        $sql = 'SELECT COUNT(DISTINCT review_d.document_id) FROM ' . table_name($config, 'campaign_documents') . ' review_cd'
            . ' JOIN ' . $documents . ' review_d ON review_d.document_id = review_cd.document_id'
            . ' LEFT JOIN ' . $rights . ' review_sr ON review_sr.source_right_id = review_d.source_right_id'
            . ' WHERE review_cd.campaign_id = ? AND ' . $visibility;
        $stmt = $pdo->prepare($sql); $stmt->execute(array($entityId));
        return (int)$stmt->fetchColumn();
    }
    if ($entityType === 'claim') {
        $sql = 'SELECT COUNT(*) FROM ' . table_name($config, 'claim_evidence') . ' review_ce'
            . ' JOIN ' . $documents . ' review_d ON review_d.document_id = review_ce.document_id'
            . ' LEFT JOIN ' . $rights . ' review_sr ON review_sr.source_right_id = review_d.source_right_id'
            . ' WHERE review_ce.claim_id = ? AND ' . $visibility;
        $stmt = $pdo->prepare($sql); $stmt->execute(array($entityId));
        return (int)$stmt->fetchColumn();
    }
    $documentField = array(
        'proposal_vote' => array('proposal_votes', 'proposal_vote_id', 'evidence_document_id'),
        'commitment' => array('commitment_outcomes', 'commitment_id', 'evidence_document_id'),
        'timeline' => array('timeline_entries', 'timeline_entry_id', 'document_id'),
    );
    if (isset($documentField[$entityType])) {
        list($table, $primary, $field) = $documentField[$entityType];
        $sql = 'SELECT COUNT(*) FROM ' . table_name($config, $table) . ' review_entity'
            . ' JOIN ' . $documents . ' review_d ON review_d.document_id = review_entity.' . $field
            . ' LEFT JOIN ' . $rights . ' review_sr ON review_sr.source_right_id = review_d.source_right_id'
            . ' WHERE review_entity.' . $primary . ' = ? AND ' . $visibility;
        $stmt = $pdo->prepare($sql); $stmt->execute(array($entityId));
        return (int)$stmt->fetchColumn();
    }
    if ($entityType !== 'actor') { return 0; }
    $queries = array(
        'SELECT COUNT(DISTINCT review_d.document_id) FROM ' . table_name($config, 'event_actors') . ' review_ea'
            . ' JOIN ' . table_name($config, 'event_documents') . ' review_ed ON review_ed.event_id = review_ea.event_id'
            . ' JOIN ' . $documents . ' review_d ON review_d.document_id = review_ed.document_id'
            . ' LEFT JOIN ' . $rights . ' review_sr ON review_sr.source_right_id = review_d.source_right_id'
            . ' WHERE review_ea.actor_id = ? AND ' . $visibility,
        'SELECT COUNT(DISTINCT review_d.document_id) FROM ' . table_name($config, 'campaigns') . ' review_cp'
            . ' JOIN ' . table_name($config, 'campaign_documents') . ' review_cd ON review_cd.campaign_id = review_cp.campaign_id'
            . ' JOIN ' . $documents . ' review_d ON review_d.document_id = review_cd.document_id'
            . ' LEFT JOIN ' . $rights . ' review_sr ON review_sr.source_right_id = review_d.source_right_id'
            . ' WHERE review_cp.lead_actor_id = ? AND ' . $visibility,
        'SELECT COUNT(DISTINCT review_d.document_id) FROM ' . table_name($config, 'claim_evidence') . ' review_ce'
            . ' JOIN ' . $documents . ' review_d ON review_d.document_id = review_ce.document_id'
            . ' LEFT JOIN ' . $rights . ' review_sr ON review_sr.source_right_id = review_d.source_right_id'
            . ' WHERE review_ce.actor_id = ? AND ' . $visibility,
    );
    $count = 0;
    foreach ($queries as $sql) {
        $stmt = $pdo->prepare($sql); $stmt->execute(array($entityId));
        $count += (int)$stmt->fetchColumn();
    }
    return $count;
}

function v1_admin_review_editorial_entity(PDO $pdo, array $config, string $entityType, string $entityId, string $role): void {
    $payload = v1_admin_json_body($config);
    $decision = isset($payload['decision']) ? trim((string)$payload['decision']) : '';
    $reason = isset($payload['reason']) ? trim((string)$payload['reason']) : '';
    if (!in_array($decision, array('approve', 'reject', 'changes_requested'), true) || mb_strlen($reason, 'UTF-8') < 5) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_review_decision'));
    }
    $expectedUpdatedAt = v1_review_expected_updated_at($payload);
    $definitions = array(
        'actor' => array('actors', 'actor_id', 'review_status', 'record_status'),
        'campaign' => array('campaigns', 'campaign_id', 'review_status', 'publication_status'),
        'claim' => array('claim_evidence', 'claim_id', 'editorial_status', null),
        'proposal_vote' => array('proposal_votes', 'proposal_vote_id', 'review_status', 'publication_status'),
        'commitment' => array('commitment_outcomes', 'commitment_id', 'review_status', 'publication_status'),
        'timeline' => array('timeline_entries', 'timeline_entry_id', 'review_status', 'publication_status'),
    );
    if (!isset($definitions[$entityType]) || !v1_valid_entity_id($entityId)) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_editorial_entity'));
    }
    list($table, $primary, $reviewColumn, $publicationColumn) = $definitions[$entityType];
    $reviewStatus = $decision === 'approve' ? 'approved' : ($decision === 'reject' ? 'rejected' : 'changes_requested');
    $publicationStatus = $decision === 'approve' ? ($entityType === 'actor' ? 'active' : 'published') : ($entityType === 'actor' ? 'inactive' : 'draft');
    $pdo->beginTransaction();
    try {
        $fields = $reviewColumn . ' AS review_status, updated_at' . ($publicationColumn !== null ? ', ' . $publicationColumn . ' AS publication_status' : '');
        $select = $pdo->prepare('SELECT ' . $fields . ' FROM ' . table_name($config, $table) . ' WHERE ' . $primary . ' = ? FOR UPDATE');
        $select->execute(array($entityId)); $before = $select->fetch();
        if (!$before) { $pdo->rollBack(); v1_respond(404, array('ok' => false, 'error' => 'editorial_entity_not_found')); }
        if (!hash_equals((string)$before['updated_at'], $expectedUpdatedAt)) {
            $pdo->rollBack(); v1_respond(409, array('ok' => false, 'error' => 'stale_review'));
        }
        if ($decision === 'approve' && in_array($entityType, array('campaign', 'claim', 'proposal_vote'), true)) {
            $dependency = array(
                'campaign' => array('campaigns', 'campaign_id', 'lead_actor_id'),
                'claim' => array('claim_evidence', 'claim_id', 'actor_id'),
                'proposal_vote' => array('proposal_votes', 'proposal_vote_id', 'proposer_actor_id'),
            );
            list($dependencyTable, $dependencyPrimary, $actorField) = $dependency[$entityType];
            $nullableActor = $entityType === 'campaign' ? '0=1' : 'dependency_entity.' . $actorField . ' IS NULL';
            $actorCheck = $pdo->prepare('SELECT COUNT(*) FROM ' . table_name($config, $dependencyTable) . ' dependency_entity'
                . ' WHERE dependency_entity.' . $dependencyPrimary . ' = ? AND (' . $nullableActor . ' OR EXISTS ('
                . 'SELECT 1 FROM ' . table_name($config, 'actors') . ' dependency_actor WHERE dependency_actor.actor_id = dependency_entity.' . $actorField
                . ' AND dependency_actor.review_status = \'approved\' AND dependency_actor.record_status = \'active\'))');
            $actorCheck->execute(array($entityId));
            if ((int)$actorCheck->fetchColumn() !== 1) {
                $pdo->rollBack();
                v1_respond(409, array('ok' => false, 'error' => 'approved_actor_required_before_publication'));
            }
        }
        if ($decision === 'approve' && v1_editorial_publishable_evidence_count($pdo, $config, $entityType, $entityId) < 1) {
            $pdo->rollBack();
            v1_respond(409, array('ok' => false, 'error' => 'publishable_evidence_required_before_publication'));
        }
        $set = $reviewColumn . ' = ?';
        $params = array($reviewStatus);
        if ($publicationColumn !== null) { $set .= ', ' . $publicationColumn . ' = ?'; $params[] = $publicationStatus; }
        $set .= ', updated_at = GREATEST(UTC_TIMESTAMP(), DATE_ADD(updated_at, INTERVAL 1 SECOND))';
        $params[] = $entityId;
        $update = $pdo->prepare('UPDATE ' . table_name($config, $table) . ' SET ' . $set . ' WHERE ' . $primary . ' = ?');
        $update->execute($params);
        $revisionId = 'rev_' . bin2hex(random_bytes(16));
        $revisedValue = $publicationColumn === null ? $reviewStatus : $reviewStatus . ':' . $publicationStatus;
        $previousValue = (string)$before['review_status'] . ($publicationColumn === null ? '' : ':' . (string)$before['publication_status']);
        $revision = $pdo->prepare('INSERT INTO ' . table_name($config, 'editorial_revisions') . ' (revision_id, entity_type, entity_id, field_name, '
            . 'previous_value, revised_value, reason, revision_status, requested_by, reviewed_by, reviewed_at, published_at, created_at, updated_at) '
            . 'VALUES (?,?,?,?,?,?,?,\'published\',?,?,UTC_TIMESTAMP(),UTC_TIMESTAMP(),UTC_TIMESTAMP(),UTC_TIMESTAMP())');
        $revision->execute(array($revisionId, $entityType, $entityId, 'review_status', $previousValue, $revisedValue, $reason, 'review_api', 'api_role:' . $role));
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    $response = array('ok' => true, 'entity_type' => $entityType, 'entity_id' => $entityId, 'review_status' => $reviewStatus);
    if ($publicationColumn !== null) { $response['publication_status'] = $publicationStatus; }
    v1_respond(200, $response);
}

function v1_admin_review_feedback(PDO $pdo, array $config, string $feedbackId, string $role): void {
    $payload = v1_admin_json_body($config);
    $status = isset($payload['status']) ? trim((string)$payload['status']) : '';
    $note = isset($payload['review_note']) ? trim((string)$payload['review_note']) : '';
    if (!in_array($status, array('reviewing', 'resolved', 'rejected'), true) || mb_strlen($note, 'UTF-8') < 5) {
        v1_respond(400, array('ok' => false, 'error' => 'invalid_feedback_review'));
    }
    $stmt = $pdo->prepare('UPDATE ' . table_name($config, 'feedback') . ' SET status=?, is_public=0, review_note=?, reviewed_by=?, '
        . 'reviewed_at=UTC_TIMESTAMP(), updated_at=UTC_TIMESTAMP() WHERE feedback_id=?');
    $stmt->execute(array($status, mb_substr($note, 0, 65535, 'UTF-8'), 'api_role:' . $role, $feedbackId));
    if ($stmt->rowCount() < 1) { v1_respond(404, array('ok' => false, 'error' => 'feedback_not_found')); }
    v1_respond(200, array('ok' => true, 'feedback_id' => $feedbackId, 'status' => $status, 'is_public' => false));
}

function v1_first(array $row, array $keys, $default = null) {
    foreach ($keys as $key) {
        if (array_key_exists($key, $row) && $row[$key] !== null && $row[$key] !== '') {
            return $row[$key];
        }
    }
    return $default;
}

function v1_language($value, string $fallback = 'und'): string {
    $language = trim((string)$value);
    if (!preg_match('/^[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*$/', $language)) {
        return $fallback;
    }
    return mb_substr($language, 0, 16, 'UTF-8');
}

function v1_stable_id(string $prefix, string $external, int $max = 96): string {
    $candidate = $prefix . ':' . $external;
    if (v1_valid_entity_id($candidate, $max)) { return $candidate; }
    return $prefix . ':' . substr(hash('sha256', $external), 0, min(64, $max - strlen($prefix) - 1));
}

function v1_editorial_invalid(string $message): void {
    throw new InvalidArgumentException($message);
}

function v1_editorial_assert_keys(array $record, array $allowed, string $location): void {
    foreach (array_keys($record) as $key) {
        if (!is_string($key) || !in_array($key, $allowed, true)) {
            v1_editorial_invalid($location . ': unknown field');
        }
    }
}

function v1_editorial_is_list(array $value): bool {
    $expected = 0;
    foreach (array_keys($value) as $key) {
        if ($key !== $expected) { return false; }
        $expected++;
    }
    return true;
}

function v1_editorial_id(array $record, string $field, string $location, int $max = 96, bool $required = true): ?string {
    if (!array_key_exists($field, $record) || $record[$field] === null) {
        if ($required) { v1_editorial_invalid($location . '.' . $field . ': required'); }
        return null;
    }
    if (!is_string($record[$field]) || $record[$field] !== trim($record[$field]) || !v1_valid_entity_id($record[$field], $max)) {
        v1_editorial_invalid($location . '.' . $field . ': invalid id');
    }
    return $record[$field];
}

function v1_editorial_company_id(array $record, string $location): string {
    $companyId = isset($record['company_id']) && is_string($record['company_id']) ? $record['company_id'] : '';
    if (preg_match('/^[0-9]{8}$/', $companyId) !== 1) { v1_editorial_invalid($location . '.company_id: invalid'); }
    return $companyId;
}

function v1_editorial_code(array $record, string $field, string $location, int $max = 40, bool $required = true): ?string {
    if (!array_key_exists($field, $record) || $record[$field] === null) {
        if ($required) { v1_editorial_invalid($location . '.' . $field . ': required'); }
        return null;
    }
    $value = $record[$field];
    if (!is_string($value) || $value !== trim($value) || strlen($value) > $max || preg_match('/^[A-Za-z0-9_.:\-]+$/', $value) !== 1) {
        v1_editorial_invalid($location . '.' . $field . ': invalid code');
    }
    return $value;
}

/** Validate lengths without changing source-language text. */
function v1_editorial_text(array $record, string $field, string $location, int $max, bool $required = true): ?string {
    if (!array_key_exists($field, $record) || $record[$field] === null) {
        if ($required) { v1_editorial_invalid($location . '.' . $field . ': required'); }
        return null;
    }
    $value = $record[$field];
    if (!is_string($value) || trim($value) === '' || mb_strlen($value, 'UTF-8') > $max
        || ($max === 65535 && strlen($value) > 65535)) {
        v1_editorial_invalid($location . '.' . $field . ': invalid source text');
    }
    return $value;
}

function v1_editorial_language_field(array $record, string $location): string {
    $language = isset($record['original_language']) ? v1_editorial_language($record['original_language']) : null;
    if ($language === null) { v1_editorial_invalid($location . '.original_language: invalid'); }
    return $language;
}

function v1_editorial_timestamp(array $record, string $field, string $location, bool $required = true): ?string {
    if (!array_key_exists($field, $record) || $record[$field] === null) {
        if ($required) { v1_editorial_invalid($location . '.' . $field . ': required'); }
        return null;
    }
    $timestamp = v1_editorial_datetime_utc($record[$field]);
    if ($timestamp === null) { v1_editorial_invalid($location . '.' . $field . ': timezone-aware timestamp required'); }
    return $timestamp;
}

function v1_editorial_fail_closed(array $record, string $field, string $requiredValue, string $location): string {
    if (array_key_exists($field, $record) && $record[$field] !== $requiredValue) {
        v1_editorial_invalid($location . '.' . $field . ': must be ' . $requiredValue);
    }
    return $requiredValue;
}

function v1_editorial_parent_ids(array $record, string $location): array {
    $eventId = v1_editorial_id($record, 'event_id', $location, 96, false);
    $campaignId = v1_editorial_id($record, 'campaign_id', $location, 96, false);
    if ($eventId === null && $campaignId === null) { v1_editorial_invalid($location . ': event_id or campaign_id required'); }
    return array($eventId, $campaignId);
}

function v1_editorial_number(array $record, string $field, string $location): ?float {
    if (!array_key_exists($field, $record) || $record[$field] === null) { return null; }
    if (!is_int($record[$field]) && !is_float($record[$field])) { v1_editorial_invalid($location . '.' . $field . ': invalid percentage'); }
    $value = (float)$record[$field];
    if (!is_finite($value) || $value < 0 || $value > 100) { v1_editorial_invalid($location . '.' . $field . ': percentage out of range'); }
    return $value;
}

function v1_editorial_document_ids(array $record, string $field, string $location): array {
    if (!isset($record[$field]) || !is_array($record[$field]) || !v1_editorial_is_list($record[$field]) || count($record[$field]) < 1) {
        v1_editorial_invalid($location . '.' . $field . ': evidence required');
    }
    $ids = array();
    foreach ($record[$field] as $value) {
        if (!is_string($value) || $value !== trim($value) || !v1_valid_entity_id($value)) {
            v1_editorial_invalid($location . '.' . $field . ': invalid document id');
        }
        if (isset($ids[$value])) { v1_editorial_invalid($location . '.' . $field . ': duplicate document id'); }
        $ids[$value] = true;
    }
    return array_keys($ids);
}

function v1_editorial_normalize_record(string $entity, array $record, int $index): array {
    $location = $entity . '[' . $index . ']';
    $allowed = array(
        'actors' => array('actor_id','actor_type','display_name','display_name_en','company_id','country_code','aliases','homepage_url','record_status','review_status'),
        'event_actors' => array('event_id','actor_id','actor_role','review_status'),
        'campaigns' => array('campaign_id','company_id','lead_actor_id','title','original_language','demand_text','stage','outcome','started_at','ended_at','review_status','publication_status','evidence_document_ids'),
        'claim_evidence' => array('claim_id','event_id','campaign_id','actor_id','document_id','claim_type','claim_text','original_language','evidence_locator','editorial_status'),
        'proposal_votes' => array('proposal_vote_id','event_id','campaign_id','company_id','proposer_actor_id','agenda_no','agenda_title','original_language','meeting_at','recommendation','recommendation_source','result','votes_for','votes_against','votes_abstain','evidence_document_id','review_status','publication_status'),
        'commitment_outcomes' => array('commitment_id','event_id','campaign_id','company_id','commitment_text','original_language','target_at','actual_action','status','target_metrics','actual_metrics','evidence_document_id','review_status','publication_status'),
        'timeline_entries' => array('timeline_entry_id','event_id','campaign_id','document_id','occurred_at','entry_type','title','description','original_language','review_status','publication_status'),
    );
    if (!isset($allowed[$entity])) { v1_editorial_invalid('unsupported editorial entity'); }
    v1_editorial_assert_keys($record, $allowed[$entity], $location);
    if ($entity === 'actors') {
        $actorType = v1_editorial_code($record, 'actor_type', $location);
        if (!in_array($actorType, array('company','activist_shareholder','institution','shareholder_coalition','regulator','advisor'), true)) {
            v1_editorial_invalid($location . '.actor_type: unsupported');
        }
        $companyId = null;
        if (array_key_exists('company_id', $record) && $record['company_id'] !== null) { $companyId = v1_editorial_company_id($record, $location); }
        $country = v1_editorial_text($record, 'country_code', $location, 2, false);
        if ($country !== null && preg_match('/^[A-Z]{2}$/', $country) !== 1) { v1_editorial_invalid($location . '.country_code: invalid'); }
        $aliases = isset($record['aliases']) ? $record['aliases'] : array();
        if (!is_array($aliases) || !v1_editorial_is_list($aliases) || count($aliases) > 20) { v1_editorial_invalid($location . '.aliases: invalid'); }
        $seen = array();
        foreach ($aliases as $alias) {
            if (!is_string($alias) || trim($alias) === '' || mb_strlen($alias, 'UTF-8') > 255 || isset($seen[$alias])) { v1_editorial_invalid($location . '.aliases: invalid'); }
            $seen[$alias] = true;
        }
        if (strlen(json_value($aliases)) > 65535) { v1_editorial_invalid($location . '.aliases: encoded value too large'); }
        return array(
            'actor_id' => v1_editorial_id($record, 'actor_id', $location, 64),
            'actor_type' => $actorType,
            'display_name' => v1_editorial_text($record, 'display_name', $location, 255),
            'display_name_en' => v1_editorial_text($record, 'display_name_en', $location, 255, false),
            'company_id' => $companyId, 'country_code' => $country, 'aliases' => $aliases,
            'homepage_url' => v1_editorial_text($record, 'homepage_url', $location, 65535, false),
            'review_status' => v1_editorial_fail_closed($record, 'review_status', 'pending', $location),
            'record_status' => v1_editorial_fail_closed($record, 'record_status', 'inactive', $location),
        );
    }
    if ($entity === 'event_actors') {
        return array('event_id' => v1_editorial_id($record, 'event_id', $location), 'actor_id' => v1_editorial_id($record, 'actor_id', $location, 64),
            'actor_role' => v1_editorial_code($record, 'actor_role', $location),
            'review_status' => v1_editorial_fail_closed($record, 'review_status', 'pending', $location));
    }
    if ($entity === 'campaigns') {
        $stages = array('initial_signal','private_engagement','public_letter','public_campaign','shareholder_proposal','proxy_vote','resolution','implementation_tracking','closed');
        $stage = v1_editorial_code($record, 'stage', $location);
        if (!in_array($stage, $stages, true)) { v1_editorial_invalid($location . '.stage: unsupported'); }
        $started = v1_editorial_timestamp($record, 'started_at', $location);
        $ended = v1_editorial_timestamp($record, 'ended_at', $location, false);
        if ($ended !== null && $ended < $started) { v1_editorial_invalid($location . '.ended_at: precedes started_at'); }
        $outcome = v1_editorial_text($record, 'outcome', $location, 40, false);
        if ($outcome !== null && !in_array($outcome, array('settled','withdrawn','passed','failed'), true)) { v1_editorial_invalid($location . '.outcome: unsupported'); }
        return array('campaign_id' => v1_editorial_id($record, 'campaign_id', $location), 'company_id' => v1_editorial_company_id($record, $location),
            'lead_actor_id' => v1_editorial_id($record, 'lead_actor_id', $location, 64), 'title' => v1_editorial_text($record, 'title', $location, 700),
            'original_language' => v1_editorial_language_field($record, $location), 'demand_text' => v1_editorial_text($record, 'demand_text', $location, 1000000),
            'stage' => $stage, 'outcome' => $outcome, 'started_at' => $started, 'ended_at' => $ended,
            'evidence_document_ids' => v1_editorial_document_ids($record, 'evidence_document_ids', $location),
            'review_status' => v1_editorial_fail_closed($record, 'review_status', 'pending', $location),
            'publication_status' => v1_editorial_fail_closed($record, 'publication_status', 'draft', $location));
    }
    if ($entity === 'claim_evidence') {
        $claimType = v1_editorial_code($record, 'claim_type', $location);
        if (!in_array($claimType, array('actor_claim','company_response','official_fact','media_report','editorial_interpretation'), true)) { v1_editorial_invalid($location . '.claim_type: unsupported'); }
        return array('claim_id' => v1_editorial_id($record, 'claim_id', $location), 'event_id' => v1_editorial_id($record, 'event_id', $location),
            'campaign_id' => v1_editorial_id($record, 'campaign_id', $location, 96, false), 'actor_id' => v1_editorial_id($record, 'actor_id', $location, 64, false),
            'document_id' => v1_editorial_id($record, 'document_id', $location), 'claim_type' => $claimType,
            'claim_text' => v1_editorial_text($record, 'claim_text', $location, 1000000), 'original_language' => v1_editorial_language_field($record, $location),
            'evidence_locator' => v1_editorial_text($record, 'evidence_locator', $location, 500, false),
            'editorial_status' => v1_editorial_fail_closed($record, 'editorial_status', 'pending', $location));
    }
    if ($entity === 'proposal_votes') {
        list($eventId, $campaignId) = v1_editorial_parent_ids($record, $location);
        $result = v1_editorial_text($record, 'result', $location, 24, false) ?: 'pending';
        if (!in_array($result, array('pending','passed','failed','withdrawn'), true)) { v1_editorial_invalid($location . '.result: unsupported'); }
        return array('proposal_vote_id' => v1_editorial_id($record, 'proposal_vote_id', $location), 'event_id' => $eventId, 'campaign_id' => $campaignId,
            'company_id' => v1_editorial_company_id($record, $location), 'proposer_actor_id' => v1_editorial_id($record, 'proposer_actor_id', $location, 64, false),
            'agenda_no' => v1_editorial_text($record, 'agenda_no', $location, 40, false), 'agenda_title' => v1_editorial_text($record, 'agenda_title', $location, 700),
            'original_language' => v1_editorial_language_field($record, $location), 'meeting_at' => v1_editorial_timestamp($record, 'meeting_at', $location),
            'recommendation' => v1_editorial_text($record, 'recommendation', $location, 40, false),
            'recommendation_source' => v1_editorial_text($record, 'recommendation_source', $location, 255, false),
            'result' => $result,
            'votes_for' => v1_editorial_number($record, 'votes_for', $location), 'votes_against' => v1_editorial_number($record, 'votes_against', $location),
            'votes_abstain' => v1_editorial_number($record, 'votes_abstain', $location), 'evidence_document_id' => v1_editorial_id($record, 'evidence_document_id', $location),
            'review_status' => v1_editorial_fail_closed($record, 'review_status', 'pending', $location),
            'publication_status' => v1_editorial_fail_closed($record, 'publication_status', 'draft', $location));
    }
    if ($entity === 'commitment_outcomes') {
        list($eventId, $campaignId) = v1_editorial_parent_ids($record, $location);
        foreach (array('target_metrics','actual_metrics') as $metricsField) {
            if (isset($record[$metricsField]) && (!is_array($record[$metricsField]) || (count($record[$metricsField]) > 0 && v1_editorial_is_list($record[$metricsField])))) {
                v1_editorial_invalid($location . '.' . $metricsField . ': object required');
            }
        }
        $commitmentStatus = v1_editorial_text($record, 'status', $location, 32, false) ?: 'announced';
        if (!in_array($commitmentStatus, array('planned','announced','in_progress','met','partially_met','missed','cancelled'), true)) {
            v1_editorial_invalid($location . '.status: unsupported');
        }
        return array('commitment_id' => v1_editorial_id($record, 'commitment_id', $location), 'event_id' => $eventId, 'campaign_id' => $campaignId,
            'company_id' => v1_editorial_company_id($record, $location), 'commitment_text' => v1_editorial_text($record, 'commitment_text', $location, 1000000),
            'original_language' => v1_editorial_language_field($record, $location), 'target_at' => v1_editorial_timestamp($record, 'target_at', $location, false),
            'actual_action' => v1_editorial_text($record, 'actual_action', $location, 1000000, false),
            'status' => $commitmentStatus,
            'target_metrics' => isset($record['target_metrics']) ? $record['target_metrics'] : null,
            'actual_metrics' => isset($record['actual_metrics']) ? $record['actual_metrics'] : null,
            'evidence_document_id' => v1_editorial_id($record, 'evidence_document_id', $location),
            'review_status' => v1_editorial_fail_closed($record, 'review_status', 'pending', $location),
            'publication_status' => v1_editorial_fail_closed($record, 'publication_status', 'draft', $location));
    }
    list($eventId, $campaignId) = v1_editorial_parent_ids($record, $location);
    return array('timeline_entry_id' => v1_editorial_id($record, 'timeline_entry_id', $location), 'event_id' => $eventId, 'campaign_id' => $campaignId,
        'document_id' => v1_editorial_id($record, 'document_id', $location), 'occurred_at' => v1_editorial_timestamp($record, 'occurred_at', $location),
        'entry_type' => v1_editorial_code($record, 'entry_type', $location), 'title' => v1_editorial_text($record, 'title', $location, 700),
        'description' => v1_editorial_text($record, 'description', $location, 1000000, false), 'original_language' => v1_editorial_language_field($record, $location),
        'review_status' => v1_editorial_fail_closed($record, 'review_status', 'pending', $location),
        'publication_status' => v1_editorial_fail_closed($record, 'publication_status', 'draft', $location));
}

function v1_official_site_contract_error(string $detail): void {
    respond(400,array('ok'=>false,'error'=>'invalid_official_site_snapshot','detail'=>$detail));
}

function v1_official_site_source_right(PDO $pdo, array $config, string $sourceRightId, string $sourceType,
    string $sourceKey, bool $forUpdate): ?array {
    $stmt = $pdo->prepare('SELECT source_right_id,source_type,source_key,permission_scope,evidence_uri,evidence_hash,'
        . 'valid_from,valid_until,revoked_at,redistribution_allowed,status,updated_at FROM '
        . table_name($config,'source_rights') . ' WHERE source_right_id=?' . ($forUpdate ? ' FOR UPDATE' : ''));
    $stmt->execute(array($sourceRightId)); $row = $stmt->fetch();
    if (!is_array($row) || (string)$row['source_type'] !== $sourceType || (string)$row['source_key'] !== $sourceKey) { return null; }
    $now = gmdate('Y-m-d H:i:s');
    $evidence = trim((string)$row['evidence_uri']) !== '' || preg_match('/^[a-f0-9]{64}$/i',(string)$row['evidence_hash']) === 1;
    if ((string)$row['status'] !== 'active' || (string)$row['valid_from'] > $now
        || ($row['valid_until'] !== null && (string)$row['valid_until'] <= $now) || $row['revoked_at'] !== null
        || (int)$row['redistribution_allowed'] !== 1 || trim((string)$row['permission_scope']) === '' || !$evidence) {
        return null;
    }
    return $row;
}

function v1_official_site_connector_id(string $entityType, string $entityId): ?string {
    if ($entityType === 'company') {
        return preg_match('/^[0-9]{8}$/',$entityId) === 1 ? 'company-site:' . $entityId : null;
    }
    if ($entityType !== 'actor' || !v1_valid_entity_id($entityId,96)) { return null; }
    $readable = 'activist-site:' . $entityId;
    if (strlen($readable) <= 64 && v1_valid_entity_id($readable,64)) { return $readable; }
    return 'activist-site:' . substr(hash('sha256',$entityId),0,32);
}

function v1_official_site_source_right_id(string $connectorId): string {
    $readable = 'right:' . $connectorId;
    if (strlen($readable) <= 64 && v1_valid_entity_id($readable,64)) { return $readable; }
    return 'right:official-site:' . substr(hash('sha256',$connectorId),0,32);
}

function v1_official_site_stable_id(string $prefix, array $parts, int $length): string {
    $normalized = array();
    foreach ($parts as $part) { $normalized[] = trim((string)$part); }
    return $prefix . ':' . substr(hash('sha256',implode("\x1f",$normalized)),0,$length);
}

function v1_official_site_review_semantic_payload(array $payload): array {
    if (isset($payload['draft_document']) && is_array($payload['draft_document'])) {
        unset($payload['draft_document']['retrieved_at']);
    }
    return $payload;
}

/** HMAC-only atomic connector receipt for allowlisted official websites. */
function upsert_official_site_snapshot(PDO $pdo, array $config, array $payload, object $payloadObject): void {
    v1_assert_object_keys($payload,array('schema_version','snapshot_id','receipt_sha256','code_revision','collected_at',
        'manifest_sha256','payload_sha256','connector','companies','documents','events','review_items','tombstones','expected'),'body');
    if (!isset($payload['schema_version']) || $payload['schema_version'] !== 1) { v1_official_site_contract_error('schema_version'); }
    $snapshotId = isset($payload['snapshot_id']) ? trim((string)$payload['snapshot_id']) : '';
    $receiptSha = strtolower(trim((string)($payload['receipt_sha256'] ?? '')));
    $manifestSha = strtolower(trim((string)($payload['manifest_sha256'] ?? '')));
    $payloadSha = strtolower(trim((string)($payload['payload_sha256'] ?? '')));
    $revision = v1_valid_build_sha($payload['code_revision'] ?? null);
    $collectedAt = v1_editorial_datetime_utc($payload['collected_at'] ?? null);
    $connector = isset($payload['connector']) && is_array($payload['connector']) ? $payload['connector'] : null;
    if (!v1_valid_entity_id($snapshotId) || preg_match('/^official-site-snapshot:[a-f0-9]{64}$/',$snapshotId) !== 1
        || preg_match('/^[a-f0-9]{64}$/',$receiptSha) !== 1 || preg_match('/^[a-f0-9]{64}$/',$manifestSha) !== 1
        || preg_match('/^[a-f0-9]{64}$/',$payloadSha) !== 1 || $revision === null || $collectedAt === null || $connector === null) {
        v1_official_site_contract_error('receipt_metadata');
    }
    $payloadCoreObject = clone $payloadObject;
    unset($payloadCoreObject->snapshot_id,$payloadCoreObject->payload_sha256);
    if (!hash_equals($payloadSha,hash('sha256',v1_strict_canonical_json_encode($payloadCoreObject,'official_site_payload_encode_failed')))) {
        v1_official_site_contract_error('payload_sha256');
    }
    v1_assert_object_keys($connector,array('connector_id','entity_type','entity_id','source_class','source_right_id',
        'pages_fetched','total_count','payload_sha256'),'connector');
    $connectorId = trim((string)($connector['connector_id'] ?? ''));
    $sourceRightId = trim((string)($connector['source_right_id'] ?? ''));
    $sourceType = trim((string)($connector['source_class'] ?? ''));
    $sourceKey = $connectorId;
    $entityType = trim((string)($connector['entity_type'] ?? ''));
    $entityId = trim((string)($connector['entity_id'] ?? ''));
    $connectorPayloadSha = strtolower(trim((string)($connector['payload_sha256'] ?? '')));
    $expectedConnectorId = v1_official_site_connector_id($entityType,$entityId);
    $sourceIdentityValid = $expectedConnectorId !== null && hash_equals($expectedConnectorId,$sourceKey);
    if (!v1_valid_entity_id($connectorId) || !v1_valid_entity_id($sourceRightId,64) || !$sourceIdentityValid
        || !in_array($entityType,array('company','actor'),true) || !v1_valid_entity_id($entityId,96)
        || ($entityType === 'company' && $sourceType !== 'company_statement')
        || ($entityType === 'actor' && $sourceType !== 'activist_statement')
        || !hash_equals(v1_official_site_source_right_id($connectorId),$sourceRightId)
        || preg_match('/^[a-f0-9]{64}$/',$connectorPayloadSha) !== 1
        || !isset($connector['pages_fetched'],$connector['total_count']) || !is_int($connector['pages_fetched'])
        || !is_int($connector['total_count']) || $connector['pages_fetched'] < 1 || $connector['total_count'] < 0) {
        v1_official_site_contract_error('connector_receipt');
    }
    $expectedSnapshotId = v1_official_site_stable_id('official-site-snapshot',array($connectorId,$receiptSha,$payloadSha),64);
    if (!hash_equals($expectedSnapshotId,$snapshotId)) { v1_official_site_contract_error('snapshot_identity'); }
    $companies = isset($payload['companies']) && is_array($payload['companies']) ? $payload['companies'] : null;
    $documents = isset($payload['documents']) && is_array($payload['documents']) ? $payload['documents'] : null;
    $events = isset($payload['events']) && is_array($payload['events']) ? $payload['events'] : null;
    $reviewItems = isset($payload['review_items']) && is_array($payload['review_items']) ? $payload['review_items'] : null;
    $tombstones = isset($payload['tombstones']) && is_array($payload['tombstones']) ? $payload['tombstones'] : null;
    if ($companies === null || $documents === null || $events === null || $reviewItems === null || $tombstones === null
        || count($companies) > 50 || count($documents) > 500 || count($events) > 500
        || count($reviewItems) > 1000 || count($tombstones) > 500) {
        v1_official_site_contract_error('record_arrays');
    }

    $normalizedCompanies = array(); $companyIds = array();
    foreach ($companies as $index => $company) {
        if (!is_array($company)) { v1_official_site_contract_error('companies['.$index.']'); }
        $id = trim((string)($company['company_id'] ?? $company['corp_code'] ?? ''));
        $name = trim((string)($company['legal_name'] ?? $company['corp_name'] ?? ''));
        if (preg_match('/^[0-9]{8}$/',$id) !== 1 || $name === '' || isset($companyIds[$id])
            || $entityType !== 'company' || $id !== $entityId
            || trim((string)($company['record_status'] ?? '')) !== 'active') {
            v1_official_site_contract_error('company_identity');
        }
        $companyIds[$id] = true; $normalizedCompanies[] = array('company_id'=>$id,'legal_name'=>$name,
            'stock_code'=>trim((string)($company['stock_code'] ?? '')),'market'=>trim((string)($company['market'] ?? '')),
            'homepage_url'=>trim((string)($company['homepage_url'] ?? '')));
    }

    $normalizedDocuments = array(); $documentIds = array();
    foreach ($documents as $index => $document) {
        if (!is_array($document)) { v1_official_site_contract_error('documents['.$index.']'); }
        $id = trim((string)($document['document_id'] ?? '')); $companyId = trim((string)($document['company_id'] ?? ''));
        $externalId = trim((string)($document['external_id'] ?? '')); $title = (string)($document['title'] ?? '');
        $language = v1_editorial_language($document['original_language'] ?? null);
        $collectionKey = trim((string)($document['collection_key'] ?? ''));
        $url = trim((string)($document['original_url'] ?? '')); $contentHash = strtolower(trim((string)($document['content_hash'] ?? '')));
        $publishedAt = array_key_exists('published_at',$document) && $document['published_at'] !== null
            ? v1_editorial_datetime_utc($document['published_at']) : null;
        $retrievedAt = v1_editorial_datetime_utc($document['retrieved_at'] ?? $payload['collected_at']);
        $expectedDocumentId = v1_official_site_stable_id('site-doc',array($connectorId,$externalId,$contentHash),32);
        $expectedCollectionKey = v1_official_site_stable_id('site-collection',array($connectorId,$externalId),32);
        $bodyText = array_key_exists('body_text',$document) && is_string($document['body_text']) ? $document['body_text'] : null;
        $expectedContentHash = $bodyText === null ? null : hash('sha256',$title . "\n" . $bodyText . "\n" . $url);
        if (!v1_valid_entity_id($id) || isset($documentIds[$id]) || ($companyId !== '' && preg_match('/^[0-9]{8}$/',$companyId) !== 1)
            || !hash_equals($expectedDocumentId,$id) || !hash_equals($expectedCollectionKey,$collectionKey)
            || $externalId === '' || strlen($externalId) > 191
            || trim($title) === '' || mb_strlen($title,'UTF-8') > 700 || $language === null || preg_match('#^https?://#i',$url) !== 1
            || !array_key_exists('body_text',$document) || !is_string($document['body_text'])
            || !v1_valid_entity_id($collectionKey)
            || preg_match('/^[a-f0-9]{64}$/',$contentHash) !== 1 || $expectedContentHash === null
            || !hash_equals($expectedContentHash,$contentHash) || $retrievedAt === null
            || (array_key_exists('published_at',$document) && $document['published_at'] !== null && $publishedAt === null)
            || trim((string)($document['source_right_id'] ?? '')) !== $sourceRightId
            || trim((string)($document['source_class'] ?? '')) !== $sourceType
            || ($document['version_no'] ?? null) !== 1
            || trim((string)($document['verification_status'] ?? '')) !== 'unverified'
            || trim((string)($document['publication_status'] ?? '')) !== 'draft') {
            v1_official_site_contract_error('document_contract');
        }
        $documentIds[$id] = true; $normalizedDocuments[] = array('document_id'=>$id,'company_id'=>$companyId ?: null,
            'external_id'=>$externalId,'document_type'=>trim((string)($document['document_type'] ?? '')) ?: null,
            'original_language'=>$language,'title'=>$title,'body_text'=>$bodyText,'original_url'=>$url,'content_hash'=>$contentHash,
            'collection_key'=>$collectionKey,'published_at'=>$publishedAt,'retrieved_at'=>$retrievedAt,'payload'=>$document);
    }

    $normalizedEvents = array(); $eventIds = array(); $expectedObservations = 0;
    foreach ($events as $index => $event) {
        if (!is_array($event)) { v1_official_site_contract_error('events['.$index.']'); }
        $id = trim((string)($event['event_id'] ?? '')); $companyId = trim((string)($event['company_id'] ?? ''));
        $externalId = trim((string)($event['collection_key'] ?? '')); $eventType = trim((string)($event['event_type'] ?? ''));
        $title = (string)($event['title'] ?? ''); $language = v1_editorial_language($event['original_language'] ?? null);
        $occurredDate = v1_normalize_identity_datetime($event['occurred_at'] ?? null,false);
        $deadlineDate = v1_normalize_identity_datetime($event['deadline_at'] ?? null,false);
        $occurredAt = $occurredDate === null ? null : (string)$occurredDate['mysql'];
        $deadlineAt = $deadlineDate === null ? null : (string)$deadlineDate['mysql'];
        $ids = isset($event['document_ids']) && is_array($event['document_ids']) ? array_values(array_unique(array_map('strval',$event['document_ids']))) : array();
        $importance = trim((string)($event['importance'] ?? 'medium'));
        $identity = v1_build_event_identity($companyId,$eventType,$event['identity_action'] ?? null,$event['identity_target'] ?? null,
            $event['identity_actor_id'] ?? null,$event['identity_effective_at'] ?? null,$event['identity_deadline_at'] ?? null,false);
        $comparisonKey = trim((string)($event['comparison_key'] ?? ''));
        $eventRightIds = isset($event['source_right_ids']) && is_array($event['source_right_ids'])
            ? array_values(array_unique(array_map('strval',$event['source_right_ids']))) : array();
        if (!in_array($importance,array('low','medium','high','market_sensitive','critical'),true)) { v1_official_site_contract_error('event_importance'); }
        if (!v1_valid_entity_id($id) || isset($eventIds[$id]) || preg_match('/^[0-9]{8}$/',$companyId) !== 1
            || $externalId === '' || strlen($externalId) > 191
            || preg_match('/^[A-Za-z0-9_.:\-]{1,64}$/',$eventType) !== 1 || trim($title) === '' || mb_strlen($title,'UTF-8') > 700 || $language === null
            || $occurredAt === null || $deadlineAt === null
            || trim((string)($event['identity_status'] ?? '')) !== 'complete' || $identity === null
            || ($identity !== null && $eventType !== (string)$identity['event_type'])
            || $comparisonKey === '' || $comparisonKey !== $id || !hash_equals((string)$identity['comparison_key'],$comparisonKey)
            || !hash_equals((string)$identity['identity_effective_at'],$occurredAt)
            || !hash_equals((string)$identity['identity_deadline_at'],$deadlineAt)
            || $eventRightIds !== array($sourceRightId)
            || trim((string)($event['verification_status'] ?? '')) !== 'unverified'
            || trim((string)($event['review_status'] ?? '')) !== 'pending'
            || trim((string)($event['publication_status'] ?? '')) !== 'draft'
            || ($event['review_required'] ?? null) !== true
            || count($ids) < 1) { v1_official_site_contract_error('event_contract'); }
        foreach ($ids as $documentId) {
            if (!v1_valid_entity_id($documentId) || !isset($documentIds[$documentId])) { v1_official_site_contract_error('event_document_scope'); }
        }
        $eventIds[$id] = true; $expectedObservations += count($ids);
        $normalizedEvents[] = array('event_id'=>$id,'company_id'=>$companyId,'external_id'=>$externalId,'event_type'=>$eventType,
            'title'=>$title,'original_language'=>$language,'summary'=>array_key_exists('summary',$event) ? (string)$event['summary'] : null,
            'occurred_at'=>$occurredAt,'deadline_at'=>$deadlineAt,'importance'=>$importance,'document_ids'=>$ids,'payload'=>$event,
            'identity_action'=>$identity['identity_action'],'identity_target'=>$identity['identity_target'],
            'identity_actor_id'=>$identity['identity_actor_id'],'identity_effective_at'=>$identity['identity_effective_at'],
            'identity_deadline_at'=>$identity['identity_deadline_at'],'comparison_key'=>$comparisonKey);
    }

    $normalizedReviews = array(); $reviewIds = array();
    foreach ($reviewItems as $index => $item) {
        if (!is_array($item)) { v1_official_site_contract_error('review_items['.$index.']'); }
        $id = trim((string)($item['review_id'] ?? '')); $itemConnector = trim((string)($item['connector_id'] ?? ''));
        $entityType = trim((string)($item['entity_type'] ?? '')); $entityId = trim((string)($item['entity_id'] ?? ''));
        $reasons = isset($item['review_reasons']) && is_array($item['review_reasons']) ? $item['review_reasons'] : array();
        $normalizedReasons = array(); foreach ($reasons as $reason) { $reason = trim((string)$reason); if ($reason !== '') { $normalizedReasons[] = $reason; } }
        $reason = implode('; ',array_values(array_unique($normalizedReasons)));
        $reviewExternalId = trim((string)($item['external_id'] ?? ''));
        if (!v1_valid_entity_id($id) || isset($reviewIds[$id]) || $itemConnector !== $connectorId
            || $entityType !== (string)$connector['entity_type'] || $entityId !== (string)$connector['entity_id']
            || !v1_valid_entity_id($entityId,96) || $reason === '' || $reviewExternalId === '' || strlen($reviewExternalId) > 191
            || trim((string)($item['source_class'] ?? '')) !== $sourceType
            || trim((string)($item['source_right_id'] ?? '')) !== $sourceRightId
            || trim((string)($item['action'] ?? '')) !== 'editor_identity_review_required'
            || !hash_equals(v1_official_site_stable_id('site-review',array($connectorId,$reviewExternalId,
                (string)($item['draft_document']['content_hash'] ?? '')),32),$id)
            || !isset($item['draft_document']) || !is_array($item['draft_document'])
            || !array_key_exists('proposed_identity',$item) || !is_array($item['proposed_identity'])) {
            v1_official_site_contract_error('review_item_contract');
        }
        $reviewIds[$id] = true; $normalizedReviews[] = array('review_item_id'=>$id,'entity_type'=>$entityType,
            'entity_id'=>$entityId,'reason'=>$reason,'payload'=>$item);
    }

    $normalizedTombstones = array(); $tombstoneIds = array();
    foreach ($tombstones as $index => $item) {
        if (!is_array($item)) { v1_official_site_contract_error('tombstones['.$index.']'); }
        $id = trim((string)($item['tombstone_id'] ?? '')); $itemConnector = trim((string)($item['connector_id'] ?? ''));
        $entityType = trim((string)($item['entity_type'] ?? ''));
        $externalId = trim((string)($item['external_id'] ?? '')); $entityId = trim((string)($item['entity_id'] ?? ''));
        $reason = 'Official-site delete signal; review only, no automatic delete';
        $observedAt = v1_editorial_datetime_utc($item['deleted_at'] ?? null);
        if (!v1_valid_entity_id($id) || isset($tombstoneIds[$id]) || $itemConnector !== $connectorId
            || $entityType !== (string)$connector['entity_type'] || $entityId !== (string)$connector['entity_id']
            || $externalId === '' || strlen($externalId) > 191 || !v1_valid_entity_id($entityId,96)
            || trim((string)($item['source_class'] ?? '')) !== $sourceType
            || trim((string)($item['source_right_id'] ?? '')) !== $sourceRightId
            || !hash_equals(v1_official_site_stable_id('site-tombstone',array($connectorId,$externalId,
                (string)($item['deleted_at'] ?? '')),32),$id)
            || $observedAt === null || trim((string)($item['action'] ?? '')) !== 'review_only_no_automatic_delete') {
            v1_official_site_contract_error('tombstone_contract');
        }
        $tombstoneIds[$id] = true; $normalizedTombstones[] = array('tombstone_id'=>$id,'entity_type'=>$entityType,
            'external_id'=>$externalId,'entity_id'=>$entityId ?: null,'reason'=>$reason,'observed_at'=>$observedAt,'payload'=>$item);
    }

    $requestHash = hash('sha256',v1_strict_canonical_json_encode($payloadObject,'official_site_snapshot_encode_failed'));
    $accepted = array('companies'=>count($normalizedCompanies),'documents'=>count($normalizedDocuments),'events'=>count($normalizedEvents),
        'event_observations'=>$expectedObservations,'review_items'=>count($normalizedReviews),'tombstones'=>count($normalizedTombstones));
    $expected = isset($payload['expected']) && is_array($payload['expected']) ? $payload['expected'] : null;
    if ($expected === null || count($expected) !== count($accepted)) { v1_official_site_contract_error('expected_ack_contract'); }
    foreach ($accepted as $field => $count) {
        if (!array_key_exists($field,$expected) || !is_int($expected[$field]) || $expected[$field] !== $count) {
            v1_official_site_contract_error('expected_ack_count');
        }
    }
    if ((int)$connector['total_count'] !== $accepted['documents'] + $accepted['review_items'] + $accepted['tombstones']) {
        v1_official_site_contract_error('connector_total_count_mismatch');
    }
    $now = gmdate('Y-m-d H:i:s'); $idempotent = false;
    $pdo->beginTransaction();
    try {
        $snapshotLookup = $pdo->prepare('SELECT receipt_sha256,request_sha256,manifest_sha256,accepted_json,status FROM '
            . table_name($config,'official_site_snapshots') . ' WHERE snapshot_id=? FOR UPDATE');
        $snapshotLookup->execute(array($snapshotId)); $existingSnapshot = $snapshotLookup->fetch();
        if ($existingSnapshot) {
            $storedAccepted = json_decode((string)$existingSnapshot['accepted_json'],true);
            if (!hash_equals((string)$existingSnapshot['receipt_sha256'],$receiptSha)
                || !hash_equals((string)$existingSnapshot['request_sha256'],$requestHash)
                || !hash_equals((string)$existingSnapshot['manifest_sha256'],$manifestSha)
                || $storedAccepted !== $accepted || (string)$existingSnapshot['status'] !== 'succeeded') {
                throw new RuntimeException('official_site_snapshot_idempotency_conflict');
            }
            $pdo->commit();
            respond(200,array('ok'=>true,'snapshot_id'=>$snapshotId,'receipt_sha256'=>$receiptSha,
                'accepted'=>$accepted,'rejected'=>0,'idempotent'=>true));
        }
        if (v1_official_site_source_right($pdo,$config,$sourceRightId,$sourceType,$sourceKey,true) === null) {
            throw new RuntimeException('official_site_source_right_ineligible');
        }
        // Migration 011 adds the canonical document source identity. Keep the
        // schema-10 writer compatible during a staged DB-first deployment, and
        // persist the exact locked SourceRight key whenever the global schema
        // is available.
        $documentSourceIdentityEnabled = v1_global_dart_bridge_enabled($pdo,$config);

        $companyStmt = $pdo->prepare('INSERT INTO ' . table_name($config,'companies')
            . ' (company_id,stock_code,market,legal_name,legal_name_en,short_name,aliases_json,homepage_url,record_status,listing_status,master_modified_at,created_at,updated_at)'
            . ' VALUES (?,?,?, ?,NULL,NULL,\'[]\',?,\'active\',\'unknown\',NULL,?,?) ON DUPLICATE KEY UPDATE '
            . 'stock_code=COALESCE(NULLIF(VALUES(stock_code),\'\'),stock_code),market=COALESCE(NULLIF(VALUES(market),\'\'),market),'
            . 'legal_name=COALESCE(NULLIF(VALUES(legal_name),\'\'),legal_name),homepage_url=COALESCE(NULLIF(VALUES(homepage_url),\'\'),homepage_url),updated_at=VALUES(updated_at)');
        foreach ($normalizedCompanies as $company) {
            $companyStmt->execute(array($company['company_id'],$company['stock_code'] ?: null,$company['market'] ?: null,
                mb_substr($company['legal_name'],0,255,'UTF-8'),$company['homepage_url'] ?: null,$now,$now));
        }
        $companyExists = $pdo->prepare('SELECT company_id FROM ' . table_name($config,'companies')
            . ' WHERE company_id=? AND record_status=\'active\' LIMIT 1 FOR UPDATE');
        $referencedCompanies = array();
        foreach ($normalizedDocuments as $row) { if ($row['company_id'] !== null) { $referencedCompanies[$row['company_id']] = true; } }
        foreach ($normalizedEvents as $row) { $referencedCompanies[$row['company_id']] = true; }
        foreach (array_keys($referencedCompanies) as $companyId) {
            $companyExists->execute(array($companyId));
            if ($companyExists->fetchColumn() === false) { throw new RuntimeException('official_site_company_missing'); }
        }

        $documentExisting = $pdo->prepare('SELECT source_right_id,source_class,'
            . ($documentSourceIdentityEnabled ? 'source_key,' : '')
            . 'external_id,content_hash,collection_key,version_no,'
            . 'correction_of_document_id,original_language,title,body_text,original_url,verification_status,publication_status,retrieved_at FROM '
            . table_name($config,'documents') . ' WHERE document_id=? FOR UPDATE');
        if ($documentSourceIdentityEnabled) {
            $documentStmt = $pdo->prepare('INSERT INTO ' . table_name($config,'documents')
                . ' (document_id,company_id,source_right_id,source_class,source_key,external_id,document_type,original_language,title,body_text,original_url,content_hash,'
                . 'collection_key,correction_of_document_id,version_no,published_at,retrieved_at,verification_status,publication_status,payload_json,created_at,updated_at)'
                . ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\'unverified\',\'draft\',?,?,?)');
        } else {
            $documentStmt = $pdo->prepare('INSERT INTO ' . table_name($config,'documents')
                . ' (document_id,company_id,source_right_id,source_class,external_id,document_type,original_language,title,body_text,original_url,content_hash,'
                . 'collection_key,correction_of_document_id,version_no,published_at,retrieved_at,verification_status,publication_status,payload_json,created_at,updated_at)'
                . ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\'unverified\',\'draft\',?,?,?)');
        }
        $documentLatest = $pdo->prepare('SELECT document_id,source_right_id,version_no FROM '
            . table_name($config,'documents') . ' WHERE source_right_id=? AND source_class=? AND external_id=?'
            . ' ORDER BY version_no DESC,created_at DESC,document_id DESC LIMIT 1 FOR UPDATE');
        $documentRefresh = $pdo->prepare('UPDATE ' . table_name($config,'documents')
            . ' SET retrieved_at=GREATEST(retrieved_at,?),updated_at=? WHERE document_id=?');
        foreach ($normalizedDocuments as $document) {
            $documentExisting->execute(array($document['document_id'])); $existing = $documentExisting->fetch();
            if ($existing) {
                if ((string)$existing['source_right_id'] !== $sourceRightId || (string)$existing['source_class'] !== $sourceType
                    || ($documentSourceIdentityEnabled && (string)($existing['source_key'] ?? '') !== $sourceKey)
                    || (string)$existing['external_id'] !== $document['external_id'] || (string)$existing['content_hash'] !== $document['content_hash']
                    || (string)$existing['collection_key'] !== $document['collection_key']
                    || (string)$existing['original_language'] !== $document['original_language']
                    || (string)$existing['title'] !== $document['title'] || (string)$existing['body_text'] !== $document['body_text']
                    || (string)$existing['original_url'] !== $document['original_url']) {
                    throw new RuntimeException('official_site_document_identity_conflict');
                }
                // Never downgrade or overwrite reviewed/published content. A
                // same-content observation advances only retrieval metadata.
                $documentRefresh->execute(array($document['retrieved_at'],$now,$document['document_id']));
                continue;
            }
            $documentLatest->execute(array($sourceRightId,$sourceType,$document['external_id'])); $latest = $documentLatest->fetch();
            if ($latest && (string)$latest['source_right_id'] !== $sourceRightId) {
                throw new RuntimeException('official_site_document_lineage_conflict');
            }
            $versionNo = $latest ? (int)$latest['version_no'] + 1 : 1;
            $correctionOf = $latest ? (string)$latest['document_id'] : null;
            $documentValues = array($document['document_id'],$document['company_id'],$sourceRightId,$sourceType);
            if ($documentSourceIdentityEnabled) { $documentValues[] = $sourceKey; }
            $documentValues = array_merge($documentValues,array($document['external_id'],$document['document_type'],
                $document['original_language'],$document['title'],$document['body_text'],$document['original_url'],
                $document['content_hash'],$document['collection_key'],$correctionOf,$versionNo,
                $document['published_at'],$document['retrieved_at'],json_value($document['payload']),$now,$now));
            $documentStmt->execute($documentValues);
        }

        $eventExisting = $pdo->prepare('SELECT company_id,event_type,identity_action,identity_target,identity_actor_id,'
            . 'identity_effective_at,identity_deadline_at,identity_status,comparison_key,publication_status FROM '
            . table_name($config,'governance_events') . ' WHERE event_id=? FOR UPDATE');
        $eventStmt = $pdo->prepare('INSERT INTO ' . table_name($config,'governance_events')
            . ' (event_id,company_id,event_type,title,original_language,summary,occurred_at,deadline_at,importance,verification_status,review_status,'
            . 'publication_status,collection_key,identity_action,identity_target,identity_actor_id,identity_effective_at,identity_deadline_at,identity_status,'
            . 'comparison_key,payload_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,\'unverified\',\'pending\',\'draft\',?,?,?,?,?,?,\'complete\',?,?,?,?) '
            );
        $eventDocumentStmt = $pdo->prepare('INSERT INTO ' . table_name($config,'event_documents')
            . ' (event_id,document_id,relation_type,position_no,created_at) VALUES (?,?,\'evidence\',?,?) ON DUPLICATE KEY UPDATE position_no=VALUES(position_no)');
        $documentObservation = $pdo->prepare('SELECT content_hash,retrieved_at FROM ' . table_name($config,'documents')
            . ' WHERE document_id=? AND source_right_id=? LIMIT 1');
        $observationStmt = $pdo->prepare('INSERT INTO ' . table_name($config,'event_observations')
            . ' (observation_id,event_id,document_id,source_class,source_key,first_observed_at,observed_at,payload_hash,payload_json,created_at,updated_at)'
            . ' VALUES (?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE first_observed_at=LEAST(first_observed_at,VALUES(first_observed_at)),'
            . 'observed_at=GREATEST(observed_at,VALUES(observed_at)),payload_hash=VALUES(payload_hash),payload_json=VALUES(payload_json),updated_at=VALUES(updated_at)');
        $priorEventLink = $pdo->prepare('SELECT entity_id FROM ' . table_name($config,'official_site_identity_links')
            . ' WHERE connector_id=? AND entity_type=\'event\' AND external_id=? AND active=1 LIMIT 1 FOR UPDATE');
        $oldEventObservationsDelete = $pdo->prepare('DELETE FROM ' . table_name($config,'event_observations')
            . ' WHERE event_id=? AND source_key=? AND document_id=?');
        $oldEventDocumentsDelete = $pdo->prepare('DELETE FROM ' . table_name($config,'event_documents')
            . ' WHERE event_id=? AND document_id=?');
        foreach ($normalizedEvents as $event) {
            $eventExisting->execute(array($event['event_id'])); $existing = $eventExisting->fetch();
            if ($existing) {
                $storedIdentity = v1_resolve_stored_event_identity((string)$existing['company_id'],(string)$existing['event_type'],
                    $existing['identity_action'],$existing['identity_target'],$existing['identity_actor_id'],
                    $existing['identity_effective_at'],$existing['identity_deadline_at'],$existing['comparison_key']);
                $sameIdentity = $storedIdentity !== null && (string)$existing['identity_status'] === 'complete'
                    && hash_equals((string)$existing['comparison_key'],$event['comparison_key']);
                foreach (array('company_id','event_type','identity_action','identity_target','identity_actor_id',
                    'identity_effective_at','identity_deadline_at','comparison_key') as $field) {
                    if (!$sameIdentity || (string)$storedIdentity[$field] !== (string)$event[$field]) { $sameIdentity = false; break; }
                }
                if (!$sameIdentity) { throw new RuntimeException('official_site_event_identity_conflict'); }
                // A canonical DART/KIND/editorial event keeps its lifecycle and
                // presentation fields.  This connector only adds evidence.
            } else {
                $eventStmt->execute(array($event['event_id'],$event['company_id'],$event['event_type'],$event['title'],
                    $event['original_language'],$event['summary'],$event['occurred_at'],$event['deadline_at'],$event['importance'],
                    mb_substr($event['external_id'],0,96,'UTF-8'),$event['identity_action'],$event['identity_target'],$event['identity_actor_id'],
                    $event['identity_effective_at'],$event['identity_deadline_at'],$event['comparison_key'],json_value($event['payload']),$now,$now));
            }
            $priorEventLink->execute(array($connectorId,$event['external_id'])); $oldLink = $priorEventLink->fetch();
            if ($oldLink && (string)$oldLink['entity_id'] !== $event['event_id']) {
                $oldEventId = (string)$oldLink['entity_id'];
                foreach ($event['document_ids'] as $changedDocumentId) {
                    $oldEventObservationsDelete->execute(array($oldEventId,$sourceKey,$changedDocumentId));
                    $oldEventDocumentsDelete->execute(array($oldEventId,$changedDocumentId));
                }
            }
            foreach ($event['document_ids'] as $position => $documentId) {
                $eventDocumentStmt->execute(array($event['event_id'],$documentId,$position,$now));
                $documentObservation->execute(array($documentId,$sourceRightId)); $doc = $documentObservation->fetch();
                if (!$doc) { throw new RuntimeException('official_site_observation_document_missing'); }
                $observationAt = (string)$doc['retrieved_at'];
                $observationStmt->execute(array(v1_stable_id('observation',$event['event_id'].'|'.$documentId.'|'.$sourceKey),
                    $event['event_id'],$documentId,$sourceType,$sourceKey,$observationAt,$observationAt,(string)$doc['content_hash'],
                    json_value(array('snapshot_id'=>$snapshotId,'identity_status'=>'complete','review_status'=>'pending')),$now,$now));
            }
        }

        $linkLookup = $pdo->prepare('SELECT link_id,entity_id FROM ' . table_name($config,'official_site_identity_links')
            . ' WHERE connector_id=? AND entity_type=? AND external_id=? AND active=1 FOR UPDATE');
        $linkRetire = $pdo->prepare('UPDATE ' . table_name($config,'official_site_identity_links')
            . ' SET active=0,retired_at=?,updated_at=? WHERE link_id=? AND active=1');
        $linkInsert = $pdo->prepare('INSERT INTO ' . table_name($config,'official_site_identity_links')
            . ' (link_id,connector_id,source_right_id,entity_type,external_id,entity_id,snapshot_id,active,retired_at,created_at,updated_at)'
            . ' VALUES (?,?,?,?,?,?,?,1,NULL,?,?)');
        $identityRows = array();
        foreach ($normalizedDocuments as $row) { $identityRows[] = array('document',$row['external_id'],$row['document_id']); }
        foreach ($normalizedEvents as $row) { $identityRows[] = array('event',$row['external_id'],$row['event_id']); }
        foreach ($identityRows as $identity) {
            $linkLookup->execute(array($connectorId,$identity[0],$identity[1])); $active = $linkLookup->fetch();
            if ($active && (string)$active['entity_id'] === $identity[2]) { continue; }
            if ($active) { $linkRetire->execute(array($collectedAt,$now,(string)$active['link_id'])); }
            $linkInsert->execute(array(v1_stable_id('site-link',$snapshotId.'|'.$identity[0].'|'.$identity[1].'|'.$identity[2]),
                $connectorId,$sourceRightId,$identity[0],$identity[1],$identity[2],$snapshotId,$now,$now));
        }

        $reviewExisting = $pdo->prepare('SELECT connector_id,entity_type,entity_id,reason,payload_json FROM '
            . table_name($config,'official_site_review_items') . ' WHERE review_item_id=? FOR UPDATE');
        $reviewStmt = $pdo->prepare('INSERT INTO ' . table_name($config,'official_site_review_items')
            . ' (review_item_id,snapshot_id,connector_id,entity_type,entity_id,reason,payload_json,review_status,reviewed_by,reviewed_at,created_at,updated_at)'
            . ' VALUES (?,?,?,?,?,?,?,\'pending\',NULL,NULL,?,?)');
        $reviewRefresh = $pdo->prepare('UPDATE ' . table_name($config,'official_site_review_items')
            . ' SET snapshot_id=?,payload_json=?,updated_at=? WHERE review_item_id=?');
        foreach ($normalizedReviews as $item) {
            $reviewExisting->execute(array($item['review_item_id'])); $existing = $reviewExisting->fetch();
            if ($existing) {
                $storedPayload = json_decode((string)$existing['payload_json'],true);
                $storedSemantic = is_array($storedPayload) ? v1_official_site_review_semantic_payload($storedPayload) : null;
                $incomingSemantic = v1_official_site_review_semantic_payload($item['payload']);
                if ((string)$existing['connector_id'] !== $connectorId || (string)$existing['entity_type'] !== $item['entity_type']
                    || (string)$existing['entity_id'] !== $item['entity_id'] || (string)$existing['reason'] !== $item['reason']
                    || $storedSemantic === null || !hash_equals(
                        hash('sha256',v1_strict_canonical_json_encode($storedSemantic,'official_site_review_encode_failed')),
                        hash('sha256',v1_strict_canonical_json_encode($incomingSemantic,'official_site_review_encode_failed')))) {
                    throw new RuntimeException('official_site_review_idempotency_conflict');
                }
                $reviewRefresh->execute(array($snapshotId,json_value($item['payload']),$now,$item['review_item_id']));
            } else {
                $reviewStmt->execute(array($item['review_item_id'],$snapshotId,$connectorId,$item['entity_type'],$item['entity_id'],
                    $item['reason'],json_value($item['payload']),$now,$now));
            }
        }
        $tombstoneExisting = $pdo->prepare('SELECT connector_id,entity_type,external_id,entity_id,reason,observed_at,payload_json FROM '
            . table_name($config,'official_site_tombstones') . ' WHERE tombstone_id=? FOR UPDATE');
        $tombstoneStmt = $pdo->prepare('INSERT INTO ' . table_name($config,'official_site_tombstones')
            . ' (tombstone_id,snapshot_id,connector_id,entity_type,external_id,entity_id,reason,observed_at,payload_json,review_status,'
            . 'reviewed_by,reviewed_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,\'pending\',NULL,NULL,?,?)');
        $tombstoneRefresh = $pdo->prepare('UPDATE ' . table_name($config,'official_site_tombstones')
            . ' SET snapshot_id=?,payload_json=?,updated_at=? WHERE tombstone_id=?');
        foreach ($normalizedTombstones as $item) {
            $tombstoneExisting->execute(array($item['tombstone_id'])); $existing = $tombstoneExisting->fetch();
            if ($existing) {
                $storedPayload = json_decode((string)$existing['payload_json'],true);
                $storedHash = is_array($storedPayload)
                    ? hash('sha256',v1_strict_canonical_json_encode($storedPayload,'official_site_tombstone_encode_failed')) : null;
                $incomingHash = hash('sha256',v1_strict_canonical_json_encode($item['payload'],'official_site_tombstone_encode_failed'));
                if ((string)$existing['connector_id'] !== $connectorId || (string)$existing['entity_type'] !== $item['entity_type']
                    || (string)$existing['external_id'] !== $item['external_id'] || (string)($existing['entity_id'] ?? '') !== (string)($item['entity_id'] ?? '')
                    || (string)$existing['reason'] !== $item['reason'] || (string)$existing['observed_at'] !== $item['observed_at']
                    || $storedHash === null || !hash_equals($storedHash,$incomingHash)) {
                    throw new RuntimeException('official_site_tombstone_idempotency_conflict');
                }
                $tombstoneRefresh->execute(array($snapshotId,json_value($item['payload']),$now,$item['tombstone_id']));
            } else {
                $tombstoneStmt->execute(array($item['tombstone_id'],$snapshotId,$connectorId,$item['entity_type'],$item['external_id'],
                    $item['entity_id'],$item['reason'],$item['observed_at'],json_value($item['payload']),$now,$now));
            }
        }

        $snapshotStmt = $pdo->prepare('INSERT INTO ' . table_name($config,'official_site_snapshots')
            . ' (snapshot_id,receipt_sha256,request_sha256,manifest_sha256,connector_id,source_right_id,source_type,source_key,connector_receipt_json,'
            . 'code_revision,collected_at,accepted_json,status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,\'succeeded\',?,?)');
        $snapshotStmt->execute(array($snapshotId,$receiptSha,$requestHash,$manifestSha,$connectorId,$sourceRightId,$sourceType,$sourceKey,
            v1_strict_canonical_json_encode($connector,'official_site_receipt_encode_failed'),$revision,$collectedAt,json_value($accepted),$now,$now));
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        if (strpos($e->getMessage(),'official_site_') === 0 || (string)$e->getCode() === '23000') {
            respond(409,array('ok'=>false,'error'=>$e->getMessage()));
        }
        throw $e;
    }
    respond(200,array('ok'=>true,'snapshot_id'=>$snapshotId,'receipt_sha256'=>$receiptSha,
        'accepted'=>$accepted,'rejected'=>0,'idempotent'=>$idempotent));
}

/**
 * HMAC action contracts: ?action=upsert_governance_snapshot for non-DART
 * writes and ?action=upsert_governance_snapshot_dart_guarded for DART.
 * payload={companies,documents,events,source_rights,run,
 * expected_source_right_revisions,expected_deployment_code_revision,
 * expected_backend_binding_id}.
 */
function v1_official_completion_semantic_sha(array $run): string {
    $semantic = $run;
    foreach (array('github_run_attempt','started_at','finished_at','first_observed_at','metrics') as $volatile) {
        unset($semantic[$volatile]);
    }
    if (isset($semantic['source_outcomes']) && is_array($semantic['source_outcomes'])) {
        foreach ($semantic['source_outcomes'] as &$outcome) {
            if (is_array($outcome)) { unset($outcome['elapsed_ms']); }
        }
        unset($outcome);
    }
    return hash('sha256',v1_strict_canonical_json_encode($semantic,'scheduled_slot_completion_encode_failed'));
}

function v1_lock_official_slot_claim_for_run(PDO $pdo, array $config, array $run, string $runId,
    string $pipeline, ?string $codeRevision): ?array {
    $runKind = v1_official_run_metric($run,'run_kind');
    $claimId = v1_official_run_metric($run,'slot_claim_id');
    if ($runKind !== 'scheduled_incremental') {
        if ($claimId !== null && $claimId !== '') {
            throw new RuntimeException('non_scheduled_run_has_slot_claim:' . $runId);
        }
        return null;
    }
    $schedule = v1_official_run_metric($run,'event_schedule');
    $slot = v1_editorial_datetime_utc(v1_official_run_metric($run,'scheduled_slot_at'));
    $trigger = v1_editorial_datetime_utc(v1_official_run_metric($run,'trigger_created_at'));
    $claimedAt = v1_editorial_datetime_utc(v1_official_run_metric($run,'slot_claimed_at'));
    $nextSlot = v1_editorial_datetime_utc(v1_official_run_metric($run,'next_cadence_slot_at'));
    $githubRunId = v1_official_run_metric($run,'github_run_id');
    $githubAttempt = v1_official_run_metric($run,'github_run_attempt');
    $triggerLag = v1_official_run_metric($run,'trigger_lag_seconds');
    $claimLag = v1_official_run_metric($run,'claim_lag_seconds');
    $late = v1_official_run_metric($run,'slot_claim_late');
    if (!is_string($claimId) || !v1_valid_entity_id($claimId) || !is_string($schedule)
        || !is_string($githubRunId) || preg_match('/^[0-9]{1,64}$/',$githubRunId) !== 1
        || !is_int($githubAttempt) || $githubAttempt < 1 || !is_int($triggerLag) || $triggerLag < 0
        || !is_int($claimLag) || $claimLag < 0 || !is_bool($late)
        || $slot === null || $trigger === null || $claimedAt === null || $nextSlot === null
        || !v1_official_schedule_slot_matches($schedule,$slot) || $codeRevision === null) {
        throw new RuntimeException('invalid_scheduled_slot_claim_provenance:' . $runId);
    }
    $stmt = $pdo->prepare('SELECT * FROM ' . table_name($config,'official_slot_claims') . ' WHERE claim_id=? FOR UPDATE');
    $claim = v1_pdo_fetch_one_and_close($stmt,array($claimId));
    if (!$claim
        || (string)$claim['pipeline'] !== $pipeline
        || (string)$claim['event_schedule'] !== $schedule
        || (string)$claim['scheduled_slot_at'] !== $slot
        || (string)$claim['trigger_created_at'] !== $trigger
        || (string)$claim['claimed_at'] !== $claimedAt
        || (string)$claim['next_cadence_slot_at'] !== $nextSlot
        || (string)$claim['github_run_id'] !== $githubRunId
        || ((string)$claim['status'] === 'completed'
            ? $githubAttempt < (int)$claim['github_run_attempt']
            : $githubAttempt !== (int)$claim['github_run_attempt'])
        || (int)$claim['trigger_lag_seconds'] !== $triggerLag
        || (int)$claim['claim_lag_seconds'] !== $claimLag
        || ((int)$claim['late'] === 1) !== $late
        || !hash_equals((string)$claim['code_revision'],$codeRevision)
        || !in_array((string)$claim['status'],array('claimed','failed','completed'),true)
        || ((string)$claim['status'] === 'completed' && (string)$claim['completed_run_id'] !== $runId)) {
        throw new RuntimeException('scheduled_slot_claim_conflict:' . $runId);
    }
    return $claim;
}

/** Map only caller-controlled event identity conflicts to stable HTTP 409 codes. */
function v1_governance_snapshot_identity_conflict_code(Throwable $error): ?string {
    $message = $error->getMessage();
    foreach (array(
        'followup_event_identity_conflict',
        'invalid_complete_event_identity',
        'incomplete_event_identity_has_comparison_key',
        'event_identity_scope_conflict',
        'event_identity_field_conflict',
    ) as $code) {
        if ($message === $code || strpos($message,$code . ':') === 0) { return $code; }
    }
    return null;
}

/**
 * Return whether OpenDART added only its monotonic "later correction exists"
 * marker to a previously stored list row.
 *
 * DART's `rm` field is mutable: after a later correction is filed, an older
 * receipt can gain the `정` marker without changing the receipt itself.  This
 * must not weaken any event identity, ownership or document-lineage check.
 */
function v1_dart_later_correction_marker_added(array $stored, array $submitted): bool {
    if (!array_key_exists('has_later_correction',$stored)
        || !array_key_exists('has_later_correction',$submitted)
        || $stored['has_later_correction'] !== false
        || $submitted['has_later_correction'] !== true) {
        return false;
    }
    if (!array_key_exists('is_withdrawn_by_remark',$stored)
        || !array_key_exists('is_withdrawn_by_remark',$submitted)
        || !is_bool($stored['is_withdrawn_by_remark'])
        || !is_bool($submitted['is_withdrawn_by_remark'])
        || $stored['is_withdrawn_by_remark']
            !== $submitted['is_withdrawn_by_remark']) {
        return false;
    }
    $storedRemarks = (string)($stored['remarks'] ?? '');
    $submittedRemarks = (string)($submitted['remarks'] ?? '');
    if (strpos($storedRemarks,'철') !== false
        || strpos($submittedRemarks,'철') !== false) {
        return false;
    }
    $markerCount = 0;
    $withoutLaterCorrectionMarker = str_replace(
        '정',
        '',
        $submittedRemarks,
        $markerCount
    );
    return $markerCount === 1
        && $withoutLaterCorrectionMarker === $storedRemarks;
}

/**
 * Compare an isolated event replay while permitting only the DART marker
 * upgrade whose matching evidence document is checked separately.
 */
function v1_followup_event_replay_payload_matches(
    array $stored,
    array $submitted,
    bool $allowDartMarkerUpgrade,
    &$dartMarkerUpgrade
): bool {
    $dartMarkerUpgrade = false;
    $storedHash = hash('sha256',v1_strict_canonical_json_encode(
        $stored,
        'stored_followup_event_payload_encode_failed'
    ));
    $submittedHash = hash('sha256',v1_strict_canonical_json_encode(
        $submitted,
        'submitted_followup_event_payload_encode_failed'
    ));
    if (hash_equals($storedHash,$submittedHash)) {
        return true;
    }
    if (!$allowDartMarkerUpgrade
        || !array_key_exists('has_later_correction',$stored)
        || !array_key_exists('has_later_correction',$submitted)
        || $stored['has_later_correction'] !== false
        || $submitted['has_later_correction'] !== true) {
        return false;
    }
    $normalizedSubmitted = $submitted;
    $normalizedSubmitted['has_later_correction'] =
        $stored['has_later_correction'];
    $normalizedHash = hash('sha256',v1_strict_canonical_json_encode(
        $normalizedSubmitted,
        'normalized_followup_event_payload_encode_failed'
    ));
    if (!hash_equals($storedHash,$normalizedHash)) {
        return false;
    }
    $dartMarkerUpgrade = true;
    return true;
}

/**
 * Compare an isolated DART document replay and identify the same monotonic
 * marker upgrade. The two content hashes must be exactly derivable from the
 * immutable title/URL plus their respective DART remarks.
 */
function v1_followup_document_replay_payload_matches(
    array $stored,
    array $submitted,
    bool $allowDartMarkerUpgrade,
    &$dartMarkerUpgrade
): bool {
    $dartMarkerUpgrade = false;
    $storedHash = hash('sha256',v1_strict_canonical_json_encode(
        $stored,
        'stored_followup_document_payload_encode_failed'
    ));
    $submittedHash = hash('sha256',v1_strict_canonical_json_encode(
        $submitted,
        'submitted_followup_document_payload_encode_failed'
    ));
    if (hash_equals($storedHash,$submittedHash)) {
        return true;
    }
    if (!$allowDartMarkerUpgrade
        || !v1_dart_later_correction_marker_added($stored,$submitted)) {
        return false;
    }
    $storedRemarks = (string)($stored['remarks'] ?? '');
    $submittedRemarks = (string)($submitted['remarks'] ?? '');
    $title = (string)($stored['title'] ?? '');
    $url = (string)($stored['original_url'] ?? '');
    $storedContentHash = strtolower((string)($stored['content_hash'] ?? ''));
    $submittedContentHash = strtolower((string)($submitted['content_hash'] ?? ''));
    if (preg_match('/^[a-f0-9]{64}$/',$storedContentHash) !== 1
        || preg_match('/^[a-f0-9]{64}$/',$submittedContentHash) !== 1
        || !hash_equals(
            hash('sha256',$title . "\n" . $url . "\n" . $storedRemarks),
            $storedContentHash
        )
        || !hash_equals(
            hash('sha256',$title . "\n" . $url . "\n" . $submittedRemarks),
            $submittedContentHash
        )) {
        return false;
    }
    $normalizedSubmitted = $submitted;
    $normalizedSubmitted['has_later_correction'] =
        $stored['has_later_correction'];
    $normalizedSubmitted['remarks'] = $storedRemarks;
    $normalizedSubmitted['content_hash'] = $storedContentHash;
    $normalizedHash = hash('sha256',v1_strict_canonical_json_encode(
        $normalizedSubmitted,
        'normalized_followup_document_payload_encode_failed'
    ));
    if (!hash_equals($storedHash,$normalizedHash)) {
        return false;
    }
    $dartMarkerUpgrade = true;
    return true;
}

/**
 * Match an append-only DART lifecycle row while deliberately retaining the
 * first server observation timestamp on exact replays.
 */
function v1_dart_lifecycle_observation_matches(
    array $stored,
    array $submitted
): bool {
    $storedMetadata = json_decode((string)($stored['payload_json'] ?? ''),true);
    if (!is_array($storedMetadata)) {
        return false;
    }
    $storedParent = $stored['parent_external_id'] === null
        ? null : (string)$stored['parent_external_id'];
    $submittedParent = $submitted['parent_external_id'] === null
        ? null : (string)$submitted['parent_external_id'];
    if ((string)($stored['connector_id'] ?? '') !== 'connector:kr:dart'
        || (string)($stored['country_code'] ?? '') !== 'KR'
        || (string)($stored['source_key'] ?? '') !== 'dart'
        || (string)($stored['external_id'] ?? '')
            !== (string)$submitted['external_id']
        || $storedParent !== $submittedParent
        || (string)($stored['change_type'] ?? '') !== 'updated'
        || (string)($stored['resolution_status'] ?? '') !== 'resolved'
        || (string)($stored['resolved_document_id'] ?? '')
            !== (string)$submitted['document_id']
        || (string)($stored['resolved_event_id'] ?? '')
            !== (string)$submitted['event_id']) {
        return false;
    }
    return hash_equals(
        hash('sha256',v1_strict_canonical_json_encode(
            $storedMetadata,
            'stored_dart_lifecycle_payload_encode_failed'
        )),
        hash('sha256',v1_strict_canonical_json_encode(
            $submitted['metadata'],
            'submitted_dart_lifecycle_payload_encode_failed'
        ))
    );
}

/**
 * Identify reviewed DART rows that must never re-enter the generic source
 * upsert path. The source payload, evidence and canonical identity are checked
 * separately before an exact replay can be acknowledged.
 */
function v1_dart_reviewed_event_is_protected(
    array $storedEvent
): bool {
    $companyId = trim((string)($storedEvent['company_id'] ?? ''));
    return preg_match('/^[0-9]{8}$/',$companyId) === 1
        && (string)($storedEvent['issuer_id'] ?? '')
            === 'issuer:kr:dart:' . $companyId
        && (string)($storedEvent['country_code'] ?? '') === 'KR'
        && (string)($storedEvent['identity_status'] ?? '') === 'complete'
        && (string)($storedEvent['review_status'] ?? '') === 'approved'
        && (string)($storedEvent['publication_status'] ?? '') === 'published';
}

/**
 * Identify a DART event whose human rejection is final for this source
 * snapshot.  A rejected event is deliberately not a canonicalized reviewed
 * event: its first-seen source payload remains the comparison basis and its
 * rejected/draft lifecycle must never re-enter the generic upsert path.
 */
function v1_dart_rejected_event_is_protected(
    array $storedEvent
): bool {
    $companyId = trim((string)($storedEvent['company_id'] ?? ''));
    return preg_match('/^[0-9]{8}$/',$companyId) === 1
        && (string)($storedEvent['issuer_id'] ?? '')
            === 'issuer:kr:dart:' . $companyId
        && (string)($storedEvent['country_code'] ?? '') === 'KR'
        && (string)($storedEvent['identity_status'] ?? '') === 'rejected'
        && (string)($storedEvent['review_status'] ?? '') === 'rejected'
        && (string)($storedEvent['publication_status'] ?? '') === 'draft'
        && trim((string)($storedEvent['comparison_key'] ?? '')) === '';
}

/**
 * Prove an exact source replay of a human-rejected DART event.  Unlike the
 * approved-event matcher below, this compares the immutable raw source event
 * and never treats a rejected row as a canonicalized Production Alpha event.
 */
function v1_dart_rejected_event_replay(
    array $storedEvent,
    array $storedPayload,
    array $submittedEvent
): ?array {
    if (!v1_dart_rejected_event_is_protected($storedEvent)
        || !empty($storedPayload['is_cancelled'])
        || !empty($submittedEvent['is_cancelled'])) {
        return null;
    }
    $isCorrection = !empty($storedPayload['is_correction']);
    if ((!empty($submittedEvent['is_correction'])) !== $isCorrection) {
        return null;
    }
    $canonicalSubmittedEvent = $submittedEvent;
    if ($isCorrection) {
        if ((string)($storedPayload['event_link_status'] ?? '')
                !== 'ambiguous_independent'
            || (array_key_exists('event_link_status',$canonicalSubmittedEvent)
                && (string)$canonicalSubmittedEvent['event_link_status']
                    !== 'ambiguous_independent')) {
            return null;
        }
        $canonicalSubmittedEvent['event_link_status'] =
            'ambiguous_independent';
    } elseif (array_key_exists('event_link_status',$storedPayload)
        || array_key_exists('event_link_status',$canonicalSubmittedEvent)) {
        return null;
    }
    foreach (array(&$storedPayload,&$canonicalSubmittedEvent) as &$eventPayload) {
        if (isset($eventPayload['metadata'])
            && !is_array($eventPayload['metadata'])) {
            return null;
        }
        if (!isset($eventPayload['metadata'])) {
            $eventPayload['metadata'] = array();
        }
        $titleProvenance = trim((string)(
            $eventPayload['metadata']['title_provenance'] ?? ''
        ));
        if ($titleProvenance !== '' && $titleProvenance !== 'source') {
            return null;
        }
        $eventPayload['metadata']['title_provenance'] = 'source';
    }
    unset($eventPayload);
    $markerUpgrade = false;
    if (!v1_followup_event_replay_payload_matches(
            $storedPayload,
            $canonicalSubmittedEvent,
            false,
            $markerUpgrade
        ) || $markerUpgrade) {
        return null;
    }

    $eventId = trim((string)($storedEvent['event_id'] ?? ''));
    $companyId = trim((string)($storedEvent['company_id'] ?? ''));
    $submittedEventId = trim((string)v1_first(
        $submittedEvent,
        array('event_id'),
        ''
    ));
    $submittedCompanyId = trim((string)v1_first(
        $submittedEvent,
        array('company_id','corp_code'),
        ''
    ));
    $eventType = trim((string)v1_first(
        $submittedEvent,
        array('event_type'),
        ''
    ));
    $title = trim((string)v1_first(
        $submittedEvent,
        array('title','action'),
        ''
    ));
    $language = v1_language(v1_first(
        $submittedEvent,
        array('original_language','language'),
        'ko'
    ),'ko');
    $summary = (string)v1_first(
        $submittedEvent,
        array('summary','target'),
        ''
    ) ?: null;
    $occurredAt = mysql_dt(v1_first(
        $submittedEvent,
        array('occurred_at','occurred_on'),
        null
    ));
    $deadlineAt = mysql_dt(v1_first(
        $submittedEvent,
        array('deadline_at','deadline'),
        null
    ));
    $importance = trim((string)v1_first(
        $submittedEvent,
        array('importance'),
        'medium'
    ));
    if ($importance === 'normal') { $importance = 'medium'; }
    if (!in_array(
        $importance,
        array('low','medium','high','market_sensitive','critical'),
        true
    )) {
        $importance = 'medium';
    }
    $identityAction = trim((string)v1_first(
        $submittedEvent,
        array('identity_action'),
        ''
    ));
    $identityTarget = trim((string)v1_first(
        $submittedEvent,
        array('identity_target'),
        ''
    ));
    $identityActorId = trim((string)v1_first(
        $submittedEvent,
        array('identity_actor_id','actor_id'),
        ''
    ));
    $identityEffectiveAt = mysql_dt(v1_first(
        $submittedEvent,
        array('identity_effective_at'),
        null
    ));
    $identityDeadlineAt = mysql_dt(v1_first(
        $submittedEvent,
        array('identity_deadline_at'),
        null
    ));
    $identityStatus = trim((string)v1_first(
        $submittedEvent,
        array('identity_status'),
        'needs_review'
    ));
    $comparisonKey = trim((string)v1_first(
        $submittedEvent,
        array('comparison_key'),
        ''
    ));
    $expectedFamily = v1_global_event_family_for_legacy_type($eventType);
    $expectedVerification = $isCorrection ? 'corrected' : 'official';
    $storedFields = array(
        (string)($storedEvent['event_type'] ?? ''),
        (string)($storedEvent['title'] ?? ''),
        (string)($storedEvent['original_language'] ?? ''),
        (string)($storedEvent['summary'] ?? ''),
        (string)($storedEvent['occurred_at'] ?? ''),
        (string)($storedEvent['deadline_at'] ?? ''),
        (string)($storedEvent['importance'] ?? ''),
        (string)($storedEvent['verification_status'] ?? ''),
        (string)($storedEvent['collection_key'] ?? ''),
        (string)($storedEvent['identity_action'] ?? ''),
        (string)($storedEvent['identity_target'] ?? ''),
        (string)($storedEvent['identity_actor_id'] ?? ''),
        (string)($storedEvent['identity_effective_at'] ?? ''),
        (string)($storedEvent['identity_deadline_at'] ?? ''),
    );
    $submittedFields = array(
        $eventType,
        mb_substr($title,0,700,'UTF-8'),
        $language,
        (string)$summary,
        (string)($occurredAt ?? ''),
        (string)($deadlineAt ?? ''),
        $importance,
        $expectedVerification,
        mb_substr(trim((string)v1_first(
            $submittedEvent,
            array('collection_key'),
            ''
        )),0,96,'UTF-8'),
        $identityAction,
        $identityTarget,
        $identityActorId,
        (string)($identityEffectiveAt ?? ''),
        (string)($identityDeadlineAt ?? ''),
    );
    $actor = isset($submittedEvent['actor'])
        && is_array($submittedEvent['actor'])
        ? $submittedEvent['actor'] : null;
    $eventActor = isset($submittedEvent['event_actor'])
        && is_array($submittedEvent['event_actor'])
        ? $submittedEvent['event_actor'] : null;
    $submittedActorCountryCode = $actor === null
        ? '' : trim((string)($actor['country_code'] ?? ''));
    if ($eventId !== $submittedEventId
        || $companyId !== $submittedCompanyId
        || $eventType === ''
        || $title === ''
        || $occurredAt === null
        || $expectedFamily === null
        || (string)($storedEvent['global_event_family'] ?? '')
            !== $expectedFamily
        || trim((string)v1_first(
            $submittedEvent,
            array('verification_status','status'),
            ''
        )) !== 'official'
        || $identityStatus !== 'needs_review'
        || $comparisonKey !== ''
        || !v1_valid_entity_id($identityActorId,64)
        || $storedFields !== $submittedFields
        || $actor === null
        || $eventActor === null
        || (string)($actor['actor_id'] ?? '') !== $identityActorId
        || (string)($eventActor['actor_id'] ?? '') !== $identityActorId
        || (string)($eventActor['event_id'] ?? '') !== $eventId
        || (string)($eventActor['actor_role'] ?? '') !== 'filer'
        || !in_array(
            (string)($actor['actor_type'] ?? ''),
            array('company','institution'),
            true
        )
        || trim((string)($actor['display_name'] ?? '')) === ''
        || ($submittedActorCountryCode !== ''
            && $submittedActorCountryCode !== 'KR')
        || (string)($actor['review_status'] ?? '') !== 'pending'
        || (string)($actor['record_status'] ?? '') !== 'inactive'
        || (string)($eventActor['review_status'] ?? '') !== 'pending') {
        return null;
    }
    $actorCompanyId = trim((string)($actor['company_id'] ?? ''));
    if (((string)$actor['actor_type'] === 'company'
            && $actorCompanyId !== $companyId)
        || ((string)$actor['actor_type'] === 'institution'
            && $actorCompanyId !== '')) {
        return null;
    }
    return array(
        'canonical_actor_id'=>$identityActorId,
        'is_correction'=>$isCorrection,
        'actor_type'=>(string)$actor['actor_type'],
        'actor_display_name'=>(string)$actor['display_name'],
        'actor_company_id'=>$actorCompanyId,
    );
}

/**
 * Recompute the Production Alpha identity written by the v2 editorial review
 * endpoint. This prevents a merely labelled "complete" row from entering the
 * reviewed DART replay exception.
 */
function v1_dart_reviewed_event_canonical_identity(
    array $storedEvent,
    bool $isCorrection
): ?array {
    $eventId = trim((string)($storedEvent['event_id'] ?? ''));
    $companyId = trim((string)($storedEvent['company_id'] ?? ''));
    $issuerId = trim((string)($storedEvent['issuer_id'] ?? ''));
    $countryCode = trim((string)($storedEvent['country_code'] ?? ''));
    $eventFamily = trim((string)($storedEvent['global_event_family'] ?? ''));
    $eventType = trim((string)($storedEvent['event_type'] ?? ''));
    $action = v1_normalize_identity_text(
        (string)($storedEvent['identity_action'] ?? '')
    );
    $target = v1_normalize_identity_text(
        (string)($storedEvent['identity_target'] ?? '')
    );
    $actorId = v1_normalize_identity_text(
        (string)($storedEvent['identity_actor_id'] ?? '')
    );
    $effectiveAt = trim((string)($storedEvent['identity_effective_at'] ?? ''));
    $deadlineAt = ($storedEvent['identity_deadline_at'] ?? null) === null
        ? null : trim((string)$storedEvent['identity_deadline_at']);
    $eventDeadlineAt = ($storedEvent['deadline_at'] ?? null) === null
        ? null : trim((string)$storedEvent['deadline_at']);
    $comparisonKey = trim((string)($storedEvent['comparison_key'] ?? ''));
    $families = array(
        'large_ownership',
        'meeting_and_vote',
        'tender_offer_and_mna',
        'capital_issuance',
        'capital_return',
        'board_and_compensation',
        'listing_status',
        'correction_and_withdrawal',
    );
    if (!v1_valid_entity_id($eventId)
        || preg_match('/^[0-9]{8}$/',$companyId) !== 1
        || $issuerId !== 'issuer:kr:dart:' . $companyId
        || $countryCode !== 'KR'
        || !in_array($eventFamily,$families,true)
        || $eventType !== $eventFamily
        || $action === ''
        || $target === ''
        || !v1_valid_entity_id($actorId,64)
        || preg_match('/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/',$effectiveAt)
            !== 1
        || ($deadlineAt !== null
            && preg_match(
                '/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/',
                $deadlineAt
            ) !== 1)
        || $eventDeadlineAt !== $deadlineAt
        || (string)($storedEvent['verification_status'] ?? '')
            !== ($isCorrection ? 'corrected' : 'official')
        || trim((string)($storedEvent['title'] ?? '')) === '') {
        return null;
    }
    $identity = array(
        'issuer_id'=>$issuerId,
        'event_family'=>$eventFamily,
        'action'=>$action,
        'target'=>$target,
        'actor_id'=>$actorId,
        'effective_at'=>$effectiveAt,
        'deadline_at'=>$deadlineAt,
    );
    $expectedComparisonKey = 'global:' . substr(
        hash(
            'sha256',
            v1_strict_canonical_json_encode(
                $identity,
                'reviewed_dart_event_identity_encode_failed'
            )
        ),
        0,
        64
    );
    if (!hash_equals($expectedComparisonKey,$comparisonKey)) {
        return null;
    }
    $identity['comparison_key'] = $comparisonKey;
    return $identity;
}

/**
 * Match the exact first-seen source event behind an editorially canonicalized
 * DART event. Editorial event fields may differ from that raw payload, but the
 * source payload, actor and deadlines may not change. Corrections normalize
 * only the server-owned isolation marker; cancellations are never eligible.
 */
function v1_dart_reviewed_event_replay(
    array $storedEvent,
    array $storedPayload,
    array $submittedEvent
): ?array {
    if (!empty($storedPayload['is_cancelled'])) {
        return null;
    }
    $isCorrection = !empty($storedPayload['is_correction']);
    if ($isCorrection) {
        if ((string)($storedPayload['event_link_status'] ?? '')
            !== 'ambiguous_independent') {
            return null;
        }
    } elseif (array_key_exists('event_link_status',$storedPayload)) {
        return null;
    }
    $canonicalIdentity = v1_dart_reviewed_event_canonical_identity(
        $storedEvent,
        $isCorrection
    );
    $eventId = trim((string)($storedEvent['event_id'] ?? ''));
    $companyId = trim((string)($storedEvent['company_id'] ?? ''));
    $submittedEventId = trim((string)v1_first(
        $submittedEvent,
        array('event_id'),
        ''
    ));
    $submittedCompanyId = trim((string)v1_first(
        $submittedEvent,
        array('company_id','corp_code'),
        ''
    ));
    $submittedActorId = v1_normalize_identity_text((string)v1_first(
        $submittedEvent,
        array('identity_actor_id','actor_id'),
        ''
    ));
    $submittedDeadlineAt = mysql_dt(v1_first(
        $submittedEvent,
        array('deadline_at','deadline'),
        null
    ));
    $submittedIdentityDeadlineAt = mysql_dt(v1_first(
        $submittedEvent,
        array('identity_deadline_at'),
        null
    ));
    $canonicalActorId = $canonicalIdentity === null
        ? '' : (string)$canonicalIdentity['actor_id'];
    $canonicalDeadlineAt = $canonicalIdentity === null
        ? null : $canonicalIdentity['deadline_at'];
    $actor = isset($submittedEvent['actor'])
        && is_array($submittedEvent['actor'])
        ? $submittedEvent['actor'] : null;
    $eventActor = isset($submittedEvent['event_actor'])
        && is_array($submittedEvent['event_actor'])
        ? $submittedEvent['event_actor'] : null;
    if ($canonicalIdentity === null
        || $eventId !== $submittedEventId
        || $companyId !== $submittedCompanyId
        || (!empty($submittedEvent['is_correction'])) !== $isCorrection
        || !empty($submittedEvent['is_cancelled'])
        || trim((string)v1_first(
            $submittedEvent,
            array('verification_status','status'),
            ''
        )) !== 'official'
        || $submittedActorId === ''
        || !hash_equals($canonicalActorId,$submittedActorId)
        || $submittedDeadlineAt !== $canonicalDeadlineAt
        || $submittedIdentityDeadlineAt !== $canonicalDeadlineAt
        || $actor === null
        || $eventActor === null
        || v1_normalize_identity_text(
            (string)($actor['actor_id'] ?? '')
        ) !== $canonicalActorId
        || v1_normalize_identity_text(
            (string)($eventActor['actor_id'] ?? '')
        ) !== $canonicalActorId
        || (string)($eventActor['event_id'] ?? '') !== $eventId
        || (string)($eventActor['actor_role'] ?? '') !== 'filer') {
        return null;
    }
    $canonicalSubmittedEvent = $submittedEvent;
    if ($isCorrection) {
        if (array_key_exists(
                'event_link_status',
                $canonicalSubmittedEvent
            )
            && (string)$canonicalSubmittedEvent['event_link_status']
                !== 'ambiguous_independent') {
            return null;
        }
        $canonicalSubmittedEvent['event_link_status'] =
            'ambiguous_independent';
    } elseif (array_key_exists(
        'event_link_status',
        $canonicalSubmittedEvent
    )) {
        return null;
    }
    if (isset($storedPayload['metadata'])
        && is_array($storedPayload['metadata'])
        && (string)($storedPayload['metadata']['title_provenance'] ?? '')
            === 'source') {
        if (!isset($canonicalSubmittedEvent['metadata'])) {
            $canonicalSubmittedEvent['metadata'] = array();
        }
        if (!is_array($canonicalSubmittedEvent['metadata'])) {
            return null;
        }
        if (!isset(
            $canonicalSubmittedEvent['metadata']['title_provenance']
        )) {
            $canonicalSubmittedEvent['metadata']['title_provenance'] =
                'source';
        }
    }
    $dartMarkerUpgrade = false;
    if (!v1_followup_event_replay_payload_matches(
            $storedPayload,
            $canonicalSubmittedEvent,
            false,
            $dartMarkerUpgrade
        )
        || $dartMarkerUpgrade) {
        return null;
    }
    return array(
        'canonical_actor_id'=>$canonicalActorId,
        'is_correction'=>$isCorrection,
    );
}

/**
 * Validate the pending filer candidate that accompanies an incomplete DART
 * identity. It is internal review evidence, not an approved canonical actor.
 */
function v1_dart_pending_filer_candidate_matches(
    array $payload,
    string $eventId,
    string $companyId,
    string $identityActorId
): bool {
    $actor = isset($payload['actor']) && is_array($payload['actor'])
        ? $payload['actor'] : null;
    $eventActor = isset($payload['event_actor']) && is_array($payload['event_actor'])
        ? $payload['event_actor'] : null;
    if ($actor === null || $eventActor === null
        || (string)($payload['actor_id'] ?? '') !== $identityActorId
        || (string)($payload['identity_actor_id'] ?? '') !== $identityActorId
        || (string)($actor['actor_id'] ?? '') !== $identityActorId
        || (string)($eventActor['actor_id'] ?? '') !== $identityActorId
        || (string)($eventActor['event_id'] ?? '') !== $eventId
        || (string)($eventActor['actor_role'] ?? '') !== 'filer'
        || (string)($eventActor['review_status'] ?? '') !== 'pending'
        || (string)($actor['review_status'] ?? '') !== 'pending'
        || (string)($actor['record_status'] ?? '') !== 'inactive'
        || !in_array((string)($actor['actor_type'] ?? ''),array('company','institution'),true)
        || trim((string)($actor['display_name'] ?? '')) === '') {
        return false;
    }
    $actorCompanyId = trim((string)($actor['company_id'] ?? ''));
    return (string)$actor['actor_type'] === 'company'
        ? $actorCompanyId === $companyId
        : $actorCompanyId === '';
}

/**
 * Detect only a mutable OpenDART filer identity on the same unpublished,
 * incomplete event. Every other source-derived event field must be identical.
 *
 * The returned values are hashes only; raw actor identifiers and names are
 * intentionally excluded from the append-only lifecycle metadata.
 */
function v1_dart_pending_identity_actor_change(
    array $storedEvent,
    array $storedPayload,
    array $submittedEvent
): ?array {
    $eventId = trim((string)($storedEvent['event_id'] ?? ''));
    $companyId = trim((string)($storedEvent['company_id'] ?? ''));
    $eventType = trim((string)($storedEvent['event_type'] ?? ''));
    $submittedEventId = trim((string)v1_first($submittedEvent,array('event_id'),''));
    $submittedCompanyId = trim((string)v1_first(
        $submittedEvent,
        array('company_id','corp_code'),
        ''
    ));
    $submittedEventType = trim((string)v1_first($submittedEvent,array('event_type'),''));
    $submittedIdentityStatus = trim((string)v1_first(
        $submittedEvent,
        array('identity_status'),
        'needs_review'
    ));
    if (!v1_valid_entity_id($eventId)
        || $eventId !== $submittedEventId
        || preg_match('/^[0-9]{8}$/',$companyId) !== 1
        || $companyId !== $submittedCompanyId
        || $eventType === ''
        || $eventType !== $submittedEventType
        || (string)($storedEvent['identity_status'] ?? '') !== 'needs_review'
        || $submittedIdentityStatus !== 'needs_review'
        || (string)($storedEvent['verification_status'] ?? '') !== 'official'
        || trim((string)v1_first(
            $submittedEvent,
            array('verification_status','status'),
            ''
        )) !== 'official'
        || (string)($storedEvent['review_status'] ?? '') !== 'pending'
        || (string)($storedEvent['publication_status'] ?? '') !== 'draft'
        || !empty($storedPayload['is_correction'])
        || !empty($storedPayload['is_cancelled'])
        || !empty($submittedEvent['is_correction'])
        || !empty($submittedEvent['is_cancelled'])
        || trim((string)($storedPayload['event_link_status'] ?? '')) !== ''
        || trim((string)($submittedEvent['event_link_status'] ?? '')) !== '') {
        return null;
    }

    $storedIdentity = array(
        'company_id'=>$companyId,
        'event_type'=>$eventType,
        'identity_action'=>trim((string)($storedEvent['identity_action'] ?? '')),
        'identity_target'=>trim((string)($storedEvent['identity_target'] ?? '')),
        'identity_actor_id'=>trim((string)($storedEvent['identity_actor_id'] ?? '')),
        'identity_effective_at'=>trim((string)($storedEvent['identity_effective_at'] ?? '')),
        'identity_deadline_at'=>trim((string)($storedEvent['identity_deadline_at'] ?? '')),
        'identity_status'=>'needs_review',
        'comparison_key'=>trim((string)($storedEvent['comparison_key'] ?? '')),
    );
    $submittedIdentity = array(
        'company_id'=>$submittedCompanyId,
        'event_type'=>$submittedEventType,
        'identity_action'=>trim((string)v1_first(
            $submittedEvent,
            array('identity_action'),
            ''
        )),
        'identity_target'=>trim((string)v1_first(
            $submittedEvent,
            array('identity_target'),
            ''
        )),
        'identity_actor_id'=>trim((string)v1_first(
            $submittedEvent,
            array('identity_actor_id','actor_id'),
            ''
        )),
        'identity_effective_at'=>(string)(mysql_dt(v1_first(
            $submittedEvent,
            array('identity_effective_at'),
            null
        )) ?? ''),
        'identity_deadline_at'=>(string)(mysql_dt(v1_first(
            $submittedEvent,
            array('identity_deadline_at'),
            null
        )) ?? ''),
        'identity_status'=>'needs_review',
        'comparison_key'=>trim((string)v1_first(
            $submittedEvent,
            array('comparison_key'),
            ''
        )),
    );
    if ($storedIdentity['identity_actor_id'] === ''
        || $submittedIdentity['identity_actor_id'] === ''
        || hash_equals(
            $storedIdentity['identity_actor_id'],
            $submittedIdentity['identity_actor_id']
        )
        || $storedIdentity['comparison_key'] !== ''
        || $submittedIdentity['comparison_key'] !== '') {
        return null;
    }
    foreach (array(
        'company_id','event_type','identity_action','identity_target',
        'identity_effective_at','identity_deadline_at','identity_status',
        'comparison_key',
    ) as $identityField) {
        if ((string)$storedIdentity[$identityField]
            !== (string)$submittedIdentity[$identityField]) {
            return null;
        }
    }
    if (!v1_dart_pending_filer_candidate_matches(
            $storedPayload,
            $eventId,
            $companyId,
            $storedIdentity['identity_actor_id']
        )
        || !v1_dart_pending_filer_candidate_matches(
            $submittedEvent,
            $eventId,
            $companyId,
            $submittedIdentity['identity_actor_id']
        )) {
        return null;
    }

    $normalizedStored = $storedPayload;
    $normalizedSubmitted = $submittedEvent;
    foreach (array('actor_id','identity_actor_id','actor','event_actor') as $actorField) {
        unset($normalizedStored[$actorField],$normalizedSubmitted[$actorField]);
    }
    foreach (array(&$normalizedStored,&$normalizedSubmitted) as &$normalizedPayload) {
        if (isset($normalizedPayload['metadata'])
            && !is_array($normalizedPayload['metadata'])) {
            return null;
        }
        if (!isset($normalizedPayload['metadata'])) {
            $normalizedPayload['metadata'] = array();
        }
        $titleProvenance = trim((string)(
            $normalizedPayload['metadata']['title_provenance'] ?? ''
        ));
        if ($titleProvenance !== '' && $titleProvenance !== 'source') {
            return null;
        }
        $normalizedPayload['metadata']['title_provenance'] = 'source';
    }
    unset($normalizedPayload);
    if (!hash_equals(
        hash('sha256',v1_strict_canonical_json_encode(
            $normalizedStored,
            'stored_dart_identity_change_payload_encode_failed'
        )),
        hash('sha256',v1_strict_canonical_json_encode(
            $normalizedSubmitted,
            'submitted_dart_identity_change_payload_encode_failed'
        ))
    )) {
        return null;
    }
    return array(
        'previous_identity_sha256'=>hash(
            'sha256',
            v1_strict_canonical_json_encode(
                $storedIdentity,
                'stored_dart_identity_change_encode_failed'
            )
        ),
        'current_identity_sha256'=>hash(
            'sha256',
            v1_strict_canonical_json_encode(
                $submittedIdentity,
                'submitted_dart_identity_change_encode_failed'
            )
        ),
    );
}

/**
 * Require the evidence document of a filer-identity change to be the exact
 * same official DART receipt. The collector timestamp is deliberately ignored.
 */
function v1_dart_identity_change_document_matches(
    array $stored,
    array $submitted,
    bool $allowCorrectionReplay = false
): bool {
    if (!empty($submitted['is_cancelled'])) {
        return false;
    }
    if (array_key_exists('is_correction',$submitted)
        && (!is_bool($submitted['is_correction'])
            || $submitted['is_correction'] !== $allowCorrectionReplay)) {
        return false;
    }
    $sourceClass = trim((string)v1_first(
        $submitted,
        array('source_class','source_category'),
        'official_disclosure'
    ));
    $sourceRightId = strtolower(trim((string)v1_first(
        $submitted,
        array('source_right_id'),
        ''
    )));
    $externalId = trim((string)v1_first(
        $submitted,
        array('external_id','stable_source_id','rcept_no'),
        ''
    ));
    $companyId = trim((string)v1_first(
        $submitted,
        array('company_id','corp_code'),
        ''
    ));
    $title = trim((string)v1_first($submitted,array('title','report_nm'),''));
    $url = trim((string)v1_first($submitted,array('original_url','url'),''));
    $body = (string)v1_first($submitted,array('body_text','content'),'');
    $contentHash = strtolower(trim((string)v1_first(
        $submitted,
        array('content_hash'),
        ''
    )));
    if (preg_match('/^[a-f0-9]{64}$/',$contentHash) !== 1) {
        $contentHash = hash('sha256',$title . "\n" . $url . "\n" . $body);
    }
    $publication = trim((string)v1_first(
        $submitted,
        array('publication_status'),
        'published'
    ));
    $verification = trim((string)v1_first(
        $submitted,
        array('verification_status'),
        'official'
    ));
    $submittedFields = array(
        $companyId,
        $sourceRightId,
        $sourceClass,
        mb_substr($externalId,0,191,'UTF-8'),
        mb_substr((string)v1_first(
            $submitted,
            array('document_type','pblntf_detail_ty'),
            ''
        ),0,80,'UTF-8'),
        v1_language(v1_first($submitted,array('original_language','language'),'ko'),'ko'),
        mb_substr($title,0,700,'UTF-8'),
        $body,
        $url,
        $contentHash,
        mb_substr(trim((string)v1_first(
            $submitted,
            array('collection_key'),
            ''
        )),0,96,'UTF-8'),
        (string)(mysql_dt(v1_first(
            $submitted,
            array('published_at','received_at','rcept_dt'),
            null
        )) ?? ''),
        $verification,
        $publication,
        trim((string)v1_first(
            $submitted,
            array('correction_of_document_id','correction_of'),
            ''
        )),
        max(1,(int)v1_first(
            $submitted,
            array('version_no'),
            ((int)v1_first($submitted,array('correction_sequence'),0)) + 1
        )),
    );
    $storedFields = array(
        (string)($stored['company_id'] ?? ''),
        strtolower((string)($stored['source_right_id'] ?? '')),
        (string)($stored['source_class'] ?? ''),
        (string)($stored['external_id'] ?? ''),
        (string)($stored['document_type'] ?? ''),
        (string)($stored['original_language'] ?? ''),
        (string)($stored['title'] ?? ''),
        (string)($stored['body_text'] ?? ''),
        (string)($stored['original_url'] ?? ''),
        strtolower((string)($stored['content_hash'] ?? '')),
        (string)($stored['collection_key'] ?? ''),
        (string)($stored['published_at'] ?? ''),
        (string)($stored['verification_status'] ?? ''),
        (string)($stored['publication_status'] ?? ''),
        (string)($stored['correction_of_document_id'] ?? ''),
        (int)($stored['version_no'] ?? 0),
    );
    if ($sourceClass !== 'official_disclosure'
        || $sourceRightId !== 'official:dart'
        || preg_match('/^[0-9]{14}$/',$externalId) !== 1
        || preg_match('/^[0-9]{8}$/',$companyId) !== 1
        || $verification !== 'official'
        || $publication !== 'published'
        || $storedFields !== $submittedFields) {
        return false;
    }
    $storedPayload = json_decode((string)($stored['payload_json'] ?? ''),true);
    if (!is_array($storedPayload)) {
        return false;
    }
    $submittedPayload = $submitted;
    foreach (array(&$storedPayload,&$submittedPayload) as &$documentPayload) {
        if (array_key_exists('is_correction',$documentPayload)
            && (!is_bool($documentPayload['is_correction'])
                || $documentPayload['is_correction']
                    !== $allowCorrectionReplay)) {
            return false;
        }
        // Older exact DART receipts may omit this redundant document flag.
        // The already-validated reviewed event is authoritative, so compare
        // both immutable payloads with the same strict boolean representation.
        $documentPayload['is_correction'] = $allowCorrectionReplay;
    }
    unset($documentPayload);
    unset($storedPayload['retrieved_at'],$submittedPayload['retrieved_at']);
    if ($allowCorrectionReplay
        && (string)($storedPayload['correction_link_status'] ?? '')
            === 'ambiguous_independent'
        && !isset($submittedPayload['correction_link_status'])) {
        $submittedPayload['correction_link_status'] =
            'ambiguous_independent';
    }
    foreach (array(&$storedPayload,&$submittedPayload) as &$documentPayload) {
        if (isset($documentPayload['metadata'])
            && !is_array($documentPayload['metadata'])) {
            return false;
        }
        if (!isset($documentPayload['metadata'])) {
            $documentPayload['metadata'] = array();
        }
        $titleProvenance = trim((string)(
            $documentPayload['metadata']['title_provenance'] ?? ''
        ));
        if ($titleProvenance !== '' && $titleProvenance !== 'source') {
            return false;
        }
        $documentPayload['metadata']['title_provenance'] = 'source';
    }
    unset($documentPayload);
    return hash_equals(
        hash('sha256',v1_strict_canonical_json_encode(
            $storedPayload,
            'stored_dart_identity_change_document_encode_failed'
        )),
        hash('sha256',v1_strict_canonical_json_encode(
            $submittedPayload,
            'submitted_dart_identity_change_document_encode_failed'
        ))
    );
}

/**
 * Prove that a company row accompanying a reviewed DART event replay would be
 * a semantic no-op and that the guarded global issuer projection is intact.
 *
 * The reviewed event/document exception must not silently swallow a legitimate
 * company-master change. Empty optional values retain the generic upsert's
 * existing "preserve stored value" semantics; every caller-provided value that
 * would otherwise update a row must already match. Projection rows are locked
 * and checked separately because the read-only path deliberately skips their
 * timestamp-changing upserts.
 */
function v1_dart_reviewed_company_replay_matches(
    PDO $pdo,
    array $config,
    array $submitted
): bool {
    $companyId = trim((string)v1_first(
        $submitted,
        array('company_id','corp_code'),
        ''
    ));
    $legalName = trim((string)v1_first(
        $submitted,
        array('legal_name','corp_name'),
        ''
    ));
    if (preg_match('/^[0-9]{8}$/',$companyId) !== 1 || $legalName === '') {
        return false;
    }

    $companyStatement = $pdo->prepare(
        'SELECT company_id,stock_code,market,legal_name,legal_name_en,'
        . 'short_name,aliases_json,homepage_url,record_status,listing_status,'
        . 'master_modified_at FROM ' . table_name($config,'companies')
        . ' WHERE company_id=? LIMIT 1 FOR UPDATE'
    );
    $stored = v1_pdo_fetch_one_and_close(
        $companyStatement,
        array($companyId)
    );
    if (!$stored || (string)$stored['company_id'] !== $companyId) {
        return false;
    }

    $submittedStockCode = mb_substr(
        (string)v1_first($submitted,array('stock_code'),''),
        0,
        12,
        'UTF-8'
    ) ?: null;
    $submittedMarket = mb_substr(
        (string)v1_first($submitted,array('market','corp_cls'),''),
        0,
        40,
        'UTF-8'
    ) ?: null;
    $submittedLegalNameEn = mb_substr(
        (string)v1_first(
            $submitted,
            array('legal_name_en','corp_name_eng'),
            ''
        ),
        0,
        255,
        'UTF-8'
    ) ?: null;
    $submittedShortName = mb_substr(
        (string)v1_first($submitted,array('short_name'),''),
        0,
        255,
        'UTF-8'
    ) ?: null;
    $submittedHomepage = (string)v1_first(
        $submitted,
        array('homepage_url','hm_url'),
        ''
    ) ?: null;
    $submittedAliases = array();
    foreach (array_slice(
        isset($submitted['aliases']) && is_array($submitted['aliases'])
            ? $submitted['aliases'] : array(),
        0,
        20
    ) as $alias) {
        $alias = mb_substr(trim((string)$alias),0,255,'UTF-8');
        if ($alias !== '') { $submittedAliases[] = $alias; }
    }
    $submittedAliases = array_values(array_unique($submittedAliases));
    $storedAliases = json_decode((string)($stored['aliases_json'] ?? ''),true);
    $recordStatus = (string)v1_first(
        $submitted,
        array('record_status'),
        'active'
    );
    if (!in_array(
        $recordStatus,
        array('active','inactive','merged','delisted'),
        true
    )) {
        $recordStatus = 'active';
    }
    $allowedListingStatuses = array(
        'unknown','listed','unlisted','suspended','delisted'
    );
    $requestedListingStatus = trim((string)v1_first(
        $submitted,
        array('listing_status'),
        ''
    ));
    $hasListingStatus = array_key_exists('listing_status',$submitted)
        && in_array(
            $requestedListingStatus,
            $allowedListingStatuses,
            true
        );
    $hasMasterModifiedAt = array_key_exists('master_modified_at',$submitted)
        || array_key_exists('modified_at',$submitted);
    $submittedMasterModifiedAt = mysql_dt(v1_first(
        $submitted,
        array('master_modified_at','modified_at'),
        null
    ));
    if ($hasMasterModifiedAt && $submittedMasterModifiedAt === null) {
        return false;
    }
    if ((string)$stored['legal_name']
            !== mb_substr($legalName,0,255,'UTF-8')
        || (string)$stored['record_status'] !== $recordStatus
        || ($submittedStockCode !== null
            && (string)($stored['stock_code'] ?? '')
                !== $submittedStockCode)
        || ($submittedMarket !== null
            && (string)($stored['market'] ?? '') !== $submittedMarket)
        || ($submittedLegalNameEn !== null
            && (string)($stored['legal_name_en'] ?? '')
                !== $submittedLegalNameEn)
        || ($submittedShortName !== null
            && (string)($stored['short_name'] ?? '')
                !== $submittedShortName)
        || ($submittedHomepage !== null
            && (string)($stored['homepage_url'] ?? '')
                !== $submittedHomepage)
        || (count($submittedAliases) > 0
            && (!is_array($storedAliases)
                || array_values($storedAliases) !== $submittedAliases))
        || ($hasListingStatus
            && (string)$stored['listing_status']
                !== $requestedListingStatus)
        || ($hasMasterModifiedAt
            && (string)($stored['master_modified_at'] ?? '')
                !== $submittedMasterModifiedAt)) {
        return false;
    }

    $issuerId = 'issuer:kr:dart:' . $companyId;
    $issuerStatement = $pdo->prepare(
        'SELECT issuer_id,country_code,legal_name,legal_name_en,short_name,'
        . 'original_language,homepage_url,listing_status,record_status,'
        . 'master_modified_at,payload_json FROM '
        . table_name($config,'issuers')
        . ' WHERE issuer_id=? LIMIT 1 FOR UPDATE'
    );
    $issuer = v1_pdo_fetch_one_and_close(
        $issuerStatement,
        array($issuerId)
    );
    $expectedIssuerPayload = array(
        'legacy_company_id'=>$companyId,
        'identity_namespace'=>'DART_CORP_CODE',
        'bridge'=>'v1_official_ingest',
    );
    $issuerPayload = $issuer
        ? json_decode((string)($issuer['payload_json'] ?? ''),true) : null;
    if (!$issuer
        || (string)$issuer['issuer_id'] !== $issuerId
        || (string)$issuer['country_code'] !== 'KR'
        || (string)$issuer['legal_name'] !== (string)$stored['legal_name']
        || (string)$issuer['original_language'] !== 'ko'
        || (string)$issuer['listing_status']
            !== (string)$stored['listing_status']
        || (string)$issuer['record_status']
            !== (string)$stored['record_status']
        || ((string)($stored['legal_name_en'] ?? '') !== ''
            && (string)($issuer['legal_name_en'] ?? '')
                !== (string)$stored['legal_name_en'])
        || ((string)($stored['short_name'] ?? '') !== ''
            && (string)($issuer['short_name'] ?? '')
                !== (string)$stored['short_name'])
        || ((string)($stored['homepage_url'] ?? '') !== ''
            && (string)($issuer['homepage_url'] ?? '')
                !== (string)$stored['homepage_url'])
        || (($stored['master_modified_at'] ?? null) !== null
            && (string)($issuer['master_modified_at'] ?? '')
                !== (string)$stored['master_modified_at'])
        || !is_array($issuerPayload)
        || $issuerPayload != $expectedIssuerPayload) {
        return false;
    }

    $identifierStatement = $pdo->prepare(
        'SELECT issuer_id,identifier_type,identifier_value,market,is_primary'
        . ' FROM ' . table_name($config,'issuer_identifiers')
        . ' WHERE identifier_type=? AND identifier_value=? AND market=?'
        . ' ORDER BY issuer_id LIMIT 2 FOR UPDATE'
    );
    $dartIdentifiers = v1_pdo_fetch_all_and_close(
        $identifierStatement,
        array('DART_CORP_CODE',$companyId,'KRX')
    );
    if (count($dartIdentifiers) !== 1
        || (string)$dartIdentifiers[0]['issuer_id'] !== $issuerId
        || (int)$dartIdentifiers[0]['is_primary'] !== 1) {
        return false;
    }

    $projectionStockCode = mb_substr(
        trim((string)v1_first($submitted,array('stock_code'),'')),
        0,
        12,
        'UTF-8'
    );
    $projectionMarket = mb_substr(
        trim((string)v1_first($submitted,array('market','corp_cls'),'')),
        0,
        40,
        'UTF-8'
    );
    if ($projectionMarket === '') { $projectionMarket = 'KRX'; }
    $storedStockCode = trim((string)($stored['stock_code'] ?? ''));
    if ($storedStockCode === '') {
        return $projectionStockCode === '';
    }
    $storedMarket = trim((string)($stored['market'] ?? ''));
    if ($storedMarket === '') { $storedMarket = 'KRX'; }
    if ($projectionStockCode !== ''
        && ($projectionStockCode !== $storedStockCode
            || $projectionMarket !== $storedMarket)) {
        return false;
    }
    $tickerIdentifiers = v1_pdo_fetch_all_and_close(
        $identifierStatement,
        array('TICKER',$storedStockCode,$storedMarket)
    );
    if (count($tickerIdentifiers) !== 1
        || (string)$tickerIdentifiers[0]['issuer_id'] !== $issuerId
        || (int)$tickerIdentifiers[0]['is_primary'] !== 0) {
        return false;
    }

    $listingStatement = $pdo->prepare(
        'SELECT listing_id,issuer_id,country_code,market,ticker,'
        . 'listing_status,is_primary FROM '
        . table_name($config,'issuer_listings')
        . ' WHERE listing_id=? LIMIT 1 FOR UPDATE'
    );
    $listing = v1_pdo_fetch_one_and_close(
        $listingStatement,
        array('listing:kr:' . $companyId)
    );
    return $listing
        && (string)$listing['issuer_id'] === $issuerId
        && (string)$listing['country_code'] === 'KR'
        && (string)$listing['market'] === $storedMarket
        && (string)$listing['ticker'] === $storedStockCode
        && (int)$listing['is_primary'] === 1
        && (
            (string)$stored['listing_status'] === 'unknown'
            || (string)$listing['listing_status']
                === (string)$stored['listing_status']
        );
}

/**
 * Return only a stable, caller-actionable validation reason.
 *
 * Runtime exceptions may append a record identifier after a colon for local
 * control flow.  The suffix is never copied into an HTTP response.
 */
function v1_governance_snapshot_validation_reason(Throwable $error): ?string {
    if (!$error instanceof RuntimeException || $error instanceof PDOException) {
        return null;
    }
    $message = $error->getMessage();
    $separator = strpos($message,':');
    $reason = $separator === false ? $message : substr($message,0,$separator);
    if (preg_match('/^[a-z][a-z0-9_]{0,63}$/D',$reason) !== 1) {
        return null;
    }
    $allowed = array(
        'dart_document_title_provenance_conflict',
        'dart_event_metadata_invalid',
        'dart_title_provenance_conflict',
        'document_lineage_conflict',
        'event_identity_field_conflict',
        'event_identity_scope_conflict',
        'event_observation_document_missing',
        'event_observation_hash_invalid',
        'followup_event_identity_conflict',
        'global_dart_connector_not_writable',
        'global_release_state_guard_unavailable',
        'incomplete_event_identity_has_comparison_key',
        'invalid_collection_run_code_revision',
        'invalid_collection_run_counts',
        'invalid_complete_event_identity',
        'invalid_scheduled_slot_claim_provenance',
        'non_scheduled_run_has_slot_claim',
        'release_state_unavailable',
        'scheduled_slot_claim_completion_conflict',
        'scheduled_slot_claim_conflict',
    );
    return in_array($reason,$allowed,true) ? $reason : null;
}

/**
 * Extract bounded PDO diagnostics without retaining SQL, messages or values.
 *
 * `sqlstate_class` keeps the complete five-character SQLSTATE so the remote
 * collector can validate it without receiving a query or exception message.
 *
 * @return array{sqlstate_class:string,driver_code:int}|null
 */
function v1_governance_snapshot_persistence_diagnostic(Throwable $error): ?array {
    $candidate = $error;
    for ($depth=0; $depth<5; $depth++) {
        if ($candidate instanceof PDOException) {
            $errorInfo = isset($candidate->errorInfo) && is_array($candidate->errorInfo)
                ? $candidate->errorInfo : array();
            $sqlState = strtoupper((string)($errorInfo[0] ?? ''));
            if (preg_match('/^[A-Z0-9]{5}$/D',$sqlState) !== 1) {
                $sqlState = 'HY000';
            }
            $rawDriverCode = $errorInfo[1] ?? 0;
            $driverCode = 0;
            if (is_int($rawDriverCode)
                && $rawDriverCode >= -2147483648
                && $rawDriverCode <= 2147483647) {
                $driverCode = $rawDriverCode;
            } elseif (is_string($rawDriverCode)
                && preg_match('/^-?(?:0|[1-9][0-9]{0,9})$/D',$rawDriverCode) === 1) {
                $parsed = (int)$rawDriverCode;
                if ($parsed >= -2147483648 && $parsed <= 2147483647) {
                    $driverCode = $parsed;
                }
            }
            return array(
                'sqlstate_class'=>$sqlState,
                'driver_code'=>$driverCode,
            );
        }
        $previous = $candidate->getPrevious();
        if (!$previous instanceof Throwable) {
            break;
        }
        $candidate = $previous;
    }
    return null;
}

/**
 * Build the complete safe HTTP result for a failed governance transaction.
 *
 * @return array{status:int,payload:array<string,mixed>}
 */
function v1_governance_snapshot_failure_response(Throwable $error): array {
    $diagnostic = v1_governance_snapshot_persistence_diagnostic($error);
    if ($diagnostic !== null) {
        return array(
            'status'=>503,
            'payload'=>array(
                'ok'=>false,
                'error'=>'governance_snapshot_persistence_failed',
                'sqlstate_class'=>$diagnostic['sqlstate_class'],
                'driver_code'=>$diagnostic['driver_code'],
            ),
        );
    }
    $validationReason = v1_governance_snapshot_validation_reason($error);
    if ($validationReason !== null) {
        return array(
            'status'=>409,
            'payload'=>array(
                'ok'=>false,
                'error'=>$validationReason,
                'validation_reason'=>$validationReason,
            ),
        );
    }
    return array(
        'status'=>500,
        'payload'=>array('ok'=>false,'error'=>'internal_error'),
    );
}

/**
 * Enable the additive DART -> global-terminal bridge only after the immutable
 * v2 migration manifest and the DART credential-pool migration are present.
 * Older production schemas never reference v2-only tables or columns.
 */
function v1_global_dart_bridge_enabled(PDO $pdo, array $config): bool {
    if (function_exists('v2_schema_manifest_status')) {
        $manifest = v2_schema_manifest_status($pdo,$config);
        return isset($manifest['valid'],$manifest['highest_version'])
            && $manifest['valid'] === true
            && (int)$manifest['highest_version'] >= 12;
    }
    return false;
}

/**
 * Validate the signed DART SourceRight precondition without exposing either
 * digest. The same values are rechecked under a row lock before every write.
 */
function v1_dart_source_right_expectation(array $payload): ?array {
    $all = isset($payload['expected_source_right_revisions'])
        && is_array($payload['expected_source_right_revisions'])
        ? $payload['expected_source_right_revisions'] : array();
    $expected = isset($all['official:dart'])
        && is_array($all['official:dart']) ? $all['official:dart'] : null;
    if ($expected === null
        || count($expected) !== 2
        || !isset($expected['rights_revision'],$expected['contract_revision'])
        || preg_match('/^[a-f0-9]{64}$/',(string)$expected['rights_revision']) !== 1
        || preg_match('/^[a-f0-9]{64}$/',(string)$expected['contract_revision']) !== 1) {
        return null;
    }
    return array(
        'rights_revision'=>(string)$expected['rights_revision'],
        'contract_revision'=>(string)$expected['contract_revision'],
    );
}

/** Exact candidate deployment SHA carried inside the guarded HMAC body. */
function v1_dart_deployment_expectation(array $payload): ?string {
    $expected = isset($payload['expected_deployment_code_revision'])
        ? strtolower(trim((string)$payload['expected_deployment_code_revision']))
        : '';
    return preg_match('/^[a-f0-9]{40}$/',$expected) === 1 ? $expected : null;
}

/** Exact v1/v2 release state observed by the collector preflight. */
function v1_dart_release_state_expectation(array $payload): ?string {
    $expected = isset($payload['expected_release_state'])
        ? strtolower(trim((string)$payload['expected_release_state'])) : '';
    return in_array($expected,array('closed','preview','live'),true)
        ? $expected : null;
}

/**
 * Resolve the exact document identity used by the downstream upsert before
 * lineage checks run. A caller must not be able to omit document_id and make
 * the generic path miss an existing derived OpenDART identity.
 */
function v1_governance_snapshot_document_id(array $document): string {
    $documentId = trim((string)v1_first(
        $document,
        array('document_id'),
        ''
    ));
    if ($documentId !== '') { return $documentId; }
    $sourceClass = trim((string)v1_first(
        $document,
        array('source_class','source_category'),
        'official_disclosure'
    ));
    if ($sourceClass === 'authorized_telegram') {
        $sourceClass = 'licensed_telegram';
    }
    if (!in_array($sourceClass,array(
        'official_disclosure',
        'company_statement',
        'activist_statement',
        'media_report',
        'licensed_telegram',
        'editorial_analysis',
    ),true)) {
        return '';
    }
    $externalId = trim((string)v1_first(
        $document,
        array('external_id','stable_source_id','rcept_no'),
        ''
    ));
    if ($externalId === '') { return ''; }
    return v1_stable_id(
        $sourceClass === 'official_disclosure' ? 'dart' : 'doc',
        $externalId
    );
}

function v1_normalize_governance_snapshot_documents(array $documents): array {
    $normalized = array();
    foreach ($documents as $document) {
        if (!is_array($document)) {
            $normalized[] = $document;
            continue;
        }
        $documentId = v1_governance_snapshot_document_id($document);
        if ($documentId !== '') { $document['document_id'] = $documentId; }
        $declaredSource = strtolower(trim((string)v1_first(
            $document,
            array('source','source_key'),
            ''
        )));
        $sourceRightId = strtolower(trim((string)v1_first(
            $document,
            array('source_right_id'),
            ''
        )));
        $isDart = strpos(strtolower($documentId),'dart:') === 0
            || $declaredSource === 'dart'
            || $sourceRightId === 'official:dart';
        if ($isDart) {
            foreach (array('body_text','content') as $bodyField) {
                if (!array_key_exists($bodyField,$document)
                    || $document[$bodyField] === null) {
                    continue;
                }
                if (!is_string($document[$bodyField])
                    || $document[$bodyField] !== '') {
                    respond(409,array(
                        'ok'=>false,
                        'error'=>'dart_body_text_forbidden',
                    ));
                }
            }
            // Empty legacy collector values are normalized to SQL NULL by the
            // downstream upsert. Never let a secondary content alias revive
            // source body storage.
            $document['body_text'] = '';
            unset($document['content']);
        }
        $normalized[] = $document;
    }
    return $normalized;
}

/**
 * Gather every submitted identity that can resolve to an existing DART-backed
 * company, document, event or collection run. The rows are re-read under
 * locks before any mutation.
 */
function v1_governance_snapshot_lineage_candidates(
    array $companies,
    array $documents,
    array $events,
    array $run
): array {
    $companyIds = array();
    $documentIds = array();
    $eventIds = array();
    $comparisonKeys = array();
    $runIds = array();
    foreach ($companies as $company) {
        if (!is_array($company)) { continue; }
        $companyId = trim((string)v1_first(
            $company,
            array('company_id','corp_code'),
            ''
        ));
        if (preg_match('/^[0-9]{8}$/',$companyId) === 1) {
            $companyIds[$companyId] = true;
        }
    }
    foreach ($documents as $document) {
        if (!is_array($document)) { continue; }
        $documentId = v1_governance_snapshot_document_id($document);
        if (v1_valid_entity_id($documentId)) { $documentIds[$documentId] = true; }
        $correctionOf = trim((string)v1_first(
            $document,
            array('correction_of_document_id','correction_of'),
            ''
        ));
        if (v1_valid_entity_id($correctionOf)) {
            // A caller cannot disguise a relationship to an existing DART
            // predecessor by declaring the new document as another source.
            $documentIds[$correctionOf] = true;
        }
    }
    foreach ($events as $event) {
        if (!is_array($event)) { continue; }
        $eventId = trim((string)v1_first($event,array('event_id'),''));
        if (v1_valid_entity_id($eventId)) { $eventIds[$eventId] = true; }
        $comparisonKey = trim((string)v1_first(
            $event,
            array('comparison_key'),
            ''
        ));
        if (preg_match('/^eventcmp:v1:[a-f0-9]{64}$/',$comparisonKey) === 1) {
            $comparisonKeys[$comparisonKey] = true;
        }
        $references = isset($event['document_ids'])
            && is_array($event['document_ids'])
            ? $event['document_ids'] : array();
        if (isset($event['document_id'])) { $references[] = $event['document_id']; }
        foreach ($references as $reference) {
            if (!is_string($reference) && !is_int($reference)) { continue; }
            $documentId = trim((string)$reference);
            if (v1_valid_entity_id($documentId)) {
                $documentIds[$documentId] = true;
            }
        }
    }
    $runId = trim((string)v1_first($run,array('run_id'),''));
    if (v1_valid_entity_id($runId)) { $runIds[$runId] = true; }
    $result = array(
        'company_ids'=>array_keys($companyIds),
        'document_ids'=>array_keys($documentIds),
        'event_ids'=>array_keys($eventIds),
        'comparison_keys'=>array_keys($comparisonKeys),
        'run_ids'=>array_keys($runIds),
    );
    foreach ($result as &$values) { sort($values,SORT_STRING); }
    unset($values);
    return $result;
}

/**
 * Lock submitted lineage targets and report whether any target is already
 * backed by an approved OpenDART document.
 *
 * Callers must lock release-state rows first. That makes an absent lookup safe
 * from a concurrent guarded DART writer and preserves cutover lock ordering.
 */
function v1_lock_existing_dart_lineage(
    PDO $pdo,
    array $config,
    array $candidates,
    bool $globalDartBridgeEnabled
): bool {
    $companyLookup = $pdo->prepare(
        'SELECT company_id FROM ' . table_name($config,'companies')
        . ' WHERE company_id=? LIMIT 1 FOR UPDATE'
    );
    foreach ($candidates['company_ids'] as $companyId) {
        // Legacy company_id is the eight-digit OpenDART corp_code namespace.
        if (v1_pdo_fetch_column_and_close(
            $companyLookup,
            array($companyId)
        ) !== false) {
            return true;
        }
    }
    $documentLookup = $pdo->prepare(
        'SELECT CASE WHEN current_document.source_right_id=\'official:dart\''
        . ' OR predecessor.source_right_id=\'official:dart\''
        . ' THEN \'official:dart\' ELSE current_document.source_right_id END'
        . ' FROM ' . table_name($config,'documents') . ' current_document'
        . ' LEFT JOIN ' . table_name($config,'documents') . ' predecessor'
        . ' ON predecessor.document_id=current_document.correction_of_document_id'
        . ' WHERE current_document.document_id=? LIMIT 1 FOR UPDATE'
    );
    foreach ($candidates['document_ids'] as $documentId) {
        if ((string)v1_pdo_fetch_column_and_close(
            $documentLookup,
            array($documentId)
        ) === 'official:dart') {
            return true;
        }
    }
    $eventProjectionFields = $globalDartBridgeEnabled
        ? 'issuer_id,country_code' : 'NULL AS issuer_id,NULL AS country_code';
    $eventIdentityLookup = $pdo->prepare(
        'SELECT event_id,company_id,' . $eventProjectionFields . ' FROM '
        . table_name($config,'governance_events')
        . ' WHERE event_id=? LIMIT 1 FOR UPDATE'
    );
    $comparisonIdentityLookup = $pdo->prepare(
        'SELECT event_id,company_id,' . $eventProjectionFields . ' FROM '
        . table_name($config,'governance_events')
        . ' WHERE comparison_key=? LIMIT 1 FOR UPDATE'
    );
    $eventDocumentLookup = $pdo->prepare(
        'SELECT d.document_id FROM '
        . table_name($config,'governance_events') . ' e'
        . ' JOIN ' . table_name($config,'event_documents') . ' ed'
        . ' ON ed.event_id=e.event_id'
        . ' JOIN ' . table_name($config,'documents') . ' d'
        . ' ON d.document_id=ed.document_id'
        . ' LEFT JOIN ' . table_name($config,'documents') . ' predecessor'
        . ' ON predecessor.document_id=d.correction_of_document_id'
        . ' WHERE e.event_id=? AND (d.source_right_id=\'official:dart\''
        . ' OR predecessor.source_right_id=\'official:dart\')'
        . ' ORDER BY BINARY d.document_id LIMIT 1 FOR UPDATE'
    );
    $eventObservationLookup = $pdo->prepare(
        'SELECT eo.observation_id FROM '
        . table_name($config,'event_observations') . ' eo'
        . ' LEFT JOIN ' . table_name($config,'documents') . ' d'
        . ' ON d.document_id=eo.document_id'
        . ' LEFT JOIN ' . table_name($config,'documents') . ' predecessor'
        . ' ON predecessor.document_id=d.correction_of_document_id'
        . ' WHERE eo.event_id=? AND (eo.source_key=\'dart\''
        . ' OR d.source_right_id=\'official:dart\''
        . ' OR predecessor.source_right_id=\'official:dart\')'
        . ' ORDER BY BINARY eo.observation_id LIMIT 1 FOR UPDATE'
    );
    $eventHasDartLineage = function (array $event) use (
        $eventDocumentLookup,
        $eventObservationLookup
    ): bool {
        $eventId = isset($event['event_id']) ? (string)$event['event_id'] : '';
        $companyId = isset($event['company_id'])
            ? (string)$event['company_id'] : '';
        $issuerId = isset($event['issuer_id'])
            ? (string)$event['issuer_id'] : '';
        $countryCode = isset($event['country_code'])
            ? (string)$event['country_code'] : '';
        // Migration 011 and the guarded bridge use this exact projection.
        // Treat a projection-only row conservatively when historical link
        // tables are partial; generic writers must not claim it.
        if ($countryCode === 'KR'
            && preg_match('/^[0-9]{8}$/',$companyId) === 1
            && $issuerId === 'issuer:kr:dart:' . $companyId) {
            return true;
        }
        if (v1_pdo_fetch_column_and_close(
            $eventDocumentLookup,
            array($eventId)
        ) !== false) {
            return true;
        }
        return v1_pdo_fetch_column_and_close(
            $eventObservationLookup,
            array($eventId)
        ) !== false;
    };
    foreach ($candidates['event_ids'] as $eventId) {
        $event = v1_pdo_fetch_one_and_close(
            $eventIdentityLookup,
            array($eventId)
        );
        if (is_array($event) && $eventHasDartLineage($event)) { return true; }
    }
    foreach ($candidates['comparison_keys'] as $comparisonKey) {
        $event = v1_pdo_fetch_one_and_close(
            $comparisonIdentityLookup,
            array($comparisonKey)
        );
        if (is_array($event) && $eventHasDartLineage($event)) { return true; }
    }
    $runLookup = $pdo->prepare(
        'SELECT source_key FROM ' . table_name($config,'collection_runs')
        . ' WHERE run_id=? LIMIT 1 FOR UPDATE'
    );
    foreach ($candidates['run_ids'] as $runId) {
        $sourceKey = v1_pdo_fetch_column_and_close(
            $runLookup,
            array($runId)
        );
        if (!is_string($sourceKey)) { continue; }
        $sourceTokens = array_values(array_filter(array_map(
            'trim',
            explode('+',strtolower($sourceKey))
        )));
        if (in_array('dart',$sourceTokens,true)) { return true; }
    }
    return false;
}

/** Map the legacy Korean event taxonomy to the eight global terminal lanes. */
function v1_global_event_family_for_legacy_type(string $eventType): ?string {
    $families = array(
        'five_percent_holding'=>'large_ownership',
        'shareholder_proposal'=>'meeting_and_vote',
        'general_meeting'=>'meeting_and_vote',
        'tender_offer'=>'tender_offer_and_mna',
        'merger'=>'tender_offer_and_mna',
        'split'=>'tender_offer_and_mna',
        'rights_issue'=>'capital_issuance',
        'convertible_bond'=>'capital_issuance',
        'bond_with_warrant'=>'capital_issuance',
        'exchangeable_bond'=>'capital_issuance',
        'dividend'=>'capital_return',
        'treasury_shares'=>'capital_return',
        'value_up'=>'capital_return',
        'board'=>'board_and_compensation',
        'executive_compensation'=>'board_and_compensation',
        'trading_suspension'=>'listing_status',
        'delisting'=>'listing_status',
        'duplicate_listing'=>'listing_status',
    );
    return isset($families[$eventType]) ? $families[$eventType] : null;
}

function v1_global_dart_metric_count($value): ?int {
    if (is_int($value) && $value >= 0 && $value <= 4294967295) { return $value; }
    if (is_string($value) && strlen($value) <= 10
        && preg_match('/^(0|[1-9][0-9]*)$/',$value) === 1) {
        $normalized = (int)$value;
        return $normalized >= 0 && $normalized <= 4294967295 ? $normalized : null;
    }
    return null;
}

function v1_global_dart_window_date($value): ?string {
    if (!is_string($value)
        || preg_match('/^(\d{4})-(\d{2})-(\d{2})$/',trim($value),$parts) !== 1
        || !checkdate((int)$parts[2],(int)$parts[3],(int)$parts[1])) {
        return null;
    }
    return trim($value);
}

/**
 * Lock the single OpenDART connector after release-state and SourceRight rows.
 * Every DART write path shares this ordering with connector administration.
 */
function v1_lock_global_dart_connector(
    PDO $pdo,
    array $config
): ?array {
    $statement = $pdo->prepare(
        'SELECT connector_id,country_code,source_key,source_type,'
        . 'source_right_id,connector_status,cursor_json,last_checked_at,'
        . 'last_success_at FROM ' . table_name($config,'source_connectors')
        . ' WHERE connector_id=? LIMIT 1 FOR UPDATE'
    );
    $connector = v1_pdo_fetch_one_and_close(
        $statement,
        array('connector:kr:dart')
    );
    return is_array($connector) ? $connector : null;
}

/**
 * Extract a source-scoped DART outcome.  A combined DART+KIND run may advance
 * DART freshness when (and only when) DART itself succeeded and its exact
 * document ACK denominator matches.  KIND-only and partial runs cannot pass.
 */
function v1_global_dart_run_outcome(array $run): array {
    $sourceTokens = array_values(array_filter(array_map('trim',
        explode('+',strtolower((string)v1_first($run,array('source_key'),''))))));
    $selected = in_array('dart',$sourceTokens,true);
    $outcomesValue = v1_official_run_metric($run,'source_outcomes');
    $outcomes = is_array($outcomesValue) ? $outcomesValue : array();
    $outcome = isset($outcomes['dart']) && is_array($outcomes['dart'])
        ? $outcomes['dart'] : array();
    $ackValue = v1_official_run_metric($run,'source_ack_counts');
    $ackCounts = is_array($ackValue) ? $ackValue : array();
    $raw = v1_global_dart_metric_count(isset($outcome['raw_count']) ? $outcome['raw_count'] : null);
    $ack = v1_global_dart_metric_count(isset($ackCounts['dart']) ? $ackCounts['dart'] : null);
    $errors = v1_global_dart_metric_count(isset($outcome['error_count']) ? $outcome['error_count'] : null);
    $status = strtolower(trim((string)(isset($outcome['status']) ? $outcome['status'] : 'missing')));
    $succeeded = $selected
        && in_array($status,array('success','succeeded'),true)
        && $raw !== null && $ack !== null && $raw === $ack
        && $errors === 0;
    $errorClass = !$selected ? null
        : ($status === 'missing' ? 'dart_outcome_missing'
            : (($raw === null || $ack === null) ? 'dart_ack_missing'
                : ($raw !== $ack ? 'dart_ack_mismatch'
                    : ($errors !== 0 ? 'dart_source_failed'
                        : mb_substr('dart_source_' . $status,0,80,'UTF-8')))));
    return array(
        'selected'=>$selected,
        'succeeded'=>$succeeded,
        'raw_count'=>$raw,
        'acknowledged_count'=>$ack,
        'error_class'=>$errorClass,
    );
}

/**
 * Project a completed legacy DART run into the v2 connector registry.
 *
 * The caller supplies the connector row already locked after release-state and
 * SourceRight. Only a source-scoped, ACK-complete run with a durable window
 * and code revision may advance last_success/cursor. Failed, partial and older
 * observations may update last_checked/error only; they never regress the
 * last successful checkpoint or override an administrative kill switch.
 */
function v1_bridge_dart_connector_run(PDO $pdo, array $config, array $connector,
    array $run, string $runId,
    ?string $codeRevision, ?string $finishedAt, string $firstObservedAt, string $now,
    bool $runCompletionValid): void {
    $outcome = v1_global_dart_run_outcome($run);
    if ($outcome['selected'] !== true) { return; }
    if ((string)v1_first($connector,array('connector_id'),'') !== 'connector:kr:dart'
        || (string)v1_first($connector,array('country_code'),'') !== 'KR'
        || (string)v1_first($connector,array('source_key'),'') !== 'dart'
        || (string)v1_first($connector,array('source_type'),'') !== 'official_disclosure'
        || (string)v1_first($connector,array('source_right_id'),'') !== 'official:dart'
        || !in_array(
            (string)v1_first($connector,array('connector_status'),''),
            array('configured','active'),
            true
        )) {
        throw new RuntimeException('global_dart_connector_not_writable');
    }
    $checkedAt = $finishedAt !== null ? $finishedAt : $now;
    $lastCheckedAt = isset($connector['last_checked_at']) && is_string($connector['last_checked_at'])
        ? $connector['last_checked_at'] : null;
    $isNewestCheck = $lastCheckedAt === null || strcmp($checkedAt,$lastCheckedAt) >= 0;
    $windowStartValue = v1_official_run_metric($run,'window_start');
    $windowEndValue = v1_official_run_metric($run,'window_end');
    $windowStart = v1_global_dart_window_date($windowStartValue);
    $windowEnd = v1_global_dart_window_date($windowEndValue);
    $hasDurableWindow = $windowStart !== null && $windowEnd !== null
        && $windowStart <= $windowEnd;
    $canAdvance = $outcome['succeeded'] === true
        && $runCompletionValid
        && $finishedAt !== null && $codeRevision !== null && $hasDurableWindow;
    if ($canAdvance) {
        $lastSuccessAt = isset($connector['last_success_at']) && is_string($connector['last_success_at'])
            ? $connector['last_success_at'] : null;
        if ($lastSuccessAt !== null && strcmp($finishedAt,$lastSuccessAt) < 0) {
            return;
        }
        $retrievedValue = v1_official_run_metric($run,'retrieved_at');
        $observedAt = is_string($retrievedValue) ? v1_mysql_datetime_utc($retrievedValue) : null;
        if ($observedAt === null) { $observedAt = $firstObservedAt; }
        $cursorJson = json_value(array(
            'schema_version'=>1,
            'source_key'=>'dart',
            'run_id'=>$runId,
            'window_start'=>$windowStart,
            'window_end_inclusive'=>$windowEnd,
        ));
        $storedCursor = isset($connector['cursor_json']) && is_string($connector['cursor_json'])
            ? json_decode($connector['cursor_json'],true) : null;
        $storedWindowEndValue = is_array($storedCursor)
            ? (isset($storedCursor['window_end_exclusive'])
                ? $storedCursor['window_end_exclusive']
                : (isset($storedCursor['window_end_inclusive'])
                    ? $storedCursor['window_end_inclusive'] : null))
            : null;
        $storedWindowEnd = v1_global_dart_window_date($storedWindowEndValue);
        if ($storedWindowEnd !== null && $storedWindowEnd > $windowEnd) {
            $cursorJson = (string)$connector['cursor_json'];
        }
        $update = $pdo->prepare('UPDATE ' . table_name($config,'source_connectors')
            . ' SET connector_status=\'active\',cursor_json=?,last_checked_at=?,last_success_at=?,'
            . 'last_observed_at=?,last_raw_count=?,last_acknowledged_count=?,last_error_class=NULL,'
            . 'code_revision=?,updated_at=? WHERE connector_id=? AND country_code=\'KR\''
            . ' AND source_key=\'dart\' AND connector_status IN (\'configured\',\'active\')');
        $update->execute(array(
            $cursorJson,$checkedAt,$finishedAt,$observedAt,(int)$outcome['raw_count'],
            (int)$outcome['acknowledged_count'],$codeRevision,$now,'connector:kr:dart',
        ));
        return;
    }
    if ($isNewestCheck) {
        $errorClass = is_string($outcome['error_class']) && $outcome['error_class'] !== ''
            ? $outcome['error_class'] : 'dart_checkpoint_incomplete';
        if ($outcome['succeeded'] === true) { $errorClass = 'dart_checkpoint_incomplete'; }
        $failure = $pdo->prepare('UPDATE ' . table_name($config,'source_connectors')
            . ' SET last_checked_at=?,last_error_class=?,updated_at=?'
            . ' WHERE connector_id=? AND country_code=\'KR\' AND source_key=\'dart\''
            . ' AND connector_status IN (\'configured\',\'active\')');
        $failure->execute(array($checkedAt,$errorClass,$now,'connector:kr:dart'));
    }
}

/**
 * Validate the signed, DART-only replay marker.
 *
 * A replay is not an upsert retry. It must prove that the exact successful
 * apply receipt already exists, after which the transaction returns a
 * read-only acknowledgement.
 */
function v1_dart_replay_expectation(array $payload): ?array {
    $mode = isset($payload['ingest_mode'])
        ? strtolower(trim((string)$payload['ingest_mode'])) : 'apply';
    $hasContract = array_key_exists('dart_replay',$payload);
    if ($mode !== 'apply' && $mode !== 'replay') {
        respond(400,array('ok'=>false,'error'=>'dart_replay_contract_invalid'));
    }
    if ($mode !== 'replay') {
        if ($hasContract) {
            respond(400,array('ok'=>false,'error'=>'dart_replay_contract_invalid'));
        }
        return null;
    }
    if (!$hasContract || !is_array($payload['dart_replay'])) {
        respond(400,array('ok'=>false,'error'=>'dart_replay_contract_invalid'));
    }
    $contract = $payload['dart_replay'];
    $required = array(
        'contract_version','run_id','pipeline','source_key','code_revision',
        'idempotency_key','stable_payload_sha256','attempted_at','raw_count',
        'acknowledged_count','fetched_count','resolved_count','accepted_count',
        'error_count',
    );
    $actual = array_keys($contract); sort($actual);
    $expected = $required; sort($expected);
    if ($actual !== $expected
        || !isset($contract['contract_version'])
        || !is_int($contract['contract_version'])
        || $contract['contract_version'] !== 1) {
        respond(400,array('ok'=>false,'error'=>'dart_replay_contract_invalid'));
    }
    $runId = trim((string)$contract['run_id']);
    $pipeline = trim((string)$contract['pipeline']);
    $sourceKey = strtolower(trim((string)$contract['source_key']));
    $codeRevision = strtolower(trim((string)$contract['code_revision']));
    $idempotencyKey = trim((string)$contract['idempotency_key']);
    $stablePayloadSha = strtolower(trim((string)$contract['stable_payload_sha256']));
    $attemptedAt = trim((string)$contract['attempted_at']);
    $attemptedEpoch = strtotime($attemptedAt);
    if (!v1_valid_entity_id($runId)
        || $pipeline !== 'ingest-official'
        || $sourceKey !== 'dart'
        || preg_match('/^[a-f0-9]{40}$/D',$codeRevision) !== 1
        || strpos($idempotencyKey,'official-backfill-v1:') !== 0
        || strlen($idempotencyKey) > 255
        || preg_match('/^[a-f0-9]{64}$/D',$stablePayloadSha) !== 1
        || $attemptedEpoch === false) {
        respond(400,array('ok'=>false,'error'=>'dart_replay_contract_invalid'));
    }
    $counts = array();
    foreach (array(
        'raw_count','acknowledged_count','fetched_count','resolved_count',
        'accepted_count','error_count',
    ) as $field) {
        if (!isset($contract[$field])
            || !is_int($contract[$field])
            || $contract[$field] < 0) {
            respond(400,array('ok'=>false,'error'=>'dart_replay_contract_invalid'));
        }
        $counts[$field] = $contract[$field];
    }
    return array(
        'run_id'=>$runId,
        'pipeline'=>$pipeline,
        'source_key'=>$sourceKey,
        'code_revision'=>$codeRevision,
        'idempotency_key'=>$idempotencyKey,
        'stable_payload_sha256'=>$stablePayloadSha,
        'attempted_at'=>gmdate('Y-m-d\TH:i:s\Z',$attemptedEpoch),
        'counts'=>$counts,
    );
}

function v1_dart_replay_error(PDO $pdo, string $code): void {
    if ($pdo->inTransaction()) { $pdo->rollBack(); }
    respond(409,array('ok'=>false,'error'=>$code));
}

function v1_dart_replay_read_only_ack(
    PDO $pdo,
    array $config,
    array $expectation,
    array $payload,
    string $backendBindingId
): void {
    $statement = $pdo->prepare(
        'SELECT run_id,pipeline,source_key,code_revision,status,raw_count,'
        . 'acknowledged_count,fetched_count,resolved_count,accepted_count,'
        . 'error_count,metrics_json FROM '
        . table_name($config,'collection_runs')
        . ' WHERE run_id=? LIMIT 1 FOR UPDATE'
    );
    $statement->execute(array($expectation['run_id']));
    $existing = $statement->fetch();
    $statement->closeCursor();
    if (!$existing) {
        v1_dart_replay_error($pdo,'dart_replay_existing_run_missing');
    }
    if (!in_array(
        strtolower(trim((string)$existing['status'])),
        array('success','succeeded'),
        true
    )) {
        v1_dart_replay_error($pdo,'dart_replay_existing_run_not_successful');
    }
    if ((string)$existing['pipeline'] !== $expectation['pipeline']
        || strtolower(trim((string)$existing['source_key']))
            !== $expectation['source_key']
        || !is_string($existing['code_revision'])
        || !hash_equals(
            $expectation['code_revision'],
            strtolower((string)$existing['code_revision'])
        )) {
        v1_dart_replay_error($pdo,'dart_replay_revision_mismatch');
    }
    $metrics = json_decode((string)$existing['metrics_json'],true);
    if (!is_array($metrics)
        || !isset($metrics['stable_payload_contract_version'])
        || (int)$metrics['stable_payload_contract_version'] !== 1
        || !isset($metrics['stable_payload_sha256'])
        || !is_string($metrics['stable_payload_sha256'])
        || !hash_equals(
            $expectation['stable_payload_sha256'],
            strtolower((string)$metrics['stable_payload_sha256'])
        )
        || !isset($metrics['idempotency_key'])
        || !is_string($metrics['idempotency_key'])
        || !hash_equals(
            $expectation['idempotency_key'],
            (string)$metrics['idempotency_key']
        )) {
        v1_dart_replay_error($pdo,'dart_replay_semantic_mismatch');
    }
    foreach ($expectation['counts'] as $field=>$expectedCount) {
        if (!array_key_exists($field,$existing)
            || (int)$existing[$field] !== $expectedCount) {
            v1_dart_replay_error($pdo,'dart_replay_count_mismatch');
        }
    }
    $run = isset($payload['run']) && is_array($payload['run'])
        ? $payload['run'] : array();
    if ($run) {
        if (!isset($run['run_id'])
            || (string)$run['run_id'] !== $expectation['run_id']
            || !isset($run['stable_payload_sha256'])
            || !is_string($run['stable_payload_sha256'])
            || !hash_equals(
                $expectation['stable_payload_sha256'],
                strtolower((string)$run['stable_payload_sha256'])
            )) {
            v1_dart_replay_error($pdo,'dart_replay_semantic_mismatch');
        }
    }
    $counts = array(
        'companies'=>count(isset($payload['companies']) && is_array($payload['companies'])
            ? $payload['companies'] : array()),
        'documents'=>count(isset($payload['documents']) && is_array($payload['documents'])
            ? $payload['documents'] : array()),
        'events'=>count(isset($payload['events']) && is_array($payload['events'])
            ? $payload['events'] : array()),
        'actors'=>0,'event_actors'=>0,
        'source_rights'=>count(isset($payload['source_rights']) && is_array($payload['source_rights'])
            ? $payload['source_rights'] : array()),
        'source_rights_rejected'=>0,'event_documents'=>0,
        'event_observations'=>0,'timeline_entries'=>0,
        'editorial_revisions'=>0,'correction_link_ambiguous'=>0,
        'event_link_ambiguous'=>0,'runs'=>$run ? 1 : 0,
    );
    $pdo->commit();
    respond(200,array(
        'ok'=>true,
        'upserted'=>$counts,
        'backend_binding_id'=>$backendBindingId,
        'replay_verified'=>true,
        'replay_run_id'=>$expectation['run_id'],
        'stable_payload_sha256'=>$expectation['stable_payload_sha256'],
        'replay_attempted_at'=>$expectation['attempted_at'],
    ));
}

function upsert_governance_snapshot(
    PDO $pdo,
    array $config,
    array $payload,
    bool $dartGuardedAction = false
): void {
    $expectedBackendBindingId = isset($payload['expected_backend_binding_id'])
        ? trim((string)$payload['expected_backend_binding_id']) : '';
    if (preg_match('/^[a-f0-9]{64}$/',$expectedBackendBindingId) !== 1) {
        respond(400,array('ok'=>false,'error'=>'backend_binding_required'));
    }
    $backendBindingId = v1_backend_binding_id($pdo,$config);
    if (!hash_equals($backendBindingId,$expectedBackendBindingId)) {
        respond(409,array('ok'=>false,'error'=>'backend_binding_mismatch'));
    }
    $companies = isset($payload['companies']) && is_array($payload['companies']) ? $payload['companies'] : array();
    $documents = isset($payload['documents']) && is_array($payload['documents']) ? $payload['documents'] : array();
    $events = isset($payload['events']) && is_array($payload['events']) ? $payload['events'] : array();
    $rights = isset($payload['source_rights']) && is_array($payload['source_rights']) ? $payload['source_rights'] : array();
    $run = isset($payload['run']) && is_array($payload['run']) ? $payload['run'] : array();
    $dartReplayExpectation = v1_dart_replay_expectation($payload);
    if (count($companies) > 2000 || count($documents) > 2500 || count($events) > 2500 || count($rights) > 1000) {
        respond(413, array('ok' => false, 'error' => 'too_many_records'));
    }
    $explicitDartDocumentIdentities = array();
    foreach ($documents as $document) {
        $explicitDartIdentity = false;
        if (is_array($document)) {
            $submittedDocumentId = strtolower(trim((string)v1_first(
                $document,
                array('document_id'),
                ''
            )));
            $submittedSource = strtolower(trim((string)v1_first(
                $document,
                array('source','source_key'),
                ''
            )));
            $submittedSourceRightId = strtolower(trim((string)v1_first(
                $document,
                array('source_right_id'),
                ''
            )));
            $explicitDartIdentity = strpos($submittedDocumentId,'dart:') === 0
                || $submittedSource === 'dart'
                || $submittedSourceRightId === 'official:dart';
        }
        $explicitDartDocumentIdentities[] = $explicitDartIdentity;
    }
    $documents = v1_normalize_governance_snapshot_documents($documents);
    $globalDartBridgeEnabled = v1_global_dart_bridge_enabled($pdo,$config);
    $lineageCandidates = v1_governance_snapshot_lineage_candidates(
        $companies,
        $documents,
        $events,
        $run
    );
    $lineageCandidateCount = count($lineageCandidates['company_ids'])
        + count($lineageCandidates['document_ids'])
        + count($lineageCandidates['event_ids'])
        + count($lineageCandidates['comparison_keys'])
        + count($lineageCandidates['run_ids']);
    if ($lineageCandidateCount > 7500) {
        respond(413,array('ok'=>false,'error'=>'too_many_lineage_candidates'));
    }
    $containsDartWrite = false;
    $containsKindDocument = false;
    foreach ($documents as $documentIndex => $document) {
        if (!is_array($document)) { continue; }
        $documentId = strtolower(trim((string)v1_first($document,array('document_id'),'')));
        $declaredSource = strtolower(trim((string)v1_first($document,array('source','source_key'),'')));
        $sourceRightId = strtolower(trim((string)v1_first($document,array('source_right_id'),'')));
        $isDart = strpos($documentId,'dart:') === 0 || $declaredSource === 'dart'
            || $sourceRightId === 'official:dart';
        if ($isDart && $sourceRightId !== 'official:dart') {
            if (!$dartGuardedAction
                && !empty($explicitDartDocumentIdentities[$documentIndex])) {
                respond(409,array(
                    'ok'=>false,
                    'error'=>'dart_guarded_action_required',
                ));
            }
            respond(409,array('ok'=>false,'error'=>'dart_document_requires_approved_source_right'));
        }
        if ($isDart) { $containsDartWrite = true; }
        $isKind = strpos($documentId,'kind:') === 0 || $declaredSource === 'kind' || $sourceRightId === 'official:kind';
        if ($isKind && $sourceRightId !== 'official:kind') {
            respond(409,array('ok'=>false,'error'=>'kind_document_requires_approved_source_right'));
        }
        if ($isKind) { $containsKindDocument = true; }
    }
    foreach ($lineageCandidates['document_ids'] as $lineageDocumentId) {
        if (strpos(strtolower($lineageDocumentId),'dart:') === 0) {
            $containsDartWrite = true;
        }
    }
    $runSourceTokens = array_values(array_filter(array_map(
        'trim',
        explode('+',strtolower((string)v1_first($run,array('source_key'),'')))
    )));
    if (in_array('dart',$runSourceTokens,true)) { $containsDartWrite = true; }
    foreach ($rights as $right) {
        if (!is_array($right)) { continue; }
        if (strtolower(trim((string)v1_first(
            $right,
            array('source_right_id'),
            ''
        ))) === 'official:dart') {
            $containsDartWrite = true;
            respond(409,array(
                'ok'=>false,
                'error'=>'dart_source_right_managed_out_of_band',
            ));
        }
    }
    $dartExpectation = v1_dart_source_right_expectation($payload);
    $dartDeploymentExpectation = v1_dart_deployment_expectation($payload);
    $dartReleaseStateExpectation = v1_dart_release_state_expectation($payload);
    if (isset($payload['expected_source_right_revisions'])
        && !is_array($payload['expected_source_right_revisions'])) {
        respond(400,array('ok'=>false,'error'=>'invalid_source_right_precondition'));
    }
    if ($dartExpectation !== null) { $containsDartWrite = true; }
    if ($containsDartWrite && !$dartGuardedAction) {
        respond(409,array(
            'ok'=>false,
            'error'=>'dart_guarded_action_required',
        ));
    }
    if ($dartGuardedAction && !$globalDartBridgeEnabled) {
        respond(503,array('ok'=>false,'error'=>'dart_global_bridge_unavailable'));
    }
    if ($dartGuardedAction && $dartExpectation === null) {
        respond(409,array(
            'ok'=>false,
            'error'=>'dart_source_right_precondition_required',
        ));
    }
    if ($dartGuardedAction && $dartDeploymentExpectation === null) {
        respond(409,array(
            'ok'=>false,
            'error'=>'dart_deployment_revision_required',
        ));
    }
    if ($dartGuardedAction && $dartReleaseStateExpectation === null) {
        respond(409,array(
            'ok'=>false,
            'error'=>'dart_release_state_precondition_required',
        ));
    }
    if ($dartReplayExpectation !== null && !$dartGuardedAction) {
        respond(409,array(
            'ok'=>false,
            'error'=>'dart_guarded_action_required',
        ));
    }
    // This becomes true only after stored lineage, release state, deployment,
    // and SourceRight have all been revalidated under the transaction guards.
    $globalDartProjectionEnabled = false;
    $now = gmdate('Y-m-d H:i:s');
    $counts = array('companies' => 0, 'documents' => 0, 'events' => 0, 'actors' => 0, 'event_actors' => 0,
        'source_rights' => 0, 'source_rights_rejected' => 0,
        'event_documents' => 0, 'event_observations' => 0, 'timeline_entries' => 0, 'editorial_revisions' => 0, 'correction_link_ambiguous' => 0,
        'event_link_ambiguous' => 0, 'runs' => 0);
    $followupDocumentIds = array();
    $documentSourceClasses = array();
    $documentSourceRightIds = array();
    $terminalCompletionFailure = false;
    foreach ($events as $event) {
        if (!is_array($event) || (empty($event['is_correction']) && empty($event['is_cancelled']))) { continue; }
        $documentIds = isset($event['document_ids']) && is_array($event['document_ids']) ? $event['document_ids'] : array();
        if (isset($event['document_id'])) { $documentIds[] = $event['document_id']; }
        foreach ($documentIds as $documentId) {
            $documentId = trim((string)$documentId);
            if (v1_valid_entity_id($documentId)) { $followupDocumentIds[$documentId] = true; }
        }
    }
    $pdo->beginTransaction();
    try {
        // Match the shared DART lock order before resolving stored lineage:
        // release-state rows, SourceRight, connector, existing lineage, then
        // data rows. An event-only payload therefore cannot hide its existing
        // DART provenance or race an administrative connector kill switch.
        $dartReleaseStates = null;
        $dartRight = null;
        $dartConnector = null;
        $mustResolveStoredLineage = $lineageCandidateCount > 0;
        if ($globalDartBridgeEnabled
            && ($dartGuardedAction
                || $containsDartWrite
                || $mustResolveStoredLineage)) {
            if (!function_exists('v2_release_state_rows_for_update')) {
                throw new RuntimeException('global_release_state_guard_unavailable');
            }
            $dartReleaseStates = v2_release_state_rows_for_update($pdo,$config);
            $dartRight = v2_source_right_row(
                $pdo,
                $config,
                'official:dart',
                true
            );
            $dartConnector = v1_lock_global_dart_connector($pdo,$config);
        } elseif (v1_release_state($pdo,$config,true) === null) {
            throw new RuntimeException('release_state_unavailable');
        }
        if ($mustResolveStoredLineage
            && v1_lock_existing_dart_lineage(
                $pdo,
                $config,
                $lineageCandidates,
                $globalDartBridgeEnabled
            )) {
            $containsDartWrite = true;
        }
        if ($containsDartWrite && !$dartGuardedAction) {
            $pdo->rollBack();
            respond(409,array(
                'ok'=>false,
                'error'=>'dart_guarded_action_required',
            ));
        }
        if ($dartGuardedAction && !$containsDartWrite) {
            $pdo->rollBack();
            respond(409,array(
                'ok'=>false,
                'error'=>'dart_guarded_payload_required',
            ));
        }
        if ($containsDartWrite && !$globalDartBridgeEnabled) {
            $pdo->rollBack();
            respond(503,array(
                'ok'=>false,
                'error'=>'dart_global_bridge_unavailable',
            ));
        }
        if ($containsDartWrite) {
            $storedV1ReleaseState = $dartReleaseStates === null
                ? '' : (string)$dartReleaseStates[GOV_V1_RELEASE_STATE_KEY]['release_state'];
            $storedV2ReleaseState = $dartReleaseStates === null
                ? '' : (string)$dartReleaseStates[GOV_V2_RELEASE_STATE_KEY]['release_state'];
            if ($dartReleaseStates === null
                || $storedV1ReleaseState !== $storedV2ReleaseState
                || $storedV1ReleaseState !== $dartReleaseStateExpectation) {
                $pdo->rollBack();
                respond(409,array(
                    'ok'=>false,
                    'error'=>'dart_release_state_mismatch',
                ));
            }
            $deploymentIdentity = v2_deployment_identity_status();
            if ($deploymentIdentity['valid'] !== true
                || !isset($deploymentIdentity['code_revision'])
                || !is_string($deploymentIdentity['code_revision'])
                || !hash_equals(
                    $dartDeploymentExpectation,
                    (string)$deploymentIdentity['code_revision']
                )) {
                $pdo->rollBack();
                respond(409,array(
                    'ok'=>false,
                    'error'=>'dart_deployment_revision_mismatch',
                ));
            }
            $dartReasons = v2_source_right_ineligible_reasons(
                $dartRight,
                'collect'
            );
            $dartRevision = $dartRight === null
                ? null : v2_source_right_revision($dartRight);
            $dartContractRevision = $dartRight === null
                ? null : v2_source_right_contract_revision($dartRight);
            if ($dartReasons
                || $dartRevision === null
                || $dartContractRevision === null
                || !hash_equals(
                    $dartExpectation['rights_revision'],
                    $dartRevision
                )
                || !hash_equals(
                    $dartExpectation['contract_revision'],
                    $dartContractRevision
                )) {
                $pdo->rollBack();
                respond(409,array(
                    'ok'=>false,
                    'error'=>'dart_source_right_ineligible_or_changed',
                    'source_right_id'=>'official:dart',
                    'reasons'=>$dartReasons,
                ));
            }
            $dartConnectorIdentityValid = is_array($dartConnector)
                && (string)v1_first($dartConnector,array('connector_id'),'')
                    === 'connector:kr:dart'
                && (string)v1_first($dartConnector,array('country_code'),'') === 'KR'
                && (string)v1_first($dartConnector,array('source_key'),'') === 'dart'
                && (string)v1_first($dartConnector,array('source_type'),'')
                    === 'official_disclosure'
                && (string)v1_first($dartConnector,array('source_right_id'),'')
                    === 'official:dart';
            if (!$dartConnectorIdentityValid) {
                $pdo->rollBack();
                respond(409,array(
                    'ok'=>false,
                    'error'=>'dart_connector_unavailable',
                ));
            }
            $dartConnectorStatus = (string)v1_first(
                $dartConnector,
                array('connector_status'),
                ''
            );
            if (!in_array(
                $dartConnectorStatus,
                array('configured','active'),
                true
            )) {
                $pdo->rollBack();
                respond(409,array(
                    'ok'=>false,
                    'error'=>$dartConnectorStatus === 'inactive'
                        ? 'dart_connector_inactive'
                        : 'dart_connector_not_ready',
                ));
            }
            $globalDartProjectionEnabled = true;
            if ($dartReplayExpectation !== null) {
                // This is intentionally before the first company/document/event
                // upsert. The existing successful apply run is the immutable
                // replay receipt; an exact replay only acknowledges it.
                v1_dart_replay_read_only_ack(
                    $pdo,
                    $config,
                    $dartReplayExpectation,
                    $payload,
                    $backendBindingId
                );
            }
        }
        // This lock is intentionally acquired before processing source_rights
        // from the HMAC payload. A collector cannot bootstrap its own KIND
        // permission and use it in the same transaction.
        if ($containsKindDocument) {
            $kindEligibility = v1_kind_source_right_eligibility($pdo,$config,true);
            if (!$kindEligibility['eligible']) {
                $pdo->rollBack();
                respond(409,array('ok'=>false,'error'=>'kind_source_right_ineligible','source_right_id'=>'official:kind',
                    'eligible'=>false,'rights_revision'=>$kindEligibility['rights_revision'],'reasons'=>$kindEligibility['reasons']));
            }
        }
        // An incomplete correction/cancellation is deliberately isolated under
        // its receipt-derived event_id. Once stored, only an exact semantic
        // replay is accepted; DART's one-way later-correction marker is kept
        // as a separate append-only lifecycle observation without rewriting
        // the row or evidence document. Perform this check before the first
        // company/document upsert so a reused ID cannot transiently rewrite
        // content even inside this transaction.
        $isolatedReplayEventStmt = $pdo->prepare('SELECT event_id,company_id,issuer_id,country_code,global_event_family,event_type,title,original_language,summary,'
            . 'occurred_at,deadline_at,importance,current_status,collection_key,identity_action,identity_target,identity_actor_id,'
            . 'identity_effective_at,identity_deadline_at,identity_status,comparison_key,verification_status,review_status,publication_status,payload_json FROM '
            . table_name($config,'governance_events') . ' WHERE event_id=? LIMIT 1 FOR UPDATE');
        $isolatedReplayDocumentsStmt = $pdo->prepare('SELECT ed.document_id,ed.relation_type,ed.position_no,d.company_id,d.source_right_id,d.source_class,d.external_id,'
            . 'd.document_type,d.original_language,d.title,d.body_text,d.original_url,d.content_hash,d.collection_key,d.published_at,'
            . 'd.retrieved_at,d.verification_status,d.publication_status,d.correction_of_document_id,d.version_no,d.payload_json FROM '
            . table_name($config,'event_documents') . ' ed'
            . ' JOIN ' . table_name($config,'documents') . ' d ON d.document_id=ed.document_id'
            . ' WHERE ed.event_id=? ORDER BY ed.position_no,ed.document_id FOR UPDATE');
        $isolatedReplayDocumentOwnersStmt = $pdo->prepare('SELECT ed.document_id,e.event_id,e.company_id,e.issuer_id,e.country_code,'
            . 'e.identity_status,e.comparison_key,e.verification_status,e.review_status,e.publication_status,e.payload_json FROM '
            . table_name($config,'event_documents') . ' ed JOIN ' . table_name($config,'governance_events') . ' e'
            . ' ON e.event_id=ed.event_id WHERE ed.document_id=? ORDER BY e.event_id FOR UPDATE');
        $submittedDocumentsById = array();
        $duplicateSubmittedDocumentIds = array();
        $isolatedReplayDocumentSnapshots = array();
        $isolatedReplayCanonicalEventPayloads = array();
        $readOnlyDartIdentityMutationEventIds = array();
        $readOnlyDartReviewedEventCompanyIds = array();
        $readOnlyDartReviewedDocumentIds = array();
        $pendingDartLifecycleObservations = array();
        $approvedIsolatedReplayEventIds = array();
        $submittedDocumentReferenceEventIds = array();
        $dartLifecycleObservationByIdStmt = null;
        $dartLifecycleObservationInsertStmt = null;
        $reviewedDartIdentityActorStmt = null;
        $rejectedDartIdentityActorStmt = null;
        $reviewedDartEventObservationsStmt = null;
        if ($globalDartProjectionEnabled) {
            $dartLifecycleObservationByIdStmt = $pdo->prepare(
                'SELECT connector_id,country_code,source_key,external_id,'
                . 'parent_external_id,change_type,payload_json,resolution_status,'
                . 'resolved_document_id,resolved_event_id FROM '
                . table_name($config,'global_lifecycle_observations')
                . ' WHERE observation_id=? LIMIT 1 FOR UPDATE'
            );
            $dartLifecycleObservationInsertStmt = $pdo->prepare(
                'INSERT INTO ' . table_name($config,'global_lifecycle_observations')
                . ' (observation_id,connector_id,country_code,source_key,external_id,'
                . 'parent_external_id,change_type,observed_at,payload_json,'
                . 'resolution_status,resolved_document_id,resolved_event_id,'
                . 'created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
            );
            $reviewedDartIdentityActorStmt = $pdo->prepare(
                'SELECT reviewed_replay_ea.actor_id FROM ' . table_name($config,'event_actors')
                . ' reviewed_replay_ea JOIN ' . table_name($config,'actors')
                . ' reviewed_replay_a'
                . ' ON reviewed_replay_a.actor_id=reviewed_replay_ea.actor_id'
                . ' WHERE reviewed_replay_ea.event_id=?'
                . ' AND reviewed_replay_ea.actor_id=?'
                . ' AND reviewed_replay_ea.actor_role=\'filer\''
                . ' AND reviewed_replay_ea.review_status=\'approved\''
                . ' AND reviewed_replay_a.review_status=\'approved\''
                . ' AND reviewed_replay_a.record_status=\'active\''
                . ' AND reviewed_replay_a.country_code=\'KR\''
                . ' ORDER BY reviewed_replay_ea.actor_id LIMIT 2 FOR UPDATE'
            );
            $rejectedDartIdentityActorStmt = $pdo->prepare(
                'SELECT rejected_replay_ea.actor_id,'
                . 'rejected_replay_ea.actor_role,'
                . 'rejected_replay_ea.review_status AS relation_review_status,'
                . 'rejected_replay_a.actor_type,'
                . 'rejected_replay_a.display_name,'
                . 'rejected_replay_a.company_id,'
                . 'rejected_replay_a.country_code,'
                . 'rejected_replay_a.review_status AS actor_review_status,'
                . 'rejected_replay_a.record_status FROM '
                . table_name($config,'event_actors')
                . ' rejected_replay_ea JOIN ' . table_name($config,'actors')
                . ' rejected_replay_a'
                . ' ON rejected_replay_a.actor_id=rejected_replay_ea.actor_id'
                . ' WHERE rejected_replay_ea.event_id=?'
                . ' AND rejected_replay_ea.actor_id=?'
                . ' AND rejected_replay_ea.actor_role=\'filer\''
                . ' ORDER BY rejected_replay_ea.actor_id LIMIT 2 FOR UPDATE'
            );
            $reviewedDartEventObservationsStmt = $pdo->prepare(
                'SELECT observation_id,event_id,document_id,source_class,'
                . 'source_key FROM '
                . table_name($config,'event_observations')
                . ' WHERE event_id=? ORDER BY observation_id FOR UPDATE'
            );
        }
        foreach ($documents as $submittedDocument) {
            if (!is_array($submittedDocument)) { continue; }
            $submittedDocumentId = v1_governance_snapshot_document_id($submittedDocument);
            if (v1_valid_entity_id($submittedDocumentId)) {
                if (isset($submittedDocumentsById[$submittedDocumentId])) {
                    $duplicateSubmittedDocumentIds[$submittedDocumentId] = true;
                }
                $submittedDocumentsById[$submittedDocumentId] = $submittedDocument;
            }
        }
        foreach ($events as $submittedEvent) {
            if (!is_array($submittedEvent)) { continue; }
            $submittedEventId = trim((string)v1_first($submittedEvent,array('event_id'),''));
            $submittedCompanyId = trim((string)v1_first($submittedEvent,array('company_id','corp_code'),''));
            $submittedEventDocumentIds = isset($submittedEvent['document_ids']) && is_array($submittedEvent['document_ids'])
                ? $submittedEvent['document_ids'] : array();
            if (isset($submittedEvent['document_id'])) {
                array_unshift($submittedEventDocumentIds,$submittedEvent['document_id']);
            }
            foreach ($submittedEventDocumentIds as $submittedEventDocumentId) {
                if (!is_string($submittedEventDocumentId) && !is_int($submittedEventDocumentId)) { continue; }
                $submittedEventDocumentId = trim((string)$submittedEventDocumentId);
                if (v1_valid_entity_id($submittedEventDocumentId)) {
                    if (!isset($submittedDocumentReferenceEventIds[$submittedEventDocumentId])) {
                        $submittedDocumentReferenceEventIds[$submittedEventDocumentId] = array();
                    }
                    $submittedReferenceEventId = v1_valid_entity_id($submittedEventId)
                        ? $submittedEventId : '__invalid_event_id__';
                    $submittedDocumentReferenceEventIds[$submittedEventDocumentId][$submittedReferenceEventId] = true;
                }
            }
            if (!v1_valid_entity_id($submittedEventId)) {
                continue;
            }
            $isolatedReplayEvent = v1_pdo_fetch_one_and_close(
                $isolatedReplayEventStmt,
                array($submittedEventId)
            );
            if (!$isolatedReplayEvent) {
                continue;
            }
            $storedEventPayload = json_decode((string)$isolatedReplayEvent['payload_json'],true);
            $reviewedDartEventProtected =
                v1_dart_reviewed_event_is_protected($isolatedReplayEvent);
            $rejectedDartEventProtected =
                v1_dart_rejected_event_is_protected($isolatedReplayEvent);
            if ($reviewedDartEventProtected || $rejectedDartEventProtected) {
                if (!$globalDartProjectionEnabled
                    || !$dartGuardedAction
                    || !$reviewedDartIdentityActorStmt
                    || !$rejectedDartIdentityActorStmt
                    || !$reviewedDartEventObservationsStmt
                    || !is_array($storedEventPayload)) {
                    throw new RuntimeException(
                        'followup_event_identity_conflict:' . $submittedEventId
                    );
                }
                $reviewedEventReplay = $reviewedDartEventProtected
                    ? v1_dart_reviewed_event_replay(
                        $isolatedReplayEvent,
                        $storedEventPayload,
                        $submittedEvent
                    )
                    : v1_dart_rejected_event_replay(
                        $isolatedReplayEvent,
                        $storedEventPayload,
                        $submittedEvent
                    );
                if ($reviewedEventReplay === null) {
                    throw new RuntimeException(
                        'followup_event_identity_conflict:' . $submittedEventId
                    );
                }
                $storedDocumentRows = v1_pdo_fetch_all_and_close(
                    $isolatedReplayDocumentsStmt,
                    array($submittedEventId)
                );
                $rawSubmittedDocumentIds = isset($submittedEvent['document_ids'])
                    && is_array($submittedEvent['document_ids'])
                    ? $submittedEvent['document_ids'] : array();
                if (isset($submittedEvent['document_id'])) {
                    array_unshift(
                        $rawSubmittedDocumentIds,
                        $submittedEvent['document_id']
                    );
                }
                $submittedDocumentIdSet = array();
                foreach ($rawSubmittedDocumentIds as $rawSubmittedDocumentId) {
                    if (!is_string($rawSubmittedDocumentId)
                        && !is_int($rawSubmittedDocumentId)) {
                        continue;
                    }
                    $submittedDocumentId =
                        trim((string)$rawSubmittedDocumentId);
                    if (v1_valid_entity_id($submittedDocumentId)) {
                        $submittedDocumentIdSet[$submittedDocumentId] = true;
                    }
                }
                if (count($storedDocumentRows) !== 1
                    || count($submittedDocumentIdSet) !== 1) {
                    throw new RuntimeException(
                        'followup_event_identity_conflict:' . $submittedEventId
                    );
                }
                $storedDocumentRow = $storedDocumentRows[0];
                $storedDocumentId = trim((string)(
                    $storedDocumentRow['document_id'] ?? ''
                ));
                $submittedDocumentIds = array_keys($submittedDocumentIdSet);
                $submittedDocumentId = (string)$submittedDocumentIds[0];
                if ($storedDocumentId === ''
                    || $storedDocumentId !== $submittedDocumentId
                    || (string)($storedDocumentRow['relation_type'] ?? '')
                        !== 'evidence'
                    || (int)($storedDocumentRow['position_no'] ?? -1) !== 0
                    || isset($duplicateSubmittedDocumentIds[$storedDocumentId])
                    || !isset($submittedDocumentsById[$storedDocumentId])
                    || !v1_dart_identity_change_document_matches(
                        $storedDocumentRow,
                        $submittedDocumentsById[$storedDocumentId],
                        !empty($reviewedEventReplay['is_correction'])
                    )) {
                    throw new RuntimeException(
                        'followup_event_identity_conflict:' . $submittedEventId
                    );
                }
                $storedDocumentOwners = v1_pdo_fetch_all_and_close(
                    $isolatedReplayDocumentOwnersStmt,
                    array($storedDocumentId)
                );
                $submittedReferenceOwners = isset(
                    $submittedDocumentReferenceEventIds[$storedDocumentId]
                ) ? array_keys(
                    $submittedDocumentReferenceEventIds[$storedDocumentId]
                ) : array();
                $canonicalActorId =
                    (string)$reviewedEventReplay['canonical_actor_id'];
                $approvedActorRows = v1_pdo_fetch_all_and_close(
                    $rejectedDartEventProtected
                        ? $rejectedDartIdentityActorStmt
                        : $reviewedDartIdentityActorStmt,
                    array($submittedEventId,$canonicalActorId)
                );
                $storedObservationRows = v1_pdo_fetch_all_and_close(
                    $reviewedDartEventObservationsStmt,
                    array($submittedEventId)
                );
                if (count($storedDocumentOwners) !== 1
                    || count($submittedReferenceOwners) !== 1
                    || (string)$submittedReferenceOwners[0]
                        !== $submittedEventId
                    || (string)($storedDocumentOwners[0]['document_id'] ?? '')
                        !== $storedDocumentId
                    || (string)($storedDocumentOwners[0]['event_id'] ?? '')
                        !== $submittedEventId
                    || (string)($storedDocumentOwners[0]['company_id'] ?? '')
                        !== $submittedCompanyId
                    || (string)($storedDocumentOwners[0]['identity_status'] ?? '')
                        !== ($rejectedDartEventProtected
                            ? 'rejected' : 'complete')
                    || count($storedObservationRows) !== 1
                    || (string)($storedObservationRows[0]['event_id'] ?? '')
                        !== $submittedEventId
                    || (string)($storedObservationRows[0]['document_id'] ?? '')
                        !== $storedDocumentId
                    || (string)($storedObservationRows[0]['source_class'] ?? '')
                        !== 'official_disclosure'
                    || strtolower((string)(
                        $storedObservationRows[0]['source_key'] ?? ''
                    )) !== 'dart'
                    || count($approvedActorRows) !== 1) {
                    throw new RuntimeException(
                        'followup_event_identity_conflict:' . $submittedEventId
                    );
                }
                if ($rejectedDartEventProtected) {
                    $rejectedActorRow = $approvedActorRows[0];
                    $actorReviewStatus = (string)(
                        $rejectedActorRow['actor_review_status'] ?? ''
                    );
                    $actorRecordStatus = (string)(
                        $rejectedActorRow['record_status'] ?? ''
                    );
                    $actorCountryCode = (string)(
                        $rejectedActorRow['country_code'] ?? ''
                    );
                    $relationReviewStatus = (string)(
                        $rejectedActorRow['relation_review_status'] ?? ''
                    );
                    $pendingActorState = $actorReviewStatus === 'pending'
                        && $actorRecordStatus === 'inactive'
                        && $actorCountryCode === '';
                    $approvedActorState = $actorReviewStatus === 'approved'
                        && $actorRecordStatus === 'active'
                        && $actorCountryCode === 'KR';
                    if ((string)($rejectedActorRow['actor_id'] ?? '')
                            !== $canonicalActorId
                        || (string)($rejectedActorRow['actor_role'] ?? '')
                            !== 'filer'
                        || (string)($rejectedActorRow['actor_type'] ?? '')
                            !== (string)$reviewedEventReplay['actor_type']
                        || v1_normalize_identity_text((string)(
                            $rejectedActorRow['display_name'] ?? ''
                        )) !== v1_normalize_identity_text((string)
                            $reviewedEventReplay['actor_display_name'])
                        || trim((string)(
                            $rejectedActorRow['company_id'] ?? ''
                        )) !== (string)$reviewedEventReplay['actor_company_id']
                        || (!$pendingActorState && !$approvedActorState)
                        || !in_array(
                            $relationReviewStatus,
                            array('pending','approved'),
                            true
                        )
                        || ($relationReviewStatus === 'approved'
                            && !$approvedActorState)) {
                        throw new RuntimeException(
                            'followup_event_identity_conflict:'
                            . $submittedEventId
                        );
                    }
                }
                $isolatedReplayDocumentSnapshots[$storedDocumentId] =
                    $storedDocumentRow;
                $readOnlyDartIdentityMutationEventIds[$submittedEventId] =
                    array(
                        'event_documents'=>count($storedDocumentRows),
                        'event_observations'=>count($storedObservationRows),
                    );
                $readOnlyDartReviewedEventCompanyIds[$submittedEventId] =
                    $submittedCompanyId;
                $readOnlyDartReviewedDocumentIds[$storedDocumentId] = true;
                continue;
            }
            if ((string)$isolatedReplayEvent['identity_status']
                !== 'needs_review') {
                continue;
            }
            $storedFollowupFlag = is_array($storedEventPayload)
                && (!empty($storedEventPayload['is_correction']) || !empty($storedEventPayload['is_cancelled']));
            $storedIsolationMarker = is_array($storedEventPayload)
                ? (string)($storedEventPayload['event_link_status'] ?? '') : '';
            $storedFollowupLifecycle = in_array(
                (string)($isolatedReplayEvent['verification_status'] ?? ''),
                array('corrected','withdrawn'),
                true
            );
            if (!$storedFollowupFlag && $storedIsolationMarker === '' && !$storedFollowupLifecycle) {
                $dartIdentityActorChange = $globalDartProjectionEnabled && $dartGuardedAction
                    && is_array($storedEventPayload)
                    ? v1_dart_pending_identity_actor_change(
                        $isolatedReplayEvent,
                        $storedEventPayload,
                        $submittedEvent
                    )
                    : null;
                if ($dartIdentityActorChange === null) {
                    continue;
                }
                $storedDocumentRows = v1_pdo_fetch_all_and_close(
                    $isolatedReplayDocumentsStmt,
                    array($submittedEventId)
                );
                $rawSubmittedDocumentIds = isset($submittedEvent['document_ids'])
                    && is_array($submittedEvent['document_ids'])
                    ? $submittedEvent['document_ids'] : array();
                if (isset($submittedEvent['document_id'])) {
                    array_unshift($rawSubmittedDocumentIds,$submittedEvent['document_id']);
                }
                $submittedDocumentIdSet = array();
                foreach ($rawSubmittedDocumentIds as $rawSubmittedDocumentId) {
                    if (!is_string($rawSubmittedDocumentId)
                        && !is_int($rawSubmittedDocumentId)) {
                        continue;
                    }
                    $submittedDocumentId = trim((string)$rawSubmittedDocumentId);
                    if (v1_valid_entity_id($submittedDocumentId)) {
                        $submittedDocumentIdSet[$submittedDocumentId] = true;
                    }
                }
                if (count($storedDocumentRows) !== 1
                    || count($submittedDocumentIdSet) !== 1) {
                    throw new RuntimeException(
                        'event_identity_field_conflict:' . $submittedEventId
                    );
                }
                $storedDocumentRow = $storedDocumentRows[0];
                $storedDocumentId = trim((string)($storedDocumentRow['document_id'] ?? ''));
                $submittedDocumentIds = array_keys($submittedDocumentIdSet);
                $submittedDocumentId = (string)$submittedDocumentIds[0];
                if ($storedDocumentId === ''
                    || $storedDocumentId !== $submittedDocumentId
                    || (string)($storedDocumentRow['relation_type'] ?? '') !== 'evidence'
                    || (int)($storedDocumentRow['position_no'] ?? -1) !== 0
                    || isset($duplicateSubmittedDocumentIds[$storedDocumentId])
                    || !isset($submittedDocumentsById[$storedDocumentId])
                    || !v1_dart_identity_change_document_matches(
                        $storedDocumentRow,
                        $submittedDocumentsById[$storedDocumentId]
                    )) {
                    throw new RuntimeException(
                        'event_identity_field_conflict:' . $submittedEventId
                    );
                }
                $storedDocumentOwners = v1_pdo_fetch_all_and_close(
                    $isolatedReplayDocumentOwnersStmt,
                    array($storedDocumentId)
                );
                $submittedReferenceOwners = isset(
                    $submittedDocumentReferenceEventIds[$storedDocumentId]
                ) ? array_keys(
                    $submittedDocumentReferenceEventIds[$storedDocumentId]
                ) : array();
                if (count($storedDocumentOwners) !== 1
                    || count($submittedReferenceOwners) !== 1
                    || (string)$submittedReferenceOwners[0] !== $submittedEventId
                    || (string)($storedDocumentOwners[0]['document_id'] ?? '')
                        !== $storedDocumentId
                    || (string)($storedDocumentOwners[0]['event_id'] ?? '')
                        !== $submittedEventId
                    || (string)($storedDocumentOwners[0]['company_id'] ?? '')
                        !== $submittedCompanyId
                    || (string)($storedDocumentOwners[0]['identity_status'] ?? '')
                        !== 'needs_review') {
                    throw new RuntimeException(
                        'event_identity_field_conflict:' . $submittedEventId
                    );
                }
                $lifecycleObservation = array(
                    'document_id'=>$storedDocumentId,
                    'event_id'=>$submittedEventId,
                    'observation_id'=>v1_stable_id(
                        'dart-lifecycle',
                        'identity-actor-change-v1|' . $submittedEventId . '|'
                            . $storedDocumentId . '|'
                            . (string)$dartIdentityActorChange['previous_identity_sha256']
                            . '|'
                            . (string)$dartIdentityActorChange['current_identity_sha256']
                    ),
                    'country_code'=>'KR',
                    'source_key'=>'dart',
                    'external_id'=>(string)$storedDocumentRow['external_id'],
                    'parent_external_id'=>null,
                    'change_type'=>'updated',
                    'metadata'=>array(
                        'source_semantics'=>'event_identity_changed',
                        'conflict_field'=>'identity_actor_id',
                        'source_right_id'=>'official:dart',
                        'previous_identity_sha256'=>
                            (string)$dartIdentityActorChange['previous_identity_sha256'],
                        'current_identity_sha256'=>
                            (string)$dartIdentityActorChange['current_identity_sha256'],
                    ),
                );
                if (!$dartLifecycleObservationByIdStmt) {
                    throw new RuntimeException('global_lifecycle_guard_unavailable');
                }
                $existingLifecycleObservation = v1_pdo_fetch_one_and_close(
                    $dartLifecycleObservationByIdStmt,
                    array($lifecycleObservation['observation_id'])
                );
                if ($existingLifecycleObservation
                    && !v1_dart_lifecycle_observation_matches(
                        $existingLifecycleObservation,
                        $lifecycleObservation
                    )) {
                    throw new RuntimeException(
                        'event_identity_field_conflict:' . $submittedEventId
                    );
                }
                $pendingDartLifecycleObservations[] = $lifecycleObservation;
                $isolatedReplayDocumentSnapshots[$storedDocumentId] =
                    $storedDocumentRow;
                $isolatedReplayCanonicalEventPayloads[$submittedEventId] =
                    $storedEventPayload;
                $readOnlyDartIdentityMutationEventIds[$submittedEventId] =
                    array(
                        'event_documents'=>count($storedDocumentRows),
                        // Preserve the existing ACK contract: this guarded
                        // event already owns one observation per evidence row.
                        'event_observations'=>count($storedDocumentRows),
                    );
                $approvedIsolatedReplayEventIds[$submittedEventId] = true;
                continue;
            }
            if (!$storedFollowupFlag || $storedIsolationMarker !== 'ambiguous_independent') {
                throw new RuntimeException('followup_event_identity_conflict:' . $submittedEventId);
            }
            if ((string)$isolatedReplayEvent['event_id'] !== $submittedEventId) {
                throw new RuntimeException('followup_event_identity_conflict:' . $submittedEventId);
            }
            $storedDocumentRows = v1_pdo_fetch_all_and_close(
                $isolatedReplayDocumentsStmt,
                array($submittedEventId)
            );
            $canonicalSubmittedEvent = $submittedEvent;
            $canonicalSubmittedEvent['event_link_status'] = 'ambiguous_independent';
            $isolatedHasOfficialDartEvidence = false;
            foreach ($storedDocumentRows as $storedDocumentRow) {
                if ((string)($storedDocumentRow['source_class'] ?? '') === 'official_disclosure'
                    && strtolower((string)($storedDocumentRow['source_right_id'] ?? '')) === 'official:dart') {
                    $isolatedHasOfficialDartEvidence = true;
                    break;
                }
            }
            if ($isolatedHasOfficialDartEvidence) {
                if (!isset($canonicalSubmittedEvent['metadata'])) {
                    $canonicalSubmittedEvent['metadata'] = array();
                }
                if (is_array($canonicalSubmittedEvent['metadata'])
                    && !isset($canonicalSubmittedEvent['metadata']['title_provenance'])) {
                    $canonicalSubmittedEvent['metadata']['title_provenance'] = 'source';
                }
            }
            $eventDartMarkerUpgrade = false;
            $allowDartMarkerUpgrade = $globalDartProjectionEnabled
                && $dartGuardedAction
                && $isolatedHasOfficialDartEvidence;
            $eventCanCreateDartLifecycleObservation =
                (string)$isolatedReplayEvent['review_status'] === 'pending'
                && (string)$isolatedReplayEvent['publication_status'] === 'draft';
            if (!is_array($storedEventPayload)
                || !v1_followup_event_replay_payload_matches(
                    $storedEventPayload,
                    $canonicalSubmittedEvent,
                    $allowDartMarkerUpgrade,
                    $eventDartMarkerUpgrade
                )) {
                throw new RuntimeException('followup_event_identity_conflict:' . $submittedEventId);
            }
            $rawSubmittedDocumentIds = isset($submittedEvent['document_ids']) && is_array($submittedEvent['document_ids'])
                ? $submittedEvent['document_ids'] : array();
            if (isset($submittedEvent['document_id'])) {
                array_unshift($rawSubmittedDocumentIds,$submittedEvent['document_id']);
            }
            $submittedDocumentIdSet = array();
            foreach ($rawSubmittedDocumentIds as $rawSubmittedDocumentId) {
                if (!is_string($rawSubmittedDocumentId) && !is_int($rawSubmittedDocumentId)) { continue; }
                $submittedDocumentId = trim((string)$rawSubmittedDocumentId);
                if (v1_valid_entity_id($submittedDocumentId)) {
                    $submittedDocumentIdSet[$submittedDocumentId] = true;
                }
            }
            $submittedDocumentIdsOrdered = array_keys($submittedDocumentIdSet);
            $submittedDocumentIdsSorted = $submittedDocumentIdsOrdered;
            sort($submittedDocumentIdsSorted,SORT_STRING);
            $storedDocumentIds = array();
            $storedDocumentRelations = array();
            foreach ($storedDocumentRows as $storedDocumentRow) {
                $storedDocumentId = trim((string)($storedDocumentRow['document_id'] ?? ''));
                if ($storedDocumentId !== '') {
                    if (isset($duplicateSubmittedDocumentIds[$storedDocumentId])) {
                        throw new RuntimeException('followup_event_identity_conflict:' . $submittedEventId);
                    }
                    $storedDocumentIds[] = $storedDocumentId;
                    $storedDocumentRelations[] = array(
                        $storedDocumentId,
                        (string)($storedDocumentRow['relation_type'] ?? ''),
                        (int)($storedDocumentRow['position_no'] ?? -1),
                    );
                }
            }
            sort($storedDocumentIds,SORT_STRING);
            $submittedDocumentRelations = array();
            foreach ($submittedDocumentIdsOrdered as $submittedPosition => $submittedDocumentId) {
                $submittedDocumentRelations[] = array($submittedDocumentId,'evidence',$submittedPosition);
            }
            if ($storedDocumentIds !== $submittedDocumentIdsSorted
                || $storedDocumentRelations !== $submittedDocumentRelations) {
                throw new RuntimeException('followup_event_identity_conflict:' . $submittedEventId);
            }
            $dartMarkerUpgradeDocuments = array();
            foreach ($storedDocumentRows as $storedDocumentRow) {
                $storedDocumentId = (string)$storedDocumentRow['document_id'];
                if (!isset($submittedDocumentsById[$storedDocumentId])) {
                    continue;
                }
                $submittedDocument = $submittedDocumentsById[$storedDocumentId];
                $sourceClass = trim((string)v1_first($submittedDocument,array('source_class','source_category'),'official_disclosure'));
                if ($sourceClass === 'authorized_telegram') { $sourceClass = 'licensed_telegram'; }
                $externalId = trim((string)v1_first($submittedDocument,array('external_id','stable_source_id','rcept_no'),''));
                $documentTitle = trim((string)v1_first($submittedDocument,array('title','report_nm'),''));
                $documentUrl = trim((string)v1_first($submittedDocument,array('original_url','url'),''));
                $documentCompanyId = trim((string)v1_first($submittedDocument,array('company_id','corp_code'),''));
                if (preg_match('/^[0-9]{8}$/',$documentCompanyId) !== 1) { $documentCompanyId = ''; }
                $sourceRightId = strtolower(trim((string)v1_first($submittedDocument,array('source_right_id'),'')));
                $documentBody = (string)v1_first($submittedDocument,array('body_text','content'),'');
                $documentContentHash = strtolower(trim((string)v1_first($submittedDocument,array('content_hash'),'')));
                if (preg_match('/^[a-f0-9]{64}$/',$documentContentHash) !== 1) {
                    $documentContentHash = hash('sha256',$documentTitle . "\n" . $documentUrl . "\n" . $documentBody);
                }
                $documentVerification = (string)v1_first($submittedDocument,array('verification_status'),
                    $sourceClass === 'official_disclosure' ? 'official' : 'unverified');
                $documentPublication = (string)v1_first($submittedDocument,array('publication_status'),
                    $sourceClass === 'official_disclosure' ? 'published' : 'draft');
                if (!empty($submittedDocument['is_cancelled'])) {
                    $documentVerification = 'withdrawn';
                    $documentPublication = 'published';
                }
                if (!in_array($documentPublication,array('draft','published','withdrawn'),true)) {
                    $documentPublication = 'draft';
                }
                $storedDocumentPayload = json_decode((string)$storedDocumentRow['payload_json'],true);
                if (is_array($storedDocumentPayload)
                    && (string)($storedDocumentPayload['correction_link_status'] ?? '') === 'ambiguous_independent') {
                    $documentPublication = 'draft';
                }
                $storedDocumentFields = array(
                    (string)($storedDocumentRow['company_id'] ?? ''),
                    (string)($storedDocumentRow['source_right_id'] ?? ''),
                    (string)$storedDocumentRow['source_class'],
                    (string)$storedDocumentRow['external_id'],
                    (string)($storedDocumentRow['document_type'] ?? ''),
                    (string)$storedDocumentRow['original_language'],
                    (string)$storedDocumentRow['title'],
                    (string)($storedDocumentRow['body_text'] ?? ''),
                    (string)$storedDocumentRow['original_url'],
                    (string)$storedDocumentRow['content_hash'],
                    (string)($storedDocumentRow['collection_key'] ?? ''),
                    (string)($storedDocumentRow['published_at'] ?? ''),
                    (string)$storedDocumentRow['verification_status'],
                    (string)$storedDocumentRow['publication_status'],
                );
                $submittedDocumentFields = array(
                    $documentCompanyId,
                    $sourceRightId,
                    $sourceClass,
                    mb_substr($externalId,0,191,'UTF-8'),
                    mb_substr((string)v1_first($submittedDocument,array('document_type','pblntf_detail_ty'),''),0,80,'UTF-8'),
                    v1_language(v1_first($submittedDocument,array('original_language','language'),'ko'),'ko'),
                    mb_substr($documentTitle,0,700,'UTF-8'),
                    $documentBody,
                    $documentUrl,
                    $documentContentHash,
                    mb_substr(trim((string)v1_first($submittedDocument,array('collection_key'),'')),0,96,'UTF-8'),
                    (string)(mysql_dt(v1_first($submittedDocument,array('published_at','received_at','rcept_dt'),null)) ?? ''),
                    $documentVerification,
                    $documentPublication,
                );
                $submittedDocumentPayload = $submittedDocument;
                if (is_array($storedDocumentPayload)) { unset($storedDocumentPayload['retrieved_at']); }
                unset($submittedDocumentPayload['retrieved_at']);
                if ($sourceClass === 'official_disclosure' && $sourceRightId === 'official:dart') {
                    if (!isset($submittedDocumentPayload['metadata'])) {
                        $submittedDocumentPayload['metadata'] = array();
                    }
                    if (is_array($submittedDocumentPayload['metadata'])
                        && !isset($submittedDocumentPayload['metadata']['title_provenance'])) {
                        $submittedDocumentPayload['metadata']['title_provenance'] = 'source';
                    }
                }
                if (is_array($storedDocumentPayload)
                    && (string)($storedDocumentPayload['correction_link_status'] ?? '') === 'ambiguous_independent'
                    && !isset($submittedDocumentPayload['correction_link_status'])) {
                    $submittedDocumentPayload['correction_link_status'] = 'ambiguous_independent';
                }
                $documentDartMarkerUpgrade = false;
                $documentPayloadMatches = is_array($storedDocumentPayload)
                    && v1_followup_document_replay_payload_matches(
                        $storedDocumentPayload,
                        $submittedDocumentPayload,
                        $allowDartMarkerUpgrade
                            && $sourceClass === 'official_disclosure'
                            && $sourceRightId === 'official:dart',
                        $documentDartMarkerUpgrade
                    );
                $replayDocumentFields = $submittedDocumentFields;
                if ($documentDartMarkerUpgrade) {
                    // The content hash includes DART's mutable rm field. The
                    // helper above independently proved both hashes, so compare
                    // every other persisted scalar against the first-seen row.
                    if (!hash_equals(
                        strtolower((string)$storedDocumentFields[9]),
                        strtolower((string)($storedDocumentPayload['content_hash'] ?? ''))
                    ) || !hash_equals(
                        strtolower((string)$submittedDocumentFields[9]),
                        strtolower((string)($submittedDocumentPayload['content_hash'] ?? ''))
                    )) {
                        throw new RuntimeException(
                            'followup_event_identity_conflict:' . $submittedEventId
                        );
                    }
                    $replayDocumentFields[9] = $storedDocumentFields[9];
                }
                if ($storedDocumentFields !== $replayDocumentFields
                    || !$documentPayloadMatches) {
                    throw new RuntimeException('followup_event_identity_conflict:' . $submittedEventId);
                }
                if ($documentDartMarkerUpgrade) {
                    $storedRemarks = (string)($storedDocumentPayload['remarks'] ?? '');
                    $submittedRemarks = (string)($submittedDocumentPayload['remarks'] ?? '');
                    $dartMarkerUpgradeDocuments[] = array(
                        'document_id'=>$storedDocumentId,
                        'observation_id'=>v1_stable_id(
                            'dart-lifecycle',
                            'has-later-correction-v1|'
                                . $submittedEventId . '|' . $storedDocumentId . '|'
                                . (string)$storedDocumentFields[9] . '|'
                                . (string)$submittedDocumentFields[9]
                        ),
                        'country_code'=>'KR',
                        'source_key'=>'dart',
                        'external_id'=>(string)$storedDocumentRow['external_id'],
                        'parent_external_id'=>null,
                        'change_type'=>'updated',
                        'metadata'=>array(
                            'source_semantics'=>'has_later_correction',
                            'marker'=>'정',
                            'source_right_id'=>'official:dart',
                            'previous_remarks'=>$storedRemarks,
                            'current_remarks'=>$submittedRemarks,
                            'previous_content_hash'=>(string)$storedDocumentFields[9],
                            'current_content_hash'=>(string)$submittedDocumentFields[9],
                        ),
                    );
                }
                $isolatedReplayDocumentSnapshots[$storedDocumentId] = $storedDocumentRow;
            }
            $hasDartDocumentMarkerUpgrade = count($dartMarkerUpgradeDocuments) === 1
                && count($storedDocumentRows) === 1;
            if ($eventDartMarkerUpgrade !== $hasDartDocumentMarkerUpgrade
                || count($dartMarkerUpgradeDocuments) > 1) {
                throw new RuntimeException('followup_event_identity_conflict:' . $submittedEventId);
            }
            if ($eventDartMarkerUpgrade) {
                $lifecycleObservation = $dartMarkerUpgradeDocuments[0];
                $lifecycleObservation['event_id'] = $submittedEventId;
                if (!$dartLifecycleObservationByIdStmt) {
                    throw new RuntimeException('global_lifecycle_guard_unavailable');
                }
                $existingLifecycleObservation = v1_pdo_fetch_one_and_close(
                    $dartLifecycleObservationByIdStmt,
                    array($lifecycleObservation['observation_id'])
                );
                if ($existingLifecycleObservation) {
                    if (!v1_dart_lifecycle_observation_matches(
                        $existingLifecycleObservation,
                        $lifecycleObservation
                    )) {
                        throw new RuntimeException(
                            'followup_event_identity_conflict:' . $submittedEventId
                        );
                    }
                } elseif (!$eventCanCreateDartLifecycleObservation) {
                    // A newly observed source change cannot silently alter a
                    // reviewed or public event. An already-recorded exact
                    // lifecycle replay remains safe after later human review.
                    throw new RuntimeException(
                        'followup_event_identity_conflict:' . $submittedEventId
                    );
                }
                $pendingDartLifecycleObservations[] = $lifecycleObservation;
                // Preserve the first-seen canonical payload. The source's
                // monotonic marker change lives only in the lifecycle record.
                $isolatedReplayCanonicalEventPayloads[$submittedEventId] =
                    $storedEventPayload;
            }
            $approvedIsolatedReplayEventIds[$submittedEventId] = true;
        }
        $submittedOrReferencedDocumentIds = array_fill_keys(array_keys($submittedDocumentsById),true);
        foreach (array_keys($submittedDocumentReferenceEventIds) as $submittedReferenceDocumentId) {
            $submittedOrReferencedDocumentIds[$submittedReferenceDocumentId] = true;
        }
        foreach (array_keys($submittedOrReferencedDocumentIds) as $submittedOrReferencedDocumentId) {
            $submittedOrReferencedDocumentId = (string)$submittedOrReferencedDocumentId;
            $storedOwnerRows = v1_pdo_fetch_all_and_close(
                $isolatedReplayDocumentOwnersStmt,
                array($submittedOrReferencedDocumentId)
            );
            foreach ($storedOwnerRows as $storedOwnerRow) {
                $storedOwnerEventId = (string)$storedOwnerRow['event_id'];
                $submittedReferenceOwners = isset(
                    $submittedDocumentReferenceEventIds[
                        $submittedOrReferencedDocumentId
                    ]
                ) ? array_keys(
                    $submittedDocumentReferenceEventIds[
                        $submittedOrReferencedDocumentId
                    ]
                ) : array();
                if (v1_dart_reviewed_event_is_protected($storedOwnerRow)
                    || v1_dart_rejected_event_is_protected($storedOwnerRow)) {
                    if ((string)$storedOwnerRow['document_id']
                            !== $submittedOrReferencedDocumentId
                        || count($submittedReferenceOwners) !== 1
                        || (string)$submittedReferenceOwners[0]
                            !== $storedOwnerEventId
                        || !isset(
                            $readOnlyDartReviewedEventCompanyIds[
                                $storedOwnerEventId
                            ]
                        )
                        || !isset(
                            $readOnlyDartReviewedDocumentIds[
                                $submittedOrReferencedDocumentId
                            ]
                        )) {
                        throw new RuntimeException(
                            'followup_event_identity_conflict:'
                            . $storedOwnerEventId
                        );
                    }
                    continue;
                }
                if ((string)($storedOwnerRow['identity_status'] ?? '') !== 'needs_review') {
                    continue;
                }
                $storedOwnerPayload = json_decode((string)($storedOwnerRow['payload_json'] ?? ''),true);
                $storedOwnerFollowup = is_array($storedOwnerPayload)
                    && (!empty($storedOwnerPayload['is_correction']) || !empty($storedOwnerPayload['is_cancelled']));
                $storedOwnerMarker = is_array($storedOwnerPayload)
                    ? (string)($storedOwnerPayload['event_link_status'] ?? '') : '';
                $storedOwnerLifecycle = in_array(
                    (string)($storedOwnerRow['verification_status'] ?? ''),
                    array('corrected','withdrawn'),
                    true
                );
                if (!$storedOwnerFollowup && $storedOwnerMarker === '' && !$storedOwnerLifecycle) {
                    continue;
                }
                if ((string)$storedOwnerRow['document_id'] !== $submittedOrReferencedDocumentId
                    || !$storedOwnerFollowup
                    || $storedOwnerMarker !== 'ambiguous_independent'
                    || !isset($approvedIsolatedReplayEventIds[$storedOwnerEventId])) {
                    throw new RuntimeException('followup_event_identity_conflict:' . $storedOwnerEventId);
                }
                foreach ($submittedReferenceOwners as $submittedReferenceOwner) {
                    if ($submittedReferenceOwner !== $storedOwnerEventId) {
                        throw new RuntimeException('followup_event_identity_conflict:' . $storedOwnerEventId);
                    }
                }
            }
        }
        foreach ($pendingDartLifecycleObservations as $lifecycleObservation) {
            if (!$dartLifecycleObservationByIdStmt
                || !$dartLifecycleObservationInsertStmt) {
                throw new RuntimeException('global_lifecycle_guard_unavailable');
            }
            $storedLifecycleObservation = v1_pdo_fetch_one_and_close(
                $dartLifecycleObservationByIdStmt,
                array($lifecycleObservation['observation_id'])
            );
            if ($storedLifecycleObservation) {
                if (!v1_dart_lifecycle_observation_matches(
                    $storedLifecycleObservation,
                    $lifecycleObservation
                )) {
                    throw new RuntimeException(
                        'followup_event_identity_conflict:'
                        . (string)$lifecycleObservation['event_id']
                    );
                }
                continue;
            }
            $dartLifecycleObservationInsertStmt->execute(array(
                (string)$lifecycleObservation['observation_id'],
                'connector:kr:dart',
                'KR',
                'dart',
                (string)$lifecycleObservation['external_id'],
                $lifecycleObservation['parent_external_id'],
                'updated',
                $now,
                json_value($lifecycleObservation['metadata']),
                'resolved',
                (string)$lifecycleObservation['document_id'],
                (string)$lifecycleObservation['event_id'],
                $now,
                $now,
            ));
        }
        $readOnlyDartReviewedCompanyIds = array_fill_keys(
            array_values($readOnlyDartReviewedEventCompanyIds),
            true
        );
        foreach ($events as $submittedEvent) {
            if (!is_array($submittedEvent)) { continue; }
            $submittedEventId = trim((string)v1_first(
                $submittedEvent,
                array('event_id'),
                ''
            ));
            $submittedCompanyId = trim((string)v1_first(
                $submittedEvent,
                array('company_id','corp_code'),
                ''
            ));
            if (isset($readOnlyDartReviewedCompanyIds[$submittedCompanyId])
                && !isset(
                    $readOnlyDartReviewedEventCompanyIds[$submittedEventId]
                )) {
                unset($readOnlyDartReviewedCompanyIds[$submittedCompanyId]);
            }
        }
        foreach ($documents as $submittedDocument) {
            if (!is_array($submittedDocument)) { continue; }
            $submittedDocumentId =
                v1_governance_snapshot_document_id($submittedDocument);
            $submittedCompanyId = trim((string)v1_first(
                $submittedDocument,
                array('company_id','corp_code'),
                ''
            ));
            if (isset($readOnlyDartReviewedCompanyIds[$submittedCompanyId])
                && !isset(
                    $readOnlyDartReviewedDocumentIds[$submittedDocumentId]
                )) {
                unset($readOnlyDartReviewedCompanyIds[$submittedCompanyId]);
            }
        }
        $submittedReadOnlyCompanies = array();
        foreach ($companies as $submittedCompany) {
            if (!is_array($submittedCompany)) { continue; }
            $submittedCompanyId = trim((string)v1_first(
                $submittedCompany,
                array('company_id','corp_code'),
                ''
            ));
            if (!isset($readOnlyDartReviewedCompanyIds[$submittedCompanyId])) {
                continue;
            }
            if (isset($submittedReadOnlyCompanies[$submittedCompanyId])) {
                throw new RuntimeException(
                    'followup_event_identity_conflict:' . $submittedCompanyId
                );
            }
            $submittedReadOnlyCompanies[$submittedCompanyId] =
                $submittedCompany;
        }
        foreach ($readOnlyDartReviewedCompanyIds as $companyId => $_unused) {
            if (!isset($submittedReadOnlyCompanies[$companyId])
                || !v1_dart_reviewed_company_replay_matches(
                    $pdo,
                    $config,
                    $submittedReadOnlyCompanies[$companyId]
                )) {
                throw new RuntimeException(
                    'followup_event_identity_conflict:' . $companyId
                );
            }
        }
        $companyStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'companies') . ' (company_id, stock_code, market, legal_name, legal_name_en, short_name, aliases_json, homepage_url, record_status, listing_status, master_modified_at, created_at, updated_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE stock_code=COALESCE(NULLIF(VALUES(stock_code),\'\'),stock_code), '
            . 'market=COALESCE(NULLIF(VALUES(market),\'\'),market), legal_name=COALESCE(NULLIF(VALUES(legal_name),\'\'),legal_name), '
            . 'legal_name_en=COALESCE(NULLIF(VALUES(legal_name_en),\'\'),legal_name_en), short_name=COALESCE(NULLIF(VALUES(short_name),\'\'),short_name), '
            . 'aliases_json=IF(VALUES(aliases_json) IS NULL OR VALUES(aliases_json)=\'[]\',aliases_json,VALUES(aliases_json)), '
            . 'homepage_url=COALESCE(NULLIF(VALUES(homepage_url),\'\'),homepage_url), record_status=VALUES(record_status), '
            . 'listing_status=listing_status, master_modified_at=master_modified_at, updated_at=VALUES(updated_at)');
        $companyMasterStmt = $pdo->prepare('UPDATE ' . table_name($config, 'companies')
            . ' SET listing_status=IF(?=1,?,listing_status), master_modified_at=IF(?=1,?,master_modified_at) WHERE company_id=?');
        $globalIssuerStmt = null;
        $globalIdentifierStmt = null;
        $globalListingStmt = null;
        if ($globalDartProjectionEnabled) {
            $globalIssuerTable = table_name($config,'issuers');
            $globalIssuerStmt = $pdo->prepare('INSERT INTO ' . $globalIssuerTable
                . ' (issuer_id,country_code,legal_name,legal_name_en,short_name,original_language,homepage_url,'
                . 'listing_status,record_status,master_modified_at,payload_json,created_at,updated_at)'
                . ' SELECT ?,\'KR\',c.legal_name,c.legal_name_en,c.short_name,\'ko\',c.homepage_url,'
                . 'c.listing_status,c.record_status,c.master_modified_at,?,?,? FROM '
                . table_name($config,'companies') . ' c WHERE c.company_id=?'
                . ' ON DUPLICATE KEY UPDATE '
                . 'country_code=\'KR\',legal_name=VALUES(legal_name),'
                . 'legal_name_en=COALESCE(NULLIF(VALUES(legal_name_en),\'\'),' . $globalIssuerTable . '.legal_name_en),'
                . 'short_name=COALESCE(NULLIF(VALUES(short_name),\'\'),' . $globalIssuerTable . '.short_name),'
                . 'homepage_url=COALESCE(NULLIF(VALUES(homepage_url),\'\'),' . $globalIssuerTable . '.homepage_url),'
                . 'listing_status=VALUES(listing_status),record_status=VALUES(record_status),'
                . 'master_modified_at=COALESCE(VALUES(master_modified_at),' . $globalIssuerTable . '.master_modified_at),'
                . 'payload_json=VALUES(payload_json),updated_at=VALUES(updated_at)');
            $globalIdentifierStmt = $pdo->prepare('INSERT INTO ' . table_name($config,'issuer_identifiers')
                . ' (issuer_id,identifier_type,identifier_value,market,is_primary,valid_from,valid_until,created_at,updated_at)'
                . ' VALUES (?,?,?,?,?,NULL,NULL,?,?) ON DUPLICATE KEY UPDATE '
                . 'issuer_id=IF(issuer_id=VALUES(issuer_id),VALUES(issuer_id),issuer_id),'
                . 'is_primary=IF(issuer_id=VALUES(issuer_id),VALUES(is_primary),is_primary),'
                . 'updated_at=IF(issuer_id=VALUES(issuer_id),VALUES(updated_at),updated_at)');
            $globalListingStmt = $pdo->prepare('INSERT INTO ' . table_name($config,'issuer_listings')
                . ' (listing_id,issuer_id,country_code,market,ticker,isin,currency_code,listing_status,is_primary,created_at,updated_at)'
                . ' VALUES (?,?,\'KR\',?,?,NULL,\'KRW\',?,1,?,?) ON DUPLICATE KEY UPDATE '
                . 'issuer_id=IF(listing_id=VALUES(listing_id),VALUES(issuer_id),issuer_id),'
                . 'country_code=IF(listing_id=VALUES(listing_id),\'KR\',country_code),'
                . 'market=IF(listing_id=VALUES(listing_id),VALUES(market),market),'
                . 'ticker=IF(listing_id=VALUES(listing_id),VALUES(ticker),ticker),'
                . 'listing_status=IF(listing_id=VALUES(listing_id)'
                . ' AND VALUES(listing_status)<>\'unknown\',VALUES(listing_status),listing_status),'
                . 'is_primary=IF(listing_id=VALUES(listing_id),1,is_primary),'
                . 'updated_at=IF(listing_id=VALUES(listing_id),VALUES(updated_at),updated_at)');
        }
        foreach ($companies as $company) {
            if (!is_array($company)) { continue; }
            $companyId = trim((string)v1_first($company, array('company_id', 'corp_code'), ''));
            if (isset($readOnlyDartReviewedCompanyIds[$companyId])) {
                // The reviewed-event preflight already proved this exact
                // company projection and deliberately keeps it immutable.
                // Snapshot ACK counts represent submitted/idempotently
                // acknowledged rows, not only rows that executed an upsert.
                $counts['companies']++;
                continue;
            }
            $legalName = trim((string)v1_first($company, array('legal_name', 'corp_name'), ''));
            if (!preg_match('/^[0-9]{8}$/', $companyId) || $legalName === '') { continue; }
            $aliases = array();
            foreach (array_slice(isset($company['aliases']) && is_array($company['aliases']) ? $company['aliases'] : array(), 0, 20) as $alias) {
                $alias = mb_substr(trim((string)$alias), 0, 255, 'UTF-8');
                if ($alias !== '') { $aliases[] = $alias; }
            }
            $allowedListingStatuses = array('unknown','listed','unlisted','suspended','delisted');
            $requestedListingStatus = trim((string)v1_first($company, array('listing_status'), ''));
            $hasListingStatus = array_key_exists('listing_status',$company)
                && in_array($requestedListingStatus,$allowedListingStatuses,true);
            $listingStatus = $hasListingStatus ? $requestedListingStatus : 'unknown';
            $masterModifiedAt = mysql_dt(v1_first($company, array('master_modified_at','modified_at'), null));
            $hasMasterModifiedAt = (array_key_exists('master_modified_at',$company) || array_key_exists('modified_at',$company))
                && $masterModifiedAt !== null;
            $companyStmt->execute(array(
                $companyId,
                mb_substr((string)v1_first($company, array('stock_code'), ''), 0, 12, 'UTF-8') ?: null,
                mb_substr((string)v1_first($company, array('market', 'corp_cls'), ''), 0, 40, 'UTF-8') ?: null,
                mb_substr($legalName, 0, 255, 'UTF-8'),
                mb_substr((string)v1_first($company, array('legal_name_en', 'corp_name_eng'), ''), 0, 255, 'UTF-8') ?: null,
                mb_substr((string)v1_first($company, array('short_name'), ''), 0, 255, 'UTF-8') ?: null,
                json_value(array_values(array_unique($aliases))),
                (string)v1_first($company, array('homepage_url', 'hm_url'), '') ?: null,
                in_array(v1_first($company, array('record_status'), 'active'), array('active', 'inactive', 'merged', 'delisted'), true) ? (string)v1_first($company, array('record_status'), 'active') : 'active',
                $listingStatus, $masterModifiedAt,
                $now, $now,
            ));
            if ($hasListingStatus || $hasMasterModifiedAt) {
                $companyMasterStmt->execute(array($hasListingStatus ? 1 : 0,$listingStatus,
                    $hasMasterModifiedAt ? 1 : 0,$masterModifiedAt,$companyId));
            }
            if ($globalDartProjectionEnabled) {
                $issuerId = 'issuer:kr:dart:' . $companyId;
                $stockCode = mb_substr(trim((string)v1_first($company,array('stock_code'),'')),0,12,'UTF-8');
                $market = mb_substr(trim((string)v1_first($company,array('market','corp_cls'),'')),0,40,'UTF-8');
                if ($market === '') { $market = 'KRX'; }
                $globalIssuerStmt->execute(array(
                    $issuerId,json_value(array(
                        'legacy_company_id'=>$companyId,
                        'identity_namespace'=>'DART_CORP_CODE',
                        'bridge'=>'v1_official_ingest',
                    )),$now,$now,$companyId,
                ));
                $globalIdentifierStmt->execute(array(
                    $issuerId,'DART_CORP_CODE',$companyId,'KRX',1,$now,$now,
                ));
                if ($stockCode !== '') {
                    $globalIdentifierStmt->execute(array(
                        $issuerId,'TICKER',$stockCode,$market,0,$now,$now,
                    ));
                    $globalListingStmt->execute(array(
                        'listing:kr:' . $companyId,$issuerId,$market,$stockCode,
                        $listingStatus,$now,$now,
                    ));
                }
            }
            $counts['companies']++;
        }

        $rightStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'source_rights') . ' (source_right_id, source_type, source_key, source_name, permission_scope, '
            . 'evidence_uri, evidence_hash, valid_from, valid_until, revoked_at, ai_allowed, redistribution_allowed, status, notes, created_at, updated_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE source_right_id=source_right_id');
        foreach ($rights as $right) {
            if (!is_array($right)) { continue; }
            $sourceType = trim((string)v1_first($right, array('source_type', 'source_category'), ''));
            $sourceKey = trim((string)v1_first($right, array('source_key', 'source_identity'), ''));
            $sourceName = trim((string)v1_first($right, array('source_name', 'name'), $sourceKey));
            $scope = trim((string)v1_first($right, array('permission_scope', 'scope'), ''));
            if (!preg_match('/^[A-Za-z0-9_.:\-]{1,40}$/', $sourceType) || $sourceKey === '' || $sourceName === '' || $scope === '') { continue; }
            $id = trim((string)v1_first($right, array('source_right_id'), ''));
            if ($id === '') { $id = v1_stable_id('sr', $sourceType . ':' . $sourceKey, 64); }
            if (!v1_valid_entity_id($id, 64)) { continue; }
            if ($sourceType !== 'official_disclosure' || strpos($id, 'official:') !== 0 || !in_array(strtolower($sourceKey), array('dart', 'kind'), true)) {
                $counts['source_rights_rejected']++;
                continue;
            }
            $validFrom = mysql_dt(v1_first($right, array('valid_from'), $now));
            if ($validFrom === null) { continue; }
            $validUntil = mysql_dt(v1_first($right, array('valid_until', 'expires_at'), null));
            $evidenceRef = trim((string)v1_first($right, array('evidence_uri', 'evidence_ref'), ''));
            $status = (string)v1_first($right, array('status'), '');
            if ($status === '') { $status = v1_first($right, array('revoked_at'), null) ? 'revoked' : 'active'; }
            if (!in_array($status, array('pending', 'active', 'expired', 'revoked'), true)) { $status = 'pending'; }
            $evidenceHash = strtolower(trim((string)v1_first($right, array('evidence_hash'), '')));
            if ($evidenceHash !== '' && !preg_match('/^[a-f0-9]{64}$/', $evidenceHash)) { $evidenceHash = ''; }
            if ($status === 'active' && $evidenceRef === '' && $evidenceHash === '') { $status = 'pending'; }
            $rightStmt->execute(array(
                $id, $sourceType, mb_substr($sourceKey, 0, 191, 'UTF-8'), mb_substr($sourceName, 0, 255, 'UTF-8'), $scope,
                $evidenceRef ?: null, $evidenceHash ?: null, $validFrom, $validUntil,
                mysql_dt(v1_first($right, array('revoked_at'), null)), v1_bool_int(v1_first($right, array('ai_allowed', 'allow_ai'), false)),
                v1_bool_int(v1_first($right, array('redistribution_allowed', 'allow_redistribution'), false)), $status,
                (string)v1_first($right, array('notes'), '') ?: null, $now, $now,
            ));
            $counts['source_rights']++;
        }

        $documentStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'documents') . ' (document_id, company_id, source_right_id, source_class, external_id, '
            . 'document_type, original_language, title, body_text, original_url, content_hash, collection_key, correction_of_document_id, version_no, published_at, retrieved_at, '
            . 'verification_status, publication_status, payload_json, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '
            . 'ON DUPLICATE KEY UPDATE company_id=VALUES(company_id), source_right_id=VALUES(source_right_id), document_type=VALUES(document_type), '
            . 'original_language=VALUES(original_language), title=VALUES(title), body_text=VALUES(body_text), original_url=VALUES(original_url), '
            . 'content_hash=VALUES(content_hash), collection_key=VALUES(collection_key), '
            . 'updated_at=IF(payload_json<=>VALUES(payload_json) '
            . 'AND correction_of_document_id<=>COALESCE(correction_of_document_id,VALUES(correction_of_document_id)) '
            . 'AND version_no=GREATEST(version_no,VALUES(version_no)),updated_at,GREATEST(VALUES(updated_at),DATE_ADD(updated_at, INTERVAL 1 SECOND))), '
            . 'correction_of_document_id=COALESCE(correction_of_document_id,VALUES(correction_of_document_id)), '
            . 'version_no=GREATEST(version_no,VALUES(version_no)), published_at=VALUES(published_at), '
            . 'retrieved_at=VALUES(retrieved_at), verification_status=VALUES(verification_status), publication_status=VALUES(publication_status), '
            . 'payload_json=VALUES(payload_json)');
        $previousDocumentStmt = $pdo->prepare('SELECT predecessor.document_id, predecessor.version_no FROM ' . table_name($config, 'documents') . ' predecessor'
            . ' WHERE predecessor.company_id=? AND predecessor.source_class=? AND predecessor.source_right_id=?'
            . ' AND predecessor.collection_key=? AND predecessor.document_id<>?'
            . ' AND COALESCE(predecessor.published_at,predecessor.retrieved_at) BETWEEN DATE_SUB(?, INTERVAL ' . V1_CORRECTION_LOOKBACK_DAYS . ' DAY) AND DATE_ADD(?, INTERVAL 7 DAY)'
            . ' AND NOT EXISTS (SELECT 1 FROM ' . table_name($config, 'documents') . ' successor WHERE successor.correction_of_document_id=predecessor.document_id AND successor.document_id<>?)'
            . ' ORDER BY predecessor.version_no DESC, COALESCE(predecessor.published_at,predecessor.retrieved_at) DESC, predecessor.document_id DESC LIMIT 2 FOR UPDATE');
        $providedPredecessorStmt = $pdo->prepare('SELECT predecessor.document_id, predecessor.version_no FROM ' . table_name($config, 'documents') . ' predecessor'
            . ' WHERE predecessor.document_id=? AND predecessor.company_id=? AND predecessor.source_class=?'
            . ' AND predecessor.source_right_id=? AND predecessor.collection_key=?'
            . ' AND COALESCE(predecessor.published_at,predecessor.retrieved_at) BETWEEN DATE_SUB(?, INTERVAL ' . V1_CORRECTION_LOOKBACK_DAYS . ' DAY) AND DATE_ADD(?, INTERVAL 7 DAY)'
            . ' AND NOT EXISTS (SELECT 1 FROM ' . table_name($config, 'documents') . ' successor WHERE successor.correction_of_document_id=predecessor.document_id AND successor.document_id<>?)'
            . ' LIMIT 1 FOR UPDATE');
        $existingDocumentLineageStmt = $pdo->prepare('SELECT correction_of_document_id, version_no FROM '
            . table_name($config, 'documents') . ' WHERE document_id=? LIMIT 1 FOR UPDATE');
        $globalDartDocumentStmt = null;
        if ($globalDartProjectionEnabled) {
            $globalDartDocumentStmt = $pdo->prepare('UPDATE ' . table_name($config,'documents')
                . ' SET issuer_id=?,country_code=\'KR\',source_key=\'dart\',filed_at=COALESCE(filed_at,?)'
                . ' WHERE document_id=? AND company_id=? AND source_right_id=\'official:dart\'');
        }
        foreach ($documents as $document) {
            if (!is_array($document)) { continue; }
            $sourceClass = trim((string)v1_first($document, array('source_class', 'source_category'), 'official_disclosure'));
            if ($sourceClass === 'authorized_telegram') { $sourceClass = 'licensed_telegram'; }
            if (!in_array($sourceClass, array('official_disclosure', 'company_statement', 'activist_statement', 'media_report', 'licensed_telegram', 'editorial_analysis'), true)) { continue; }
            $externalId = trim((string)v1_first($document, array('external_id', 'stable_source_id', 'rcept_no'), ''));
            $title = trim((string)v1_first($document, array('title', 'report_nm'), ''));
            $url = trim((string)v1_first($document, array('original_url', 'url'), ''));
            if ($externalId === '' || $title === '' || !preg_match('#^https?://#i', $url)) { continue; }
            $id = v1_governance_snapshot_document_id($document);
            if (!v1_valid_entity_id($id)) { continue; }
            $sourceRightId = strtolower(trim((string)v1_first($document, array('source_right_id'), '')));
            $documentSourceClasses[$id] = $sourceClass;
            $documentSourceRightIds[$id] = $sourceRightId;
            if (isset($isolatedReplayDocumentSnapshots[$id])) {
                // The preflight proved this is the exact evidence document of an
                // isolated self replay. Do not run lineage discovery or an
                // upsert again: either could change a previously stored
                // predecessor, version, marker, publication state or retrieval
                // timestamp as the surrounding corpus grows.
                $storedReplayDocument = $isolatedReplayDocumentSnapshots[$id];
                $documentSourceClasses[$id] = (string)$storedReplayDocument['source_class'];
                $documentSourceRightIds[$id] = strtolower((string)($storedReplayDocument['source_right_id'] ?? ''));
                $counts['documents']++;
                continue;
            }
            if (
                $globalDartProjectionEnabled
                && $sourceClass === 'official_disclosure'
                && $sourceRightId === 'official:dart'
            ) {
                if (
                    isset($document['metadata'])
                    && (
                        !is_array($document['metadata'])
                        || (
                            isset($document['metadata']['title_provenance'])
                            && $document['metadata']['title_provenance'] !== 'source'
                        )
                    )
                ) {
                    throw new RuntimeException(
                        'dart_document_title_provenance_conflict:' . $id
                    );
                }
                if (!isset($document['metadata'])) {
                    $document['metadata'] = array();
                }
                $document['metadata']['title_provenance'] = 'source';
            }
            $companyId = trim((string)v1_first($document, array('company_id', 'corp_code'), ''));
            if ($companyId !== '' && !preg_match('/^[0-9]{8}$/', $companyId)) { $companyId = ''; }
            $collectionKey = mb_substr(trim((string)v1_first($document, array('collection_key'), '')), 0, 96, 'UTF-8');
            $correctionOf = trim((string)v1_first($document, array('correction_of_document_id', 'correction_of'), ''));
            $versionNo = max(1, (int)v1_first($document, array('version_no'), ((int)v1_first($document, array('correction_sequence'), 0)) + 1));
            $publishedAt = mysql_dt(v1_first($document, array('published_at', 'received_at', 'rcept_dt'), null));
            $retrievedAt = mysql_dt(v1_first($document, array('retrieved_at', 'received_at'), $now)) ?: $now;
            $documentReferenceAt = $publishedAt ?: $retrievedAt;
            $isFollowup = !empty($document['is_correction']) || !empty($document['is_cancelled'])
                || isset($followupDocumentIds[$id])
                || preg_match('/(^|[\[\(])\s*(정정|첨부정정|취소)\s*([\]\)]|$)/u', $title) === 1;
            $linkageAmbiguous = false;
            if ($isFollowup) {
                if ($companyId === '' || $collectionKey === '' || ($correctionOf !== '' && (!v1_valid_entity_id($correctionOf) || $correctionOf === $id))) {
                    $linkageAmbiguous = true;
                } elseif ($correctionOf !== '') {
                    $previousDocument = v1_pdo_fetch_one_and_close(
                        $providedPredecessorStmt,
                        array(
                            $correctionOf,$companyId,$sourceClass,$sourceRightId,
                            $collectionKey,$documentReferenceAt,$documentReferenceAt,$id
                        )
                    );
                    if (!$previousDocument) { $linkageAmbiguous = true; }
                } else {
                    $candidates = v1_pdo_fetch_all_and_close(
                        $previousDocumentStmt,
                        array(
                            $companyId,$sourceClass,$sourceRightId,$collectionKey,$id,
                            $documentReferenceAt,$documentReferenceAt,$id
                        )
                    );
                    if (count($candidates) !== 1) { $linkageAmbiguous = true; }
                    else { $previousDocument = $candidates[0]; }
                }
                if ($linkageAmbiguous) {
                    $correctionOf = '';
                    $versionNo = 1;
                    $document['correction_link_status'] = 'ambiguous_independent';
                } else {
                    $correctionOf = (string)$previousDocument['document_id'];
                    $versionNo = max($versionNo, ((int)$previousDocument['version_no']) + 1);
                }
            }
            $existingLineage = v1_pdo_fetch_one_and_close(
                $existingDocumentLineageStmt,
                array($id)
            );
            if ($existingLineage) {
                $existingCorrectionOf = trim((string)($existingLineage['correction_of_document_id'] ?? ''));
                if ($existingCorrectionOf !== '' && $correctionOf !== '' && $existingCorrectionOf !== $correctionOf) {
                    throw new RuntimeException('document_lineage_conflict:' . $id);
                }
                if ($existingCorrectionOf !== '') {
                    $correctionOf = $existingCorrectionOf;
                    $linkageAmbiguous = false;
                    unset($document['correction_link_status']);
                }
                $versionNo = max($versionNo, (int)$existingLineage['version_no']);
            }
            if ($linkageAmbiguous) { $counts['correction_link_ambiguous']++; }
            $body = (string)v1_first($document, array('body_text', 'content'), '');
            $contentHash = strtolower(trim((string)v1_first($document, array('content_hash'), '')));
            if (!preg_match('/^[a-f0-9]{64}$/', $contentHash)) { $contentHash = hash('sha256', $title . "\n" . $url . "\n" . $body); }
            $verification = (string)v1_first($document, array('verification_status'), $sourceClass === 'official_disclosure' ? 'official' : 'unverified');
            $publication = (string)v1_first($document, array('publication_status'), $sourceClass === 'official_disclosure' ? 'published' : 'draft');
            if (!empty($document['is_cancelled'])) {
                $verification = 'withdrawn';
                $publication = 'published';
            }
            if ($linkageAmbiguous) { $publication = 'draft'; }
            if ($sourceClass === 'licensed_telegram' && trim((string)v1_first($document, array('source_right_id'), '')) === '') { $publication = 'draft'; }
            $documentStmt->execute(array(
                $id, $companyId ?: null, (string)v1_first($document, array('source_right_id'), '') ?: null, $sourceClass, mb_substr($externalId, 0, 191, 'UTF-8'),
                mb_substr((string)v1_first($document, array('document_type', 'pblntf_detail_ty'), ''), 0, 80, 'UTF-8') ?: null,
                v1_language(v1_first($document, array('original_language', 'language'), 'ko'), 'ko'), mb_substr($title, 0, 700, 'UTF-8'), $body ?: null, $url,
                $contentHash, $collectionKey ?: null, $correctionOf ?: null, $versionNo,
                $publishedAt, $retrievedAt, mb_substr($verification, 0, 24, 'UTF-8'),
                in_array($publication, array('draft', 'published', 'withdrawn'), true) ? $publication : 'draft', json_value($document), $now, $now,
            ));
            if ($globalDartProjectionEnabled
                && $companyId !== ''
                && strtolower(trim((string)v1_first($document,array('source_right_id'),''))) === 'official:dart') {
                $globalDartDocumentStmt->execute(array(
                    'issuer:kr:dart:' . $companyId,$publishedAt ?: $retrievedAt,$id,$companyId,
                ));
            }
            $counts['documents']++;
        }

        $eventStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'governance_events') . ' (event_id, company_id, event_type, title, original_language, summary, '
            . 'occurred_at, deadline_at, importance, verification_status, review_status, publication_status, collection_key, identity_action, identity_target, '
            . 'identity_actor_id, identity_effective_at, identity_deadline_at, identity_status, comparison_key, payload_json, created_at, updated_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE company_id=VALUES(company_id), '
            . 'event_type=IF(VALUES(verification_status)=\'withdrawn\',event_type,VALUES(event_type)), '
            . 'title=IF(VALUES(verification_status)=\'withdrawn\',title,VALUES(title)), '
            . 'original_language=IF(VALUES(verification_status)=\'withdrawn\',original_language,VALUES(original_language)), '
            . 'summary=IF(VALUES(verification_status)=\'withdrawn\',summary,VALUES(summary)), '
            . 'occurred_at=IF(VALUES(verification_status)=\'withdrawn\',occurred_at,VALUES(occurred_at)), '
            . 'deadline_at=IF(VALUES(verification_status)=\'withdrawn\',deadline_at,VALUES(deadline_at)), '
            . 'importance=IF(VALUES(verification_status)=\'withdrawn\',importance,VALUES(importance)), '
            . 'verification_status=VALUES(verification_status), collection_key=VALUES(collection_key), '
            . 'identity_action=COALESCE(VALUES(identity_action),identity_action), identity_target=COALESCE(VALUES(identity_target),identity_target), '
            . 'identity_actor_id=COALESCE(VALUES(identity_actor_id),identity_actor_id), identity_effective_at=COALESCE(VALUES(identity_effective_at),identity_effective_at), '
            . 'identity_deadline_at=COALESCE(VALUES(identity_deadline_at),identity_deadline_at), '
            . 'identity_status=IF(identity_status=\'complete\',identity_status,VALUES(identity_status)), comparison_key=COALESCE(comparison_key,VALUES(comparison_key)), '
            . 'review_status=IF(payload_json<=>VALUES(payload_json),review_status,VALUES(review_status)), '
            . 'publication_status=IF(payload_json<=>VALUES(payload_json),publication_status,VALUES(publication_status)), '
            . 'updated_at=IF(payload_json<=>VALUES(payload_json),updated_at,GREATEST(VALUES(updated_at),DATE_ADD(updated_at, INTERVAL 1 SECOND))), '
            . 'payload_json=VALUES(payload_json)');
        $eventDocumentStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'event_documents') . ' (event_id, document_id, relation_type, position_no, created_at) '
            . 'VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE position_no=VALUES(position_no)');
        $eventByIdStmt = $pdo->prepare('SELECT event_id, event_type, title, original_language, summary, occurred_at, deadline_at, importance, verification_status, '
            . 'identity_action,identity_target,identity_actor_id,identity_effective_at,identity_deadline_at,identity_status,comparison_key,payload_json FROM '
            . table_name($config, 'governance_events') . ' WHERE event_id=? AND company_id=? LIMIT 1 FOR UPDATE');
        $eventDocumentIdsStmt = $pdo->prepare('SELECT document_id,relation_type,position_no FROM '
            . table_name($config, 'event_documents')
            . ' WHERE event_id=? ORDER BY position_no,document_id FOR UPDATE');
        $eventLifecycleStmt = $pdo->prepare('SELECT verification_status FROM ' . table_name($config, 'governance_events') . ' WHERE event_id=? LIMIT 1 FOR UPDATE');
        $eventIdentityStmt = $pdo->prepare('SELECT company_id,event_type,identity_action,identity_target,identity_actor_id,identity_effective_at,identity_deadline_at,identity_status,comparison_key '
            . 'FROM ' . table_name($config, 'governance_events') . ' WHERE event_id=? LIMIT 1 FOR UPDATE');
        $eventComparisonOwnerStmt = $pdo->prepare('SELECT event_id FROM ' . table_name($config, 'governance_events')
            . ' WHERE comparison_key=? LIMIT 1 FOR UPDATE');
        $followupTimelineUnchanged = '(document_id<=>VALUES(document_id) AND occurred_at<=>VALUES(occurred_at) '
            . 'AND title<=>VALUES(title) AND description<=>VALUES(description) AND original_language<=>VALUES(original_language))';
        $cancellationTimelineStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'timeline_entries') . ' (timeline_entry_id, event_id, campaign_id, document_id, '
            . 'occurred_at, entry_type, title, description, original_language, review_status, publication_status, created_at, updated_at) '
            . 'VALUES (?,?,NULL,?,?,\'cancellation\',?,?,?,\'pending\',\'draft\',?,?) ON DUPLICATE KEY UPDATE '
            . 'review_status=IF(' . $followupTimelineUnchanged . ',review_status,\'pending\'), '
            . 'publication_status=IF(' . $followupTimelineUnchanged . ',publication_status,\'draft\'), '
            . 'updated_at=IF(' . $followupTimelineUnchanged . ',updated_at,GREATEST(VALUES(updated_at),DATE_ADD(updated_at, INTERVAL 1 SECOND))), '
            . 'document_id=VALUES(document_id), occurred_at=VALUES(occurred_at), title=VALUES(title), description=VALUES(description), '
            . 'original_language=VALUES(original_language)');
        $cancellationRevisionStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'editorial_revisions') . ' (revision_id, entity_type, entity_id, field_name, '
            . 'previous_value, revised_value, reason, revision_status, requested_by, reviewed_by, reviewed_at, published_at, created_at, updated_at) '
            . 'VALUES (?,\'event\',?,\'lifecycle_status\',?,\'withdrawn\',?,\'published\',\'official_ingest\',\'official_ingest\',?,?,?,?) '
            . 'ON DUPLICATE KEY UPDATE revision_id=revision_id');
        $correctionTimelineStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'timeline_entries') . ' (timeline_entry_id, event_id, campaign_id, document_id, '
            . 'occurred_at, entry_type, title, description, original_language, review_status, publication_status, created_at, updated_at) '
            . 'VALUES (?,?,NULL,?,?,\'correction\',?,?,?,\'pending\',\'draft\',?,?) ON DUPLICATE KEY UPDATE '
            . 'review_status=IF(' . $followupTimelineUnchanged . ',review_status,\'pending\'), '
            . 'publication_status=IF(' . $followupTimelineUnchanged . ',publication_status,\'draft\'), '
            . 'updated_at=IF(' . $followupTimelineUnchanged . ',updated_at,GREATEST(VALUES(updated_at),DATE_ADD(updated_at, INTERVAL 1 SECOND))), '
            . 'document_id=VALUES(document_id), occurred_at=VALUES(occurred_at), title=VALUES(title), description=VALUES(description), '
            . 'original_language=VALUES(original_language)');
        $correctionRevisionStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'editorial_revisions') . ' (revision_id, entity_type, entity_id, field_name, '
            . 'previous_value, revised_value, reason, revision_status, requested_by, reviewed_by, reviewed_at, published_at, created_at, updated_at) '
            . 'VALUES (?,\'event\',?,\'lifecycle_status\',?,\'corrected\',?,\'published\',\'official_ingest\',\'official_ingest\',?,?,?,?) '
            . 'ON DUPLICATE KEY UPDATE revision_id=revision_id');
        $documentClassStmt = $pdo->prepare('SELECT source_class,source_right_id FROM ' . table_name($config, 'documents') . ' WHERE document_id=? LIMIT 1');
        $documentObservationStmt = $pdo->prepare('SELECT d.source_class,COALESCE(NULLIF(sr.source_key,\'\'),d.source_class) AS source_key,d.content_hash,d.retrieved_at '
            . 'FROM ' . table_name($config, 'documents') . ' d LEFT JOIN ' . table_name($config, 'source_rights') . ' sr ON sr.source_right_id=d.source_right_id '
            . 'WHERE d.document_id=? LIMIT 1');
        $eventObservationStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'event_observations')
            . ' (observation_id,event_id,document_id,source_class,source_key,first_observed_at,observed_at,payload_hash,payload_json,created_at,updated_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE first_observed_at=LEAST(first_observed_at,VALUES(first_observed_at)), '
            . 'observed_at=GREATEST(observed_at,VALUES(observed_at)), payload_hash=VALUES(payload_hash), payload_json=VALUES(payload_json), updated_at=VALUES(updated_at)');
        $globalDartEventStmt = null;
        if ($globalDartProjectionEnabled) {
            $globalDartEventStmt = $pdo->prepare('UPDATE ' . table_name($config,'governance_events') . ' bridge_event'
                . ' SET bridge_event.issuer_id=?,bridge_event.country_code=\'KR\','
                . 'bridge_event.global_event_family=?,'
                . 'bridge_event.first_observed_at=COALESCE(bridge_event.first_observed_at,?)'
                . ' WHERE bridge_event.event_id=? AND bridge_event.company_id=?'
                . ' AND EXISTS (SELECT 1 FROM ' . table_name($config,'event_documents') . ' bridge_ed'
                . ' JOIN ' . table_name($config,'documents') . ' bridge_d'
                . ' ON bridge_d.document_id=bridge_ed.document_id'
                . ' WHERE bridge_ed.event_id=bridge_event.event_id'
                . ' AND bridge_d.source_right_id=\'official:dart\')');
        }
        $companyPublicationEligibilityStmt = $pdo->prepare('SELECT stock_code,listing_status,record_status FROM '
            . table_name($config, 'companies') . ' WHERE company_id=? LIMIT 1');
        $officialActorLookupStmt = $pdo->prepare('SELECT display_name FROM ' . table_name($config, 'actors')
            . ' WHERE actor_id=? LIMIT 1 FOR UPDATE');
        $officialActorStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'actors')
            . ' (actor_id,actor_type,display_name,display_name_en,company_id,country_code,aliases_json,homepage_url,review_status,record_status,created_at,updated_at) '
            . 'VALUES (?,?,?,NULL,?,NULL,\'[]\',NULL,\'pending\',\'inactive\',?,?) ON DUPLICATE KEY UPDATE actor_id=actor_id');
        $officialEventActorStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'event_actors')
            . ' (event_id,actor_id,actor_role,review_status,created_at,updated_at) '
            . 'VALUES (?,?,\'filer\',\'pending\',?,?) ON DUPLICATE KEY UPDATE event_id=event_id');
        $approvedIdentityActorRelationStmt = $pdo->prepare('SELECT COUNT(*) FROM ' . table_name($config, 'event_actors') . ' ingest_identity_ea'
            . ' JOIN ' . table_name($config, 'actors') . ' ingest_identity_a ON ingest_identity_a.actor_id=ingest_identity_ea.actor_id'
            . ' WHERE ingest_identity_ea.event_id=? AND ingest_identity_ea.actor_id=?'
            . ' AND ingest_identity_ea.review_status=\'approved\' AND ingest_identity_a.review_status=\'approved\''
            . ' AND ingest_identity_a.record_status=\'active\' AND NULLIF(TRIM(ingest_identity_a.display_name),\'\') IS NOT NULL');
        foreach ($events as $event) {
            if (!is_array($event)) { continue; }
            $submittedEventId = trim((string)v1_first($event, array('event_id'), ''));
            if (isset($readOnlyDartIdentityMutationEventIds[$submittedEventId])) {
                // The preflight locked and proved the canonical event, its sole
                // DART evidence document and observation, approved actor
                // relation and exact raw source payload. Acknowledge it without
                // touching canonical rows, lifecycle rows or timestamps.
                $readOnlyAckCounts =
                    $readOnlyDartIdentityMutationEventIds[$submittedEventId];
                if (!is_array($readOnlyAckCounts)
                    || !isset(
                        $readOnlyAckCounts['event_documents'],
                        $readOnlyAckCounts['event_observations']
                    )
                    || !is_int($readOnlyAckCounts['event_documents'])
                    || !is_int($readOnlyAckCounts['event_observations'])
                    || $readOnlyAckCounts['event_documents'] < 0
                    || $readOnlyAckCounts['event_observations'] < 0) {
                    throw new RuntimeException(
                        'followup_event_identity_conflict:' . $submittedEventId
                    );
                }
                $counts['events']++;
                $counts['event_documents'] +=
                    (int)$readOnlyAckCounts['event_documents'];
                $counts['event_observations'] +=
                    (int)$readOnlyAckCounts['event_observations'];
                continue;
            }
            if (isset($isolatedReplayCanonicalEventPayloads[$submittedEventId])) {
                $event = $isolatedReplayCanonicalEventPayloads[$submittedEventId];
            }
            $eventId = trim((string)v1_first($event, array('event_id'), ''));
            $companyId = trim((string)v1_first($event, array('company_id', 'corp_code'), ''));
            $eventType = trim((string)v1_first($event, array('event_type'), ''));
            $title = trim((string)v1_first($event, array('title', 'action'), ''));
            $occurred = mysql_dt(v1_first($event, array('occurred_at', 'occurred_on'), null));
            $collectionKey = mb_substr(trim((string)v1_first($event, array('collection_key'), '')), 0, 96, 'UTF-8');
            $isCorrection = !empty($event['is_correction']);
            $isCancelled = !empty($event['is_cancelled']);
            $isEventFollowup = $isCorrection || $isCancelled;
            $followupTitle = $title;
            $followupOccurred = $occurred;
            $followupLanguage = v1_language(v1_first($event, array('original_language', 'language'), 'ko'), 'ko');
            $followupDescription = (string)v1_first($event, array('summary', 'target'), '') ?: null;
            $identityStatus = trim((string)v1_first($event, array('identity_status'), 'needs_review'));
            if (!in_array($identityStatus, array('complete','needs_review'), true)) { $identityStatus = 'needs_review'; }
            $identityAction = trim((string)v1_first($event, array('identity_action'), ''));
            $identityTarget = trim((string)v1_first($event, array('identity_target'), ''));
            $identityActorId = trim((string)v1_first($event, array('identity_actor_id','actor_id'), ''));
            $identityEffectiveInput = v1_first($event, array('identity_effective_at'), null);
            $identityDeadlineInput = v1_first($event, array('identity_deadline_at'), null);
            $identityEffectiveAt = mysql_dt($identityEffectiveInput);
            $identityDeadlineAt = mysql_dt($identityDeadlineInput);
            $comparisonKey = trim((string)v1_first($event, array('comparison_key'), ''));
            $rawDocumentIds = isset($event['document_ids']) && is_array($event['document_ids']) ? $event['document_ids'] : array();
            if (isset($event['document_id'])) { array_unshift($rawDocumentIds, $event['document_id']); }
            $documentIdSet = array();
            foreach ($rawDocumentIds as $rawDocumentId) {
                if (!is_string($rawDocumentId) && !is_int($rawDocumentId)) { continue; }
                $candidateDocumentId = trim((string)$rawDocumentId);
                if (v1_valid_entity_id($candidateDocumentId)) { $documentIdSet[$candidateDocumentId] = true; }
            }
            $documentIds = array_keys($documentIdSet);
            $documentIdsSorted = $documentIds;
            sort($documentIdsSorted,SORT_STRING);
            $canonicalEvent = null;
            $canonicalStoredIdentity = null;
            if (preg_match('/^[0-9]{8}$/', $companyId) && v1_valid_entity_id($eventId)) {
                $candidateCanonicalEvent = v1_pdo_fetch_one_and_close(
                    $eventByIdStmt,
                    array($eventId,$companyId)
                );
                $candidateCanonicalPayload = $candidateCanonicalEvent
                    ? json_decode((string)$candidateCanonicalEvent['payload_json'],true) : null;
                $candidateIsolatedFollowup = $candidateCanonicalEvent
                    && (string)$candidateCanonicalEvent['identity_status'] === 'needs_review'
                    && is_array($candidateCanonicalPayload)
                    && (string)($candidateCanonicalPayload['event_link_status'] ?? '') === 'ambiguous_independent';
                if ($isEventFollowup || $candidateIsolatedFollowup) {
                    $canonicalEvent = $candidateCanonicalEvent;
                }
                if ($canonicalEvent) {
                    $canonicalIdentityComplete = (string)$canonicalEvent['identity_status'] === 'complete';
                    $canonicalStoredIdentity = $canonicalIdentityComplete
                        ? v1_resolve_stored_event_identity($companyId,(string)$canonicalEvent['event_type'],
                            $canonicalEvent['identity_action'],$canonicalEvent['identity_target'],$canonicalEvent['identity_actor_id'],
                            $canonicalEvent['identity_effective_at'],$canonicalEvent['identity_deadline_at'],
                            $canonicalEvent['comparison_key'])
                        : null;
                    if ($canonicalIdentityComplete) {
                        $submittedIdentity = $identityStatus === 'complete'
                            ? v1_build_event_identity($companyId,$eventType,$identityAction,$identityTarget,$identityActorId,
                                $identityEffectiveInput,$identityDeadlineInput,false)
                            : null;
                        $followupIdentityMatches = $submittedIdentity !== null && $canonicalStoredIdentity !== null
                            && preg_match('/^eventcmp:v1:[a-f0-9]{64}$/',$comparisonKey) === 1
                            && hash_equals((string)$submittedIdentity['comparison_key'],$comparisonKey);
                        foreach (array('company_id','event_type','identity_action','identity_target','identity_actor_id',
                            'identity_effective_at','identity_deadline_at','comparison_key') as $identityField) {
                            if (!$followupIdentityMatches
                                || (string)$submittedIdentity[$identityField] !== (string)$canonicalStoredIdentity[$identityField]) {
                                $followupIdentityMatches = false;
                                break;
                            }
                        }
                    } else {
                        $storedPayload = $candidateCanonicalPayload;
                        $storedDocumentRows = v1_pdo_fetch_all_and_close(
                            $eventDocumentIdsStmt,
                            array($submittedEventId)
                        );
                        $storedDocumentIds = array();
                        $storedDocumentRelations = array();
                        foreach ($storedDocumentRows as $storedDocumentRow) {
                            $storedDocumentId = trim((string)($storedDocumentRow['document_id'] ?? ''));
                            if ($storedDocumentId !== '') {
                                $storedDocumentIds[] = $storedDocumentId;
                                $storedDocumentRelations[] = array(
                                    $storedDocumentId,
                                    (string)($storedDocumentRow['relation_type'] ?? ''),
                                    (int)($storedDocumentRow['position_no'] ?? -1),
                                );
                            }
                        }
                        sort($storedDocumentIds,SORT_STRING);
                        $submittedDocumentRelations = array();
                        foreach ($documentIds as $submittedPosition => $submittedDocumentId) {
                            $submittedDocumentRelations[] = array($submittedDocumentId,'evidence',$submittedPosition);
                        }
                        $storedEventFields = array(
                            (string)$canonicalEvent['event_type'],
                            (string)$canonicalEvent['title'],
                            (string)$canonicalEvent['original_language'],
                            (string)$canonicalEvent['occurred_at'],
                            (string)($canonicalEvent['deadline_at'] ?? ''),
                        );
                        $submittedEventFields = array(
                            $eventType,
                            $title,
                            $followupLanguage,
                            (string)($followupOccurred ?? ''),
                            (string)(mysql_dt(v1_first($event,array('deadline_at','deadline'),null)) ?? ''),
                        );
                        $storedIdentityFields = array(
                            (string)($canonicalEvent['identity_action'] ?? ''),
                            (string)($canonicalEvent['identity_target'] ?? ''),
                            (string)($canonicalEvent['identity_actor_id'] ?? ''),
                            (string)($canonicalEvent['identity_effective_at'] ?? ''),
                            (string)($canonicalEvent['identity_deadline_at'] ?? ''),
                            (string)$canonicalEvent['identity_status'],
                            (string)($canonicalEvent['comparison_key'] ?? ''),
                        );
                        $submittedIdentityFields = array(
                            $identityAction,
                            $identityTarget,
                            $identityActorId,
                            (string)($identityEffectiveAt ?? ''),
                            (string)($identityDeadlineAt ?? ''),
                            $identityStatus,
                            $comparisonKey,
                        );
                        $submittedReplayPayload = $event;
                        $submittedReplayPayload['event_link_status'] = 'ambiguous_independent';
                        if (is_array($storedPayload)
                            && isset($storedPayload['metadata'])
                            && is_array($storedPayload['metadata'])
                            && (string)($storedPayload['metadata']['title_provenance'] ?? '') === 'source') {
                            if (!isset($submittedReplayPayload['metadata'])) {
                                $submittedReplayPayload['metadata'] = array();
                            }
                            if (is_array($submittedReplayPayload['metadata'])
                                && !isset($submittedReplayPayload['metadata']['title_provenance'])) {
                                $submittedReplayPayload['metadata']['title_provenance'] = 'source';
                            }
                        }
                        $semanticPayloadMatches = is_array($storedPayload)
                            && hash_equals(
                                hash('sha256',v1_strict_canonical_json_encode(
                                    $storedPayload,
                                    'stored_followup_event_payload_encode_failed'
                                )),
                                hash('sha256',v1_strict_canonical_json_encode(
                                    $submittedReplayPayload,
                                    'submitted_followup_event_payload_encode_failed'
                                ))
                            );
                        $followupIdentityMatches = is_array($storedPayload)
                            && (string)($storedPayload['event_link_status'] ?? '') === 'ambiguous_independent'
                            && !empty($storedPayload['is_correction']) === $isCorrection
                            && !empty($storedPayload['is_cancelled']) === $isCancelled
                            && $semanticPayloadMatches
                            && $storedEventFields === $submittedEventFields
                            && $storedIdentityFields === $submittedIdentityFields
                            && $storedDocumentIds === $documentIdsSorted
                            && $storedDocumentRelations === $submittedDocumentRelations;
                        if ($followupIdentityMatches) {
                            // Preserve the server-added isolation marker so an exact
                            // self replay remains byte-equivalent and idempotent.
                            $event['event_link_status'] = 'ambiguous_independent';
                        }
                    }
                    if (!$followupIdentityMatches) {
                        throw new RuntimeException('followup_event_identity_conflict:' . $submittedEventId);
                    }
                    $eventId = (string)$canonicalEvent['event_id'];
                }
            }
            if ($isEventFollowup && !$canonicalEvent) {
                $event['event_link_status'] = 'ambiguous_independent';
                $counts['event_link_ambiguous']++;
            }
            if (!v1_valid_entity_id($eventId) || !preg_match('/^[0-9]{8}$/', $companyId) || !preg_match('/^[A-Za-z0-9_.:\-]{1,64}$/', $eventType) || $title === '' || $occurred === null) { continue; }
            $importance = (string)v1_first($event, array('importance'), 'medium');
            if ($importance === 'normal') { $importance = 'medium'; }
            if (!in_array($importance, array('low', 'medium', 'high', 'market_sensitive', 'critical'), true)) { $importance = 'medium'; }
            $language = v1_language(v1_first($event, array('original_language', 'language'), 'ko'), 'ko');
            $summary = (string)v1_first($event, array('summary', 'target'), '') ?: null;
            $deadline = mysql_dt(v1_first($event, array('deadline_at', 'deadline'), null));
            if ($canonicalEvent) {
                $eventType = (string)$canonicalEvent['event_type'];
                $title = (string)$canonicalEvent['title'];
                $language = (string)$canonicalEvent['original_language'];
                $summary = isset($canonicalEvent['summary']) ? $canonicalEvent['summary'] : null;
                $occurred = (string)$canonicalEvent['occurred_at'];
                $deadline = isset($canonicalEvent['deadline_at']) ? $canonicalEvent['deadline_at'] : null;
                $importance = (string)$canonicalEvent['importance'];
            }
            if ($canonicalEvent) {
                $canonicalIdentityValues = $canonicalStoredIdentity !== null
                    ? $canonicalStoredIdentity
                    : $canonicalEvent;
                $identityAction = (string)($canonicalIdentityValues['identity_action'] ?? '');
                $identityTarget = (string)($canonicalIdentityValues['identity_target'] ?? '');
                $identityActorId = (string)($canonicalIdentityValues['identity_actor_id'] ?? '');
                $identityEffectiveAt = ($canonicalIdentityValues['identity_effective_at'] ?? null) !== null
                    ? (string)$canonicalIdentityValues['identity_effective_at'] : null;
                $identityDeadlineAt = ($canonicalIdentityValues['identity_deadline_at'] ?? null) !== null
                    ? (string)$canonicalIdentityValues['identity_deadline_at'] : null;
                $identityEffectiveInput = $identityEffectiveAt;
                $identityDeadlineInput = $identityDeadlineAt;
                $identityStatus = $canonicalStoredIdentity !== null ? 'complete' : 'needs_review';
                $comparisonKey = $canonicalStoredIdentity !== null
                    ? (string)$canonicalStoredIdentity['comparison_key'] : '';
            }
            $computedIdentity = null;
            if ($identityStatus === 'complete') {
                $computedIdentity = $canonicalEvent
                    ? $canonicalStoredIdentity
                    : v1_build_event_identity($companyId,$eventType,$identityAction,$identityTarget,$identityActorId,
                        $identityEffectiveInput,$identityDeadlineInput,false);
            }
            $completeIdentityValid = $computedIdentity !== null
                && preg_match('/^eventcmp:v1:[a-f0-9]{64}$/', $comparisonKey) === 1
                && hash_equals((string)$computedIdentity['comparison_key'],$comparisonKey);
            if ($identityStatus === 'complete' && !$completeIdentityValid) {
                throw new RuntimeException('invalid_complete_event_identity:' . $eventId);
            }
            if ($computedIdentity !== null) {
                $identityAction = (string)$computedIdentity['identity_action'];
                $identityTarget = (string)$computedIdentity['identity_target'];
                $identityActorId = (string)$computedIdentity['identity_actor_id'];
                $identityEffectiveAt = (string)$computedIdentity['identity_effective_at'];
                $identityDeadlineAt = (string)$computedIdentity['identity_deadline_at'];
            }
            if ($identityStatus === 'needs_review' && $comparisonKey !== '') {
                throw new RuntimeException('incomplete_event_identity_has_comparison_key:' . $eventId);
            }
            if ($identityStatus === 'needs_review') { $comparisonKey = ''; }
            if ($comparisonKey !== '') {
                // The unique comparison-key lookup also locks the absent-key gap
                // in InnoDB, preventing ON DUPLICATE KEY from ever updating a
                // different event row during a concurrent cross-source ingest.
                $comparisonOwner = v1_pdo_fetch_column_and_close(
                    $eventComparisonOwnerStmt,
                    array($comparisonKey)
                );
                if ($comparisonOwner !== false && (string)$comparisonOwner !== '') {
                    $eventId = (string)$comparisonOwner;
                }
            }
            $storedIdentity = v1_pdo_fetch_one_and_close(
                $eventIdentityStmt,
                array($eventId)
            );
            if ($storedIdentity) {
                if ((string)$storedIdentity['company_id'] !== $companyId || (string)$storedIdentity['event_type'] !== $eventType) {
                    throw new RuntimeException('event_identity_scope_conflict:' . $eventId);
                }
                $incomingIdentity = array($identityAction,$identityTarget,$identityActorId,$identityEffectiveAt,$identityDeadlineAt,$comparisonKey);
                $storedIdentityValues = array((string)($storedIdentity['identity_action'] ?? ''),(string)($storedIdentity['identity_target'] ?? ''),
                    (string)($storedIdentity['identity_actor_id'] ?? ''),(string)($storedIdentity['identity_effective_at'] ?? ''),
                    (string)($storedIdentity['identity_deadline_at'] ?? ''),(string)($storedIdentity['comparison_key'] ?? ''));
                foreach ($incomingIdentity as $identityIndex => $incomingValue) {
                    if ((string)$incomingValue !== '' && $storedIdentityValues[$identityIndex] !== '' && (string)$incomingValue !== $storedIdentityValues[$identityIndex]) {
                        throw new RuntimeException('event_identity_field_conflict:' . $eventId);
                    }
                }
                if ((string)$storedIdentity['identity_status'] === 'complete') {
                    $verifiedStoredIdentity = v1_resolve_stored_event_identity($companyId,$eventType,$storedIdentityValues[0],
                        $storedIdentityValues[1],$storedIdentityValues[2],$storedIdentityValues[3],
                        $storedIdentityValues[4],$storedIdentityValues[5]);
                    if ($verifiedStoredIdentity === null) {
                        throw new RuntimeException('stored_event_identity_integrity_error:' . $eventId);
                    }
                    list($identityAction,$identityTarget,$identityActorId,$identityEffectiveAt,$identityDeadlineAt,$comparisonKey) = $storedIdentityValues;
                    $identityStatus = 'complete';
                } else {
                    $identityAction = $identityAction !== '' ? $identityAction : $storedIdentityValues[0];
                    $identityTarget = $identityTarget !== '' ? $identityTarget : $storedIdentityValues[1];
                    $identityActorId = $identityActorId !== '' ? $identityActorId : $storedIdentityValues[2];
                    $identityEffectiveAt = $identityEffectiveAt !== null ? $identityEffectiveAt : ($storedIdentityValues[3] ?: null);
                    $identityDeadlineAt = $identityDeadlineAt !== null ? $identityDeadlineAt : ($storedIdentityValues[4] ?: null);
                }
            }
            $officialActorCandidateValid = false;
            $officialActorDisplayName = '';
            $officialActorType = '';
            $officialActorCompanyId = null;
            $actorCandidate = isset($event['actor']) && is_array($event['actor']) ? $event['actor'] : null;
            $eventActorCandidate = isset($event['event_actor']) && is_array($event['event_actor']) ? $event['event_actor'] : null;
            if ($identityActorId !== '' && $actorCandidate !== null && $eventActorCandidate !== null) {
                $candidateActorId = v1_normalize_identity_text(v1_first($actorCandidate,array('actor_id'),''));
                $candidateRelationActorId = v1_normalize_identity_text(v1_first($eventActorCandidate,array('actor_id'),''));
                $candidateRelationEventId = trim((string)v1_first($eventActorCandidate,array('event_id'),''));
                $candidateActorType = trim((string)v1_first($actorCandidate,array('actor_type'),''));
                $candidateDisplayName = trim((string)v1_first($actorCandidate,array('display_name'),''));
                $candidateCompanyId = trim((string)v1_first($actorCandidate,array('company_id'),''));
                $candidateRole = trim((string)v1_first($eventActorCandidate,array('actor_role'),''));
                $candidateReviewStatus = trim((string)v1_first($actorCandidate,array('review_status'),'pending'));
                $candidateRecordStatus = trim((string)v1_first($actorCandidate,array('record_status'),'inactive'));
                $candidateRelationReview = trim((string)v1_first($eventActorCandidate,array('review_status'),'pending'));
                $candidateScopeValid = $candidateActorId === $identityActorId
                    && $candidateRelationActorId === $identityActorId
                    && ($candidateRelationEventId === $submittedEventId || $candidateRelationEventId === $eventId)
                    && in_array($candidateActorType,array('company','institution'),true)
                    && $candidateDisplayName !== '' && mb_strlen($candidateDisplayName,'UTF-8') <= 255
                    && $candidateRole === 'filer' && $candidateReviewStatus === 'pending'
                    && $candidateRecordStatus === 'inactive' && $candidateRelationReview === 'pending'
                    && (($candidateActorType === 'company' && $candidateCompanyId === $companyId)
                        || ($candidateActorType === 'institution' && $candidateCompanyId === ''));
                if ($candidateScopeValid) {
                    $existingActorDisplayName = v1_pdo_fetch_column_and_close(
                        $officialActorLookupStmt,
                        array($identityActorId)
                    );
                    $actorNameConsistent = $existingActorDisplayName === false
                        || v1_normalize_identity_text((string)$existingActorDisplayName) === v1_normalize_identity_text($candidateDisplayName);
                    if ($actorNameConsistent) {
                        $officialActorDisplayName = $candidateDisplayName;
                        $officialActorType = $candidateActorType;
                        $officialActorCompanyId = $candidateActorType === 'company' ? $companyId : null;
                        if ($existingActorDisplayName === false) {
                            $officialActorStmt->execute(array($identityActorId,$officialActorType,$officialActorDisplayName,
                                $officialActorCompanyId,$now,$now));
                            $counts['actors']++;
                        }
                        $officialActorCandidateValid = true;
                    }
                }
            }
            $verification = (string)v1_first($event, array('verification_status', 'status'), 'signal');
            if ($verification === 'published') { $verification = 'confirmed'; }
            if ($verification === 'needs_review') { $verification = 'unverified'; }
            if ($verification === 'closed') { $verification = 'confirmed'; }
            $hasTelegramEvidence = false;
            $hasIndependentEvidence = false;
            $hasOfficialDartEvidence = false;
            foreach ($documentIds as $evidenceDocumentId) {
                $evidenceDocumentId = trim((string)$evidenceDocumentId);
                if (!v1_valid_entity_id($evidenceDocumentId)) { continue; }
                $evidenceClass = isset($documentSourceClasses[$evidenceDocumentId]) ? (string)$documentSourceClasses[$evidenceDocumentId] : '';
                $evidenceSourceRightId = isset($documentSourceRightIds[$evidenceDocumentId]) ? (string)$documentSourceRightIds[$evidenceDocumentId] : '';
                if ($evidenceClass === '' || $evidenceSourceRightId === '') {
                    $storedEvidence = v1_pdo_fetch_one_and_close(
                        $documentClassStmt,
                        array($evidenceDocumentId)
                    );
                    if (is_array($storedEvidence)) {
                        if ($evidenceClass === '') { $evidenceClass = (string)($storedEvidence['source_class'] ?? ''); }
                        if ($evidenceSourceRightId === '') {
                            $evidenceSourceRightId = strtolower(trim((string)($storedEvidence['source_right_id'] ?? '')));
                        }
                    }
                }
                if ($evidenceClass === 'official_disclosure' && $evidenceSourceRightId === 'official:dart') {
                    $hasOfficialDartEvidence = true;
                }
                if (in_array($evidenceClass, array('licensed_telegram', 'authorized_telegram'), true)) {
                    $hasTelegramEvidence = true;
                } elseif ($evidenceClass !== '') {
                    $hasIndependentEvidence = true;
                }
            }
            $telegramOnly = $hasTelegramEvidence && !$hasIndependentEvidence;
            $evidenceMissing = !$hasTelegramEvidence && !$hasIndependentEvidence;
            if ($telegramOnly) { $verification = 'signal'; }
            if ($evidenceMissing) { $verification = 'unverified'; }
            $isConfirmed = in_array($verification, array('official', 'confirmed', 'corroborated'), true);
            $publicationCompany = v1_pdo_fetch_one_and_close(
                $companyPublicationEligibilityStmt,
                array($companyId)
            );
            $companyAutoPublishEligible = is_array($publicationCompany)
                && trim((string)($publicationCompany['stock_code'] ?? '')) !== ''
                && in_array((string)($publicationCompany['listing_status'] ?? ''),array('listed','suspended'),true)
                && (string)($publicationCompany['record_status'] ?? '') === 'active';
            $approvedIdentityActorRelation = $identityActorId === '';
            if ($identityActorId !== '') {
                $approvedIdentityActorRelation = (int)v1_pdo_fetch_column_and_close(
                    $approvedIdentityActorRelationStmt,
                    array($eventId,$identityActorId)
                ) > 0;
            }
            $requiresReview = $identityStatus !== 'complete' || $telegramOnly || $evidenceMissing
                || !$companyAutoPublishEligible || !$approvedIdentityActorRelation
                || in_array($importance, array('high', 'market_sensitive', 'critical'), true)
                || !empty($event['review_required']) || $isEventFollowup;
            $previousLifecycle = 'active';
            if ($isCancelled) {
                $storedLifecycle = v1_pdo_fetch_column_and_close(
                    $eventLifecycleStmt,
                    array($eventId)
                );
                if (is_string($storedLifecycle) && $storedLifecycle !== '') { $previousLifecycle = $storedLifecycle; }
                $verification = 'withdrawn';
            } elseif ($isCorrection) {
                $storedLifecycle = v1_pdo_fetch_column_and_close(
                    $eventLifecycleStmt,
                    array($eventId)
                );
                if (is_string($storedLifecycle) && $storedLifecycle !== '') { $previousLifecycle = $storedLifecycle; }
                $verification = 'corrected';
            }
            $review = $isEventFollowup ? 'pending' : ($requiresReview ? 'pending' : ($isConfirmed ? 'not_required' : 'pending'));
            $publication = $isEventFollowup ? 'draft' : ((!$requiresReview && $isConfirmed) ? 'published' : 'draft');
            if ($hasOfficialDartEvidence) {
                if (isset($event['metadata']) && !is_array($event['metadata'])) {
                    throw new RuntimeException('dart_event_metadata_invalid');
                }
                if (!isset($event['metadata'])) { $event['metadata'] = array(); }
                $declaredTitleProvenance = trim((string)($event['metadata']['title_provenance'] ?? ''));
                if ($declaredTitleProvenance !== '' && $declaredTitleProvenance !== 'source') {
                    throw new RuntimeException('dart_title_provenance_conflict');
                }
                // The public v2 title gate may trust this value only after a
                // linked official DART document has been verified above.
                $event['metadata']['title_provenance'] = 'source';
            }
            $eventStmt->execute(array(
                $eventId, $companyId, $eventType, mb_substr($title, 0, 700, 'UTF-8'),
                $language, $summary, $occurred,
                $deadline, $importance, mb_substr($verification, 0, 24, 'UTF-8'),
                $review, $publication, $collectionKey ?: null, $identityAction ?: null, $identityTarget ?: null,
                $identityActorId ?: null, $identityEffectiveAt, $identityDeadlineAt, $identityStatus, $comparisonKey ?: null,
                json_value($event), $now, $now,
            ));
            $counts['events']++;
            if ($officialActorCandidateValid) {
                $officialEventActorStmt->execute(array($eventId,$identityActorId,$now,$now));
                $counts['event_actors']++;
            }
            $position = 0;
            foreach (array_values(array_unique($documentIds)) as $documentId) {
                $documentId = trim((string)$documentId);
                if (!v1_valid_entity_id($documentId)) { continue; }
                $eventDocumentStmt->execute(array($eventId, $documentId, 'evidence', $position, $now));
                $observationDocument = v1_pdo_fetch_one_and_close(
                    $documentObservationStmt,
                    array($documentId)
                );
                if (!$observationDocument) { throw new RuntimeException('event_observation_document_missing:' . $documentId); }
                $observationSource = mb_substr((string)$observationDocument['source_key'],0,191,'UTF-8');
                $observationAt = (string)($observationDocument['retrieved_at'] ?: $now);
                $observationHash = strtolower((string)$observationDocument['content_hash']);
                if (preg_match('/^[a-f0-9]{64}$/', $observationHash) !== 1) { throw new RuntimeException('event_observation_hash_invalid:' . $documentId); }
                $observationId = v1_stable_id('observation',$eventId . '|' . $documentId . '|' . $observationSource);
                $eventObservationStmt->execute(array($observationId,$eventId,$documentId,(string)$observationDocument['source_class'],$observationSource,
                    $observationAt,$observationAt,$observationHash,json_value(array('relation_type'=>'evidence','identity_status'=>$identityStatus)), $now,$now));
                $position++; $counts['event_documents']++; $counts['event_observations']++;
            }
            if ($globalDartProjectionEnabled) {
                $globalEventFamily = v1_global_event_family_for_legacy_type($eventType);
                if ($globalEventFamily !== null) {
                    $globalDartEventStmt->execute(array(
                        'issuer:kr:dart:' . $companyId,$globalEventFamily,$now,$eventId,$companyId,
                    ));
                }
            }
            if ($isCancelled) {
                $cancellationDocumentId = $documentIds ? (string)$documentIds[0] : null;
                $cancellationKey = $eventId . ':cancellation:' . ($cancellationDocumentId ?: $followupOccurred);
                $cancellationTimelineStmt->execute(array(
                    v1_stable_id('timeline', $cancellationKey), $eventId, $cancellationDocumentId, $followupOccurred,
                    mb_substr($followupTitle, 0, 700, 'UTF-8'), $followupDescription, $followupLanguage, $now, $now,
                ));
                $cancellationRevisionStmt->execute(array(
                    v1_stable_id('revision', $cancellationKey), $eventId, $previousLifecycle,
                    'Official cancellation disclosure: ' . ($cancellationDocumentId ?: $eventId), $followupOccurred, $followupOccurred, $now, $now,
                ));
                $counts['timeline_entries']++; $counts['editorial_revisions']++;
            } elseif ($isCorrection) {
                $correctionDocumentId = $documentIds ? (string)$documentIds[0] : null;
                $correctionKey = $eventId . ':correction:' . ($correctionDocumentId ?: $followupOccurred);
                $correctionTimelineStmt->execute(array(
                    v1_stable_id('timeline', $correctionKey), $eventId, $correctionDocumentId, $followupOccurred,
                    mb_substr($followupTitle, 0, 700, 'UTF-8'), $followupDescription, $followupLanguage, $now, $now,
                ));
                $correctionRevisionStmt->execute(array(
                    v1_stable_id('revision', $correctionKey), $eventId, $previousLifecycle,
                    'Official correction disclosure: ' . ($correctionDocumentId ?: $eventId), $followupOccurred, $followupOccurred, $now, $now,
                ));
                $counts['timeline_entries']++; $counts['editorial_revisions']++;
            }
        }

        if ($run) {
            $runId = trim((string)v1_first($run, array('run_id'), ''));
            if (v1_valid_entity_id($runId)) {
                $runMetrics = $run;
                $runMetrics['server_correction_link_ambiguous'] = $counts['correction_link_ambiguous'];
                $runMetrics['server_event_link_ambiguous'] = $counts['event_link_ambiguous'];
                $codeRevision = v1_first($run, array('code_revision'), null);
                if ($codeRevision !== null && $codeRevision !== '') {
                    $codeRevision = v1_valid_build_sha($codeRevision);
                    if ($codeRevision === null) { throw new RuntimeException('invalid_collection_run_code_revision:' . $runId); }
                } else { $codeRevision = null; }
                $runPipeline = mb_substr((string)v1_first($run, array('pipeline'), 'ingest-official'), 0, 64, 'UTF-8');
                $slotClaim = v1_lock_official_slot_claim_for_run(
                    $pdo,$config,$run,$runId,$runPipeline,$codeRevision
                );
                $startedAt = mysql_dt(v1_first($run, array('started_at'), $now)) ?: $now;
                $finishedAt = mysql_dt(v1_first($run, array('finished_at'), $now));
                $firstObservedAt = mysql_dt(v1_first($run, array('first_observed_at'), $startedAt)) ?: $startedAt;
                $rawCount = (int)v1_first($run, array('raw_count'), (int)v1_first($run, array('fetched_count','fetched'), count($documents)));
                $acknowledgedCount = (int)v1_first($run, array('acknowledged_count','ack_count'), (int)v1_first($run, array('accepted_count','accepted'), count($events)));
                if ($rawCount < 0 || $acknowledgedCount < 0) { throw new RuntimeException('invalid_collection_run_counts:' . $runId); }
                $terminalReason = null;
                if ($slotClaim !== null) {
                    if ((int)$slotClaim['late'] === 1) { $terminalReason = 'claim_after_next_cadence'; }
                    elseif ($now >= (string)$slotClaim['next_cadence_slot_at']) {
                        $terminalReason = 'completion_after_next_cadence';
                    }
                }
                $incomingRunStatus = strtolower(trim((string)v1_first($run,array('status'),'failed')));
                $incomingCompleted = in_array($incomingRunStatus,array('success','succeeded'),true)
                    && $rawCount === $acknowledgedCount;
                $completionDigest = $slotClaim === null ? null : v1_official_completion_semantic_sha($run);
                if ($slotClaim !== null) {
                    $terminalAttempt = $slotClaim['completed_run_attempt'] === null
                        ? null : (int)$slotClaim['completed_run_attempt'];
                    $sameTerminalAttempt = $terminalAttempt !== null
                        && $terminalAttempt === (int)v1_official_run_metric($run,'github_run_attempt');
                    if ((string)$slotClaim['status'] === 'completed' || $sameTerminalAttempt) {
                        if ((string)$slotClaim['completed_run_id'] !== $runId
                            || !is_string($slotClaim['completion_sha256'])
                            || !hash_equals((string)$slotClaim['completion_sha256'],(string)$completionDigest)
                            || (int)$slotClaim['completion_raw_count'] !== $rawCount
                            || (int)$slotClaim['completion_ack_count'] !== $acknowledgedCount
                            || ((string)$slotClaim['status'] === 'completed' && !$incomingCompleted)) {
                            throw new RuntimeException('scheduled_slot_claim_completion_conflict:' . $runId);
                        }
                    }
                }
                $completedNoop = $slotClaim !== null && (string)$slotClaim['status'] === 'completed';
                if (!$completedNoop) {
                    $runStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'collection_runs') . ' (run_id, pipeline, source_key, code_revision, status, started_at, finished_at, '
                    . 'first_observed_at, raw_count, acknowledged_count, fetched_count, resolved_count, accepted_count, error_count, lag_seconds_p95, metrics_json, created_at, updated_at) '
                    . 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE pipeline=VALUES(pipeline), source_key=VALUES(source_key), code_revision=VALUES(code_revision), status=VALUES(status), '
                    . 'started_at=VALUES(started_at), finished_at=VALUES(finished_at), first_observed_at=LEAST(first_observed_at,VALUES(first_observed_at)), '
                    . 'raw_count=VALUES(raw_count), acknowledged_count=VALUES(acknowledged_count), fetched_count=VALUES(fetched_count), resolved_count=VALUES(resolved_count), '
                    . 'accepted_count=VALUES(accepted_count), error_count=VALUES(error_count), lag_seconds_p95=VALUES(lag_seconds_p95), metrics_json=VALUES(metrics_json), updated_at=VALUES(updated_at)');
                    $runStmt->execute(array(
                    $runId, $runPipeline,
                    mb_substr((string)v1_first($run, array('source_key'), ''), 0, 191, 'UTF-8') ?: null,
                    $codeRevision,
                    mb_substr((string)v1_first($run, array('status'), 'succeeded'), 0, 24, 'UTF-8'),
                    $startedAt, $finishedAt, $firstObservedAt, $rawCount, $acknowledgedCount,
                    (int)v1_first($run, array('fetched_count', 'fetched'), count($documents)),
                    (int)v1_first($run, array('resolved_count', 'resolved'), count($documents)),
                    (int)v1_first($run, array('accepted_count', 'accepted'), count($events)),
                    (int)v1_first($run, array('error_count', 'errors'), 0),
                    v1_first($run, array('lag_seconds_p95'), null) !== null ? (int)$run['lag_seconds_p95'] : null,
                    json_value($runMetrics), $now, $now,
                    ));
                    if ($slotClaim !== null) {
                        $claimStatus = $incomingCompleted && $terminalReason === null ? 'completed' : 'failed';
                        if ($terminalReason !== null) { $terminalCompletionFailure = true; }
                        $githubAttempt = (int)v1_official_run_metric($run,'github_run_attempt');
                        $claimUpdate = $pdo->prepare('UPDATE ' . table_name($config,'official_slot_claims')
                            . ' SET status=?,terminal_reason=?,failed_at=?,completed_run_id=?,completed_run_attempt=?,completion_raw_count=?,completion_ack_count=?,completion_sha256=?,'
                            . 'completed_at=?,updated_at=? WHERE claim_id=?');
                        $claimUpdate->execute(array($claimStatus,$terminalReason,$terminalReason === null ? null : $now,
                            $runId,$githubAttempt,$rawCount,$acknowledgedCount,$completionDigest,
                            $claimStatus === 'completed' ? $now : null,$now,
                            (string)$slotClaim['claim_id']));
                    }
                }
                if ($globalDartProjectionEnabled) {
                    v1_bridge_dart_connector_run(
                        $pdo,$config,$dartConnector,$run,$runId,$codeRevision,
                        $finishedAt,$firstObservedAt,$now,
                        $terminalReason === null
                    );
                }
                $counts['runs']++;
            }
        }
        $pdo->commit();
    } catch (Throwable $e) {
        $failureError = $e;
        try {
            if ($pdo->inTransaction() && $pdo->rollBack() !== true) {
                $failureError = new RuntimeException('governance_snapshot_rollback_failed');
            }
        } catch (Throwable $rollbackError) {
            $failureError = $rollbackError;
        }
        $failure = v1_governance_snapshot_failure_response($failureError);
        respond($failure['status'],$failure['payload']);
    }
    if ($terminalCompletionFailure) {
        respond(409,array('ok'=>false,'error'=>'official_slot_completion_terminal_failure','upserted'=>$counts));
    }
    respond(200, array(
        'ok' => true,
        'upserted' => $counts,
        'backend_binding_id' => $backendBindingId,
    ));
}

function v1_editorial_reference_exists(PDO $pdo, array $config, string $table, string $primary, string $value): bool {
    $allowed = array(
        'companies' => 'company_id', 'actors' => 'actor_id', 'governance_events' => 'event_id',
        'campaigns' => 'campaign_id', 'documents' => 'document_id',
    );
    if (!isset($allowed[$table]) || $allowed[$table] !== $primary) { return false; }
    $stmt = $pdo->prepare('SELECT COUNT(*) FROM ' . table_name($config, $table) . ' WHERE ' . $primary . ' = ?');
    $stmt->execute(array($value));
    return (int)$stmt->fetchColumn() === 1;
}

function v1_editorial_require_reference(PDO $pdo, array $config, string $table, string $primary, ?string $value, string $field): void {
    if ($value === null || !v1_editorial_reference_exists($pdo, $config, $table, $primary, $value)) {
        v1_editorial_invalid($field . ': referenced record not found');
    }
}

function v1_editorial_parent_company(PDO $pdo, array $config, string $table, string $primary, ?string $value): ?string {
    if ($value === null) { return null; }
    $allowed = array('governance_events' => 'event_id', 'campaigns' => 'campaign_id');
    if (!isset($allowed[$table]) || $allowed[$table] !== $primary) { v1_editorial_invalid('invalid parent lookup'); }
    $stmt = $pdo->prepare('SELECT company_id FROM ' . table_name($config, $table) . ' WHERE ' . $primary . ' = ? LIMIT 1');
    $stmt->execute(array($value)); $companyId = $stmt->fetchColumn();
    if (!is_string($companyId) || $companyId === '') { v1_editorial_invalid($primary . ': referenced record not found'); }
    return $companyId;
}

function v1_editorial_validate_references(PDO $pdo, array $config, string $entity, array $record): void {
    if ($entity === 'actors') {
        if ($record['company_id'] !== null) { v1_editorial_require_reference($pdo, $config, 'companies', 'company_id', $record['company_id'], 'company_id'); }
        return;
    }
    if ($entity === 'event_actors') {
        v1_editorial_require_reference($pdo, $config, 'governance_events', 'event_id', $record['event_id'], 'event_id');
        v1_editorial_require_reference($pdo, $config, 'actors', 'actor_id', $record['actor_id'], 'actor_id');
        return;
    }
    if ($entity === 'campaigns') {
        v1_editorial_require_reference($pdo, $config, 'companies', 'company_id', $record['company_id'], 'company_id');
        v1_editorial_require_reference($pdo, $config, 'actors', 'actor_id', $record['lead_actor_id'], 'lead_actor_id');
        foreach ($record['evidence_document_ids'] as $documentId) {
            v1_editorial_require_reference($pdo, $config, 'documents', 'document_id', $documentId, 'evidence_document_ids');
        }
        return;
    }
    if ($entity === 'claim_evidence') {
        v1_editorial_require_reference($pdo, $config, 'governance_events', 'event_id', $record['event_id'], 'event_id');
        if ($record['campaign_id'] !== null) { v1_editorial_require_reference($pdo, $config, 'campaigns', 'campaign_id', $record['campaign_id'], 'campaign_id'); }
        if ($record['actor_id'] !== null) { v1_editorial_require_reference($pdo, $config, 'actors', 'actor_id', $record['actor_id'], 'actor_id'); }
        v1_editorial_require_reference($pdo, $config, 'documents', 'document_id', $record['document_id'], 'document_id');
        $eventCompany = v1_editorial_parent_company($pdo, $config, 'governance_events', 'event_id', $record['event_id']);
        $campaignCompany = v1_editorial_parent_company($pdo, $config, 'campaigns', 'campaign_id', $record['campaign_id']);
        if ($campaignCompany !== null && $campaignCompany !== $eventCompany) { v1_editorial_invalid('event_id/campaign_id: company mismatch'); }
        return;
    }
    if (isset($record['company_id'])) { v1_editorial_require_reference($pdo, $config, 'companies', 'company_id', $record['company_id'], 'company_id'); }
    if ($record['event_id'] !== null) { v1_editorial_require_reference($pdo, $config, 'governance_events', 'event_id', $record['event_id'], 'event_id'); }
    if ($record['campaign_id'] !== null) { v1_editorial_require_reference($pdo, $config, 'campaigns', 'campaign_id', $record['campaign_id'], 'campaign_id'); }
    if (isset($record['proposer_actor_id']) && $record['proposer_actor_id'] !== null) {
        v1_editorial_require_reference($pdo, $config, 'actors', 'actor_id', $record['proposer_actor_id'], 'proposer_actor_id');
    }
    $documentId = isset($record['evidence_document_id']) ? $record['evidence_document_id'] : $record['document_id'];
    v1_editorial_require_reference($pdo, $config, 'documents', 'document_id', $documentId, 'document_id');
    $eventCompany = v1_editorial_parent_company($pdo, $config, 'governance_events', 'event_id', $record['event_id']);
    $campaignCompany = v1_editorial_parent_company($pdo, $config, 'campaigns', 'campaign_id', $record['campaign_id']);
    if ($eventCompany !== null && $campaignCompany !== null && $eventCompany !== $campaignCompany) {
        v1_editorial_invalid('event_id/campaign_id: company mismatch');
    }
    if (isset($record['company_id'])) {
        $parentCompany = $eventCompany !== null ? $eventCompany : $campaignCompany;
        if ($parentCompany !== null && $parentCompany !== $record['company_id']) { v1_editorial_invalid('company_id: parent company mismatch'); }
    }
}

function v1_editorial_apply_record(PDO $pdo, array $config, string $entity, array $record, string $now): void {
    if ($entity === 'actors') {
        $unchanged = '(actor_type <=> VALUES(actor_type) AND display_name <=> VALUES(display_name) AND display_name_en <=> VALUES(display_name_en)'
            . ' AND company_id <=> VALUES(company_id) AND country_code <=> VALUES(country_code) AND aliases_json <=> VALUES(aliases_json) AND homepage_url <=> VALUES(homepage_url))';
        $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'actors') . ' (actor_id, actor_type, display_name, display_name_en, company_id, country_code, aliases_json, homepage_url, review_status, record_status, created_at, updated_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,\'pending\',\'inactive\',?,?) ON DUPLICATE KEY UPDATE review_status=IF(' . $unchanged . ',review_status,\'pending\'), '
            . 'record_status=IF(' . $unchanged . ',record_status,\'inactive\'), updated_at=IF(' . $unchanged . ',updated_at,GREATEST(VALUES(updated_at),DATE_ADD(updated_at, INTERVAL 1 SECOND))), '
            . 'actor_type=VALUES(actor_type), display_name=VALUES(display_name), '
            . 'display_name_en=VALUES(display_name_en), company_id=VALUES(company_id), country_code=VALUES(country_code), aliases_json=VALUES(aliases_json), '
            . 'homepage_url=VALUES(homepage_url)');
        $stmt->execute(array($record['actor_id'], $record['actor_type'], $record['display_name'], $record['display_name_en'], $record['company_id'],
            $record['country_code'], json_value($record['aliases']), $record['homepage_url'], $now, $now));
        return;
    }
    if ($entity === 'event_actors') {
        $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'event_actors') . ' (event_id, actor_id, actor_role, review_status, created_at, updated_at) '
            . 'VALUES (?,?,?,\'pending\',?,?) ON DUPLICATE KEY UPDATE review_status=review_status, updated_at=updated_at');
        $stmt->execute(array($record['event_id'], $record['actor_id'], $record['actor_role'], $now, $now));
        return;
    }
    if ($entity === 'campaigns') {
        $unchanged = '(payload_json <=> VALUES(payload_json))';
        $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'campaigns') . ' (campaign_id, company_id, lead_actor_id, title, original_language, demand_text, stage, outcome, started_at, ended_at, review_status, publication_status, payload_json, created_at, updated_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,?,?,\'pending\',\'draft\',?,?,?) ON DUPLICATE KEY UPDATE review_status=IF(' . $unchanged . ',review_status,\'pending\'), '
            . 'publication_status=IF(' . $unchanged . ',publication_status,\'draft\'), updated_at=IF(' . $unchanged . ',updated_at,GREATEST(VALUES(updated_at),DATE_ADD(updated_at, INTERVAL 1 SECOND))), '
            . 'company_id=VALUES(company_id), lead_actor_id=VALUES(lead_actor_id), '
            . 'title=VALUES(title), original_language=VALUES(original_language), demand_text=VALUES(demand_text), stage=VALUES(stage), outcome=VALUES(outcome), '
            . 'started_at=VALUES(started_at), ended_at=VALUES(ended_at), payload_json=VALUES(payload_json)');
        $stmt->execute(array($record['campaign_id'], $record['company_id'], $record['lead_actor_id'], $record['title'], $record['original_language'], $record['demand_text'],
            $record['stage'], $record['outcome'], $record['started_at'], $record['ended_at'], json_value($record), $now, $now));
        $delete = $pdo->prepare('DELETE FROM ' . table_name($config, 'campaign_documents') . ' WHERE campaign_id = ?');
        $delete->execute(array($record['campaign_id']));
        $link = $pdo->prepare('INSERT INTO ' . table_name($config, 'campaign_documents') . ' (campaign_id, document_id, relation_type, position_no, created_at) VALUES (?,?,\'evidence\',?,?)');
        foreach ($record['evidence_document_ids'] as $position => $documentId) { $link->execute(array($record['campaign_id'], $documentId, $position, $now)); }
        return;
    }
    if ($entity === 'claim_evidence') {
        $unchanged = '(event_id <=> VALUES(event_id) AND campaign_id <=> VALUES(campaign_id) AND actor_id <=> VALUES(actor_id) AND document_id <=> VALUES(document_id)'
            . ' AND claim_type <=> VALUES(claim_type) AND claim_text <=> VALUES(claim_text) AND original_language <=> VALUES(original_language) AND evidence_locator <=> VALUES(evidence_locator))';
        $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'claim_evidence') . ' (claim_id, event_id, campaign_id, actor_id, document_id, claim_type, claim_text, original_language, evidence_locator, editorial_status, created_at, updated_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,?,\'pending\',?,?) ON DUPLICATE KEY UPDATE editorial_status=IF(' . $unchanged . ',editorial_status,\'pending\'), '
            . 'updated_at=IF(' . $unchanged . ',updated_at,GREATEST(VALUES(updated_at),DATE_ADD(updated_at, INTERVAL 1 SECOND))), event_id=VALUES(event_id), campaign_id=VALUES(campaign_id), actor_id=VALUES(actor_id), '
            . 'document_id=VALUES(document_id), claim_type=VALUES(claim_type), claim_text=VALUES(claim_text), original_language=VALUES(original_language), '
            . 'evidence_locator=VALUES(evidence_locator)');
        $stmt->execute(array($record['claim_id'], $record['event_id'], $record['campaign_id'], $record['actor_id'], $record['document_id'], $record['claim_type'],
            $record['claim_text'], $record['original_language'], $record['evidence_locator'], $now, $now));
        return;
    }
    if ($entity === 'proposal_votes') {
        $unchanged = '(event_id <=> VALUES(event_id) AND campaign_id <=> VALUES(campaign_id) AND company_id <=> VALUES(company_id)'
            . ' AND proposer_actor_id <=> VALUES(proposer_actor_id) AND agenda_no <=> VALUES(agenda_no) AND agenda_title <=> VALUES(agenda_title)'
            . ' AND original_language <=> VALUES(original_language) AND meeting_at <=> VALUES(meeting_at) AND recommendation <=> VALUES(recommendation)'
            . ' AND recommendation_source <=> VALUES(recommendation_source) AND result <=> VALUES(result) AND votes_for <=> VALUES(votes_for)'
            . ' AND votes_against <=> VALUES(votes_against) AND votes_abstain <=> VALUES(votes_abstain) AND evidence_document_id <=> VALUES(evidence_document_id))';
        $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'proposal_votes') . ' (proposal_vote_id, event_id, campaign_id, company_id, proposer_actor_id, agenda_no, agenda_title, original_language, meeting_at, recommendation, recommendation_source, result, votes_for, votes_against, votes_abstain, evidence_document_id, review_status, publication_status, created_at, updated_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,\'pending\',\'draft\',?,?) ON DUPLICATE KEY UPDATE review_status=IF(' . $unchanged . ',review_status,\'pending\'), '
            . 'publication_status=IF(' . $unchanged . ',publication_status,\'draft\'), updated_at=IF(' . $unchanged . ',updated_at,GREATEST(VALUES(updated_at),DATE_ADD(updated_at, INTERVAL 1 SECOND))), '
            . 'event_id=VALUES(event_id), campaign_id=VALUES(campaign_id), company_id=VALUES(company_id), '
            . 'proposer_actor_id=VALUES(proposer_actor_id), agenda_no=VALUES(agenda_no), agenda_title=VALUES(agenda_title), original_language=VALUES(original_language), meeting_at=VALUES(meeting_at), '
            . 'recommendation=VALUES(recommendation), recommendation_source=VALUES(recommendation_source), result=VALUES(result), votes_for=VALUES(votes_for), votes_against=VALUES(votes_against), '
            . 'votes_abstain=VALUES(votes_abstain), evidence_document_id=VALUES(evidence_document_id)');
        $stmt->execute(array($record['proposal_vote_id'], $record['event_id'], $record['campaign_id'], $record['company_id'], $record['proposer_actor_id'], $record['agenda_no'],
            $record['agenda_title'], $record['original_language'], $record['meeting_at'], $record['recommendation'], $record['recommendation_source'], $record['result'],
            $record['votes_for'], $record['votes_against'], $record['votes_abstain'], $record['evidence_document_id'], $now, $now));
        return;
    }
    if ($entity === 'commitment_outcomes') {
        $unchanged = '(event_id <=> VALUES(event_id) AND campaign_id <=> VALUES(campaign_id) AND company_id <=> VALUES(company_id)'
            . ' AND commitment_text <=> VALUES(commitment_text) AND original_language <=> VALUES(original_language) AND target_at <=> VALUES(target_at)'
            . ' AND actual_action <=> VALUES(actual_action) AND status <=> VALUES(status) AND target_metrics_json <=> VALUES(target_metrics_json)'
            . ' AND actual_metrics_json <=> VALUES(actual_metrics_json) AND evidence_document_id <=> VALUES(evidence_document_id))';
        $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'commitment_outcomes') . ' (commitment_id, event_id, campaign_id, company_id, commitment_text, original_language, target_at, actual_action, status, target_metrics_json, actual_metrics_json, evidence_document_id, review_status, publication_status, created_at, updated_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,\'pending\',\'draft\',?,?) ON DUPLICATE KEY UPDATE review_status=IF(' . $unchanged . ',review_status,\'pending\'), '
            . 'publication_status=IF(' . $unchanged . ',publication_status,\'draft\'), updated_at=IF(' . $unchanged . ',updated_at,GREATEST(VALUES(updated_at),DATE_ADD(updated_at, INTERVAL 1 SECOND))), '
            . 'event_id=VALUES(event_id), campaign_id=VALUES(campaign_id), company_id=VALUES(company_id), '
            . 'commitment_text=VALUES(commitment_text), original_language=VALUES(original_language), target_at=VALUES(target_at), actual_action=VALUES(actual_action), status=VALUES(status), '
            . 'target_metrics_json=VALUES(target_metrics_json), actual_metrics_json=VALUES(actual_metrics_json), evidence_document_id=VALUES(evidence_document_id), '
            . 'commitment_id=commitment_id');
        $stmt->execute(array($record['commitment_id'], $record['event_id'], $record['campaign_id'], $record['company_id'], $record['commitment_text'], $record['original_language'],
            $record['target_at'], $record['actual_action'], $record['status'], $record['target_metrics'] === null ? null : json_value($record['target_metrics']),
            $record['actual_metrics'] === null ? null : json_value($record['actual_metrics']), $record['evidence_document_id'], $now, $now));
        return;
    }
    $unchanged = '(event_id <=> VALUES(event_id) AND campaign_id <=> VALUES(campaign_id) AND document_id <=> VALUES(document_id)'
        . ' AND occurred_at <=> VALUES(occurred_at) AND entry_type <=> VALUES(entry_type) AND title <=> VALUES(title)'
        . ' AND description <=> VALUES(description) AND original_language <=> VALUES(original_language))';
    $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'timeline_entries') . ' (timeline_entry_id, event_id, campaign_id, document_id, occurred_at, entry_type, title, description, original_language, review_status, publication_status, created_at, updated_at) '
        . 'VALUES (?,?,?,?,?,?,?,?,?,\'pending\',\'draft\',?,?) ON DUPLICATE KEY UPDATE review_status=IF(' . $unchanged . ',review_status,\'pending\'), '
        . 'publication_status=IF(' . $unchanged . ',publication_status,\'draft\'), updated_at=IF(' . $unchanged . ',updated_at,GREATEST(VALUES(updated_at),DATE_ADD(updated_at, INTERVAL 1 SECOND))), '
        . 'event_id=VALUES(event_id), campaign_id=VALUES(campaign_id), document_id=VALUES(document_id), '
        . 'occurred_at=VALUES(occurred_at), entry_type=VALUES(entry_type), title=VALUES(title), description=VALUES(description), original_language=VALUES(original_language), '
        . 'timeline_entry_id=timeline_entry_id');
    $stmt->execute(array($record['timeline_entry_id'], $record['event_id'], $record['campaign_id'], $record['document_id'], $record['occurred_at'],
        $record['entry_type'], $record['title'], $record['description'], $record['original_language'], $now, $now));
}

/** HMAC action contract for the exact seven-array, fail-closed editorial chunk. */
function upsert_editorial_snapshot(PDO $pdo, array $config, array $payload): void {
    $entities = array('actors','event_actors','campaigns','claim_evidence','proposal_votes','commitment_outcomes','timeline_entries');
    $allowedTop = array_merge(array('schema_version','bundle_sha256','chunk_id','chunk_index','chunk_count'), $entities);
    try {
        v1_editorial_assert_keys($payload, $allowedTop, 'payload');
        if (!isset($payload['schema_version']) || !is_int($payload['schema_version']) || $payload['schema_version'] !== 1) { v1_editorial_invalid('schema_version: must be 1'); }
        $bundle = isset($payload['bundle_sha256']) && is_string($payload['bundle_sha256']) ? $payload['bundle_sha256'] : '';
        if (preg_match('/^[a-f0-9]{64}$/', $bundle) !== 1) { v1_editorial_invalid('bundle_sha256: invalid'); }
        $chunkIndex = isset($payload['chunk_index']) && is_int($payload['chunk_index']) ? $payload['chunk_index'] : 0;
        $chunkCount = isset($payload['chunk_count']) && is_int($payload['chunk_count']) ? $payload['chunk_count'] : 0;
        if ($chunkIndex < 1 || $chunkCount < 1 || $chunkIndex > $chunkCount || $chunkCount > 9999) { v1_editorial_invalid('chunk bounds: invalid'); }
        $chunkId = isset($payload['chunk_id']) && is_string($payload['chunk_id']) ? $payload['chunk_id'] : '';
        $expectedChunkId = 'editorial:' . substr($bundle, 0, 32) . ':' . sprintf('%04d', $chunkIndex);
        if ($chunkId !== $expectedChunkId || !v1_valid_entity_id($chunkId)) { v1_editorial_invalid('chunk_id: invalid'); }
        $activeEntity = null; $records = array();
        foreach ($entities as $entity) {
            if (!array_key_exists($entity, $payload) || !is_array($payload[$entity]) || !v1_editorial_is_list($payload[$entity])) {
                v1_editorial_invalid($entity . ': array required');
            }
            if (count($payload[$entity]) > 0) {
                if ($activeEntity !== null) { v1_editorial_invalid('exactly one entity array must be non-empty'); }
                $activeEntity = $entity; $records = $payload[$entity];
            }
        }
        if ($activeEntity === null || count($records) > 500) { v1_editorial_invalid('one to 500 records required'); }
        $normalized = array();
        foreach ($records as $index => $record) {
            if (!is_array($record) || v1_editorial_is_list($record)) { v1_editorial_invalid($activeEntity . '[' . $index . ']: object required'); }
            $normalized[] = v1_editorial_normalize_record($activeEntity, $record, $index);
        }
    } catch (InvalidArgumentException $e) {
        respond(400, array('ok' => false, 'error' => 'editorial_validation_failed', 'detail' => $e->getMessage()));
    }
    $payloadHash = hash('sha256', json_value($records));
    $accepted = array($activeEntity => count($normalized));
    $now = gmdate('Y-m-d H:i:s');
    $pdo->beginTransaction();
    try {
        $existing = $pdo->prepare('SELECT chunk_id, bundle_sha256, chunk_index, chunk_count, entity_type, payload_sha256, accepted_json FROM ' . table_name($config, 'editorial_ingest_chunks')
            . ' WHERE chunk_id = ? OR (bundle_sha256 = ? AND chunk_index = ?) FOR UPDATE');
        $existing->execute(array($chunkId, $bundle, $chunkIndex)); $chunk = $existing->fetch();
        if ($chunk) {
            $same = (string)$chunk['chunk_id'] === $chunkId && (string)$chunk['bundle_sha256'] === $bundle
                && (int)$chunk['chunk_index'] === $chunkIndex && (int)$chunk['chunk_count'] === $chunkCount && (string)$chunk['entity_type'] === $activeEntity
                && hash_equals((string)$chunk['payload_sha256'], $payloadHash);
            if (!$same) { $pdo->rollBack(); respond(409, array('ok' => false, 'error' => 'editorial_chunk_conflict')); }
            $storedAccepted = json_decode((string)$chunk['accepted_json'], true);
            $pdo->commit();
            respond(200, array('ok' => true, 'accepted' => is_array($storedAccepted) ? $storedAccepted : $accepted, 'rejected' => 0, 'idempotent' => true));
        }
        $bundleState = $pdo->prepare('SELECT chunk_count FROM ' . table_name($config, 'editorial_ingest_chunks')
            . ' WHERE bundle_sha256 = ? ORDER BY chunk_index ASC LIMIT 1 FOR UPDATE');
        $bundleState->execute(array($bundle)); $storedChunkCount = $bundleState->fetchColumn();
        if ($storedChunkCount !== false && (int)$storedChunkCount !== $chunkCount) {
            $pdo->rollBack(); respond(409, array('ok' => false, 'error' => 'editorial_bundle_chunk_count_conflict'));
        }
        foreach ($normalized as $record) {
            v1_editorial_validate_references($pdo, $config, $activeEntity, $record);
            v1_editorial_apply_record($pdo, $config, $activeEntity, $record, $now);
        }
        $insertChunk = $pdo->prepare('INSERT INTO ' . table_name($config, 'editorial_ingest_chunks') . ' (chunk_id, bundle_sha256, chunk_index, chunk_count, entity_type, payload_sha256, accepted_json, created_at) VALUES (?,?,?,?,?,?,?,?)');
        $insertChunk->execute(array($chunkId, $bundle, $chunkIndex, $chunkCount, $activeEntity, $payloadHash, json_value($accepted), $now));
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        if ($e instanceof InvalidArgumentException) {
            respond(409, array('ok' => false, 'error' => 'editorial_reference_validation_failed', 'detail' => $e->getMessage()));
        }
        if ((string)$e->getCode() === '23000') {
            $retry = $pdo->prepare('SELECT chunk_id, chunk_count, entity_type, payload_sha256, accepted_json FROM ' . table_name($config, 'editorial_ingest_chunks') . ' WHERE bundle_sha256 = ? AND chunk_index = ? LIMIT 1');
            $retry->execute(array($bundle, $chunkIndex)); $chunk = $retry->fetch();
            if ($chunk && (string)$chunk['chunk_id'] === $chunkId && (int)$chunk['chunk_count'] === $chunkCount && (string)$chunk['entity_type'] === $activeEntity
                && hash_equals((string)$chunk['payload_sha256'], $payloadHash)) {
                $storedAccepted = json_decode((string)$chunk['accepted_json'], true);
                respond(200, array('ok' => true, 'accepted' => is_array($storedAccepted) ? $storedAccepted : $accepted, 'rejected' => 0, 'idempotent' => true));
            }
        }
        throw $e;
    }
    respond(200, array('ok' => true, 'accepted' => $accepted, 'rejected' => 0, 'idempotent' => false));
}

function delivery_event_is_publishable(PDO $pdo, array $config, string $eventId): bool {
    if ($eventId === '') { return false; }
    $stmt = $pdo->prepare('SELECT COUNT(*) FROM ' . table_name($config, 'governance_events') . ' delivery_e'
        . ' WHERE delivery_e.event_id = ? AND ' . v1_event_visibility_sql($config, 'delivery_e'));
    $stmt->execute(array($eventId));
    return (int)$stmt->fetchColumn() === 1;
}

function enqueue_delivery_outbox(PDO $pdo, array $config, array $payload): void {
    // Product invariant: governance distribution is web-only.  Keep historical
    // rows auditable, but never permit any HMAC caller to create new outbound work.
    respond(410,array('ok'=>false,'error'=>'outbound_delivery_disabled',
        'distribution_mode'=>'web_only','accepted'=>0));
    $deliveries = isset($payload['deliveries']) && is_array($payload['deliveries']) ? $payload['deliveries'] : array();
    if (count($deliveries) > 500) { respond(413, array('ok' => false, 'error' => 'too_many_deliveries')); }
    $now = gmdate('Y-m-d H:i:s');
    $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'delivery_outbox') . ' (delivery_id, event_id, delivery_channel, destination, idempotency_key, '
        . 'payload_json, status, attempt_count, next_attempt_at, created_at, updated_at) VALUES (?,?,?,?,?,?,\'pending\',0,?,?,?) '
        . 'ON DUPLICATE KEY UPDATE updated_at=VALUES(updated_at)');
    $accepted = 0; $rejected = 0;
    foreach ($deliveries as $delivery) {
        if (!is_array($delivery)) { $rejected++; continue; }
        $channel = trim((string)v1_first($delivery, array('delivery_channel', 'channel'), ''));
        $destination = trim((string)v1_first($delivery, array('destination'), ''));
        $idempotency = trim((string)v1_first($delivery, array('idempotency_key'), ''));
        if (!preg_match('/^[A-Za-z0-9_.:\-]{1,40}$/', $channel) || $destination === '' || $idempotency === '') { $rejected++; continue; }
        $id = trim((string)v1_first($delivery, array('delivery_id'), ''));
        if ($id === '') { $id = v1_stable_id('delivery', $channel . ':' . $destination . ':' . $idempotency); }
        if (!v1_valid_entity_id($id) || mb_strlen($destination, 'UTF-8') > 191 || mb_strlen($idempotency, 'UTF-8') > 191) { $rejected++; continue; }
        $eventId = trim((string)v1_first($delivery, array('event_id'), ''));
        if ($eventId !== '' && !v1_valid_entity_id($eventId)) { $rejected++; continue; }
        if ($eventId !== '' && !delivery_event_is_publishable($pdo, $config, $eventId)) { $rejected++; continue; }
        $content = isset($delivery['payload']) && is_array($delivery['payload']) ? $delivery['payload'] : array();
        if (!$content) { $rejected++; continue; }
        $contentJson = json_value($content);
        if (!delivery_source_rights_valid($pdo, $config, $contentJson)) { $rejected++; continue; }
        $stmt->execute(array(
            $id, $eventId ?: null, $channel, $destination, $idempotency, $contentJson,
            mysql_dt(v1_first($delivery, array('next_attempt_at'), null)), $now, $now,
        ));
        $accepted++;
    }
    respond(200, array('ok' => true, 'accepted' => $accepted, 'rejected' => $rejected));
}

/**
 * Returns false unless the signed producer explicitly attests that the rights
 * lineage is complete and supplies an array (which may be empty for sources
 * that do not require a contractual SourceRight). Otherwise returns the
 * de-duplicated SourceRight IDs that must still be active at claim time.
 */
function delivery_payload_source_right_ids(string $payloadJson) {
    $decoded = json_decode($payloadJson, true);
    if (!is_array($decoded) || !array_key_exists('rights_lineage_complete', $decoded)
        || $decoded['rights_lineage_complete'] !== true || !array_key_exists('source_right_ids', $decoded)) { return false; }
    if (!is_array($decoded['source_right_ids'])) { return false; }
    $ids = array();
    foreach ($decoded['source_right_ids'] as $value) {
        $id = trim((string)$value);
        if (!v1_valid_entity_id($id, 64)) { return false; }
        if ($id !== '') { $ids[$id] = true; }
    }
    return array_keys($ids);
}

function delivery_source_rights_valid(PDO $pdo, array $config, string $payloadJson): bool {
    $ids = delivery_payload_source_right_ids($payloadJson);
    if ($ids === false) { return false; }
    if (!$ids) { return true; }
    $placeholders = implode(',', array_fill(0, count($ids), '?'));
    $sql = 'SELECT COUNT(DISTINCT sr.source_right_id) FROM ' . table_name($config, 'source_rights') . ' sr'
        . ' WHERE sr.source_right_id IN (' . $placeholders . ') AND ' . source_right_redistribution_sql('sr');
    $stmt = $pdo->prepare($sql); $stmt->execute($ids);
    return (int)$stmt->fetchColumn() === count($ids);
}

function claim_delivery_outbox(PDO $pdo, array $config, array $payload): void {
    // Product invariant: no worker may lease historical or newly submitted
    // delivery rows while outbound delivery is permanently disabled.
    respond(410,array('ok'=>false,'error'=>'outbound_delivery_disabled',
        'distribution_mode'=>'web_only','claimed'=>0));
    $worker = trim((string)v1_first($payload, array('worker_id'), 'publisher'));
    if (!v1_valid_entity_id($worker, 96)) { respond(400, array('ok' => false, 'error' => 'invalid_worker_id')); }
    $channel = trim((string)v1_first($payload, array('channel', 'delivery_channel'), ''));
    if ($channel !== '' && !preg_match('/^[A-Za-z0-9_.:\-]{1,40}$/', $channel)) { respond(400, array('ok' => false, 'error' => 'invalid_delivery_channel')); }
    $requestedDeliveryId = trim((string)v1_first($payload, array('delivery_id', 'outbox_id'), ''));
    if ($requestedDeliveryId !== '' && !v1_valid_entity_id($requestedDeliveryId)) { respond(400, array('ok' => false, 'error' => 'invalid_delivery_id')); }
    // Delivery claims are intentionally singleton. A batch lease starts the
    // clock for later rows before their external send begins, which can allow a
    // second worker to reclaim and duplicate them. Throughput comes from
    // repeated singleton claims after each row is durably acknowledged.
    $limit = 1;
    $leaseSeconds = max(300, min(1800, (int)v1_first($payload, array('lease_seconds'), 900)));
    $leaseToken = 'lease_' . bin2hex(random_bytes(16));
    $requestedStatus = null;
    $requestedExternalId = null;
    $blockedCount = 0;
    $rightsBlockedCount = 0;
    $editorialBlockedCount = 0;
    $outcomeUnknownCount = 0;
    $rows = array();
    $pdo->beginTransaction();
    try {
        // A worker can disappear after Telegram accepted sendMessage but before
        // the external message ID was acknowledged. Telegram offers no caller
        // idempotency key, so reclaiming an expired processing lease could
        // duplicate a market-sensitive alert. Quarantine every such lease for
        // explicit reconciliation instead of attempting an automatic resend.
        $expiredSql = 'UPDATE ' . table_name($config, 'delivery_outbox') . ' SET status=\'dead_letter\', attempt_count=attempt_count+1, '
            . 'last_error=\'delivery_lease_expired_outcome_unknown\', dead_lettered_at=UTC_TIMESTAMP(), lease_token=NULL, locked_by=NULL, locked_at=NULL, '
            . 'lease_expires_at=NULL, updated_at=UTC_TIMESTAMP() WHERE status=\'processing\' AND lease_expires_at < UTC_TIMESTAMP()';
        $expiredParams = array();
        if ($channel !== '') { $expiredSql .= ' AND delivery_channel=?'; $expiredParams[] = $channel; }
        if ($requestedDeliveryId !== '') { $expiredSql .= ' AND delivery_id=?'; $expiredParams[] = $requestedDeliveryId; }
        $expired = $pdo->prepare($expiredSql); $expired->execute($expiredParams);
        $outcomeUnknownCount = $expired->rowCount();
        $blockedCount += $outcomeUnknownCount;
        if ($requestedDeliveryId !== '') {
            $requested = $pdo->prepare('SELECT status, external_message_id FROM ' . table_name($config, 'delivery_outbox') . ' WHERE delivery_id=? FOR UPDATE');
            $requested->execute(array($requestedDeliveryId)); $requestedRow = $requested->fetch();
            if ($requestedRow) {
                $requestedStatus = (string)$requestedRow['status'];
                $requestedExternalId = isset($requestedRow['external_message_id']) ? (string)$requestedRow['external_message_id'] : null;
            } else {
                $requestedStatus = 'not_found';
            }
        }
        if ($requestedStatus !== 'delivered' && $requestedStatus !== 'not_found' && $requestedStatus !== 'dead_letter') {
            $sql = 'SELECT delivery_id, event_id, delivery_channel, destination, idempotency_key, payload_json, attempt_count, created_at '
                . 'FROM ' . table_name($config, 'delivery_outbox') . ' WHERE '
                . '(status IN (\'pending\',\'retry\',\'remote_queued\') AND (next_attempt_at IS NULL OR next_attempt_at <= UTC_TIMESTAMP())) ';
            $selectParams = array();
            if ($channel !== '') { $sql .= 'AND delivery_channel = ? '; $selectParams[] = $channel; }
            if ($requestedDeliveryId !== '') { $sql .= 'AND delivery_id = ? '; $selectParams[] = $requestedDeliveryId; }
            // Scan enough candidates to quarantine blocked rows without ever
            // leasing more than the one publishable row returned to the worker.
            $candidateLimit = 30;
            $sql .= 'ORDER BY created_at ASC, delivery_id ASC LIMIT ' . $candidateLimit . ' FOR UPDATE';
            $select = $pdo->prepare($sql); $select->execute($selectParams); $candidates = $select->fetchAll();
            $block = $pdo->prepare('UPDATE ' . table_name($config, 'delivery_outbox') . ' SET status=\'dead_letter\', attempt_count=attempt_count+1, '
                . 'last_error=\'source_right_inactive_or_missing\', dead_lettered_at=UTC_TIMESTAMP(), lease_token=NULL, locked_by=NULL, locked_at=NULL, '
                . 'lease_expires_at=NULL, updated_at=UTC_TIMESTAMP() WHERE delivery_id=?');
            $editorialBlock = $pdo->prepare('UPDATE ' . table_name($config, 'delivery_outbox') . ' SET status=\'dead_letter\', attempt_count=attempt_count+1, '
                . 'last_error=\'event_not_publishable_or_unapproved\', dead_lettered_at=UTC_TIMESTAMP(), lease_token=NULL, locked_by=NULL, locked_at=NULL, '
                . 'lease_expires_at=NULL, updated_at=UTC_TIMESTAMP() WHERE delivery_id=?');
            foreach ($candidates as $candidate) {
                $candidateEventId = isset($candidate['event_id']) ? trim((string)$candidate['event_id']) : '';
                if ($candidateEventId !== '' && !delivery_event_is_publishable($pdo, $config, $candidateEventId)) {
                    $editorialBlock->execute(array((string)$candidate['delivery_id']));
                    $blockedCount++;
                    $editorialBlockedCount++;
                    if ($requestedDeliveryId !== '') { $requestedStatus = 'dead_letter'; }
                    continue;
                }
                if (!delivery_source_rights_valid($pdo, $config, (string)$candidate['payload_json'])) {
                    $block->execute(array((string)$candidate['delivery_id']));
                    $blockedCount++;
                    $rightsBlockedCount++;
                    if ($requestedDeliveryId !== '') { $requestedStatus = 'dead_letter'; }
                    continue;
                }
                $rows[] = $candidate;
                if (count($rows) >= $limit) { break; }
            }
        }
        if ($rows) {
            $ids = array_map(function ($row) { return (string)$row['delivery_id']; }, $rows);
            $placeholders = implode(',', array_fill(0, count($ids), '?'));
            $update = $pdo->prepare('UPDATE ' . table_name($config, 'delivery_outbox') . ' SET status=\'processing\', lease_token=?, locked_by=?, '
                . 'locked_at=UTC_TIMESTAMP(), lease_expires_at=DATE_ADD(UTC_TIMESTAMP(), INTERVAL ' . $leaseSeconds . ' SECOND), updated_at=UTC_TIMESTAMP() '
                . 'WHERE delivery_id IN (' . $placeholders . ')');
            $update->execute(array_merge(array($leaseToken, $worker), $ids));
            if ($requestedDeliveryId !== '') { $requestedStatus = 'processing'; }
        }
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    foreach ($rows as &$row) {
        $decoded = json_decode((string)$row['payload_json'], true);
        $row['payload'] = is_array($decoded) ? $decoded : array();
        unset($row['payload_json']);
        $row['outbox_id'] = (string)$row['delivery_id'];
        $row['lease_token'] = $leaseToken;
        $row['attempt_count'] = (int)$row['attempt_count'];
    }
    unset($row);
    $deadParams = array();
    $deadSql = 'SELECT COUNT(*) FROM ' . table_name($config, 'delivery_outbox') . ' WHERE status=\'dead_letter\'';
    if ($channel !== '') { $deadSql .= ' AND delivery_channel=?'; $deadParams[] = $channel; }
    $deadLetterCount = scalar_int($pdo, $deadSql, $deadParams);
    respond(200, array(
        'ok' => true,
        'lease_token' => $rows ? $leaseToken : null,
        'lease_seconds' => $leaseSeconds,
        'max_claim_items' => 1,
        'items' => $rows,
        'deliveries' => $rows,
        'dead_letter_count' => $deadLetterCount,
        'blocked_count' => $blockedCount,
        'rights_blocked_count' => $rightsBlockedCount,
        'editorial_blocked_count' => $editorialBlockedCount,
        'outcome_unknown_count' => $outcomeUnknownCount,
        'requested_status' => $requestedDeliveryId !== '' ? $requestedStatus : null,
        'external_message_id' => $requestedDeliveryId !== '' && $requestedStatus === 'delivered' ? $requestedExternalId : null,
    ));
}

function ack_delivery_outbox(PDO $pdo, array $config, array $payload): void {
    $deliveryId = trim((string)v1_first($payload, array('delivery_id', 'outbox_id'), ''));
    $leaseToken = trim((string)v1_first($payload, array('lease_token'), ''));
    $externalId = trim((string)v1_first($payload, array('external_message_id'), ''));
    if (!v1_valid_entity_id($deliveryId) || !v1_valid_entity_id($leaseToken, 64) || $externalId === '') {
        respond(400, array('ok' => false, 'error' => 'delivery_id_lease_and_external_message_id_required'));
    }
    $externalId = mb_substr($externalId, 0, 191, 'UTF-8');
    $pdo->beginTransaction();
    try {
        $select = $pdo->prepare('SELECT status, lease_token, external_message_id FROM ' . table_name($config, 'delivery_outbox') . ' WHERE delivery_id = ? FOR UPDATE');
        $select->execute(array($deliveryId)); $row = $select->fetch();
        if (!$row) { $pdo->rollBack(); respond(404, array('ok' => false, 'error' => 'delivery_not_found')); }
        if ((string)$row['status'] === 'delivered') {
            $matches = hash_equals((string)$row['external_message_id'], $externalId);
            $pdo->commit();
            if (!$matches) { respond(409, array('ok' => false, 'error' => 'delivery_already_acked_with_different_external_id')); }
            respond(200, array('ok' => true, 'delivery_id' => $deliveryId, 'status' => 'delivered', 'idempotent' => true));
        }
        if ((string)$row['status'] !== 'processing' || !hash_equals((string)$row['lease_token'], $leaseToken)) {
            $pdo->rollBack(); respond(409, array('ok' => false, 'error' => 'invalid_or_expired_lease'));
        }
        $update = $pdo->prepare('UPDATE ' . table_name($config, 'delivery_outbox') . ' SET status=\'delivered\', external_message_id=?, delivered_at=UTC_TIMESTAMP(), '
            . 'lease_token=NULL, locked_by=NULL, locked_at=NULL, lease_expires_at=NULL, last_error=NULL, updated_at=UTC_TIMESTAMP() WHERE delivery_id=?');
        $update->execute(array($externalId, $deliveryId));
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    respond(200, array('ok' => true, 'delivery_id' => $deliveryId, 'status' => 'delivered', 'external_message_id' => $externalId));
}

function fail_delivery_outbox(PDO $pdo, array $config, array $payload): void {
    $deliveryId = trim((string)v1_first($payload, array('delivery_id', 'outbox_id'), ''));
    $leaseToken = trim((string)v1_first($payload, array('lease_token'), ''));
    $error = trim((string)v1_first($payload, array('error'), 'delivery_failed'));
    $maxAttempts = max(1, min(20, (int)v1_first($payload, array('max_attempts'), 5)));
    $retryAfter = max(30, min(86400, (int)v1_first($payload, array('retry_after_seconds'), 300)));
    $retryable = !array_key_exists('retryable', $payload) || !empty($payload['retryable']);
    $externalId = mb_substr(trim((string)v1_first($payload, array('external_message_id'), '')), 0, 191, 'UTF-8');
    if (!v1_valid_entity_id($deliveryId) || !v1_valid_entity_id($leaseToken, 64)) {
        respond(400, array('ok' => false, 'error' => 'delivery_id_and_lease_required'));
    }
    $pdo->beginTransaction();
    try {
        $select = $pdo->prepare('SELECT status, lease_token, attempt_count FROM ' . table_name($config, 'delivery_outbox') . ' WHERE delivery_id = ? FOR UPDATE');
        $select->execute(array($deliveryId)); $row = $select->fetch();
        if (!$row) { $pdo->rollBack(); respond(404, array('ok' => false, 'error' => 'delivery_not_found')); }
        if ((string)$row['status'] !== 'processing' || !hash_equals((string)$row['lease_token'], $leaseToken)) {
            $pdo->rollBack(); respond(409, array('ok' => false, 'error' => 'invalid_or_expired_lease'));
        }
        $attempts = (int)$row['attempt_count'] + 1;
        $status = (!$retryable || $attempts >= $maxAttempts) ? 'dead_letter' : 'retry';
        $nextAttempt = $status === 'retry' ? gmdate('Y-m-d H:i:s', time() + $retryAfter) : null;
        $deadAt = $status === 'dead_letter' ? gmdate('Y-m-d H:i:s') : null;
        $update = $pdo->prepare('UPDATE ' . table_name($config, 'delivery_outbox') . ' SET status=?, attempt_count=?, next_attempt_at=?, '
            . 'lease_token=NULL, locked_by=NULL, locked_at=NULL, lease_expires_at=NULL, last_error=?, dead_lettered_at=?, '
            . 'external_message_id=COALESCE(?,external_message_id), updated_at=UTC_TIMESTAMP() WHERE delivery_id=?');
        $update->execute(array($status, $attempts, $nextAttempt, mb_substr($error, 0, 65535, 'UTF-8'), $deadAt, $externalId ?: null, $deliveryId));
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    respond(200, array('ok' => true, 'delivery_id' => $deliveryId, 'status' => $status, 'attempt_count' => $attempts, 'next_attempt_at' => $nextAttempt));
}

function v1_runtime_resource(array $config, string $resource): ?array {
    $resources = array(
        'runs' => array('run_id', 'SELECT run_id, started_at, finished_at, mode, fetched, accepted, duplicates, rejected, published_now, pending, published_total, payload_json, updated_at FROM ' . table_name($config, 'runs'), 'updated_at'),
        'articles' => array('record_id', 'SELECT record_id, canonical_url_hash, title_hash, canonical_url, title, normalized_title, summary, source, feed_name, feed_category, image_url, published_at, seen_at, status, reason, relevance_level, priority_score, priority_level, story_key, source_right_id, sort_at, updated_at FROM ' . table_name($config, 'articles'), 'updated_at'),
        'stories' => array('story_key', 'SELECT story_key, guid, representative_title, representative_url, relevance_level, theme_group, status, article_count, priority_score, source_right_id, published_at, last_article_seen_at, payload_json, sort_at, updated_at FROM ' . table_name($config, 'stories'), 'updated_at'),
        'reports' => array('date_id', 'SELECT date_id, title, start_at, end_at, public_url, story_count, article_count, payload_json, updated_at FROM ' . table_name($config, 'reports'), 'updated_at'),
        'telegram_channels' => array('handle', 'SELECT handle, telegram_channel_id, title, description, joined, enabled, source, source_type, is_public_channel, quality_score, last_message_id, last_collected_at, last_error, payload_json, updated_at FROM ' . table_name($config, 'telegram_channels'), 'updated_at'),
        'telegram_messages' => array('message_key', 'SELECT message_key, channel_handle, telegram_channel_id, telegram_message_id, posted_at, edited_at, deleted_at, collected_at, text, normalized_text, views, forwards, replies_count, message_url, urls_json, risk_flags_json, updated_at FROM ' . table_name($config, 'telegram_messages'), 'updated_at'),
        'telegram_article_matches' => array('runtime_cursor', 'SELECT * FROM (SELECT CONCAT(article_id, CHAR(31), message_key, CHAR(31), match_type) AS runtime_cursor, article_id, message_key, match_type, score, reason, channel_handle, telegram_message_id, message_url, updated_at FROM ' . table_name($config, 'telegram_article_matches') . ') runtime_data', 'updated_at'),
        'telegram_signal_messages' => array('message_key', 'SELECT message_key, channel_handle, telegram_channel_id, telegram_message_id, posted_at, edited_at, deleted_at, collected_at, text, normalized_text, views, forwards, replies_count, message_url, urls_json, risk_flags_json, updated_at FROM ' . table_name($config, 'telegram_messages'), 'posted_at'),
        'telegram_signal_matches' => array('runtime_cursor', 'SELECT * FROM (SELECT CONCAT(runtime_match.article_id, CHAR(31), runtime_match.message_key, CHAR(31), runtime_match.match_type) AS runtime_cursor, runtime_match.article_id, runtime_match.message_key, runtime_match.match_type, runtime_match.score, runtime_match.reason, runtime_match.channel_handle, runtime_match.telegram_message_id, runtime_match.message_url, runtime_message.posted_at, runtime_match.updated_at FROM ' . table_name($config, 'telegram_article_matches') . ' runtime_match JOIN ' . table_name($config, 'telegram_messages') . ' runtime_message ON runtime_message.message_key=runtime_match.message_key) runtime_data', 'posted_at'),
        'telegram_issue_signals' => array('article_id', 'SELECT article_id, related_telegram_count, related_telegram_channels_count, first_seen_at, latest_seen_at, confidence_score, payload_json, updated_at FROM ' . table_name($config, 'telegram_issue_signals'), 'updated_at'),
        'delivery_outbox' => array('delivery_id', 'SELECT delivery_id, event_id, delivery_channel, destination, idempotency_key, payload_json, status, attempt_count, next_attempt_at, lease_token, locked_by, locked_at, lease_expires_at, external_message_id, last_error, delivered_at, dead_lettered_at, created_at, updated_at FROM ' . table_name($config, 'delivery_outbox'), 'updated_at'),
        'companies' => array('company_id', 'SELECT company_id, stock_code, market, legal_name, legal_name_en, short_name, aliases_json, homepage_url, record_status, created_at, updated_at FROM ' . table_name($config, 'companies'), 'updated_at'),
        'source_rights' => array('source_right_id', 'SELECT source_right_id, source_type, source_key, source_name, permission_scope, evidence_uri, evidence_hash, valid_from, valid_until, revoked_at, ai_allowed, redistribution_allowed, status, notes, created_at, updated_at FROM ' . table_name($config, 'source_rights'), 'updated_at'),
        'collection_runs' => array('run_id', 'SELECT run_id, pipeline, source_key, status, started_at, finished_at, fetched_count, resolved_count, accepted_count, error_count, lag_seconds_p95, metrics_json, created_at, updated_at FROM ' . table_name($config, 'collection_runs'), 'updated_at'),
        'governance_events' => array('event_id', 'SELECT event_id, company_id, event_type, title, original_language, summary, occurred_at, deadline_at, importance, verification_status, review_status, publication_status, collection_key, updated_at FROM ' . table_name($config, 'governance_events'), 'updated_at'),
        'documents' => array('document_id', 'SELECT document_id, company_id, source_right_id, source_class, external_id, document_type, original_language, title, SHA2(body_text,256) AS body_sha256, original_url, content_hash, collection_key, correction_of_document_id, version_no, published_at, retrieved_at, verification_status, publication_status, updated_at FROM ' . table_name($config, 'documents'), 'updated_at'),
        'link_discoveries' => array('discovery_id', 'SELECT discovery_id, discovered_url, resolved_url, source, title, summary, feed_name, feed_category, source_kind, source_right_id, lineage_version, published_at, status, attempt_count, discovered_at, resolved_at, expired_at, last_error, updated_at FROM ' . table_name($config, 'link_discoveries'), 'updated_at'),
    );
    return isset($resources[$resource]) ? $resources[$resource] : null;
}

function v1_runtime_cursor_encode(string $updatedAt, string $primary): string {
    return rtrim(strtr(base64_encode($updatedAt . chr(31) . $primary), '+/', '-_'), '=');
}

function v1_runtime_cursor_decode(string $cursor): ?array {
    if ($cursor === '' || preg_match('/^[A-Za-z0-9_-]{1,512}$/', $cursor) !== 1) { return null; }
    $padding = strlen($cursor) % 4;
    if ($padding !== 0) { $cursor .= str_repeat('=', 4 - $padding); }
    $decoded = base64_decode(strtr($cursor, '-_', '+/'), true);
    if ($decoded === false) { return null; }
    $parts = explode(chr(31), $decoded, 2);
    if (count($parts) !== 2 || mysql_dt($parts[0]) === null || $parts[1] === '' || strlen($parts[1]) > 512) { return null; }
    return array(mysql_dt($parts[0]), $parts[1]);
}

function runtime_state_page(PDO $pdo, array $config, array $input): array {
    $resource = trim((string)v1_first($input, array('resource'), 'articles'));
    $definition = v1_runtime_resource($config, $resource);
    if ($definition === null) { return array('error' => 'invalid_runtime_resource'); }
    $primary = $definition[0];
    $baseSql = $definition[1];
    $timeColumn = isset($definition[2]) ? $definition[2] : null;
    $limit = max(1, min(100, (int)v1_first($input, array('limit'), 50)));
    $order = trim((string)v1_first($input, array('order'), 'primary_asc'));
    if (!in_array($order, array('primary_asc', 'updated_desc'), true)) { return array('error' => 'invalid_order'); }
    if ($order === 'updated_desc' && $timeColumn === null) { return array('error' => 'order_not_supported'); }
    $after = trim((string)v1_first($input, array('after', 'cursor'), ''));
    if (strlen($after) > 512) { return array('error' => 'invalid_cursor'); }
    $sinceRaw = trim((string)v1_first($input, array('since'), ''));
    $since = $sinceRaw !== '' ? mysql_dt($sinceRaw) : null;
    if ($sinceRaw !== '' && $since === null) { return array('error' => 'invalid_since'); }
    $params = array();
    $sql = $baseSql;
    $where = array();
    if ($timeColumn !== null && $since !== null) { $where[] = '`' . $timeColumn . '` >= ?'; $params[] = $since; }
    if ($after !== '') {
        if ($order === 'updated_desc') {
            $decodedCursor = v1_runtime_cursor_decode($after);
            if ($decodedCursor === null) { return array('error' => 'invalid_cursor'); }
            $where[] = '(`' . $timeColumn . '` < ? OR (`' . $timeColumn . '` = ? AND `' . $primary . '` < ?))';
            array_push($params, $decodedCursor[0], $decodedCursor[0], $decodedCursor[1]);
        } else {
            $where[] = '`' . $primary . '` > ?'; $params[] = $after;
        }
    }
    if ($where) { $sql .= ' WHERE ' . implode(' AND ', $where); }
    $sql .= $order === 'updated_desc'
        ? ' ORDER BY `' . $timeColumn . '` DESC, `' . $primary . '` DESC LIMIT ' . ($limit + 1)
        : ' ORDER BY `' . $primary . '` ASC LIMIT ' . ($limit + 1);
    $stmt = $pdo->prepare($sql); $stmt->execute($params); $fetched = $stmt->fetchAll();
    $hasMore = count($fetched) > $limit;
    if ($hasMore) { $fetched = array_slice($fetched, 0, $limit); }
    $eventLineage = array();
    if ($resource === 'governance_events' && $fetched) {
        $eventIds = array();
        foreach ($fetched as $row) {
            $eventId = isset($row['event_id']) ? (string)$row['event_id'] : '';
            if (!v1_valid_entity_id($eventId)) { return array('error' => 'invalid_governance_event_lineage'); }
            $eventIds[] = $eventId;
            $eventLineage[$eventId] = array('rights' => array(), 'documents' => array(), 'publishable' => array());
        }
        $placeholders = implode(',', array_fill(0, count($eventIds), '?'));
        $lineageSql = 'SELECT DISTINCT runtime_ed.event_id, runtime_d.document_id, runtime_d.source_right_id, runtime_d.content_hash, runtime_d.version_no, '
            . 'CASE WHEN ' . v1_document_visibility_sql('runtime_d', 'runtime_sr') . ' THEN 1 ELSE 0 END AS is_publishable '
            . 'FROM ' . table_name($config, 'event_documents') . ' runtime_ed '
            . 'JOIN ' . table_name($config, 'documents') . ' runtime_d ON runtime_d.document_id = runtime_ed.document_id '
            . 'LEFT JOIN ' . table_name($config, 'source_rights') . ' runtime_sr ON runtime_sr.source_right_id = runtime_d.source_right_id '
            . 'WHERE runtime_ed.event_id IN (' . $placeholders . ') ORDER BY runtime_ed.event_id, runtime_d.document_id';
        $lineageStmt = $pdo->prepare($lineageSql); $lineageStmt->execute($eventIds);
        foreach ($lineageStmt->fetchAll() as $lineageRow) {
            $eventId = (string)$lineageRow['event_id'];
            $documentId = (string)$lineageRow['document_id'];
            if (!isset($eventLineage[$eventId]) || !v1_valid_entity_id($documentId)) {
                return array('error' => 'invalid_governance_event_lineage');
            }
            $sourceRightId = isset($lineageRow['source_right_id']) ? trim((string)$lineageRow['source_right_id']) : '';
            if ($sourceRightId !== '') {
                if (!v1_valid_entity_id($sourceRightId, 64)) { return array('error' => 'invalid_governance_event_source_right_lineage'); }
                $eventLineage[$eventId]['rights'][$sourceRightId] = true;
            }
            $eventLineage[$eventId]['documents'][$documentId] = $documentId . chr(31)
                . strtolower(trim((string)$lineageRow['content_hash'])) . chr(31) . (string)((int)$lineageRow['version_no']);
            if ((int)$lineageRow['is_publishable'] === 1) { $eventLineage[$eventId]['publishable'][$documentId] = true; }
        }
    }
    $records = array();
    foreach ($fetched as $row) {
        if ($resource === 'governance_events') {
            $lineage = $eventLineage[(string)$row['event_id']];
            ksort($lineage['rights'], SORT_STRING);
            ksort($lineage['documents'], SORT_STRING);
            $row['source_right_ids'] = array_keys($lineage['rights']);
            $row['evidence_revision'] = hash('sha256', implode(chr(30), array_values($lineage['documents'])));
            $row['publishable_evidence_count'] = count($lineage['publishable']);
        }
        $candidate = $records; $candidate[] = $row;
        $probe = json_encode(array('records' => $candidate), JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
        if ($probe !== false && strlen($probe) > 230000) { $hasMore = true; break; }
        $records[] = $row;
    }
    if (!$records && $fetched) { return array('error' => 'runtime_record_exceeds_response_budget'); }
    $next = null;
    if ($records && $hasMore) {
        $last = $records[count($records) - 1];
        $next = $order === 'updated_desc'
            ? v1_runtime_cursor_encode((string)$last[$timeColumn], (string)$last[$primary])
            : (string)$last[$primary];
    }
    return array(
        'resource' => $resource,
        'order' => $order,
        'records' => $records,
        'next_cursor' => $next,
        'has_more' => $hasMore,
        'max_records' => 100,
    );
}

function export_runtime_state(PDO $pdo, array $config, array $payload): void {
    $result = runtime_state_page($pdo, $config, $payload);
    if (isset($result['error'])) { respond(400, array('ok' => false, 'error' => $result['error'])); }
    respond(200, array('ok' => true, 'state' => $result));
}

function v1_runtime_state_route(PDO $pdo, array $config): void {
    $result = runtime_state_page($pdo, $config, $_GET);
    if (isset($result['error'])) { v1_respond(400, array('ok' => false, 'error' => $result['error'])); }
    v1_respond(200, array('ok' => true, 'data' => $result));
}

function enqueue_link_discoveries(PDO $pdo, array $config, array $payload): void {
    $discoveries = isset($payload['discoveries']) && is_array($payload['discoveries']) ? $payload['discoveries'] : array();
    if (count($discoveries) > 2000) { respond(413, array('ok' => false, 'error' => 'too_many_discoveries')); }
    $now = gmdate('Y-m-d H:i:s');
    $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'link_discoveries') . ' (discovery_id, discovered_url, discovered_url_hash, source, title, '
        . 'summary, feed_name, feed_category, source_kind, source_right_id, lineage_version, published_at, status, attempt_count, discovered_at, created_at, updated_at) '
        . 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,\'discovered\',0,?,?,?) ON DUPLICATE KEY UPDATE '
        . 'source=IF(VALUES(lineage_version)>=lineage_version,COALESCE(VALUES(source),source),source), '
        . 'title=IF(VALUES(lineage_version)>=lineage_version,COALESCE(VALUES(title),title),title), '
        . 'summary=IF(VALUES(lineage_version)>=lineage_version,COALESCE(VALUES(summary),summary),summary), '
        . 'feed_name=IF(VALUES(lineage_version)>=lineage_version,COALESCE(VALUES(feed_name),feed_name),feed_name), '
        . 'feed_category=IF(VALUES(lineage_version)>=lineage_version,COALESCE(VALUES(feed_category),feed_category),feed_category), '
        . 'source_kind=CASE WHEN source_right_id IS NOT NULL THEN source_kind '
        . 'WHEN VALUES(source_right_id) IS NOT NULL THEN COALESCE(VALUES(source_kind),source_kind) '
        . 'WHEN VALUES(lineage_version)>=lineage_version THEN COALESCE(VALUES(source_kind),source_kind) ELSE source_kind END, '
        . 'source_right_id=CASE WHEN source_right_id IS NOT NULL THEN source_right_id ELSE VALUES(source_right_id) END, '
        . 'published_at=IF(VALUES(lineage_version)>=lineage_version,COALESCE(VALUES(published_at),published_at),published_at), '
        . 'lineage_version=GREATEST(lineage_version,VALUES(lineage_version)), updated_at=VALUES(updated_at)');
    $accepted = 0; $rejected = 0;
    foreach ($discoveries as $discovery) {
        if (!is_array($discovery)) { $rejected++; continue; }
        $url = trim((string)v1_first($discovery, array('discovered_url', 'url'), ''));
        if (strlen($url) > 65535 || !filter_var($url, FILTER_VALIDATE_URL) || !preg_match('#^https?://#i', $url)) { $rejected++; continue; }
        $hash = hash('sha256', $url);
        $id = trim((string)v1_first($discovery, array('discovery_id'), ''));
        if ($id === '') { $id = 'link:' . substr($hash, 0, 40); }
        if (!v1_valid_entity_id($id)) { $rejected++; continue; }
        $discoveredAt = mysql_dt(v1_first($discovery, array('discovered_at'), $now)) ?: $now;
        $publishedAtRaw = trim((string)v1_first($discovery, array('published_at'), ''));
        $publishedAt = $publishedAtRaw !== '' ? mysql_dt($publishedAtRaw) : null;
        if ($publishedAtRaw !== '' && $publishedAt === null) { $rejected++; continue; }
        $sourceRightId = trim((string)v1_first($discovery, array('source_right_id'), ''));
        if ($sourceRightId !== '' && !v1_valid_entity_id($sourceRightId, 64)) { $rejected++; continue; }
        $lineageVersion = max(0, min(1, (int)v1_first($discovery, array('lineage_version'), 0)));
        $stmt->execute(array(
            $id, $url, $hash,
            mb_substr((string)v1_first($discovery, array('source'), ''), 0, 191, 'UTF-8') ?: null,
            mb_substr((string)v1_first($discovery, array('title'), ''), 0, 700, 'UTF-8') ?: null,
            mb_substr((string)v1_first($discovery, array('summary'), ''), 0, 4000, 'UTF-8') ?: null,
            mb_substr((string)v1_first($discovery, array('feed_name'), ''), 0, 191, 'UTF-8') ?: null,
            mb_substr((string)v1_first($discovery, array('feed_category'), ''), 0, 64, 'UTF-8') ?: null,
            mb_substr((string)v1_first($discovery, array('source_kind'), ''), 0, 40, 'UTF-8') ?: null,
            $sourceRightId !== '' ? $sourceRightId : null,
            $lineageVersion,
            $publishedAt,
            $discoveredAt, $now, $now,
        ));
        $accepted++;
    }
    respond(200, array('ok' => true, 'accepted' => $accepted, 'rejected' => $rejected));
}

function claim_link_discoveries(PDO $pdo, array $config, array $payload): void {
    $worker = trim((string)v1_first($payload, array('worker_id'), 'link-resolver'));
    if (!v1_valid_entity_id($worker, 96)) { respond(400, array('ok' => false, 'error' => 'invalid_worker_id')); }
    $limit = max(1, min(100, (int)v1_first($payload, array('limit'), 25)));
    $leaseSeconds = max(30, min(900, (int)v1_first($payload, array('lease_seconds'), 180)));
    $leaseToken = 'lease_' . bin2hex(random_bytes(16));
    $pdo->beginTransaction();
    try {
        $sql = 'SELECT discovery_id, discovered_url, source, title, attempt_count, discovered_at FROM ' . table_name($config, 'link_discoveries') . ' WHERE '
            . '((status = \'discovered\' AND (next_attempt_at IS NULL OR next_attempt_at <= UTC_TIMESTAMP())) '
            . 'OR (status = \'resolving\' AND lease_expires_at < UTC_TIMESTAMP())) '
            . 'ORDER BY lineage_version DESC, discovered_at DESC, discovery_id ASC LIMIT ' . $limit . ' FOR UPDATE';
        $rows = $pdo->query($sql)->fetchAll();
        if ($rows) {
            $ids = array_map(function ($row) { return (string)$row['discovery_id']; }, $rows);
            $placeholders = implode(',', array_fill(0, count($ids), '?'));
            $update = $pdo->prepare('UPDATE ' . table_name($config, 'link_discoveries') . ' SET status=\'resolving\', attempt_count=attempt_count+1, '
                . 'lease_token=?, locked_by=?, locked_at=UTC_TIMESTAMP(), lease_expires_at=DATE_ADD(UTC_TIMESTAMP(), INTERVAL ' . $leaseSeconds . ' SECOND), '
                . 'updated_at=UTC_TIMESTAMP() WHERE discovery_id IN (' . $placeholders . ')');
            $update->execute(array_merge(array($leaseToken, $worker), $ids));
            foreach ($rows as &$row) { $row['attempt_count'] = (int)$row['attempt_count'] + 1; }
            unset($row);
        }
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    respond(200, array('ok' => true, 'lease_token' => $rows ? $leaseToken : null, 'lease_seconds' => $leaseSeconds, 'discoveries' => $rows));
}

function resolve_link_discovery(PDO $pdo, array $config, array $payload): void {
    $id = trim((string)v1_first($payload, array('discovery_id'), ''));
    $leaseToken = trim((string)v1_first($payload, array('lease_token'), ''));
    $outcome = trim((string)v1_first($payload, array('outcome', 'status'), ''));
    if (!v1_valid_entity_id($id) || !v1_valid_entity_id($leaseToken, 64) || !in_array($outcome, array('resolved', 'retry', 'discovered', 'expired'), true)) {
        respond(400, array('ok' => false, 'error' => 'invalid_resolution'));
    }
    $resolvedUrl = trim((string)v1_first($payload, array('resolved_url'), ''));
    if ($outcome === 'resolved' && (!filter_var($resolvedUrl, FILTER_VALIDATE_URL) || !preg_match('#^https?://#i', $resolvedUrl))) {
        respond(400, array('ok' => false, 'error' => 'valid_resolved_url_required'));
    }
    $maxAttempts = max(1, min(20, (int)v1_first($payload, array('max_attempts'), 5)));
    $retryAfter = max(60, min(604800, (int)v1_first($payload, array('retry_after_seconds'), 3600)));
    $error = mb_substr(trim((string)v1_first($payload, array('error'), '')), 0, 65535, 'UTF-8');
    $pdo->beginTransaction();
    try {
        $select = $pdo->prepare('SELECT status, lease_token, attempt_count, resolved_url FROM ' . table_name($config, 'link_discoveries') . ' WHERE discovery_id = ? FOR UPDATE');
        $select->execute(array($id)); $row = $select->fetch();
        if (!$row) { $pdo->rollBack(); respond(404, array('ok' => false, 'error' => 'discovery_not_found')); }
        if ((string)$row['status'] === 'resolved' && $outcome === 'resolved' && (string)$row['resolved_url'] === $resolvedUrl) {
            $pdo->commit(); respond(200, array('ok' => true, 'discovery_id' => $id, 'status' => 'resolved', 'idempotent' => true));
        }
        if ((string)$row['status'] !== 'resolving' || !hash_equals((string)$row['lease_token'], $leaseToken)) {
            $pdo->rollBack(); respond(409, array('ok' => false, 'error' => 'invalid_or_expired_lease'));
        }
        $attempts = (int)$row['attempt_count'];
        if (($outcome === 'retry' || $outcome === 'discovered') && $attempts >= $maxAttempts) { $outcome = 'expired'; }
        $status = ($outcome === 'retry') ? 'discovered' : $outcome;
        $nextAttempt = $status === 'discovered' ? gmdate('Y-m-d H:i:s', time() + $retryAfter) : null;
        $resolvedAt = $status === 'resolved' ? gmdate('Y-m-d H:i:s') : null;
        $expiredAt = $status === 'expired' ? gmdate('Y-m-d H:i:s') : null;
        $update = $pdo->prepare('UPDATE ' . table_name($config, 'link_discoveries') . ' SET status=?, resolved_url=?, next_attempt_at=?, '
            . 'lease_token=NULL, locked_by=NULL, locked_at=NULL, lease_expires_at=NULL, last_error=?, resolved_at=?, expired_at=?, updated_at=UTC_TIMESTAMP() '
            . 'WHERE discovery_id=?');
        $update->execute(array($status, $status === 'resolved' ? $resolvedUrl : null, $nextAttempt, $error ?: null, $resolvedAt, $expiredAt, $id));
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    respond(200, array('ok' => true, 'discovery_id' => $id, 'status' => $status, 'attempt_count' => $attempts, 'next_attempt_at' => $nextAttempt));
}

/**
 * Collapse handle-only and renamed Telegram rows into id:{channel_id}:{message}
 * before the normal snapshot upsert. Temporary tables avoid primary/unique-key
 * collisions while preserving the newest message and strongest match.
 */
function migrate_telegram_channel_identity(PDO $pdo, array $config, string $handle, string $channelId): void {
    if ($handle === '' || $channelId === '') { return; }
    $channelsTable = table_name($config, 'telegram_channels');
    $messagesTable = table_name($config, 'telegram_messages');
    $matchesTable = table_name($config, 'telegram_article_matches');

    // A Telegram handle can be reassigned. Preserve a row that already has a
    // different authoritative channel ID under an internal stable handle; it
    // must never be folded into the newly observed channel merely by username.
    $conflictStmt = $pdo->prepare('SELECT * FROM ' . $channelsTable . ' WHERE handle = ? AND telegram_channel_id IS NOT NULL '
        . 'AND telegram_channel_id <> \'\' AND telegram_channel_id <> ? LIMIT 1');
    $conflictStmt->execute(array($handle, $channelId));
    $conflict = $conflictStmt->fetch();
    if ($conflict) {
        $previousId = (string)$conflict['telegram_channel_id'];
        $stableHandle = 'channel_' . substr(hash('sha256', $previousId), 0, 24);
        migrate_telegram_channel_identity($pdo, $config, $stableHandle, $previousId);
        $preserve = $pdo->prepare('INSERT INTO ' . $channelsTable . ' (handle, telegram_channel_id, title, description, joined, enabled, source, source_type, '
            . 'is_public_channel, quality_score, last_message_id, last_collected_at, last_recommendation_checked_at, last_error, payload_json, '
            . 'identity_migration_version, updated_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?) ON DUPLICATE KEY UPDATE telegram_channel_id=VALUES(telegram_channel_id), title=VALUES(title), '
            . 'description=VALUES(description), joined=VALUES(joined), enabled=VALUES(enabled), source=VALUES(source), source_type=VALUES(source_type), '
            . 'is_public_channel=VALUES(is_public_channel), quality_score=VALUES(quality_score), last_message_id=GREATEST(last_message_id,VALUES(last_message_id)), '
            . 'last_collected_at=COALESCE(VALUES(last_collected_at),last_collected_at), last_recommendation_checked_at=COALESCE(VALUES(last_recommendation_checked_at),last_recommendation_checked_at), '
            . 'last_error=VALUES(last_error), payload_json=VALUES(payload_json), identity_migration_version=1, updated_at=VALUES(updated_at)');
        $preserve->execute(array(
            $stableHandle, $previousId, $conflict['title'], $conflict['description'], $conflict['joined'], $conflict['enabled'],
            $conflict['source'], $conflict['source_type'], $conflict['is_public_channel'], $conflict['quality_score'], $conflict['last_message_id'],
            $conflict['last_collected_at'], $conflict['last_recommendation_checked_at'], $conflict['last_error'], $conflict['payload_json'],
            gmdate('Y-m-d H:i:s'),
        ));
    }

    $aliasStmt = $pdo->prepare('SELECT handle FROM ' . $channelsTable . ' WHERE telegram_channel_id = ? '
        . 'OR (handle = ? AND (telegram_channel_id IS NULL OR telegram_channel_id = \'\'))');
    $aliasStmt->execute(array($channelId, $handle));
    $aliases = array($handle);
    foreach ($aliasStmt->fetchAll() as $row) { $aliases[] = (string)$row['handle']; }
    $aliases = array_values(array_unique(array_filter($aliases)));
    $stableMappingStmt = $pdo->prepare('SELECT identity_migration_version FROM ' . $channelsTable
        . ' WHERE handle = ? AND telegram_channel_id = ? LIMIT 1');
    $stableMappingStmt->execute(array($handle, $channelId));
    $identityMigrationVersion = $stableMappingStmt->fetchColumn();
    $hasStableMapping = $identityMigrationVersion !== false;
    // This mapping is written only after migration and the normal snapshot
    // upsert commit in this API. Once no alias remains, rescanning an entire
    // channel history on every incremental metadata refresh is redundant and
    // grows linearly with the 365-day archive.
    if ($hasStableMapping && (int)$identityMigrationVersion >= 1 && count($aliases) === 1) { return; }
    $needsChannelMerge = count($aliases) > 1;
    $messageCheck = $pdo->prepare('SELECT COUNT(*) FROM ' . $messagesTable . ' WHERE (telegram_channel_id = ? OR channel_handle = ?) '
        . 'AND (telegram_channel_id IS NULL OR telegram_channel_id <> ? OR channel_handle <> ? OR message_key NOT LIKE ?)');
    $messageCheck->execute(array($channelId, $handle, $channelId, $handle, 'id:' . $channelId . ':%'));
    $needsMessageMerge = (int)$messageCheck->fetchColumn() > 0;
    if (!$needsChannelMerge && !$needsMessageMerge) { return; }

    $aliasMarks = implode(',', array_fill(0, count($aliases), '?'));
    $pdo->exec('DROP TEMPORARY TABLE IF EXISTS tmp_bside_canonical_messages');
    $pdo->exec('DROP TEMPORARY TABLE IF EXISTS tmp_bside_canonical_matches');
    $pdo->exec('CREATE TEMPORARY TABLE tmp_bside_canonical_messages LIKE ' . $messagesTable);
    $pdo->exec('CREATE TEMPORARY TABLE tmp_bside_canonical_matches LIKE ' . $matchesTable);

    $messageSql = 'INSERT INTO tmp_bside_canonical_messages (message_key, channel_handle, telegram_channel_id, telegram_message_id, posted_at, edited_at, deleted_at, '
        . 'collected_at, text, normalized_text, views, forwards, replies_count, message_url, urls_json, risk_flags_json, raw_json, updated_at) '
        . 'SELECT CONCAT(\'id:\', ?, \':\', m.telegram_message_id), ?, ?, m.telegram_message_id, m.posted_at, m.edited_at, m.deleted_at, '
        . 'm.collected_at, m.text, m.normalized_text, m.views, m.forwards, m.replies_count, m.message_url, m.urls_json, m.risk_flags_json, m.raw_json, m.updated_at '
        . 'FROM ' . $messagesTable . ' m WHERE m.telegram_channel_id = ? OR m.channel_handle IN (' . $aliasMarks . ') ORDER BY m.updated_at ASC '
        . 'ON DUPLICATE KEY UPDATE posted_at=COALESCE(VALUES(posted_at),tmp_bside_canonical_messages.posted_at), '
        . 'edited_at=COALESCE(VALUES(edited_at),tmp_bside_canonical_messages.edited_at), deleted_at=VALUES(deleted_at), '
        . 'collected_at=COALESCE(VALUES(collected_at),tmp_bside_canonical_messages.collected_at), '
        . 'text=COALESCE(VALUES(text),tmp_bside_canonical_messages.text), '
        . 'normalized_text=COALESCE(VALUES(normalized_text),tmp_bside_canonical_messages.normalized_text), '
        . 'views=GREATEST(tmp_bside_canonical_messages.views,VALUES(views)), '
        . 'forwards=GREATEST(tmp_bside_canonical_messages.forwards,VALUES(forwards)), '
        . 'replies_count=GREATEST(tmp_bside_canonical_messages.replies_count,VALUES(replies_count)), '
        . 'message_url=COALESCE(VALUES(message_url),tmp_bside_canonical_messages.message_url), '
        . 'urls_json=COALESCE(VALUES(urls_json),tmp_bside_canonical_messages.urls_json), '
        . 'risk_flags_json=COALESCE(VALUES(risk_flags_json),tmp_bside_canonical_messages.risk_flags_json), '
        . 'raw_json=COALESCE(VALUES(raw_json),tmp_bside_canonical_messages.raw_json), '
        . 'updated_at=GREATEST(tmp_bside_canonical_messages.updated_at,VALUES(updated_at))';
    $messageParams = array_merge(array($channelId, $handle, $channelId, $channelId), $aliases);
    $messageStmt = $pdo->prepare($messageSql); $messageStmt->execute($messageParams);

    $matchSql = 'INSERT INTO tmp_bside_canonical_matches (article_id, message_key, match_type, score, reason, channel_handle, telegram_message_id, message_url, updated_at) '
        . 'SELECT tm.article_id, CONCAT(\'id:\', ?, \':\', COALESCE(tm.telegram_message_id,m.telegram_message_id)), tm.match_type, tm.score, tm.reason, ?, '
        . 'COALESCE(tm.telegram_message_id,m.telegram_message_id), tm.message_url, tm.updated_at '
        . 'FROM ' . $matchesTable . ' tm JOIN ' . $messagesTable . ' m ON m.message_key = tm.message_key '
        . 'WHERE (m.telegram_channel_id = ? OR m.channel_handle IN (' . $aliasMarks . ')) AND COALESCE(tm.telegram_message_id,m.telegram_message_id) IS NOT NULL '
        . 'ON DUPLICATE KEY UPDATE score=GREATEST(tmp_bside_canonical_matches.score,VALUES(score)), '
        . 'reason=COALESCE(VALUES(reason),tmp_bside_canonical_matches.reason), channel_handle=VALUES(channel_handle), '
        . 'telegram_message_id=VALUES(telegram_message_id), '
        . 'message_url=COALESCE(VALUES(message_url),tmp_bside_canonical_matches.message_url), '
        . 'updated_at=GREATEST(tmp_bside_canonical_matches.updated_at,VALUES(updated_at))';
    $matchParams = array_merge(array($channelId, $handle, $channelId), $aliases);
    $matchStmt = $pdo->prepare($matchSql); $matchStmt->execute($matchParams);

    $deleteMatches = $pdo->prepare('DELETE tm FROM ' . $matchesTable . ' tm LEFT JOIN ' . $messagesTable . ' m ON m.message_key = tm.message_key '
        . 'WHERE m.telegram_channel_id = ? OR m.channel_handle IN (' . $aliasMarks . ') OR tm.channel_handle IN (' . $aliasMarks . ')');
    $deleteMatches->execute(array_merge(array($channelId), $aliases, $aliases));
    $pdo->exec('INSERT INTO ' . $matchesTable . ' SELECT * FROM tmp_bside_canonical_matches');

    $deleteMessages = $pdo->prepare('DELETE FROM ' . $messagesTable . ' WHERE telegram_channel_id = ? OR channel_handle IN (' . $aliasMarks . ')');
    $deleteMessages->execute(array_merge(array($channelId), $aliases));
    $pdo->exec('INSERT INTO ' . $messagesTable . ' SELECT * FROM tmp_bside_canonical_messages');

    $deleteChannels = $pdo->prepare('DELETE FROM ' . $channelsTable . ' WHERE telegram_channel_id = ? OR handle IN (' . $aliasMarks . ')');
    $deleteChannels->execute(array_merge(array($channelId), $aliases));
    $pdo->exec('DROP TEMPORARY TABLE IF EXISTS tmp_bside_canonical_matches');
    $pdo->exec('DROP TEMPORARY TABLE IF EXISTS tmp_bside_canonical_messages');
}
