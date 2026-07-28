<?php
/**
 * BSIDE Global Market Terminal API v2.
 *
 * This module is intentionally additive. The v1 and legacy routes continue to
 * use their existing handlers while v2 uses an independent fail-closed release
 * state. PHP 7.3 compatibility is required by the production runtime.
 */

const V2_RESPONSE_BUDGET_BYTES = 250000;
const V2_DEFAULT_PAGE_SIZE = 25;
const V2_MAX_PAGE_SIZE = 100;
const V2_MAX_PAGE_NUMBER = 100;
const V2_MAX_OFFSET = 10000;
const GOV_V2_SCHEMA_VERSION = 12;
const GOV_V2_RELEASE_STATE_KEY = 'global_terminal_v2';
const V2_DEPLOYMENT_MANIFEST_SCHEMA_VERSION = 1;
const V2_DEPLOYMENT_MANIFEST_MAX_BYTES = 65536;
const V2_RELEASE_AUTHORIZATION_MIN_TTL_SECONDS = 60;
const V2_RELEASE_AUTHORIZATION_MAX_TTL_SECONDS = 900;

require_once __DIR__ . '/governance_v2_write.php';

function v2_deployment_core_files(): array {
    return array(
        '.htaccess',
        'api.php',
        'governance_v1.php',
        'governance_v2.php',
        'governance_v2_write.php',
        'openapi.yaml',
        'openapi-v2.yaml',
        'migrations/011_global_terminal_v2.sql',
        'migrations/012_dart_credential_pool.sql',
    );
}

function v2_exact_string_keys(array $value, array $expected): bool {
    $actual = array_keys($value);
    sort($actual, SORT_STRING);
    sort($expected, SORT_STRING);
    return $actual === $expected;
}

function v2_deployment_core_file_path(
    string $deploymentRoot,
    string $relativeName
): ?string {
    if (
        $relativeName === ''
        || substr($relativeName, 0, 1) === '/'
        || strpos($relativeName, '\\') !== false
    ) {
        return null;
    }
    $parts = explode('/', $relativeName);
    $candidate = $deploymentRoot;
    foreach ($parts as $index => $part) {
        if ($part === '' || $part === '.' || $part === '..') {
            return null;
        }
        $candidate .= DIRECTORY_SEPARATOR . $part;
        if (is_link($candidate)) {
            return null;
        }
        if ($index < count($parts) - 1 && !is_dir($candidate)) {
            return null;
        }
    }
    if (!is_file($candidate)) {
        return null;
    }
    $resolved = realpath($candidate);
    $expected = $deploymentRoot
        . DIRECTORY_SEPARATOR
        . str_replace('/', DIRECTORY_SEPARATOR, $relativeName);
    if ($resolved === false || $resolved !== $expected) {
        return null;
    }
    return $resolved;
}

function v2_deployment_identity_status(): array {
    $manifestPath = __DIR__ . '/deployment-manifest.json';
    if (is_link($manifestPath) || !is_file($manifestPath)) {
        return array('valid' => false, 'error' => 'deployment_manifest_missing');
    }
    $size = filesize($manifestPath);
    if ($size === false || $size < 1 || $size > V2_DEPLOYMENT_MANIFEST_MAX_BYTES) {
        return array('valid' => false, 'error' => 'deployment_manifest_invalid');
    }
    $raw = file_get_contents($manifestPath);
    if ($raw === false) {
        return array('valid' => false, 'error' => 'deployment_manifest_invalid');
    }
    $manifest = json_decode($raw, true);
    if (
        !is_array($manifest)
        || !v2_exact_string_keys($manifest, array('schema_version', 'code_revision', 'files'))
        || !isset($manifest['schema_version'])
        || !is_int($manifest['schema_version'])
        || $manifest['schema_version'] !== V2_DEPLOYMENT_MANIFEST_SCHEMA_VERSION
        || !isset($manifest['code_revision'])
        || !is_string($manifest['code_revision'])
        || preg_match('/^[0-9a-f]{40}$/D', $manifest['code_revision']) !== 1
        || !isset($manifest['files'])
        || !is_array($manifest['files'])
    ) {
        return array('valid' => false, 'error' => 'deployment_manifest_invalid');
    }
    $expectedFiles = v2_deployment_core_files();
    if (!v2_exact_string_keys($manifest['files'], $expectedFiles)) {
        return array('valid' => false, 'error' => 'deployment_manifest_invalid');
    }
    $deploymentRoot = realpath(__DIR__);
    if ($deploymentRoot === false) {
        return array('valid' => false, 'error' => 'deployment_core_file_missing');
    }
    $verifiedFiles = array();
    foreach ($expectedFiles as $relativeName) {
        $expectedHash = $manifest['files'][$relativeName];
        if (
            !is_string($expectedHash)
            || preg_match('/^[0-9a-f]{64}$/D', $expectedHash) !== 1
        ) {
            return array('valid' => false, 'error' => 'deployment_manifest_invalid');
        }
        $resolved = v2_deployment_core_file_path($deploymentRoot, $relativeName);
        if ($resolved === null) {
            return array('valid' => false, 'error' => 'deployment_core_file_missing');
        }
        $actualHash = hash_file('sha256', $resolved);
        if (!is_string($actualHash) || !hash_equals($expectedHash, $actualHash)) {
            return array('valid' => false, 'error' => 'deployment_core_hash_mismatch');
        }
        $verifiedFiles[$relativeName] = $actualHash;
    }
    return array(
        'valid' => true,
        'error' => null,
        'code_revision' => $manifest['code_revision'],
        'files' => $verifiedFiles,
    );
}

function v2_request_path(): ?string {
    if (isset($_GET['_route'])) {
        $route = v1_canonical_route_path(trim((string)$_GET['_route']));
        if (strpos($route, '/api/v2') === 0) {
            $rest = substr($route, strlen('/api/v2'));
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
        $canonical = v1_canonical_route_path($candidate);
        $marker = '/api/v2';
        $position = strpos($canonical, $marker);
        if ($position === false) {
            continue;
        }
        $rest = substr($canonical, $position + strlen($marker));
        if ($rest !== '' && substr($rest, 0, 1) !== '/') {
            continue;
        }
        return $rest === '' ? '/' : '/' . trim($rest, '/');
    }
    return null;
}

function v2_respond(int $status, array $payload): void {
    $payload['api_version'] = 'v2';
    $encoded = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    if ($encoded === false) {
        respond(500, array('ok' => false, 'error' => 'json_encoding_failed'));
    }
    if (strlen($encoded) > V2_RESPONSE_BUDGET_BYTES) {
        respond(500, array(
            'ok' => false,
            'error' => 'response_budget_exceeded',
            'max_bytes' => V2_RESPONSE_BUDGET_BYTES,
        ));
    }
    header('Content-Type: application/json; charset=utf-8');
    header('X-BSIDE-API-Version: v2');
    header('X-Response-Bytes: ' . strlen($encoded));
    http_response_code($status);
    echo $encoded;
    exit;
}

function v2_integer_query_param(
    string $name,
    int $default,
    int $minimum,
    int $maximum
): int {
    if (!array_key_exists($name, $_GET)) {
        return $default;
    }
    if (is_array($_GET[$name])) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_' . $name));
    }
    $raw = trim((string)$_GET[$name]);
    $value = filter_var(
        $raw,
        FILTER_VALIDATE_INT,
        array('options' => array('min_range' => $minimum, 'max_range' => $maximum))
    );
    if ($value === false) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_' . $name));
    }
    return (int)$value;
}

function v2_list_params(): array {
    $limit = v2_integer_query_param('limit', V2_DEFAULT_PAGE_SIZE, 1, V2_MAX_PAGE_SIZE);
    $hasPage = array_key_exists('page', $_GET);
    $hasOffset = array_key_exists('offset', $_GET);
    if ($hasPage && $hasOffset) {
        v2_respond(400, array('ok' => false, 'error' => 'ambiguous_pagination'));
    }
    $page = $hasOffset
        ? null
        : v2_integer_query_param('page', 1, 1, V2_MAX_PAGE_NUMBER);
    $offset = $hasOffset
        ? v2_integer_query_param('offset', 0, 0, V2_MAX_OFFSET)
        : (((int)$page - 1) * $limit);
    return array(
        'limit' => $limit,
        'page' => $page,
        'offset' => $offset,
    );
}

function v2_page_meta(array $page, int $returned, bool $hasMore): array {
    $nextOffset = $hasMore ? ((int)$page['offset'] + $returned) : null;
    if ($nextOffset !== null && $nextOffset > V2_MAX_OFFSET) {
        $nextOffset = null;
    }
    $pageNumber = $page['page'] === null ? null : (int)$page['page'];
    $nextPage = null;
    if (
        $hasMore
        && $nextOffset !== null
        && $pageNumber !== null
        && $returned === (int)$page['limit']
        && $pageNumber < V2_MAX_PAGE_NUMBER
    ) {
        $nextPage = $pageNumber + 1;
    }
    return array(
        'page' => $pageNumber,
        'offset' => (int)$page['offset'],
        'limit' => (int)$page['limit'],
        'returned' => $returned,
        'has_more' => $hasMore,
        'next_page' => $nextPage,
        'next_offset' => $nextOffset,
        'continuation_limited' => $hasMore && $nextOffset === null,
    );
}

function v2_fetch_page(PDOStatement $statement, array $page): array {
    $rows = $statement->fetchAll();
    $hasMore = count($rows) > (int)$page['limit'];
    if ($hasMore) {
        $rows = array_slice($rows, 0, (int)$page['limit']);
    }
    return array($rows, $hasMore);
}

function v2_json_payload_size(array $payload): int {
    $payload['api_version'] = 'v2';
    $encoded = json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
    return $encoded === false ? PHP_INT_MAX : strlen($encoded);
}

function v2_fit_json_list_page(
    array $rows,
    array $page,
    bool $sourceHasMore,
    array $extraMeta = array()
): array {
    $acceptedCount = count($rows);
    while ($acceptedCount > 0) {
        $candidateCount = $acceptedCount;
        $candidateHasMore = $sourceHasMore || $candidateCount < count($rows);
        $candidateMeta = array_merge(
            v2_page_meta($page, $candidateCount, $candidateHasMore),
            $extraMeta
        );
        $candidatePayload = array(
            'ok' => true,
            'data' => array('items' => array_slice($rows, 0, $candidateCount)),
            'meta' => $candidateMeta,
        );
        if (v2_json_payload_size($candidatePayload) <= V2_RESPONSE_BUDGET_BYTES) {
            break;
        }
        $acceptedCount--;
    }
    if ($acceptedCount === 0 && count($rows) > 0) {
        v2_respond(422, array(
            'ok' => false,
            'error' => 'response_item_exceeds_budget',
            'max_bytes' => V2_RESPONSE_BUDGET_BYTES,
        ));
    }
    $hasMore = $sourceHasMore || $acceptedCount < count($rows);
    return array(
        array_slice($rows, 0, $acceptedCount),
        array_merge(
            v2_page_meta($page, $acceptedCount, $hasMore),
            $extraMeta
        ),
    );
}

function v2_like_literal(string $value): string {
    return str_replace(
        array('!', '%', '_', '\\'),
        array('!!', '!%', '!_', '!\\'),
        $value
    );
}

function v2_require_role(array $config, array $allowedRoles): string {
    $token = v1_bearer_token();
    if ($token === '') {
        header('WWW-Authenticate: Bearer realm="BSIDE API v2", charset="UTF-8"');
        v2_respond(401, array('ok' => false, 'error' => 'bearer_token_required'));
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
    v2_respond(403, array('ok' => false, 'error' => 'insufficient_role'));
}

function v2_require_exact_role(array $config, string $requiredRole): string {
    $token = v1_bearer_token();
    if ($token === '') {
        header('WWW-Authenticate: Bearer realm="BSIDE protected release", charset="UTF-8"');
        v2_respond(401, array('ok' => false, 'error' => 'bearer_token_required'));
    }
    $candidate = hash('sha256', $token);
    foreach (v1_role_hashes($config, $requiredRole) as $expected) {
        if (hash_equals($expected, $candidate)) {
            return $requiredRole;
        }
    }
    v2_respond(403, array('ok' => false, 'error' => 'insufficient_role'));
}

function v2_require_preview_token(array $config): void {
    $token = v1_bearer_token();
    if ($token === '') {
        header('WWW-Authenticate: Bearer realm="BSIDE global preview", charset="UTF-8"');
        v2_respond(401, array('ok' => false, 'error' => 'preview_token_required'));
    }
    $candidate = hash('sha256', $token);
    foreach (v1_preview_token_hashes($config) as $expected) {
        if (hash_equals($expected, $candidate)) {
            return;
        }
    }
    v2_respond(403, array('ok' => false, 'error' => 'invalid_preview_token'));
}

function v2_expected_migration_manifest(
    string $migration011Checksum,
    string $migration012Checksum
): array {
    if (
        preg_match('/^[0-9a-f]{64}$/D', $migration011Checksum) !== 1
        || preg_match('/^[0-9a-f]{64}$/D', $migration012Checksum) !== 1
    ) {
        return array();
    }
    $manifest = v1_expected_migration_manifest();
    $manifest[11] = array(
        '011_global_terminal_v2',
        $migration011Checksum,
    );
    $manifest[12] = array(
        '012_dart_credential_pool',
        $migration012Checksum,
    );
    return $manifest;
}

function v2_schema_manifest_status(PDO $pdo, array $config): array {
    $identity = v2_deployment_identity_status();
    $migration011Path = 'migrations/011_global_terminal_v2.sql';
    $migration012Path = 'migrations/012_dart_credential_pool.sql';
    if (
        $identity['valid'] !== true
        || !isset($identity['files'])
        || !is_array($identity['files'])
        || !isset($identity['files'][$migration011Path])
        || !is_string($identity['files'][$migration011Path])
        || !isset($identity['files'][$migration012Path])
        || !is_string($identity['files'][$migration012Path])
    ) {
        return array(
            'valid' => false,
            'highest_version' => null,
            'error' => 'migration_deployment_identity_unavailable',
        );
    }
    $expected = v2_expected_migration_manifest(
        (string)$identity['files'][$migration011Path],
        (string)$identity['files'][$migration012Path]
    );
    if (count($expected) !== GOV_V2_SCHEMA_VERSION) {
        return array(
            'valid' => false,
            'highest_version' => null,
            'error' => 'migration_deployment_identity_unavailable',
        );
    }
    try {
        $statement = $pdo->query(
            'SELECT migration_version,migration_name,migration_checksum FROM '
            . table_name($config, 'schema_migrations')
            . ' WHERE migration_version<=' . GOV_V2_SCHEMA_VERSION
            . ' ORDER BY migration_version'
        );
        $rows = $statement->fetchAll();
    } catch (PDOException $error) {
        return array(
            'valid' => false,
            'highest_version' => null,
            'error' => 'migration_manifest_unavailable',
        );
    }
    $highest = null;
    foreach ($rows as $row) {
        if (isset($row['migration_version']) && is_numeric($row['migration_version'])) {
            $highest = max(
                (int)($highest === null ? 0 : $highest),
                (int)$row['migration_version']
            );
        }
    }
    if (count($rows) !== count($expected)) {
        return array(
            'valid' => false,
            'highest_version' => $highest,
            'error' => 'migration_manifest_cardinality_mismatch',
        );
    }
    foreach ($rows as $index => $row) {
        $version = $index + 1;
        if (
            (int)$row['migration_version'] !== $version
            || !isset($expected[$version])
            || !hash_equals($expected[$version][0], (string)$row['migration_name'])
            || !hash_equals(
                $expected[$version][1],
                strtolower((string)$row['migration_checksum'])
            )
        ) {
            return array(
                'valid' => false,
                'highest_version' => $highest,
                'error' => 'migration_manifest_entry_mismatch',
                'invalid_version' => $version,
            );
        }
    }
    return array(
        'valid' => true,
        'highest_version' => GOV_V2_SCHEMA_VERSION,
        'error' => null,
    );
}

function v2_require_schema_version(PDO $pdo, array $config): int {
    $manifest = v2_schema_manifest_status($pdo, $config);
    if ($manifest['valid'] !== true) {
        header('Retry-After: 300');
        v2_respond(503, array(
            'ok' => false,
            'error' => 'schema_version_mismatch',
            'expected_schema_version' => GOV_V2_SCHEMA_VERSION,
            'actual_schema_version' => $manifest['highest_version'],
            'schema_manifest_error' => $manifest['error'],
        ));
    }
    return GOV_V2_SCHEMA_VERSION;
}

function v2_release_state(PDO $pdo, array $config, bool $forUpdate = false): ?array {
    $sql = 'SELECT release_state,state_version,updated_by,update_reason,cutover_at,sunset_at,updated_at FROM '
        . table_name($config, 'governance_release_state') . ' WHERE state_key=?'
        . ($forUpdate ? ' FOR UPDATE' : '');
    $statement = $pdo->prepare($sql);
    $statement->execute(array(GOV_V2_RELEASE_STATE_KEY));
    $row = $statement->fetch();
    return is_array($row) ? $row : null;
}

function v2_require_public_release_access(PDO $pdo, array $config): string {
    $row = v2_release_state($pdo, $config);
    $state = is_array($row) && isset($row['release_state']) ? (string)$row['release_state'] : '';
    if (!in_array($state, array('closed', 'preview', 'live'), true)) {
        header('Retry-After: 300');
        v2_respond(503, array('ok' => false, 'error' => 'release_state_unavailable'));
    }
    if ($state === 'closed') {
        header('Retry-After: 300');
        v2_respond(503, array('ok' => false, 'error' => 'global_terminal_release_closed'));
    }
    if ($state === 'preview') {
        v2_require_preview_token($config);
        header('Cache-Control: private, no-store');
        header('Vary: Authorization');
        return $state;
    }
    header('Cache-Control: public, max-age=60, stale-while-revalidate=300');
    return $state;
}

function v2_serve_openapi(string $path): void {
    $file = __DIR__ . '/openapi-v2.yaml';
    if (!is_file($file)) {
        v2_respond(404, array('ok' => false, 'error' => 'openapi_not_deployed'));
    }
    if ($path === '/openapi.json') {
        v2_respond(406, array(
            'ok' => false,
            'error' => 'json_spec_not_available',
            'yaml' => '/api/v2/openapi.yaml',
        ));
    }
    header('Content-Type: application/yaml; charset=utf-8');
    header('X-BSIDE-API-Version: v2');
    header('Cache-Control: public, max-age=300');
    readfile($file);
    exit;
}

function v2_path_is_defined(string $path): bool {
    if (in_array($path, array(
        '/',
        '/health',
        '/openapi.yaml',
        '/openapi.json',
        '/briefs/latest',
        '/live',
        '/events',
        '/issuers',
        '/calendar',
        '/search',
        '/sources/status',
        '/ops/source-right-eligibility',
        '/ops/alpha-release-evidence',
        '/ops/release-state',
        '/ops/ingest',
        '/exports/events.json',
        '/exports/events.csv',
        '/feeds/events.atom',
        '/admin/release-state',
        '/admin/release-authorizations',
        '/admin/cutover',
        '/admin/connectors',
        '/admin/review-queue',
        '/admin/brief-candidates',
        '/admin/briefs',
    ), true)) {
        return true;
    }
    foreach (array(
        '#^/events/[A-Za-z0-9_.:\-]{1,96}$#',
        '#^/issuers/[A-Za-z0-9_.:\-]{1,96}$#',
        '#^/ops/connectors/connector:[a-z]{2}:[a-z0-9_.:\-]{1,64}/checkpoint$#',
        '#^/admin/connectors/connector:[a-z]{2}:[a-z0-9_.:\-]{1,64}$#',
        '#^/admin/events/[A-Za-z0-9_.:\-]{1,96}/review$#',
    ) as $pattern) {
        if (preg_match($pattern, $path) === 1) {
            return true;
        }
    }
    return false;
}

function v2_valid_country($value, bool $allowGlobal = false): ?string {
    if (!is_string($value)) {
        return null;
    }
    $country = strtoupper(trim($value));
    $allowed = array('KR', 'US', 'JP', 'GB', 'CA', 'AU');
    if ($allowGlobal) {
        $allowed[] = 'GLOBAL';
    }
    return in_array($country, $allowed, true) ? $country : null;
}

function v2_document_visibility_sql(
    string $documentAlias = 'd',
    string $rightsAlias = 'sr'
): string {
    return '(' . v1_document_visibility_sql(
        $documentAlias,
        $rightsAlias,
        false
    ) . ' AND ' . v2_document_source_right_identity_sql(
        $documentAlias,
        $rightsAlias
    ) . ' AND ' . v2_non_telegram_document_sql($documentAlias) . ')';
}

/**
 * A v2 document is authorized only by the grant registered for its exact
 * source identity. BINARY comparisons keep this fail-closed under the
 * case-insensitive database collation.
 */
function v2_document_source_right_identity_sql(
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

/** A connector must retain the exact source identity of its referenced grant. */
function v2_connector_source_right_identity_sql(
    string $connectorAlias = 'sc',
    string $rightsAlias = 'sr'
): string {
    return '(' . $connectorAlias . '.source_right_id IS NOT NULL'
        . ' AND ' . $rightsAlias . '.source_right_id='
        . $connectorAlias . '.source_right_id'
        . ' AND BINARY ' . $connectorAlias . '.source_type=BINARY '
        . $rightsAlias . '.source_type'
        . ' AND BINARY ' . $connectorAlias . '.source_key=BINARY '
        . $rightsAlias . '.source_key)';
}

/**
 * Telegram is an internal signal source only in the global terminal.
 *
 * Keep this exclusion in one SQL predicate and compose it into every v2
 * document visibility check. A valid redistribution grant must never make a
 * Telegram document, observation, evidence count, or representative URL
 * public.
 */
function v2_non_telegram_document_sql(string $documentAlias = 'd'): string {
    return '(LOWER(COALESCE(' . $documentAlias . '.source_class,\'\'))'
        . ' NOT IN (\'licensed_telegram\',\'authorized_telegram\'))';
}

/** A current, evidence-backed SourceRight, independent of redistribution. */
function v2_current_source_right_sql(string $rightsAlias = 'sr'): string {
    return '(' . $rightsAlias . '.source_right_id IS NOT NULL'
        . ' AND ' . $rightsAlias . '.status=\'active\''
        . ' AND NULLIF(TRIM(' . $rightsAlias . '.permission_scope),\'\') IS NOT NULL'
        . ' AND ' . $rightsAlias . '.valid_from<=UTC_TIMESTAMP()'
        . ' AND (' . $rightsAlias . '.valid_until IS NULL'
        . ' OR ' . $rightsAlias . '.valid_until>UTC_TIMESTAMP())'
        . ' AND ' . $rightsAlias . '.revoked_at IS NULL'
        . ' AND (NULLIF(TRIM(' . $rightsAlias . '.evidence_uri),\'\') IS NOT NULL'
        . ' OR ' . $rightsAlias . '.evidence_hash'
        . ' REGEXP \'^[A-Fa-f0-9]{64}$\'))';
}

function v2_event_visibility_sql(array $config, string $eventAlias = 'e'): string {
    return '(' . $eventAlias . '.issuer_id IS NOT NULL'
        . ' AND ' . $eventAlias . '.country_code NOT IN (\'JP\',\'GB\')'
        . ' AND ' . $eventAlias . '.publication_status=\'published\''
        . ' AND ' . $eventAlias . '.identity_status=\'complete\''
        . ' AND ' . $eventAlias . '.global_event_family'
        . ' IN (\'large_ownership\',\'meeting_and_vote\',\'tender_offer_and_mna\','
        . '\'capital_issuance\',\'capital_return\',\'board_and_compensation\','
        . '\'listing_status\',\'correction_and_withdrawal\')'
        . ' AND ' . $eventAlias . '.importance'
        . ' IN (\'low\',\'medium\',\'high\',\'critical\',\'market_sensitive\')'
        . ' AND ' . $eventAlias . '.review_status IN (\'approved\',\'not_required\')'
        . ' AND ' . $eventAlias . '.verification_status'
        . ' IN (\'official\',\'confirmed\',\'corroborated\',\'corrected\',\'withdrawn\')'
        . ' AND (' . $eventAlias . '.importance NOT IN (\'high\',\'critical\',\'market_sensitive\')'
        . ' OR ' . $eventAlias . '.review_status=\'approved\')'
        . ' AND (' . $eventAlias . '.verification_status<>\'withdrawn\''
        . ' OR ' . $eventAlias . '.review_status=\'approved\')'
        . ' AND JSON_UNQUOTE(JSON_EXTRACT(' . $eventAlias
        . '.payload_json,\'$.metadata.title_provenance\'))'
        . ' IN (\'source\',\'generated_metadata\',\'operator_metadata\')'
        . ' AND EXISTS (SELECT 1 FROM ' . table_name($config, 'event_documents') . ' visible_v2_ed'
        . ' JOIN ' . table_name($config, 'documents') . ' visible_v2_d'
        . ' ON visible_v2_d.document_id=visible_v2_ed.document_id'
        . ' LEFT JOIN ' . table_name($config, 'source_rights') . ' visible_v2_sr'
        . ' ON visible_v2_sr.source_right_id=visible_v2_d.source_right_id'
        . ' WHERE visible_v2_ed.event_id=' . $eventAlias . '.event_id AND '
        . v2_document_visibility_sql('visible_v2_d', 'visible_v2_sr') . '))';
}

function v2_official_evidence_sql(array $config, string $eventAlias = 'e'): string {
    return '(SELECT COUNT(*) FROM ' . table_name($config, 'event_documents') . ' official_v2_ed'
        . ' JOIN ' . table_name($config, 'documents') . ' official_v2_d'
        . ' ON official_v2_d.document_id=official_v2_ed.document_id'
        . ' LEFT JOIN ' . table_name($config, 'source_rights') . ' official_v2_sr'
        . ' ON official_v2_sr.source_right_id=official_v2_d.source_right_id'
        . ' WHERE official_v2_ed.event_id=' . $eventAlias . '.event_id'
        . ' AND official_v2_d.source_class IN (\'official_disclosure\',\'official_register\','
        . '\'company_statement\',\'official_issuer\')'
        . ' AND ' . v2_document_visibility_sql('official_v2_d', 'official_v2_sr') . ')';
}

function v2_event_select(array $config): string {
    $officialCount = v2_official_evidence_sql($config, 'e');
    $visibleDocument = v2_document_visibility_sql('lead_d', 'lead_sr');
    return 'SELECT e.event_id,e.issuer_id,i.legal_name AS issuer_name,'
        . 'COALESCE(primary_listing.ticker,\'\') AS ticker,'
        . 'COALESCE(primary_listing.market,j.default_market,\'\') AS market,'
        . 'e.country_code AS country,e.global_event_family AS event_family,e.importance,'
        . 'e.verification_status,e.change_type,'
        . 'e.title,JSON_UNQUOTE(JSON_EXTRACT(e.payload_json,'
        . '\'$.metadata.title_provenance\')) AS title_provenance,'
        . 'e.original_language,LEFT(COALESCE(e.summary,\'\'),2000) AS change_summary,'
        . 'COALESCE(e.current_status,e.verification_status) AS current_status,'
        . '(SELECT a.display_name FROM ' . table_name($config, 'event_actors') . ' event_actor'
        . ' JOIN ' . table_name($config, 'actors') . ' a ON a.actor_id=event_actor.actor_id'
        . ' WHERE event_actor.event_id=e.event_id AND event_actor.review_status=\'approved\''
        . ' AND a.review_status=\'approved\' AND a.record_status=\'active\''
        . ' ORDER BY event_actor.actor_role,event_actor.actor_id LIMIT 1) AS actor_name,'
        . '(SELECT event_actor.actor_role FROM ' . table_name($config, 'event_actors') . ' event_actor'
        . ' JOIN ' . table_name($config, 'actors') . ' a ON a.actor_id=event_actor.actor_id'
        . ' WHERE event_actor.event_id=e.event_id AND event_actor.review_status=\'approved\''
        . ' AND a.review_status=\'approved\' AND a.record_status=\'active\''
        . ' ORDER BY event_actor.actor_role,event_actor.actor_id LIMIT 1) AS actor_role,'
        . 'e.occurred_at,'
        . '(SELECT COALESCE(lead_d.filed_at,lead_d.published_at) FROM ' . table_name($config, 'event_documents') . ' lead_ed'
        . ' JOIN ' . table_name($config, 'documents') . ' lead_d ON lead_d.document_id=lead_ed.document_id'
        . ' LEFT JOIN ' . table_name($config, 'source_rights') . ' lead_sr'
        . ' ON lead_sr.source_right_id=lead_d.source_right_id'
        . ' WHERE lead_ed.event_id=e.event_id AND ' . $visibleDocument
        . ' ORDER BY lead_ed.position_no,lead_d.filed_at,lead_d.document_id LIMIT 1) AS filed_at,'
        . 'e.first_observed_at,e.updated_at,e.deadline_at,'
        . $officialCount . ' AS official_evidence_count,'
        . '(SELECT COUNT(*) FROM ' . table_name($config, 'event_documents') . ' media_ed'
        . ' JOIN ' . table_name($config, 'documents') . ' media_d ON media_d.document_id=media_ed.document_id'
        . ' LEFT JOIN ' . table_name($config, 'source_rights') . ' media_sr'
        . ' ON media_sr.source_right_id=media_d.source_right_id'
        . ' WHERE media_ed.event_id=e.event_id'
        . ' AND media_d.source_class IN (\'media_report\',\'press_report\',\'news\')'
        . ' AND ' . v2_document_visibility_sql('media_d', 'media_sr') . ') AS media_count,'
        . '(SELECT sc.coverage_mode FROM ' . table_name($config, 'source_coverage') . ' sc'
        . ' WHERE sc.country_code=e.country_code AND sc.effective_from<=UTC_TIMESTAMP()'
        . ' AND (sc.effective_until IS NULL OR sc.effective_until>UTC_TIMESTAMP())'
        . ' AND sc.event_family=e.global_event_family'
        . ' ORDER BY sc.effective_from DESC LIMIT 1) AS coverage_mode,'
        . '(SELECT lead_url_d.original_url FROM ' . table_name($config, 'event_documents') . ' lead_url_ed'
        . ' JOIN ' . table_name($config, 'documents') . ' lead_url_d'
        . ' ON lead_url_d.document_id=lead_url_ed.document_id'
        . ' LEFT JOIN ' . table_name($config, 'source_rights') . ' lead_url_sr'
        . ' ON lead_url_sr.source_right_id=lead_url_d.source_right_id'
        . ' WHERE lead_url_ed.event_id=e.event_id AND '
        . v2_document_visibility_sql('lead_url_d', 'lead_url_sr')
        . ' ORDER BY lead_url_ed.position_no,lead_url_d.document_id LIMIT 1) AS source_url '
        . 'FROM ' . table_name($config, 'governance_events') . ' e '
        . 'JOIN ' . table_name($config, 'issuers') . ' i ON i.issuer_id=e.issuer_id '
        . 'LEFT JOIN ' . table_name($config, 'jurisdictions') . ' j ON j.country_code=e.country_code '
        . 'LEFT JOIN ' . table_name($config, 'issuer_listings') . ' primary_listing'
        . ' ON primary_listing.issuer_id=e.issuer_id AND primary_listing.is_primary=1 ';
}

function v2_event_filters(array $config, bool $includeQuery = false): array {
    $where = array(v2_event_visibility_sql($config, 'e'), 'i.record_status=\'active\'');
    $params = array();
    $simple = array(
        'market' => array('primary_listing.market', '/^[A-Za-z0-9_.:\-]{1,40}$/'),
        'issuer_id' => array('e.issuer_id', '/^[A-Za-z0-9_.:\-]{1,96}$/'),
        'event_family' => array('e.global_event_family', '/^[a-z][a-z0-9_]{1,63}$/'),
        'verification_status' => array('e.verification_status', '/^[a-z][a-z0-9_]{1,39}$/'),
        'change_type' => array('e.change_type', '/^[a-z][a-z0-9_]{1,23}$/'),
    );
    $country = isset($_GET['country']) && trim((string)$_GET['country']) !== ''
        ? v2_valid_country((string)$_GET['country']) : '';
    if (isset($_GET['country']) && trim((string)$_GET['country']) !== '' && $country === null) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_country'));
    }
    if ($country !== '') {
        $where[] = 'e.country_code=?';
        $params[] = $country;
    }
    foreach ($simple as $key => $definition) {
        $value = isset($_GET[$key]) ? trim((string)$_GET[$key]) : '';
        if ($value === '') {
            continue;
        }
        if (preg_match($definition[1], $value) !== 1) {
            v2_respond(400, array('ok' => false, 'error' => 'invalid_' . $key));
        }
        $where[] = $definition[0] . '=?';
        $params[] = $value;
    }
    foreach (array('from' => '>=', 'to' => '<=') as $key => $operator) {
        $value = isset($_GET[$key]) ? trim((string)$_GET[$key]) : '';
        if ($value === '') {
            continue;
        }
        $datetime = v1_mysql_datetime_utc($value);
        if ($datetime === null) {
            v2_respond(400, array('ok' => false, 'error' => 'invalid_' . $key));
        }
        $where[] = 'e.occurred_at ' . $operator . ' ?';
        $params[] = $datetime;
    }
    if ($includeQuery) {
        $query = isset($_GET['q']) ? trim((string)$_GET['q']) : '';
        if (mb_strlen($query, 'UTF-8') < 2 || mb_strlen($query, 'UTF-8') > 100) {
            v2_respond(400, array('ok' => false, 'error' => 'invalid_query'));
        }
        $like = '%' . v2_like_literal($query) . '%';
        $where[] = '(e.title LIKE ? ESCAPE \'!\' OR e.summary LIKE ? ESCAPE \'!\''
            . ' OR COALESCE(e.current_status,\'\') LIKE ? ESCAPE \'!\''
            . ' OR e.global_event_family LIKE ? ESCAPE \'!\''
            . ' OR i.legal_name LIKE ? ESCAPE \'!\''
            . ' OR COALESCE(primary_listing.ticker,\'\') LIKE ? ESCAPE \'!\''
            . ' OR COALESCE(primary_listing.market,\'\') LIKE ? ESCAPE \'!\''
            . ' OR EXISTS (SELECT 1 FROM ' . table_name($config, 'issuer_identifiers') . ' search_ii'
            . ' WHERE search_ii.issuer_id=e.issuer_id'
            . ' AND (search_ii.identifier_value LIKE ? ESCAPE \'!\''
            . ' OR search_ii.identifier_type LIKE ? ESCAPE \'!\'))'
            . ' OR EXISTS (SELECT 1 FROM ' . table_name($config, 'event_actors') . ' search_ea'
            . ' JOIN ' . table_name($config, 'actors') . ' search_a ON search_a.actor_id=search_ea.actor_id'
            . ' WHERE search_ea.event_id=e.event_id AND search_ea.review_status=\'approved\''
            . ' AND search_a.review_status=\'approved\' AND search_a.record_status=\'active\''
            . ' AND search_a.display_name LIKE ? ESCAPE \'!\')'
            . ' OR EXISTS (SELECT 1 FROM ' . table_name($config, 'event_documents') . ' search_ed'
            . ' JOIN ' . table_name($config, 'documents') . ' search_d'
            . ' ON search_d.document_id=search_ed.document_id'
            . ' LEFT JOIN ' . table_name($config, 'source_rights') . ' search_sr'
            . ' ON search_sr.source_right_id=search_d.source_right_id'
            . ' WHERE search_ed.event_id=e.event_id AND '
            . v2_document_visibility_sql('search_d', 'search_sr')
            . ' AND (search_d.title LIKE ? ESCAPE \'!\''
            . ' OR search_d.document_type LIKE ? ESCAPE \'!\')))';
        array_push(
            $params,
            $like,
            $like,
            $like,
            $like,
            $like,
            $like,
            $like,
            $like,
            $like,
            $like,
            $like,
            $like
        );
    }
    return array($where, $params);
}

/**
 * Serialize database UTC DATETIME values as a single public wire format.
 *
 * MySQL DATETIME has no offset information. Governance timestamps are stored
 * in UTC, so public v2 responses must add the explicit UTC designator instead
 * of exposing the database representation. Accepting the already-normalized
 * form keeps frozen BriefItem snapshots idempotent.
 */
function v2_public_iso_time($value): ?string {
    $mysqlTime = v1_release_iso_time($value);
    if ($mysqlTime !== null) {
        return $mysqlTime;
    }
    if (
        is_string($value)
        && preg_match(
            '/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/D',
            $value
        ) === 1
    ) {
        return $value;
    }
    return null;
}

function v2_normalize_public_time_fields(
    array $row,
    array $fields
): array {
    foreach ($fields as $field) {
        if (
            !array_key_exists($field, $row)
            || $row[$field] === null
            || $row[$field] === ''
        ) {
            if (array_key_exists($field, $row)) {
                $row[$field] = null;
            }
            continue;
        }
        $normalized = v2_public_iso_time($row[$field]);
        if ($normalized === null) {
            throw new RuntimeException(
                'invalid_public_timestamp_' . (string)$field
            );
        }
        $row[$field] = $normalized;
    }
    return $row;
}

function v2_normalize_event_rows(array $rows): array {
    foreach ($rows as &$row) {
        $row = v2_normalize_public_time_fields(
            $row,
            array(
                'occurred_at',
                'filed_at',
                'first_observed_at',
                'updated_at',
                'deadline_at',
            )
        );
        $row['official_evidence_count'] = (int)$row['official_evidence_count'];
        $row['media_count'] = (int)$row['media_count'];
        if ($row['coverage_mode'] === null || $row['coverage_mode'] === '') {
            $row['coverage_mode'] = 'unavailable';
        }
    }
    unset($row);
    return $rows;
}

function v2_query_events(PDO $pdo, array $config, array $page, string $orderBy, bool $search = false): array {
    list($where, $params) = v2_event_filters($config, $search);
    $sql = v2_event_select($config) . ' WHERE ' . implode(' AND ', $where)
        . ' ORDER BY ' . $orderBy . ' LIMIT ' . ((int)$page['limit'] + 1)
        . ' OFFSET ' . (int)$page['offset'];
    $statement = $pdo->prepare($sql);
    $statement->execute($params);
    list($rows, $hasMore) = v2_fetch_page($statement, $page);
    return array(v2_normalize_event_rows($rows), $hasMore);
}

function v2_list_events(PDO $pdo, array $config, bool $live = false): void {
    $page = v2_list_params();
    $order = $live ? 'e.updated_at DESC,e.event_id DESC' : 'e.occurred_at DESC,e.event_id DESC';
    list($rows, $hasMore) = v2_query_events($pdo, $config, $page, $order);
    list($rows, $meta) = v2_fit_json_list_page($rows, $page, $hasMore);
    v2_respond(200, array(
        'ok' => true,
        'data' => array('items' => $rows),
        'meta' => $meta,
    ));
}

function v2_get_event(PDO $pdo, array $config, string $eventId): void {
    $sql = v2_event_select($config) . ' WHERE e.event_id=? AND '
        . v2_event_visibility_sql($config, 'e') . ' LIMIT 1';
    $statement = $pdo->prepare($sql);
    $statement->execute(array($eventId));
    $event = $statement->fetch();
    if (!$event) {
        v2_respond(404, array('ok' => false, 'error' => 'event_not_found'));
    }
    $event = v2_normalize_event_rows(array($event))[0];
    $documents = $pdo->prepare(
        'SELECT d.document_id,d.document_type,d.source_class,d.source_key,d.original_language,d.title,'
        . 'd.original_url,d.filed_at,d.published_at,d.verification_status,d.correction_of_document_id '
        . 'FROM ' . table_name($config, 'event_documents') . ' ed '
        . 'JOIN ' . table_name($config, 'documents') . ' d ON d.document_id=ed.document_id '
        . 'LEFT JOIN ' . table_name($config, 'source_rights') . ' sr ON sr.source_right_id=d.source_right_id '
        . 'WHERE ed.event_id=? AND ' . v2_document_visibility_sql('d', 'sr')
        . ' ORDER BY ed.position_no,d.filed_at,d.document_id LIMIT 100'
    );
    $documents->execute(array($eventId));
    $documentRows = $documents->fetchAll();
    foreach ($documentRows as &$documentRow) {
        $documentRow = v2_normalize_public_time_fields(
            $documentRow,
            array('filed_at', 'published_at')
        );
    }
    unset($documentRow);
    $observations = $pdo->prepare(
        'SELECT eo.observation_id,eo.document_id,eo.source_class,eo.source_key,'
        . 'eo.first_observed_at,eo.observed_at FROM ' . table_name($config, 'event_observations') . ' eo '
        . 'JOIN ' . table_name($config, 'documents') . ' d ON d.document_id=eo.document_id '
        . 'LEFT JOIN ' . table_name($config, 'source_rights') . ' sr ON sr.source_right_id=d.source_right_id '
        . 'WHERE eo.event_id=? AND ' . v2_document_visibility_sql('d', 'sr')
        . ' ORDER BY eo.observed_at DESC LIMIT 100'
    );
    $observations->execute(array($eventId));
    $observationRows = $observations->fetchAll();
    foreach ($observationRows as &$observationRow) {
        $observationRow = v2_normalize_public_time_fields(
            $observationRow,
            array('first_observed_at', 'observed_at')
        );
    }
    unset($observationRow);
    $actors = $pdo->prepare(
        'SELECT a.actor_id,a.display_name,a.display_name_en,a.actor_type,a.country_code,'
        . 'ea.actor_role FROM ' . table_name($config, 'event_actors') . ' ea '
        . 'JOIN ' . table_name($config, 'actors') . ' a ON a.actor_id=ea.actor_id '
        . 'WHERE ea.event_id=? AND ea.review_status=\'approved\''
        . ' AND a.review_status=\'approved\' AND a.record_status=\'active\''
        . ' ORDER BY ea.actor_role,a.display_name,a.actor_id LIMIT 100'
    );
    $actors->execute(array($eventId));
    v2_respond(200, array(
        'ok' => true,
        'data' => array(
            'event' => $event,
            'actors' => $actors->fetchAll(),
            'documents' => $documentRows,
            'observations' => $observationRows,
        ),
    ));
}

function v2_list_issuers(PDO $pdo, array $config): void {
    $page = v2_list_params();
    $country = isset($_GET['country']) && trim((string)$_GET['country']) !== ''
        ? v2_valid_country((string)$_GET['country']) : '';
    if (isset($_GET['country']) && trim((string)$_GET['country']) !== '' && $country === null) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_country'));
    }
    $query = isset($_GET['q']) ? trim((string)$_GET['q']) : '';
    $where = array(
        'i.record_status=\'active\'',
        'EXISTS (SELECT 1 FROM ' . table_name($config, 'governance_events') . ' visible_issuer_event'
            . ' WHERE visible_issuer_event.issuer_id=i.issuer_id AND '
            . v2_event_visibility_sql($config, 'visible_issuer_event') . ')',
    );
    $params = array();
    if ($country !== '') {
        $where[] = 'i.country_code=?';
        $params[] = $country;
    }
    if ($query !== '') {
        if (mb_strlen($query, 'UTF-8') < 2 || mb_strlen($query, 'UTF-8') > 100) {
            v2_respond(400, array('ok' => false, 'error' => 'invalid_query'));
        }
        $like = '%' . v2_like_literal($query) . '%';
        $where[] = '(i.legal_name LIKE ? ESCAPE \'!\''
            . ' OR i.legal_name_en LIKE ? ESCAPE \'!\''
            . ' OR i.short_name LIKE ? ESCAPE \'!\')';
        array_push($params, $like, $like, $like);
    }
    $sql = 'SELECT i.issuer_id,i.country_code,i.legal_name,i.legal_name_en,i.short_name,'
        . 'i.original_language,i.homepage_url,i.listing_status,'
        . 'COALESCE(l.market,j.default_market,\'\') AS market,COALESCE(l.ticker,\'\') AS ticker,'
        . '(SELECT COUNT(*) FROM ' . table_name($config, 'governance_events') . ' event_count_v2'
        . ' WHERE event_count_v2.issuer_id=i.issuer_id AND '
        . v2_event_visibility_sql($config, 'event_count_v2') . ') AS event_count '
        . 'FROM ' . table_name($config, 'issuers') . ' i '
        . 'LEFT JOIN ' . table_name($config, 'issuer_listings') . ' l'
        . ' ON l.issuer_id=i.issuer_id AND l.is_primary=1 '
        . 'LEFT JOIN ' . table_name($config, 'jurisdictions') . ' j ON j.country_code=i.country_code '
        . 'WHERE ' . implode(' AND ', $where)
        . ' ORDER BY i.legal_name,i.issuer_id LIMIT ' . ((int)$page['limit'] + 1)
        . ' OFFSET ' . (int)$page['offset'];
    $statement = $pdo->prepare($sql);
    $statement->execute($params);
    list($rows, $hasMore) = v2_fetch_page($statement, $page);
    foreach ($rows as &$row) {
        $row['event_count'] = (int)$row['event_count'];
    }
    unset($row);
    list($rows, $meta) = v2_fit_json_list_page($rows, $page, $hasMore);
    v2_respond(200, array(
        'ok' => true,
        'data' => array('items' => $rows),
        'meta' => $meta,
    ));
}

function v2_get_issuer(PDO $pdo, array $config, string $issuerId): void {
    $issuerStatement = $pdo->prepare(
        'SELECT i.issuer_id,i.country_code,i.legal_name,i.legal_name_en,i.short_name,'
        . 'i.original_language,i.homepage_url,i.listing_status,i.master_modified_at '
        . 'FROM ' . table_name($config, 'issuers') . ' i WHERE i.issuer_id=?'
        . ' AND i.record_status=\'active\' AND EXISTS (SELECT 1 FROM '
        . table_name($config, 'governance_events') . ' visible_issuer_event'
        . ' WHERE visible_issuer_event.issuer_id=i.issuer_id AND '
        . v2_event_visibility_sql($config, 'visible_issuer_event') . ') LIMIT 1'
    );
    $issuerStatement->execute(array($issuerId));
    $issuer = $issuerStatement->fetch();
    if (!$issuer) {
        v2_respond(404, array('ok' => false, 'error' => 'issuer_not_found'));
    }
    $identifiers = $pdo->prepare(
        'SELECT identifier_type,identifier_value,market,is_primary,valid_from,valid_until FROM '
        . table_name($config, 'issuer_identifiers') . ' WHERE issuer_id=?'
        . ' ORDER BY is_primary DESC,identifier_type,identifier_value'
    );
    $identifiers->execute(array($issuerId));
    $listings = $pdo->prepare(
        'SELECT listing_id,country_code,market,ticker,isin,currency_code,listing_status,is_primary FROM '
        . table_name($config, 'issuer_listings') . ' WHERE issuer_id=?'
        . ' ORDER BY is_primary DESC,market,ticker'
    );
    $listings->execute(array($issuerId));
    $originalQuery = $_GET;
    $_GET['issuer_id'] = $issuerId;
    $page = array('limit' => 50, 'page' => 1, 'offset' => 0);
    list($events) = v2_query_events($pdo, $config, $page, 'e.occurred_at DESC,e.event_id DESC');
    $_GET = $originalQuery;
    v2_respond(200, array(
        'ok' => true,
        'data' => array(
            'issuer' => $issuer,
            'identifiers' => $identifiers->fetchAll(),
            'listings' => $listings->fetchAll(),
            'events' => $events,
        ),
    ));
}

function v2_source_freshness_limit_minutes($scheduleMinutes): int {
    $cadence = max(1, (int)$scheduleMinutes);
    return min(45, max(15, $cadence * 3));
}

function v2_source_age_minutes($value): ?int {
    if ($value === null || trim((string)$value) === '') {
        return null;
    }
    $timestamp = strtotime((string)$value . ' UTC');
    if ($timestamp === false) {
        return null;
    }
    $ageSeconds = time() - $timestamp;
    if ($ageSeconds < -60) {
        return null;
    }
    return max(0, (int)floor($ageSeconds / 60));
}

/**
 * Decode the SEC Latest Filings cursor and return the whole-minute UTC age.
 *
 * The connector writes a canonical unpadded base64url JSON payload:
 * sec-current-v1:{"schema_version":1,"updated_at":"...+00:00"}.
 * Reject malformed/non-canonical base64url, extra JSON fields, non-UTC
 * timestamps, impossible dates, and timestamps more than 60 seconds ahead.
 */
function v2_sec_current_cursor_age_minutes($value): ?int {
    if (!is_string($value)) {
        return null;
    }
    $prefix = 'sec-current-v1:';
    $cursor = trim($value);
    if (strpos($cursor, $prefix) !== 0) {
        return null;
    }
    $encoded = substr($cursor, strlen($prefix));
    if (
        $encoded === ''
        || strlen($encoded) > 1000
        || preg_match('/^[A-Za-z0-9_-]+$/D', $encoded) !== 1
        || strlen($encoded) % 4 === 1
    ) {
        return null;
    }
    $standard = strtr($encoded, '-_', '+/');
    $standard .= str_repeat('=', (4 - strlen($standard) % 4) % 4);
    $decoded = base64_decode($standard, true);
    if (
        $decoded === false
        || strlen($decoded) > 512
        || rtrim(strtr(base64_encode($decoded), '+/', '-_'), '=') !== $encoded
    ) {
        return null;
    }
    $payload = json_decode($decoded, true);
    if (
        json_last_error() !== JSON_ERROR_NONE
        || !is_array($payload)
        || count($payload) !== 2
        || !array_key_exists('schema_version', $payload)
        || $payload['schema_version'] !== 1
        || !array_key_exists('updated_at', $payload)
        || !is_string($payload['updated_at'])
    ) {
        return null;
    }
    $updatedAt = $payload['updated_at'];
    if (
        preg_match(
            '/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|\+00:00)$/D',
            $updatedAt
        ) !== 1
    ) {
        return null;
    }
    $normalized = substr($updatedAt, 0, 19) . '+00:00';
    $parsed = DateTimeImmutable::createFromFormat(
        '!Y-m-d\TH:i:sP',
        $normalized,
        new DateTimeZone('UTC')
    );
    $parseErrors = DateTimeImmutable::getLastErrors();
    if (
        $parsed === false
        || (
            is_array($parseErrors)
            && (
                (int)$parseErrors['warning_count'] !== 0
                || (int)$parseErrors['error_count'] !== 0
            )
        )
        || $parsed->format('Y-m-d\TH:i:sP') !== $normalized
    ) {
        return null;
    }
    $ageSeconds = time() - $parsed->getTimestamp();
    if ($ageSeconds < -60) {
        return null;
    }
    return max(0, (int)floor($ageSeconds / 60));
}

/**
 * One readiness contract shared by the public status endpoint and cutover.
 *
 * A successful zero-result poll remains valid for market-wide/register
 * connectors. Link-only sources additionally need at least one recently
 * observed and acknowledged approved metadata record.
 */
function v2_source_connector_readiness(array $connector): array {
    $limit = v2_source_freshness_limit_minutes(
        isset($connector['schedule_minutes']) ? $connector['schedule_minutes'] : 30
    );
    $successAge = v2_source_age_minutes(
        isset($connector['last_success_at']) ? $connector['last_success_at'] : null
    );
    $checkedAge = v2_source_age_minutes(
        isset($connector['last_checked_at']) ? $connector['last_checked_at'] : null
    );
    $observedAge = v2_source_age_minutes(
        isset($connector['last_observed_at']) ? $connector['last_observed_at'] : null
    );
    $reasons = array();
    if ((string)($connector['connector_status'] ?? '') !== 'active') {
        $reasons[] = 'connector_not_active';
    }
    if (trim((string)($connector['last_error_class'] ?? '')) !== '') {
        $reasons[] = 'connector_has_error';
    }
    if ($successAge === null || $successAge > $limit) {
        $reasons[] = 'last_success_missing_or_stale';
    }
    if ($checkedAge === null || $checkedAge > $limit) {
        $reasons[] = 'last_checked_missing_or_stale';
    }
    $isUsLiveConnector = (string)($connector['connector_id'] ?? '')
        === 'connector:us:sec-edgar';
    $liveCursorReady = true;
    $liveCursorAge = null;
    if ($isUsLiveConnector) {
        $cursor = json_decode(
            (string)($connector['cursor_json'] ?? ''),
            true
        );
        $sourceCursor = (
            is_array($cursor)
            && isset($cursor['schema_version'])
            && $cursor['schema_version'] === 2
            && isset($cursor['source_cursor'])
            && is_string($cursor['source_cursor'])
        ) ? $cursor['source_cursor'] : null;
        $liveCursorAge = v2_sec_current_cursor_age_minutes($sourceCursor);
        $liveCursorReady = (
            $liveCursorAge !== null
            && $liveCursorAge <= $limit
        );
        if (!$liveCursorReady) {
            $reasons[] = 'intraday_cursor_missing_or_stale';
        }
    }
    if ((string)($connector['coverage_mode'] ?? '') === 'link-only') {
        $raw = isset($connector['last_raw_count'])
            ? (int)$connector['last_raw_count'] : 0;
        $acknowledged = isset($connector['last_acknowledged_count'])
            ? (int)$connector['last_acknowledged_count'] : 0;
        if ($observedAge === null || $observedAge > $limit) {
            $reasons[] = 'link_observation_missing_or_stale';
        }
        if ($raw < 1 || $acknowledged < 1 || $raw < $acknowledged) {
            $reasons[] = 'link_observation_not_acknowledged';
        }
    }
    return array(
        'ready' => count($reasons) === 0,
        'freshness_limit_minutes' => $limit,
        'success_age_minutes' => $successAge,
        'checked_age_minutes' => $checkedAge,
        'observed_age_minutes' => $observedAge,
        'live_ready' => $liveCursorReady,
        'live_cursor_age_minutes' => $liveCursorAge,
        'reasons' => $reasons,
    );
}

/**
 * Optional JP/GB connectors are deliberately dormant in Production Alpha.
 *
 * Do not hide activity by rewriting it to zero. Both the public source status
 * response and the atomic cutover guard use this predicate so an activated
 * connector, a cursor, an observation, a non-zero receipt, or an active
 * SourceRight fails closed.
 */
function v2_optional_alpha_source_policy_reasons(array $connector): array {
    $reasons = array();
    $connectorStatus = strtolower(trim(
        (string)($connector['connector_status'] ?? '')
    ));
    if (!in_array($connectorStatus, array('pending_rights', 'inactive'), true)) {
        $reasons[] = 'connector_policy_activity';
    }
    foreach (array(
        'last_success_at',
        'last_checked_at',
        'last_observed_at',
    ) as $field) {
        if (
            isset($connector[$field])
            && trim((string)$connector[$field]) !== ''
        ) {
            $reasons[] = 'connector_policy_activity';
            break;
        }
    }
    if (
        isset($connector['cursor_json'])
        && trim((string)$connector['cursor_json']) !== ''
    ) {
        $reasons[] = 'connector_policy_activity';
    }
    if (
        (int)($connector['last_raw_count'] ?? 0) !== 0
        || (int)($connector['last_acknowledged_count'] ?? 0) !== 0
    ) {
        $reasons[] = 'connector_policy_activity';
    }

    $rightId = trim((string)(
        $connector['connector_right_row_id'] ?? ''
    ));
    if ($rightId === '') {
        $reasons[] = 'source_right_missing';
    } elseif (
        !hash_equals(
            (string)($connector['connector_source_right_id'] ?? ''),
            $rightId
        )
        || !hash_equals(
            (string)($connector['connector_source_type'] ?? ''),
            (string)($connector['connector_right_source_type'] ?? '')
        )
        || !hash_equals(
            (string)($connector['connector_source_key'] ?? ''),
            (string)($connector['connector_right_source_key'] ?? '')
        )
    ) {
        $reasons[] = 'source_right_identity_mismatch';
    }
    $rightStatus = strtolower(trim(
        (string)($connector['connector_right_status'] ?? '')
    ));
    if (
        $rightStatus === 'active'
        || (int)($connector['connector_right_collect_eligible'] ?? 0) === 1
    ) {
        $reasons[] = 'source_right_policy_active';
    } elseif (!in_array(
        $rightStatus,
        array('pending', 'expired', 'revoked'),
        true
    )) {
        $reasons[] = 'source_right_policy_invalid';
    }
    return array_values(array_unique($reasons));
}

function v2_source_status_data(PDO $pdo, array $config, string $country = ''): array {
    $where = array('j.record_status=\'active\'');
    $params = array();
    if ($country !== '') {
        $where[] = 'j.country_code=?';
        $params[] = $country;
    }
    $collectRights = v2_current_source_right_sql('sr');
    $identityMatches = v2_connector_source_right_identity_sql('sc', 'sr');
    $publicRights = '(' . source_right_redistribution_sql('sr')
        . ' AND LOWER(COALESCE(sr.source_type,\'\')) NOT LIKE \'%telegram%\')';
    $sql = 'SELECT sc.connector_id,j.country_code AS country,sc.source_name,sc.coverage_mode,'
        . 'sc.country_code AS connector_country_code,sc.source_key AS connector_source_key,'
        . 'sc.source_type AS connector_source_type,'
        . 'sc.source_right_id AS connector_source_right_id,'
        . 'sr.source_right_id AS connector_right_row_id,'
        . 'sr.source_type AS connector_right_source_type,'
        . 'sr.source_key AS connector_right_source_key,'
        . 'sr.status AS connector_right_status,'
        . 'CASE WHEN ' . $collectRights
        . ' THEN 1 ELSE 0 END AS connector_right_collect_eligible,'
        . 'sc.connector_status,'
        . 'CASE WHEN sc.connector_id IS NULL THEN \'inactive\''
        . ' WHEN sc.source_right_id IS NULL OR sr.source_right_id IS NULL'
        . ' THEN \'blocked_rights\''
        . ' WHEN NOT ' . $identityMatches . ' THEN \'blocked_identity\''
        . ' WHEN NOT ' . $collectRights . ' THEN \'blocked_rights\''
        . ' ELSE sc.connector_status END AS collect_status,'
        . 'CASE WHEN sc.connector_id IS NULL THEN \'inactive\''
        . ' WHEN sc.source_right_id IS NULL OR sr.source_right_id IS NULL'
        . ' THEN \'blocked_rights\''
        . ' WHEN NOT ' . $identityMatches . ' THEN \'blocked_identity\''
        . ' WHEN NOT ' . $collectRights . ' THEN \'blocked_rights\''
        . ' WHEN NOT ' . $publicRights
        . ' AND LOWER(COALESCE(sr.source_type,\'\')) LIKE \'%telegram%\''
        . ' THEN \'excluded_source\''
        . ' WHEN NOT ' . $publicRights . ' THEN \'redistribution_blocked\''
        . ' ELSE sc.connector_status END AS public_status,'
        . 'sc.last_success_at,sc.last_checked_at,sc.last_observed_at,sc.schedule_minutes,'
        . 'sc.cursor_json,'
        . 'sc.last_error_class,sc.last_raw_count,sc.last_acknowledged_count,'
        . '(SELECT GROUP_CONCAT(DISTINCT coverage_note.public_note ORDER BY coverage_note.public_note SEPARATOR \'; \')'
        . ' FROM ' . table_name($config, 'source_coverage') . ' coverage_note'
        . ' WHERE coverage_note.connector_id=sc.connector_id'
        . ' AND coverage_note.effective_from<=UTC_TIMESTAMP()'
        . ' AND (coverage_note.effective_until IS NULL OR coverage_note.effective_until>UTC_TIMESTAMP())) AS public_note '
        . 'FROM ' . table_name($config, 'jurisdictions') . ' j '
        . 'LEFT JOIN ' . table_name($config, 'source_connectors') . ' sc ON sc.country_code=j.country_code '
        . 'LEFT JOIN ' . table_name($config, 'source_rights') . ' sr'
        . ' ON sr.source_right_id=sc.source_right_id '
        . 'WHERE ' . implode(' AND ', $where)
        . ' ORDER BY j.launch_order,sc.source_name,sc.connector_id';
    $statement = $pdo->prepare($sql);
    $statement->execute($params);
    $rows = $statement->fetchAll();
    $optionalPolicies = array();
    foreach (v2_optional_alpha_source_identities() as $optionalPolicy) {
        $optionalPolicies[(string)$optionalPolicy['country_code']] = $optionalPolicy;
    }
    foreach ($rows as &$row) {
        $readiness = v2_source_connector_readiness($row);
        $row['lag_minutes'] = $readiness['success_age_minutes'];
        $cadenceMinutes = isset($row['schedule_minutes'])
            ? max(1, (int)$row['schedule_minutes']) : 30;
        $freshnessLimit = (int)$readiness['freshness_limit_minutes'];
        $row['expected_cadence_minutes'] = $cadenceMinutes;
        $row['freshness_limit_minutes'] = $freshnessLimit;
        $row['live_ready'] = $readiness['live_ready'];
        $row['live_cursor_age_minutes'] = $readiness['live_cursor_age_minutes'];
        $row['collect_fresh'] = (
            (string)$row['collect_status'] === 'active'
            && $readiness['ready'] === true
        );
        if (
            (string)$row['public_status'] === 'active'
            && $row['collect_fresh'] !== true
        ) {
            $row['public_status'] = (
                (string)$row['connector_id'] === 'connector:us:sec-edgar'
                && $readiness['live_ready'] !== true
            ) ? 'delayed' : 'stale';
        }
        $row['public_ready'] = (
            (string)$row['public_status'] === 'active'
            && $row['collect_fresh'] === true
        );
        // Compatibility aliases remain collection-only and must not be used
        // as a public publication gate.
        $row['status'] = (string)$row['collect_status'];
        $row['fresh'] = $row['collect_fresh'];
        $row['raw_count'] = isset($row['last_raw_count']) ? (int)$row['last_raw_count'] : 0;
        $row['acknowledged_count'] = isset($row['last_acknowledged_count'])
            ? (int)$row['last_acknowledged_count'] : 0;
        $rowCountry = (string)($row['country'] ?? '');
        if (isset($optionalPolicies[$rowCountry])) {
            $policy = $optionalPolicies[$rowCountry];
            $identityMatchesPolicy = true;
            foreach (array(
                'connector_id' => 'connector_id',
                'country_code' => 'connector_country_code',
                'source_key' => 'connector_source_key',
                'source_type' => 'connector_source_type',
                'source_right_id' => 'connector_source_right_id',
                'coverage_mode' => 'coverage_mode',
            ) as $expectedField => $actualField) {
                if (
                    !isset($row[$actualField])
                    || !hash_equals(
                        (string)$policy[$expectedField],
                        (string)$row[$actualField]
                    )
                ) {
                    $identityMatchesPolicy = false;
                    break;
                }
            }
            $policyReasons = v2_optional_alpha_source_policy_reasons($row);
            $row['coverage_mode'] = 'link-only';
            $row['fresh'] = false;
            $row['collect_fresh'] = false;
            $row['public_ready'] = false;
            $row['live_ready'] = false;
            if (!$identityMatchesPolicy) {
                $row['status'] = 'blocked_identity';
                $row['collect_status'] = 'blocked_identity';
                $row['public_status'] = 'blocked_identity';
                $row['last_error_class'] = 'connector_identity_mismatch';
                $row['public_note'] =
                    'Connector identity mismatch; coverage is blocked.';
            } elseif ($policyReasons) {
                $row['status'] = 'blocked_policy_activity';
                $row['collect_status'] = 'blocked_policy_activity';
                $row['public_status'] = 'blocked_policy_activity';
                $row['last_error_class'] = (string)$policyReasons[0];
                $row['public_note'] =
                    'Unexpected connector or SourceRight activity; '
                    . 'Production Alpha coverage is blocked.';
            } else {
                $row['lag_minutes'] = null;
                $row['last_success_at'] = null;
                $row['last_checked_at'] = null;
                $row['last_observed_at'] = null;
                $row['raw_count'] = 0;
                $row['acknowledged_count'] = 0;
                $row['status'] = 'inactive';
                $row['collect_status'] = 'inactive';
                $row['public_status'] = 'coverage_unavailable';
                $row['last_error_class'] = null;
                $row['public_note'] = (string)$policy['public_note'];
            }
        }
        $row = v2_normalize_public_time_fields(
            $row,
            array('last_success_at', 'last_checked_at', 'last_observed_at')
        );
        unset(
            $row['schedule_minutes'],
            $row['connector_status'],
            $row['last_raw_count'],
            $row['last_acknowledged_count'],
            $row['cursor_json'],
            $row['connector_country_code'],
            $row['connector_source_key'],
            $row['connector_source_type'],
            $row['connector_source_right_id'],
            $row['connector_right_row_id'],
            $row['connector_right_source_type'],
            $row['connector_right_source_key'],
            $row['connector_right_status'],
            $row['connector_right_collect_eligible']
        );
        if ($row['connector_id'] === null) {
            $row['source_name'] = null;
            $row['coverage_mode'] = 'unavailable';
            $row['status'] = 'inactive';
            $row['collect_status'] = 'inactive';
            $row['public_status'] = 'inactive';
            $row['fresh'] = false;
            $row['collect_fresh'] = false;
            $row['public_ready'] = false;
            $row['live_ready'] = false;
        }
    }
    unset($row);
    return $rows;
}

function v2_sources_status(PDO $pdo, array $config): void {
    $country = '';
    if (isset($_GET['country']) && trim((string)$_GET['country']) !== '') {
        $country = (string)v2_valid_country((string)$_GET['country']);
        if ($country === '') {
            v2_respond(400, array('ok' => false, 'error' => 'invalid_country'));
        }
    }
    $rows = v2_source_status_data($pdo, $config, $country);
    $readyByConnector = array();
    foreach ($rows as $row) {
        if (isset($row['connector_id']) && is_string($row['connector_id'])) {
            $readyByConnector[$row['connector_id']] = (
                isset($row['public_ready']) && $row['public_ready'] === true
            );
        }
    }
    $requiredSourceReady = array();
    foreach (v2_required_alpha_source_identities() as $required) {
        if ($country !== '' && (string)$required['country_code'] !== $country) {
            continue;
        }
        $connectorId = (string)$required['connector_id'];
        $requiredSourceReady[$connectorId] = isset($readyByConnector[$connectorId])
            && $readyByConnector[$connectorId] === true;
    }
    $allRequiredReady = count($requiredSourceReady) > 0
        && !in_array(false, $requiredSourceReady, true);
    v2_respond(200, array(
        'ok' => true,
        'data' => array(
            'items' => $rows,
            'checked_at' => gmdate('c'),
            'required_source_ready' => $requiredSourceReady,
            'all_required_ready' => $allRequiredReady,
        ),
        'meta' => array('returned' => count($rows)),
    ));
}

function v2_ops_connector_checkpoint(
    PDO $pdo,
    array $config,
    string $connectorId
): void {
    $statement = $pdo->prepare(
        'SELECT connector_id,cursor_json,last_success_at,last_checked_at,code_revision'
        . ' FROM ' . table_name($config, 'source_connectors')
        . ' WHERE connector_id=? LIMIT 1'
    );
    $statement->execute(array($connectorId));
    $row = $statement->fetch();
    if (!$row) {
        v2_respond(404, array('ok' => false, 'error' => 'connector_not_found'));
    }
    $checkpoint = null;
    if ($row['cursor_json'] !== null && trim((string)$row['cursor_json']) !== '') {
        $decoded = json_decode((string)$row['cursor_json'], true);
        $cursorKeys = is_array($decoded) ? array_keys($decoded) : array();
        $legacyCursorKeys = array(
            'schema_version', 'window_end_exclusive', 'batch_id',
        );
        $liveCursorKeys = array(
            'schema_version', 'window_end_exclusive', 'batch_id',
            'source_cursor',
        );
        sort($cursorKeys);
        sort($legacyCursorKeys);
        sort($liveCursorKeys);
        $isLegacyCursor = $cursorKeys === $legacyCursorKeys
            && isset($decoded['schema_version'])
            && $decoded['schema_version'] === 1;
        $isLiveCursor = $cursorKeys === $liveCursorKeys
            && isset($decoded['schema_version'])
            && $decoded['schema_version'] === 2
            && isset($decoded['source_cursor'])
            && is_string($decoded['source_cursor'])
            && strlen($decoded['source_cursor']) <= 1000
            && strpos($decoded['source_cursor'], 'sec-current-v1:') === 0;
        if (
            is_array($decoded)
            && !v2_write_is_list($decoded)
            && ($isLegacyCursor || $isLiveCursor)
            && isset($decoded['window_end_exclusive'])
            && is_string($decoded['window_end_exclusive'])
            && preg_match(
                '/^\d{4}-\d{2}-\d{2}$/',
                $decoded['window_end_exclusive']
            ) === 1
            && isset($decoded['batch_id'])
            && is_string($decoded['batch_id'])
            && preg_match(
                '/^global-batch:[a-f0-9]{64}$/',
                $decoded['batch_id']
            ) === 1
        ) {
            $checkpoint = array(
                'schema_version' => (int)$decoded['schema_version'],
                'window_end_exclusive' => $decoded['window_end_exclusive'],
                'batch_id' => $decoded['batch_id'],
            );
            if ($isLiveCursor) {
                $checkpoint['source_cursor'] = $decoded['source_cursor'];
            }
        }
        if ($checkpoint === null) {
            v2_respond(503, array(
                'ok' => false,
                'error' => 'invalid_connector_checkpoint',
            ));
        }
    }
    v2_respond(200, array(
        'ok' => true,
        'data' => array(
            'connector_id' => (string)$row['connector_id'],
            'cursor_json' => $checkpoint,
            'last_success_at' => v1_release_iso_time($row['last_success_at']),
            'last_checked_at' => v1_release_iso_time($row['last_checked_at']),
            'code_revision' => $row['code_revision'] === null
                ? null : (string)$row['code_revision'],
        ),
    ));
}

function v2_admin_connectors(
    PDO $pdo,
    array $config,
    ?string $connectorId = null
): void {
    $sql = 'SELECT connector_id,country_code,source_key,source_name,source_type,'
        . 'base_url,source_right_id,coverage_mode,connector_status,'
        . 'schedule_minutes,last_checked_at,last_success_at,last_error_class,'
        . 'code_revision,updated_at FROM '
        . table_name($config, 'source_connectors');
    $params = array();
    if ($connectorId !== null) {
        $sql .= ' WHERE connector_id=?';
        $params[] = $connectorId;
    }
    $sql .= ' ORDER BY country_code,source_key,connector_id';
    $statement = $pdo->prepare($sql);
    $statement->execute($params);
    $connectors = $statement->fetchAll();
    if ($connectorId !== null && count($connectors) === 0) {
        v2_respond(404, array(
            'ok' => false,
            'error' => 'connector_not_found',
        ));
    }
    $items = array();
    foreach ($connectors as $connector) {
        $right = $connector['source_right_id'] === null
            ? null : v2_source_right_row(
                $pdo,
                $config,
                (string)$connector['source_right_id']
            );
        $items[] = v2_admin_connector_view($connector, $right);
    }
    if ($connectorId === null) {
        v2_respond(200, array(
            'ok' => true,
            'data' => array('items' => $items),
            'meta' => array('returned' => count($items)),
        ));
    }
    $audits = $pdo->prepare(
        'SELECT audit_id,previous_status,new_status,reason,changed_by,created_at'
        . ' FROM ' . table_name($config, 'global_connector_audit')
        . ' WHERE connector_id=? ORDER BY created_at DESC,audit_id DESC LIMIT 50'
    );
    $audits->execute(array($connectorId));
    $auditRows = $audits->fetchAll();
    foreach ($auditRows as &$auditRow) {
        $auditRow['created_at'] = v1_release_iso_time($auditRow['created_at']);
    }
    unset($auditRow);
    v2_respond(200, array(
        'ok' => true,
        'data' => array(
            'connector' => $items[0],
            'audit_log' => $auditRows,
        ),
    ));
}

function v2_ops_source_right_eligibility(PDO $pdo, array $config): void {
    $sourceRightId = isset($_GET['source_right_id'])
        ? trim((string)$_GET['source_right_id']) : '';
    $use = isset($_GET['use']) ? trim((string)$_GET['use']) : '';
    if (
        preg_match('/^official:[a-z0-9_.:\-]{1,48}$/', $sourceRightId) !== 1
        || !in_array($use, array('collect', 'public', 'ai'), true)
    ) {
        v2_respond(400, array(
            'ok' => false,
            'error' => 'unsupported_source_right_eligibility_query',
        ));
    }
    $right = v2_source_right_row($pdo, $config, $sourceRightId);
    $reasons = v2_source_right_ineligible_reasons($right, $use);
    $revision = $right === null ? null : v2_source_right_revision($right);
    $contractRevision = $right === null
        ? null : v2_source_right_contract_revision($right);
    $connectorId = null;
    $connectorReady = null;
    if ($sourceRightId === 'official:dart' && $use === 'collect') {
        $connectorId = 'connector:kr:dart';
        $connectorStatement = $pdo->prepare(
            'SELECT connector_id,country_code,source_key,source_type,'
            . 'source_right_id,connector_status FROM '
            . table_name($config, 'source_connectors')
            . ' WHERE connector_id=? LIMIT 1'
        );
        $connectorStatement->execute(array($connectorId));
        $connector = $connectorStatement->fetch();
        $connectorReady = is_array($connector)
            && (string)$connector['connector_id'] === $connectorId
            && (string)$connector['country_code'] === 'KR'
            && (string)$connector['source_key'] === 'dart'
            && (string)$connector['source_type'] === 'official_disclosure'
            && (string)$connector['source_right_id'] === 'official:dart'
            && in_array(
                (string)$connector['connector_status'],
                array('configured', 'active'),
                true
            );
    }
    if ($reasons) {
        $response = array(
            'ok' => false,
            'error' => 'source_right_ineligible',
            'source_right_id' => $sourceRightId,
            'use' => $use,
            'eligible' => false,
            'rights_revision' => $revision,
            'contract_revision' => $contractRevision,
            'reasons' => array_values(array_unique($reasons)),
            'checked_at' => gmdate('c'),
        );
        if ($connectorId !== null) {
            $response['connector_id'] = $connectorId;
            $response['connector_ready'] = $connectorReady === true;
        }
        v2_respond(409, $response);
    }
    $response = array(
        'ok' => true,
        'source_right_id' => $sourceRightId,
        'source_type' => (string)$right['source_type'],
        'source_key' => (string)$right['source_key'],
        'use' => $use,
        'eligible' => true,
        'rights_revision' => $revision,
        'contract_revision' => $contractRevision,
        'redistribution_allowed' => (int)$right['redistribution_allowed'] === 1,
        'ai_allowed' => (int)$right['ai_allowed'] === 1,
        'checked_at' => gmdate('c'),
    );
    if ($connectorId !== null) {
        // This protected response intentionally exposes only a readiness bit,
        // never the raw administrative status or permission/evidence values.
        $response['connector_id'] = $connectorId;
        $response['connector_ready'] = $connectorReady === true;
    }
    v2_respond(200, $response);
}

function v2_brief_event_rows(PDO $pdo, array $config, string $briefId, string $lane): array {
    $sql = 'SELECT brief_item.event_id,brief_item.event_snapshot_json,'
        . 'brief_item.selection_reason,'
        . '(SELECT current_url_d.original_url FROM '
        . table_name($config, 'event_documents') . ' current_url_ed'
        . ' JOIN ' . table_name($config, 'documents') . ' current_url_d'
        . ' ON current_url_d.document_id=current_url_ed.document_id'
        . ' LEFT JOIN ' . table_name($config, 'source_rights') . ' current_url_sr'
        . ' ON current_url_sr.source_right_id=current_url_d.source_right_id'
        . ' WHERE current_url_ed.event_id=e.event_id AND '
        . v2_document_visibility_sql('current_url_d', 'current_url_sr')
        . ' ORDER BY current_url_ed.position_no,current_url_d.document_id LIMIT 1)'
        . ' AS current_source_url FROM '
        . table_name($config, 'brief_items') . ' brief_item'
        . ' JOIN ' . table_name($config, 'governance_events') . ' e'
        . ' ON e.event_id=brief_item.event_id'
        . ' WHERE brief_item.brief_id=? AND brief_item.lane=?'
        . ' AND brief_item.review_status=\'approved\' AND '
        . v2_event_visibility_sql($config, 'e')
        . ' ORDER BY brief_item.position_no,brief_item.event_id';
    $statement = $pdo->prepare($sql);
    $statement->execute(array($briefId, $lane));
    $items = array();
    foreach ($statement->fetchAll() as $row) {
        $snapshot = json_decode((string)$row['event_snapshot_json'], true);
        if (
            !is_array($snapshot)
            || !isset($snapshot['event_id'])
            || (string)$snapshot['event_id'] !== (string)$row['event_id']
            || !isset($snapshot['title_provenance'])
            || !in_array(
                (string)$snapshot['title_provenance'],
                array('source', 'generated_metadata', 'operator_metadata'),
                true
            )
        ) {
            continue;
        }
        // URLs are never trusted from the frozen snapshot. Reconstruct the
        // representative URL from a currently eligible, non-Telegram
        // document so revocation takes effect immediately.
        unset($snapshot['source_url']);
        $snapshot['source_url'] = $row['current_source_url'] === null
            ? null : (string)$row['current_source_url'];
        $snapshot = v2_normalize_event_rows(array($snapshot))[0];
        $snapshot['selection_reason'] = (string)$row['selection_reason'];
        $items[] = $snapshot;
    }
    return $items;
}

function v2_latest_brief(PDO $pdo, array $config): void {
    $requested = isset($_GET['edition']) ? trim((string)$_GET['edition']) : 'global';
    $edition = strtolower($requested) === 'global' ? 'global' : v2_valid_country($requested);
    if ($edition === null) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_edition'));
    }
    $statusCountry = $edition === 'global' ? '' : (string)$edition;
    $sourceStatus = v2_source_status_data($pdo, $config, $statusCountry);
    $statement = $pdo->prepare(
        'SELECT brief_id,edition,cutoff_at,published_at,updated_at,build_sha,'
        . 'payload_json FROM '
        . table_name($config, 'brief_editions')
        . ' WHERE edition=? AND publication_status=\'published\' AND published_at IS NOT NULL'
        . ' ORDER BY cutoff_at DESC,brief_id DESC LIMIT 1'
    );
    $statement->execute(array($edition));
    $brief = $statement->fetch();
    if (!$brief) {
        v2_respond(200, array(
            'ok' => true,
            'data' => array(
                'schema_version' => 1,
                'brief_id' => null,
                'edition' => $edition,
                'cutoff_at' => null,
                'published_at' => null,
                'last_updated_at' => null,
                'build_sha' => null,
                'stale' => false,
                'coverage_notice' => null,
                'top' => array(),
                'watch' => array(),
                'deadlines' => array(),
                'source_status' => $sourceStatus,
                'empty_reason' => 'no_approved_brief',
            ),
        ));
    }
    $briefId = (string)$brief['brief_id'];
    $briefPayload = json_decode((string)$brief['payload_json'], true);
    $emptyReason = is_array($briefPayload)
        && isset($briefPayload['empty_reason'])
        && in_array(
            (string)$briefPayload['empty_reason'],
            array('no_confirmed_material_events', 'coverage_unavailable'),
            true
        )
        ? (string)$briefPayload['empty_reason'] : null;
    $top = $emptyReason === 'coverage_unavailable'
        ? array() : v2_brief_event_rows($pdo, $config, $briefId, 'top');
    // A current brief whose frozen Top rows are no longer public (for example,
    // after a SourceRight revocation) must not silently fall back to an older
    // edition. Treat the current edition as unavailable until it is republished.
    if ($emptyReason === null && count($top) === 0) {
        $emptyReason = 'coverage_unavailable';
    }
    $readySourceCount = 0;
    $unavailableCountries = array();
    $unavailableSources = array();
    foreach ($sourceStatus as $sourceRow) {
        if (
            isset($sourceRow['public_ready'])
            && $sourceRow['public_ready'] === true
        ) {
            $readySourceCount++;
            continue;
        }
        $country = strtoupper(trim((string)($sourceRow['country'] ?? '')));
        if ($country !== '') {
            $unavailableCountries[$country] = true;
        }
        $sourceName = trim((string)($sourceRow['source_name'] ?? ''));
        $connectorId = trim((string)($sourceRow['connector_id'] ?? ''));
        $sourceKey = $sourceName !== '' ? $sourceName
            : ($connectorId !== '' ? $connectorId : $country);
        if ($sourceKey !== '') {
            $unavailableSources[$sourceKey] = true;
        }
    }
    $coverageNotice = null;
    if (
        $emptyReason === 'coverage_unavailable'
        || count($unavailableSources) > 0
    ) {
        $blockingCoverage = $emptyReason === 'coverage_unavailable'
            || $readySourceCount === 0;
        $coverageNotice = array(
            'reason' => $blockingCoverage
                ? 'coverage_unavailable' : 'partial_coverage',
            'scope' => $blockingCoverage ? 'blocking' : 'warning',
            'brief_id' => $briefId,
            'cutoff_at' => v2_public_iso_time($brief['cutoff_at']),
            'published_at' => v2_public_iso_time($brief['published_at']),
            'unavailable_countries' => array_keys($unavailableCountries),
            'unavailable_sources' => array_keys($unavailableSources),
        );
    }
    $cutoffTimestamp = strtotime((string)$brief['cutoff_at'] . ' UTC');
    v2_respond(200, array(
        'ok' => true,
        'data' => array(
            'schema_version' => 1,
            'brief_id' => $briefId,
            'edition' => (string)$brief['edition'],
            'cutoff_at' => v2_public_iso_time($brief['cutoff_at']),
            'published_at' => v2_public_iso_time($brief['published_at']),
            'last_updated_at' => v2_public_iso_time($brief['updated_at']),
            'build_sha' => (string)$brief['build_sha'],
            'stale' => $cutoffTimestamp === false
                ? true : $cutoffTimestamp < (time() - 129600),
            'coverage_notice' => $coverageNotice,
            'top' => $top,
            'watch' => v2_brief_event_rows($pdo, $config, $briefId, 'watch'),
            'deadlines' => v2_brief_event_rows($pdo, $config, $briefId, 'deadline'),
            'source_status' => $sourceStatus,
            'empty_reason' => $emptyReason,
        ),
    ));
}

function v2_calendar(PDO $pdo, array $config): void {
    $original = $_GET;
    $page = v2_list_params();
    list($where, $params) = v2_event_filters($config);
    $where[] = 'e.deadline_at IS NOT NULL';
    $sql = v2_event_select($config) . ' WHERE ' . implode(' AND ', $where)
        . ' ORDER BY e.deadline_at,e.event_id LIMIT ' . ((int)$page['limit'] + 1)
        . ' OFFSET ' . (int)$page['offset'];
    $statement = $pdo->prepare($sql);
    $statement->execute($params);
    list($rows, $hasMore) = v2_fetch_page($statement, $page);
    $_GET = $original;
    $rows = v2_normalize_event_rows($rows);
    list($rows, $meta) = v2_fit_json_list_page($rows, $page, $hasMore);
    v2_respond(200, array(
        'ok' => true,
        'data' => array('items' => $rows),
        'meta' => $meta,
    ));
}

function v2_search(PDO $pdo, array $config): void {
    $page = v2_list_params();
    list($events, $hasMore) = v2_query_events(
        $pdo,
        $config,
        $page,
        'e.updated_at DESC,e.event_id DESC',
        true
    );
    list($events, $meta) = v2_fit_json_list_page($events, $page, $hasMore);
    v2_respond(200, array(
        'ok' => true,
        'data' => array('items' => $events),
        'meta' => $meta,
    ));
}

function v2_export_event_rows(PDO $pdo, array $config): array {
    $page = v2_list_params();
    list($rows, $hasMore) = v2_query_events(
        $pdo,
        $config,
        $page,
        'e.occurred_at DESC,e.event_id DESC'
    );
    return array($rows, $hasMore, $page);
}

function v2_export_query_params(array $page, ?int $nextOffset = null): array {
    $params = array();
    foreach (
        array(
            'country',
            'market',
            'issuer_id',
            'event_family',
            'verification_status',
            'change_type',
            'from',
            'to',
        ) as $name
    ) {
        if (
            isset($_GET[$name])
            && !is_array($_GET[$name])
            && trim((string)$_GET[$name]) !== ''
        ) {
            $params[$name] = trim((string)$_GET[$name]);
        }
    }
    $params['limit'] = (int)$page['limit'];
    if ($nextOffset !== null) {
        $params['offset'] = $nextOffset;
    } elseif ($page['page'] !== null && (int)$page['page'] > 1) {
        $params['page'] = (int)$page['page'];
    } elseif ((int)$page['offset'] > 0) {
        $params['offset'] = (int)$page['offset'];
    }
    return $params;
}

function v2_export_url(string $path, array $page, ?int $nextOffset = null): string {
    $base = 'https://alignpe.gabia.io/activist/api.php/api/v2' . $path;
    $query = http_build_query(
        v2_export_query_params($page, $nextOffset),
        '',
        '&',
        PHP_QUERY_RFC3986
    );
    return $query === '' ? $base : $base . '?' . $query;
}

function v2_export_continuation_headers(
    string $path,
    array $page,
    array $meta
): void {
    header('X-BSIDE-Offset: ' . (int)$meta['offset']);
    header('X-BSIDE-Returned: ' . (int)$meta['returned']);
    header('X-BSIDE-Has-More: ' . ($meta['has_more'] ? 'true' : 'false'));
    if ($meta['next_offset'] !== null) {
        header('X-BSIDE-Next-Offset: ' . (int)$meta['next_offset']);
        header(
            'Link: <'
            . v2_export_url($path, $page, (int)$meta['next_offset'])
            . '>; rel="next"'
        );
    }
}

function v2_export_events_json(PDO $pdo, array $config): void {
    list($rows, $hasMore, $page) = v2_export_event_rows($pdo, $config);
    $extraMeta = array(
        'maximum' => V2_MAX_PAGE_SIZE,
        'generated_at' => gmdate('c'),
    );
    list($rows, $meta) = v2_fit_json_list_page(
        $rows,
        $page,
        $hasMore,
        $extraMeta
    );
    v2_respond(200, array(
        'ok' => true,
        'data' => array('items' => $rows),
        'meta' => $meta,
    ));
}

function v2_csv_safe_cell($value): string {
    if ($value === null) {
        return '';
    }
    $text = (string)$value;
    if (preg_match('/^[\x09\x0A\x0D\x20]*[=+\-@]/', $text) === 1) {
        return '\'' . $text;
    }
    return $text;
}

function v2_export_events_csv(PDO $pdo, array $config): void {
    list($rows, $sourceHasMore, $page) = v2_export_event_rows($pdo, $config);
    $stream = fopen('php://temp', 'w+');
    if ($stream === false) {
        v2_respond(500, array('ok' => false, 'error' => 'csv_stream_failed'));
    }
    fwrite($stream, "\xEF\xBB\xBF");
    $columns = array(
        'event_id',
        'issuer_id',
        'issuer_name',
        'ticker',
        'market',
        'country',
        'event_family',
        'importance',
        'verification_status',
        'change_type',
        'title',
        'title_provenance',
        'original_language',
        'occurred_at',
        'filed_at',
        'first_observed_at',
        'updated_at',
        'deadline_at',
        'official_evidence_count',
        'media_count',
        'coverage_mode',
        'source_url',
    );
    fputcsv($stream, $columns);
    $returned = 0;
    foreach ($rows as $row) {
        $position = ftell($stream);
        $values = array();
        foreach ($columns as $column) {
            $values[] = v2_csv_safe_cell(
                isset($row[$column]) ? $row[$column] : ''
            );
        }
        fputcsv($stream, $values);
        $nextPosition = ftell($stream);
        if ($nextPosition === false || $nextPosition > V2_RESPONSE_BUDGET_BYTES) {
            ftruncate($stream, (int)$position);
            fseek($stream, (int)$position);
            break;
        }
        $returned++;
    }
    if ($returned === 0 && count($rows) > 0) {
        fclose($stream);
        v2_respond(422, array(
            'ok' => false,
            'error' => 'response_item_exceeds_budget',
            'max_bytes' => V2_RESPONSE_BUDGET_BYTES,
        ));
    }
    $hasMore = $sourceHasMore || $returned < count($rows);
    $meta = v2_page_meta($page, $returned, $hasMore);
    rewind($stream);
    $csv = stream_get_contents($stream);
    fclose($stream);
    if (!is_string($csv) || strlen($csv) > V2_RESPONSE_BUDGET_BYTES) {
        v2_respond(500, array(
            'ok' => false,
            'error' => 'response_budget_exceeded',
            'max_bytes' => V2_RESPONSE_BUDGET_BYTES,
        ));
    }
    header('Content-Type: text/csv; charset=utf-8');
    header('Content-Disposition: attachment; filename="bside-global-events.csv"');
    header('X-BSIDE-API-Version: v2');
    header('X-Response-Bytes: ' . strlen($csv));
    v2_export_continuation_headers('/exports/events.csv', $page, $meta);
    echo $csv;
    exit;
}

function v2_atom_escape($value): string {
    return htmlspecialchars((string)$value, ENT_XML1 | ENT_QUOTES, 'UTF-8');
}

function v2_atom_entry(array $row, string $fallbackUpdated): string {
    $eventId = (string)$row['event_id'];
    $sourceUrl = isset($row['source_url']) ? (string)$row['source_url'] : '';
    $eventUpdated = !empty($row['updated_at'])
        ? gmdate('c', strtotime((string)$row['updated_at']))
        : $fallbackUpdated;
    $xml = '<entry><id>urn:bside:event:' . v2_atom_escape($eventId) . '</id>'
        . '<title>' . v2_atom_escape($row['title']) . '</title>'
        . '<updated>' . v2_atom_escape($eventUpdated) . '</updated>'
        . '<category term="' . v2_atom_escape($row['event_family']) . '"/>'
        . '<summary>' . v2_atom_escape($row['change_summary']) . '</summary>';
    if ($sourceUrl !== '') {
        $xml .= '<link href="' . v2_atom_escape($sourceUrl) . '"/>';
    }
    return $xml . '</entry>';
}

function v2_events_atom(PDO $pdo, array $config): void {
    list($rows, $sourceHasMore, $page) = v2_export_event_rows($pdo, $config);
    $updated = count($rows) > 0 && !empty($rows[0]['updated_at'])
        ? gmdate('c', strtotime((string)$rows[0]['updated_at']))
        : gmdate('c');
    $self = v2_export_url('/feeds/events.atom', $page);
    $prefix = '<?xml version="1.0" encoding="UTF-8"?>'
        . '<feed xmlns="http://www.w3.org/2005/Atom">'
        . '<id>https://news.bside.ai/</id>'
        . '<title>BSIDE Global Governance Events</title>'
        . '<updated>' . v2_atom_escape($updated) . '</updated>'
        . '<link rel="self" href="' . v2_atom_escape($self) . '"/>';
    $entries = '';
    $returned = 0;
    foreach ($rows as $row) {
        $candidateReturned = $returned + 1;
        $candidateHasMore = $sourceHasMore || $candidateReturned < count($rows);
        $candidateMeta = v2_page_meta($page, $candidateReturned, $candidateHasMore);
        $nextLink = $candidateMeta['next_offset'] === null
            ? ''
            : '<link rel="next" href="'
                . v2_atom_escape(
                    v2_export_url(
                        '/feeds/events.atom',
                        $page,
                        (int)$candidateMeta['next_offset']
                    )
                )
                . '"/>';
        $candidateEntries = $entries . v2_atom_entry($row, $updated);
        if (
            strlen($prefix . $candidateEntries . $nextLink . '</feed>')
            > V2_RESPONSE_BUDGET_BYTES
        ) {
            break;
        }
        $entries = $candidateEntries;
        $returned = $candidateReturned;
    }
    if ($returned === 0 && count($rows) > 0) {
        v2_respond(422, array(
            'ok' => false,
            'error' => 'response_item_exceeds_budget',
            'max_bytes' => V2_RESPONSE_BUDGET_BYTES,
        ));
    }
    $hasMore = $sourceHasMore || $returned < count($rows);
    $meta = v2_page_meta($page, $returned, $hasMore);
    $nextLink = $meta['next_offset'] === null
        ? ''
        : '<link rel="next" href="'
            . v2_atom_escape(
                v2_export_url(
                    '/feeds/events.atom',
                    $page,
                    (int)$meta['next_offset']
                )
            )
            . '"/>';
    $xml = $prefix . $entries . $nextLink . '</feed>';
    header('Content-Type: application/atom+xml; charset=utf-8');
    header('X-BSIDE-API-Version: v2');
    header('X-Response-Bytes: ' . strlen($xml));
    v2_export_continuation_headers('/feeds/events.atom', $page, $meta);
    echo $xml;
    exit;
}

function v2_admin_release_state(PDO $pdo, array $config): void {
    $row = v2_release_state($pdo, $config);
    if ($row === null) {
        v2_respond(503, array('ok' => false, 'error' => 'release_state_unavailable'));
    }
    $row = v2_normalize_public_time_fields(
        $row,
        array('cutover_at', 'sunset_at', 'updated_at')
    );
    $history = $pdo->prepare(
        'SELECT audit_id,state_version,previous_state,new_state,changed_by,change_reason,'
        . 'request_id,release_authorization_id,cutover_at,sunset_at,created_at FROM '
        . table_name($config, 'governance_release_audit')
        . ' WHERE state_key=? ORDER BY state_version DESC LIMIT 50'
    );
    $history->execute(array(GOV_V2_RELEASE_STATE_KEY));
    $historyRows = $history->fetchAll();
    foreach ($historyRows as &$historyRow) {
        $historyRow['state_version'] = (int)$historyRow['state_version'];
        $historyRow = v2_normalize_public_time_fields(
            $historyRow,
            array('cutover_at', 'sunset_at', 'created_at')
        );
    }
    unset($historyRow);
    v2_respond(200, array(
        'ok' => true,
        'data' => array(
            'release_state' => (string)$row['release_state'],
            'state_version' => (int)$row['state_version'],
            'updated_by' => (string)$row['updated_by'],
            'update_reason' => (string)$row['update_reason'],
            'cutover_at' => $row['cutover_at'],
            'sunset_at' => $row['sunset_at'],
            'updated_at' => $row['updated_at'],
            'preview_auth_configured' => v1_preview_auth_configured($config),
            'history' => $historyRows,
        ),
    ));
}

function v2_current_public_document_rights_guard(
    PDO $pdo,
    array $config
): array {
    $validRight = '(' . source_right_redistribution_sql('sr')
        . ' AND ' . v2_document_source_right_identity_sql('d', 'sr') . ')';
    $statement = $pdo->query(
        'SELECT COUNT(DISTINCT d.document_id) AS total_count,'
        . 'COUNT(DISTINCT CASE WHEN NOT ('
        . $validRight
        . ') THEN d.document_id END) AS invalid_count '
        . 'FROM ' . table_name($config, 'governance_events') . ' e '
        . 'JOIN ' . table_name($config, 'event_documents')
        . ' ed ON ed.event_id=e.event_id '
        . 'JOIN ' . table_name($config, 'documents')
        . ' d ON d.document_id=ed.document_id '
        . 'LEFT JOIN ' . table_name($config, 'source_rights')
        . ' sr ON sr.source_right_id=d.source_right_id '
        . 'WHERE e.issuer_id IS NOT NULL '
        . 'AND e.review_status IN (\'approved\',\'not_required\') '
        . 'AND e.publication_status=\'published\' '
        . 'AND d.publication_status=\'published\' '
        . 'AND ' . v2_non_telegram_document_sql('d')
    );
    $row = $statement->fetch();
    return array(
        'checked_at' => gmdate('c'),
        'total_count' => $row ? (int)$row['total_count'] : 0,
        'invalid_count' => $row ? (int)$row['invalid_count'] : 0,
    );
}

function v2_lock_current_public_source_rights(
    PDO $pdo,
    array $config
): void {
    $statement = $pdo->query(
        'SELECT sr.source_right_id FROM '
        . table_name($config, 'source_rights') . ' sr '
        . 'WHERE sr.source_right_id IN (SELECT DISTINCT d.source_right_id FROM '
        . table_name($config, 'governance_events') . ' e '
        . 'JOIN ' . table_name($config, 'event_documents')
        . ' ed ON ed.event_id=e.event_id '
        . 'JOIN ' . table_name($config, 'documents')
        . ' d ON d.document_id=ed.document_id '
        . 'WHERE e.issuer_id IS NOT NULL '
        . 'AND e.review_status IN (\'approved\',\'not_required\') '
        . 'AND e.publication_status=\'published\' '
        . 'AND d.publication_status=\'published\' '
        . 'AND d.source_right_id IS NOT NULL '
        . 'AND ' . v2_non_telegram_document_sql('d')
        . ') ORDER BY BINARY sr.source_right_id FOR UPDATE'
    );
    $statement->fetchAll();
}

/**
 * Exact connector identities required for Production Alpha cutover.
 *
 * This registry is deliberately server-side and independent of published
 * documents. A country with zero current documents must not escape the
 * cutover-time SourceRight check.
 */
function v2_required_alpha_source_identities(): array {
    return array(
        array(
            'connector_id' => 'connector:kr:dart',
            'country_code' => 'KR',
            'source_key' => 'dart',
            'source_type' => 'official_disclosure',
            'source_right_id' => 'official:dart',
            'coverage_mode' => 'market-wide',
        ),
        array(
            'connector_id' => 'connector:us:sec-edgar',
            'country_code' => 'US',
            'source_key' => 'sec-edgar',
            'source_type' => 'official_disclosure',
            'source_right_id' => 'official:sec-edgar',
            'coverage_mode' => 'market-wide',
        ),
        array(
            'connector_id' => 'connector:ca:issuer-ir',
            'country_code' => 'CA',
            'source_key' => 'issuer-ir',
            'source_type' => 'official_issuer',
            'source_right_id' => 'official:ca-issuer-ir',
            'coverage_mode' => 'link-only',
        ),
        array(
            'connector_id' => 'connector:au:asic-register',
            'country_code' => 'AU',
            'source_key' => 'asic-register',
            'source_type' => 'official_register',
            'source_right_id' => 'official:asic-register',
            'coverage_mode' => 'link-only',
        ),
    );
}

/**
 * Optional country rows stay visible, but are never considered public-ready.
 *
 * The stored identities are retained for a future authenticated connector.
 * Production Alpha does not call EDINET or Companies House without their
 * official credentials and does not fall back to HTML scraping.
 */
function v2_optional_alpha_source_identities(): array {
    return array(
        array(
            'connector_id' => 'connector:jp:edinet',
            'country_code' => 'JP',
            'source_key' => 'edinet',
            'source_type' => 'official_disclosure',
            'source_right_id' => 'official:edinet',
            'coverage_mode' => 'market-wide',
            'public_note' =>
                'Production Alpha: link-only coverage unavailable; '
                . 'EDINET API credentials are not configured and HTML scraping is disabled.',
        ),
        array(
            'connector_id' => 'connector:gb:companies-house',
            'country_code' => 'GB',
            'source_key' => 'companies-house',
            'source_type' => 'official_register',
            'source_right_id' => 'official:companies-house',
            'coverage_mode' => 'official-register',
            'public_note' =>
                'Production Alpha: link-only coverage unavailable; '
                . 'Companies House API credentials are not configured and HTML scraping is disabled.',
        ),
    );
}

/**
 * Optional rows are not readiness gates, but their stored identities must not
 * drift. This keeps a corrupted connector from masquerading as an intentionally
 * unavailable country during cutover.
 */
function v2_optional_alpha_source_identity_guard(
    PDO $pdo,
    array $config
): array {
    $expectedRows = v2_optional_alpha_source_identities();
    $connectorIds = array();
    $sourceRightIds = array();
    foreach ($expectedRows as $expected) {
        $connectorIds[] = (string)$expected['connector_id'];
        $sourceRightIds[] = (string)$expected['source_right_id'];
    }
    $sourceRightIds = array_values(array_unique($sourceRightIds));
    sort($connectorIds, SORT_STRING);
    sort($sourceRightIds, SORT_STRING);

    // Global ingest and connector administration both lock SourceRight before
    // connector. Cutover must use the same order for dormant identities too.
    $rightPlaceholders = implode(',', array_fill(0, count($sourceRightIds), '?'));
    $rightStatement = $pdo->prepare(
        'SELECT sr.source_right_id,sr.source_type,sr.source_key,sr.status,'
        . 'CASE WHEN ' . v2_current_source_right_sql('sr')
        . ' THEN 1 ELSE 0 END AS collect_eligible FROM '
        . table_name($config, 'source_rights') . ' sr'
        . ' WHERE sr.source_right_id IN (' . $rightPlaceholders . ')'
        . ' ORDER BY BINARY sr.source_right_id FOR UPDATE'
    );
    $rightStatement->execute($sourceRightIds);
    $rights = array();
    while ($right = $rightStatement->fetch()) {
        $rights[(string)$right['source_right_id']] = $right;
    }

    $connectorPlaceholders = implode(',', array_fill(0, count($connectorIds), '?'));
    $connectorStatement = $pdo->prepare(
        'SELECT sc.connector_id,sc.country_code,sc.source_key,sc.source_type,'
        . 'sc.source_right_id,sc.coverage_mode,sc.connector_status,'
        . 'sc.last_success_at,sc.last_checked_at,sc.last_observed_at,'
        . 'sc.cursor_json,sc.last_raw_count,sc.last_acknowledged_count,'
        . 'sc.country_code AS connector_country_code,'
        . 'sc.source_key AS connector_source_key,'
        . 'sc.source_type AS connector_source_type,'
        . 'sc.source_right_id AS connector_source_right_id '
        . 'FROM ' . table_name($config, 'source_connectors') . ' sc '
        . 'WHERE sc.connector_id IN (' . $connectorPlaceholders . ')'
        . ' ORDER BY BINARY sc.connector_id FOR UPDATE'
    );
    $connectorStatement->execute($connectorIds);
    $connectors = array();
    while ($connector = $connectorStatement->fetch()) {
        $rightId = (string)$connector['source_right_id'];
        $right = isset($rights[$rightId]) ? $rights[$rightId] : null;
        $connector['connector_right_row_id'] = $right === null
            ? null : (string)$right['source_right_id'];
        $connector['connector_right_source_type'] = $right === null
            ? null : (string)$right['source_type'];
        $connector['connector_right_source_key'] = $right === null
            ? null : (string)$right['source_key'];
        $connector['connector_right_status'] = $right === null
            ? null : (string)$right['status'];
        $connector['connector_right_collect_eligible'] = $right === null
            ? 0 : (int)$right['collect_eligible'];
        $connectors[(string)$connector['connector_id']] = $connector;
    }
    $invalid = array();
    foreach ($expectedRows as $expected) {
        $connectorId = (string)$expected['connector_id'];
        $connector = isset($connectors[$connectorId])
            ? $connectors[$connectorId] : null;
        $reasons = array();
        if ($connector === null) {
            $reasons[] = 'connector_missing';
        } else {
            foreach (array(
                'country_code',
                'source_key',
                'source_type',
                'source_right_id',
                'coverage_mode',
            ) as $field) {
                if (
                    !isset($connector[$field])
                    || !hash_equals(
                        (string)$expected[$field],
                        (string)$connector[$field]
                    )
                ) {
                    $reasons[] = 'connector_identity_mismatch';
                    break;
                }
            }
            $reasons = array_merge(
                $reasons,
                v2_optional_alpha_source_policy_reasons($connector)
            );
        }
        if ($reasons) {
            $invalid[] = array(
                'connector_id' => $connectorId,
                'reasons' => array_values(array_unique($reasons)),
            );
        }
    }
    return array(
        'checked_count' => count($expectedRows),
        'invalid_count' => count($invalid),
        'invalid_sources' => $invalid,
    );
}

/**
 * Lock and validate every Production Alpha connector and grant.
 *
 * Every SourceRight row is locked first, followed by every connector row; both
 * sets use deterministic binary order. This matches global ingest and admin
 * connector updates. Every connector must have a recent successful check and
 * be active. Link-only sources must additionally prove a recent acknowledged
 * approved-link observation even when no event is public.
 */
function v2_required_alpha_source_rights_guard(
    PDO $pdo,
    array $config
): array {
    $expectedRows = v2_required_alpha_source_identities();
    $connectorIds = array();
    $sourceRightIds = array();
    foreach ($expectedRows as $expected) {
        $connectorIds[] = (string)$expected['connector_id'];
        $sourceRightIds[] = (string)$expected['source_right_id'];
    }
    $sourceRightIds = array_values(array_unique($sourceRightIds));
    sort($connectorIds, SORT_STRING);
    sort($sourceRightIds, SORT_STRING);

    $rightPlaceholders = implode(',', array_fill(0, count($sourceRightIds), '?'));
    $rightStatement = $pdo->prepare(
        'SELECT sr.source_right_id,sr.source_type,sr.source_key,sr.source_name,'
        . 'sr.permission_scope,sr.evidence_uri,sr.evidence_hash,sr.valid_from,'
        . 'sr.valid_until,sr.revoked_at,sr.ai_allowed,sr.redistribution_allowed,'
        . 'sr.status,sr.updated_at,CASE WHEN '
        . v2_current_source_right_sql('sr')
        . ' THEN 1 ELSE 0 END AS collect_eligible,CASE WHEN '
        . source_right_redistribution_sql('sr')
        . ' THEN 1 ELSE 0 END AS public_eligible FROM '
        . table_name($config, 'source_rights') . ' sr'
        . ' WHERE sr.source_right_id IN (' . $rightPlaceholders . ')'
        . ' ORDER BY BINARY sr.source_right_id FOR UPDATE'
    );
    $rightStatement->execute($sourceRightIds);
    $rights = array();
    while ($right = $rightStatement->fetch()) {
        $rights[(string)$right['source_right_id']] = $right;
    }

    $connectorPlaceholders = implode(',', array_fill(0, count($connectorIds), '?'));
    $connectorStatement = $pdo->prepare(
        'SELECT connector_id,country_code,source_key,source_type,source_right_id,'
        . 'coverage_mode,connector_status,schedule_minutes,last_checked_at,'
        . 'last_success_at,last_observed_at,last_raw_count,last_acknowledged_count,'
        . 'last_error_class,cursor_json FROM '
        . table_name($config, 'source_connectors')
        . ' WHERE connector_id IN (' . $connectorPlaceholders . ')'
        . ' ORDER BY BINARY connector_id FOR UPDATE'
    );
    $connectorStatement->execute($connectorIds);
    $connectors = array();
    while ($connector = $connectorStatement->fetch()) {
        $connectors[(string)$connector['connector_id']] = $connector;
    }

    $invalid = array();
    $identityFields = array(
        'country_code',
        'source_key',
        'source_type',
        'source_right_id',
        'coverage_mode',
    );
    foreach ($expectedRows as $expected) {
        $connectorId = (string)$expected['connector_id'];
        $sourceRightId = (string)$expected['source_right_id'];
        $connector = isset($connectors[$connectorId])
            ? $connectors[$connectorId] : null;
        $right = isset($rights[$sourceRightId])
            ? $rights[$sourceRightId] : null;
        $reasons = array();
        if ($connector === null) {
            $reasons[] = 'connector_missing';
        } else {
            foreach ($identityFields as $field) {
                if (
                    !isset($connector[$field])
                    || !hash_equals(
                        (string)$expected[$field],
                        (string)$connector[$field]
                    )
                ) {
                    $reasons[] = 'connector_identity_mismatch';
                    break;
                }
            }
            $readiness = v2_source_connector_readiness($connector);
            foreach ($readiness['reasons'] as $readinessReason) {
                $reasons[] = (string)$readinessReason;
            }
        }
        if ($right === null) {
            $reasons[] = 'source_right_missing';
        } else {
            if (
                !hash_equals($sourceRightId, (string)$right['source_right_id'])
                || !hash_equals(
                    (string)$expected['source_type'],
                    (string)$right['source_type']
                )
                || !hash_equals(
                    (string)$expected['source_key'],
                    (string)$right['source_key']
                )
            ) {
                $reasons[] = 'source_right_identity_mismatch';
            }
            if ((int)$right['collect_eligible'] !== 1) {
                $reasons[] = 'collect_not_allowed';
            }
            if ((int)$right['public_eligible'] !== 1) {
                $reasons[] = 'public_redistribution_not_allowed';
            }
        }
        $reasons = array_values(array_unique($reasons));
        if (count($reasons) > 0) {
            $invalid[] = array(
                'connector_id' => $connectorId,
                'reasons' => $reasons,
            );
        }
    }
    return array(
        'checked_count' => count($expectedRows),
        'invalid_count' => count($invalid),
        'invalid_sources' => $invalid,
    );
}

function v2_json_body(array $config): array {
    $contentType = isset($_SERVER['CONTENT_TYPE']) ? strtolower((string)$_SERVER['CONTENT_TYPE']) : '';
    if (strpos($contentType, 'application/json') !== 0) {
        v2_respond(415, array('ok' => false, 'error' => 'application_json_required'));
    }
    $decoded = json_decode(read_body($config), true);
    if (!is_array($decoded) || json_last_error() !== JSON_ERROR_NONE) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_json_object'));
    }
    return $decoded;
}

function v2_release_state_rows_for_update(PDO $pdo, array $config): array {
    $statement = $pdo->prepare(
        'SELECT state_key,release_state,state_version,cutover_at,sunset_at FROM '
        . table_name($config, 'governance_release_state')
        . ' WHERE state_key IN (?,?) ORDER BY BINARY state_key FOR UPDATE'
    );
    $rows = array();
    foreach (v1_pdo_fetch_all_and_close(
        $statement,
        array(GOV_V1_RELEASE_STATE_KEY, GOV_V2_RELEASE_STATE_KEY)
    ) as $row) {
        $rows[(string)$row['state_key']] = $row;
    }
    if (
        !isset($rows[GOV_V1_RELEASE_STATE_KEY])
        || !isset($rows[GOV_V2_RELEASE_STATE_KEY])
        || count($rows) !== 2
    ) {
        throw new RuntimeException('release_state_unavailable');
    }
    return $rows;
}

function v2_release_authorization_fields(array $payload): array {
    try {
        v2_write_assert_keys(
            $payload,
            array(
                'candidate_sha',
                'evidence_artifact_digest',
                'evidence_run_id',
                'evidence_artifact_id',
                'release_nonce',
                'expected_v1_state_version',
                'expected_v2_state_version',
                'expires_at',
                'reason',
            ),
            'release-authorization'
        );
    } catch (InvalidArgumentException $error) {
        v2_respond(400, array(
            'ok' => false,
            'error' => 'unknown_release_authorization_field',
        ));
    }
    $candidateSha = isset($payload['candidate_sha'])
        ? strtolower(trim((string)$payload['candidate_sha'])) : '';
    $digest = isset($payload['evidence_artifact_digest'])
        ? strtolower(trim((string)$payload['evidence_artifact_digest'])) : '';
    $nonce = isset($payload['release_nonce'])
        ? strtolower(trim((string)$payload['release_nonce'])) : '';
    $reason = isset($payload['reason']) ? trim((string)$payload['reason']) : '';
    $expiresInput = isset($payload['expires_at']) ? trim((string)$payload['expires_at']) : '';
    $expiresAt = preg_match('/(?:Z|[+-][0-9]{2}:[0-9]{2})$/', $expiresInput) === 1
        ? v1_mysql_datetime_utc($expiresInput) : null;
    $integerFields = array(
        'evidence_run_id',
        'evidence_artifact_id',
        'expected_v1_state_version',
        'expected_v2_state_version',
    );
    foreach ($integerFields as $field) {
        if (
            !isset($payload[$field])
            || !is_int($payload[$field])
            || (int)$payload[$field] < 0
            || (
                in_array($field, array('evidence_run_id', 'evidence_artifact_id'), true)
                && (int)$payload[$field] < 1
            )
        ) {
            v2_respond(400, array(
                'ok' => false,
                'error' => 'invalid_release_authorization_binding',
                'field' => $field,
            ));
        }
    }
    if (preg_match('/^[0-9a-f]{40}$/D', $candidateSha) !== 1) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_candidate_sha'));
    }
    if (preg_match('/^sha256:[0-9a-f]{64}$/D', $digest) !== 1) {
        v2_respond(400, array(
            'ok' => false,
            'error' => 'invalid_evidence_artifact_digest',
        ));
    }
    if (preg_match('/^[0-9a-f]{64}$/D', $nonce) !== 1) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_release_nonce'));
    }
    if ($expiresAt === null) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_authorization_expiry'));
    }
    if (mb_strlen($reason, 'UTF-8') < 8 || mb_strlen($reason, 'UTF-8') > 1000) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_release_reason'));
    }
    return array(
        'candidate_sha' => $candidateSha,
        'evidence_artifact_digest' => $digest,
        'evidence_run_id' => (int)$payload['evidence_run_id'],
        'evidence_artifact_id' => (int)$payload['evidence_artifact_id'],
        'release_nonce' => $nonce,
        'nonce_sha256' => hash('sha256', $nonce),
        'expected_v1_state_version' => (int)$payload['expected_v1_state_version'],
        'expected_v2_state_version' => (int)$payload['expected_v2_state_version'],
        'expires_at' => $expiresAt,
        'reason' => $reason,
    );
}

function v2_assert_deployed_candidate(array $fields): void {
    $identity = v2_deployment_identity_status();
    if (
        !isset($identity['valid'], $identity['code_revision'])
        || $identity['valid'] !== true
        || !is_string($identity['code_revision'])
    ) {
        v2_respond(503, array(
            'ok' => false,
            'error' => 'deployment_identity_unavailable',
        ));
    }
    if (!hash_equals((string)$identity['code_revision'], $fields['candidate_sha'])) {
        v2_respond(409, array(
            'ok' => false,
            'error' => 'release_candidate_sha_mismatch',
        ));
    }
}

function v2_admin_issue_release_authorization(
    PDO $pdo,
    array $config,
    string $role
): void {
    $fields = v2_release_authorization_fields(v2_json_body($config));
    v2_assert_deployed_candidate($fields);
    $authorizationId = 'release-auth:' . substr(hash(
        'sha256',
        $fields['candidate_sha'] . "\x1f"
        . $fields['evidence_artifact_digest'] . "\x1f"
        . $fields['nonce_sha256']
    ), 0, 48);
    $pdo->beginTransaction();
    try {
        $states = v2_release_state_rows_for_update($pdo, $config);
        if (
            (string)$states[GOV_V1_RELEASE_STATE_KEY]['release_state'] !== 'preview'
            || (string)$states[GOV_V2_RELEASE_STATE_KEY]['release_state'] !== 'preview'
        ) {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'release_authorization_requires_preview',
            ));
        }
        if (
            (int)$states[GOV_V1_RELEASE_STATE_KEY]['state_version']
                !== $fields['expected_v1_state_version']
            || (int)$states[GOV_V2_RELEASE_STATE_KEY]['state_version']
                !== $fields['expected_v2_state_version']
        ) {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'release_authorization_state_mismatch',
            ));
        }
        $ttlStatement = $pdo->prepare(
            'SELECT TIMESTAMPDIFF(SECOND,UTC_TIMESTAMP(),?)'
        );
        $ttlStatement->execute(array($fields['expires_at']));
        $ttl = (int)$ttlStatement->fetchColumn();
        if (
            $ttl < V2_RELEASE_AUTHORIZATION_MIN_TTL_SECONDS
            || $ttl > V2_RELEASE_AUTHORIZATION_MAX_TTL_SECONDS
        ) {
            $pdo->rollBack();
            v2_respond(400, array(
                'ok' => false,
                'error' => 'release_authorization_ttl_out_of_range',
                'minimum_seconds' => V2_RELEASE_AUTHORIZATION_MIN_TTL_SECONDS,
                'maximum_seconds' => V2_RELEASE_AUTHORIZATION_MAX_TTL_SECONDS,
            ));
        }
        $revoke = $pdo->prepare(
            'UPDATE ' . table_name($config, 'release_authorizations')
            . ' SET revoked_at=UTC_TIMESTAMP(),'
            . 'revoke_reason=?,updated_at=UTC_TIMESTAMP()'
            . ' WHERE revoked_at IS NULL AND fully_consumed_at IS NULL'
        );
        $revoke->execute(array('Superseded by a newer protected cutover authorization'));
        $insert = $pdo->prepare(
            'INSERT INTO ' . table_name($config, 'release_authorizations')
            . ' (authorization_id,candidate_sha,evidence_artifact_digest,'
            . 'evidence_run_id,evidence_artifact_id,nonce_sha256,'
            . 'expected_v1_state_version,expected_v2_state_version,expires_at,'
            . 'created_by,create_reason,created_at,updated_at)'
            . ' VALUES (?,?,?,?,?,?,?,?,?,?,?,UTC_TIMESTAMP(),UTC_TIMESTAMP())'
        );
        $insert->execute(array(
            $authorizationId,
            $fields['candidate_sha'],
            $fields['evidence_artifact_digest'],
            $fields['evidence_run_id'],
            $fields['evidence_artifact_id'],
            $fields['nonce_sha256'],
            $fields['expected_v1_state_version'],
            $fields['expected_v2_state_version'],
            $fields['expires_at'],
            'api_role:' . $role,
            $fields['reason'],
        ));
        $pdo->commit();
    } catch (PDOException $error) {
        if ($pdo->inTransaction()) {
            $pdo->rollBack();
        }
        if ((string)$error->getCode() === '23000') {
            v2_respond(409, array(
                'ok' => false,
                'error' => 'release_authorization_conflict',
            ));
        }
        throw $error;
    } catch (Throwable $error) {
        if ($pdo->inTransaction()) {
            $pdo->rollBack();
        }
        throw $error;
    }
    v2_respond(201, array(
        'ok' => true,
        'data' => array(
            'authorization_id' => $authorizationId,
            'candidate_sha' => $fields['candidate_sha'],
            'evidence_artifact_digest' => $fields['evidence_artifact_digest'],
            'evidence_run_id' => $fields['evidence_run_id'],
            'evidence_artifact_id' => $fields['evidence_artifact_id'],
            'expected_v1_state_version' => $fields['expected_v1_state_version'],
            'expected_v2_state_version' => $fields['expected_v2_state_version'],
            'expires_at' => v2_public_iso_time($fields['expires_at']),
        ),
    ));
}

function v2_atomic_cutover_fields(array $payload): array {
    try {
        v2_write_assert_keys(
            $payload,
            array(
                'candidate_sha',
                'evidence_artifact_digest',
                'release_nonce',
                'expected_v1_state_version',
                'expected_v2_state_version',
                'reason',
            ),
            'atomic-cutover'
        );
    } catch (InvalidArgumentException $error) {
        v2_respond(400, array(
            'ok' => false,
            'error' => 'unknown_atomic_cutover_field',
        ));
    }
    $candidateSha = isset($payload['candidate_sha'])
        ? strtolower(trim((string)$payload['candidate_sha'])) : '';
    $digest = isset($payload['evidence_artifact_digest'])
        ? strtolower(trim((string)$payload['evidence_artifact_digest'])) : '';
    $nonce = isset($payload['release_nonce'])
        ? strtolower(trim((string)$payload['release_nonce'])) : '';
    $reason = isset($payload['reason']) ? trim((string)$payload['reason']) : '';
    if (preg_match('/^[0-9a-f]{40}$/D', $candidateSha) !== 1) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_candidate_sha'));
    }
    if (preg_match('/^sha256:[0-9a-f]{64}$/D', $digest) !== 1) {
        v2_respond(400, array(
            'ok' => false,
            'error' => 'invalid_evidence_artifact_digest',
        ));
    }
    if (preg_match('/^[0-9a-f]{64}$/D', $nonce) !== 1) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_release_nonce'));
    }
    foreach (array('expected_v1_state_version', 'expected_v2_state_version') as $field) {
        if (!isset($payload[$field]) || !is_int($payload[$field]) || $payload[$field] < 0) {
            v2_respond(400, array(
                'ok' => false,
                'error' => 'invalid_release_authorization_binding',
                'field' => $field,
            ));
        }
    }
    if (mb_strlen($reason, 'UTF-8') < 8 || mb_strlen($reason, 'UTF-8') > 1000) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_release_reason'));
    }
    return array(
        'candidate_sha' => $candidateSha,
        'evidence_artifact_digest' => $digest,
        'release_nonce' => $nonce,
        'nonce_sha256' => hash('sha256', $nonce),
        'expected_v1_state_version' => (int)$payload['expected_v1_state_version'],
        'expected_v2_state_version' => (int)$payload['expected_v2_state_version'],
        'reason' => $reason,
    );
}

function v2_admin_atomic_cutover(PDO $pdo, array $config, string $role): void {
    $fields = v2_atomic_cutover_fields(v2_json_body($config));
    v2_assert_deployed_candidate($fields);
    $pdo->beginTransaction();
    try {
        $states = v2_release_state_rows_for_update($pdo, $config);
        if (
            (string)$states[GOV_V1_RELEASE_STATE_KEY]['release_state'] !== 'preview'
            || (string)$states[GOV_V2_RELEASE_STATE_KEY]['release_state'] !== 'preview'
        ) {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'protected_cutover_requires_preview',
            ));
        }
        if (
            (int)$states[GOV_V1_RELEASE_STATE_KEY]['state_version']
                !== $fields['expected_v1_state_version']
            || (int)$states[GOV_V2_RELEASE_STATE_KEY]['state_version']
                !== $fields['expected_v2_state_version']
        ) {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'release_authorization_state_mismatch',
            ));
        }
        $authorizationStatement = $pdo->prepare(
            'SELECT *,expires_at>UTC_TIMESTAMP() AS is_current FROM '
            . table_name($config, 'release_authorizations')
            . ' WHERE nonce_sha256=? LIMIT 1 FOR UPDATE'
        );
        $authorizationStatement->execute(array($fields['nonce_sha256']));
        $authorization = $authorizationStatement->fetch();
        if (!is_array($authorization)) {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'release_authorization_invalid',
            ));
        }
        if ($authorization['revoked_at'] !== null) {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'release_authorization_revoked',
            ));
        }
        if ($authorization['fully_consumed_at'] !== null) {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'release_authorization_replayed',
            ));
        }
        if ((int)$authorization['is_current'] !== 1) {
            $pdo->rollBack();
            v2_respond(410, array(
                'ok' => false,
                'error' => 'release_authorization_expired',
            ));
        }
        if (
            !hash_equals((string)$authorization['candidate_sha'], $fields['candidate_sha'])
            || !hash_equals(
                (string)$authorization['evidence_artifact_digest'],
                $fields['evidence_artifact_digest']
            )
            || (int)$authorization['expected_v1_state_version']
                !== $fields['expected_v1_state_version']
            || (int)$authorization['expected_v2_state_version']
                !== $fields['expected_v2_state_version']
        ) {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'release_authorization_binding_mismatch',
            ));
        }
        $requiredSources = v2_required_alpha_source_rights_guard($pdo, $config);
        $optionalSources = v2_optional_alpha_source_identity_guard($pdo, $config);
        if (
            (int)$requiredSources['invalid_count'] > 0
            || (int)$optionalSources['invalid_count'] > 0
        ) {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'required_alpha_sources_invalid',
                'required_connector_count' =>
                    (int)$requiredSources['checked_count'],
                'invalid_required_connector_count' =>
                    (int)$requiredSources['invalid_count'],
                'optional_connector_count' =>
                    (int)$optionalSources['checked_count'],
                'invalid_optional_connector_count' =>
                    (int)$optionalSources['invalid_count'],
                'invalid_sources' => array_merge(
                    $requiredSources['invalid_sources'],
                    $optionalSources['invalid_sources']
                ),
            ));
        }
        v2_lock_current_public_source_rights($pdo, $config);
        $v1Rights = v1_current_public_document_rights_guard($pdo, $config);
        $v2Rights = v2_current_public_document_rights_guard($pdo, $config);
        if ((int)$v1Rights['invalid_count'] > 0 || (int)$v2Rights['invalid_count'] > 0) {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'current_source_rights_invalid',
                'v1_invalid_source_right_document_count' => (int)$v1Rights['invalid_count'],
                'v2_invalid_source_right_document_count' => (int)$v2Rights['invalid_count'],
            ));
        }
        $clock = $pdo->query(
            'SELECT UTC_TIMESTAMP() AS cutover_at,'
            . 'DATE_ADD(UTC_TIMESTAMP(),INTERVAL 90 DAY) AS sunset_at'
        )->fetch();
        if (!is_array($clock)) {
            throw new RuntimeException('release_clock_unavailable');
        }
        $cutoverAt = (string)$clock['cutover_at'];
        $sunsetAt = (string)$clock['sunset_at'];
        $authorizationId = (string)$authorization['authorization_id'];
        $newVersions = array(
            GOV_V1_RELEASE_STATE_KEY => $fields['expected_v1_state_version'] + 1,
            GOV_V2_RELEASE_STATE_KEY => $fields['expected_v2_state_version'] + 1,
        );
        $update = $pdo->prepare(
            'UPDATE ' . table_name($config, 'governance_release_state')
            . ' SET release_state=\'live\',state_version=?,updated_by=?,'
            . 'update_reason=?,cutover_at=?,sunset_at=?,updated_at=?'
            . ' WHERE state_key=? AND release_state=\'preview\' AND state_version=?'
        );
        $audit = $pdo->prepare(
            'INSERT INTO ' . table_name($config, 'governance_release_audit')
            . ' (audit_id,state_key,state_version,previous_state,new_state,'
            . 'changed_by,change_reason,request_id,release_authorization_id,'
            . 'cutover_at,sunset_at,created_at)'
            . ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'
        );
        foreach (array(GOV_V1_RELEASE_STATE_KEY, GOV_V2_RELEASE_STATE_KEY) as $stateKey) {
            $expectedVersion = (int)$states[$stateKey]['state_version'];
            $newVersion = $newVersions[$stateKey];
            $update->execute(array(
                $newVersion,
                'api_role:' . $role,
                $fields['reason'],
                $cutoverAt,
                $sunsetAt,
                $cutoverAt,
                $stateKey,
                $expectedVersion,
            ));
            if ($update->rowCount() !== 1) {
                throw new RuntimeException('stale_release_state');
            }
            $auditId = 'release:atomic:' . substr(hash(
                'sha256',
                $authorizationId . "\x1f" . $stateKey . "\x1f" . $newVersion
            ), 0, 40);
            $audit->execute(array(
                $auditId,
                $stateKey,
                $newVersion,
                'preview',
                'live',
                'api_role:' . $role,
                $fields['reason'],
                v1_release_request_id(),
                $authorizationId,
                $cutoverAt,
                $sunsetAt,
                $cutoverAt,
            ));
        }
        $consume = $pdo->prepare(
            'UPDATE ' . table_name($config, 'release_authorizations')
            . ' SET v1_consumed_at=?,v1_consumed_state_version=?,'
            . 'v2_consumed_at=?,v2_consumed_state_version=?,'
            . 'fully_consumed_at=?,updated_at=?'
            . ' WHERE authorization_id=? AND fully_consumed_at IS NULL'
            . ' AND revoked_at IS NULL'
        );
        $consume->execute(array(
            $cutoverAt,
            $newVersions[GOV_V1_RELEASE_STATE_KEY],
            $cutoverAt,
            $newVersions[GOV_V2_RELEASE_STATE_KEY],
            $cutoverAt,
            $cutoverAt,
            $authorizationId,
        ));
        if ($consume->rowCount() !== 1) {
            throw new RuntimeException('release_authorization_consume_conflict');
        }
        $pdo->commit();
        v2_respond(200, array(
            'ok' => true,
            'data' => array(
                'changed' => true,
                'authorization_id' => $authorizationId,
                'candidate_sha' => $fields['candidate_sha'],
                'evidence_artifact_digest' => $fields['evidence_artifact_digest'],
                'cutover_at' => v2_public_iso_time($cutoverAt),
                'sunset_at' => v2_public_iso_time($sunsetAt),
                'states' => array(
                    GOV_V1_RELEASE_STATE_KEY => array(
                        'release_state' => 'live',
                        'state_version' => $newVersions[GOV_V1_RELEASE_STATE_KEY],
                    ),
                    GOV_V2_RELEASE_STATE_KEY => array(
                        'release_state' => 'live',
                        'state_version' => $newVersions[GOV_V2_RELEASE_STATE_KEY],
                    ),
                ),
            ),
        ));
    } catch (Throwable $error) {
        if ($pdo->inTransaction()) {
            $pdo->rollBack();
        }
        throw $error;
    }
}

function v2_admin_update_release_state(PDO $pdo, array $config, string $role): void {
    $payload = v2_json_body($config);
    try {
        v2_write_assert_keys(
            $payload,
            array(
                'release_state',
                'expected_version',
                'reason',
                'cutover_at',
                'sunset_at',
            ),
            'release-state'
        );
    } catch (InvalidArgumentException $error) {
        v2_respond(400, array(
            'ok' => false,
            'error' => 'unknown_release_state_field',
        ));
    }
    $target = isset($payload['release_state']) ? trim((string)$payload['release_state']) : '';
    $reason = isset($payload['reason']) ? trim((string)$payload['reason']) : '';
    $expectedVersion = isset($payload['expected_version']) && is_int($payload['expected_version'])
        ? (int)$payload['expected_version'] : -1;
    if (!in_array($target, array('closed', 'preview', 'live'), true)) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_release_state'));
    }
    if ($expectedVersion < 0) {
        v2_respond(400, array('ok' => false, 'error' => 'expected_version_required'));
    }
    if (mb_strlen($reason, 'UTF-8') < 8 || mb_strlen($reason, 'UTF-8') > 2000) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_release_reason'));
    }
    if ($target === 'preview' && !v1_preview_auth_configured($config)) {
        v2_respond(503, array('ok' => false, 'error' => 'preview_auth_not_configured'));
    }
    $cutoverAt = isset($payload['cutover_at']) ? v1_mysql_datetime_utc($payload['cutover_at']) : null;
    $sunsetAt = isset($payload['sunset_at']) ? v1_mysql_datetime_utc($payload['sunset_at']) : null;
    if (isset($payload['cutover_at']) && $cutoverAt === null) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_cutover_at'));
    }
    if (isset($payload['sunset_at']) && $sunsetAt === null) {
        v2_respond(400, array('ok' => false, 'error' => 'invalid_sunset_at'));
    }
    $pdo->beginTransaction();
    try {
        $before = v2_release_state($pdo, $config, true);
        if ($before === null) {
            $pdo->rollBack();
            v2_respond(503, array('ok' => false, 'error' => 'release_state_unavailable'));
        }
        $currentVersion = (int)$before['state_version'];
        if ($currentVersion !== $expectedVersion) {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'stale_release_state',
                'current_state' => (string)$before['release_state'],
                'current_version' => $currentVersion,
            ));
        }
        if ($target === (string)$before['release_state']) {
            $pdo->commit();
            v2_respond(200, array(
                'ok' => true,
                'data' => array(
                    'release_state' => $target,
                    'state_version' => $currentVersion,
                    'changed' => false,
                ),
            ));
        }
        $current = (string)$before['release_state'];
        $allowedTransitions = array(
            'closed' => array('preview'),
            'preview' => array('closed', 'live'),
            'live' => array('closed'),
        );
        if (
            !isset($allowedTransitions[$current])
            || !in_array($target, $allowedTransitions[$current], true)
        ) {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'invalid_release_transition',
                'current_state' => $current,
                'requested_state' => $target,
            ));
        }
        if ($current === 'preview' && $target === 'live') {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'protected_atomic_cutover_required',
            ));
        }
        $nextVersion = $currentVersion + 1;
        $update = $pdo->prepare(
            'UPDATE ' . table_name($config, 'governance_release_state')
            . ' SET release_state=?,state_version=?,updated_by=?,update_reason=?,'
            . 'cutover_at=?,sunset_at=?,updated_at=UTC_TIMESTAMP()'
            . ' WHERE state_key=? AND state_version=?'
        );
        $update->execute(array(
            $target,
            $nextVersion,
            $role,
            $reason,
            $cutoverAt,
            $sunsetAt,
            GOV_V2_RELEASE_STATE_KEY,
            $currentVersion,
        ));
        if ($update->rowCount() !== 1) {
            $pdo->rollBack();
            v2_respond(409, array('ok' => false, 'error' => 'stale_release_state'));
        }
        $auditId = 'release:v2:' . substr(hash(
            'sha256',
            GOV_V2_RELEASE_STATE_KEY . "\x1f" . $nextVersion . "\x1f" . gmdate('c')
        ), 0, 40);
        $audit = $pdo->prepare(
            'INSERT INTO ' . table_name($config, 'governance_release_audit')
            . ' (audit_id,state_key,state_version,previous_state,new_state,changed_by,change_reason,'
            . 'request_id,cutover_at,sunset_at,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,UTC_TIMESTAMP())'
        );
        $audit->execute(array(
            $auditId,
            GOV_V2_RELEASE_STATE_KEY,
            $nextVersion,
            (string)$before['release_state'],
            $target,
            $role,
            $reason,
            v1_release_request_id(),
            $cutoverAt,
            $sunsetAt,
        ));
        $pdo->commit();
        v2_respond(200, array(
            'ok' => true,
            'data' => array(
                'release_state' => $target,
                'state_version' => $nextVersion,
                'changed' => true,
            ),
        ));
    } catch (Throwable $error) {
        if ($pdo->inTransaction()) {
            $pdo->rollBack();
        }
        throw $error;
    }
}

function v2_alpha_date_epoch(string $value): ?int {
    if (preg_match('/^\d{4}-\d{2}-\d{2}$/D', $value) !== 1) {
        return null;
    }
    $parsed = DateTimeImmutable::createFromFormat(
        '!Y-m-d',
        $value,
        new DateTimeZone('UTC')
    );
    $errors = DateTimeImmutable::getLastErrors();
    if (
        $parsed === false
        || (
            is_array($errors)
            && (
                (int)$errors['warning_count'] !== 0
                || (int)$errors['error_count'] !== 0
            )
        )
        || $parsed->format('Y-m-d') !== $value
    ) {
        return null;
    }
    return $parsed->getTimestamp();
}

function v2_alpha_latest_contiguous_windows(array $windows): array {
    if (!$windows) {
        throw new RuntimeException('alpha_evidence_windows_missing');
    }
    ksort($windows, SORT_STRING);
    $sequence = array();
    foreach ($windows as $window) {
        $start = (string)$window['window_start'];
        $end = (string)$window['window_end_exclusive'];
        $startEpoch = v2_alpha_date_epoch($start);
        $endEpoch = v2_alpha_date_epoch($end);
        if (
            $startEpoch === null
            || $endEpoch === null
            || $endEpoch - $startEpoch !== 86400
            || $endEpoch > time()
        ) {
            throw new RuntimeException('alpha_evidence_window_invalid');
        }
        if (
            $sequence
            && (string)$sequence[count($sequence) - 1]['window_end_exclusive']
                !== $start
        ) {
            $sequence = array();
        }
        $sequence[] = $window;
    }
    if (count($sequence) < 30) {
        throw new RuntimeException('alpha_evidence_30_day_horizon_missing');
    }
    return array_slice($sequence, -30);
}

function v2_alpha_global_connector_windows(
    PDO $pdo,
    array $config,
    string $connectorId,
    string $codeRevision
): array {
    $statement = $pdo->prepare(
        'SELECT ingest_id,connector_id,idempotency_key,payload_sha256,batch_id,'
        . 'chunk_index,chunk_count,window_start,window_end_exclusive,request_count,'
        . 'raw_count,acknowledged_count,batch_raw_count,batch_acknowledged_count,'
        . 'batch_request_count,code_revision,started_at,completed_at,created_at FROM '
        . table_name($config, 'global_ingest_receipts')
        . ' WHERE connector_id=? AND code_revision=?'
        . ' ORDER BY window_start,batch_id,chunk_index LIMIT 50001'
    );
    $statement->execute(array($connectorId, $codeRevision));
    $rows = $statement->fetchAll();
    if (count($rows) > 50000) {
        throw new RuntimeException('alpha_evidence_receipt_scan_limit');
    }
    $batches = array();
    foreach ($rows as $row) {
        $idempotencyKey = (string)$row['idempotency_key'];
        $completedDayPrefix = 'global-ingest-v2-day:us:';
        $currentPollPrefix = 'global-ingest-v2-current:us:';
        if (preg_match(
            '/^' . preg_quote($completedDayPrefix, '/') . '[a-f0-9]{64}$/D',
            $idempotencyKey
        ) !== 1) {
            if (strpos($idempotencyKey, 'global-ingest-v2-day:') === 0) {
                throw new RuntimeException(
                    'alpha_evidence_completed_day_marker_invalid'
                );
            }
            if (preg_match(
                '/^' . preg_quote($currentPollPrefix, '/')
                    . '[a-f0-9]{64}$/D',
                $idempotencyKey
            ) === 1) {
                // Intraday/current receipts prove operational freshness, not
                // immutable completed-day coverage.
                continue;
            }
            if (strpos($idempotencyKey, 'global-ingest-v2-current:') === 0) {
                throw new RuntimeException(
                    'alpha_evidence_current_poll_marker_invalid'
                );
            }
            // Intraday/hybrid cursor receipts are operational freshness
            // observations, not completed-day evidence windows.
            continue;
        }
        $batchId = (string)$row['batch_id'];
        $key = (string)$row['window_start']
            . ':' . (string)$row['window_end_exclusive'];
        if (!isset($batches[$batchId])) {
            $batches[$batchId] = array(
                'window_key' => $key,
                'rows' => array(),
            );
        }
        if ($batches[$batchId]['window_key'] !== $key) {
            throw new RuntimeException('alpha_evidence_batch_window_conflict');
        }
        $batches[$batchId]['rows'][] = $row;
    }

    $windows = array();
    foreach ($batches as $batch) {
        $batchRows = $batch['rows'];
        if (!$batchRows) {
            continue;
        }
        $first = $batchRows[0];
        $chunkCount = (int)$first['chunk_count'];
        if ($chunkCount < 1 || count($batchRows) !== $chunkCount) {
            throw new RuntimeException('alpha_evidence_batch_incomplete');
        }
        $final = $batchRows[$chunkCount - 1];
        $raw = 0;
        $acknowledged = 0;
        $requests = 0;
        $receiptRows = array();
        foreach ($batchRows as $position => $row) {
            $receiptRaw = (int)$row['raw_count'];
            $receiptAcknowledged = (int)$row['acknowledged_count'];
            if (
                (int)$row['chunk_index'] !== $position + 1
                || (int)$row['chunk_count'] !== $chunkCount
                || (string)$row['window_start'] !== (string)$first['window_start']
                || (string)$row['window_end_exclusive']
                    !== (string)$first['window_end_exclusive']
                || (int)$row['batch_raw_count'] !== (int)$first['batch_raw_count']
                || (int)$row['batch_acknowledged_count']
                    !== (int)$first['batch_acknowledged_count']
                || (int)$row['batch_request_count'] !== 1
                || (string)$row['code_revision'] !== $codeRevision
                || preg_match(
                    '/^[a-f0-9]{64}$/D',
                    (string)$row['payload_sha256']
                ) !== 1
                || $receiptRaw < 0
                || $receiptAcknowledged < 0
                || $receiptRaw < $receiptAcknowledged
            ) {
                throw new RuntimeException('alpha_evidence_receipt_corrupt');
            }
            $raw += $receiptRaw;
            $acknowledged += $receiptAcknowledged;
            $requests += (int)$row['request_count'];
            $receiptRows[] = array(
                'ingest_id' => (string)$row['ingest_id'],
                'idempotency_key' => (string)$row['idempotency_key'],
                'payload_sha256' => (string)$row['payload_sha256'],
                'chunk_index' => (int)$row['chunk_index'],
                'chunk_count' => (int)$row['chunk_count'],
                'raw_count' => $receiptRaw,
                'filtered_out_count' =>
                    $receiptRaw - $receiptAcknowledged,
                'accepted_count' => $receiptAcknowledged,
                'acknowledged_count' => $receiptAcknowledged,
                'request_count' => (int)$row['request_count'],
                'started_at' => (string)$row['started_at'],
                'completed_at' => (string)$row['completed_at'],
            );
        }
        if (
            $raw !== (int)$first['batch_raw_count']
            || $acknowledged !== (int)$first['batch_acknowledged_count']
            || (int)$final['batch_request_count'] !== 1
            || $requests !== (int)$final['batch_request_count']
            || $raw < $acknowledged
        ) {
            throw new RuntimeException('alpha_evidence_receipt_totals_mismatch');
        }
        $accepted = $acknowledged;
        $filteredOut = $raw - $accepted;
        $windowKey = (string)$batch['window_key'];
        if (isset($windows[$windowKey])) {
            throw new RuntimeException('alpha_evidence_duplicate_window');
        }
        $windows[$windowKey] = array(
            'window_start' => (string)$first['window_start'],
            'window_end_exclusive' => (string)$first['window_end_exclusive'],
            'raw_count' => $raw,
            'filtered_out_count' => $filteredOut,
            'accepted_count' => $accepted,
            'acknowledged_count' => $acknowledged,
            'status' => 'complete',
            'code_revision' => $codeRevision,
            'receipt_sha256' => hash(
                'sha256',
                v1_strict_canonical_json_encode(
                    array(
                        'connector_id' => $connectorId,
                        'batch_id' => (string)$first['batch_id'],
                        'window_start' => (string)$first['window_start'],
                        'window_end_exclusive' =>
                            (string)$first['window_end_exclusive'],
                        'code_revision' => $codeRevision,
                        'receipts' => $receiptRows,
                    ),
                    'alpha_evidence_receipt_encoding_failed'
                )
            ),
        );
    }
    return v2_alpha_latest_contiguous_windows($windows);
}

/**
 * Bind a DART evidence checkpoint to the exact Python apply-job contract.
 *
 * ``curator.official_backfill`` hashes the canonical job object before adding
 * its ``fingerprint`` member.  The database path key, embedded member, and
 * recomputed hash must all agree, and an apply checkpoint must carry the exact
 * release revision requested by the evidence exporter.  Legacy checkpoints
 * without a revision and checkpoints from another release are not evidence.
 */
function v2_alpha_dart_job_is_release_bound(
    array $job,
    string $rowFingerprint,
    string $codeRevision
): bool {
    $expectedKeys = array(
        'chunk_days',
        'code_revision',
        'fingerprint',
        'max_pages',
        'page_count',
        'range_end_exclusive',
        'range_start',
        'sources',
        'sync_company_master',
    );
    $actualKeys = array_keys($job);
    sort($actualKeys, SORT_STRING);
    if ($actualKeys !== $expectedKeys) {
        return false;
    }
    if (
        !is_string($job['range_start'])
        || preg_match('/^\d{4}-\d{2}-\d{2}$/D', $job['range_start']) !== 1
        || !is_string($job['range_end_exclusive'])
        || preg_match(
            '/^\d{4}-\d{2}-\d{2}$/D',
            $job['range_end_exclusive']
        ) !== 1
        || $job['range_end_exclusive'] <= $job['range_start']
        || !is_int($job['chunk_days'])
        || $job['chunk_days'] !== 1
        || !is_array($job['sources'])
        || array_keys($job['sources']) !== array(0)
        || array_values($job['sources']) !== array('dart')
        || !is_int($job['page_count'])
        || $job['page_count'] < 1
        || $job['page_count'] > 100
        || !is_int($job['max_pages'])
        || $job['max_pages'] < 1
        || !is_bool($job['sync_company_master'])
        || !is_string($job['code_revision'])
        || preg_match('/^[a-f0-9]{40}$/D', $job['code_revision']) !== 1
        || !hash_equals($codeRevision, $job['code_revision'])
        || !is_string($job['fingerprint'])
        || preg_match('/^[a-f0-9]{64}$/D', $job['fingerprint']) !== 1
        || preg_match('/^[a-f0-9]{64}$/D', $rowFingerprint) !== 1
        || !hash_equals($rowFingerprint, $job['fingerprint'])
    ) {
        return false;
    }
    $contract = $job;
    unset($contract['fingerprint']);
    try {
        $canonical = v1_strict_canonical_json_encode(
            $contract,
            'alpha_evidence_dart_job_encoding_failed'
        );
    } catch (Throwable $error) {
        return false;
    }
    return hash_equals($rowFingerprint, hash('sha256', $canonical));
}

function v2_alpha_dart_windows(
    PDO $pdo,
    array $config,
    string $codeRevision
): array {
    $statement = $pdo->query(
        'SELECT job_fingerprint,checkpoint_json,payload_hash,updated_at FROM '
        . table_name($config, 'official_backfill_checkpoints')
        . ' ORDER BY updated_at DESC LIMIT 100'
    );
    $best = array();
    $bestEnd = '';
    foreach ($statement->fetchAll() as $row) {
        $raw = (string)$row['checkpoint_json'];
        $payloadHash = (string)$row['payload_hash'];
        if (
            preg_match('/^[a-f0-9]{64}$/D', $payloadHash) !== 1
            || !hash_equals($payloadHash, hash('sha256', $raw))
        ) {
            throw new RuntimeException('alpha_evidence_checkpoint_integrity');
        }
        $checkpoint = json_decode($raw, true);
        if (!is_array($checkpoint)) {
            throw new RuntimeException('alpha_evidence_checkpoint_invalid');
        }
        $job = isset($checkpoint['job']) && is_array($checkpoint['job'])
            ? $checkpoint['job'] : array();
        $sources = isset($job['sources']) && is_array($job['sources'])
            ? $job['sources'] : array();
        if (
            !in_array('dart', $sources, true)
            || (int)($job['chunk_days'] ?? 0) !== 1
        ) {
            continue;
        }
        if (!v2_alpha_dart_job_is_release_bound(
            $job,
            (string)$row['job_fingerprint'],
            $codeRevision
        )) {
            continue;
        }
        $jobFingerprint = (string)$row['job_fingerprint'];
        $failed = isset($checkpoint['failed_windows'])
            && is_array($checkpoint['failed_windows'])
            ? $checkpoint['failed_windows'] : null;
        $completed = isset($checkpoint['completed_windows'])
            && is_array($checkpoint['completed_windows'])
            ? $checkpoint['completed_windows'] : null;
        if ($failed === null || $completed === null || count($failed) !== 0) {
            continue;
        }
        $windows = array();
        foreach ($completed as $windowKey => $window) {
            $expectedIdempotencyKey = 'official-backfill-v1:' . substr(
                hash('sha256', $jobFingerprint . '|' . (string)$windowKey),
                0,
                32
            );
            if (
                !is_array($window)
                || (string)($window['status'] ?? '') !== 'succeeded'
                || (string)($window['code_revision'] ?? '') !== $codeRevision
                || !hash_equals(
                    $expectedIdempotencyKey,
                    (string)($window['idempotency_key'] ?? '')
                )
                || !isset($window['summary'])
                || !is_array($window['summary'])
            ) {
                continue;
            }
            $summary = $window['summary'];
            $rawCount = (int)($summary['official_remote_raw_count'] ?? -1);
            $ackCount = (int)($summary['official_remote_ack_count'] ?? -2);
            if (
                $rawCount < 0
                || $ackCount < 0
                || $rawCount < $ackCount
                || (int)($summary['official_remote_run_persisted'] ?? 0) !== 1
                || (int)($summary['official_remote_ack_mismatches'] ?? 0) !== 0
                || (int)($summary['official_remote_failed'] ?? 0) !== 0
                || (int)($summary['official_remote_skipped'] ?? 0) !== 0
                || (int)($summary['official_failed'] ?? 0) !== 0
                || (int)($summary['official_skipped'] ?? 0) !== 0
            ) {
                throw new RuntimeException('alpha_evidence_dart_ack_invalid');
            }
            $acceptedCount = $ackCount;
            $filteredOutCount = $rawCount - $acceptedCount;
            $start = (string)($window['window_start'] ?? '');
            $end = (string)($window['window_end_exclusive'] ?? '');
            if ($windowKey !== $start . ':' . $end) {
                throw new RuntimeException('alpha_evidence_dart_window_key');
            }
            $windows[$windowKey] = array(
                'window_start' => $start,
                'window_end_exclusive' => $end,
                'raw_count' => $rawCount,
                'filtered_out_count' => $filteredOutCount,
                'accepted_count' => $acceptedCount,
                'acknowledged_count' => $ackCount,
                'status' => 'complete',
                'code_revision' => $codeRevision,
                'receipt_sha256' => hash(
                    'sha256',
                    v1_strict_canonical_json_encode(
                        array(
                            'job_fingerprint' =>
                                $jobFingerprint,
                            'checkpoint_payload_sha256' => $payloadHash,
                            'window_key' => (string)$windowKey,
                            'idempotency_key' =>
                                (string)$window['idempotency_key'],
                            'raw_count' => $rawCount,
                            'filtered_out_count' => $filteredOutCount,
                            'accepted_count' => $acceptedCount,
                            'acknowledged_count' => $ackCount,
                            'code_revision' => $codeRevision,
                        ),
                        'alpha_evidence_dart_receipt_encoding_failed'
                    )
                ),
            );
        }
        try {
            $candidate = v2_alpha_latest_contiguous_windows($windows);
        } catch (RuntimeException $error) {
            continue;
        }
        $candidateEnd = (string)$candidate[count($candidate) - 1][
            'window_end_exclusive'
        ];
        if ($candidateEnd > $bestEnd) {
            $best = $candidate;
            $bestEnd = $candidateEnd;
        }
    }
    if (!$best) {
        throw new RuntimeException('alpha_evidence_dart_30_day_horizon_missing');
    }
    return $best;
}

function v2_alpha_content_integrity(
    PDO $pdo,
    array $config,
    string $codeRevision,
    string $collectedAt
): array {
    $eventStatement = $pdo->query(
        'SELECT e.event_id,e.title,e.original_language,'
        . 'JSON_UNQUOTE(JSON_EXTRACT(e.payload_json,'
        . '\'$.metadata.title_provenance\')) AS title_provenance FROM '
        . table_name($config, 'governance_events') . ' e WHERE '
        . v2_event_visibility_sql($config, 'e')
        . ' ORDER BY e.event_id LIMIT 10001'
    );
    $events = $eventStatement->fetchAll();
    if (count($events) > 10000) {
        throw new RuntimeException('alpha_evidence_content_scan_limit');
    }
    $documentsByEvent = array();
    $eventIds = array();
    foreach ($events as $event) {
        $eventIds[] = (string)$event['event_id'];
        $documentsByEvent[(string)$event['event_id']] = array();
    }
    foreach (array_chunk($eventIds, 100) as $ids) {
        $placeholders = implode(',', array_fill(0, count($ids), '?'));
        $documentStatement = $pdo->prepare(
            'SELECT ed.event_id,d.title,d.original_language,d.original_url,'
            . 'd.source_class,d.payload_json FROM '
            . table_name($config, 'event_documents') . ' ed JOIN '
            . table_name($config, 'documents')
            . ' d ON d.document_id=ed.document_id LEFT JOIN '
            . table_name($config, 'source_rights')
            . ' sr ON sr.source_right_id=d.source_right_id WHERE ed.event_id IN ('
            . $placeholders . ') AND '
            . v2_document_visibility_sql('d', 'sr')
            . ' ORDER BY ed.event_id,ed.position_no,d.document_id'
        );
        $documentStatement->execute($ids);
        foreach ($documentStatement->fetchAll() as $document) {
            $documentsByEvent[(string)$document['event_id']][] = $document;
        }
    }

    $counts = array(
        'public_event_count' => count($events),
        'original_language_preserved_count' => 0,
        'official_url_preserved_count' => 0,
        'title_provenance_labeled_count' => 0,
        'source_title_event_count' => 0,
        'source_title_preserved_count' => 0,
        'generated_metadata_title_count' => 0,
        'operator_metadata_title_count' => 0,
        'unknown_title_provenance_count' => 0,
        'scanned_response_count' => count($events),
        'telegram_exposure_count' => 0,
        'internal_field_exposure_count' => 0,
    );
    $officialClasses = array(
        'official_disclosure',
        'official_register',
        'company_statement',
        'official_issuer',
    );
    foreach ($events as $event) {
        $provenance = (string)$event['title_provenance'];
        if (
            in_array(
                $provenance,
                array('source', 'generated_metadata', 'operator_metadata'),
                true
            )
        ) {
            $counts['title_provenance_labeled_count']++;
        } else {
            $counts['unknown_title_provenance_count']++;
        }
        if ($provenance === 'source') {
            $counts['source_title_event_count']++;
        } elseif ($provenance === 'generated_metadata') {
            $counts['generated_metadata_title_count']++;
        } elseif ($provenance === 'operator_metadata') {
            $counts['operator_metadata_title_count']++;
        }
        $languagePreserved = false;
        $urlPreserved = false;
        $sourceTitlePreserved = false;
        foreach ($documentsByEvent[(string)$event['event_id']] as $document) {
            $sourceClass = strtolower((string)$document['source_class']);
            if (strpos($sourceClass, 'telegram') !== false) {
                $counts['telegram_exposure_count']++;
                continue;
            }
            if (!in_array($sourceClass, $officialClasses, true)) {
                continue;
            }
            if (
                (string)$event['original_language']
                    === (string)$document['original_language']
            ) {
                $languagePreserved = true;
            }
            if (
                preg_match(
                    '#^https?://#i',
                    (string)$document['original_url']
                ) === 1
            ) {
                $urlPreserved = true;
            }
            $documentPayload = json_decode(
                (string)$document['payload_json'],
                true
            );
            $documentProvenance = is_array($documentPayload)
                && isset($documentPayload['metadata'])
                && is_array($documentPayload['metadata'])
                ? (string)($documentPayload['metadata']['title_provenance'] ?? '')
                : '';
            if (
                $provenance === 'source'
                && $documentProvenance === 'source'
                && (string)$event['title'] === (string)$document['title']
            ) {
                $sourceTitlePreserved = true;
            }
        }
        $counts['original_language_preserved_count'] +=
            $languagePreserved ? 1 : 0;
        $counts['official_url_preserved_count'] += $urlPreserved ? 1 : 0;
        $counts['source_title_preserved_count'] +=
            $sourceTitlePreserved ? 1 : 0;
    }
    return array(
        'schema_version' => 1,
        'kind' => 'bside-global-alpha-content-integrity',
        'environment' => 'production',
        'evidence_source' => 'production_database_export',
        'is_synthetic' => false,
        'code_revision' => $codeRevision,
        'collected_at' => $collectedAt,
        'raw_counts' => $counts,
    );
}

function v2_ops_alpha_release_evidence(PDO $pdo, array $config): void {
    $revision = isset($_GET['code_revision'])
        ? strtolower(trim((string)$_GET['code_revision'])) : '';
    if (preg_match('/^[a-f0-9]{40}$/D', $revision) !== 1) {
        v2_respond(400, array(
            'ok' => false,
            'error' => 'invalid_code_revision',
        ));
    }
    $identity = v2_deployment_identity_status();
    if (
        $identity['valid'] !== true
        || !hash_equals((string)$identity['code_revision'], $revision)
    ) {
        v2_respond(409, array(
            'ok' => false,
            'error' => 'deployed_revision_mismatch',
        ));
    }
    try {
        $definitions = array(
            array('dart', 'KR', null),
            array('sec-edgar', 'US', 'connector:us:sec-edgar'),
        );
        $coverage = array();
        foreach ($definitions as $definition) {
            $windows = $definition[2] === null
                ? v2_alpha_dart_windows($pdo, $config, $revision)
                : v2_alpha_global_connector_windows(
                    $pdo,
                    $config,
                    (string)$definition[2],
                    $revision
                );
            $coverage[] = array(
                'connector_family' => $definition[0],
                'country' => $definition[1],
                'coverage_started_at' =>
                    $windows[0]['window_start'] . 'T00:00:00+00:00',
                'coverage_ended_at' =>
                    $windows[count($windows) - 1]['window_end_exclusive']
                    . 'T00:00:00+00:00',
                'successful_window_count' => count($windows),
                'failed_window_count' => 0,
                'completed_windows' => $windows,
            );
        }
        $collectedAt = gmdate('Y-m-d\TH:i:s\Z');
        v2_respond(200, array(
            'ok' => true,
            'data' => array(
                'schema_version' => 1,
                'kind' => 'bside-global-alpha-automated-evidence',
                'environment' => 'production',
                'evidence_source' => 'production_database_export',
                'is_synthetic' => false,
                'code_revision' => $revision,
                'collected_at' => $collectedAt,
                'connector_coverage' => $coverage,
                'content_integrity' => v2_alpha_content_integrity(
                    $pdo,
                    $config,
                    $revision,
                    $collectedAt
                ),
            ),
        ));
    } catch (Throwable $error) {
        v2_respond(409, array(
            'ok' => false,
            'error' => 'automated_evidence_unavailable',
        ));
    }
}

function handle_v2_request(string $method, string $path, array $config): void {
    $path = '/' . trim($path, '/');
    if ($method === 'GET' && $path === '/health') {
        $identity = v2_deployment_identity_status();
        if ($identity['valid'] !== true) {
            v2_respond(503, array(
                'ok' => false,
                'error' => 'deployment_identity_unavailable',
                'reason' => $identity['error'],
            ));
        }
        v2_respond(200, array(
            'ok' => true,
            'service' => 'bside-global-market-terminal',
            'code_revision' => $identity['code_revision'],
            'schema_version' => GOV_V2_SCHEMA_VERSION,
            'time' => gmdate('c'),
        ));
    }
    if ($method === 'GET' && ($path === '/openapi.yaml' || $path === '/openapi.json')) {
        v2_serve_openapi($path);
    }
    if (!v2_path_is_defined($path)) {
        v2_respond(404, array('ok' => false, 'error' => 'not_found'));
    }

    $role = null;
    if ($path === '/admin/release-authorizations') {
        $role = v2_require_exact_role($config, 'release_authorizer');
    } elseif (
        $path === '/admin/release-state'
        || $path === '/admin/cutover'
        || $path === '/admin/connectors'
        || preg_match(
            '#^/admin/connectors/connector:[a-z]{2}:[a-z0-9_.:\-]{1,64}$#',
            $path
        ) === 1
    ) {
        $role = v2_require_role($config, array('admin'));
    } elseif (
        $path === '/admin/review-queue'
        || $path === '/admin/brief-candidates'
        || $path === '/admin/briefs'
        || preg_match('#^/admin/events/[A-Za-z0-9_.:\-]{1,96}/review$#', $path) === 1
    ) {
        $role = v2_require_role($config, array('editor'));
    } elseif (strpos($path, '/admin/') === 0 || strpos($path, '/ops/') === 0) {
        $role = v2_require_role($config, array('ops'));
    } elseif ($method !== 'GET') {
        header('Allow: GET');
        v2_respond(405, array('ok' => false, 'error' => 'method_not_allowed'));
    }

    $pdo = pdo_conn($config);
    v2_require_schema_version($pdo, $config);
    $privileged = strpos($path, '/admin/') === 0 || strpos($path, '/ops/') === 0;
    if (!$privileged) {
        v2_require_public_release_access($pdo, $config);
    }

    if ($method === 'GET') {
        if ($path === '/') {
            v2_respond(200, array(
                'ok' => true,
                'service' => 'bside-global-market-terminal',
                'documentation' => '/api/v2/openapi.yaml',
            ));
        }
        if ($path === '/briefs/latest') {
            v2_latest_brief($pdo, $config);
        }
        if ($path === '/live' || $path === '/events') {
            v2_list_events($pdo, $config, $path === '/live');
        }
        if (preg_match('#^/events/([A-Za-z0-9_.:\-]{1,96})$#', $path, $matches) === 1) {
            v2_get_event($pdo, $config, $matches[1]);
        }
        if ($path === '/issuers') {
            v2_list_issuers($pdo, $config);
        }
        if (preg_match('#^/issuers/([A-Za-z0-9_.:\-]{1,96})$#', $path, $matches) === 1) {
            v2_get_issuer($pdo, $config, $matches[1]);
        }
        if ($path === '/calendar') {
            v2_calendar($pdo, $config);
        }
        if ($path === '/search') {
            v2_search($pdo, $config);
        }
        if ($path === '/sources/status') {
            v2_sources_status($pdo, $config);
        }
        if (
            preg_match(
                '#^/ops/connectors/(connector:[a-z]{2}:[a-z0-9_.:\-]{1,64})/checkpoint$#',
                $path,
                $matches
            ) === 1
        ) {
            v2_ops_connector_checkpoint($pdo, $config, $matches[1]);
        }
        if ($path === '/ops/source-right-eligibility') {
            v2_ops_source_right_eligibility($pdo, $config);
        }
        if ($path === '/ops/alpha-release-evidence') {
            v2_ops_alpha_release_evidence($pdo, $config);
        }
        if ($path === '/ops/release-state') {
            v2_admin_release_state($pdo, $config);
        }
        if ($path === '/exports/events.json') {
            v2_export_events_json($pdo, $config);
        }
        if ($path === '/exports/events.csv') {
            v2_export_events_csv($pdo, $config);
        }
        if ($path === '/feeds/events.atom') {
            v2_events_atom($pdo, $config);
        }
        if ($path === '/admin/release-state') {
            v2_admin_release_state($pdo, $config);
        }
        if ($path === '/admin/connectors') {
            v2_admin_connectors($pdo, $config);
        }
        if (
            preg_match(
                '#^/admin/connectors/(connector:[a-z]{2}:[a-z0-9_.:\-]{1,64})$#',
                $path,
                $matches
            ) === 1
        ) {
            v2_admin_connectors($pdo, $config, $matches[1]);
        }
        if ($path === '/admin/review-queue') {
            v2_admin_review_queue($pdo, $config);
        }
        if ($path === '/admin/brief-candidates') {
            v2_admin_brief_candidates($pdo, $config);
        }
    }
    if ($method === 'POST' && $path === '/admin/release-state') {
        v2_admin_update_release_state($pdo, $config, (string)$role);
    }
    if ($method === 'POST' && $path === '/admin/release-authorizations') {
        v2_admin_issue_release_authorization($pdo, $config, (string)$role);
    }
    if ($method === 'POST' && $path === '/admin/cutover') {
        v2_admin_atomic_cutover($pdo, $config, (string)$role);
    }
    if (
        $method === 'POST'
        && preg_match(
            '#^/admin/connectors/(connector:[a-z]{2}:[a-z0-9_.:\-]{1,64})$#',
            $path,
            $matches
        ) === 1
    ) {
        v2_admin_update_connector(
            $pdo,
            $config,
            $matches[1],
            (string)$role
        );
    }
    if (
        $method === 'POST'
        && preg_match(
            '#^/admin/events/([A-Za-z0-9_.:\-]{1,96})/review$#',
            $path,
            $matches
        ) === 1
    ) {
        v2_admin_review_event($pdo, $config, $matches[1], (string)$role);
    }
    if ($method === 'POST' && $path === '/admin/briefs') {
        v2_admin_publish_brief($pdo, $config, (string)$role);
    }
    if ($method === 'POST' && $path === '/ops/ingest') {
        v2_ops_ingest($pdo, $config);
    }
    v2_respond(404, array('ok' => false, 'error' => 'not_found'));
}
