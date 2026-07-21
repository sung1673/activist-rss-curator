<?php
/**
 * BSIDE Governance Intelligence API v1.
 *
 * This file is loaded by api.php after the private configuration. It contains
 * public read routes, role-gated operations routes, ingestion contracts and
 * outbox/link-discovery lease handlers. Existing ?action= APIs remain in
 * api.php and keep their original response shapes.
 */

const V1_RESPONSE_BUDGET_BYTES = 256000;
const V1_DEFAULT_PAGE_SIZE = 25;
const V1_MAX_PAGE_SIZE = 100;
const V1_CORRECTION_LOOKBACK_DAYS = 730;

function v1_request_path(): ?string {
    if (isset($_GET['_route'])) {
        $route = trim((string)$_GET['_route']);
        if (strpos($route, '/api/v1') === 0) {
            $rest = substr($route, strlen('/api/v1'));
            return $rest === '' ? '/' : '/' . ltrim($rest, '/');
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
        $marker = '/api/v1';
        $position = strpos($candidate, $marker);
        if ($position === false) {
            continue;
        }
        $rest = substr($candidate, $position + strlen($marker));
        if ($rest !== '' && substr($rest, 0, 1) !== '/') {
            continue;
        }
        return $rest === '' ? '/' : $rest;
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

function handle_v1_request(string $method, string $path, array $config): void {
    $path = '/' . trim(rawurldecode($path), '/');
    if ($method === 'GET' && strpos($path, '/ops/') !== 0 && strpos($path, '/admin/') !== 0) {
        header('Cache-Control: public, max-age=60, stale-while-revalidate=300');
    }
    if ($path === '/') {
        v1_respond(200, array(
            'ok' => true,
            'service' => 'bside-governance-intelligence',
            'documentation' => '/api/v1/openapi.yaml',
        ));
    }
    if ($method === 'GET' && $path === '/health') {
        v1_respond(200, array('ok' => true, 'service' => 'bside-governance-intelligence', 'time' => gmdate('c')));
    }
    if ($method === 'GET' && ($path === '/openapi.yaml' || $path === '/openapi.json')) {
        v1_serve_openapi($path);
    }

    if ($method === 'GET' && ($path === '/ops/health' || $path === '/ops/runtime-state')) {
        v1_require_role($config, array('ops'));
    } elseif (strpos($path, '/admin/') === 0) {
        if ($method !== 'GET' && $method !== 'POST') {
            header('Allow: GET, POST');
            v1_respond(405, array('ok' => false, 'error' => 'method_not_allowed'));
        }
    } elseif ($method !== 'GET' && !($method === 'POST' && $path === '/feedback')) {
        header('Allow: GET, POST');
        v1_respond(405, array('ok' => false, 'error' => 'method_not_allowed'));
    }

    $pdo = pdo_conn($config);
    ensure_schema($pdo, $config);

    if ($method === 'GET') {
        if ($path === '/companies') { v1_list_companies($pdo, $config); }
        if (preg_match('#^/companies/([0-9]{8})$#', $path, $m)) { v1_get_company($pdo, $config, $m[1]); }
        if ($path === '/events') { v1_list_events($pdo, $config); }
        if (preg_match('#^/events/([A-Za-z0-9_.:\-]{1,96})$#', $path, $m)) { v1_get_event($pdo, $config, $m[1]); }
        if (preg_match('#^/campaigns/([A-Za-z0-9_.:\-]{1,96})$#', $path, $m)) { v1_get_campaign($pdo, $config, $m[1]); }
        if (preg_match('#^/documents/([A-Za-z0-9_.:\-]{1,96})$#', $path, $m)) { v1_get_document($pdo, $config, $m[1]); }
        if ($path === '/calendar') { v1_calendar($pdo, $config); }
        if ($path === '/search') { v1_search($pdo, $config); }
        if ($path === '/exports/events.json') { v1_export_events_json($pdo, $config); }
        if ($path === '/exports/events.csv') { v1_export_events_csv($pdo, $config); }
        if ($path === '/feeds/events.atom') { v1_events_atom($pdo, $config); }
        if ($path === '/ops/health') { v1_ops_health($pdo, $config); }
        if ($path === '/ops/runtime-state') { v1_runtime_state_route($pdo, $config); }
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

function v1_document_visibility_sql(string $documentAlias = 'd', string $rightsAlias = 'sr'): string {
    return '(' . $documentAlias . '.publication_status = \'published\' AND ('
        . '(' . $documentAlias . '.source_right_id IS NULL AND ' . $documentAlias . '.source_class NOT IN (\'licensed_telegram\',\'authorized_telegram\'))'
        . ' OR (' . $rightsAlias . '.source_right_id IS NOT NULL'
        . ' AND ' . $rightsAlias . '.status = \'active\''
        . ' AND ' . $rightsAlias . '.redistribution_allowed = 1'
        . ' AND ' . $rightsAlias . '.valid_from <= UTC_TIMESTAMP()'
        . ' AND (' . $rightsAlias . '.valid_until IS NULL OR ' . $rightsAlias . '.valid_until > UTC_TIMESTAMP())'
        . ' AND ' . $rightsAlias . '.revoked_at IS NULL'
        . ' AND (NULLIF(TRIM(' . $rightsAlias . '.evidence_uri), \'\') IS NOT NULL'
        . ' OR NULLIF(TRIM(' . $rightsAlias . '.evidence_hash), \'\') IS NOT NULL))'
        . '))';
}

/** Published events require at least one currently publishable evidence document. */
function v1_event_visibility_sql(array $config, string $eventAlias = 'e'): string {
    $links = table_name($config, 'event_documents');
    $documents = table_name($config, 'documents');
    $rights = table_name($config, 'source_rights');
    return '(' . $eventAlias . '.publication_status = \'published\''
        . ' AND ' . $eventAlias . '.review_status IN (\'approved\',\'not_required\')'
        . ' AND (' . $eventAlias . '.importance NOT IN (\'high\',\'critical\',\'market_sensitive\') OR ' . $eventAlias . '.review_status = \'approved\')'
        . ' AND (' . $eventAlias . '.verification_status <> \'withdrawn\' OR ' . $eventAlias . '.review_status = \'approved\')'
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

function v1_list_companies(PDO $pdo, array $config): void {
    $page = v1_list_params();
    $query = isset($_GET['q']) ? trim((string)$_GET['q']) : '';
    $market = isset($_GET['market']) ? trim((string)$_GET['market']) : '';
    if ($query !== '' && mb_strlen($query, 'UTF-8') < 2) {
        v1_respond(400, array('ok' => false, 'error' => 'query_too_short'));
    }
    $where = array('c.record_status = \'active\'');
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
    $sql = 'SELECT c.company_id, c.stock_code, c.market, c.legal_name, c.legal_name_en, c.short_name, c.aliases_json, c.homepage_url, '
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
    $stmt = $pdo->prepare('SELECT company_id, stock_code, market, legal_name, legal_name_en, short_name, aliases_json, homepage_url, updated_at '
        . 'FROM ' . table_name($config, 'companies') . ' WHERE company_id = ? AND record_status = \'active\' LIMIT 1');
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

function v1_event_query_parts(array $config): array {
    $where = array(v1_event_visibility_sql($config, 'e'));
    $params = array();
    $companyId = isset($_GET['company_id']) ? trim((string)$_GET['company_id']) : '';
    $eventType = isset($_GET['event_type']) ? trim((string)$_GET['event_type']) : '';
    $verification = isset($_GET['verification_status']) ? trim((string)$_GET['verification_status']) : '';
    $importance = isset($_GET['importance']) ? trim((string)$_GET['importance']) : '';
    $from = isset($_GET['from']) ? trim((string)$_GET['from']) : '';
    $to = isset($_GET['to']) ? trim((string)$_GET['to']) : '';
    if ($companyId !== '') {
        if (!preg_match('/^[0-9]{8}$/', $companyId)) { v1_respond(400, array('ok' => false, 'error' => 'invalid_company_id')); }
        $where[] = 'e.company_id = ?'; $params[] = $companyId;
    }
    foreach (array('event_type' => $eventType, 'verification_status' => $verification, 'importance' => $importance) as $field => $value) {
        if ($value === '') { continue; }
        if (!preg_match('/^[A-Za-z0-9_.:\-]{1,64}$/', $value)) { v1_respond(400, array('ok' => false, 'error' => 'invalid_' . $field)); }
        $where[] = 'e.' . $field . ' = ?'; $params[] = $value;
    }
    if ($from !== '') {
        $dt = mysql_dt($from);
        if ($dt === null) { v1_respond(400, array('ok' => false, 'error' => 'invalid_from')); }
        $where[] = 'e.occurred_at >= ?'; $params[] = $dt;
    }
    if ($to !== '') {
        $dt = mysql_dt($to);
        if ($dt === null) { v1_respond(400, array('ok' => false, 'error' => 'invalid_to')); }
        $where[] = 'e.occurred_at <= ?'; $params[] = $dt;
    }
    return array($where, $params);
}

function v1_public_event_select(array $config): string {
    return 'SELECT e.event_id, e.company_id, c.stock_code, c.market, c.legal_name AS company_name, e.event_type, '
        . 'e.title, e.original_language, e.occurred_at, e.deadline_at, e.importance, e.verification_status, e.updated_at '
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

function v1_get_document(PDO $pdo, array $config, string $documentId): void {
    $includeBody = isset($_GET['include']) && (string)$_GET['include'] === 'body';
    $bodyField = $includeBody ? 'LEFT(d.body_text, 100000) AS body_text,' : 'LEFT(d.body_text, 4000) AS body_excerpt,';
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
    $sql = 'SELECT * FROM ('
        . 'SELECT CONCAT(\'event:\', e.event_id) AS calendar_id, \'event\' AS item_type, e.event_id AS entity_id, e.company_id, '
        . 'c.legal_name AS company_name, COALESCE(e.deadline_at, e.occurred_at) AS scheduled_at, e.title, e.original_language, e.event_type AS category '
        . 'FROM ' . table_name($config, 'governance_events') . ' e JOIN ' . table_name($config, 'companies') . ' c ON c.company_id = e.company_id '
        . 'WHERE ' . v1_event_visibility_sql($config, 'e') . ' AND COALESCE(e.deadline_at, e.occurred_at) BETWEEN ? AND ? '
        . 'UNION ALL '
        . 'SELECT CONCAT(\'vote:\', v.proposal_vote_id), \'proposal_vote\', v.proposal_vote_id, v.company_id, c.legal_name, '
        . 'v.meeting_at, v.agenda_title, v.original_language, \'proposal_vote\' '
        . 'FROM ' . table_name($config, 'proposal_votes') . ' v JOIN ' . table_name($config, 'companies') . ' c ON c.company_id = v.company_id '
        . 'WHERE v.review_status = \'approved\' AND v.publication_status = \'published\' AND v.meeting_at BETWEEN ? AND ? AND '
        . v1_required_document_visibility_sql($config, 'v.evidence_document_id')
        . ') calendar_items ORDER BY scheduled_at ASC, calendar_id ASC LIMIT ' . ((int)$page['limit'] + 1)
        . ' OFFSET ' . (int)$page['offset'];
    $stmt = $pdo->prepare($sql); $stmt->execute(array($start, $end, $start, $end));
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
    $sql = 'SELECT * FROM ('
        . 'SELECT \'company\' AS kind, c.company_id AS entity_id, c.legal_name AS title, c.legal_name_en AS subtitle, '
        . 'NULL AS company_id, NULL AS occurred_at, c.updated_at AS sort_at '
        . 'FROM ' . table_name($config, 'companies') . ' c WHERE c.record_status = \'active\' '
        . 'AND (c.legal_name LIKE ? OR c.legal_name_en LIKE ? OR c.short_name LIKE ? OR c.stock_code = ?) '
        . 'UNION ALL '
        . 'SELECT \'actor\', a.actor_id, a.display_name, a.actor_type, a.company_id, NULL, a.updated_at '
        . 'FROM ' . table_name($config, 'actors') . ' a WHERE a.review_status = \'approved\' AND a.record_status = \'active\' '
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

function v1_export_events_csv(PDO $pdo, array $config): void {
    $page = v1_list_params();
    list($rows, $hasMore) = v1_query_public_events($pdo, $config, $page);
    $stream = fopen('php://temp', 'w+');
    if ($stream === false) { v1_respond(500, array('ok' => false, 'error' => 'export_failed')); }
    fputcsv($stream, array('event_id', 'company_id', 'stock_code', 'market', 'company_name', 'event_type', 'title', 'original_language', 'occurred_at', 'deadline_at', 'importance', 'verification_status', 'updated_at'));
    foreach ($rows as $row) {
        fputcsv($stream, array(
            $row['event_id'], $row['company_id'], $row['stock_code'], $row['market'], $row['company_name'],
            $row['event_type'], $row['title'], $row['original_language'], $row['occurred_at'], $row['deadline_at'],
            $row['importance'], $row['verification_status'], $row['updated_at'],
        ));
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

function v1_events_atom(PDO $pdo, array $config): void {
    $_GET['limit'] = isset($_GET['limit']) ? min(100, (int)$_GET['limit']) : 50;
    $_GET['page'] = 1;
    $page = v1_list_params();
    list($rows) = v1_query_public_events($pdo, $config, $page);
    $base = isset($config['public_base_url']) ? rtrim((string)$config['public_base_url'], '/') : '';
    $updated = $rows ? (string)$rows[0]['updated_at'] : gmdate('Y-m-d H:i:s');
    $updatedIso = gmdate('c', strtotime($updated . ' UTC'));
    $xml = '<?xml version="1.0" encoding="UTF-8"?>' . "\n";
    $xml .= '<feed xmlns="http://www.w3.org/2005/Atom">' . "\n";
    $xml .= '<id>' . v1_xml(($base !== '' ? $base : 'urn:bside') . '/api/v1/feeds/events.atom') . '</id>' . "\n";
    $xml .= '<title>BSIDE Governance Events</title>' . "\n";
    $xml .= '<updated>' . v1_xml($updatedIso) . '</updated>' . "\n";
    foreach ($rows as $row) {
        $eventUrl = ($base !== '' ? $base : '') . '/api/v1/events/' . rawurlencode((string)$row['event_id']);
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
    if ($entityType !== '' && (!in_array($entityType, array('company', 'event', 'campaign', 'document'), true) || !v1_valid_entity_id($entityId))) {
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
    $lastStmt = $pdo->query('SELECT MAX(finished_at) FROM ' . table_name($config, 'collection_runs') . ' WHERE status IN (\'succeeded\',\'success\')');
    $lastSuccess = $lastStmt->fetchColumn();
    if (!$lastSuccess) {
        $fallback = $pdo->query('SELECT MAX(finished_at) FROM ' . table_name($config, 'runs') . ' WHERE finished_at IS NOT NULL');
        $lastSuccess = $fallback->fetchColumn();
    }
    $pending = scalar_int($pdo, 'SELECT COUNT(*) FROM ' . table_name($config, 'delivery_outbox') . ' WHERE status IN (\'pending\',\'retry\',\'remote_queued\',\'processing\')');
    $oldestStmt = $pdo->query('SELECT MIN(created_at) FROM ' . table_name($config, 'delivery_outbox') . ' WHERE status IN (\'pending\',\'retry\',\'remote_queued\',\'processing\')');
    $oldestPending = $oldestStmt->fetchColumn();
    $dead = scalar_int($pdo, 'SELECT COUNT(*) FROM ' . table_name($config, 'delivery_outbox') . ' WHERE status = \'dead_letter\'');
    $status = $lastSuccess ? 'ok' : 'degraded';
    v1_respond(200, array(
        'ok' => true,
        'status' => $status,
        'last_success_at' => $lastSuccess ?: null,
        'pending_outbox' => $pending,
        'oldest_pending_at' => $oldestPending ?: null,
        'dead_letter_count' => $dead,
        'checked_at' => gmdate('c'),
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
        . 'WHERE tl.review_status IN (\'pending\',\'changes_requested\')'
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
    $now = gmdate('Y-m-d H:i:s');
    $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'source_rights') . ' (source_right_id, source_type, source_key, source_name, '
        . 'permission_scope, evidence_uri, evidence_hash, valid_from, valid_until, revoked_at, ai_allowed, redistribution_allowed, status, notes, created_at, updated_at) '
        . 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE source_type=VALUES(source_type), source_key=VALUES(source_key), '
        . 'source_name=VALUES(source_name), permission_scope=VALUES(permission_scope), evidence_uri=VALUES(evidence_uri), evidence_hash=VALUES(evidence_hash), '
        . 'valid_from=VALUES(valid_from), valid_until=VALUES(valid_until), revoked_at=VALUES(revoked_at), ai_allowed=VALUES(ai_allowed), '
        . 'redistribution_allowed=VALUES(redistribution_allowed), status=VALUES(status), notes=VALUES(notes), updated_at=VALUES(updated_at)');
    $stmt->execute(array(
        $id, $sourceType, mb_substr($sourceKey, 0, 191, 'UTF-8'), mb_substr($sourceName, 0, 255, 'UTF-8'), $scope,
        $evidenceUri !== '' ? mb_substr($evidenceUri, 0, 65535, 'UTF-8') : null,
        $evidenceHash ?: null, $validFrom, $validUntil, $revokedAt, v1_bool_int(isset($payload['ai_allowed']) ? $payload['ai_allowed'] : false),
        v1_bool_int(isset($payload['redistribution_allowed']) ? $payload['redistribution_allowed'] : false), $status,
        isset($payload['notes']) ? mb_substr((string)$payload['notes'], 0, 65535, 'UTF-8') : 'updated_by:' . $role,
        $now, $now,
    ));
    v1_respond(200, array('ok' => true, 'source_right_id' => $id, 'status' => $status));
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
        $select = $pdo->prepare('SELECT review_status, publication_status, verification_status, updated_at FROM ' . table_name($config, 'governance_events') . ' WHERE event_id = ? FOR UPDATE');
        $select->execute(array($eventId)); $before = $select->fetch();
        if (!$before) { $pdo->rollBack(); v1_respond(404, array('ok' => false, 'error' => 'event_not_found')); }
        if (!hash_equals((string)$before['updated_at'], $expectedUpdatedAt)) {
            $pdo->rollBack(); v1_respond(409, array('ok' => false, 'error' => 'stale_review'));
        }
        if ($decision === 'approve' && !in_array((string)$before['verification_status'], array('official', 'confirmed', 'corroborated', 'corrected', 'withdrawn'), true)) {
            $pdo->rollBack();
            v1_respond(409, array('ok' => false, 'error' => 'verified_evidence_required_before_publication'));
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

/**
 * HMAC action contract: ?action=upsert_governance_snapshot
 * payload={companies,documents,events,source_rights,run}.
 */
function upsert_governance_snapshot(PDO $pdo, array $config, array $payload): void {
    $companies = isset($payload['companies']) && is_array($payload['companies']) ? $payload['companies'] : array();
    $documents = isset($payload['documents']) && is_array($payload['documents']) ? $payload['documents'] : array();
    $events = isset($payload['events']) && is_array($payload['events']) ? $payload['events'] : array();
    $rights = isset($payload['source_rights']) && is_array($payload['source_rights']) ? $payload['source_rights'] : array();
    $run = isset($payload['run']) && is_array($payload['run']) ? $payload['run'] : array();
    if (count($companies) > 2000 || count($documents) > 2500 || count($events) > 2500 || count($rights) > 1000) {
        respond(413, array('ok' => false, 'error' => 'too_many_records'));
    }
    $now = gmdate('Y-m-d H:i:s');
    $counts = array('companies' => 0, 'documents' => 0, 'events' => 0, 'source_rights' => 0, 'source_rights_rejected' => 0,
        'event_documents' => 0, 'timeline_entries' => 0, 'editorial_revisions' => 0, 'correction_link_ambiguous' => 0,
        'event_link_ambiguous' => 0, 'runs' => 0);
    $followupDocumentIds = array();
    $documentSourceClasses = array();
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
        $companyStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'companies') . ' (company_id, stock_code, market, legal_name, legal_name_en, short_name, aliases_json, homepage_url, record_status, created_at, updated_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE stock_code=COALESCE(NULLIF(VALUES(stock_code),\'\'),stock_code), '
            . 'market=COALESCE(NULLIF(VALUES(market),\'\'),market), legal_name=COALESCE(NULLIF(VALUES(legal_name),\'\'),legal_name), '
            . 'legal_name_en=COALESCE(NULLIF(VALUES(legal_name_en),\'\'),legal_name_en), short_name=COALESCE(NULLIF(VALUES(short_name),\'\'),short_name), '
            . 'aliases_json=IF(VALUES(aliases_json) IS NULL OR VALUES(aliases_json)=\'[]\',aliases_json,VALUES(aliases_json)), '
            . 'homepage_url=COALESCE(NULLIF(VALUES(homepage_url),\'\'),homepage_url), record_status=VALUES(record_status), updated_at=VALUES(updated_at)');
        foreach ($companies as $company) {
            if (!is_array($company)) { continue; }
            $companyId = trim((string)v1_first($company, array('company_id', 'corp_code'), ''));
            $legalName = trim((string)v1_first($company, array('legal_name', 'corp_name'), ''));
            if (!preg_match('/^[0-9]{8}$/', $companyId) || $legalName === '') { continue; }
            $aliases = array();
            foreach (array_slice(isset($company['aliases']) && is_array($company['aliases']) ? $company['aliases'] : array(), 0, 20) as $alias) {
                $alias = mb_substr(trim((string)$alias), 0, 255, 'UTF-8');
                if ($alias !== '') { $aliases[] = $alias; }
            }
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
                $now, $now,
            ));
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
            . ' WHERE predecessor.company_id=? AND predecessor.source_class=? AND predecessor.collection_key=? AND predecessor.document_id<>?'
            . ' AND COALESCE(predecessor.published_at,predecessor.retrieved_at) BETWEEN DATE_SUB(?, INTERVAL ' . V1_CORRECTION_LOOKBACK_DAYS . ' DAY) AND DATE_ADD(?, INTERVAL 7 DAY)'
            . ' AND NOT EXISTS (SELECT 1 FROM ' . table_name($config, 'documents') . ' successor WHERE successor.correction_of_document_id=predecessor.document_id AND successor.document_id<>?)'
            . ' ORDER BY predecessor.version_no DESC, COALESCE(predecessor.published_at,predecessor.retrieved_at) DESC, predecessor.document_id DESC LIMIT 2 FOR UPDATE');
        $providedPredecessorStmt = $pdo->prepare('SELECT predecessor.document_id, predecessor.version_no FROM ' . table_name($config, 'documents') . ' predecessor'
            . ' WHERE predecessor.document_id=? AND predecessor.company_id=? AND predecessor.source_class=? AND predecessor.collection_key=?'
            . ' AND COALESCE(predecessor.published_at,predecessor.retrieved_at) BETWEEN DATE_SUB(?, INTERVAL ' . V1_CORRECTION_LOOKBACK_DAYS . ' DAY) AND DATE_ADD(?, INTERVAL 7 DAY)'
            . ' AND NOT EXISTS (SELECT 1 FROM ' . table_name($config, 'documents') . ' successor WHERE successor.correction_of_document_id=predecessor.document_id AND successor.document_id<>?)'
            . ' LIMIT 1 FOR UPDATE');
        $existingDocumentLineageStmt = $pdo->prepare('SELECT correction_of_document_id, version_no FROM '
            . table_name($config, 'documents') . ' WHERE document_id=? LIMIT 1 FOR UPDATE');
        foreach ($documents as $document) {
            if (!is_array($document)) { continue; }
            $sourceClass = trim((string)v1_first($document, array('source_class', 'source_category'), 'official_disclosure'));
            if ($sourceClass === 'authorized_telegram') { $sourceClass = 'licensed_telegram'; }
            if (!in_array($sourceClass, array('official_disclosure', 'company_statement', 'activist_statement', 'media_report', 'licensed_telegram', 'editorial_analysis'), true)) { continue; }
            $externalId = trim((string)v1_first($document, array('external_id', 'stable_source_id', 'rcept_no'), ''));
            $title = trim((string)v1_first($document, array('title', 'report_nm'), ''));
            $url = trim((string)v1_first($document, array('original_url', 'url'), ''));
            if ($externalId === '' || $title === '' || !preg_match('#^https?://#i', $url)) { continue; }
            $id = trim((string)v1_first($document, array('document_id'), ''));
            if ($id === '') { $id = v1_stable_id($sourceClass === 'official_disclosure' ? 'dart' : 'doc', $externalId); }
            if (!v1_valid_entity_id($id)) { continue; }
            $documentSourceClasses[$id] = $sourceClass;
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
                    $providedPredecessorStmt->execute(array($correctionOf, $companyId, $sourceClass, $collectionKey, $documentReferenceAt, $documentReferenceAt, $id));
                    $previousDocument = $providedPredecessorStmt->fetch();
                    if (!$previousDocument) { $linkageAmbiguous = true; }
                } else {
                    $previousDocumentStmt->execute(array($companyId, $sourceClass, $collectionKey, $id, $documentReferenceAt, $documentReferenceAt, $id));
                    $candidates = $previousDocumentStmt->fetchAll();
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
            $existingDocumentLineageStmt->execute(array($id));
            $existingLineage = $existingDocumentLineageStmt->fetch();
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
            $counts['documents']++;
        }

        $eventStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'governance_events') . ' (event_id, company_id, event_type, title, original_language, summary, '
            . 'occurred_at, deadline_at, importance, verification_status, review_status, publication_status, collection_key, payload_json, created_at, updated_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE company_id=VALUES(company_id), '
            . 'event_type=IF(VALUES(verification_status)=\'withdrawn\',event_type,VALUES(event_type)), '
            . 'title=IF(VALUES(verification_status)=\'withdrawn\',title,VALUES(title)), '
            . 'original_language=IF(VALUES(verification_status)=\'withdrawn\',original_language,VALUES(original_language)), '
            . 'summary=IF(VALUES(verification_status)=\'withdrawn\',summary,VALUES(summary)), '
            . 'occurred_at=IF(VALUES(verification_status)=\'withdrawn\',occurred_at,VALUES(occurred_at)), '
            . 'deadline_at=IF(VALUES(verification_status)=\'withdrawn\',deadline_at,VALUES(deadline_at)), '
            . 'importance=IF(VALUES(verification_status)=\'withdrawn\',importance,VALUES(importance)), '
            . 'verification_status=VALUES(verification_status), collection_key=VALUES(collection_key), '
            . 'review_status=IF(payload_json<=>VALUES(payload_json),review_status,VALUES(review_status)), '
            . 'publication_status=IF(payload_json<=>VALUES(payload_json),publication_status,VALUES(publication_status)), '
            . 'updated_at=IF(payload_json<=>VALUES(payload_json),updated_at,GREATEST(VALUES(updated_at),DATE_ADD(updated_at, INTERVAL 1 SECOND))), '
            . 'payload_json=VALUES(payload_json)');
        $eventDocumentStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'event_documents') . ' (event_id, document_id, relation_type, position_no, created_at) '
            . 'VALUES (?,?,?,?,?) ON DUPLICATE KEY UPDATE position_no=VALUES(position_no)');
        $eventByIdStmt = $pdo->prepare('SELECT event_id, event_type, title, original_language, summary, occurred_at, deadline_at, importance, verification_status FROM '
            . table_name($config, 'governance_events') . ' WHERE event_id=? AND company_id=? LIMIT 1 FOR UPDATE');
        $previousEventStmt = $pdo->prepare('SELECT event_id, event_type, title, original_language, summary, occurred_at, deadline_at, importance, verification_status FROM '
            . table_name($config, 'governance_events') . ' WHERE company_id=? AND collection_key=? AND event_id<>?'
            . ' AND occurred_at BETWEEN DATE_SUB(?, INTERVAL ' . V1_CORRECTION_LOOKBACK_DAYS . ' DAY) AND DATE_ADD(?, INTERVAL 7 DAY)'
            . ' ORDER BY occurred_at DESC, updated_at DESC, event_id DESC LIMIT 2 FOR UPDATE');
        $eventLifecycleStmt = $pdo->prepare('SELECT verification_status FROM ' . table_name($config, 'governance_events') . ' WHERE event_id=? LIMIT 1 FOR UPDATE');
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
        $documentClassStmt = $pdo->prepare('SELECT source_class FROM ' . table_name($config, 'documents') . ' WHERE document_id=? LIMIT 1');
        foreach ($events as $event) {
            if (!is_array($event)) { continue; }
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
            $canonicalEvent = null;
            if ($isEventFollowup && preg_match('/^[0-9]{8}$/', $companyId) && v1_valid_entity_id($eventId)) {
                $eventByIdStmt->execute(array($eventId, $companyId));
                $canonicalEvent = $eventByIdStmt->fetch();
                if (!$canonicalEvent && $collectionKey !== '' && $occurred !== null) {
                    $previousEventStmt->execute(array($companyId, $collectionKey, $eventId, $occurred, $occurred));
                    $eventCandidates = $previousEventStmt->fetchAll();
                    if (count($eventCandidates) === 1) { $canonicalEvent = $eventCandidates[0]; }
                }
                if ($canonicalEvent) { $eventId = (string)$canonicalEvent['event_id']; }
            }
            if ($isEventFollowup && !$canonicalEvent) {
                $event['event_link_status'] = 'ambiguous_independent';
                $counts['event_link_ambiguous']++;
            }
            if (!v1_valid_entity_id($eventId) || !preg_match('/^[0-9]{8}$/', $companyId) || !preg_match('/^[A-Za-z0-9_.:\-]{1,64}$/', $eventType) || $title === '' || $occurred === null) { continue; }
            $importance = (string)v1_first($event, array('importance'), 'medium');
            if ($importance === 'normal') { $importance = 'medium'; }
            if ($importance === 'market_sensitive') { $importance = 'critical'; }
            if (!in_array($importance, array('low', 'medium', 'high', 'critical'), true)) { $importance = 'medium'; }
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
            $verification = (string)v1_first($event, array('verification_status', 'status'), 'signal');
            if ($verification === 'published') { $verification = 'confirmed'; }
            if ($verification === 'needs_review') { $verification = 'unverified'; }
            if ($verification === 'closed') { $verification = 'confirmed'; }
            $rawDocumentIds = isset($event['document_ids']) && is_array($event['document_ids']) ? $event['document_ids'] : array();
            if (isset($event['document_id'])) { array_unshift($rawDocumentIds, $event['document_id']); }
            $documentIdSet = array();
            foreach ($rawDocumentIds as $rawDocumentId) {
                if (!is_string($rawDocumentId) && !is_int($rawDocumentId)) { continue; }
                $candidateDocumentId = trim((string)$rawDocumentId);
                if (v1_valid_entity_id($candidateDocumentId)) { $documentIdSet[$candidateDocumentId] = true; }
            }
            $documentIds = array_keys($documentIdSet);
            $hasTelegramEvidence = false;
            $hasIndependentEvidence = false;
            foreach ($documentIds as $evidenceDocumentId) {
                $evidenceDocumentId = trim((string)$evidenceDocumentId);
                if (!v1_valid_entity_id($evidenceDocumentId)) { continue; }
                $evidenceClass = isset($documentSourceClasses[$evidenceDocumentId]) ? (string)$documentSourceClasses[$evidenceDocumentId] : '';
                if ($evidenceClass === '') {
                    $documentClassStmt->execute(array($evidenceDocumentId));
                    $evidenceClass = (string)($documentClassStmt->fetchColumn() ?: '');
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
            $requiresReview = $telegramOnly || $evidenceMissing || in_array($importance, array('high', 'critical'), true)
                || !empty($event['review_required']) || $isEventFollowup;
            $previousLifecycle = 'active';
            if ($isCancelled) {
                $eventLifecycleStmt->execute(array($eventId));
                $storedLifecycle = $eventLifecycleStmt->fetchColumn();
                if (is_string($storedLifecycle) && $storedLifecycle !== '') { $previousLifecycle = $storedLifecycle; }
                $verification = 'withdrawn';
            } elseif ($isCorrection) {
                $eventLifecycleStmt->execute(array($eventId));
                $storedLifecycle = $eventLifecycleStmt->fetchColumn();
                if (is_string($storedLifecycle) && $storedLifecycle !== '') { $previousLifecycle = $storedLifecycle; }
                $verification = 'corrected';
            }
            $review = $isEventFollowup ? 'pending' : ($requiresReview ? 'pending' : ($isConfirmed ? 'not_required' : 'pending'));
            $publication = $isEventFollowup ? 'draft' : ((!$requiresReview && $isConfirmed) ? 'published' : 'draft');
            $eventStmt->execute(array(
                $eventId, $companyId, $eventType, mb_substr($title, 0, 700, 'UTF-8'),
                $language, $summary, $occurred,
                $deadline, $importance, mb_substr($verification, 0, 24, 'UTF-8'),
                $review, $publication, $collectionKey ?: null,
                json_value($event), $now, $now,
            ));
            $counts['events']++;
            $position = 0;
            foreach (array_values(array_unique($documentIds)) as $documentId) {
                $documentId = trim((string)$documentId);
                if (!v1_valid_entity_id($documentId)) { continue; }
                $eventDocumentStmt->execute(array($eventId, $documentId, 'evidence', $position, $now));
                $position++; $counts['event_documents']++;
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
                $runStmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'collection_runs') . ' (run_id, pipeline, source_key, status, started_at, finished_at, '
                    . 'fetched_count, resolved_count, accepted_count, error_count, lag_seconds_p95, metrics_json, created_at, updated_at) '
                    . 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE pipeline=VALUES(pipeline), source_key=VALUES(source_key), status=VALUES(status), '
                    . 'started_at=VALUES(started_at), finished_at=VALUES(finished_at), fetched_count=VALUES(fetched_count), resolved_count=VALUES(resolved_count), '
                    . 'accepted_count=VALUES(accepted_count), error_count=VALUES(error_count), lag_seconds_p95=VALUES(lag_seconds_p95), metrics_json=VALUES(metrics_json), updated_at=VALUES(updated_at)');
                $runStmt->execute(array(
                    $runId, mb_substr((string)v1_first($run, array('pipeline'), 'ingest-official'), 0, 64, 'UTF-8'),
                    mb_substr((string)v1_first($run, array('source_key'), ''), 0, 191, 'UTF-8') ?: null,
                    mb_substr((string)v1_first($run, array('status'), 'succeeded'), 0, 24, 'UTF-8'),
                    mysql_dt(v1_first($run, array('started_at'), $now)) ?: $now, mysql_dt(v1_first($run, array('finished_at'), $now)),
                    (int)v1_first($run, array('fetched_count', 'fetched'), count($documents)),
                    (int)v1_first($run, array('resolved_count', 'resolved'), count($documents)),
                    (int)v1_first($run, array('accepted_count', 'accepted'), count($events)),
                    (int)v1_first($run, array('error_count', 'errors'), 0),
                    v1_first($run, array('lag_seconds_p95'), null) !== null ? (int)$run['lag_seconds_p95'] : null,
                    json_value($runMetrics), $now, $now,
                ));
                $counts['runs']++;
            }
        }
        $pdo->commit();
    } catch (Throwable $e) {
        if ($pdo->inTransaction()) { $pdo->rollBack(); }
        throw $e;
    }
    respond(200, array('ok' => true, 'upserted' => $counts));
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
        'documents' => array('document_id', 'SELECT document_id, company_id, source_right_id, source_class, external_id, document_type, original_language, title, original_url, content_hash, collection_key, correction_of_document_id, version_no, published_at, retrieved_at, verification_status, publication_status, updated_at FROM ' . table_name($config, 'documents'), 'updated_at'),
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
    $stmt = $pdo->prepare('INSERT INTO ' . table_name($config, 'link_discoveries') . ' (discovery_id, discovered_url, discovered_url_hash, source, title, status, '
        . 'attempt_count, discovered_at, created_at, updated_at) VALUES (?,?,?,?,?,\'discovered\',0,?,?,?) '
        . 'ON DUPLICATE KEY UPDATE source=COALESCE(VALUES(source),source), title=COALESCE(VALUES(title),title), updated_at=VALUES(updated_at)');
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
        $stmt->execute(array(
            $id, $url, $hash,
            mb_substr((string)v1_first($discovery, array('source'), ''), 0, 191, 'UTF-8') ?: null,
            mb_substr((string)v1_first($discovery, array('title'), ''), 0, 700, 'UTF-8') ?: null,
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
            . 'ORDER BY discovered_at ASC, discovery_id ASC LIMIT ' . $limit . ' FOR UPDATE';
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
            . 'is_public_channel, quality_score, last_message_id, last_collected_at, last_recommendation_checked_at, last_error, payload_json, updated_at) '
            . 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE telegram_channel_id=VALUES(telegram_channel_id), title=VALUES(title), '
            . 'description=VALUES(description), joined=VALUES(joined), enabled=VALUES(enabled), source=VALUES(source), source_type=VALUES(source_type), '
            . 'is_public_channel=VALUES(is_public_channel), quality_score=VALUES(quality_score), last_message_id=GREATEST(last_message_id,VALUES(last_message_id)), '
            . 'last_collected_at=COALESCE(VALUES(last_collected_at),last_collected_at), last_recommendation_checked_at=COALESCE(VALUES(last_recommendation_checked_at),last_recommendation_checked_at), '
            . 'last_error=VALUES(last_error), payload_json=VALUES(payload_json), updated_at=VALUES(updated_at)');
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
        . 'ON DUPLICATE KEY UPDATE posted_at=COALESCE(VALUES(posted_at),posted_at), edited_at=COALESCE(VALUES(edited_at),edited_at), '
        . 'deleted_at=VALUES(deleted_at), collected_at=COALESCE(VALUES(collected_at),collected_at), text=COALESCE(VALUES(text),text), '
        . 'normalized_text=COALESCE(VALUES(normalized_text),normalized_text), views=GREATEST(views,VALUES(views)), forwards=GREATEST(forwards,VALUES(forwards)), '
        . 'replies_count=GREATEST(replies_count,VALUES(replies_count)), message_url=COALESCE(VALUES(message_url),message_url), '
        . 'urls_json=COALESCE(VALUES(urls_json),urls_json), risk_flags_json=COALESCE(VALUES(risk_flags_json),risk_flags_json), '
        . 'raw_json=COALESCE(VALUES(raw_json),raw_json), updated_at=GREATEST(updated_at,VALUES(updated_at))';
    $messageParams = array_merge(array($channelId, $handle, $channelId, $channelId), $aliases);
    $messageStmt = $pdo->prepare($messageSql); $messageStmt->execute($messageParams);

    $matchSql = 'INSERT INTO tmp_bside_canonical_matches (article_id, message_key, match_type, score, reason, channel_handle, telegram_message_id, message_url, updated_at) '
        . 'SELECT tm.article_id, CONCAT(\'id:\', ?, \':\', COALESCE(tm.telegram_message_id,m.telegram_message_id)), tm.match_type, tm.score, tm.reason, ?, '
        . 'COALESCE(tm.telegram_message_id,m.telegram_message_id), tm.message_url, tm.updated_at '
        . 'FROM ' . $matchesTable . ' tm JOIN ' . $messagesTable . ' m ON m.message_key = tm.message_key '
        . 'WHERE (m.telegram_channel_id = ? OR m.channel_handle IN (' . $aliasMarks . ')) AND COALESCE(tm.telegram_message_id,m.telegram_message_id) IS NOT NULL '
        . 'ON DUPLICATE KEY UPDATE score=GREATEST(score,VALUES(score)), reason=COALESCE(VALUES(reason),reason), channel_handle=VALUES(channel_handle), '
        . 'telegram_message_id=VALUES(telegram_message_id), message_url=COALESCE(VALUES(message_url),message_url), updated_at=GREATEST(updated_at,VALUES(updated_at))';
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
