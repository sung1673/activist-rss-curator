<?php
/**
 * Mutating contracts for the BSIDE global terminal.
 *
 * All automated writes remain draft/pending. Only an authenticated editor can
 * complete event identity and publish a frozen brief edition.
 */

function v2_write_invalid(string $message): void {
    throw new InvalidArgumentException($message);
}

function v2_write_assert_keys(array $value, array $allowed, string $location): void {
    foreach (array_keys($value) as $key) {
        if (!is_string($key) || !in_array($key, $allowed, true)) {
            v2_write_invalid($location . ': unknown field');
        }
    }
}

function v2_write_text(
    array $value,
    string $field,
    string $location,
    int $maximum,
    bool $required = true
): ?string {
    if (!array_key_exists($field, $value) || $value[$field] === null) {
        if ($required) {
            v2_write_invalid($location . '.' . $field . ': required');
        }
        return null;
    }
    if (!is_string($value[$field])) {
        v2_write_invalid($location . '.' . $field . ': string required');
    }
    $text = trim((string)$value[$field]);
    if (($required && $text === '') || mb_strlen($text, 'UTF-8') > $maximum) {
        v2_write_invalid($location . '.' . $field . ': invalid length');
    }
    return $text === '' ? null : $text;
}

function v2_write_source_text(
    array $value,
    string $field,
    string $location,
    int $maximum,
    bool $required = true
): ?string {
    if (!array_key_exists($field, $value) || $value[$field] === null) {
        if ($required) {
            v2_write_invalid($location . '.' . $field . ': required');
        }
        return null;
    }
    if (!is_string($value[$field])) {
        v2_write_invalid($location . '.' . $field . ': string required');
    }
    $text = (string)$value[$field];
    if (
        ($required && trim($text) === '')
        || mb_strlen($text, 'UTF-8') > $maximum
    ) {
        v2_write_invalid($location . '.' . $field . ': invalid length');
    }
    return $text === '' ? null : $text;
}

function v2_write_code(
    array $value,
    string $field,
    string $location,
    string $pattern,
    bool $required = true
): ?string {
    $text = v2_write_text($value, $field, $location, 191, $required);
    if ($text !== null && preg_match($pattern, $text) !== 1) {
        v2_write_invalid($location . '.' . $field . ': invalid');
    }
    return $text;
}

function v2_write_timestamp(
    array $value,
    string $field,
    string $location,
    bool $required = true
): ?string {
    $text = v2_write_text($value, $field, $location, 64, $required);
    if ($text === null) {
        return null;
    }
    $normalized = v1_editorial_datetime_utc($text);
    if ($normalized === null) {
        v2_write_invalid($location . '.' . $field . ': explicit UTC offset required');
    }
    return $normalized;
}

function v2_write_https_url(string $value, string $location): string {
    if (strlen($value) > 4096 || preg_match('/[\x00-\x1f\x7f]/', $value) === 1) {
        v2_write_invalid($location . ': invalid URL');
    }
    $parts = parse_url($value);
    if (
        !is_array($parts)
        || !isset($parts['scheme'], $parts['host'])
        || strtolower((string)$parts['scheme']) !== 'https'
        || isset($parts['user'])
        || isset($parts['pass'])
        || isset($parts['fragment'])
    ) {
        v2_write_invalid($location . ': absolute HTTPS URL required');
    }
    if (isset($parts['query']) && (string)$parts['query'] !== '') {
        $query = (string)$parts['query'];
        if (strpos($query, ';') !== false) {
            v2_write_invalid($location . ': ambiguous URL query');
        }
        $seenKeys = array();
        foreach (explode('&', $query) as $segment) {
            if ($segment === '') {
                v2_write_invalid($location . ': ambiguous URL query');
            }
            if (preg_match('/%(?![A-Fa-f0-9]{2})/', $segment) === 1) {
                v2_write_invalid($location . ': malformed URL query');
            }
            $pair = explode('=', $segment, 2);
            $rawKey = (string)$pair[0];
            if (
                $rawKey === ''
                || preg_match('/%(?![A-Fa-f0-9]{2})/', $rawKey) === 1
            ) {
                v2_write_invalid($location . ': malformed URL query key');
            }
            $decodedKey = str_replace('+', ' ', $rawKey);
            for ($decodePass = 0; $decodePass < 3; $decodePass++) {
                if (preg_match('/%(?![A-Fa-f0-9]{2})/', $decodedKey) === 1) {
                    v2_write_invalid($location . ': malformed URL query key');
                }
                $nextKey = rawurldecode($decodedKey);
                if ($nextKey === $decodedKey) {
                    break;
                }
                $decodedKey = $nextKey;
            }
            $normalizedKey = strtolower(trim($decodedKey));
            if (
                $normalizedKey === ''
                || preg_match('/[\x00-\x1f\x7f&=;]/', $normalizedKey) === 1
                || isset($seenKeys[$normalizedKey])
            ) {
                v2_write_invalid($location . ': ambiguous URL query key');
            }
            $seenKeys[$normalizedKey] = true;
            foreach (array(
                'token', 'secret', 'key', 'signature', 'credential',
            ) as $credentialWord) {
                if (strpos($normalizedKey, $credentialWord) !== false) {
                    v2_write_invalid($location . ': credential query is forbidden');
                }
            }
            if (
                strpos($normalizedKey, 'x-amz-') === 0
                || strpos($normalizedKey, 'x-goog-') === 0
            ) {
                v2_write_invalid($location . ': credential query is forbidden');
            }
        }
    }
    return $value;
}

function v2_write_is_list(array $value): bool {
    $expected = 0;
    foreach (array_keys($value) as $key) {
        if ($key !== $expected) {
            return false;
        }
        $expected++;
    }
    return true;
}

function v2_write_canonical_payload_hash(array $payload): string {
    return hash(
        'sha256',
        v1_strict_canonical_json_encode($payload, 'global_ingest_payload_encode_failed')
    );
}

function v2_write_semantic_metadata_node(
    $value,
    int $depth,
    int &$nodeCount,
    bool $root = false
) {
    $nodeCount++;
    if ($depth > 12 || $nodeCount > 5000) {
        v2_write_invalid('record.metadata: canonical metadata budget exceeded');
    }
    if (
        $value === null
        || is_string($value)
        || is_bool($value)
        || is_int($value)
    ) {
        return $value;
    }
    if (is_float($value)) {
        v2_write_invalid(
            'record.metadata: floats are not cross-runtime canonical'
        );
    }
    if (!is_array($value)) {
        v2_write_invalid('record.metadata: JSON values only');
    }
    $isList = v2_write_is_list($value);
    if ($root && count($value) > 0 && $isList) {
        v2_write_invalid('record.metadata: object required');
    }
    if ($root) {
        $result = new stdClass();
        foreach ($value as $key => $child) {
            if (!is_string($key)) {
                v2_write_invalid('record.metadata: string object keys required');
            }
            $result->{$key} = v2_write_semantic_metadata_node(
                $child,
                $depth + 1,
                $nodeCount
            );
        }
        return $result;
    }
    if ($isList) {
        $result = array();
        foreach ($value as $child) {
            $result[] = v2_write_semantic_metadata_node(
                $child,
                $depth + 1,
                $nodeCount
            );
        }
        return $result;
    }
    $result = new stdClass();
    foreach ($value as $key => $child) {
        if (!is_string($key)) {
            v2_write_invalid('record.metadata: string object keys required');
        }
        $result->{$key} = v2_write_semantic_metadata_node(
            $child,
            $depth + 1,
            $nodeCount
        );
    }
    return $result;
}

function v2_global_document_semantic_payload(
    array $record,
    array $right
): array {
    return array(
        'schema_version' => 1,
        'record_id' => $record['record_id'],
        'external_id' => $record['external_id'],
        'issuer_id' => $record['issuer_id'],
        'issuer_reference' => array(
            'namespace' => $record['namespace'],
            'identifier_type' => $record['identifier_type'],
            'value' => $record['identifier_value'],
            'legal_name' => $record['legal_name'],
            'market' => $record['market'],
            'ticker' => $record['ticker'] === null ? '' : $record['ticker'],
        ),
        'country_code' => $record['country_code'],
        'source_key' => $record['source_key'],
        'source_right_id' => $record['source_right_id'],
        'source_type' => (string)$right['source_type'],
        'record_kind' => $record['record_kind'],
        'document_type' => $record['document_type'],
        'event_family' => $record['event_family'],
        'title' => $record['title'],
        'original_language' => $record['original_language'],
        'filed_at' => str_replace(' ', 'T', $record['filed_at']) . '+00:00',
        'original_url' => $record['original_url'],
        'body_text' => $record['body_text'],
        'correction_of_external_id' => $record['correction_of_external_id'],
        'change_type' => $record['change_type'],
        'metadata' => $record['metadata'],
        'public_allowed' => $record['public_allowed'],
        'ai_allowed' => $record['ai_allowed'],
    );
}

function v2_global_document_content_hash(
    array $record,
    array $right
): string {
    return v2_write_canonical_payload_hash(
        v2_global_document_semantic_payload($record, $right)
    );
}

/**
 * Hash the logical ingest request, excluding only attempt-time observations.
 *
 * A retry may be assembled later than the original HTTP attempt, so
 * envelope.retrieved_at, transport request counts, and each
 * record.first_observed_at are deliberately excluded. Rights, source-row
 * counts, cursor, records, content, and lifecycle fields remain covered;
 * changing any substantive field is still a conflict.
 */
function v2_write_ingest_idempotency_hash(array $payload): string {
    $semantic = $payload;
    // Execution policy is not part of the source payload identity. A
    // replay-only request must match the receipt created by the original
    // apply request without being allowed to create a replacement receipt.
    unset($semantic['ingest_mode']);
    unset($semantic['expected_release_state']);
    if (
        isset($semantic['envelope'])
        && is_array($semantic['envelope'])
    ) {
        unset($semantic['envelope']['retrieved_at']);
        unset($semantic['envelope']['request_count']);
        if (
            isset($semantic['envelope']['chunk'])
            && is_array($semantic['envelope']['chunk'])
        ) {
            unset($semantic['envelope']['chunk']['batch_request_count']);
        }
        if (
            isset($semantic['envelope']['records'])
            && is_array($semantic['envelope']['records'])
        ) {
            foreach ($semantic['envelope']['records'] as &$record) {
                if (is_array($record)) {
                    unset($record['first_observed_at']);
                }
            }
            unset($record);
        }
    }
    return v2_write_canonical_payload_hash($semantic);
}

/**
 * Recompute the Python content-idempotency digest for a classified SEC poll.
 *
 * The namespace is evidence-significant, so accepting a caller-selected
 * prefix would let an ops credential relabel hybrid data as completed-day
 * evidence. Keep this byte contract aligned with content_idempotency_key().
 */
function v2_write_expected_classified_ingest_key(
    array $payload,
    string $codeRevision,
    string $country,
    array $chunk,
    array $normalizedRecords,
    string $namespace
): string {
    $stableEnvelope = $payload['envelope'];
    unset($stableEnvelope['retrieved_at']);
    unset($stableEnvelope['request_count']);
    unset($stableEnvelope['chunk']);
    if (
        isset($stableEnvelope['records'])
        && is_array($stableEnvelope['records'])
    ) {
        foreach ($stableEnvelope['records'] as $index => &$record) {
            if (is_array($record)) {
                unset($record['first_observed_at']);
                if (
                    isset($normalizedRecords[$index])
                    && is_array($normalizedRecords[$index])
                    && isset($normalizedRecords[$index]['metadata'])
                ) {
                    // json_decode(..., true) cannot distinguish an empty JSON
                    // object from an empty list. The normalized metadata tree
                    // restores stdClass object nodes for cross-runtime parity.
                    $record['metadata'] =
                        $normalizedRecords[$index]['metadata'];
                }
            }
        }
        unset($record);
    }
    $content = array(
        'code_revision' => $codeRevision,
        'window_start' => (string)$chunk['window_start'],
        'window_end_exclusive' => (string)$chunk['window_end_exclusive'],
        'chunk_index' => (int)$chunk['index'] - 1,
        'envelope' => $stableEnvelope,
    );
    return $namespace . ':'
        . strtolower($country) . ':'
        . hash(
            'sha256',
            v1_strict_canonical_json_encode(
                $content,
                'global_ingest_classified_key_encode_failed'
            )
        );
}

function v2_write_valid_sec_current_cursor($value): bool {
    if (!is_string($value)) {
        return false;
    }
    $prefix = 'sec-current-v1:';
    if (strpos($value, $prefix) !== 0) {
        return false;
    }
    $encoded = substr($value, strlen($prefix));
    if (
        $encoded === ''
        || strlen($encoded) > 1000
        || preg_match('/^[A-Za-z0-9_-]+$/D', $encoded) !== 1
        || strlen($encoded) % 4 === 1
    ) {
        return false;
    }
    $standard = strtr($encoded, '-_', '+/');
    $standard .= str_repeat('=', (4 - strlen($standard) % 4) % 4);
    $decoded = base64_decode($standard, true);
    if (
        $decoded === false
        || strlen($decoded) > 512
        || rtrim(
            strtr(base64_encode($decoded), '+/', '-_'),
            '='
        ) !== $encoded
    ) {
        return false;
    }
    $payload = json_decode($decoded, true);
    if (
        json_last_error() !== JSON_ERROR_NONE
        || !is_array($payload)
        || !v2_exact_string_keys(
            $payload,
            array('schema_version', 'updated_at')
        )
        || !isset($payload['schema_version'])
        || $payload['schema_version'] !== 1
        || !isset($payload['updated_at'])
        || !is_string($payload['updated_at'])
        || preg_match(
            '/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00$/D',
            (string)$payload['updated_at']
        ) !== 1
    ) {
        return false;
    }
    $parsed = DateTimeImmutable::createFromFormat(
        '!Y-m-d\TH:i:sP',
        (string)$payload['updated_at'],
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
        || $parsed->format('Y-m-d\TH:i:sP')
            !== (string)$payload['updated_at']
    ) {
        return false;
    }
    $canonical = '{"schema_version":1,"updated_at":"'
        . (string)$payload['updated_at'] . '"}';
    return hash_equals($canonical, $decoded);
}

function v2_source_right_row(
    PDO $pdo,
    array $config,
    string $sourceRightId,
    bool $forUpdate = false
): ?array {
    $statement = $pdo->prepare(
        'SELECT source_right_id,source_type,source_key,source_name,permission_scope,'
        . 'evidence_uri,evidence_hash,valid_from,valid_until,revoked_at,ai_allowed,'
        . 'redistribution_allowed,status,updated_at FROM '
        . table_name($config, 'source_rights')
        . ' WHERE source_right_id=? LIMIT 1'
        . ($forUpdate ? ' FOR UPDATE' : '')
    );
    $row = v1_pdo_fetch_one_and_close($statement,array($sourceRightId));
    return is_array($row) ? $row : null;
}

function v2_source_right_revision(array $right): string {
    $payload = array(
        'source_right_id' => (string)$right['source_right_id'],
        'source_type' => (string)$right['source_type'],
        'source_key' => (string)$right['source_key'],
        'permission_scope_sha256' => hash(
            'sha256',
            (string)$right['permission_scope']
        ),
        'evidence_present' => (
            trim((string)$right['evidence_uri']) !== ''
            || preg_match('/^[a-f0-9]{64}$/i', (string)$right['evidence_hash']) === 1
        ),
        'valid_from' => (string)$right['valid_from'],
        'valid_until' => $right['valid_until'],
        'revoked_at' => $right['revoked_at'],
        'ai_allowed' => (int)$right['ai_allowed'],
        'redistribution_allowed' => (int)$right['redistribution_allowed'],
        'status' => (string)$right['status'],
        'updated_at' => (string)$right['updated_at'],
    );
    return hash(
        'sha256',
        v1_strict_canonical_json_encode(
            $payload,
            'global_source_right_revision_encode_failed'
        )
    );
}

/**
 * Stable semantic digest for an approved SourceRight contract.
 *
 * Unlike rights_revision, this deliberately excludes updated_at so an
 * idempotent administrative write does not change the contract identity.
 * valid_from is normalized to its current eligibility state because the
 * protected bootstrap chooses that timestamp from the server clock.
 */
function v2_source_right_contract_revision(array $right): string {
    $evidenceUri = (string)$right['evidence_uri'];
    $evidenceHash = strtolower(trim((string)$right['evidence_hash']));
    $payload = array(
        'contract_version' => 1,
        'source_right_id' => (string)$right['source_right_id'],
        'source_type' => (string)$right['source_type'],
        'source_key' => (string)$right['source_key'],
        'source_name' => (string)$right['source_name'],
        'permission_scope_sha256' => hash(
            'sha256',
            (string)$right['permission_scope']
        ),
        'evidence_uri_sha256' => $evidenceUri === ''
            ? null : hash('sha256', $evidenceUri),
        'evidence_hash' => $evidenceHash === '' ? null : $evidenceHash,
        'valid_from_state' => (string)$right['valid_from'] <= gmdate('Y-m-d H:i:s')
            ? 'eligible' : 'future',
        'valid_until' => $right['valid_until'],
        'revoked_at' => $right['revoked_at'],
        'ai_allowed' => (int)$right['ai_allowed'] === 1,
        'redistribution_allowed' => (int)$right['redistribution_allowed'] === 1,
        'status' => (string)$right['status'],
    );
    return hash(
        'sha256',
        v1_strict_canonical_json_encode(
            $payload,
            'global_source_right_contract_revision_encode_failed'
        )
    );
}

function v2_source_right_ineligible_reasons(?array $right, string $use): array {
    if ($right === null) {
        return array('not_registered');
    }
    $reasons = array();
    if ((string)$right['status'] !== 'active') {
        $reasons[] = 'not_active';
    }
    if (trim((string)$right['permission_scope']) === '') {
        $reasons[] = 'permission_scope_missing';
    }
    if (
        trim((string)$right['evidence_uri']) === ''
        && preg_match('/^[a-f0-9]{64}$/i', (string)$right['evidence_hash']) !== 1
    ) {
        $reasons[] = 'evidence_missing';
    }
    $now = gmdate('Y-m-d H:i:s');
    if ((string)$right['valid_from'] > $now) {
        $reasons[] = 'not_yet_valid';
    }
    if ($right['valid_until'] !== null && (string)$right['valid_until'] <= $now) {
        $reasons[] = 'expired';
    }
    if ($right['revoked_at'] !== null && (string)$right['revoked_at'] <= $now) {
        $reasons[] = 'revoked';
    }
    if ($use === 'public' && (int)$right['redistribution_allowed'] !== 1) {
        $reasons[] = 'redistribution_not_allowed';
    }
    if ($use === 'ai' && (int)$right['ai_allowed'] !== 1) {
        $reasons[] = 'ai_not_allowed';
    }
    return array_values(array_unique($reasons));
}

function v2_admin_connector_view(
    array $connector,
    ?array $right
): array {
    $reasons = v2_source_right_ineligible_reasons($right, 'collect');
    $identityMatch = (
        $right !== null
        && (string)$connector['source_right_id']
            === (string)$right['source_right_id']
        && (string)$connector['source_type'] === (string)$right['source_type']
        && (string)$connector['source_key'] === (string)$right['source_key']
    );
    return array(
        'connector_id' => (string)$connector['connector_id'],
        'country_code' => (string)$connector['country_code'],
        'source_key' => (string)$connector['source_key'],
        'source_name' => (string)$connector['source_name'],
        'source_type' => (string)$connector['source_type'],
        'base_url' => (string)$connector['base_url'],
        'source_right_id' => $connector['source_right_id'] === null
            ? null : (string)$connector['source_right_id'],
        'coverage_mode' => (string)$connector['coverage_mode'],
        'connector_status' => (string)$connector['connector_status'],
        'schedule_minutes' => $connector['schedule_minutes'] === null
            ? null : (int)$connector['schedule_minutes'],
        'last_checked_at' => v1_release_iso_time($connector['last_checked_at']),
        'last_success_at' => v1_release_iso_time($connector['last_success_at']),
        'last_error_class' => $connector['last_error_class'] === null
            ? null : (string)$connector['last_error_class'],
        'code_revision' => $connector['code_revision'] === null
            ? null : (string)$connector['code_revision'],
        'updated_at' => v1_release_iso_time($connector['updated_at']),
        'collect_eligibility' => array(
            'eligible' => $identityMatch && count($reasons) === 0,
            'identity_match' => $identityMatch,
            'ineligible_reasons' => $reasons,
            'rights_revision' => $right === null
                ? null : v2_source_right_revision($right),
            'right_status' => $right === null
                ? null : (string)$right['status'],
            'valid_from' => $right === null
                ? null : v1_release_iso_time($right['valid_from']),
            'valid_until' => $right === null
                ? null : v1_release_iso_time($right['valid_until']),
            'revoked_at' => $right === null
                ? null : v1_release_iso_time($right['revoked_at']),
            'redistribution_allowed' => $right !== null
                && (int)$right['redistribution_allowed'] === 1,
            'ai_allowed' => $right !== null
                && (int)$right['ai_allowed'] === 1,
        ),
    );
}

function v2_admin_update_connector(
    PDO $pdo,
    array $config,
    string $connectorId,
    string $role
): void {
    $payload = v2_json_body($config);
    try {
        v2_write_assert_keys($payload, array(
            'target_status', 'expected_updated_at', 'reason',
        ), 'payload');
        $targetStatus = v2_write_code(
            $payload,
            'target_status',
            'payload',
            '/^(configured|inactive)$/'
        );
        $expectedUpdatedAt = v2_write_timestamp(
            $payload,
            'expected_updated_at',
            'payload'
        );
        $reason = v2_write_text($payload, 'reason', 'payload', 1000);
        if ($reason === null || mb_strlen($reason, 'UTF-8') < 8) {
            v2_write_invalid('payload.reason: at least 8 characters required');
        }
    } catch (InvalidArgumentException $error) {
        v2_respond(400, array(
            'ok' => false,
            'error' => 'connector_update_validation_failed',
            'detail' => $error->getMessage(),
        ));
    }

    // Discover only the SourceRight identity before the transaction so the
    // shared lock order can remain release-state -> SourceRight -> connector.
    // The identity is rechecked after the connector row itself is locked.
    $identityStatement = $pdo->prepare(
        'SELECT source_right_id FROM '
        . table_name($config, 'source_connectors')
        . ' WHERE connector_id=? LIMIT 1'
    );
    $identityStatement->execute(array($connectorId));
    $identity = $identityStatement->fetch();
    if (!$identity) {
        v2_respond(404, array(
            'ok' => false,
            'error' => 'connector_not_found',
        ));
    }
    $expectedSourceRightId = $identity['source_right_id'] === null
        ? null : (string)$identity['source_right_id'];

    $pdo->beginTransaction();
    try {
        v2_release_state_rows_for_update($pdo, $config);
        $right = $expectedSourceRightId === null
            ? null : v2_source_right_row(
                $pdo,
                $config,
                $expectedSourceRightId,
                true
            );
        $statement = $pdo->prepare(
            'SELECT connector_id,country_code,source_key,source_name,source_type,'
            . 'base_url,source_right_id,coverage_mode,connector_status,'
            . 'schedule_minutes,last_checked_at,last_success_at,last_error_class,'
            . 'code_revision,updated_at FROM '
            . table_name($config, 'source_connectors')
            . ' WHERE connector_id=? LIMIT 1 FOR UPDATE'
        );
        $statement->execute(array($connectorId));
        $connector = $statement->fetch();
        if (!$connector) {
            $pdo->rollBack();
            v2_respond(404, array(
                'ok' => false,
                'error' => 'connector_not_found',
            ));
        }
        $lockedSourceRightId = $connector['source_right_id'] === null
            ? null : (string)$connector['source_right_id'];
        if ($lockedSourceRightId !== $expectedSourceRightId) {
            throw new RuntimeException('connector_source_right_changed');
        }
        if (
            $targetStatus === 'configured'
            && in_array(
                (string)$connector['connector_id'],
                array(
                    'connector:jp:edinet',
                    'connector:gb:companies-house',
                ),
                true
            )
        ) {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'connector_disabled_by_alpha_policy',
                'connector_id' => (string)$connector['connector_id'],
                'country' => (string)$connector['country_code'],
            ));
        }
        if ((string)$connector['updated_at'] !== $expectedUpdatedAt) {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'stale_connector_update',
                'current_updated_at' => v1_release_iso_time(
                    $connector['updated_at']
                ),
            ));
        }
        $view = v2_admin_connector_view($connector, $right);
        if (
            $targetStatus === 'configured'
            && $view['collect_eligibility']['eligible'] !== true
        ) {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'connector_source_right_ineligible',
                'collect_eligibility' => $view['collect_eligibility'],
            ));
        }
        $now = gmdate('Y-m-d H:i:s');
        if ($now <= $expectedUpdatedAt) {
            $expectedEpoch = strtotime($expectedUpdatedAt . ' UTC');
            if ($expectedEpoch === false) {
                throw new RuntimeException('invalid_connector_update_clock');
            }
            $now = gmdate('Y-m-d H:i:s', $expectedEpoch + 1);
        }
        $previousStatus = (string)$connector['connector_status'];
        $update = $pdo->prepare(
            'UPDATE ' . table_name($config, 'source_connectors')
            . ' SET connector_status=?,last_error_class=?,updated_at=?'
            . ' WHERE connector_id=? AND updated_at=?'
        );
        $update->execute(array(
            $targetStatus,
            $targetStatus === 'configured' ? null : 'admin_inactive',
            $now,
            $connectorId,
            $expectedUpdatedAt,
        ));
        $auditId = 'connector-audit:' . substr(hash(
            'sha256',
            $connectorId . "\x1f" . $previousStatus . "\x1f"
                . $targetStatus . "\x1f" . $reason . "\x1f"
                . v1_release_request_id() . "\x1f" . gmdate('c')
        ), 0, 64);
        $audit = $pdo->prepare(
            'INSERT INTO ' . table_name($config, 'global_connector_audit')
            . ' (audit_id,connector_id,previous_status,new_status,reason,'
            . 'changed_by,created_at) VALUES (?,?,?,?,?,?,?)'
        );
        $audit->execute(array(
            $auditId,
            $connectorId,
            $previousStatus,
            $targetStatus,
            $reason,
            $role,
            $now,
        ));
        $connector['connector_status'] = $targetStatus;
        $connector['last_error_class'] = $targetStatus === 'configured'
            ? null : 'admin_inactive';
        $connector['updated_at'] = $now;
        $result = v2_admin_connector_view($connector, $right);
        $result['previous_status'] = $previousStatus;
        $result['changed'] = $previousStatus !== $targetStatus;
        $result['audit_id'] = $auditId;
        $pdo->commit();
        v2_respond(200, array('ok' => true, 'data' => $result));
    } catch (Throwable $error) {
        if ($pdo->inTransaction()) {
            $pdo->rollBack();
        }
        throw $error;
    }
}

function v2_global_issuer_id(
    string $country,
    string $namespace,
    string $identifier
): string {
    $countryPart = strtolower($country);
    $namespaceParts = preg_split(
        '/[^a-z0-9]+/',
        mb_strtolower(trim($namespace), 'UTF-8'),
        -1,
        PREG_SPLIT_NO_EMPTY
    );
    $filtered = array();
    foreach (is_array($namespaceParts) ? $namespaceParts : array() as $part) {
        if ($part !== $countryPart) {
            $filtered[] = $part;
        }
    }
    $namespacePart = implode('-', $filtered);
    $identifierPart = preg_replace(
        '/\s+/u',
        '',
        mb_strtolower(trim($identifier), 'UTF-8')
    );
    if (!is_string($identifierPart)) {
        $identifierPart = '';
    }
    if ($namespacePart === '' || $identifierPart === '') {
        v2_write_invalid('issuer_reference: namespace and value required');
    }
    if (preg_match('/^[a-z0-9_.-]{1,40}$/', $identifierPart) !== 1) {
        $identifierPart = substr(hash('sha256', $identifierPart), 0, 32);
    }
    $issuerId = 'issuer:' . $countryPart . ':' . $namespacePart . ':' . $identifierPart;
    if (!v1_valid_entity_id($issuerId, 96)) {
        v2_write_invalid('issuer_reference: derived issuer_id is invalid');
    }
    return $issuerId;
}

function v2_normalize_ingest_record(
    array $record,
    int $index,
    array $connector,
    array $right
): array {
    $location = 'envelope.records[' . $index . ']';
    v2_write_assert_keys($record, array(
        'record_id', 'external_id', 'issuer_id', 'issuer_reference',
        'country_code', 'source_key', 'source_right_id', 'record_kind',
        'document_type', 'event_family', 'title', 'original_language',
        'filed_at', 'first_observed_at', 'original_url', 'content_hash',
        'body_text', 'correction_of_external_id', 'change_type', 'metadata',
    ), $location);
    $country = v2_write_code(
        $record,
        'country_code',
        $location,
        '/^(KR|US|JP|GB|CA|AU)$/'
    );
    if ($country !== (string)$connector['country_code']) {
        v2_write_invalid($location . '.country_code: connector mismatch');
    }
    $issuerReference = isset($record['issuer_reference'])
        && is_array($record['issuer_reference'])
        && !v2_write_is_list($record['issuer_reference'])
        ? $record['issuer_reference'] : null;
    if ($issuerReference === null) {
        v2_write_invalid($location . '.issuer_reference: object required');
    }
    v2_write_assert_keys($issuerReference, array(
        'namespace', 'identifier_type', 'value', 'legal_name', 'market', 'ticker',
    ), $location . '.issuer_reference');
    $namespace = v2_write_code(
        $issuerReference,
        'namespace',
        $location . '.issuer_reference',
        '/^[A-Z][A-Z0-9_:.\-]{1,63}$/'
    );
    $identifierType = v2_write_code(
        $issuerReference,
        'identifier_type',
        $location . '.issuer_reference',
        '/^[A-Z][A-Z0-9_]{1,39}$/'
    );
    $identifierValue = v2_write_text(
        $issuerReference,
        'value',
        $location . '.issuer_reference',
        191
    );
    $legalName = v2_write_source_text(
        $issuerReference,
        'legal_name',
        $location . '.issuer_reference',
        255
    );
    $market = v2_write_text(
        $issuerReference,
        'market',
        $location . '.issuer_reference',
        40,
        false
    );
    $ticker = v2_write_text(
        $issuerReference,
        'ticker',
        $location . '.issuer_reference',
        24,
        false
    );
    $issuerId = v2_write_code(
        $record,
        'issuer_id',
        $location,
        '/^issuer:[a-z]{2}:[a-z0-9][a-z0-9\-]{0,31}:[a-z0-9_.\-]{1,40}$/'
    );
    $derivedIssuerId = v2_global_issuer_id(
        (string)$country,
        (string)$namespace,
        (string)$identifierValue
    );
    if (!hash_equals($derivedIssuerId, (string)$issuerId)) {
        v2_write_invalid($location . '.issuer_id: derived identity mismatch');
    }
    $recordId = v2_write_code(
        $record,
        'record_id',
        $location,
        '/^globaldoc:[a-f0-9]{40}$/'
    );
    $externalId = v2_write_text($record, 'external_id', $location, 191);
    $expectedRecordId = 'globaldoc:' . substr(
        hash(
            'sha256',
            (string)$connector['connector_id'] . "\x1f"
            . (string)$issuerId . "\x1f" . (string)$externalId
        ),
        0,
        40
    );
    if (!hash_equals($expectedRecordId, (string)$recordId)) {
        v2_write_invalid($location . '.record_id: stable identity mismatch');
    }
    $sourceKey = v2_write_text($record, 'source_key', $location, 191);
    $sourceRightId = v2_write_code(
        $record,
        'source_right_id',
        $location,
        '/^official:[a-z0-9_.:\-]{1,48}$/'
    );
    if (
        !hash_equals((string)$connector['source_key'], (string)$sourceKey)
        || !hash_equals((string)$right['source_right_id'], (string)$sourceRightId)
    ) {
        v2_write_invalid($location . ': source identity mismatch');
    }
    $recordKind = v2_write_code(
        $record,
        'record_kind',
        $location,
        '/^(disclosure|registry_filing|link)$/'
    );
    $documentType = v2_write_text($record, 'document_type', $location, 80);
    $eventFamily = v2_write_code(
        $record,
        'event_family',
        $location,
        '/^(large_ownership|meeting_and_vote|tender_offer_and_mna|capital_issuance|capital_return|board_and_compensation|listing_status|correction_and_withdrawal|unclassified)$/'
    );
    $title = v2_write_source_text($record, 'title', $location, 700);
    $language = v2_write_code(
        $record,
        'original_language',
        $location,
        '/^[a-z]{2,3}(?:-[A-Z]{2})?$/'
    );
    $filedAt = v2_write_timestamp($record, 'filed_at', $location);
    $firstObservedAt = v2_write_timestamp(
        $record,
        'first_observed_at',
        $location
    );
    $url = v2_write_https_url(
        (string)v2_write_text($record, 'original_url', $location, 4096),
        $location . '.original_url'
    );
    $contentHash = v2_write_code(
        $record,
        'content_hash',
        $location,
        '/^[a-f0-9]{64}$/'
    );
    $body = v2_write_source_text(
        $record,
        'body_text',
        $location,
        500000,
        false
    );
    $fixedMetadataOnlySourceRights = array(
        'official:sec-edgar',
        'official:edinet',
        'official:companies-house',
        'official:ca-issuer-ir',
        'official:asic-register',
    );
    if (
        in_array(
            (string)$right['source_right_id'],
            $fixedMetadataOnlySourceRights,
            true
        )
        && $body !== null
    ) {
        v2_write_invalid(
            $location
            . '.body_text: fixed Production Alpha source contract requires null'
        );
    }
    if ((int)$right['redistribution_allowed'] !== 1 && $body !== null) {
        v2_write_invalid($location . '.body_text: redistribution is not allowed');
    }
    $correctionExternalId = v2_write_text(
        $record,
        'correction_of_external_id',
        $location,
        191,
        false
    );
    $changeType = v2_write_code(
        $record,
        'change_type',
        $location,
        '/^(new|updated|corrected|withdrawn)$/'
    );
    if (
        !array_key_exists('metadata', $record)
        || !is_array($record['metadata'])
    ) {
        v2_write_invalid($location . '.metadata: object required');
    }
    if (
        !array_key_exists('title_provenance', $record['metadata'])
        || !is_string($record['metadata']['title_provenance'])
        || !in_array(
            $record['metadata']['title_provenance'],
            array('source', 'generated_metadata', 'operator_metadata'),
            true
        )
    ) {
        v2_write_invalid(
            $location . '.metadata.title_provenance: invalid value'
        );
    }
    $metadataNodeCount = 0;
    $metadata = v2_write_semantic_metadata_node(
        $record['metadata'],
        0,
        $metadataNodeCount,
        true
    );
    return array(
        'record_id' => $recordId,
        'external_id' => $externalId,
        'issuer_id' => $issuerId,
        'country_code' => $country,
        'namespace' => $namespace,
        'identifier_type' => $identifierType,
        'identifier_value' => $identifierValue,
        'legal_name' => $legalName,
        'market' => $market === null ? '' : $market,
        'ticker' => $ticker,
        'source_key' => $sourceKey,
        'source_right_id' => $sourceRightId,
        'record_kind' => $recordKind,
        'document_type' => $documentType,
        'event_family' => $eventFamily,
        'title' => $title,
        'original_language' => $language,
        'filed_at' => $filedAt,
        'first_observed_at' => $firstObservedAt,
        'original_url' => $url,
        'content_hash' => $contentHash,
        'body_text' => $body,
        'correction_of_external_id' => $correctionExternalId,
        'change_type' => $changeType,
        'metadata' => $metadata,
    );
}

function v2_normalize_lifecycle_observation(
    array $observation,
    int $index,
    array $connector
): array {
    $location = 'envelope.lifecycle_observations[' . $index . ']';
    v2_write_assert_keys($observation, array(
        'observation_id', 'country_code', 'source_key', 'external_id',
        'parent_external_id', 'change_type', 'observed_at', 'metadata',
    ), $location);
    $observationId = v2_write_code(
        $observation,
        'observation_id',
        $location,
        '/^[A-Za-z0-9_.:\-]{1,96}$/'
    );
    $country = v2_write_code(
        $observation,
        'country_code',
        $location,
        '/^(KR|US|JP|GB|CA|AU)$/'
    );
    $sourceKey = v2_write_text($observation, 'source_key', $location, 191);
    if (
        $country !== (string)$connector['country_code']
        || $sourceKey !== (string)$connector['source_key']
    ) {
        v2_write_invalid($location . ': connector identity mismatch');
    }
    $externalId = v2_write_text($observation, 'external_id', $location, 191);
    $parentExternalId = v2_write_text(
        $observation,
        'parent_external_id',
        $location,
        191,
        false
    );
    $changeType = v2_write_code(
        $observation,
        'change_type',
        $location,
        '/^(updated|corrected|withdrawn)$/'
    );
    $observedAt = v2_write_timestamp(
        $observation,
        'observed_at',
        $location
    );
    $metadata = isset($observation['metadata']) && is_array($observation['metadata'])
        && !v2_write_is_list($observation['metadata'])
        ? $observation['metadata'] : array();
    return array(
        'observation_id' => $observationId,
        'country_code' => $country,
        'source_key' => $sourceKey,
        'external_id' => $externalId,
        'parent_external_id' => $parentExternalId,
        'change_type' => $changeType,
        'observed_at' => $observedAt,
        'metadata' => $metadata,
    );
}

function v2_normalize_ingest_payload(PDO $pdo, array $config, array $payload): array {
    v2_write_assert_keys(
        $payload,
        array(
            'idempotency_key',
            'code_revision',
            'expected_release_state',
            'ingest_mode',
            'envelope',
        ),
        'payload'
    );
    $idempotencyKey = v2_write_code(
        $payload,
        'idempotency_key',
        'payload',
        '/^[A-Za-z0-9_.:\-]{8,191}$/'
    );
    $codeRevision = v2_write_code(
        $payload,
        'code_revision',
        'payload',
        '/^[a-f0-9]{7,64}$/'
    );
    $ingestMode = array_key_exists('ingest_mode', $payload)
        ? v2_write_code(
            $payload,
            'ingest_mode',
            'payload',
            '/^(apply|replay)$/'
        )
        : 'apply';
    $expectedReleaseState = array_key_exists(
        'expected_release_state',
        $payload
    )
        ? v2_write_code(
            $payload,
            'expected_release_state',
            'payload',
            '/^(closed|preview|live)$/'
        )
        : null;
    $envelope = isset($payload['envelope']) && is_array($payload['envelope'])
        && !v2_write_is_list($payload['envelope']) ? $payload['envelope'] : null;
    if ($envelope === null) {
        v2_write_invalid('payload.envelope: object required');
    }
    v2_write_assert_keys($envelope, array(
        'schema_version', 'connector_id', 'country_code', 'source_right_id',
        'rights_revision', 'retrieved_at', 'coverage_mode', 'records',
        'next_cursor', 'exhausted', 'request_count', 'raw_count',
        'public_allowed', 'ai_allowed', 'lifecycle_observations', 'chunk',
        'source_manifest_sha256',
    ), 'envelope');
    if (!isset($envelope['schema_version']) || $envelope['schema_version'] !== 1) {
        v2_write_invalid('envelope.schema_version: must be 1');
    }
    $connectorId = v2_write_code(
        $envelope,
        'connector_id',
        'envelope',
        '/^connector:[a-z]{2}:[a-z0-9_.:\-]{1,64}$/'
    );
    $statement = $pdo->prepare(
        'SELECT connector_id,country_code,source_key,source_type,source_right_id,'
        . 'coverage_mode,connector_status FROM '
        . table_name($config, 'source_connectors')
        . ' WHERE connector_id=? LIMIT 1'
    );
    $statement->execute(array($connectorId));
    $connector = $statement->fetch();
    if (!$connector) {
        v2_write_invalid('envelope.connector_id: connector is not registered');
    }
    if ((string)$connector['connector_id'] === 'connector:kr:dart') {
        v2_write_invalid(
            'envelope.connector_id: OpenDART uses the established official-ingest pipeline'
        );
    }
    if (!in_array(
        (string)$connector['connector_status'],
        array('configured', 'active', 'degraded'),
        true
    )) {
        v2_write_invalid('envelope.connector_id: connector is not enabled');
    }
    $country = v2_write_code(
        $envelope,
        'country_code',
        'envelope',
        '/^(KR|US|JP|GB|CA|AU)$/'
    );
    $sourceRightId = v2_write_code(
        $envelope,
        'source_right_id',
        'envelope',
        '/^official:[a-z0-9_.:\-]{1,48}$/'
    );
    $coverageMode = v2_write_code(
        $envelope,
        'coverage_mode',
        'envelope',
        '/^(market-wide|official-register|selected-issuers|link-only)$/'
    );
    if (
        $country !== (string)$connector['country_code']
        || $sourceRightId !== (string)$connector['source_right_id']
        || $coverageMode !== (string)$connector['coverage_mode']
    ) {
        v2_write_invalid('envelope: registered connector contract mismatch');
    }
    $retrievedAt = v2_write_timestamp($envelope, 'retrieved_at', 'envelope');
    $rightsRevision = v2_write_code(
        $envelope,
        'rights_revision',
        'envelope',
        '/^[a-f0-9]{64}$/'
    );
    $right = v2_source_right_row($pdo, $config, (string)$sourceRightId);
    $rightReasons = v2_source_right_ineligible_reasons($right, 'collect');
    if ($right === null || count($rightReasons) > 0) {
        v2_write_invalid(
            'envelope.source_right_id: source right is not eligible for collection'
        );
    }
    if (
        (string)$right['source_type'] !== (string)$connector['source_type']
        || (string)$right['source_key'] !== (string)$connector['source_key']
        || !hash_equals(v2_source_right_revision($right), (string)$rightsRevision)
    ) {
        v2_write_invalid('envelope.rights_revision: current server grant mismatch');
    }
    $sourceManifestSha256 = null;
    if (array_key_exists('source_manifest_sha256', $envelope)) {
        $sourceManifestSha256 = v2_write_code(
            $envelope,
            'source_manifest_sha256',
            'envelope',
            '/^[a-f0-9]{64}$/'
        );
    }
    if (in_array($country, array('CA', 'AU'), true)) {
        if (
            $sourceManifestSha256 === null
            || preg_match(
                '/^[a-f0-9]{64}$/',
                (string)$right['evidence_hash']
            ) !== 1
            || !hash_equals(
                (string)$right['evidence_hash'],
                (string)$sourceManifestSha256
            )
        ) {
            v2_write_invalid(
                'envelope.source_manifest_sha256: approved manifest mismatch'
            );
        }
    } elseif ($sourceManifestSha256 !== null) {
        v2_write_invalid(
            'envelope.source_manifest_sha256: not permitted for this connector'
        );
    }
    foreach (array('public_allowed', 'ai_allowed') as $rightsFlag) {
        if (
            !array_key_exists($rightsFlag, $envelope)
            || !is_bool($envelope[$rightsFlag])
        ) {
            v2_write_invalid(
                'envelope.' . $rightsFlag . ': boolean required'
            );
        }
    }
    $expectedPublicAllowed = (int)$right['redistribution_allowed'] === 1;
    $expectedAiAllowed = (int)$right['ai_allowed'] === 1;
    if (
        $envelope['public_allowed'] !== $expectedPublicAllowed
        || $envelope['ai_allowed'] !== $expectedAiAllowed
    ) {
        v2_write_invalid('envelope: current source-right flags mismatch');
    }
    if (
        !isset($envelope['records'])
        || !is_array($envelope['records'])
        || !v2_write_is_list($envelope['records'])
        || count($envelope['records']) > 500
    ) {
        v2_write_invalid('envelope.records: zero to 500 records required');
    }
    $lifecycle = isset($envelope['lifecycle_observations'])
        ? $envelope['lifecycle_observations'] : array();
    if (
        !is_array($lifecycle)
        || !v2_write_is_list($lifecycle)
        || count($lifecycle) > 500
    ) {
        v2_write_invalid(
            'envelope.lifecycle_observations: zero to 500 records required'
        );
    }
    $records = array();
    foreach ($envelope['records'] as $index => $record) {
        if (!is_array($record) || v2_write_is_list($record)) {
            v2_write_invalid('envelope.records[' . $index . ']: object required');
        }
        $normalizedRecord = v2_normalize_ingest_record(
            $record,
            (int)$index,
            $connector,
            $right
        );
        $normalizedRecord['public_allowed'] = $expectedPublicAllowed;
        $normalizedRecord['ai_allowed'] = $expectedAiAllowed;
        $expectedContentHash = v2_global_document_content_hash(
            $normalizedRecord,
            $right
        );
        if (!hash_equals(
            (string)$normalizedRecord['content_hash'],
            $expectedContentHash
        )) {
            v2_write_invalid(
                'envelope.records[' . $index
                . '].content_hash: semantic contract mismatch'
            );
        }
        $records[] = $normalizedRecord;
    }
    $observations = array();
    foreach ($lifecycle as $index => $observation) {
        if (!is_array($observation) || v2_write_is_list($observation)) {
            v2_write_invalid(
                'envelope.lifecycle_observations[' . $index . ']: object required'
            );
        }
        $observations[] = v2_normalize_lifecycle_observation(
            $observation,
            (int)$index,
            $connector
        );
    }
    $recordIds = array_column($records, 'record_id');
    if (count($recordIds) !== count(array_unique($recordIds))) {
        v2_write_invalid('envelope.records: duplicate record_id');
    }
    $observationIds = array_column($observations, 'observation_id');
    if (count($observationIds) !== count(array_unique($observationIds))) {
        v2_write_invalid('envelope.lifecycle_observations: duplicate observation_id');
    }
    foreach (array('request_count', 'raw_count') as $countField) {
        if (
            !isset($envelope[$countField])
            || !is_int($envelope[$countField])
            || $envelope[$countField] < 0
        ) {
            v2_write_invalid('envelope.' . $countField . ': non-negative integer required');
        }
    }
    $acknowledged = count($records) + count($observations);
    if ($envelope['raw_count'] < $acknowledged) {
        v2_write_invalid(
            'envelope.raw_count: smaller than accepted entity count'
        );
    }
    if (!isset($envelope['exhausted']) || !is_bool($envelope['exhausted'])) {
        v2_write_invalid('envelope.exhausted: boolean required');
    }
    $chunk = isset($envelope['chunk']) && is_array($envelope['chunk'])
        && !v2_write_is_list($envelope['chunk']) ? $envelope['chunk'] : null;
    if ($chunk === null) {
        v2_write_invalid('envelope.chunk: object required');
    }
    v2_write_assert_keys($chunk, array(
        'index', 'count', 'batch_raw_count', 'batch_acknowledged_count',
        'batch_request_count', 'batch_id', 'window_start',
        'window_end_exclusive',
    ), 'envelope.chunk');
    foreach (array(
        'index', 'count', 'batch_raw_count', 'batch_acknowledged_count',
        'batch_request_count',
    ) as $chunkCountField) {
        if (
            !isset($chunk[$chunkCountField])
            || !is_int($chunk[$chunkCountField])
            || $chunk[$chunkCountField] < 0
        ) {
            v2_write_invalid(
                'envelope.chunk.' . $chunkCountField
                . ': non-negative integer required'
            );
        }
    }
    if (
        $chunk['index'] < 1
        || $chunk['count'] < 1
        || $chunk['count'] > 10000
        || $chunk['index'] > $chunk['count']
    ) {
        v2_write_invalid('envelope.chunk: invalid index or count');
    }
    $batchId = v2_write_code(
        $chunk,
        'batch_id',
        'envelope.chunk',
        '/^global-batch:[a-f0-9]{64}$/'
    );
    $windowStart = v2_write_code(
        $chunk,
        'window_start',
        'envelope.chunk',
        '/^\d{4}-\d{2}-\d{2}$/'
    );
    $windowEnd = v2_write_code(
        $chunk,
        'window_end_exclusive',
        'envelope.chunk',
        '/^\d{4}-\d{2}-\d{2}$/'
    );
    $startParts = array_map('intval', explode('-', (string)$windowStart));
    $endParts = array_map('intval', explode('-', (string)$windowEnd));
    if (
        count($startParts) !== 3
        || count($endParts) !== 3
        || !checkdate($startParts[1], $startParts[2], $startParts[0])
        || !checkdate($endParts[1], $endParts[2], $endParts[0])
    ) {
        v2_write_invalid('envelope.chunk: invalid window date');
    }
    $windowDays = (int)((
        gmmktime(0, 0, 0, $endParts[1], $endParts[2], $endParts[0])
        - gmmktime(0, 0, 0, $startParts[1], $startParts[2], $startParts[0])
    ) / 86400);
    if ($windowDays < 1 || $windowDays > 31) {
        v2_write_invalid('envelope.chunk: window must be 1 to 31 days');
    }
    if (
        $chunk['batch_raw_count'] < $envelope['raw_count']
        || $chunk['batch_acknowledged_count'] < $acknowledged
        || $chunk['batch_raw_count'] < $chunk['batch_acknowledged_count']
        || $chunk['batch_request_count'] < $envelope['request_count']
    ) {
        v2_write_invalid('envelope.chunk: batch totals smaller than chunk');
    }
    if (
        $chunk['count'] === 1
        && (
            $chunk['batch_raw_count'] !== $envelope['raw_count']
            || $chunk['batch_acknowledged_count'] !== $acknowledged
            || $chunk['batch_request_count'] !== $envelope['request_count']
        )
    ) {
        v2_write_invalid('envelope.chunk: single chunk totals mismatch');
    }
    if (
        in_array($country, array('CA', 'AU'), true)
        && $coverageMode === 'link-only'
        && (
            count($records) < 1
            || count($observations) !== 0
            || (int)$envelope['raw_count'] < 1
            || $acknowledged < 1
            || (int)$envelope['request_count'] !== 0
            || (int)$chunk['index'] !== 1
            || (int)$chunk['count'] !== 1
            || (int)$chunk['batch_raw_count'] < 1
            || (int)$chunk['batch_acknowledged_count'] < 1
            || (int)$chunk['batch_request_count'] !== 0
            || $envelope['exhausted'] !== true
        )
    ) {
        v2_write_invalid(
            'envelope: link-only verification requires one complete '
            . 'non-empty metadata batch without source requests'
        );
    }
    if ($chunk['index'] < $chunk['count'] && $envelope['exhausted'] === true) {
        v2_write_invalid('envelope.chunk: non-final chunk cannot be exhausted');
    }
    $cursor = null;
    if (array_key_exists('next_cursor', $envelope) && $envelope['next_cursor'] !== null) {
        if (!is_string($envelope['next_cursor']) || strlen($envelope['next_cursor']) > 1000) {
            v2_write_invalid('envelope.next_cursor: invalid');
        }
        $cursor = $envelope['next_cursor'];
    }
    $completedDayPrefix = 'global-ingest-v2-day:'
        . strtolower((string)$country) . ':';
    $currentPollPrefix = 'global-ingest-v2-current:'
        . strtolower((string)$country) . ':';
    $hasCompletedDayNamespace = strpos(
        (string)$idempotencyKey,
        'global-ingest-v2-day:'
    ) === 0;
    $hasCurrentPollNamespace = strpos(
        (string)$idempotencyKey,
        'global-ingest-v2-current:'
    ) === 0;
    $isCompletedDayEvidence = preg_match(
        '/^' . preg_quote($completedDayPrefix, '/') . '[a-f0-9]{64}$/D',
        (string)$idempotencyKey
    ) === 1;
    $isCurrentPoll = preg_match(
        '/^' . preg_quote($currentPollPrefix, '/') . '[a-f0-9]{64}$/D',
        (string)$idempotencyKey
    ) === 1;
    if ($hasCompletedDayNamespace && !$isCompletedDayEvidence) {
        v2_write_invalid(
            'payload.idempotency_key: invalid completed-day evidence namespace'
        );
    }
    if ($hasCurrentPollNamespace && !$isCurrentPoll) {
        v2_write_invalid(
            'payload.idempotency_key: invalid current-poll namespace'
        );
    }
    $isFinalChunk = (int)$chunk['index'] === (int)$chunk['count'];
    $syntheticChunkCursorPattern = '/^global-ingest-chunk:'
        . preg_quote((string)$windowStart, '/') . ':'
        . preg_quote((string)$windowEnd, '/') . ':'
        . (int)$chunk['index'] . ':' . (int)$chunk['count']
        . ':[a-f0-9]{24}$/D';
    $hasValidSyntheticChunkCursor = (
        !$isFinalChunk
        && $cursor !== null
        && preg_match(
            $syntheticChunkCursorPattern,
            (string)$cursor
        ) === 1
    );
    $receiptKind = 'standard';
    if ($isCompletedDayEvidence) {
        if (
            (string)$connector['connector_id'] !== 'connector:us:sec-edgar'
            || $windowDays !== 1
            || (int)$chunk['batch_request_count'] !== 1
            || ($isFinalChunk && $cursor !== null)
            || (!$isFinalChunk && !$hasValidSyntheticChunkCursor)
            || ($isFinalChunk && $envelope['exhausted'] !== true)
            || count($observations) !== 0
        ) {
            v2_write_invalid(
                'payload.idempotency_key: completed-day evidence contract mismatch'
            );
        }
        foreach ($records as $record) {
            $metadata = isset($record['metadata'])
                && is_object($record['metadata'])
                ? get_object_vars($record['metadata'])
                : (
                    isset($record['metadata'])
                    && is_array($record['metadata'])
                    ? $record['metadata'] : array()
                );
            if (
                !isset($metadata['discovery'])
                || !is_string($metadata['discovery'])
                || !hash_equals(
                    'daily-master-index',
                    (string)$metadata['discovery']
                )
            ) {
                v2_write_invalid(
                    'payload.idempotency_key: completed-day source provenance mismatch'
                );
            }
        }
        $receiptKind = 'completed-day';
    } elseif ($isCurrentPoll) {
        if (
            (string)$connector['connector_id'] !== 'connector:us:sec-edgar'
            || ($isFinalChunk && $envelope['exhausted'] !== true)
            || (
                $isFinalChunk
                && (
                    $cursor === null
                    || !v2_write_valid_sec_current_cursor($cursor)
                )
            )
            || (!$isFinalChunk && !$hasValidSyntheticChunkCursor)
        ) {
            v2_write_invalid(
                'payload.idempotency_key: current-poll contract mismatch'
            );
        }
        $receiptKind = 'current';
    }
    if ($receiptKind !== 'standard') {
        if ($expectedReleaseState === null) {
            v2_write_invalid(
                'payload.expected_release_state: classified receipt requires release binding'
            );
        }
        $expectedClassifiedKey = v2_write_expected_classified_ingest_key(
            $payload,
            (string)$codeRevision,
            (string)$country,
            array(
                'index' => (int)$chunk['index'],
                'window_start' => (string)$windowStart,
                'window_end_exclusive' => (string)$windowEnd,
            ),
            $records,
            $receiptKind === 'completed-day'
                ? 'global-ingest-v2-day'
                : 'global-ingest-v2-current'
        );
        if (!hash_equals($expectedClassifiedKey, (string)$idempotencyKey)) {
            v2_write_invalid(
                'payload.idempotency_key: classified semantic digest mismatch'
            );
        }
    }
    return array(
        'idempotency_key' => $idempotencyKey,
        'code_revision' => $codeRevision,
        'expected_release_state' => $expectedReleaseState,
        'ingest_mode' => $ingestMode,
        'connector' => $connector,
        'right' => $right,
        'rights_revision' => $rightsRevision,
        'source_manifest_sha256' => $sourceManifestSha256,
        'retrieved_at' => $retrievedAt,
        'records' => $records,
        'lifecycle_observations' => $observations,
        'raw_count' => (int)$envelope['raw_count'],
        'acknowledged_count' => $acknowledged,
        'request_count' => (int)$envelope['request_count'],
        'chunk' => array(
            'index' => (int)$chunk['index'],
            'count' => (int)$chunk['count'],
            'batch_raw_count' => (int)$chunk['batch_raw_count'],
            'batch_acknowledged_count' => (int)$chunk['batch_acknowledged_count'],
            'batch_request_count' => (int)$chunk['batch_request_count'],
            'batch_id' => $batchId,
            'window_start' => $windowStart,
            'window_end_exclusive' => $windowEnd,
        ),
        'next_cursor' => $cursor,
        'exhausted' => $envelope['exhausted'],
        'receipt_kind' => $receiptKind,
        'payload_hash' => v2_write_ingest_idempotency_hash($payload),
    );
}

function v2_ingest_upsert_issuer(
    PDO $pdo,
    array $config,
    array $record,
    string $now
): void {
    $issuerLookup = $pdo->prepare(
        'SELECT country_code FROM ' . table_name($config, 'issuers')
        . ' WHERE issuer_id=? LIMIT 1 FOR UPDATE'
    );
    $issuerLookup->execute(array($record['issuer_id']));
    $existingCountry = $issuerLookup->fetchColumn();
    if ($existingCountry !== false && (string)$existingCountry !== $record['country_code']) {
        throw new RuntimeException('global_issuer_country_conflict');
    }
    $issuer = $pdo->prepare(
        'INSERT INTO ' . table_name($config, 'issuers')
        . ' (issuer_id,country_code,legal_name,legal_name_en,short_name,'
        . 'original_language,homepage_url,listing_status,record_status,'
        . 'master_modified_at,payload_json,created_at,updated_at)'
        . ' VALUES (?,?,?,NULL,NULL,?,NULL,\'unknown\',\'active\',?,NULL,?,?)'
        . ' ON DUPLICATE KEY UPDATE legal_name=VALUES(legal_name),'
        . 'original_language=VALUES(original_language),'
        . 'master_modified_at=GREATEST(COALESCE(master_modified_at,VALUES(master_modified_at)),'
        . 'VALUES(master_modified_at)),updated_at=VALUES(updated_at)'
    );
    $issuer->execute(array(
        $record['issuer_id'],
        $record['country_code'],
        $record['legal_name'],
        $record['original_language'],
        $record['filed_at'],
        $now,
        $now,
    ));
    $identifierConflict = $pdo->prepare(
        'SELECT issuer_id FROM ' . table_name($config, 'issuer_identifiers')
        . ' WHERE identifier_type=? AND identifier_value=? AND market=?'
        . ' LIMIT 1 FOR UPDATE'
    );
    $identifierConflict->execute(array(
        $record['identifier_type'],
        $record['identifier_value'],
        $record['market'],
    ));
    $mappedIssuer = $identifierConflict->fetchColumn();
    if ($mappedIssuer !== false && (string)$mappedIssuer !== $record['issuer_id']) {
        throw new RuntimeException('global_issuer_identifier_conflict');
    }
    $identifier = $pdo->prepare(
        'INSERT INTO ' . table_name($config, 'issuer_identifiers')
        . ' (issuer_id,identifier_type,identifier_value,market,is_primary,'
        . 'valid_from,valid_until,created_at,updated_at)'
        . ' VALUES (?,?,?,?,1,NULL,NULL,?,?)'
        . ' ON DUPLICATE KEY UPDATE is_primary=GREATEST(is_primary,VALUES(is_primary)),'
        . 'updated_at=VALUES(updated_at)'
    );
    $identifier->execute(array(
        $record['issuer_id'],
        $record['identifier_type'],
        $record['identifier_value'],
        $record['market'],
        $now,
        $now,
    ));
    if ($record['ticker'] === null || $record['ticker'] === '') {
        return;
    }
    $listingConflict = $pdo->prepare(
        'SELECT issuer_id FROM ' . table_name($config, 'issuer_listings')
        . ' WHERE country_code=? AND market=? AND ticker=? LIMIT 1 FOR UPDATE'
    );
    $listingConflict->execute(array(
        $record['country_code'],
        $record['market'],
        $record['ticker'],
    ));
    $listingIssuer = $listingConflict->fetchColumn();
    if ($listingIssuer !== false && (string)$listingIssuer !== $record['issuer_id']) {
        throw new RuntimeException('global_issuer_listing_conflict');
    }
    $listingId = v1_stable_id(
        'listing',
        $record['issuer_id'] . '|' . $record['market'] . '|' . $record['ticker']
    );
    $listing = $pdo->prepare(
        'INSERT INTO ' . table_name($config, 'issuer_listings')
        . ' (listing_id,issuer_id,country_code,market,ticker,isin,currency_code,'
        . 'listing_status,is_primary,created_at,updated_at)'
        . ' VALUES (?,?,?,?,?,NULL,NULL,\'unknown\',1,?,?)'
        . ' ON DUPLICATE KEY UPDATE issuer_id=VALUES(issuer_id),'
        . 'listing_status=VALUES(listing_status),is_primary=1,updated_at=VALUES(updated_at)'
    );
    $listing->execute(array(
        $listingId,
        $record['issuer_id'],
        $record['country_code'],
        $record['market'],
        $record['ticker'],
        $now,
        $now,
    ));
}

function v2_ingest_upsert_record(
    PDO $pdo,
    array $config,
    array $record,
    array $right,
    string $now
): string {
    v2_ingest_upsert_issuer($pdo, $config, $record, $now);
    $recordPayload = array(
        'record_kind' => $record['record_kind'],
        'event_family' => $record['event_family'],
        'change_type' => $record['change_type'],
        'base_record_id' => $record['record_id'],
        'correction_of_external_id' => $record['correction_of_external_id'],
        'issuer_reference' => array(
            'namespace' => $record['namespace'],
            'identifier_type' => $record['identifier_type'],
            'value' => $record['identifier_value'],
            'legal_name' => $record['legal_name'],
            'market' => $record['market'],
            'ticker' => $record['ticker'],
        ),
        'metadata' => $record['metadata'],
        'public_allowed' => $record['public_allowed'],
        'ai_allowed' => $record['ai_allowed'],
    );
    $recordPayloadHash = v2_write_canonical_payload_hash($recordPayload);
    $versions = $pdo->prepare(
        'SELECT document_id,issuer_id,country_code,source_right_id,source_key,'
        . 'source_class,external_id,document_type,original_language,title,'
        . 'body_text,original_url,content_hash,version_no,filed_at,payload_json FROM '
        . table_name($config, 'documents')
        . ' WHERE source_right_id=? AND external_id=?'
        . ' ORDER BY version_no DESC,document_id DESC FOR UPDATE'
    );
    $versions->execute(array(
        $record['source_right_id'],
        $record['external_id'],
    ));
    $storedVersions = $versions->fetchAll();
    $document = count($storedVersions) > 0 ? $storedVersions[0] : null;
    foreach ($storedVersions as $candidate) {
        if (
            (string)$candidate['issuer_id'] !== $record['issuer_id']
            || (string)$candidate['country_code'] !== $record['country_code']
            || (string)$candidate['source_right_id'] !== $record['source_right_id']
            || (string)$candidate['source_key'] !== $record['source_key']
        ) {
            throw new RuntimeException(
                hash_equals(
                    (string)$candidate['content_hash'],
                    $record['content_hash']
                )
                    ? 'global_document_hash_contract_conflict'
                    : 'global_document_external_id_conflict'
            );
        }
    }
    if (
        $document !== null
        && !hash_equals((string)$document['content_hash'], $record['content_hash'])
    ) {
        // A payload that reverts to an older hash is still a new observation
        // version. Only the latest version can make a retry idempotent.
        $document = null;
    }
    $newVersion = $document === null;
    $documentId = $record['record_id'];
    $versionNo = 1;
    $previousDocumentId = null;
    if (count($storedVersions) > 0) {
        $latest = $storedVersions[0];
        $previousDocumentId = (string)$latest['document_id'];
        $versionNo = (int)$latest['version_no'] + ($newVersion ? 1 : 0);
        if ($newVersion) {
            $documentId = 'globaldoc:' . substr(
                hash(
                    'sha256',
                    $record['record_id'] . "\x1f" . $versionNo . "\x1f"
                        . $record['content_hash']
                ),
                0,
                40
            );
        }
    }
    if ($document !== null) {
        $storedPayload = json_decode(
            (string)$document['payload_json'],
            true
        );
        if (
            is_array($storedPayload)
            && array_key_exists('metadata', $storedPayload)
            && is_array($storedPayload['metadata'])
        ) {
            $storedMetadataNodeCount = 0;
            $storedPayload['metadata'] = v2_write_semantic_metadata_node(
                $storedPayload['metadata'],
                0,
                $storedMetadataNodeCount,
                true
            );
        }
        $storedPayloadHash = is_array($storedPayload)
            && !v2_write_is_list($storedPayload)
            ? v2_write_canonical_payload_hash($storedPayload) : '';
        $same = (
            (string)$document['issuer_id'] === $record['issuer_id']
            && (string)$document['country_code'] === $record['country_code']
            && (string)$document['source_right_id'] === $record['source_right_id']
            && (string)$document['source_key'] === $record['source_key']
            && (string)$document['source_class'] === (string)$right['source_type']
            && (string)$document['external_id'] === $record['external_id']
            && (string)$document['document_type'] === $record['document_type']
            && (string)$document['original_language'] === $record['original_language']
            && (string)$document['title'] === $record['title']
            && (
                $document['body_text'] === null
                    ? null : (string)$document['body_text']
            ) === $record['body_text']
            && (string)$document['original_url'] === $record['original_url']
            && (string)$document['filed_at'] === $record['filed_at']
            && hash_equals((string)$document['content_hash'], $record['content_hash'])
            && $storedPayloadHash !== ''
            && hash_equals($storedPayloadHash, $recordPayloadHash)
        );
        if (!$same) {
            throw new RuntimeException('global_document_hash_contract_conflict');
        }
        $documentId = (string)$document['document_id'];
        $versionNo = (int)$document['version_no'];
        $touch = $pdo->prepare(
            'UPDATE ' . table_name($config, 'documents')
            . ' SET retrieved_at=GREATEST(retrieved_at,?),updated_at=?'
            . ' WHERE document_id=?'
        );
        $touch->execute(array(
            $record['first_observed_at'],
            $now,
            $documentId,
        ));
    } else {
        $correctionDocumentId = $previousDocumentId;
        if ($record['correction_of_external_id'] !== null) {
            $correction = $pdo->prepare(
                'SELECT document_id FROM ' . table_name($config, 'documents')
                . ' WHERE source_right_id=? AND external_id=?'
                . ' ORDER BY version_no DESC,document_id DESC LIMIT 1'
            );
            $correction->execute(array(
                $record['source_right_id'],
                $record['correction_of_external_id'],
            ));
            $candidate = $correction->fetchColumn();
            $correctionDocumentId = $candidate === false ? null : (string)$candidate;
        }
        $insert = $pdo->prepare(
            'INSERT INTO ' . table_name($config, 'documents')
            . ' (document_id,company_id,issuer_id,country_code,source_right_id,'
            . 'source_class,source_key,external_id,document_type,original_language,'
            . 'title,body_text,original_url,content_hash,collection_key,'
            . 'correction_of_document_id,version_no,published_at,filed_at,retrieved_at,'
            . 'verification_status,publication_status,payload_json,created_at,updated_at)'
            . ' VALUES (?,NULL,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?,?,?,?,?,?,?,?,?,?)'
        );
        $insert->execute(array(
            $documentId,
            $record['issuer_id'],
            $record['country_code'],
            $record['source_right_id'],
            (string)$right['source_type'],
            $record['source_key'],
            $record['external_id'],
            $record['document_type'],
            $record['original_language'],
            $record['title'],
            $record['body_text'],
            $record['original_url'],
            $record['content_hash'],
            $correctionDocumentId,
            $versionNo,
            null,
            $record['filed_at'],
            $record['first_observed_at'],
            'official',
            'draft',
            json_value($recordPayload),
            $now,
            $now,
        ));
    }
    $eventId = v1_stable_id('global-event', (string)$record['record_id']);
    $eventExisting = $pdo->prepare(
        'SELECT issuer_id,country_code,global_event_family,publication_status,'
        . 'review_status FROM ' . table_name($config, 'governance_events')
        . ' WHERE event_id=? LIMIT 1 FOR UPDATE'
    );
    $eventExisting->execute(array($eventId));
    $event = $eventExisting->fetch();
    if ($event) {
        if (
            (string)$event['issuer_id'] !== $record['issuer_id']
            || (string)$event['country_code'] !== $record['country_code']
            || (string)$event['global_event_family'] !== $record['event_family']
        ) {
            throw new RuntimeException('global_event_identity_conflict');
        }
        if ($newVersion) {
            $updateEvent = $pdo->prepare(
                'UPDATE ' . table_name($config, 'governance_events')
                . ' SET title=?,original_language=?,occurred_at=?,'
                . 'verification_status=?,change_type=?,current_status=?,'
                . 'first_observed_at=LEAST(COALESCE(first_observed_at,?),?),'
                . 'review_status=\'pending\',publication_status=\'draft\','
                . 'identity_status=\'needs_review\',comparison_key=NULL,'
                . 'payload_json=?,updated_at=? WHERE event_id=?'
            );
            $verification = $record['change_type'] === 'withdrawn'
                ? 'withdrawn'
                : ($record['change_type'] === 'corrected' ? 'corrected' : 'official');
            $updateEvent->execute(array(
                $record['title'],
                $record['original_language'],
                $record['filed_at'],
                $verification,
                $record['change_type'],
                $record['change_type'],
                $record['first_observed_at'],
                $record['first_observed_at'],
                json_value(array(
                    'source_document_id' => $documentId,
                    'source_external_id' => $record['external_id'],
                    'record_kind' => $record['record_kind'],
                    'metadata' => $record['metadata'],
                )),
                $now,
                $eventId,
            ));
        } else {
            // Completed-day windows deliberately overlap. A repeat observation
            // may improve the earliest-seen timestamp, but it is not a semantic
            // event change and must not move a published event in /live.
            $touchEvent = $pdo->prepare(
                'UPDATE ' . table_name($config, 'governance_events')
                . ' SET first_observed_at=LEAST(COALESCE(first_observed_at,?),?)'
                . ' WHERE event_id=?'
            );
            $touchEvent->execute(array(
                $record['first_observed_at'],
                $record['first_observed_at'],
                $eventId,
            ));
        }
    } else {
        $insertEvent = $pdo->prepare(
            'INSERT INTO ' . table_name($config, 'governance_events')
            . ' (event_id,company_id,issuer_id,country_code,global_event_family,'
            . 'event_type,title,original_language,summary,occurred_at,deadline_at,'
            . 'importance,verification_status,change_type,current_status,'
            . 'first_observed_at,review_status,publication_status,collection_key,'
            . 'identity_action,identity_target,identity_actor_id,identity_effective_at,'
            . 'identity_deadline_at,identity_status,comparison_key,payload_json,'
            . 'created_at,updated_at)'
            . ' VALUES (?,NULL,?,?,?,?,?,?,NULL,?,NULL,\'medium\',\'official\',?,?,?,'
            . '\'pending\',\'draft\',?,NULL,NULL,NULL,NULL,NULL,\'needs_review\',NULL,?,?,?)'
        );
        $insertEvent->execute(array(
            $eventId,
            $record['issuer_id'],
            $record['country_code'],
            $record['event_family'],
            $record['event_family'],
            $record['title'],
            $record['original_language'],
            $record['filed_at'],
            $record['change_type'],
            $record['change_type'],
            $record['first_observed_at'],
            mb_substr($record['external_id'], 0, 96, 'UTF-8'),
            json_value(array(
                'source_document_id' => $documentId,
                'source_external_id' => $record['external_id'],
                'record_kind' => $record['record_kind'],
                'metadata' => $record['metadata'],
            )),
            $now,
            $now,
        ));
    }
    $link = $pdo->prepare(
        'INSERT INTO ' . table_name($config, 'event_documents')
        . ' (event_id,document_id,relation_type,position_no,created_at)'
        . ' VALUES (?,?,\'evidence\',?,?)'
        . ' ON DUPLICATE KEY UPDATE position_no=VALUES(position_no)'
    );
    $link->execute(array($eventId, $documentId, $versionNo - 1, $now));
    $observationId = v1_stable_id(
        'observation',
        $eventId . '|' . $documentId . '|' . $record['source_key']
    );
    $observation = $pdo->prepare(
        'INSERT INTO ' . table_name($config, 'event_observations')
        . ' (observation_id,event_id,document_id,source_class,source_key,'
        . 'first_observed_at,observed_at,payload_hash,payload_json,created_at,updated_at)'
        . ' VALUES (?,?,?,?,?,?,?,?,?,?,?)'
        . ' ON DUPLICATE KEY UPDATE '
        . 'first_observed_at=LEAST(first_observed_at,VALUES(first_observed_at)),'
        . 'observed_at=GREATEST(observed_at,VALUES(observed_at)),'
        . 'payload_hash=VALUES(payload_hash),payload_json=VALUES(payload_json),'
        . 'updated_at=VALUES(updated_at)'
    );
    $observation->execute(array(
        $observationId,
        $eventId,
        $documentId,
        (string)$right['source_type'],
        $record['source_key'],
        $record['first_observed_at'],
        $record['first_observed_at'],
        $record['content_hash'],
        json_value(array(
            'change_type' => $record['change_type'],
            'event_family' => $record['event_family'],
        )),
        $now,
        $now,
    ));
    return $eventId;
}

function v2_lifecycle_semantic_hash(
    string $connectorId,
    array $observation
): string {
    return v2_write_canonical_payload_hash(array(
        'connector_id' => $connectorId,
        'country_code' => $observation['country_code'],
        'source_key' => $observation['source_key'],
        'external_id' => $observation['external_id'],
        'parent_external_id' => $observation['parent_external_id'],
        'change_type' => $observation['change_type'],
        'observed_at' => $observation['observed_at'],
        'metadata' => $observation['metadata'],
    ));
}

function v2_ingest_lifecycle_observation(
    PDO $pdo,
    array $config,
    array $connector,
    array $observation,
    string $now
): void {
    $documentLookup = $pdo->prepare(
        'SELECT document_id FROM ' . table_name($config, 'documents')
        . ' WHERE source_right_id=? AND source_key=? AND external_id IN (?,?)'
        . ' ORDER BY CASE WHEN external_id=? THEN 0 ELSE 1 END,version_no DESC'
        . ' LIMIT 1'
    );
    $parent = $observation['parent_external_id'] === null
        ? $observation['external_id'] : $observation['parent_external_id'];
    $documentLookup->execute(array(
        (string)$connector['source_right_id'],
        $observation['source_key'],
        $observation['external_id'],
        $parent,
        $observation['external_id'],
    ));
    $documentIdRaw = $documentLookup->fetchColumn();
    $documentId = $documentIdRaw === false ? null : (string)$documentIdRaw;
    $eventId = null;
    if ($documentId !== null) {
        $eventLookup = $pdo->prepare(
            'SELECT ed.event_id FROM ' . table_name($config, 'event_documents')
            . ' ed JOIN ' . table_name($config, 'governance_events')
            . ' e ON e.event_id=ed.event_id'
            . ' WHERE ed.document_id=?'
            . ' AND e.identity_status<>\'merged\''
            . ' AND e.review_status<>\'merged\''
            . ' ORDER BY '
            . 'CASE WHEN e.publication_status=\'published\' THEN 0 ELSE 1 END,'
            . 'e.updated_at DESC,ed.event_id LIMIT 1'
        );
        $eventLookup->execute(array($documentId));
        $eventRaw = $eventLookup->fetchColumn();
        if ($eventRaw !== false) {
            $candidateEventId = (string)$eventRaw;
            // Use the editor's event-then-association lock order. A merge or
            // relink can finish before this point or wait until this lifecycle
            // transaction commits, but cannot occur between resolution and
            // demotion.
            $eventLock = $pdo->prepare(
                'SELECT event_id FROM '
                . table_name($config, 'governance_events')
                . ' WHERE event_id=? AND identity_status<>\'merged\''
                . ' AND review_status<>\'merged\' LIMIT 1 FOR UPDATE'
            );
            $eventLock->execute(array($candidateEventId));
            if ($eventLock->fetchColumn() !== false) {
                $associationLock = $pdo->prepare(
                    'SELECT event_id FROM '
                    . table_name($config, 'event_documents')
                    . ' WHERE event_id=? AND document_id=?'
                    . ' LIMIT 1 FOR UPDATE'
                );
                $associationLock->execute(array(
                    $candidateEventId,
                    $documentId,
                ));
                if ($associationLock->fetchColumn() !== false) {
                    $eventId = $candidateEventId;
                }
            }
        }
    }
    $resolution = $documentId !== null && $eventId !== null ? 'resolved' : 'pending';
    $existingLookup = $pdo->prepare(
        'SELECT connector_id,country_code,source_key,external_id,parent_external_id,'
        . 'change_type,observed_at,payload_json,resolution_status,'
        . 'resolved_document_id,resolved_event_id FROM '
        . table_name($config, 'global_lifecycle_observations')
        . ' WHERE observation_id=? LIMIT 1 FOR UPDATE'
    );
    $existingLookup->execute(array($observation['observation_id']));
    $existing = $existingLookup->fetch();
    $applyEventChange = false;
    if ($existing) {
        $storedMetadata = json_decode((string)$existing['payload_json'], true);
        if (!is_array($storedMetadata)) {
            throw new RuntimeException(
                'global_lifecycle_observation_conflict'
            );
        }
        $storedObservation = array(
            'country_code' => (string)$existing['country_code'],
            'source_key' => (string)$existing['source_key'],
            'external_id' => (string)$existing['external_id'],
            'parent_external_id' => $existing['parent_external_id'] === null
                ? null : (string)$existing['parent_external_id'],
            'change_type' => (string)$existing['change_type'],
            'observed_at' => (string)$existing['observed_at'],
            'metadata' => $storedMetadata,
        );
        if (!hash_equals(
            v2_lifecycle_semantic_hash(
                (string)$existing['connector_id'],
                $storedObservation
            ),
            v2_lifecycle_semantic_hash(
                (string)$connector['connector_id'],
                $observation
            )
        )) {
            throw new RuntimeException(
                'global_lifecycle_observation_conflict'
            );
        }
        $storedResolution = (string)$existing['resolution_status'];
        $storedDocumentId = $existing['resolved_document_id'] === null
            ? null : (string)$existing['resolved_document_id'];
        $storedEventId = $existing['resolved_event_id'] === null
            ? null : (string)$existing['resolved_event_id'];
        if (
            $storedResolution === 'resolved'
            && $storedDocumentId !== null
            && $storedEventId !== null
        ) {
            if (
                $resolution === 'resolved'
                && (
                    $storedDocumentId !== $documentId
                    || $storedEventId !== $eventId
                )
            ) {
                throw new RuntimeException(
                    'global_lifecycle_resolution_conflict'
                );
            }
            // Never regress a durable resolution when a later lookup is
            // temporarily incomplete. An exact replay is a complete no-op.
            return;
        }
        if (
            $storedResolution !== 'pending'
            || $storedDocumentId !== null
            || $storedEventId !== null
        ) {
            throw new RuntimeException(
                'global_lifecycle_resolution_conflict'
            );
        }
        if ($resolution === 'pending') {
            return;
        }
        $resolve = $pdo->prepare(
            'UPDATE ' . table_name($config, 'global_lifecycle_observations')
            . ' SET resolution_status=\'resolved\',resolved_document_id=?,'
            . 'resolved_event_id=?,updated_at=? WHERE observation_id=?'
        );
        $resolve->execute(array(
            $documentId,
            $eventId,
            $now,
            $observation['observation_id'],
        ));
        $applyEventChange = true;
    } else {
        $insert = $pdo->prepare(
            'INSERT INTO ' . table_name($config, 'global_lifecycle_observations')
            . ' (observation_id,connector_id,country_code,source_key,external_id,'
            . 'parent_external_id,change_type,observed_at,payload_json,resolution_status,'
            . 'resolved_document_id,resolved_event_id,created_at,updated_at)'
            . ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        );
        $insert->execute(array(
            $observation['observation_id'],
            (string)$connector['connector_id'],
            $observation['country_code'],
            $observation['source_key'],
            $observation['external_id'],
            $observation['parent_external_id'],
            $observation['change_type'],
            $observation['observed_at'],
            json_value($observation['metadata']),
            $resolution,
            $documentId,
            $eventId,
            $now,
            $now,
        ));
        $applyEventChange = $eventId !== null;
    }
    if ($applyEventChange && $eventId !== null) {
        $update = $pdo->prepare(
            'UPDATE ' . table_name($config, 'governance_events')
            . ' SET verification_status=?,change_type=?,current_status=?,'
            . 'review_status=\'pending\',publication_status=\'draft\','
            . 'identity_status=\'needs_review\',comparison_key=NULL,updated_at=?'
            . ' WHERE event_id=?'
        );
        $verification = $observation['change_type'] === 'withdrawn'
            ? 'withdrawn' : 'corrected';
        $update->execute(array(
            $verification,
            $observation['change_type'],
            $observation['change_type'],
            $now,
            $eventId,
        ));
    }
}

function v2_ingest_batch_receipts(
    PDO $pdo,
    array $config,
    string $connectorId,
    string $batchId
): array {
    $statement = $pdo->prepare(
        'SELECT batch_id,chunk_index,chunk_count,window_start,'
        . 'window_end_exclusive,raw_count,acknowledged_count,request_count,'
        . 'batch_raw_count,batch_acknowledged_count,batch_request_count,'
        . 'code_revision FROM '
        . table_name($config, 'global_ingest_receipts')
        . ' WHERE connector_id=? AND batch_id=?'
        . ' ORDER BY chunk_index FOR UPDATE'
    );
    $statement->execute(array($connectorId, $batchId));
    return $statement->fetchAll();
}

function v2_ingest_assert_batch_metadata(
    array $row,
    array $normalized
): void {
    $chunk = $normalized['chunk'];
    if (
        (string)$row['batch_id'] !== (string)$chunk['batch_id']
        || (int)$row['chunk_count'] !== (int)$chunk['count']
        || (string)$row['window_start'] !== (string)$chunk['window_start']
        || (string)$row['window_end_exclusive']
            !== (string)$chunk['window_end_exclusive']
        || (int)$row['batch_raw_count']
            !== (int)$chunk['batch_raw_count']
        || (int)$row['batch_acknowledged_count']
            !== (int)$chunk['batch_acknowledged_count']
        || (string)$row['code_revision']
            !== (string)$normalized['code_revision']
    ) {
        throw new RuntimeException(
            'global_ingest_batch_metadata_conflict'
        );
    }
}

function v2_ingest_assert_batch_prefix(
    array $rows,
    array $normalized
): void {
    $chunk = $normalized['chunk'];
    $expectedPriorCount = (int)$chunk['index'] - 1;
    if (count($rows) !== $expectedPriorCount) {
        throw new RuntimeException(
            'global_ingest_chunk_out_of_order'
        );
    }
    foreach ($rows as $position => $row) {
        if ((int)$row['chunk_index'] !== $position + 1) {
            throw new RuntimeException(
                'global_ingest_batch_receipt_corrupt'
            );
        }
        v2_ingest_assert_batch_metadata($row, $normalized);
    }
}

function v2_ingest_assert_batch_complete(
    array $rows,
    array $normalized
): void {
    $chunk = $normalized['chunk'];
    if (count($rows) !== (int)$chunk['count']) {
        throw new RuntimeException(
            'global_ingest_batch_incomplete'
        );
    }
    $rawTotal = 0;
    $acknowledgedTotal = 0;
    $requestTotal = 0;
    foreach ($rows as $position => $row) {
        if ((int)$row['chunk_index'] !== $position + 1) {
            throw new RuntimeException(
                'global_ingest_batch_receipt_corrupt'
            );
        }
        v2_ingest_assert_batch_metadata($row, $normalized);
        $rawTotal += (int)$row['raw_count'];
        $acknowledgedTotal += (int)$row['acknowledged_count'];
        $requestTotal += (int)$row['request_count'];
    }
    if (
        $rawTotal !== (int)$chunk['batch_raw_count']
        || $acknowledgedTotal
            !== (int)$chunk['batch_acknowledged_count']
        || $requestTotal !== (int)$chunk['batch_request_count']
    ) {
        throw new RuntimeException(
            'global_ingest_batch_totals_mismatch'
        );
    }
}

/**
 * Verify an already committed batch without comparing attempt-only request
 * telemetry from the new poll. Content identity intentionally excludes those
 * counters, while the stored batch must still be internally complete.
 */
function v2_ingest_assert_stored_batch_complete(
    array $rows,
    array $normalized
): void {
    $chunk = $normalized['chunk'];
    if (
        (int)$chunk['index'] !== (int)$chunk['count']
        || count($rows) !== (int)$chunk['count']
        || !$rows
    ) {
        throw new RuntimeException('global_ingest_batch_incomplete');
    }
    $first = $rows[0];
    $rawTotal = 0;
    $acknowledgedTotal = 0;
    $requestTotal = 0;
    foreach ($rows as $position => $row) {
        if (
            (int)$row['chunk_index'] !== $position + 1
            || (int)$row['batch_request_count']
                !== (int)$first['batch_request_count']
        ) {
            throw new RuntimeException(
                'global_ingest_batch_receipt_corrupt'
            );
        }
        v2_ingest_assert_batch_metadata($row, $normalized);
        $rawTotal += (int)$row['raw_count'];
        $acknowledgedTotal += (int)$row['acknowledged_count'];
        $requestTotal += (int)$row['request_count'];
    }
    if (
        $rawTotal !== (int)$first['batch_raw_count']
        || $acknowledgedTotal
            !== (int)$first['batch_acknowledged_count']
        || $requestTotal !== (int)$first['batch_request_count']
    ) {
        throw new RuntimeException(
            'global_ingest_batch_totals_mismatch'
        );
    }
}

function v2_ingest_locked_checkpoint(array $connector): ?array {
    $raw = isset($connector['cursor_json'])
        ? trim((string)$connector['cursor_json']) : '';
    if ($raw === '') {
        return null;
    }
    $cursor = json_decode($raw, true);
    if (
        !is_array($cursor)
        || !isset(
            $cursor['schema_version'],
            $cursor['window_end_exclusive'],
            $cursor['batch_id']
        )
        || !in_array((int)$cursor['schema_version'], array(1, 2), true)
        || !is_string($cursor['window_end_exclusive'])
        || preg_match(
            '/^\d{4}-\d{2}-\d{2}$/D',
            (string)$cursor['window_end_exclusive']
        ) !== 1
        || !is_string($cursor['batch_id'])
        || preg_match(
            '/^global-batch:[a-f0-9]{64}$/D',
            (string)$cursor['batch_id']
        ) !== 1
    ) {
        throw new RuntimeException('global_connector_checkpoint_corrupt');
    }
    $expectedKeys = (int)$cursor['schema_version'] === 2
        ? array(
            'schema_version',
            'window_end_exclusive',
            'batch_id',
            'source_cursor',
        )
        : array('schema_version', 'window_end_exclusive', 'batch_id');
    if (!v2_exact_string_keys($cursor, $expectedKeys)) {
        throw new RuntimeException('global_connector_checkpoint_corrupt');
    }
    $dateParts = array_map(
        'intval',
        explode('-', (string)$cursor['window_end_exclusive'])
    );
    if (
        count($dateParts) !== 3
        || !checkdate($dateParts[1], $dateParts[2], $dateParts[0])
    ) {
        throw new RuntimeException('global_connector_checkpoint_corrupt');
    }
    if ((int)$cursor['schema_version'] === 2) {
        if (
            !isset($cursor['source_cursor'])
            || !is_string($cursor['source_cursor'])
            || trim((string)$cursor['source_cursor']) === ''
            || strlen((string)$cursor['source_cursor']) > 1000
        ) {
            throw new RuntimeException('global_connector_checkpoint_corrupt');
        }
    }
    return $cursor;
}

function v2_ingest_checkpoint_should_advance(
    ?array $existing,
    array $normalized
): bool {
    if ($existing === null) {
        return true;
    }
    $incomingEnd = (string)$normalized['chunk']['window_end_exclusive'];
    $existingEnd = (string)$existing['window_end_exclusive'];
    $comparison = strcmp($incomingEnd, $existingEnd);
    if ($comparison > 0) {
        return true;
    }
    // An incremental poll can advance its opaque source cursor without
    // changing the completed-day boundary. Historical completed-day runs
    // never replace an equal or newer durable checkpoint.
    return $comparison === 0 && $normalized['next_cursor'] !== null;
}

function v2_ingest_is_link_only_apply(array $normalized): bool {
    return (
        $normalized['ingest_mode'] === 'apply'
        && (string)$normalized['connector']['coverage_mode'] === 'link-only'
        && in_array(
            (string)$normalized['connector']['country_code'],
            array('CA', 'AU'),
            true
        )
    );
}

function v2_ingest_link_only_receipt_is_complete(
    array $receipt,
    array $normalized
): bool {
    if (!v2_ingest_is_link_only_apply($normalized)) {
        return false;
    }
    $chunk = $normalized['chunk'];
    return (
        (int)$receipt['raw_count'] >= 1
        && (int)$receipt['acknowledged_count'] >= 1
        && (int)$receipt['request_count'] === 0
        && (int)$receipt['chunk_index'] === 1
        && (int)$receipt['chunk_count'] === 1
        && (int)$receipt['batch_raw_count'] === (int)$receipt['raw_count']
        && (int)$receipt['batch_acknowledged_count']
            === (int)$receipt['acknowledged_count']
        && (int)$receipt['batch_request_count'] === 0
        && (string)$receipt['batch_id'] === (string)$chunk['batch_id']
        && (int)$receipt['raw_count'] === (int)$normalized['raw_count']
        && (int)$receipt['acknowledged_count']
            === (int)$normalized['acknowledged_count']
    );
}

/**
 * Revalidate the exact approved manifest and current SourceRight while the
 * right row is locked. The pre-transaction validation is useful feedback, but
 * only this check can authorize a link-only freshness heartbeat.
 */
function v2_ingest_assert_locked_link_only_right(
    array $normalized,
    array $lockedRight
): void {
    if (!v2_ingest_is_link_only_apply($normalized)) {
        return;
    }
    $connector = $normalized['connector'];
    $manifestSha256 = $normalized['source_manifest_sha256'];
    if (
        !is_string($manifestSha256)
        || preg_match('/^[a-f0-9]{64}$/', $manifestSha256) !== 1
        || (string)$lockedRight['source_right_id']
            !== (string)$connector['source_right_id']
        || (string)$lockedRight['source_type']
            !== (string)$connector['source_type']
        || (string)$lockedRight['source_key']
            !== (string)$connector['source_key']
        || count(v2_source_right_ineligible_reasons(
            $lockedRight,
            'collect'
        )) > 0
        || count(v2_source_right_ineligible_reasons(
            $lockedRight,
            'public'
        )) > 0
        || !hash_equals(
            v2_source_right_revision($lockedRight),
            (string)$normalized['rights_revision']
        )
        || preg_match(
            '/^[a-f0-9]{64}$/',
            (string)$lockedRight['evidence_hash']
        ) !== 1
        || !hash_equals(
            (string)$lockedRight['evidence_hash'],
            $manifestSha256
        )
    ) {
        throw new RuntimeException('global_source_right_changed');
    }
}

/**
 * Refresh only connector verification telemetry. Existing receipts, documents,
 * review state, and checkpoints stay byte-for-byte untouched.
 */
function v2_ingest_refresh_link_only_connector(
    PDO $pdo,
    array $config,
    array $normalized,
    array $receipt,
    string $verifiedAt
): void {
    if (
        !v2_ingest_is_link_only_apply($normalized)
        || (int)$receipt['raw_count'] < 1
        || (int)$receipt['acknowledged_count'] < 1
    ) {
        return;
    }
    $update = $pdo->prepare(
        'UPDATE ' . table_name($config, 'source_connectors')
        . ' SET connector_status=\'active\',last_checked_at=?,'
        . 'last_success_at=?,last_observed_at=?,last_raw_count=?,'
        . 'last_acknowledged_count=?,last_error_class=NULL,code_revision=?,'
        . 'updated_at=? WHERE connector_id=?'
    );
    $update->execute(array(
        $verifiedAt,
        $verifiedAt,
        $verifiedAt,
        (int)$receipt['raw_count'],
        (int)$receipt['acknowledged_count'],
        $normalized['code_revision'],
        $verifiedAt,
        $normalized['connector']['connector_id'],
    ));
}

function v2_ingest_require_preview_binding(
    array $config,
    array $normalized
): void {
    if (
        !isset($normalized['expected_release_state'])
        || (string)$normalized['expected_release_state'] !== 'preview'
    ) {
        return;
    }
    $token = isset($_SERVER['HTTP_X_BSIDE_PREVIEW_TOKEN'])
        ? trim((string)$_SERVER['HTTP_X_BSIDE_PREVIEW_TOKEN']) : '';
    if ($token === '' || strlen($token) > 4096) {
        v2_respond(401, array(
            'ok' => false,
            'error' => 'ingest_preview_token_required',
        ));
    }
    $candidate = hash('sha256', $token);
    foreach (v1_preview_token_hashes($config) as $expected) {
        if (hash_equals($expected, $candidate)) {
            return;
        }
    }
    v2_respond(403, array(
        'ok' => false,
        'error' => 'invalid_ingest_preview_token',
    ));
}

/**
 * Bind a classified official-source write to both release-state rows.
 *
 * Apply calls repeat this check with FOR UPDATE before the SourceRight and
 * connector locks, matching the protected cutover lock order. Read-only
 * replay still receives a non-locking boundary check before its early return.
 */
function v2_ingest_assert_release_boundary(
    PDO $pdo,
    array $config,
    array $normalized,
    bool $forUpdate
): void {
    $expected = isset($normalized['expected_release_state'])
        ? (string)$normalized['expected_release_state'] : '';
    if ($expected === '') {
        return;
    }
    $statement = $pdo->prepare(
        'SELECT state_key,release_state FROM '
        . table_name($config, 'governance_release_state')
        . ' WHERE state_key IN (?,?) ORDER BY BINARY state_key'
        . ($forUpdate ? ' FOR UPDATE' : '')
    );
    $statement->execute(array(
        GOV_V1_RELEASE_STATE_KEY,
        GOV_V2_RELEASE_STATE_KEY,
    ));
    $rows = array();
    foreach ($statement->fetchAll() as $row) {
        $rows[(string)$row['state_key']] = (string)$row['release_state'];
    }
    if (
        count($rows) !== 2
        || !isset(
            $rows[GOV_V1_RELEASE_STATE_KEY],
            $rows[GOV_V2_RELEASE_STATE_KEY]
        )
        || !hash_equals(
            $expected,
            (string)$rows[GOV_V1_RELEASE_STATE_KEY]
        )
        || !hash_equals(
            $expected,
            (string)$rows[GOV_V2_RELEASE_STATE_KEY]
        )
    ) {
        throw new RuntimeException(
            'global_ingest_release_state_mismatch'
        );
    }
}

/**
 * Refresh SEC readiness after a real, unchanged current-feed poll.
 *
 * This path is deliberately unavailable to replay-only and completed-day
 * requests. It preserves the durable completed-day checkpoint verbatim and
 * only refreshes when the locked source cursor exactly matches the fetched
 * current-feed cursor, preventing a stale receipt from reviving readiness.
 */
function v2_ingest_refresh_idempotent_current_poll(
    PDO $pdo,
    array $config,
    array $lockedConnector,
    array $normalized,
    string $completedAt
): bool {
    if (
        (string)$normalized['receipt_kind'] !== 'current'
        || (string)$normalized['ingest_mode'] !== 'apply'
        || (int)$normalized['chunk']['index']
            !== (int)$normalized['chunk']['count']
    ) {
        return false;
    }
    $checkpoint = v2_ingest_locked_checkpoint($lockedConnector);
    if (
        $checkpoint === null
        || (int)$checkpoint['schema_version'] !== 2
        || !isset($checkpoint['source_cursor'])
        || !is_string($checkpoint['source_cursor'])
        || $normalized['next_cursor'] === null
        || !hash_equals(
            (string)$checkpoint['source_cursor'],
            (string)$normalized['next_cursor']
        )
        || !isset($lockedConnector['code_revision'])
        || !is_string($lockedConnector['code_revision'])
        || !hash_equals(
            (string)$lockedConnector['code_revision'],
            (string)$normalized['code_revision']
        )
    ) {
        return false;
    }
    $rows = v2_ingest_batch_receipts(
        $pdo,
        $config,
        (string)$lockedConnector['connector_id'],
        (string)$normalized['chunk']['batch_id']
    );
    v2_ingest_assert_stored_batch_complete($rows, $normalized);
    $update = $pdo->prepare(
        'UPDATE ' . table_name($config, 'source_connectors')
        . ' SET connector_status=\'active\',last_checked_at=?,'
        . 'last_success_at=?,last_observed_at=?,last_raw_count=?,'
        . 'last_acknowledged_count=?,last_error_class=NULL,updated_at=?'
        . ' WHERE connector_id=? AND code_revision=?'
    );
    $update->execute(array(
        $completedAt,
        $completedAt,
        $normalized['retrieved_at'],
        $normalized['chunk']['batch_raw_count'],
        $normalized['chunk']['batch_acknowledged_count'],
        $completedAt,
        $lockedConnector['connector_id'],
        $normalized['code_revision'],
    ));
    return true;
}

function v2_ops_ingest(PDO $pdo, array $config): void {
    $payload = v2_json_body($config);
    $requestedConnectorId = (
        isset($payload['envelope'])
        && is_array($payload['envelope'])
        && isset($payload['envelope']['connector_id'])
        && is_string($payload['envelope']['connector_id'])
    ) ? trim((string)$payload['envelope']['connector_id']) : '';
    if (in_array(
        $requestedConnectorId,
        array(
            'connector:jp:edinet',
            'connector:gb:companies-house',
        ),
        true
    )) {
        v2_respond(409, array(
            'ok' => false,
            'error' => 'global_ingest_source_disabled',
            'connector_id' => $requestedConnectorId,
        ));
    }
    try {
        $normalized = v2_normalize_ingest_payload($pdo, $config, $payload);
    } catch (InvalidArgumentException $error) {
        v2_respond(400, array(
            'ok' => false,
            'error' => 'global_ingest_validation_failed',
            'detail' => $error->getMessage(),
        ));
    }
    $deploymentIdentity = v2_deployment_identity_status();
    if (
        $deploymentIdentity['valid'] !== true
        || !isset($deploymentIdentity['code_revision'])
        || !is_string($deploymentIdentity['code_revision'])
        || !hash_equals(
            (string)$deploymentIdentity['code_revision'],
            (string)$normalized['code_revision']
        )
    ) {
        v2_respond(409, array(
            'ok' => false,
            'error' => 'global_ingest_code_revision_mismatch',
        ));
    }
    v2_ingest_require_preview_binding($config, $normalized);
    try {
        v2_ingest_assert_release_boundary(
            $pdo,
            $config,
            $normalized,
            false
        );
    } catch (RuntimeException $error) {
        if (
            $error->getMessage()
                !== 'global_ingest_release_state_mismatch'
        ) {
            throw $error;
        }
        v2_respond(409, array(
            'ok' => false,
            'error' => 'global_ingest_release_state_mismatch',
        ));
    }
    $connector = $normalized['connector'];
    $receiptLookup = $pdo->prepare(
        'SELECT ingest_id,payload_sha256,raw_count,acknowledged_count,'
        . 'request_count,'
        . 'batch_id,chunk_index,chunk_count,batch_raw_count,'
        . 'batch_acknowledged_count,batch_request_count,code_revision '
        . 'FROM ' . table_name($config, 'global_ingest_receipts')
        . ' WHERE connector_id=? AND idempotency_key=? LIMIT 1'
    );
    $receiptLookup->execute(array(
        $connector['connector_id'],
        $normalized['idempotency_key'],
    ));
    $existingReceipt = $receiptLookup->fetch();
    if (
        $existingReceipt
        && (
            (string)$normalized['receipt_kind'] !== 'current'
            || (string)$normalized['ingest_mode'] !== 'apply'
        )
    ) {
        if (
            !hash_equals(
                (string)$existingReceipt['payload_sha256'],
                $normalized['payload_hash']
            )
            || (string)$existingReceipt['code_revision'] !== $normalized['code_revision']
        ) {
            v2_respond(409, array(
                'ok' => false,
                'error' => 'global_ingest_idempotency_conflict',
            ));
        }
        if (
            v2_ingest_is_link_only_apply($normalized)
            && (
                (int)$existingReceipt['raw_count'] >= 1
                || (int)$existingReceipt['acknowledged_count'] >= 1
            )
            && !v2_ingest_link_only_receipt_is_complete(
                $existingReceipt,
                $normalized
            )
        ) {
            v2_respond(409, array(
                'ok' => false,
                'error' => 'global_ingest_batch_receipt_corrupt',
            ));
        }
        // Replay is strictly read-only. A normal CA/AU apply with a
        // non-empty, already acknowledged receipt continues into the locked
        // transaction so the current grant and manifest can authorize a new
        // verification heartbeat.
        if (
            !v2_ingest_is_link_only_apply($normalized)
            || (int)$existingReceipt['raw_count'] < 1
            || (int)$existingReceipt['acknowledged_count'] < 1
        ) {
            v2_respond(200, array(
                'ok' => true,
                'data' => array(
                    'ingest_id' => (string)$existingReceipt['ingest_id'],
                    'connector_id' => (string)$connector['connector_id'],
                    'raw_count' => (int)$existingReceipt['raw_count'],
                    'acknowledged_count' =>
                        (int)$existingReceipt['acknowledged_count'],
                    'idempotent' => true,
                ),
            ));
        }
    }
    $startedAt = gmdate('Y-m-d H:i:s');
    $pdo->beginTransaction();
    try {
        v2_ingest_assert_release_boundary(
            $pdo,
            $config,
            $normalized,
            true
        );
        // Release state was locked first; keep the remaining
        // SourceRight -> connector order used by cutover and the DART bridge.
        // This avoids an ingest/admin deadlock and makes connector inactivity
        // an authoritative kill switch.
        $lockedRight = v2_source_right_row(
            $pdo,
            $config,
            (string)$normalized['right']['source_right_id'],
            true
        );
        if (
            $lockedRight === null
            || count(v2_source_right_ineligible_reasons($lockedRight, 'collect')) > 0
            || !hash_equals(
                v2_source_right_revision($lockedRight),
                $normalized['rights_revision']
            )
        ) {
            throw new RuntimeException('global_source_right_changed');
        }
        v2_ingest_assert_locked_link_only_right(
            $normalized,
            $lockedRight
        );
        $connectorLock = $pdo->prepare(
            'SELECT connector_id,country_code,source_key,source_type,'
            . 'source_right_id,coverage_mode,connector_status,cursor_json,'
            . 'code_revision FROM '
            . table_name($config, 'source_connectors')
            . ' WHERE connector_id=? LIMIT 1 FOR UPDATE'
        );
        $connectorLock->execute(array($connector['connector_id']));
        $lockedConnector = $connectorLock->fetch();
        if (
            !$lockedConnector
            || (string)$lockedConnector['country_code']
                !== (string)$connector['country_code']
            || (string)$lockedConnector['source_key']
                !== (string)$connector['source_key']
            || (string)$lockedConnector['source_type']
                !== (string)$connector['source_type']
            || (string)$lockedConnector['source_right_id']
                !== (string)$connector['source_right_id']
            || (string)$lockedConnector['coverage_mode']
                !== (string)$connector['coverage_mode']
            || !in_array(
                (string)$lockedConnector['connector_status'],
                array('configured', 'active', 'degraded'),
                true
            )
        ) {
            throw new RuntimeException('global_connector_changed');
        }
        $lockedReceipt = $pdo->prepare(
            'SELECT ingest_id,payload_sha256,raw_count,acknowledged_count,'
            . 'request_count,'
            . 'batch_id,chunk_index,chunk_count,batch_raw_count,'
            . 'batch_acknowledged_count,batch_request_count,code_revision FROM '
            . table_name($config, 'global_ingest_receipts')
            . ' WHERE connector_id=? AND idempotency_key=? LIMIT 1 FOR UPDATE'
        );
        $lockedReceipt->execute(array(
            $connector['connector_id'],
            $normalized['idempotency_key'],
        ));
        $storedReceipt = $lockedReceipt->fetch();
        if ($storedReceipt) {
            if (
                !hash_equals(
                    (string)$storedReceipt['payload_sha256'],
                    $normalized['payload_hash']
                )
                || (string)$storedReceipt['code_revision']
                    !== $normalized['code_revision']
            ) {
                throw new RuntimeException(
                    'global_ingest_idempotency_conflict'
                );
            }
            if (v2_ingest_is_link_only_apply($normalized)) {
                if (!v2_ingest_link_only_receipt_is_complete(
                    $storedReceipt,
                    $normalized
                )) {
                    throw new RuntimeException(
                        'global_ingest_batch_receipt_corrupt'
                    );
                }
                v2_ingest_refresh_link_only_connector(
                    $pdo,
                    $config,
                    $normalized,
                    $storedReceipt,
                    gmdate('Y-m-d H:i:s')
                );
            } else {
                v2_ingest_refresh_idempotent_current_poll(
                    $pdo,
                    $config,
                    $lockedConnector,
                    $normalized,
                    gmdate('Y-m-d H:i:s')
                );
            }
            $pdo->commit();
            v2_respond(200, array(
                'ok' => true,
                'data' => array(
                    'ingest_id' => (string)$storedReceipt['ingest_id'],
                    'connector_id' => (string)$connector['connector_id'],
                    'raw_count' => (int)$storedReceipt['raw_count'],
                    'acknowledged_count' =>
                        (int)$storedReceipt['acknowledged_count'],
                    'idempotent' => true,
                ),
            ));
        }
        if ($normalized['ingest_mode'] === 'replay') {
            throw new RuntimeException('global_ingest_replay_missing');
        }
        $chunk = $normalized['chunk'];
        $batchRows = v2_ingest_batch_receipts(
            $pdo,
            $config,
            (string)$connector['connector_id'],
            (string)$chunk['batch_id']
        );
        v2_ingest_assert_batch_prefix($batchRows, $normalized);
        $acknowledged = 0;
        foreach ($normalized['records'] as $record) {
            v2_ingest_upsert_record(
                $pdo,
                $config,
                $record,
                $lockedRight,
                $startedAt
            );
            $acknowledged++;
        }
        foreach ($normalized['lifecycle_observations'] as $observation) {
            v2_ingest_lifecycle_observation(
                $pdo,
                $config,
                $connector,
                $observation,
                $startedAt
            );
            $acknowledged++;
        }
        if ($acknowledged !== (int)$normalized['acknowledged_count']) {
            throw new RuntimeException('global_ingest_acknowledgment_mismatch');
        }
        $ingestId = v1_stable_id(
            'global-ingest',
            (string)$connector['connector_id'] . '|'
            . (string)$normalized['idempotency_key']
        );
        $completedAt = gmdate('Y-m-d H:i:s');
        $receipt = $pdo->prepare(
            'INSERT INTO ' . table_name($config, 'global_ingest_receipts')
            . ' (ingest_id,connector_id,idempotency_key,payload_sha256,batch_id,'
            . 'chunk_index,chunk_count,window_start,window_end_exclusive,raw_count,'
            . 'acknowledged_count,request_count,batch_raw_count,'
            . 'batch_acknowledged_count,batch_request_count,code_revision,'
            . 'started_at,completed_at,created_at)'
            . ' VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        );
        $receipt->execute(array(
            $ingestId,
            $connector['connector_id'],
            $normalized['idempotency_key'],
            $normalized['payload_hash'],
            $chunk['batch_id'],
            $chunk['index'],
            $chunk['count'],
            $chunk['window_start'],
            $chunk['window_end_exclusive'],
            $normalized['raw_count'],
            $acknowledged,
            $normalized['request_count'],
            $chunk['batch_raw_count'],
            $chunk['batch_acknowledged_count'],
            $chunk['batch_request_count'],
            $normalized['code_revision'],
            $startedAt,
            $completedAt,
            $startedAt,
        ));
        if ((int)$chunk['index'] === (int)$chunk['count']) {
            $completeRows = v2_ingest_batch_receipts(
                $pdo,
                $config,
                (string)$connector['connector_id'],
                (string)$chunk['batch_id']
            );
            v2_ingest_assert_batch_complete(
                $completeRows,
                $normalized
            );
            $existingCursor = v2_ingest_locked_checkpoint($lockedConnector);
            $shouldAdvance = v2_ingest_checkpoint_should_advance(
                $existingCursor,
                $normalized
            );
            $shouldRefreshLinkOnly = (
                v2_ingest_is_link_only_apply($normalized)
                && (int)$chunk['batch_raw_count'] >= 1
                && (int)$chunk['batch_acknowledged_count'] >= 1
            );
            // A zero-record link-only receipt is not proof that an approved
            // link was observed, so it cannot advance freshness or checkpoint
            // state. A non-empty verification refreshes freshness even when
            // its historical day boundary is already durable.
            $shouldUpdateConnector = v2_ingest_is_link_only_apply($normalized)
                ? $shouldRefreshLinkOnly : $shouldAdvance;
            if ($shouldUpdateConnector) {
                $cursorJson = (string)$lockedConnector['cursor_json'];
                if ($shouldAdvance) {
                    $cursorPayload = array(
                        'schema_version' => 1,
                        'window_end_exclusive' => $chunk['window_end_exclusive'],
                        'batch_id' => $chunk['batch_id'],
                    );
                    if ($normalized['next_cursor'] !== null) {
                        $cursorPayload['schema_version'] = 2;
                        $cursorPayload['source_cursor'] = $normalized['next_cursor'];
                    } elseif (
                        $existingCursor !== null
                        && (int)$existingCursor['schema_version'] === 2
                    ) {
                        $cursorPayload['schema_version'] = 2;
                        $cursorPayload['source_cursor'] =
                            $existingCursor['source_cursor'];
                    }
                    $cursorJson = json_value($cursorPayload);
                }
                $observedAt = v2_ingest_is_link_only_apply($normalized)
                    ? $completedAt : $normalized['retrieved_at'];
                $updateConnector = $pdo->prepare(
                    'UPDATE ' . table_name($config, 'source_connectors')
                    . ' SET connector_status=\'active\',cursor_json=?,last_checked_at=?,'
                    . 'last_success_at=?,last_observed_at=?,last_raw_count=?,'
                    . 'last_acknowledged_count=?,last_error_class=NULL,code_revision=?,'
                    . 'updated_at=? WHERE connector_id=?'
                );
                $updateConnector->execute(array(
                    $cursorJson,
                    $completedAt,
                    $completedAt,
                    $observedAt,
                    $chunk['batch_raw_count'],
                    $chunk['batch_acknowledged_count'],
                    $normalized['code_revision'],
                    $completedAt,
                    $connector['connector_id'],
                ));
            }
        }
        $pdo->commit();
    } catch (Throwable $error) {
        if ($pdo->inTransaction()) {
            $pdo->rollBack();
        }
        if ((string)$error->getCode() === '23000') {
            $retryReceipt = $pdo->prepare(
                'SELECT ingest_id,payload_sha256,raw_count,acknowledged_count,'
                . 'code_revision FROM '
                . table_name($config, 'global_ingest_receipts')
                . ' WHERE connector_id=? AND idempotency_key=? LIMIT 1'
            );
            $retryReceipt->execute(array(
                $connector['connector_id'],
                $normalized['idempotency_key'],
            ));
            $storedRetry = $retryReceipt->fetch();
            if (
                $storedRetry
                && hash_equals(
                    (string)$storedRetry['payload_sha256'],
                    $normalized['payload_hash']
                )
                && (string)$storedRetry['code_revision']
                    === $normalized['code_revision']
            ) {
                v2_respond(200, array(
                    'ok' => true,
                    'data' => array(
                        'ingest_id' => (string)$storedRetry['ingest_id'],
                        'connector_id' => (string)$connector['connector_id'],
                        'raw_count' => (int)$storedRetry['raw_count'],
                        'acknowledged_count' => (int)$storedRetry['acknowledged_count'],
                        'idempotent' => true,
                    ),
                ));
            }
        }
        $known = array(
            'global_connector_changed',
            'global_connector_checkpoint_corrupt',
            'global_source_right_changed',
            'global_ingest_acknowledgment_mismatch',
            'global_ingest_idempotency_conflict',
            'global_ingest_release_state_mismatch',
            'global_ingest_replay_missing',
            'global_ingest_chunk_out_of_order',
            'global_ingest_batch_metadata_conflict',
            'global_ingest_batch_receipt_corrupt',
            'global_ingest_batch_incomplete',
            'global_ingest_batch_totals_mismatch',
            'global_issuer_country_conflict',
            'global_issuer_identifier_conflict',
            'global_issuer_listing_conflict',
            'global_document_identity_conflict',
            'global_document_external_id_conflict',
            'global_document_hash_contract_conflict',
            'global_event_identity_conflict',
            'global_lifecycle_observation_conflict',
            'global_lifecycle_resolution_conflict',
        );
        if (in_array($error->getMessage(), $known, true)
            || (string)$error->getCode() === '23000') {
            v2_respond(409, array(
                'ok' => false,
                'error' => in_array($error->getMessage(), $known, true)
                    ? $error->getMessage() : 'global_ingest_constraint_conflict',
            ));
        }
        throw $error;
    }
    v2_respond(200, array(
        'ok' => true,
        'data' => array(
            'ingest_id' => $ingestId,
            'connector_id' => (string)$connector['connector_id'],
            'raw_count' => (int)$normalized['raw_count'],
            'acknowledged_count' => (int)$normalized['acknowledged_count'],
            'idempotent' => false,
            'public_events_created' => 0,
            'review_required' => count($normalized['records']),
        ),
    ));
}

function v2_global_comparison_key(
    string $issuerId,
    string $eventFamily,
    string $action,
    string $target,
    string $actorId,
    string $effectiveAt,
    ?string $deadlineAt
): string {
    $identity = array(
        'issuer_id' => $issuerId,
        'event_family' => $eventFamily,
        'action' => v1_normalize_identity_text($action),
        'target' => v1_normalize_identity_text($target),
        'actor_id' => v1_normalize_identity_text($actorId),
        'effective_at' => $effectiveAt,
        'deadline_at' => $deadlineAt,
    );
    return 'global:' . substr(
        hash(
            'sha256',
            v1_strict_canonical_json_encode(
                $identity,
                'global_event_identity_encode_failed'
            )
        ),
        0,
        64
    );
}

function v2_publish_event_documents(
    PDO $pdo,
    array $config,
    string $eventId,
    string $now
): int {
    $publish = $pdo->prepare(
        'UPDATE ' . table_name($config, 'documents') . ' pd'
        . ' JOIN ' . table_name($config, 'event_documents') . ' ped'
        . ' ON ped.document_id=pd.document_id'
        . ' JOIN ' . table_name($config, 'source_rights') . ' psr'
        . ' ON psr.source_right_id=pd.source_right_id'
        . ' SET pd.publication_status=\'published\','
        . 'pd.published_at=COALESCE(pd.published_at,pd.filed_at),pd.updated_at=?'
        . ' WHERE ped.event_id=?'
        . ' AND pd.issuer_id IS NOT NULL'
        . ' AND pd.verification_status=\'official\''
        . ' AND ' . source_right_redistribution_sql('psr')
    );
    $publish->execute(array($now, $eventId));
    $count = $pdo->prepare(
        'SELECT COUNT(*) FROM ' . table_name($config, 'event_documents') . ' ced'
        . ' JOIN ' . table_name($config, 'documents') . ' cd'
        . ' ON cd.document_id=ced.document_id'
        . ' JOIN ' . table_name($config, 'source_rights') . ' csr'
        . ' ON csr.source_right_id=cd.source_right_id'
        . ' WHERE ced.event_id=? AND cd.issuer_id IS NOT NULL'
        . ' AND cd.verification_status=\'official\''
        . ' AND cd.publication_status=\'published\''
        . ' AND ' . source_right_redistribution_sql('csr')
    );
    $count->execute(array($eventId));
    return (int)$count->fetchColumn();
}

function v2_merge_reviewed_event(
    PDO $pdo,
    array $config,
    array $sourceEvent,
    string $canonicalEventId,
    string $comparisonKey,
    string $reason,
    string $role,
    string $now
): void {
    $canonical = $pdo->prepare(
        'SELECT event_id,issuer_id,country_code,global_event_family,'
        . 'identity_status,review_status,publication_status,comparison_key '
        . 'FROM ' . table_name($config, 'governance_events')
        . ' WHERE event_id=? LIMIT 1 FOR UPDATE'
    );
    $canonical->execute(array($canonicalEventId));
    $target = $canonical->fetch();
    if (
        !$target
        || (string)$target['issuer_id'] !== (string)$sourceEvent['issuer_id']
        || (string)$target['country_code'] !== (string)$sourceEvent['country_code']
        || (string)$target['global_event_family']
            !== (string)$sourceEvent['global_event_family']
        || (string)$target['identity_status'] !== 'complete'
        || !in_array(
            (string)$target['review_status'],
            array('approved', 'not_required'),
            true
        )
        || (string)$target['publication_status'] !== 'published'
    ) {
        throw new RuntimeException('canonical_merge_target_not_publishable');
    }
    $moveDocuments = $pdo->prepare(
        'INSERT IGNORE INTO ' . table_name($config, 'event_documents')
        . ' (event_id,document_id,relation_type,position_no,created_at)'
        . ' SELECT ?,document_id,relation_type,position_no,? FROM '
        . table_name($config, 'event_documents') . ' WHERE event_id=?'
    );
    $moveDocuments->execute(array(
        $canonicalEventId,
        $now,
        (string)$sourceEvent['event_id'],
    ));
    $removeSourceDocuments = $pdo->prepare(
        'DELETE FROM ' . table_name($config, 'event_documents')
        . ' WHERE event_id=?'
    );
    $removeSourceDocuments->execute(array((string)$sourceEvent['event_id']));
    $observations = $pdo->prepare(
        'SELECT document_id,source_class,source_key,first_observed_at,observed_at,'
        . 'payload_hash,payload_json FROM '
        . table_name($config, 'event_observations')
        . ' WHERE event_id=? ORDER BY observation_id'
    );
    $observations->execute(array((string)$sourceEvent['event_id']));
    $insertObservation = $pdo->prepare(
        'INSERT INTO ' . table_name($config, 'event_observations')
        . ' (observation_id,event_id,document_id,source_class,source_key,'
        . 'first_observed_at,observed_at,payload_hash,payload_json,created_at,updated_at)'
        . ' VALUES (?,?,?,?,?,?,?,?,?,?,?)'
        . ' ON DUPLICATE KEY UPDATE '
        . 'first_observed_at=LEAST(first_observed_at,VALUES(first_observed_at)),'
        . 'observed_at=GREATEST(observed_at,VALUES(observed_at)),'
        . 'payload_hash=VALUES(payload_hash),payload_json=VALUES(payload_json),'
        . 'updated_at=VALUES(updated_at)'
    );
    foreach ($observations->fetchAll() as $observation) {
        $newObservationId = v1_stable_id(
            'observation',
            $canonicalEventId . '|' . (string)$observation['document_id']
            . '|' . (string)$observation['source_key']
        );
        $insertObservation->execute(array(
            $newObservationId,
            $canonicalEventId,
            $observation['document_id'],
            $observation['source_class'],
            $observation['source_key'],
            $observation['first_observed_at'],
            $observation['observed_at'],
            $observation['payload_hash'],
            $observation['payload_json'],
            $now,
            $now,
        ));
    }
    $moveActors = $pdo->prepare(
        'INSERT IGNORE INTO ' . table_name($config, 'event_actors')
        . ' (event_id,actor_id,actor_role,review_status,created_at,updated_at)'
        . ' SELECT ?,actor_id,actor_role,review_status,created_at,? FROM '
        . table_name($config, 'event_actors') . ' WHERE event_id=?'
    );
    $moveActors->execute(array(
        $canonicalEventId,
        $now,
        (string)$sourceEvent['event_id'],
    ));
    $lifecycle = $pdo->prepare(
        'UPDATE ' . table_name($config, 'global_lifecycle_observations')
        . ' SET resolved_event_id=?,updated_at=? WHERE resolved_event_id=?'
    );
    $lifecycle->execute(array(
        $canonicalEventId,
        $now,
        (string)$sourceEvent['event_id'],
    ));
    $sourcePayload = array(
        'merged_into_event_id' => $canonicalEventId,
        'comparison_key' => $comparisonKey,
        'merged_at' => gmdate('c'),
    );
    $retire = $pdo->prepare(
        'UPDATE ' . table_name($config, 'governance_events')
        . ' SET review_status=\'merged\',publication_status=\'draft\','
        . 'identity_status=\'merged\',comparison_key=NULL,current_status=\'merged\','
        . 'payload_json=?,updated_at=? WHERE event_id=?'
    );
    $retire->execute(array(
        json_value($sourcePayload),
        $now,
        (string)$sourceEvent['event_id'],
    ));
    $touchTarget = $pdo->prepare(
        'UPDATE ' . table_name($config, 'governance_events')
        . ' SET updated_at=? WHERE event_id=?'
    );
    $touchTarget->execute(array($now, $canonicalEventId));
    $revisionId = v1_stable_id(
        'revision',
        (string)$sourceEvent['event_id'] . '|merge|' . $canonicalEventId
        . '|' . $now
    );
    $revision = $pdo->prepare(
        'INSERT INTO ' . table_name($config, 'editorial_revisions')
        . ' (revision_id,entity_type,entity_id,field_name,previous_value,'
        . 'revised_value,reason,revision_status,requested_by,reviewed_by,'
        . 'reviewed_at,published_at,created_at,updated_at)'
        . ' VALUES (?,\'event\',?,\'canonical_event_id\',NULL,?,?,'
        . '\'internal_merged\',?,?,?,NULL,?,?)'
    );
    $revision->execute(array(
        $revisionId,
        (string)$sourceEvent['event_id'],
        $canonicalEventId,
        $reason,
        $role,
        $role,
        $now,
        $now,
        $now,
    ));
}

function v2_admin_review_queue(PDO $pdo, array $config): void {
    $page = v2_list_params();
    $country = '';
    if (isset($_GET['country']) && trim((string)$_GET['country']) !== '') {
        $country = (string)v2_valid_country((string)$_GET['country']);
        if ($country === '') {
            v2_respond(400, array('ok' => false, 'error' => 'invalid_country'));
        }
    }
    $where = array(
        'e.issuer_id IS NOT NULL',
        'e.identity_status=\'needs_review\'',
        'e.review_status=\'pending\'',
        'e.publication_status=\'draft\'',
    );
    $params = array();
    if ($country !== '') {
        $where[] = 'e.country_code=?';
        $params[] = $country;
    }
    $sql = 'SELECT e.event_id,e.issuer_id,i.legal_name AS issuer_name,'
        . 'e.country_code AS country,e.global_event_family AS event_family,'
        . 'e.title,JSON_UNQUOTE(JSON_EXTRACT(e.payload_json,'
        . '\'$.metadata.title_provenance\')) AS title_provenance,'
        . 'e.original_language,e.occurred_at,e.first_observed_at,'
        . 'e.verification_status,e.change_type,e.importance,e.updated_at,'
        . '(SELECT COUNT(*) FROM ' . table_name($config, 'event_documents') . ' qed'
        . ' JOIN ' . table_name($config, 'documents') . ' qd'
        . ' ON qd.document_id=qed.document_id'
        . ' LEFT JOIN ' . table_name($config, 'source_rights') . ' qsr'
        . ' ON qsr.source_right_id=qd.source_right_id'
        . ' WHERE qed.event_id=e.event_id'
        . ' AND qd.issuer_id IS NOT NULL'
        . ' AND qd.verification_status=\'official\''
        . ' AND ' . source_right_redistribution_sql('qsr')
        . ') AS visible_evidence_count '
        . 'FROM ' . table_name($config, 'governance_events') . ' e'
        . ' JOIN ' . table_name($config, 'issuers') . ' i ON i.issuer_id=e.issuer_id'
        . ' WHERE ' . implode(' AND ', $where)
        . ' ORDER BY CASE e.importance WHEN \'critical\' THEN 0'
        . ' WHEN \'market_sensitive\' THEN 1 WHEN \'high\' THEN 2 ELSE 3 END,'
        . 'e.occurred_at DESC,e.event_id'
        . ' LIMIT ' . ((int)$page['limit'] + 1)
        . ' OFFSET ' . (int)$page['offset'];
    $statement = $pdo->prepare($sql);
    $statement->execute($params);
    list($rows, $hasMore) = v2_fetch_page($statement, $page);
    foreach ($rows as &$row) {
        $row['visible_evidence_count'] = (int)$row['visible_evidence_count'];
        $row['updated_at'] = v1_release_iso_time(
            isset($row['updated_at']) ? $row['updated_at'] : null
        );
    }
    unset($row);
    v2_respond(200, array(
        'ok' => true,
        'data' => array('items' => $rows),
        'meta' => v2_page_meta($page, count($rows), $hasMore),
    ));
}

function v2_admin_review_event(
    PDO $pdo,
    array $config,
    string $eventId,
    string $role
): void {
    $payload = v2_json_body($config);
    try {
        v2_write_assert_keys($payload, array(
            'decision', 'expected_updated_at', 'reason', 'identity_action',
            'identity_target', 'identity_effective_at', 'identity_deadline_at',
            'event_family', 'importance', 'summary', 'current_status', 'actor',
            'merge_into_event_id',
        ), 'payload');
        $decision = v2_write_code(
            $payload,
            'decision',
            'payload',
            '/^(approve|reject)$/'
        );
        $expectedUpdatedAt = v2_write_timestamp(
            $payload,
            'expected_updated_at',
            'payload'
        );
        $reason = v2_write_text($payload, 'reason', 'payload', 2000);
        if ($reason === null || mb_strlen($reason, 'UTF-8') < 8) {
            v2_write_invalid('payload.reason: at least 8 characters required');
        }
    } catch (InvalidArgumentException $error) {
        v2_respond(400, array(
            'ok' => false,
            'error' => 'event_review_validation_failed',
            'detail' => $error->getMessage(),
        ));
    }
    $now = gmdate('Y-m-d H:i:s');
    $pdo->beginTransaction();
    try {
        $statement = $pdo->prepare(
            'SELECT event_id,issuer_id,country_code,global_event_family,title,'
            . 'original_language,occurred_at,verification_status,change_type,'
            . 'review_status,publication_status,updated_at FROM '
            . table_name($config, 'governance_events')
            . ' WHERE event_id=? LIMIT 1 FOR UPDATE'
        );
        $statement->execute(array($eventId));
        $event = $statement->fetch();
        if (!$event) {
            $pdo->rollBack();
            v2_respond(404, array('ok' => false, 'error' => 'event_not_found'));
        }
        if ((string)$event['updated_at'] !== $expectedUpdatedAt) {
            $pdo->rollBack();
            v2_respond(409, array(
                'ok' => false,
                'error' => 'stale_event_review',
                'current_updated_at' => (string)$event['updated_at'],
            ));
        }
        if ($decision === 'reject') {
            $reject = $pdo->prepare(
                'UPDATE ' . table_name($config, 'governance_events')
                . ' SET review_status=\'rejected\',publication_status=\'draft\','
                . 'identity_status=\'rejected\',comparison_key=NULL,updated_at=?'
                . ' WHERE event_id=? AND updated_at=?'
            );
            $reject->execute(array($now, $eventId, $expectedUpdatedAt));
            if ($reject->rowCount() !== 1) {
                throw new RuntimeException('stale_event_review');
            }
            $revisionId = v1_stable_id(
                'revision',
                $eventId . '|reject|' . $now . '|' . $reason
            );
            $revision = $pdo->prepare(
                'INSERT INTO ' . table_name($config, 'editorial_revisions')
                . ' (revision_id,entity_type,entity_id,field_name,previous_value,'
                . 'revised_value,reason,revision_status,requested_by,reviewed_by,'
                . 'reviewed_at,published_at,created_at,updated_at)'
                . ' VALUES (?,\'event\',?,\'review_status\',?,\'rejected\',?,'
                . '\'internal_rejected\',?,?,?,NULL,?,?)'
            );
            $revision->execute(array(
                $revisionId,
                $eventId,
                (string)$event['review_status'],
                $reason,
                $role,
                $role,
                $now,
                $now,
                $now,
            ));
            $pdo->commit();
            v2_respond(200, array(
                'ok' => true,
                'data' => array(
                    'event_id' => $eventId,
                    'decision' => 'rejected',
                    'published' => false,
                ),
            ));
        }
        try {
            $reviewedEventFamily = v2_write_code(
                $payload,
                'event_family',
                'payload',
                '/^(large_ownership|meeting_and_vote|tender_offer_and_mna|capital_issuance|capital_return|board_and_compensation|listing_status|correction_and_withdrawal)$/',
                false
            );
            if ($reviewedEventFamily === null) {
                $reviewedEventFamily = (string)$event['global_event_family'];
            }
            if ($reviewedEventFamily === 'unclassified') {
                v2_write_invalid(
                    'payload.event_family: editor classification required'
                );
            }
            $action = v2_write_text(
                $payload,
                'identity_action',
                'payload',
                255
            );
            $target = v2_write_text(
                $payload,
                'identity_target',
                'payload',
                700
            );
            $effectiveAt = v2_write_timestamp(
                $payload,
                'identity_effective_at',
                'payload'
            );
            $deadlineAt = v2_write_timestamp(
                $payload,
                'identity_deadline_at',
                'payload',
                false
            );
            $importance = v2_write_code(
                $payload,
                'importance',
                'payload',
                '/^(low|medium|high|critical|market_sensitive)$/'
            );
            $summary = v2_write_text($payload, 'summary', 'payload', 4000);
            $currentStatus = v2_write_text(
                $payload,
                'current_status',
                'payload',
                64
            );
            $actor = isset($payload['actor']) && is_array($payload['actor'])
                && !v2_write_is_list($payload['actor']) ? $payload['actor'] : null;
            if ($actor === null) {
                v2_write_invalid('payload.actor: object required');
            }
            v2_write_assert_keys($actor, array(
                'actor_id', 'display_name', 'actor_type', 'actor_role', 'country_code',
            ), 'payload.actor');
            $actorId = v2_write_code(
                $actor,
                'actor_id',
                'payload.actor',
                '/^[A-Za-z0-9_.:\-]{1,64}$/'
            );
            $actorName = v2_write_text(
                $actor,
                'display_name',
                'payload.actor',
                255
            );
            $actorType = v2_write_code(
                $actor,
                'actor_type',
                'payload.actor',
                '/^[a-z][a-z0-9_]{1,39}$/'
            );
            $actorRole = v2_write_code(
                $actor,
                'actor_role',
                'payload.actor',
                '/^[a-z][a-z0-9_]{1,39}$/'
            );
            $actorCountry = v2_write_code(
                $actor,
                'country_code',
                'payload.actor',
                '/^(KR|US|JP|GB|CA|AU)$/'
            );
            $mergeIntoEventId = v2_write_code(
                $payload,
                'merge_into_event_id',
                'payload',
                '/^[A-Za-z0-9_.:\-]{1,96}$/',
                false
            );
        } catch (InvalidArgumentException $error) {
            $pdo->rollBack();
            v2_respond(400, array(
                'ok' => false,
                'error' => 'event_review_validation_failed',
                'detail' => $error->getMessage(),
            ));
        }
        $evidence = $pdo->prepare(
            'SELECT COUNT(*) FROM ' . table_name($config, 'event_documents') . ' red'
            . ' JOIN ' . table_name($config, 'documents') . ' rd'
            . ' ON rd.document_id=red.document_id'
            . ' LEFT JOIN ' . table_name($config, 'source_rights') . ' rsr'
            . ' ON rsr.source_right_id=rd.source_right_id'
            . ' WHERE red.event_id=?'
            . ' AND rd.issuer_id IS NOT NULL'
            . ' AND rd.verification_status=\'official\''
            . ' AND rd.source_class IN (\'official_disclosure\',\'official_register\','
            . '\'company_statement\',\'official_issuer\')'
            . ' AND ' . source_right_redistribution_sql('rsr')
        );
        $evidence->execute(array($eventId));
        if ((int)$evidence->fetchColumn() < 1) {
            throw new RuntimeException('event_official_evidence_required');
        }
        $normalizedAction = v1_normalize_identity_text((string)$action);
        $normalizedTarget = v1_normalize_identity_text((string)$target);
        $normalizedActorId = v1_normalize_identity_text((string)$actorId);
        $comparisonKey = v2_global_comparison_key(
            (string)$event['issuer_id'],
            (string)$reviewedEventFamily,
            $normalizedAction,
            $normalizedTarget,
            $normalizedActorId,
            (string)$effectiveAt,
            $deadlineAt
        );
        $conflict = $pdo->prepare(
            'SELECT event_id FROM ' . table_name($config, 'governance_events')
            . ' WHERE event_id<>? AND identity_status=\'complete\''
            . ' AND (comparison_key=? OR (issuer_id=? AND global_event_family=?'
            . ' AND identity_action=? AND identity_target=?'
            . ' AND LOWER(TRIM(identity_actor_id))=?'
            . ' AND identity_effective_at=?'
            . ' AND identity_deadline_at <=> ?))'
            . ' ORDER BY CASE review_status WHEN \'approved\' THEN 0 ELSE 1 END,'
            . 'event_id LIMIT 1 FOR UPDATE'
        );
        $conflict->execute(array(
            $eventId,
            $comparisonKey,
            (string)$event['issuer_id'],
            (string)$reviewedEventFamily,
            $normalizedAction,
            $normalizedTarget,
            $normalizedActorId,
            $effectiveAt,
            $deadlineAt,
        ));
        $conflictingEvent = $conflict->fetchColumn();
        if ($conflictingEvent !== false) {
            if (
                $mergeIntoEventId === null
                || !hash_equals((string)$conflictingEvent, $mergeIntoEventId)
            ) {
                $pdo->rollBack();
                v2_respond(409, array(
                    'ok' => false,
                    'error' => 'event_comparison_key_conflict',
                    'conflicting_event_id' => (string)$conflictingEvent,
                    'merge_requires_explicit_target' => true,
                ));
            }
            if (v2_publish_event_documents($pdo, $config, $eventId, $now) < 1) {
                throw new RuntimeException('event_official_evidence_required');
            }
            $event['global_event_family'] = $reviewedEventFamily;
            v2_merge_reviewed_event(
                $pdo,
                $config,
                $event,
                $mergeIntoEventId,
                $comparisonKey,
                (string)$reason,
                $role,
                $now
            );
            $pdo->commit();
            v2_respond(200, array(
                'ok' => true,
                'data' => array(
                    'event_id' => $eventId,
                    'decision' => 'merged',
                    'published' => false,
                    'canonical_event_id' => $mergeIntoEventId,
                ),
            ));
        }
        $actorLookup = $pdo->prepare(
            'SELECT display_name,actor_type,country_code FROM '
            . table_name($config, 'actors')
            . ' WHERE actor_id=? LIMIT 1 FOR UPDATE'
        );
        $actorLookup->execute(array($actorId));
        $existingActor = $actorLookup->fetch();
        if ($existingActor && (
            (string)$existingActor['display_name'] !== $actorName
            || (string)$existingActor['actor_type'] !== $actorType
            || (string)$existingActor['country_code'] !== $actorCountry
        )) {
            throw new RuntimeException('event_actor_identity_conflict');
        }
        $actorInsert = $pdo->prepare(
            'INSERT INTO ' . table_name($config, 'actors')
            . ' (actor_id,actor_type,display_name,display_name_en,company_id,'
            . 'country_code,aliases_json,homepage_url,review_status,record_status,'
            . 'created_at,updated_at)'
            . ' VALUES (?,?,?,NULL,NULL,?,NULL,NULL,\'approved\',\'active\',?,?)'
            . ' ON DUPLICATE KEY UPDATE review_status=\'approved\','
            . 'record_status=\'active\',updated_at=VALUES(updated_at)'
        );
        $actorInsert->execute(array(
            $actorId,
            $actorType,
            $actorName,
            $actorCountry,
            $now,
            $now,
        ));
        $eventActor = $pdo->prepare(
            'INSERT INTO ' . table_name($config, 'event_actors')
            . ' (event_id,actor_id,actor_role,review_status,created_at,updated_at)'
            . ' VALUES (?,?,?,\'approved\',?,?)'
            . ' ON DUPLICATE KEY UPDATE actor_role=VALUES(actor_role),'
            . 'review_status=\'approved\',updated_at=VALUES(updated_at)'
        );
        $eventActor->execute(array(
            $eventId,
            $actorId,
            $actorRole,
            $now,
            $now,
        ));
        if (v2_publish_event_documents($pdo, $config, $eventId, $now) < 1) {
            throw new RuntimeException('event_official_evidence_required');
        }
        $verification = (string)$event['verification_status'];
        if (!in_array($verification, array('withdrawn', 'corrected'), true)) {
            $verification = 'official';
        }
        $approve = $pdo->prepare(
            'UPDATE ' . table_name($config, 'governance_events')
            . ' SET global_event_family=?,event_type=?,summary=?,deadline_at=?,'
            . 'importance=?,verification_status=?,'
            . 'current_status=?,review_status=\'approved\','
            . 'publication_status=\'published\',identity_action=?,identity_target=?,'
            . 'identity_actor_id=?,identity_effective_at=?,identity_deadline_at=?,'
            . 'identity_status=\'complete\',comparison_key=?,updated_at=?'
            . ' WHERE event_id=? AND updated_at=?'
        );
        $approve->execute(array(
            $reviewedEventFamily,
            $reviewedEventFamily,
            $summary,
            $deadlineAt,
            $importance,
            $verification,
            $currentStatus,
            $normalizedAction,
            $normalizedTarget,
            $actorId,
            $effectiveAt,
            $deadlineAt,
            $comparisonKey,
            $now,
            $eventId,
            $expectedUpdatedAt,
        ));
        if ($approve->rowCount() !== 1) {
            throw new RuntimeException('stale_event_review');
        }
        $revisionId = v1_stable_id(
            'revision',
            $eventId . '|approve|' . $now . '|' . $reason
        );
        $revision = $pdo->prepare(
            'INSERT INTO ' . table_name($config, 'editorial_revisions')
            . ' (revision_id,entity_type,entity_id,field_name,previous_value,'
            . 'revised_value,reason,revision_status,requested_by,reviewed_by,'
            . 'reviewed_at,published_at,created_at,updated_at)'
            . ' VALUES (?,\'event\',?,\'review_status\',?,\'approved\',?,'
            . '\'internal_approved\',?,?,?,NULL,?,?)'
        );
        $revision->execute(array(
            $revisionId,
            $eventId,
            (string)$event['review_status'],
            $reason,
            $role,
            $role,
            $now,
            $now,
            $now,
        ));
        $pdo->commit();
    } catch (Throwable $error) {
        if ($pdo->inTransaction()) {
            $pdo->rollBack();
        }
        $known = array(
            'event_official_evidence_required',
            'event_actor_identity_conflict',
            'canonical_merge_target_not_publishable',
            'stale_event_review',
        );
        if (in_array($error->getMessage(), $known, true)
            || (string)$error->getCode() === '23000') {
            v2_respond(409, array(
                'ok' => false,
                'error' => in_array($error->getMessage(), $known, true)
                    ? $error->getMessage() : 'event_review_constraint_conflict',
            ));
        }
        throw $error;
    }
    v2_respond(200, array(
        'ok' => true,
        'data' => array(
            'event_id' => $eventId,
            'decision' => 'approved',
            'published' => true,
            'event_family' => $reviewedEventFamily,
            'comparison_key' => $comparisonKey,
        ),
    ));
}

function v2_admin_brief_candidates(PDO $pdo, array $config): void {
    $limit = isset($_GET['limit'])
        ? max(1, min(100, (int)$_GET['limit'])) : 50;
    $country = '';
    if (isset($_GET['country']) && trim((string)$_GET['country']) !== '') {
        $country = (string)v2_valid_country((string)$_GET['country']);
        if ($country === '') {
            v2_respond(400, array('ok' => false, 'error' => 'invalid_country'));
        }
    }
    $sql = v2_event_select($config)
        . ' WHERE ' . v2_event_visibility_sql($config, 'e')
        . ' AND i.record_status=\'active\'';
    $params = array();
    if ($country !== '') {
        $sql .= ' AND e.country_code=?';
        $params[] = $country;
    }
    $sql .= ' ORDER BY CASE e.importance WHEN \'critical\' THEN 0'
        . ' WHEN \'market_sensitive\' THEN 1 WHEN \'high\' THEN 2'
        . ' WHEN \'medium\' THEN 3 ELSE 4 END,e.updated_at DESC,e.event_id'
        . ' LIMIT ' . $limit;
    $statement = $pdo->prepare($sql);
    $statement->execute($params);
    $rows = v2_normalize_event_rows($statement->fetchAll());
    v2_respond(200, array(
        'ok' => true,
        'data' => array('items' => $rows),
        'meta' => array('returned' => count($rows), 'limit' => $limit),
    ));
}

function v2_normalize_brief_items(array $items): array {
    if (!v2_write_is_list($items) || count($items) > 105) {
        v2_write_invalid('payload.items: zero to 105 items required');
    }
    $normalized = array();
    $positions = array();
    $events = array();
    $laneCounts = array('top' => 0, 'watch' => 0, 'deadline' => 0);
    foreach ($items as $index => $item) {
        $location = 'payload.items[' . $index . ']';
        if (!is_array($item) || v2_write_is_list($item)) {
            v2_write_invalid($location . ': object required');
        }
        v2_write_assert_keys(
            $item,
            array('event_id', 'lane', 'position_no', 'selection_reason'),
            $location
        );
        $eventId = v2_write_code(
            $item,
            'event_id',
            $location,
            '/^[A-Za-z0-9_.:\-]{1,96}$/'
        );
        $lane = v2_write_code(
            $item,
            'lane',
            $location,
            '/^(top|watch|deadline)$/'
        );
        if (!isset($item['position_no']) || !is_int($item['position_no'])
            || $item['position_no'] < 1 || $item['position_no'] > 100) {
            v2_write_invalid($location . '.position_no: invalid');
        }
        $reason = v2_write_text($item, 'selection_reason', $location, 500);
        $positionKey = $lane . ':' . $item['position_no'];
        $eventKey = $eventId;
        if (isset($positions[$positionKey]) || isset($events[$eventKey])) {
            v2_write_invalid($location . ': duplicate lane position or event');
        }
        $positions[$positionKey] = true;
        $events[$eventKey] = true;
        $laneCounts[$lane]++;
        $normalized[] = array(
            'event_id' => $eventId,
            'lane' => $lane,
            'position_no' => $item['position_no'],
            'selection_reason' => $reason,
        );
    }
    if (
        $laneCounts['top'] > 5
        || $laneCounts['watch'] > 50
        || $laneCounts['deadline'] > 50
    ) {
        v2_write_invalid('payload.items: lane limit exceeded');
    }
    usort($normalized, function (array $left, array $right): int {
        $laneOrder = array('top' => 0, 'watch' => 1, 'deadline' => 2);
        $laneCompare = $laneOrder[$left['lane']] <=> $laneOrder[$right['lane']];
        return $laneCompare !== 0
            ? $laneCompare : ($left['position_no'] <=> $right['position_no']);
    });
    return $normalized;
}

function v2_admin_publish_brief(
    PDO $pdo,
    array $config,
    string $role
): void {
    $payload = v2_json_body($config);
    try {
        v2_write_assert_keys($payload, array(
            'edition', 'cutoff_at', 'build_sha', 'empty_reason', 'items',
        ), 'payload');
        $editionRaw = v2_write_text($payload, 'edition', 'payload', 16);
        $edition = strtolower((string)$editionRaw) === 'global'
            ? 'global' : v2_valid_country((string)$editionRaw);
        if ($edition === null) {
            v2_write_invalid('payload.edition: invalid');
        }
        $cutoffAt = v2_write_timestamp($payload, 'cutoff_at', 'payload');
        $buildSha = v2_write_code(
            $payload,
            'build_sha',
            'payload',
            '/^[a-f0-9]{7,64}$/'
        );
        $emptyReason = v2_write_code(
            $payload,
            'empty_reason',
            'payload',
            '/^(no_confirmed_material_events|coverage_unavailable)$/',
            false
        );
        if (!isset($payload['items']) || !is_array($payload['items'])) {
            v2_write_invalid('payload.items: array required');
        }
        $items = v2_normalize_brief_items($payload['items']);
    } catch (InvalidArgumentException $error) {
        v2_respond(400, array(
            'ok' => false,
            'error' => 'brief_validation_failed',
            'detail' => $error->getMessage(),
        ));
    }
    $topCount = 0;
    foreach ($items as $item) {
        if ($item['lane'] === 'top') {
            $topCount++;
        }
    }
    if (($topCount === 0) !== ($emptyReason !== null)) {
        v2_respond(400, array(
            'ok' => false,
            'error' => 'brief_empty_reason_mismatch',
        ));
    }
    $now = gmdate('Y-m-d H:i:s');
    if ($cutoffAt > gmdate('Y-m-d H:i:s', time() + 300)) {
        v2_respond(400, array('ok' => false, 'error' => 'brief_cutoff_in_future'));
    }
    $briefId = v1_stable_id('brief', (string)$edition . '|' . $cutoffAt);
    $semanticPayload = array(
        'edition' => $edition,
        'cutoff_at' => $cutoffAt,
        'build_sha' => $buildSha,
        'empty_reason' => $emptyReason,
        'items' => $items,
    );
    $semanticHash = v2_write_canonical_payload_hash($semanticPayload);
    $pdo->beginTransaction();
    try {
        $existing = $pdo->prepare(
            'SELECT brief_id,build_sha,payload_json FROM '
            . table_name($config, 'brief_editions')
            . ' WHERE brief_id=? OR (edition=? AND cutoff_at=?)'
            . ' LIMIT 1 FOR UPDATE'
        );
        $existing->execute(array($briefId, $edition, $cutoffAt));
        $stored = $existing->fetch();
        if ($stored) {
            $storedPayload = json_decode((string)$stored['payload_json'], true);
            $storedHash = is_array($storedPayload)
                && isset($storedPayload['semantic_sha256'])
                ? (string)$storedPayload['semantic_sha256'] : '';
            if (
                (string)$stored['brief_id'] !== $briefId
                || !hash_equals($semanticHash, $storedHash)
            ) {
                $pdo->rollBack();
                v2_respond(409, array(
                    'ok' => false,
                    'error' => 'brief_edition_conflict',
                ));
            }
            $pdo->commit();
            v2_respond(200, array(
                'ok' => true,
                'data' => array(
                    'brief_id' => $briefId,
                    'edition' => $edition,
                    'published' => true,
                    'idempotent' => true,
                ),
            ));
        }
        $snapshots = array();
        foreach ($items as $item) {
            $sql = v2_event_select($config)
                . ' WHERE e.event_id=? AND '
                . v2_event_visibility_sql($config, 'e')
                . ' AND i.record_status=\'active\'';
            if ($edition !== 'global') {
                $sql .= ' AND e.country_code=?';
            }
            $eventStatement = $pdo->prepare($sql);
            $params = array($item['event_id']);
            if ($edition !== 'global') {
                $params[] = $edition;
            }
            $eventStatement->execute($params);
            $row = $eventStatement->fetch();
            if (!$row) {
                throw new RuntimeException('brief_event_not_publishable');
            }
            $normalizedRow = v2_normalize_event_rows(array($row))[0];
            if (
                $item['lane'] === 'top'
                && (int)$normalizedRow['official_evidence_count'] < 1
            ) {
                throw new RuntimeException('brief_top_official_evidence_required');
            }
            // A frozen URL can outlive its SourceRight. Persist the event
            // facts, but reconstruct the representative URL from currently
            // eligible non-Telegram evidence on every public read.
            unset($normalizedRow['source_url']);
            $snapshots[] = array(
                'item' => $item,
                'event' => $normalizedRow,
            );
        }
        if ($topCount === 0) {
            $statusCountry = $edition === 'global' ? '' : (string)$edition;
            $sourceRows = v2_source_status_data($pdo, $config, $statusCountry);
            $readyCountries = array();
            foreach ($sourceRows as $sourceRow) {
                if (
                    isset($sourceRow['public_ready'])
                    && $sourceRow['public_ready'] === true
                ) {
                    $readyCountries[(string)$sourceRow['country']] = true;
                }
            }
            $requiredCountries = $edition === 'global'
                ? array('KR', 'US', 'CA', 'AU')
                : array((string)$edition);
            $ready = true;
            foreach ($requiredCountries as $requiredCountry) {
                if (!isset($readyCountries[$requiredCountry])) {
                    $ready = false;
                    break;
                }
            }
            if ($emptyReason === 'no_confirmed_material_events' && !$ready) {
                throw new RuntimeException('brief_sources_unavailable');
            }
            if ($emptyReason === 'coverage_unavailable' && $ready) {
                throw new RuntimeException('brief_coverage_is_available');
            }
        }
        $editionPayload = array(
            'semantic_sha256' => $semanticHash,
            'empty_reason' => $emptyReason,
            'item_count' => count($items),
            'top_count' => $topCount,
            'contract_version' => 1,
        );
        $editionInsert = $pdo->prepare(
            'INSERT INTO ' . table_name($config, 'brief_editions')
            . ' (brief_id,edition,cutoff_at,published_at,publication_status,'
            . 'approved_by,approved_at,build_sha,payload_json,created_at,updated_at)'
            . ' VALUES (?,?,?,? ,\'published\',?,?,?,?,?,?)'
        );
        $editionInsert->execute(array(
            $briefId,
            $edition,
            $cutoffAt,
            $now,
            $role,
            $now,
            $buildSha,
            json_value($editionPayload),
            $now,
            $now,
        ));
        $itemInsert = $pdo->prepare(
            'INSERT INTO ' . table_name($config, 'brief_items')
            . ' (brief_id,event_id,lane,position_no,event_updated_at,'
            . 'event_snapshot_json,selection_reason,review_status,approved_by,'
            . 'approved_at,created_at,updated_at)'
            . ' VALUES (?,?,?,?,?,?,?,\'approved\',?,?,?,?)'
        );
        foreach ($snapshots as $snapshot) {
            $item = $snapshot['item'];
            $event = $snapshot['event'];
            $itemInsert->execute(array(
                $briefId,
                $item['event_id'],
                $item['lane'],
                $item['position_no'],
                v1_mysql_datetime_utc($event['updated_at']),
                json_value($event),
                $item['selection_reason'],
                $role,
                $now,
                $now,
                $now,
            ));
        }
        $pdo->commit();
    } catch (Throwable $error) {
        if ($pdo->inTransaction()) {
            $pdo->rollBack();
        }
        $known = array(
            'brief_event_not_publishable',
            'brief_top_official_evidence_required',
            'brief_sources_unavailable',
            'brief_coverage_is_available',
        );
        if (in_array($error->getMessage(), $known, true)
            || (string)$error->getCode() === '23000') {
            v2_respond(409, array(
                'ok' => false,
                'error' => in_array($error->getMessage(), $known, true)
                    ? $error->getMessage() : 'brief_constraint_conflict',
            ));
        }
        throw $error;
    }
    v2_respond(200, array(
        'ok' => true,
        'data' => array(
            'brief_id' => $briefId,
            'edition' => $edition,
            'published' => true,
            'idempotent' => false,
            'top_count' => $topCount,
            'item_count' => count($items),
            'empty_reason' => $emptyReason,
        ),
    ));
}
