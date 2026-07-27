<?php
declare(strict_types=1);

require_once __DIR__ . '/../deploy/activist/governance_v1.php';

function expect_true(bool $condition, string $message): void {
    if (!$condition) {
        fwrite(STDERR, "FAILED: {$message}\n");
        exit(1);
    }
}

expect_true(v1_valid_entity_id('005930'), 'DART company identifiers must be accepted');
expect_true(v1_valid_entity_id('event:005930:2026-07-16'), 'stable event identifiers must be accepted');
expect_true(!v1_valid_entity_id('../etc/passwd'), 'path traversal identifiers must be rejected');
expect_true(!v1_valid_entity_id(str_repeat('a', 97)), 'oversized identifiers must be rejected');
expect_true(v1_mysql_datetime_utc('2026-07-16T15:30:00+09:00') === '2026-07-16 06:30:00', 'offset timestamps must be normalized to UTC');
expect_true(v1_mysql_datetime_utc('2026-07-16 06:30:00') === '2026-07-16 06:30:00', 'timezone-less database timestamps must be interpreted as UTC');
expect_true(v1_mysql_datetime_utc('') === null, 'empty database timestamps must be rejected');
expect_true(v1_editorial_datetime_utc('2026-07-16T15:30:00+09:00') === '2026-07-16 06:30:00', 'editorial timestamps must normalize explicit offsets');
expect_true(v1_editorial_datetime_utc('2026-07-16 06:30:00') === null, 'editorial timestamps without an offset must fail closed');
expect_true(v1_editorial_language('ko') === 'ko', 'lowercase source language must be accepted');
expect_true(v1_editorial_language('ko-KR') === 'ko-KR', 'language-region source tags must be accepted');
expect_true(v1_editorial_language('KO') === null, 'non-canonical source language must be rejected');
expect_true(v1_editorial_is_list(array('a', 'b')), 'sequential arrays must be recognized');
expect_true(!v1_editorial_is_list(array('key' => 'value')), 'objects must not be accepted as record lists');

$_GET = array('limit' => '9999', 'page' => '-2');
$page = v1_list_params();
expect_true($page['limit'] === V1_MAX_PAGE_SIZE, 'page size must be capped at 100');
expect_true($page['page'] === 1, 'page number must be at least one');
expect_true($page['offset'] === 0, 'first page offset must be zero');

$meta = v1_page_meta(array('page' => 2, 'limit' => 25, 'offset' => 25), 25, true);
expect_true($meta['next_page'] === 3, 'pagination must return the next page');
expect_true($meta['has_more'] === true, 'pagination must preserve has_more');

$_SERVER['HTTP_AUTHORIZATION'] = 'Bearer ' . str_repeat('x', 24);
expect_true(v1_bearer_token() === str_repeat('x', 24), 'valid Bearer tokens must be parsed');
$_SERVER['HTTP_AUTHORIZATION'] = 'Basic invalid';
expect_true(v1_bearer_token() === '', 'non-Bearer authorization must be rejected');

$previewToken = 'php-contract-preview-token-00000000000000';
$previewConfig = array('governance_preview_token_hash' => hash('sha256', $previewToken));
expect_true(v1_preview_auth_configured($previewConfig), 'a SHA-256 preview token hash must enable preview auth');
expect_true(
    v1_preview_token_hashes($previewConfig) === array(hash('sha256', $previewToken)),
    'preview auth must retain only normalized token hashes'
);
expect_true(!v1_preview_auth_configured(array()), 'preview auth must fail closed without a token hash');

expect_true(v1_xml('<company>&') === '&lt;company&gt;&amp;', 'Atom values must be XML escaped');

expect_true(delivery_payload_source_right_ids('{}') === false, 'missing delivery rights lineage must fail closed');
expect_true(
    delivery_payload_source_right_ids('{"rights_lineage_complete":false,"source_right_ids":[]}') === false,
    'incomplete delivery rights lineage must fail closed'
);
expect_true(
    delivery_payload_source_right_ids('{"rights_lineage_complete":true,"source_right_ids":"official:dart"}') === false,
    'scalar delivery rights lineage must fail closed'
);
$emptyRights = delivery_payload_source_right_ids('{"rights_lineage_complete":true,"source_right_ids":[]}');
expect_true(is_array($emptyRights) && count($emptyRights) === 0, 'explicit complete public-source lineage may be empty');
$deduplicatedRights = delivery_payload_source_right_ids(
    '{"rights_lineage_complete":true,"source_right_ids":["official:dart","official:dart","telegram:licensed"]}'
);
expect_true(
    $deduplicatedRights === array('official:dart', 'telegram:licensed'),
    'delivery rights lineage must be validated and de-duplicated'
);

$dateOnlyIdentity = v1_build_event_identity(
    '00126380', 'shareholder_proposal', 'submit', 'board seat', 'actor:test',
    '2026-07-20', '2026-08-31'
);
$midnightIdentity = v1_build_event_identity(
    '00126380', 'shareholder_proposal', 'submit', 'board seat', 'actor:test',
    '2026-07-20T00:00:00Z', '2026-08-31T00:00:00Z'
);
expect_true(
    is_array($dateOnlyIdentity) && is_array($midnightIdentity)
        && $dateOnlyIdentity['identity_effective_at'] === $midnightIdentity['identity_effective_at']
        && $dateOnlyIdentity['identity_deadline_at'] === $midnightIdentity['identity_deadline_at']
        && $dateOnlyIdentity['comparison_key'] !== $midnightIdentity['comparison_key'],
    'date-only and explicit midnight identities must keep distinct canonical keys in the same DATETIME storage'
);
$recoveredDateOnly = v1_resolve_stored_event_identity(
    '00126380', 'shareholder_proposal', 'submit', 'board seat', 'actor:test',
    '2026-07-20 00:00:00', '2026-08-31 00:00:00', $dateOnlyIdentity['comparison_key']
);
$recoveredMidnight = v1_resolve_stored_event_identity(
    '00126380', 'shareholder_proposal', 'submit', 'board seat', 'actor:test',
    '2026-07-20 00:00:00', '2026-08-31 00:00:00', $midnightIdentity['comparison_key']
);
expect_true(
    is_array($recoveredDateOnly)
        && $recoveredDateOnly['comparison_key'] === $dateOnlyIdentity['comparison_key'],
    'stored date-only identity must be recovered by its canonical key'
);
expect_true(
    is_array($recoveredMidnight)
        && $recoveredMidnight['comparison_key'] === $midnightIdentity['comparison_key'],
    'stored explicit midnight identity must be recovered by its canonical key'
);
$tamperedKey = substr($dateOnlyIdentity['comparison_key'],0,-1)
    . (substr($dateOnlyIdentity['comparison_key'],-1) === '0' ? '1' : '0');
expect_true(
    v1_resolve_stored_event_identity(
        '00126380', 'shareholder_proposal', 'submit', 'board seat', 'actor:test',
        '2026-07-20 00:00:00', '2026-08-31 00:00:00', $tamperedKey
    ) === null,
    'a stored identity with a tampered canonical key must fail closed'
);
expect_true(
    v1_resolve_stored_event_identity(
        '00126380', 'shareholder_proposal', 'Submit', 'board seat', 'actor:test',
        '2026-07-20 00:00:00', '2026-08-31 00:00:00', $dateOnlyIdentity['comparison_key']
    ) === null,
    'a noncanonical stored identity field must not be silently normalized'
);
expect_true(
    v1_resolve_stored_event_identity(
        '00126380', 'shareholder_proposal', 'submit', 'board seat', 'actor:test',
        '2026-07-20', '2026-08-31 00:00:00', $dateOnlyIdentity['comparison_key']
    ) === null,
    'stored identity dates must use the exact MySQL DATETIME representation'
);

foreach (array(
    'followup_event_identity_conflict',
    'invalid_complete_event_identity',
    'incomplete_event_identity_has_comparison_key',
    'event_identity_scope_conflict',
    'event_identity_field_conflict',
) as $clientConflictCode) {
    expect_true(
        v1_governance_snapshot_identity_conflict_code(
            new RuntimeException($clientConflictCode . ':sensitive-event-id')
        ) === $clientConflictCode,
        'caller-controlled identity conflicts must expose only a stable code'
    );
}
expect_true(
    v1_governance_snapshot_identity_conflict_code(
        new RuntimeException('stored_event_identity_integrity_error:sensitive-event-id')
    ) === null,
    'stored identity corruption must remain an internal server failure'
);

$validationSecret = 'record-id-should-never-leak';
$validationFailure = v1_governance_snapshot_failure_response(
    new RuntimeException('event_observation_document_missing:' . $validationSecret)
);
expect_true(
    $validationFailure === array(
        'status'=>409,
        'payload'=>array(
            'ok'=>false,
            'error'=>'event_observation_document_missing',
            'validation_reason'=>'event_observation_document_missing',
        ),
    ),
    'known snapshot validation failures must expose only their allowlisted prefix'
);
expect_true(
    strpos(json_encode($validationFailure),$validationSecret) === false,
    'snapshot validation responses must not expose record identifiers'
);

$pdoSecret = 'sql-message-api-key-should-never-leak';
$pdoFailure = new PDOException($pdoSecret);
$pdoFailure->errorInfo = array('23000','1062','duplicate value ' . $pdoSecret);
$persistenceFailure = v1_governance_snapshot_failure_response($pdoFailure);
expect_true(
    $persistenceFailure === array(
        'status'=>503,
        'payload'=>array(
            'ok'=>false,
            'error'=>'governance_snapshot_persistence_failed',
            'sqlstate_class'=>'23000',
            'driver_code'=>1062,
        ),
    ),
    'snapshot persistence failures must expose only validated numeric PDO diagnostics'
);
expect_true(
    strpos(json_encode($persistenceFailure),$pdoSecret) === false,
    'snapshot persistence responses must not expose SQL or exception messages'
);

$invalidPdoSecret = 'oversized-driver-and-unicode-sqlstate-secret';
$invalidPdoFailure = new PDOException($invalidPdoSecret);
$invalidPdoFailure->errorInfo = array(
    '보안',
    '999999999999999999999999',
    $invalidPdoSecret,
);
$normalizedPdoFailure = v1_governance_snapshot_failure_response($invalidPdoFailure);
expect_true(
    $normalizedPdoFailure['payload']['sqlstate_class'] === 'HY000'
        && $normalizedPdoFailure['payload']['driver_code'] === 0,
    'invalid PDO diagnostics must normalize to bounded generic values'
);
expect_true(
    strpos(json_encode($normalizedPdoFailure),$invalidPdoSecret) === false,
    'invalid PDO diagnostics must not expose rejected values or messages'
);

$unknownSecret = 'https://secret.invalid/?token=never-log-this';
$unknownFailure = v1_governance_snapshot_failure_response(
    new RuntimeException('알수없는오류:' . $unknownSecret)
);
expect_true(
    $unknownFailure === array(
        'status'=>500,
        'payload'=>array('ok'=>false,'error'=>'internal_error'),
    ),
    'unknown or non-ASCII snapshot failures must collapse to internal_error'
);
expect_true(
    strpos(json_encode($unknownFailure),$unknownSecret) === false,
    'unknown snapshot failures must not expose secret-bearing messages'
);

fwrite(STDOUT, "PHP governance API contracts passed.\n");
