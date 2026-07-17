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

fwrite(STDOUT, "PHP governance API contracts passed.\n");
