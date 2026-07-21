# Governance Intelligence API v1 운영 계약

`deploy/activist/api.php`는 기존 `?action=search|articles|reports|telegram_dashboard` 계약을 유지하면서 `deploy/activist/governance_v1.php`를 불러와 `/api/v1`을 제공한다. 기존 클라이언트는 전환 공지 이후 최소 90일 동안 그대로 사용할 수 있다.

## 배포 순서

1. Telegram writer를 모두 중지하고 `ENABLE_LEGACY_PIPELINE=false`, 세 거버넌스 전환 플래그 `false`, 관련 Actions 실행 0건을 확인한다. 이 정지 구간은 marker 감사·승인이 끝날 때까지 유지한다.
2. 기존 DB 백업과 현재 `_private/config.php`를 외부 접근이 차단된 `_private/deployment-backups/`에 보존한다.
3. 장기 transaction·metadata lock 대기·가용 디스크를 사전 점검한 뒤, 기본 접두사 `activist_`를 사용하면 `deploy/activist/migrations/001_governance_v1.sql`부터 `005_telegram_channel_identity_index.sql`까지 번호 순서대로 적용한다. 다른 `table_prefix`를 사용하면 SQL의 접두사를 운영 값으로 치환한다. 대형 Telegram 메시지 인덱스와 identity migration marker는 API 요청 중 만들지 않으므로 005를 명시적으로 적용해야 한다. 005는 metadata lock 대기를 30초로 제한하고 MySQL의 `ALGORITHM=INPLACE, LOCK=NONE`을 요구하며, 동명 컬럼·인덱스의 정확한 형태가 다르면 성공 처리하지 않고 실패한다. 지원되지 않거나 시간 제한을 넘으면 PHP를 배포하지 않고 중단한다. `POST api.php?action=schema`도 그 밖의 누락 테이블·컬럼을 보완하지만, 명시적 migration 이력을 대신하지 않는다.
4. marker 무효화 로직이 포함된 `api.php`와 `governance_v1.php`를 원자 배포하고 읽기·무변경 smoke를 통과시킨다. 구 PHP writer가 남아 있는 동안 marker를 1로 올리지 않는다.
5. 기존 데이터의 marker는 0으로 생성된다. 새 PHP 배포 뒤 중복 channel ID, 비정규 message key·handle·channel ID, orphan match가 각각 0건인지 감사한다. 하나라도 0이 아니면 writer 정지를 유지하고 marker를 승인하지 않는다. 모두 0이면 동일 정지 구간에서 현재 권위 channel ID와 불일치 row가 없는 채널만 조건부 단일 SQL로 marker 1을 설정하고, 승인 수가 권위 채널 수와 일치하는지 재확인한다.
6. `POST api.php?action=schema`를 기존 HMAC 방식으로 호출한다. 런타임 스키마 생성은 누락된 테이블·lease 컬럼을 추가하는 안전망이다.
7. 웹 서버가 `/api/v1/events`를 `api.php/api/v1/events`로 전달하도록 rewrite한다. rewrite가 어려우면 `api.php/api/v1/events` 또는 `api.php?_route=/api/v1/events`도 동일하게 동작한다.
8. `/api/v1/health`, 공개 목록, 역할별 관리자 API를 확인한다. outbox claim/ack는 delivery가 별도 승인된 환경에서만 검증하며 현재 Telegram outbound는 계속 비활성화한다.

PHP API는 응답 한 건을 256,000바이트 미만으로 제한한다. 목록의 기본 크기는 25건, 최댓값은 100건이다. CSV와 JSON 내보내기도 같은 페이지 제한을 사용하며 다음 페이지는 `page`로 요청한다.

`GET /api/v1/search?q=...`는 회사, 행동주주·기관 등 actor, 사건, 캠페인, 공개 문서를 함께 찾는다. 결과의 `kind`는 `company|actor|event|campaign|document` 중 하나이며 actor 결과는 `entity_id=actor_id`, `title=display_name`, `subtitle=actor_type` 계약을 사용한다.

## 권한 설정

공개 읽기 API는 인증이 필요 없다. `/ops/*`와 `/admin/*`은 쿼리 문자열이 아닌 `Authorization: Bearer …`만 허용한다. 운영 설정에는 평문 토큰이 아니라 SHA-256 해시를 등록한다.

```php
return array(
    // 기존 DB/API 설정 생략
    'role_token_hashes' => array(
        'ops' => array('sha256-hex-without-prefix'),
        'editor' => array('sha256-hex-without-prefix'),
        'rights' => array('sha256-hex-without-prefix'),
        'admin' => array('sha256-hex-without-prefix'),
    ),
    'feedback_ip_salt' => 'separate-random-secret',
    'public_base_url' => 'https://news.bside.ai',
    'public_api_cors_origins' => array('*'), // 공개 API만 적용, /ops·/admin 제외
);
```

`admin`은 모든 역할을 포함한다. `ops`는 watchdog과 runner 상태 복구, `editor`는 사건 검수·정정, `rights`는 이용권한 등록만 허용한다. 토큰은 32바이트 이상의 무작위 값으로 만들고 환경별로 분리한다.

운영 `SourceRight`의 기준은 MySQL과 `POST /api/v1/admin/source-rights`다. 저장소 `config.yaml`의 `telegram:activistkorea` 레코드는 스키마 예시이자 차단 확인용 `pending` 자리표시자이며, 빈 `evidence_ref` 상태에서는 어떤 권한도 부여하지 않는다. 운영자는 설정 파일을 `active`로 수정해 승인 절차를 우회하지 않는다.

## 공개 상태와 이용권한

- 공개 쿼리는 `publication_status=published`인 레코드만 반환한다.
- `licensed_telegram` 문서는 활성 `SourceRight`가 연결되어야 한다.
- 연결된 권한은 현재 시각에 유효하고, 철회되지 않았으며, `redistribution_allowed=1`이어야 한다.
- 권한이 만료·철회되면 해당 문서는 공개 문서·검색·사건 근거에서 즉시 제외된다.
- Telegram-only 사건은 `verification_status=signal`, `publication_status=draft`로 남는다.
- `high`와 `market_sensitive` 사건은 HMAC 수집으로 자동 공개할 수 없다. 편집자가 `/admin/events/{event_id}/review`에서 승인해야 한다.
- `title`, `body_text`, `claim_text`는 `original_language`와 함께 원문 그대로 저장하며 번역 필드를 생성하지 않는다.
- `/feedback` 접수는 항상 `pending`, `is_public=0`이다. 편집 검토 전 공개 데이터에 합쳐지지 않는다.

## 공식 수집 HMAC 계약

기존 `X-Activist-Timestamp`, `X-Activist-Nonce`, `X-Activist-Signature` 검증을 그대로 사용한다. 서명되지 않은 요청은 스키마나 데이터에 접근하지 못한다.

### 공식 사건 upsert

`POST api.php?action=upsert_governance_snapshot`

```json
{
  "companies": [{"corp_code":"00126380","stock_code":"005930","corp_name":"삼성전자","market":"KOSPI"}],
  "source_rights": [],
  "documents": [{
    "document_id":"dart:20260716000123",
    "corp_code":"00126380",
    "source_class":"official_disclosure",
    "external_id":"20260716000123",
    "document_type":"value_up_plan",
    "original_language":"ko",
    "title":"기업가치 제고 계획",
    "original_url":"https://dart.fss.or.kr/…",
    "published_at":"2026-07-16T00:00:00Z"
  }],
  "events": [{
    "event_id":"event:dart:20260716000123",
    "company_id":"00126380",
    "event_type":"value_up_plan",
    "title":"기업가치 제고 계획",
    "original_language":"ko",
    "occurred_at":"2026-07-16T00:00:00Z",
    "importance":"normal",
    "verification_status":"official",
    "document_ids":["dart:20260716000123"]
  }],
  "run": {
    "run_id":"dart:20260716T000000Z",
    "pipeline":"ingest-official",
    "source_key":"dart",
    "status":"succeeded",
    "started_at":"2026-07-16T00:00:00Z",
    "finished_at":"2026-07-16T00:02:00Z",
    "fetched_count":100,
    "accepted_count":4,
    "error_count":0,
    "lag_seconds_p95":900
  }
}
```

회사 ID는 8자리 DART `corp_code`다. 문서의 `(source_class, external_id, version_no)`와 각 엔터티 ID는 멱등 키다. 정정 공시는 새 `version_no` 및 `correction_of_document_id`로 연결한다.

## DeliveryOutbox 계약

Producer는 `enqueue_delivery_outbox`에 다음을 보낸다.

```json
{"deliveries":[{
  "channel":"telegram",
  "destination":"@channel",
  "idempotency_key":"event:dart:20260716000123:telegram:v1",
  "event_id":"event:dart:20260716000123",
  "payload":{
    "text":"…",
    "disable_web_page_preview":false,
    "rights_lineage_complete":true,
    "source_right_ids":["official:dart"]
  }
}]}
```

`rights_lineage_complete: true`와 `source_right_ids` 배열은 필수다. 별도 계약상 이용권한이 필요 없는 공개 소스만 사용한 경우 배열은 비어 있을 수 있지만, 완전성 표시는 생략할 수 없다. 누락·형식 오류·비활성 권한은 enqueue에서 거절하고, enqueue 이후 만료·철회된 권한은 claim 시 `dead_letter`로 전환한다.

Consumer 순서는 다음과 같다.

1. `claim_delivery_outbox`: `{channel, worker_id, limit, lease_seconds}`. 응답 `items` 각각에 `delivery_id`, `outbox_id`, `lease_token`, `destination`, `payload`가 포함된다.
2. 외부 API 성공 뒤에만 `ack_delivery_outbox`: `{delivery_id, lease_token, external_message_id}`. 외부 메시지 ID가 없으면 성공으로 확정할 수 없다.
3. 실패하면 `fail_delivery_outbox`: `{delivery_id, lease_token, retryable, retry_after_seconds, max_attempts, error}`. `retryable=false` 또는 최대 횟수 도달 시 `dead_letter`가 된다.
4. lease가 만료된 `processing` 레코드는 외부 발송 결과를 알 수 없으므로 자동 재claim하지 않는다. 다음 claim 시 `delivery_lease_expired_outcome_unknown` dead-letter로 격리하고 외부 메시지 이력을 수동 대조한다.

`/api/v1/ops/health`는 `last_success_at`, `pending_outbox`, `oldest_pending_at`, `dead_letter_count`를 반환한다. watchdog은 이 응답으로 90분 공백과 적체를 판정한다.

## 발견 URL 후처리 계약

Google News URL은 공개 기사 채택과 분리한다.

- `enqueue_link_discoveries`: `{discoveries:[{url, source, title, discovered_at}]}`
- `claim_link_discoveries`: `{worker_id, limit, lease_seconds}`
- `resolve_link_discovery`: 성공은 `{discovery_id, lease_token, outcome:"resolved", resolved_url}`, 재시도는 `outcome:"retry"`, 영구 종료는 `outcome:"expired"`

상태는 `discovered → resolving → resolved|expired`다. retry는 대기 시각을 가진 `discovered`로 돌아가며, URL 미해결은 공식 수집·사건 발행을 실패시키지 않는다.

## Fresh runner 복구

생성 `state.json`을 저장소에 커밋하지 않는다. 새 runner는 HMAC `export_runtime_state` 또는 ops Bearer `/api/v1/ops/runtime-state`로 다음 리소스를 cursor 순회한다.

`runs`, `articles`, `stories`, `reports`, `telegram_channels`, `telegram_messages`, `telegram_article_matches`, `telegram_issue_signals`, `delivery_outbox`, `companies`, `source_rights`, `collection_runs`, `governance_events`, `documents`

요청은 `resource`, `after`/`cursor`, 선택적 증분 기준 `since`, `limit<=100`을 사용한다. 응답의 `next_cursor`가 없을 때까지 반복하며, 각 페이지는 230KB 안쪽에서 자동으로 끊긴다.

## 고정 enum

캠페인 단계 키는 아래 순서를 사용한다. 화면에서만 한·영 라벨을 병기한다.

1. `initial_signal` — 초기 신호
2. `private_engagement` — 비공개 관여
3. `public_letter` — 공개서한·질의
4. `public_campaign` — 공개 캠페인
5. `shareholder_proposal` — 주주제안
6. `proxy_vote` — 위임·표결
7. `resolution` — 합의·철회·가결·부결
8. `implementation_tracking` — 이행 추적
9. `closed` — 종료

전체 공개 필드와 enum은 `deploy/activist/openapi.yaml`이 기준이다.
