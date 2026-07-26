# Governance Intelligence API v1 운영 계약

`deploy/activist/api.php`는 기존 `?action=search|articles|reports|telegram_dashboard` 계약을 유지하면서 `deploy/activist/governance_v1.php`를 불러와 `/api/v1`을 제공한다. 기존 클라이언트는 전환 공지 이후 최소 90일 동안 그대로 사용할 수 있다.

## 배포 순서

1. 초기 migration-only 유지보수 구간에서만 레거시 DB writer를 일시 중지하고 관련 Actions 실행이 0건인지 확인한다. 이때 공개 Pages 소유권은 `PAGES_OWNER=legacy`, 신규 파이프라인은 `GOVERNANCE_PIPELINE_MODE=off`, 두 outbound 변수는 `false`로 고정한다. 호환 boolean은 이 구간에서만 `ENABLE_LEGACY_PIPELINE=false`로 내려 writer를 멈추며, migration·marker 감사가 끝나면 `true`로 복구해 기존 Pages와 레거시 수집을 계속 운영한다. 신규 governance 데이터는 release state `closed`에서만 적재한다.
2. 기존 DB 백업과 현재 `_private/config.php`를 외부 접근이 차단된 `_private/deployment-backups/`에 보존한다.
3. 장기 transaction·metadata lock 대기·가용 디스크를 사전 점검한 뒤, 기본 접두사 `activist_`를 사용하면 `deploy/activist/migrations/001_governance_v1.sql`부터 `010_official_slot_claim_ledger.sql`까지 번호 순서대로 적용한다. 다른 `table_prefix`를 사용하면 SQL의 접두사를 운영 값으로 치환한다. 대형 Telegram 메시지 인덱스와 identity migration marker는 API 요청 중 만들지 않으므로 005를 명시적으로 적용해야 한다. 005는 metadata lock 대기를 30초로 제한하고 MySQL의 `ALGORITHM=INPLACE, LOCK=NONE`을 요구하며, 동명 컬럼·인덱스의 정확한 형태가 다르면 성공 처리하지 않고 실패한다. 006은 공개 상태를 `closed`로 생성하고 007은 canonical identity·관측·운영 증빙을, 008은 공식사이트 원자 snapshot·문서 버전 이력을, 009는 DART 전역 요청 쿼터를, 010은 서버 기준 official slot claim·epoch 감사 원장을 추가한다. 008이 기존 1~7 migration에 체크섬을 최초 등록한 뒤에는 이름·체크섬을 덮어쓰지 않으며, 009·010도 선행 manifest와 테이블·인덱스 형태가 정확할 때만 append된다. 모두 PHP보다 먼저 적용하고 각 파일을 두 번 적용해 멱등성을 확인한다. 지원되지 않거나 시간 제한을 넘으면 PHP를 배포하지 않고 중단한다. `POST api.php?action=schema`도 레거시 writer가 사용하는 누락 테이블·컬럼을 보완하지만, 명시적 migration 이력을 대신하지 않는다.
4. marker 무효화 로직이 포함된 `api.php`와 `governance_v1.php`를 원자 배포하고 읽기·무변경 smoke를 통과시킨다. 구 PHP writer가 남아 있는 동안 marker를 1로 올리지 않는다.
5. 기존 데이터의 marker는 0으로 생성된다. 새 PHP 배포 뒤 중복 channel ID, 비정규 message key·handle·channel ID, orphan match가 각각 0건인지 감사한다. 하나라도 0이 아니면 writer 정지를 유지하고 marker를 승인하지 않는다. 모두 0이면 동일 정지 구간에서 현재 권위 channel ID와 불일치 row가 없는 채널만 조건부 단일 SQL로 marker 1을 설정하고, 승인 수가 권위 채널 수와 일치하는지 재확인한다.
6. `POST api.php?action=schema`를 기존 HMAC 방식으로 호출해 레거시 writer용 누락 스키마를 확인한다. `/api/v1`은 이 호출이나 요청 시점 DDL에 의존하지 않는다. `activist_schema_migrations`가 1~10의 정확한 버전·이름·체크섬 manifest와 일치하지 않으면 HTTP 503 `schema_version_mismatch`로 차단한다. 단순 최댓값 비교는 사용하지 않는다.
7. 웹 서버가 `/api/v1/events`를 `api.php/api/v1/events`로 전달하도록 rewrite한다. rewrite가 어려우면 `api.php/api/v1/events` 또는 `api.php?_route=/api/v1/events`도 동일하게 동작한다.
8. `/api/v1/health`, OpenAPI, 역할별 관리자 API를 확인한다. 공개 목록은 `closed`에서 503이어야 한다. 관리자 release-state API로 `preview` 전환 후 preview token으로 검증하고, 공개 승인 전에는 다시 `closed`로 되돌린다. `enqueue_delivery_outbox`와 `claim_delivery_outbox`가 DB 변경 전에 HTTP 410 `outbound_delivery_disabled`를 반환하는지 확인한다. 신규 outbound는 제품 정책상 영구 비활성이고 기존 row의 감사·보존만 허용한다.

PHP API는 응답 한 건을 250,000바이트 이하로 제한한다. 목록의 기본 크기는 25건, 최댓값은 100건이다. CSV와 JSON 내보내기도 같은 페이지 제한을 사용하며 다음 페이지는 `page`로 요청한다.

Atom `self` 링크는 실제 API origin과 검증·정규화된 사건 필터를 보존한다. 각 entry의 `alternate` 링크는 API JSON이 아니라 `https://news.bside.ai/#/events/{event_id}` 공개 화면을 가리킨다.

`GET /api/v1/search?q=...`는 회사, 행동주주·기관 등 actor, 사건, 캠페인, 공개 문서를 함께 찾는다. 결과의 `kind`는 `company|actor|event|campaign|document` 중 하나이며 actor 결과는 `entity_id=actor_id`, `title=display_name`, `subtitle=actor_type` 계약을 사용한다.

## 권한 설정

공개 읽기 API는 `live` 상태에서만 인증이 필요 없다. `preview`에서는 preview Bearer token이 필요하고 `closed`에서는 데이터 API가 503을 반환한다. `/ops/*`와 `/admin/*`은 쿼리 문자열이 아닌 `Authorization: Bearer …`만 허용한다. 운영 설정에는 평문 토큰이 아니라 SHA-256 해시를 등록한다.

```php
return array(
    // 기존 DB/API 설정 생략
    'role_token_hashes' => array(
        'ops' => array('sha256-hex-without-prefix'),
        'editor' => array('sha256-hex-without-prefix'),
        'rights' => array('sha256-hex-without-prefix'),
        'admin' => array('sha256-hex-without-prefix'),
        'release_authorizer' => array('sha256-hex-without-prefix'),
    ),
    'governance_preview_token_hash' => 'sha256-hex-without-prefix',
    'feedback_ip_salt' => 'separate-random-secret',
    'public_base_url' => 'https://news.bside.ai',
    'governance_api_base_url' => 'https://alignpe.gabia.io/activist/api.php/api/v1',
    'public_api_cors_origins' => array('*'), // 공개 API만 적용, /ops·/admin 제외
);
```

`admin`은 일반 운영 관리자 경로에 접근하지만 보호된 공개 승인 발급 권한을 포함하지 않는다. `ops`는 watchdog과 runner 상태 복구, `editor`는 사건 검수·정정, `rights`는 이용권한 등록만 허용한다. `release_authorizer`는 정확히 일치하는 별도 역할이며 보호된 `/api/v2/admin/release-authorizations`만 호출한다. 토큰은 32바이트 이상의 무작위 값으로 만들고 환경별로 분리한다.

preview 평문 token은 GitHub Pages 자산, URL query, PHP 설정에 저장하지 않는다. 브라우저는 URL fragment에서 token을 받아 세션 저장소에만 보관하고 API 요청의 `Authorization: Bearer …` 헤더로 전달한다. PHP 설정에는 `GOVERNANCE_PREVIEW_TOKEN`의 SHA-256 hex만 `governance_preview_token_hash`로 기록한다.

## 공개 release state

- `GET /api/v1/health`와 `/api/v1/openapi.yaml`만 상태와 무관하게 공개한다. API 안내 루트 `/api/v1/`도 데이터 경로이므로 `closed`에서는 503이다.
- `/ops/*`, `/admin/*`은 release state와 무관하게 각 역할 인증 후 접근할 수 있다.
- 그 밖의 회사·사건·문서·검색·내보내기·feed·feedback 경로는 `closed`에서 503이다.
- `preview`에서는 올바른 preview Bearer token이 필요하며 응답은 `Cache-Control: private, no-store`와 `Vary: Authorization`을 사용한다.
- `live`에서만 공개 캐시를 허용한다.

관리자는 `GET /api/v1/admin/release-state`로 현재 상태·버전·최근 감사 로그를 읽는다. `POST` 본문은 다음과 같다.

```json
{
  "release_state": "preview",
  "expected_version": 0,
  "reason": "DART canary preview 검증 시작"
}
```

허용 전이는 `closed → preview`, `preview → closed`, `live → closed`다. 직접 `preview → live`를 요청하면 409 `protected_atomic_cutover_required`를 반환하며, 보호된 `/api/v2/admin/release-authorizations`와 `/api/v2/admin/cutover`의 원자 전환만 두 API를 함께 공개할 수 있다. 같은 상태 재요청은 멱등 응답이며 버전과 감사 로그를 늘리지 않는다. 다른 관리자가 먼저 변경했다면 `409 stale_release_state`를 반환한다. 모든 실제 변경은 요청 ID, 역할, 사유와 함께 `governance_release_audit`에 원자적으로 기록한다. 공개 응답에는 preview token이나 그 해시가 포함되지 않는다.

운영 `SourceRight`의 기준은 MySQL과 `POST /api/v1/admin/source-rights`다. 보호 자동화는 사전 조회에서 받은 `expected_status`와 `expected_updated_at`을 함께 보내며, 서버는 release-state 다음 SourceRight row를 `FOR UPDATE`로 잠근 같은 transaction에서 두 값을 비교한다. 누락 row가 새로 생기거나 기존 row가 변경·만료·철회되면 409 `stale_source_right`로 중단하므로 사전 확인 뒤 발생한 철회를 다시 활성화할 수 없다. 저장소 `config.yaml`의 `telegram:activistkorea` 레코드는 스키마 예시이자 차단 확인용 `pending` 자리표시자이며, 빈 `evidence_ref` 상태에서는 어떤 권한도 부여하지 않는다. 운영자는 설정 파일을 `active`로 수정해 승인 절차를 우회하지 않는다. KIND 수집기는 네트워크 요청 전에 ops/admin 인증의 `GET /api/v1/ops/source-right-eligibility?source_right_id=official:kind&use=ingest`를 호출하고 `eligible=true`와 64자리 `rights_revision`을 확인한다. 서버는 HMAC snapshot transaction 안에서도 같은 row를 `FOR UPDATE`로 다시 검증하므로 payload가 자신의 권한을 동시에 생성해 우회할 수 없다.

회사·행동주주 공식사이트 connector는 ops/admin 인증의 `GET /api/v1/ops/official-site-rights`에서 현재 유효하고 재배포가 허용된 allowlist만 받는다. `ai_allowed=false`여도 원문 저장·재배포 범위가 유효하면 수집할 수 있지만 AI 입력에는 사용할 수 없다. 수집 결과는 HMAC `upsert_official_site_snapshot` 한 요청으로 적용하며 snapshot·ACK·문서 버전·사건 관측을 한 transaction으로 확정한다. 같은 외부 ID의 동일 내용은 멱등이고 내용이 바뀌면 기존 문서를 덮지 않고 새 `version_no`와 `correction_of_document_id`를 만든다.

DART를 호출하는 모든 workflow는 실제 HTTP 요청 직전에 ops 인증의 `POST /api/v1/ops/dart-quota`로 물리 요청 1건을 소비한다. `action=consume` 요청에는 고유 `attempt_id`, 현재 KST `quota_day`, 비밀이 아닌 소문자 SHA-256 `credential_id`, `operation=list|corp_code`, 실행 `code_revision`, 검증된 backend binding을 포함한다. 모든 키를 합산한 KST 일일 상한은 40,000건이며 각 실행의 별도 안전 상한은 10,000건이다. 같은 요청 재전송은 소비량을 늘리지 않으며 다른 payload로 같은 ID를 쓰면 409다. OpenDART 코드 `020`을 받으면 같은 attempt로 `action=block_020`, `reason=opendart_status_020`을 기록해 해당 키만 다음 KST 자정까지 차단하고 다른 유효 키로 계속한다. 코드 `901`은 `action=disable_901`, `reason=opendart_status_901`로 해당 키만 durable disable한다. `GET /ops/dart-quota?quota_day=YYYY-MM-DD`는 전역 사용량과 credential별 상태를 반환하는 읽기 전용 조회다. 키 원문은 API, DB, 로그, checkpoint, artifact에 기록하지 않는다.

## 공개 상태와 이용권한

- 공개 쿼리는 `publication_status=published`인 레코드만 반환한다.
- 소스 유형과 무관하게 모든 공개 문서는 활성 `SourceRight`가 연결되어야 한다.
- 연결된 권한은 현재 시각에 유효하고, 철회되지 않았으며, `redistribution_allowed=1`이어야 한다.
- 권한이 만료·철회되면 해당 문서는 공개 문서·검색·사건 근거에서 즉시 제외된다.
- Telegram-only 사건은 `verification_status=signal`, `publication_status=draft`로 남는다.
- `high`와 `market_sensitive` 사건은 HMAC 수집으로 자동 공개할 수 없다. 편집자가 `/admin/events/{event_id}/review`에서 승인해야 한다.
- 불완전 identity는 자동 병합하지 않는다. 편집자는 `POST /api/v1/admin/events/{event_id}/identity`로 회사·유형·행위·대상·당사자·효력일·기한을 완성하며, 서버가 `comparison_key`를 재계산한다. 이때 안정적인 `event_id`는 바뀌지 않고 동일 키 소유자가 이미 있으면 409와 함께 검수 대상으로 남는다.
- `title`, `body_text`, `claim_text`는 `original_language`와 함께 원문 그대로 저장하며 번역 필드를 생성하지 않는다.
- `/feedback` 접수는 항상 `pending`, `is_public=0`이다. 편집 검토 전 공개 데이터에 합쳐지지 않는다.

## 공개 정렬·필터 계약

`GET /api/v1/today`는 공개 가능한 전체 사건을 서버에서 평가해 `top` 최대 5건과 `watch` 최대 10건을 반환한다. `signal`은 항상 제외하며 중요도, 공식 근거, 확인 상태와 기한 근접도를 고정된 `today-v1` 규칙으로 계산한다. 같은 점수는 발생일과 `event_id`로 결정적으로 정렬한다. 이 응답은 페이지네이션하지 않으며 전체 목록은 `/events`의 일반 페이지네이션을 사용한다.

`/events`, `/search`, `/calendar`, Atom과 CSV·JSON export는 회사, actor, 사건 유형, 확인·identity 상태, 중요도, 근거 문서·소스 유형과 기간 필터를 공통으로 적용한다. `/calendar`의 주총 의안(`proposal_vote`) 분기도 같은 필터를 적용하며 필터가 있다는 이유로 의안 결과를 제거하지 않는다. 대형 문서 본문은 byte-aware `body_page`·`body_page_size`와 `body_truncated`를 사용한다.

`GET /api/v1/ops/official-site-candidates`는 ops/admin 인증 뒤 실제 공식 근거가 있는 사건만 집계해 회사 20곳과 승인된 행동주주·기관·주주연대 actor 10곳을 결정적으로 반환한다. 응답은 ID, 이름, 사건 수, 원시 점수, 최신 사건 시각과 순위만 포함하며 문서 본문이나 내부 payload를 내보내지 않는다.

## 운영 관측과 릴리스 증빙

Pages/API 배포 결과는 `POST /api/v1/ops/web-distribution-observations`에 한 번에 최대 50건까지 기록한다. 각 항목은 full 40자 `build_sha`, GitHub `workflow_run_id`, 1 이상의 `workflow_run_attempt`, `distribution_target=pages|api`, 성공 여부와 관측 시각을 포함한다. `(workflow_run_id, workflow_run_attempt, distribution_target)`은 유일하며 같은 관측의 재전송만 멱등 처리한다. 실패는 `failure_detected_at`을 반드시 포함하고 성공은 이를 null로 둔다.

KIND 지연은 client가 만든 날짜·자정 추정값을 받지 않는다. 서버가 `EventObservation.first_observed_at - Document.published_at`의 실제 비음수 표본으로 p95를 계산한다. 실제 관측이 0건인 무공시일만 표본 0·지연 `null`의 N/A를 허용하며, 관측이 있으면 모든 표본에 실제 시각이 있어야 한다. same-event precision은 일일 품질 payload에 넣지 않고 사람 라벨 benchmark만 정답 근거로 사용한다.

`GET /api/v1/ops/release-evidence`는 실제 DB 분자·분모와 동일 SHA의 immutable hash를 반환한다. availability는 최근 7일 동안 `watchdog-v1-kst-5m-minute01` cadence의 4개 route×일 288 slot을 반환하며, route별 72자리 coverage bitmap·missing·duplicate·off-cadence·첫/마지막 관측·실제 interval p95·일 경계 포함 최대 공백을 포함한다. 콘텐츠 품질은 `governance_corpus_2021_plus_kst_day_end_v2` 범위의 2021년 이후 누적 corpus를 각 KST 일자 종료 시점으로 고정해 계산한다. v2는 공개 승인 객체가 참조한 모든 source class의 고유 문서를 분모로 사용하며 원문 변형·권리 만료·철회 후에도 해당 문서를 분모에서 제거하지 않는다. 정확한 `code_revision`을 지정해도 사람 라벨 문서는 응답 크기 예산 때문에 hash·상태 metadata만 제공하며 전체 benchmark·사용성·승인 문서는 admin 전용 `GET /api/v1/admin/release-evidence-inputs`에서 확인한다.

보호된 `preview → live` 전환은 일별 증빙만 신뢰하지 않고 같은 v2 문서 분모의 현재 SourceRight를 단일 트랜잭션 안에서 다시 확인한다. 일회용 승인의 candidate SHA·evidence digest·run/artifact ID·v1/v2 예상 version·nonce·만료가 하나라도 맞지 않거나, 활성 상태·AI 허용·재배포 허용·유효기간·철회 여부·허가 증빙 중 하나라도 실패하면 409로 거절하고 두 release state와 version을 그대로 유지한다. SourceRight 쓰기와 cutover는 모두 release-state row를 먼저 잠그고 rights/data row를 나중에 처리해, cutover 직전 철회가 검사를 추월하거나 역순 잠금으로 교착되는 것을 막는다.

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
    "importance":"medium",
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

회사 ID는 8자리 DART `corp_code`다. 문서의 `(source_right_id, external_id, version_no)`와 각 엔터티 ID는 멱등 키다. 정정 공시는 새 `version_no` 및 `correction_of_document_id`로 연결한다.

## DeliveryOutbox 보존 계약

현재 배포 모드는 `web_only`다. `enqueue_delivery_outbox`와 `claim_delivery_outbox`는 인증 성공 여부와 무관하게 신규 outbound를 만들거나 lease하기 전에 HTTP 410 `outbound_delivery_disabled`를 반환한다. 아래 payload와 consumer 순서는 과거 row의 스키마·감사 및 호환 문서로만 유지하며 신규 발송 절차로 사용하지 않는다.

과거 Producer payload 형식은 다음과 같다.

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

과거 Consumer 순서는 다음과 같다. 현재는 신규 claim이 항상 410이므로 실행하지 않는다.

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
