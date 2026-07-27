# DART 전역 요청 원장

정기 수집, 회사 마스터, canary, dry-run, apply 백필은 실행별 메모리 예산이
아니라 MySQL의 KST 일자별 전역 원장을 공유한다. 서로 다른 workflow dispatch,
날짜 범위, checkpoint fingerprint로 재실행해도 모든 키를 합산한 하루
40,000회 제한은 하나다. 별도로 단일 실행은 최대 10,000회까지만 소비할 수
있어 하나의 canary나 백필이 전역 일일 예산을 독점하지 못한다.

각 물리 OpenDART HTTP 시도 직전에 클라이언트가
`POST /api/v1/ops/dart-quota`의 `consume` ACK를 받는다. timeout, 5xx, 불완전
ACK, 차단 또는 한도 소진이면 DART 요청을 보내지 않는다. HTTP 전송 실패와
OpenDART 429/5xx 재시도도 이미 소비된 1회이며 반환하지 않는다.

`attempt_id`는 GitHub run ID·run attempt·job·phase, 프로세스별 난수 nonce와
단조 증가 counter로 구성한다. quota API ACK를 잃어 같은 consume을 재시도할
때만 동일 ID를 사용한다. 다음 물리 HTTP 재시도와 새 프로세스는 새 ID를 쓴다.

## durable ACK와 명시적 replay

`consume`, `block_020`, `disable_901`의 HTTP 200은 단순히 SQL 실행이
성공했다는 뜻이 아니다. 서버는 다음 조건을 모두 확인한 뒤에만 200을 반환한다.

1. mutation transaction의 `commit()`이 명시적으로 성공하고 transaction이
   종료됐는지 확인한다.
2. 기존 transaction과 다른 fresh PDO connection을 열고 동일한
   `BSIDE_BACKEND_BINDING_ID`인지 다시 확인한다.
3. fresh connection에서 해당 `attempt_id`, quota day, credential,
   code revision, action별 request hash와 `consumed_units=1`을 다시 읽는다.
4. 전역·credential별 사용량과 `block_020`·`disable_901`의 실제 상태를
   독립 readback 결과로 검증한다.

클라이언트도 첫 200만으로 DART 요청을 보내거나 다음 credential로 넘어가지
않는다. 첫 응답의 identity, backend binding, 카운터와 상태를 검증한 다음
**JSON 필드와 값이 완전히 같은 POST를 별도 호출로 한 번 더 전송**한다. 이
명시적 replay 응답은 반드시 `duplicate=true`여야 한다. 첫 호출 내부의
transport retry가 유실된 응답을 복구했다면 첫 ACK부터 `duplicate=true`일 수
있지만, 이 경우에도 별도 replay는 생략하지 않는다.

replay는 같은 attempt를 다시 소비하지 않으므로 한 번의 논리적 `consume`은
전역·credential 원장에서 각각 1회만 증가한다. 두 번째 ACK가
`duplicate=false`이거나 불완전하고, 카운터가 역행하거나, timeout·5xx가
발생하면 실제 OpenDART 요청과 credential 전환을 모두 중단한다.

commit 또는 독립 readback을 증명하지 못하면 서버는 HTTP 503과 고정 code
`dart_quota_persistence_failed`를 반환한다. 외부에 공개되는 `detail`은 다음
일곱 값뿐이다.

- `transaction_commit_failed`
- `transaction_state_invalid`
- `transaction_readback_connection_failed`
- `transaction_readback_binding_failed`
- `transaction_readback_attempt_failed`
- `transaction_readback_day_failed`
- `transaction_readback_credential_failed`

그 밖의 exception message, SQL·host 정보, API key와 provider 응답은 응답
detail에 포함하지 않는다. 클라이언트도 이 고정 code와 detail만 안전한 진단
정보로 취급한다.

각 consume은 키 원문 대신 그 키 바이트의 전체 소문자 SHA-256
`credential_id`에 묶인다. OpenDART 상태 `020`을 받으면 해당 consume의
`attempt_id`로 그 credential만 다음 KST 자정까지 `block_020`하고, pool의
다음 유효 키로 동일한 논리 요청을 계속한다. 상태 `901`은 해당 credential을
`disable_901`로 durable disable하며 새 키로 교체할 때까지 다시 선택하지
않는다. block·disable ACK 실패는 workflow 실패이고, 다른 유효 키가 없을 때만
전체 DART 수집을 중단한다. 모든 물리 provider 요청은 성공 여부와 관계없이
전역 40,000회와 credential별 원장에 각각 1회로 남는다.

운영 workflow는 다음 설정을 필수화한다.

- 보호된 `governance-runtime` 환경의 `OPENDART_API_KEYS`: 줄바꿈 또는 쉼표로
  구분한 중복 없는 소문자 40자리 hex 키 목록
- `DART_API_KEY`: pool이 없을 때만 허용하는 기존 단일 키 fallback
- `CURATOR_REQUIRE_DURABLE_DART_QUOTA=1`
- `BSIDE_API_BASE_URL`
- `BSIDE_OPS_TOKEN`
- `BSIDE_BACKEND_BINDING_ID`
- `GITHUB_SHA` 또는 `CURATOR_CODE_REVISION`

apply 백필은 이 revision을 각 일별 성공·실패 checkpoint 결과에 기록한다.
revision은 장기 작업의 fingerprint에는 포함하지 않으므로 새 배포 SHA에서도
같은 작업을 재개할 수 있다. 다만 30일 검수 증빙은 모든 window와 exporter가
동일한 revision일 때만 생성된다.

GitHub Actions에서는 설정 플래그가 누락돼도 `GITHUB_ACTIONS=true`가 durable
원장을 강제한다. API URL이나 token 중 하나만 설정된 경우에도 로컬 예산으로
후퇴하지 않고 설정 오류로 종료한다.

`BSIDE_BACKEND_BINDING_ID`는 MySQL server UUID, database 이름, table prefix를
직접 노출하지 않고 SHA-256으로 묶은 비밀이 아닌 운영 식별자다. quota API의
요청은 이 기대값을 포함하며 PHP는 트랜잭션 전에 실제 바인딩과 비교한다.
불일치하면 quota row를 변경하지 않는다. 성공 ACK도 이 값과 정확히 일치하지
않으면 실제 OpenDART 요청 전에 fail-closed한다.
30일 사람 검수 corpus와 manifest도 같은 값을 검증해 수집과 증빙이 동일한
운영 DB를 사용했음을 확인한다.
HMAC `upsert_governance_snapshot`의 성공 ACK도 같은 값을 반환해야 한다.
클라이언트는 기대 바인딩을 HMAC 서명 본문에 포함하고, PHP는 트랜잭션을 열기
전에 실제 MySQL 바인딩과 비교한다. 불일치하면 어떤 row도 쓰지 않고 즉시
중단한다. 클라이언트는 row별 ACK 수와 DB 바인딩이 모두 일치한 배치만
checkpoint의 acknowledged count에 포함하며, 바인딩 오류 뒤에는 다음 청크나
최종 run 저장도 시도하지 않는다.

GitHub Actions는 pool을 검증한 직후 각 키를 개별 `::add-mask::` 처리한다.
Provider 오류 로그는 DART API key 원문, key가 포함된 URL 또는 응답 본문을
출력하지 않는다. DART는 `OpenDART HTTP <status>`, KIND는
`KIND HTTP <status>` 형식의 안전한 오류만 기록한다. 키 원문은 checkpoint,
metrics, evidence artifact, `attempt_id`에도 들어가지 않는다.

이 계약은 migration 012가 만든 credential·credential-day 원장과
40,000건 전역 한도에 의존한다. `012_dart_credential_pool.sql`은 원본
exact bytes의 SHA-256을 같은 MySQL 세션의
`@bside_migration_012_sha256`에 설정한 뒤
`scripts/apply_migration_012.py`로 apply·replay한다. schema 12와 배포
manifest의 migration hash가 일치하지 않으면 API는 DART 요청 전에
fail-closed한다.

## schema 12 전환 게이트

`.github/workflows/ingest-official.yml`의 schedule·수동 dispatch와
`.github/workflows/official-backfill.yml`은 repository variable
`DART_OFFICIAL_INGEST_ENABLED`가 정확히 `true`인 경우에만 시작한다. 변수가
없거나 다른 값이면 job 전체가 skip된다. 이 게이트는 schema 12 클라이언트가 구
PHP quota API를 호출하지 못하게 하는 배포용 안전장치다.

증분 수집과 백필은 protected cutover·SourceRight bootstrap·slot epoch reset과
같은 repository·exact Git ref 단위 non-cancelling concurrency queue를 공유한다.
따라서 정상 공개 전환은 한 논리 실행의 list·회사 master·document chunk·final run
ACK가 끝난 뒤에만 시작한다. 긴급 rollback은 백필을 기다리지 않고 release state를
즉시 `closed`로 내린 뒤 Pages lock 안에서 다시 확인한다. 이미 진행 중인 DART 실행은 다음 chunk의 서명된
기대 상태 검증에서 mutation 없이 실패하며 partial 데이터는 비공개 상태에서 replay
대상이 된다. 같은 짧은 이름의 tag는 exact 기본 branch 검사를 통과하지 못한다.

전환은 `DART_OFFICIAL_INGEST_ENABLED=false` 설정, `ingest-official`과
`official-backfill`의 queued·running run 0건 확인, pending-schema-upgrade
PHP 배포, migration 012 apply·replay, schema 12 exact smoke,
`DART_OFFICIAL_INGEST_ENABLED=true` 복원의 순서로만
진행한다. 한 단계라도 실패하면 게이트를 다시 열지 않는다.
