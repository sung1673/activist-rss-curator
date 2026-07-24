# DART 전역 요청 원장

정기 수집, 회사 마스터, canary, dry-run, apply 백필은 실행별 메모리 예산이
아니라 MySQL의 KST 일자별 전역 원장을 공유한다. 서로 다른 workflow dispatch,
날짜 범위, checkpoint fingerprint로 재실행해도 하루 10,000회 제한은 하나다.

각 물리 OpenDART HTTP 시도 직전에 클라이언트가
`POST /api/v1/ops/dart-quota`의 `consume` ACK를 받는다. timeout, 5xx, 불완전
ACK, 차단 또는 한도 소진이면 DART 요청을 보내지 않는다. HTTP 전송 실패와
OpenDART 429/5xx 재시도도 이미 소비된 1회이며 반환하지 않는다.

`attempt_id`는 GitHub run ID·run attempt·job·phase, 프로세스별 난수 nonce와
단조 증가 counter로 구성한다. quota API ACK를 잃어 같은 consume을 재시도할
때만 동일 ID를 사용한다. 다음 물리 HTTP 재시도와 새 프로세스는 새 ID를 쓴다.

OpenDART 상태 `020`을 받으면 해당 consume의 `attempt_id`로 `block_020`을
기록하며 다음 KST 자정까지 모든 실행을 차단한다. block ACK 실패도 workflow
실패다. apply 백필은 같은 차단일을 durable checkpoint에도 기록한다.

운영 workflow는 다음 설정을 필수화한다.

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

Provider 오류 로그는 DART API key가 포함된 URL이나 응답 본문을 출력하지
않는다. DART는 `OpenDART HTTP <status>`, KIND는 `KIND HTTP <status>` 형식의
안전한 오류만 기록한다.
