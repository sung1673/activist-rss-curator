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
- `GITHUB_SHA` 또는 `CURATOR_CODE_REVISION`

GitHub Actions에서는 설정 플래그가 누락돼도 `GITHUB_ACTIONS=true`가 durable
원장을 강제한다. API URL이나 token 중 하나만 설정된 경우에도 로컬 예산으로
후퇴하지 않고 설정 오류로 종료한다.

Provider 오류 로그는 DART API key가 포함된 URL이나 응답 본문을 출력하지
않는다. DART는 `OpenDART HTTP <status>`, KIND는 `KIND HTTP <status>` 형식의
안전한 오류만 기록한다.
