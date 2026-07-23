# 공식 수집 예약 슬롯 증빙

정식 shadow와 release evidence는 DB에 존재하는 실행 건수 자체를 성공률 분모로
사용하지 않는다. `official-v1-82-slots` 고정 cadence에 따라 완료된 KST 날짜마다
다음 82개 증분 수집 슬롯을 먼저 만든다.

- KST 00:00~06:30: 30분 간격 14개
- KST 07:00~23:45: 15분 간격 68개
- DART와 KIND 각각 82개, 합계 164개 source-slot이 일일 분모다.

각 실행은 `run_kind`, `event_schedule`, `scheduled_slot_at`,
`company_master_sync`를 MySQL run metrics에 보존한다. `run_kind`는
`scheduled_incremental|manual|backfill|company_master` 중 하나다. 수동 실행,
백필, 주간 회사 마스터 실행은 실제 실행·ACK 감사에는 남지만 예약 성공률의
분자와 분모에는 들어가지 않는다.

정식 증빙은 인증된 페이지형 `/api/v1/ops/official-run-ledger`를 끝까지 읽고,
서버가 `/api/v1/ops/release-evidence`에 제공한 행 수·digest·일별 집계와 다시
대조한다. 다음 중 하나라도 발생하면 증빙 artifact를 만들지 않는다.

- 예약 슬롯이 DART 또는 KIND 중 한 소스라도 없음
- 같은 소스와 슬롯을 두 실행이 주장함
- 실행 실패, 소스별 ACK 불일치, 소스 합계와 실행 전체 raw/ACK 합계 불일치
- 과거 행에 `run_kind`나 `scheduled_slot_at`이 없어 분류가 모호함
- 원장 페이지 누락·cursor 반복·행 수 또는 digest 불일치
- 품질 스냅샷의 expected/succeeded 수가 원장 계산과 다름

따라서 기존의 분류 불가능한 run은 새 shadow 기간에 사용할 수 없다. 동일한
release candidate SHA와 이 계약이 적용된 뒤에 수집된 첫 완전한 KST 날짜부터
14일 shadow 창을 새로 시작한다.

Watchdog availability도 예약 workflow의 `GITHUB_SHA`를 사용하지 않는다.
인증된 `/ops/health`가 durable 배포 관측에서 돌려준 `active_deployment.build_sha`를
사용한다. 공개 `config.js` SHA와 다르거나 수집 최신성 incident가 이미 발생한
경우에도, 인증된 활성 SHA가 있으면 HTTP probe 결과를 빠짐없이 DB에 전송한다.
release evidence는 KST 00:01부터 5분 간격의 route당 288개 slot을 실제
`observed_at`으로 bucket하며, 각 route의 72자리 coverage bitmap, missing·duplicate,
첫·마지막 관측, interval p95와 일 경계 포함 최대 공백을 보존한다. 도착한 row 사이의
간격만으로 가용성을 계산하지 않으므로 하루 앞부분의 성공 몇 건이 장시간 공백을
숨길 수 없다.
공식 수집 최신성은 소스별 `last_scheduled_success_at`만 읽으므로 수동 실행이나
백필 성공으로 예약 수집 공백을 덮을 수 없다.
