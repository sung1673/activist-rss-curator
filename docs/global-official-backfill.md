# 글로벌 공식 소스 백필

Production Alpha의 실제 자동 30일 백필 대상은 **한국 DART와 미국 SEC EDGAR
두 소스뿐**이다. 한국은 `official-backfill.yml`의 `source=dart`, 미국은
`global-backfill.yml`의 `source=US`로 실행한다. JP EDINET·GB Companies House는
optional dormant identity이며 `link-only`, `coverage_unavailable`,
`public_ready=false`로 유지한다. 이 둘을 백필하거나 30일 connector 증빙에
포함하지 않는다. CA·AU도 승인된 링크 메타데이터만 적재하므로 자동 30일 백필
대상이 아니다.

`global-backfill.yml`은 예약 SEC 증분 수집 `ingest-global.yml`, DART/KIND
증분·백필, 공식사이트·CA/AU 링크 수집, 보호된 cutover와 같은
repository·exact Git ref 단위 concurrency group을 사용하고
`cancel-in-progress=false`로 실행한다. 따라서 공식 소스의 증분 수집·백필
다중 청크와 정상 `preview → live` 전환은 겹치지 않는다. 긴급 rollback은 장기
백필을 기다리지 않는 별도 queue에서 API를 즉시 closed로 만들고, Pages lock 안에서
다시 확인한 뒤 legacy artifact를 복원한다.

이 문서의 SEC `global-backfill.yml` 수동 적재는
`release_state=closed|preview`를 명시적으로 선택한다. 기본값은 `closed`이며,
`GOVERNANCE_PIPELINE_MODE=off`에서도 실행할 수 있다. `preview`는
`GOVERNANCE_PIPELINE_MODE=shadow`, ops 인증, 별도의 preview token을 모두
요구한다. 두 모드 모두 기본 브랜치의 보호된 `governance-runtime` 환경에서만
실행되며, 수집 전에 운영 API가 정확한 dispatch SHA·schema 12·선택한 비공개
상태인지 확인한다. 이 검사가 실패하면 공식 소스 요청과 DB 쓰기를 시작하지
않는다. 이 예외는 DART
`official-backfill.yml` apply에 적용되지 않는다. DART는 `off`와 `closed`에서
dry-run을 통과한 뒤 `GOVERNANCE_PIPELINE_MODE=dart_canary`로 전환하고,
v1·v2가 계속 `closed`인 상태에서만 apply한다.

## SEC 입력 계약

- `source`: `US|all` (`all`도 Alpha에서는 US 한 job만 생성)
- `from_date`: 포함 시작일, `YYYY-MM-DD`
- `to_date`: 미포함 종료일, `YYYY-MM-DD`
- `mode`: `apply|replay`
- `release_state`: `closed|preview` (기본 `closed`)
- `max_windows`: `1..31`

날짜 범위는 완료된 날짜만 포함해야 한다. 요청 범위 전체가 `max_windows` 안에
들어와야 하며 최대 31일이다. 범위를 조용히 잘라 처리하지 않는다. 각 1일
window는 오래된 날짜부터 순서대로 실행한다.

미국 역사 구간은 SEC daily master index만 사용한다. 현재 시점의 Atom feed와
source cursor를 역사 receipt에 섞지 않는다. 공개 EDGAR 수집에는 API key를
사용하지 않으며 승인된 `SEC_EDGAR_USER_AGENT`를 사용한다. 실행은 활성
`official:sec-edgar` SourceRight와 v2 ops API ACK를 검증하며 공개 사건을 직접
만들지 않는다. workflow와 runner는 JP·GB 입력을 허용하지 않는다.

## DART 입력 계약

`official-backfill.yml`은 `source=dart`, 완료된 KST 날짜의 `from_date`,
`to_date`, `max_windows=1..31`을 받는다. 먼저 `mode=dry-run`으로 정정·철회
표본과 요청 예산을 검증한 뒤 `mode=apply`로 적재한다. 같은 범위를 다시
`apply`하면 durable checkpoint와 idempotency key를 통해 문서·사건·checkpoint
증가가 없어야 한다. 자격정보는 보호된 `governance-runtime`의
`OPENDART_API_KEYS`에 줄바꿈 또는 쉼표로 구분한 중복 없는 소문자 40자리 hex
pool로 등록한다. pool이 우선이며 `DART_API_KEY`는 pool이 없는 단일 키
fallback이다. 모든 키 합산 KST 일일 원장은 40,000건, 단일 실행은 10,000건이다.
OpenDART `020`은 해당 키만 다음 KST 자정까지 차단하고 다음 키로 계속한다.
`901`은 해당 키를 durable disable한다. 모든 키가 사용할 수 없을 때만 같은
checkpoint에서 중단하며, 키 원문과 요청 URL은 receipt·로그·artifact에 넣지
않는다. GitHub Actions는 collector보다 먼저 각 키를 개별 mask한다.

운영 적재의 각 data batch는 PHP 7.3 native PDO가 unbuffered인 경우에도 모든
조회 cursor를 닫은 뒤 다음 명령을 실행해야 한다. MySQL driver 2014 또는
`governance_snapshot_persistence_failed`가 한 batch라도 발생하면 ACK는 0으로
간주하고 해당 날짜 checkpoint를 완료하지 않는다. 같은 checkpoint 재개 전에는
원인이 해소됐는지 unbuffered guarded-snapshot 통합 테스트로 확인한다.

## 실행 모드와 멱등성

`apply`는 한 번 수집하고 적재한다. 운영 checkpoint는 들어온 완료 경계가 현재
경계보다 새로울 때만 전진하므로 과거 백필이 증분 수집 상태를 되감을 수 없다.
`replay`는 같은 완료일 자료를 수집하되 모든 chunk를 서버 강제 읽기 전용
`ingest_mode=replay`로 보낸다. API에는 같은 idempotency key, 정규화 payload
hash, 배포 SHA의 receipt가 이미 존재해야 한다. 없거나 내용이 달라지면 문서,
사건, receipt, checkpoint를 변경하기 전에 실패한다. 성공 증빙은 모든 날짜에
대해 `idempotent=true`, `read_only=true`여야 한다.

SEC 완료일 receipt만 `global-ingest-v2-day:us:<64자리 SHA-256>` namespace를
사용한다. 서버는 정확한 1일 window, 실제 `batch_request_count=1`, daily master
index provenance와 Python producer가 계산한 semantic digest를 다시 검증한다.
예약 Atom/current refresh는 `global-ingest-v2-current:us:<digest>`를 사용하며
30일 출시 증빙 대상이 아니다. 같은 current 결과를 다시 확인하면 문서·사건·
receipt·checkpoint는 바꾸지 않고 exact source cursor와 SHA가 일치할 때 connector
heartbeat만 갱신한다. 완료일 replay는 heartbeat도 갱신하지 않는다.

완료일과 current SEC 요청은 본문에 `expected_release_state`를 포함한다. `preview`
쓰기에는 ops Bearer와 별도로 `X-BSIDE-Preview-Token`을 보내며, 서버는 v1·v2
release state 두 row가 모두 기대 상태인지 먼저 확인한다. `apply`는 같은 두 row를
transaction에서 다시 잠근 다음 SourceRight와 connector를 잠가 상태 전환 경쟁을
차단한다. `closed` 적재도 같은 경계를 사용한다. `replay`는 경계를 확인하지만
receipt·heartbeat·문서·사건·checkpoint를 변경하지 않는다. 상태와 preview token은
semantic idempotency identity에서 제외되므로 같은 공식 자료의 증빙 key는 전환
과정에서 바뀌지 않는다.

SEC 각 날짜는 `US-<YYYY-MM-DD>.json` receipt로 저장된다. summary는
처리 날짜, receipt SHA-256, raw·ACK 합계, replay 검증 여부를 기록한다.
workflow artifact는 날짜별 receipt와 summary를 함께 30일 보존하고, 실패해도
이미 완료된 날짜와 실패 날짜를 보존한다. 원문, 응답 본문, API 키, Bearer
token은 evidence에 포함하지 않는다.

## 30일 출시 증빙 실행

최근 완료 30일을 실행할 때는 DART와 SEC 각각 정확한 포함 시작일과 미포함
종료일을 입력하고 `max_windows=30`을 사용한다. SEC는 `apply` 완료 뒤 같은
범위를 `replay`로 실행한다. DART는 동일 범위 `apply` 재실행과 durable
checkpoint 대조로 멱등성을 검증한다. 어떤 경우에도 새로운 자료나 checkpoint
증가가 없어야 한다.

성공 기준은 DART·SEC 각각 `processed_windows=30`, 실패 window 0, 모든
receipt/checkpoint의 `code_revision`이 dispatch SHA와 동일,
`raw_count = filtered_out_count + accepted_count`,
`acknowledged_count = accepted_count`다. SEC replay는 모든 날짜의
`replay_verified=true`, DART 재실행은 문서·사건·checkpoint 증가 0이어야 한다.
운영 `/ops/alpha-release-evidence`의 `connector_coverage`는 정확히
`dart/KR`, `sec-edgar/US` 두 항목과 항목별 30개 window만 반환해야 한다. SEC
exporter는 완료일 namespace만 집계하고 current receipt는 무시한다. 동일
완료일이 둘 이상 존재하거나 완료일 namespace가 변조되면 fail-closed한다.
