# 글로벌 공식 소스 백필

`global-backfill.yml`은 Production Alpha 출시 증빙에 필요한 미국·일본·영국의
완료일 자료를 날짜별로 적재하는 수동 워크플로다. 예약 증분 수집
`ingest-global.yml`과 같은 `ingest-global-${{ github.ref_name }}` concurrency
group을 사용하고 `cancel-in-progress=false`로 실행하므로, 동일 브랜치의 증분
수집과 백필이 겹치지 않는다.

출시 전 수동 적재는 `GOVERNANCE_PIPELINE_MODE=off`에서도 실행할 수 있다.
대신 기본 브랜치의 보호된 `governance-runtime` 환경에서만 실행되며, 수집 전에
운영 API가 정확한 dispatch SHA·schema 11·`closed` 상태인지 확인한다. 이
검사가 실패하면 공식 소스 요청과 DB 쓰기를 시작하지 않는다.

## 입력 계약

- `source`: `US|JP|GB|all`
- `from_date`: 포함 시작일, `YYYY-MM-DD`
- `to_date`: 미포함 종료일, `YYYY-MM-DD`
- `mode`: `apply|replay`
- `max_windows`: `1..31`

날짜 범위는 완료된 날짜만 포함해야 한다. 요청 범위 전체가 `max_windows` 안에
들어와야 하며 최대 31일이다. 범위를 조용히 잘라 처리하지 않는다. `all`은
국가별 job을 병렬로 만들지만, 각 job 안의 1일 window는 오래된 날짜부터
순서대로 실행한다.

미국 역사 구간은 SEC daily master index만 사용한다. 현재 시점의 Atom feed와
source cursor를 역사 receipt에 섞지 않는다. 일본과 영국은 각각
`EDINET_CONNECTOR_MODE=active`, `COMPANIES_HOUSE_CONNECTOR_MODE=active` 및
해당 공식 API 자격정보가 있어야 한다. 모든 실행은 활성 SourceRight와 v2 ops
API ACK를 검증하며 공개 사건을 직접 만들지 않는다.

## 실행 모드와 멱등성

`apply`는 한 번 수집하고 적재한다. 운영 checkpoint는 들어온 완료 경계가 현재
경계보다 새로울 때만 전진하므로 과거 백필이 증분 수집 상태를 되감을 수 없다.
Companies House처럼 현재 이력을 훑어 과거 날짜에 도달하는 소스는 요청 횟수를
콘텐츠 identity에서 제외하고, 요청 날짜 안에서 관측한 원자료 수만 `raw_count`로
기록한다. 따라서 이후 새 공시로 페이지 수가 늘어도 같은 과거 자료는 같은
identity를 유지하고, 과거 자료 자체가 바뀐 경우에는 충돌로 차단된다.

`replay`는 같은 완료일 자료를 수집하되 모든 chunk를 서버 강제 읽기 전용
`ingest_mode=replay`로 보낸다. API에는 같은 idempotency key, 정규화 payload
hash, 배포 SHA의 receipt가 이미 존재해야 한다. 없거나 내용이 달라지면 문서,
사건, receipt, checkpoint를 변경하기 전에 실패한다. 성공 증빙은 모든 날짜에
대해 `idempotent=true`, `read_only=true`여야 한다.

각 날짜는 `<COUNTRY>-<YYYY-MM-DD>.json` receipt로 저장된다. 국가별 summary는
처리 날짜, receipt SHA-256, raw·ACK 합계, replay 검증 여부를 기록한다.
workflow artifact는 날짜별 receipt와 summary를 함께 30일 보존하고, 실패해도
이미 완료된 날짜와 실패 날짜를 보존한다. 원문, 응답 본문, API 키, Bearer
token은 evidence에 포함하지 않는다.

## 30일 출시 증빙 실행

최근 완료 30일을 실행할 때는 정확한 포함 시작일과 미포함 종료일을 입력하고
`max_windows=30`을 사용한다. 먼저 `apply`를 완료한 뒤, 필요하면 같은 범위를
`replay`로 실행한다. replay는 기존 apply receipt를 읽기 전용으로 검증하며
새로운 자료나 checkpoint를 만들지 않는다.

성공 기준은 국가별 `processed_windows=30`, 실패 window 0, 모든 receipt의
`code_revision`이 dispatch SHA와 동일, `raw_count >= acknowledged_count`,
replay 실행에서는 모든 날짜의 `replay_verified=true`다.
