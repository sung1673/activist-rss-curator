# KIND SourceRight 수집 사전검증

KIND 수집기는 저장소의 기본값으로 이용권한을 생성하지 않는다. 편집자가 운영 DB에
`official:kind` 권한과 증빙·범위·유효기간·AI 및 재배포 허용 여부를 먼저 등록해야 한다.
공식 수집과 백필은 매 window마다 커넥터를 만들기 전에 운영 API에서 현재 적격성을
확인한다.

## 실행 계약

- 요청: `GET /api/v1/ops/source-right-eligibility`
- query: `source_right_id=official:kind&use=ingest`
- 인증: `Authorization: Bearer $BSIDE_OPS_TOKEN`
- API base 우선순위: `BSIDE_API_BASE_URL` → `GOVERNANCE_API_BASE_URL` →
  `ACTIVIST_API_URL`
- URL은 credentials·query·fragment가 없는 절대 HTTPS 주소여야 한다.

HTTP 200 응답이 `ok=true`, `eligible=true`, 요청과 동일한 source/use, 64자리 소문자
SHA-256 `rights_revision`을 모두 포함해야만 KIND adapter를 생성한다. HTTP 409
`source_right_ineligible`, 권한 만료·철회, 누락 설정, 네트워크 오류, timeout, 잘못된
JSON, 다른 HTTP 상태와 계약 불일치는 모두 해당 source 실패로 기록한다. 이때 KIND
connector 호출은 0건이고 백필 checkpoint는 완료 구간으로 전진하지 않는다.

검증된 `rights_revision`은 `collection_runs.source_outcomes.kind`와 run metrics에 남는다.
HMAC snapshot payload에는 `official:kind` SourceRight를 넣지 않으므로, 수집 프로세스가
편집 승인 레코드를 생성하거나 덮어쓸 수 없다. DART-only canary에는 이 사전검증이나
`BSIDE_OPS_TOKEN`이 필요하지 않다.

## GitHub Actions 설정

KIND 승인 후 실제 adapter를 최초 검증할 때는 기본 브랜치의
`Validate KIND adapter preflight` (`kind-adapter-preflight.yml`)을 수동 실행한다.
이 workflow에는 예약 trigger와 사용자 입력이 없고, 보호된 `governance-runtime`
environment에서 실행한 정확한 기본 브랜치 SHA만 checkout한다. 현재
`GOVERNANCE_PIPELINE_MODE`가 `off|dart_canary|shadow|live` 중 어느 값이든 실행할 수
있지만 Telegram·governance outbound는 항상 `false`/`disabled`로 강제한다.

실행 순서는 다음과 같이 고정한다.

1. endpoint·API key·운영 API URL·ops token이 모두 있는지 값 노출 없이 확인한다.
2. 인증된 `OfficialSourceRightClient().check_kind_ingest()`가 운영 DB의 현재 권한과
   `rights_revision`을 확인한다.
3. 2번이 성공한 경우에만 기존 `validate-kind-adapter.py`가 KIND endpoint의 첫
   페이지를 한 번 요청해 실제 JSON·pagination·접수번호·`corp_code`·접수시각
   계약을 검증한다.

수동 preflight에는 다음 값이 모두 필요하다.

- Secret `BSIDE_OPS_TOKEN`
- Secret `BSIDE_API_BASE_URL` 또는 Variable `GOVERNANCE_API_BASE_URL`
- Variable `KIND_DISCLOSURE_ENDPOINT`
- Secret `KIND_API_KEY`

endpoint/key 누락, 미등록·만료·철회된 권한, 권한 API 오류 또는 adapter 계약
불일치는 모두 KIND endpoint 수집이나 다음 단계 진행 없이 fail-closed된다. 성공은
adapter와 권한 계약을 확인한 것일 뿐 pipeline mode를 변경하거나 수집·공개·발송을
시작하지 않는다.

KIND가 실제 선택되는 `ingest-official` 및 `official-backfill`도 다음 값을 사용한다.
일반 수집 adapter가 인증을 요구하지 않는 계약이라면 그 경로에서 `KIND_API_KEY`는
선택값일 수 있지만, 승인 직후 수동 preflight에서는 운영 설정 누락을 허용하지 않는다.

예약 `ingest-official`의 기본값은 Repository variable `KIND_CONNECTOR_MODE=off`다.
이 상태에서는 `shadow|live`여도 DART만 실행하고 KIND endpoint·key·freshness를
요구하지 않는다. `active`로 전환한 뒤에는 예약 실행이 KIND를 반드시 선택하며,
endpoint·SourceRight·수집 계약 중 하나라도 없거나 실패하면 기존처럼 fail-closed한다.
수동 `include_kind=true`와 이 문서의 preflight는 예약 토글과 별개인 명시적 검증이다.
`off|active` 이외의 값은 수집 전에 거절한다.

KIND dry-run도 실제 권한 상태를 확인한다. 설정이 하나라도 없으면 adapter contract
검사나 네트워크 수집 전에 workflow가 종료된다.
