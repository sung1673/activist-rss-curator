# BSIDE Production Alpha 출시 증빙

이 문서는 `news.bside.ai` 글로벌 데이터 터미널의 **Production Alpha 전환 전용** 증빙 계약을 정의한다. 기존 14일 GA 출시 게이트인 `curator/release_gate.py`, `release-evidence-inputs.yml`, `release-evidence.yml`, `release-gate.yml`은 그대로 유지한다. Alpha 통과를 최종 recall 95% 또는 동일 사건 precision 97% 인증으로 해석하거나 표시해서는 안 된다.

## 신뢰 경계

모든 파일과 watchdog 관측은 전환 대상과 동일한 40자리 Git SHA에서 생성되어야 한다. SHA 일치만으로는 충분하지 않다. 증빙은 전환할 정확한 `daily.yml` Pages artifact의 run ID·run attempt·artifact ID·이름·GitHub `sha256:` digest와 전체 사이트 content digest를 고정한다. `fixture`, `synthetic`, `sample`, `test` 출처, 합성값, 0인 분모, 다른 SHA, 중복 관측, 불완전한 24시간 관측은 fail-closed로 거절한다.

사람 검수 및 승인 데이터는 **실제 사람이 직접 검토하고 작성한 결과만** 허용한다. Codex·LLM은 후보와 입력 양식을 준비할 수 있지만 `event_reviews`, `same_event_pair_reviews`, `top5_reviews`, 감독자 승인, SourceRight 승인의 작성자나 정답 라벨러가 될 수 없다. `reviewer_type`과 `approver_type`은 `human`이어야 하고, AI가 정답을 생성했다는 표시는 반드시 `false`여야 한다.

보호 입력은 UTF-8 JSON을 gzip으로 압축한 뒤 base64로 인코딩한 일회용 `GLOBAL_ALPHA_RELEASE_INPUTS_GZIP_B64` secret으로 `global-alpha-evidence-inputs.yml`에 전달한다. 일반 base64 JSON은 GitHub Secret 크기 한도를 넘을 수 있으므로 허용하지 않는다. 압축 입력은 48,000 byte 이하, 압축 해제 결과는 2MB 이하이며 truncated gzip, concatenated member, trailing data를 모두 거절한다. `governance-release` environment의 실제 검토자가 실행을 승인한 뒤 immutable artifact가 만들어지면 즉시 secret을 삭제한다.

다만 운영 수치 자체는 secret을 신뢰하지 않는다. 같은 workflow가 인증된
`GET /api/v2/ops/alpha-release-evidence?code_revision=<SHA>`를 호출해 운영
MySQL에서 직접 30일 수집 영수증과 공개 사건·공식 문서를 다시 계산한다.
보호 입력의 connector coverage 필드와 `content-integrity.json` 전체는 이
DB export로 강제 교체된다. 배포 manifest SHA가 요청 SHA와 다르거나, receipt
누락·중복·공백·raw/ACK 불일치·DART checkpoint hash 불일치가 있으면 endpoint와
materialization이 모두 fail-closed한다.

## 입력 파일

보호 입력 bundle은 다음 최상위 구조를 사용한다.

```json
{
  "schema_version": 1,
  "kind": "bside-global-production-alpha-release-inputs",
  "code_revision": "<40-hex SHA>",
  "files": {
    "connector-idempotency.json": {},
    "human-review.json": {},
    "content-integrity.json": {},
    "experience.json": {},
    "approval.json": {}
  }
}
```

각 파일에는 공통으로 다음 provenance가 필요하다.

```json
{
  "schema_version": 1,
  "kind": "<아래 파일별 kind>",
  "environment": "production",
  "evidence_source": "protected_production_export",
  "is_synthetic": false,
  "code_revision": "<40-hex SHA>",
  "collected_at": "<timezone 포함 ISO-8601>"
}
```

| 파일 | `kind` | 필수 실제 증빙 |
|---|---|---|
| `connector-idempotency.json` | `bside-global-alpha-connector-idempotency` | 보호 입력에는 최초/동일 payload replay audit만 포함한다. 최초·replay 모두 raw·filtered-out·accepted·ACK 분할을 기록한다. DART·SEC EDGAR·EDINET·Companies House의 30일 window와 동일 분할·receipt hash는 운영 DB export가 덮어쓴다. |
| `human-review.json` | `bside-global-alpha-human-review` | 고유 사건 60건 이상, 고유 동일사건 pair 120쌍 이상, 공개 Top 5 전체 사람 승인 |
| `content-integrity.json` | `bside-global-alpha-content-integrity` | 보호 bundle의 값은 사용하지 않는다. 운영 DB exporter가 공개 사건 분모, 원문 언어·공식 URL, 제목 출처와 공식 문서 제목 byte 일치, Telegram 노출 수를 계산해 전체 파일을 교체한다. |
| `experience.json` | `bside-global-alpha-experience` | 390×844·768×1024·1440×900 시각 회귀와 axe, Web Vitals 실제 sample, 공개 API 실제 byte, 실패 탐지와 rollback drill |
| `approval.json` | `bside-global-alpha-release-approval` | 실제 감독자와 SourceRight 승인자, 6개국 유효 권한 범위, Production Alpha 범위 인정 |

`human-review.json`의 `raw_counts`는 배열 길이와 정확히 일치해야 한다. Top 5 분모가 0인 입력도 출시 증빙으로 인정하지 않는다. 중요한 사건이 없는 날의 빈 브리핑 정책은 제품 동작으로 허용되지만, Alpha 전환 자체는 사람이 검수한 실제 공개 Top 항목으로 검증한다.

최종 `content-integrity.json`은 운영 DB에서 계산된 원문 언어·공식 URL 보존율
100%, Telegram 노출 0, 내부 점수·큐 상태 등 내부 필드 노출 0을 증명해야 한다.
`title_provenance=source` 사건은 연결된 현재 유효 공식 문서에도
`title_provenance=source`가 있어야 하며 PHP string byte equality로 제목이
정확히 같아야 한다. 글자 하나 변경과 Unicode 정규화 차이도 보존으로 인정하지
않는다.

`experience.json`은 다음 하한을 적용한다.

- axe serious/critical 0, 모바일 첫 중요 사건 위치 300px 이하
- LCP p75 2.5초 이하, INP p75 200ms 이하, CLS p75 0.1 이하
- 모든 필수 API 경로 응답 250,000 byte 이하
- 실제 실패 탐지 10분 이하, pinned legacy artifact rollback drill 10분 이하

## 24시간 watchdog 증빙

`global-alpha-watchdog.yml`의 같은 SHA·default branch 완료 run을 성공/실패 구분 없이 모두 수집한다. 실패 run을 제외해 incident를 숨길 수 없으며, 취소·시간초과·artifact 누락도 즉시 실패한다. 각 artifact의 GitHub SHA-256 digest가 유효해야 한다. 하나의 preview candidate window에 대해 최소 287개 관측, 첫·마지막 경계 각 5분 이내, 관측 간격 2~8분을 요구한다.

Watchdog은 `BSIDE_OPS_TOKEN`과 읽기 전용 `GET /api/v2/ops/release-state`를 사용한다. `BSIDE_ADMIN_TOKEN`과 `BSIDE_RELEASE_AUTHORIZER_TOKEN`은 watchdog environment에 제공하지 않으므로 관측 job은 공개 상태를 바꾸거나 cutover 승인을 발급할 수 없다.

모든 관측은 다음을 만족해야 한다.

- `pipeline_mode=shadow`, `release_state=preview`, `web_surface=governance-preview`, Pages 배포 SHA·API health `code_revision`·workflow SHA 일치
- Pages `config.js`의 `apiBase`에서 계산한 canonical v2 주소와, 명시된 경우 `apiV2Base`가 watchdog이 실제 probe한 API base와 정확히 일치
- preview의 `index.html`, `config.js`, `app.js`, `styles.css` 원본 UTF-8 바이트를 매 관측마다 SHA-256 canonical file manifest로 계산하고 24시간 동안 단 하나의 terminal content identity만 유지
- public root·health·release state·deployed build·source status·live·search probe 모두 HTTP 200 및 contract valid
- live 사건이 있으면 같은 credential로 사건 상세를 실제 검증하고, 사건이 없고 소스가 정상일 때만 `event_detail={skipped:true, reason:no_live_event_available}`를 허용
- incident·degraded·warning 없음
- `KR/US/JP=market-wide`, `GB=official-register`, `CA/AU=link-only`
- 각 국가 공개 상태가 `active`이고 `public_ready=true`

누락 관측, 중복 ID/시각, 8분 초과 공백, 다른 candidate window, 소스 장애는 모두 전환을 차단한다.

## Artifact와 전환

`global-alpha-release-evidence.yml`을 실행할 때 검토자는 관측 대상과 동일한 `daily.yml` run ID와 `pages-<run_id>-<run_attempt>` 이름을 지정한다. workflow는 same SHA·default branch·성공·72시간 이내·GitHub artifact digest를 검증하고 그 artifact를 직접 다운로드한다. `global_alpha_pages_identity`는 root와 `/governance`의 네 terminal asset이 byte-identical인지 확인하고, 모든 정규 파일을 포함한 전체 사이트 content digest와 terminal content identity를 `pages-artifact-identity.json`에 기록한다. `daily.yml`과 `build-feed.yml`은 같은 `BSIDE_PUBLIC_WEB_URL` 입력을 사용하므로 config 바이트 차이를 기본값으로 숨길 수 없다.

그 다음 보호 입력 artifact와 최근 watchdog artifact를 exact workflow·default branch·same SHA·digest로 검증한다. 모든 watchdog terminal identity가 위 daily artifact의 terminal identity와 정확히 같아야 한다. 통과 시 다음을 포함한 `global-alpha-release-evidence`를 90일 보존한다.

- 원본 5개 보호 입력
- `observations.jsonl`
- watchdog artifact ID·run ID·digest manifest
- 입력/생성 workflow provenance
- `production-alpha-release-report.json`
- `pages-artifact-identity.json` — exact daily run/attempt/artifact/name/digest, 전체 사이트 content digest, 관측된 네 UI/config asset digest

보고서 JSON Schema는 [`schemas/global-alpha-release-report-v1.schema.json`](schemas/global-alpha-release-report-v1.schema.json)에 있다.

`governance-cutover.yml`은 최근 48시간 이내의 정확한 `global-alpha-release-evidence.yml` 성공 artifact만 받되, 실제 전환 시점에는 evidence run 생성 시각·보고서 `evidence_as_of`·관측 종료 시각이 모두 60분 이내여야 한다. cutover 입력에는 Pages run ID나 artifact 이름이 없다. workflow는 evidence 안의 binding만 읽어 GitHub API의 run·attempt·workflow·branch·SHA·artifact ID·이름·digest를 다시 대조하고 그 정확한 artifact만 다운로드한다. 다운로드한 전체 사이트 및 terminal content identity를 다시 계산하고 binding과 일치시킨 뒤 Alpha 게이트를 재실행해 기존 보고서와 byte-for-byte 일치할 때만 Pages 전환과 API live 승격을 계속한다. 같은 SHA의 다른 daily artifact를 지정하거나 교체하는 것은 불가능하다.

공개 승격은 이 검증 결과를 단순 workflow 조건으로만 사용하지 않는다. 보호된 `governance-release` environment에만 둔 `BSIDE_RELEASE_AUTHORIZER_TOKEN`으로 `POST /api/v2/admin/release-authorizations`를 호출해 다음 값을 DB의 일회용 승인에 고정한다.

- 현재 배포 manifest와 일치하는 40자리 candidate SHA
- GitHub가 제공하고 workflow가 재검증한 release-evidence artifact `sha256:` digest
- evidence workflow run ID와 artifact ID
- 현재 preview 상태인 v1·v2의 정확한 state version
- log에서 마스킹한 새로운 32바이트 nonce의 SHA-256
- 서버 시각 기준 60~900초 유효기간

`release_authorizer` 역할은 admin의 상위·하위 역할이 아니라 분리된 정확 일치 역할이다. 일반 `BSIDE_ADMIN_TOKEN`, ops, editor token으로 승인 발급 endpoint를 호출할 수 없다. 승인 API는 nonce 원문을 저장하거나 응답하지 않으며, 새 승인은 이전의 미사용 승인을 철회한다.

그다음 workflow는 preview `/sources/status`에서 정확한 필수 6개 connector가 모두 `public_ready=true`인지 확인하고 `BSIDE_ADMIN_TOKEN`으로 `POST /api/v2/admin/cutover`를 한 번 호출한다. 서버는 같은 transaction에서 승인 nonce·candidate SHA·artifact digest·두 state version을 대조한다. 이어 공개 문서가 없는 국가도 포함해 KR DART·US SEC·JP EDINET·GB Companies House·CA issuer IR link·AU ASIC link의 필수 connector와 SourceRight를 모두 잠그고 exact identity, active/error-free 상태, 실행 주기에서 계산한 15~45분 최신 성공·확인 시각, SEC cursor UTC 시각, link-only 관측·ACK, 증빙·현재 유효·미철회, 수집 및 공개 재배포 자격을 검사한다. 기존 v1·v2 공개 문서 권한도 별도로 재검사한 뒤에만 `governance_v1`과 `global_terminal_v2`를 함께 `live`로 바꾼다. 필수 소스 오류는 409 `required_alpha_sources_invalid`이며 상태와 승인은 소비되지 않는다. 두 감사 row는 같은 `release_authorization_id`, `cutover_at`, `sunset_at`을 가진다. 두 개의 `/admin/release-state` 호출로 순차 승격하는 방식은 금지된다.

만료·철회·소비·replay·binding 불일치는 fail-closed하며 상태 변경은 0건이다. 긴급 롤백은 직접 `live → closed`를 허용하지만 이미 소비된 승인을 다시 쓸 수 없으므로 재전환에는 새 evidence digest·현재 state version·새 nonce에 묶인 승인이 필요하다.

이 경로는 기존 GA gate를 우회하거나 약화하지 않는다. 향후 GA 선언은 기존 14일 shadow와 정식 benchmark·사용성·법률/편집/제품 승인 계약을 별도로 통과해야 한다.

## 30일 수집 범위와 제목 출처 증빙

운영 DB exporter가 만든 `connector-idempotency.json`의
DART·SEC EDGAR·EDINET·Companies House 각 항목은
`coverage_started_at`, `coverage_ended_at`, `successful_window_count`,
`failed_window_count`, `completed_windows`를 포함해야 한다. 각 completed window는
정확한 1일 반개구간, `raw_count`, `filtered_out_count`, `accepted_count`,
`acknowledged_count`, `complete` 상태, 동일 40자리 SHA와 고유한 receipt
SHA-256을 기록한다. 공식 소스의 비대상 문서 필터링은 정상 동작이므로 raw와 ACK가
같을 필요는 없다. 대신 `raw_count = filtered_out_count + accepted_count`와
`acknowledged_count = accepted_count`가 반드시 성립해야 한다. 30개 이상 window가
중복·공백·겹침 없이 이어져야
하며 요약 건수와 정확히 일치해야 한다. 시작과 끝은 timezone이 있는
ISO-8601이어야 하고, 마지막 완료 시각이 증빙 기준 시각의 24시간 이내이며 실패
창이 0개여야 한다. 예약 수집의 최초 2일 bootstrap은 운영 복구용 안전장치일 뿐
Alpha 전환 증빙으로 인정하지 않는다.

운영 DB exporter는 `content-integrity.json`에 다음 원시 건수를 산출한다.

- `title_provenance_labeled_count / public_event_count = 100%`
- `source_title_preserved_count / source_title_event_count = 100%`
- `generated_metadata_title_count` — SEC daily index처럼 소스가 제목을 제공하지 않아
  공시 메타데이터로 만든 표제의 정보성 건수
- `operator_metadata_title_count` — 허용된 공식 링크에 사람이 입력한 표제
- `unknown_title_provenance_count = 0`

공개 `title_provenance`은 `source`, `generated_metadata`,
`operator_metadata` 중 하나다. 세 분류의 합은 labeled count와, labeled count와
unknown count의 합은 공개 사건 분모와 정확히 일치해야 한다. 생성 표제와
운영자가 등록한 링크 표제를 원문 제목으로 주장해서는 안 된다.
