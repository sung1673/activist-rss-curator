# 운영 자동화 / Operations automation

이 문서는 BSIDE 거버넌스 인텔리전스의 GitHub Actions 운영 계약을 설명한다. 모든 생성 HTML, `state.json`, 아카이브는 더 이상 `main` 브랜치에 커밋하지 않는다. 운영 데이터는 MySQL을 기준으로 읽고 쓰며, 공개 페이지는 GitHub Pages artifact로만 배포한다.

## 워크플로

| 파일 | 역할 | 주기 |
|---|---|---|
| `ci.yml` | Python 테스트, PHP 구문 검사, 린트·타입·의존성 보안 검사 | PR, `main` 코드 push |
| `ingest-official.yml` | DART 공식 공시 자동 수집. `KIND_CONNECTOR_MODE=active`에서만 KIND도 같은 예약 실행에 포함 | KST 07:00~23:45 15분, KST 00:00~06:30 30분 |
| `ingest-global.yml` | SEC EDGAR Latest Filings Atom 당일 증분과 일일 인덱스를 대조해 v2 비공개 검수 큐에 적재 | 매시 17분·47분 |
| `ingest-selected-markets.yml` | 캐나다·호주 수동 승인 공식 링크 metadata를 URL 요청·본문 저장 없이 `link-only`로 v2 검수 큐에 적재 | 매시 07분·37분 |
| `source-right-bootstrap.yml` | exact SHA·closed 상태를 확인한 뒤 DART·SEC와 승인된 CA/AU SourceRight를 보호 환경에서 등록 | 운영자 수동 실행만 |
| `global-brief.yml` | 사람 검수용 후보 artifact 생성, 승인된 동일 SHA payload 수동 발행 | KST 05:45 후보 생성, 발행은 수동 |
| `global-alpha-review-candidates.yml` | 실제 Preview API에서 무라벨 60사건·120문서쌍·Top 5 검수 artifact 생성 | 기본 브랜치에서 운영자 수동 실행 |
| `global-alpha-preview-smoke.yml` | 실제 PHP v2·운영 DB의 Today·사건·발행사·검색·캘린더와 3개 viewport 증빙 검증 | Preview 배포 뒤 운영자 수동 실행 |
| `global-alpha-watchdog.yml` | `GLOBAL_ALPHA_OBSERVATION_ENABLED=true`인 Production Alpha의 API·release state·source freshness·공개 루트 관측 | 5분 |
| `kind-adapter-preflight.yml` | 운영 SourceRight 확인 후 KIND adapter 실제 계약을 1회 검증 | 승인 후 운영자 수동 실행만 |
| `ingest-media.yml` | 허가된 Telegram·뉴스 발견 큐 수집 | 30분 |
| `resolve-links.yml` | Google News 발견 URL 후처리 | 1시간 |
| `publish.yml` | Telegram outbound 영구 비활성 정책을 fail-closed로 검증 | 수동 검증만 허용, 발송 job은 실행하지 않음 |
| `daily.yml` | 일일 페이지 생성·Pages 배포와 06:05 무발송 정책 검증 | KST 05:45, 06:05 |
| `watchdog.yml` | 수집 최신성·outbox·dead letter 감시 | 5분 |
| `web-vitals.yml` | 모바일 Chromium으로 4개 governance SPA 경로의 LCP·INP·CLS 실측·적재 | KST 23:00 |
| `pages-deployment-incident.yml` | Pages 최종 검증 실패·회복 이슈 조정 | Pages workflow 완료 직후 |
| `repair-telegram-history.yml` | MySQL 상태를 먼저 복원한 뒤 허가 채널 이력을 멱등 백필 | 운영자 수동 실행만 |
| `release-gate.yml` | production 증빙 artifact의 14일 shadow·7일 운영·성능·benchmark 전환 판정 | 운영자 수동 실행 |
| `governance-cutover.yml` | 증빙에 고정된 exact daily Pages artifact·현재 SourceRight를 확인한 보호된 공개 전환 | 운영자 수동 실행 |
| `governance-rollback.yml` | API를 먼저 닫고 고정 legacy artifact를 복구하는 보호된 롤백 | 운영자 수동 실행 |

`ci.yml`의 테스트와 품질 job은 모두 필수다. 린트, 신규 거버넌스 핵심 모듈 타입 검사, `requirements.txt` 의존성 취약점 감사 중 하나라도 실패하면 CI가 실패한다. 기존 수집기 전체에 일괄 예외를 두지 않고 typed-core 범위를 점진적으로 넓힌다.

공식 JavaScript action은 GitHub-hosted runner의 Node.js 24 계열 major를 사용한다. `checkout@v7`, `setup-python@v7`, `setup-node@v7`, Pages action v5/v6, artifact action v7/v8, `github-script@v9`보다 오래된 Node.js 20 계열 major를 새 workflow에 추가하지 않는다.

GitHub cron은 UTC로 해석된다. 일일 생성은 `45 20 * * *`(KST 05:45), 발송은 `5 21 * * *`(KST 06:05)이다. GitHub Actions 예약 실행은 지연될 수 있으므로 애플리케이션은 실행 시각이 아니라 DB cursor와 idempotency key를 기준으로 처리해야 한다.

모바일 운영 성능 수집은 KST 23:00에 시작해 다음 날 KST 00:35 evidence input 수집 전에 완료한다. `/today`, `/events`, `/issuers`, `/calendar`를 각각 5회 실제 Chromium journey로 측정하며, 상세 fail-closed 계약은 [운영 모바일 Web Vitals 수집](web-vitals-production-probe.md)을 따른다.

배포 workflow는 Pages/API 결과를 `/api/v1/ops/web-distribution-observations`에 최대 50건씩 적재한다. full 40자 build SHA와 GitHub run ID·run attempt·target 조합을 사용하고, 같은 조합의 재전송만 멱등이다. 실패 관측에는 실제 탐지 시각을 기록하며 시도 0건을 성공으로 계산하지 않는다. KIND 지연은 공시 문서의 실제 `published_at`과 최초 `EventObservation.first_observed_at`이 모두 있는 경우에만 서버가 계산한다. 날짜만 있는 자료에 자정을 합성하지 않는다. 실제 관측이 0건인 무공시일만 표본 0·지연 `null`의 N/A를 허용하고, 최근 7일 창 전체에는 실제 KIND 관측과 완전한 시각 표본이 1건 이상 있어야 한다.

## 필수 설정

운영 Secret:

- `ACTIVIST_API_URL`, `ACTIVIST_API_SECRET`: 서명된 운영 API
- `OPENDART_API_KEYS`: 보호된 `governance-runtime` 환경에 등록하는 OpenDART
  키 pool. 줄바꿈 또는 쉼표로 구분한 중복 없는 소문자 40자리 hex만 허용한다.
  GitHub Actions는 collector 실행 전에 각 키를 개별 mask한다.
- `DART_API_KEY`: `OPENDART_API_KEYS`가 없는 기존 단일 키 환경에서만 사용하는
  호환 fallback. pool과 동시에 주입하지 않는다.
- `KIND_API_KEY`: 승인 직후 수동 adapter preflight에서는 필수. 일반 예약 수집에서는 `KIND_CONNECTOR_MODE=active`이고 승인된 adapter가 인증을 요구할 때만 사용
- `CURATOR_FEEDS`: 비공개 보조 발견 피드. 운영 범위 정책이 켜져 있으므로 단순 URL 문자열이 아니라 `name`, `url`, `scope`, `enabled`를 담은 JSON 배열로 등록한다. 세부 형식은 [미디어 발견 피드 범위 정책](media-source-scope-policy.md)을 따른다.
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`: 등록하지 않는다. 과거 값이 남아 있으면 삭제하며, 현재 코드와 workflow에서는 outbound Telegram을 재활성화할 수 없다.
- `TELEGRAM_API_ID`, `TELEGRAM_API_HASH`, `TELEGRAM_SESSION_STRING`: 허가 채널 수집
- `BSIDE_API_BASE_URL`, `BSIDE_OPS_TOKEN`: v1 운영 관측, v2 공식 source ingest와 5분 Production Alpha watchdog. Watchdog의 release state 조회는 읽기 전용 `GET /api/v2/ops/release-state`만 사용
- `BSIDE_ADMIN_TOKEN`: `/api/v1|v2/admin/release-state`의 preview·긴급 차단과, 보호된 승인을 소비하는 `/api/v2/admin/cutover`에만 사용하는 32바이트 이상 token. Watchdog에는 제공하지 않으며 PHP에는 평문 대신 SHA-256을 admin 역할 hash로 등록
- `BSIDE_RELEASE_AUTHORIZER_TOKEN`: 보호된 `governance-release` environment에만 등록하는 32바이트 이상 token. 정확한 `release_authorizer` 역할로 `/api/v2/admin/release-authorizations`만 호출하며, 일반 repository/environment나 watchdog에는 제공하지 않는다. PHP에는 평문 대신 SHA-256을 별도 역할 hash로 등록
- `BSIDE_EDITOR_TOKEN`: 사건·캠페인·정정·shadow discrepancy 검수와 v2 brief 후보·발행에 사용하는 32바이트 이상 token. PHP에는 평문 대신 SHA-256을 editor 역할 hash로 등록
- `GOVERNANCE_PREVIEW_TOKEN`: 비공개 preview 조회용 32바이트 이상 token. PHP에는 평문 대신 `governance_preview_token_hash` SHA-256만 등록하고 Pages artifact·URL query에는 포함하지 않음
- `STORY_REVIEW_ACCESS_TOKEN`, `TELEGRAM_ADMIN_ACCESS_TOKEN`: 명시적으로 생성·등록하는 편집 검수 token. Telegram 메시지나 URL에는 넣지 않고 관리자가 고정된 관리자 URL에서 직접 입력

Repository variable의 단일 기준:

- `PAGES_OWNER=legacy|governance`: 한 번에 하나의 Pages 소유자만 선택
- `GOVERNANCE_PIPELINE_MODE=off|dart_canary|shadow|live`: 공식 수집과 후보 파이프라인 단계
- `KIND_CONNECTOR_MODE=off|active`: 기본 `off`. `off`에서는 `shadow|live` 예약 실행도 DART만 수집하고 일반 watchdog도 KIND 설정·freshness를 요구하지 않는다. `active`에서는 기존처럼 KIND endpoint·SourceRight·수집 최신성을 fail-closed로 요구한다. 수동 `include_kind=true`와 `kind-adapter-preflight.yml`은 이 예약 토글과 별개인 명시적 검증 경로다.
- `GLOBAL_ALPHA_OBSERVATION_ENABLED=false|true`: 기본 `false`. `false` 또는 빈 값에서는 5분 Alpha 관측 job을 만들지 않는다. 소스·preview·동일 SHA가 준비된 뒤 `true`로 바꾸며, 다른 값은 관측 run을 실패시킨다.
- `ENABLE_TELEGRAM_DELIVERY=false`, `ENABLE_GOVERNANCE_DELIVERY=false`: 영구 비활성. `true`이면 workflow가 fail-closed
- `ACTIVIST_PUBLIC_API_URL`: 브라우저에서 읽는 공개 API URL
- `GOVERNANCE_API_BASE_URL`: 공개 거버넌스 UI의 `/api/v1` 기준 URL. 비어 있으면 `ACTIVIST_PUBLIC_API_URL` 뒤에 `/api/v1`을 붙여 사용
- `BSIDE_PUBLIC_WEB_URL`: Production Alpha watchdog과 전환 smoke가 확인할 공개 루트. 기본값은 `https://news.bside.ai`
- `SEC_EDGAR_USER_AGENT`: SEC 정책에 맞는 서비스명과 실제 연락 가능한 이메일을 포함한 User-Agent. SEC 공개 EDGAR 수집은 별도 API key를 사용하지 않는다.
- `CA_OFFICIAL_LINKS_JSON`, `AU_OFFICIAL_LINKS_JSON`: 승인된 캐나다·호주 수동 공식 링크 metadata의 닫힌 JSON object. 아래 Production Alpha 계약을 따른다.
- `KIND_DISCLOSURE_ENDPOINT`: 이 저장소가 정의한 JSON·pagination 계약을 충족하는 검증된 KIND 어댑터 URL. 일반 KIND HTML 화면이나 임의 자리표시자 URL을 넣지 않으며, 값이 없거나 계약 검증에 실패하면 공식 수집 workflow가 fail-closed로 종료

## 글로벌 터미널 Production Alpha

`/api/v2`와 신규 데이터 터미널은 국가별 접근권 차이를 공개하는 Production Alpha다. 미국 공식 수집과 캐나다·호주 `link-only / manual-metadata` 검증은 `GOVERNANCE_PIPELINE_MODE=shadow|live`에서만 예약 실행된다. 일본·영국은 화면의 국가 범위를 유지하지만 `link-only`, `coverage_unavailable`, `public_ready=false`로 고정하며 EDINET·Companies House HTML/API를 요청하지 않는다. OpenDART는 기존 `ingest-official.yml` 경로를 계속 사용하며 `ingest-global.yml`로 중복 적재하지 않는다.

API v2 배포 manifest의 필수 파일에는 `migrations/011_global_terminal_v2.sql`도 포함된다. 운영 적용 시 이 파일을 binary/byte-preserving 방식으로 전송하고, 원본 바이트의 SHA-256을 계산해 같은 MySQL 연결의 같은 입력 stream에서 `SET @bside_migration_011_sha256 = '<64자리 소문자 SHA-256>';` 다음에 SQL 파일을 그대로 보낸다. 별도 연결에서 `SET`하거나 파일을 변환한 뒤 적용하지 않는다. 적용된 DB의 version 11 checksum, 배포 manifest의 SQL hash, 서버 파일의 실제 hash 중 하나라도 다르면 v2 schema gate가 503으로 차단한다. 정확한 명령과 원자 배포·롤백 순서는 [v2 API 운영 계약](governance-api-v2.md)을 따른다.

OpenDART credential pool 원장은
`migrations/012_dart_credential_pool.sql`로 추가한다. 이 파일도 manifest에
포함하며 exact bytes의 SHA-256을 같은 MySQL 세션의
`@bside_migration_012_sha256`에 설정한 뒤 적용한다. 운영 적용과 멱등 replay는
`python scripts/apply_migration_012.py ...`를 사용한다. DB version 12,
manifest hash, 서버 파일 hash 중 하나라도 다르면 schema gate는 fail-closed한다.

`ingest-global.yml`에서 `from_date`와 `to_date`가 모두 비어 있으면 SEC connector의 MySQL durable checkpoint를 읽어 완료된 끝 날짜의 하루 전부터 겹쳐 수집한다. 한 번의 half-open window는 최대 31일이며 오래된 누락 구간부터 순차 처리해 장애 기간을 건너뛰지 않는다. checkpoint가 아직 없으면 최근 완료 2일을 사용한다. SEC connector는 같은 실행에서 공식 Latest Filings Atom을 page budget 안에서 newest-first로 읽고 90분 overlap이 있는 source cursor를 checkpoint schema v2에 함께 저장한다. Atom은 매 실행 요청하되 completed-day index는 America/New_York 06:00 전까지 직전 날짜를 완료 범위에 넣지 않아 SEC 야간 생성 지연과 분리한다. Atom과 completed-day index 중 하나라도 실패하거나 Atom cursor가 없으면 US 공개 상태는 `delayed`, `live_ready=false`이며 45분 출시 gate를 통과하지 못한다. 일일 인덱스는 역사적 완결성 대조용이고 Atom source title·acceptance time·filing index URL이 중복 accession의 우선 관측이다. 두 날짜를 모두 지정한 수동 실행도 SEC source cursor 확인을 위해 checkpoint를 읽으며, 그 외 source는 지정 범위만 처리한다. 한쪽 날짜만 입력하면 fail-closed한다. batch ID는 code revision을 포함해 배포 SHA별로 분리한다. 각 receipt는 batch ID·chunk 순번·전체 chunk 수·window·chunk 요청 수·전체 batch 합계를 저장하고 `(connector_id, batch_id, chunk_index)` 중복을 거절한다. 서버는 1번부터 순서대로만 받아 final에서 1..N 완전성, 동일 revision·window·합계, chunk 합계의 정확한 일치를 확인한 뒤 checkpoint를 전진시킨다. final 선행·순서 역전·metadata/합계 불일치는 HTTP 409다.

Migration 011이 만드는 비한국 SourceRight는 모두 `pending`이다. 필수 4개 권한(KR·US·CA·AU)은 증빙 참조 또는 SHA-256, 비어 있지 않은 permission scope, 유효기간과 철회 상태를 갖춘 현재 레코드인지 확인해야 한다. JP·GB row는 선택적 dormant identity이며 Alpha 권한 승인이나 수집 대상으로 계산하지 않는다.

| 국가 | SourceRight | source type / key | 실제 Production Alpha 범위 |
|---|---|---|---|
| KR | `official:dart` | `official_disclosure / dart` | 기존 OpenDART 공식 수집 |
| US | `official:sec-edgar` | `official_disclosure / sec-edgar` | SEC 일일 인덱스의 허용 form |
| JP | `official:edinet` | `official_disclosure / edinet` | 정책 비활성, `coverage_unavailable`, 요청 0건 |
| GB | `official:companies-house` | `official_register / companies-house` | 정책 비활성, `coverage_unavailable`, 요청 0건 |
| CA | `official:ca-issuer-ir` | `official_issuer / issuer-ir` | issuer 식별자·호스트 증빙에 묶인 수동 IR 링크 metadata, `link-only`, 최대 50개 issuer |
| AU | `official:asic-register` | `official_register / asic-register` | `asic.gov.au` 공식 호스트의 수동 등록부 링크 metadata, `link-only`, 최대 50개 issuer |

SourceRight 증빙을 등록한 뒤 admin은 `GET /api/v2/admin/connectors/{connector_id}`에서 `collect_eligibility.eligible=true`와 `identity_match=true`를 먼저 확인한다. 그다음 `POST /api/v2/admin/connectors/{connector_id}`에 `target_status=configured`, 직전 `expected_updated_at`, 8~1,000자 `reason`을 보낸다. 권한·source identity 불일치는 `409 connector_source_right_ineligible`, 경쟁 변경은 `409 stale_connector_update`로 중단한다. 긴급 중지는 같은 API의 `target_status=inactive`이며 SourceRight 상태와 무관하게 허용된다. 같은 상태 재요청도 포함해 모든 POST는 `activist_global_connector_audit`에 이전·신규 상태, 사유, admin 역할, 시각을 기록한다. 등록된 `source_right_id`의 `source_type`·`source_key`는 불변이며, 변경 요청은 `409 source_right_identity_immutable`로 거절한다. 새로운 source identity에는 새로운 `source_right_id`를 발급해야 한다. v2 공개 문서, `/sources/status`, 보호된 원자적 preview→live 검사는 모두 문서 또는 connector의 source identity와 grant identity를 대소문자까지 정확히 비교하며, 불일치는 각각 비공개·`blocked_identity`·전환 거절로 처리한다.

수집기는 네트워크 요청 전과 각 페이지 전에 `collect` 자격과 rights revision을 다시 확인한다. 공개 사건 검수와 보호된 원자적 `preview → live` 전환은 현재 재배포 자격을 별도로 확인한다. 캐나다·호주 수동 링크 경로는 record마다 권한 API를 호출하지 않는다. SourceRight 단위 batch 앞뒤에서 `collect`와 `public` ACK를 한 번씩 확인하고 네 ACK의 revision이 모두 같아야 한다.

보호된 cutover는 공개 문서에 연결된 권한만 보지 않는다. 위 표의 필수 4개 connector(KR·US·CA·AU)와 정확히 대응하는 SourceRight를 같은 transaction에서 모두 검증하되, SourceRight row 전체를 ID순으로 먼저 잠그고 connector row 전체를 ID순으로 다음에 `FOR UPDATE`로 잠근다. 수집·connector 관리와 같은 SourceRight→connector 순서를 사용해 교차 transaction deadlock을 막는다. connector의 국가·source key/type·SourceRight ID·coverage mode가 서버 registry와 정확히 일치하고 상태가 `active`인지, 마지막 성공·확인 시각이 `min(45분, max(15분, 실행 주기의 3배))` 이내이고 현재 오류가 없는지 확인한다. SEC는 `sec-current-v1` cursor 내부 UTC `updated_at`도 같은 한도 안이어야 하며, CA/AU `link-only`는 최근 관측과 1건 이상의 raw/ACK가 필요하다. 권한은 `active`·증빙 보유·현재 유효·미철회·수집 가능·공개 재배포 가능이어야 한다. CA/AU처럼 현재 공개 문서가 0건이어도 최신성·만료·철회·identity 변경은 HTTP 409 `required_alpha_sources_invalid`로 전환을 중단한다. JP·GB connector row와 대응 SourceRight도 같은 순서로 잠그며 required freshness·권한 gate에서는 제외하되 exact identity가 정책상 dormant 값과 일치하는지 검증한다. 이 경우 두 release state는 preview에 남고 일회용 승인은 소비되지 않는다. 필수 4개 검사를 통과한 뒤에도 기존 v1·v2 공개 문서 권한 guard를 별도로 유지한다.

`CA_OFFICIAL_LINKS_JSON`과 `AU_OFFICIAL_LINKS_JSON`은 최상위 필드가 정확히 `schema_version`, `approved_hosts`, `records`인 닫힌 JSON object다. `schema_version=1`이고 국가별 최대 50개 issuer·50개 승인 호스트 mapping·500개 record를 허용한다. `approved_hosts` 항목은 `hostname`, `issuer_identifier_type`, `issuer_identifier`, `evidence_sha256`만 가지며 64자리 소문자 SHA-256 증빙을 issuer와 호스트에 고정한다. `records` 필수 필드는 `country_code`, `issuer_identifier_type`, `issuer_identifier`, `issuer_name`, `source_right_id`, `official_host`, `original_url`, `title`, `original_language`, `filed_at`, `first_observed_at`, `event_family`이다. `issuer_namespace`, `market`, `ticker`, `external_id`, `document_type`만 선택적으로 허용한다. record의 `official_host`와 issuer 식별자는 승인 호스트 항목과 정확히 일치해야 하며 사용되지 않은 승인 호스트도 거절한다.

Bootstrap은 이 JSON 전체를 키 정렬·공백 제거 방식으로 canonicalize한 SHA-256을 SourceRight `evidence_hash`에 저장한다. 예약 수집도 같은 canonical digest를 `source_manifest_sha256`으로 전송하며, 서버는 두 값을 상수시간 비교한다. 따라서 Repository variable이 승인 후 바뀌면 새 사람 승인과 bootstrap 없이는 수집이 실패한다.

캐나다는 issuer가 통제하는 IR 호스트별 증빙이 필요하고 하나의 호스트를 서로 다른 issuer에 공유 승인할 수 없다. SEDAR+·ASX·ASIC·data.gov 및 알려진 제3자 포털도 허용하지 않는다. 호주는 `asic.gov.au`와 그 하위 공식 호스트만 허용하며 ASX·data.gov·issuer·제3자 호스트를 허용하지 않는다. 모든 URL query를 기본 거절하고 `x-amz-*`, `x-goog-*`, token·key·signature·credential 계열은 자격정보 오류로 별도 차단한다. timestamp 순서는 timezone을 적용한 실제 UTC 시각으로 비교한다.

허용 사건 유형은 migration 011 SourceCoverage와 동일하다. 캐나다는 `meeting_and_vote`, `tender_offer_and_mna`, `capital_return`, `board_and_compensation`, 호주는 `board_and_compensation`, `listing_status`만 허용한다. 이 workflow는 설정된 `original_url`을 요청하지 않고 본문도 저장하지 않는다. SEDAR+·ASX 전문 수집기로 해석하면 안 된다. 닫힌 빈 object 또는 미설정은 사건 0건 성공으로 처리하지 않고 secret-free artifact에 `coverage_unavailable`을 기록한다.

`global-brief.yml`의 예약 작업은 후보 bundle만 30일 보존하며 공개 API에 쓰지 않는다. 사람 1명이 후보의 Top 5와 근거를 승인한 뒤 동일 SHA의 JSON 또는 base64 payload로 `publish` 작업을 수동 실행해야 한다. 발행 receipt는 90일 보존한다.

Production Alpha의 배포·관측 분모는 Pages/API뿐이다. Telegram outbound는 제외가 아니라 영구 비활성 정책이며 `ENABLE_TELEGRAM_DELIVERY=false`, `ENABLE_GOVERNANCE_DELIVERY=false`, `telegram.enabled=false`, 빈 `telegram.chat_id`를 유지한다. Telegram 자격정보는 허가 채널의 내부 읽기 수집에만 사용한다.

아래 기존 boolean은 90일 전환 기간의 입력 어댑터일 뿐 새 운영값으로 사용하지 않는다. 새 값과 충돌하면 항상 fail-closed한다.

- `ENABLE_LEGACY_PIPELINE=true`: 90일 호환 기간 동안 기존 수집·Pages workflow 유지
- `ENABLE_PAGES=true`: 기존 workflow의 Pages artifact 배포 유지
- `GOVERNANCE_PIPELINE_MODE=off`: 신규 governance 예약 실행 차단. DART canary는 `dart_canary`, 비교 운영은 `shadow`, 공개 후에는 `live`. KIND는 이 단계와 분리된 `KIND_CONNECTOR_MODE`로만 예약 활성화
- `PAGES_OWNER=legacy`: 기존 Pages만 배포. 보호된 전환 직전 `governance`로 바꾸며 workflow가 직접 Repository Variables를 수정하지 않음

거버넌스 v1 공개 데이터 경로는 repository variable이 아니라 MySQL의 서버 측 release state로 최종 제어한다. migration 001~010을 순서대로 적용한 요구 schema version은 10이며, 공개 상태는 006이 만든 `closed`를 유지한다. 서버는 최댓값이 아니라 1~10의 정확한 버전·이름·체크섬 manifest를 검증한다. DART guarded write는 실행 모드와 공개 상태를 `dart_canary → closed`, `shadow → preview`, `live → live`로 묶는다. collector가 서명한 `expected_release_state`와 transaction 안에서 잠근 v1·v2 상태가 서로 정확히 같을 때만 적재하며 `off`, 빈 값, `manual` 및 상태 경쟁 변경은 mutation 없이 실패한다. 검수자는 관리자가 `preview`로 전환한 동안에만 preview Bearer token으로 접근한다. v1·v2 `POST /admin/release-state`는 `preview → live`를 409 `protected_atomic_cutover_required`로 거절한다. `live` 승격은 아래 보호된 v2 원자 전환만 사용하고, `live → closed` 긴급 차단은 각 관리자 API의 optimistic version과 감사 로그를 사용한다.

### 보호된 원자적 공개 전환

`governance-cutover.yml`의 실제 승격 job은 reviewer가 있는 `governance-release` environment에서만 실행한다. 이 environment에만 `BSIDE_RELEASE_AUTHORIZER_TOKEN`을 두고, repository 수준이나 `governance-runtime`, watchdog job에는 복사하지 않는다. PHP의 `release_authorizer` hash에도 이 토큰 하나만 등록한다. 일반 `BSIDE_ADMIN_TOKEN`은 승인 발급 endpoint를 통과할 수 없으므로 environment 승인 없이 직접 공개할 수 없다.

Alpha evidence workflow는 exact `daily.yml` run의 `pages-<run_id>-<attempt>` artifact를 다운로드해 run·attempt·artifact ID·이름·GitHub digest, 모든 정규 파일의 전체 사이트 content digest, root와 `/governance`에서 byte-identical인 `index.html`·`config.js`·`app.js`·`styles.css` terminal identity를 고정한다. 5분 watchdog은 preview에서 같은 네 파일의 원본 UTF-8 바이트를 매번 계산하며, 24시간 전 관측이 해당 terminal identity와 모두 같아야 한다. `daily.yml`과 `build-feed.yml`은 동일한 `BSIDE_PUBLIC_WEB_URL` 값을 UI 구성에 사용한다.

전환 workflow는 최근 48시간 이내 동일 SHA release-evidence artifact를 digest까지 검증하되, 실제 전환에는 evidence run 생성 시각·보고서 `evidence_as_of`·관측 종료 시각이 모두 현재로부터 60분 이내인 보고서만 허용한다. evidence에 포함된 exact Pages artifact만 GitHub API에서 다시 찾아 전체 사이트·terminal identity를 재계산한다. cutover dispatch는 별도 Pages run/name을 받지 않으므로 같은 SHA의 다른 artifact를 선택할 수 없다. v1·v2를 모두 preview로 확인하고 `/api/v2/sources/status`의 필수 4개 connector가 모두 `public_ready=true`, JP·GB가 정책상 unavailable identity인지 검증한 뒤 다음 순서로 진행한다.

1. 32바이트 난수 nonce를 생성해 GitHub log mask와 job environment에만 보관한다. nonce를 artifact, URL, 출력 또는 API 응답에 남기지 않는다.
2. `BSIDE_RELEASE_AUTHORIZER_TOKEN`으로 `POST /api/v2/admin/release-authorizations`를 호출해 candidate SHA, `sha256:` evidence artifact digest·run ID·artifact ID, v1·v2 현재 state version, 서버 시각 기준 60~900초 expiry를 한 승인 row에 묶는다. 서버에는 nonce SHA-256만 저장한다.
3. `BSIDE_ADMIN_TOKEN`으로 `POST /api/v2/admin/cutover`를 한 번 호출한다. 서버는 승인과 배포 SHA·digest·nonce·두 state version을 대조하고, v1·v2 상태, 필수 4개 connector·SourceRight, JP·GB dormant identity, 기존 공개 문서 SourceRight를 잠근 뒤 모든 권한 검사가 통과한 경우에만 두 상태를 하나의 transaction에서 `live`로 변경한다.
4. 두 audit row의 `release_authorization_id`, `cutover_at`, `sunset_at`과 응답의 두 state version이 일치할 때만 smoke test를 계속한다.

승인은 일회용이며 새 승인 발급 시 이전 미사용 승인은 철회된다. 만료는 410, replay·잘못된 SHA·digest·state version·이미 소비된 nonce는 409로 거절한다. 전환 도중 오류가 나면 두 상태 모두 바뀌지 않는다. 공개 뒤 장애가 발생하면 기존 절차대로 v1·v2를 `closed`로 긴급 차단하고 legacy artifact를 복구한다. 이 롤백은 소비된 승인을 되살리지 않으며 재전환에는 새 증빙·새 nonce·새 승인이 필요하다.

예약·수동 `ingest-official`과 수동 `official-backfill`은 repository variable
`DART_OFFICIAL_INGEST_ENABLED`가 정확히 `true`일 때만 실행된다. 값이 없거나
`false`이면 job 시작 전에 skip되며, 수동 입력으로 이 게이트를 우회할 수 없다.
평상시에는 검증된 schema 12 PHP·MySQL 조합에서만 `true`를 유지한다.
`KIND_CONNECTOR_MODE=off`가 기본이므로 `shadow|live`에서도 DART-only로
실행된다. `active`로 전환하면 KIND 설정·SourceRight·수집 실패를 건너뛰지 않고
기존처럼 전체 workflow를 실패시킨다. 수동 실행의 `include_kind=true`는 토글이
`off`여도 KIND를 명시적으로 검증하며, `include_kind=false`는 DART-only smoke다.

예약·수동 `ingest-official`, `official-backfill`, `ingest-global`,
`global-backfill`, `ingest-official-sites`, `ingest-selected-markets`,
official slot epoch reset, SourceRight bootstrap, 사람 승인 global brief publish,
보호된 cutover는
`governance-production-official-write-${repository}-${ref}` 하나의
non-cancelling queue를 공유한다. 운영 기본 branch에서는 공식 소스의 다중 청크
실행·공개 brief 쓰기와 release-state 변경이 절대 겹치지 않는다. Global brief
candidate 생성은 읽기 전용이므로 기존 별도 queue를 유지하고 publish job만 이
경계에 참여한다.

긴급 rollback은 6시간 백필을 기다리지 않는 별도 non-cancelling queue를 사용한다.
입력과 exact 기본 branch를 확인한 직후 artifact 준비보다 먼저 optimistic retry로
v2→v1을 closed로 만들며, Pages lock 안에서 다시 닫은 뒤 legacy를 복원한다.
cutover와 rollback의 실제 artifact 배포는 모두 같은 Pages deployment lock 안에서
실행된다. cutover는 upload 직전 v1·v2가 preflight 때의 정확한 preview version인지
다시 확인한다. 모든 보호 writer와 transition은 `github.ref_type=branch`와 exact
`refs/heads/<default>`를 확인한다. lock identity에도 전체 `github.ref`를 포함하므로
같은 짧은 이름의 tag나 다른 branch가 운영 기본 branch로 오인되지 않는다.

OpenDART credential pool과 schema 12를 배포할 때는 다음 순서를 바꾸지 않는다.

1. `DART_OFFICIAL_INGEST_ENABLED=false`로 설정하고 `ingest-official`과
   `official-backfill`의 queued·running run이 모두 0인지 확인한다.
2. schema 11 DB를 명시적으로 허용하는 pending-schema-upgrade 모드로 새 PHP bundle을
   먼저 배포한다.
3. migration 012를 같은 세션의 exact checksum으로 apply·replay한다.
4. schema 12, 배포 SHA, OpenAPI, closed 응답을 요구하는 exact smoke를 통과시킨다.
5. 모든 검증이 끝난 뒤에만 `DART_OFFICIAL_INGEST_ENABLED=true`로 되돌린다.

중간 단계가 실패하면 게이트를 `false`로 유지한다. migration 012 이후에는 구 PHP
파일만 단독 복원하지 않으며, 기존 PHP로 되돌려야 한다면 writer가 정지된 상태에서
사전 DB 백업을 먼저 복원한다.

모든 DART list·회사 master HTTP 시도는 전역 MySQL 쿼터 ledger의 `POST /api/v1/ops/dart-quota` consume ACK를 먼저 받아야 한다. 서버 기준 KST 날짜의 모든 키 합산 한도는 40,000건이고 단일 실행의 안전 예산은 10,000건이다. 서버는 commit 성공 뒤 기존 transaction과 다른 fresh PDO connection으로 exact attempt와 credential 상태를 readback한 경우에만 200을 반환한다. 클라이언트는 첫 ACK의 `used_count + remaining_count = limit_count`, 비밀이 아닌 credential SHA-256 identity, attempt ID와 backend binding을 검증한 다음 exact POST를 별도로 replay한다. 이 두 번째 응답이 `duplicate=true`가 아니면 외부 요청을 보내지 않는다. 첫 ACK가 transport retry 결과 duplicate여도 별도 replay는 필수이며 논리 consume의 사용량은 1만 증가해야 한다. OpenDART `020`과 `901`의 `block_020`·`disable_901`도 같은 duplicate replay를 통과한 뒤에만 다른 유효 키로 계속한다. commit/readback 실패는 503 `dart_quota_persistence_failed`와 고정된 일곱 `transaction_*` detail 중 하나로만 진단하며, 임의 exception message나 credential material을 응답·로그 artifact에 복사하지 않는다. 사용하지 않은 예약량을 반환하거나 완료 처리하는 별도 모델은 두지 않는다. 키 원문과 키가 든 URL·응답 본문은 로그·checkpoint·artifact에 기록하지 않는다.

공식 수집 schedule 증빙은 `workflow_run.created_at`을 slot으로 내림하지 않는다. 예약·수동 repair run은 수집 전에 `/api/v1/ops/official-slot-claims`에서 MySQL의 가장 오래된 due/unclaimed slot을 원자적으로 claim하고, `trigger_created_at`은 불변 GitHub run 출처로만 저장한다. 첫 접촉은 다음 완전한 KST 날짜의 00:00 경계를 활성화하고 해당 run에 slot을 부여하지 않는다. repair는 세 cron family 전체의 전역 최고 due slot을 정확히 지정할 때만 허용한다. claim-time `late`는 불변이고, 다음 cadence 경계 후 완료·재실행은 `terminal_reason`이 있는 영구 실패로 남는다. 완료된 claim의 재실행은 외부 poll과 DB row 수정 없이 검증된 no-op로 종료한다. 각 소스는 `source_key`에 실제 선택됐고, 0건인 날도 명시적 0/0 ACK와 top-level/source raw·ACK 일치를 모두 만족해야 성공 denominator에 들어간다.

2026-07-22 timeout 보강본의 무배포·무발송 safe-full과 후속 Pages 전용 배포가 모두 성공했다. 현재 값은 `ENABLE_LEGACY_PIPELINE=true`, `ENABLE_PAGES=true`, `ENABLE_TELEGRAM_DELIVERY=false`, 세 거버넌스 전환 플래그 `false`다. 기존 읽기 수집과 Pages만 재개했으며 Telegram 발송은 현재 제품 범위에서 승인하지 않는다.

`PAGES_OWNER`가 Pages 배포의 단일 결정값이다. 전환 기간에는 `ENABLE_PAGES`·`ENABLE_GOVERNANCE_PAGES`를 어댑터로 읽지만 선택된 owner와 충돌하면 legacy와 governance workflow가 모두 fail-closed한다. 코드/API만 바뀐 push에서 생성 단계가 하나도 선택되지 않으면 legacy workflow도 Pages artifact를 배포하지 않는다.

정규 수집 job은 `CURATOR_DELIVERY_MODE=disabled`와 `CURATOR_DISABLE_TELEGRAM_SEND=1`로 고정하며 bot/chat Secret도 주입하지 않는다. 수동 smoke·resend·briefing 입력은 삭제했고, `publish.yml`은 예약·수집 연동 trigger 없이 literal `false`인 정책 검증 job만 유지한다. `ENABLE_TELEGRAM_DELIVERY` 또는 `ENABLE_GOVERNANCE_DELIVERY`가 `true`면 관련 workflow는 fail-closed로 종료한다. PHP도 `enqueue_delivery_outbox`와 `claim_delivery_outbox`를 DB 변경 전에 HTTP 410으로 거절하므로 workflow 변수만으로 발송을 되살릴 수 없다. 허가된 공개 채널의 읽기 수집은 별도 MTProto 자격증명을 사용하므로 내부 신호 분석용으로만 계속한다.

`build-feed.yml`의 수동 `full` 실행은 `allow_pages_deploy=false`가 기본값이며 outbound 발송 입력을 제공하지 않는다. 따라서 실제 MySQL 수집·동기화를 검증하면서도 Pages와 outbound Telegram을 건드리지 않는다. Build 단계는 45분을 넘으면 중단되며, 단계별 시간과 처리량은 `curator-run-metrics-*` artifact로 14일 보존한다.

Telegram 증분 수집은 채널별 한 페이지에서 durable checkpoint를 만들고 다음 예약 실행에서 이어 간다. 신규 메시지와 매치는 MySQL API의 정확한 건수 ACK가 확인된 뒤에만 DB cursor를 전진시키고 로컬 5,000건 제한을 적용한다. 각 원격 요청은 레코드 수뿐 아니라 실제 UTF-8 JSON 직렬화 크기를 측정해 1.75MB 이하로 동적 분할한다. 메시지 checkpoint에는 signal을 반복해서 싣지 않는다. 전체 signal 재구축은 결정적인 SHA-256 세대 토큰과 DB `live_revision` fence를 사용하고 최대 500개 단위로 staging 테이블에만 적재한다. snapshot 직전 revision과 begin 시점 revision이 다르면 재구축을 거부한다. staging 요청은 signal만 허용하며 메시지·매치·채널 identity 변경은 모두 별도의 revision 증가 transaction으로 처리한다. 활성 재구축 중에는 일반 live 입력을 거부하지만 각 staging batch가 heartbeat를 갱신하고, 기본 10분 lease가 만료되면 다음 정상 입력이나 새 begin이 stale staging을 정리해 자동 복구한다. finalize 요청은 같은 토큰의 staging 전체를 단일 트랜잭션에서 공개 테이블에 반영하고, 같은 72시간 범위의 누락 row를 삭제한 뒤 revision을 올린다. 응답 유실 후 같은 finalize를 재시도하면 `finalized_token`으로 멱등 ACK한다. 따라서 중간 실패, 동시 입력, 오래된 finalize는 현재 공개 signal을 부분적으로 바꾸거나 새 입력을 덮어쓰지 않는다. staging·finalize 응답의 처리 건수와 토큰 ACK가 정확히 일치하지 않으면 클라이언트도 실패로 처리한다. `repair-telegram-history.yml`은 과거 누락 의심 구간을 복구할 때만 수동 실행하며 outbound bot/chat Secret을 전달받지 않는다. 이 작업은 기본 브랜치와 `telegram-history-repair` environment로 제한하고, 일반 Telegram 수집과 같은 최대 100건 대기열에서 직렬 실행한다. 입력은 최대 365일·페이지당 3,000건·500개 채널·전체 300,000건으로 제한하며, 각 채널은 요청 기간의 시작까지 페이지를 계속 순회한다. 과거 prune-before-sync 장애가 DB cursor를 실제 저장 범위보다 앞당겼을 수 있으므로 복구 실행은 선택 채널을 강제 재동기화한다. 복구 전용 PHP 리소스와 revision protocol이 운영 서버에 배포됐는지 preflight로 먼저 확인하며, 지원되지 않으면 Telegram을 읽기 전에 즉시 실패한다. 각 페이지는 MySQL ACK가 확인된 뒤 다음 페이지로 넘어가며 metrics에 `telegram_repair_resume_before_message_id`를 저장한다. 전역 한도나 timeout으로 채널 중간에서 멈추면 `telegram_backfill_resume_handle`과 `telegram_backfill_resume_before_message_id`를 남겨 이미 ACK된 페이지보다 앞선 구간부터 재개한다. 제한 없는 전체 실행이 한 번에 완전 성공한 경우에만 최근 72시간 signal을 자동 재구축한다. `only_handles`, `channel_limit`, `start_after`, `before_message_id`를 사용한 부분 실행과 실패·절단 실행은 기존 signal을 파생·upsert·stage·finalize하지 않는다. 분할 복구는 모든 구간 artifact를 확인한 뒤 별도의 signal-only 최종화 실행에서 MySQL의 최근 72시간 메시지와 매치를 `posted_at` 기준으로 끝까지 다시 읽어 재계산한다.

복구 실행의 메시지 없는 페이지 완료 checkpoint도 hydrated `issue_signals`를 재전송하지 않는다. 중간 요청은 현재 checkpoint 채널 하나의 metadata와 durable cursor만 전송하고 채널 1건과 signal 0건 ACK를 정확히 확인한다. 부분·절단·실패 복구는 모든 선택 채널이 이미 page checkpoint에서 저장됐으므로 실행 말미에 전체 채널 metadata를 다시 보내지 않는다. 일반 metadata 갱신과 메시지 payload 모두 채널 identity를 최대 5개 단위의 별도 transaction으로 저장하고 signal payload를 분리해 PHP/MySQL 처리시간에 의한 timeout을 제한한다. signal은 전체 복구 창을 마지막으로 재계산한 뒤의 signal-only rebuild 요청에서만 전송한다. 메시지 batch는 POST 전에 각 메시지 채널 identity가 정확히 하나의 채널 snapshot에 대응하는지 확인하고, 응답의 메시지·매치·채널 건수가 모두 정확히 일치한 뒤에만 cursor를 전진시킨다.

### Telegram 이력 복구 운영 절차

1. 실행 전 GitHub repository의 `Settings → Environments`에 `telegram-history-repair` environment를 미리 만들고 deployment branch policy를 기본 브랜치 `main`만 허용한다. 이 environment가 없는 상태에서 workflow를 먼저 실행하지 않는다.
2. `ENABLE_LEGACY_PIPELINE=false`, `ENABLE_GOVERNANCE_SHADOW=false`로 일반 수집을 일시 중지하고 진행 중인 `telegram-collection-*` run이 없는지 확인한다. `ENABLE_TELEGRAM_DELIVERY=false`는 전 과정에서 유지한다.
3. 기본 브랜치에서 `Repair Telegram history`를 수동 실행한다. 실패나 timeout 후에는 임의로 DB cursor를 되돌리지 않는다. 채널 중간에서 멈춘 경우 metrics의 마지막 handle 하나만 `only_handles`에 넣고 `telegram_repair_resume_before_message_id`를 `before_message_id`로 전달한다. 해당 채널이 완료된 뒤 나머지는 `start_after`로 이어 간다. 두 재개 방식 모두 직전 metrics의 `telegram_backfill_selection_fingerprint`를 `expected_selection_fingerprint`로 함께 전달해야 한다. `before_message_id=0`으로 동일 채널을 처음부터 재시도하거나 새 실행에서 채널 universe 불변을 확인할 때는 유효한 fingerprint를 선택적 assertion으로 전달할 수 있다. fingerprint는 `only_handles`, `skip_handles`, `start_after`, `channel_limit` 적용 전의 수집 가능한 전체 채널 집합을 canonical handle·권위 있는 Telegram ID·명시적 정렬 버전으로 고정한다. 채널 추가·삭제·수집 가능 여부를 바꾸는 권한 변경·handle 변경·ID 변경이나 marker 부재·중복 marker가 감지되면 Telegram 호출 전에 fail-closed한다. handle-only 채널이 같은 실행에서 권위 있는 ID를 얻으면 checkpoint의 current fingerprint가 갱신되므로 다음 실행에는 `started` 값이 아니라 최신 `telegram_backfill_selection_fingerprint`를 전달한다. fingerprint 필드가 도입되기 전에 생성된 metrics로는 `start_after` 또는 `before_message_id>0` 재개를 할 수 없으며, 입력을 모두 비운 새 최초 실행부터 다시 시작한다. metrics가 이전 모든 채널의 `telegram_repair_remote_checkpoint_complete=1`을 입증할 때만 재개한다.
4. `telegram-repair-metrics-*` artifact에서 `ok=true`, `status=complete`, `telegram_channel_failed=0`, `telegram_remote_failed=0`, `telegram_remote_pending=0`, `telegram_remote_metadata_failed=0`, `telegram_backfill_truncated_channels=0`, `telegram_repair_remote_checkpoint_complete=1`을 모두 확인한다. 실패 시에는 `telegram_remote_last_error`, `telegram_remote_last_status_code`, `telegram_remote_max_request_bytes`도 함께 확인한다. 하나라도 완료 조건을 만족하지 않으면 복구 완료로 판정하지 않는다.
5. 분할 복구였다면 모든 artifact가 동일한 연속 체인을 이루는지 사람이 확인한 뒤 signal-only 최종화를 별도로 실행한다. `finalize_signal_rebuild=true`, `channel_limit=0`, 빈 `only_handles`, `before_message_id=0`, 현재 universe의 마지막 handle을 `start_after`, 마지막 artifact의 current fingerprint를 `expected_selection_fingerprint`로 전달한다. 이 zero-channel tail 검사는 현재 마지막 handle과 fingerprint만 검증하며 과거 모든 분할 구간의 합집합 완료를 코드로 증명하지는 않는다. 따라서 4단계의 모든 artifact 검증이 최종화의 필수 승인 근거다. 최종 metrics에서 `ok=true`, `status=complete`, `telegram_backfill_selected_count=0`, 기대 universe count·fingerprint 일치, `telegram_signal_rebuild_authorized=1`, `telegram_signal_rebuild_finalize_mode=1`, `telegram_signal_window_rebuilt=1`, `telegram_signal_rebuild_durable_complete=1`, `telegram_remote_failed=0`, `telegram_remote_pending=0`, `telegram_remote_metadata_failed=0`을 확인한다.
6. signal 최종화 성공 후 `ENABLE_PAGES=false`를 먼저 확인하고 `ENABLE_LEGACY_PIPELINE=true`로 바꾼 다음, `Build curated RSS feed`를 `run_mode=full`, `allow_pages_deploy=false`로 즉시 실행한다. outbound는 workflow에서 영구 비활성이다. 이 safe-full이 성공하면 legacy pipeline을 `true`로 유지하고 `ENABLE_PAGES=true`를 복구한다. 실패하면 즉시 `ENABLE_LEGACY_PIPELINE=false`로 되돌리고 Pages도 `false`로 유지해 다음 예약 실행과 배포를 차단한다.
7. 최종 상태는 `ENABLE_LEGACY_PIPELINE=true`, `ENABLE_PAGES=true`, `ENABLE_TELEGRAM_DELIVERY=false`, 세 거버넌스 전환 플래그 `false`다. safe-full은 Pages를 배포하지 않으므로 기존 검증된 Pages artifact는 롤백 경계로 남고, 신규 MySQL upsert 데이터는 삭제하지 않는다.

2026-07-21에는 `Yeouido_Lab` 단일 채널 카나리 뒤 동일한 97채널 fingerprint로 전체 365일 이력 복구를 분할 실행했다. 97/97개 canonical 채널과 durable ACK 1,468,220건을 완료했으며 실패·대기·잘림이 남은 구간은 0개다. `dada_news2`는 전역 300,000건 상한에 맞춰 세 구간으로 재개했고, `anyoungjin`과 `kiwoom_semibat` timeout은 마지막 성공 cursor를 보존한 단일 채널 재시도로 복구했다.

후속 signal-only run 29872608749는 zero-channel tail과 동일 fingerprint를 확인한 뒤 최근 72시간 메시지 21,317건·매치 693건에서 signal 40건을 원자 재구축하고 누락 17건을 삭제했다. 최종 metrics는 authorized·finalize·window rebuilt·durable complete가 모두 1이고 원격 실패·대기가 0이다. 전체 실행 ID와 구간별 ACK는 [운영 기반 반영 기록](production-foundation-deployment-2026-07-16.md)에 보존한다.

첫 safe-full run 29873829199는 14분 04초 뒤 증분 메시지 530건의 원격 ACK 전 `ReadTimeout`으로 실패했고, 당시 구현은 실패 뒤 metadata도 호출해 두 번째 timeout을 만들었다. 실패 artifact는 pending 530, remote failed 2, metadata failed 1, sent 0, cursor·prune 전진 0을 기록했다. 이에 legacy pipeline과 Pages를 다시 `false`로 차단했다. 이후 migration 005로 `(telegram_channel_id, telegram_message_id)` 인덱스와 채널별 identity migration marker를 명시 적용하고, 전체 canonical identity 감사와 97개 marker 승인을 완료했다. 메시지·metadata transaction을 최대 5채널로 제한하고 메시지 pending 시 metadata를 생략하는 PHP도 운영 배포했다. handle-only·충돌·handle 변경 메시지가 들어오면 해당 marker를 0으로 내려 다음 권위 metadata에서 다시 정규화한다.

marker 승인 순서는 반드시 `모든 Telegram writer 정지 확인 → 장기 transaction·metadata lock·가용 디스크 preflight → migration 005 → marker 무효화가 포함된 새 PHP 원자 배포 → canonical mismatch 감사 0건 확인 → 조건부 단일 SQL로 현재 권위 채널 marker 승인`이다. migration은 lock 대기를 30초로 제한하고 `ALGORITHM=INPLACE, LOCK=NONE`을 요구하므로 지원되지 않거나 대기 제한을 넘으면 PHP를 배포하지 않는다. 구 PHP가 쓰기를 계속할 수 있는 상태나 audit와 marker UPDATE 사이에 writer가 열리는 상태에서는 승인하지 않는다. 새 PHP의 조건부 marker UPDATE와 message invalidation이 같은 채널 row lock을 사용하므로, 예상하지 못한 동시 write가 있더라도 나중 transaction이 marker를 0으로 되돌리게 하며 승인 직후 mismatch·marker 수를 다시 확인한다.

### 2026-07-22 복구 완료 증빙

- 운영 DB를 92개 테이블·1,940,943행 기준으로 전체 백업하고 압축본 SHA-256 `e851085b65060f4bb169e7032dc52ca9674299564d16e9ca46b797625844ea72`를 별도 보존했다.
- migration 005를 작업 터미널 관측 9.141초에 적용했다. 97개 채널의 identity marker 컬럼과 1,524,369개 메시지 테이블의 `(telegram_channel_id, telegram_message_id)` 인덱스가 정의와 정확히 일치했다.
- release `telegram-timeout-fix-1f8c2ac-20260722T091300KST`를 비공개 백업·후보 smoke test 뒤 원자 배포했다. 후보와 운영 smoke test는 합계 12/12 통과했고 배포 중 outbound Telegram은 실행하지 않았다.
- marker 전 감사에서 canonical identity 누락·중복·mapping mismatch·bad live match·collision은 모두 0건이었다. 기존 orphan match 164건은 모두 원본이 없는 `truly_missing` 레거시 행이고 재연결 가능한 행은 0건이었다. 삭제 대신 감사 증빙을 보존했으며, 조건부 UPDATE 뒤 marker는 `0 → 1`로 97/97개 전환됐다.
- [safe-full run 29880780637](https://github.com/sung1673/activist-rss-curator/actions/runs/29880780637)은 23분 19초에 성공했다. 메시지 1,175건·match 32건, failed/pending 0건, 최대 요청 456,875바이트, signal 40건, outbound 발송 0건이었다. metrics의 `telegram_messages_pruned`와 `telegram_matches_pruned`는 hydrate된 로컬 상태의 5,000건 상한 정리량이며 원격 MySQL 삭제량이 아니다.
- 변수 복구 뒤 [Pages 전용 run 29882176705](https://github.com/sung1673/activist-rss-curator/actions/runs/29882176705)은 총 11분 26초, 페이지 생성 10분 27초에 성공했다. immutable `github-pages` artifact ID `8515364933`을 첫 시도에 배포했고 검증 URL은 [https://news.bside.ai/](https://news.bside.ai/)다. Telegram smoke·resend·daily send 단계는 모두 건너뛰었다.
- 최종 상태는 `ENABLE_LEGACY_PIPELINE=true`, `ENABLE_PAGES=true`, `ENABLE_TELEGRAM_DELIVERY=false`, `ENABLE_GOVERNANCE_SHADOW=false`, `ENABLE_GOVERNANCE_PAGES=false`, `ENABLE_GOVERNANCE_DELIVERY=false`다.

이번 marker 승인을 위해 사용한 일회성 운영 helper는 작업 뒤 로컬에서 제거했다. DB transport의 종단 서버 인증은 일회성 helper뿐 아니라 PHP/PDO 운영 경로까지 아직 별도 증빙이 필요한 hardening 항목이다. 따라서 같은 helper를 반복 사용하지 않으며, 향후 직접 MySQL 유지보수나 PHP DB 설정 변경 전에는 공급자 CA·고정 인증서 또는 공급자가 보장하는 private route를 확인하고 실제 연결의 TLS 협상·서버 인증을 검증해야 한다. 상세 endpoint와 검증 자료는 공개 문서가 아닌 비공개 운영 기록에 보존한다.

PHP 배포 백업은 공개 파일 경로가 아니라 외부 접근이 차단된 `/www_root/activist/_private/deployment-backups/`에만 저장한다. `.htaccess`는 방어적으로 `.bak`과 `.bak.*` 접근도 거부하지만, 공개 경로의 차단 규칙을 백업 저장소로 간주하지 않는다. 배포는 후보 경로의 PHP 7.3·서명 인증·DB smoke test를 통과한 뒤 같은 파일시스템에서 원자적으로 교체하고, 실패하면 비공개 백업으로 복구한다.

## 소스 이용권한

운영 이용권한의 단일 기준은 MySQL `SourceRight`이며 `rights` 또는 `admin` 역할의 `/api/v1/admin/source-rights`로만 승인·변경한다. `config.yaml`에 있는 `telegram:activistkorea` 항목은 fail-closed 동작을 보여 주는 `pending` 자리표시자이고 증빙이 없으므로 수집·AI·재배포 권한이 아니다. 운영자는 이를 직접 `active`로 고치는 대신 권한 범위, 증빙 참조 또는 해시, 유효기간, 철회일을 관리자 API에 등록해야 한다. KIND adapter는 외부 endpoint 호출 전에 `/api/v1/ops/source-right-eligibility?source_right_id=official:kind&use=ingest`에서 `eligible=true`와 `rights_revision`을 받아야 하며, 409·비정상 응답·revision 누락이면 요청 0건 상태로 종료한다. 승인 직후에는 schedule이 없는 수동 `kind-adapter-preflight.yml`을 기본 브랜치에서 실행한다. 이 workflow는 `governance-runtime` 보호 규칙을 적용하고 outbound를 강제로 비활성화하며, 권한 ACK가 성공한 뒤에만 설정된 endpoint/API key로 adapter contract를 한 번 검증한다. 성공해도 pipeline mode나 공개 상태는 변경하지 않는다.

권한이 만료되거나 철회되면 다음 실행부터 수집과 AI 입력을 중단하고 공개 API에서도 연결 문서와 파생 신호를 제외한다. 철회 상태는 Pages artifact 롤백으로 되돌리지 않는다.

`DeliveryOutbox.payload_json`은 과거 row의 감사 계약으로 `rights_lineage_complete: true`와 `source_right_ids` 배열을 보존한다. 신규 enqueue·claim은 서버에서 영구 거절한다. 기존 `pending`·`retry`·`remote_queued` row는 외부로 보내지 않고 계보와 상태만 점검하며, 입증할 수 없는 row를 추정해 복구하지 않는다.

예약 실행에서 필수 Secret이 빠지면 해당 작업은 명시적으로 실패한다. PR CI는 운영 Secret을 읽거나 요구하지 않는다.

`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`는 로컬 유지보수용 `google_news_repair` 또는 직접 MySQL 동기화를 선택한 경우에만 로컬 비공개 환경 파일에서 사용한다. 현재 GitHub Actions 경로는 서명된 `ACTIVIST_API_URL`을 사용하므로 repository Secret으로 요구하지 않는다.

## 장애와 복구

Watchdog은 `/api/v1/ops/health`의 공식 수집 최신성을 확인하고 실제 공개 route를 KST 매시 01·06·11·16·21·26·31·36·41·46·51·56분에 측정해 `/api/v1/ops/availability-observations`에 적재한다. release evidence는 `watchdog-v1-kst-5m-minute01`의 route당 일 288개 slot을 raw `observed_at`으로 재구성하며, 4개 route 일 1,152개와 7일 8,064개가 모두 덮이지 않으면 실패한다. 중복 관측은 missing을 상쇄하지 않고, interval p95와 일 경계 포함 최대 공백이 모두 600초 이하여야 한다. Telegram outbox는 발송이 영구 비활성인 현재 release gate의 분모가 아니다.

Production Alpha의 `global-alpha-watchdog.yml`은 `GOVERNANCE_PIPELINE_MODE=shadow|live`와 `GLOBAL_ALPHA_OBSERVATION_ENABLED=true`가 모두 충족될 때만 `BSIDE_OPS_TOKEN`으로 읽기 전용 `GET /api/v2/ops/release-state`를 호출한다. 빈 값과 `false`는 관측 시작 전 안전한 기본값이며, `true|false` 이외의 값은 fail-closed한다. 이 job에는 `BSIDE_ADMIN_TOKEN`과 `BSIDE_RELEASE_AUTHORIZER_TOKEN`을 주입하지 않는다. 따라서 주기 관측 코드가 탈취되거나 오작동해도 release state 변경이나 일회용 공개 승인 발급 권한을 갖지 않는다.

- 마지막 정상 수집이 90분을 넘으면 incident
- 루트, feed, 활성화된 governance Pages 또는 API health 중 하나라도 실패하면 incident
- 5분 관측 간격을 실제로 저장해 web 배포 실패 탐지 p95 10분 이내 여부를 판정
- endpoint, 인증, 응답 형식이 유효하지 않아도 incident

Incident가 발생하면 `[ops/incident] Governance pipeline unhealthy` 이슈를 만들거나 기존 열린 이슈 본문을 최신 진단으로 갱신한다. 정상 회복을 확인하면 같은 이슈에 회복 기록을 남기고 닫는다.

Legacy Pages는 배포 전에 같은 저장소·기본 브랜치·legacy workflow의 직전 성공 artifact를 찾고, artifact digest를 검증한 뒤 2026-05-01 이후 날짜형 `feed/YYYY-MM-DD.html`만 복원한다. 날짜가 끊기거나 필수 호환 경계인 2026-07-20보다 일찍 끝나면 과거 Telegram 링크를 지우지 않도록 workflow를 fail-closed한다. 최근 Pages artifact가 만료된 경우에는 매일 05:00 실행에서 30일 보존하는 `legacy-pages-archive-seed`를 사용한다. seed도 없으면 운영자가 정상 artifact를 확인해 다시 만들기 전에는 배포하지 않는다.

그 뒤 별도 임시 디렉터리를 만들고 루트의 `CNAME`, `404.html`, `feed.xml`, `index.html`과 `feed/`의 고정 공개 페이지·유효한 날짜 페이지만 명시적으로 복사한다. 따라서 신규 `public/governance/`, 예상하지 않은 루트 파일, `feed/` 내부의 debug·JSON·하위 디렉터리는 `ENABLE_GOVERNANCE_PAGES=false`인 동안 legacy artifact에 들어가지 않는다. 정적 `story-review.html` 또는 검수 메타데이터가 남아 있거나 필수 공개 파일이 없거나 심볼릭 링크가 발견되면 staging과 workflow를 fail-closed한다.

Pages artifact는 한 번만 업로드한 뒤 같은 immutable artifact를 최대 세 번 순차 배포한다. 첫 실패 후 180초, 두 번째 실패 후 300초를 기다리며, 성공한 시도의 URL만 최종 Pages environment URL로 확정한다. 세 번 모두 실패하면 workflow가 실패하고 `[ops/incident] GitHub Pages deployment unhealthy` 이슈를 별도로 생성·갱신한다. 다음 실제 Pages 검증 성공 때 회복 기록을 남기고 닫으며, Pages를 실행하지 않은 workflow 성공은 이 이슈를 닫지 않는다. Incident listener는 기본 브랜치의 완료된 workflow에서 최종 검증 step만 읽고, triggering revision을 checkout하거나 artifact·운영 Secret을 실행하지 않는다.

Governance Pages 생성 결과는 `pages-<run_id>-<attempt>` artifact로 30일 보존한다. Legacy Pages는 최종 배포 실패본을 `pages-failed-<run_id>-<attempt>` artifact로 7일 보존한다. 배포 문제가 발생하면 GitHub Actions의 정상 artifact를 내려받아 `daily.yml`을 수동 실행해 재배포한다. DB의 신규 데이터와 outbox는 롤백하지 않는다.

운영 Pages 배포는 저장소 기본 브랜치에서만 허용한다. `github-pages` environment의 branch policy와 workflow 내부 기본 브랜치 gate를 함께 유지하며, 기능 브랜치 수동 실행은 페이지 생성·검증 artifact까지만 만들 수 있다. Pages는 저장소 설정에서 미리 활성화하고 workflow가 별도 PAT로 자동 활성화하지 않도록 `configure-pages`의 `enablement` 옵션을 사용하지 않는다. 05:45 생성이 Pages 재시도로 늦어질 수 있으므로 06:05 발송 검증은 당일 05:40~07:00 KST에 생성된 성공 marker를 허용한다. workflow 경로, 실행 성공 여부, 당일 artifact 검증은 그대로 fail-closed로 유지한다.

`daily.yml`의 생성 단계는 `python -m curator.governance_ui`를 실행해 `public/governance/config.js`에 공개 API 기준 URL만 기록하고 HTML·JS·CSS 성능 예산을 검사한다. 인증값이나 운영 Secret은 브라우저 자산에 포함하지 않는다.

## 배포 전 점검

1. PR의 `CI` 필수 테스트가 통과했는지 확인한다.
2. 수동 `Ingest official sources`와 `Ingest media sources`를 한 번씩 실행한다.
3. 현재는 `ENABLE_TELEGRAM_DELIVERY=false`와 모든 발송 workflow의 skip 상태를 확인한다.
4. `Daily governance pages`를 `generate`로 실행해 Pages artifact와 실제 페이지를 확인한다.
5. `Operations watchdog`을 실행해 건강 상태와 incident 자동 회복을 확인한다.
6. 14일 shadow와 최근 7일 production 증빙 artifact를 준비해 `Governance release transition gate`를 실행하고, 통과 보고서와 사람 승인을 보존한다.

Production Alpha 전환에는 위 정기 점검과 별도로 DART·SEC EDGAR 각각의 최근
30일 이상 명시적 수동 backfill/replay가 필요하다. EDINET·Companies House는
정책 비활성 상태와 요청 0건을 증명하며 30일 receipt 대상으로 계산하지 않는다.
checkpoint가 없는 connector의 자동 최근 2일 bootstrap은 빈 기간을 건너뛰지 않기
위한 안전 시작점일 뿐 출시 범위를 증명하지 않는다. 보호된 Alpha 증빙에는 동일
SHA의 시작·종료 시각, 성공/실패 window 수와 멱등 재실행 결과를 기록하며, 한
connector라도 30일 범위·최신성·실패 0건 조건을 충족하지 못하면 cutover를
실행하지 않는다.

## OpenDART apply 권한 경계

`source-right-bootstrap.yml`이 등록한 `official:dart`만 OpenDART apply에 사용할
수 있다. 공식 수집 payload는 더 이상 SourceRight를 생성하거나 upsert하지
않으며, `official:dart`를 payload에 넣으면 서버가
`dart_source_right_managed_out_of_band`로 거절한다.
`official:dart`도 고정 metadata-only 계약이다. `body_text` 또는 `content`의
빈 문자열은 기존 collector 호환을 위해 허용하되 SQL `NULL`로 저장하고,
비어 있지 않은 본문은 permission scope와 무관하게 transaction 전체를
`dart_body_text_forbidden`으로 거절한다.

예약 수집은 durable slot claim 전에, backfill은 `mode=apply`일 때 어떤
checkpoint write보다 먼저 다음을 확인한다.

1. 고정 production v2 endpoint와 `bside-global-market-terminal` service
2. schema version 12와 workflow의 exact 40자리 `GITHUB_SHA`
3. 실행 모드에 대응하는 인증 없는 v1 `/events?limit=1` 계약
   (`closed=503`, `preview=401/403`, `live=200`)과 동일한 v2 release state
4. `official:dart / official_disclosure / dart`의 현재 `collect` 자격
5. `ai_allowed=false`, `redistribution_allowed=true`
6. 보호된 metadata-only 계약과 같은 `contract_revision`

collector는 OpenDART 요청 전에 한 번, remote write 직전에 다시 확인하고 두
`rights_revision`·`contract_revision`과 release state가 같을 때만 HMAC payload에
`expected_release_state` precondition을 담는다. PHP는 각 HMAC write
transaction에서 v1·v2 release-state row를 cutover와 같은 정렬 순서로 함께
잠그고 두 상태가 서로 같으며 서명된 기대 상태와도 정확히 같은지 확인한 다음
`official:dart` row를 `FOR UPDATE`로 잠가 자격과 두 digest를 다시 비교한다.
누락·계약 변경·만료·철회·경쟁 변경은 데이터, 회사 master, run row를 쓰기 전에
409로 중단한다. `dry-run`은 이 운영 API preflight를 호출하지 않고 기존처럼
OpenDART fetch·정규화만 수행한다.
