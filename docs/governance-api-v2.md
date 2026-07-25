# BSIDE Global Market Terminal API v2

이 문서는 `news.bside.ai`의 글로벌 주주·자본시장 데이터 터미널이 사용하는 `/api/v2` 계약을 설명한다. v2는 완성된 품질 인증판이 아니라 **Production Alpha**다. 동일한 화면과 사건 분류를 6개국에 제공하지만, 국가별 데이터 접근권과 공식 소스 범위가 다르므로 동일한 시장 재현율을 보장하지 않는다.

정확한 기계 판독 계약은 [`deploy/activist/openapi-v2.yaml`](../deploy/activist/openapi-v2.yaml)이다.

## 주소와 버전

- 운영 API: `https://alignpe.gabia.io/activist/api.php/api/v2`
- 공개 화면: `https://news.bside.ai`
- 스키마 버전: `11`
- 기존 `/api/v1`과 레거시 피드는 호환 기간 동안 별도로 유지한다.

`GET /api/v2/health`와 `GET /api/v2/openapi.yaml`은 release state와 무관하다. 다만 `health`는 `deployment-manifest.json`과 API 핵심 파일 6개의 SHA-256이 모두 일치할 때만 200을 반환하고, 응답의 `code_revision`에 정확한 40자리 Git SHA를 제공한다. manifest가 없거나 잘못됐거나 파일이 하나라도 다르면 503 `deployment_identity_unavailable`이다. 이 성공은 API 배포 신원만 증명하며 데이터베이스·공식 소스·공개 데이터 경로의 정상 상태까지 뜻하지는 않는다.

API 배포 artifact는 다음 명령으로 checkout의 정확한 SHA를 고정한다.

```bash
python -m curator.deployment_manifest \
  --root deploy/activist \
  --code-revision "$GITHUB_SHA" \
  --output deploy/activist/deployment-manifest.json
```

`api.php`, `governance_v1.php`, `governance_v2.php`, `governance_v2_write.php`, `openapi-v2.yaml`, `migrations/011_global_terminal_v2.sql`, 생성된 `deployment-manifest.json`은 항상 하나의 배포 transaction으로 교체한다. 전송은 줄바꿈을 바꾸지 않는 binary/byte-preserving 방식이어야 한다. 디렉터리 단위 원자 교체가 불가능한 환경에서는 v2를 `closed`로 둔 채 핵심 파일을 먼저 올리고 manifest를 마지막 commit marker로 교체한다. 중간 상태의 health 503은 정상적인 fail-closed 동작이며, 이전 파일과 새 manifest를 섞어 200으로 우회해서는 안 된다. 롤백도 이전 핵심 파일 전체와 그 파일들에서 생성한 이전 manifest를 한 묶음으로 복원하고 manifest를 마지막에 교체한다. PHP OPcache를 사용하는 서버는 교체 transaction 직후 캐시를 무효화하거나 PHP 프로세스를 안전하게 reload한 다음 health를 확인해야 한다.

Migration 011은 이름으로 만든 고정 checksum을 사용하지 않는다. 배포한 SQL 파일의 원본 바이트를 SHA-256으로 계산하고, 파일과 같은 MySQL 세션·같은 입력 stream에서 먼저 세션 변수를 설정한 뒤 파일 바이트를 그대로 적용한다. 예시는 다음과 같다.

```bash
migration=deploy/activist/migrations/011_global_terminal_v2.sql
migration_sha256="$(sha256sum "$migration" | cut -d ' ' -f1)"
{
  printf "SET @bside_migration_011_sha256 = '%s';\n" "$migration_sha256"
  cat "$migration"
} | mysql --database="$MYSQL_DATABASE"
```

세션 변수가 없거나 소문자 64자리 SHA-256이 아니면 migration은 중단된다. 적용 후 `schema_migrations`의 version 11 checksum은 이 값과 정확히 같아야 한다. v2는 요청마다 검증된 deployment manifest의 `migrations/011_global_terminal_v2.sql` 해시와 DB row를 비교하므로, SQL 파일의 1바이트 변경·다른 세션에서의 `SET`·텍스트 모드 전송에 따른 줄바꿈 변경은 모두 fail-closed 503으로 이어진다. 따라서 실제 DB에 적용할 파일과 API artifact에 포함할 파일은 반드시 동일한 byte-preserving 복사본이어야 한다.

## Production Alpha의 공개 범위

국가는 `KR`, `US`, `JP`, `GB`, `CA`, `AU`로 고정한다. 화면과 API에는 소스별·사건 유형별 범위를 다음 값 중 하나로 표시한다.

| 값 | 의미 |
|---|---|
| `market-wide` | 허용된 공식 시장 단위 소스의 명시적 문서 유형 범위 |
| `official-register` | 공식 회사 등록부 범위. 거래소 공시 전체를 뜻하지 않음 |
| `selected-issuers` | 승인된 회사 목록만 수집 |
| `link-only` | 원문을 저장·재배포하지 않고 링크만 제공 |
| `unavailable` | 해당 사건 유형에 현재 유효한 공개 범위를 확인할 수 없음 |

현재 migration 011이 선언하는 초기 범위는 다음과 같다.

- 한국: OpenDART의 8개 거버넌스 사건 유형, `market-wide`
- 미국: SEC EDGAR 공식 Latest Filings Atom 당일 증분과 completed-day 일일 인덱스 대조의 대량보유·주총·공개매수/M&A 허용 서식, `market-wide`. Atom cursor가 없거나 마지막 hybrid 성공이 45분을 넘으면 `delayed`, `live_ready=false`로 공개·출시를 차단한다.

SEC 당일 경로는 SEC가 [Latest Filings Search와 RSS feed](https://www.sec.gov/about/rss-feeds)로 공개한 `https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&output=atom`만 사용한다. 모든 요청은 연락 가능한 이메일이 든 `SEC_EDGAR_USER_AGENT`를 전송하고 redirect를 따르지 않는다. 같은 hybrid 실행의 daily index와 Atom 요청은 공통 120ms 최소 간격(초당 10회 미만)을 사용하고 실제 요청 수 합계가 `max_pages`를 넘으면 적재하지 않는다. 첫 요청 전에는 인위적 지연을 넣지 않는다. Atom 응답은 source title·acceptance timestamp·official filing-index URL을 보존한다. 8-K/8-K/A는 `unclassified` 비공개 검수 후보이며 관련 item을 사람이 확정하기 전 공개 사건이 되지 않는다.
- 일본: EDINET의 대량보유·주총·공개매수/M&A·자본환원·정정/철회 허용 문서 유형, `market-wide`
- 영국: 설정된 company number에 대한 Companies House 등록부 범위, `official-register`
- 캐나다: issuer 식별자와 별도 호스트 증빙에 묶인 회사 IR 링크 metadata, `link-only / manual-metadata`; SEDAR+ 전문 수집·재배포 제외
- 호주: `asic.gov.au` 공식 호스트의 수동 등록부 링크 metadata, `link-only / manual-metadata`; ASX 공시 전문 수집·재배포 제외

`GET /sources/status`는 수집 상태와 공개 준비 상태를 분리한다. `collect_status`와 `collect_fresh`는 현재 증빙이 등록된 SourceRight로 수집이 가능하고 마지막 성공·확인 시각이 `min(45분, max(15분, 실행 주기의 3배))` 이내인지 나타낸다. connector는 `active`이고 현재 오류가 없어야 한다. SEC는 `sec-current-v1:` 뒤의 canonical base64url JSON을 엄격히 해석해 `{schema_version: 1, updated_at}`의 UTC 시각도 같은 최신성 한도 안이어야 하며, CA/AU `link-only`는 같은 한도 안의 관측과 1건 이상의 raw/ACK가 모두 필요하다. `required_source_ready`와 `all_required_ready`는 이 동일한 계약을 필수 6개 connector에 적용한 결과다. connector의 `source_type`·`source_key`가 grant의 불변 identity와 대소문자까지 정확히 일치하지 않으면 `blocked_identity`로 차단한다. `public_status`와 `public_ready`는 여기에 현재 재배포 허용 여부와 공개 소스 정책까지 적용한 결과다. `status`와 `fresh`는 호환성을 위해 남긴 수집 상태 alias이며 brief 발행 판단에 사용하지 않는다. `public_ready=true`인 국가만 `no_confirmed_material_events` 발행이 가능하다. 수집은 최신이지만 재배포가 허용되지 않으면 `collect_fresh=true`, `public_status=redistribution_blocked`, `public_ready=false`가 된다. 사건이 0건인 것과 소스 장애·권한 대기·미지원은 서로 다른 상태다.

## 공개 사건 기준

v2 공개 목록에는 다음 조건을 모두 만족하는 사건만 나타난다.

- issuer와 8개 글로벌 사건 유형 중 하나가 지정됨
- `publication_status=published`, 완전한 canonical identity
- 검수 상태가 `approved` 또는 `not_required`
- 공개 확인 상태가 `official|confirmed|corroborated|corrected|withdrawn`
- 유효하고 재배포 가능한 SourceRight가 연결된 공개 근거 문서가 최소 1개 존재
- `licensed_telegram`과 `authorized_telegram` 문서·관측·URL은 SourceRight 상태와 무관하게 v2 공개 응답에서 항상 제외
- 중요도가 `high|critical|market_sensitive`이면 사람이 승인함
- `withdrawn` 사건이면 사람이 승인함

`signal`은 공개 API 상태가 아니며 Top 5에도 들어가지 않는다. Brief item은 발행 당시 사건 값을 `event_snapshot_json`에 고정하되 `source_url`은 snapshot에 저장하지 않는다. 조회할 때 사건 사실은 불변 snapshot에서 반환하고, 대표 URL만 현재 유효하고 재배포 가능한 비-Telegram 문서에서 다시 계산한다. 따라서 SourceRight가 만료·철회되면 해당 URL은 즉시 다른 적격 근거 URL로 바뀌거나 `null`이 되며, 다른 적격 문서가 있으면 사건 자체는 계속 공개된다. 최신 edition의 Top 근거가 더 이상 안전하게 공개될 수 없으면 이전 발행본으로 되돌아가지 않고 최신 edition을 `coverage_unavailable`로 fail-closed한다. 응답의 `stale=true`는 현재 발행본의 cutoff가 36시간을 넘었다는 뜻이며 화면에 기준 시각과 함께 명시한다.

승인된 brief가 없으면 `/briefs/latest`는 오류나 무관한 대체 뉴스를 만들지 않고 빈 목록과 `empty_reason=no_approved_brief`를 반환한다. Top 5가 0건인 발행본은 `no_confirmed_material_events` 또는 `coverage_unavailable` 중 실제 소스 상태와 일치하는 이유를 반드시 고정한다.

새 `coverage_unavailable` 발행본은 항상 canonical latest다. `/briefs/latest`는 Top을 빈 목록으로 반환하고 `coverage_notice.scope=blocking`으로 장애를 명시한다. 일부 필수 소스만 사용할 수 없으면 정상 소스의 사건은 계속 제공하면서 `coverage_notice.reason=partial_coverage`, `scope=warning`, 영향 국가·소스 목록을 함께 반환한다. 정상 수집 중 실제 중요 사건이 없는 경우에는 별도의 `no_confirmed_material_events`를 사용한다.

제목과 `original_language`는 원문을 보존한다. API가 제목이나 본문을 자동 번역하지 않는다.

## Release state와 인증

v2는 v1과 독립적인 `global_terminal_v2` release state를 사용한다.

| 상태 | 공개 데이터 API |
|---|---|
| `closed` | HTTP 503 `global_terminal_release_closed` |
| `preview` | preview Bearer token 필요, `Cache-Control: private, no-store` |
| `live` | 인증 없이 공개, 짧은 public cache 허용 |

`/health`, `/openapi.yaml`, `/openapi.json`은 이 상태를 통과하지 않는다. `/ops/*`와 `/admin/*`도 공개 release state를 통과하지 않지만, 정확한 migration manifest 1~11과 역할 토큰을 요구한다.

- `/ops/source-right-eligibility`: `ops` 또는 `admin` Bearer token
- `/ops/alpha-release-evidence`: `ops` 또는 `admin` Bearer token
- `/ops/ingest`: `ops` 또는 `admin` Bearer token
- `/ops/release-state`: `ops` 또는 `admin` Bearer token. 5분 watchdog은 이 읽기 전용 경로만 사용
- `/admin/review-queue`, `/admin/events/{event_id}/review`: `editor` 역할
- `/admin/brief-candidates`, `/admin/briefs`: `editor` 역할
- `/admin/release-state`: `admin` Bearer token
- `/admin/release-authorizations`: 정확한 `release_authorizer` Bearer token만 허용. `admin` 상위 역할 대체 없음
- `/admin/cutover`: 이미 발급된 일회용 승인을 소비하는 `admin` Bearer token

`admin` token은 상위 역할로서 `ops`와 `editor` 경로도 사용할 수 있다. 사건 검수와 Brief 발행 계약 자체는 editor 전용으로 분리되어 있으며 일반 ops token으로 호출할 수 없다.

서버에는 평문 토큰이 아니라 SHA-256 해시만 등록한다. preview token은 URL query, Pages 자산 또는 로그에 넣지 않고 브라우저 세션에서 `Authorization` 헤더로만 전달한다.

`POST /admin/release-state`는 `expected_version`을 사용한 낙관적 잠금을 적용한다. `reason`은 8~2,000자이며, preview 인증이 설정되지 않은 서버는 `closed → preview`를 거절한다. 이 경로가 허용하는 변경은 `closed → preview`, `preview → closed`, `live → closed`뿐이다. 같은 상태 재요청은 멱등이며 `closed → live`는 거절한다. 특히 v1과 v2 어느 경로에서도 `preview → live`를 직접 요청하면 HTTP 409 `protected_atomic_cutover_required`를 반환한다. 따라서 일반 `BSIDE_ADMIN_TOKEN`만으로 공개 승격을 우회할 수 없다.

```json
{
  "release_state": "preview",
  "expected_version": 0,
  "reason": "Production Alpha 24-hour review"
}
```

공개 승격은 보호된 `governance-release` workflow가 다음 두 단계를 연속 수행할 때만 가능하다.

1. `POST /admin/release-authorizations`는 해당 보호 환경에만 둔 `BSIDE_RELEASE_AUTHORIZER_TOKEN`으로 호출한다. 토큰은 서버에서 정확한 `release_authorizer` 역할이어야 하며 `admin`, `editor`, `ops` 토큰으로 대체할 수 없다. 요청은 현재 배포 manifest의 40자리 candidate SHA, 검증한 GitHub release-evidence artifact의 `sha256:` digest·run ID·artifact ID, v1·v2 preview state version, 32바이트 난수 nonce와 서버 시각 기준 60~900초 유효기간을 함께 고정한다. 서버에는 nonce 원문 대신 SHA-256만 저장하고, 응답에도 nonce를 되돌려 주지 않는다. 새 승인은 이전의 미사용 승인을 철회한다.
2. `POST /admin/cutover`는 `BSIDE_ADMIN_TOKEN`으로 호출하되 첫 단계에서 마스킹해 보관한 같은 nonce, candidate SHA, evidence digest와 두 state version을 모두 제출한다. 보호 workflow는 호출 전에 evidence run 생성 시각, 보고서 `evidence_as_of`, 관측 종료 시각이 모두 현재로부터 60분 이내인지 검증한다. 서버는 하나의 MySQL transaction 안에서 두 release state row와 승인을 잠근 다음, KR DART·US SEC EDGAR·JP EDINET·GB Companies House·CA issuer IR link·AU ASIC link의 필수 6개 connector row와 SourceRight row를 모두 `FOR UPDATE`로 잠근다. 현재 공개 문서가 0건인 국가도 예외가 아니다. connector는 등록된 국가·source key/type·SourceRight ID·coverage mode가 정확히 일치하고 `active`여야 하며, 마지막 성공·확인 시각과 SEC intraday cursor 또는 link-only 관측이 위 15~45분 최신성 계약을 만족해야 한다. SourceRight는 정확한 source identity, `active`, 증빙, 비어 있지 않은 permission scope, 유효기간·철회 상태, `collect` 및 공개 재배포 자격을 모두 충족해야 한다. 그 뒤 기존 v1·v2 공개 문서 SourceRight guard를 다시 실행하고, 모든 검사가 통과한 경우에만 `governance_v1`과 `global_terminal_v2`를 같은 `cutover_at`·`sunset_at`으로 함께 `live`로 바꾼다. 두 감사 row에는 같은 `release_authorization_id`가 기록된다.

승인은 만료·철회·사용·candidate/evidence/version 불일치 중 하나라도 있으면 fail-closed한다. 필수 connector 또는 SourceRight 검사가 실패하면 HTTP 409 `required_alpha_sources_invalid`와 connector별 비민감 사유를 반환하고, 두 release state와 승인 소비 시각은 모두 변경하지 않는다. 소비된 nonce는 긴급 `live → closed` 롤백 뒤에도 다시 쓸 수 없다. 다시 공개하려면 새 증빙과 현재 두 state version에 묶인 새 nonce 승인을 받아야 한다. 반면 긴급 차단은 승인 발급 없이 기존 v1·v2 `POST /admin/release-state`에서 계속 가능하다.

## 경로

### 공개 읽기

- `GET /briefs/latest?edition=global|KR|US|JP|GB|CA|AU`
- `GET /live`
- `GET /events`, `GET /events/{event_id}`
- `GET /issuers`, `GET /issuers/{issuer_id}`
- `GET /calendar`
- `GET /search?q=...`
- `GET /sources/status`
- `GET /exports/events.json`
- `GET /exports/events.csv`
- `GET /feeds/events.atom`

`/live`는 `updated_at` 내림차순 스트림이다. 실시간 시세 피드나 자동 24시간 범위를 뜻하지 않는다. `/events`는 발생일 내림차순이고, `/calendar`는 기한이 있는 사건만 기한 순으로 반환한다.

사건 목록·검색·캘린더·내보내기·Atom에는 다음 공통 필터를 적용할 수 있다.

`country, market, issuer_id, event_family, verification_status, change_type, from, to`

`from`과 `to`는 `occurred_at`에 적용한다. 검색어는 2~100자이며 사건 제목·요약·현재 상태·사건 유형, 회사 법인명·주 종목코드·시장·식별자, 승인된 활성 당사자 이름을 검색한다. 문서는 현재 재배포 권한이 유효한 비-Telegram 근거의 제목과 문서 유형만 검색하며, 검색 결과는 사건 형태로 반환한다.

### 운영과 관리

- `GET /ops/source-right-eligibility?source_right_id=official:...&use=collect|public|ai`
- `GET /ops/release-state`
- `GET /ops/connectors/{connector_id}/checkpoint`
- `GET /ops/alpha-release-evidence?code_revision=<40자리 SHA>`
- `POST /ops/ingest`
- `GET /admin/connectors`
- `GET /admin/connectors/{connector_id}`
- `POST /admin/connectors/{connector_id}`
- `GET /admin/review-queue`
- `POST /admin/events/{event_id}/review`
- `GET /admin/brief-candidates`
- `POST /admin/briefs`
- `GET /admin/release-state`
- `POST /admin/release-state`
- `POST /admin/release-authorizations`
- `POST /admin/cutover`

SourceRight eligibility 응답의 `rights_revision`은 현재 권한 row의 상태·증빙 유무·유효기간·철회·AI/재배포 허용 값을 묶은 SHA-256이다. 이 조회는 권한의 현재 상태를 확인하는 운영 preflight이며, 공개 데이터 목록이 아니다.

Alpha evidence endpoint는 요청 SHA가 현재 배포 manifest와 정확히 같을 때만
운영 MySQL을 읽는다. 비한국 소스는 `global_ingest_receipts`, DART는 hash가
일치하는 durable backfill checkpoint에서 최신 30개 연속 1일 window를
재구성한다. 공개 사건의 원문 언어·공식 URL·제목 출처도 다시 세며,
`title_provenance=source` 제목은 연결된 현재 유효 공식 문서 제목과 byte 단위로
같을 때만 보존으로 계산한다. 누락·중복·공백·raw/ACK 불일치·다른 SHA·10,000건
scan 상한 초과는 409로 실패한다. 이 결과가 출시 workflow에서 보호 입력의
수집 범위와 content-integrity 수치를 강제 교체하므로 사람이 임의 집계한
숫자로 전환 게이트를 통과할 수 없다.

admin connector API는 SourceRight를 새로 승인하거나 수정하지 않는다. `GET /admin/connectors`는 전체 connector의 현재 상태와 collect eligibility를, detail GET은 해당 connector와 최근 감사 로그를 반환한다. collect eligibility에는 `eligible`, connector↔right의 `identity_match`, 불가 사유, rights revision·상태·유효기간·철회일·재배포·AI 허용값이 포함된다.

`POST /admin/connectors/{connector_id}`는 정확히 `target_status`, `expected_updated_at`, `reason`만 받는다. 대상 상태는 `configured|inactive`, 사유는 8~1,000자다. `configured` 전환은 현재 `collect` 자격이 유효하고 connector와 SourceRight의 `source_right_id`, `source_type`, `source_key`가 모두 일치할 때만 허용한다. 권한 또는 identity가 맞지 않으면 `409 connector_source_right_ineligible`, 직전 조회 뒤 상태가 바뀌면 `409 stale_connector_update`다. `inactive` 전환은 권한과 무관하게 허용하고 `last_error_class=admin_inactive`로 남긴다. 같은 상태를 다시 요청해 `changed=false`가 되어도 모든 POST는 `previous_status`, 변경자, 사유를 `activist_global_connector_audit`에 기록하며 응답에 `audit_id`를 포함한다.

## 공식 수집 적용

`POST /ops/ingest`는 등록된 connector가 만든 schema version 1 envelope만 받는다. 한 envelope에는 document record와 lifecycle observation을 각각 최대 500개까지 넣을 수 있다. 서버는 다음을 한 transaction에서 보장한다.

- connector의 국가·소스·coverage mode가 등록값과 정확히 일치함
- 직전에 받은 64자리 `rights_revision`이 현재 SourceRight와 일치함
- transaction lock 이후에도 권한이 유효하고 수집 가능한 상태임
- issuer identifier·listing·document·event stable ID가 기존 row와 충돌하지 않음
- `(connector_id, idempotency_key)`를 같은 payload와 code revision으로 재실행하면 row를 늘리지 않음
- record와 lifecycle observation의 실제 ACK 수를 receipt에 기록함

500건을 넘는 한 소스 window는 닫힌 `chunk` 객체의 `index`, `count`, `batch_id`, `window_start`, `window_end_exclusive`로 묶는다. window는 반개구간이며 1~31일만 허용한다. `batch_id`는 source·window·내용뿐 아니라 `code_revision`도 포함한 결정적 해시이므로 배포 SHA가 달라지면 별도 batch가 된다. 각 receipt는 `batch_id`, `chunk_index`, `chunk_count`, 두 window 날짜, chunk의 `request_count`, 전체 `batch_raw_count`, `batch_acknowledged_count`, `batch_request_count`를 함께 고정한다. receipt의 `raw_count`와 `acknowledged_count`는 해당 chunk 수량만 기록하고 `(connector_id, batch_id, chunk_index)`는 유일하다. `raw_count`는 공식 소스에서 관측한 행, `acknowledged_count`는 저장 계약이 수락한 record와 lifecycle observation의 합이다. 따라서 서버는 모든 chunk에서 `raw_count >= acknowledged_count`를 강제한다. 증빙 exporter는 스키마 변경 없이 `accepted_count = acknowledged_count`, `filtered_out_count = raw_count - accepted_count`를 산출하고 `raw = filtered_out + accepted`, `ACK = accepted`를 검증한다. 서버는 chunk를 1부터 순서대로만 받는다. final chunk에서는 1..N receipt가 모두 존재하고 code revision·window·chunk count·전체 batch 합계가 같으며 chunk별 raw·ACK·request 합이 선언값과 정확히 일치하는지 확인한다. final 선행, 순서 역전, metadata 또는 합계 불일치는 HTTP 409로 거절한다. `source_connectors`의 마지막 수집량과 checkpoint는 이 검증을 통과한 final chunk에서만 원자 확정하며, 중간 chunk가 이전 완료 checkpoint를 덮어쓰지 않는다. 운영 재개 시 ops 전용 checkpoint API가 자격정보 없이 이 완료 지점을 반환한다.

수집기가 보내는 `public_allowed`와 `ai_allowed`는 참고 snapshot일 뿐이다. 서버가 현재 등록된 권한을 다시 판단한다.

자동 수집은 사건을 직접 공개하지 않는다. 신규 사건은 항상 아래 상태로 생성된다.

```text
publication_status=draft
review_status=pending
identity_status=needs_review
comparison_key=NULL
```

성공 응답의 `public_events_created`는 0이다. 재배포 권한이 없는 문서는 본문과 공개 상태가 제한되며, 그 문서로 사건을 자동 공개할 수 없다.

OpenDART는 이 v2 수집 경로의 대상이 아니다. 한국 공시는 기존 `official-ingest` 파이프라인에서 수집하며, `connector:kr:dart`를 `/api/v2/ops/ingest`로 보내면 validation error로 거절한다. 이렇게 두 경로가 같은 DART 접수번호를 중복 적재하는 것을 막는다.

자동 수집기가 8개 공개 사건 유형 중 하나를 확정할 수 없으면 `event_family=unclassified`로 보존한다. 이 값은 수집 envelope와 비공개 검수 큐에서만 허용되고 공개 사건 enum에는 포함되지 않는다. 특히 SEC 일일 인덱스의 `8-K`/`8-K/A`는 item 번호가 없으므로 자동 분류하지 않는다. EDINET 임시보고서도 명시적 사유가 정확히 하나의 허용 유형과 일치할 때만 분류하고, 사유가 불투명하거나 여러 유형과 충돌하면 `unclassified`로 둔다. 두 경우 모두 원문 제목, `original_language`, 공식 `original_url`을 그대로 보존한다.

### 비한국 공식 소스 runner

`.github/workflows/ingest-global.yml`은 기본 브랜치에서 `GOVERNANCE_PIPELINE_MODE=shadow|live`일 때 매시 17분·47분에 미국·일본·영국 connector를 병렬 실행한다. 수동 실행에서는 국가와 반개구간 날짜 범위도 지정할 수 있다. 기본 범위는 아직 끝난 최근 2개 UTC 날짜이며, 각 실행은 SourceRight를 첫 요청 전·각 페이지 전·API 전송 전에 확인하고 ACK가 실제 record와 lifecycle observation 합계와 다르면 실패한다. 실행 결과는 원문이나 자격정보를 넣지 않은 30일 보존 evidence artifact로 남긴다.

필요한 설정은 다음과 같다.

- 공통: `BSIDE_API_BASE_URL` secret 또는 `GOVERNANCE_API_BASE_URL` variable, `BSIDE_OPS_TOKEN` secret, `GOVERNANCE_PIPELINE_MODE` variable
- SEC EDGAR: 연락 가능한 이메일을 포함한 `SEC_EDGAR_USER_AGENT` variable
- EDINET: `EDINET_API_KEY` secret
- Companies House: `COMPANIES_HOUSE_API_KEY` secret과 `COMPANIES_HOUSE_ISSUERS_JSON` variable

`COMPANIES_HOUSE_ISSUERS_JSON`은 빈 배열을 허용하지 않는 최대 50개 회사의 명시적 allowlist다. 각 항목은 `company_number`, `legal_name`을 필수로 하고 `market`, `ticker`만 선택적으로 허용한다. 캐나다·호주는 이 runner가 원문 수집하지 않는다.

예약 실행처럼 `from_date`와 `to_date`가 모두 비어 있으면 runner는 `GET /ops/connectors/{connector_id}/checkpoint`에서 MySQL durable checkpoint를 읽는다. 완료된 `window_end_exclusive`의 하루 전부터 겹쳐 읽고 한 번에 최대 31일의 half-open window를 순서대로 처리해 긴 장애 뒤에도 날짜를 건너뛰지 않는다. 아직 checkpoint가 없으면 최근 완료 2일부터 시작한다. SEC는 completed-day 날짜 checkpoint와 별도로 `sec-current-v1` source cursor를 같은 schema v2 cursor에 저장하며, 90분 overlap으로 재관측한 Atom 항목은 content idempotency key와 DB upsert로 멱등 처리한다. 수동 범위에서도 SEC source cursor를 읽고, 다른 connector는 지정 범위만 처리한다. 둘 중 하나만 입력하면 `partial_explicit_window`로 fail-closed한다.

### 캐나다·호주 수동 공식 링크 metadata runner

`.github/workflows/ingest-selected-markets.yml`은 `GOVERNANCE_PIPELINE_MODE=shadow|live`일 때 매시 07분·37분에 캐나다와 호주를 분리 실행한다. 입력은 Repository variable `CA_OFFICIAL_LINKS_JSON`, `AU_OFFICIAL_LINKS_JSON`이고 공통 v2 API와 `BSIDE_OPS_TOKEN`을 사용한다.

이 경로는 명시적으로 승인된 링크 metadata만 적재한다. 설정된 `original_url`을 요청하지 않고 본문을 저장하지 않으며, 국가별 최대 50개 issuer·50개 승인 호스트 mapping·500개 record를 허용한다. 캐나다는 `official:ca-issuer-ir`, 호주는 `official:asic-register`만 사용할 수 있다. SourceRight 권한 확인은 record마다 호출하지 않고 권한 단위 batch의 앞뒤에서 `collect`와 `public`을 한 번씩 확인하며 네 ACK의 revision이 같아야 한다. 빈 설정은 무사건 성공으로 가장하지 않고 secret-free artifact에 `coverage_unavailable`을 기록한다.

설정은 JSON 배열이 아니라 최상위 필드가 정확히 `schema_version`, `approved_hosts`, `records`인 닫힌 object다. `schema_version`은 `1`이어야 한다. `approved_hosts`의 각 항목은 `hostname`, `issuer_identifier_type`, `issuer_identifier`, `evidence_sha256`만 가지며 64자리 소문자 SHA-256 증빙을 issuer 식별자와 호스트에 고정한다. `records`의 필수 필드는 `country_code`, `issuer_identifier_type`, `issuer_identifier`, `issuer_name`, `source_right_id`, `official_host`, `original_url`, `title`, `original_language`, `filed_at`, `first_observed_at`, `event_family`이다. `issuer_namespace`, `market`, `ticker`, `external_id`, `document_type`만 선택적으로 허용한다. 각 record의 `official_host`는 같은 issuer에 묶인 `approved_hosts` 항목과 정확히 일치해야 하며 사용되지 않은 승인 호스트도 거절한다.

캐나다 호스트는 issuer가 통제하는 IR 호스트의 별도 증빙이 필요하고 하나의 호스트를 서로 다른 issuer에 공유 승인할 수 없다. SEDAR+·ASX·ASIC·data.gov 및 알려진 제3자 포털은 aggregate SourceRight 아래 승인 호스트로 가장할 수 없다. 호주는 `asic.gov.au` 또는 그 하위 공식 호스트만 허용하며 ASX·data.gov·issuer·제3자 호스트는 거절한다. URL은 공개 HTTPS·기본 포트·무자격정보·무fragment여야 하고 query는 기본적으로 전부 금지한다. `x-amz-*`, `x-goog-*`, token·key·signature·credential 계열 query는 자격정보 오류로 명시적으로 차단한다.

수동 링크에서 허용하는 사건 유형은 migration 011의 현재 SourceCoverage와 정확히 같다. 캐나다는 `meeting_and_vote`, `tender_offer_and_mna`, `capital_return`, `board_and_compensation`, 호주는 `board_and_compensation`, `listing_status`다. 날짜 순서는 문자열이 아니라 timezone을 적용한 실제 UTC 시각으로 비교한다. 이 계약은 SEDAR+·ASX 전문 수집 또는 재배포 권한을 뜻하지 않는다.

Production Alpha를 실행하려면 `official:dart`, `official:sec-edgar`, `official:edinet`, `official:companies-house`, `official:ca-issuer-ir`, `official:asic-register`의 6개국 SourceRight를 각각 실제 증빙과 permission scope, 유효기간으로 등록해야 한다. Migration 011의 비한국 `pending` row는 이용허가가 아니다. 보호된 cutover는 공개 문서 존재 여부와 무관하게 이 여섯 connector와 권한을 모두 잠그고 재검사하므로, 빈 문서 국가의 만료·철회·identity mismatch도 공개 승격을 차단한다.

### 사람 감독 brief와 관측

`.github/workflows/global-brief.yml`의 KST 05:45 예약 작업은 후보 bundle만 만들고 공개 brief를 쓰지 않는다. 사람 1명이 Top 5·근거·빈 결과 이유를 승인한 뒤 후보 작업의 `candidate_run_id`와 승인 JSON을 수동 `publish` 작업에 제출해야 한다. 발행 작업은 그 run이 같은 기본 브랜치·같은 code SHA에서 성공했는지, edition과 run ID에 정확히 묶인 후보 artifact가 하나뿐인지 확인한 뒤 bundle의 실제 `sha256(basis)`와 승인 hash·선택 사건 버전을 다시 대조한다. 후보 artifact는 30일, publication receipt는 90일 보존한다. 필요한 secret은 `BSIDE_EDITOR_TOKEN`, `GOVERNANCE_PREVIEW_TOKEN`, API 주소는 `BSIDE_API_BASE_URL` secret 또는 `GOVERNANCE_API_BASE_URL` variable이다.

`.github/workflows/global-alpha-watchdog.yml`은 5분마다 release state, source 상태와 공개 루트를 관측하며 읽기 전용 `GET /ops/release-state`와 `BSIDE_OPS_TOKEN`, `GOVERNANCE_PREVIEW_TOKEN`, `BSIDE_PUBLIC_WEB_URL`을 사용한다. watchdog에는 `BSIDE_ADMIN_TOKEN`이나 `BSIDE_RELEASE_AUTHORIZER_TOKEN`을 제공하지 않는다. 이 Production Alpha의 공개 배포는 web-only다. Telegram은 허가된 내부 신호 읽기에만 남고 outbound는 영구 비활성이므로 `ENABLE_TELEGRAM_DELIVERY=false`와 `ENABLE_GOVERNANCE_DELIVERY=false`를 유지한다.

## 사건 검수

`GET /admin/review-queue`는 `draft + pending + needs_review` 사건을 중요도와 발생일 순으로 반환한다. editor는 원문 근거를 확인한 뒤 `POST /admin/events/{event_id}/review`에서 승인 또는 거절한다.

모든 결정은 직전에 조회한 `expected_updated_at`을 요구한다. 다른 작업이 먼저 row를 변경했다면 `409 stale_event_review`가 발생하므로 최신 값을 다시 확인해야 한다.

현재 review queue의 `updated_at`은 UTC RFC 3339 `YYYY-MM-DDTHH:MM:SSZ` 형식이다. review 요청의 `expected_updated_at`에는 직전 조회에서 받은 값을 그대로 보낸다.

- 거절: `decision=reject`, 8자 이상의 `reason`만으로 가능하며 사건은 계속 비공개다.
- 승인: 행위·대상·당사자·효력일·선택 기한으로 canonical identity를 완성하고, 중요도·요약·현재 상태를 입력한다.
- 수집된 사건이 `unclassified`라면 승인 요청에 8개 공개 유형 중 하나인 `event_family`를 반드시 입력한다.
- 승인에는 현재 공개 가능한 공식 근거가 최소 1개 필요하다.
- 동일 identity 후보가 있어도 서버는 자동 병합하지 않는다. 먼저 `409 event_comparison_key_conflict`와 `conflicting_event_id`, `merge_requires_explicit_target=true`를 반환한다.
- 사람이 해당 사건이 정말 동일하다고 확인한 경우에만 같은 승인 payload에 `merge_into_event_id=conflicting_event_id`를 명시한다. 성공 응답은 `decision=merged`, `published=false`, `canonical_event_id`를 반환하며 원래 후보는 비공개 merged 상태가 된다.
- 충돌하는 actor identity, 만료·철회된 근거는 승인할 수 없다.
- 승인·거절 이유와 editor 역할은 내부 editorial revision으로 기록한다.

## Brief 발행

`GET /admin/brief-candidates`는 이미 사람 검수를 통과하고 현재 공개 가능한 사건을 최대 100건 반환한다. `POST /admin/briefs`는 `edition + cutoff_at`별 불변 발행본을 만든다.

- lane은 `top|watch|deadline`이다.
- Top은 최대 5건이며 모든 Top 사건은 공식 근거가 1개 이상 있어야 한다.
- Watch와 deadline은 각각 최대 50건이다.
- 같은 event는 한 발행본에서 하나의 lane에만 들어갈 수 있다. Top·Watch·deadline 중복은 API와 DB unique constraint가 모두 거절한다.
- Top이 0건이면 `empty_reason`이 필수다.
- 국가 edition의 `no_confirmed_material_events`는 해당 국가에 `public_ready=true` connector가 하나 이상 있을 때만 승인된다. global edition에서는 6개국 각각에 `public_ready=true` connector가 하나 이상 있어야 한다. 그 조건을 충족하지 못하면 `coverage_unavailable`만 사용할 수 있다.
- 반대로 필요한 모든 국가가 준비된 상태에서 `coverage_unavailable`을 발행할 수 없다.
- 국가 edition에는 같은 국가 사건만 넣을 수 있다.
- 각 item에는 선정 이유와 발행 시점 event snapshot을 저장한다.
- 같은 edition·cutoff·semantic payload의 재요청은 멱등 응답을 돌려준다.
- 같은 edition·cutoff를 다른 내용으로 덮어쓰지 않으며 `409 brief_edition_conflict`로 거절한다.

따라서 공개 `/briefs/latest`는 작성 후 사건 row가 바뀌어도 당시 제목·상태·근거 수를 임의로 다시 계산하지 않는다. 현재 공개 자격만 재검증하며, 최신 발행본을 안전하게 제공할 수 없으면 과거 발행본으로 되돌아가지 않고 `coverage_unavailable`을 명시한다.

## 응답 예산과 페이지

- JSON, CSV, Atom 데이터 응답: 최대 250,000바이트
- 목록 기본 크기: 25건
- 목록 1회 최대 크기: 100건
- JSON/CSV/Atom export: 필터 결과 중 최대 100건
- event detail의 documents와 observations: 각각 최대 100건
- issuer detail의 recent events: 최대 50건

목록 JSON은 `meta.page`, `meta.limit`, `meta.returned`, `meta.has_more`, `meta.next_page`를 제공한다. 제한을 넘는 응답은 일부 필드를 조용히 누락하지 않고 `response_budget_exceeded`로 실패한다.

모든 JSON 응답에는 `api_version=v2`가 들어간다. CSV와 Atom에는 `X-BSIDE-API-Version: v2` 응답 헤더를 사용한다.

## 사건 유형

v2가 공개하는 `event_family`는 다음 8개다.

1. `large_ownership`
2. `meeting_and_vote`
3. `tender_offer_and_mna`
4. `capital_issuance`
5. `capital_return`
6. `board_and_compensation`
7. `listing_status`
8. `correction_and_withdrawal`

## 문서 의미 해시 계약

글로벌 수집기의 `content_hash`는 단순 본문 해시가 아니다. 서버는 첫 DB 쓰기 전에 다음 의미 필드를 정규화해 Python 수집기와 동일한 정렬 JSON으로 SHA-256을 다시 계산한다.

- 문서·외부·발행사 ID와 전체 `issuer_reference`
- 국가, source key, SourceRight ID, 서버에 등록된 source type
- record kind, 문서 유형, 사건 유형, 원문 제목·언어
- 접수시각, 원문 URL, 본문, 정정 대상, 변경 유형
- canonical metadata와 `public_allowed`, `ai_allowed`

수집 시도 시각인 `first_observed_at`만 의미 해시에서 제외한다. metadata 루트는 객체여야 하며 float를 허용하지 않고, 정수는 signed 64-bit 범위, 깊이는 12, 전체 노드는 5,000 이하로 제한한다. PHP associative decoder의 한계 때문에 중첩된 빈 객체와 빈 배열은 해시에서 모두 `[]`로 정규화한다.

생산자 해시가 서버 재계산값과 다르면 쓰기 전에 `400 global_ingest_validation_failed`로 거절한다. 해시는 같지만 이미 저장된 핵심 필드가 다르면 저장 row 손상 또는 계약 위반으로 보고 `409 global_document_hash_contract_conflict`를 반환한다. 해시가 정상적으로 달라진 문서는 기존 문서를 덮어쓰지 않고 새 버전과 정정 체인으로 저장한다.

이 API는 투자 추천, 목표가, 매수·매도 판단 또는 국가 간 수집 완전성 보장을 제공하지 않는다.

## 제목 출처와 Alpha 최소 수집 범위

공개 사건의 `title_provenance`은 `source`, `generated_metadata`,
`operator_metadata` 중 하나다. `source`는 소스가 제공한 제목을 변환 없이
보존했다는 뜻이다. `generated_metadata`는 SEC daily master index처럼 원문
제목이 없는 발견 자료에서 문서 유형과 회사명으로 만든 표제이며 화면에서 원문
제목으로 표시하지 않는다. `operator_metadata`는 허용된 CA/AU 공식 링크를
사람이 등록할 때 입력한 표제다.

checkpoint가 없을 때 최근 2일에서 시작하는 자동 수집 계약은 그대로 유지한다.
다만 Production Alpha 전환 게이트는 DART·SEC EDGAR·EDINET·Companies House
각각에서 동일 SHA의 최근 30일 이상 수집, 30개 이상 성공 window, 실패 window
0개, 증빙 기준 시각 24시간 이내의 마지막 완료를 별도로 요구한다.
