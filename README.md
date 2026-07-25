# BSIDE Governance Intelligence

BSIDE는 한국 기업의 거버넌스 사건을 **공식 공시, 당사자 주장, 보도, 결과, 후속 이행**까지 연결해 확인할 수 있게 만드는 공개 기록 시스템이다. 뉴스의 양보다 사건 단위 정리, 원문 보존, 근거 추적, 정정 이력과 안정적인 전달을 우선한다.

이 저장소는 기존 Python 수집기, PHP/MySQL API, GitHub Pages를 유지하면서 거버넌스 데이터 모델과 `/api/v1`, 공식 공시 수집, 이용권한 통제, 편집 검수, DeliveryOutbox, 공개 UI를 단계적으로 전환하는 구현 브랜치다. 현재 구현 범위와 운영 전 필수 작업은 [구현 상태와 전환 체크리스트](docs/implementation-status.md)에 정리했다.

## 제품 원칙

- MySQL을 운영 데이터의 단일 기준으로 사용한다.
- DART·KIND와 당사자 공식 자료를 일차 근거로 삼고 뉴스와 허가된 Telegram은 발견·맥락 보강에 사용한다.
- 제목과 본문은 원문 언어를 보존한다. 화면 메뉴와 상태·분류 필드만 한국어·영어를 병기한다.
- Telegram-only 정보는 `signal`로 남기며 공식 또는 독립 근거가 생기기 전 핵심 사건으로 자동 공개하지 않는다.
- 상위·시장 민감 사건은 편집자 승인 전 공개하지 않는다.
- 주주인증, 공동보유, 위임·투표, 종목 추천 기능은 구현하지 않는다.

## 구성

```text
DART / KIND / official statements
                 |
licensed Telegram + media discovery
                 |
          normalize and link
                 |
     MySQL governance entities
                 |
 editorial review + web-only release guard
                 |
  /api/v1 + Pages UI + RSS/CSV/JSON
```

주요 구현은 다음 위치에 있다.

- `curator/governance.py`: 회사, 문서, 사건, 캠페인, 근거, 투표, 약속·이행 모델
- `curator/official_ingest.py`, `curator/official_sources.py`: DART·KIND 증분 자동 수집과 KIND 권한 preflight
- `curator/source_rights.py`: 수집·AI·재배포 권한의 fail-closed 판정
- `curator/governance_publisher.py`, `curator/publish_outbox.py`: 과거 호환 surface를 유지하되 모든 outbound 경로를 web-only 정책으로 영구 차단
- `deploy/activist/governance_v1.php`, `deploy/activist/openapi.yaml`: 공개·관리 API 계약
- `public/governance/`: 접근 가능한 공개 UI
- `.github/workflows/`: 수집, 링크 해결, 발행, 일일 배포, 감시, CI 분리

## 로컬 준비와 검증

Python 3.12를 기준으로 한다.

```bash
python -m venv .venv
python -m pip install -r requirements.txt
pytest -q
python -m curator.governance_ui --root .
```

공개 UI는 Node.js 22와 Chromium으로 실제 주요 흐름을 검증한다.

```bash
npm ci
npx playwright install chromium
npm run test:web-vitals-probe
npm run test:ui
```

UI E2E는 데스크톱·모바일 사용자 여정, 역할·라벨 기반 WCAG 2.2 AA 자동 검사, 원문 언어 표시, 비공개 피드백, 250KB 전송 예산과 LCP 2.5초·INP 200ms·CLS 0.1 예산을 확인한다. 로컬 synthetic 측정은 회귀 차단용이며 정식 공개 전환에는 production RUM/가용성 증빙을 별도로 제출한다.

품질 게이트는 GitHub Actions에서 실패를 허용하지 않는다.

```bash
ruff check curator tests
mypy curator/governance.py curator/governance_ui.py curator/source_rights.py curator/link_discovery.py curator/editorial_ingest.py curator/governance_publisher.py curator/release_gate.py --ignore-missing-imports --follow-imports=skip --check-untyped-defs --disallow-untyped-defs --no-implicit-optional --warn-redundant-casts --warn-unused-ignores
pip-audit --requirement requirements.txt
```

타입 검사는 신규 거버넌스 핵심 모듈부터 강제하고 있으며, 기존 수집기 전체는 동작 변경 없이 점진적으로 범위를 넓힌다.

## 운영 설정

민감값은 저장소에 넣지 않는다. 주요 GitHub Secret은 다음과 같다.

```text
ACTIVIST_API_URL
ACTIVIST_API_SECRET
BSIDE_API_BASE_URL
BSIDE_OPS_TOKEN
BSIDE_ADMIN_TOKEN
BSIDE_RELEASE_AUTHORIZER_TOKEN
BSIDE_EDITOR_TOKEN
GOVERNANCE_PREVIEW_TOKEN
DART_API_KEY
KIND_API_KEY
EDINET_API_KEY
COMPANIES_HOUSE_API_KEY
OFFICIAL_SITE_ALLOWLIST_B64
CURATOR_FEEDS
STORY_REVIEW_ACCESS_TOKEN
TELEGRAM_ADMIN_ACCESS_TOKEN
TELEGRAM_API_ID
TELEGRAM_API_HASH
TELEGRAM_SESSION_STRING
```

`BSIDE_RELEASE_AUTHORIZER_TOKEN`은 repository 공용 Secret이 아니라 reviewer가 보호하는 `governance-release` environment에만 둔다. PHP에는 정확한 `release_authorizer` 역할의 SHA-256 hash로 등록하며, 일반 admin token은 승인 발급을 대신할 수 없다. 5분 `global-alpha-watchdog.yml`은 읽기 전용 `BSIDE_OPS_TOKEN`만 사용하고 admin·release-authorizer token을 받지 않는다.

`KIND_API_KEY`는 일반 수집에서는 내부 KIND 어댑터가 인증을 요구할 때만 쓰지만,
승인 직후 실행하는 수동 `kind-adapter-preflight.yml`은 운영 설정 완전성을 확인하기
위해 endpoint와 key를 모두 필수로 요구한다.

현재 제품 정책은 Telegram 채팅으로 콘텐츠를 발송하지 않는 것이다. `ENABLE_TELEGRAM_DELIVERY=false`, `config.yaml`의 `telegram.enabled=false`, 빈 `telegram.chat_id`를 함께 유지하며, Python sender·로컬/원격 outbox·PHP enqueue/claim·Actions worker가 모두 코드 수준에서 거절하므로 runtime 값이나 수동 입력으로 재활성화할 수 없다. `TELEGRAM_API_ID`·`TELEGRAM_API_HASH`·`TELEGRAM_SESSION_STRING`을 이용한 허가 공개 채널 읽기 수집은 이 발송 정책과 분리되어 계속 운영한다. 비공개 Telegram 관리자 채팅과 `TELEGRAM_ADMIN_CHAT_ID`도 사용하지 않으며, 관리자는 고정 URL `https://news.bside.ai/feed/telegram-admin.html`에서 `TELEGRAM_ADMIN_ACCESS_TOKEN`을 직접 입력한다.

주요 Repository variable은 `ACTIVIST_PUBLIC_API_URL`, `GOVERNANCE_API_BASE_URL`, `BSIDE_PUBLIC_WEB_URL`, `KIND_DISCLOSURE_ENDPOINT`, `SEC_EDGAR_USER_AGENT`, `COMPANIES_HOUSE_ISSUERS_JSON`, `CA_OFFICIAL_LINKS_JSON`, `AU_OFFICIAL_LINKS_JSON`, `PAGES_OWNER=legacy|governance`, `GOVERNANCE_PIPELINE_MODE=off|dart_canary|shadow|live`다. 이전 boolean은 전환기 어댑터로만 읽으며 충돌하면 fail-closed한다. `ENABLE_TELEGRAM_DELIVERY=false`와 `ENABLE_GOVERNANCE_DELIVERY=false`는 유지하지만 어떤 runtime 값도 outbound를 다시 활성화할 수 없다. 전체 목록과 예약 시각은 [운영 자동화 문서](docs/operations-automation.md)를 따른다.

### 글로벌 터미널 Production Alpha

글로벌 `/api/v2`와 신규 루트는 완성된 품질 인증판이 아니라 **Production Alpha**다. 한국·미국·일본은 허용된 공식 공시 범위, 영국은 Companies House 등록부 범위로 제공한다. 캐나다·호주는 자동 수집 범위가 아니라 승인된 공식 링크의 `link-only / manual-metadata` 범위다. SEDAR+·ASX 원문을 저장하거나 재배포하지 않으며 국가별 범위를 `/api/v2/sources/status`에 표시한다.

신규 운영 workflow는 다음과 같다.

- `ingest-global.yml`: SEC EDGAR Latest Filings Atom 당일 증분과 일일 인덱스 완결성 대조, EDINET, Companies House를 매시 17분·47분에 수집한다. 예약 request budget은 US 200, JP·GB 100이다. SEC 당일 feed나 durable source cursor가 유효하지 않으면 US는 `live_ready=false`로 fail-closed한다. 날짜 입력이 없으면 MySQL connector checkpoint에서 하루 overlap을 두고 최대 31일씩 누락 구간을 이어서 처리하며, Companies House allowlist는 최대 50개 회사다.
- `ingest-selected-markets.yml`: `CA_OFFICIAL_LINKS_JSON`과 `AU_OFFICIAL_LINKS_JSON`의 수동 승인 링크 metadata를 매시 07분·37분에 검증·적재한다. 설정 URL에 네트워크 요청을 보내거나 본문을 저장하지 않는다. 캐나다는 issuer 식별자에 묶인 별도 호스트 증빙이 필요하고, 호주는 `asic.gov.au` 공식 호스트만 허용한다.
- `global-brief.yml`: KST 05:45 예약 실행은 검수 후보 artifact만 만든다. 공개 brief는 사람이 승인한 동일 SHA payload를 `workflow_dispatch`의 `publish` 작업으로 전달할 때만 생성한다.
- `global-alpha-watchdog.yml`: 5분마다 `BSIDE_OPS_TOKEN`과 읽기 전용 release-state 경로로 API 상태, source freshness와 공개 루트를 관측한다.
- `governance-cutover.yml`, `governance-rollback.yml`: 보호된 `governance-release` 환경에서만 수동 전환·복구한다. Alpha evidence는 exact daily Pages run/artifact/digest와 전체 사이트·UI/config content identity를 고정하며, 24시간 preview 관측이 같은 terminal 바이트임을 증명한다. 전환은 evidence가 가리키는 그 artifact만 허용하고 exact SHA·evidence artifact digest·v1/v2 state version에 묶인 짧은 일회용 승인을 발급한 뒤 두 API state를 한 transaction에서 승격한다.

Migration 011은 미국·일본·영국·캐나다·호주 권한을 `pending`으로만 만든다. 이것은 이용허가가 아니다. 공개 전 다음 6개 SourceRight가 실제 증빙·권한 범위·유효기간과 함께 등록되어야 한다.

`official:dart`, `official:sec-edgar`, `official:edinet`, `official:companies-house`, `official:ca-issuer-ir`, `official:asic-register`

공식 수집은 `collect` 자격을 실행 전과 실행 도중 다시 검사하며, 공개 검수·전환 시에는 재배포 가능한 현재 권한을 다시 확인한다. 캐나다·호주 수동 링크 metadata는 `collect`와 `public` 자격의 revision이 일치해야 하고 국가별 최대 50개 issuer만 허용한다. 캐나다의 SEDAR+·ASX·ASIC·data.gov 및 제3자 포털, 호주의 ASX·data.gov·issuer 임의 호스트는 승인 목록으로 가장할 수 없으며 모든 URL query를 거절한다. 빈 설정은 정상적인 무사건으로 위장하지 않고 `coverage_unavailable` 증빙을 남긴다.

Production Alpha에서도 Telegram은 허가 채널의 내부 신호 수집에만 사용한다. 공개 화면·브리프·workflow 어디에서도 outbound Telegram 발송을 수행하지 않으며 `ENABLE_TELEGRAM_DELIVERY=false`, `ENABLE_GOVERNANCE_DELIVERY=false`를 계속 유지한다.

### 미디어 발견 피드 범위

운영 뉴스 수집은 네트워크 요청 전에 fail-closed 범위 정책을 적용한다. `korean_governance` 또는 `korean_governance_context`로 승인되지 않은 거시경제·산업·STO/ISA/IB·비한국 해외 행동주의 피드는 가져오지 않는다. Google News는 계속 발견 큐로만 사용하고 원문 제목과 언어를 바꾸지 않는다. 분류 기준과 비공개 피드 JSON 형식은 [미디어 발견 피드 범위 정책](docs/media-source-scope-policy.md)에 있다.

### Telegram 이용권한

`config.yaml`의 `telegram:activistkorea` 레코드는 형식과 차단 동작을 보여 주는 **`pending` 자리표시자**다. 증빙이 없으므로 수집, AI 입력, 재배포에 사용할 수 없다. 이를 `active`로 바꾸거나 임의 증빙을 넣어 우회하지 않는다.

운영 권한의 기준은 MySQL `SourceRight`와 역할이 제한된 `POST /api/v1/admin/source-rights`다. 권한 범위, 증빙 참조·해시, 유효기간, 철회일, AI·재배포 허용 여부를 등록한 뒤에만 처리한다. 원격 권한 레코드는 실행 시 로컬 자리표시자보다 우선하며, 만료·철회된 소스와 파생 공개 데이터는 차단된다.

구두 승인은 [Telegram 채널 정보 이용 구두 승인 사실 확인서](docs/telegram-channel-verbal-permission-confirmation-ko.md)로 채널·허락자·범위·유효기간·철회 방법을 기록한다. 작성 완료본은 비공개 증빙 보관소에 두고 URI 또는 SHA-256만 `SourceRight`에 등록한다.

## 공식 공시 증분 수집과 백필

단일 증분 실행은 운영 workflow가 담당한다. 2021년 이후 백필은 먼저 짧은 dry-run으로 확인한 뒤 고정된 날짜 범위로 실행한다.

```powershell
.\.venv\Scripts\python.exe -m curator.official_backfill `
  --from-date 2021-01-01 --to-date 2021-01-15 `
  --source dart --chunk-days 7 --max-chunks 1 --dry-run

.\.venv\Scripts\python.exe -m curator.official_backfill `
  --from-date 2021-01-01 --to-date 2026-01-01 `
  --source both --chunk-days 14 --max-pages 100 --max-chunks 10
```

완료 청크만 체크포인트에 기록되며 같은 명령을 다시 실행하면 이어서 처리한다. 페이지 상한 때문에 결과가 잘리면 성공으로 간주하지 않는다. 세부 계약은 [공식 공시 백필과 품질 릴리스 게이트](docs/official-backfill-and-quality-gates.md)에 있다.

## 편집 엔터티 입력과 검수

행동주주·기관, 캠페인, 주장·반론, 의안 표결, 약속·이행, 타임라인은 UTF-8 JSON bundle로 검증한 뒤 HMAC API에 입력한다. 입력 단계에서는 actor와 관계가 `inactive/pending`, 나머지 엔터티가 `pending/draft`로 강제되며 공개 가능한 근거 문서 ID가 필요하다. 제목·주장·요구·설명은 번역하거나 공백을 정규화하지 않고, DB 한도를 넘는 문자열은 자르지 않고 거부한다.

```powershell
.\.venv\Scripts\python.exe -m curator.editorial_ingest `
  --bundle data/editorial/campaign-example.json --dry-run

.\.venv\Scripts\python.exe -m curator.editorial_ingest `
  --bundle data/editorial/campaign-example.json --chunk-size 100
```

입력 형식은 [편집 bundle JSON Schema](docs/schemas/editorial-ingest-bundle.schema.json)에 고정돼 있다. 서버는 청크 멱등성을 확인하고, 역할이 제한된 검수 API에서 근거와 이용권한을 다시 확인한 뒤 승인된 엔터티만 공개한다.

## 사람 라벨 품질 게이트

정식 전환에는 실제 사람 라벨의 article pair 500개 이상과 사건 300개 이상이 필요하다.

```powershell
$revision = (git rev-parse HEAD).Trim()
.\.venv\Scripts\python.exe -m curator.quality_benchmark `
  --same-story data/benchmarks/same_story_pairs.jsonl `
  --relevance data/benchmarks/relevance_events.jsonl `
  --environment production `
  --code-revision $revision
```

기본 릴리스 기준은 동일 사건 묶음 precision 0.97 이상, 핵심 사건 relevance recall 0.95 이상이다. 저장소의 작은 fixture는 CLI와 스키마 검증용이며 릴리스 증빙으로 사용할 수 없다.

최종 공개 전환은 benchmark만으로 결정하지 않는다. [Shadow 비교와 공개 전환 게이트](docs/release-transition-gate.md)가 14일 비교, 최근 7일 운영·성능 지표, 같은 코드 리비전의 사람 라벨 결과를 함께 fail-closed 판정한다.

## API와 공개 화면

공개 API는 회사, 사건, 캠페인, 문서, 캘린더, 검색, Atom, CSV·JSON, 비공개 피드백 접수를 제공한다. 관리자 API는 서버 측 역할 토큰을 요구한다. 기존 `?action=search|articles|reports|telegram_dashboard`와 `feed.xml`은 전환 뒤 90일 동안 어댑터로 유지한다.

- [API 운영 계약](docs/governance-api-v1.md)
- [OpenAPI 문서](deploy/activist/openapi.yaml)
- [공개 UI](public/governance/index.html)

Pages는 `main`에 생성 HTML, `state.json`, 아카이브를 커밋하지 않고 artifact로 배포한다. 운영 장애 시 이전 Pages artifact로 되돌리되 신규 DB 데이터는 보존한다.

## 문서

- [구현 상태와 전환 체크리스트](docs/implementation-status.md)
- [운영 자동화](docs/operations-automation.md)
- [공식 공시 백필과 품질 릴리스 게이트](docs/official-backfill-and-quality-gates.md)
- [KIND SourceRight와 수동 adapter preflight](docs/kind-source-right-preflight.md)
- [사용자·법률 검증 실행 가이드](docs/human-usability-and-legal-validation.md)
- [KIND 연동 결정과 외부 선행조건](docs/kind-integration-decision-2026-07-16.md)
- [KRX KIND 공시 데이터 이용 문의 초안](docs/krx-kind-data-inquiry-ko.md)
- [Shadow 비교와 공개 전환 게이트](docs/release-transition-gate.md)
- [레거시 Pages 90일 호환·복구 자산 보존](docs/legacy-recovery-retention.md)
- [Governance API v1 운영 계약](docs/governance-api-v1.md)
- [Telegram 공개 채널 운영 정책](docs/telegram-public-channels.md)
- [2026-07-16 운영 기반 반영 기록](docs/production-foundation-deployment-2026-07-16.md)

## 안전 경계

이 서비스는 사실·근거·상태를 기록하며 매수·매도 권고, 목표가, 종목 추천을 제공하지 않는다. 공개 전 법률 검토가 필요한 투자 권고 경계, 이해상충, 저작권·이용허가, 정정·당사자 답변 절차는 별도 운영 정책과 함께 승인해야 한다.
